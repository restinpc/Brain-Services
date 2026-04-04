"""
cache_helper.py — кеш /values для всех brain-* микросервисов.

Логика на каждый запрос /values:
  1. SELECT → если в кеше есть → вернуть сразу (cache HIT)
  2. Вычислить через compute_fn() (cache MISS)
  3. INSERT IGNORE — атомарная защита от гонки и дублей
  4. Вернуть результат

Исправления v3:
  - conn.invalidate() при InternalError — повреждённое соединение
    уничтожается, а не возвращается в пул. Именно это устраняет
    "Packet sequence number wrong" — retry на испорченном пуле бесполезен.
  - Retry при OperationalError 1205 (Lock wait timeout) в _cache_set —
    параллельные INSERT IGNORE на одну строку дерутся за InnoDB gap-lock;
    1–2 retry с коротким jitter решают это без потери данных.
  - Семафор инициализируется через asyncio.Lock, чтобы не было
    race condition при создании в разных корутинах одновременно.
"""

import asyncio
import hashlib
import json
import logging
import random
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError

log = logging.getLogger(__name__)

# ── Семафор ───────────────────────────────────────────────────────────────────
_DB_SEM: asyncio.Semaphore | None = None
_DB_SEM_LOCK: asyncio.Lock | None = None


def _get_sem_lock() -> asyncio.Lock:
    global _DB_SEM_LOCK
    if _DB_SEM_LOCK is None:
        _DB_SEM_LOCK = asyncio.Lock()
    return _DB_SEM_LOCK


async def get_db_sem() -> asyncio.Semaphore:
    global _DB_SEM
    if _DB_SEM is not None:
        return _DB_SEM
    async with _get_sem_lock():
        if _DB_SEM is None:
            # 30 одновременных cache-операций при pool_size=20+max_overflow=20=40
            _DB_SEM = asyncio.Semaphore(30)
    return _DB_SEM


# ── DDL ────────────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS vlad_values_cache (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    service_url VARCHAR(255) NOT NULL,
    pair        TINYINT      NOT NULL,
    day_flag    TINYINT      NOT NULL,
    date_val    DATETIME     NOT NULL,
    params_hash CHAR(32)     NOT NULL,
    params_json TEXT         NOT NULL,
    result_json TEXT         NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_cache (service_url(100), pair, day_flag, date_val, params_hash),
    INDEX idx_lookup   (service_url(100), pair, day_flag, params_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

_DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%d-%m %H:%M:%S",
    "%Y-%d-%m %H:%M:%S.%f",
    "%Y-%m-%d",
)

# MySQL error codes
_LOCK_WAIT_TIMEOUT = 1205
_DEADLOCK          = 1213


def _parse_dt(s: str) -> datetime | None:
    stripped = s.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(stripped, fmt)
        except ValueError:
            continue
    log.error(f"_parse_dt: не удалось распарсить дату: repr={s!r} (stripped={stripped!r})")
    return None


def cache_hash(params: dict) -> str:
    """MD5 от канонического JSON параметров."""
    return hashlib.md5(
        json.dumps(params, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


async def ensure_cache_table(engine_vlad: AsyncEngine) -> None:
    async with engine_vlad.connect() as conn:
        result = await conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = 'vlad_values_cache'
        """))
        exists = result.scalar() > 0

    if not exists:
        async with engine_vlad.begin() as conn:
            await conn.execute(text(_DDL))
        log.info("✅ vlad_values_cache создана")
    else:
        log.debug("vlad_values_cache уже существует")


async def load_service_url(engine_super: AsyncEngine, service_id: int) -> str:
    async with engine_super.connect() as conn:
        row = (await conn.execute(
            text("SELECT url FROM brain_service WHERE id = :sid"),
            {"sid": service_id},
        )).fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"URL для SERVICE_ID={service_id} не найден в brain_service")
    url = row[0].rstrip("/")
    log.info(f"  SERVICE_URL (id={service_id}): {url}")
    return url


async def _cache_get(engine_vlad: AsyncEngine, service_url: str,
                     pair: int, day: int, date_val: datetime,
                     p_hash: str) -> dict | None:
    """
    SELECT из кеша.

    При InternalError ("Packet sequence number wrong" и аналогичных):
      — conn.invalidate() удаляет повреждённое соединение из пула навсегда,
        вместо того чтобы вернуть его туда (что приводило к лавине ошибок).
      — Один retry открывает свежее соединение из пула / создаёт новое.
    """
    params = {"url": service_url, "pair": pair, "day": day, "dv": date_val, "ph": p_hash}
    query  = text("""
        SELECT result_json FROM vlad_values_cache
        WHERE service_url = :url AND pair = :pair
          AND day_flag = :day AND date_val = :dv AND params_hash = :ph
        LIMIT 1
    """)

    async def _attempt() -> dict | None:
        async with engine_vlad.connect() as conn:
            try:
                row = (await conn.execute(query, params)).fetchone()
                return json.loads(row[0]) if row else None
            except InternalError:
                # Инвалидируем соединение ДО выхода из блока async with,
                # иначе оно вернётся в пул повреждённым → следующий запрос
                # тоже упадёт, и так по цепочке.
                await conn.invalidate()
                raise

    sem = await get_db_sem()
    async with sem:
        try:
            return await _attempt()
        except InternalError as e:
            log.warning(f"cache_get InternalError attempt 1, retry: {e}")
            try:
                return await _attempt()
            except Exception as e2:
                log.warning(f"cache_get outer error: {e2}")
                return None
        except Exception as e:
            log.warning(f"cache_get outer error: {e}")
            return None


async def _cache_set(engine_vlad: AsyncEngine, service_url: str,
                     pair: int, day: int, date_val: datetime,
                     params: dict, p_hash: str, result: dict) -> None:
    """
    INSERT IGNORE в кеш.

    При InternalError — инвалидируем соединение + retry.
    При OperationalError 1205/1213 (Lock wait timeout / Deadlock) —
      короткий jitter-sleep + retry (параллельные INSERT IGNORE на одну
      строку вызывают InnoDB gap-lock contention).
    """
    insert_sql = text("""
        INSERT IGNORE INTO vlad_values_cache
            (service_url, pair, day_flag, date_val,
             params_hash, params_json, result_json)
        VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
    """)
    row_params = {
        "url":  service_url, "pair": pair, "day":  day,
        "dv":   date_val,    "ph":   p_hash,
        "pj":   json.dumps(params, sort_keys=True, ensure_ascii=False),
        "rj":   json.dumps(result, ensure_ascii=False),
    }

    async def _attempt() -> None:
        async with engine_vlad.begin() as conn:
            try:
                await conn.execute(insert_sql, row_params)
            except InternalError:
                await conn.invalidate()
                raise

    sem = await get_db_sem()
    async with sem:
        for attempt in range(1, 4):  # максимум 3 попытки
            try:
                await _attempt()
                return
            except InternalError as e:
                if attempt < 3:
                    log.warning(f"cache_set InternalError attempt {attempt}, retry: {e}")
                    await asyncio.sleep(0.05 * attempt)
                else:
                    log.warning(f"cache_set outer error: {e}")
                    return
            except OperationalError as e:
                orig_args = getattr(e.orig, "args", (None,))
                err_code  = orig_args[0] if orig_args else None
                if err_code in (_LOCK_WAIT_TIMEOUT, _DEADLOCK) and attempt < 3:
                    # Небольшой jitter чтобы параллельные запросы не retry-лись синхронно
                    delay = 0.1 * attempt + random.uniform(0, 0.05)
                    log.warning(
                        f"cache_set attempt {attempt} failed (lock/deadlock), "
                        f"retry in {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    log.debug(f"cache_set (ok if duplicate): {e}")
                    return
            except Exception as e:
                log.debug(f"cache_set (ok if duplicate): {e}")
                return


async def cached_values(
    engine_vlad:  AsyncEngine,
    service_url:  str,
    pair:         int,
    day:          int,
    date:         str,
    extra_params: dict,
    compute_fn,
    node:         str = "",
) -> dict:
    """
    Универсальная обёртка для endpoint /values.
    Возвращает готовый FastAPI-совместимый dict.
    """
    from common import ok_response, err_response

    date_val = _parse_dt(date)
    if date_val is None:
        msg = f"Invalid date format: {date!r}"
        log.error(f"cached_values: {msg} | pair={pair} day={day} params={extra_params} node={node}")
        return err_response(msg)

    p_hash = cache_hash(extra_params)

    # ── 1. Cache HIT ──────────────────────────────────────────────────────────
    cached = await _cache_get(engine_vlad, service_url, pair, day, date_val, p_hash)
    if cached is not None:
        log.debug(f"HIT  pair={pair} day={day} date={date} params={extra_params}")
        return ok_response(cached)

    # ── 2. Вычисляем ──────────────────────────────────────────────────────────
    log.debug(f"MISS pair={pair} day={day} date={date} params={extra_params}")
    result = await compute_fn()

    if result is None:
        msg = f"Computation returned None | date={date!r} pair={pair} day={day} params={extra_params}"
        log.error(f"cached_values: {msg} node={node}")
        return err_response(
            f"Computation failed (check date or params): date={date!r} params={extra_params}"
        )

    # ── 3. INSERT IGNORE — атомарная защита от гонки и дублей ────────────────
    await _cache_set(engine_vlad, service_url, pair, day, date_val,
                     extra_params, p_hash, result)

    return ok_response(result)
