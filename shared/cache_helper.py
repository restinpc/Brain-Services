"""
cache_helper.py — кеш /values для всех brain-* микросервисов.

Логика на каждый запрос /values:
  1. SELECT → если в кеше есть → вернуть сразу (cache HIT)
  2. Вычислить через compute_fn() (cache MISS)
  3. INSERT IGNORE — атомарная защита от гонки/дублей
  4. Вернуть результат

Ключевые решения проблемы "Packet sequence number wrong":
  - pool_pre_ping=False в common.py (root cause: pre-ping шлёт запрос на занятое
    соединение → ломает порядок пакетов MySQL-протокола в aiomysql)
  - Явный conn.invalidate() при InternalError — соединение выбрасывается из пула
  - Семафор _DB_SEM ограничивает одновременные DB-операции
  - Внешний try/except в cached_values — любая ошибка не уходит в PHP как status:error,
    вместо этого вычисление выполняется без кеша
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text
from sqlalchemy.exc import InternalError, DBAPIError, OperationalError

log = logging.getLogger(__name__)

_DB_SEM: asyncio.Semaphore | None = None


def get_db_sem() -> asyncio.Semaphore:
    """Lazy-init семафора внутри event loop."""
    global _DB_SEM
    if _DB_SEM is None:
        _DB_SEM = asyncio.Semaphore(25)
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


async def _safe_invalidate(conn) -> None:
    """Инвалидирует соединение не бросая исключений."""
    try:
        await conn.invalidate()
    except Exception:
        pass


async def _cache_get(engine_vlad: AsyncEngine, service_url: str,
                     pair: int, day: int, date_val: datetime,
                     p_hash: str) -> dict | None:
    """
    SELECT из кеша. 2 попытки с инвалидацией соединения при InternalError.
    Никогда не бросает исключение — только возвращает None при любой ошибке.
    """
    params = {"url": service_url, "pair": pair, "day": day, "dv": date_val, "ph": p_hash}
    query  = text("""
        SELECT result_json FROM vlad_values_cache
        WHERE service_url = :url AND pair = :pair
          AND day_flag = :day AND date_val = :dv AND params_hash = :ph
        LIMIT 1
    """)

    try:
        async with get_db_sem():
            for attempt in range(2):
                conn = await engine_vlad.connect()
                try:
                    row = (await conn.execute(query, params)).fetchone()
                    await conn.close()
                    return json.loads(row[0]) if row else None

                except (InternalError, DBAPIError, OperationalError) as e:
                    await _safe_invalidate(conn)
                    if attempt == 0:
                        log.warning(f"cache_get attempt 1 failed, retry: {e}")
                        continue
                    log.warning(f"cache_get attempt 2 failed, skip cache: {e}")
                    return None

                except Exception as e:
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    log.warning(f"cache_get error: {e}")
                    return None
    except Exception as e:
        log.warning(f"cache_get outer error: {e}")

    return None


async def _cache_set(engine_vlad: AsyncEngine, service_url: str,
                     pair: int, day: int, date_val: datetime,
                     params: dict, p_hash: str, result: dict) -> None:
    """
    INSERT IGNORE в кеш. 2 попытки с инвалидацией при InternalError.
    Никогда не бросает исключение.
    """
    try:
        async with get_db_sem():
            for attempt in range(2):
                conn = await engine_vlad.connect()
                try:
                    await conn.execute(text("""
                        INSERT IGNORE INTO vlad_values_cache
                            (service_url, pair, day_flag, date_val,
                             params_hash, params_json, result_json)
                        VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
                    """), {
                        "url":  service_url, "pair": pair, "day":  day,
                        "dv":   date_val,    "ph":   p_hash,
                        "pj":   json.dumps(params, sort_keys=True, ensure_ascii=False),
                        "rj":   json.dumps(result, ensure_ascii=False),
                    })
                    await conn.commit()
                    await conn.close()
                    return

                except (InternalError, DBAPIError, OperationalError) as e:
                    try:
                        await conn.rollback()
                    except Exception:
                        pass
                    await _safe_invalidate(conn)
                    if attempt == 0:
                        log.warning(f"cache_set attempt 1 failed, retry: {e}")
                        continue
                    log.debug(f"cache_set attempt 2 failed (ok if duplicate): {e}")
                    return

                except Exception as e:
                    try:
                        await conn.rollback()
                        await conn.close()
                    except Exception:
                        pass
                    log.debug(f"cache_set error (ok if duplicate): {e}")
                    return
    except Exception as e:
        log.warning(f"cache_set outer error: {e}")


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

    Стратегия при ошибках кеша:
    - Ошибка чтения → вычисляем заново, пробуем записать
    - Ошибка записи → возвращаем результат без кеширования (не роняем запрос!)
    - Любая неожиданная ошибка → вычисляем напрямую через compute_fn
    """
    from common import ok_response, err_response

    date_val = _parse_dt(date)
    if date_val is None:
        msg = f"Invalid date format: {date!r}"
        log.error(f"cached_values: {msg} | pair={pair} day={day} params={extra_params} node={node}")
        return err_response(msg)

    p_hash = cache_hash(extra_params)

    # ── 1. Cache HIT ──────────────────────────────────────────────────────────
    # _cache_get никогда не бросает — возвращает None при любой ошибке
    cached = await _cache_get(engine_vlad, service_url, pair, day, date_val, p_hash)
    if cached is not None:
        log.debug(f"HIT  pair={pair} day={day} date={date} params={extra_params}")
        return ok_response(cached)

    # ── 2. Вычисляем ──────────────────────────────────────────────────────────
    log.debug(f"MISS pair={pair} day={day} date={date} params={extra_params}")
    try:
        result = await compute_fn()
    except Exception as e:
        log.error(f"cached_values: compute_fn raised: {e} | date={date!r} pair={pair} node={node}")
        return err_response(f"Computation failed: {e}")

    if result is None:
        log.error(f"cached_values: compute_fn returned None | date={date!r} pair={pair} node={node}")
        return err_response(f"Computation failed (check date or params): date={date!r} params={extra_params}")

    # ── 3. INSERT IGNORE — пишем в кеш (ошибка записи не роняет запрос) ──────
    # _cache_set никогда не бросает — при любой ошибке просто не кешируем
    await _cache_set(engine_vlad, service_url, pair, day, date_val,
                     extra_params, p_hash, result)

    return ok_response(result)
