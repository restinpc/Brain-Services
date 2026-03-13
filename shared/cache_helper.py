"""
cache_helper.py — кеш /values для всех brain-* микросервисов.

Логика на каждый запрос /values:
  1. SELECT → если в кеше есть → вернуть сразу (cache HIT)
  2. Вычислить через compute_fn() (cache MISS)
  3. SELECT снова перед записью → защита от гонки при параллельных запросах
     (два сигнала с интервалом 0.1 сек с одинаковыми параметрами)
  4. INSERT IGNORE → финальная защита от дубликатов
  5. Вернуть результат

Подключение к серверу — 3 изменения:
  1. Импорт:
       from cache_helper import ensure_cache_table, load_service_url, cached_values

  2. В preload_all_data() — добавить в конец:
       global SERVICE_URL
       SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
       await ensure_cache_table(engine_vlad)

  3. В @app.get("/values") — заменить тело на cached_values(...)
"""

import hashlib
import json
import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text

log = logging.getLogger(__name__)

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


def _parse_dt(s: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%d-%m %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def cache_hash(params: dict) -> str:
    """MD5 от канонического JSON параметров."""
    return hashlib.md5(
        json.dumps(params, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


async def ensure_cache_table(engine_vlad: AsyncEngine) -> None:
    """Создаёт vlad_values_cache, только если её нет."""
    # Проверяем существование таблицы
    async with engine_vlad.connect() as conn:
        result = await conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = 'vlad_values_cache'
        """))
        exists = (await result.scalar()) > 0

    if not exists:
        async with engine_vlad.begin() as conn:
            await conn.execute(text(_DDL))
        log.info("✅ vlad_values_cache создана")
    else:
        log.debug("vlad_values_cache уже существует")


async def load_service_url(engine_super: AsyncEngine, service_id: int) -> str:
    """
    Читает URL сервиса из brain_service по service_id.
    Использует engine_super — супер-нода где гарантированно живёт brain_service.
    Бросает RuntimeError если не найдено.
    """
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
    """SELECT из кеша. Возвращает dict или None."""
    try:
        async with engine_vlad.connect() as conn:
            row = (await conn.execute(text("""
                SELECT result_json FROM vlad_values_cache
                WHERE service_url = :url AND pair = :pair
                  AND day_flag = :day AND date_val = :dv AND params_hash = :ph
                LIMIT 1
            """), {"url": service_url, "pair": pair, "day": day,
                   "dv": date_val, "ph": p_hash})).fetchone()
        if row:
            return json.loads(row[0])
    except Exception as e:
        log.warning(f"cache_get error: {e}")
    return None


async def _cache_set(engine_vlad: AsyncEngine, service_url: str,
                     pair: int, day: int, date_val: datetime,
                     params: dict, p_hash: str, result: dict) -> None:
    """
    INSERT IGNORE в кеш.
    Безопасно при параллельных запросах — дубликат просто игнорируется.
    """
    try:
        async with engine_vlad.begin() as conn:
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
    except Exception as e:
        # UNIQUE-конфликт при параллельном запуске — штатная ситуация
        log.debug(f"cache_set (ok if duplicate): {e}")


async def cached_values(
    engine_vlad:  AsyncEngine,
    service_url:  str,
    pair:         int,
    day:          int,
    date:         str,
    extra_params: dict,
    compute_fn,               # async callable () -> dict | None
    node:         str = "",
) -> dict:
    """
    Универсальная обёртка для endpoint /values.

    Возвращает готовый FastAPI-совместимый dict.
    """
    from common import ok_response, err_response

    date_val = _parse_dt(date)
    if date_val is None:
        return err_response("Invalid date format")

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
        return err_response("Invalid date format")

    # ── 3. SELECT снова — защита от гонки 0.1 сек ────────────────────────────
    # Пока мы считали, параллельный поток мог уже записать тот же результат.
    if result:
        already = await _cache_get(engine_vlad, service_url, pair, day, date_val, p_hash)
        if already is None:
            # ── 4. INSERT IGNORE — финальная защита ───────────────────────────
            await _cache_set(engine_vlad, service_url, pair, day, date_val,
                             extra_params, p_hash, result)

    return ok_response(result)