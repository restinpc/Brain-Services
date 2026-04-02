"""
cache.py — параллельное заполнение кеша + бэктест.

Ключевые оптимизации vs sequential версии:
  1. Параллельные HTTP-запросы: вместо одного запроса за раз — батч из HTTP_CONCURRENCY
  2. Параллельные слоты: все 6 (pair × day) работают одновременно через asyncio.gather
  3. Bulk INSERT: результаты батча сохраняются одной транзакцией вместо N отдельных
  4. Keep-alive TCP: один коннектор на всё время работы скрипта
  5. Retry: упавшие даты прогоняются повторно RETRY_PASSES раз
"""

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

# ── ID моделей ────────────────────────────────────────────────────────────────
MODEL_IDS = [31]

# ── Параллельность ────────────────────────────────────────────────────────────
# Сколько одновременных HTTP-запросов к сервису.
# Начни с 5 — это безопасно. Поднимай по 5 если сервис стабилен.
# Слишком высокое значение = OOM или pool exhaustion на стороне сервиса.
HTTP_CONCURRENCY = 1

# Размер батча свечей для asyncio.gather.
# batch=50 при concurrency=5 → отправляем 50 запросов, но одновременно идут только 5.
SLOT_BATCH_SIZE = 50

# Retry для упавших дат.
RETRY_PASSES      = 3
RETRY_PASS_DELAYS = [30, 60, 120]  # сервису нужно время восстановиться
HTTP_TIMEOUT      = 30             # таймаут одного запроса (секунды)
RETRY_TIMEOUT     = 90             # таймаут при retry

# Circuit breaker: если подряд столько батчей = 100% ошибок → сервис лежит.
# Пауза CB_PAUSE секунд, потом пробуем снова.
CB_THRESHOLD = 2    # батчей подряд с 100% ошибок
CB_PAUSE     = 60   # секунд паузы

# ── Логирование ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Трассировка ───────────────────────────────────────────────────────────────
_HANDLER    = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL   = f"{_HANDLER}/trace.php"
NODE_NAME   = os.getenv("NODE_NAME",   "cache_runner")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_trace(subject: str, body: str, is_error: bool = False) -> None:
    level = "ERROR" if is_error else "INFO"
    full_body = f"[{level}] {subject}\n\nNode: {NODE_NAME}\n\n{body}"
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME,
                  "email": ALERT_EMAIL, "logs": full_body},
            timeout=10,
        )
        log.info(f"📤 Трассировка отправлена: {subject}")
    except Exception as e:
        log.warning(f"⚠️  Не удалось отправить трассировку: {e}")


def send_error_trace(exc: Exception, context: str = "") -> None:
    body = (
        f"Context: {context}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    send_trace(f"❌ Ошибка: {type(exc).__name__}", body, is_error=True)


# ── Торговые константы ─────────────────────────────────────────────────────────
INITIAL_BALANCE = 10_000.0
MIN_LOT         = 0.01

PAIR_CFG = {
    1: (0.0002, 100_000.0, 50_000.0),
    3: (60.0,        1.0, 100_000.0),
    4: (10.0,        1.0,   5_000.0),
}

RATES_TABLE = {
    (1, 0): "brain_rates_eur_usd",
    (1, 1): "brain_rates_eur_usd_day",
    (3, 0): "brain_rates_btc_usd",
    (3, 1): "brain_rates_btc_usd_day",
    (4, 0): "brain_rates_eth_usd",
    (4, 1): "brain_rates_eth_usd_day",
}

PAIR_NAMES = {1: "EUR/USD", 3: "BTC/USD", 4: "ETH/USD"}
DAY_NAMES  = {0: "hourly",  1: "daily"}


def _slot_label(pair: int, day: int) -> str:
    return f"{PAIR_NAMES.get(pair, f'pair{pair}')}-{'day' if day else 'hour'}"


# ── DDL ────────────────────────────────────────────────────────────────────────
DDL_CACHE = """
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
    INDEX idx_lookup (service_url(100), pair, day_flag, params_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_BACKTEST = """
CREATE TABLE IF NOT EXISTS vlad_backtest_results (
    id              BIGINT         NOT NULL AUTO_INCREMENT,
    service_url     VARCHAR(255)   NOT NULL,
    model_id        INT            NOT NULL DEFAULT 0,
    pair            TINYINT        NOT NULL,
    day_flag        TINYINT        NOT NULL,
    tier            TINYINT        NOT NULL,
    params_hash     CHAR(32)       NOT NULL,
    params_json     TEXT           NOT NULL,
    date_from       DATETIME       NOT NULL,
    date_to         DATETIME       NOT NULL,
    balance_final   DECIMAL(18,4)  NOT NULL DEFAULT 0,
    total_result    DECIMAL(18,4)  NOT NULL DEFAULT 0,
    summary_lost    DECIMAL(18,6)  NOT NULL DEFAULT 0,
    value_score     DECIMAL(18,4)  NOT NULL DEFAULT 0,
    trade_count     INT            NOT NULL DEFAULT 0,
    win_count       INT            NOT NULL DEFAULT 0,
    accuracy        DECIMAL(7,4)   NOT NULL DEFAULT 0,
    created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_backtest (service_url(100), pair, day_flag, tier,
                            params_hash, date_from, date_to),
    INDEX idx_score (service_url(100), pair, day_flag, tier, value_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_SUMMARY = """
CREATE TABLE IF NOT EXISTS vlad_backtest_summary (
    id                 INT            NOT NULL AUTO_INCREMENT,
    model_id           INT            NOT NULL,
    service_url        VARCHAR(255)   NOT NULL,
    pair               TINYINT        NOT NULL,
    day_flag           TINYINT        NOT NULL,
    tier               TINYINT        NOT NULL,
    date_from          DATETIME       NOT NULL,
    date_to            DATETIME       NOT NULL,
    total_combinations INT            NOT NULL DEFAULT 0,
    best_score         DECIMAL(18,4)  NOT NULL DEFAULT 0,
    avg_score          DECIMAL(18,4)  NOT NULL DEFAULT 0,
    best_accuracy      DECIMAL(7,4)   NOT NULL DEFAULT 0,
    avg_accuracy       DECIMAL(7,4)   NOT NULL DEFAULT 0,
    best_params_json   TEXT,
    computed_at        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                       ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_summary (model_id, pair, day_flag, tier, date_from, date_to)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


class Deadline:
    def __init__(self, hours: float):
        self.limit    = timedelta(hours=hours)
        self.started  = datetime.now()
        self.deadline = self.started + self.limit

    def exceeded(self) -> bool:
        return datetime.now() >= self.deadline

    def remaining_str(self) -> str:
        rem = self.deadline - datetime.now()
        if rem.total_seconds() <= 0:
            return "0s"
        h, rem_s = divmod(int(rem.total_seconds()), 3600)
        m, s     = divmod(rem_s, 60)
        return f"{h}h {m}m {s}s"

    def elapsed_str(self) -> str:
        el   = datetime.now() - self.started
        h, r = divmod(int(el.total_seconds()), 3600)
        m, s = divmod(r, 60)
        return f"{h}h {m}m {s}s"


# ── Вспомогательные функции ───────────────────────────────────────────────────

def get_service_url(sync_engine, model_id: int) -> str:
    with sync_engine.connect() as conn:
        row = conn.execute(
            sa_text("SELECT url FROM brain_service WHERE id = :mid"),
            {"mid": model_id},
        ).fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"URL для модели {model_id} не найден в brain_service")
    url = row[0].rstrip("/")
    log.info(f"  URL модели {model_id}: {url}")
    return url


def discover_param_combos(sync_engine, model_id: int) -> list[dict]:
    table = f"brain_signal{model_id}"
    with sync_engine.connect() as conn:
        try:
            desc = conn.execute(sa_text(f"DESCRIBE `{table}`")).fetchall()
        except Exception:
            log.warning(f"  ⚠️  Таблица {table} не найдена — одна комбинация {{}}")
            return [{}]

        available  = {row[0] for row in desc}
        param_cols = [c for c in ("type", "var") if c in available]

        if not param_cols:
            log.info(f"  Модель {model_id}: нет type/var → одна комбинация {{}}")
            return [{}]

        cols_str = ", ".join(param_cols)
        try:
            rows = conn.execute(
                sa_text(
                    f"SELECT DISTINCT {cols_str} FROM `{table}` ORDER BY {cols_str}"
                )
            ).fetchall()
        except Exception as e:
            log.warning(f"  ⚠️  Ошибка чтения {table}: {e} → одна комбинация")
            return [{}]

    combos = [
        {col: int(row[i]) for i, col in enumerate(param_cols) if row[i] is not None}
        for row in rows
    ] or [{}]

    log.info(f"  Модель {model_id}: {len(combos)} комбинаций (колонки: {param_cols})")
    return combos


def _params_hash(params: dict) -> str:
    return hashlib.md5(
        json.dumps(params, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Неверный формат даты: {s!r}")


def _compute_signal(values: dict, tier: int) -> int:
    if not values:
        return 0
    total = (
        sum(math.copysign(1.0, v) for v in values.values() if v != 0)
        if tier == 0
        else sum(values.values())
    )
    return 1 if total > 0 else (-1 if total < 0 else 0)


# ── HTTP ──────────────────────────────────────────────────────────────────────

async def _call_one(
    session: aiohttp.ClientSession,
    url: str,
    params: dict,
    global_sem: asyncio.Semaphore,
    timeout_sec: float = HTTP_TIMEOUT,
) -> tuple[dict | None, str | None]:
    """
    Один GET /values. Возвращает (payload | None, error_reason | None).
    Ограничен глобальным семафором чтобы не перегрузить сервис.
    """
    async with global_sem:
        try:
            # Используем /compute — чистое вычисление без кеша на стороне сервиса.
            # Это исключает конкуренцию за vlad_values_cache между кешером и сервисом,
            # что было причиной "Packet sequence number wrong" при concurrent запросах.
            async with session.get(
                f"{url}/compute",
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
            ) as r:
                if r.status != 200:
                    reason = f"HTTP {r.status}"
                    try:
                        body = await r.text()
                        if body:
                            reason += f" body={body[:200]}"
                    except Exception:
                        pass
                    return None, reason

                try:
                    data = await r.json(content_type=None)
                except Exception as e:
                    return None, f"JSON parse error: {e}"

                # /compute возвращает результат напрямую (dict без обёртки payLoad)
                # /values возвращает {"status":"ok","payLoad":{...}}
                # Обрабатываем оба формата для совместимости
                if "payLoad" in data:
                    payload = data["payLoad"]
                elif "payload" in data:
                    payload = data["payload"]
                elif "error" in data and "status" not in data:
                    # /compute вернул {"error": "..."}
                    return None, f"Сервис вернул error: {data['error'][:200]}"
                elif "status" not in data:
                    # /compute вернул dict с результатом напрямую
                    payload = data
                else:
                    return None, f"Нет payLoad в ответе: {str(data)[:200]}"

                if isinstance(payload, dict) and payload.get("status") == "error":
                    return None, f"Сервис вернул error: {str(payload)[:200]}"

                return payload, None

        except asyncio.TimeoutError:
            return None, f"Таймаут >{timeout_sec}s"
        except aiohttp.ClientConnectorError as e:
            return None, f"Нет соединения: {e}"
        except aiohttp.ClientError as e:
            return None, f"aiohttp {type(e).__name__}: {e}"
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"


# ── Bulk INSERT ───────────────────────────────────────────────────────────────

async def _bulk_insert(
    engine_vlad,
    rows: list[dict],
) -> None:
    """
    Вставляет список строк в vlad_values_cache одной транзакцией.
    Намного быстрее N отдельных INSERT IGNORE.
    """
    if not rows:
        return
    try:
        async with engine_vlad.begin() as conn:
            await conn.execute(
                text("""
                    INSERT IGNORE INTO vlad_values_cache
                        (service_url, pair, day_flag, date_val,
                         params_hash, params_json, result_json)
                    VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
                """),
                rows,
            )
    except Exception as e:
        log.warning(f"  ⚠️  Bulk INSERT failed ({len(rows)} rows): {type(e).__name__}: {e}")


# ── Загрузка кешированных дат ─────────────────────────────────────────────────

async def _load_cached_dates(
    engine_vlad,
    service_url: str,
    pair: int,
    day: int,
    p_hash: str,
) -> set:
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT date_val FROM vlad_values_cache
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND params_hash = :ph
        """), {"url": service_url, "pair": pair, "day": day, "ph": p_hash})
        return {row[0] for row in res.fetchall()}


# ── Один проход по списку свечей ──────────────────────────────────────────────

FailedItem = dict  # {"candle": dict, "reason": str}



async def _check_service_alive(
    session: aiohttp.ClientSession,
    url: str,
    label: str = "",
) -> bool:
    """
    Проверяет что сервис отвечает на GET /.
    Если нет — логирует причину.
    """
    try:
        async with session.get(
            f"{url}/",
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status == 200:
                return True
            log.warning(f"{label} ⚠️  Сервис отвечает HTTP {r.status}")
            return False
    except asyncio.TimeoutError:
        log.warning(f"{label} ⚠️  Сервис не отвечает (таймаут 5s)")
        return False
    except aiohttp.ClientConnectorError as e:
        log.warning(f"{label} ⚠️  Сервис недоступен: {e}")
        return False
    except Exception as e:
        log.warning(f"{label} ⚠️  Ошибка проверки сервиса: {e}")
        return False

async def _run_pass(
    candles: list[dict],
    session: aiohttp.ClientSession,
    service_url: str,
    pair: int,
    day: int,
    extra_params: dict,
    p_hash: str,
    global_sem: asyncio.Semaphore,
    engine_vlad,
    deadline: Deadline,
    label: str,
    pass_name: str,
    timeout_sec: float,
) -> tuple[int, list[FailedItem]]:
    """
    Проходит по candles батчами параллельных HTTP-запросов.
    Не останавливается на ошибках — собирает все упавшие в failed_list.
    Bulk INSERT после каждого батча.
    Circuit breaker: если CB_THRESHOLD батчей подряд дают 100% ошибок →
    сервис скорее всего упал → пауза CB_PAUSE секунд → проверка alive.
    Возвращает (ok_count, failed_list).
    """
    if not candles:
        return 0, []

    params_json  = json.dumps(extra_params, ensure_ascii=False)
    ok_count     = 0
    failed: list[FailedItem] = []
    cb_streak    = 0   # батчей подряд с 100% ошибками

    for batch_start in range(0, len(candles), SLOT_BATCH_SIZE):
        if deadline.exceeded():
            log.warning(f"{label} ⏰ {pass_name}: дедлайн, прерываем")
            break

        batch = candles[batch_start : batch_start + SLOT_BATCH_SIZE]

        # Параллельно запрашиваем все свечи в батче
        tasks = [
            _call_one(
                session,
                service_url,
                {"pair": pair, "day": day,
                 "date": c["date"].strftime("%Y-%m-%d %H:%M:%S"),
                 **extra_params},
                global_sem,
                timeout_sec,
            )
            for c in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Разбираем результаты и готовим bulk INSERT
        insert_rows: list[dict] = []
        batch_ok  = 0
        batch_err = 0

        for i, res in enumerate(results):
            candle = batch[i]
            if isinstance(res, Exception):
                failed.append({"candle": candle, "reason": f"gather exception: {res}"})
                batch_err += 1
                continue

            payload, err = res
            if err is not None or payload is None:
                failed.append({"candle": candle, "reason": err or "payload is None"})
                batch_err += 1
                continue

            insert_rows.append({
                "url":  service_url,
                "pair": pair,
                "day":  day,
                "dv":   candle["date"],
                "ph":   p_hash,
                "pj":   params_json,
                "rj":   json.dumps(payload, ensure_ascii=False),
            })
            batch_ok += 1
            ok_count += 1

        # Bulk INSERT успешных результатов
        await _bulk_insert(engine_vlad, insert_rows)

        done = batch_start + len(batch)
        pct  = done / len(candles) * 100
        log.info(
            f"{label} {pass_name} [{done}/{len(candles)}] {pct:.1f}%  "
            f"ok={ok_count}  err={len(failed)}  cb_streak={cb_streak}"
        )

        # ── Circuit breaker ────────────────────────────────────────────────────
        if batch_err == len(batch) and batch_ok == 0:
            # Весь батч упал — возможно сервис лежит
            cb_streak += 1
            log.warning(
                f"{label} ⚡ Батч 100% ошибок (streak={cb_streak}/{CB_THRESHOLD}). "
                f"Ошибка: {failed[-1]['reason'][:120] if failed else 'unknown'}"
            )

            if cb_streak >= CB_THRESHOLD:
                log.warning(
                    f"{label} 🔴 Circuit breaker: {cb_streak} батчей подряд с ошибками. "
                    f"Пауза {CB_PAUSE}s, потом проверяем сервис..."
                )
                await asyncio.sleep(CB_PAUSE)

                alive = await _check_service_alive(session, service_url, label)
                if not alive:
                    log.error(
                        f"{label} ❌ Сервис недоступен после паузы. "
                        f"Прерываем проход — оставшиеся даты пойдут в retry."
                    )
                    # Добавляем оставшиеся необработанные свечи в failed
                    next_start = batch_start + len(batch)
                    for c in candles[next_start:]:
                        failed.append({
                            "candle": c,
                            "reason": "сервис недоступен (circuit breaker)",
                        })
                    break
                else:
                    log.info(f"{label} ✅ Сервис снова доступен, продолжаем")
                    cb_streak = 0   # сбрасываем счётчик
        else:
            cb_streak = 0   # был хоть один успех — сбрасываем

    return ok_count, failed


# ── fill_cache_parallel ───────────────────────────────────────────────────────

async def fill_cache_parallel(
    engine_vlad,
    candles: list[dict],
    service_url: str,
    pair: int,
    day: int,
    extra_params: dict,
    global_sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    deadline: Deadline,
    slot: str = "",
) -> dict:
    """
    Заполняет кеш для одного (pair, day, combo).
    Параллельные HTTP-запросы + bulk INSERT + retry для упавших.
    """
    if not candles:
        return {"new": 0, "skipped": 0, "errors": 0, "total": 0}

    label   = f"[{slot}]" if slot else "[cache]"
    p_hash  = _params_hash(extra_params)

    # Загружаем уже кешированные даты
    cached_dates = await _load_cached_dates(engine_vlad, service_url, pair, day, p_hash)
    skipped      = sum(1 for c in candles if c["date"] in cached_dates)
    to_fetch     = [c for c in candles if c["date"] not in cached_dates]

    log.info(
        f"{label} Начало: {len(candles)} свечей  "
        f"в кеше={len(cached_dates)}  нужно={len(to_fetch)}  "
        f"params={json.dumps(extra_params)}"
    )

    if not to_fetch:
        return {"new": 0, "skipped": skipped, "errors": 0, "total": len(candles)}

    # Основной проход
    ok, failed = await _run_pass(
        to_fetch, session, service_url, pair, day,
        extra_params, p_hash, global_sem, engine_vlad,
        deadline, label, "▶ Основной", HTTP_TIMEOUT,
    )

    # Retry-проходы для упавших дат
    for pass_num in range(1, RETRY_PASSES + 1):
        if not failed or deadline.exceeded():
            break

        delay = RETRY_PASS_DELAYS[min(pass_num - 1, len(RETRY_PASS_DELAYS) - 1)]
        log.info(
            f"{label} 🔄 Retry {pass_num}/{RETRY_PASSES}: "
            f"{len(failed)} дат, пауза {delay}s..."
        )
        await asyncio.sleep(delay)

        retry_candles = [item["candle"] for item in failed]
        ok_r, failed = await _run_pass(
            retry_candles, session, service_url, pair, day,
            extra_params, p_hash, global_sem, engine_vlad,
            deadline, label, f"♻ Retry {pass_num}", RETRY_TIMEOUT,
        )
        ok += ok_r
        if not failed:
            log.info(f"{label} ✅ Retry {pass_num}: все ошибки устранены")

    errors = len(failed)

    if errors == 0:
        log.info(f"{label} ✅ Готово: new={ok}  skip={skipped}  err=0")
    else:
        log.warning(
            f"{label} ⚠️  Готово с ошибками: new={ok}  skip={skipped}  err={errors}"
        )
        # Логируем примеры ошибок — группируем по причине
        reasons: dict[str, list] = {}
        for item in failed:
            key = item["reason"][:80]
            reasons.setdefault(key, []).append(
                item["candle"]["date"].strftime("%Y-%m-%d %H:%M:%S")
            )
        for reason, dates in sorted(reasons.items(), key=lambda x: -len(x[1])):
            log.warning(f"  [{len(dates)}x] {reason}  пример: {dates[0]}")

    return {"new": ok, "skipped": skipped, "errors": errors, "total": len(candles)}


# ── Загрузка свечей ───────────────────────────────────────────────────────────

async def fetch_candles(
    engine_brain, pair: int, day: int,
    date_from: datetime, date_to: datetime,
) -> list[dict]:
    table = RATES_TABLE.get((pair, day))
    if not table:
        return []
    async with engine_brain.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT date, open, close FROM {table}
            WHERE date >= :df AND date < :dt
            ORDER BY date ASC
        """), {"df": date_from, "dt": date_to})
        return [
            {"date": r[0], "open": float(r[1] or 0), "close": float(r[2] or 0)}
            for r in res.fetchall()
        ]


# ── Бэктест ───────────────────────────────────────────────────────────────────

async def run_backtest(
    engine_vlad, candles: list[dict],
    service_url: str, pair: int, day: int, tier: int,
    extra_params: dict, date_from: datetime, date_to: datetime,
    model_id: int, deadline: Deadline, slot: str = "",
) -> dict:
    if not candles:
        return {"error": "no candles"}

    label  = f"[{slot}]" if slot else "[backtest]"
    p_hash = _params_hash(extra_params)

    async with engine_vlad.connect() as conn:
        existing = (await conn.execute(text("""
            SELECT value_score, accuracy, trade_count FROM vlad_backtest_results
            WHERE service_url = :url AND pair = :pair AND day_flag = :day
              AND tier = :tier AND params_hash = :ph
              AND date_from = :df AND date_to = :dt
        """), {"url": service_url, "pair": pair, "day": day, "tier": tier,
               "ph": p_hash, "df": date_from, "dt": date_to}
        )).fetchone()

    if existing:
        return {
            "value_score": float(existing[0]),
            "accuracy":    float(existing[1]),
            "trade_count": int(existing[2]),
            "params":      extra_params,
            "params_hash": p_hash,
            "skipped":     True,
        }

    if deadline.exceeded():
        return {"error": "timeout"}

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT date_val, result_json FROM vlad_values_cache
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND params_hash = :ph
        """), {"url": service_url, "pair": pair, "day": day, "ph": p_hash})
        cache_map = {row[0]: json.loads(row[1]) for row in res.fetchall()}

    spread, modification, lot_divisor = PAIR_CFG.get(pair, (0.0002, 100_000.0, 10_000.0))

    balance      = INITIAL_BALANCE
    highest      = INITIAL_BALANCE
    summary_lost = 0.0
    trade_count  = win_count = 0

    for candle in candles:
        values = cache_map.get(candle["date"])
        if not values:
            continue
        signal = _compute_signal(values, tier)
        if signal == 0:
            continue

        raw_move = candle["close"] - candle["open"]
        lot      = max(round(balance / lot_divisor, 2), MIN_LOT)
        amount   = (signal * raw_move - spread) * lot * modification

        balance += amount
        trade_count += 1
        if signal * raw_move > spread:
            win_count += 1

        if balance > highest:
            highest = balance
        drawdown = highest - balance
        if drawdown > 0:
            summary_lost += drawdown / highest

    if trade_count < 10:
        return {"error": "not enough trades", "trade_count": trade_count}

    total_result = balance - INITIAL_BALANCE
    value_score  = total_result - summary_lost
    accuracy     = win_count / trade_count

    result = {
        "balance_final": round(balance,      4),
        "total_result":  round(total_result, 4),
        "summary_lost":  round(summary_lost, 6),
        "value_score":   round(value_score,  4),
        "trade_count":   trade_count,
        "win_count":     win_count,
        "accuracy":      round(accuracy,     4),
        "params":        extra_params,
        "params_hash":   p_hash,
    }

    try:
        async with engine_vlad.begin() as conn:
            await conn.execute(text("""
                INSERT INTO vlad_backtest_results
                    (service_url, model_id, pair, day_flag, tier,
                     params_hash, params_json, date_from, date_to,
                     balance_final, total_result, summary_lost,
                     value_score, trade_count, win_count, accuracy)
                VALUES
                    (:url, :mid, :pair, :day, :tier,
                     :ph, :pj, :df, :dt,
                     :bf, :tr, :sl, :vs, :tc, :wc, :acc)
                ON DUPLICATE KEY UPDATE
                    balance_final = VALUES(balance_final),
                    total_result  = VALUES(total_result),
                    summary_lost  = VALUES(summary_lost),
                    value_score   = VALUES(value_score),
                    trade_count   = VALUES(trade_count),
                    win_count     = VALUES(win_count),
                    accuracy      = VALUES(accuracy),
                    created_at    = CURRENT_TIMESTAMP
            """), {
                "url": service_url, "mid": model_id,
                "pair": pair,       "day": day,
                "tier": tier,       "ph":  p_hash,
                "pj":  json.dumps(extra_params, ensure_ascii=False),
                "df":  date_from,   "dt":  date_to,
                "bf":  result["balance_final"],
                "tr":  result["total_result"],
                "sl":  result["summary_lost"],
                "vs":  result["value_score"],
                "tc":  trade_count,
                "wc":  win_count,
                "acc": result["accuracy"],
            })
    except Exception as e:
        log.warning(f"{label} ⚠️  Не удалось сохранить бэктест: {e}")

    return result


async def upsert_summary(
    engine_vlad, service_url: str, model_id: int,
    pair: int, day: int, tier: int,
    date_from: datetime, date_to: datetime,
) -> None:
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text("""
            SELECT COUNT(*), MAX(value_score), AVG(value_score),
                   MAX(accuracy), AVG(accuracy),
                   MAX(CASE WHEN value_score = (
                       SELECT MAX(value_score) FROM vlad_backtest_results r2
                       WHERE r2.service_url = :url AND r2.pair = :pair
                         AND r2.day_flag = :day AND r2.tier = :tier
                         AND r2.date_from = :df AND r2.date_to = :dt
                   ) THEN params_json END)
            FROM vlad_backtest_results
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND tier = :tier
              AND date_from = :df AND date_to = :dt
        """), {"url": service_url, "pair": pair, "day": day,
               "tier": tier, "df": date_from, "dt": date_to}
        )).fetchone()

    if not row or not row[0]:
        return

    async with engine_vlad.begin() as conn:
        await conn.execute(text("""
            INSERT INTO vlad_backtest_summary
                (model_id, service_url, pair, day_flag, tier,
                 date_from, date_to,
                 total_combinations, best_score, avg_score,
                 best_accuracy, avg_accuracy, best_params_json)
            VALUES (:mid, :url, :pair, :day, :tier, :df, :dt,
                    :cnt, :bs, :as_, :ba, :aa, :bpj)
            ON DUPLICATE KEY UPDATE
                total_combinations = VALUES(total_combinations),
                best_score         = VALUES(best_score),
                avg_score          = VALUES(avg_score),
                best_accuracy      = VALUES(best_accuracy),
                avg_accuracy       = VALUES(avg_accuracy),
                best_params_json   = VALUES(best_params_json),
                computed_at        = CURRENT_TIMESTAMP
        """), {
            "mid": model_id, "url": service_url,
            "pair": pair, "day": day, "tier": tier,
            "df": date_from, "dt": date_to,
            "cnt": row[0],
            "bs":  float(row[1] or 0), "as_": float(row[2] or 0),
            "ba":  float(row[3] or 0), "aa":  float(row[4] or 0),
            "bpj": row[5],
        })


# ── run_slot — один (pair, day) слот ─────────────────────────────────────────

async def run_slot(
    pair: int,
    day: int,
    candles: list[dict],
    param_combos: list[dict],
    engine_vlad,
    service_url: str,
    model_id: int,
    args,
    date_from: datetime,
    date_to: datetime,
    deadline: Deadline,
    global_sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
) -> dict:
    """
    Обрабатывает один слот (pair, day) полностью:
    кеш для всех комбо + бэктест для всех тиров.
    Запускается параллельно с другими слотами через asyncio.gather.
    """
    slot  = _slot_label(pair, day)
    label = f"[{slot}]"
    stats_cache    = {"new": 0, "skipped": 0, "errors": 0}
    stats_backtest = {"done": 0, "skipped": 0, "failed": 0}
    timed_out      = False

    if not candles:
        log.warning(f"{label} ⚠️  Нет свечей — пропускаем")
        return {"slot": slot, "cache": stats_cache, "backtest": stats_backtest, "timed_out": False}

    # Проверяем что сервис доступен перед началом работы
    alive = await _check_service_alive(session, service_url, label)
    if not alive:
        log.error(f"{label} ❌ Сервис {service_url} недоступен. Проверь что он запущен и порт доступен.")
        return {"slot": slot, "cache": stats_cache, "backtest": stats_backtest, "timed_out": False}

    log.info(f"{label} ▶  Кеш: {len(candles)} свечей, {len(param_combos)} комбинаций")

    # ── Кеш ───────────────────────────────────────────────────────────────────
    if not args.skip_fill:
        for idx, combo in enumerate(param_combos, 1):
            if deadline.exceeded():
                timed_out = True
                break

            combo_str = json.dumps(combo) if combo else "{}"
            log.info(f"{label} 📥 [{idx}/{len(param_combos)}] params={combo_str}")

            r = await fill_cache_parallel(
                engine_vlad, candles, service_url, pair, day,
                combo, global_sem, session, deadline, slot=slot,
            )
            stats_cache["new"]     += r["new"]
            stats_cache["skipped"] += r["skipped"]
            stats_cache["errors"]  += r["errors"]

            if r["errors"] > 0:
                send_trace(
                    f"⚠️  Ошибки кеша — {slot} params={combo_str}",
                    f"model={model_id}  pair={PAIR_NAMES.get(pair)}  "
                    f"day={DAY_NAMES.get(day)}\nparams={combo_str}\n"
                    f"url={service_url}\n\n"
                    f"Ошибок после всех retry: {r['errors']} из {r['total']}",
                    is_error=True,
                )

        if not timed_out:
            log.info(
                f"{label} ✅ Кеш завершён: "
                f"new={stats_cache['new']}  "
                f"skip={stats_cache['skipped']}  "
                f"err={stats_cache['errors']}"
            )
    else:
        log.info(f"{label} ⏭️  Кеш пропущен (--skip-fill)")

    # ── Бэктест ───────────────────────────────────────────────────────────────
    if not args.only_fill and not timed_out and not deadline.exceeded():
        for tier in args.tiers:
            if deadline.exceeded():
                timed_out = True
                break

            log.info(f"{label} 🧪 Бэктест tier={tier} ({len(param_combos)} комбинаций)")
            results = []

            for idx, combo in enumerate(param_combos, 1):
                if deadline.exceeded():
                    timed_out = True
                    break

                r = await run_backtest(
                    engine_vlad, candles, service_url, pair, day, tier,
                    combo, date_from, date_to, model_id, deadline, slot=slot,
                )
                if r.get("skipped"):
                    stats_backtest["skipped"] += 1
                    log.info(
                        f"{label} [{idx}/{len(param_combos)}] tier={tier} ⏭  "
                        f"score={r.get('value_score')}  acc={r.get('accuracy')}"
                    )
                elif "error" in r:
                    stats_backtest["failed"] += 1
                    log.info(
                        f"{label} [{idx}/{len(param_combos)}] tier={tier} ✗ "
                        f"{r['error']} (trades={r.get('trade_count', 0)})"
                    )
                else:
                    stats_backtest["done"] += 1
                    results.append(r)
                    log.info(
                        f"{label} [{idx}/{len(param_combos)}] tier={tier} ✓ "
                        f"score={r['value_score']:>10.2f}  acc={r['accuracy']:.3f}  "
                        f"trades={r['trade_count']}  params={json.dumps(combo)}"
                    )

            await upsert_summary(
                engine_vlad, service_url, model_id, pair, day, tier,
                date_from, date_to,
            )

            log.info(
                f"{label} 📊 tier={tier}: done={len(results)}  "
                f"skip={stats_backtest['skipped']}  fail={stats_backtest['failed']}"
            )
            if results:
                best = max(results, key=lambda x: x["value_score"])
                log.info(
                    f"{label} 🏆 tier={tier} лучший: "
                    f"score={best['value_score']}  acc={best['accuracy']}  "
                    f"trades={best['trade_count']}  params={best['params']}"
                )

    log.info(f"{label} ■  Готово (timed_out={timed_out})")
    return {"slot": slot, "cache": stats_cache, "backtest": stats_backtest, "timed_out": timed_out}


# ── run_model ─────────────────────────────────────────────────────────────────

async def run_model(
    model_id: int,
    service_url: str,
    param_combos: list[dict],
    engine_vlad,
    engine_brain,
    args,
    date_from: datetime,
    date_to: datetime,
    deadline: Deadline,
) -> tuple[dict, dict, bool]:
    slots = [(p, d) for p in args.pairs for d in args.days]

    log.info(
        f"\n{'#'*60}\n"
        f"  🤖 model_id={model_id}  url={service_url}\n"
        f"  Слотов: {len(slots)}  Комбинаций: {len(param_combos)}\n"
        f"  HTTP_CONCURRENCY={HTTP_CONCURRENCY}  SLOT_BATCH_SIZE={SLOT_BATCH_SIZE}\n"
        f"  Retry: passes={RETRY_PASSES}  delays={RETRY_PASS_DELAYS}s\n"
        f"{'#'*60}"
    )

    # Загружаем свечи для всех слотов параллельно
    log.info("  📊 Загружаем свечи для всех слотов параллельно...")
    candles_list = await asyncio.gather(*[
        fetch_candles(engine_brain, p, d, date_from, date_to)
        for p, d in slots
    ])
    candles_map = dict(zip(slots, candles_list))
    for (p, d), cc in candles_map.items():
        log.info(f"  [{_slot_label(p, d)}] свечей: {len(cc)}")

    # Один глобальный семафор для всех HTTP-запросов к этому сервису
    global_sem = asyncio.Semaphore(HTTP_CONCURRENCY)

    # Один keep-alive сессия на всё время работы модели
    connector = aiohttp.TCPConnector(keepalive_timeout=60, limit=HTTP_CONCURRENCY + 10)
    async with aiohttp.ClientSession(connector=connector) as session:

        log.info(f"\n  🚀 Запускаем {len(slots)} слотов параллельно...")

        # Все слоты стартуют одновременно
        slot_tasks = [
            run_slot(
                pair=p, day=d,
                candles=candles_map[(p, d)],
                param_combos=param_combos,
                engine_vlad=engine_vlad,
                service_url=service_url,
                model_id=model_id,
                args=args,
                date_from=date_from,
                date_to=date_to,
                deadline=deadline,
                global_sem=global_sem,
                session=session,
            )
            for p, d in slots
        ]
        slot_results = await asyncio.gather(*slot_tasks, return_exceptions=True)

    # Агрегируем статистику
    sc = {"new": 0, "skipped": 0, "errors": 0}
    sb = {"done": 0, "skipped": 0, "failed": 0}
    timed_out = False

    for res in slot_results:
        if isinstance(res, Exception):
            log.error(f"  ❌ Исключение в слоте: {res!r}")
            send_error_trace(res, f"run_slot model={model_id}")
            continue

        slot_name = res["slot"]
        c = res["cache"]
        b = res["backtest"]
        sc["new"]     += c.get("new",     0)
        sc["skipped"] += c.get("skipped", 0)
        sc["errors"]  += c.get("errors",  0)
        sb["done"]    += b.get("done",    0)
        sb["skipped"] += b.get("skipped", 0)
        sb["failed"]  += b.get("failed",  0)
        if res.get("timed_out"):
            timed_out = True

        log.info(
            f"  [{slot_name}] итог: "
            f"cache new={c.get('new',0)}  skip={c.get('skipped',0)}  err={c.get('errors',0)}"
        )

    return sc, sb, timed_out


# ── run / main ────────────────────────────────────────────────────────────────

async def run(args) -> None:
    deadline = Deadline(hours=args.timeout_hours)
    log.info(
        f"⏰ Таймаут: {args.timeout_hours}h  "
        f"(дедлайн: {deadline.deadline.strftime('%Y-%m-%d %H:%M:%S')})"
    )

    vlad_host     = os.getenv("VLAD_HOST",     os.getenv("DB_HOST",     "localhost"))
    vlad_port     = os.getenv("VLAD_PORT",     os.getenv("DB_PORT",     "3306"))
    vlad_user     = os.getenv("VLAD_USER",     os.getenv("DB_USER",     "root"))
    vlad_password = os.getenv("VLAD_PASSWORD", os.getenv("DB_PASSWORD", ""))
    vlad_database = os.getenv("VLAD_DATABASE", os.getenv("DB_NAME",     "vlad"))

    super_host     = os.getenv("SUPER_HOST",     vlad_host)
    super_port     = os.getenv("SUPER_PORT",     vlad_port)
    super_user     = os.getenv("SUPER_USER",     vlad_user)
    super_password = os.getenv("SUPER_PASSWORD", vlad_password)
    super_name     = os.getenv("SUPER_NAME",     "brain")

    brain_host     = os.getenv("MASTER_HOST",     vlad_host)
    brain_port     = os.getenv("MASTER_PORT",     vlad_port)
    brain_user     = os.getenv("MASTER_USER",     vlad_user)
    brain_password = os.getenv("MASTER_PASSWORD", vlad_password)
    brain_name     = os.getenv("MASTER_NAME",     "brain")

    vlad_url  = f"mysql+aiomysql://{vlad_user}:{vlad_password}@{vlad_host}:{vlad_port}/{vlad_database}"
    brain_url = f"mysql+aiomysql://{brain_user}:{brain_password}@{brain_host}:{brain_port}/{brain_name}"
    super_sync_url = (
        f"mysql+mysqlconnector://{super_user}:{super_password}"
        f"@{super_host}:{super_port}/{super_name}"
    )

    log.info("=" * 60)
    log.info(f"🚀 models={MODEL_IDS}  HTTP_CONCURRENCY={HTTP_CONCURRENCY}  SLOT_BATCH_SIZE={SLOT_BATCH_SIZE}")
    log.info(f"   vlad  DB : {vlad_user}@{vlad_host}:{vlad_port}/{vlad_database}")
    log.info(f"   brain DB : {brain_user}@{brain_host}:{brain_port}/{brain_name}")
    log.info(f"   super DB : {super_user}@{super_host}:{super_port}/{super_name}")
    log.info("=" * 60)

    model_configs: list[tuple[int, str, list[dict]]] = []
    try:
        sync_engine = create_engine(
            super_sync_url, pool_recycle=3600,
            connect_args={"auth_plugin": "caching_sha2_password"},
        )
        for model_id in MODEL_IDS:
            try:
                service_url  = get_service_url(sync_engine, model_id)
                param_combos = discover_param_combos(sync_engine, model_id)
                model_configs.append((model_id, service_url, param_combos))
            except Exception as e:
                log.error(f"❌ Модель {model_id}: {e}")
                send_error_trace(e, f"get_service_config model={model_id}")
        sync_engine.dispose()
    except Exception as e:
        log.critical(f"❌ super DB: {e}")
        send_error_trace(e, "sync_engine_connect")
        sys.exit(1)

    if not model_configs:
        log.critical("❌ Ни одна модель не загружена.")
        sys.exit(1)

    n_slots = len(args.pairs) * len(args.days)
    engine_vlad = create_async_engine(
        vlad_url,
        pool_size=min(n_slots * 4 + 5, 40),
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )
    engine_brain = create_async_engine(
        brain_url,
        pool_size=max(n_slots, 6),
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )

    log.info(f"  DB pool: vlad={min(n_slots*4+5,40)}(+10)  brain={max(n_slots,6)}  слотов={n_slots}")

    try:
        async with engine_vlad.begin() as conn:
            for ddl in (DDL_CACHE, DDL_BACKTEST, DDL_SUMMARY):
                await conn.execute(text(ddl))
        log.info("✅ Таблицы проверены/созданы")
    except Exception as e:
        log.critical(f"❌ Ошибка DDL: {e}")
        send_error_trace(e, "ensure_tables")
        sys.exit(1)

    date_from = _parse_dt(args.date_from) if args.date_from else datetime(2025, 1, 15)
    date_to   = (
        _parse_dt(args.date_to) if args.date_to
        else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    )

    log.info(f"  Период : {date_from} → {date_to}")
    log.info(f"  Модели : {MODEL_IDS}")
    log.info(f"  Пары   : {args.pairs}")
    log.info(f"  Дни    : {args.days}")
    log.info(f"  Тиры   : {args.tiers}")
    log.info(f"  Слотов : {n_slots} (параллельно)")

    all_timed_out = False
    try:
        for model_id, service_url, param_combos in model_configs:
            if deadline.exceeded():
                all_timed_out = True
                break

            sc, sb, timed_out = await run_model(
                model_id=model_id, service_url=service_url,
                param_combos=param_combos,
                engine_vlad=engine_vlad, engine_brain=engine_brain,
                args=args, date_from=date_from, date_to=date_to,
                deadline=deadline,
            )

            elapsed    = deadline.elapsed_str()
            body_stats = (
                f"Кеш  : new={sc['new']}  skip={sc['skipped']}  err={sc['errors']}\n"
                f"Бэктест: done={sb['done']}  skip={sb['skipped']}  fail={sb['failed']}"
            )

            if timed_out:
                all_timed_out = True
                msg = (
                    f"⏰ Модель {model_id} остановлена по таймауту.\n"
                    f"Прошло: {elapsed}\n\n{body_stats}\n\n"
                    f"Запусти повторно — продолжит с места остановки."
                )
                log.warning(f"\n{msg}")
                send_trace(f"⏰ Таймаут — model={model_id}", msg)
                break
            else:
                msg = f"✅ Модель {model_id}.\nПрошло: {elapsed}\n\n{body_stats}"
                log.info(f"\n{'='*55}\n{msg}\n{'='*55}")
                send_trace(f"✅ Готово — model={model_id}", msg)

        elapsed   = deadline.elapsed_str()
        completed = [m[0] for m in model_configs]

        if all_timed_out:
            final = (
                f"⏰ Прогон остановлен по таймауту {args.timeout_hours}h.\n"
                f"Прошло: {elapsed}\nМодели: {completed}\n"
                f"Запусти повторно."
            )
            log.warning(f"\n{final}")
            send_trace(f"⏰ Таймаут — models={MODEL_IDS}", final)
        else:
            final = f"✅ Все модели завершены.\nПрошло: {elapsed}\nМодели: {completed}"
            log.info(f"\n{'='*55}\n{final}\n{'='*55}")
            send_trace(f"✅ Готово — {MODEL_IDS}", final)

    except Exception as e:
        log.critical(f"❌ Критическая ошибка: {e!r}")
        send_error_trace(e, "main_loop")
        raise
    finally:
        await engine_vlad.dispose()
        await engine_brain.dispose()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Кеш + бэктест. Параллельные HTTP-запросы + parallel slots.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Параметры производительности (в коде):
  HTTP_CONCURRENCY  = {HTTP_CONCURRENCY}   — одновременных запросов к сервису
  SLOT_BATCH_SIZE   = {SLOT_BATCH_SIZE}  — свечей в параллельном батче
  RETRY_PASSES      = {RETRY_PASSES}    — перепрогонов для упавших дат

Примеры:
  python cache.py
  python cache.py --date-from 2025-01-15 --date-to 2026-03-12
  python cache.py --pair 1 --day 0 --only-fill
        """,
    )
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to",   default=None)
    parser.add_argument("--pair",  type=int, nargs="+", default=[1, 3, 4],
                        choices=[1, 3, 4], dest="pairs")
    parser.add_argument("--day",   type=int, nargs="+", default=[0, 1],
                        choices=[0, 1], dest="days")
    parser.add_argument("--tier",  type=int, nargs="+", default=[0, 1],
                        choices=[0, 1], dest="tiers")
    parser.add_argument("--skip-fill",     action="store_true")
    parser.add_argument("--only-fill",     action="store_true")
    parser.add_argument("--timeout-hours", type=float, default=24.0)

    args, unknown = parser.parse_known_args()
    if unknown:
        log.warning(f"⚠️  Игнорируем неизвестные аргументы: {unknown}")
    return args


def main():
    args    = parse_args()
    n_slots = len(args.pairs) * len(args.days)
    slots   = [_slot_label(p, d) for p in args.pairs for d in args.days]

    log.info("=" * 60)
    log.info("⚙️  Параметры запуска")
    log.info(f"   models            : {MODEL_IDS}")
    log.info(f"   date_from         : {args.date_from or '2025-01-15 (default)'}")
    log.info(f"   date_to           : {args.date_to   or 'today (default)'}")
    log.info(f"   pairs             : {args.pairs}")
    log.info(f"   days              : {args.days}")
    log.info(f"   tiers             : {args.tiers}")
    log.info(f"   skip_fill         : {args.skip_fill}")
    log.info(f"   only_fill         : {args.only_fill}")
    log.info(f"   timeout_hours     : {args.timeout_hours}")
    log.info(f"   слотов            : {n_slots} → {slots}")
    log.info(f"   HTTP_CONCURRENCY  : {HTTP_CONCURRENCY}")
    log.info(f"   SLOT_BATCH_SIZE   : {SLOT_BATCH_SIZE}")
    log.info(f"   RETRY_PASSES      : {RETRY_PASSES}")
    log.info(f"   RETRY_PASS_DELAYS : {RETRY_PASS_DELAYS}s")
    log.info(f"   HTTP/RETRY timeout: {HTTP_TIMEOUT}/{RETRY_TIMEOUT}s")
    log.info("=" * 60)

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("🛑 Прервано пользователем (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        log.critical(f"❌ Завершено с ошибкой: {e!r}")
        send_error_trace(e, "main")
        sys.exit(1)


if __name__ == "__main__":
    main()
