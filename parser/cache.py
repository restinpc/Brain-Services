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

import aiohttp
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

# ── Логирование ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Трассировка ───────────────────────────────────────────────────────────────
TRACE_URL   = os.getenv("TRACE_URL",   "https://server.brain-project.online/trace.php")
NODE_NAME   = os.getenv("NODE_NAME",   "cache_runner")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_trace(subject: str, body: str, is_error: bool = False) -> None:
    """Отправляет трассировку на почту. Не бросает исключений."""
    level     = "ERROR" if is_error else "INFO"
    full_body = f"[{level}] {subject}\n\nNode: {NODE_NAME}\n\n{body}"
    try:
        requests.post(
            TRACE_URL,
            data={
                "url":   "cli_script",
                "node":  NODE_NAME,
                "email": ALERT_EMAIL,
                "logs":  full_body,
            },
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
    1: (0.0002, 100_000.0, 50_000.0),   # EUR/USD
    3: (60.0,        1.0, 100_000.0),   # BTC/USD
    4: (10.0,        1.0,   5_000.0),   # ETH/USD
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Таймер — ограничение 24 часа
# ═══════════════════════════════════════════════════════════════════════════════
class Deadline:
    """Проверяет, не истёк ли таймаут. Используется везде в циклах."""

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


# ═══════════════════════════════════════════════════════════════════════════════
#  Определение параметров модели (синхронно, один раз при старте)
# ═══════════════════════════════════════════════════════════════════════════════
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
    """
    Читает brain_signal<model_id> → список уникальных комбинаций type/var.
    Если таблицы нет или колонок нет → [{}] (одна пустая комбинация).
    """
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
                sa_text(f"SELECT DISTINCT {cols_str} FROM `{table}` ORDER BY {cols_str}")
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Утилиты
# ═══════════════════════════════════════════════════════════════════════════════
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


def _print_progress(done: int, total: int, label: str,
                    errors: int = 0, skipped: int = 0) -> None:
    pct   = done / total * 100 if total else 0
    extra = ""
    if skipped:
        extra += f"  skip={skipped}"
    if errors:
        extra += f"  err={errors}"
    print(f"\r    {label} [{done}/{total}] {pct:.1f}%{extra}",
          end="", flush=True)


def _compute_signal(values: dict, tier: int) -> int:
    if not values:
        return 0
    total = (
        sum(math.copysign(1.0, v) for v in values.values() if v != 0)
        if tier == 0
        else sum(values.values())
    )
    return 1 if total > 0 else (-1 if total < 0 else 0)


# ═══════════════════════════════════════════════════════════════════════════════
#  HTTP вызов /values
# ═══════════════════════════════════════════════════════════════════════════════
async def _call_values(session: aiohttp.ClientSession,
                       url: str, params: dict) -> dict | None:
    try:
        async with session.get(
            f"{url}/values",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if "payload" in data:
                return data["payload"]
            if "status" not in data:
                return data
            return None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  Загрузка свечей
# ═══════════════════════════════════════════════════════════════════════════════
async def fetch_candles(engine_brain, pair: int, day: int,
                        date_from: datetime, date_to: datetime) -> list[dict]:
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Шаг 1: Заполнение кеша
# ═══════════════════════════════════════════════════════════════════════════════
async def fill_cache(engine_vlad, candles: list[dict],
                     service_url: str, pair: int, day: int,
                     extra_params: dict,
                     deadline: Deadline) -> dict:
    if not candles:
        return {"done": 0, "total": 0, "errors": 0, "skipped": 0}

    p_hash = _params_hash(extra_params)
    total  = len(candles)
    done   = errors = skipped = 0

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT date_val FROM vlad_values_cache
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND params_hash = :ph
        """), {"url": service_url, "pair": pair, "day": day, "ph": p_hash})
        cached_dates = {row[0] for row in res.fetchall()}

    skipped = sum(1 for c in candles if c["date"] in cached_dates)

    async with aiohttp.ClientSession() as session:
        for candle in candles:
            if deadline.exceeded():
                log.warning(f"\n  ⏰ Таймаут! Осталось обработать "
                            f"{total - done} свечей этой комбинации")
                break

            date_val = candle["date"]

            if date_val in cached_dates:
                done += 1
                _print_progress(done, total, "cache", errors, skipped)
                continue

            date_str    = date_val.strftime("%Y-%m-%d %H:%M:%S")
            call_params = {"pair": pair, "day": day,
                           "date": date_str, **extra_params}

            result = await _call_values(session, service_url, call_params)
            if result is None:
                errors += 1
                done   += 1
                _print_progress(done, total, "cache", errors, skipped)
                continue

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
                        "pj":   json.dumps(extra_params, ensure_ascii=False),
                        "rj":   json.dumps(result,       ensure_ascii=False),
                    })
            except Exception:
                pass

            done += 1
            _print_progress(done, total, "cache", errors, skipped)

    print()
    return {"done": done, "total": total, "errors": errors, "skipped": skipped}


# ═══════════════════════════════════════════════════════════════════════════════
#  Шаг 2: Бэктест (читает из кеша — без HTTP)
# ═══════════════════════════════════════════════════════════════════════════════
async def run_backtest(engine_vlad, candles: list[dict],
                       service_url: str, pair: int, day: int, tier: int,
                       extra_params: dict, date_from: datetime, date_to: datetime,
                       model_id: int,
                       deadline: Deadline) -> dict:
    if not candles:
        return {"error": "no candles"}

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

    spread, modification, lot_divisor = PAIR_CFG.get(
        pair, (0.0002, 100_000.0, 10_000.0)
    )

    balance      = INITIAL_BALANCE
    highest      = INITIAL_BALANCE
    summary_lost = 0.0
    trade_count  = win_count = 0
    total        = len(candles)

    for i, candle in enumerate(candles):
        _print_progress(i + 1, total, f"backtest tier={tier}")
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

    print()

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
                     params_hash, params_json,
                     date_from, date_to,
                     balance_final, total_result, summary_lost,
                     value_score, trade_count, win_count, accuracy)
                VALUES
                    (:url, :mid, :pair, :day, :tier,
                     :ph, :pj,
                     :df, :dt,
                     :bf, :tr, :sl,
                     :vs, :tc, :wc, :acc)
                ON DUPLICATE KEY UPDATE
                    balance_final = VALUES(balance_final),
                    total_result  = VALUES(total_result),
                    summary_lost  = VALUES(summary_lost),
                    value_score   = VALUES(value_score),
                    trade_count   = VALUES(trade_count),
                    win_count     = VALUES(win_count),
                    accuracy      = VALUES(accuracy)
            """), {
                "url": service_url, "mid": model_id,
                "pair": pair, "day": day, "tier": tier,
                "ph": p_hash,
                "pj": json.dumps(extra_params, ensure_ascii=False),
                "df": date_from, "dt": date_to,
                "bf": result["balance_final"],
                "tr": result["total_result"],
                "sl": result["summary_lost"],
                "vs": result["value_score"],
                "tc": result["trade_count"],
                "wc": result["win_count"],
                "acc": result["accuracy"],
            })
    except Exception as e:
        log.warning(f"  ⚠️  Не удалось сохранить backtest: {e}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  Шаг 3: Summary
# ═══════════════════════════════════════════════════════════════════════════════
async def update_summary(engine_vlad,
                         service_url: str, model_id: int,
                         pair: int, day: int, tier: int,
                         date_from: datetime, date_to: datetime) -> None:
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT value_score, accuracy, params_json
            FROM vlad_backtest_results
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND tier = :tier
              AND date_from = :df AND date_to = :dt
        """), {"url": service_url, "pair": pair, "day": day, "tier": tier,
               "df": date_from, "dt": date_to})
        rows = res.fetchall()

    if not rows:
        return

    scores    = [float(r[0]) for r in rows]
    accs      = [float(r[1]) for r in rows]
    best_idx  = scores.index(max(scores))

    try:
        async with engine_vlad.begin() as conn:
            await conn.execute(text("""
                INSERT INTO vlad_backtest_summary
                    (model_id, service_url, pair, day_flag, tier,
                     date_from, date_to,
                     total_combinations,
                     best_score, avg_score,
                     best_accuracy, avg_accuracy,
                     best_params_json)
                VALUES
                    (:mid, :url, :pair, :day, :tier,
                     :df, :dt,
                     :tc,
                     :bs, :avs,
                     :ba, :ava,
                     :bp)
                ON DUPLICATE KEY UPDATE
                    total_combinations = VALUES(total_combinations),
                    best_score         = VALUES(best_score),
                    avg_score          = VALUES(avg_score),
                    best_accuracy      = VALUES(best_accuracy),
                    avg_accuracy       = VALUES(avg_accuracy),
                    best_params_json   = VALUES(best_params_json)
            """), {
                "mid": model_id, "url": service_url,
                "pair": pair, "day": day, "tier": tier,
                "df": date_from, "dt": date_to,
                "tc":  len(rows),
                "bs":  round(max(scores), 4),
                "avs": round(sum(scores) / len(scores), 4),
                "ba":  round(max(accs),   4),
                "ava": round(sum(accs)   / len(accs),   4),
                "bp":  rows[best_idx][2],
            })
    except Exception as e:
        log.warning(f"  ⚠️  Не удалось обновить summary: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Аргументы CLI
# ═══════════════════════════════════════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="cache.py — кеш + бэктест + summary"
    )

    # Обязательные позиционные
    parser.add_argument("table_name", help="Имя целевой таблицы")
    parser.add_argument("host",     nargs="?", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("port",     nargs="?", default=os.getenv("DB_PORT", "3306"))
    parser.add_argument("user",     nargs="?", default=os.getenv("DB_USER", "root"))
    parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME", "test"))

    # model_id — необязательный, можно взять из .env
    parser.add_argument(
        "model_id",
        nargs="?",
        type=int,
        default=None,
        help="ID модели (необязательный — можно задать MODEL_ID в .env)",
    )

    # Флаги
    parser.add_argument("--date-from",     default="2025-01-15",
                        help="Начальная дата (YYYY-MM-DD)")
    parser.add_argument("--date-to",       default=None,
                        help="Конечная дата (YYYY-MM-DD), по умолчанию — сегодня")
    parser.add_argument("--pair",          nargs="+", type=int,
                        choices=[1, 3, 4],  default=[1, 3, 4],
                        help="Пары (1=EUR/USD  3=BTC/USD  4=ETH/USD)")
    parser.add_argument("--day",           nargs="+", type=int,
                        choices=[0, 1],     default=[0, 1],
                        help="Day-flag (0=hourly  1=daily)")
    parser.add_argument("--tier",          nargs="+", type=int,
                        choices=[0, 1],     default=[0, 1],
                        help="Tier (0=sign  1=sum)")
    parser.add_argument("--skip-fill",     action="store_true",
                        help="Пропустить заполнение кеша")
    parser.add_argument("--only-fill",     action="store_true",
                        help="Только кеш, бэктест не запускать")
    parser.add_argument("--timeout-hours", type=float, default=24.0,
                        help="Лимит времени в часах (по умолчанию: 24)")

    args = parser.parse_args()

    # Фолбэк model_id из .env
    if args.model_id is None:
        env_val = os.getenv("MODEL_ID")
        if env_val:
            try:
                args.model_id = int(env_val)
            except ValueError:
                parser.error(f"MODEL_ID в .env не является числом: {env_val!r}")
        else:
            parser.error(
                "Не указан model_id — передайте его аргументом "
                "или задайте MODEL_ID в .env\n\n"
                "Пример: python cache.py vlad_data_bot localhost 3307 "
                "vlad pass vlad 33"
            )

    return args


# ═══════════════════════════════════════════════════════════════════════════════
#  Главная асинхронная функция
# ═══════════════════════════════════════════════════════════════════════════════
async def main_async(args: argparse.Namespace) -> None:
    date_from = _parse_dt(args.date_from)
    date_to   = _parse_dt(args.date_to) if args.date_to else datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    log.info("=" * 60)
    log.info("🚀 cache.py — старт")
    log.info(f"   База:      {args.host}:{args.port}/{args.database}")
    log.info(f"   Таблица:   {args.table_name}")
    log.info(f"   model_id:  {args.model_id}")
    log.info(f"   Период:    {date_from.date()} → {date_to.date()}")
    log.info(f"   Пары:      {args.pair}")
    log.info(f"   Day-flags: {args.day}")
    log.info(f"   Tiers:     {args.tier}")
    log.info(f"   Таймаут:   {args.timeout_hours}h")
    log.info("=" * 60)

    deadline = Deadline(args.timeout_hours)

    sync_url  = (f"mysql+mysqlconnector://{args.user}:{args.password}"
                 f"@{args.host}:{args.port}/{args.database}")
    async_url = (f"mysql+aiomysql://{args.user}:{args.password}"
                 f"@{args.host}:{args.port}/{args.database}")

    sync_engine  = create_engine(sync_url, pool_recycle=3600)
    engine_vlad  = create_async_engine(async_url, pool_recycle=3600)
    engine_brain = create_async_engine(async_url, pool_recycle=3600)

    # ── DDL ───────────────────────────────────────────────────────────────────
    with sync_engine.connect() as conn:
        conn.execute(sa_text(DDL_CACHE))
        conn.execute(sa_text(DDL_BACKTEST))
        conn.execute(sa_text(DDL_SUMMARY))
        conn.commit()
    log.info("✅ DDL применён")

    # ── Параметры модели ──────────────────────────────────────────────────────
    try:
        service_url = get_service_url(sync_engine, args.model_id)
        combos      = discover_param_combos(sync_engine, args.model_id)
    except Exception as e:
        send_error_trace(e, "init")
        log.critical(f"❌ Инициализация: {e}")
        return

    send_trace(
        "🚀 Запуск cache.py",
        f"model_id={args.model_id}  url={service_url}\n"
        f"Период: {date_from.date()} → {date_to.date()}\n"
        f"Комбинаций: {len(combos)}"
    )

    total_combos = len(combos) * len(args.pair) * len(args.day) * len(args.tier)
    done_combos  = 0

    # ── Основной цикл ─────────────────────────────────────────────────────────
    for pair in args.pair:
        for day in args.day:
            candles = await fetch_candles(
                engine_brain, pair, day, date_from, date_to
            )
            log.info(f"\n▶ {PAIR_NAMES[pair]} / {DAY_NAMES[day]}: "
                     f"{len(candles)} свечей")

            for combo in combos:
                if deadline.exceeded():
                    log.warning("⏰ Глобальный таймаут — выход из цикла")
                    goto_finish = True
                    break

                log.info(f"  Комбинация: {combo or '{}'}")

                # ── Кеш ───────────────────────────────────────────────────────
                if not args.skip_fill:
                    cr = await fill_cache(
                        engine_vlad, candles, service_url,
                        pair, day, combo, deadline
                    )
                    log.info(f"  Кеш: done={cr['done']} "
                             f"skip={cr['skipped']} err={cr['errors']}")

                if args.only_fill:
                    continue

                # ── Бэктест ────────────────────────────────────────────────────
                for tier in args.tier:
                    if deadline.exceeded():
                        break
                    br = await run_backtest(
                        engine_vlad, candles, service_url,
                        pair, day, tier, combo,
                        date_from, date_to, args.model_id, deadline
                    )
                    if "error" in br:
                        log.info(f"  Tier {tier}: ⚠ {br['error']}")
                    elif br.get("skipped"):
                        log.info(f"  Tier {tier}: ⏭ пропущен (уже есть)")
                    else:
                        log.info(
                            f"  Tier {tier}: "
                            f"score={br['value_score']}  "
                            f"acc={br['accuracy']:.2%}  "
                            f"trades={br['trade_count']}"
                        )

                done_combos += 1
            else:
                continue
            break
        else:
            continue
        break

    # ── Summary ───────────────────────────────────────────────────────────────
    if not args.only_fill:
        log.info("\n📊 Обновление summary…")
        for pair in args.pair:
            for day in args.day:
                for tier in args.tier:
                    await update_summary(
                        engine_vlad, service_url, args.model_id,
                        pair, day, tier, date_from, date_to
                    )
        log.info("✅ Summary обновлён")

    # ── Финал ─────────────────────────────────────────────────────────────────
    elapsed = deadline.elapsed_str()
    msg = (
        f"model_id={args.model_id}  url={service_url}\n"
        f"Период: {date_from.date()} → {date_to.date()}\n"
        f"Время работы: {elapsed}\n"
        f"Таймаут: {'да' if deadline.exceeded() else 'нет'}"
    )

    if deadline.exceeded():
        send_trace("⏰ cache.py завершён по таймауту", msg, is_error=True)
        log.warning(f"\n⏰ Завершено по таймауту. Прошло: {elapsed}")
    else:
        send_trace("✅ cache.py завершён успешно", msg)
        log.info(f"\n✅ Готово. Прошло: {elapsed}")

    await engine_vlad.dispose()
    await engine_brain.dispose()
    sync_engine.dispose()


# ═══════════════════════════════════════════════════════════════════════════════
#  Точка входа
# ═══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.info("🛑 Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        log.critical(f"❌ Критическая ошибка: {e!r}", exc_info=True)
        send_error_trace(e, "main")
        sys.exit(1)


if __name__ == "__main__":
    main()