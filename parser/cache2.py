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

# ── ID моделей — перечисли нужные через запятую ───────────────────────────────
MODEL_IDS = [35]

# ── Логирование ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Трассировка ───────────────────────────────────────────────────────────────
_HANDLER  = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL = f"{_HANDLER}/trace.php"
NODE_NAME   = os.getenv("NODE_NAME",   "cache_runner")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_trace(subject: str, body: str, is_error: bool = False) -> None:
    level = "ERROR" if is_error else "INFO"
    full_body = f"[{level}] {subject}\n\nNode: {NODE_NAME}\n\n{body}"
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME, "email": ALERT_EMAIL, "logs": full_body},
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
    print(f"\r    {label} [{done}/{total}] {pct:.1f}%{extra}", end="", flush=True)


def _compute_signal(values: dict, tier: int) -> int:
    if not values:
        return 0
    total = (
        sum(math.copysign(1.0, v) for v in values.values() if v != 0)
        if tier == 0
        else sum(values.values())
    )
    return 1 if total > 0 else (-1 if total < 0 else 0)


async def _call_values(session: aiohttp.ClientSession,
                       url: str, params: dict) -> dict | None:
    try:
        async with session.get(
            f"{url}/values",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                log.warning(f"[CACHE] HTTP {r.status} для date={params.get('date')} params={params}")
                return None
            data = await r.json()
            if "payLoad" in data:
                payload = data["payLoad"]
            elif "payload" in data:
                payload = data["payload"]
            elif "status" not in data:
                payload = data
            else:
                log.warning(f"[CACHE] Нет payLoad в ответе для date={params.get('date')} params={params}: {data}")
                return None
            # Если сервис вернул {"status":"error",...}
            if isinstance(payload, dict) and payload.get("status") == "error":
                log.warning(f"[CACHE] Сервис вернул error для date={params.get('date')}: {payload}")
                return None
            return payload
    except asyncio.TimeoutError:
        log.warning(f"[CACHE] Таймаут для date={params.get('date')} url={url}")
        return None
    except Exception as e:
        log.warning(f"[CACHE] Исключение для date={params.get('date')}: {e}")
        return None


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


async def fill_cache(engine_vlad, candles: list[dict],
                     service_url: str, pair: int, day: int,
                     extra_params: dict,
                     deadline: Deadline) -> dict:
    if not candles:
        return {"done": 0, "total": 0, "errors": 0, "skipped": 0, "new": 0, "error_dates": []}

    p_hash = _params_hash(extra_params)
    total  = len(candles)
    done   = errors = skipped = 0
    error_dates = []   # накапливаем даты с ошибками

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT date_val FROM vlad_values_cache
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND params_hash = :ph
        """), {"url": service_url, "pair": pair, "day": day, "ph": p_hash})
        cached_dates = {row[0] for row in res.fetchall()}

    async with aiohttp.ClientSession() as session:
        for candle in candles:
            if deadline.exceeded():
                log.warning(f"\n  ⏰ Таймаут! Осталось обработать {total - done} свечей")
                break

            date_val = candle["date"]

            if date_val in cached_dates:
                skipped += 1
                done    += 1
                _print_progress(done, total, "cache", errors, skipped)
                continue

            date_str    = date_val.strftime("%Y-%m-%d %H:%M:%S")
            call_params = {"pair": pair, "day": day, "date": date_str, **extra_params}
            result = await _call_values(session, service_url, call_params)

            if result is None:
                errors += 1
                done   += 1
                error_dates.append(date_str)
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
    new = done - skipped - errors
    return {"done": done, "total": total, "errors": errors, "skipped": skipped,
            "new": new, "error_dates": error_dates}


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

    spread, modification, lot_divisor = PAIR_CFG.get(pair, (0.0002, 100_000.0, 10_000.0))

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
                "url":  service_url, "mid":  model_id,
                "pair": pair,        "day":  day,
                "tier": tier,        "ph":   p_hash,
                "pj":   json.dumps(extra_params, ensure_ascii=False),
                "df":   date_from,   "dt":   date_to,
                "bf":   result["balance_final"],
                "tr":   result["total_result"],
                "sl":   result["summary_lost"],
                "vs":   result["value_score"],
                "tc":   trade_count,
                "wc":   win_count,
                "acc":  result["accuracy"],
            })
    except Exception as e:
        log.warning(f"  ⚠️  Не удалось сохранить результат бэктеста: {e}")

    return result


async def upsert_summary(engine_vlad, service_url: str, model_id: int,
                          pair: int, day: int, tier: int,
                          date_from: datetime, date_to: datetime) -> None:
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text("""
            SELECT
                COUNT(*)         AS cnt,
                MAX(value_score) AS best_score,
                AVG(value_score) AS avg_score,
                MAX(accuracy)    AS best_acc,
                AVG(accuracy)    AS avg_acc,
                MAX(CASE WHEN value_score = (
                    SELECT MAX(value_score) FROM vlad_backtest_results r2
                    WHERE r2.service_url = :url AND r2.pair = :pair
                      AND r2.day_flag = :day    AND r2.tier = :tier
                      AND r2.date_from = :df    AND r2.date_to = :dt
                ) THEN params_json END) AS best_pj
            FROM vlad_backtest_results
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day    AND tier = :tier
              AND date_from = :df    AND date_to = :dt
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
            VALUES
                (:mid, :url, :pair, :day, :tier, :df, :dt,
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
            "pair": pair,    "day": day,
            "tier": tier,    "df":  date_from, "dt": date_to,
            "cnt": row[0],
            "bs":  float(row[1] or 0), "as_": float(row[2] or 0),
            "ba":  float(row[3] or 0), "aa":  float(row[4] or 0),
            "bpj": row[5],
        })


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
    stats_cache    = {"new": 0, "skipped": 0, "errors": 0}
    stats_backtest = {"done": 0, "skipped": 0, "failed": 0}
    timed_out      = False

    log.info(f"\n{'#'*60}\n  🤖 model_id={model_id}  url={service_url}\n  Комбинации: {len(param_combos)}\n{'#'*60}")

    for pair in args.pairs:
        for day in args.days:
            if deadline.exceeded():
                timed_out = True
                break

            log.info(
                f"\n{'='*55}\n"
                f"  model={model_id}  pair={pair} ({PAIR_NAMES.get(pair)})  "
                f"day={day} ({DAY_NAMES.get(day)})  "
                f"[осталось: {deadline.remaining_str()}]"
            )

            candles = await fetch_candles(engine_brain, pair, day, date_from, date_to)
            if not candles:
                log.warning("  ⚠️  Нет свечей — пропускаем")
                continue
            log.info(f"  Свечей: {len(candles)}")

            if not args.skip_fill:
                log.info(f"\n  📥 Кеш ({len(param_combos)} комбинаций)...")
                for idx, combo in enumerate(param_combos, 1):
                    if deadline.exceeded():
                        timed_out = True
                        break
                    combo_str = json.dumps(combo) if combo else "{}"
                    log.info(f"  [{idx}/{len(param_combos)}] params={combo_str}")
                    r = await fill_cache(engine_vlad, candles, service_url, pair, day, combo, deadline)
                    stats_cache["new"]     += r["new"]
                    stats_cache["skipped"] += r["skipped"]
                    stats_cache["errors"]  += r["errors"]
                    log.info(f"  ✓ total={r['total']}  new={r['new']}  skip={r['skipped']}  err={r['errors']}")

                    if r["errors"] > 0:
                        sample = r["error_dates"][:50]
                        tail   = f"\n  ... и ещё {len(r["error_dates"]) - 50}" if len(r["error_dates"]) > 50 else ""
                        send_trace(
                            f"⚠️ Ошибки кеша — model={model_id} pair={pair} day={day} params={combo_str}",
                            f"model={model_id}  pair={PAIR_NAMES.get(pair)}  day={DAY_NAMES.get(day)}\n"
                            f"params={combo_str}\nurl={service_url}\n\n"
                            f"Ошибок: {r["errors"]} из {r["total"]}\n"
                            f"Примеры дат:\n" + "\n".join(f"  {d}" for d in sample) + tail,
                            is_error=True,
                        )

                if not timed_out:
                    send_trace(
                        f"✅ Кеш завершён — pair={pair} day={day} model={model_id}",
                        f"pair={PAIR_NAMES.get(pair)}  day={DAY_NAMES.get(day)}\n"
                        f"new={stats_cache['new']}  skipped={stats_cache['skipped']}  errors={stats_cache['errors']}\n"
                        f"Прошло: {deadline.elapsed_str()}  Осталось: {deadline.remaining_str()}",
                    )
            else:
                log.info("  ⏭️  Кеш пропущен (--skip-fill)")

            if args.only_fill or timed_out:
                continue

            for tier in args.tiers:
                if deadline.exceeded():
                    timed_out = True
                    break

                log.info(f"\n  🧪 Бэктест tier={tier} ({len(param_combos)} комбинаций)...")
                results = []
                for idx, combo in enumerate(param_combos, 1):
                    if deadline.exceeded():
                        timed_out = True
                        break
                    combo_str = json.dumps(combo) if combo else "{}"
                    log.info(f"  [{idx}/{len(param_combos)}] params={combo_str}")
                    r = await run_backtest(
                        engine_vlad, candles, service_url, pair, day, tier,
                        combo, date_from, date_to, model_id, deadline,
                    )
                    if r.get("skipped"):
                        stats_backtest["skipped"] += 1
                        log.info(f"  ⏭  уже посчитано: score={r['value_score']}  acc={r['accuracy']}")
                    elif "error" in r:
                        stats_backtest["failed"] += 1
                        log.info(f"  ✗ {r['error']} (trades={r.get('trade_count', 0)})")
                    else:
                        stats_backtest["done"] += 1
                        results.append(r)
                        log.info(f"  ✓ score={r['value_score']:>10.2f}  acc={r['accuracy']:.3f}  trades={r['trade_count']}")

                await upsert_summary(engine_vlad, service_url, model_id, pair, day, tier, date_from, date_to)

                log.info(f"\n  📊 tier={tier}: done={len(results)}  skip={stats_backtest['skipped']}  fail={stats_backtest['failed']}")

                if results:
                    best = max(results, key=lambda x: x["value_score"])
                    log.info(f"  🏆 Лучший: score={best['value_score']}  acc={best['accuracy']}  trades={best['trade_count']}  params={best['params']}")

                if not timed_out:
                    best_score = max((r["value_score"] for r in results), default=0)
                    send_trace(
                        f"✅ Бэктест завершён — pair={pair} day={day} tier={tier} model={model_id}",
                        f"pair={PAIR_NAMES.get(pair)}  day={DAY_NAMES.get(day)}  tier={tier}\n"
                        f"done={len(results)}  skip={stats_backtest['skipped']}  fail={stats_backtest['failed']}\n"
                        f"best_score={best_score}\n"
                        f"Прошло: {deadline.elapsed_str()}  Осталось: {deadline.remaining_str()}",
                    )

        if timed_out:
            break

    return stats_cache, stats_backtest, timed_out


async def run(args) -> None:
    deadline = Deadline(hours=args.timeout_hours)
    log.info(f"⏰ Таймаут: {args.timeout_hours}h  (дедлайн: {deadline.deadline.strftime('%Y-%m-%d %H:%M:%S')})")

    # ── Все параметры подключения ТОЛЬКО из env ───────────────────────────────
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
    log.info(f"🚀 models={MODEL_IDS}")
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
                log.error(f"❌ Модель {model_id}: не удалось получить конфигурацию: {e}")
                send_error_trace(e, f"get_service_config model={model_id}")
        sync_engine.dispose()
    except Exception as e:
        log.critical(f"❌ Не удалось подключиться к super DB: {e}")
        send_error_trace(e, "sync_engine_connect")
        sys.exit(1)

    if not model_configs:
        log.critical("❌ Ни одна модель не загружена. Выходим.")
        sys.exit(1)

    engine_vlad  = create_async_engine(vlad_url,  pool_size=10, echo=False)
    engine_brain = create_async_engine(brain_url, pool_size=6,  echo=False)

    try:
        async with engine_vlad.begin() as conn:
            for ddl in (DDL_CACHE, DDL_BACKTEST, DDL_SUMMARY):
                await conn.execute(text(ddl))
        log.info("✅ Таблицы проверены/созданы")
    except Exception as e:
        log.critical(f"❌ Ошибка создания таблиц: {e}")
        send_error_trace(e, "ensure_tables")
        sys.exit(1)

    date_from = _parse_dt(args.date_from) if args.date_from else datetime(2025, 1, 15)
    date_to   = (
        _parse_dt(args.date_to) if args.date_to
        else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    )

    log.info(f"  Период : {date_from} → {date_to}")
    log.info(f"  Модели : {[m[0] for m in model_configs]}")
    log.info(f"  Пары   : {args.pairs}")
    log.info(f"  Дни    : {args.days}")
    log.info(f"  Тиры   : {args.tiers}")

    all_timed_out = False

    try:
        for model_id, service_url, param_combos in model_configs:
            if deadline.exceeded():
                all_timed_out = True
                break

            stats_cache, stats_backtest, timed_out = await run_model(
                model_id=model_id, service_url=service_url, param_combos=param_combos,
                engine_vlad=engine_vlad, engine_brain=engine_brain,
                args=args, date_from=date_from, date_to=date_to, deadline=deadline,
            )

            elapsed = deadline.elapsed_str()

            if timed_out:
                all_timed_out = True
                msg = (
                    f"⏰ Модель {model_id} остановлена по таймауту ({args.timeout_hours}h).\n"
                    f"Прошло: {elapsed}\n\n"
                    f"Кеш  : new={stats_cache['new']}  skip={stats_cache['skipped']}  err={stats_cache['errors']}\n"
                    f"Бэктест: done={stats_backtest['done']}  skip={stats_backtest['skipped']}  fail={stats_backtest['failed']}\n\n"
                    f"Запусти повторно — скрипт продолжит с места остановки."
                )
                log.warning(f"\n{msg}")
                send_trace(f"⏰ Таймаут — model={model_id}", msg, is_error=False)
                break
            else:
                msg = (
                    f"✅ Модель {model_id} завершена.\n"
                    f"Прошло: {elapsed}\n\n"
                    f"Кеш  : new={stats_cache['new']}  skip={stats_cache['skipped']}  err={stats_cache['errors']}\n"
                    f"Бэктест: done={stats_backtest['done']}  skip={stats_backtest['skipped']}  fail={stats_backtest['failed']}"
                )
                log.info(f"\n{'='*55}\n{msg}\n{'='*55}")
                send_trace(f"✅ Готово — model={model_id}", msg)

        elapsed   = deadline.elapsed_str()
        completed = [m[0] for m in model_configs]

        if all_timed_out:
            final_msg = (
                f"⏰ Прогон остановлен по таймауту {args.timeout_hours}h.\n"
                f"Прошло: {elapsed}\nМодели: {completed}\n"
                f"Запусти повторно — скрипт продолжит с места остановки."
            )
            log.warning(f"\n{final_msg}")
            send_trace(f"⏰ Таймаут — models={MODEL_IDS}", final_msg, is_error=False)
        else:
            final_msg = f"✅ Все модели завершены.\nПрошло: {elapsed}\nМодели: {completed}"
            log.info(f"\n{'='*55}\n{final_msg}\n{'='*55}")
            send_trace(f"✅ Все модели готовы — {MODEL_IDS}", final_msg)

    except Exception as e:
        log.critical(f"❌ Критическая ошибка: {e!r}")
        send_error_trace(e, "main_loop")
        raise
    finally:
        await engine_vlad.dispose()
        await engine_brain.dispose()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Кеш + бэктест. Подключение из env (VLAD_HOST/VLAD_PORT/VLAD_USER/VLAD_PASSWORD/VLAD_DATABASE).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python cache.py
  python cache.py --date-from 2025-01-15 --date-to 2026-03-12
  python cache.py --pair 1 --day 0
  python cache.py --only-fill
  python cache.py --skip-fill
  python cache.py --timeout-hours 12
        """,
    )
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to",   default=None)
    parser.add_argument("--pair",  type=int, nargs="+", default=[1, 3, 4], choices=[1, 3, 4], dest="pairs")
    parser.add_argument("--day",   type=int, nargs="+", default=[0, 1],    choices=[0, 1],    dest="days")
    parser.add_argument("--tier",  type=int, nargs="+", default=[0, 1],    choices=[0, 1],    dest="tiers")
    parser.add_argument("--skip-fill",      action="store_true")
    parser.add_argument("--only-fill",      action="store_true")
    parser.add_argument("--timeout-hours",  type=float, default=24.0)

    # ── Игнорируем любые неизвестные позиционные/именованные аргументы ────────
    # Это нужно если скрипт запускается через враппер, который передаёт
    # лишние аргументы (например: vlad_values_cache 127.0.0.1 3306 root pass db)
    args, unknown = parser.parse_known_args()
    if unknown:
        log.warning(f"⚠️  Игнорируем неизвестные аргументы: {unknown}")
    return args


def main():
    args = parse_args()

    log.info("=" * 60)
    log.info("⚙️  Параметры запуска")
    log.info(f"   models        : {MODEL_IDS}")
    log.info(f"   date_from     : {args.date_from or '2025-01-15 (default)'}")
    log.info(f"   date_to       : {args.date_to   or 'today (default)'}")
    log.info(f"   pairs         : {args.pairs}")
    log.info(f"   days          : {args.days}")
    log.info(f"   tiers         : {args.tiers}")
    log.info(f"   skip_fill     : {args.skip_fill}")
    log.info(f"   only_fill     : {args.only_fill}")
    log.info(f"   timeout_hours : {args.timeout_hours}")
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
