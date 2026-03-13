import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, time as time_type

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from dotenv import load_dotenv

from common import (
    MODE, IS_DEV,
    log, send_error_trace,
    ok_response, err_response,
    resolve_workers,
    build_engines,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values

SERVICE_ID = 30
NODE_NAME  = os.getenv("NODE_NAME", "brain-ecb-weights-microservice")
PORT       = 8893

RATE_CHANGE_MAP = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}
TREND_MAP       = {"UNKNOWN": "X", "ABOVE": "A", "BELOW": "B", "AT": "T"}
MOMENTUM_MAP    = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

SMA_SHORT           = 5
SMA_LONG            = 20
THRESHOLD_PCT       = 0.0003
RECURRING_MIN_COUNT = 2

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_ECB_BY_CCY    = {}
GLOBAL_ECB_CONTEXT   = {}
GLOBAL_ECB_OBS_DATES = defaultdict(set)
GLOBAL_ECB_CTX_HIST  = {}
GLOBAL_CTX_INDEX     = {}
GLOBAL_WEIGHT_CODES  = []
GLOBAL_RATES         = {}
GLOBAL_EXTREMUMS     = {}
GLOBAL_CANDLE_RANGES = {}
GLOBAL_AVG_RANGE     = {}
GLOBAL_LAST_CANDLES  = {}
SERVICE_URL          = ""
LAST_RELOAD_TIME     = None


def get_rates_table_name(pair_id, day_flag):
    return {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + (
        "_day" if day_flag == 1 else "")

def get_modification_factor(pair_id):
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)

def parse_date_string(date_str):
    for fmt in ("%Y-%d-%m %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None

def find_prev_candle_trend(table, target_date):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None

def _sma(rates_sorted, idx, window):
    if idx < window - 1:
        return None
    return sum(r for _, r in rates_sorted[idx - window + 1: idx + 1]) / window

def _dir(a, b, up="UP", down="DOWN", flat="FLAT"):
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct >  THRESHOLD_PCT: return up
    if pct < -THRESHOLD_PCT: return down
    return flat

def classify_observations(rates_sorted):
    results = []
    for i, (dt, rate) in enumerate(rates_sorted):
        rcd  = "UNKNOWN" if i == 0 else _dir(rate, rates_sorted[i - 1][1])
        smal = _sma(rates_sorted, i, SMA_LONG)
        td   = "UNKNOWN" if smal is None else _dir(rate, smal, "ABOVE", "BELOW", "AT")
        smas = _sma(rates_sorted, i, SMA_SHORT)
        md   = "UNKNOWN" if (smas is None or smal is None) else _dir(smas, smal)
        results.append((dt, rcd, td, md))
    return results

def make_weight_code(ccy, rcd, td, md, mode, day_shift=None):
    base = (f"{ccy}_{RATE_CHANGE_MAP.get(rcd,'X')}_"
            f"{TREND_MAP.get(td,'X')}_{MOMENTUM_MAP.get(md,'X')}_{mode}")
    return base if day_shift is None else f"{base}_{day_shift}"

def compute_t1_value(t_dates, calc_var, ram_rates, candle_ranges, avg_range):
    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4
    total = 0.0
    for d in t_dates:
        rng = candle_ranges.get(d, 0.0)
        if need_filter and rng <= avg_range:
            continue
        if use_range:
            total += rng - avg_range
        else:
            t1 = ram_rates.get(d, 0.0)
            total += t1 * abs(t1) if use_square else t1
    return total

def compute_extremum_value(t_dates, calc_var, ext_set, candle_ranges, avg_range,
                            modification, total_hist):
    need_filter = calc_var in (1, 3, 4)
    use_range   = calc_var == 4
    pool = [d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range] if need_filter else t_dates
    if not pool:
        return None
    if use_range:
        val = sum(candle_ranges.get(d, 0.0) - avg_range for d in pool if d in ext_set)
        return val if val != 0 else None
    if total_hist == 0:
        return None
    val = ((sum(1 for d in pool if d in ext_set) / total_hist) * 2 - 1) * modification
    return val if val != 0 else None


async def preload_all_data():
    global SERVICE_URL, LAST_RELOAD_TIME
    log("🔄 ECB FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    GLOBAL_WEIGHT_CODES.clear(); GLOBAL_CTX_INDEX.clear()
    GLOBAL_ECB_BY_CCY.clear(); GLOBAL_ECB_CONTEXT.clear()
    GLOBAL_ECB_OBS_DATES.clear(); GLOBAL_ECB_CTX_HIST.clear()
    GLOBAL_RATES.clear(); GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear(); GLOBAL_AVG_RANGE.clear(); GLOBAL_LAST_CANDLES.clear()

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(text("SELECT weight_code FROM vlad_ecb_rate_weights"))
            GLOBAL_WEIGHT_CODES.extend(r["weight_code"] for r in res.mappings().all())
            log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)
        except Exception as e:
            log(f"❌ weight_codes: {e}", NODE_NAME, level="error")

        try:
            res = await conn.execute(text(
                "SELECT currency, rate_change_dir, trend_dir, momentum_dir, occurrence_count "
                "FROM vlad_ecb_rate_context_idx"))
            for r in res.mappings().all():
                key = (r["currency"], r["rate_change_dir"], r["trend_dir"], r["momentum_dir"])
                GLOBAL_CTX_INDEX[key] = {"occurrence_count": r["occurrence_count"] or 0}
            log(f"  ctx_index: {len(GLOBAL_CTX_INDEX)}", NODE_NAME)
        except Exception as e:
            log(f"❌ ctx_index: {e}", NODE_NAME, level="error")

        try:
            res = await conn.execute(text(
                "SELECT currency, rate_date, rate FROM vlad_ecb_exchange_rates "
                "ORDER BY currency, rate_date"))
            by_ccy = defaultdict(list)
            for r in res.mappings().all():
                by_ccy[r["currency"]].append((r["rate_date"], float(r["rate"])))
            for ccy, rates in by_ccy.items():
                GLOBAL_ECB_BY_CCY[ccy] = rates
                for dt, rcd, td, md in classify_observations(rates):
                    GLOBAL_ECB_CONTEXT[(ccy, dt)] = (rcd, td, md)
                    GLOBAL_ECB_OBS_DATES[dt].add(ccy)
                    GLOBAL_ECB_CTX_HIST.setdefault((ccy, rcd, td, md), []).append(dt)
            for key in GLOBAL_ECB_CTX_HIST:
                GLOBAL_ECB_CTX_HIST[key].sort()
            log(f"  ECB currencies: {len(by_ccy)}, observations: {len(GLOBAL_ECB_CONTEXT)}",
                NODE_NAME)
        except Exception as e:
            log(f"❌ ECB rates: {e}", NODE_NAME, level="error")

    for table in ["brain_rates_eur_usd", "brain_rates_eur_usd_day",
                  "brain_rates_btc_usd", "brain_rates_btc_usd_day",
                  "brain_rates_eth_usd", "brain_rates_eth_usd_day"]:
        GLOBAL_RATES[table] = {}; GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_CANDLE_RANGES[table] = {}; GLOBAL_AVG_RANGE[table] = 0.0
        GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}
        try:
            async with engine_brain.connect() as conn:
                res    = await conn.execute(text(
                    f"SELECT date, open, close, `max`, `min`, t1 FROM {table}"))
                rows   = sorted(res.mappings().all(), key=lambda x: x["date"])
                ranges = []
                for r in rows:
                    dt = r["date"]
                    if r["t1"] is not None:
                        GLOBAL_RATES[table][dt] = float(r["t1"])
                    GLOBAL_LAST_CANDLES[table].append((dt, r["close"] > r["open"]))
                    rng = float(r["max"] or 0) - float(r["min"] or 0)
                    GLOBAL_CANDLE_RANGES[table][dt] = rng
                    ranges.append(rng)
                GLOBAL_AVG_RANGE[table] = sum(ranges) / len(ranges) if ranges else 0.0
                for typ in ("min", "max"):
                    op = ">" if typ == "max" else "<"
                    q  = f"""SELECT t1.date FROM {table} t1
                        JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 DAY
                        JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 DAY
                        WHERE t1.{typ} {op} t_prev.{typ} AND t1.{typ} {op} t_next.{typ}"""
                    res_ext = await conn.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}
            log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)
        except Exception as e:
            log(f"❌ {table}: {e}", NODE_NAME, level="error")

    SERVICE_URL      = await load_service_url(engine_super, SERVICE_ID)
    await ensure_cache_table(engine_vlad)
    LAST_RELOAD_TIME = datetime.now()
    log("✅ ECB FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f"❌ Background reload: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "ecb_background_reload")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_all_data()
    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()
    await engine_vlad.dispose()
    await engine_brain.dispose()
    await engine_super.dispose()


app = FastAPI(lifespan=lifespan)


async def calculate_pure_memory(pair, day, date_str, calc_type=0, calc_var=0):
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window       = 12
    target_d     = target_date.date() if isinstance(target_date, datetime) else target_date
    check_dates  = [target_d + timedelta(days=d) for d in range(-window, window + 1)]

    observations = []
    for dt in check_dates:
        for ccy in GLOBAL_ECB_OBS_DATES.get(dt, set()):
            ctx = GLOBAL_ECB_CONTEXT.get((ccy, dt))
            if ctx:
                observations.append((ccy, dt, ctx, (target_d - dt).days))

    if not observations:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}
    for ccy, obs_date, (rcd, td, md), shift in observations:
        ctx_key  = (ccy, rcd, td, md)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue
        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > window:
            continue

        all_ctx_dates = GLOBAL_ECB_CTX_HIST.get(ctx_key, [])
        idx           = bisect.bisect_left(all_ctx_dates, target_d)
        valid_dates   = all_ctx_dates[:idx]
        if not valid_dates:
            continue

        t_dates = [
            datetime.combine(d + timedelta(days=shift), time_type(0, 0))
            for d in valid_dates
            if datetime.combine(d + timedelta(days=shift), time_type(0, 0)) < target_date
        ]
        shift_arg = shift if is_recurring else None

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc     = make_weight_code(ccy, rcd, td, md, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            ext_val    = compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges,
                                                avg_range, modification, len(valid_dates))
            if ext_val is not None:
                wc = make_weight_code(ccy, rcd, td, md, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


@app.get("/")
async def get_metadata():
    for t in ["vlad_ecb_rate_weights", "vlad_ecb_rate_context_idx",
              "vlad_ecb_exchange_rates", "version_microservice"]:
        try:
            async with engine_vlad.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"vlad.{t} inaccessible: {e}"}
    for t in ["brain_rates_eur_usd", "brain_rates_btc_usd", "brain_rates_eth_usd"]:
        try:
            async with engine_brain.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"brain.{t} inaccessible: {e}"}
    async with engine_vlad.connect() as conn:
        res     = await conn.execute(text(
            "SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        version = (res.fetchone() or [0])[0]
    return {
        "status": "ok", "version": f"1.{version}.0", "mode": MODE,
        "name": NODE_NAME, "text": "ECB exchange rate context weights",
        "weight_code_format": "{currency}_{rcd}_{td}_{md}_{mode}[_{day_shift}]",
        "metadata": {
            "ctx_index_rows": len(GLOBAL_CTX_INDEX),
            "weight_codes":   len(GLOBAL_WEIGHT_CODES),
            "ecb_currencies": len(GLOBAL_ECB_BY_CCY),
            "last_reload":    LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
        },
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response(GLOBAL_WEIGHT_CODES)
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_weights")


@app.get("/values")
async def get_values(
    pair: int = Query(1), day: int = Query(1), date: str = Query(...),
    type: int = Query(0, ge=0, le=2), var: int = Query(0, ge=0, le=4),
):
    try:
        return await cached_values(
            engine_vlad  = engine_vlad,
            service_url  = SERVICE_URL,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = {"type": type, "var": var},
            compute_fn   = lambda: calculate_pure_memory(pair, day, date,
                                                          calc_type=type, calc_var=var),
            node         = NODE_NAME,
        )
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_values")


@app.post("/patch")
async def patch_service():
    async with engine_vlad.begin() as conn:
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        row = res.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail=f"Service ID {SERVICE_ID} not found")
        old = row[0]; new = max(old, 3)
        if new != old:
            await conn.execute(
                text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                {"v": new, "id": SERVICE_ID})
    return {"status": "ok", "from_version": old, "to_version": new}


if __name__ == "__main__":
    import asyncio as _asyncio
    async def _get_workers():
        return await resolve_workers(engine_super, SERVICE_ID, default=1)
    _workers = _asyncio.run(_get_workers())
    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False, workers=_workers)
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
