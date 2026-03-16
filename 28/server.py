import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

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

SERVICE_ID = 28
NODE_NAME  = os.getenv("NODE_NAME", "brain-investing-context-weights-microservice")
PORT       = 8892

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

THRESHOLD           = 0.0001
RECURRING_MIN_COUNT = 2

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_EXTREMUMS     = {}
GLOBAL_RATES         = {}
GLOBAL_CANDLE_RANGES = {}
GLOBAL_AVG_RANGE     = {}
GLOBAL_CALENDAR      = {}
GLOBAL_HISTORY       = {}
GLOBAL_LAST_CANDLES  = {}
GLOBAL_CTX_INDEX     = {}
GLOBAL_WEIGHT_CODES  = []
SERVICE_URL          = ""
LAST_RELOAD_TIME     = None


def get_rates_table_name(pair_id, day_flag):
    return {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + (
        "_day" if day_flag == 1 else "")

def get_modification_factor(pair_id):
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)

_DATE_FORMATS_28 = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",    # ← NEW
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",    # ← NEW
    "%Y-%m-%d",
    "%Y-%d-%m %H:%M:%S",
    "%Y-%d-%m %H:%M:%S.%f",    # ← NEW
)
 
 
def parse_date_string(date_str):
    for fmt in _DATE_FORMATS_28:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    log(f"⚠️  parse_date_string failed: repr={date_str!r}", NODE_NAME, level="error", force=True)
    return None

def find_prev_candle_trend(table, target_date):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None

def _direction(a, b, beat_label="UP", miss_label="DOWN", inline_label="FLAT"):
    if a is None or b is None:
        return "UNKNOWN"
    if a > b + THRESHOLD: return beat_label
    if a < b - THRESHOLD: return miss_label
    return inline_label

def resolve_event_context(actual, forecast, previous):
    fdir = _direction(forecast, previous, "UP",   "DOWN",   "FLAT")
    sdir = _direction(actual,   forecast,  "BEAT", "MISS",   "INLINE")
    adir = _direction(actual,   previous,  "UP",   "DOWN",   "FLAT")
    return fdir, sdir, adir

def try_float(val):
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None

def build_weight_code(event_id, fdir, sdir, adir, mode, hour=None):
    base = f"{event_id}__{fdir}__{sdir}__{adir}__{mode}"
    return base if hour is None else f"{base}__{hour}"

def get_last_known_context(event_id, before_date):
    history = GLOBAL_HISTORY.get(event_id, [])
    idx = bisect.bisect_left(history, before_date)
    if idx == 0:
        return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    for i in range(idx - 1, -1, -1):
        dt = history[i]
        for ev in GLOBAL_CALENDAR.get(dt, []):
            if ev["EventId"] == event_id:
                return resolve_event_context(ev["actual"], ev["forecast"], ev["previous"])
    return "UNKNOWN", "UNKNOWN", "UNKNOWN"

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
    log("🔄 FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("SELECT weight_code FROM vlad_investing_weights"))
        GLOBAL_WEIGHT_CODES[:] = [r["weight_code"] for r in res.mappings().all()]
        log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)

        res = await conn.execute(text("""
            SELECT event_id, forecast_direction, surprise_direction, actual_direction,
                   occurrence_count, importance, currency
            FROM vlad_investing_event_context_idx
        """))
        GLOBAL_CTX_INDEX.clear()
        for r in res.mappings().all():
            key = (r["event_id"], r["forecast_direction"],
                   r["surprise_direction"], r["actual_direction"])
            GLOBAL_CTX_INDEX[key] = {
                "occurrence_count": r["occurrence_count"] or 0,
                "importance": r["importance"], "currency": r["currency"],
            }
        log(f"  ctx_index: {len(GLOBAL_CTX_INDEX)}", NODE_NAME)

        res = await conn.execute(text("""
            SELECT event_id, occurrence_time_utc, importance, actual, forecast, previous
            FROM vlad_investing_calendar WHERE event_id IS NOT NULL
        """))
        GLOBAL_CALENDAR.clear(); GLOBAL_HISTORY.clear()
        for r in res.mappings().all():
            dt, eid = r["occurrence_time_utc"], r["event_id"]
            GLOBAL_HISTORY.setdefault(eid, []).append(dt)
            GLOBAL_CALENDAR.setdefault(dt, []).append({
                "EventId": eid, "Importance": r["importance"], "event_date": dt,
                "actual":   try_float(r["actual"]),
                "forecast": try_float(r["forecast"]),
                "previous": try_float(r["previous"]),
            })
        for eid in GLOBAL_HISTORY:
            GLOBAL_HISTORY[eid].sort()

    for table in ["brain_rates_eur_usd", "brain_rates_eur_usd_day",
                  "brain_rates_btc_usd", "brain_rates_btc_usd_day",
                  "brain_rates_eth_usd", "brain_rates_eth_usd_day"]:
        GLOBAL_RATES[table] = {}; GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_CANDLE_RANGES[table] = {}; GLOBAL_AVG_RANGE[table] = 0.0
        GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}
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
                op  = ">" if typ == "max" else "<"
                col = "max" if typ == "max" else "min"
                q   = f"""SELECT t1.date FROM {table} t1
                    JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                    JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                    WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}"""
                res_ext = await conn.execute(text(q))
                GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}
        log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)

    SERVICE_URL      = await load_service_url(engine_super, SERVICE_ID)
    await ensure_cache_table(engine_vlad)
    LAST_RELOAD_TIME = datetime.now()
    log("✅ FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f"❌ Background reload: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "server_background_reload")


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
        log(f"❌ calculate_pure_memory: date parse failed | "
            f"date_str={date_str!r} pair={pair} day={day} calc_type={calc_type} calc_var={calc_var}",
            NODE_NAME, level="error", force=True)
        return None
 
    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window       = 12
    check_dates  = (
        [target_date + timedelta(hours=h) for h in range(-window, window + 1)]
        if day == 0 else
        [target_date + timedelta(days=d)  for d in range(-window, window + 1)]
    )
 
    events_in_window = [
        e for dt in check_dates for e in GLOBAL_CALENDAR.get(dt, [])
        if e["Importance"] != 1 or dt == target_date
    ]
    if not events_in_window:
        log(f"ℹ️  calculate_pure_memory: no events in window | "
            f"date={date_str!r} pair={pair} day={day}",
            NODE_NAME, force=False)
        return {}
 
    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)
 
    result = {}
    for e in events_in_window:
        diff  = target_date - e["event_date"]
        shift = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        eid   = e["EventId"]
 
        fdir, sdir, adir = get_last_known_context(eid, target_date)
        ctx_key  = (eid, fdir, sdir, adir)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue
 
        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > window:
            continue
 
        valid_dates = [d for d in GLOBAL_HISTORY.get(eid, []) if d < target_date]
        if not valid_dates:
            continue
 
        delta  = timedelta(hours=shift) if day == 0 else timedelta(days=shift)
        t_dates = [d + delta for d in valid_dates if (d + delta) < target_date]
        hour_arg = shift if is_recurring else None
 
        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc     = build_weight_code(eid, fdir, sdir, adir, 0, hour_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum
 
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            ext_val    = compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges,
                                                avg_range, modification, len(valid_dates))
            if ext_val is not None:
                wc = build_weight_code(eid, fdir, sdir, adir, 1, hour_arg)
                result[wc] = result.get(wc, 0.0) + ext_val
 
    return {k: round(v, 6) for k, v in result.items() if v != 0}


@app.get("/")
async def get_metadata():
    for t in ["vlad_investing_weights", "vlad_investing_event_context_idx",
              "vlad_investing_calendar", "version_microservice"]:
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
        "name": NODE_NAME, "text": "Event context weights (fdir/sdir/adir)",
        "weight_code_format": "{event_id}__{fdir}__{sdir}__{adir}__{mode}[__{hour}]",
        "metadata": {
            "ctx_index_rows": len(GLOBAL_CTX_INDEX),
            "weight_codes":   len(GLOBAL_WEIGHT_CODES),
            "last_reload":    LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
        },
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response(GLOBAL_WEIGHT_CODES)
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(str(e))


@app.get("/new_weights")
async def get_new_weights(
    code: str = Query(...),
    limit: int = Query(500, ge=1, le=5000),
):
    try:
        parts = code.split("__")
        if len(parts) < 5:
            return err_response("Invalid weight_code format")
        try:
            event_id = int(parts[0])
        except ValueError:
            return err_response("event_id must be an integer")
        fdir, sdir, adir = parts[1], parts[2], parts[3]
        try:
            mode = int(parts[4])
            hour = int(parts[5]) if len(parts) > 5 else None
        except ValueError:
            return err_response("mode/hour must be integers")
        hour_val = hour if hour is not None else -999999
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT weight_code FROM vlad_investing_weights
                WHERE
                    event_id > :event_id
                    OR (event_id = :event_id AND forecast_direction > :fdir)
                    OR (event_id = :event_id AND forecast_direction = :fdir AND surprise_direction > :sdir)
                    OR (event_id = :event_id AND forecast_direction = :fdir AND surprise_direction = :sdir AND actual_direction > :adir)
                    OR (event_id = :event_id AND forecast_direction = :fdir AND surprise_direction = :sdir AND actual_direction = :adir AND `mode_val` > :mode)
                    OR (event_id = :event_id AND forecast_direction = :fdir AND surprise_direction = :sdir AND actual_direction = :adir AND `mode_val` = :mode AND COALESCE(hour_shift, -999999) > :hour)
                ORDER BY event_id, forecast_direction, surprise_direction, actual_direction,
                         `mode_val`, hour_shift IS NULL, hour_shift
                LIMIT :limit
            """), {
                "event_id": event_id, "fdir": fdir, "sdir": sdir, "adir": adir,
                "mode": mode, "hour": hour_val, "limit": limit,
            })
        return ok_response([r["weight_code"] for r in res.mappings().all()])
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_new_weights")
        return err_response(str(e))


@app.get("/values")
async def get_values(
    pair: int = Query(1), day: int = Query(0), date: str = Query(...),
    type: int = Query(0, ge=0, le=2), var: int = Query(0, ge=0, le=4),
):
    try:
        return await cached_values(
            engine_vlad=engine_vlad,
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
        send_error_trace(e, node=NODE_NAME, script="get_values")
        return err_response(str(e))


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
