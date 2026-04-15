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
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from dotenv import load_dotenv

from common import (
    MODE, IS_DEV,
    log, send_error_trace,
    ok_response, err_response,
    resolve_workers,
    build_engines,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values

SERVICE_ID = 25
NODE_NAME  = os.getenv("NODE_NAME", "brain-investing-weights-microservice")
PORT       = 8890

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

#  VAR конфиги 
VAR_CONFIGS = {
    0: (-12, 12,  50,   "bayes", 10),
    1: (-12, 12,  75,   "bayes", 10),
    2: (-6,   6,  50,   "bayes", 10),
    3: (-24, 24,  50,   "bayes", 10),
    4: (-12, 12,  None, "none",  0),
}

def confidence_bayes(count, prior=10):
    return count / (count + prior) if count > 0 else 0.0

def confidence_none(count, prior=0):
    return 1.0

CONFIDENCE_FUNCS = {"bayes": confidence_bayes, "none": confidence_none}

#  Глобальные данные 
GLOBAL_EXTREMUMS        = {}
GLOBAL_RATES            = {}
GLOBAL_CALENDAR         = {}
_CAL_SORTED_DATES = []
_CAL_SORTED_DATA  = []
GLOBAL_HISTORY          = {}
GLOBAL_LAST_CANDLES     = {}
GLOBAL_WEIGHT_CODES     = []
GLOBAL_EVENT_TYPES      = {}
GLOBAL_CANDLE_SIZES     = {}
GLOBAL_CANDLE_THRESHOLD = {}
SERVICE_URL             = ""
LAST_RELOAD_TIME        = None


def get_rates_table_name(pair_id, day_flag):
    return {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + (
        "_day" if day_flag == 1 else "")

def get_modification_factor(pair_id):
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)

def parse_date_string(date_str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%d-%m %H:%M:%S"):
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


async def preload_all_data():
    global SERVICE_URL, LAST_RELOAD_TIME
    log(" FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("SELECT weight_code FROM vlad_investing_weights_table"))
        GLOBAL_WEIGHT_CODES[:] = [r["weight_code"] for r in res.mappings().all()]
        log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)

        res = await conn.execute(text("SELECT event_id, occurrence_count FROM vlad_investing_event_index"))
        GLOBAL_EVENT_TYPES.clear()
        for r in res.mappings().all():
            GLOBAL_EVENT_TYPES[r["event_id"]] = 1 if (r["occurrence_count"] or 0) > 1 else 0

    #  Календарь событий — читается из Brain 
    async with engine_brain.connect() as conn:
        res = await conn.execute(text("""
            SELECT c.event_id, c.occurrence_time_utc, c.importance
            FROM vlad_investing_calendar c WHERE c.event_id IS NOT NULL
        """))
        GLOBAL_CALENDAR.clear(); GLOBAL_HISTORY.clear()
        for r in res.mappings().all():
            dt, eid, imp = r["occurrence_time_utc"], r["event_id"], r["importance"]
            GLOBAL_HISTORY.setdefault(eid, []).append(dt)
            GLOBAL_CALENDAR.setdefault(dt, []).append({"EventId": eid, "Importance": imp, "event_date": dt})
        log(f"  calendar: {sum(len(v) for v in GLOBAL_CALENDAR.values())} events", NODE_NAME)

    #  bisect: перестройка отсортированных списков 
    if GLOBAL_CALENDAR:
        _si = sorted(GLOBAL_CALENDAR.items())
        _CAL_SORTED_DATES[:] = [x[0] for x in _si]
        _CAL_SORTED_DATA[:]  = [x[1] for x in _si]
    else:
        _CAL_SORTED_DATES.clear()
        _CAL_SORTED_DATA.clear()

    tables = [
        "brain_rates_eur_usd", "brain_rates_eur_usd_day",
        "brain_rates_btc_usd", "brain_rates_btc_usd_day",
        "brain_rates_eth_usd", "brain_rates_eth_usd_day",
    ]
    for table in tables:
        GLOBAL_RATES[table] = {}; GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}
        async with engine_brain.connect() as conn:
            res        = await conn.execute(text(f"SELECT date, open, close, t1 FROM {table}"))
            rows       = sorted(res.mappings().all(), key=lambda x: x["date"])
            sizes_list = []
            for r in rows:
                dt = r["date"]
                if r["t1"] is not None:
                    GLOBAL_RATES[table][dt] = float(r["t1"])
                GLOBAL_LAST_CANDLES[table].append((dt, r["close"] > r["open"]))
                sz = abs(float(r["close"] or 0) - float(r["open"] or 0))
                GLOBAL_CANDLE_SIZES.setdefault(table, {})[dt] = sz
                sizes_list.append(sz)
            sizes_list.sort(); n = len(sizes_list)
            GLOBAL_CANDLE_THRESHOLD[table] = {}
            if n > 0:
                for pct in (25, 50, 75, 90):
                    GLOBAL_CANDLE_THRESHOLD[table][pct] = sizes_list[min(int(n * pct / 100), n - 1)]
            interval = "1 DAY" if table.endswith("_day") else "1 HOUR"
            for typ in ("min", "max"):
                op = ">" if typ == "max" else "<"
                q  = f"""
                    SELECT t1.date FROM {table} t1
                    JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL {interval}
                    JOIN {table} t_next ON t_next.date = t1.date + INTERVAL {interval}
                    WHERE t1.{typ if typ=='min' else 'max'} {op} t_prev.{typ if typ=='min' else 'max'}
                      AND t1.{typ if typ=='min' else 'max'} {op} t_next.{typ if typ=='min' else 'max'}
                """
                res_ext = await conn.execute(text(q))
                GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}
        log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)

    try:
        SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
        log(f"  SERVICE_URL loaded", NODE_NAME)
    except (OperationalError, Exception) as e:
        log(f" load_service_url failed: {e}", NODE_NAME, level="error", force=True)
        SERVICE_URL = ""
    try:
        await ensure_cache_table(engine_vlad)
    except Exception as e:
        log(f" ensure_cache_table failed: {e}", NODE_NAME, level="error")
    LAST_RELOAD_TIME = datetime.now()
    log(" FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f" Background reload error: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "server_background_reload")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await preload_all_data()
    except Exception as e:
        log(f" Initial data load failed, server continues: {e}",
            NODE_NAME, level="error", force=True)

    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()

    for _name, _eng in [("vlad", engine_vlad), ("brain", engine_brain), ("super", engine_super)]:
        try:
            await _eng.dispose()
        except Exception as e:
            log(f"Error disposing {_name}: {e}", NODE_NAME, level="error")


app = FastAPI(lifespan=lifespan)



#  Подгрузка свежих свечей из БД 
_LAST_RATES_REFRESH = {}

async def _refresh_rates_if_needed(rates_table):
    """Если в RAM нет свечи за последний час — подгрузить из БД. Не чаще раз в 30 сек."""
    now = datetime.now()
    last = _LAST_RATES_REFRESH.get(rates_table)
    if last and (now - last).total_seconds() < 30:
        return
    _LAST_RATES_REFRESH[rates_table] = now

    ram = GLOBAL_RATES.get(rates_table)
    if not ram:
        return
    max_dt = max(ram.keys())
    try:
        async with engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, t1 "
                f"FROM `{rates_table}` WHERE date > :dt ORDER BY date"),
                {"dt": max_dt})
            n = 0
            for r in res.mappings().all():
                dt = r["date"]
                if r["t1"] is not None:
                    ram[dt] = float(r["t1"])
                cl = GLOBAL_LAST_CANDLES.get(rates_table)
                if cl is not None:
                    cl.append((dt, (r["close"] or 0) > (r["open"] or 0)))
                n += 1
            if n > 0:
                log(f"   Refreshed {n} candle(s) for {rates_table}", NODE_NAME)
    except Exception as e:
        log(f"   Rates refresh error ({rates_table}): {e}", NODE_NAME, level="warning")

async def calculate_pure_memory(pair, day, date_str, type_=0, var=0) -> dict | None:
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    var_cfg = VAR_CONFIGS.get(var)
    if var_cfg is None:
        return {}

    wl, wr, candle_pct, conf_name, conf_param = var_cfg
    conf_fn      = CONFIDENCE_FUNCS[conf_name]
    rates_table  = get_rates_table_name(pair, day)
    await _refresh_rates_if_needed(rates_table)
    modification = get_modification_factor(pair)

    check_dates = [
        target_date + (timedelta(hours=h) if day == 0 else timedelta(days=h))
        for h in range(wl, wr + 1)
    ]
    delta_unit = timedelta(hours=1) if day == 0 else timedelta(days=1)
    events_in_window = []
    for dt in check_dates:
        dt_end = dt + delta_unit
        _l = bisect.bisect_left(_CAL_SORTED_DATES, dt)
        _r = bisect.bisect_left(_CAL_SORTED_DATES, dt_end)
        for _i in range(_l, _r):
            for e in _CAL_SORTED_DATA[_i]:
                if e["Importance"] != 1 or dt == target_date:
                    events_in_window.append(e)

    needed_events = []
    for e in events_in_window:
        diff     = target_date - e["event_date"]
        shift    = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        evt_type = GLOBAL_EVENT_TYPES.get(e["EventId"], 0)
        if (evt_type == 0 and shift != 0) or (evt_type == 1 and abs(shift) > 12):
            continue
        needed_events.append((e, shift, evt_type))

    if not needed_events:
        return {}

    ram_rates      = GLOBAL_RATES.get(rates_table, {})
    ram_ext        = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle    = find_prev_candle_trend(rates_table, target_date)
    size_threshold = 0
    if candle_pct is not None:
        size_threshold = GLOBAL_CANDLE_THRESHOLD.get(rates_table, {}).get(candle_pct, 0)

    result = {}
    for e, shift, evt_type in needed_events:
        valid_dates = [d for d in GLOBAL_HISTORY.get(e["EventId"], []) if d < target_date]
        if not valid_dates:
            continue

        key0  = f"{e['EventId']}_{evt_type}_0" + (f"_{shift}" if evt_type == 1 else "")
        key1  = f"{e['EventId']}_{evt_type}_1" + (f"_{shift}" if evt_type == 1 else "")
        delta = timedelta(hours=shift) if day == 0 else timedelta(days=shift)
        t_dates = [d + delta for d in valid_dates if (d + delta) < target_date]

        filtered = (
            [td for td in t_dates if GLOBAL_CANDLE_SIZES.get(rates_table, {}).get(td, 0) >= size_threshold]
            if size_threshold > 0 else t_dates
        )
        n    = len(filtered)
        conf = conf_fn(n, conf_param)
        result[key0] = sum(ram_rates.get(td, 0) for td in filtered) * conf

        if prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            matches    = sum(1 for td in filtered if td in ext_set)
            total      = len(filtered)
            if total > 0:
                result[key1] = ((matches / total) * 2 - 1) * conf * modification

    return {k: round(v, 6) for k, v in result.items() if v != 0}


@app.get("/")
async def get_metadata():
    for t in ["vlad_investing_weights_table", "vlad_investing_event_index",
              "version_microservice"]:
        try:
            async with engine_vlad.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"vlad.{t} inaccessible: {e}"}
    for t in ["brain_rates_eur_usd", "brain_rates_btc_usd", "brain_rates_eth_usd",
              "vlad_investing_calendar"]:
        try:
            async with engine_brain.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"brain.{t} inaccessible: {e}"}
    async with engine_vlad.connect() as conn:
        res     = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        version = (res.fetchone() or [0])[0]
    return {
        "status": "ok", "version": f"1.{version}.0", "mode": MODE,
        "name": NODE_NAME, "text": "Investing weights (5 VAR_CONFIGS)",
        "var_count": len(VAR_CONFIGS),
        "metadata": {"author": "Vlad", "stack": "Python 3 + MySQL"},
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response(GLOBAL_WEIGHT_CODES)
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(str(e))


@app.get("/new_weights")
async def get_new_weights(code: str = Query(...)):
    try:
        parts = code.split("_")
        if len(parts) < 3:
            return err_response("Invalid weight_code format")
        try:
            eid    = int(parts[0]); etype = int(parts[1]); mval = int(parts[2])
            hshift = int(parts[3]) if len(parts) > 3 else None
        except ValueError:
            return err_response("All components must be integers")
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT weight_code FROM vlad_investing_weights_table
                WHERE (EventId, event_type, mode_val, COALESCE(hour_shift, -999999))
                      > (:eid, :etype, :mval, :hshift)
                ORDER BY EventId, event_type, mode_val, hour_shift IS NULL, hour_shift
            """), {
                "eid": eid, "etype": etype, "mval": mval,
                "hshift": hshift if hshift is not None else -999999,
            })
        return ok_response([r["weight_code"] for r in res.mappings().all()])
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_new_weights")
        return err_response(str(e))


@app.get("/values")
async def get_values(
    pair: int = Query(1), day: int = Query(0), date: str = Query(...),
    type: int = Query(0), var: int = Query(0),
):
    try:
        return await cached_values(
            engine_vlad=engine_vlad,
            service_url  = SERVICE_URL,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = {"type": type, "var": var},
            compute_fn   = lambda: calculate_pure_memory(pair, day, date, type_=type, var=var),
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
        old = row[0]; new = max(old, 1)
        if new != old:
            await conn.execute(
                text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                {"v": new, "id": SERVICE_ID})
    return {"status": "ok", "from_version": old, "to_version": new}


if __name__ == "__main__":
    import asyncio as _asyncio
    async def _get_workers():
        try:
            return await resolve_workers(engine_super, SERVICE_ID, default=1)
        except Exception as e:
            log(f"resolve_workers failed, default=1: {e}", NODE_NAME, level="error")
            return 1
    try:
        _workers = _asyncio.run(_get_workers())
    except Exception as e:
        log(f"Failed to get workers: {e}", NODE_NAME, level="error")
        _workers = 1
    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False, workers=_workers)
    except KeyboardInterrupt:
        log("Server stopped", NODE_NAME, force=True)
    except SystemExit:
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
