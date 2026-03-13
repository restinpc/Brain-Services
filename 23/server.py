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
    resolve_workers, build_engines,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values

# ── Идентификаторы ─────────────────────────────────────────────────────────────
SERVICE_ID = 23
NODE_NAME  = os.getenv("NODE_NAME", "brain-weights-microservice")
PORT       = 8888

# ── Конфигурация БД ───────────────────────────────────────────────────────────
load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"vlad:  {os.getenv('DB_USER')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",           NODE_NAME)
log(f"brain: {os.getenv('MASTER_USER')}@{os.getenv('MASTER_HOST')}:{os.getenv('MASTER_PORT')}/{os.getenv('MASTER_NAME')}", NODE_NAME)
log(f"super: {os.getenv('SUPER_USER')}@{os.getenv('SUPER_HOST')}:{os.getenv('SUPER_PORT')}/{os.getenv('SUPER_NAME')}",     NODE_NAME)

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_EXTREMUMS    = {}
GLOBAL_RATES        = {}
GLOBAL_CALENDAR     = {}
GLOBAL_HISTORY      = {}
GLOBAL_LAST_CANDLES = {}
GLOBAL_WEIGHT_CODES = []
SERVICE_URL         = ""   # загружается в preload_all_data


def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    suffix = "_day" if day_flag == 1 else ""
    return {
        1: "brain_rates_eur_usd",
        3: "brain_rates_btc_usd",
        4: "brain_rates_eth_usd",
    }.get(pair_id, "brain_rates_eur_usd") + suffix


def get_modification_factor(pair_id: int) -> float:
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str: str) -> datetime | None:
    for fmt in ("%Y-%d-%m %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def find_prev_candle_trend(table: str, target_date: datetime):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None


async def preload_all_data():
    global SERVICE_URL
    log("STARTING FULL DATA LOAD", NODE_NAME, force=True)

    async with engine_vlad.connect() as conn_vlad:
        try:
            res = await conn_vlad.execute(text("SELECT weight_code FROM vlad_weight_codes"))
            GLOBAL_WEIGHT_CODES.clear()
            GLOBAL_WEIGHT_CODES.extend(r["weight_code"] for r in res.mappings().all())
            log(f"  Weight codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)
        except Exception as e:
            send_error_trace(e, NODE_NAME, "preload_weight_codes"); raise

        index_map = {}
        try:
            res_idx = await conn_vlad.execute(text(
                "SELECT EventName, Country, EventId, Importance "
                "FROM vlad_brain_calendar_event_index"
            ))
            for r in res_idx.mappings():
                index_map[(r["EventName"], r["Country"])] = {
                    "EventId": r["EventId"], "Importance": r["Importance"]
                }
            log(f"  Event index: {len(index_map)}", NODE_NAME)
        except Exception as e:
            send_error_trace(e, NODE_NAME, "preload_event_index"); raise

    async with engine_brain.connect() as conn_brain:
        try:
            res_cal = await conn_brain.execute(
                text("SELECT EventName, Country, FullDate FROM brain_calendar"))
            brain_events = res_cal.mappings().all()
            log(f"  brain_calendar rows: {len(brain_events)}", NODE_NAME)
        except Exception as e:
            send_error_trace(e, NODE_NAME, "preload_calendar"); raise

        matched = 0
        GLOBAL_CALENDAR.clear(); GLOBAL_HISTORY.clear()
        for event in brain_events:
            key = (event["EventName"], event["Country"])
            if key in index_map:
                eid = index_map[key]["EventId"]
                imp = index_map[key]["Importance"]
                dt  = event["FullDate"]
                GLOBAL_HISTORY.setdefault(eid, []).append(dt)
                GLOBAL_CALENDAR.setdefault(dt, []).append(
                    {"EventId": eid, "Importance": imp, "event_date": dt}
                )
                matched += 1
        log(f"  Matched calendar entries: {matched}", NODE_NAME)

        tables = [
            "brain_rates_eur_usd", "brain_rates_eur_usd_day",
            "brain_rates_btc_usd", "brain_rates_btc_usd_day",
            "brain_rates_eth_usd", "brain_rates_eth_usd_day",
        ]
        for table in tables:
            log(f"  Loading {table}…", NODE_NAME)
            GLOBAL_RATES[table] = {}; GLOBAL_LAST_CANDLES[table] = []
            try:
                res  = await conn_brain.execute(text(f"SELECT date, open, close, t1 FROM {table}"))
                rows = sorted(res.mappings().all(), key=lambda x: x["date"])
                for r in rows:
                    dt = r["date"]
                    if r["t1"] is not None:
                        GLOBAL_RATES[table][dt] = float(r["t1"])
                    GLOBAL_LAST_CANDLES[table].append((dt, r["close"] > r["open"]))
                GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}
                for typ in ("min", "max"):
                    op  = ">" if typ == "max" else "<"
                    col = "max" if typ == "max" else "min"
                    q   = f"""
                        SELECT t1.date FROM {table} t1
                        JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                        JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                        WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}
                    """
                    res_ext = await conn_brain.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}
                log(f"    {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)
            except Exception as e:
                send_error_trace(e, NODE_NAME, f"preload_rates_{table}"); raise

    # ── Загружаем URL сервиса через супер-ноду и создаём таблицу кеша ─────────
    SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
    await ensure_cache_table(engine_brain)

    log("SERVER READY. ALL DATA PRELOADED.", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            log("🔄 Background reload started…", NODE_NAME, force=True)
            await preload_all_data()
            log("✅ Background reload completed", NODE_NAME, force=True)
        except Exception as e:
            log(f"❌ Background reload error: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "background_reload")


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


async def calculate_pure_memory(pair: int, day: int, date_str: str) -> dict | None:
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window       = 12

    check_dates = (
        [target_date + timedelta(hours=h) for h in range(-window, window + 1)]
        if day == 0 else
        [target_date + timedelta(days=d)  for d in range(-window, window + 1)]
    )

    events_in_window = []
    for dt in check_dates:
        for e in GLOBAL_CALENDAR.get(dt, []):
            if e["Importance"] == "low" and dt != target_date:
                continue
            events_in_window.append(e)

    if not events_in_window:
        return {}

    needed_events = []
    for event in events_in_window:
        diff     = target_date - event["event_date"]
        shift    = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        evt_type = 1 if event["Importance"] in ("medium", "high") else 0
        if (evt_type == 0 and shift != 0) or (evt_type == 1 and abs(shift) > 12):
            continue
        needed_events.append((event, shift, evt_type))

    if not needed_events:
        return {}

    ram_rates        = GLOBAL_RATES.get(rates_table, {})
    ram_ext          = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle_info = find_prev_candle_trend(rates_table, target_date)

    raw_result = {}
    for event, shift, evt_type in needed_events:
        evt_id  = event["EventId"]
        valid_h = [d for d in GLOBAL_HISTORY.get(evt_id, []) if d < target_date]
        if not valid_h:
            continue

        key0 = f"{evt_id}_{evt_type}_0" + (f"_{shift}" if evt_type == 1 else "")
        key1 = f"{evt_id}_{evt_type}_1" + (f"_{shift}" if evt_type == 1 else "")

        delta   = timedelta(hours=shift) if day == 0 else timedelta(days=shift)
        t_dates = [d + delta for d in valid_h if (d + delta) < target_date]

        raw_result[key0] = sum(ram_rates.get(td, 0) for td in t_dates)

        if prev_candle_info:
            _, is_bull = prev_candle_info
            ext_set    = ram_ext["max" if is_bull else "min"]
            matches    = sum(1 for d in t_dates if d in ext_set)
            total      = len(valid_h)
            if total > 0:
                raw_result[key1] = ((matches / total) * 2 - 1) * modification

    return {k: round(v, 6) for k, v in raw_result.items() if v != 0}


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/")
async def get_metadata():
    vlad_tables  = ["vlad_weight_codes", "vlad_brain_calendar_event_index", "version_microservice"]
    brain_tables = ["brain_calendar", "brain_rates_eur_usd", "brain_rates_btc_usd", "brain_rates_eth_usd"]
    async with engine_vlad.connect() as conn:
        for t in vlad_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM {t} LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"vlad.{t} inaccessible: {e}"}
    async with engine_brain.connect() as conn:
        for t in brain_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM {t} LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"brain.{t} inaccessible: {e}"}
    async with engine_vlad.connect() as conn:
        res     = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID},
        )
        row     = res.fetchone()
        version = row[0] if row else 0
    return {
        "status": "ok", "version": f"1.{version}.6", "name": NODE_NAME, "mode": MODE,
        "text": "Calculates historical market weights based on cyclical economic events",
        "metadata": {"author": "Vlad", "stack": "Python 3 + MySQL"},
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response(GLOBAL_WEIGHT_CODES)
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_weights")


@app.get("/new_weights")
async def get_new_weights(code: str = Query(...)):
    try:
        parts = code.split("_")
        if len(parts) < 3:
            return err_response("Invalid weight_code format")
        try:
            eid    = int(parts[0])
            etype  = int(parts[1])
            mval   = int(parts[2])
            hshift = int(parts[3]) if len(parts) > 3 else None
        except ValueError:
            return err_response("All components must be integers")
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT weight_code FROM vlad_weight_codes
                WHERE (EventId, event_type, mode_val, COALESCE(hour_shift, -999999))
                      > (:eid, :etype, :mval, :hshift)
                ORDER BY EventId, event_type, mode_val, hour_shift IS NULL, hour_shift
            """), {
                "eid": eid, "etype": etype, "mval": mval,
                "hshift": hshift if hshift is not None else -999999,
            })
        return ok_response([r["weight_code"] for r in res.mappings().all()])
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_new_weights")


@app.get("/values")
async def get_values(
    pair: int = Query(1),
    day:  int = Query(0),
    date: str = Query(...),
):
    try:
        return await cached_values(
            engine_vlad=engine_brain,
            service_url  = SERVICE_URL,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = {},
            compute_fn   = lambda: calculate_pure_memory(pair, day, date),
            node         = NODE_NAME,
        )
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_values")


@app.post("/patch")
async def patch_service():
    try:
        async with engine_vlad.begin() as conn:
            res = await conn.execute(
                text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
                {"id": SERVICE_ID},
            )
            row = res.fetchone()
            if not row:
                raise HTTPException(status_code=500, detail=f"Service ID {SERVICE_ID} not found")
            old = row[0]; new = max(old, 1)
            if new != old:
                await conn.execute(
                    text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                    {"v": new, "id": SERVICE_ID},
                )
        return {"status": "ok", "from_version": old, "to_version": new}
    except HTTPException:
        raise
    except Exception as e:
        send_error_trace(e, NODE_NAME, "patch_service")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import asyncio as _asyncio
    async def _get_workers():
        return await resolve_workers(engine_super, SERVICE_ID, default=4)
    _workers = _asyncio.run(_get_workers())
    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False, workers=_workers)
    except KeyboardInterrupt:
        log("Server stopped by user", NODE_NAME, force=True)
    except SystemExit:
        pass
    except Exception as e:
        log(f"Critical error: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)