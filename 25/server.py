import uvicorn
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv
import bisect

load_dotenv()

DB_USER = os.getenv("DB_USER", "vlad")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3307")
DB_NAME = os.getenv("DB_NAME", "vlad")
DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"  Host: {DB_HOST}:{DB_PORT}")
print(f"  Database: {DB_NAME}")
print(f"  User: {DB_USER}")
print(f"  Connection URL: mysql+aiomysql://{DB_USER}:@{DB_HOST}:{DB_PORT}/{DB_NAME}")

engine = create_async_engine(DATABASE_URL, pool_size=10, echo=False)

GLOBAL_EXTREMUMS = {}
GLOBAL_RATES = {}
GLOBAL_CALENDAR = {}
GLOBAL_HISTORY = {}
GLOBAL_LAST_CANDLES = {}
GLOBAL_WEIGHT_CODES = []
GLOBAL_EVENT_TYPES = {}


def get_rates_table_name(pair_id, day_flag):
    """Если у тебя нет brain_rates_* — замени на свои таблицы котировок"""
    suffix = "_day" if day_flag == 1 else ""
    table_map = {
        1: "brain_rates_eur_usd",
        3: "brain_rates_btc_usd",
        4: "brain_rates_eth_usd"
    }
    return f"{table_map.get(pair_id, 'brain_rates_eur_usd')}{suffix}"


def get_modification_factor(pair_id):
    if pair_id == 1: return 0.001
    if pair_id == 3: return 1000.0
    if pair_id == 4: return 100.0
    return 1.0


def parse_date_string(date_str):
    date_str = date_str.strip()

    formats = [
        "%Y-%d-%m %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",  
        "%Y-%m-%d" 
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


async def preload_all_data():
    print("STARTING FULL DATA LOAD")
    async with engine.connect() as conn:

        print("  Loading Weight Codes...")
        q_weights = "SELECT weight_code FROM vlad_investing_weights_table"
        try:
            res = await conn.execute(text(q_weights))
            rows = res.mappings().all()
            GLOBAL_WEIGHT_CODES.clear()
            for r in rows:
                GLOBAL_WEIGHT_CODES.append(r['weight_code'])
            print(f"  Loaded {len(GLOBAL_WEIGHT_CODES)} weight codes.")
        except Exception as e:
            print(f"  Weight Codes Load Error: {e}")

        print("  Loading Event Types...")
        q_etypes = """
        SELECT event_id, occurrence_count
        FROM vlad_investing_event_index
        """
        try:
            res = await conn.execute(text(q_etypes))
            rows = res.mappings().all()
            GLOBAL_EVENT_TYPES.clear()
            for r in rows:
                eid = r['event_id']
                cnt = r['occurrence_count'] or 0
                GLOBAL_EVENT_TYPES[eid] = 1 if cnt > 1 else 0
            print(f"  Loaded {len(GLOBAL_EVENT_TYPES)} event types.")
        except Exception as e:
            print(f"  Event Types Load Error: {e}")

        print("  Loading Calendar & Event History...")
        q_cal = """
        SELECT c.event_id, c.occurrence_time_utc, c.importance
        FROM vlad_investing_calendar c
        WHERE c.event_id IS NOT NULL
        """
        try:
            res = await conn.execute(text(q_cal))
            rows = res.mappings().all()
            for r in rows:
                dt = r['occurrence_time_utc']
                eid = r['event_id']
                imp = r['importance']  

                if eid not in GLOBAL_HISTORY:
                    GLOBAL_HISTORY[eid] = []
                GLOBAL_HISTORY[eid].append(dt)

                if dt not in GLOBAL_CALENDAR:
                    GLOBAL_CALENDAR[dt] = []
                GLOBAL_CALENDAR[dt].append({
                    'EventId': eid,
                    'Importance': imp,
                    'event_date': dt
                })
            print(f"  Loaded {len(rows)} calendar entries.")
        except Exception as e:
            print(f"  Calendar Load Error: {e}")

        
        tables = [
            "brain_rates_eur_usd", "brain_rates_eur_usd_day",
            "brain_rates_btc_usd", "brain_rates_btc_usd_day",
            "brain_rates_eth_usd", "brain_rates_eth_usd_day"
        ]

        for table in tables:
            print(f"  Loading {table}...")
            GLOBAL_RATES[table] = {}
            GLOBAL_LAST_CANDLES[table] = []
            q_rates = f"SELECT date, open, close, t1 FROM {table}"
            try:
                res = await conn.execute(text(q_rates))
                rows = res.mappings().all()
                sorted_rows = sorted(rows, key=lambda x: x['date'])
                for r in sorted_rows:
                    dt = r['date']
                    if r['t1'] is not None:
                        GLOBAL_RATES[table][dt] = float(r['t1'])
                    is_bull = r['close'] > r['open']
                    GLOBAL_LAST_CANDLES[table].append((dt, is_bull))

                GLOBAL_EXTREMUMS[table] = {'min': set(), 'max': set()}
                for type_ in ['min', 'max']:
                    op = ">" if type_ == 'max' else "<"
                    col = "max" if type_ == 'max' else "min"
                    q_ext = f"""
                    SELECT t1.date FROM {table} t1
                    JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                    JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                    WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}
                    """
                    res_ext = await conn.execute(text(q_ext))
                    GLOBAL_EXTREMUMS[table][type_] = {r['date'] for r in res_ext.mappings().all()}
                print(f"  Loaded {len(sorted_rows)} rows from {table}.")
            except Exception as e:
                print(f"  Error loading {table}: {e}")

    print("  SERVER READY. DATABASE DISCONNECTED.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_all_data()
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


def find_prev_candle_trend(table, target_date):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles: return None
    idx = bisect.bisect_left(candles, (target_date, False))
    if idx > 0:
        return candles[idx - 1]
    return None


async def calculate_pure_memory(pair, day, date_str):
    target_date = parse_date_string(date_str)
    if not target_date:
        return {"error": "Invalid date format"}

    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window = 12

    events_in_window = []
    check_dates = []

    if day == 0:
        for h in range(-window, window + 1):
            check_dates.append(target_date + timedelta(hours=h))
    else:
        for d in range(-window, window + 1):
            check_dates.append(target_date + timedelta(days=d))

    for dt in check_dates:
        events = GLOBAL_CALENDAR.get(dt, [])
        for e in events:
            # importance: 1=low, 2=medium, 3=high
            if e['Importance'] == 1 and dt != target_date:
                continue
            events_in_window.append(e)

    if not events_in_window:
        return {}

    needed_events = []
    for event in events_in_window:
        diff = target_date - event['event_date']
        shift = int(diff.total_seconds() / 3600) if day == 0 else diff.days

        
        is_rare = event['Importance'] in [2, 3]
        evt_id = event['EventId']
        evt_type = GLOBAL_EVENT_TYPES.get(evt_id, 0)

        if (evt_type == 0 and shift != 0) or (evt_type == 1 and abs(shift) > 12):
            continue
        needed_events.append((event, shift, evt_type))

    if not needed_events:
        return {}

    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {'min': set(), 'max': set()})
    prev_candle_info = find_prev_candle_trend(rates_table, target_date)

    raw_result = {}
    for event, shift, evt_type in needed_events:
        evt_id = event['EventId']
        h_dates = GLOBAL_HISTORY.get(evt_id, [])
        valid_h_dates = [d for d in h_dates if d < target_date]
        if not valid_h_dates:
            continue

        key0 = f"{evt_id}_{evt_type}_0" + (f"_{shift}" if evt_type == 1 else "")
        key1 = f"{evt_id}_{evt_type}_1" + (f"_{shift}" if evt_type == 1 else "")

        delta = timedelta(hours=shift) if day == 0 else timedelta(days=shift)
        t_dates = [d + delta for d in valid_h_dates]

        sum_t1 = 0
        for td in t_dates:
            val = ram_rates.get(td)
            if val is not None:
                sum_t1 += val
        raw_result[key0] = sum_t1

        if prev_candle_info:
            _, is_bull = prev_candle_info
            target = 'max' if is_bull else 'min'
            ext_set = ram_ext[target]
            matches = sum(1 for d in t_dates if d in ext_set)
            total = len(valid_h_dates)
            if total > 0:
                raw_result[key1] = ((matches / total) * 2 - 1) * modification

    return {k: round(v, 6) for k, v in raw_result.items() if v != 0}


@app.get("/")
async def get_metadata():
    required_tables = [
        "vlad_investing_weights_table",  
        "vlad_investing_event_index",
        "brain_rates_eur_usd",
        "brain_rates_btc_usd",
        "brain_rates_eth_usd",
        "version_microservice"
    ]
    brain_table = "vlad_investing_calendar" 

    async with engine.connect() as conn:
       
        for table in required_tables + [brain_table]:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"Table {table} inaccessible: {e}"}

       
        try:
            res = await conn.execute(
                text("SELECT version FROM version_microservice WHERE microservice_id = 25")
            )
            row = res.fetchone()
            version = row[0] if row else 0
        except Exception as e:
            return {"status": "error", "error": str(e)}

    return {
        "status": "ok",
        "version": f"1.{version}.0",
        "name": "brain-weights-microservice",
        "text": "Calculates historical market weights based on cyclical economic events",
        "metadata": {
            "author": "Vlad",
            "stack": "Python 3 + MySQL",
        }
    }


@app.get("/weights")
async def get_weights():
    return {"weights": GLOBAL_WEIGHT_CODES}


@app.get("/values")
async def get_values(
        pair: int = Query(1),
        day: int = Query(0),
        date: str = Query(...)
):
    return await calculate_pure_memory(pair, day, date)

@app.post("/patch")
async def patch_service():
    service_id = 25
    async with engine.begin() as conn:  
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": service_id}
        )
        row = res.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail=f"Service ID {service_id} not found")

        current_version = row[0]

        if current_version < 1:
            print("[PATCH] Applying v1...")
            await conn.execute(
                text("UPDATE version_microservice SET version = 1 WHERE microservice_id = :id"),
                {"id": service_id}
            )
            current_version = 1

        return {
            "status": "ok",
            "from_version": row[0],
            "to_version": current_version,
            "message": f"Applied patches up to version {current_version}"
        }

@app.get("/new_weights")
async def get_new_weights(code: str = Query(...)):
    parts = code.split("_")
    if len(parts) < 3:
        raise HTTPException(status_code=400, detail="Invalid weight_code format")

    try:
        target_eid = int(parts[0])
        target_etype = int(parts[1])
        target_mval = int(parts[2])
        target_hshift = int(parts[3]) if len(parts) > 3 else None
    except ValueError:
        raise HTTPException(status_code=400, detail="All components must be integers")

    async with engine.connect() as conn:  
        query = """
            SELECT weight_code
            FROM vlad_investing_weights_table
            WHERE (EventId, event_type, mode_val, COALESCE(hour_shift, -999999)) 
                  > (:eid, :etype, :mval, :hshift)
            ORDER BY EventId, event_type, mode_val, hour_shift IS NULL, hour_shift
        """
        res = await conn.execute(
            text(query),
            {
                "eid": target_eid,
                "etype": target_etype,
                "mval": target_mval,
                "hshift": target_hshift if target_hshift is not None else -999999
            }
        )
        rows = res.mappings().all()
        return {"weights": [r["weight_code"] for r in rows]}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8890, reload=False, workers=1)
