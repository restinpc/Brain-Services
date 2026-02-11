import uvicorn
import os
import asyncio
import traceback
import requests
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv
import bisect

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "brain-weights-microservice")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "server.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {
        "url": "fastapi_microservice",
        "node": NODE_NAME,
        "email": EMAIL,
        "logs": logs,
    }
    try:
        print(f"\n📤 [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"✅ [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")


# === Загрузка конфигурации ===
load_dotenv()

DB_USER = os.getenv("DB_USER", "vlad")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3307")
DB_NAME = os.getenv("DB_NAME", "vlad")
BRAIN_DB_NAME = "brain"

DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BRAIN_DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{BRAIN_DB_NAME}"

print(f"  Host: {DB_HOST}:{DB_PORT}")
print(f"  DB (vlad): {DB_NAME}")
print(f"  DB (brain): {BRAIN_DB_NAME}")

engine_vlad = create_async_engine(DATABASE_URL, pool_size=10, echo=False)
engine_brain = create_async_engine(BRAIN_DATABASE_URL, pool_size=5, echo=False)

# === Глобальные переменные ===
GLOBAL_EXTREMUMS = {}
GLOBAL_RATES = {}
GLOBAL_CALENDAR = {}
GLOBAL_HISTORY = {}
GLOBAL_LAST_CANDLES = {}
GLOBAL_WEIGHT_CODES = []
GLOBAL_EVENT_TYPES = {}
LAST_RELOAD_TIME = None


def get_rates_table_name(pair_id, day_flag):
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
    global LAST_RELOAD_TIME
    print("🔄 FULL DATA RELOAD STARTED")

    # --- Из vlad ---
    async with engine_vlad.connect() as conn:
        # Weight Codes
        res = await conn.execute(text("SELECT weight_code FROM vlad_investing_weights_table"))
        GLOBAL_WEIGHT_CODES[:] = [r['weight_code'] for r in res.mappings().all()]

        # Event Types
        res = await conn.execute(text("SELECT event_id, occurrence_count FROM vlad_investing_event_index"))
        GLOBAL_EVENT_TYPES.clear()
        for r in res.mappings().all():
            eid = r['event_id']
            cnt = r['occurrence_count'] or 0
            GLOBAL_EVENT_TYPES[eid] = 1 if cnt > 1 else 0

        # Calendar & History
        res = await conn.execute(text("""
            SELECT c.event_id, c.occurrence_time_utc, c.importance
            FROM vlad_investing_calendar c WHERE c.event_id IS NOT NULL
        """))
        GLOBAL_CALENDAR.clear()
        GLOBAL_HISTORY.clear()
        for r in res.mappings().all():
            dt = r['occurrence_time_utc']
            eid = r['event_id']
            imp = r['importance']
            if eid not in GLOBAL_HISTORY:
                GLOBAL_HISTORY[eid] = []
            GLOBAL_HISTORY[eid].append(dt)
            if dt not in GLOBAL_CALENDAR:
                GLOBAL_CALENDAR[dt] = []
            GLOBAL_CALENDAR[dt].append({'EventId': eid, 'Importance': imp, 'event_date': dt})

    # --- Из brain ---
    tables = [
        "brain_rates_eur_usd", "brain_rates_eur_usd_day",
        "brain_rates_btc_usd", "brain_rates_btc_usd_day",
        "brain_rates_eth_usd", "brain_rates_eth_usd_day"
    ]

    for table in tables:
        GLOBAL_RATES[table] = {}
        GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_EXTREMUMS[table] = {'min': set(), 'max': set()}

        async with engine_brain.connect() as conn:
            res = await conn.execute(text(f"SELECT date, open, close, t1 FROM {table}"))
            rows = sorted(res.mappings().all(), key=lambda x: x['date'])
            for r in rows:
                dt = r['date']
                if r['t1'] is not None:
                    GLOBAL_RATES[table][dt] = float(r['t1'])
                is_bull = r['close'] > r['open']
                GLOBAL_LAST_CANDLES[table].append((dt, is_bull))

            for typ in ['min', 'max']:
                op = ">" if typ == 'max' else "<"
                col = "max" if typ == 'max' else "min"
                q = f"""
                SELECT t1.date FROM {table} t1
                JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}
                """
                res_ext = await conn.execute(text(q))
                GLOBAL_EXTREMUMS[table][typ] = {r['date'] for r in res_ext.mappings().all()}

    LAST_RELOAD_TIME = datetime.now()
    print("✅ FULL DATA RELOAD COMPLETED")


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)  # ← сначала ждём!
        try:
            await preload_all_data()
        except Exception as e:
            print(f"❌ Background reload error: {e}")
            send_error_trace(e, "server_background_reload")


# === FastAPI ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Первая загрузка
    await preload_all_data()
    # Фон
    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()
    await engine_vlad.dispose()
    await engine_brain.dispose()


app = FastAPI(lifespan=lifespan)

def find_prev_candle_trend(table, target_date):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles: return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None

async def calculate_pure_memory(pair, day, date_str):
    target_date = parse_date_string(date_str)
    if not target_date:
        return {"error": "Invalid date format"}
    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window = 12

    events_in_window = []
    check_dates = [
        target_date + (timedelta(hours=h) if day == 0 else timedelta(days=d))
        for h in range(-window, window + 1)
        for d in ([0] if day == 0 else [h])
    ] if day == 0 else [
        target_date + timedelta(days=d) for d in range(-window, window + 1)
    ]

    for dt in check_dates:
        for e in GLOBAL_CALENDAR.get(dt, []):
            if e['Importance'] != 1 or dt == target_date:
                events_in_window.append(e)

    needed_events = []
    for e in events_in_window:
        diff = target_date - e['event_date']
        shift = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        evt_id = e['EventId']
        evt_type = GLOBAL_EVENT_TYPES.get(evt_id, 0)
        if (evt_type == 0 and shift != 0) or (evt_type == 1 and abs(shift) > 12):
            continue
        needed_events.append((e, shift, evt_type))

    if not needed_events:
        return {}

    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {'min': set(), 'max': set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}
    for e, shift, evt_type in needed_events:
        valid_dates = [d for d in GLOBAL_HISTORY.get(e['EventId'], []) if d < target_date]
        if not valid_dates: continue

        key0 = f"{e['EventId']}_{evt_type}_0" + (f"_{shift}" if evt_type == 1 else "")
        key1 = f"{e['EventId']}_{evt_type}_1" + (f"_{shift}" if evt_type == 1 else "")

        t_dates = [d + (timedelta(hours=shift) if day == 0 else timedelta(days=shift)) for d in valid_dates]
        sum_t1 = sum(ram_rates.get(td, 0) for td in t_dates)
        result[key0] = sum_t1

        if prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext['max' if is_bull else 'min']
            matches = sum(1 for d in t_dates if d in ext_set)
            total = len(valid_dates)
            if total > 0:
                result[key1] = ((matches / total) * 2 - 1) * modification

    return {k: round(v, 6) for k, v in result.items() if v != 0}

@app.get("/")
async def get_metadata():
    required_tables = [
        "vlad_investing_weights_table",
        "vlad_investing_event_index",
        "vlad_investing_calendar",
        "version_microservice"
    ]
    brain_tables = [
        "brain_rates_eur_usd",
        "brain_rates_btc_usd",
        "brain_rates_eth_usd"
    ]

    # Проверка таблиц в vlad
    async with engine_vlad.connect() as conn:
        for table in required_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"Table {table} in 'vlad' inaccessible: {e}"}

    # Проверка таблиц в brain
    async with engine_brain.connect() as conn:
        for table in brain_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1"))
            except Exception as e:
                return {"status": "error", "error": f"Table {table} in 'brain' inaccessible: {e}"}

    # Чтение версии из vlad
    async with engine_vlad.connect() as conn:
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
async def get_values(pair: int = Query(1), day: int = Query(0), date: str = Query(...)):
    return await calculate_pure_memory(pair, day, date)


@app.post("/patch")
async def patch_service():
    service_id = 25
    async with engine_vlad.begin() as conn:
        res = await conn.execute(text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
                                 {"id": service_id})
        row = res.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail=f"Service ID {service_id} not found")
        current_version = row[0]
        if current_version < 1:
            await conn.execute(text("UPDATE version_microservice SET version = 1 WHERE microservice_id = :id"),
                               {"id": service_id})
            current_version = 1
        return {"status": "ok", "from_version": row[0], "to_version": current_version}


@app.get("/new_weights")
async def get_new_weights(code: str = Query(...)):
    parts = code.split("_")
    if len(parts) < 3:
        raise HTTPException(status_code=400, detail="Invalid weight_code format")
    try:
        eid, etype, mval = int(parts[0]), int(parts[1]), int(parts[2])
        hshift = int(parts[3]) if len(parts) > 3 else None
    except ValueError:
        raise HTTPException(status_code=400, detail="All components must be integers")

    async with engine_vlad.connect() as conn:
        query = """
            SELECT weight_code FROM vlad_investing_weights_table
            WHERE (EventId, event_type, mode_val, COALESCE(hour_shift, -999999)) 
                   > (:eid, :etype, :mval, :hshift)
            ORDER BY EventId, event_type, mode_val, hour_shift IS NULL, hour_shift
        """
        res = await conn.execute(text(query), {
            "eid": eid, "etype": etype, "mval": mval,
            "hshift": hshift if hshift is not None else -999999
        })
        return {"weights": [r["weight_code"] for r in res.mappings().all()]}


# === Точка входа ===
if __name__ == "__main__":
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=8890, reload=False, workers=1)
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен пользователем")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка при запуске сервера: {e!r}")
        send_error_trace(e)