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


load_dotenv()

# --- Чтение переменных из .env файла ---
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "")

MASTER_HOST = os.getenv("MASTER_HOST", "")
MASTER_PORT = os.getenv("MASTER_PORT", "")
MASTER_USER = os.getenv("MASTER_USER", "")
MASTER_PASSWORD = os.getenv("MASTER_PASSWORD", "")
MASTER_NAME = os.getenv("MASTER_NAME", "")

# --- Основная БД (vlad) использует DB_* ---
DATABASE_URL_VLAD = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine_vlad = create_async_engine(DATABASE_URL_VLAD, pool_size=10, echo=False)

# --- БД brain (только для чтения) использует MASTER_* ---
DATABASE_URL_BRAIN = f"mysql+aiomysql://{MASTER_USER}:{MASTER_PASSWORD}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}"
engine_brain = create_async_engine(DATABASE_URL_BRAIN, pool_size=5, echo=False)

print(f"📊 Конфигурация подключения:")
print(f"  Основная БД (vlad): {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"  Мастер БД (brain): {MASTER_USER}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}")

GLOBAL_EXTREMUMS = {}
GLOBAL_RATES = {}
GLOBAL_CALENDAR = {}
GLOBAL_HISTORY = {}
GLOBAL_LAST_CANDLES = {}
GLOBAL_WEIGHT_CODES = []


def get_rates_table_name(pair_id, day_flag):
    suffix = "_day" if day_flag == 1 else ""
    table_map = {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd", 4: "brain_rates_eth_usd"}
    return f"{table_map.get(pair_id, 'brain_rates_eur_usd')}{suffix}"


def get_modification_factor(pair_id):
    if pair_id == 1: return 0.001
    if pair_id == 3: return 1000.0
    if pair_id == 4: return 100.0
    return 1.0


def parse_date_string(date_str):
    """Поддержка 4 форматов даты"""
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

    try:
        # --- ЗАГРУЗКА ДАННЫХ ИЗ vlad (используем DB_* подключение) ---
        async with engine_vlad.connect() as conn_vlad:
            # 1. Веса (vlad_weight_codes)
            print("  Loading Weight Codes from vlad...")
            try:
                res = await conn_vlad.execute(text("SELECT weight_code FROM vlad_weight_codes"))
                rows = res.mappings().all()
                GLOBAL_WEIGHT_CODES.clear()
                GLOBAL_WEIGHT_CODES.extend(r['weight_code'] for r in rows)
                print(f"    Loaded {len(GLOBAL_WEIGHT_CODES)} weight codes.")
            except Exception as e:
                print(f"    Weight Codes Load Error: {e}")
                send_error_trace(e, "preload_weight_codes")
                raise

            # 2. Индекс событий (vlad_brain_calendar_event_index)
            print("  Loading Event Index from vlad...")
            index_map = {}
            try:
                res_idx = await conn_vlad.execute(text(
                    "SELECT EventName, Country, EventId, Importance FROM vlad_brain_calendar_event_index"
                ))
                for r in res_idx.mappings():
                    key = (r['EventName'], r['Country'])
                    index_map[key] = {'EventId': r['EventId'], 'Importance': r['Importance']}
                print(f"    Loaded {len(index_map)} index entries.")
            except Exception as e:
                print(f"    Event Index Load Error: {e}")
                send_error_trace(e, "preload_event_index")
                raise

        # --- ЗАГРУЗКА ДАННЫХ ИЗ brain (используем MASTER_* подключение) ---
        async with engine_brain.connect() as conn_brain:
            # 3. Календарь событий (brain_calendar)
            print("  Loading Calendar from brain...")
            try:
                res_cal = await conn_brain.execute(text("SELECT EventName, Country, FullDate FROM brain_calendar"))
                brain_events = res_cal.mappings().all()
                print(f"    Fetched {len(brain_events)} events from brain.brain_calendar")
            except Exception as e:
                print(f"    Error loading brain_calendar: {e}")
                send_error_trace(e, "preload_calendar")
                raise

            # Объединяем в памяти
            matched_rows = 0
            GLOBAL_CALENDAR.clear()
            GLOBAL_HISTORY.clear()
            for event in brain_events:
                key = (event['EventName'], event['Country'])
                if key in index_map:
                    eid = index_map[key]['EventId']
                    imp = index_map[key]['Importance']
                    dt = event['FullDate']
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
                    matched_rows += 1
            print(f"    Matched {matched_rows} calendar entries.")

            # 4. Котировки (brain_rates_*) из brain!
            tables = [
                "brain_rates_eur_usd", "brain_rates_eur_usd_day",
                "brain_rates_btc_usd", "brain_rates_btc_usd_day",
                "brain_rates_eth_usd", "brain_rates_eth_usd_day"
            ]

            for table in tables:
                print(f"  Loading {table} from brain...")
                GLOBAL_RATES[table] = {}
                GLOBAL_LAST_CANDLES[table] = []
                try:
                    res = await conn_brain.execute(text(f"SELECT date, open, close, t1 FROM {table}"))
                    rows = res.mappings().all()
                    sorted_rows = sorted(rows, key=lambda x: x['date'])
                    for r in sorted_rows:
                        dt = r['date']
                        if r['t1'] is not None:
                            GLOBAL_RATES[table][dt] = float(r['t1'])
                        is_bull = r['close'] > r['open']
                        GLOBAL_LAST_CANDLES[table].append((dt, is_bull))

                    # Экстремумы
                    GLOBAL_EXTREMUMS[table] = {'min': set(), 'max': set()}
                    for typ in ['min', 'max']:
                        op = ">" if typ == 'max' else "<"
                        col = "max" if typ == 'max' else "min"
                        q_ext = f"""
                        SELECT t1.date FROM {table} t1
                        JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                        JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                        WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}
                        """
                        res_ext = await conn_brain.execute(text(q_ext))
                        GLOBAL_EXTREMUMS[table][typ] = {r['date'] for r in res_ext.mappings().all()}
                except Exception as e:
                    print(f"    Error loading {table}: {e}")
                    send_error_trace(e, f"preload_rates_{table}")
                    raise

        print("  SERVER READY. ALL DATA PRELOADED.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR DURING PRELOAD: {e}")
        send_error_trace(e, "preload_all_data")
        raise


async def background_reload_data():
    """Фоновая перезагрузка данных каждый час"""
    while True:
        await asyncio.sleep(3600)  # 1 час
        try:
            print("🔄 Background reload started...")
            await preload_all_data()
            print("✅ Background reload completed")
        except Exception as e:
            print(f"❌ Background reload error: {e}")
            send_error_trace(e, "server_background_reload")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Первая загрузка
    await preload_all_data()
    # Фоновая перезагрузка
    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()
    await engine_vlad.dispose()
    await engine_brain.dispose()


app = FastAPI(lifespan=lifespan)


def find_prev_candle_trend(table, target_date):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    if idx > 0:
        return candles[idx - 1]
    return None

async def calculate_pure_memory(pair, day, date_str):
    target_date = parse_date_string(date_str)
    if not target_date: return {"error": "Invalid date format"}

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
            if e['Importance'] == 'low' and dt != target_date:
                continue
            events_in_window.append(e)

    if not events_in_window: return {}

    needed_events = []
    for event in events_in_window:
        diff = target_date - event['event_date']
        shift = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        is_rare = event['Importance'] in ['medium', 'high']
        evt_type = 1 if is_rare else 0
        if (evt_type == 0 and shift != 0) or (evt_type == 1 and abs(shift) > 12): continue
        needed_events.append((event, shift, evt_type))

    if not needed_events: return {}

    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {'min': set(), 'max': set()})
    prev_candle_info = find_prev_candle_trend(rates_table, target_date)

    raw_result = {}
    for event, shift, evt_type in needed_events:
        evt_id = event['EventId']
        h_dates = GLOBAL_HISTORY.get(evt_id, [])
        valid_h_dates = [d for d in h_dates if d < target_date]
        if not valid_h_dates: continue

        key0 = f"{evt_id}_{evt_type}_0" + (f"_{shift}" if evt_type == 1 else "")
        key1 = f"{evt_id}_{evt_type}_1" + (f"_{shift}" if evt_type == 1 else "")

        delta = timedelta(hours=shift) if day == 0 else timedelta(days=shift)
        t_dates = [d + delta for d in valid_h_dates]

        sum_t1 = 0
        for td in t_dates:
            val = ram_rates.get(td)
            if val is not None: sum_t1 += val
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


@app.post("/patch")
async def patch_service():
    try:
        service_id = 23
        async with engine_vlad.begin() as conn:
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
    except Exception as e:
        print(f"❌ Error in patch_service: {e}")
        send_error_trace(e, "patch_service")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
async def get_metadata():
    try:
        vlad_tables = [
            "vlad_weight_codes",
            "vlad_brain_calendar_event_index",
            "version_microservice"
        ]
        brain_tables = [
            "brain_calendar",
            "brain_rates_eur_usd",
            "brain_rates_btc_usd",
            "brain_rates_eth_usd"
        ]

        # Проверка vlad-таблиц (через DB_* подключение)
        async with engine_vlad.connect() as conn:
            for table in vlad_tables:
                try:
                    await conn.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
                except Exception as e:
                    return {"status": "error", "error": f"vlad.{table} inaccessible: {e}"}

        # Проверка brain-таблиц (через MASTER_* подключение)
        async with engine_brain.connect() as conn:
            for table in brain_tables:
                try:
                    await conn.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
                except Exception as e:
                    return {"status": "error", "error": f"brain.{table} inaccessible: {e}"}

        # Версия микросервиса (из vlad)
        async with engine_vlad.connect() as conn:
            try:
                res = await conn.execute(
                    text("SELECT version FROM version_microservice WHERE microservice_id = 23")
                )
                row = res.fetchone()
                version = row[0] if row else 0
            except Exception as e:
                return {"status": "error", "error": str(e)}

        return {
            "status": "ok",
            "version": f"1.{version}.6",
            "name": "brain-weights-microservice",
            "text": "Calculates historical market weights based on cyclical economic events",
            "metadata": {
                "author": "Vlad",
                "stack": "Python 3 + MySQL",
            }
        }
    except Exception as e:
        print(f"❌ Error in get_metadata: {e}")
        send_error_trace(e, "get_metadata")
        raise


@app.get("/weights")
async def get_weights():
    try:
        return {"weights": GLOBAL_WEIGHT_CODES}
    except Exception as e:
        send_error_trace(e, "get_weights")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/new_weights")
async def get_new_weights(code: str = Query(...)):
    try:
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

        async with engine_vlad.connect() as conn:
            query = """
                SELECT weight_code
                FROM vlad_weight_codes
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

    except HTTPException:
        raise
    except Exception as e:
        send_error_trace(e, "get_new_weights")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/values")
async def get_values(pair: int = Query(1), day: int = Query(0), date: str = Query(...)):
    return await calculate_pure_memory(pair, day, date)


if __name__ == "__main__":
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=8888, reload=False, workers=1)
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен пользователем")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка при запуске сервера: {e!r}")
        send_error_trace(e)