import uvicorn
import os
import asyncio
import bisect
import traceback
import requests
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv


# ── Словари кодировки (зеркало brain_calendar_weights.py) ────────────────────

FORECAST_MAP = {
    "UNKNOWN": "X",
    "BEAT":    "B",
    "MISS":    "M",
    "INLINE":  "I",
}
SURPRISE_MAP = {
    "UNKNOWN": "X",
    "UP":      "U",
    "DOWN":    "D",
    "FLAT":    "F",
}
REVISION_MAP = {
    "NONE":    "N",
    "FLAT":    "T",
    "UP":      "U",
    "DOWN":    "D",
    "UNKNOWN": "X",
}
IMPORTANCE_MAP = {
    "high":   "H",
    "medium": "M",
    "low":    "L",
    "none":   "N",
}


# ── Трассировка ошибок ────────────────────────────────────────────────────────

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME  = os.getenv("NODE_NAME",    "brain-calendar-weights-microservice")
EMAIL      = os.getenv("ALERT_EMAIL",  "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "server.py"):
    logs = (
        f"Node: {NODE_NAME}\nScript: {script_name}\n"
        f"Exception: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    )
    try:
        print(f"\n📤 Sending error trace to {TRACE_URL}")
        r = requests.post(TRACE_URL, data={
            "url": "fastapi_microservice", "node": NODE_NAME,
            "email": EMAIL, "logs": logs}, timeout=10)
        print(f"✅ Status: {r.status_code}")
    except Exception as e:
        print(f"⚠️ Failed to send trace: {e}")


# ── Конфигурация ──────────────────────────────────────────────────────────────

load_dotenv()

DB_HOST         = os.getenv("DB_HOST",         "127.0.0.1")
DB_PORT         = os.getenv("DB_PORT",         "3306")
DB_USER         = os.getenv("DB_USER",         "vlad")
DB_PASSWORD     = os.getenv("DB_PASSWORD",     "")
DB_NAME         = os.getenv("DB_NAME",         "vlad")
MASTER_HOST     = os.getenv("MASTER_HOST",     "127.0.0.1")
MASTER_PORT     = os.getenv("MASTER_PORT",     "3306")
MASTER_USER     = os.getenv("MASTER_USER",     "vlad")
MASTER_PASSWORD = os.getenv("MASTER_PASSWORD", "")
MASTER_NAME     = os.getenv("MASTER_NAME",     "brain")

DATABASE_URL       = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BRAIN_DATABASE_URL = f"mysql+aiomysql://{MASTER_USER}:{MASTER_PASSWORD}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}"

print(f"📊 vlad:  {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"📊 brain: {MASTER_USER}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}")

engine_vlad  = create_async_engine(DATABASE_URL,       pool_size=10, echo=False)
engine_brain = create_async_engine(BRAIN_DATABASE_URL, pool_size=5,  echo=False)

# Параметры классификации (из brain_calendar_context_idx.py)
DIRECTION_THRESHOLD = float(os.getenv("DIRECTION_THRESHOLD", "0.01"))
SHIFT_WINDOW        = int(os.getenv("SHIFT_WINDOW", "12"))    # ±12 часов
RECURRING_MIN_COUNT = 2
SKIP_EVENT_TYPES    = {2}


# ── Глобальные данные ─────────────────────────────────────────────────────────

# (event_id, currency, importance, fcd, scd, rcd) → {occurrence_count, [datetime, ...]}
GLOBAL_CAL_CTX_INDEX   = {}
# (event_id, currency, importance, fcd, scd, rcd) → sorted [datetime, ...]
GLOBAL_CAL_CTX_HIST    = {}
# datetime → [(event_id, currency, importance, fcd, scd, rcd), ...]
GLOBAL_CAL_BY_DT       = {}

GLOBAL_WEIGHT_CODES    = []

GLOBAL_RATES           = {}   # table → {datetime: t1}
GLOBAL_EXTREMUMS       = {}   # table → {min: set, max: set}
GLOBAL_CANDLE_RANGES   = {}   # table → {datetime: range}
GLOBAL_AVG_RANGE       = {}   # table → float
GLOBAL_LAST_CANDLES    = {}   # table → [(datetime, is_bull), ...]

LAST_RELOAD_TIME       = None


# ── Утилиты ───────────────────────────────────────────────────────────────────

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
    for fmt in ("%Y-%d-%m %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
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


# ── Классификация событий ─────────────────────────────────────────────────────

def _rel_direction(actual, reference, threshold,
                   up_label="UP", down_label="DOWN", flat_label="FLAT"):
    if actual is None or reference is None:
        return "UNKNOWN"
    if reference == 0:
        if actual > 0:
            return up_label
        if actual < 0:
            return down_label
        return flat_label
    pct = (actual - reference) / abs(reference)
    if pct > threshold:
        return up_label
    if pct < -threshold:
        return down_label
    return flat_label


def classify_event(forecast, previous, old_prev, actual, threshold):
    if forecast is None or forecast == 0:
        fcd = "UNKNOWN"
    else:
        fcd = _rel_direction(actual, forecast, threshold,
                             up_label="BEAT", down_label="MISS",
                             flat_label="INLINE")
    scd = _rel_direction(actual, previous, threshold)
    if old_prev is None or old_prev == 0 or previous is None:
        rcd = "NONE"
    elif previous == old_prev:
        rcd = "FLAT"
    else:
        rcd = _rel_direction(previous, old_prev, threshold)
    return fcd, scd, rcd


# ── Weight code ───────────────────────────────────────────────────────────────

def make_weight_code(event_id: int, currency: str, importance: str,
                     fcd: str, scd: str, rcd: str,
                     mode: int, hour_shift: int | None = None) -> str:
    imp_c = IMPORTANCE_MAP.get(importance, "X")
    fcd_c = FORECAST_MAP.get(fcd, "X")
    scd_c = SURPRISE_MAP.get(scd, "X")
    rcd_c = REVISION_MAP.get(rcd, "X")
    base  = f"E{event_id}_{currency}_{imp_c}_{fcd_c}_{scd_c}_{rcd_c}_{mode}"
    return base if hour_shift is None else f"{base}_{hour_shift}"


# ── Вычисление T1 / Extremum ──────────────────────────────────────────────────

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


def compute_extremum_value(t_dates, calc_var, ext_set,
                            candle_ranges, avg_range,
                            modification, total_hist):
    need_filter = calc_var in (1, 3, 4)
    use_range   = calc_var == 4
    pool = ([d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range]
            if need_filter else t_dates)
    if not pool:
        return None
    if use_range:
        val = sum(candle_ranges.get(d, 0.0) - avg_range
                  for d in pool if d in ext_set)
        return val if val != 0 else None
    if total_hist == 0:
        return None
    matches = sum(1 for d in pool if d in ext_set)
    val = ((matches / total_hist) * 2 - 1) * modification
    return val if val != 0 else None


# ── Предзагрузка данных в RAM ─────────────────────────────────────────────────

async def preload_all_data():
    global LAST_RELOAD_TIME
    global GLOBAL_WEIGHT_CODES
    global GLOBAL_CAL_CTX_INDEX
    global GLOBAL_CAL_CTX_HIST
    global GLOBAL_CAL_BY_DT
    global GLOBAL_RATES
    global GLOBAL_EXTREMUMS
    global GLOBAL_CANDLE_RANGES
    global GLOBAL_AVG_RANGE
    global GLOBAL_LAST_CANDLES

    print("🔄 CALENDAR FULL DATA RELOAD STARTED")

    GLOBAL_WEIGHT_CODES.clear()
    GLOBAL_CAL_CTX_INDEX.clear()
    GLOBAL_CAL_CTX_HIST.clear()
    GLOBAL_CAL_BY_DT.clear()
    GLOBAL_RATES.clear()
    GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear()
    GLOBAL_AVG_RANGE.clear()
    GLOBAL_LAST_CANDLES.clear()

    # ── 1. vlad БД: веса, контексты, события календаря ──────────────────────
    async with engine_vlad.connect() as conn:

        # 1.1 Веса
        try:
            res = await conn.execute(
                text("SELECT weight_code FROM brain_calendar_weights")
            )
            GLOBAL_WEIGHT_CODES = [r[0] for r in res.fetchall()]
            print(f"  weight_codes загружено: {len(GLOBAL_WEIGHT_CODES)}")
            if GLOBAL_WEIGHT_CODES:
                print(f"  Примеры: {GLOBAL_WEIGHT_CODES[:5]}")
        except Exception as e:
            print(f"❌ Ошибка загрузки weight_codes: {e}")

        # 1.2 Контекстный индекс
        try:
            res = await conn.execute(text("""
                SELECT event_id, currency_code, importance,
                       forecast_dir, surprise_dir, revision_dir,
                       occurrence_count
                FROM brain_calendar_context_idx
            """))
            for r in res.mappings().all():
                key = (r["event_id"], r["currency_code"], r["importance"],
                       r["forecast_dir"], r["surprise_dir"], r["revision_dir"])
                GLOBAL_CAL_CTX_INDEX[key] = {
                    "occurrence_count": r["occurrence_count"] or 0
                }
            print(f"  ctx_index загружено: {len(GLOBAL_CAL_CTX_INDEX)} контекстов")
        except Exception as e:
            print(f"❌ Ошибка загрузки ctx_index: {e}")

        # 1.3 Исторические события brain_calendar → классификация
        try:
            res = await conn.execute(text("""
                SELECT EventId, CurrencyCode, Importance,
                       ForecastValue, PreviousValue, OldPreviousValue,
                       ActualValue, FullDate, EventType
                FROM brain_calendar
                WHERE ActualValue IS NOT NULL
                  AND Processed = 1
                ORDER BY FullDate
            """))
            rows = res.fetchall()
            print(f"  brain_calendar строк: {len(rows)}")

            for (event_id, currency, importance,
                 forecast, previous, old_prev,
                 actual, full_date, event_type) in rows:

                if event_type in SKIP_EVENT_TYPES:
                    continue
                if event_id is None or currency is None:
                    continue

                fcd, scd, rcd = classify_event(
                    forecast, previous, old_prev, actual, DIRECTION_THRESHOLD
                )
                imp = (importance or "none").lower()
                key = (event_id, currency, imp, fcd, scd, rcd)

                GLOBAL_CAL_CTX_HIST.setdefault(key, []).append(full_date)
                GLOBAL_CAL_BY_DT.setdefault(full_date, []).append(key)

            for key in GLOBAL_CAL_CTX_HIST:
                GLOBAL_CAL_CTX_HIST[key].sort()

            total_obs = sum(len(v) for v in GLOBAL_CAL_CTX_HIST.values())
            print(f"  Контекстов: {len(GLOBAL_CAL_CTX_HIST)}, "
                  f"наблюдений: {total_obs}")
        except Exception as e:
            print(f"❌ Ошибка загрузки brain_calendar: {e}")

    # ── 2. brain БД: котировки для расчёта T1/Extremum ───────────────────────
    tables = [
        "brain_rates_eur_usd",     "brain_rates_eur_usd_day",
        "brain_rates_btc_usd",     "brain_rates_btc_usd_day",
        "brain_rates_eth_usd",     "brain_rates_eth_usd_day",
    ]
    for table in tables:
        GLOBAL_RATES[table]         = {}
        GLOBAL_LAST_CANDLES[table]  = []
        GLOBAL_CANDLE_RANGES[table] = {}
        GLOBAL_AVG_RANGE[table]     = 0.0
        GLOBAL_EXTREMUMS[table]     = {"min": set(), "max": set()}
        try:
            async with engine_brain.connect() as conn:
                res = await conn.execute(text(
                    f"SELECT date, open, close, `max`, `min`, t1 FROM `{table}`"
                ))
                rows_b = sorted(res.mappings().all(), key=lambda x: x["date"])
                ranges = []
                for r in rows_b:
                    dt = r["date"]
                    if r["t1"] is not None:
                        GLOBAL_RATES[table][dt] = float(r["t1"])
                    is_bull = r["close"] > r["open"]
                    GLOBAL_LAST_CANDLES[table].append((dt, is_bull))
                    rng = float(r["max"] or 0) - float(r["min"] or 0)
                    GLOBAL_CANDLE_RANGES[table][dt] = rng
                    ranges.append(rng)

                GLOBAL_AVG_RANGE[table] = (
                    sum(ranges) / len(ranges) if ranges else 0.0
                )

                for typ in ("min", "max"):
                    op  = ">" if typ == "max" else "<"
                    q   = f"""
                        SELECT t1.date FROM `{table}` t1
                        JOIN `{table}` t_prev ON t_prev.date = t1.date - INTERVAL 1 DAY
                        JOIN `{table}` t_next ON t_next.date = t1.date + INTERVAL 1 DAY
                        WHERE t1.`{typ}` {op} t_prev.`{typ}`
                          AND t1.`{typ}` {op} t_next.`{typ}`
                    """
                    res_ext = await conn.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {
                        r["date"] for r in res_ext.mappings().all()
                    }

            print(f"  {table}: {len(GLOBAL_RATES[table])} свечей")
        except Exception as e:
            print(f"❌ Ошибка загрузки {table}: {e}")

    LAST_RELOAD_TIME = datetime.now()
    print("✅ CALENDAR FULL DATA RELOAD COMPLETED")


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            print(f"❌ Background reload error: {e}")
            send_error_trace(e, "calendar_background_reload")


# ── FastAPI lifespan ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_all_data()
    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()
    await engine_vlad.dispose()
    await engine_brain.dispose()


app = FastAPI(lifespan=lifespan)


# ── Основной расчёт ───────────────────────────────────────────────────────────

async def calculate_pure_memory(pair: int, day: int, date_str: str,
                                 calc_type: int = 0,
                                 calc_var: int = 0) -> dict:
    target_date = parse_date_string(date_str)
    if not target_date:
        return {"error": "Invalid date format"}

    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    delta_unit   = timedelta(days=1) if day == 1 else timedelta(hours=1)

    # Ищем события в окне ±SHIFT_WINDOW относительно target_date
    check_dts = [
        target_date + delta_unit * s
        for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1)
    ]

    # Собираем все события из окна
    observations = []
    for dt in check_dts:
        if dt > target_date:
            continue
        for ctx_key in GLOBAL_CAL_BY_DT.get(dt, []):
            shift_steps = round((target_date - dt) / delta_unit)
            observations.append((ctx_key, dt, shift_steps))

    if not observations:
        return {}

    ram_rates  = GLOBAL_RATES.get(rates_table, {})
    ram_ranges = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range  = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext    = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}

    for obs in observations:
        # Проверяем, что наблюдение состоит из трёх элементов
        if len(obs) != 3:
            continue
        ctx_key, obs_dt, shift = obs
        # Проверяем, что ключ контекста — кортеж из 6 элементов
        if not isinstance(ctx_key, tuple) or len(ctx_key) != 6:
            continue
        event_id, currency, importance, fcd, scd, rcd = ctx_key

        # Далее идёт оригинальное тело цикла (без изменений)
        ctx_key_full = (event_id, currency, importance, fcd, scd, rcd)
        ctx_info = GLOBAL_CAL_CTX_INDEX.get(ctx_key_full)
        if ctx_info is None:
            continue

        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT

        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            continue

        all_ctx_dts = GLOBAL_CAL_CTX_HIST.get(ctx_key_full, [])
        idx = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts = all_ctx_dts[:idx]
        if not valid_dts:
            continue

        t_dates = [
            d + delta_unit * shift
            for d in valid_dts
            if d + delta_unit * shift <= target_date
        ]
        shift_arg = shift if is_recurring else None

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(
                t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(event_id, currency, importance,
                                  fcd, scd, rcd, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges, avg_range,
                modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(event_id, currency, importance,
                                      fcd, scd, rcd, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val
        ctx_key  = (event_id, currency, importance, fcd, scd, rcd)
        ctx_info = GLOBAL_CAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue

        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT

        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            continue

        # Исторические datetime с тем же контекстом ДО target_date
        all_ctx_dts = GLOBAL_CAL_CTX_HIST.get(ctx_key, [])
        idx = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts = all_ctx_dts[:idx]
        if not valid_dts:
            continue

        t_dates = [
            d + delta_unit * shift
            for d in valid_dts
            if d + delta_unit * shift <= target_date
        ]
        shift_arg = shift if is_recurring else None

        # mode=0: T1
        if calc_type in (0, 1):
            t1_sum = compute_t1_value(
                t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(event_id, currency, importance,
                                  fcd, scd, rcd, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        # mode=1: Extremum
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges, avg_range,
                modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(event_id, currency, importance,
                                      fcd, scd, rcd, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    required_tables_vlad = [
        "brain_calendar_weights",
        "brain_calendar_context_idx",
        "brain_calendar",
        "version_microservice",
    ]
    brain_tables = [
        "brain_rates_eur_usd",
        "brain_rates_btc_usd",
        "brain_rates_eth_usd",
    ]

    async with engine_vlad.connect() as conn:
        for t in required_tables_vlad:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            except Exception as e:
                return {"status": "error",
                        "error": f"Table {t} in 'vlad' inaccessible: {e}"}

    async with engine_brain.connect() as conn:
        for t in brain_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            except Exception as e:
                return {"status": "error",
                        "error": f"Table {t} in 'brain' inaccessible: {e}"}

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(text(
                "SELECT version FROM version_microservice "
                "WHERE microservice_id = 33"))
            row = res.fetchone()
            version = row[0] if row else 0
        except Exception as e:
            return {"status": "error", "error": str(e)}

    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "name":    "brain-calendar-weights-microservice",
        "text":    (
            "Calculates historical calendar weights keyed by "
            "brain_calendar event context "
            "(forecast_dir / surprise_dir / revision_dir)"
        ),
        "weight_code_format":
            "E{event_id}_{currency}_{imp}_{fcd}_{scd}_{rcd}_{mode}[_{hour_shift}]",
        "params": {
            "pair": "1=EURUSD, 3=BTC, 4=ETH",
            "day":  "1=daily brain table, 0=hourly brain table",
            "type": "0=T1+extremum, 1=T1 only, 2=extremum only",
            "var":  "0=all/linear, 1=filtered/linear, 2=all/squared, "
                    "3=filtered/squared, 4=filtered/range_delta",
        },
        "classification": {
            "direction_threshold": DIRECTION_THRESHOLD,
            "shift_window":        SHIFT_WINDOW,
            "forecast_dirs":       list(FORECAST_MAP.keys()),
            "surprise_dirs":       list(SURPRISE_MAP.keys()),
            "revision_dirs":       list(REVISION_MAP.keys()),
        },
        "metadata": {
            "author":              "Vlad",
            "stack":               "Python 3 + MySQL + FastAPI",
            "ctx_index_rows":      len(GLOBAL_CAL_CTX_INDEX),
            "weight_codes":        len(GLOBAL_WEIGHT_CODES),
            "calendar_contexts":   len(GLOBAL_CAL_CTX_HIST),
            "calendar_obs_total":  sum(len(v) for v in GLOBAL_CAL_CTX_HIST.values()),
            "last_reload":         (LAST_RELOAD_TIME.isoformat()
                                    if LAST_RELOAD_TIME else None),
        },
    }


@app.get("/weights")
async def get_weights():
    return {"weights": GLOBAL_WEIGHT_CODES,
            "total":   len(GLOBAL_WEIGHT_CODES)}


@app.get("/values")
async def get_values(
    pair: int = Query(1,   description="1=EURUSD, 3=BTC, 4=ETH"),
    day:  int = Query(1,   description="1=daily brain table, 0=hourly"),
    date: str = Query(..., description="Target datetime, e.g. 2025-01-15 or 2025-01-15 14:00:00"),
    type: int = Query(0, ge=0, le=2,
                      description="0=T1+extremum, 1=T1 only, 2=extremum only"),
    var:  int = Query(0, ge=0, le=4,
                      description="Calculation variant 0..4"),
):
    return await calculate_pure_memory(
        pair, day, date, calc_type=type, calc_var=var)


@app.post("/patch")
async def patch_service():
    service_id = 33
    async with engine_vlad.begin() as conn:
        res = await conn.execute(text(
            "SELECT version FROM version_microservice "
            "WHERE microservice_id = :id"), {"id": service_id})
        row = res.fetchone()
        if not row:
            raise HTTPException(
                status_code=500,
                detail=f"Service ID {service_id} not found")
        old_version = row[0]
        new_version = max(old_version, 1)
        if new_version != old_version:
            await conn.execute(text(
                "UPDATE version_microservice "
                "SET version = :v WHERE microservice_id = :id"),
                {"v": new_version, "id": service_id})
    return {"status":       "ok",
            "from_version": old_version,
            "to_version":   new_version}


# ── Точка входа ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        uvicorn.run("server:app",
                    host="0.0.0.0", port=8896,
                    reload=False, workers=1)
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
