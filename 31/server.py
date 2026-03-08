import uvicorn
import os
import asyncio
import traceback
import requests
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, date as date_type, time as time_type
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv
import bisect
from collections import defaultdict

# ── Словари кодировки ─────────────────────────────────────────────────────────
RATE_CHANGE_MAP = {
    "UNKNOWN": "X",
    "UP":      "U",
    "DOWN":    "D",
    "FLAT":    "F",
}

TREND_MAP = {
    "UNKNOWN": "X",
    "ABOVE":   "A",
    "BELOW":   "B",
    "AT":      "T",
}

MOMENTUM_MAP = {
    "UNKNOWN": "X",
    "UP":      "U",
    "DOWN":    "D",
    "FLAT":    "F",
}

# ── Трассировка ошибок ────────────────────────────────────────────────────────
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "brain-market-weights-microservice")
EMAIL      = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "market_server.py"):
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

# ── Параметры классификации (из market_context_idx.py) ───────────────────────
SMA_SHORT = int(os.getenv("SMA_SHORT", "24"))
SMA_LONG  = int(os.getenv("SMA_LONG",  "168"))

THRESHOLD_BY_INSTRUMENT = {
    "EURUSD": 0.0003,
    "DXY":    0.0003,
    "BTC":    0.002,
    "ETH":    0.003,
    "GOLD":   0.001,
    "OIL":    0.002,
}
DEFAULT_THRESHOLD = 0.001

# Колонки из vlad_market_history для каждого инструмента
INSTRUMENT_COLUMNS = {
    "EURUSD": "EURUSD_Close",
    "BTC":    "BTC_Close",
    "ETH":    "ETH_Close",
    "DXY":    "DXY_Close",
    "GOLD":   "Gold_Close",
    "OIL":    "Oil_Close",
}

SHIFT_WINDOW = int(os.getenv("SHIFT_WINDOW", "12"))   # ±12 часов (часовые данные)
RECURRING_MIN_COUNT = 2

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_MKT_BY_INSTR    = {}          # instrument → [(datetime, close), ...]
GLOBAL_MKT_CONTEXT     = {}          # (instrument, datetime) → (rcd, td, md)
GLOBAL_MKT_OBS_DTS     = defaultdict(set)   # datetime → {instruments}
GLOBAL_MKT_CTX_HIST    = {}          # (instrument, rcd, td, md) → [datetime, ...] sorted

GLOBAL_CTX_INDEX       = {}          # (instrument, rcd, td, md) → {occurrence_count}
GLOBAL_WEIGHT_CODES    = []

GLOBAL_RATES           = {}          # table → {datetime: t1}
GLOBAL_EXTREMUMS       = {}          # table → {min: set, max: set}
GLOBAL_CANDLE_RANGES   = {}          # table → {datetime: range}
GLOBAL_AVG_RANGE       = {}          # table → float
GLOBAL_LAST_CANDLES    = {}          # table → [(datetime, is_bull), ...]

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


# ── Классификация наблюдений (аналог market_context_idx.py) ──────────────────

def _compute_sma(series, idx, window):
    if idx < window - 1:
        return None
    return sum(v for _, v in series[idx - window + 1: idx + 1]) / window


def _direction_label(a, b, threshold,
                     up_label="UP", down_label="DOWN", flat_label="FLAT"):
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct > threshold:
        return up_label
    if pct < -threshold:
        return down_label
    return flat_label


def classify_market_observations(series, threshold):
    """
    series: [(datetime, close), ...]  уже отсортированный по datetime.
    Возвращает [(dt, rcd, td, md), ...]
    """
    results = []
    for i, (dt, close) in enumerate(series):
        rcd = ("UNKNOWN" if i == 0
               else _direction_label(close, series[i - 1][1], threshold))

        sma_long = _compute_sma(series, i, SMA_LONG)
        td = (_direction_label(close, sma_long, threshold, "ABOVE", "BELOW", "AT")
              if sma_long is not None else "UNKNOWN")

        sma_short = _compute_sma(series, i, SMA_SHORT)
        md = (_direction_label(sma_short, sma_long, threshold)
              if (sma_short is not None and sma_long is not None)
              else "UNKNOWN")

        results.append((dt, rcd, td, md))
    return results


# ── Weight code ───────────────────────────────────────────────────────────────

def make_weight_code(instrument: str, rcd: str, td: str, md: str,
                     mode: int, hour_shift: int | None = None) -> str:
    rcd_c = RATE_CHANGE_MAP.get(rcd, "X")
    td_c  = TREND_MAP.get(td, "X")
    md_c  = MOMENTUM_MAP.get(md, "X")
    base  = f"{instrument}_{rcd_c}_{td_c}_{md_c}_{mode}"
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


def find_prev_candle_trend(table: str, target_date: datetime):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None


# ── Предзагрузка данных в RAM ─────────────────────────────────────────────────

async def preload_all_data():
    global LAST_RELOAD_TIME
    global GLOBAL_WEIGHT_CODES
    global GLOBAL_CTX_INDEX
    global GLOBAL_MKT_BY_INSTR
    global GLOBAL_MKT_CONTEXT
    global GLOBAL_MKT_OBS_DTS
    global GLOBAL_MKT_CTX_HIST
    global GLOBAL_RATES
    global GLOBAL_EXTREMUMS
    global GLOBAL_CANDLE_RANGES
    global GLOBAL_AVG_RANGE
    global GLOBAL_LAST_CANDLES

    print("🔄 MARKET FULL DATA RELOAD STARTED")

    GLOBAL_WEIGHT_CODES.clear()
    GLOBAL_CTX_INDEX.clear()
    GLOBAL_MKT_BY_INSTR.clear()
    GLOBAL_MKT_CONTEXT.clear()
    GLOBAL_MKT_OBS_DTS.clear()
    GLOBAL_MKT_CTX_HIST.clear()
    GLOBAL_RATES.clear()
    GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear()
    GLOBAL_AVG_RANGE.clear()
    GLOBAL_LAST_CANDLES.clear()

    # ── 1. vlad БД: веса, контексты, рыночные данные ────────────────────────
    async with engine_vlad.connect() as conn:

        # 1.1 Веса
        try:
            res = await conn.execute(
                text("SELECT weight_code FROM vlad_market_weights")
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
                SELECT instrument, rate_change_dir, trend_dir,
                       momentum_dir, occurrence_count
                FROM vlad_market_context_idx
            """))
            for r in res.mappings().all():
                key = (r["instrument"], r["rate_change_dir"],
                       r["trend_dir"], r["momentum_dir"])
                GLOBAL_CTX_INDEX[key] = {
                    "occurrence_count": r["occurrence_count"] or 0
                }
            print(f"  ctx_index загружено: {len(GLOBAL_CTX_INDEX)} контекстов")
        except Exception as e:
            print(f"❌ Ошибка загрузки ctx_index: {e}")

        # 1.3 Рыночные данные vlad_market_history + классификация
        try:
            # Загружаем все колонки за один запрос
            col_list = ", ".join(
                f"`{col}`" for col in INSTRUMENT_COLUMNS.values()
            )
            res = await conn.execute(text(f"""
                SELECT `datetime`, {col_list}
                FROM vlad_market_history
                ORDER BY `datetime`
            """))
            rows = res.fetchall()
            print(f"  vlad_market_history строк: {len(rows)}")

            # Разбиваем по инструментам
            by_instr = {instr: [] for instr in INSTRUMENT_COLUMNS}
            col_names = list(INSTRUMENT_COLUMNS.values())
            instruments = list(INSTRUMENT_COLUMNS.keys())

            for row in rows:
                dt = row[0]
                for i, instr in enumerate(instruments):
                    val = row[i + 1]
                    if val is not None:
                        by_instr[instr].append((dt, float(val)))

            # Классифицируем каждый инструмент
            for instr, series in by_instr.items():
                if not series:
                    print(f"  ⚠️  Нет данных для {instr}")
                    continue
                threshold = THRESHOLD_BY_INSTRUMENT.get(instr, DEFAULT_THRESHOLD)
                classified = classify_market_observations(series, threshold)
                GLOBAL_MKT_BY_INSTR[instr] = series

                for dt, rcd, td, md in classified:
                    GLOBAL_MKT_CONTEXT[(instr, dt)] = (rcd, td, md)
                    GLOBAL_MKT_OBS_DTS[dt].add(instr)
                    ctx_key = (instr, rcd, td, md)
                    GLOBAL_MKT_CTX_HIST.setdefault(ctx_key, []).append(dt)

            for key in GLOBAL_MKT_CTX_HIST:
                GLOBAL_MKT_CTX_HIST[key].sort()

            obs_total = sum(len(v) for v in GLOBAL_MKT_BY_INSTR.values())
            print(f"  Инструментов: {len(GLOBAL_MKT_BY_INSTR)}, "
                  f"наблюдений: {obs_total}, "
                  f"контекстов: {len(GLOBAL_MKT_CONTEXT)}")
        except Exception as e:
            print(f"❌ Ошибка загрузки market_history: {e}")

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
    print("✅ MARKET FULL DATA RELOAD COMPLETED")


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            print(f"❌ Background reload error: {e}")
            send_error_trace(e, "market_background_reload")


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

    # Для часовых данных окно — ±SHIFT_WINDOW часов
    # Для дневных — ±SHIFT_WINDOW дней
    delta_unit = timedelta(days=1) if day == 1 else timedelta(hours=1)

    check_dts = [
        target_date + delta_unit * s
        for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1)
    ]

    # Собираем наблюдения из рыночного контекста в окне
    observations = []
    for dt in check_dts:
        for instr in GLOBAL_MKT_OBS_DTS.get(dt, set()):
            ctx = GLOBAL_MKT_CONTEXT.get((instr, dt))
            if ctx:
                shift_steps = round(
                    (target_date - dt) / delta_unit
                )
                observations.append((instr, dt, ctx, shift_steps))

    if not observations:
        return {}

    ram_rates  = GLOBAL_RATES.get(rates_table, {})
    ram_ranges = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range  = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext    = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}

    for instr, obs_dt, (rcd, td, md), shift in observations:
        ctx_key  = (instr, rcd, td, md)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue

        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT

        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            continue

        # Исторические datetime с тем же контекстом ДО target_date
        all_ctx_dts = GLOBAL_MKT_CTX_HIST.get(ctx_key, [])
        idx = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts = all_ctx_dts[:idx]
        if not valid_dts:
            continue

        # Применяем сдвиг к историческим датам
        t_dates = [d + delta_unit * shift for d in valid_dts]
        shift_arg = shift if is_recurring else None

        # mode=0: T1
        if calc_type in (0, 1):
            t1_sum = compute_t1_value(
                t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(instr, rcd, td, md, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        # mode=1: Extremum
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges, avg_range,
                modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(instr, rcd, td, md, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    required_tables_vlad = [
        "vlad_market_weights",
        "vlad_market_context_idx",
        "vlad_market_history",
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
                "WHERE microservice_id = 31"))
            row = res.fetchone()
            version = row[0] if row else 0
        except Exception as e:
            return {"status": "error", "error": str(e)}

    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "name":    "brain-market-weights-microservice",
        "text":    (
            "Calculates historical market weights keyed by "
            "vlad_market_history context "
            "(rate_change_dir / trend_dir / momentum_dir)"
        ),
        "weight_code_format":
            "{instrument}_{rcd}_{td}_{md}_{mode}[_{hour_shift}]",
        "instruments": list(INSTRUMENT_COLUMNS.keys()),
        "params": {
            "pair": "1=EURUSD, 3=BTC, 4=ETH",
            "day":  "1=daily brain table, 0=hourly brain table",
            "type": "0=T1+extremum, 1=T1 only, 2=extremum only",
            "var":  "0=all/linear, 1=filtered/linear, 2=all/squared, "
                    "3=filtered/squared, 4=filtered/range_delta",
        },
        "classification": {
            "sma_short": SMA_SHORT,
            "sma_long":  SMA_LONG,
            "thresholds": THRESHOLD_BY_INSTRUMENT,
        },
        "metadata": {
            "author":            "Vlad",
            "stack":             "Python 3 + MySQL + FastAPI",
            "ctx_index_rows":    len(GLOBAL_CTX_INDEX),
            "weight_codes":      len(GLOBAL_WEIGHT_CODES),
            "instruments_loaded": list(GLOBAL_MKT_BY_INSTR.keys()),
            "mkt_observations":  sum(len(v) for v in GLOBAL_MKT_BY_INSTR.values()),
            "last_reload":       (LAST_RELOAD_TIME.isoformat()
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
    service_id = 31
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
    return {"status": "ok",
            "from_version": old_version,
            "to_version":   new_version}


# ── Точка входа ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        uvicorn.run("server:app",
                    host="0.0.0.0", port=8894,
                    reload=False, workers=8)
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
