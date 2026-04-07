import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
import functools
import numpy as np
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, time as time_type

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

SERVICE_ID = 30
NODE_NAME = os.getenv("NODE_NAME", "brain-ecb-weights-microservice")
PORT = 8893

RATE_CHANGE_MAP = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}
TREND_MAP = {"UNKNOWN": "X", "ABOVE": "A", "BELOW": "B", "AT": "T"}
MOMENTUM_MAP = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

SMA_SHORT = 5
SMA_LONG = 20
THRESHOLD_PCT = 0.0003
RECURRING_MIN_COUNT = 2
SHIFT_WINDOW = int(os.getenv("SHIFT_WINDOW", "12"))

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_ECB_BY_CCY = {}
GLOBAL_ECB_CONTEXT = {}
GLOBAL_ECB_OBS_DATES = defaultdict(set)
GLOBAL_ECB_CTX_HIST = {}
GLOBAL_CTX_INDEX = {}
GLOBAL_WEIGHT_CODES = []
GLOBAL_RATES = {}
GLOBAL_EXTREMUMS = {}
GLOBAL_CANDLE_RANGES = {}
GLOBAL_AVG_RANGE = {}
GLOBAL_LAST_CANDLES = {}
_RATES_DATE_INDEX = {}   # table → {date → [datetime, ...]} для поиска hourly свечей по дате
SERVICE_URL = ""
LAST_RELOAD_TIME = None

# ── NumPy-ускоренные структуры (строятся в preload, используются в _compute_cpu_only) ──
# Для каждой rates-таблицы: отсортированный массив дат, t1, ranges, ext_min, ext_max
NP_RATES:    dict = {}   # table → {"dates_ns": int64[], "t1": float64[], "ranges": float64[], "ext_min": bool[], "ext_max": bool[]}
NP_CTX_HIST: dict = {}   # ctx_key → int64[] (unix timestamps, sorted)
NP_CTX_HIST_BUILT: bool = False


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


def _sma(rates_sorted, idx, window):
    if idx < window - 1:
        return None
    return sum(r for _, r in rates_sorted[idx - window + 1: idx + 1]) / window


def _dir(a, b, up="UP", down="DOWN", flat="FLAT"):
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct > THRESHOLD_PCT:
        return up
    if pct < -THRESHOLD_PCT:
        return down
    return flat


def make_weight_code(ccy, rcd, td, md, mode, shift):
    base = f"{ccy}_{rcd}_{td}_{md}_{mode}"
    if shift is not None:
        base += f"_{shift}"
    return base


def compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range):
    if not t_dates:
        return 0.0
    
    t1s = []
    for t_date in t_dates:
        t1 = ram_rates.get(t_date)
        if t1 is not None:
            t1s.append(float(t1))
    
    if not t1s:
        return 0.0
    
    mean_t1 = sum(t1s) / len(t1s)
    
    if calc_var == 0:  # raw
        return mean_t1
    elif calc_var == 1:  # vol_filter
        return mean_t1
    elif calc_var == 2:  # squared
        return mean_t1 ** 2
    elif calc_var == 3:  # flt+sq
        return mean_t1 ** 2
    elif calc_var == 4:  # range
        ranges = [ram_ranges.get(t_date, 0.0) for t_date in t_dates]
        return sum(ranges) / len(ranges) if ranges else 0.0
    
    return mean_t1


def compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges, avg_range, modification, ctx_count):
    ext_count = sum(1 for t_date in t_dates if t_date in ext_set)
    if ext_count == 0:
        return None
    
    prob = ext_count / len(t_dates) if t_dates else 0.0
    
    if calc_var == 0:  # raw
        val = prob
    elif calc_var == 1:  # vol_filter
        val = prob
    elif calc_var == 2:  # squared
        val = prob ** 2
    elif calc_var == 3:  # flt+sq
        val = prob ** 2
    elif calc_var == 4:  # range
        ranges = [ram_ranges.get(t_date, 0.0) for t_date in t_dates]
        val = (sum(ranges) / len(ranges) if ranges else 0.0) / (avg_range or 1.0)
    else:
        val = prob
    
    return val * modification


def classify_observations(rates):
    """Классифицировать ECB наблюдения (rate_change, trend, momentum)."""
    result = []
    rates_sorted = sorted(rates)
    
    if not rates_sorted:
        return result
    
    for i, (dt, rate) in enumerate(rates_sorted):
        idx = i
        
        # Rate change
        b1 = _sma(rates_sorted, idx, SMA_SHORT)
        b2 = _sma(rates_sorted, idx, SMA_LONG)
        rcd = _dir(b1, b2, up="U", down="D", flat="F")
        if rcd == "UNKNOWN":
            rcd = "X"
        
        # Trend (vs long)
        a = b1
        b = _sma(rates_sorted, idx, SMA_LONG)
        td = _dir(a, b, up="A", down="B", flat="T")
        if td == "UNKNOWN":
            td = "X"
        
        # Momentum (acceleration)
        ma_short_prev = _sma(rates_sorted, max(0, idx - 1), SMA_SHORT) if idx > 0 else None
        ma_short_curr = _sma(rates_sorted, idx, SMA_SHORT)
        md = _dir(ma_short_curr, ma_short_prev, up="U", down="D", flat="F") if ma_short_prev else "X"
        
        result.append((dt, rcd, td, md))
    
    return result


async def preload_all_data():
    """Инициализация всех глобальных структур."""
    global GLOBAL_ECB_BY_CCY, GLOBAL_ECB_CONTEXT, GLOBAL_ECB_OBS_DATES
    global GLOBAL_ECB_CTX_HIST, GLOBAL_CTX_INDEX, GLOBAL_WEIGHT_CODES
    global GLOBAL_RATES, GLOBAL_LAST_CANDLES, GLOBAL_CANDLE_RANGES, GLOBAL_AVG_RANGE
    global GLOBAL_EXTREMUMS, _RATES_DATE_INDEX, SERVICE_URL, LAST_RELOAD_TIME
    global NP_RATES, NP_CTX_HIST, NP_CTX_HIST_BUILT

    log("🔄 ECB FULL DATA RELOAD START", NODE_NAME, force=True)

    # Очищаем старые данные
    GLOBAL_ECB_BY_CCY.clear()
    GLOBAL_ECB_CONTEXT.clear()
    GLOBAL_ECB_OBS_DATES.clear()
    GLOBAL_ECB_CTX_HIST.clear()
    GLOBAL_CTX_INDEX.clear()
    GLOBAL_WEIGHT_CODES.clear()
    NP_RATES.clear()
    NP_CTX_HIST_BUILT = False

    # Загрузка ECB курсов с весами и контекстом
    try:
        async with engine_vlad.connect() as conn:
            # Веса
            res = await conn.execute(text("""
                SELECT context_code FROM vlad_ecb_rate_weights
                GROUP BY context_code
            """))
            GLOBAL_WEIGHT_CODES = [r[0] for r in res.fetchall()]

            # Индекс контекстов
            res = await conn.execute(text("""
                SELECT context_code, occurrence_count FROM vlad_ecb_rate_context_idx
            """))
            for r in res.mappings().all():
                ctx_code = r["context_code"]
                parts = ctx_code.split("_")
                if len(parts) >= 4:
                    ccy, rcd, td, md = parts[0], parts[1], parts[2], parts[3]
                    GLOBAL_CTX_INDEX[(ccy, rcd, td, md)] = {
                        "occurrence_count": int(r["occurrence_count"])
                    }

        log(f"  ECB weights: {len(GLOBAL_WEIGHT_CODES)}, contexts: {len(GLOBAL_CTX_INDEX)}",
            NODE_NAME)
    except Exception as e:
        log(f"❌ ECB weights/contexts from vlad: {e}", NODE_NAME, level="error", force=True)

    # Загрузка ECB курсов от brain
    try:
        async with engine_brain.connect() as conn:
            res = await conn.execute(text(
                "SELECT currency, rate_date, rate FROM vlad_ecb_exchange_rates ORDER BY rate_date"))
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
            log(f"  ECB currencies (from brain): {len(by_ccy)}, observations: {len(GLOBAL_ECB_CONTEXT)}",
                NODE_NAME)
    except Exception as e:
        log(f"❌ ECB rates from brain: {e}", NODE_NAME, level="error", force=True)

    # Загрузка свечных данных
    for table in ["brain_rates_eur_usd", "brain_rates_eur_usd_day",
                  "brain_rates_btc_usd", "brain_rates_btc_usd_day",
                  "brain_rates_eth_usd", "brain_rates_eth_usd_day"]:
        GLOBAL_RATES[table] = {}
        GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_CANDLE_RANGES[table] = {}
        GLOBAL_AVG_RANGE[table] = 0.0
        GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}
        _RATES_DATE_INDEX[table] = defaultdict(list)
        try:
            async with engine_brain.connect() as conn:
                res = await conn.execute(text(
                    f"SELECT date, open, close, `max`, `min`, t1 FROM {table}"))
                rows = sorted(res.mappings().all(), key=lambda x: x["date"])
                ranges = []
                for r in rows:
                    dt = r["date"]
                    if r["t1"] is not None:
                        GLOBAL_RATES[table][dt] = float(r["t1"])
                    GLOBAL_LAST_CANDLES[table].append((dt, r["close"] > r["open"]))
                    rng = float(r["max"] or 0) - float(r["min"] or 0)
                    GLOBAL_CANDLE_RANGES[table][dt] = rng
                    ranges.append(rng)
                    if isinstance(dt, datetime):
                        _RATES_DATE_INDEX[table][dt.date()].append(dt)
                GLOBAL_AVG_RANGE[table] = sum(ranges) / len(ranges) if ranges else 0.0
                interval = "1 DAY" if table.endswith("_day") else "1 HOUR"
                for typ in ("min", "max"):
                    op = ">" if typ == "max" else "<"
                    q = f"""SELECT t1.date FROM {table} t1
                        JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL {interval}
                        JOIN {table} t_next ON t_next.date = t1.date + INTERVAL {interval}
                        WHERE t1.{typ} {op} t_prev.{typ} AND t1.{typ} {op} t_next.{typ}"""
                    res_ext = await conn.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}
            log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)
        except Exception as e:
            log(f"❌ {table}: {e}", NODE_NAME, level="error")

    try:
        SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
        log(f"  SERVICE_URL loaded", NODE_NAME)
    except (OperationalError, Exception) as e:
        log(f"❌ load_service_url failed: {e}", NODE_NAME, level="error", force=True)
        SERVICE_URL = ""
    try:
        await ensure_cache_table(engine_vlad)
    except Exception as e:
        log(f"❌ ensure_cache_table failed: {e}", NODE_NAME, level="error")
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
    try:
        await preload_all_data()
    except Exception as e:
        log(f"❌ Initial data load failed, server continues: {e}",
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

# ── Подгрузка свежих свечей из БД ─────────────────────────────────────────
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
    max_dt = max(ram.keys()) if ram else None
    if not max_dt:
        return
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
                log(f"  📥 Refreshed {n} candle(s) for {rates_table}", NODE_NAME)
    except Exception as e:
        log(f"  ⚠️ Rates refresh error ({rates_table}): {e}", NODE_NAME, level="warning")


def _compute_cpu_only(pair, day, date_str, calc_type=0, calc_var=0):
    """CPU-only вычисление — использует NumPy если доступно, иначе чистый Python."""
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window = SHIFT_WINDOW
    target_d = target_date.date() if isinstance(target_date, datetime) else target_date
    check_dates = [target_d + timedelta(days=d) for d in range(-window, window + 1)]

    observations = []
    for dt in check_dates:
        if dt > target_d:
            continue
        for ccy in GLOBAL_ECB_OBS_DATES.get(dt, set()):
            ctx = GLOBAL_ECB_CONTEXT.get((ccy, dt))
            if ctx:
                observations.append((ccy, dt, ctx, (target_d - dt).days))

    if not observations:
        return {}

    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ranges = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}
    for ccy, obs_date, (rcd, td, md), shift in observations:
        ctx_key = (ccy, rcd, td, md)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue
        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > window:
            continue

        all_ctx_dates = GLOBAL_ECB_CTX_HIST.get(ctx_key, [])
        idx = bisect.bisect_left(all_ctx_dates, target_d)
        valid_dates = all_ctx_dates[:idx]
        if not valid_dates:
            continue

        t_dates = []
        date_index = _RATES_DATE_INDEX.get(rates_table, {})
        for d in valid_dates:
            shifted_date = d + timedelta(days=shift)
            if date_index:
                hourly_dts = date_index.get(shifted_date, [])
                for hdt in hourly_dts:
                    if hdt < target_date:
                        t_dates.append(hdt)
            else:
                dt_val = datetime.combine(shifted_date, time_type(0, 0))
                if dt_val < target_date:
                    t_dates.append(dt_val)
        shift_arg = shift if is_recurring else None

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(ccy, rcd, td, md, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges,
                                             avg_range, modification, len(valid_dates))
            if ext_val is not None:
                wc = make_weight_code(ccy, rcd, td, md, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


async def calculate_pure_memory(pair, day, date_str, calc_type=0, calc_var=0):
    """Основной endpoint /values — использует кеш + вычисление."""
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table = get_rates_table_name(pair, day)
    await _refresh_rates_if_needed(rates_table)
    
    # CPU вычисление в thread pool чтобы не блокировать event loop
    result = await asyncio.to_thread(
        functools.partial(_compute_cpu_only, pair, day, date_str,
                          calc_type=calc_type, calc_var=calc_var)
    )
    return result if result is not None else {}


@app.get("/")
async def get_metadata():
    for t in ["vlad_ecb_rate_weights", "vlad_ecb_rate_context_idx",
              "version_microservice"]:
        try:
            async with engine_vlad.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"vlad.{t} inaccessible: {e}"}

    try:
        async with engine_brain.connect() as conn:
            await conn.execute(text("SELECT 1 FROM vlad_ecb_exchange_rates LIMIT 1"))
    except Exception as e:
        return {"status": "error", "error": f"brain.vlad_ecb_exchange_rates inaccessible: {e}"}

    for t in ["brain_rates_eur_usd", "brain_rates_btc_usd", "brain_rates_eth_usd"]:
        try:
            async with engine_brain.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"brain.{t} inaccessible: {e}"}

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            "SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        version = (res.fetchone() or [0])[0]
    return {
        "status": "ok", "version": f"1.{version}.0", "mode": MODE,
        "name": NODE_NAME, "text": "ECB exchange rate context weights",
        "weight_code_format": "{currency}_{rcd}_{td}_{md}_{mode}[_{day_shift}]",
        "metadata": {
            "ctx_index_rows": len(GLOBAL_CTX_INDEX),
            "weight_codes": len(GLOBAL_WEIGHT_CODES),
            "ecb_currencies": len(GLOBAL_ECB_BY_CCY),
            "ecb_observations": len(GLOBAL_ECB_CONTEXT),
            "last_reload": LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
        },
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response(GLOBAL_WEIGHT_CODES)
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(str(e))


@app.get("/values")
async def get_values(
        pair: int = Query(1), day: int = Query(1), date: str = Query(...),
        type: int = Query(0, ge=0, le=2), var: int = Query(0, ge=0, le=4),
):
    """
    Основной эндпоинт для получения весов с кешированием.
    Проверяет vlad_values_cache, если нет — вычисляет и сохраняет.
    """
    try:
        return await cached_values(
            engine_vlad=engine_vlad,
            service_url=SERVICE_URL,
            pair=pair,
            day=day,
            date=date,
            extra_params={"type": type, "var": var},
            compute_fn=functools.partial(_compute_cpu_only, pair, day, date,
                                        calc_type=type, calc_var=var),
            node=NODE_NAME,
        )
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_values")
        return err_response(str(e))


@app.get("/compute")
async def compute_values(
    pair: int = Query(1), day: int = Query(1), date: str = Query(...),
    type: int = Query(0, ge=0, le=2), var: int = Query(0, ge=0, le=4),
):
    """
    Чистое вычисление без кеширования. Используется cache.py при построении кеша.

    Ключевое отличие от /values:
    1. Нет чтения/записи в vlad_values_cache — исключает конкуренцию за БД
    2. CPU-тяжёлая часть вычисления выполняется в thread pool через asyncio.to_thread —
       event loop остаётся отзывчивым и принимает другие запросы пока идёт вычисление.

    Возвращает результат напрямую: {"key": float, ...} или {}
    """
    try:
        rates_table = get_rates_table_name(pair, day)
        await _refresh_rates_if_needed(rates_table)
        result = await asyncio.to_thread(
            functools.partial(_compute_cpu_only, pair, day, date,
                              calc_type=type, calc_var=var)
        )
        return result if result is not None else {}
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="compute_values")
        return {"error": str(e)}


@app.post("/compute_batch")
async def compute_batch(
    dates: list[str],
    pair: int = Query(1), day: int = Query(1),
    type: int = Query(0, ge=0, le=2), var: int = Query(0, ge=0, le=4),
):
    """
    Батчевое вычисление без кеширования.
    Принимает список дат в теле запроса (JSON array of strings).
    Возвращает dict: {"2025-01-15 10:00:00": {"key": float, ...}, ...}

    Используется cache.py для массового заполнения кеша —
    один HTTP-запрос на батч вместо N отдельных.
    Каждая дата считается в thread pool через asyncio.to_thread +
    numpy-ускоренный _compute_cpu_only — event loop не блокируется.
    """
    if not dates:
        return {}

    rates_table = get_rates_table_name(pair, day)
    await _refresh_rates_if_needed(rates_table)

    async def _one(date_str: str) -> tuple[str, dict]:
        try:
            result = await asyncio.to_thread(
                functools.partial(_compute_cpu_only, pair, day, date_str,
                                  calc_type=type, calc_var=var)
            )
            return date_str, (result if result is not None else {})
        except Exception as e:
            log(f"  ⚠️ compute_batch error {date_str}: {e}", NODE_NAME, level="warning")
            return date_str, {}

    results = await asyncio.gather(*[_one(d) for d in dates])
    return dict(results)


@app.post("/patch")
async def patch_service():
    async with engine_vlad.begin() as conn:
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        row = res.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail=f"Service ID {SERVICE_ID} not found")
        old = row[0]
        new = max(old, 3)
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
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
