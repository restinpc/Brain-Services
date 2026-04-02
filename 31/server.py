import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
import functools
from collections import defaultdict
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

SERVICE_ID = 31
NODE_NAME  = os.getenv("NODE_NAME", "brain-market-weights-microservice")
PORT       = 8894

RATE_CHANGE_MAP = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}
TREND_MAP       = {"UNKNOWN": "X", "ABOVE": "A", "BELOW": "B", "AT": "T"}
MOMENTUM_MAP    = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

SMA_SHORT    = int(os.getenv("SMA_SHORT",    "24"))
SMA_LONG     = int(os.getenv("SMA_LONG",     "168"))
SHIFT_WINDOW = int(os.getenv("SHIFT_WINDOW", "12"))
RECURRING_MIN_COUNT = 2

THRESHOLD_BY_INSTRUMENT = {
    "EURUSD": 0.0003, "DXY": 0.0003, "BTC": 0.002,
    "ETH": 0.003, "GOLD": 0.001, "OIL": 0.002,
}
DEFAULT_THRESHOLD = 0.001

INSTRUMENT_COLUMNS = {
    "EURUSD": "EURUSD_Close", "BTC": "BTC_Close", "ETH": "ETH_Close",
    "DXY": "DXY_Close", "GOLD": "Gold_Close", "OIL": "Oil_Close",
}

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_MKT_BY_INSTR  = {}
GLOBAL_MKT_CONTEXT   = {}
GLOBAL_MKT_OBS_DTS   = defaultdict(set)
_MKT_SORTED_DATES = []
GLOBAL_MKT_CTX_HIST  = {}
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

def _compute_sma(series, idx, window):
    if idx < window - 1:
        return None
    return sum(v for _, v in series[idx - window + 1: idx + 1]) / window

def _direction_label(a, b, threshold, up="UP", down="DOWN", flat="FLAT"):
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct >  threshold: return up
    if pct < -threshold: return down
    return flat

def classify_market_observations(series, threshold):
    results = []
    for i, (dt, close) in enumerate(series):
        rcd  = "UNKNOWN" if i == 0 else _direction_label(close, series[i - 1][1], threshold)
        smal = _compute_sma(series, i, SMA_LONG)
        td   = (_direction_label(close, smal, threshold, "ABOVE", "BELOW", "AT")
                if smal else "UNKNOWN")
        smas = _compute_sma(series, i, SMA_SHORT)
        md   = _direction_label(smas, smal, threshold) if (smas and smal) else "UNKNOWN"
        results.append((dt, rcd, td, md))
    return results

def make_weight_code(instrument, rcd, td, md, mode, hour_shift=None):
    base = (f"{instrument}_{RATE_CHANGE_MAP.get(rcd,'X')}_"
            f"{TREND_MAP.get(td,'X')}_{MOMENTUM_MAP.get(md,'X')}_{mode}")
    return base if hour_shift is None else f"{base}_{hour_shift}"

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
    log("🔄 MARKET FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    GLOBAL_WEIGHT_CODES.clear(); GLOBAL_CTX_INDEX.clear()
    GLOBAL_MKT_BY_INSTR.clear(); GLOBAL_MKT_CONTEXT.clear()
    GLOBAL_MKT_OBS_DTS.clear(); GLOBAL_MKT_CTX_HIST.clear()
    GLOBAL_RATES.clear(); GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear(); GLOBAL_AVG_RANGE.clear(); GLOBAL_LAST_CANDLES.clear()

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(text("SELECT weight_code FROM vlad_market_weights"))
            GLOBAL_WEIGHT_CODES.extend(r[0] for r in res.fetchall())
            log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)
        except Exception as e:
            log(f"❌ weight_codes: {e}", NODE_NAME, level="error")

        try:
            res = await conn.execute(text(
                "SELECT instrument, rate_change_dir, trend_dir, momentum_dir, occurrence_count "
                "FROM vlad_market_context_idx"))
            for r in res.mappings().all():
                key = (r["instrument"], r["rate_change_dir"], r["trend_dir"], r["momentum_dir"])
                GLOBAL_CTX_INDEX[key] = {"occurrence_count": r["occurrence_count"] or 0}
            log(f"  ctx_index: {len(GLOBAL_CTX_INDEX)}", NODE_NAME)
        except Exception as e:
            log(f"❌ ctx_index: {e}", NODE_NAME, level="error")

        try:
            col_list = ", ".join(f"`{col}`" for col in INSTRUMENT_COLUMNS.values())
            res  = await conn.execute(text(
                f"SELECT `datetime`, {col_list} FROM vlad_market_history ORDER BY `datetime`"))
            rows = res.fetchall()
            log(f"  vlad_market_history (from engine_vlad): {len(rows)} rows", NODE_NAME)
            if rows:
                log(f"    date range: {rows[0][0]} → {rows[-1][0]}", NODE_NAME)
            by_instr    = {instr: [] for instr in INSTRUMENT_COLUMNS}
            instruments = list(INSTRUMENT_COLUMNS.keys())
            for row in rows:
                dt = row[0]
                for i, instr in enumerate(instruments):
                    val = row[i + 1]
                    if val is not None:
                        by_instr[instr].append((dt, float(val)))
            for instr, series in by_instr.items():
                log(f"    {instr}: {len(series)} observations" +
                    (f", last={series[-1][0]}" if series else ""), NODE_NAME)
                if not series:
                    continue
                threshold = THRESHOLD_BY_INSTRUMENT.get(instr, DEFAULT_THRESHOLD)
                GLOBAL_MKT_BY_INSTR[instr] = series
                for dt, rcd, td, md in classify_market_observations(series, threshold):
                    GLOBAL_MKT_CONTEXT[(instr, dt)] = (rcd, td, md)
                    GLOBAL_MKT_OBS_DTS[dt].add(instr)
                    GLOBAL_MKT_CTX_HIST.setdefault((instr, rcd, td, md), []).append(dt)
            for key in GLOBAL_MKT_CTX_HIST:
                GLOBAL_MKT_CTX_HIST[key].sort()
            log(f"  instruments: {len(GLOBAL_MKT_BY_INSTR)}, contexts: {len(GLOBAL_MKT_CONTEXT)}",
                NODE_NAME)
        except Exception as e:
            log(f"❌ market_history: {e}", NODE_NAME, level="error")

    # ── bisect: отсортированный список дат market ──
    _MKT_SORTED_DATES[:] = sorted(GLOBAL_MKT_OBS_DTS.keys())

    for table in ["brain_rates_eur_usd", "brain_rates_eur_usd_day",
                  "brain_rates_btc_usd", "brain_rates_btc_usd_day",
                  "brain_rates_eth_usd", "brain_rates_eth_usd_day"]:
        GLOBAL_RATES[table] = {}; GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_CANDLE_RANGES[table] = {}; GLOBAL_AVG_RANGE[table] = 0.0
        GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}
        try:
            async with engine_brain.connect() as conn:
                res    = await conn.execute(text(
                    f"SELECT date, open, close, `max`, `min`, t1 FROM `{table}`"))
                rows_b = sorted(res.mappings().all(), key=lambda x: x["date"])
                ranges = []
                for r in rows_b:
                    dt = r["date"]
                    if r["t1"] is not None:
                        GLOBAL_RATES[table][dt] = float(r["t1"])
                    GLOBAL_LAST_CANDLES[table].append((dt, r["close"] > r["open"]))
                    rng = float(r["max"] or 0) - float(r["min"] or 0)
                    GLOBAL_CANDLE_RANGES[table][dt] = rng
                    ranges.append(rng)
                GLOBAL_AVG_RANGE[table] = sum(ranges) / len(ranges) if ranges else 0.0
                interval = "1 DAY" if table.endswith("_day") else "1 HOUR"
                for typ in ("min", "max"):
                    op = ">" if typ == "max" else "<"
                    q  = f"""SELECT t1.date FROM `{table}` t1
                        JOIN `{table}` t_prev ON t_prev.date = t1.date - INTERVAL {interval}
                        JOIN `{table}` t_next ON t_next.date = t1.date + INTERVAL {interval}
                        WHERE t1.`{typ}` {op} t_prev.`{typ}` AND t1.`{typ}` {op} t_next.`{typ}`"""
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
    log("✅ MARKET FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f"❌ Background reload error: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "market_background_reload")


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
                log(f"  📥 Refreshed {n} candle(s) for {rates_table}", NODE_NAME)
    except Exception as e:
        log(f"  ⚠️ Rates refresh error ({rates_table}): {e}", NODE_NAME, level="warning")


def _compute_cpu_only(
    pair: int, day: int, date_str: str,
    calc_type: int = 0, calc_var: int = 0,
) -> dict | None:
    """
    Оптимизированная синхронная CPU-часть вычисления весов.
    Запускается через asyncio.to_thread() из /compute.

    Оптимизации vs оригинальный calculate_pure_memory:
    1. Без DIAG-логов (70k+ print вызовов на 10k свечей)
    2. bisect для отсечения valid_dts — early exit вместо полного перебора
    3. delta_shift вычисляется один раз вне внутреннего цикла
    4. t_dates как список не создаётся — значения суммируются напрямую
    5. Встроенный fast-path: skip нулевых t1 без вызовов функций
    """
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    delta_unit   = timedelta(days=1) if day == 1 else timedelta(hours=1)

    # Собираем наблюдения: все (инструмент, дата, контекст, сдвиг) <= target_date
    observations = []
    for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1):
        dt = target_date + delta_unit * s
        if dt > target_date:
            continue
        dt_end = dt + delta_unit
        _l = bisect.bisect_left(_MKT_SORTED_DATES, dt)
        _r = bisect.bisect_left(_MKT_SORTED_DATES, dt_end)
        for _i in range(_l, _r):
            obs_dt = _MKT_SORTED_DATES[_i]
            for instr in GLOBAL_MKT_OBS_DTS.get(obs_dt, set()):
                ctx = GLOBAL_MKT_CONTEXT.get((instr, obs_dt))
                if ctx:
                    observations.append(
                        (instr, obs_dt, ctx, round((target_date - obs_dt) / delta_unit))
                    )

    if not observations:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    # Предвычисляем флаги для calc_var (одинаковы для всех итераций)
    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4

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

        all_ctx_dts = GLOBAL_MKT_CTX_HIST.get(ctx_key, [])
        if not all_ctx_dts:
            continue

        # bisect для поиска верхней границы (строго < target_date)
        # valid_dts = all_ctx_dts[:idx] — отсортированы, поэтому используем срез напрямую
        idx = bisect.bisect_left(all_ctx_dts, target_date)
        if idx == 0:
            continue

        # ── Оптимизация: delta_shift вычисляем один раз ────────────────────────
        delta_shift = delta_unit * shift

        # Верхняя граница для d: d + delta_shift < target_date → d < target_date - delta_shift
        cutoff_d = target_date - delta_shift

        # ── T1 сумма без создания промежуточного списка t_dates ───────────────
        if calc_type in (0, 1):
            t1_total   = 0.0
            t1_count   = 0
            for d in all_ctx_dts[:idx]:
                if d >= cutoff_d:
                    break  # список отсортирован → все последующие тоже >= cutoff_d
                t = d + delta_shift
                rng = ram_ranges.get(t, 0.0)
                if need_filter and rng <= avg_range:
                    continue
                if use_range:
                    t1_total += rng - avg_range
                else:
                    t1 = ram_rates.get(t, 0.0)
                    if t1 != 0.0:
                        t1_total += t1 * abs(t1) if use_square else t1
                t1_count += 1

            if t1_count > 0:
                wc = make_weight_code(instr, rcd, td, md, 0, shift if is_recurring else None)
                result[wc] = result.get(wc, 0.0) + t1_total

        # ── Extremum без создания промежуточного pool-списка ──────────────────
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]

            pool_total = 0
            pool_in_ext = 0
            total_hist_count = idx  # len(all_ctx_dts[:idx])

            for d in all_ctx_dts[:idx]:
                if d >= cutoff_d:
                    break
                t = d + delta_shift
                rng = ram_ranges.get(t, 0.0)
                if need_filter and rng <= avg_range:
                    continue
                if use_range:
                    if t in ext_set:
                        pool_in_ext += 1
                        pool_total  += rng - avg_range
                else:
                    pool_total += 1
                    if t in ext_set:
                        pool_in_ext += 1

            if pool_total > 0 and total_hist_count > 0:
                if use_range:
                    ext_val = pool_in_ext  # sum of (rng - avg_range) for ext_set dates
                else:
                    ext_val = ((pool_in_ext / total_hist_count) * 2 - 1) * modification
                if ext_val != 0:
                    wc = make_weight_code(instr, rcd, td, md, 1, shift if is_recurring else None)
                    result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}



async def calculate_pure_memory(pair, day, date_str, calc_type=0, calc_var=0):
    target_date = parse_date_string(date_str)
    if not target_date:
        log(f"  🔍 DIAG: parse_date_string failed for '{date_str}'", NODE_NAME)
        return None

    rates_table  = get_rates_table_name(pair, day)
    await _refresh_rates_if_needed(rates_table)
    modification = get_modification_factor(pair)
    delta_unit   = timedelta(days=1) if day == 1 else timedelta(hours=1)
    check_dts    = [target_date + delta_unit * s
                    for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1)]

    log(f"  🔍 DIAG: target={target_date} pair={pair} day={day} type={calc_type} var={calc_var}", NODE_NAME)
    log(f"  🔍 DIAG: rates_table={rates_table} rates_keys={len(GLOBAL_RATES.get(rates_table, {}))}", NODE_NAME)
    log(f"  🔍 DIAG: _MKT_SORTED_DATES len={len(_MKT_SORTED_DATES)}"
        + (f" range={_MKT_SORTED_DATES[0]}→{_MKT_SORTED_DATES[-1]}" if _MKT_SORTED_DATES else " EMPTY"),
        NODE_NAME)
    log(f"  🔍 DIAG: check_dts range={check_dts[0]}→{check_dts[-1]} (after filter: ≤{target_date})", NODE_NAME)

    observations = []
    _diag_checked = 0
    _diag_found_in_sorted = 0
    _diag_found_instr = 0
    _diag_found_ctx = 0
    for dt in check_dts:
        if dt > target_date:
            continue
        _diag_checked += 1
        dt_end = dt + delta_unit
        _l = bisect.bisect_left(_MKT_SORTED_DATES, dt)
        _r = bisect.bisect_left(_MKT_SORTED_DATES, dt_end)
        _diag_found_in_sorted += (_r - _l)
        for _i in range(_l, _r):
            obs_dt = _MKT_SORTED_DATES[_i]
            for instr in GLOBAL_MKT_OBS_DTS.get(obs_dt, set()):
                _diag_found_instr += 1
                ctx = GLOBAL_MKT_CONTEXT.get((instr, obs_dt))
                if ctx:
                    _diag_found_ctx += 1
                    observations.append((instr, obs_dt, ctx, round((target_date - obs_dt) / delta_unit)))

    log(f"  🔍 DIAG: checked={_diag_checked} bisect_hits={_diag_found_in_sorted} "
        f"instr_hits={_diag_found_instr} ctx_hits={_diag_found_ctx} observations={len(observations)}",
        NODE_NAME)

    if not observations:
        log(f"  🔍 DIAG: EMPTY — no observations found → return {{}}", NODE_NAME)
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    log(f"  🔍 DIAG: ram_rates={len(ram_rates)} ram_ranges={len(ram_ranges)} "
        f"avg_range={avg_range:.6f} ext_min={len(ram_ext.get('min',set()))} "
        f"ext_max={len(ram_ext.get('max',set()))} prev_candle={'YES' if prev_candle else 'NO'}",
        NODE_NAME)

    result = {}
    _diag_no_ctx = 0
    _diag_not_recurring = 0
    _diag_shift_out = 0
    _diag_no_valid = 0
    _diag_no_tdates = 0
    _diag_computed = 0
    for instr, obs_dt, (rcd, td, md), shift in observations:
        ctx_key  = (instr, rcd, td, md)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            _diag_no_ctx += 1
            continue
        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
        if not is_recurring and shift != 0:
            _diag_not_recurring += 1
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            _diag_shift_out += 1
            continue

        all_ctx_dts = GLOBAL_MKT_CTX_HIST.get(ctx_key, [])
        idx         = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts   = all_ctx_dts[:idx]
        if not valid_dts:
            _diag_no_valid += 1
            continue

        t_dates   = [d + delta_unit * shift for d in valid_dts
                     if (d + delta_unit * shift) < target_date]
        if not t_dates:
            _diag_no_tdates += 1
            continue
        shift_arg = shift if is_recurring else None
        _diag_computed += 1

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc     = make_weight_code(instr, rcd, td, md, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            ext_val    = compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges,
                                                avg_range, modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(instr, rcd, td, md, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    log(f"  🔍 DIAG: no_ctx_idx={_diag_no_ctx} not_recurring={_diag_not_recurring} "
        f"shift_out={_diag_shift_out} no_valid_history={_diag_no_valid} "
        f"no_tdates={_diag_no_tdates} computed={_diag_computed}",
        NODE_NAME)

    final = {k: round(v, 6) for k, v in result.items() if v != 0}
    log(f"  🔍 DIAG: result_keys_before_filter={len(result)} result_keys_final={len(final)}", NODE_NAME)
    return final


@app.get("/")
async def get_metadata():
    for t in ["vlad_market_weights", "vlad_market_context_idx",
              "vlad_market_history", "version_microservice"]:
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
        "name": NODE_NAME, "text": "Market context weights (rate_change/trend/momentum)",
        "weight_code_format": "{instrument}_{rcd}_{td}_{md}_{mode}[_{hour_shift}]",
        "metadata": {
            "ctx_index_rows":    len(GLOBAL_CTX_INDEX),
            "weight_codes":      len(GLOBAL_WEIGHT_CODES),
            "instruments_loaded": list(GLOBAL_MKT_BY_INSTR.keys()),
            "last_reload":       LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
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
        # Async часть: обновить свечи если нужно (с 30-сек кешем — редко бьёт в БД)
        await _refresh_rates_if_needed(rates_table)
        # CPU часть: в thread pool чтобы не блокировать event loop
        result = await asyncio.to_thread(
            functools.partial(_compute_cpu_only, pair, day, date,
                              calc_type=type, calc_var=var)
        )
        return result if result is not None else {}
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="compute_values")
        return {"error": str(e)}

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
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
