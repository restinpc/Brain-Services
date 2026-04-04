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

# ── NumPy-ускоренные структуры (строятся в preload, используются в _compute_cpu_only) ──
# Для каждой rates-таблицы: отсортированный массив дат, t1, ranges, ext_min, ext_max
# Для каждого ctx_key: отсортированный int64-массив unix-timestamp (секунды)
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



def _build_numpy_arrays():
    """
    Строит NumPy-массивы из загруженных dict-структур.
    Вызывается один раз после preload_all_data.
    После этого _compute_cpu_only использует только NP_RATES и NP_CTX_HIST —
    vectorized операции вместо Python-циклов с dict.get().
    """
    global NP_CTX_HIST_BUILT

    # ── rates таблицы ─────────────────────────────────────────────────────────
    for table, rates_dict in GLOBAL_RATES.items():
        if not rates_dict:
            NP_RATES[table] = None
            continue

        # Даты в секундах (int64) — совместимо с datetime через .timestamp()
        sorted_dates = sorted(rates_dict.keys())
        n = len(sorted_dates)

        dates_ns  = np.array([int(d.timestamp()) for d in sorted_dates], dtype=np.int64)
        t1_arr    = np.array([rates_dict.get(d, 0.0) for d in sorted_dates], dtype=np.float64)
        ranges_d  = GLOBAL_CANDLE_RANGES.get(table, {})
        ranges_arr = np.array([ranges_d.get(d, 0.0) for d in sorted_dates], dtype=np.float64)

        ext_min_set = GLOBAL_EXTREMUMS.get(table, {}).get("min", set())
        ext_max_set = GLOBAL_EXTREMUMS.get(table, {}).get("max", set())
        ext_min_arr = np.array([d in ext_min_set for d in sorted_dates], dtype=bool)
        ext_max_arr = np.array([d in ext_max_set for d in sorted_dates], dtype=bool)

        NP_RATES[table] = {
            "dates_ns":  dates_ns,
            "t1":        t1_arr,
            "ranges":    ranges_arr,
            "ext_min":   ext_min_arr,
            "ext_max":   ext_max_arr,
        }
        log(f"  NP {table}: {n} записей", NODE_NAME)

    # ── ctx history ──────────────────────────────────────────────────────────
    for ctx_key, dates_list in GLOBAL_MKT_CTX_HIST.items():
        if dates_list:
            NP_CTX_HIST[ctx_key] = np.array(
                [int(d.timestamp()) for d in dates_list], dtype=np.int64
            )
        else:
            NP_CTX_HIST[ctx_key] = np.empty(0, dtype=np.int64)

    NP_CTX_HIST_BUILT = True
    log(f"  NP_CTX_HIST: {len(NP_CTX_HIST)} контекстов", NODE_NAME, force=True)


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

    # ── market_history читается из engine_brain ───────────────────────────────
    try:
        col_list = ", ".join(f"`{col}`" for col in INSTRUMENT_COLUMNS.values())
        async with engine_brain.connect() as conn:
            res  = await conn.execute(text(
                f"SELECT `datetime`, {col_list} FROM vlad_market_history ORDER BY `datetime`"))
            rows = res.fetchall()
        log(f"  vlad_market_history (from engine_brain): {len(rows)} rows", NODE_NAME)
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
    # Строим NumPy-ускоренные массивы после загрузки всех данных
    try:
        _build_numpy_arrays()
    except Exception as e:
        log(f"❌ _build_numpy_arrays: {e}", NODE_NAME, level="error")

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

_BRAIN_SEM: asyncio.Semaphore | None = None


def get_brain_sem() -> asyncio.Semaphore:
    global _BRAIN_SEM
    if _BRAIN_SEM is None:
        _BRAIN_SEM = asyncio.Semaphore(8)
    return _BRAIN_SEM


async def _safe_brain_invalidate(conn) -> None:
    try:
        await conn.invalidate()
    except Exception:
        pass


async def _refresh_rates_if_needed(rates_table):
    """
    Если в RAM нет свечи за последний час — подгрузить из БД. Не чаще раз в 30 сек.
    При InternalError ("Packet sequence number wrong") инвалидирует соединение и
    делает retry — никогда не бросает исключение наверх.
    """
    now = datetime.now()
    last = _LAST_RATES_REFRESH.get(rates_table)
    if last and (now - last).total_seconds() < 30:
        return
    _LAST_RATES_REFRESH[rates_table] = now

    ram = GLOBAL_RATES.get(rates_table)
    if not ram:
        return
    max_dt = max(ram.keys())

    query = text(
        f"SELECT date, open, close, t1 "
        f"FROM `{rates_table}` WHERE date > :dt ORDER BY date"
    )

    try:
        from sqlalchemy.exc import InternalError as SAInternalError, DBAPIError
        async with get_brain_sem():
            for attempt in range(2):
                conn = await engine_brain.connect()
                try:
                    res = await conn.execute(query, {"dt": max_dt})
                    rows = res.mappings().all()
                    await conn.close()
                    n = 0
                    for r in rows:
                        dt = r["date"]
                        if r["t1"] is not None:
                            ram[dt] = float(r["t1"])
                        cl = GLOBAL_LAST_CANDLES.get(rates_table)
                        if cl is not None:
                            cl.append((dt, (r["close"] or 0) > (r["open"] or 0)))
                        n += 1
                    if n > 0:
                        log(f"  📥 Refreshed {n} candle(s) for {rates_table}", NODE_NAME)
                    return

                except (SAInternalError, DBAPIError) as e:
                    await _safe_brain_invalidate(conn)
                    if attempt == 0:
                        log(f"  ⚠️ Rates refresh InternalError, retry ({rates_table}): {e}",
                            NODE_NAME, level="warning")
                        continue
                    log(f"  ⚠️ Rates refresh retry failed, skipping ({rates_table}): {e}",
                        NODE_NAME, level="warning")
                    return

                except Exception as e:
                    try:
                        await conn.close()
                    except Exception:
                        pass
                    log(f"  ⚠️ Rates refresh error ({rates_table}): {e}", NODE_NAME, level="warning")
                    return
    except Exception as e:
        log(f"  ⚠️ Rates refresh outer error ({rates_table}): {e}", NODE_NAME, level="warning")


def _compute_cpu_only(
        pair: int, day: int, date_str: str,
        calc_type: int = 0, calc_var: int = 0,
) -> dict | None:
    """
    NumPy-матричная версия.

    Для каждого ctx_key все сдвиги обрабатываются ОДНИМ searchsorted-вызовом
    над матрицей (n_valid × n_shifts). Это устраняет ~50x лишних аллокаций
    numpy-массивов на свечу по сравнению с предыдущей версией.

    Дополнительно: сдвиги дедуплицируются — одинаковый shift из разных obs_dt
    вычисляется ровно один раз.
    """
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    if not NP_CTX_HIST_BUILT:
        return _compute_cpu_only_py(pair, day, date_str, calc_type, calc_var)

    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    delta_unit = timedelta(days=1) if day == 1 else timedelta(hours=1)
    delta_sec = int(delta_unit.total_seconds())
    target_ts = int(target_date.timestamp())

    np_rates = NP_RATES.get(rates_table)
    if np_rates is None:
        return {}

    dates_ns = np_rates["dates_ns"]
    t1_arr = np_rates["t1"]
    ranges_arr = np_rates["ranges"]
    avg_range = GLOBAL_AVG_RANGE.get(rates_table, 0.0)

    prev_candle = find_prev_candle_trend(rates_table, target_date)
    ext_arr = None
    if prev_candle and calc_type in (0, 2):
        _, is_bull = prev_candle
        ext_arr = np_rates["ext_max" if is_bull else "ext_min"]

    need_filter = calc_var in (1, 3, 4)
    use_square = calc_var in (2, 3)
    use_range = calc_var == 4

    # ── Шаг 1: собираем уникальные сдвиги по ctx_key ─────────────────────────
    # Используем dict вместо list — автоматически дедуплицирует сдвиги.
    # Структура: ctx_key → {shift: is_recurring}
    ctx_shifts: dict[tuple, dict[int, bool]] = {}

    for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1):
        dt = target_date + delta_unit * s
        if dt > target_date:
            continue
        dt_end = dt + delta_unit
        _l = bisect.bisect_left(_MKT_SORTED_DATES, dt)
        _r = bisect.bisect_left(_MKT_SORTED_DATES, dt_end)
        for _i in range(_l, _r):
            obs_dt = _MKT_SORTED_DATES[_i]
            shift = round((target_date - obs_dt) / delta_unit)
            for instr in GLOBAL_MKT_OBS_DTS.get(obs_dt, set()):
                ctx = GLOBAL_MKT_CONTEXT.get((instr, obs_dt))
                if not ctx:
                    continue
                rcd, td, md = ctx
                ctx_key = (instr, rcd, td, md)
                ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
                if ctx_info is None:
                    continue
                is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
                if not is_recurring and shift != 0:
                    continue
                if is_recurring and abs(shift) > SHIFT_WINDOW:
                    continue
                # dict.setdefault + присвоение = дедупликация без if-проверки
                ctx_shifts.setdefault(ctx_key, {})[shift] = is_recurring

    if not ctx_shifts:
        return {}

    result: dict[str, float] = {}
    len_dates = len(dates_ns)

    # ── Шаг 2: матричный расчёт для каждого ctx_key ──────────────────────────
    for ctx_key, shift_map in ctx_shifts.items():
        ctx_ts = NP_CTX_HIST.get(ctx_key)
        if ctx_ts is None or len(ctx_ts) == 0:
            continue

        # Исторические даты контекста ДО target_date
        idx = np.searchsorted(ctx_ts, target_ts, side="left")
        if idx == 0:
            continue
        valid_ts = ctx_ts[:idx]  # (n_valid,) int64
        total_hist = len(valid_ts)  # для расчёта extremum
        n_valid = total_hist

        instr, rcd, td, md = ctx_key

        # Список уникальных сдвигов
        shifts_items = list(shift_map.items())  # [(shift, is_recurring), ...]
        shifts_sec = np.fromiter(
            (s * delta_sec for s, _ in shifts_items),
            dtype=np.int64, count=len(shifts_items),
        )  # (n_shifts,)
        n_shifts = len(shifts_sec)

        # ── Матрица сдвинутых timestamps (n_valid, n_shifts) ─────────────────
        # Каждая строка: один исторический момент + все сдвиги
        t_ts_mat = valid_ts[:, np.newaxis] + shifts_sec[np.newaxis, :]
        # (n_valid, n_shifts), int64

        # Фильтр: timestamp должен быть < target
        lt_mask = t_ts_mat < target_ts  # (n_valid, n_shifts), bool

        # ── ОДИН вызов searchsorted на весь ctx_key ───────────────────────────
        flat_ts = t_ts_mat.ravel()  # (n_valid*n_shifts,)
        ri_flat = np.searchsorted(dates_ns, flat_ts, side="left")

        # Exact match + bounds
        in_bnd = ri_flat < len_dates
        exact = np.zeros(n_valid * n_shifts, dtype=bool)
        if np.any(in_bnd):
            exact[in_bnd] = dates_ns[ri_flat[in_bnd]] == flat_ts[in_bnd]

        # Итоговая маска совпадений (n_valid, n_shifts)
        hit_mat = (exact & lt_mask.ravel()).reshape(n_valid, n_shifts)
        ri_mat = ri_flat.reshape(n_valid, n_shifts)

        # ── Обрабатываем каждый столбец (один сдвиг) ─────────────────────────
        for col, (shift, is_recurring) in enumerate(shifts_items):
            col_hit = hit_mat[:, col]
            if not np.any(col_hit):
                continue

            ri = ri_mat[:, col][col_hit]  # индексы в dates_ns/t1/ranges
            rng_vals = ranges_arr[ri]
            shift_arg = shift if is_recurring else None

            # ── T1 ────────────────────────────────────────────────────────────
            if calc_type in (0, 1):
                if need_filter:
                    fm = rng_vals > avg_range
                    ri_f = ri[fm];
                    rv_f = rng_vals[fm]
                else:
                    ri_f = ri;
                    rv_f = rng_vals

                if len(ri_f):
                    if use_range:
                        t1_v = float(np.sum(rv_f - avg_range))
                    elif use_square:
                        v = t1_arr[ri_f]
                        t1_v = float(np.sum(v * np.abs(v)))
                    else:
                        t1_v = float(np.sum(t1_arr[ri_f]))

                    if t1_v:
                        wc = make_weight_code(instr, rcd, td, md, 0, shift_arg)
                        result[wc] = result.get(wc, 0.0) + t1_v

            # ── Extremum ──────────────────────────────────────────────────────
            if calc_type in (0, 2) and ext_arr is not None and total_hist:
                if need_filter:
                    fm = rng_vals > avg_range
                    ri_e = ri[fm];
                    rv_e = rng_vals[fm]
                else:
                    ri_e = ri;
                    rv_e = rng_vals

                if len(ri_e):
                    ext_hits = ext_arr[ri_e]
                    if use_range:
                        ev = float(np.sum((rv_e - avg_range) * ext_hits))
                    else:
                        ev = (
                                     float(np.count_nonzero(ext_hits)) / total_hist
                             ) * 2 - 1
                        ev *= modification

                    if ev:
                        wc = make_weight_code(instr, rcd, td, md, 1, shift_arg)
                        result[wc] = result.get(wc, 0.0) + ev

    return {k: round(v, 6) for k, v in result.items() if v}



def _compute_cpu_only_py(pair, day, date_str, calc_type=0, calc_var=0):
    """Python fallback — используется только если numpy ещё не инициализирован."""
    target_date = parse_date_string(date_str)
    if not target_date:
        return None
    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    delta_unit = timedelta(days=1) if day == 1 else timedelta(hours=1)
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
                    observations.append((instr, obs_dt, ctx,
                        round((target_date - obs_dt) / delta_unit)))
    if not observations:
        return {}
    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ranges = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)
    result = {}
    for instr, obs_dt, (rcd, td, md), shift in observations:
        ctx_key = (instr, rcd, td, md)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue
        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            continue
        all_ctx_dts = GLOBAL_MKT_CTX_HIST.get(ctx_key, [])
        idx = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts = all_ctx_dts[:idx]
        if not valid_dts:
            continue
        delta_shift = delta_unit * shift
        t_dates = [d + delta_shift for d in valid_dts if (d + delta_shift) < target_date]
        if not t_dates:
            continue
        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(instr, rcd, td, md, 0, shift if is_recurring else None)
            result[wc] = result.get(wc, 0.0) + t1_sum
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges,
                                             avg_range, modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(instr, rcd, td, md, 1, shift if is_recurring else None)
                result[wc] = result.get(wc, 0.0) + ext_val
    return {k: round(v, 6) for k, v in result.items() if v != 0}



async def calculate_pure_memory(pair, day, date_str, calc_type=0, calc_var=0):
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table  = get_rates_table_name(pair, day)
    # _refresh_rates_if_needed никогда не бросает — при ошибке просто пропускает обновление
    await _refresh_rates_if_needed(rates_table)
    modification = get_modification_factor(pair)
    delta_unit   = timedelta(days=1) if day == 1 else timedelta(hours=1)

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
                    observations.append((instr, obs_dt, ctx,
                                         round((target_date - obs_dt) / delta_unit)))

    if not observations:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
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

        all_ctx_dts = GLOBAL_MKT_CTX_HIST.get(ctx_key, [])
        idx         = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts   = all_ctx_dts[:idx]
        if not valid_dts:
            continue

        t_dates   = [d + delta_unit * shift for d in valid_dts
                     if (d + delta_unit * shift) < target_date]
        if not t_dates:
            continue
        shift_arg = shift if is_recurring else None

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

    return {k: round(v, 6) for k, v in result.items() if v != 0}


@app.get("/")
async def get_metadata():
    for t in ["vlad_market_weights", "vlad_market_context_idx", "version_microservice"]:
        try:
            async with engine_vlad.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"vlad.{t} inaccessible: {e}"}
    for t in ["vlad_market_history", "brain_rates_eur_usd", "brain_rates_btc_usd", "brain_rates_eth_usd"]:
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
        rates_table = get_rates_table_name(pair, day)
 
        async def _compute():
            # Async-часть: обновить свежие свечи если нужно
            await _refresh_rates_if_needed(rates_table)
            # CPU-часть: в thread pool — event loop НЕ блокируется
            return await asyncio.to_thread(
                functools.partial(_compute_cpu_only, pair, day, date,
                                  calc_type=type, calc_var=var)
            )
 
        return await cached_values(
            engine_vlad=engine_vlad,
            service_url  = SERVICE_URL,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = {"type": type, "var": var},
            compute_fn   = _compute,
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
