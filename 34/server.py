"""
server.py — brain-market-tech-microservice (port 8897, SERVICE_ID=34)
Папка: 34/
Режим запуска: MODE=dev | MODE=prod

5 измерений: rate_change / trend / momentum / vol_zone / bb_zone
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
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

SERVICE_ID = 34
NODE_NAME  = os.getenv("NODE_NAME", "brain-market-tech-microservice")
PORT       = 8897

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

# ── Параметры ─────────────────────────────────────────────────────────────────
SMA_SHORT    = int(os.getenv("SMA_SHORT",    "24"))
SMA_LONG     = int(os.getenv("SMA_LONG",     "168"))
BB_PERIOD    = int(os.getenv("BB_PERIOD",    "20"))
VOL_PERIOD   = int(os.getenv("VOL_PERIOD",   "24"))
SHIFT_WINDOW = int(os.getenv("SHIFT_WINDOW", "12"))
RECURRING_MIN_COUNT = 2

THRESHOLD_BY_INSTRUMENT = {
    "EURUSD": 0.0003, "DXY": 0.0003, "BTC": 0.002,
    "ETH": 0.003, "GOLD": 0.001, "OIL": 0.002,
}
DEFAULT_THRESHOLD = 0.001

INSTRUMENT_COLUMNS = {
    "EURUSD": ("EURUSD_Close", "EURUSD_Volume"),
    "BTC":    ("BTC_Close",    "BTC_Volume"),
    "ETH":    ("ETH_Close",    "ETH_Volume"),
    "DXY":    ("DXY_Close",    "DXY_Volume"),
    "GOLD":   ("Gold_Close",   "Gold_Volume"),
    "OIL":    ("Oil_Close",    "Oil_Volume"),
}

# ── Числовые карты (совпадают с market_weights.py) ────────────────────────────
INSTRUMENT_MAP = {
    "EURUSD": 1, "BTC": 3, "ETH": 4,
    "DXY": 5, "GOLD": 6, "OIL": 7,
}
RATE_CHANGE_MAP = {"UNKNOWN": 0, "UP": 1, "DOWN": 2, "FLAT": 3}
TREND_MAP       = {"UNKNOWN": 0, "ABOVE": 1, "BELOW": 2, "AT": 3}
MOMENTUM_MAP    = {"UNKNOWN": 0, "UP": 1, "DOWN": 2, "FLAT": 3}
VOL_MAP         = {"UNKNOWN": 0, "HIGH": 1, "LOW": 2}
BB_MAP          = {"UNKNOWN": 0, "UPPER": 1, "MID": 2, "LOWER": 3}

# ── Глобальные данные в RAM ───────────────────────────────────────────────────
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


# ── Вспомогательные функции ───────────────────────────────────────────────────

def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    return {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + (
        "_day" if day_flag == 1 else "")


def get_modification_factor(pair_id: int) -> float:
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%Y-%d-%m %H:%M:%S"):
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


# ── Классификаторы ────────────────────────────────────────────────────────────

def _direction_label(a, b, threshold, up="UP", down="DOWN", flat="FLAT") -> str:
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct >  threshold: return up
    if pct < -threshold: return down
    return flat


def _compute_sma_series(series: list, idx: int, window: int):
    if idx < window - 1:
        return None
    return sum(v for _, v in series[idx - window + 1: idx + 1]) / window


def _compute_sma_raw(values: list, idx: int, window: int):
    if idx < window - 1:
        return None
    return sum(values[idx - window + 1: idx + 1]) / window


def _compute_std_raw(values: list, idx: int, window: int):
    if idx < window - 1:
        return None
    sl  = values[idx - window + 1: idx + 1]
    avg = sum(sl) / window
    return (sum((x - avg) ** 2 for x in sl) / window) ** 0.5


def _classify_vol(volume, vol_ma) -> str:
    if volume is None or vol_ma is None or vol_ma == 0:
        return "UNKNOWN"
    return "HIGH" if float(volume) > float(vol_ma) else "LOW"


def _classify_bb(close, bb_mid, bb_std) -> str:
    if None in (close, bb_mid, bb_std) or bb_std == 0:
        return "UNKNOWN"
    upper = bb_mid + 2 * bb_std
    lower = bb_mid - 2 * bb_std
    width = upper - lower
    if width == 0:
        return "UNKNOWN"
    pct_b = (close - lower) / width
    if pct_b >= 0.8: return "UPPER"
    if pct_b <= 0.2: return "LOWER"
    return "MID"


def classify_market_observations(close_series: list,
                                  volume_series: list,
                                  threshold: float) -> list:
    """Возвращает list[(dt, rcd, td, md, vol_zone, bb_zone)]."""
    closes  = [v for _, v in close_series]
    results = []
    for i, (dt, close) in enumerate(close_series):
        rcd  = ("UNKNOWN" if i == 0
                else _direction_label(close, close_series[i-1][1], threshold))
        smal = _compute_sma_series(close_series, i, SMA_LONG)
        td   = (_direction_label(close, smal, threshold, "ABOVE", "BELOW", "AT")
                if smal else "UNKNOWN")
        smas = _compute_sma_series(close_series, i, SMA_SHORT)
        md   = (_direction_label(smas, smal, threshold)
                if (smas and smal) else "UNKNOWN")
        vol_ma   = _compute_sma_raw(volume_series, i, VOL_PERIOD)
        vol_zone = _classify_vol(volume_series[i], vol_ma)
        bb_mid   = _compute_sma_raw(closes, i, BB_PERIOD)
        bb_std   = _compute_std_raw(closes, i, BB_PERIOD)
        bb_zone  = _classify_bb(close, bb_mid, bb_std)
        results.append((dt, rcd, td, md, vol_zone, bb_zone))
    return results


# ── Числовой weight_code ──────────────────────────────────────────────────────

def make_weight_code(instrument: str, rcd: str, td: str, md: str,
                     vol: str, bb: str,
                     mode: int, hour_shift: int | None = None) -> str:
    base = (f"{INSTRUMENT_MAP.get(instrument, 9)}_"
            f"{RATE_CHANGE_MAP.get(rcd, 0)}_"
            f"{TREND_MAP.get(td, 0)}_"
            f"{MOMENTUM_MAP.get(md, 0)}_"
            f"{VOL_MAP.get(vol, 0)}_"
            f"{BB_MAP.get(bb, 0)}_"
            f"{mode}")
    return base if hour_shift is None else f"{base}_{hour_shift}"


# ── T1 / Extremum ─────────────────────────────────────────────────────────────

def compute_t1_value(t_dates, calc_var, ram_rates, candle_ranges, avg_range) -> float:
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


def compute_extremum_value(t_dates, calc_var, ext_set, candle_ranges,
                            avg_range, modification, total_hist):
    need_filter = calc_var in (1, 3, 4)
    use_range   = calc_var == 4
    pool = ([d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range]
            if need_filter else t_dates)
    if not pool:
        return None
    if use_range:
        val = sum(candle_ranges.get(d, 0.0) - avg_range for d in pool if d in ext_set)
        return val if val != 0 else None
    if total_hist == 0:
        return None
    val = ((sum(1 for d in pool if d in ext_set) / total_hist) * 2 - 1) * modification
    return val if val != 0 else None


# ── Preload ───────────────────────────────────────────────────────────────────

async def preload_all_data():
    global SERVICE_URL, LAST_RELOAD_TIME
    log("🔄 MARKET-TECH FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    GLOBAL_WEIGHT_CODES.clear();  GLOBAL_CTX_INDEX.clear()
    GLOBAL_MKT_BY_INSTR.clear();  GLOBAL_MKT_CONTEXT.clear()
    GLOBAL_MKT_OBS_DTS.clear();   GLOBAL_MKT_CTX_HIST.clear()
    GLOBAL_RATES.clear();          GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear();  GLOBAL_AVG_RANGE.clear()
    GLOBAL_LAST_CANDLES.clear()

    # ── vlad DB: weight_codes + ctx_index ────────────────────────────────────
    async with engine_vlad.connect() as conn:

        try:
            res = await conn.execute(
                text("SELECT weight_code FROM vlad_market_tech_weights"))
            GLOBAL_WEIGHT_CODES.extend(r[0] for r in res.fetchall())
            log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)
        except Exception as e:
            log(f"❌ weight_codes: {e}", NODE_NAME, level="error")

        try:
            res = await conn.execute(text("""
                SELECT instrument, rate_change_dir, trend_dir, momentum_dir,
                       vol_zone, bb_zone, occurrence_count
                FROM vlad_market_tech_context_idx
            """))
            for r in res.mappings().all():
                key = (r["instrument"], r["rate_change_dir"], r["trend_dir"],
                       r["momentum_dir"], r["vol_zone"], r["bb_zone"])
                GLOBAL_CTX_INDEX[key] = {"occurrence_count": r["occurrence_count"] or 0}
            log(f"  ctx_index (5D): {len(GLOBAL_CTX_INDEX)}", NODE_NAME)
        except Exception as e:
            log(f"❌ ctx_index: {e}", NODE_NAME, level="error")

        # ── vlad_market_history из базы vlad (исправлено) ────────────────────────
        try:
            col_parts  = []
            instruments = list(INSTRUMENT_COLUMNS.keys())
            for instr in instruments:
                close_col, vol_col = INSTRUMENT_COLUMNS[instr]
                col_parts.append(f"`{close_col}`")
                col_parts.append(f"`{vol_col}`")

            res  = await conn.execute(text(
                f"SELECT `datetime`, {', '.join(col_parts)} "
                f"FROM vlad_market_history ORDER BY `datetime`"))
            rows = res.fetchall()
            log(f"  vlad_market_history (from vlad DB): {len(rows)} rows", NODE_NAME)

            by_instr_close  = {instr: [] for instr in instruments}
            by_instr_volume = {instr: [] for instr in instruments}

            for row in rows:
                dt = row[0]
                for i, instr in enumerate(instruments):
                    close_val = row[1 + i * 2]
                    vol_val   = row[2 + i * 2]
                    if close_val is not None:
                        by_instr_close[instr].append((dt, float(close_val)))
                        by_instr_volume[instr].append(
                            float(vol_val) if vol_val is not None else None)

            for instr in instruments:
                close_series  = by_instr_close[instr]
                volume_series = by_instr_volume[instr]
                if not close_series:
                    continue
                threshold = THRESHOLD_BY_INSTRUMENT.get(instr, DEFAULT_THRESHOLD)
                GLOBAL_MKT_BY_INSTR[instr] = close_series

                for dt, rcd, td, md, vol_zone, bb_zone in classify_market_observations(
                        close_series, volume_series, threshold):
                    ctx_tuple = (rcd, td, md, vol_zone, bb_zone)
                    GLOBAL_MKT_CONTEXT[(instr, dt)] = ctx_tuple
                    GLOBAL_MKT_OBS_DTS[dt].add(instr)
                    key = (instr, rcd, td, md, vol_zone, bb_zone)
                    GLOBAL_MKT_CTX_HIST.setdefault(key, []).append(dt)

            for key in GLOBAL_MKT_CTX_HIST:
                GLOBAL_MKT_CTX_HIST[key].sort()

            total_obs = sum(len(v) for v in GLOBAL_MKT_CTX_HIST.values())
            log(f"  instruments: {len(GLOBAL_MKT_BY_INSTR)}, "
                f"contexts: {len(GLOBAL_MKT_CONTEXT)}, obs: {total_obs}", NODE_NAME)
        except Exception as e:
            log(f"❌ vlad_market_history: {e}", NODE_NAME, level="error")

        # ── bisect: отсортированный список дат market ──
        _MKT_SORTED_DATES[:] = sorted(GLOBAL_MKT_OBS_DTS.keys())

    # ── brain DB: brain_rates_* (остается без изменений) ────────────────────────
    async with engine_brain.connect() as conn:
        for table in ["brain_rates_eur_usd", "brain_rates_eur_usd_day",
                      "brain_rates_btc_usd", "brain_rates_btc_usd_day",
                      "brain_rates_eth_usd", "brain_rates_eth_usd_day"]:
            GLOBAL_RATES[table]         = {}
            GLOBAL_LAST_CANDLES[table]  = []
            GLOBAL_CANDLE_RANGES[table] = {}
            GLOBAL_AVG_RANGE[table]     = 0.0
            GLOBAL_EXTREMUMS[table]     = {"min": set(), "max": set()}
            try:
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
                    q  = f"""
                        SELECT t1.date FROM `{table}` t1
                        JOIN `{table}` t_prev ON t_prev.date = t1.date - INTERVAL {interval}
                        JOIN `{table}` t_next ON t_next.date = t1.date + INTERVAL {interval}
                        WHERE t1.`{typ}` {op} t_prev.`{typ}`
                          AND t1.`{typ}` {op} t_next.`{typ}`"""
                    res_ext = await conn.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {
                        r["date"] for r in res_ext.mappings().all()}

                log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)
            except Exception as e:
                log(f"❌ {table}: {e}", NODE_NAME, level="error")

    try:
        SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
    except Exception as e:
        log(f"⚠️  load_service_url skipped: {e}", NODE_NAME, level="warn")

    try:
        await ensure_cache_table(engine_vlad)
    except Exception as e:
        log(f"⚠️  ensure_cache_table skipped: {e}", NODE_NAME, level="warn")

    LAST_RELOAD_TIME = datetime.now()
    log("✅ MARKET-TECH FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f"❌ Background reload error: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "market_tech_background_reload")


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

async def calculate_pure_memory(pair: int, day: int, date_str: str,
                                 calc_type: int = 0, calc_var: int = 0) -> dict | None:
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    rates_table  = get_rates_table_name(pair, day)
    await _refresh_rates_if_needed(rates_table)
    modification = get_modification_factor(pair)
    delta_unit   = timedelta(days=1) if day == 1 else timedelta(hours=1)
    check_dts    = [target_date + delta_unit * s
                    for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1)]

    observations = []
    for dt in check_dts:
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
                    shift = round((target_date - obs_dt) / delta_unit)
                    observations.append((instr, obs_dt, ctx, shift))

    if not observations:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}
    for instr, obs_dt, (rcd, td, md, vol_zone, bb_zone), shift in observations:
        ctx_key  = (instr, rcd, td, md, vol_zone, bb_zone)
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
        shift_arg = shift if is_recurring else None

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc     = make_weight_code(instr, rcd, td, md, vol_zone, bb_zone, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            ext_val    = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges,
                avg_range, modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(instr, rcd, td, md, vol_zone, bb_zone, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Endpoints (без изменений) ─────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    for t in ["vlad_market_tech_weights", "vlad_market_tech_context_idx",
              "version_microservice"]:
        try:
            async with engine_vlad.connect() as conn:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"vlad.{t} inaccessible: {e}"}
    for t in ["vlad_market_history", "brain_rates_eur_usd",
              "brain_rates_btc_usd", "brain_rates_eth_usd"]:
        try:
            # Проверяем vlad_market_history в базе vlad
            if t == "vlad_market_history":
                async with engine_vlad.connect() as conn:
                    await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            else:
                async with engine_brain.connect() as conn:
                    await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
        except Exception as e:
            return {"status": "error", "error": f"{'vlad' if t=='vlad_market_history' else 'brain'}.{t} inaccessible: {e}"}
    async with engine_vlad.connect() as conn:
        res     = await conn.execute(text(
            "SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        version = (res.fetchone() or [0])[0]
    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "mode":    MODE,
        "name":    NODE_NAME,
        "text":    "Market tech weights 5D (rate_change/trend/momentum/vol/bb)",
        "weight_code_format": "{instr_id}_{rcd}_{td}_{md}_{vol}_{bb}_{mode}[_{shift}]",
        "dimensions": {
            "instrument":  "1=EURUSD 3=BTC 4=ETH 5=DXY 6=GOLD 7=OIL",
            "rate_change": "0=UNKNOWN 1=UP 2=DOWN 3=FLAT",
            "trend":       "0=UNKNOWN 1=ABOVE 2=BELOW 3=AT",
            "momentum":    "0=UNKNOWN 1=UP 2=DOWN 3=FLAT",
            "vol_zone":    "0=UNKNOWN 1=HIGH 2=LOW",
            "bb_zone":     "0=UNKNOWN 1=UPPER 2=MID 3=LOWER",
            "mode":        "0=T1 1=Extremum",
        },
        "metadata": {
            "ctx_index_rows":     len(GLOBAL_CTX_INDEX),
            "weight_codes":       len(GLOBAL_WEIGHT_CODES),
            "instruments_loaded": list(GLOBAL_MKT_BY_INSTR.keys()),
            "last_reload":        LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
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
    pair: int = Query(1,  description="1=EURUSD 3=BTC 4=ETH"),
    day:  int = Query(1,  description="1=дневные 0=часовые"),
    date: str = Query(..., description="YYYY-MM-DD [HH:MM:SS]"),
    type: int = Query(0, ge=0, le=2,
                      description="0=T1+Ext 1=T1 only 2=Ext only"),
    var:  int = Query(0, ge=0, le=4,
                      description="0=raw 1=vol_filter 2=squared 3=flt+sq 4=range"),
):
    try:
        return await cached_values(
            engine_vlad  = engine_vlad,
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


@app.post("/patch")
async def patch_service():
    async with engine_vlad.begin() as conn:
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID})
        row = res.fetchone()
        if not row:
            raise HTTPException(status_code=500,
                                detail=f"Service ID {SERVICE_ID} not found")
        old = row[0]
        new = max(old, 1)
        if new != old:
            await conn.execute(
                text("UPDATE version_microservice SET version = :v "
                     "WHERE microservice_id = :id"),
                {"v": new, "id": SERVICE_ID})
    return {"status": "ok", "from_version": old, "to_version": new}


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio as _asyncio
    async def _get_workers():
        try:
            return await resolve_workers(engine_super, SERVICE_ID, default=1)
        except Exception as _e:
            log(f"⚠️  resolve_workers unavailable ({_e}), using 1 worker", NODE_NAME, force=True)
            return 1
    _workers = _asyncio.run(_get_workers())
    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=PORT,
                    reload=False, workers=_workers)
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
