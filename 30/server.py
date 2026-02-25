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

# ── Словари кодировки ─────────────────────────────────────────
RATE_CHANGE_MAP = {
    "UNKNOWN": "X",
    "UP": "U",
    "DOWN": "D",
    "FLAT": "F",
}

TREND_MAP = {
    "UNKNOWN": "X",
    "ABOVE": "A",
    "BELOW": "B",
    "AT": "T",
}

MOMENTUM_MAP = {
    "UNKNOWN": "X",
    "UP": "U",
    "DOWN": "D",
    "FLAT": "F",
}

# ── Трассировка ошибок ────────────────────────────────────────────────────────
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "brain-ecb-weights-microservice")
EMAIL     = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "ecb_rate_server.py"):
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
        print(f"⚠️ Failed: {e}")


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

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_ECB_BY_CCY    = {}                   # currency → [(date, rate), ...]
GLOBAL_ECB_CONTEXT   = {}                   # (currency, date) → (rcd, td, md)
GLOBAL_ECB_OBS_DATES = defaultdict(set)     # date → {currencies}
GLOBAL_ECB_CTX_HIST  = {}                   # (ccy, rcd, td, md) → [date, ...] sorted

GLOBAL_CTX_INDEX     = {}                   # (ccy, rcd, td, md) → {occurrence_count}
GLOBAL_WEIGHT_CODES  = []

GLOBAL_RATES         = {}                   # table → {datetime: t1}
GLOBAL_EXTREMUMS     = {}                   # table → {min: set, max: set}
GLOBAL_CANDLE_RANGES = {}                   # table → {datetime: range}
GLOBAL_AVG_RANGE     = {}                   # table → float
GLOBAL_LAST_CANDLES  = {}                   # table → [(datetime, is_bull), ...]

LAST_RELOAD_TIME     = None
RECURRING_MIN_COUNT  = 2
SMA_SHORT            = 5
SMA_LONG             = 20
THRESHOLD_PCT        = 0.0003


# ── Утилиты ──────────────────────────────────────────────────────────────────

def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    suffix = "_day" if day_flag == 1 else ""
    return {1: "brain_rates_eur_usd",
            3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + suffix


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


# ── Контекст ECB-курсов ──────────────────────────────────────────────────────

def _sma(rates_sorted, idx, window):
    """Скользящая средняя по окну window до idx включительно."""
    if idx < window - 1:
        return None
    return sum(r for _, r in rates_sorted[idx - window + 1: idx + 1]) / window


def _dir(a, b, up="UP", down="DOWN", flat="FLAT"):
    """Направление: a vs b с порогом THRESHOLD_PCT."""
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct > THRESHOLD_PCT:
        return up
    if pct < -THRESHOLD_PCT:
        return down
    return flat


def classify_observations(rates_sorted):
    """
    Для одного currency возвращает [(date, rcd, td, md), ...].
    rcd = дневное изменение, td = цена vs SMA20, md = SMA5 vs SMA20.
    """
    results = []
    for i, (dt, rate) in enumerate(rates_sorted):
        rcd = "UNKNOWN" if i == 0 else _dir(rate, rates_sorted[i - 1][1])
        sma_l = _sma(rates_sorted, i, SMA_LONG)
        td = "UNKNOWN" if sma_l is None else _dir(rate, sma_l, "ABOVE", "BELOW", "AT")
        sma_s = _sma(rates_sorted, i, SMA_SHORT)
        md = ("UNKNOWN" if (sma_s is None or sma_l is None)
              else _dir(sma_s, sma_l))
        results.append((dt, rcd, td, md))
    return results


# ── Weight code ───────────────────────────────────────────────────────────────

def make_weight_code(ccy, rcd, td, md, mode, day_shift=None):
    """
    Генерирует компактный weight_code с однобуквенными кодами направлений.
    Пример: ("KRW", "DOWN", "ABOVE", "UP", 0, 12) → "KRW_D_A_U_0_12"
    """
    rcd_c = RATE_CHANGE_MAP.get(rcd, "X")
    td_c  = TREND_MAP.get(td, "X")
    md_c  = MOMENTUM_MAP.get(md, "X")
    base = f"{ccy}_{rcd_c}_{td_c}_{md_c}_{mode}"
    return base if day_shift is None else f"{base}_{day_shift}"


# ── Вычисление T1 / Extremum ─────────────────────────────────────────────────

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


def find_prev_candle_trend(table, target_date):
    candles = GLOBAL_LAST_CANDLES.get(table, [])
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None


# ── Предзагрузка всех данных в RAM ───────────────────────────────────────────

async def preload_all_data():
    global LAST_RELOAD_TIME
    print("🔄 ECB FULL DATA RELOAD STARTED")

    # ── vlad-БД ───────────────────────────────────────────────────────────────
    async with engine_vlad.connect() as conn:

        # 1. Weight codes
        res = await conn.execute(
            text("SELECT weight_code FROM vlad_ecb_rate_weights"))
        GLOBAL_WEIGHT_CODES[:] = [r["weight_code"]
                                  for r in res.mappings().all()]
        print(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}")

        # 2. Контекстный индекс
        res = await conn.execute(text("""
            SELECT currency, rate_change_dir, trend_dir,
                   momentum_dir, occurrence_count
            FROM vlad_ecb_rate_context_idx
        """))
        GLOBAL_CTX_INDEX.clear()
        for r in res.mappings().all():
            key = (r["currency"], r["rate_change_dir"],
                   r["trend_dir"], r["momentum_dir"])
            GLOBAL_CTX_INDEX[key] = {
                "occurrence_count": r["occurrence_count"] or 0}
        print(f"  ctx_index: {len(GLOBAL_CTX_INDEX)} contexts")

        # 3. Все ECB-курсы → контексты в Python
        res = await conn.execute(text("""
            SELECT currency, rate_date, rate
            FROM vlad_ecb_exchange_rates
            ORDER BY currency, rate_date
        """))
        by_ccy = defaultdict(list)
        for r in res.mappings().all():
            by_ccy[r["currency"]].append(
                (r["rate_date"], float(r["rate"])))

        GLOBAL_ECB_BY_CCY.clear()
        GLOBAL_ECB_CONTEXT.clear()
        GLOBAL_ECB_OBS_DATES.clear()
        GLOBAL_ECB_CTX_HIST.clear()

        for ccy, rates in by_ccy.items():
            GLOBAL_ECB_BY_CCY[ccy] = rates
            for dt, rcd, td, md in classify_observations(rates):
                GLOBAL_ECB_CONTEXT[(ccy, dt)] = (rcd, td, md)
                GLOBAL_ECB_OBS_DATES[dt].add(ccy)
                ctx_key = (ccy, rcd, td, md)
                GLOBAL_ECB_CTX_HIST.setdefault(ctx_key, []).append(dt)

        for key in GLOBAL_ECB_CTX_HIST:
            GLOBAL_ECB_CTX_HIST[key].sort()

        print(f"  ECB currencies: {len(by_ccy)}, "
              f"observations: {len(GLOBAL_ECB_CONTEXT)}")

    # ── brain-БД: котировки и экстремумы ──────────────────────────────────────
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

        async with engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `max`, `min`, t1 "
                f"FROM {table}"))
            rows = sorted(res.mappings().all(),
                          key=lambda x: x["date"])
            ranges = []
            for r in rows:
                dt = r["date"]
                if r["t1"] is not None:
                    GLOBAL_RATES[table][dt] = float(r["t1"])
                is_bull = r["close"] > r["open"]
                GLOBAL_LAST_CANDLES[table].append((dt, is_bull))
                rng = float(r["max"] or 0) - float(r["min"] or 0)
                GLOBAL_CANDLE_RANGES[table][dt] = rng
                ranges.append(rng)

            GLOBAL_AVG_RANGE[table] = (
                sum(ranges) / len(ranges) if ranges else 0.0)

            for typ in ("min", "max"):
                op  = ">" if typ == "max" else "<"
                col = typ
                q = f"""
                    SELECT t1.date FROM {table} t1
                    JOIN {table} t_prev
                         ON t_prev.date = t1.date - INTERVAL 1 DAY
                    JOIN {table} t_next
                         ON t_next.date = t1.date + INTERVAL 1 DAY
                    WHERE t1.{col} {op} t_prev.{col}
                      AND t1.{col} {op} t_next.{col}
                """
                res_ext = await conn.execute(text(q))
                GLOBAL_EXTREMUMS[table][typ] = {
                    r["date"] for r in res_ext.mappings().all()}

        print(f"  {table}: {len(GLOBAL_RATES[table])} candles")

    LAST_RELOAD_TIME = datetime.now()
    print("✅ ECB FULL DATA RELOAD COMPLETED")


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            print(f"❌ Background reload error: {e}")
            send_error_trace(e, "ecb_background_reload")


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


# ── Основной расчёт ──────────────────────────────────────────────────────────

async def calculate_pure_memory(pair: int, day: int, date_str: str,
                                 calc_type: int = 0,
                                 calc_var: int = 0) -> dict:
    target_date = parse_date_string(date_str)
    if not target_date:
        return {"error": "Invalid date format"}

    rates_table  = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    window       = 12  # ±12 дней (ECB — дневные данные)

    target_d = (target_date.date()
                if isinstance(target_date, datetime) else target_date)

    # ── Собираем ECB-наблюдения в окне ±window дней ──────────────────────────
    check_dates = [target_d + timedelta(days=d)
                   for d in range(-window, window + 1)]

    observations = []
    for dt in check_dates:
        for ccy in GLOBAL_ECB_OBS_DATES.get(dt, set()):
            ctx = GLOBAL_ECB_CONTEXT.get((ccy, dt))
            if ctx:
                shift = (target_d - dt).days
                observations.append((ccy, dt, ctx, shift))

    if not observations:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table,
                                       {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result = {}

    for ccy, obs_date, (rcd, td, md), shift in observations:
        ctx_key  = (ccy, rcd, td, md)
        ctx_info = GLOBAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue

        is_recurring = (ctx_info["occurrence_count"]
                        >= RECURRING_MIN_COUNT)

        # Non-recurring → только shift=0
        if not is_recurring and shift != 0:
            continue
        # Recurring → shift в пределах окна
        if is_recurring and abs(shift) > window:
            continue

        # Исторические даты с тем же контекстом ДО target_date
        all_ctx_dates = GLOBAL_ECB_CTX_HIST.get(ctx_key, [])
        idx = bisect.bisect_left(all_ctx_dates, target_d)
        valid_dates = all_ctx_dates[:idx]
        if not valid_dates:
            continue

        # Сдвигаем и конвертируем в datetime для lookup в brain-таблице
        t_dates = [
            datetime.combine(d + timedelta(days=shift),
                             time_type(0, 0))
            for d in valid_dates
        ]

        day_shift_arg = shift if is_recurring else None

        # ── mode=0: T1 ───────────────────────────────────────────────────
        if calc_type in (0, 1):
            t1_sum = compute_t1_value(
                t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(ccy, rcd, td, md, 0, day_shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        # ── mode=1: Extremum ─────────────────────────────────────────────
        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges, avg_range,
                modification, len(valid_dates))
            if ext_val is not None:
                wc = make_weight_code(ccy, rcd, td, md, 1,
                                      day_shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    required_tables = [
        "vlad_ecb_rate_weights",
        "vlad_ecb_rate_context_idx",
        "vlad_ecb_exchange_rates",
        "version_microservice",
    ]
    brain_tables = [
        "brain_rates_eur_usd",
        "brain_rates_btc_usd",
        "brain_rates_eth_usd",
    ]

    async with engine_vlad.connect() as conn:
        for t in required_tables:
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
                "WHERE microservice_id = 30"))
            row = res.fetchone()
            version = row[0] if row else 0
        except Exception as e:
            return {"status": "error", "error": str(e)}

    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "name":    "brain-ecb-weights-microservice",
        "text":    "Calculates historical market weights keyed by "
                   "ECB exchange rate context "
                   "(rate_change_dir / trend_dir / momentum_dir)",
        "weight_code_format":
            "{currency}_{rcd}_{td}_{md}_{mode}[_{day_shift}]",
        "params": {
            "type": "0=T1+extremum, 1=T1 only, 2=extremum only",
            "var":  "0=all/linear, 1=filtered/linear, 2=all/squared, "
                    "3=filtered/squared, 4=filtered/range_delta",
        },
        "note": "ECB data is daily. Use day=1 for best results. "
                "With day=0 only midnight brain entries match.",
        "metadata": {
            "author":           "Vlad",
            "stack":            "Python 3 + MySQL + FastAPI",
            "ctx_index_rows":   len(GLOBAL_CTX_INDEX),
            "weight_codes":     len(GLOBAL_WEIGHT_CODES),
            "ecb_currencies":   len(GLOBAL_ECB_BY_CCY),
            "ecb_observations": len(GLOBAL_ECB_CONTEXT),
            "last_reload":      (LAST_RELOAD_TIME.isoformat()
                                 if LAST_RELOAD_TIME else None),
        },
    }


@app.get("/weights")
async def get_weights():
    return {"weights": GLOBAL_WEIGHT_CODES,
            "total":   len(GLOBAL_WEIGHT_CODES)}


@app.get("/values")
async def get_values(
    pair: int = Query(1),
    day:  int = Query(1, description="1=daily (recommended for ECB), 0=hourly"),
    date: str = Query(..., description="Target date, e.g. 2024-06-15"),
    type: int = Query(0, ge=0, le=2,
                      description="0=T1+ext, 1=T1 only, 2=ext only"),
    var:  int = Query(0, ge=0, le=4,
                      description="Calculation variant 0..4"),
):
    return await calculate_pure_memory(
        pair, day, date, calc_type=type, calc_var=var)


@app.post("/patch")
async def patch_service():
    service_id = 30
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
        new_version = max(old_version, 3)
        if new_version != old_version:
            await conn.execute(text(
                "UPDATE version_microservice "
                "SET version = :v WHERE microservice_id = :id"),
                {"v": new_version, "id": service_id})
    return {"status": "ok",
            "from_version": old_version,
            "to_version": new_version}


# ── Точка входа ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        uvicorn.run("server:app",
                     host="0.0.0.0", port=8893,
                     reload=False, workers=1)
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)