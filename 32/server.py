import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

from common import (
    MODE, IS_DEV,
    log, send_error_trace,
    ok_response, err_response,
    resolve_workers,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values

# ── Идентификаторы ─────────────────────────────────────────────────────────────
SERVICE_ID = 32
NODE_NAME  = os.getenv("NODE_NAME", "brain-investing-weights-microservice-32")
PORT       = 8895

# ── Конфигурация БД ───────────────────────────────────────────────────────────
load_dotenv()

DB_HOST         = os.getenv("DB_HOST",         "")
DB_PORT         = os.getenv("DB_PORT",         "")
DB_USER         = os.getenv("DB_USER",         "")
DB_PASSWORD     = os.getenv("DB_PASSWORD",     "")
DB_NAME         = os.getenv("DB_NAME",         "")
MASTER_HOST     = os.getenv("MASTER_HOST",     "")
MASTER_PORT     = os.getenv("MASTER_PORT",     "")
MASTER_USER     = os.getenv("MASTER_USER",     "")
MASTER_PASSWORD = os.getenv("MASTER_PASSWORD", "")
MASTER_NAME     = os.getenv("MASTER_NAME",     "")

DATABASE_URL_VLAD  = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL_BRAIN = f"mysql+aiomysql://{MASTER_USER}:{MASTER_PASSWORD}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}"

engine_vlad  = create_async_engine(DATABASE_URL_VLAD,  pool_size=10, echo=False)
engine_brain = create_async_engine(DATABASE_URL_BRAIN, pool_size=5,  echo=False)

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"vlad:  {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}",              NODE_NAME)
log(f"brain: {MASTER_USER}@{MASTER_HOST}:{MASTER_PORT}/{MASTER_NAME}", NODE_NAME)

# ── VAR конфиги (урезанный набор для 32) ──────────────────────────────────────
VAR_CONFIGS = {
    0: (-12, 12,  50,   "bayes",  10),
    1: (-12, 12,  75,   "bayes",  10),
    2: (-6,   6,  50,   "bayes",  10),
    3: (-24, 24,  50,   "bayes",  10),
    4: (-12, 12,  None, "none",   0),
}

def confidence_bayes(count, prior=10):
    return count / (count + prior) if count > 0 else 0.0

def confidence_none(count, prior=0):
    return 1.0

CONFIDENCE_FUNCS = {
    "bayes": confidence_bayes,
    "none":  confidence_none,
}

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_EXTREMUMS        = {}
GLOBAL_RATES            = {}
GLOBAL_CALENDAR         = {}
GLOBAL_HISTORY          = {}
GLOBAL_LAST_CANDLES     = {}
GLOBAL_WEIGHT_CODES     = []
GLOBAL_EVENT_TYPES      = {}
GLOBAL_CANDLE_SIZES     = {}
GLOBAL_CANDLE_THRESHOLD = {}
SERVICE_URL             = ""
LAST_RELOAD_TIME        = None


def get_rates_table_name(pair_id, day_flag):
    suffix = "_day" if day_flag == 1 else ""
    return {
        1: "brain_rates_eur_usd",
        3: "brain_rates_btc_usd",
        4: "brain_rates_eth_usd",
    }.get(pair_id, "brain_rates_eur_usd") + suffix


def get_modification_factor(pair_id):
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str):
    for fmt in ("%Y-%d-%m %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
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


async def preload_all_data():
    global SERVICE_URL, LAST_RELOAD_TIME
    log("🔄 FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    async with engine_vlad.connect() as conn:
        # Весовые коды
        res = await conn.execute(text("SELECT weight_code FROM vlad_investing_weights_table"))
        GLOBAL_WEIGHT_CODES[:] = [r["weight_code"] for r in res.mappings().all()]
        log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)

        # Типы событий (регулярные/нерегулярные)
        res = await conn.execute(text("SELECT event_id, occurrence_count FROM vlad_investing_event_index"))
        GLOBAL_EVENT_TYPES.clear()
        for r in res.mappings().all():
            eid = r["event_id"]
            cnt = r["occurrence_count"] or 0
            GLOBAL_EVENT_TYPES[eid] = 1 if cnt > 1 else 0

        # Календарь событий
        res = await conn.execute(text("""
            SELECT c.event_id, c.occurrence_time_utc, c.importance
            FROM vlad_investing_calendar c WHERE c.event_id IS NOT NULL
        """))
        GLOBAL_CALENDAR.clear()
        GLOBAL_HISTORY.clear()
        for r in res.mappings().all():
            dt = r["occurrence_time_utc"]
            eid = r["event_id"]
            imp = r["importance"]
            GLOBAL_HISTORY.setdefault(eid, []).append(dt)
            GLOBAL_CALENDAR.setdefault(dt, []).append({
                "EventId": eid,
                "Importance": imp,
                "event_date": dt,
            })

    # Загрузка свечных данных
    tables = [
        "brain_rates_eur_usd", "brain_rates_eur_usd_day",
        "brain_rates_btc_usd", "brain_rates_btc_usd_day",
        "brain_rates_eth_usd", "brain_rates_eth_usd_day",
    ]
    for table in tables:
        GLOBAL_RATES[table] = {}
        GLOBAL_LAST_CANDLES[table] = []
        GLOBAL_EXTREMUMS[table] = {"min": set(), "max": set()}

        async with engine_brain.connect() as conn:
            res = await conn.execute(text(f"SELECT date, open, close, t1 FROM {table}"))
            rows = sorted(res.mappings().all(), key=lambda x: x["date"])

            sizes_list = []
            for r in rows:
                dt = r["date"]
                if r["t1"] is not None:
                    GLOBAL_RATES[table][dt] = float(r["t1"])
                is_bull = r["close"] > r["open"]
                GLOBAL_LAST_CANDLES[table].append((dt, is_bull))

                o = float(r["open"]) if r["open"] else 0
                c = float(r["close"]) if r["close"] else 0
                sz = abs(c - o)
                GLOBAL_CANDLE_SIZES.setdefault(table, {})[dt] = sz
                sizes_list.append(sz)

            # Перцентили размера свечи
            sizes_list.sort()
            n = len(sizes_list)
            GLOBAL_CANDLE_THRESHOLD[table] = {}
            if n > 0:
                for pct in (25, 50, 75, 90):
                    idx = min(int(n * pct / 100), n - 1)
                    GLOBAL_CANDLE_THRESHOLD[table][pct] = sizes_list[idx]

            # Экстремумы (локальные минимумы/максимумы)
            for typ in ("min", "max"):
                op = ">" if typ == "max" else "<"
                col = "max" if typ == "max" else "min"
                q = f"""
                    SELECT t1.date FROM {table} t1
                    JOIN {table} t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
                    JOIN {table} t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
                    WHERE t1.{col} {op} t_prev.{col} AND t1.{col} {op} t_next.{col}
                """
                res_ext = await conn.execute(text(q))
                GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}

        log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)

    # URL сервиса и таблица кеша
    SERVICE_URL = await load_service_url(engine_brain, SERVICE_ID)
    await ensure_cache_table(engine_vlad)

    LAST_RELOAD_TIME = datetime.now()
    log("✅ FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f"❌ Background reload error: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "server_background_reload")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_all_data()
    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()
    await engine_vlad.dispose()
    await engine_brain.dispose()


app = FastAPI(lifespan=lifespan)


async def calculate_pure_memory(pair: int, day: int, date_str: str,
                                type_: int = 0, var: int = 0) -> dict | None:
    """Основная логика расчёта weights."""
    target_date = parse_date_string(date_str)
    if not target_date:
        return None

    var_cfg = VAR_CONFIGS.get(var)
    if var_cfg is None:
        return {}  # неизвестный var → пустой результат (не кешируем ошибку)
    wl, wr, candle_pct, conf_name, conf_param = var_cfg
    conf_fn = CONFIDENCE_FUNCS[conf_name]

    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)

    # формируем окно дат вокруг целевой
    if day == 0:
        check_dates = [target_date + timedelta(hours=h) for h in range(wl, wr + 1)]
    else:
        check_dates = [target_date + timedelta(days=d) for d in range(wl, wr + 1)]

    # собираем события в окне
    events_in_window = []
    for dt in check_dates:
        for e in GLOBAL_CALENDAR.get(dt, []):
            # пропускаем "low" важность, кроме самой целевой даты
            if e["Importance"] != 1 or dt == target_date:
                events_in_window.append(e)

    # фильтруем по типу события (регулярное/нерегулярное)
    needed_events = []
    for e in events_in_window:
        diff = target_date - e["event_date"]
        shift = int(diff.total_seconds() / 3600) if day == 0 else diff.days
        evt_type = GLOBAL_EVENT_TYPES.get(e["EventId"], 0)
        if (evt_type == 0 and shift != 0) or (evt_type == 1 and abs(shift) > 12):
            continue
        needed_events.append((e, shift, evt_type))

    if not needed_events:
        return {}

    # подготовленные структуры
    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    size_threshold = 0
    if candle_pct is not None:
        size_threshold = GLOBAL_CANDLE_THRESHOLD.get(rates_table, {}).get(candle_pct, 0)

    result = {}
    for e, shift, evt_type in needed_events:
        valid_dates = [d for d in GLOBAL_HISTORY.get(e["EventId"], []) if d < target_date]
        if not valid_dates:
            continue

        key0 = f"{e['EventId']}_{evt_type}_0" + (f"_{shift}" if evt_type == 1 else "")
        key1 = f"{e['EventId']}_{evt_type}_1" + (f"_{shift}" if evt_type == 1 else "")

        delta = timedelta(hours=shift) if day == 0 else timedelta(days=shift)
        t_dates = [d + delta for d in valid_dates if (d + delta) < target_date]

        if size_threshold > 0:
            filtered = [td for td in t_dates
                        if GLOBAL_CANDLE_SIZES.get(rates_table, {}).get(td, 0) >= size_threshold]
        else:
            filtered = t_dates

        n = len(filtered)
        conf = conf_fn(n, conf_param)

        # type_ = 0 (mode 0) — сумма t1
        sum_t1 = sum(ram_rates.get(td, 0) for td in filtered)
        result[key0] = sum_t1 * conf

        # type_ = 1 (mode 1) — экстремумы
        if prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            matches = sum(1 for td in filtered if td in ext_set)
            total = len(filtered)
            if total > 0:
                val = (matches / total) * 2 - 1
                result[key1] = val * conf * modification

    # убираем нулевые значения
    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Эндпоинты ─────────────────────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    # Проверка доступности таблиц
    vlad_tables = [
        "vlad_investing_weights_table",
        "vlad_investing_event_index",
        "vlad_investing_calendar",
        "version_microservice",
    ]
    brain_tables = [
        "brain_rates_eur_usd",
        "brain_rates_btc_usd",
        "brain_rates_eth_usd",
    ]

    async with engine_vlad.connect() as conn:
        for t in vlad_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            except Exception as e:
                return err_response(f"vlad.{t} inaccessible: {e}")

    async with engine_brain.connect() as conn:
        for t in brain_tables:
            try:
                await conn.execute(text(f"SELECT 1 FROM `{t}` LIMIT 1"))
            except Exception as e:
                return err_response(f"brain.{t} inaccessible: {e}")

    # Версия из version_microservice
    async with engine_vlad.connect() as conn:
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID},
        )
        row = res.fetchone()
        version = row[0] if row else 0

    return {
        "status": "ok",
        "version": f"1.{version}.0",
        "name": NODE_NAME,
        "mode": MODE,
        "text": "Investing weights (basic 5 VAR_CONFIGS)",
        "var_count": len(VAR_CONFIGS),
        "metadata": {
            "author": "Vlad",
            "stack": "Python 3 + MySQL",
            "last_reload": LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
        },
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response({"weights": GLOBAL_WEIGHT_CODES})
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_weights")


@app.get("/new_weights")
async def get_new_weights(code: str = Query(...)):
    try:
        parts = code.split("_")
        if len(parts) < 3:
            return err_response("Invalid weight_code format")
        try:
            eid = int(parts[0])
            etype = int(parts[1])
            mval = int(parts[2])
            hshift = int(parts[3]) if len(parts) > 3 else None
        except ValueError:
            return err_response("All components must be integers")

        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT weight_code FROM vlad_investing_weights_table
                WHERE (EventId, event_type, mode_val, COALESCE(hour_shift, -999999))
                      > (:eid, :etype, :mval, :hshift)
                ORDER BY EventId, event_type, mode_val, hour_shift IS NULL, hour_shift
            """), {
                "eid": eid,
                "etype": etype,
                "mval": mval,
                "hshift": hshift if hshift is not None else -999999,
            })
            weights = [r["weight_code"] for r in res.mappings().all()]
        return ok_response({"weights": weights})
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_new_weights")


@app.get("/values")
async def get_values(
    pair: int = Query(1),
    day: int = Query(0),
    date: str = Query(...),
    type: int = Query(0),
    var: int = Query(0),
):
    try:
        return await cached_values(
            engine_vlad=engine_vlad,
            service_url=SERVICE_URL,
            pair=pair,
            day=day,
            date=date,
            extra_params={"type": type, "var": var},
            compute_fn=lambda: calculate_pure_memory(pair, day, date, type_=type, var=var),
            node=NODE_NAME,
        )
    except Exception as e:
        return err_response(str(e), exc=e, node=NODE_NAME, script="get_values")


@app.post("/patch")
async def patch_service():
    try:
        async with engine_vlad.begin() as conn:
            res = await conn.execute(
                text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
                {"id": SERVICE_ID},
            )
            row = res.fetchone()
            if not row:
                raise HTTPException(status_code=500, detail=f"Service ID {SERVICE_ID} not found")
            old = row[0]
            new = max(old, 1)  # минимальная версия 1
            if new != old:
                await conn.execute(
                    text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                    {"v": new, "id": SERVICE_ID},
                )
        return {"status": "ok", "from_version": old, "to_version": new}
    except HTTPException:
        raise
    except Exception as e:
        send_error_trace(e, NODE_NAME, "patch_service")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import asyncio as _asyncio

    async def _get_workers():
        return await resolve_workers(engine_brain, SERVICE_ID, default=1)

    _workers = _asyncio.run(_get_workers())
    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)

    try:
        uvicorn.run(
            "server:app",
            host="0.0.0.0",
            port=PORT,
            reload=False,
            workers=_workers,
        )
    except KeyboardInterrupt:
        log("Server stopped by user", NODE_NAME, force=True)
    except SystemExit:
        pass
    except Exception as e:
        log(f"Critical error: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)