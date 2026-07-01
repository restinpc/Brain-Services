"""
server.py — brain-calendar-weights-microservice (port 8896, SERVICE_ID=33)
Папка: 33/
Режим запуска: MODE=dev | MODE=prod
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import bisect
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

SERVICE_ID = 33
NODE_NAME  = os.getenv("NODE_NAME", "brain-calendar-weights-microservice")
PORT       = 8896

FORECAST_MAP   = {"UNKNOWN": "X", "BEAT": "B", "MISS": "M", "INLINE": "I"}
SURPRISE_MAP   = {"UNKNOWN": "X", "UP":   "U", "DOWN": "D", "FLAT":   "F"}
REVISION_MAP   = {"NONE":    "N", "FLAT": "T", "UP":   "U", "DOWN":   "D", "UNKNOWN": "X"}
IMPORTANCE_MAP = {"high": "H", "medium": "M", "low": "L", "none": "N"}

# Обратные словари: однобуквенный код → полное слово (для /new_weights)
FORECAST_MAP_REV   = {v: k for k, v in FORECAST_MAP.items()}
SURPRISE_MAP_REV   = {v: k for k, v in SURPRISE_MAP.items()}
REVISION_MAP_REV   = {v: k for k, v in REVISION_MAP.items()}
IMPORTANCE_MAP_REV = {v: k for k, v in IMPORTANCE_MAP.items()}

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)

DIRECTION_THRESHOLD = float(os.getenv("DIRECTION_THRESHOLD", "0.01"))
SHIFT_WINDOW        = int(os.getenv("SHIFT_WINDOW",          "12"))
RECURRING_MIN_COUNT = 2
SKIP_EVENT_TYPES    = {2}

# ── Глобальные данные ─────────────────────────────────────────────────────────
GLOBAL_CAL_CTX_INDEX = {}
GLOBAL_CAL_CTX_HIST  = {}
GLOBAL_CAL_BY_DT     = {}
GLOBAL_WEIGHT_CODES  = []
GLOBAL_RATES         = {}
GLOBAL_EXTREMUMS     = {}
GLOBAL_CANDLE_RANGES = {}
GLOBAL_AVG_RANGE     = {}
GLOBAL_LAST_CANDLES  = {}
GLOBAL_EVENT_NAMES   = {}   # event_id → event_name (загружается при старте)
SERVICE_URL          = ""
LAST_RELOAD_TIME     = None

# Для эффективного поиска событий по временному интервалу
_CAL_SORTED_DATES = []   # список datetime
_CAL_SORTED_DATA  = []   # список списков ключей, соответствующих датам


# ── Вспомогательные функции ───────────────────────────────────────────────────

def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    return {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + (
        "_day" if day_flag == 1 else "")


def get_modification_factor(pair_id: int) -> float:
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str: str) -> datetime | None:
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",        # формат без секунд
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",        # ISO без секунд
        "%Y-%m-%d",
        "%Y-%d-%m %H:%M:%S",
    ):
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


def _rel_direction(actual, reference, threshold,
                   up_label="UP", down_label="DOWN", flat_label="FLAT"):
    if actual is None or reference is None:
        return "UNKNOWN"
    if reference == 0:
        return up_label if actual > 0 else (down_label if actual < 0 else flat_label)
    pct = (actual - reference) / abs(reference)
    if pct >  threshold: return up_label
    if pct < -threshold: return down_label
    return flat_label


def classify_event(forecast, previous, old_prev, actual, threshold):
    fcd = ("UNKNOWN" if forecast is None or forecast == 0
           else _rel_direction(actual, forecast, threshold,
                               up_label="BEAT", down_label="MISS", flat_label="INLINE"))
    scd = _rel_direction(actual, previous, threshold)
    if old_prev is None or old_prev == 0 or previous is None:
        rcd = "NONE"
    elif previous == old_prev:
        rcd = "FLAT"
    else:
        rcd = _rel_direction(previous, old_prev, threshold)
    return fcd, scd, rcd


def make_weight_code(event_id, currency, importance, fcd, scd, rcd, mode, hour_shift=None):
    base = (f"E{event_id}_{currency}_{IMPORTANCE_MAP.get(importance,'X')}_"
            f"{FORECAST_MAP.get(fcd,'X')}_{SURPRISE_MAP.get(scd,'X')}_"
            f"{REVISION_MAP.get(rcd,'X')}_{mode}")
    return base if hour_shift is None else f"{base}_{hour_shift}"


def decode_weight_code(code: str) -> dict | None:
    """E{id}_{curr}_{imp}_{fcd}_{scd}_{rcd}_{mode}[_{shift}]  →  dict полей."""
    if not code.startswith("E"):
        return None
    parts = code[1:].split("_")
    if len(parts) < 7:
        return None
    try:
        return {
            "event_id":      int(parts[0]),
            "currency_code": parts[1],
            "importance":    IMPORTANCE_MAP_REV.get(parts[2], parts[2]),
            "forecast_dir":  FORECAST_MAP_REV.get(parts[3], parts[3]),
            "surprise_dir":  SURPRISE_MAP_REV.get(parts[4], parts[4]),
            "revision_dir":  REVISION_MAP_REV.get(parts[5], parts[5]),
            "mode_val":      int(parts[6]),
            "hour_shift":    int(parts[7]) if len(parts) > 7 else None,
        }
    except (ValueError, IndexError):
        return None


# ── Текстовый нарратив details (массив строк) ─────────────────────────────────

def _pluralize_events(n: int) -> str:
    if 11 <= n % 100 <= 19:
        return "событий"
    r = n % 10
    if r == 1: return "событие"
    if r in (2, 3, 4): return "события"
    return "событий"


def _pluralize(n: int, day_flag: int) -> str:
    """
    Склоняет единицу времени по числу n.
    day_flag=1 → день/дня/дней, day_flag=0 → час/часа/часов
    """
    if day_flag == 1:
        forms = ("день", "дня", "дней")
    else:
        forms = ("час", "часа", "часов")
    abs_n = abs(n)
    if abs_n % 100 in range(11, 20):
        return forms[2]
    rem = abs_n % 10
    if rem == 1:   return forms[0]
    if rem in (2, 3, 4): return forms[1]
    return forms[2]


def _shift_label(shift: int | None, day_flag: int) -> str:
    """Человекочитаемое описание сдвига."""
    if shift is None or shift == 0:
        return "в момент целевой свечи"
    u = _pluralize(shift, day_flag)
    if shift > 0:
        return f"{abs(shift)} {u} назад"
    return f"через {abs(shift)} {u}"


def build_details_lines(
    details: list[dict],
    date_str: str,
    calc_type: int,
    day: int,
) -> list[str]:
    """Возвращает список строк нарратива (каждая строка — отдельный элемент)."""
    if not details:
        return ["Нет контекстных событий в заданном промежутке."]

    target_date = parse_date_string(date_str)
    if not target_date:
        return [f"Не удалось разобрать целевую дату: {date_str!r}"]

    delta_unit = timedelta(days=1) if day == 1 else timedelta(hours=1)
    start_dt   = target_date - delta_unit * SHIFT_WINDOW
    end_dt     = target_date + delta_unit * SHIFT_WINDOW

    # Группировка: (event_id, currency_code, hour_shift) → {mode0: [...], mode1: [...]}
    groups: dict[tuple, dict] = {}
    for d in details:
        key = (d["event_id"], d["currency_code"], d["hour_shift"])
        if key not in groups:
            groups[key] = {
                "event_id":         d["event_id"],
                "event_name":       d["event_name"],
                "currency_code":    d["currency_code"],
                "importance":       d["importance"],
                "hour_shift":       d["hour_shift"],
                "occurrence_count": d["occurrence_count"],
                "mode0": [],
                "mode1": [],
            }
        groups[key]["mode0" if d["mode_val"] == 0 else "mode1"].append(d)

    sorted_groups = sorted(
        groups.values(),
        key=lambda g: (-(g["hour_shift"] or 0), g["event_id"]),
    )

    n = len(sorted_groups)

    type_desc = {
        0: "Type = 0, значит нас интересует сумма свечек и вероятность того, что событие станет экстремумом при аналогичном тренде.",
        1: "Type = 1, значит нас интересует только сумма свечек.",
        2: "Type = 2, значит нас интересует только вероятность того, что событие станет экстремумом при аналогичном тренде.",
    }.get(calc_type, f"Type = {calc_type}.")

    lines: list[str] = []

    # Заголовок
    lines.append(
        f"Мы рассматриваем промежуток от {start_dt.strftime('%Y-%m-%d %H:%M')} "
        f"до {end_dt.strftime('%Y-%m-%d %H:%M')}. "
        f"В этом промежутке есть {n} {_pluralize_events(n)}."
    )
    lines.append("")  # пустая строка

    # Краткий нумерованный список
    for i, g in enumerate(sorted_groups, 1):
        name = g["event_name"] or f"event_id={g['event_id']}"
        tl   = _shift_label(g["hour_shift"], day)
        lines.append(f'{i}. "{name}" — {tl}')

    lines.append("")
    lines.append(type_desc)
    lines.append("")

    # Детали по каждому событию
    for i, g in enumerate(sorted_groups, 1):
        name = g["event_name"] or f"event_id={g['event_id']}"
        occ  = g["occurrence_count"] or 0
        tl   = _shift_label(g["hour_shift"], day)

        candle_sum = round(sum(d["value"] for d in g["mode0"]), 6) if g["mode0"] else None
        ext_val    = round(sum(d["value"] for d in g["mode1"]), 6) if g["mode1"] else None

        lines.append(f'Событие {i}. "{name}" [{g["currency_code"]}], {tl}.')

        history_parts = [f"В истории это событие повторялось {occ} раз(а)"]
        if candle_sum is not None:
            history_parts.append(f"следовательно сумма свечек — {candle_sum}")
        if ext_val is not None:
            history_parts.append(f"вероятность экстремума составила {ext_val}")
        lines.append(", ".join(history_parts) + ".")

        result_parts: list[str] = []
        for d in g["mode0"]:
            result_parts.append(f"{d['weight_code']}: {d['value']}")
        for d in g["mode1"]:
            result_parts.append(f"{d['weight_code']}: {d['value']}")

        if result_parts:
            lines.append(f"Значит, мы можем срезультировать {', и '.join(result_parts)}")

        lines.append("")  # пустая строка после каждого события

    return lines


# ── Расчётные функции ─────────────────────────────────────────────────────────

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


# ── Загрузка данных при старте ────────────────────────────────────────────────

async def preload_all_data():
    global SERVICE_URL, LAST_RELOAD_TIME, _CAL_SORTED_DATES, _CAL_SORTED_DATA
    log("🔄 CALENDAR FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    GLOBAL_WEIGHT_CODES.clear()
    GLOBAL_CAL_CTX_INDEX.clear()
    GLOBAL_CAL_CTX_HIST.clear()
    GLOBAL_CAL_BY_DT.clear()
    GLOBAL_RATES.clear()
    GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear()
    GLOBAL_AVG_RANGE.clear()
    GLOBAL_LAST_CANDLES.clear()
    GLOBAL_EVENT_NAMES.clear()
    _CAL_SORTED_DATES.clear()
    _CAL_SORTED_DATA.clear()

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(text("SELECT weight_code FROM brain_calendar_weights"))
            GLOBAL_WEIGHT_CODES.extend(r[0] for r in res.fetchall())
            log(f"  weight_codes: {len(GLOBAL_WEIGHT_CODES)}", NODE_NAME)
        except Exception as e:
            log(f"❌ weight_codes error: {e}", NODE_NAME, level="error")

        try:
            res = await conn.execute(text("""
                SELECT event_id, currency_code, importance,
                       forecast_dir, surprise_dir, revision_dir, occurrence_count
                FROM brain_calendar_context_idx
            """))
            for r in res.mappings().all():
                key = (r["event_id"], r["currency_code"], r["importance"],
                       r["forecast_dir"], r["surprise_dir"], r["revision_dir"])
                GLOBAL_CAL_CTX_INDEX[key] = {"occurrence_count": r["occurrence_count"] or 0}
            log(f"  ctx_index: {len(GLOBAL_CAL_CTX_INDEX)}", NODE_NAME)
        except Exception as e:
            log(f"❌ ctx_index error: {e}", NODE_NAME, level="error")

    # url → event_id — используем engine_vlad (там есть колонка EventId)
    url_to_event_id = {}
    try:
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT Url, EventId
                FROM brain_calendar
                WHERE Url IS NOT NULL AND Url != '' AND EventId IS NOT NULL
                GROUP BY Url, EventId
            """))
            for r in res.fetchall():
                url_to_event_id[r[0]] = r[1]
        log(f"  url→event_id map: {len(url_to_event_id)} entries", NODE_NAME)
    except Exception as e:
        log(f"❌ url→event_id map error: {e}", NODE_NAME, level="error")

    # event_names — используем engine_vlad (там есть EventId и EventName)
    try:
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT EventId, EventName
                FROM brain_calendar
                WHERE EventId IS NOT NULL AND EventName IS NOT NULL AND EventName != ''
                GROUP BY EventId, EventName
            """))
            for r in res.fetchall():
                GLOBAL_EVENT_NAMES[r[0]] = r[1]
        log(f"  event_names: {len(GLOBAL_EVENT_NAMES)} entries", NODE_NAME)
    except Exception as e:
        log(f"⚠️  event_names load error: {e}", NODE_NAME, level="warning")

    # Загрузка календарных событий из engine_brain
    try:
        async with engine_brain.connect() as conn:
            res = await conn.execute(text("""
                SELECT
                    Url,
                    EventName,
                    CurrencyCode,
                    Importance,
                    ForecastValue,
                    PreviousValue,
                    OldPreviousValue,
                    ActualValue,
                    FullDate,
                    EventType
                FROM brain_calendar
                WHERE ActualValue IS NOT NULL
                  AND Processed = 1
                ORDER BY FullDate
            """))
            rows = res.fetchall()
            log(f"  brain_calendar (from engine_brain): {len(rows)} rows", NODE_NAME)

            skipped_no_event_id = 0
            skipped_no_index    = 0
            for row in rows:
                url        = row[0]
                event_name = row[1]
                currency   = row[2]
                importance = row[3]
                forecast   = row[4]
                previous   = row[5]
                old_prev   = row[6]
                actual     = row[7]
                full_date  = row[8]
                event_type = row[9]

                if event_type in SKIP_EVENT_TYPES or currency is None:
                    continue

                event_id = url_to_event_id.get(url) if url else None
                if event_id is None:
                    skipped_no_event_id += 1
                    continue

                # Заполняем GLOBAL_EVENT_NAMES из engine_brain (там всегда есть EventName)
                if event_name and event_id not in GLOBAL_EVENT_NAMES:
                    GLOBAL_EVENT_NAMES[event_id] = event_name

                fcd, scd, rcd = classify_event(
                    forecast, previous, old_prev, actual, DIRECTION_THRESHOLD
                )
                imp = (importance or "none").lower()
                key = (event_id, currency, imp, fcd, scd, rcd)

                if key not in GLOBAL_CAL_CTX_INDEX:
                    skipped_no_index += 1
                    continue

                GLOBAL_CAL_CTX_HIST.setdefault(key, []).append(full_date)
                GLOBAL_CAL_BY_DT.setdefault(full_date, []).append(key)

            for key in GLOBAL_CAL_CTX_HIST:
                GLOBAL_CAL_CTX_HIST[key].sort()

            total_obs = sum(len(v) for v in GLOBAL_CAL_CTX_HIST.values())
            log(f"  contexts: {len(GLOBAL_CAL_CTX_HIST)}, observations: {total_obs}", NODE_NAME)
            log(f"  skipped (no event_id via url): {skipped_no_event_id}", NODE_NAME)
            log(f"  skipped (not in ctx_index): {skipped_no_index}", NODE_NAME)

    except Exception as e:
        log(f"❌ brain_calendar (from engine_brain) error: {e}", NODE_NAME, level="error")

    # Построение отсортированных структур для поиска по интервалу
    if GLOBAL_CAL_BY_DT:
        sorted_items = sorted(GLOBAL_CAL_BY_DT.items())
        _sorted_dts, _sorted_vals = zip(*sorted_items)
        _CAL_SORTED_DATES[:] = list(_sorted_dts)
        _CAL_SORTED_DATA[:]  = list(_sorted_vals)
    else:
        _CAL_SORTED_DATES.clear()
        _CAL_SORTED_DATA.clear()

    # Загрузка рыночных данных из engine_brain
    for table in [
        "brain_rates_eur_usd", "brain_rates_eur_usd_day",
        "brain_rates_btc_usd", "brain_rates_btc_usd_day",
        "brain_rates_eth_usd", "brain_rates_eth_usd_day",
    ]:
        GLOBAL_RATES[table]         = {}
        GLOBAL_LAST_CANDLES[table]  = []
        GLOBAL_CANDLE_RANGES[table] = {}
        GLOBAL_AVG_RANGE[table]     = 0.0
        GLOBAL_EXTREMUMS[table]     = {"min": set(), "max": set()}
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
                GLOBAL_AVG_RANGE[table] = (
                    sum(ranges) / len(ranges) if ranges else 0.0
                )
                interval = "1 DAY" if table.endswith("_day") else "1 HOUR"
                for typ in ("min", "max"):
                    op = ">" if typ == "max" else "<"
                    q  = f"""
                        SELECT t1.date FROM `{table}` t1
                        JOIN `{table}` t_prev ON t_prev.date = t1.date - INTERVAL {interval}
                        JOIN `{table}` t_next ON t_next.date = t1.date + INTERVAL {interval}
                        WHERE t1.`{typ}` {op} t_prev.`{typ}` AND t1.`{typ}` {op} t_next.`{typ}`
                    """
                    res_ext = await conn.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {
                        r["date"] for r in res_ext.mappings().all()
                    }
            log(f"  {table}: {len(GLOBAL_RATES[table])} candles", NODE_NAME)
        except Exception as e:
            log(f"❌ {table}: {e}", NODE_NAME, level="error")

    # Загрузка SERVICE_URL
    try:
        SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
        log(f"  SERVICE_URL loaded: {SERVICE_URL}", NODE_NAME)
    except OperationalError as e:
        log(f"❌ Failed to load service URL (OperationalError): {e}",
            NODE_NAME, level="error", force=True)
        SERVICE_URL = ""
    except Exception as e:
        log(f"❌ Failed to load service URL: {e}", NODE_NAME, level="error", force=True)
        SERVICE_URL = ""

    try:
        await ensure_cache_table(engine_vlad)
    except Exception as e:
        log(f"❌ Failed to ensure cache table: {e}", NODE_NAME, level="error")

    LAST_RELOAD_TIME = datetime.now()
    log("✅ CALENDAR FULL DATA RELOAD COMPLETED", NODE_NAME, force=True)


async def background_reload_data():
    while True:
        await asyncio.sleep(3600)
        try:
            await preload_all_data()
        except Exception as e:
            log(f"❌ Background reload error: {e}", NODE_NAME, level="error", force=True)
            send_error_trace(e, NODE_NAME, "calendar_background_reload")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await preload_all_data()
    except Exception as e:
        log(f"❌ Initial data load failed, but server will continue: {e}",
            NODE_NAME, level="error", force=True)

    task = asyncio.create_task(background_reload_data())
    yield
    task.cancel()

    for name, engine in [
        ("vlad",  engine_vlad),
        ("brain", engine_brain),
        ("super", engine_super),
    ]:
        try:
            await engine.dispose()
        except Exception as e:
            log(f"Error disposing engine_{name}: {e}", NODE_NAME, level="error")


app = FastAPI(lifespan=lifespan)


# ── Подгрузка свежих свечей из БД ────────────────────────────────────────────

_LAST_RATES_REFRESH: dict[str, datetime] = {}


async def _refresh_rates_if_needed(rates_table: str):
    """Если в RAM нет свечи за последний час — подгрузить из БД. Не чаще раза в 30 сек."""
    now  = datetime.now()
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
            res = await conn.execute(
                text(f"SELECT date, open, close, t1 "
                     f"FROM `{rates_table}` WHERE date > :dt ORDER BY date"),
                {"dt": max_dt},
            )
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


async def calculate_pure_memory(
    pair: int, day: int, date_str: str,
    calc_type: int = 0, calc_var: int = 0,
) -> dict | None:
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
        left   = bisect.bisect_left(_CAL_SORTED_DATES, dt)
        right  = bisect.bisect_left(_CAL_SORTED_DATES, dt_end)
        for i in range(left, right):
            obs_dt = _CAL_SORTED_DATES[i]
            for ctx_key in _CAL_SORTED_DATA[i]:
                shift_steps = round((target_date - obs_dt) / delta_unit)
                observations.append((ctx_key, obs_dt, shift_steps))

    if not observations:
        return {}

    ram_rates   = GLOBAL_RATES.get(rates_table, {})
    ram_ranges  = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range   = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext     = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    result: dict[str, float] = {}
    for ctx_key, obs_dt, shift in observations:
        event_id, currency, importance, fcd, scd, rcd = ctx_key
        ctx_info = GLOBAL_CAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            continue
        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT
        if not is_recurring and shift != 0:
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            continue

        all_ctx_dts = GLOBAL_CAL_CTX_HIST.get(ctx_key, [])
        idx         = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts   = all_ctx_dts[:idx]
        if not valid_dts:
            continue

        t_dates   = [d + delta_unit * shift for d in valid_dts
                     if d + delta_unit * shift < target_date]
        shift_arg = shift if is_recurring else None

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc     = make_weight_code(event_id, currency, importance, fcd, scd, rcd, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set    = ram_ext["max" if is_bull else "min"]
            ext_val    = compute_extremum_value(
                t_dates, calc_var, ext_set, ram_ranges,
                avg_range, modification, len(valid_dts),
            )
            if ext_val is not None:
                wc = make_weight_code(event_id, currency, importance, fcd, scd, rcd, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ── Роуты ─────────────────────────────────────────────────────────────────────

@app.get("/")
async def get_metadata():
    db_status = {}
    for db_name, engine in [
        ("vlad",  engine_vlad),
        ("brain", engine_brain),
        ("super", engine_super),
    ]:
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            db_status[db_name] = "ok"
        except Exception as e:
            db_status[db_name] = f"error: {str(e)[:100]}"

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(
                text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
                {"id": SERVICE_ID},
            )
            row     = res.fetchone()
            version = row[0] if row else 0
        except Exception:
            version = 0

    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "mode":    MODE,
        "name":    NODE_NAME,
        "text":    "Calculates historical calendar weights keyed by event context",
        "weight_code_format": (
            "E{event_id}_{currency}_{imp}_{fcd}_{scd}_{rcd}_{mode}[_{hour_shift}]"
        ),
        "metadata": {
            "ctx_index_rows":    len(GLOBAL_CAL_CTX_INDEX),
            "weight_codes":      len(GLOBAL_WEIGHT_CODES),
            "calendar_contexts": len(GLOBAL_CAL_CTX_HIST),
            "last_reload":       LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
            "db_status":         db_status,
            "brain_calendar_source": "engine_brain",
        },
    }


@app.get("/weights")
async def get_weights():
    try:
        return ok_response(GLOBAL_WEIGHT_CODES)
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(str(e))


@app.get("/new_weights")
async def get_new_weights(
    code:  str = Query(...),
    limit: int = Query(500, ge=1, le=5000),
):
    try:
        if not code.startswith("E"):
            return err_response("weight_code must start with 'E'")
        parts = code[1:].split("_")
        if len(parts) < 7:
            return err_response("Invalid weight_code format")
        try:
            event_id = int(parts[0])
        except ValueError:
            return err_response("event_id must be an integer")

        currency_code = parts[1]
        imp_c, fcd_c, scd_c, rcd_c = parts[2], parts[3], parts[4], parts[5]

        importance   = IMPORTANCE_MAP_REV.get(imp_c, imp_c)
        forecast_dir = FORECAST_MAP_REV.get(fcd_c, fcd_c)
        surprise_dir = SURPRISE_MAP_REV.get(scd_c, scd_c)
        revision_dir = REVISION_MAP_REV.get(rcd_c, rcd_c)

        try:
            mode_val   = int(parts[6])
            hour_shift = int(parts[7]) if len(parts) > 7 else None
        except ValueError:
            return err_response("mode/hour_shift must be integers")

        hs_val = hour_shift if hour_shift is not None else -999999

        async with engine_vlad.connect() as conn:
            res = await conn.execute(text("""
                SELECT weight_code FROM brain_calendar_weights
                WHERE
                    event_id > :event_id
                    OR (event_id = :event_id AND currency_code > :currency_code)
                    OR (event_id = :event_id AND currency_code = :currency_code
                        AND importance > :importance)
                    OR (event_id = :event_id AND currency_code = :currency_code
                        AND importance = :importance AND forecast_dir > :forecast_dir)
                    OR (event_id = :event_id AND currency_code = :currency_code
                        AND importance = :importance AND forecast_dir = :forecast_dir
                        AND surprise_dir > :surprise_dir)
                    OR (event_id = :event_id AND currency_code = :currency_code
                        AND importance = :importance AND forecast_dir = :forecast_dir
                        AND surprise_dir = :surprise_dir AND revision_dir > :revision_dir)
                    OR (event_id = :event_id AND currency_code = :currency_code
                        AND importance = :importance AND forecast_dir = :forecast_dir
                        AND surprise_dir = :surprise_dir AND revision_dir = :revision_dir
                        AND mode_val > :mode_val)
                    OR (event_id = :event_id AND currency_code = :currency_code
                        AND importance = :importance AND forecast_dir = :forecast_dir
                        AND surprise_dir = :surprise_dir AND revision_dir = :revision_dir
                        AND mode_val = :mode_val
                        AND COALESCE(hour_shift, -999999) > :hs)
                ORDER BY
                    event_id, currency_code, importance,
                    forecast_dir, surprise_dir, revision_dir, mode_val,
                    hour_shift IS NULL, hour_shift
                LIMIT :limit
            """), {
                "event_id":      event_id,
                "currency_code": currency_code,
                "importance":    importance,
                "forecast_dir":  forecast_dir,
                "surprise_dir":  surprise_dir,
                "revision_dir":  revision_dir,
                "mode_val":      mode_val,
                "hs":            hs_val,
                "limit":         limit,
            })
        return ok_response([r["weight_code"] for r in res.mappings().all()])
    except OperationalError as e:
        log(f"Database connection error in get_new_weights: {e}", NODE_NAME, level="error")
        return err_response(f"Database connection error: {str(e)}")
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_new_weights")
        return err_response(str(e))


@app.get("/values")
async def get_values(
    pair: int = Query(1),
    day:  int = Query(1),
    date: str = Query(...),
    type: int = Query(0, ge=0, le=2),
    var:  int = Query(0, ge=0, le=4),
):
    try:
        response = await cached_values(
            engine_vlad  = engine_vlad,
            service_url  = SERVICE_URL,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = {"type": type, "var": var},
            compute_fn   = lambda: calculate_pure_memory(
                pair, day, date, calc_type=type, calc_var=var,
            ),
            node         = NODE_NAME,
        )

        #  Подготовка данных для нарратива
        payload = response.get("payLoad") or {}
        raw_details: list[dict] = []
        for wc, value in payload.items():
            dec = decode_weight_code(wc)
            if dec is None:
                continue
            eid     = dec["event_id"]
            ctx_key = (eid, dec["currency_code"], dec["importance"],
                       dec["forecast_dir"], dec["surprise_dir"], dec["revision_dir"])
            ctx_info = GLOBAL_CAL_CTX_INDEX.get(ctx_key, {})
            raw_details.append({
                "weight_code":      wc,
                "event_id":         eid,
                "event_name":       GLOBAL_EVENT_NAMES.get(eid),  # fallback, перезапишем ниже
                "currency_code":    dec["currency_code"],
                "importance":       dec["importance"],
                "forecast_dir":     dec["forecast_dir"],
                "surprise_dir":     dec["surprise_dir"],
                "revision_dir":     dec["revision_dir"],
                "mode_val":         dec["mode_val"],
                "hour_shift":       dec["hour_shift"],
                "occurrence_count": ctx_info.get("occurrence_count"),
                "value":            value,
            })

        # Добираем имена событий прямо из БД для event_id без имени
        missing_ids = {d["event_id"] for d in raw_details if not d["event_name"]}
        if missing_ids:
            try:
                ids_list     = list(missing_ids)
                placeholders = ", ".join(f":id{i}" for i in range(len(ids_list)))
                params       = {f"id{i}": eid for i, eid in enumerate(ids_list)}
                async with engine_brain.connect() as conn:
                    res = await conn.execute(
                        text(f"""
                            SELECT EventId, EventName
                            FROM brain_calendar
                            WHERE EventId IN ({placeholders})
                              AND EventName IS NOT NULL AND EventName != ''
                            GROUP BY EventId, EventName
                        """),
                        params,
                    )
                    db_names = {r[0]: r[1] for r in res.fetchall()}
                # Обновляем global-кэш и raw_details
                GLOBAL_EVENT_NAMES.update(db_names)
                for d in raw_details:
                    if not d["event_name"]:
                        d["event_name"] = db_names.get(d["event_id"])
            except Exception as e:
                log(f"event_names fallback query failed: {e}", NODE_NAME, level="warning")

        response["details"] = build_details_lines(raw_details, date, calc_type=type, day=day)
        return response

    except OperationalError as e:
        log(f"Database connection error in get_values: {e}", NODE_NAME, level="error")
        return err_response(f"Database connection error: {str(e)}")
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_values")
        return err_response(str(e))


@app.post("/patch")
async def patch_service():
    async with engine_vlad.begin() as conn:
        try:
            res = await conn.execute(
                text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
                {"id": SERVICE_ID},
            )
            row = res.fetchone()
            if not row:
                raise HTTPException(
                    status_code=500, detail=f"Service ID {SERVICE_ID} not found"
                )
            old = row[0]
            new = max(old, 1)
            if new != old:
                await conn.execute(
                    text("UPDATE version_microservice SET version = :v "
                         "WHERE microservice_id = :id"),
                    {"v": new, "id": SERVICE_ID},
                )
        except OperationalError as e:
            log(f"Database connection error in patch_service: {e}", NODE_NAME, level="error")
            raise HTTPException(
                status_code=503,
                detail=f"Database connection error: {str(e)}",
            )
    return {"status": "ok", "from_version": old, "to_version": new}


# ── Точка входа ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio as _asyncio

    async def _get_workers():
        try:
            return await resolve_workers(engine_super, SERVICE_ID, default=1)
        except Exception as e:
            log(f"Error resolving workers, using default 1: {e}", NODE_NAME, level="error")
            return 1

    try:
        _workers = _asyncio.run(_get_workers())
    except Exception as e:
        log(f"Failed to get workers count, using default 1: {e}", NODE_NAME, level="error")
        _workers = 1

    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False, workers=_workers)
    except KeyboardInterrupt:
        log("Server stopped", NODE_NAME, force=True)
    except SystemExit:
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
