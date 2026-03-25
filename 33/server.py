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
SERVICE_URL          = ""
LAST_RELOAD_TIME     = None


def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    return {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd") + (
        "_day" if day_flag == 1 else "")

def get_modification_factor(pair_id: int) -> float:
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)

def parse_date_string(date_str: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%d-%m %H:%M:%S"):
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
    log("🔄 CALENDAR FULL DATA RELOAD STARTED", NODE_NAME, force=True)

    GLOBAL_WEIGHT_CODES.clear(); GLOBAL_CAL_CTX_INDEX.clear()
    GLOBAL_CAL_CTX_HIST.clear(); GLOBAL_CAL_BY_DT.clear()
    GLOBAL_RATES.clear(); GLOBAL_EXTREMUMS.clear()
    GLOBAL_CANDLE_RANGES.clear(); GLOBAL_AVG_RANGE.clear(); GLOBAL_LAST_CANDLES.clear()

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

    # Шаг 1: строим словарь url → event_id из engine_vlad
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

    # Шаг 2: читаем актуальные данные из engine_brain
    try:
        async with engine_brain.connect() as conn:
            query = """
                SELECT
                    Url,
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
            """
            res = await conn.execute(text(query))
            rows = res.fetchall()
            log(f"  brain_calendar (from engine_brain): {len(rows)} rows", NODE_NAME)

            skipped_no_event_id = 0
            skipped_no_index = 0

            # Логируем диапазон дат
            if rows:
                first_date = rows[0][7]  # FullDate
                last_date = rows[-1][7]
                log(f"  Диапазон дат в brain_calendar: {first_date} .. {last_date}", NODE_NAME)

            for row in rows:
                url        = row[0]
                currency   = row[1]
                importance = row[2]
                forecast   = row[3]
                previous   = row[4]
                old_prev   = row[5]
                actual     = row[6]
                full_date  = row[7]
                event_type = row[8]

                if event_type in SKIP_EVENT_TYPES or currency is None:
                    continue

                event_id = url_to_event_id.get(url) if url else None
                if event_id is None:
                    skipped_no_event_id += 1
                    continue

                fcd, scd, rcd = classify_event(forecast, previous, old_prev,
                                               actual, DIRECTION_THRESHOLD)
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

            # Логируем статистику по датам
            if GLOBAL_CAL_BY_DT:
                all_dates = sorted(GLOBAL_CAL_BY_DT.keys())
                log(f"  Диапазон дат в GLOBAL_CAL_BY_DT: {all_dates[0]} .. {all_dates[-1]}", NODE_NAME)
                log(f"  Всего уникальных дат: {len(all_dates)}", NODE_NAME)
                log(f"  Последние 5 дат: {all_dates[-5:]}", NODE_NAME)

    except Exception as e:
        log(f"❌ brain_calendar (from engine_brain) error: {e}", NODE_NAME, level="error")

    # Загрузка рыночных данных из engine_brain
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
                for typ in ("min", "max"):
                    op = ">" if typ == "max" else "<"
                    q  = f"""
                        SELECT t1.date FROM `{table}` t1
                        JOIN `{table}` t_prev ON t_prev.date = t1.date - INTERVAL 1 DAY
                        JOIN `{table}` t_next ON t_next.date = t1.date + INTERVAL 1 DAY
                        WHERE t1.`{typ}` {op} t_prev.`{typ}` AND t1.`{typ}` {op} t_next.`{typ}`
                    """
                    res_ext = await conn.execute(text(q))
                    GLOBAL_EXTREMUMS[table][typ] = {r["date"] for r in res_ext.mappings().all()}
            log(f"  {table}: {len(GLOBAL_RATES[table])} candles, диапазон дат: {min(GLOBAL_RATES[table].keys()) if GLOBAL_RATES[table] else 'нет'} .. {max(GLOBAL_RATES[table].keys()) if GLOBAL_RATES[table] else 'нет'}", NODE_NAME)
        except Exception as e:
            log(f"❌ {table}: {e}", NODE_NAME, level="error")

    # Загрузка SERVICE_URL с обработкой ошибок
    try:
        SERVICE_URL = await load_service_url(engine_super, SERVICE_ID)
        log(f"  SERVICE_URL loaded: {SERVICE_URL}", NODE_NAME)
    except OperationalError as e:
        log(f"❌ Failed to load service URL (OperationalError): {e}", NODE_NAME, level="error", force=True)
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

    for name, engine in [("vlad", engine_vlad), ("brain", engine_brain), ("super", engine_super)]:
        try:
            await engine.dispose()
        except Exception as e:
            log(f"Error disposing engine_{name}: {e}", NODE_NAME, level="error")


app = FastAPI(lifespan=lifespan)


async def calculate_pure_memory(pair: int, day: int, date_str: str,
                                calc_type: int = 0, calc_var: int = 0) -> dict | None:
    log(f"\n{'=' * 60}", NODE_NAME)
    log(f"📊 calculate_pure_memory START", NODE_NAME)
    log(f"  Parameters: pair={pair}, day={day}, date_str='{date_str}', type={calc_type}, var={calc_var}", NODE_NAME)

    target_date = parse_date_string(date_str)
    if not target_date:
        log(f"  ❌ Invalid date string: {date_str}", NODE_NAME)
        return None

    log(f"  Parsed target_date: {target_date}", NODE_NAME)

    rates_table = get_rates_table_name(pair, day)
    modification = get_modification_factor(pair)
    delta_unit = timedelta(days=1) if day == 1 else timedelta(hours=1)

    log(f"  rates_table='{rates_table}'", NODE_NAME)
    log(f"  modification={modification}", NODE_NAME)
    log(f"  delta_unit={delta_unit} (1 {'day' if day == 1 else 'hour'})", NODE_NAME)
    log(f"  SHIFT_WINDOW={SHIFT_WINDOW}", NODE_NAME)

    # Формируем окно поиска
    check_dts = [target_date + delta_unit * s for s in range(-SHIFT_WINDOW, SHIFT_WINDOW + 1)]
    log(f"  Окно поиска (check_dts):", NODE_NAME)
    log(f"    Всего дат: {len(check_dts)}", NODE_NAME)
    log(f"    Начало: {check_dts[0]}", NODE_NAME)
    log(f"    Конец: {check_dts[-1]}", NODE_NAME)
    log(f"    Дата target_date в окне: {target_date} (индекс {SHIFT_WINDOW})", NODE_NAME)

    # Статистика по GLOBAL_CAL_BY_DT
    if GLOBAL_CAL_BY_DT:
        all_dates = sorted(GLOBAL_CAL_BY_DT.keys())
        log(f"  GLOBAL_CAL_BY_DT статистика:", NODE_NAME)
        log(f"    Всего уникальных дат: {len(all_dates)}", NODE_NAME)
        log(f"    Диапазон: {all_dates[0]} .. {all_dates[-1]}", NODE_NAME)
        log(f"    Последние 10 дат: {all_dates[-10:]}", NODE_NAME)

        # Находим ближайшие даты к target_date
        dates_before = [d for d in all_dates if d <= target_date]
        dates_after = [d for d in all_dates if d > target_date]
        log(f"    Дат <= target_date: {len(dates_before)}", NODE_NAME)
        log(f"    Дат > target_date: {len(dates_after)}", NODE_NAME)
        if dates_before:
            log(f"    Последняя дата <= target_date: {dates_before[-1]}", NODE_NAME)
        if dates_after:
            log(f"    Первая дата > target_date: {dates_after[0]}", NODE_NAME)
    else:
        log(f"  ⚠️ GLOBAL_CAL_BY_DT ПУСТ!", NODE_NAME)

    # Поиск событий в диапазонах, НО ТОЛЬКО ПРОШЛЫХ событий
    observations = []
    dates_with_events = []

    if day == 0:  # Часовые данные
        for dt in check_dts:
            if dt > target_date:
                continue

            # Определяем интервал: [dt, min(dt + 1 час, target_date)]
            dt_end = min(dt + timedelta(hours=1), target_date + timedelta(microseconds=1))

            # Находим все даты событий в этом интервале
            matching_dates = []
            for event_dt in GLOBAL_CAL_BY_DT.keys():
                if dt <= event_dt < dt_end:
                    matching_dates.append(event_dt)

            if matching_dates:
                log(f"    📅 Найдены события в интервале {dt} - {dt_end}: {len(matching_dates)} дат", NODE_NAME)
                for event_dt in matching_dates:
                    for ctx_key in GLOBAL_CAL_BY_DT[event_dt]:
                        shift_steps = round((target_date - event_dt) / delta_unit)
                        observations.append((ctx_key, event_dt, shift_steps))
                        if event_dt not in dates_with_events:
                            dates_with_events.append(event_dt)

    else:  # Дневные данные (day == 1)
        for dt in check_dts:
            if dt > target_date:
                continue

            # Определяем интервал: [dt, min(dt + 1 день, target_date)]
            dt_end = min(dt + timedelta(days=1), target_date + timedelta(microseconds=1))

            # Находим все даты событий в этом интервале
            matching_dates = []
            for event_dt in GLOBAL_CAL_BY_DT.keys():
                if dt <= event_dt < dt_end:
                    matching_dates.append(event_dt)

            if matching_dates:
                log(f"    📅 Найдены события в интервале {dt} - {dt_end}: {len(matching_dates)} дат", NODE_NAME)
                for event_dt in matching_dates:
                    for ctx_key in GLOBAL_CAL_BY_DT[event_dt]:
                        shift_steps = round((target_date - event_dt) / delta_unit)
                        observations.append((ctx_key, event_dt, shift_steps))
                        if event_dt not in dates_with_events:
                            dates_with_events.append(event_dt)

    log(f"\n  Результаты поиска событий:", NODE_NAME)
    log(f"    Дат с событиями в окне: {len(dates_with_events)}", NODE_NAME)
    if dates_with_events:
        log(f"    Даты с событиями: {dates_with_events}", NODE_NAME)
    log(f"    Всего observations: {len(observations)}", NODE_NAME)

    if not observations:
        log(f"  ❌ Нет observations! Возвращаем пустой результат", NODE_NAME)
        log(f"{'=' * 60}\n", NODE_NAME)
        return {}

    # Рыночные данные
    ram_rates = GLOBAL_RATES.get(rates_table, {})
    ram_ranges = GLOBAL_CANDLE_RANGES.get(rates_table, {})
    avg_range = GLOBAL_AVG_RANGE.get(rates_table, 0.0)
    ram_ext = GLOBAL_EXTREMUMS.get(rates_table, {"min": set(), "max": set()})
    prev_candle = find_prev_candle_trend(rates_table, target_date)

    log(f"\n  Рыночные данные для {rates_table}:", NODE_NAME)
    log(f"    ram_rates размер: {len(ram_rates)}", NODE_NAME)
    if ram_rates:
        log(f"    ram_rates диапазон: {min(ram_rates.keys())} .. {max(ram_rates.keys())}", NODE_NAME)
    log(f"    ram_ranges размер: {len(ram_ranges)}", NODE_NAME)
    log(f"    avg_range: {avg_range}", NODE_NAME)
    log(f"    extremums min: {len(ram_ext['min'])}, max: {len(ram_ext['max'])}", NODE_NAME)
    log(f"    prev_candle: {prev_candle}", NODE_NAME)

    # Обработка наблюдений
    result = {}
    processed_keys = 0
    skipped_not_recurring = 0
    skipped_shift_out_of_range = 0
    skipped_no_history = 0
    skipped_no_ctx_info = 0

    for ctx_key, obs_dt, shift in observations:
        # Дополнительная проверка: убеждаемся, что obs_dt НЕ в будущем
        if obs_dt > target_date:
            log(f"    ⚠️ Пропускаем будущее событие: {obs_dt} > {target_date}", NODE_NAME)
            continue

        processed_keys += 1
        event_id, currency, importance, fcd, scd, rcd = ctx_key

        ctx_info = GLOBAL_CAL_CTX_INDEX.get(ctx_key)
        if ctx_info is None:
            if processed_keys <= 10:
                log(f"    ⚠️ ctx_key {ctx_key} not in GLOBAL_CAL_CTX_INDEX", NODE_NAME)
            skipped_no_ctx_info += 1
            continue

        is_recurring = ctx_info["occurrence_count"] >= RECURRING_MIN_COUNT

        if not is_recurring and shift != 0:
            skipped_not_recurring += 1
            continue
        if is_recurring and abs(shift) > SHIFT_WINDOW:
            skipped_shift_out_of_range += 1
            continue

        all_ctx_dts = GLOBAL_CAL_CTX_HIST.get(ctx_key, [])
        idx = bisect.bisect_left(all_ctx_dts, target_date)
        valid_dts = all_ctx_dts[:idx]

        if not valid_dts:
            skipped_no_history += 1
            continue

        t_dates = [d + delta_unit * shift for d in valid_dts if d + delta_unit * shift < target_date]
        shift_arg = shift if is_recurring else None

        if calc_type in (0, 1):
            t1_sum = compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)
            wc = make_weight_code(event_id, currency, importance, fcd, scd, rcd, 0, shift_arg)
            result[wc] = result.get(wc, 0.0) + t1_sum
            if t1_sum != 0 and processed_keys <= 10:
                log(f"    ✓ {wc}: t1_sum={t1_sum:.6f}, total={result[wc]:.6f}", NODE_NAME)

        if calc_type in (0, 2) and prev_candle:
            _, is_bull = prev_candle
            ext_set = ram_ext["max" if is_bull else "min"]
            ext_val = compute_extremum_value(t_dates, calc_var, ext_set, ram_ranges,
                                             avg_range, modification, len(valid_dts))
            if ext_val is not None:
                wc = make_weight_code(event_id, currency, importance, fcd, scd, rcd, 1, shift_arg)
                result[wc] = result.get(wc, 0.0) + ext_val
                if ext_val != 0 and processed_keys <= 10:
                    log(f"    ✓ {wc}: ext_val={ext_val:.6f}, total={result[wc]:.6f}", NODE_NAME)

    log(f"\n  ИТОГОВАЯ СТАТИСТИКА:", NODE_NAME)
    log(f"    Всего observations: {len(observations)}", NODE_NAME)
    log(f"    Обработано keys: {processed_keys}", NODE_NAME)
    log(f"    Пропущено (нет ctx_info): {skipped_no_ctx_info}", NODE_NAME)
    log(f"    Пропущено (not recurring + shift !=0): {skipped_not_recurring}", NODE_NAME)
    log(f"    Пропущено (shift out of range): {skipped_shift_out_of_range}", NODE_NAME)
    log(f"    Пропущено (нет истории до target_date): {skipped_no_history}", NODE_NAME)
    log(f"    Результат: {len(result)} weight_code(s)", NODE_NAME)
    if result:
        if len(result) <= 20:
            log(f"    Ключи результата: {list(result.keys())}", NODE_NAME)
            log(f"    Значения: {list(result.values())}", NODE_NAME)
        else:
            log(f"    Ключей результата слишком много ({len(result)}), не вывожу все", NODE_NAME)
    else:
        log(f"    ⚠️ РЕЗУЛЬТАТ ПУСТОЙ!", NODE_NAME)

    log(f"{'=' * 60}\n", NODE_NAME)

    return {k: round(v, 6) for k, v in result.items() if v != 0}


@app.get("/")
async def get_metadata():
    db_status = {}

    for db_name, engine in [("vlad", engine_vlad), ("brain", engine_brain), ("super", engine_super)]:
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            db_status[db_name] = "ok"
        except Exception as e:
            db_status[db_name] = f"error: {str(e)[:100]}"

    async with engine_vlad.connect() as conn:
        try:
            res = await conn.execute(text(
                "SELECT version FROM version_microservice WHERE microservice_id = :id"),
                {"id": SERVICE_ID})
            row = res.fetchone()
            version = row[0] if row else 0
        except Exception:
            version = 0

    # Собираем статистику по датам
    date_stats = {}
    if GLOBAL_CAL_BY_DT:
        all_dates = sorted(GLOBAL_CAL_BY_DT.keys())
        date_stats = {
            "total_dates": len(all_dates),
            "first_date": str(all_dates[0]),
            "last_date": str(all_dates[-1]),
            "last_5_dates": [str(d) for d in all_dates[-5:]]
        }

    return {
        "status": "ok", "version": f"1.{version}.0", "mode": MODE,
        "name":   NODE_NAME,
        "text":   "Calculates historical calendar weights keyed by event context",
        "weight_code_format": "E{event_id}_{currency}_{imp}_{fcd}_{scd}_{rcd}_{mode}[_{hour_shift}]",
        "metadata": {
            "ctx_index_rows":    len(GLOBAL_CAL_CTX_INDEX),
            "weight_codes":      len(GLOBAL_WEIGHT_CODES),
            "calendar_contexts": len(GLOBAL_CAL_CTX_HIST),
            "last_reload":       LAST_RELOAD_TIME.isoformat() if LAST_RELOAD_TIME else None,
            "db_status":         db_status,
            "brain_calendar_source": "engine_brain",
            "date_stats":        date_stats,
            "shift_window":      SHIFT_WINDOW,
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
    code: str = Query(...),
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
                    OR (event_id = :event_id AND currency_code = :currency_code AND importance > :importance)
                    OR (event_id = :event_id AND currency_code = :currency_code AND importance = :importance AND forecast_dir > :forecast_dir)
                    OR (event_id = :event_id AND currency_code = :currency_code AND importance = :importance AND forecast_dir = :forecast_dir AND surprise_dir > :surprise_dir)
                    OR (event_id = :event_id AND currency_code = :currency_code AND importance = :importance AND forecast_dir = :forecast_dir AND surprise_dir = :surprise_dir AND revision_dir > :revision_dir)
                    OR (event_id = :event_id AND currency_code = :currency_code AND importance = :importance AND forecast_dir = :forecast_dir AND surprise_dir = :surprise_dir AND revision_dir = :revision_dir AND mode_val > :mode_val)
                    OR (event_id = :event_id AND currency_code = :currency_code AND importance = :importance AND forecast_dir = :forecast_dir AND surprise_dir = :surprise_dir AND revision_dir = :revision_dir AND mode_val = :mode_val AND COALESCE(hour_shift, -999999) > :hs)
                ORDER BY event_id, currency_code, importance, forecast_dir, surprise_dir, revision_dir, mode_val,
                         hour_shift IS NULL, hour_shift
                LIMIT :limit
            """), {
                "event_id":     event_id,
                "currency_code": currency_code,
                "importance":   importance,
                "forecast_dir": forecast_dir,
                "surprise_dir": surprise_dir,
                "revision_dir": revision_dir,
                "mode_val":     mode_val,
                "hs":           hs_val,
                "limit":        limit,
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
                {"id": SERVICE_ID})
            row = res.fetchone()
            if not row:
                raise HTTPException(status_code=500, detail=f"Service ID {SERVICE_ID} not found")
            old = row[0]; new = max(old, 1)
            if new != old:
                await conn.execute(
                    text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                    {"v": new, "id": SERVICE_ID})
        except OperationalError as e:
            log(f"Database connection error in patch_service: {e}", NODE_NAME, level="error")
            raise HTTPException(status_code=503, detail=f"Database connection error: {str(e)}")
    return {"status": "ok", "from_version": old, "to_version": new}


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
