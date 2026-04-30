"""
model.py — service 33, calendar weights + reverse-learning ML
DEBUG VERSION — подробные логи на каждом шаге model()
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any


SERVICE_ID = 42
PORT = 8904
NODE_NAME = "brain-vlad_MQL-reverse-learning-s42"
SERVICE_TEXT = "vlad_MQL-reverse-learning events microservice"

RATES_TABLE = os.getenv("RATES_TABLE", "brain_rates_eur_usd")

CTX_TABLE = os.getenv("CTX_TABLE", "brain_calendar_context_idx")
WEIGHTS_TABLE = os.getenv("WEIGHTS_TABLE", "brain_calendar_weights")
WEIGHTS_CODE_COLUMN = "weight_code"

CTX_KEY_COLUMNS = [
    "event_id",
    "currency_code",
    "importance",
    "forecast_dir",
    "surprise_dir",
    "revision_dir",
]

DATASET_ENGINE = os.getenv("DATASET_ENGINE", "brain")

DATASET_QUERY = """
    SELECT
        Url              AS url,
        CurrencyCode     AS currency_code,
        Importance       AS importance,
        ForecastValue    AS forecast_value,
        PreviousValue    AS previous_value,
        OldPreviousValue AS old_previous_value,
        ActualValue      AS actual_value,
        FullDate         AS event_time,
        FullDate         AS date,
        EventType        AS event_type
    FROM brain_calendar
    WHERE ActualValue IS NOT NULL
      AND Processed = 1
      AND FullDate IS NOT NULL
      AND Url IS NOT NULL
      AND CurrencyCode IS NOT NULL
      AND (EventType IS NULL OR EventType NOT IN (2))
    ORDER BY FullDate
"""

FILTER_DATASET_BY_DATE = True
DATASET_KEY = "url"

URL_MAP_QUERY = """
    SELECT Url AS url, EventId AS event_id
    FROM brain_calendar
    WHERE Url IS NOT NULL AND EventId IS NOT NULL
    GROUP BY Url, EventId
"""
URL_MAP_ENGINE = os.getenv("URL_MAP_ENGINE", "vlad")

SHIFT_WINDOW = int(os.getenv("SHIFT_WINDOW", "12"))

TYPES_RANGE = [0, 1, 2, 3, 4]
VAR_RANGE = [3, 5, 7, 9]

CACHE_DATE_FROM = os.getenv("CACHE_DATE_FROM", "2025-01-15")
RELOAD_INTERVAL = int(os.getenv("RELOAD_INTERVAL", "3600"))
REBUILD_INTERVAL = int(os.getenv("REBUILD_INTERVAL", "7200"))

USE_ML_VALUES = True
ML_TARGET_PRECISION = float(os.getenv("ML_TARGET_PRECISION", "0.95"))
ML_MAX_ITER = int(os.getenv("ML_MAX_ITER", "20"))
ML_STEP = float(os.getenv("ML_STEP", "0.10"))
ML_EXTREMUM_LIMIT = int(os.getenv("ML_EXTREMUM_LIMIT", "50"))
ML_ACTIVE_TAIL = int(os.getenv("ML_ACTIVE_TAIL", "0"))
ML_PRECISION_METRIC = os.getenv("ML_PRECISION_METRIC", "mean")

DIRECTION_THRESHOLD = float(os.getenv("DIRECTION_THRESHOLD", "0.01"))
RECURRING_MIN_COUNT = int(os.getenv("RECURRING_MIN_COUNT", "2"))

_ACTIVE_WEIGHT_MODES_RAW = os.getenv("ACTIVE_WEIGHT_MODES", "0")
ACTIVE_MODES = tuple(
    int(x.strip())
    for x in _ACTIVE_WEIGHT_MODES_RAW.split(",")
    if x.strip() != ""
)

FORECAST_MAP  = {"UNKNOWN": "X", "BEAT": "B", "MISS": "M", "INLINE": "I"}
SURPRISE_MAP  = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}
REVISION_MAP  = {"NONE": "N", "FLAT": "T", "UP": "U", "DOWN": "D", "UNKNOWN": "X"}
IMPORTANCE_MAP = {"high": "H", "medium": "M", "low": "L", "none": "N"}

# ── счётчик вызовов для ограничения вывода логов ──────────────────────────────
_DEBUG_CALL_COUNT = 0
_DEBUG_MAX_FULL_CALLS = 3   # подробный лог только для первых N вызовов


def _dbg(msg: str) -> None:
    print(f"[MODEL_DEBUG] {msg}", flush=True)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _encode(value: str, kind: str) -> str:
    maps = {
        "forecast": FORECAST_MAP,
        "surprise": SURPRISE_MAP,
        "revision": REVISION_MAP,
        "importance": IMPORTANCE_MAP,
    }
    return maps.get(kind, {}).get(value, "X")


def make_weight_code(
    event_id: int,
    currency: str,
    importance: str,
    forecast_dir: str,
    surprise_dir: str,
    revision_dir: str,
    mode: int,
    hour_shift: int | None = None,
) -> str:
    imp_c = _encode(importance, "importance")
    fcd_c = _encode(forecast_dir, "forecast")
    scd_c = _encode(surprise_dir, "surprise")
    rcd_c = _encode(revision_dir, "revision")
    base = f"E{event_id}_{currency}_{imp_c}_{fcd_c}_{scd_c}_{rcd_c}_{mode}"
    return base if hour_shift is None else f"{base}_{hour_shift}"


def _rel_direction(
    actual: Any,
    reference: Any,
    threshold: float = DIRECTION_THRESHOLD,
    *,
    up_label: str = "UP",
    down_label: str = "DOWN",
    flat_label: str = "FLAT",
) -> str:
    actual_f    = _to_float(actual)
    reference_f = _to_float(reference)
    if actual_f is None or reference_f is None:
        return "UNKNOWN"
    if reference_f == 0:
        if actual_f > 0:   return up_label
        if actual_f < 0:   return down_label
        return flat_label
    pct = (actual_f - reference_f) / abs(reference_f)
    if pct >  threshold: return up_label
    if pct < -threshold: return down_label
    return flat_label


def classify_event(
    forecast: Any,
    previous: Any,
    old_previous: Any,
    actual: Any,
) -> tuple[str, str, str]:
    forecast_f = _to_float(forecast)
    if forecast_f is None or forecast_f == 0:
        forecast_dir = "UNKNOWN"
    else:
        forecast_dir = _rel_direction(
            actual, forecast,
            up_label="BEAT", down_label="MISS", flat_label="INLINE",
        )
    surprise_dir = _rel_direction(actual, previous)
    old_prev_f   = _to_float(old_previous)
    previous_f   = _to_float(previous)
    if old_prev_f is None or old_prev_f == 0 or previous_f is None:
        revision_dir = "NONE"
    elif previous_f == old_prev_f:
        revision_dir = "FLAT"
    else:
        revision_dir = _rel_direction(previous, old_previous)
    return forecast_dir, surprise_dir, revision_dir


def _ctx_key_from_event(row: dict, url_map: dict) -> tuple | None:
    url      = row.get("url")
    currency = row.get("currency_code")
    if not url or not currency:
        return None
    event_id = url_map.get(url)
    if event_id is None:
        return None
    forecast_dir, surprise_dir, revision_dir = classify_event(
        row.get("forecast_value"),
        row.get("previous_value"),
        row.get("old_previous_value"),
        row.get("actual_value"),
    )
    return (
        int(event_id),
        str(currency),
        str(row.get("importance") or "none").lower(),
        forecast_dir,
        surprise_dir,
        revision_dir,
    )


def _is_daily_rates(rates: list[dict]) -> bool:
    if not rates:
        return False
    dt = rates[-1].get("date")
    return isinstance(dt, datetime) and dt.hour == 0 and dt.minute == 0


def LABEL_FN(key: tuple) -> str | None:
    return None


def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    *,
    type: int = 0,
    var: int = 3,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:

    global _DEBUG_CALL_COUNT
    _DEBUG_CALL_COUNT += 1
    call_n = _DEBUG_CALL_COUNT
    verbose = call_n <= _DEBUG_MAX_FULL_CALLS

    if verbose:
        _dbg(f"═══ CALL #{call_n}  date={date}  type={type}  var={var} ═══")

    # ── 1. Базовые проверки ───────────────────────────────────────────────────
    calc_type = int(type or 0)
    calc_var  = int(var  or 0)

    if calc_type not in TYPES_RANGE:
        if verbose: _dbg(f"EARLY EXIT: calc_type={calc_type} not in TYPES_RANGE={TYPES_RANGE}")
        return {}
    if calc_var not in VAR_RANGE:
        if verbose: _dbg(f"EARLY EXIT: calc_var={calc_var} not in VAR_RANGE={VAR_RANGE}")
        return {}
    if not rates:
        if verbose: _dbg("EARLY EXIT: rates is empty")
        return {}
    if not dataset:
        if verbose: _dbg("EARLY EXIT: dataset is empty")
        return {}
    if date is None:
        if verbose: _dbg("EARLY EXIT: date is None")
        return {}

    if verbose:
        _dbg(f"  rates={len(rates)} rows, dataset={len(dataset)} rows")

    # ── 2. dataset_index диагностика ──────────────────────────────────────────
    if verbose:
        if dataset_index is None:
            _dbg("  dataset_index=None ← фреймворк не передал индекс!")
        else:
            _dbg(f"  dataset_index keys: {list(dataset_index.keys())}")

    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if verbose:
        _dbg(f"  ctx_index: {len(ctx_index)} записей")
        if ctx_index:
            sample_key = next(iter(ctx_index))
            sample_val = ctx_index[sample_key]
            _dbg(f"  ctx_index sample key : {sample_key}")
            _dbg(f"  ctx_index sample val : {sample_val}")

    if not ctx_index:
        if verbose: _dbg("EARLY EXIT: ctx_index is empty")
        return {}

    # ── 3. url_map диагностика ────────────────────────────────────────────────
    url_map = (dataset_index or {}).get("url_map") or {}
    if verbose:
        _dbg(f"  url_map: {len(url_map)} записей")
        if not url_map:
            _dbg("  ⚠️ url_map ОТСУТСТВУЕТ в dataset_index — фреймворк его не передаёт!")
            _dbg("  Попытка построить url_map из ctx_index значений (fallback)...")

    # ── 3a. Fallback: строим url_map из значений ctx_index если там есть url ──
    if not url_map:
        fallback_url_map: dict = {}
        for key, val in ctx_index.items():
            u = val.get("url") or val.get("Url")
            eid = key[0] if key else None
            if u and eid:
                fallback_url_map[u] = eid
        if verbose:
            _dbg(f"  Fallback url_map из ctx_index: {len(fallback_url_map)} записей")
            if fallback_url_map:
                sample = list(fallback_url_map.items())[:2]
                _dbg(f"  Fallback sample: {sample}")
        if fallback_url_map:
            url_map = fallback_url_map

    if not url_map:
        if verbose:
            _dbg("  EARLY EXIT: url_map пуст — нет маппинга url→event_id")
            _dbg("  ДИАГНОЗ: фреймворк не поддерживает URL_MAP_QUERY.")
            _dbg("  ФИКС нужен в brain_framework.py: добавить url_map в dataset_index")
            # Покажем сколько уникальных url в датасете для понимания масштаба
            unique_urls = {r.get("url") for r in dataset if r.get("url")}
            _dbg(f"  Уникальных url в dataset: {len(unique_urls)}")
            if unique_urls:
                _dbg(f"  Примеры url: {list(unique_urls)[:3]}")
        return {}

    # ── 4. Окно дат ───────────────────────────────────────────────────────────
    is_daily     = _is_daily_rates(rates)
    unit         = timedelta(days=1) if is_daily else timedelta(hours=1)
    window_start = date - unit * SHIFT_WINDOW
    window_end   = date + unit * SHIFT_WINDOW

    if verbose:
        _dbg(f"  is_daily={is_daily}  window=[{window_start} … {window_end}]")

    # ── 5. Проход по датасету ─────────────────────────────────────────────────
    n_total          = 0
    n_no_event_time  = 0
    n_out_of_window  = 0
    n_no_ctx_key     = 0   # url не в url_map
    n_no_ctx_info    = 0   # ctx_key не в ctx_index
    n_shift_too_big  = 0
    n_added          = 0

    result: dict[str, float] = {}

    for event in dataset:
        n_total += 1
        event_time = event.get("event_time") or event.get("date")

        if not isinstance(event_time, datetime):
            n_no_event_time += 1
            continue

        if event_time < window_start or event_time > window_end:
            n_out_of_window += 1
            continue

        ctx_key = _ctx_key_from_event(event, url_map)
        if ctx_key is None:
            n_no_ctx_key += 1
            if verbose and n_no_ctx_key <= 3:
                _dbg(f"    no ctx_key: url={event.get('url')!r}  "
                     f"currency={event.get('currency_code')!r}")
            continue

        ctx_info = ctx_index.get(ctx_key)
        if ctx_info is None:
            n_no_ctx_info += 1
            if verbose and n_no_ctx_info <= 3:
                _dbg(f"    ctx_key not in ctx_index: {ctx_key}")
            continue

        occurrence_count = int(ctx_info.get("occurrence_count") or 0)
        is_recurring     = occurrence_count >= RECURRING_MIN_COUNT

        delta = date - event_time
        shift = int(round(delta.total_seconds() / (86400.0 if is_daily else 3600.0)))

        if abs(shift) > SHIFT_WINDOW:
            n_shift_too_big += 1
            continue

        event_id, currency, importance, fcd, scd, rcd = ctx_key

        for mode in ACTIVE_MODES:
            base_wc = make_weight_code(
                event_id=event_id, currency=currency, importance=importance,
                forecast_dir=fcd, surprise_dir=scd, revision_dir=rcd,
                mode=mode, hour_shift=None,
            )
            result[base_wc] = 1.0

        if is_recurring:
            for mode in ACTIVE_MODES:
                shift_wc = make_weight_code(
                    event_id=event_id, currency=currency, importance=importance,
                    forecast_dir=fcd, surprise_dir=scd, revision_dir=rcd,
                    mode=mode, hour_shift=shift,
                )
                result[shift_wc] = 1.0

        n_added += 1

    # ── 6. Итоговая сводка ────────────────────────────────────────────────────
    if verbose or (call_n <= 20 and n_added == 0):
        _dbg(f"  ИТОГ call #{call_n}:")
        _dbg(f"    dataset total         : {n_total}")
        _dbg(f"    нет event_time        : {n_no_event_time}")
        _dbg(f"    вне окна              : {n_out_of_window}")
        _dbg(f"    url не в url_map      : {n_no_ctx_key}")
        _dbg(f"    ctx_key не в ctx_index: {n_no_ctx_info}")
        _dbg(f"    shift > SHIFT_WINDOW  : {n_shift_too_big}")
        _dbg(f"    добавлено событий     : {n_added}")
        _dbg(f"    result keys           : {len(result)}")
        if n_added == 0:
            _dbg("  ❌ ПУСТО — смотри счётчики выше для диагноза")

    return result
