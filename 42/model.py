"""
model.py — service 33, calendar weights + reverse-learning ML
OPTIMIZED VERSION v2
"""

from __future__ import annotations

import bisect
import os
from datetime import datetime, timedelta
from typing import Any

import numpy as _np  # перенесён на уровень модуля — убирает повторный импорт в hot path


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

# OPT-1: прямые dict вместо двойного .get() через _encode()
_FORECAST_CHARS   = {"UNKNOWN": "X", "BEAT": "B", "MISS": "M", "INLINE": "I"}
_SURPRISE_CHARS   = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}
_REVISION_CHARS   = {"NONE": "N", "FLAT": "T", "UP": "U", "DOWN": "D", "UNKNOWN": "X"}
_IMPORTANCE_CHARS = {"high": "H", "medium": "M", "low": "L", "none": "N"}

# Обратная совместимость
FORECAST_MAP   = _FORECAST_CHARS
SURPRISE_MAP   = _SURPRISE_CHARS
REVISION_MAP   = _REVISION_CHARS
IMPORTANCE_MAP = _IMPORTANCE_CHARS

# OPT-2: кеш weight_code — одна и та же комбинация встречается на каждой свече
_wc_cache: dict[tuple, str] = {}

# Sentinel для кеширования None в _ctx_key_cache.
# Определён ДО функций, которые его используют.
_CTX_NONE_SENTINEL: object = object()

# OPT-3: кеш ctx_key — для одного и того же события (url+валюта+значения)
# результат classify_event всегда одинаковый, кешируем по ключу события
_ctx_key_cache: dict[tuple, Any] = {}

# Кеш fallback url_map: пересчитывается при изменении ctx_index.
# Ключ — id(ctx_index), значение — готовый dict.
_fallback_url_map_cache: dict[int, dict] = {}


def _dbg(msg: str) -> None:
    print(f"[MODEL_DEBUG] {msg}", flush=True)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


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
    """OPT-2: результат кешируется по полной комбинации аргументов."""
    key = (event_id, currency, importance, forecast_dir, surprise_dir, revision_dir, mode, hour_shift)
    cached = _wc_cache.get(key)
    if cached is not None:
        return cached

    imp_c = _IMPORTANCE_CHARS.get(importance, "N")
    fcd_c = _FORECAST_CHARS.get(forecast_dir, "X")
    scd_c = _SURPRISE_CHARS.get(surprise_dir, "X")
    rcd_c = _REVISION_CHARS.get(revision_dir, "X")
    base   = f"E{event_id}_{currency}_{imp_c}_{fcd_c}_{scd_c}_{rcd_c}_{mode}"
    result = base if hour_shift is None else f"{base}_{hour_shift}"
    _wc_cache[key] = result
    return result


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
        if actual_f > 0: return up_label
        if actual_f < 0: return down_label
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
    """OPT-3: кеш по (url, currency, forecast, previous, old_previous, actual)."""
    url      = row.get("url")
    currency = row.get("currency_code")
    if not url or not currency:
        return None

    cache_key = (
        url, currency,
        row.get("forecast_value"), row.get("previous_value"),
        row.get("old_previous_value"), row.get("actual_value"),
    )
    cached = _ctx_key_cache.get(cache_key)
    if cached is not None:
        return None if cached is _CTX_NONE_SENTINEL else cached

    event_id = url_map.get(url)
    if event_id is None:
        _ctx_key_cache[cache_key] = _CTX_NONE_SENTINEL
        return None

    forecast_dir, surprise_dir, revision_dir = classify_event(
        row.get("forecast_value"),
        row.get("previous_value"),
        row.get("old_previous_value"),
        row.get("actual_value"),
    )
    result = (
        int(event_id),
        str(currency),
        str(row.get("importance") or "none").lower(),
        forecast_dir,
        surprise_dir,
        revision_dir,
    )
    _ctx_key_cache[cache_key] = result
    return result


def _is_daily_rates(rates: list[dict]) -> bool:
    if not rates:
        return False
    dt = rates[-1].get("date")
    return isinstance(dt, datetime) and dt.hour == 0 and dt.minute == 0


def _resolve_url_map(ctx_index: dict, url_map: dict) -> dict:
    """
    Возвращает url_map или fallback из ctx_index.
    Результат fallback кешируется по id(ctx_index) — не пересчитывается
    при каждом вызове model().
    """
    if url_map:
        return url_map
    cid = id(ctx_index)
    cached_fb = _fallback_url_map_cache.get(cid)
    if cached_fb is not None:
        return cached_fb
    fallback: dict = {}
    for key, val in ctx_index.items():
        u   = val.get("url") or val.get("Url")
        eid = key[0] if key else None
        if u and eid:
            fallback[u] = eid
    _fallback_url_map_cache[cid] = fallback
    return fallback


def LABEL_FN(key: tuple) -> str | None:
    return None


# ══════════════════════════════════════════════════════════════════════════════
# _build_resolved_events — ядро batch_model
# ══════════════════════════════════════════════════════════════════════════════

def _build_resolved_events(
    dataset: list[dict],
    url_map: dict,
    ctx_index: dict,
) -> tuple[list[float], list[tuple]]:
    """
    Однократная подготовка датасета для пакетной обработки.

    Возвращает два списка (отсортированных по event_ts):
        ts_list       — unix-timestamp каждого события
        resolved_list — (ctx_key, occurrence_count) для каждого события

    После этого для любой target_date достаточно bisect по ts_list
    и прохода по срезу resolved_list — без повторного парсинга событий.
    """
    items: list[tuple[float, tuple, int]] = []
    for ev in dataset:
        event_time = ev.get("event_time") or ev.get("date")
        if not isinstance(event_time, datetime):
            continue
        ctx_key = _ctx_key_from_event(ev, url_map)
        if ctx_key is None:
            continue
        ctx_info = ctx_index.get(ctx_key)
        if ctx_info is None:
            continue
        items.append((
            event_time.timestamp(),
            ctx_key,
            int(ctx_info.get("occurrence_count") or 0),
        ))

    # Датасет уже отсортирован по FullDate — сортировка для надёжности.
    items.sort(key=lambda x: x[0])
    ts_list       = [x[0] for x in items]
    resolved_list = [(x[1], x[2]) for x in items]
    return ts_list, resolved_list


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

    # ── 1. Базовые проверки ───────────────────────────────────────────────────
    calc_type = int(type or 0)
    calc_var  = int(var  or 0)

    if calc_type not in TYPES_RANGE: return {}
    if calc_var  not in VAR_RANGE:   return {}
    if not rates:   return {}
    if not dataset: return {}
    if date is None: return {}

    # ── 2. ctx_index и url_map ────────────────────────────────────────────────
    di        = dataset_index or {}
    ctx_index = di.get("ctx_index") or {}
    if not ctx_index:
        return {}

    url_map = _resolve_url_map(ctx_index, di.get("url_map") or {})
    if not url_map:
        return {}

    # ── 3. Параметры окна ─────────────────────────────────────────────────────
    is_daily      = _is_daily_rates(rates)
    secs_per_unit = 86400.0 if is_daily else 3600.0
    window_secs   = SHIFT_WINDOW * secs_per_unit

    # ── 4. OPT-4: searchsorted по dataset_timestamps (numpy, без fallback-цикла) ──
    date_ts = date.timestamp()
    ws_ts   = date_ts - window_secs
    we_ts   = date_ts + window_secs

    ts_arr = di.get("dataset_timestamps")
    if ts_arr is not None and len(ts_arr) > 0:
        lo = int(_np.searchsorted(ts_arr, ws_ts, side="left"))
        hi = int(_np.searchsorted(ts_arr, we_ts, side="right"))
        window_events = dataset[lo:hi]
    else:
        unit         = timedelta(seconds=secs_per_unit)
        window_start = date - unit * SHIFT_WINDOW
        window_end   = date + unit * SHIFT_WINDOW
        window_events = [
            e for e in dataset
            if window_start <= (e.get("event_time") or e.get("date") or datetime.min) <= window_end
        ]

    if not window_events:
        return {}

    # ── 5. Горячий цикл ───────────────────────────────────────────────────────
    result: dict[str, float] = {}

    for event in window_events:
        event_time = event.get("event_time") or event.get("date")
        if not isinstance(event_time, datetime):
            continue

        ctx_key = _ctx_key_from_event(event, url_map)
        if ctx_key is None:
            continue

        ctx_info = ctx_index.get(ctx_key)
        if ctx_info is None:
            continue

        # OPT-5: timestamp-арифметика вместо timedelta.total_seconds()
        shift = int(round((date_ts - event_time.timestamp()) / secs_per_unit))
        if abs(shift) > SHIFT_WINDOW:
            continue

        event_id, currency, importance, fcd, scd, rcd = ctx_key

        for mode in ACTIVE_MODES:
            result[make_weight_code(event_id, currency, importance, fcd, scd, rcd, mode, None)] = 1.0

        if int(ctx_info.get("occurrence_count") or 0) >= RECURRING_MIN_COUNT:
            for mode in ACTIVE_MODES:
                result[make_weight_code(event_id, currency, importance, fcd, scd, rcd, mode, shift)] = 1.0

    return result


# ══════════════════════════════════════════════════════════════════════════════
# batch_model() — пакетный режим для fill_cache (O(D + N) вместо O(N × D))
# ══════════════════════════════════════════════════════════════════════════════

def batch_model(
    rates: list[dict],
    dataset: list[dict],
    dates: list[datetime],
    *,
    type: int = 0,
    var: int = 3,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[datetime, dict[str, float]]:
    """
    Пакетная версия model() для fill_cache.

    Оптимизация по сравнению с N вызовами model():
      • _build_resolved_events() парсит датасет один раз: O(D)
        (resolve ctx_key, lookup ctx_info, сортировка)
      • Для каждой из N дат — bisect по ts_list + проход по узкому срезу: O(N·W)
        где W — среднее количество событий в окне (обычно 5–30)
      • Итого: O(D + N·W) вместо O(N·(log(D) + W·lookup))

    Особенно выгодно при USE_ML_VALUES=False (fill_cache вызывает batch_model
    напрямую). При USE_ML_VALUES=True используется как утилита или при
    прямом тестировании.

    Возвращает:
        {date: {"weight_code": 1.0, ...}}  — если есть сигналы
        {date: {}}                          — если FLAT / нет совпадений
    """
    if not dates:
        return {}

    calc_type = int(type or 0)
    calc_var  = int(var  or 0)

    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {d: {} for d in dates}
    if not rates or not dataset:
        return {d: {} for d in dates}

    # ── Контекст ──────────────────────────────────────────────────────────────
    di        = dataset_index or {}
    ctx_index = di.get("ctx_index") or {}
    if not ctx_index:
        return {d: {} for d in dates}

    url_map = _resolve_url_map(ctx_index, di.get("url_map") or {})
    if not url_map:
        return {d: {} for d in dates}

    # ── Параметры окна ────────────────────────────────────────────────────────
    is_daily      = _is_daily_rates(rates)
    secs_per_unit = 86400.0 if is_daily else 3600.0
    window_secs   = SHIFT_WINDOW * secs_per_unit

    # ── Однократный resolve всего датасета ────────────────────────────────────
    ts_list, resolved_list = _build_resolved_events(dataset, url_map, ctx_index)

    # ── Цикл по датам ─────────────────────────────────────────────────────────
    results: dict[datetime, dict[str, float]] = {}

    for date in dates:
        date_ts = date.timestamp()
        ws_ts   = date_ts - window_secs
        we_ts   = date_ts + window_secs

        lo = bisect.bisect_left(ts_list,  ws_ts)
        hi = bisect.bisect_right(ts_list, we_ts)

        result: dict[str, float] = {}
        for idx in range(lo, hi):
            ev_ts               = ts_list[idx]
            ctx_key, occ_count  = resolved_list[idx]

            shift = int(round((date_ts - ev_ts) / secs_per_unit))
            if abs(shift) > SHIFT_WINDOW:
                continue

            event_id, currency, importance, fcd, scd, rcd = ctx_key

            for mode in ACTIVE_MODES:
                result[make_weight_code(event_id, currency, importance, fcd, scd, rcd, mode, None)] = 1.0

            if occ_count >= RECURRING_MIN_COUNT:
                for mode in ACTIVE_MODES:
                    result[make_weight_code(event_id, currency, importance, fcd, scd, rcd, mode, shift)] = 1.0

        results[date] = result

    return results
