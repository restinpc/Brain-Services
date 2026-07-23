"""
Модель 28 — события экономического календаря Investing.

Перенесена со старого самостоятельного FastAPI-сервиса на Brain Framework.
Фреймворк теперь отвечает за загрузку данных, котировки, кеш, /values,
/weights, /new_weights, /fill_cache и обратное обучение в ML-режиме.

Код веса:
    {event_id}__{forecast_direction}__{surprise_direction}__
    {actual_direction}__{mode}[__{shift}]

mode=0 — историческая сумма T1;
mode=1 — доля совпавших экстремумов с направлением предыдущей свечи.

type=0 — оба режима, обратное обучение назад без амплитуды;
type=1 — только T1, обучение вперёд без амплитуды;
type=2 — только экстремумы, обучение назад с амплитудой;
type=3 — оба режима, обучение вперёд с амплитудой;
type=4 — оба режима, строгое обучение вперёд с амплитудой.

var=0 — все свечи, обычный T1;
var=1 — только свечи с диапазоном выше среднего;
var=2 — знаковый квадрат T1;
var=3 — знаковый квадрат T1 только на широких свечах;
var=4 — превышение диапазона свечи над средним диапазоном.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date as date_class
from datetime import datetime, time, timedelta
from typing import Any

import numpy as np

SERVICE_ID = 28
RATES_TABLE = "brain_rates_eur_usd"
WEIGHTS_TABLE = "vlad_investing_weights"
CTX_TABLE = "vlad_investing_event_context_idx"
CTX_KEY_COLUMNS = [
    "event_id",
    "forecast_direction",
    "surprise_direction",
    "actual_direction",
]

USE_ML_VALUES = True
FILTER_DATASET_BY_DATE = False
MODEL_CAN_FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
SHIFT_WINDOW = 12
VAR_RANGE = [0, 1, 2, 3, 4]
TYPES_RANGE = [0, 1, 2, 3, 4]

THRESHOLD = 0.0001
RECURRING_MIN_COUNT = 2

# Кеш подготовленного календаря. Dataset во фреймворке заменяется целиком при reload,
# поэтому сочетания id(dataset), len(dataset) достаточно для инвалидирования.
_PREPARED_KEY: tuple[int, int] | None = None
_PREPARED: dict[str, Any] = {}


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, date_class):
        return datetime.combine(value, time.min)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        if not text:
            return None
        try:
            result = datetime.fromisoformat(text)
        except ValueError:
            return None
        return result.replace(tzinfo=None) if result.tzinfo is not None else result
    return None


def _try_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def _direction(a: float | None, b: float | None, up: str, down: str, flat: str) -> str:
    if a is None or b is None:
        return "UNKNOWN"
    if a > b + THRESHOLD:
        return up
    if a < b - THRESHOLD:
        return down
    return flat


def _event_context(row: dict) -> tuple[str, str, str]:
    actual = _try_float(row.get("actual"))
    forecast = _try_float(row.get("forecast"))
    previous = _try_float(row.get("previous"))
    return (
        _direction(forecast, previous, "UP", "DOWN", "FLAT"),
        _direction(actual, forecast, "BEAT", "MISS", "INLINE"),
        _direction(actual, previous, "UP", "DOWN", "FLAT"),
    )


def _build_weight_code(
    event_id: int,
    forecast_direction: str,
    surprise_direction: str,
    actual_direction: str,
    mode: int,
    shift: int | None,
) -> str:
    base = (
        f"{event_id}__{forecast_direction}__{surprise_direction}__"
        f"{actual_direction}__{mode}"
    )
    return base if shift is None else f"{base}__{shift}"


def _prepare_dataset(dataset: list[dict]) -> dict[str, Any]:
    global _PREPARED_KEY, _PREPARED

    key = (id(dataset), len(dataset))
    if _PREPARED_KEY == key:
        return _PREPARED

    events: list[dict] = []
    history: dict[int, list[dict]] = {}
    for source in dataset:
        event_id = source.get("event_id")
        event_date = _as_datetime(source.get("date") or source.get("occurrence_time_utc"))
        try:
            event_id = int(event_id)
        except (TypeError, ValueError):
            continue
        if event_date is None:
            continue

        row = dict(source)
        row["event_id"] = event_id
        row["date"] = event_date
        row["context"] = _event_context(row)
        events.append(row)
        history.setdefault(event_id, []).append(row)

    events.sort(key=lambda item: item["date"])
    for rows in history.values():
        rows.sort(key=lambda item: item["date"])

    _PREPARED_KEY = key
    _PREPARED = {
        "events": events,
        "dates": [item["date"] for item in events],
        "history": history,
        "history_dates": {
            event_id: [item["date"] for item in rows]
            for event_id, rows in history.items()
        },
    }
    return _PREPARED


def _last_known_context(prepared: dict[str, Any], event_id: int, target: datetime):
    rows = prepared["history"].get(event_id, [])
    dates = prepared["history_dates"].get(event_id, [])
    index = bisect_left(dates, target) - 1
    if index < 0:
        return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    return rows[index]["context"]


def _ctx_lookup(ctx_index: dict, key: tuple) -> dict | None:
    info = ctx_index.get(key)
    if info is not None:
        return info
    # Защита от драйверов, возвращающих event_id строкой.
    alt = (str(key[0]), *key[1:])
    return ctx_index.get(alt)


def _rates_view(rates: list[dict], target: datetime, dataset_index: dict | None):
    np_rates = (dataset_index or {}).get("np_rates")
    if np_rates is not None and len(np_rates.get("dates_ns", [])):
        dates_ns = np_rates["dates_ns"]
        cut = int(np.searchsorted(dates_ns, int(target.timestamp()), side="right"))
        if cut <= 0:
            return None
        ranges = np_rates["ranges"][:cut]
        all_ranges = np_rates["ranges"]
        return {
            "dates_ns": dates_ns[:cut],
            "t1": np_rates["t1"][:cut],
            "ranges": ranges,
            # Старая модель считала средний диапазон по целиком загруженной таблице.
            "avg_range": float(np.mean(all_ranges)) if len(all_ranges) else 0.0,
            "open": np_rates["open"][:cut],
            "close": np_rates["close"][:cut],
            "ext_min": np_rates["ext_min"][:cut],
            "ext_max": np_rates["ext_max"][:cut],
        }

    rows = [row for row in rates if _as_datetime(row.get("date")) <= target]
    if not rows:
        return None
    rows.sort(key=lambda row: row["date"])
    dates = np.array([int(row["date"].timestamp()) for row in rows], dtype=np.int64)
    t1 = np.array([
        float(row.get("t1")) if row.get("t1") is not None
        else float(row.get("close") or 0) - float(row.get("open") or 0)
        for row in rows
    ], dtype=np.float64)
    ranges = np.array([
        float(row.get("max") or 0) - float(row.get("min") or 0)
        for row in rows
    ], dtype=np.float64)
    highs = np.array([float(row.get("max") or 0) for row in rows], dtype=np.float64)
    lows = np.array([float(row.get("min") or 0) for row in rows], dtype=np.float64)
    ext_max = np.zeros(len(rows), dtype=bool)
    ext_min = np.zeros(len(rows), dtype=bool)
    if len(rows) >= 3:
        ext_max[1:-1] = (highs[1:-1] > highs[:-2]) & (highs[1:-1] > highs[2:])
        ext_min[1:-1] = (lows[1:-1] < lows[:-2]) & (lows[1:-1] < lows[2:])
    return {
        "dates_ns": dates,
        "t1": t1,
        "ranges": ranges,
        "avg_range": float(np.mean(ranges)) if len(ranges) else 0.0,
        "open": np.array([float(row.get("open") or 0) for row in rows]),
        "close": np.array([float(row.get("close") or 0) for row in rows]),
        "ext_min": ext_min,
        "ext_max": ext_max,
    }


def _positions(view: dict, dates: list[datetime]) -> np.ndarray:
    if not dates:
        return np.empty(0, dtype=np.int64)
    requested = np.array([int(item.timestamp()) for item in dates], dtype=np.int64)
    positions = np.searchsorted(view["dates_ns"], requested, side="left")
    valid = positions < len(view["dates_ns"])
    positions = positions[valid]
    requested = requested[valid]
    return positions[view["dates_ns"][positions] == requested]


def _t1_value(view: dict, positions: np.ndarray, var: int) -> float:
    if positions.size == 0:
        return 0.0
    values = view["t1"][positions]
    ranges = view["ranges"][positions]
    avg_range = view["avg_range"]
    if var in (1, 3, 4):
        mask = ranges > avg_range
        values = values[mask]
        ranges = ranges[mask]
    if values.size == 0:
        return 0.0
    if var in (2, 3):
        return float(np.sum(values * np.abs(values)))
    if var == 4:
        return float(np.sum(ranges - avg_range))
    return float(np.sum(values))


def _extremum_value(
    view: dict,
    positions: np.ndarray,
    var: int,
    ext_flags: np.ndarray,
    modification: float,
    total_history: int,
) -> float | None:
    if positions.size == 0 or total_history <= 0:
        return None
    ranges = view["ranges"][positions]
    if var in (1, 3, 4):
        positions = positions[ranges > view["avg_range"]]
    if positions.size == 0:
        return None
    if var == 4:
        selected = positions[ext_flags[positions]]
        value = float(np.sum(view["ranges"][selected] - view["avg_range"]))
    else:
        hits = int(np.count_nonzero(ext_flags[positions]))
        value = ((hits / total_history) * 2.0 - 1.0) * modification
    return value if value != 0.0 else None


def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:
    del param
    target = _as_datetime(date)
    if target is None or not dataset:
        return {}
    if type not in (0, 1, 2, 3, 4) or var not in (0, 1, 2, 3, 4):
        return {}

    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if not ctx_index:
        return {}

    full_dataset = (dataset_index or {}).get("full_dataset") or dataset
    prepared = _prepare_dataset(full_dataset)
    view = _rates_view(rates, target, dataset_index)
    if view is None:
        return {}

    is_daily = bool((dataset_index or {}).get("is_daily"))
    unit = timedelta(days=1) if is_daily else timedelta(hours=1)
    window_start = target - SHIFT_WINDOW * unit
    window_end = target + SHIFT_WINDOW * unit
    left = bisect_left(prepared["dates"], window_start)
    right = bisect_right(prepared["dates"], window_end)
    window_events = prepared["events"][left:right]
    if not window_events:
        return {}

    pair_modification = {1: 0.001, 3: 1000.0, 4: 100.0}
    rates_table = str((dataset_index or {}).get("rates_table") or "")
    pair = 3 if "btc" in rates_table else 4 if "eth" in rates_table else 1
    modification = pair_modification[pair]

    target_ts = int(target.timestamp())
    previous_index = int(np.searchsorted(view["dates_ns"], target_ts, side="left")) - 1
    if previous_index < 0:
        return {}
    previous_is_bull = bool(
        view["close"][previous_index] > view["open"][previous_index]
    )
    ext_flags = view["ext_max"] if previous_is_bull else view["ext_min"]
    result: dict[str, float] = {}

    for event in window_events:
        # Совпадает со старой моделью: события важности 1 пропускаются вне
        # точной целевой свечи, остальные рассматриваются во всём окне.
        if int(event.get("importance") or 0) == 1 and event["date"] != target:
            continue

        event_id = event["event_id"]
        diff = target - event["date"]
        shift = int(diff.total_seconds() / (86400 if is_daily else 3600))

        context = _last_known_context(prepared, event_id, target)
        ctx_info = _ctx_lookup(ctx_index, (event_id, *context))
        if ctx_info is None:
            continue

        occurrence_count = int(ctx_info.get("occurrence_count") or 0)
        recurring = occurrence_count >= RECURRING_MIN_COUNT
        if not recurring and shift != 0:
            continue
        if recurring and abs(shift) > SHIFT_WINDOW:
            continue

        history_rows = prepared["history"].get(event_id, [])
        history_dates = prepared["history_dates"].get(event_id, [])
        history_cut = bisect_left(history_dates, target)
        valid_history = history_rows[:history_cut]
        if not valid_history:
            continue

        shifted_dates = [row["date"] + shift * unit for row in valid_history]
        shifted_dates = [item for item in shifted_dates if item < target]
        positions = _positions(view, shifted_dates)
        code_shift = shift if recurring else None

        if type in (0, 1, 3, 4):
            value = _t1_value(view, positions, var)
            if value != 0.0:
                code = _build_weight_code(event_id, *context, 0, code_shift)
                result[code] = result.get(code, 0.0) + value

        if type in (0, 2, 3, 4):
            value = _extremum_value(
                view, positions, var, ext_flags, modification, len(valid_history)
            )
            if value is not None:
                code = _build_weight_code(event_id, *context, 1, code_shift)
                result[code] = result.get(code, 0.0) + value

    return {key: round(value, 6) for key, value in result.items() if value != 0.0}
