"""Recovered algorithmic news model for Brain services 79/80.

The original Python source of the TF-IDF -> NMF -> SVD -> hierarchical KMeans
implementation was overwritten, while its CPython 3.13 bytecode remained in
__pycache__.  This module keeps the recovered enrichment implementation as a
private sourceless core and provides a readable/corrected runtime model layer.

Important runtime fixes:
* process-local caches/lock are always initialized;
* shared enrichment lock contention is a normal ``mode=locked`` result;
* event->context matching has a deterministic fallback instead of silently
  dropping every event when the context index is stale;
* historical outcome lookup tolerates sub-frame timestamp skew, while never
  using a future candle;
* batch_model computes the 5x8 slot cube once per date batch and reuses it.
"""
from __future__ import annotations

import bisect
import importlib.machinery
import importlib.util
import os
import threading
import zlib
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from brain_framework import get_service_config

# ---------------------------------------------------------------------------
# Recovered algorithmic enrichment core
# ---------------------------------------------------------------------------
_CORE_PATH = Path(__file__).with_name("_algo_core.pyc")
if not _CORE_PATH.is_file():
    raise RuntimeError(f"Missing recovered algorithmic core: {_CORE_PATH}")

_loader = importlib.machinery.SourcelessFileLoader(
    f"_brain_news_algo_core_{Path(__file__).parent.name}", str(_CORE_PATH)
)
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
if _spec is None:
    raise RuntimeError(f"Cannot create import spec for {_CORE_PATH}")
_core = importlib.util.module_from_spec(_spec)
_loader.exec_module(_core)

# The recovered source had references to these globals but the assignments had
# been lost.  Inject them into the core as well, because enrich helpers may call
# core runtime helpers during diagnostics/rebuilds.
_core._CACHE_LOCK = threading.RLock()
_core._SINGLE_CACHE = OrderedDict()
_core._BATCH_CACHE = OrderedDict()
_core._SINGLE_CACHE_MAX = 256
_core._BATCH_CACHE_MAX = 8

PRETEST_ALLOW_EMPTY = True
RATES_TABLE = "brain_rates_eur_usd"
FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
SHIFT_WINDOW = 12
VAR_RANGE = list(range(8))
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = Path(__file__).parent.name == "80"
ENRICH_SCHEMA_VERSION = "2"
ALGO_VERSION = "tfidf-nmf-svd-hierkmeans-causal-v2-recovered-r1"

SOURCES = dict(getattr(_core, "SOURCES", {
    "cnn": "brain_cnn_news",
    "nyt": "brain_nyt_news",
    "twp": "brain_twp_news",
    "wsj": "brain_wsj_news",
    "tgd": "brain_tgd_news",
}))

TOPIC_FOCUS_MIN = float(getattr(_core, "TOPIC_FOCUS_MIN", 0.36))
CLUSTER_SIM_MIN = float(getattr(_core, "CLUSTER_SIM_MIN", 0.70))
NOVELTY_MIN = float(getattr(_core, "NOVELTY_MIN", 0.35))

_CACHE_LOCK = threading.RLock()
_SINGLE_CACHE: OrderedDict[tuple, dict] = OrderedDict()
_BATCH_CACHE: OrderedDict[tuple, dict] = OrderedDict()
_SINGLE_CACHE_MAX = 256
_BATCH_CACHE_MAX = 8

# Export the exact recovered NLP/enrichment pipeline.  Only lock contention is
# normalized below; actual NLP/data errors still propagate.
async def enrich_dataset(engine_vlad, engine_brain):
    try:
        return await _core.enrich_dataset(engine_vlad, engine_brain)
    except RuntimeError as exc:
        msg = str(exc)
        if "Could not acquire shared news enrichment lock" in msg:
            return {
                "mode": "locked",
                "reason": "another news service is enriching the shared dataset",
                "detail": msg,
            }
        raise

# ---------------------------------------------------------------------------
# Runtime event/outcome logic
# ---------------------------------------------------------------------------
def _nlp_cfg() -> dict[str, Any]:
    cfg = get_service_config() or {}
    raw = cfg.get("nlp") or {}
    cutoff_s = str(raw.get("train_cutoff") or "2025-01-15 00:00:00")
    try:
        cutoff = datetime.fromisoformat(cutoff_s.replace("T", " ")[:19])
    except ValueError:
        cutoff = datetime(2025, 1, 15)
    return {"train_cutoff": cutoff}


def _row_date(row: dict[str, Any]) -> datetime | None:
    dt = row.get("date") or row.get("date_dt")
    return dt if isinstance(dt, datetime) else None


def _var_allows_row(row: dict[str, Any], var: int) -> bool:
    try:
        focus = float(row.get("topic_focus") or 0.0)
        fit = float(row.get("cluster_similarity") or 0.0)
        novelty = float(row.get("novelty") or 0.0)
        confirm = int(row.get("confirmation_count") or 1)
    except (TypeError, ValueError):
        return int(var) in (0, 7)

    var = int(var)
    if var == 0:
        return True
    if var == 1:
        return focus >= TOPIC_FOCUS_MIN
    if var == 2:
        return fit >= CLUSTER_SIM_MIN
    if var == 3:
        return novelty >= NOVELTY_MIN
    if var == 4:
        return confirm >= 2
    if var == 5:
        return confirm >= 3
    if var == 6:
        return novelty >= NOVELTY_MIN and confirm >= 2
    if var == 7:
        return True
    return False


def _frame_date(dt: datetime, is_daily: bool) -> datetime:
    if is_daily:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(minute=0, second=0, microsecond=0)


def _ctx_reverse(ctx_index: dict | None) -> dict[str, tuple[int, dict]]:
    result: dict[str, tuple[int, dict]] = {}
    for info in (ctx_index or {}).values():
        try:
            ctx_id = int(info.get("id"))
        except (TypeError, ValueError, AttributeError):
            continue
        event_type = str(info.get("event_type") or "").strip().lower()
        if event_type:
            result[event_type] = (ctx_id, info)
    return result


def _fallback_ctx_id(event_type: str) -> int:
    # Used only if the shared ctx table is stale.  Keep outside the normal small
    # ctx-id range so it can never collide with a legitimate database id.
    return 1_000_000_000 + (zlib.crc32(event_type.encode("utf-8", "ignore")) & 0x3FFFFFFF)


def _current_rows(source: list[dict], target_date: datetime,
                  dataset_index: dict, is_daily: bool) -> list[dict]:
    if not source:
        return []
    horizon = timedelta(days=1) if is_daily else timedelta(hours=SHIFT_WINDOW)
    left_ts = int((target_date - horizon).timestamp())
    right_ts = int(target_date.timestamp())
    ts_arr = dataset_index.get("dataset_timestamps")
    if ts_arr is not None and len(ts_arr) == len(source):
        lo = int(np.searchsorted(ts_arr, left_ts, side="left"))
        hi = int(np.searchsorted(ts_arr, right_ts, side="right"))
        return source[lo:hi]

    dates = dataset_index.get("dates") or [_row_date(r) for r in source]
    lo = bisect.bisect_left(dates, target_date - horizon)
    hi = bisect.bisect_right(dates, target_date)
    return source[lo:hi]


def _previous_direction(np_rates: dict | None, target_date: datetime) -> tuple[bool, str]:
    if not np_rates:
        return False, "ext_min"
    dates_ns = np_rates.get("dates_ns")
    opens = np_rates.get("open")
    closes = np_rates.get("close")
    if dates_ns is None or opens is None or closes is None or len(dates_ns) == 0:
        return False, "ext_min"
    cut = int(np.searchsorted(dates_ns, int(target_date.timestamp()), side="left"))
    if cut <= 0:
        return False, "ext_min"
    idx = cut - 1
    predict_max = float(closes[idx]) > float(opens[idx])
    return predict_max, "ext_max" if predict_max else "ext_min"


def _rate_idx(np_rates: dict, frame: datetime, unit_seconds: int) -> int | None:
    """Find the causal rate candle for a framed timestamp.

    Exact match is preferred.  A small backward skew (< one frame) is accepted
    because historical news/rate imports are not always normalized to identical
    timezone/minute boundaries.  Never choose a rate after ``frame``.
    """
    dates_ns = np_rates.get("dates_ns")
    if dates_ns is None or len(dates_ns) == 0:
        return None
    ts = int(frame.timestamp())
    idx = int(np.searchsorted(dates_ns, ts, side="left"))
    if idx < len(dates_ns) and int(dates_ns[idx]) == ts:
        return idx
    prev = idx - 1
    if prev >= 0:
        delta = ts - int(dates_ns[prev])
        if 0 <= delta < unit_seconds:
            return prev
    return None


def _mode1_score(hits: float, total: float, predict_max: bool) -> float:
    if total <= 0:
        return 0.0
    score = (hits / total) * 2.0 - 1.0
    return -score if predict_max else score


def _add_code(target: dict[str, float], code: str, value: float) -> None:
    value = float(value)
    if value != 0.0 and np.isfinite(value):
        target[code] = target.get(code, 0.0) + value


def _aggregate_analogs_by_var(
    analog_rows: list[dict],
    analog_dates: list[datetime],
    target_date: datetime,
    current_event_time: datetime,
    shift: int,
    *,
    is_daily: bool,
    np_rates: dict,
    ext_name: str,
) -> dict[int, tuple[float, float, int, float, float, float]]:
    end = bisect.bisect_left(analog_dates, target_date)
    stats = {v: [0.0, 0.0, 0, 0.0, 0.0, 0.0] for v in VAR_RANGE}
    if end <= 0 or not np_rates:
        return {v: tuple(x) for v, x in stats.items()}

    t1_arr = np_rates.get("t1")
    ext_arr = np_rates.get(ext_name)
    if t1_arr is None or ext_arr is None:
        return {v: tuple(x) for v, x in stats.items()}

    unit = timedelta(days=1) if is_daily else timedelta(hours=1)
    unit_seconds = 86400 if is_daily else 3600

    for j in range(end):
        analog_dt = analog_dates[j]
        if analog_dt == current_event_time:
            continue
        outcome_time = analog_dt + unit * int(shift)
        frame = _frame_date(outcome_time, is_daily)
        # Historical outcome is usable only after the whole outcome interval
        # has completed by target_date.
        if frame + unit > target_date:
            continue
        idx = _rate_idx(np_rates, frame, unit_seconds)
        if idx is None:
            continue

        analog = analog_rows[j]
        stored_t1 = float(t1_arr[idx])
        if not np.isfinite(stored_t1):
            continue
        hit = 1.0 if bool(ext_arr[idx]) else 0.0
        try:
            quality = float(analog.get("quality_score") or 1.0)
        except (TypeError, ValueError):
            quality = 1.0
        q_weight = min(max(quality / 0.65, 0.25), 2.5)

        for var in VAR_RANGE:
            if not _var_allows_row(analog, var):
                continue
            st = stats[var]
            st[0] += stored_t1
            st[1] += hit
            st[2] += 1
            st[3] += stored_t1 * q_weight
            st[4] += hit * q_weight
            st[5] += q_weight

    return {v: tuple(x) for v, x in stats.items()}


def _compute_all_slots_for_date(dataset: list[dict], target_date: datetime,
                                dataset_index: dict) -> dict[tuple[int, int], dict[str, float]]:
    outputs = {(t, v): {} for t in TYPES_RANGE for v in VAR_RANGE}
    if target_date < _nlp_cfg()["train_cutoff"]:
        return outputs

    di = dataset_index or {}
    source = di.get("full_dataset") if di.get("full_dataset") is not None else dataset
    if not source:
        return outputs

    by_key = di.get("by_key") or {}
    key_dates = di.get("key_dates") or {}
    ctx_reverse = _ctx_reverse(di.get("ctx_index") or {})
    np_rates = di.get("np_rates")
    if not by_key or not key_dates or not np_rates:
        return outputs

    is_daily = bool(di.get("is_daily"))
    unit_seconds = 86400 if is_daily else 3600
    predict_max, ext_name = _previous_direction(np_rates, target_date)
    current = _current_rows(source, target_date, di, is_daily)

    for row in current:
        event_dt = _row_date(row)
        if not isinstance(event_dt, datetime) or event_dt > target_date:
            continue
        event_type = str(row.get("event_type") or "").strip().lower()
        if not event_type:
            continue

        ctx = ctx_reverse.get(event_type)
        ctx_id = ctx[0] if ctx is not None else _fallback_ctx_id(event_type)

        seconds = max(0.0, (target_date - event_dt).total_seconds())
        shift = int(seconds // unit_seconds)
        if (not is_daily and shift > SHIFT_WINDOW) or (is_daily and shift > 1):
            continue

        analog_rows = by_key.get(row.get("event_type")) or by_key.get(event_type) or []
        analog_dates = key_dates.get(row.get("event_type")) or key_dates.get(event_type) or []
        if len(analog_rows) < 3:
            continue

        by_var = _aggregate_analogs_by_var(
            analog_rows, analog_dates, target_date, event_dt, shift,
            is_daily=is_daily, np_rates=np_rates, ext_name=ext_name,
        )

        for var in VAR_RANGE:
            if not _var_allows_row(row, var):
                continue
            raw_t1, raw_hits, n, w_t1, w_hits, w_total = by_var[var]
            if n < 3:
                continue
            raw_mode1 = _mode1_score(raw_hits, float(n), predict_max)
            weighted_mode1 = _mode1_score(w_hits, w_total, predict_max)
            mode0 = w_t1 if var == 7 else raw_t1
            mode1 = weighted_mode1 if var == 7 else raw_mode1
            code0 = f"{ctx_id}_0_{shift}"
            code1 = f"{ctx_id}_1_{shift}"

            if 0 in TYPES_RANGE:
                _add_code(outputs[(0, var)], code0, round(mode0, 6))
                _add_code(outputs[(0, var)], code1, round(mode1, 6))
            if 1 in TYPES_RANGE:
                _add_code(outputs[(1, var)], code0, round(mode0, 6))
            if 2 in TYPES_RANGE:
                _add_code(outputs[(2, var)], code1, round(mode1, 6))
            for calc_type in (3, 4):
                if calc_type not in TYPES_RANGE:
                    continue
                if mode0 != 0.0:
                    _add_code(outputs[(calc_type, var)], code0, round(mode0, 6))
                if raw_hits > 0.0:
                    _add_code(outputs[(calc_type, var)], code1, 1.0)

    return outputs


def _dataset_token(source: list[dict]) -> tuple[int, int, int]:
    if not source:
        return (0, 0, 0)
    last = _row_date(source[-1])
    return (id(source), len(source), int(last.timestamp()) if isinstance(last, datetime) else 0)


def _runtime_cache_token(dataset: list[dict], dataset_index: dict,
                         target_dates: tuple[int, ...] | None = None) -> tuple:
    source = dataset_index.get("full_dataset") if dataset_index.get("full_dataset") is not None else dataset
    np_rates = dataset_index.get("np_rates") or {}
    dates_ns = np_rates.get("dates_ns")
    rates_tail = int(dates_ns[-1]) if dates_ns is not None and len(dates_ns) else 0
    base = (
        _dataset_token(source),
        str(dataset_index.get("rates_table") or ""),
        bool(dataset_index.get("is_daily")),
        rates_tail,
    )
    return base + ((target_dates,) if target_dates is not None else ())


def model(rates: list[dict], dataset: list[dict], date: datetime, *,
          type: int = 0, var: int = 0, param: str = "",
          dataset_index: dict | None = None) -> dict[str, float]:
    if not dataset or not date or dataset_index is None:
        return {}
    calc_type, calc_var = int(type), int(var)
    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {}
    di = dict(dataset_index)
    token = _runtime_cache_token(dataset, di) + (int(date.timestamp()),)
    with _CACHE_LOCK:
        cached = _SINGLE_CACHE.get(token)
        if cached is not None:
            _SINGLE_CACHE.move_to_end(token)
            return dict(cached.get((calc_type, calc_var), {}))

    all_slots = _compute_all_slots_for_date(dataset, date, di)
    with _CACHE_LOCK:
        _SINGLE_CACHE[token] = all_slots
        _SINGLE_CACHE.move_to_end(token)
        while len(_SINGLE_CACHE) > _SINGLE_CACHE_MAX:
            _SINGLE_CACHE.popitem(last=False)
    return dict(all_slots.get((calc_type, calc_var), {}))


def batch_model(rates: list[dict], dataset: list[dict], dates: list[datetime], *,
                type: int = 0, var: int = 0, param: str = "",
                dataset_index: dict | None = None) -> dict[datetime, dict[str, float]]:
    if not dates or not dataset or dataset_index is None:
        return {d: {} for d in dates}
    calc_type, calc_var = int(type), int(var)
    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {d: {} for d in dates}

    di = dict(dataset_index)
    date_token = tuple(int(d.timestamp()) for d in dates)
    key = _runtime_cache_token(dataset, di, date_token)
    with _CACHE_LOCK:
        cached = _BATCH_CACHE.get(key)
        if cached is not None:
            _BATCH_CACHE.move_to_end(key)
            selected = cached.get((calc_type, calc_var), {})
            return {d: dict(selected.get(d, {})) for d in dates}

    cube = {(t, v): {} for t in TYPES_RANGE for v in VAR_RANGE}
    for dt in dates:
        slots = _compute_all_slots_for_date(dataset, dt, di)
        for slot, value in slots.items():
            cube[slot][dt] = value

    with _CACHE_LOCK:
        _BATCH_CACHE[key] = cube
        _BATCH_CACHE.move_to_end(key)
        while len(_BATCH_CACHE) > _BATCH_CACHE_MAX:
            _BATCH_CACHE.popitem(last=False)

    selected = cube.get((calc_type, calc_var), {})
    return {d: dict(selected.get(d, {})) for d in dates}
