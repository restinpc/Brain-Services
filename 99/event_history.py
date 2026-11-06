"""Candle-deduplicated historical outcomes for USGS contexts.

stored T1 at bar i = close(i+1) - close(i). It is known only after bar i+1
closes, including over market gaps. The same guard makes local-extremum labels
observable. No T1/extremum values are cached: diagnostic mutations and rate
refreshes are seen immediately. Only immutable enriched event rows are indexed.
"""
from __future__ import annotations

from bisect import bisect_left
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
import math
import threading

import numpy as np

_CACHE = OrderedDict()
_LOCK = threading.Lock()


def invalidate():
    with _LOCK:
        _CACHE.clear()


def history_for(source, *, daily=False):
    source = source if source is not None else []
    # Framework reload replaces row objects, even if it reuses the outer list.
    key = (id(source), len(source), id(source[0]) if source else 0,
           id(source[-1]) if source else 0, bool(daily))
    with _LOCK:
        if key in _CACHE:
            _CACHE.move_to_end(key)
            return _CACHE[key]
        history = EventHistory(source, daily=daily)
        _CACHE[key] = history
        while len(_CACHE) > 4:
            _CACHE.popitem(last=False)
        return history


class EventHistory:
    def __init__(self, source, *, daily=False):
        self.source = source  # Keep identity alive while cached.
        self.daily = daily
        self.unit = timedelta(days=1) if daily else timedelta(hours=1)
        grouped = {}
        for row in source:
            dt = row.get("date_dt") or row.get("date")
            name = str(row.get("event_type") or "").strip().lower()
            if not isinstance(dt, datetime) or not name:
                continue
            try:
                score = float(row.get("pct_change", 1.0))
            except (ValueError, TypeError):
                continue
            if not math.isfinite(score) or score <= 0:
                continue
            start = self.frame(dt)
            values = grouped.setdefault(name, {})
            values[start] = max(values.get(start, 0.0), score)
        self.by_type, self.current = {}, {}
        for name, values in grouped.items():
            dates = sorted(values)
            scores = np.array([values[d] for d in dates], dtype=float)
            self.by_type[name] = (dates, np.array([int(d.timestamp()) for d in dates], dtype=np.int64), scores)
            for d in dates:
                self.current.setdefault(d, []).append(name)

    def frame(self, dt):
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        if self.daily:
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.replace(minute=0, second=0, microsecond=0)

    def values_at(self, date, index, cfg, *, calc_type=0, var=0):
        if not isinstance(date, datetime):
            return {}
        try:
            calc_type, var = int(calc_type), int(var)
        except (ValueError, TypeError, OverflowError):
            return {}
        if calc_type not in (0, 1, 2) or var not in (0, 1, 2, 3):
            return {}
        target = self.frame(date)
        current_start = target - self.unit
        active = self.current.get(current_start, ())
        rates = index.get("np_rates")
        contexts = index.get("ctx_index") or {}
        if not active or rates is None or not contexts:
            return {}
        rate_dates = rates.get("dates_ns")
        t1 = rates.get("t1")
        if rate_dates is None or t1 is None or len(rate_dates) < 2:
            return {}
        if len(t1) != len(rate_dates):
            return {}
        reverse = {str(info.get("event_type") or "").lower(): info.get("id")
                   for info in contexts.values() if info.get("id")}
        seconds = int(self.unit.total_seconds())
        target_ts = int(target.timestamp())
        last_closed = int(np.searchsorted(rate_dates, target_ts - seconds, side="right")) - 1
        predict_max = None
        if last_closed >= 0 and rates.get("open") is not None and rates.get("close") is not None:
            opened, closed = float(rates["open"][last_closed]), float(rates["close"][last_closed])
            if math.isfinite(opened) and math.isfinite(closed):
                predict_max = closed > opened
        minimum = max(2, int(cfg["min_analogs"]))
        prior = float(cfg["extremum_prior"])
        left = target - timedelta(days=float(cfg["history_days"]))
        result = {}
        for name in active:
            ctx_id = reverse.get(name)
            if ctx_id is None:
                continue
            dates, timestamps, scores = self.by_type[name]
            lo = bisect_left(dates, left)
            hi = bisect_left(dates, current_start)  # exclude the current event
            if hi - lo < minimum:
                continue
            ts, strength = timestamps[lo:hi], scores[lo:hi]
            positions = np.searchsorted(rate_dates, ts)
            valid = positions + 1 < len(rate_dates)
            positions, ts, strength = positions[valid], ts[valid], strength[valid]
            # Exact alignment; never substitute a Friday or adjacent candle.
            # T1 and extrema require the next bar to have closed by target.
            valid = ((rate_dates[positions] == ts)
                     & (rate_dates[positions + 1] + seconds <= target_ts))
            positions, strength = positions[valid], strength[valid]
            outcomes = np.asarray(t1)[positions]
            valid = np.isfinite(outcomes)
            if var == 3:
                valid &= strength >= 2.0
            positions, outcomes, strength = positions[valid], outcomes[valid], strength[valid]
            count = len(outcomes)
            if count < minimum:
                continue
            if var == 1:
                outcomes = outcomes * np.clip(strength, 0.25, 3.0)
            value = float(np.sum(outcomes))
            if var == 2:
                value /= count + minimum  # shrunken mean, stable with sample size
            if calc_type in (0, 1) and math.isfinite(value):
                # Preserve a zero-valued active code, as models 97/98 do.
                result[f"{ctx_id}_0_0"] = round(value, 6)
            ext = rates.get("ext_max" if predict_max else "ext_min")
            if calc_type in (0, 2) and predict_max is not None and ext is not None:
                hits = int(np.count_nonzero(np.asarray(ext)[positions]))
                probability = (hits + prior * 0.5) / (count + prior)
                value = (2.0 * probability - 1.0) * (-1 if predict_max else 1)
                result[f"{ctx_id}_1_0"] = round(value, 6)
        return result
