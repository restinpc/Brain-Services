"""Indexed equivalent of service 94's canonical ML inputs (type=0, var=0).

Multiplicity is retained: several markets at the same timestamp are several
analogs in the original model. Only their lookup and summation are grouped.
The reverse-learning algorithm remains owned by the framework.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter, OrderedDict
from datetime import datetime, timedelta
import threading

import numpy as np

_LOCK = threading.RLock()
_SOURCE = None
_RATE_FIELDS = ("dates_ns", "t1", "ext_max", "ext_min", "open", "close")


class UnsupportedRates(ValueError):
    """Use the original calculation for unsupported numeric inputs."""


def invalidate():
    global _SOURCE
    with _LOCK:
        _SOURCE = None


def _event(row):
    dt = row.get("event_time") or row.get("date_dt") or row.get("date")
    if isinstance(dt, str):
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(dt[:19], fmt)
                break
            except ValueError:
                continue
        dt = parsed
    if not isinstance(dt, datetime):
        return None
    try:
        float(row.get("pct_change") or 0.0)
        name = str(row["event_type"]).strip().lower()
    except (KeyError, TypeError, ValueError):
        return None
    return (name, dt) if name else None


def _rare(info, total, maximum):
    for field in ("is_rare", "rare"):
        if info.get(field) is not None:
            return bool(info[field])
    frequency = str(info.get("event_class") or info.get("frequency_class")
                    or info.get("event_frequency") or "").strip().lower()
    if frequency in ("rare", "редкое", "low", "sparse"):
        return True
    if frequency in ("frequent", "частое", "high", "dense"):
        return False
    return int(info.get("occurrence_count") or total) <= maximum


class _Series:
    def __init__(self, counts):
        self.dates = sorted(counts)
        self.counts = np.array([counts[d] for d in self.dates], dtype=np.int64)
        self.total = sum(counts.values())
        self.frames = {}

    def frame_dates(self, daily):
        if daily not in self.frames:
            self.frames[daily] = [
                d.replace(hour=0, minute=0, second=0, microsecond=0) if daily
                else d.replace(minute=0, second=0, microsecond=0)
                for d in self.dates
            ]
        return self.frames[daily]


class _Prefix:
    def __init__(self, series, rates, daily, shift):
        unit = timedelta(days=1) if daily else timedelta(hours=1)
        starts = [d + shift * unit for d in series.frame_dates(daily)]
        self.ends = [d + unit for d in starts]
        timestamps = np.array([int(d.timestamp()) for d in starts], dtype=np.int64)
        positions = np.searchsorted(rates["dates_ns"], timestamps)
        valid = positions < len(rates["dates_ns"])
        valid[valid] &= rates["dates_ns"][positions[valid]] == timestamps[valid]
        counts = np.where(valid, series.counts, 0)
        sums = np.zeros(len(starts), dtype=float)
        sums[valid] = rates["t1"][positions[valid]] * series.counts[valid]
        self.values = [counts, sums]
        for field in ("ext_max", "ext_min"):
            hits = np.zeros(len(starts), dtype=np.int64)
            if rates[field] is not None:
                hits[valid] = rates[field][positions[valid]].astype(bool) * series.counts[valid]
            self.values.append(hits)
        self.prefixes = [np.concatenate(([0], np.cumsum(v))) for v in self.values]

    def query(self, series, date, current, predict_max):
        # Both the analog event and its shifted outcome must already be known.
        end = min(bisect_left(series.dates, date), bisect_right(self.ends, date))
        count, value, max_hits, min_hits = [a[end] for a in self.prefixes]
        if current < end:
            # Exclude every row at the current timestamp, as the original does.
            own = [a[current] for a in self.values]
            count, value, max_hits, min_hits = [
                a - b for a, b in zip((count, value, max_hits, min_hits), own)
            ]
        if count < 2:
            return 0.0, 0.0
        hits = max_hits if predict_max else min_hits
        score = (float(hits) / int(count)) * 2.0 - 1.0
        return float(value), -score if predict_max else score


class _Rates:
    def __init__(self, rates):
        self.arrays = {key: rates.get(key) for key in _RATE_FIELDS}
        dates = self.arrays["dates_ns"]
        t1 = self.arrays["t1"]
        if dates is None or any(self.arrays[k] is None or len(self.arrays[k]) != len(dates)
                                for k in ("t1", "open", "close")):
            raise UnsupportedRates("Incomplete rate arrays")
        if not np.all(np.isfinite(t1)):
            raise UnsupportedRates("Non-finite stored T1")
        self.prefixes = {}
        self.answers = OrderedDict()

    def matches(self, rates):
        return all(value is rates.get(key) for key, value in self.arrays.items())


class _History:
    def __init__(self, source, token, date_array):
        self.source, self.token, self.date_array = source, token, date_array
        self.boundary_rows = (source[0], source[-1]) if source else ()
        grouped = {}
        for row in source:
            parsed = _event(row)
            if parsed is not None:
                name, dt = parsed
                counts = grouped.get(name)
                if counts is None:
                    counts = grouped[name] = Counter()
                counts[dt] += 1
        self.series = {name: _Series(counts) for name, counts in grouped.items()}
        self.rates = OrderedDict()

    def values_at(self, date, index, daily, rare_maximum):
        rates = index["np_rates"]
        key = (id(rates), daily)
        cached = self.rates.get(key)
        if cached is None or not cached.matches(rates):
            cached = _Rates(rates)
            self.rates[key] = cached
        self.rates.move_to_end(key)
        while len(self.rates) > 6:
            self.rates.popitem(last=False)

        reverse = {
            str(info["event_type"]).strip().lower(): (int(info["id"]), info)
            for info in index["ctx_index"].values()
            if info.get("id") and info.get("event_type")
        }
        contexts = tuple(
            (name, ctx_id, _rare(info, self.series[name].total, rare_maximum))
            for name, (ctx_id, info) in reverse.items() if name in self.series
        )
        if not contexts:
            return {}
        answer_key = (date, contexts)
        if answer_key in cached.answers:
            cached.answers.move_to_end(answer_key)
            return dict(cached.answers[answer_key])

        array = cached.arrays
        previous = int(np.searchsorted(array["dates_ns"], int(date.timestamp()), side="left")) - 1
        predict_max = previous >= 0 and array["close"][previous] > array["open"][previous]
        unit = timedelta(days=1) if daily else timedelta(hours=1)
        horizon = timedelta(days=1) if daily else timedelta(hours=12)
        result = {}
        for name, ctx_id, rare in contexts:
            series = self.series[name]
            left = bisect_left(series.dates, date - (horizon if rare else unit))
            right = (bisect_right(series.dates, date + horizon) if rare
                     else bisect_left(series.dates, date))
            for current in range(left, right):
                dt = series.dates[current]
                shift = (0 if date - unit <= dt < date
                         else int((date - dt).total_seconds() / unit.total_seconds()))
                prefix_key = (name, shift)
                prefix = cached.prefixes.get(prefix_key)
                if prefix is None:
                    prefix = _Prefix(series, array, daily, shift)
                    cached.prefixes[prefix_key] = prefix
                values = prefix.query(series, date, current, predict_max)
                for mode, value in enumerate(values):
                    value = round(value, 6)
                    if value != 0.0:
                        code = f"{ctx_id}_{mode}_{shift}"
                        # Preserve the original order of repeated additions,
                        # including its cancellation at the final zero filter.
                        for _ in range(int(series.counts[current])):
                            result[code] = result.get(code, 0.0) + value
        result = {code: value for code, value in result.items() if value != 0.0}
        cached.answers[answer_key] = result
        while len(cached.answers) > 128:
            cached.answers.popitem(last=False)
        return dict(result)


def values_at(rates, dataset, date, index, *, rare_occurrence_max=0):
    global _SOURCE
    index = index or {}
    if not dataset or not index.get("ctx_index") or index.get("np_rates") is None:
        return {}
    if "is_daily" in index:
        daily = bool(index["is_daily"])
    elif index.get("rates_table"):
        daily = str(index["rates_table"]).endswith("_day")
    else:
        last = rates[-1].get("date") if rates else None
        daily = isinstance(last, datetime) and not (last.hour or last.minute or last.second or last.microsecond)
    source = index.get("full_dataset")
    if source is None:
        source = dataset
    stamps = index.get("dataset_timestamps")
    token = (id(source), len(source), id(source[0]) if source else 0,
             id(source[-1]) if source else 0, id(stamps))
    with _LOCK:
        if _SOURCE is None or _SOURCE.token != token:
            _SOURCE = _History(source, token, stamps)
        return _SOURCE.values_at(date, index, daily, int(rare_occurrence_max))
