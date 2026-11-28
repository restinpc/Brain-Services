"""Focused parity tests for Model 80 analog metadata reuse.

Run from the Brain-Services repository with:
    python -m unittest tests.test_model80_hotloop_candidate

Set MODEL80_SOURCE to a candidate model.py path to test an isolated candidate.
The loader extracts only the pure functions under test, so importing server
dependencies or triggering module-level service initialization is avoided.
"""
import ast
import bisect
import os
import threading
import unittest
from collections import OrderedDict
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path

try:
    import numpy as np
except ImportError:  # pragma: no cover - production service requires NumPy
    np = None

HERE = Path(__file__).resolve().parent
MODEL_PATH = Path(os.environ.get("MODEL80_SOURCE", HERE.parent / "80" / "model.py"))

_FUNCTION_NAMES = {
    "_var_allows_row", "_var_eligibility_mask", "_rate_timestamp_first_index",
    "_frame_date", "_prepare_analog_metadata", "_aggregate_analogs_by_var",
}

# Baseline aggregate loop from before metadata reuse. It deliberately calls
# the unchanged production helpers for rate indexing and eligibility masks.
_BASELINE_AGGREGATE = '''
def _aggregate_analogs_by_var(analog_rows, analog_dates, target_date,
                              current_event_time, shift, *, is_daily,
                              np_rates, ext_name):
    end = bisect.bisect_left(analog_dates, target_date)
    stats = {v: [0.0, 0.0, 0, 0.0, 0.0, 0.0] for v in VAR_RANGE}
    if end <= 0:
        return {v: tuple(x) for v, x in stats.items()}
    dates_ns = np_rates.get('dates_ns') if np_rates else None
    t1_arr = np_rates.get('t1') if np_rates else None
    ext_arr = np_rates.get(ext_name) if np_rates else None
    if dates_ns is None or t1_arr is None or ext_arr is None:
        return {v: tuple(x) for v, x in stats.items()}
    unit = timedelta(days=1) if is_daily else timedelta(hours=1)
    date_to_idx = _rate_timestamp_first_index(dates_ns)
    for j in range(end):
        analog_dt = analog_dates[j]
        if analog_dt == current_event_time:
            continue
        outcome_time = analog_dt + unit * int(shift)
        frame = _frame_date(outcome_time, is_daily)
        if frame + unit > target_date:
            continue
        ts = int(frame.timestamp())
        if date_to_idx is None:
            idx = int(np.searchsorted(dates_ns, ts, side='left'))
            if idx >= len(dates_ns) or int(dates_ns[idx]) != ts:
                continue
        else:
            idx = date_to_idx.get(ts)
            if idx is None:
                continue
        analog = analog_rows[j]
        stored_t1 = float(t1_arr[idx])
        hit = 1.0 if bool(ext_arr[idx]) else 0.0
        quality = float(analog.get('quality_score') or 1.0)
        q_weight = min(max(quality / 0.65, 0.25), 2.5)
        weighted_t1 = stored_t1 * q_weight
        weighted_hit = hit * q_weight
        eligible_vars = _VAR_MASK_TO_VARS[_var_eligibility_mask(analog)]
        for var in eligible_vars:
            st = stats[var]
            st[0] += stored_t1
            st[1] += hit
            st[2] += 1
            st[3] += weighted_t1
            st[4] += weighted_hit
            st[5] += q_weight
    return {v: tuple(x) for v, x in stats.items()}
'''


class _ChangingOffset(tzinfo):
    """Small deterministic timezone, including offset changes without DST."""
    def __init__(self, transition_date, transition_hour, before, after):
        self.transition_date = transition_date
        self.transition_hour = transition_hour
        self.before = timedelta(hours=before)
        self.after = timedelta(hours=after)

    def utcoffset(self, dt):
        if (dt.date() > self.transition_date or
                (dt.date() == self.transition_date and dt.hour >= self.transition_hour)):
            return self.after
        return self.before

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "TEST"


def _module_for(source, baseline=False):
    tree = ast.parse(source)
    functions = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name in _FUNCTION_NAMES]
    if baseline:
        functions = [node for node in functions
                     if node.name not in {"_prepare_analog_metadata", "_aggregate_analogs_by_var"}]
    ns = {
        "np": np, "datetime": datetime, "timedelta": timedelta,
        "bisect": bisect, "Any": object, "VAR_RANGE": list(range(8)),
        "SHIFT_WINDOW": 12, "TOPIC_FOCUS_MIN": .36,
        "CLUSTER_SIM_MIN": .70, "NOVELTY_MIN": .35,
        "_CACHE_LOCK": threading.RLock(),
        "_RATE_TS_INDEX_CACHE": OrderedDict(), "_RATE_TS_INDEX_CACHE_MAX": 8,
    }
    ns["_VAR_MASK_TO_VARS"] = tuple(
        tuple(v for v in range(8) if mask & (1 << v)) for mask in range(256)
    )
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(MODEL_PATH), "exec"), ns)
    if baseline:
        exec(compile(_BASELINE_AGGREGATE, "<baseline aggregate reference>", "exec"), ns)
    return ns


if np is None:
    raise RuntimeError("NumPy is required for Model80 candidate tests")

_SOURCE = MODEL_PATH.read_text(encoding="utf-8")
BASE = _module_for(_SOURCE, baseline=True)
FAST = _module_for(_SOURCE)


def _rate_arrays(dates):
    stamps = sorted({int(dt.timestamp()) for dt in dates})
    return {
        "dates_ns": np.asarray(stamps, dtype=np.int64),
        "t1": np.linspace(0.1, 0.9, len(stamps), dtype=np.float64),
        "ext_min": np.asarray([i % 2 == 0 for i in range(len(stamps))], dtype=np.bool_),
        "ext_max": np.asarray([i % 2 == 1 for i in range(len(stamps))], dtype=np.bool_),
    }


class Model80HotloopCandidateTest(unittest.TestCase):
    def test_prepared_frames_match_original_across_timezone_transitions(self):
        cases = [
            (_ChangingOffset(datetime(2025, 3, 30).date(), 2, 1, 2), datetime(2025, 3, 30, 0, 20)),
            (_ChangingOffset(datetime(2025, 10, 26).date(), 3, 2, 1), datetime(2025, 10, 26, 0, 20)),
            (_ChangingOffset(datetime(2014, 10, 26).date(), 2, 4, 3), datetime(2014, 10, 26, 0, 20)),
            (timezone(timedelta(hours=5, minutes=30)), datetime(2025, 4, 1, 0, 20)),
        ]
        for zone, start in cases:
            analog_dates = [(start + timedelta(hours=i)).replace(tzinfo=zone) for i in range(8)]
            frames, masks = FAST["_prepare_analog_metadata"](len(analog_dates))
            self.assertEqual(masks.tolist(), [-1] * len(analog_dates))
            self.assertEqual(frames, [None] * len(analog_dates))
            for is_daily in (False, True):
                unit = timedelta(days=1) if is_daily else timedelta(hours=1)
                for analog_dt in analog_dates:
                    base = BASE["_frame_date"](analog_dt, is_daily)
                    for shift in range(13):
                        expected = BASE["_frame_date"](analog_dt + unit * shift, is_daily)
                        actual = base + unit * shift
                        self.assertEqual(actual, expected)
                        self.assertEqual(int(actual.timestamp()), int(expected.timestamp()))

    def test_missing_rate_skips_pathological_features_before_lazy_mask(self):
        target = datetime(2026, 11, 25, 12)
        dates = list(reversed([target - timedelta(hours=i + 1) for i in range(3)]))
        rows = [{"confirmation_count": float("inf"), "topic_focus": .8,
                 "cluster_similarity": .9, "novelty": .8, "quality_score": 1.0}
                for _ in dates]
        rates = {"dates_ns": np.asarray([], dtype=np.int64),
                 "t1": np.asarray([], dtype=np.float64), "ext_min": np.asarray([], dtype=np.bool_)}
        prepared = FAST["_prepare_analog_metadata"](len(dates))
        args = (rows, dates, target, target - timedelta(hours=1), 1)
        kwargs = {"is_daily": False, "np_rates": rates, "ext_name": "ext_min"}
        self.assertEqual(FAST["_aggregate_analogs_by_var"](*args, **kwargs, prepared=prepared),
                         BASE["_aggregate_analogs_by_var"](*args, **kwargs))
        self.assertEqual(prepared[1].tolist(), [-1] * len(dates))
        matching_rates = _rate_arrays([BASE["_frame_date"](date + timedelta(hours=1), False)
                                       for date in dates])
        for fn, extra in ((BASE["_aggregate_analogs_by_var"], {}),
                          (FAST["_aggregate_analogs_by_var"],
                           {"prepared": FAST["_prepare_analog_metadata"](len(dates))})):
            with self.assertRaises(OverflowError):
                fn(rows, dates, target, target - timedelta(hours=4), 1,
                   is_daily=False, np_rates=matching_rates, ext_name="ext_min", **extra)

    def test_matching_rates_preserve_aggregate_values_and_order(self):
        zone = _ChangingOffset(datetime(2025, 10, 26).date(), 3, 2, 1)
        analog_dates = [
            datetime(2025, 10, 25, 18).replace(tzinfo=zone),
            datetime(2025, 10, 26, 0, 15).replace(tzinfo=zone),
            datetime(2025, 10, 26, 1, 15).replace(tzinfo=zone, fold=0),
            datetime(2025, 10, 26, 1, 15).replace(tzinfo=zone, fold=1),
            datetime(2025, 10, 26, 3, 15).replace(tzinfo=zone),
        ]
        rows = [{"topic_focus": .4 + i / 20, "cluster_similarity": .71 + i / 100,
                 "novelty": .36 + i / 50, "confirmation_count": 2 + i % 2,
                 "quality_score": .3 + i / 5} for i in range(len(analog_dates))]
        target = datetime(2025, 10, 26, 8, tzinfo=zone)
        current = datetime(2025, 10, 26, 6, tzinfo=zone)
        rate_dates = [BASE["_frame_date"](dt + timedelta(hours=2), False) for dt in analog_dates]
        rates = _rate_arrays(rate_dates)
        expected = BASE["_aggregate_analogs_by_var"](
            rows, analog_dates, target, current, 2, is_daily=False,
            np_rates=rates, ext_name="ext_min")
        actual = FAST["_aggregate_analogs_by_var"](
            rows, analog_dates, target, current, 2, is_daily=False,
            np_rates=rates, ext_name="ext_min",
            prepared=FAST["_prepare_analog_metadata"](len(rows)))
        self.assertEqual(actual, expected)

    def test_prepared_metadata_is_private_across_concurrent_calls(self):
        barrier = threading.Barrier(3)
        results = [None, None]

        def prepare(slot):
            value = FAST["_prepare_analog_metadata"](128)
            barrier.wait()
            value[0][0] = datetime(2026, 1, 1, slot)
            value[1][0] = slot
            results[slot] = value
            barrier.wait()

        threads = [threading.Thread(target=prepare, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        barrier.wait()
        for thread in threads:
            thread.join()
        first, second = results
        self.assertIsNot(first[0], second[0])
        self.assertIsNot(first[1], second[1])
        self.assertEqual(first[0][0], datetime(2026, 1, 1, 0))
        self.assertEqual(second[0][0], datetime(2026, 1, 1, 1))
        self.assertEqual(first[1][0], 0)
        self.assertEqual(second[1][0], 1)


if __name__ == "__main__":
    unittest.main()
