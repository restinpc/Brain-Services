"""Bounded exact provider/ReverseStore-path replay for the batch prewarm candidate."""
import ast
import argparse
import asyncio
import bisect
import csv
import hashlib
import json
import math
import sys
import threading
from collections import OrderedDict, defaultdict
from concurrent.futures import Future
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent if ROOT.name == "tests" else Path("/brain/Brain-Services")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--framework", type=Path, default=REPO_ROOT / "shared/brain_framework.py")
    parser.add_argument("--model", type=Path, default=REPO_ROOT / "80/model.py")
    parser.add_argument("--reverse", type=Path, default=REPO_ROOT / "shared/reverse_learning.py")
    default_fixture = Path(__import__("os").environ.get("BRAIN_V74_FIXTURE_DIR", str(ROOT)))
    parser.add_argument("--fixture-dir", type=Path, default=default_fixture)
    return parser.parse_args()


ARGS = parse_args()
FRAMEWORK, MODEL, REVERSE, ROOT = ARGS.framework, ARGS.model, ARGS.reverse, ARGS.fixture_dir
sys.path.insert(0, str(REVERSE.parent))
import reverse_learning as rl


def extract_model(path):
    names = {
        "_var_allows_row", "_var_eligibility_mask", "_rate_timestamp_first_index",
        "_frame_date", "_prepare_analog_metadata", "_ctx_reverse", "_current_rows",
        "_previous_direction", "_aggregate_analogs_by_var", "_mode1_score",
        "_add_code", "_compute_all_slots_for_date",
        "_dataset_token", "_runtime_cache_token", "_cached_cube", "model", "batch_model",
    }
    tree = ast.parse(path.read_text(encoding="utf-8"))
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert names <= {n.name for n in funcs}, names - {n.name for n in funcs}
    ns = {
        "np": np, "datetime": datetime, "timedelta": timedelta, "bisect": bisect,
        "math": math, "sys": sys, "Any": Any, "Future": Future,
        "threading": threading, "OrderedDict": OrderedDict,
        "VAR_RANGE": list(range(8)), "TYPES_RANGE": list(range(5)),
        "SHIFT_WINDOW": 12, "TOPIC_FOCUS_MIN": .36, "CLUSTER_SIM_MIN": .70,
        "NOVELTY_MIN": .35, "_CACHE_LOCK": threading.RLock(),
        "_RATE_TS_INDEX_CACHE": OrderedDict(), "_RATE_TS_INDEX_CACHE_MAX": 8,
        "_SINGLE_CACHE": OrderedDict(), "_SINGLE_CACHE_MAX": 32,
        "_SINGLE_FLIGHTS": {}, "_BATCH_CACHE": OrderedDict(),
        "_BATCH_CACHE_MAX": 1, "_BATCH_FLIGHTS": {},
        "_nlp_cfg": lambda: {"train_cutoff": datetime(2000, 1, 1)},
    }
    ns["_VAR_MASK_TO_VARS"] = tuple(tuple(v for v in range(8) if mask & (1 << v)) for mask in range(256))
    exec(compile(ast.Module(body=funcs, type_ignores=[]), str(path), "exec"), ns)
    return ns


def extract_prewarm(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_prewarm_ml_active_cache")
    return fn


def read_rates(path):
    with path.open(newline="", encoding="utf-8") as f:
        raw = list(csv.DictReader(f, delimiter="\t"))
    rows = [{**r, "date": datetime.fromisoformat(r["date"])} for r in raw]
    return raw, rows


async def main():
    model_ns = extract_model(MODEL)
    with (ROOT / "analog.tsv").open(newline="", encoding="ascii") as f:
        analog_raw = list(csv.DictReader(f, delimiter="\t"))
    dataset = [{
        "date": datetime.fromisoformat(r["date_dt"]), "event_type": r["event_type"],
        "topic_focus": float(r["topic_focus"]),
        "cluster_similarity": float(r["cluster_similarity"]),
        "novelty": float(r["novelty"]), "confirmation_count": int(r["confirmation_count"]),
        "quality_score": float(r["quality_score"]),
    } for r in analog_raw]
    dataset.sort(key=lambda r: r["date"])
    by_key = defaultdict(list)
    key_dates = defaultdict(list)
    for row in dataset:
        by_key[row["event_type"]].append(row)
        key_dates[row["event_type"]].append(row["date"])
    ctx = {kind: {"id": i + 1, "event_type": kind}
           for i, kind in enumerate(sorted(by_key))}
    rates_raw, rates_rows = read_rates(ROOT / "rates.tsv")
    rates = {
        "dates_ns": np.array([int(datetime.fromisoformat(r["date"]).timestamp()) for r in rates_raw], dtype=np.int64),
        "open": np.array([float(r["open"]) for r in rates_raw], dtype=np.float64),
        "close": np.array([float(r["close"]) for r in rates_raw], dtype=np.float64),
        "max": np.array([float(r["max"]) for r in rates_raw], dtype=np.float64),
        "min": np.array([float(r["min"]) for r in rates_raw], dtype=np.float64),
        "t1": np.array([float(r["t1"]) for r in rates_raw], dtype=np.float64),
        "ext_min": np.array([r["ext_min"] == "1" for r in rates_raw], dtype=np.bool_),
        "ext_max": np.array([r["ext_max"] == "1" for r in rates_raw], dtype=np.bool_),
    }
    dataset_dates = [r["date"] for r in dataset]
    dataset_timestamps = np.array([int(dt.timestamp()) for dt in dataset_dates], dtype=np.int64)
    di = {
        "full_dataset": dataset, "dates": dataset_dates, "by_key": dict(by_key),
        "key_dates": dict(key_dates), "ctx_index": ctx, "np_rates": rates,
        "dataset_timestamps": dataset_timestamps, "is_daily": False,
        "rates_table": "brain_rates_eur_usd",
    }
    fixture = {"event_rows": len(dataset), "rate_rows": len(rates_raw),
               "active_types": len(by_key), "current_rows": len(dataset)}
    table = "brain_rates_eur_usd"
    control_date = datetime(2026, 11, 25)
    seq = rl.collect_extremums_back(di["np_rates"], control_date, limit=50, extremum_interval=3)
    assert len(seq) == 50, len(seq)
    dates = [d for d, _ in seq]
    slots = [(0, 3, ""), (2, 3, "")]

    # Count actual all-slot cube builds in each arm.
    original_compute = model_ns["_compute_all_slots_for_date"]
    compute_count = {"direct": 0, "candidate": 0}
    active_arm = [None]
    def counted_compute(*args, **kwargs):
        compute_count[active_arm[0]] += 1
        return original_compute(*args, **kwargs)
    model_ns["_compute_all_slots_for_date"] = counted_compute

    direct = {}
    model_ns["_SINGLE_CACHE"].clear()
    model_ns["_BATCH_CACHE"].clear()
    active_arm[0] = "direct"
    for _slot in slots:
        typ, var, param = _slot
        direct[_slot] = {
            dt: model_ns["model"](rates_rows, dataset, dt, type=typ, var=var,
                                  param=param, dataset_index=di)
            for dt in dates
        }

    # Execute the reviewed candidate helper against the exact Model80 batch_model.
    state = SimpleNamespace(
        batch_model_fn=model_ns["batch_model"], model_needs_index=True,
        np_rates={table: di["np_rates"]}, global_rates={table: rates_rows},
        dataset_dates=dataset_dates, dataset_by_key=di["by_key"],
        dataset_key_dates=di["key_dates"], dataset_key_field="event_type",
        ctx_index=di["ctx_index"], url_map={}, _dataset_ts_arr=di["dataset_timestamps"],
        FILTER_DATASET_BY_DATE=False, dataset=dataset, _standard_events_by_type={},
        model_uses_rate_history=True, model_can_filter_dataset_by_date=True,
        FILL_ML_BATCH_SIZE=100, FILL_BATCH_MODEL_SIZE=100,
        _ml_active_cache={}, fill_cancel=threading.Event(), fill_status={}, NODE_NAME="fixture",
    )
    async def run_batch_offloop(fn, **kwargs):
        return fn(**kwargs)
    helper_ns = {
        "datetime": datetime, "timedelta": timedelta, "_framework_state": state,
        "_rates_table": lambda pair, day: table, "rl": rl,
        "_run_batch_model_offloop": run_batch_offloop,
        "_ML_ACTIVE_CACHE_MAX": 100000, "_filter_dataset_lte": lambda d, s: s.dataset,
        "log": lambda *args, **kwargs: None,
    }
    prewarm = ast.fix_missing_locations(ast.Module(body=[extract_prewarm(FRAMEWORK)], type_ignores=[]))
    exec(compile(prewarm, str(FRAMEWORK), "exec"), helper_ns)
    active_arm[0] = "candidate"
    warmed = {}
    for typ, var, param in slots:
        warmed[f"type{typ}"] = await helper_ns["_prewarm_ml_active_cache"](
            1, 0, [(typ, var, param)], _state=state,
            exact_ext_dates=dates, request_context=True,
        )
    candidate = {
        (typ, var, param): {
            dt: list(state._ml_active_cache[(1, 0, int(dt.timestamp()), typ, var, param)])
            for dt in dates
        }
        for typ, var, param in slots
    }

    provider_equal = {}
    universe_equal = {}
    precision = {}
    provider_code_counts = {}
    for _typ, _var, param in slots:
        label = f"type{_typ}"
        direct_slot = direct[(_typ, _var, param)]
        candidate_slot = candidate[(_typ, _var, param)]
        provider_equal[label] = all(
            list(direct_slot[dt].keys()) == candidate_slot[dt] for dt in dates
        )
        assert provider_equal[label], f"provider code order differs for type={_typ}"
        provider_code_counts[label] = sum(len(direct_slot[dt]) for dt in dates)
        assert provider_code_counts[label] > 0, f"fixture produced no provider codes for type={_typ}"
        direct_lists = [list(direct_slot[dt].keys()) for dt in dates]
        candidate_lists = [candidate_slot[dt] for dt in dates]
        records_a = rl._build_records_from_seq_codes(
            seq=seq, pre_codes=direct_lists, np_simple_rates=di["np_rates"], train_mode=_typ,
        )
        records_b = rl._build_records_from_seq_codes(
            seq=seq, pre_codes=candidate_lists, np_simple_rates=di["np_rates"], train_mode=_typ,
        )
        train_kwargs = dict(max_iter=20, step=.1, target_precision=.95, active_tail=0,
                            metric="mean", rng_seed=123456)
        trained_a = rl._train_records_numba_exact(records_a, **train_kwargs)
        trained_b = rl._train_records_numba_exact(records_b, **train_kwargs)
        universe_equal[label] = (
            list(trained_a[0].keys()) == list(trained_b[0].keys())
            and trained_a[0] == trained_b[0] and trained_a[1:] == trained_b[1:]
        )
        assert universe_equal[label], f"ReverseStore training result/key order differs for type={_typ}"
        precision[label] = trained_a[1]

    fixture_sha = hashlib.sha256((ROOT / "rates.tsv").read_bytes()).hexdigest()
    receipt = {
        "candidate_sha256": hashlib.sha256(FRAMEWORK.read_bytes()).hexdigest(),
        "model80_sha256": hashlib.sha256(MODEL.read_bytes()).hexdigest(),
        "reverse_learning_sha256": hashlib.sha256(REVERSE.read_bytes()).hexdigest(),
        "rates_fixture_sha256": fixture_sha,
        "fixture": fixture,
        "control_date": control_date.isoformat(sep=" "),
        "provider_sequence": [[dt.isoformat(sep=" "), sign] for dt, sign in seq],
        "providers_equal_including_code_order_by_param": provider_equal,
        "provider_code_count_by_param": provider_code_counts,
        "trained_reverse_universe_values_and_key_order_equal_by_param": universe_equal,
        "precision_by_mode": precision,
        "all_slot_cube_computations_direct": compute_count["direct"],
        "all_slot_cube_computations_candidate": compute_count["candidate"],
        "batch_rows_warmed_per_param": warmed,
    }
    assert receipt["all_slot_cube_computations_direct"] == 100, receipt
    assert receipt["all_slot_cube_computations_candidate"] == 50, receipt
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
