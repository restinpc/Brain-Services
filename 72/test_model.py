from datetime import datetime, timedelta
import importlib.util
from pathlib import Path

import numpy as np

MODEL_PATH = Path(__file__).with_name("model.py")
spec = importlib.util.spec_from_file_location("model28", MODEL_PATH)
model28 = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(model28)


def _fixture():
    start = datetime(2025, 1, 1)
    rates = []
    for i in range(40):
        dt = start + timedelta(hours=i)
        rates.append({
            "date": dt,
            "open": 1.0,
            "close": 1.0 + (0.01 if i % 2 else -0.01),
            "max": 1.03 + i * 0.0001,
            "min": 0.97 - i * 0.0001,
            "t1": 0.01 if i % 2 else -0.01,
        })
    dataset = [
        {"event_id": 7, "date": start + timedelta(hours=5), "importance": 2,
         "actual": "2", "forecast": "1", "previous": "0"},
        {"event_id": 7, "date": start + timedelta(hours=15), "importance": 2,
         "actual": "3", "forecast": "2", "previous": "1"},
        {"event_id": 7, "date": start + timedelta(hours=25), "importance": 2,
         "actual": "4", "forecast": "3", "previous": "2"},
    ]
    dates_ns = np.array([int(r["date"].timestamp()) for r in rates], dtype=np.int64)
    np_rates = {
        "dates_ns": dates_ns,
        "open": np.array([r["open"] for r in rates]),
        "close": np.array([r["close"] for r in rates]),
        "t1": np.array([r["t1"] for r in rates]),
        "ranges": np.array([r["max"] - r["min"] for r in rates]),
        "ext_min": np.array([False] * len(rates)),
        "ext_max": np.array([False] * len(rates)),
    }
    ctx = {(7, "UP", "BEAT", "UP"): {"occurrence_count": 3}}
    index = {
        "ctx_index": ctx,
        "full_dataset": dataset,
        "np_rates": np_rates,
        "is_daily": False,
        "rates_table": "brain_rates_eur_usd",
    }
    return rates, dataset, index, start


def test_model_returns_framework_weight_codes():
    rates, dataset, index, start = _fixture()
    result = model28.model(
        rates, dataset, start + timedelta(hours=25), type=1, var=0,
        dataset_index=index,
    )
    assert result
    assert all(code.startswith("7__UP__BEAT__UP__0__") for code in result)


def test_type_filters_modes():
    rates, dataset, index, start = _fixture()
    target = start + timedelta(hours=25)
    t1 = model28.model(rates, dataset, target, type=1, var=0, dataset_index=index)
    ext = model28.model(rates, dataset, target, type=2, var=0, dataset_index=index)
    assert all("__0__" in code for code in t1)
    assert all("__1__" in code for code in ext)


def test_ml_types_return_both_code_modes():
    rates, dataset, index, start = _fixture()
    target = start + timedelta(hours=25)
    for calc_type in (3, 4):
        result = model28.model(
            rates, dataset, target, type=calc_type, var=0, dataset_index=index
        )
        assert result
        assert any("__0__" in code for code in result)
        assert any("__1__" in code for code in result)


def test_invalid_type_or_var_is_empty():
    rates, dataset, index, start = _fixture()
    assert model28.model(rates, dataset, start, type=9, var=0, dataset_index=index) == {}
    assert model28.model(rates, dataset, start, type=0, var=9, dataset_index=index) == {}
