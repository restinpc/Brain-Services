"""Brain service: fixed best OHLC indicators for all six pair/timeframe slots."""

from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Any

from brain_framework import build_app
from runtime import FixedIndicatorRuntime


SERVICE_ID = int(os.getenv("SERVICE_ID", "0"))
PORT = int(os.getenv("PORT", "9000"))
NODE_NAME = os.getenv("NODE_NAME", "brain-best-ohlc-indicators")
SERVICE_TEXT = "Eight frozen causal OHLC indicator features"

RATES_TABLE = "brain_rates_eur_usd"
VAR_RANGE = [0]
TYPES_RANGE = [0, 1, 2]
PARAM_RANGE = [""]
CACHE_DATE_FROM = "2022-01-01"
CACHE_FRESH_LAG_DAYS = 14
MODEL_USES_RATE_HISTORY = True
MODEL_CAN_FILTER_DATASET_BY_DATE = False
USE_ML_VALUES = False


_runtime = FixedIndicatorRuntime(Path(__file__).with_name("model_state.json"))


async def initialize_model(engine_vlad: Any, engine_brain: Any) -> dict[str, Any]:
    return _runtime.reload()


async def enrich_dataset(engine_vlad: Any, engine_brain: Any) -> dict[str, Any]:
    """No-op rebuild hook: this service has no derived dataset/index to rebuild."""
    return {
        "mode": "noop",
        "reason": "fixed_model_state_no_index",
    }


def cache_revision(pair: int, day: int, param: str = "") -> str:
    return _runtime.cache_revision(pair, day, param)


def batch_model(rates, dataset, dates, type=0, var=0, param="", dataset_index=None):
    if int(var) != 0 or dataset_index is None:
        return {date: {} for date in dates}
    return _runtime.batch(list(dates), dataset_index, calc_type=int(type), param=str(param or ""))


def model(rates, dataset, date, type=0, var=0, param="", dataset_index=None):
    return batch_model(
        rates=rates,
        dataset=dataset,
        dates=[date],
        type=type,
        var=var,
        param=param,
        dataset_index=dataset_index,
    ).get(date, {})


app = build_app(sys.modules[__name__])
