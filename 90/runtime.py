"""Fixed eight-feature runtime for Brain Framework batch_model/model hooks."""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
import json
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

import numpy as np

from selected_indicators import SLOT_FEATURES, FeatureSpec, calculate_feature, encode_feature


PAIR_DAY_TABLE = {
    (1, 0): "brain_rates_eur_usd",
    (1, 1): "brain_rates_eur_usd_day",
    (3, 0): "brain_rates_btc_usd",
    (3, 1): "brain_rates_btc_usd_day",
    (4, 0): "brain_rates_eth_usd",
    (4, 1): "brain_rates_eth_usd_day",
}


class FixedIndicatorRuntime:
    def __init__(self, state_path: str | Path, max_cache: int = 32):
        self.state_path = Path(state_path)
        self.max_cache = max(8, int(max_cache))
        self._state: dict[str, Any] = {}
        self._cache: OrderedDict[tuple, np.ndarray] = OrderedDict()
        self._lock = RLock()
        self.reload()

    def reload(self) -> dict[str, Any]:
        state = json.loads(self.state_path.read_text(encoding="utf-8"))
        if int(state.get("schema_version", 0)) != 1:
            raise RuntimeError("unsupported model_state.json schema")
        for table, specs in SLOT_FEATURES.items():
            expected = {spec.code for spec in specs}
            actual = {item["code"] for item in state["slots"][table]["features"]}
            if expected != actual:
                raise RuntimeError(f"{table}: state features do not match service code")
        with self._lock:
            self._state = state
            self._cache.clear()
        return {
            "revision": state["revision"],
            "slots": len(state["slots"]),
            "features": sum(len(slot["features"]) for slot in state["slots"].values()),
        }

    @staticmethod
    def table_from_index(dataset_index: Mapping[str, Any]) -> str | None:
        table = str(dataset_index.get("rates_table") or "")
        return table if table in SLOT_FEATURES else None

    def cache_revision(self, pair: int, day: int, param: str = "") -> str:
        table = PAIR_DAY_TABLE.get((int(pair), int(day)))
        if table not in self._state.get("slots", {}):
            return "best:none"
        selected = str(param or "").strip()
        return f"best:{self._state['revision']}:{table}:{selected}"

    @staticmethod
    def _quotes(np_rates: Mapping[str, Any]) -> tuple[np.ndarray, ...]:
        def get(name: str, fallback: str | None = None, dtype=np.float64):
            value = np_rates.get(name)
            if value is None and fallback:
                value = np_rates.get(fallback)
            if value is None:
                raise ValueError(f"np_rates has no {name}")
            return np.asarray(value, dtype=dtype)

        dates = get("dates_ns", dtype=np.int64)
        open_values = get("open")
        high = get("high", "max")
        low = get("low", "min")
        close = get("close")
        lengths = {len(dates), len(open_values), len(high), len(low), len(close)}
        if len(lengths) != 1:
            raise ValueError(f"quote arrays have different lengths: {sorted(lengths)}")
        return dates, open_values, high, low, close

    def _encoded(
        self,
        table: str,
        spec: FeatureSpec,
        encoder: dict[str, Any],
        quotes: tuple[np.ndarray, ...],
    ) -> np.ndarray:
        dates, open_values, high, low, close = quotes
        key = (
            table, spec.code, id(dates), len(dates),
            int(dates[-1]) if len(dates) else 0,
            float(close[-1]) if len(close) else 0.0,
            self._state["revision"],
        )
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                return cached
        values = calculate_feature(spec, open_values, high, low, close)
        encoded = encode_feature(values, spec, encoder)
        with self._lock:
            self._cache[key] = encoded
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_cache:
                self._cache.popitem(last=False)
        return encoded

    def batch(
        self,
        dates: list[datetime],
        dataset_index: Mapping[str, Any],
        calc_type: int = 0,
        param: str = "",
    ) -> dict[datetime, dict[str, float]]:
        result = {date: {} for date in dates}
        table = self.table_from_index(dataset_index)
        np_rates = dataset_index.get("np_rates")
        if table is None or np_rates is None or int(calc_type) not in (0, 1, 2):
            return result
        slot = self._state["slots"][table]
        valid_from_ts = int(datetime.fromisoformat(slot["valid_from_utc"]).timestamp())
        selected_param = str(param or "").strip()
        feature_rows = slot["features"]
        if selected_param:
            prefix = "feature:"
            if not selected_param.startswith(prefix):
                return result
            code = selected_param[len(prefix):]
            feature_rows = [item for item in feature_rows if item["code"] == code]
            if not feature_rows:
                return result

        specs = {spec.code: spec for spec in SLOT_FEATURES[table]}
        quotes = self._quotes(np_rates)
        quote_dates = quotes[0]
        prepared = []
        for item in feature_rows:
            spec = specs[item["code"]]
            prepared.append((item, self._encoded(table, spec, item["encoder"], quotes)))

        for target in dates:
            target_ts = int(target.timestamp())
            if target_ts < valid_from_ts:
                continue
            source_index = int(np.searchsorted(quote_dates, target_ts, side="left")) - 1
            if source_index < 0:
                continue
            payload = result[target]
            for item, states in prepared:
                state = states[source_index]
                learned = item["state_model"].get(str(state)) if state is not None else None
                if not learned:
                    continue
                code = item["code"]
                weight = float(item.get("item_weight", 1.0))
                if int(calc_type) in (0, 1):
                    value = float(learned.get("mean_bps", 0.0)) * weight
                    if value != 0.0:
                        payload[f"best:{code}:m:{state}"] = value
                if int(calc_type) in (0, 2):
                    value = float(learned.get("confidence", 0.0)) * weight
                    if value != 0.0:
                        payload[f"best:{code}:p:{state}"] = value
        return result
