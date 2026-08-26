"""The eight frozen OHLC feature definitions and their causal calculations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FeatureSpec:
    code: str
    function: str
    params: dict[str, Any]
    output: str
    representation: str
    pattern_length: int = 1
    forced_experimental: bool = False


SLOT_FEATURES: dict[str, tuple[FeatureSpec, ...]] = {
    "brain_rates_eur_usd": (
        FeatureSpec("eur_h1_cmo5_lp3", "CMO", {"timeperiod": 5}, "real", "level_pattern", 3),
        FeatureSpec("eur_h1_bb10_15_level", "BBANDS", {"timeperiod": 10, "nbdev": 1.5}, "percent_b", "level"),
    ),
    "brain_rates_eur_usd_day": (
        FeatureSpec("eur_d1_trima5_lp3", "TRIMA", {"timeperiod": 5}, "slope_bps", "level_pattern", 3),
        FeatureSpec("eur_d1_rvi10_lp2", "RVI", {"timeperiod": 10, "signalperiod": 4}, "spread", "level_pattern", 2),
    ),
    "brain_rates_btc_usd": (
        FeatureSpec("btc_h1_bb50_shape3", "BBANDS", {"timeperiod": 50, "nbdev": 2.0}, "percent_b", "shape", 3),
    ),
    "brain_rates_btc_usd_day": (
        FeatureSpec("btc_d1_ema20_50_shape3", "EMA_CROSS", {"fastperiod": 20, "slowperiod": 50}, "spread_bps", "shape", 3),
    ),
    "brain_rates_eth_usd": (
        FeatureSpec(
            "eth_h1_rsi200_lp2", "RSI", {"timeperiod": 200}, "real",
            "level_pattern", 2, forced_experimental=True,
        ),
    ),
    "brain_rates_eth_usd_day": (
        FeatureSpec("eth_d1_bb50_width_shape3", "BBANDS", {"timeperiod": 50, "nbdev": 2.0}, "bandwidth_bps", "shape", 3),
    ),
}


def _series(values: np.ndarray) -> pd.Series:
    return pd.Series(np.asarray(values, dtype=np.float64), copy=False)


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def _rma(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def _weighted_four(values: np.ndarray) -> np.ndarray:
    output = np.full(len(values), np.nan, dtype=np.float64)
    if len(values) >= 4:
        windows = np.lib.stride_tricks.sliding_window_view(values, 4)
        output[3:] = (
            windows[:, 0] + 2.0 * windows[:, 1]
            + 2.0 * windows[:, 2] + windows[:, 3]
        ) / 6.0
    return output


def calculate_feature(
    spec: FeatureSpec,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
) -> np.ndarray:
    open_s, high, low, close = map(
        _series, (open_values, high_values, low_values, close_values)
    )
    function = spec.function.upper()

    if function == "CMO":
        period = int(spec.params["timeperiod"])
        delta = close.diff()
        gains = delta.clip(lower=0.0).rolling(period).sum()
        losses = (-delta.clip(upper=0.0)).rolling(period).sum()
        return (100.0 * (gains - losses) / (gains + losses).replace(0.0, np.nan)).to_numpy()

    if function == "RSI":
        period = int(spec.params["timeperiod"])
        delta = close.diff()
        gain = _rma(delta.clip(lower=0.0), period)
        loss = _rma((-delta).clip(lower=0.0), period)
        rs = gain / loss.replace(0.0, np.nan)
        rsi = 100.0 - 100.0 / (1.0 + rs)
        rsi = rsi.where(loss != 0.0, 100.0)
        return rsi.where(gain != 0.0, 0.0).to_numpy()

    if function == "BBANDS":
        period = int(spec.params["timeperiod"])
        deviation = float(spec.params["nbdev"])
        middle = close.rolling(period).mean()
        sigma = close.rolling(period).std(ddof=0)
        upper, lower = middle + deviation * sigma, middle - deviation * sigma
        if spec.output == "percent_b":
            return ((close - lower) / (upper - lower).replace(0.0, np.nan)).to_numpy()
        if spec.output == "bandwidth_bps":
            return ((upper - lower) / middle.replace(0.0, np.nan) * 10000.0).to_numpy()

    if function == "TRIMA":
        period = int(spec.params["timeperiod"])
        trima = close.rolling((period + 1) // 2).mean().rolling(period // 2 + 1).mean()
        if spec.output == "slope_bps":
            return (trima.pct_change(fill_method=None) * 10000.0).to_numpy()

    if function == "RVI":
        period = int(spec.params["timeperiod"])
        numerator = _series(_weighted_four(close.to_numpy() - open_s.to_numpy())).rolling(period).sum()
        denominator = _series(_weighted_four(high.to_numpy() - low.to_numpy())).rolling(period).sum()
        rvi = numerator / denominator.replace(0.0, np.nan)
        signal = _series(_weighted_four(rvi.to_numpy()))
        if spec.output == "spread":
            return (rvi - signal).to_numpy()

    if function == "EMA_CROSS":
        fast = _ema(close, int(spec.params["fastperiod"]))
        slow = _ema(close, int(spec.params["slowperiod"]))
        if spec.output == "spread_bps":
            return ((fast / slow - 1.0) * 10000.0).to_numpy()

    raise ValueError(f"unsupported fixed feature: {spec}")


def fit_encoder(values: np.ndarray, spec: FeatureSpec, fit_mask: np.ndarray) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    finite_fit = np.asarray(fit_mask, dtype=bool) & np.isfinite(values)
    if not np.any(finite_fit):
        raise ValueError(f"{spec.code}: no finite training values")
    if spec.representation in {"level", "level_pattern"}:
        edges = np.unique(np.quantile(values[finite_fit], np.linspace(0.0, 1.0, 9)[1:-1]))
        return {
            "kind": spec.representation,
            "edges": edges.tolist(),
            "pattern_length": int(spec.pattern_length),
        }
    if spec.representation == "shape":
        diff = np.diff(values)
        valid = fit_mask[1:] & np.isfinite(values[1:]) & np.isfinite(values[:-1])
        scale = float(np.median(np.abs(diff[valid]))) if np.any(valid) else 0.0
        return {
            "kind": "shape",
            "epsilon": max(scale * 0.05, 1e-12),
            "pattern_length": int(spec.pattern_length),
        }
    raise ValueError(f"unsupported representation: {spec.representation}")


def encode_feature(values: np.ndarray, spec: FeatureSpec, encoder: dict[str, Any]) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    output = np.empty(len(values), dtype=object)
    output[:] = None
    finite = np.isfinite(values)

    if spec.representation in {"level", "level_pattern"}:
        edges = np.asarray(encoder["edges"], dtype=np.float64)
        indexes = np.flatnonzero(finite)
        bins = np.searchsorted(edges, values[finite], side="right")
        if spec.representation == "level":
            output[indexes] = [f"L{int(value)}" for value in bins]
            return output
        codes = np.empty(len(values), dtype=object)
        codes[:] = None
        codes[indexes] = [str(int(value)) for value in bins]
        length = int(spec.pattern_length)
        for idx in range(length - 1, len(values)):
            tokens = codes[idx - length + 1:idx + 1]
            if all(token is not None for token in tokens):
                output[idx] = "LP" + "_".join(str(token) for token in tokens)
        return output

    if spec.representation == "shape":
        directions = np.empty(len(values), dtype=object)
        directions[:] = None
        epsilon = float(encoder["epsilon"])
        valid = finite[1:] & finite[:-1]
        delta = values[1:] - values[:-1]
        indexes = np.arange(1, len(values))[valid]
        tokens = np.where(delta[valid] > epsilon, "U", np.where(delta[valid] < -epsilon, "D", "F"))
        directions[indexes] = tokens
        movements = max(1, int(spec.pattern_length) - 1)
        for idx in range(movements, len(values)):
            tokens = directions[idx - movements + 1:idx + 1]
            if all(token is not None for token in tokens):
                output[idx] = "S" + "".join(str(token) for token in tokens)
        return output

    raise ValueError(f"unsupported representation: {spec.representation}")
