"""
Fixed technical-analysis figure catalog and causal detectors.

The public contract is deliberately narrow: given OHLC known up to bar i,
return the list of figure names from FIGURE_CATALOG that are visible on
that bar.  Downstream code (enrich_dataset / reverse_learning) only consumes
that list; it does not interpret candlestick geometry itself.

Detection is rule-based (if/elif families).  Overlapping figures on one bar
are allowed: a hammer may coincide with support_bounce and rsi_oversold.
Persistent regimes (RSI already overbought, price still above EMA) fire only
on the transition bar so the event stream stays sparse enough for analog
matching.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import numpy as np


# ── Fixed catalog.  Detectors may only emit names from this list. ─────────────
FIGURE_CATALOG: tuple[str, ...] = (
    # Single-candle
    "doji",
    "dragonfly_doji",
    "gravestone_doji",
    "long_legged_doji",
    "four_price_doji",
    "hammer",
    "hanging_man",
    "inverted_hammer",
    "shooting_star",
    "takuri",
    "spinning_top_bull",
    "spinning_top_bear",
    "high_wave",
    "marubozu_bull",
    "marubozu_bear",
    "closing_marubozu_bull",
    "closing_marubozu_bear",
    "opening_marubozu_bull",
    "opening_marubozu_bear",
    "belt_hold_bull",
    "belt_hold_bear",
    "long_white_candle",
    "long_black_candle",
    "umbrella",
    "inverted_umbrella",
    "rickshaw_man",
    # Two-candle
    "bullish_engulfing",
    "bearish_engulfing",
    "piercing_line",
    "dark_cloud_cover",
    "bullish_harami",
    "bearish_harami",
    "harami_cross_bull",
    "harami_cross_bear",
    "tweezer_bottom",
    "tweezer_top",
    "kicking_bull",
    "kicking_bear",
    "separating_lines_bull",
    "separating_lines_bear",
    "counterattack_bull",
    "counterattack_bear",
    "matching_low",
    "matching_high",
    "homing_pigeon",
    "descending_hawk",
    "on_neck",
    "in_neck",
    "thrusting",
    "rising_window",
    "falling_window",
    # Three-or-more candles
    "morning_star",
    "evening_star",
    "morning_doji_star",
    "evening_doji_star",
    "three_white_soldiers",
    "three_black_crows",
    "three_inside_up",
    "three_inside_down",
    "three_outside_up",
    "three_outside_down",
    "abandoned_baby_bull",
    "abandoned_baby_bear",
    "tri_star_bull",
    "tri_star_bear",
    "unique_three_river",
    "stick_sandwich",
    "three_line_strike_bull",
    "three_line_strike_bear",
    "upside_tasuki_gap",
    "downside_tasuki_gap",
    "side_by_side_white",
    "advance_block",
    "deliberation",
    "two_crows",
    "upside_gap_two_crows",
    "breakaway_bull",
    "breakaway_bear",
    "concealing_baby_swallow",
    "ladder_bottom",
    "identical_three_crows",
    "rising_three_methods",
    "falling_three_methods",
    # Swing / chart
    "double_top",
    "double_bottom",
    "triple_top",
    "triple_bottom",
    "head_and_shoulders",
    "inverse_head_and_shoulders",
    "rising_wedge",
    "falling_wedge",
    "ascending_triangle",
    "descending_triangle",
    "symmetrical_triangle",
    "bull_flag",
    "bear_flag",
    "bull_pennant",
    "bear_pennant",
    "rectangle_break_up",
    "rectangle_break_down",
    "channel_up_break",
    "channel_down_break",
    "rounding_bottom",
    "rounding_top",
    "cup_and_handle",
    "island_reversal_bull",
    "island_reversal_bear",
    "v_bottom",
    "v_top",
    "broadening_top",
    "diamond_top",
    "higher_high",
    "lower_low",
    "higher_low",
    "lower_high",
    # Indicator / regime transitions
    "rsi_overbought",
    "rsi_oversold",
    "rsi_bull_div",
    "rsi_bear_div",
    "macd_bull_cross",
    "macd_bear_cross",
    "macd_hist_turn_up",
    "macd_hist_turn_down",
    "golden_cross",
    "death_cross",
    "ema_stack_bull",
    "ema_stack_bear",
    "bb_squeeze_on",
    "bb_break_up",
    "bb_break_down",
    "bb_walk_up",
    "bb_walk_down",
    "stoch_overbought",
    "stoch_oversold",
    "stoch_bull_cross",
    "stoch_bear_cross",
    "atr_expansion",
    "atr_compression",
    "support_bounce",
    "resistance_reject",
    "trend_flip_bull",
    "trend_flip_bear",
)

FIGURE_SET = frozenset(FIGURE_CATALOG)
PIVOT_RIGHT = 3


@dataclass
class Bars:
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    body: np.ndarray
    rng: np.ndarray
    upper: np.ndarray
    lower: np.ndarray
    mid: np.ndarray
    atr: np.ndarray
    sma10: np.ndarray
    sma20: np.ndarray
    sma50: np.ndarray
    ema12: np.ndarray
    ema26: np.ndarray
    ema50: np.ndarray
    ema200: np.ndarray
    rsi: np.ndarray
    macd: np.ndarray
    macd_signal: np.ndarray
    macd_hist: np.ndarray
    bb_mid: np.ndarray
    bb_up: np.ndarray
    bb_dn: np.ndarray
    bb_width: np.ndarray
    stoch_k: np.ndarray
    stoch_d: np.ndarray
    pivot_kind: np.ndarray
    pivot_price: np.ndarray


def _ema(values: np.ndarray, period: int) -> np.ndarray:
    n = len(values)
    out = np.full(n, np.nan, dtype=np.float64)
    if n == 0 or period <= 0:
        return out
    alpha = 2.0 / (period + 1.0)
    start = 0
    while start < n and not np.isfinite(values[start]):
        start += 1
    if start >= n:
        return out
    out[start] = values[start]
    for i in range(start + 1, n):
        prev = out[i - 1]
        x = values[i]
        if not np.isfinite(x):
            out[i] = prev
        elif not np.isfinite(prev):
            out[i] = x
        else:
            out[i] = x * alpha + prev * (1.0 - alpha)
    return out


def _sma(values: np.ndarray, period: int) -> np.ndarray:
    n = len(values)
    out = np.full(n, np.nan, dtype=np.float64)
    if n < period or period <= 0:
        return out
    window = np.ones(period, dtype=np.float64) / period
    conv = np.convolve(values, window, mode="valid")
    out[period - 1 :] = conv
    return out


def _true_range(h: np.ndarray, l: np.ndarray, c: np.ndarray) -> np.ndarray:
    prev_c = np.empty_like(c)
    prev_c[0] = c[0]
    prev_c[1:] = c[:-1]
    return np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))


def _rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    n = len(close)
    out = np.full(n, np.nan, dtype=np.float64)
    if n < period + 1:
        return out
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0.0, delta, 0.0)
    loss = np.where(delta < 0.0, -delta, 0.0)
    avg_gain = np.mean(gain[1 : period + 1])
    avg_loss = np.mean(loss[1 : period + 1])
    if avg_loss <= 1e-18:
        out[period] = 100.0
    else:
        out[period] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gain[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss[i]) / period
        if avg_loss <= 1e-18:
            out[i] = 100.0
        else:
            out[i] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return out


def _stoch(h: np.ndarray, l: np.ndarray, c: np.ndarray, period: int = 14) -> tuple[np.ndarray, np.ndarray]:
    n = len(c)
    k = np.full(n, np.nan, dtype=np.float64)
    for i in range(period - 1, n):
        hh = np.max(h[i - period + 1 : i + 1])
        ll = np.min(l[i - period + 1 : i + 1])
        denom = hh - ll
        k[i] = 50.0 if denom <= 1e-18 else 100.0 * (c[i] - ll) / denom
    d = _sma(np.nan_to_num(k, nan=50.0), 3)
    d[: period + 1] = np.nan
    return k, d


def _rolling_std(values: np.ndarray, period: int) -> np.ndarray:
    n = len(values)
    out = np.full(n, np.nan, dtype=np.float64)
    if n < period:
        return out
    c1 = np.cumsum(values)
    c2 = np.cumsum(values * values)
    for i in range(period - 1, n):
        j = i - period + 1
        s = c1[i] - (c1[j - 1] if j else 0.0)
        q = c2[i] - (c2[j - 1] if j else 0.0)
        mean = s / period
        var = max(q / period - mean * mean, 0.0)
        out[i] = np.sqrt(var)
    return out


def _mark_pivots(h: np.ndarray, l: np.ndarray, left: int = 3, right: int = PIVOT_RIGHT) -> tuple[np.ndarray, np.ndarray]:
    n = len(h)
    kind = np.zeros(n, dtype=np.int8)
    price = np.full(n, np.nan, dtype=np.float64)
    for i in range(left, n - right):
        window_h = h[i - left : i + right + 1]
        window_l = l[i - left : i + right + 1]
        if h[i] >= np.max(window_h) and h[i] > np.max(h[i + 1 : i + right + 1]):
            kind[i] = 1
            price[i] = h[i]
        elif l[i] <= np.min(window_l) and l[i] < np.min(l[i + 1 : i + right + 1]):
            kind[i] = -1
            price[i] = l[i]
    return kind, price


def build_bars(open_: Iterable[float], high: Iterable[float], low: Iterable[float], close: Iterable[float]) -> Bars:
    o = np.asarray(list(open_), dtype=np.float64)
    h = np.asarray(list(high), dtype=np.float64)
    l = np.asarray(list(low), dtype=np.float64)
    c = np.asarray(list(close), dtype=np.float64)
    n = len(c)
    if not (len(o) == len(h) == len(l) == n):
        raise ValueError("OHLC arrays must have equal length")

    top = np.maximum(o, c)
    bot = np.minimum(o, c)
    body = np.abs(c - o)
    rng = np.maximum(h - l, 1e-12)
    upper = np.maximum(h - top, 0.0)
    lower = np.maximum(bot - l, 0.0)
    mid = (h + l) / 2.0
    tr = _true_range(h, l, c)
    atr = _ema(tr, 14)
    sma20 = _sma(c, 20)
    bb_std = _rolling_std(c, 20)
    macd = _ema(c, 12) - _ema(c, 26)
    macd_signal = _ema(np.nan_to_num(macd, nan=0.0), 9)
    stoch_k, stoch_d = _stoch(h, l, c)
    pivot_kind, pivot_price = _mark_pivots(h, l)
    return Bars(
        o=o,
        h=h,
        l=l,
        c=c,
        body=body,
        rng=rng,
        upper=upper,
        lower=lower,
        mid=mid,
        atr=atr,
        sma10=_sma(c, 10),
        sma20=sma20,
        sma50=_sma(c, 50),
        ema12=_ema(c, 12),
        ema26=_ema(c, 26),
        ema50=_ema(c, 50),
        ema200=_ema(c, 200),
        rsi=_rsi(c),
        macd=macd,
        macd_signal=macd_signal,
        macd_hist=macd - macd_signal,
        bb_mid=sma20,
        bb_up=sma20 + 2.0 * bb_std,
        bb_dn=sma20 - 2.0 * bb_std,
        bb_width=np.where(np.isfinite(sma20) & (np.abs(sma20) > 1e-12), (4.0 * bb_std) / np.abs(sma20), np.nan),
        stoch_k=stoch_k,
        stoch_d=stoch_d,
        pivot_kind=pivot_kind,
        pivot_price=pivot_price,
    )


def _finite(*values: float) -> bool:
    return all(np.isfinite(v) for v in values)


def _near(a: float, b: float, tol: float) -> bool:
    return abs(a - b) <= tol


def _bull(bars: Bars, i: int) -> bool:
    return bars.c[i] > bars.o[i]


def _bear(bars: Bars, i: int) -> bool:
    return bars.c[i] < bars.o[i]


def _doji_bar(bars: Bars, i: int) -> bool:
    return bars.body[i] <= 0.12 * bars.rng[i]


def _long_bar(bars: Bars, i: int) -> bool:
    atr = bars.atr[i]
    if not np.isfinite(atr) or atr <= 0:
        return bars.body[i] >= 0.65 * bars.rng[i]
    return bars.rng[i] >= 1.15 * atr and bars.body[i] >= 0.55 * bars.rng[i]


def _downtrend(bars: Bars, i: int) -> bool:
    if i < 6 or not _finite(bars.sma10[i], bars.sma10[i - 5]):
        return bars.c[i] < bars.c[max(0, i - 5)]
    return bars.c[i] < bars.sma10[i] and bars.sma10[i] < bars.sma10[i - 5]


def _uptrend(bars: Bars, i: int) -> bool:
    if i < 6 or not _finite(bars.sma10[i], bars.sma10[i - 5]):
        return bars.c[i] > bars.c[max(0, i - 5)]
    return bars.c[i] > bars.sma10[i] and bars.sma10[i] > bars.sma10[i - 5]


def _signed_pct(bars: Bars, i: int, direction: float | None = None) -> float:
    close = bars.c[i]
    if close == 0.0:
        return 0.01 * (1.0 if direction is None else direction)
    move = (bars.c[i] - bars.o[i]) / close * 100.0
    if abs(move) < 1e-9:
        move = 0.01 if bars.c[i] >= bars.o[i] else -0.01
    if direction is None:
        return float(move)
    return float(abs(move) * direction)


def _gap_up(bars: Bars, i: int) -> bool:
    return i >= 1 and bars.l[i] > bars.h[i - 1]


def _gap_down(bars: Bars, i: int) -> bool:
    return i >= 1 and bars.h[i] < bars.l[i - 1]


def _recent_pivots(bars: Bars, i: int, count: int = 8) -> list[tuple[int, int, float]]:
    """Confirmed pivots fully known at bar i (pivot bar + PIVOT_RIGHT <= i)."""
    last = i - PIVOT_RIGHT
    if last < 0:
        return []
    found: list[tuple[int, int, float]] = []
    for j in range(last, -1, -1):
        kind = int(bars.pivot_kind[j])
        if kind == 0:
            continue
        found.append((j, kind, float(bars.pivot_price[j])))
        if len(found) >= count:
            break
    found.reverse()
    return found


def figures_on_chart(bars: Bars, i: int) -> list[tuple[str, float]]:
    """Return (figure_name, signed_pct) visible on bar i using only bars[:i+1]."""
    n = len(bars.c)
    if i < 0 or i >= n:
        return []
    out: list[tuple[str, float]] = []

    def emit(name: str, direction: float | None = None) -> None:
        if name in FIGURE_SET:
            out.append((name, _signed_pct(bars, i, direction)))

    o, h, l, c = bars.o, bars.h, bars.l, bars.c
    body, rng, upper, lower, mid = bars.body, bars.rng, bars.upper, bars.lower, bars.mid

    # ── Single candle ────────────────────────────────────────────────────────
    if rng[i] > 0:
        if body[i] <= 1e-12 * max(abs(c[i]), 1.0) and _near(o[i], h[i], rng[i] * 0.02) and _near(o[i], l[i], rng[i] * 0.02):
            emit("four_price_doji", 1.0 if _uptrend(bars, i) else -1.0)
        elif _doji_bar(bars, i):
            if lower[i] >= 0.55 * rng[i] and upper[i] <= 0.12 * rng[i]:
                emit("dragonfly_doji", 1.0)
            elif upper[i] >= 0.55 * rng[i] and lower[i] <= 0.12 * rng[i]:
                emit("gravestone_doji", -1.0)
            elif upper[i] >= 0.30 * rng[i] and lower[i] >= 0.30 * rng[i]:
                emit("long_legged_doji")
                emit("rickshaw_man")
            else:
                emit("doji")
        elif body[i] <= 0.35 * rng[i] and upper[i] >= 0.25 * rng[i] and lower[i] >= 0.25 * rng[i]:
            emit("high_wave")
            if _bull(bars, i):
                emit("spinning_top_bull", 1.0)
            elif _bear(bars, i):
                emit("spinning_top_bear", -1.0)

        if body[i] > 0.08 * rng[i]:
            if lower[i] >= 2.0 * body[i] and upper[i] <= 0.35 * body[i]:
                emit("umbrella", 1.0 if _downtrend(bars, i) else -1.0)
                if _downtrend(bars, i):
                    emit("hammer", 1.0)
                    if lower[i] >= 3.0 * body[i]:
                        emit("takuri", 1.0)
                elif _uptrend(bars, i):
                    emit("hanging_man", -1.0)
            if upper[i] >= 2.0 * body[i] and lower[i] <= 0.35 * body[i]:
                emit("inverted_umbrella", -1.0 if _uptrend(bars, i) else 1.0)
                if _uptrend(bars, i):
                    emit("shooting_star", -1.0)
                elif _downtrend(bars, i):
                    emit("inverted_hammer", 1.0)

        if body[i] >= 0.88 * rng[i]:
            if _bull(bars, i):
                emit("marubozu_bull", 1.0)
            elif _bear(bars, i):
                emit("marubozu_bear", -1.0)
        else:
            if _bull(bars, i) and upper[i] <= 0.08 * rng[i] and body[i] >= 0.70 * rng[i]:
                emit("closing_marubozu_bull", 1.0)
            if _bear(bars, i) and lower[i] <= 0.08 * rng[i] and body[i] >= 0.70 * rng[i]:
                emit("closing_marubozu_bear", -1.0)
            if _bull(bars, i) and lower[i] <= 0.08 * rng[i] and body[i] >= 0.70 * rng[i]:
                emit("opening_marubozu_bull", 1.0)
            if _bear(bars, i) and upper[i] <= 0.08 * rng[i] and body[i] >= 0.70 * rng[i]:
                emit("opening_marubozu_bear", -1.0)

        if _long_bar(bars, i):
            if _bull(bars, i):
                emit("long_white_candle", 1.0)
                if lower[i] <= 0.12 * rng[i] and _downtrend(bars, i):
                    emit("belt_hold_bull", 1.0)
            elif _bear(bars, i):
                emit("long_black_candle", -1.0)
                if upper[i] <= 0.12 * rng[i] and _uptrend(bars, i):
                    emit("belt_hold_bear", -1.0)

    # ── Two candles ──────────────────────────────────────────────────────────
    if i >= 1:
        p = i - 1
        if _bear(bars, p) and _bull(bars, i) and o[i] <= c[p] and c[i] >= o[p] and body[i] > body[p] and body[p] >= 0.40 * rng[p]:
            emit("bullish_engulfing", 1.0)
        if _bull(bars, p) and _bear(bars, i) and o[i] >= c[p] and c[i] <= o[p] and body[i] > body[p] and body[p] >= 0.40 * rng[p]:
            emit("bearish_engulfing", -1.0)
        if _bear(bars, p) and _bull(bars, i) and o[i] < l[p] and c[i] > mid[p] and c[i] < o[p]:
            emit("piercing_line", 1.0)
        if _bull(bars, p) and _bear(bars, i) and o[i] > h[p] and c[i] < mid[p] and c[i] > o[p]:
            emit("dark_cloud_cover", -1.0)
        if _bear(bars, p) and _bull(bars, i) and o[i] > c[p] and c[i] < o[p] and body[i] < body[p]:
            emit("bullish_harami", 1.0)
            if _doji_bar(bars, i):
                emit("harami_cross_bull", 1.0)
        if _bull(bars, p) and _bear(bars, i) and o[i] < c[p] and c[i] > o[p] and body[i] < body[p]:
            emit("bearish_harami", -1.0)
            if _doji_bar(bars, i):
                emit("harami_cross_bear", -1.0)
        atr_i = bars.atr[i] if np.isfinite(bars.atr[i]) else rng[i]
        if _near(l[i], l[p], 0.08 * atr_i) and _downtrend(bars, i):
            emit("tweezer_bottom", 1.0)
        if _near(h[i], h[p], 0.08 * atr_i) and _uptrend(bars, i):
            emit("tweezer_top", -1.0)
        if _gap_up(bars, i) and _bear(bars, p) and _bull(bars, i) and body[p] >= 0.70 * rng[p] and body[i] >= 0.70 * rng[i]:
            emit("kicking_bull", 1.0)
        if _gap_down(bars, i) and _bull(bars, p) and _bear(bars, i) and body[p] >= 0.70 * rng[p] and body[i] >= 0.70 * rng[i]:
            emit("kicking_bear", -1.0)
        if _bull(bars, p) and _bull(bars, i) and _near(o[i], o[p], 0.12 * atr_i) and c[i] > c[p]:
            emit("separating_lines_bull", 1.0)
        if _bear(bars, p) and _bear(bars, i) and _near(o[i], o[p], 0.12 * atr_i) and c[i] < c[p]:
            emit("separating_lines_bear", -1.0)
        if _bear(bars, p) and _bull(bars, i) and _near(c[i], c[p], 0.12 * atr_i):
            emit("counterattack_bull", 1.0)
        if _bull(bars, p) and _bear(bars, i) and _near(c[i], c[p], 0.12 * atr_i):
            emit("counterattack_bear", -1.0)
        if _bear(bars, p) and _bear(bars, i) and _near(c[i], c[p], 0.08 * atr_i) and _downtrend(bars, i):
            emit("matching_low", 1.0)
        if _bull(bars, p) and _bull(bars, i) and _near(c[i], c[p], 0.08 * atr_i) and _uptrend(bars, i):
            emit("matching_high", -1.0)
        if _bear(bars, p) and _bear(bars, i) and o[i] > c[p] and c[i] < o[p] and body[i] < body[p]:
            emit("homing_pigeon", 1.0)
        if _bull(bars, p) and _bull(bars, i) and o[i] < c[p] and c[i] > o[p] and body[i] < body[p]:
            emit("descending_hawk", -1.0)
        if _bear(bars, p) and _bear(bars, i) and _near(c[i], l[p], 0.12 * atr_i) and o[i] < c[p]:
            emit("on_neck", -1.0)
        elif _bear(bars, p) and _bear(bars, i) and _near(c[i], c[p], 0.12 * atr_i) and o[i] < c[p]:
            emit("in_neck", -1.0)
        elif _bear(bars, p) and _bull(bars, i) and c[i] > c[p] and c[i] < mid[p]:
            emit("thrusting", -1.0)
        if _gap_up(bars, i):
            emit("rising_window", 1.0)
        if _gap_down(bars, i):
            emit("falling_window", -1.0)

    # ── Three or more candles ────────────────────────────────────────────────
    if i >= 2:
        a, b = i - 2, i - 1
        mid_doji = _doji_bar(bars, b)
        if _bear(bars, a) and _bull(bars, i) and body[a] > body[b] and body[i] > body[b] and c[i] > mid[a]:
            if min(o[b], c[b]) < min(c[a], o[i]) and max(o[b], c[b]) < max(o[a], c[a]):
                emit("morning_star", 1.0)
                if mid_doji:
                    emit("morning_doji_star", 1.0)
        if _bull(bars, a) and _bear(bars, i) and body[a] > body[b] and body[i] > body[b] and c[i] < mid[a]:
            if max(o[b], c[b]) > max(c[a], o[i]) and min(o[b], c[b]) > min(o[a], c[a]):
                emit("evening_star", -1.0)
                if mid_doji:
                    emit("evening_doji_star", -1.0)
        if (
            _bull(bars, a) and _bull(bars, b) and _bull(bars, i)
            and c[a] < c[b] < c[i]
            and o[b] > o[a] and o[b] < c[a]
            and o[i] > o[b] and o[i] < c[b]
            and body[a] > 0.5 * rng[a] and body[b] > 0.5 * rng[b] and body[i] > 0.5 * rng[i]
        ):
            emit("three_white_soldiers", 1.0)
            if body[i] < 0.7 * body[a] and body[b] < 0.85 * body[a]:
                emit("advance_block", -1.0)
            if _doji_bar(bars, i) or body[i] < 0.45 * body[b]:
                emit("deliberation", -1.0)
        if (
            _bear(bars, a) and _bear(bars, b) and _bear(bars, i)
            and c[a] > c[b] > c[i]
            and o[b] < o[a] and o[b] > c[a]
            and o[i] < o[b] and o[i] > c[b]
            and body[a] > 0.5 * rng[a] and body[b] > 0.5 * rng[b] and body[i] > 0.5 * rng[i]
        ):
            emit("three_black_crows", -1.0)
            if _near(c[a], c[b], 0.15 * (bars.atr[i] if np.isfinite(bars.atr[i]) else rng[i])) and _near(c[b], c[i], 0.15 * rng[i]):
                emit("identical_three_crows", -1.0)
        if _bear(bars, a) and body[b] < body[a] and o[b] > c[a] and c[b] < o[a] and _bull(bars, i) and c[i] > o[a]:
            emit("three_inside_up", 1.0)
        if _bull(bars, a) and body[b] < body[a] and o[b] < c[a] and c[b] > o[a] and _bear(bars, i) and c[i] < o[a]:
            emit("three_inside_down", -1.0)
        if _bear(bars, a) and _bull(bars, b) and o[b] <= c[a] and c[b] >= o[a] and _bull(bars, i) and c[i] > c[b]:
            emit("three_outside_up", 1.0)
        if _bull(bars, a) and _bear(bars, b) and o[b] >= c[a] and c[b] <= o[a] and _bear(bars, i) and c[i] < c[b]:
            emit("three_outside_down", -1.0)
        if _bear(bars, a) and mid_doji and _bull(bars, i) and _gap_down(bars, b) and l[i] > h[b]:
            emit("abandoned_baby_bull", 1.0)
        if _bull(bars, a) and mid_doji and _bear(bars, i) and _gap_up(bars, b) and h[i] < l[b]:
            emit("abandoned_baby_bear", -1.0)
        if _doji_bar(bars, a) and _doji_bar(bars, b) and _doji_bar(bars, i):
            if c[i] > c[a]:
                emit("tri_star_bull", 1.0)
            elif c[i] < c[a]:
                emit("tri_star_bear", -1.0)
        if _bear(bars, a) and _bull(bars, b) and _bear(bars, i) and l[b] < l[a] and c[i] > l[b] and c[i] < o[i]:
            emit("unique_three_river", 1.0)
        if _bear(bars, a) and _bull(bars, b) and _bear(bars, i) and _near(c[i], c[a], 0.12 * rng[i]):
            emit("stick_sandwich", 1.0)
        if i >= 3:
            z = i - 3
            if _bull(bars, z) and _bull(bars, a) and _bull(bars, b) and _bear(bars, i) and c[i] < o[z]:
                emit("three_line_strike_bull", 1.0)
            if _bear(bars, z) and _bear(bars, a) and _bear(bars, b) and _bull(bars, i) and c[i] > o[z]:
                emit("three_line_strike_bear", -1.0)
            if (
                _bull(bars, z)
                and _gap_up(bars, a)
                and _bear(bars, a) and _bear(bars, b) and _bear(bars, i)
                and c[i] < o[z]
            ):
                emit("concealing_baby_swallow", 1.0)
            if (
                _bear(bars, z) and _bear(bars, a) and _bear(bars, b)
                and o[i] > h[b] and _bull(bars, i)
            ):
                emit("ladder_bottom", 1.0)
        if _gap_up(bars, b) and _bull(bars, a) and _bull(bars, b) and _bear(bars, i) and o[i] < c[b] and c[i] > c[a] and c[i] < o[b]:
            emit("upside_tasuki_gap", 1.0)
        if _gap_down(bars, b) and _bear(bars, a) and _bear(bars, b) and _bull(bars, i) and o[i] > c[b] and c[i] < c[a] and c[i] > o[b]:
            emit("downside_tasuki_gap", -1.0)
        if _gap_up(bars, b) and _bull(bars, a) and _bull(bars, b) and _bull(bars, i) and _near(o[i], o[b], 0.15 * rng[i]):
            emit("side_by_side_white", 1.0)
        if _bull(bars, a) and _gap_up(bars, b) and _bear(bars, b) and _bear(bars, i) and c[i] > c[a]:
            emit("two_crows", -1.0)
            if _gap_up(bars, b) and h[i] < l[b]:
                emit("upside_gap_two_crows", -1.0)
        if i >= 4:
            if _bear(bars, i - 4) and _gap_down(bars, i - 3) and _bull(bars, i) and c[i] > h[i - 4]:
                emit("breakaway_bull", 1.0)
            if _bull(bars, i - 4) and _gap_up(bars, i - 3) and _bear(bars, i) and c[i] < l[i - 4]:
                emit("breakaway_bear", -1.0)
            if (
                _bull(bars, i - 4)
                and _bear(bars, i - 3) and _bear(bars, i - 2) and _bear(bars, i - 1)
                and _bull(bars, i)
                and h[i - 3] < h[i - 4] and l[i - 1] > l[i - 4]
                and c[i] > c[i - 4]
            ):
                emit("rising_three_methods", 1.0)
            if (
                _bear(bars, i - 4)
                and _bull(bars, i - 3) and _bull(bars, i - 2) and _bull(bars, i - 1)
                and _bear(bars, i)
                and l[i - 3] > l[i - 4] and h[i - 1] < h[i - 4]
                and c[i] < c[i - 4]
            ):
                emit("falling_three_methods", -1.0)

    # ── Swing / chart figures (confirmed pivots only) ────────────────────────
    pivots = _recent_pivots(bars, i, 10)
    atr_now = bars.atr[i] if np.isfinite(bars.atr[i]) else rng[i]
    highs = [(idx, price) for idx, kind, price in pivots if kind == 1]
    lows = [(idx, price) for idx, kind, price in pivots if kind == -1]
    if len(highs) >= 2:
        (i1, p1), (i2, p2) = highs[-2], highs[-1]
        if _near(p1, p2, 0.35 * atr_now) and i2 > i1:
            valley = min(l[i1:i2 + 1]) if i2 > i1 else l[i]
            if c[i] < valley and i == min(n - 1, i2 + PIVOT_RIGHT):
                emit("double_top", -1.0)
            if p2 > p1 + 0.15 * atr_now and i >= i2 + PIVOT_RIGHT and i <= i2 + PIVOT_RIGHT + 2:
                emit("higher_high", 1.0)
            if p2 < p1 - 0.15 * atr_now and i >= i2 + PIVOT_RIGHT and i <= i2 + PIVOT_RIGHT + 2:
                emit("lower_high", -1.0)
    if len(lows) >= 2:
        (i1, p1), (i2, p2) = lows[-2], lows[-1]
        if _near(p1, p2, 0.35 * atr_now) and i2 > i1:
            peak = max(h[i1:i2 + 1]) if i2 > i1 else h[i]
            if c[i] > peak and i == min(n - 1, i2 + PIVOT_RIGHT):
                emit("double_bottom", 1.0)
            if p2 < p1 - 0.15 * atr_now and i >= i2 + PIVOT_RIGHT and i <= i2 + PIVOT_RIGHT + 2:
                emit("lower_low", -1.0)
            if p2 > p1 + 0.15 * atr_now and i >= i2 + PIVOT_RIGHT and i <= i2 + PIVOT_RIGHT + 2:
                emit("higher_low", 1.0)
    if len(highs) >= 3:
        (ia, pa), (ib, pb), (ic, pc) = highs[-3], highs[-2], highs[-1]
        if _near(pa, pc, 0.40 * atr_now) and _near(pa, pb, 0.40 * atr_now):
            neck = min(l[ia:ic + 1])
            if c[i] < neck and i <= ic + PIVOT_RIGHT + 2:
                emit("triple_top", -1.0)
        if pb > pa and pb > pc and _near(pa, pc, 0.55 * atr_now):
            neck = min(l[ia:ic + 1])
            if c[i] < neck and i <= ic + PIVOT_RIGHT + 3:
                emit("head_and_shoulders", -1.0)
        if pa < pb < pc and i <= ic + PIVOT_RIGHT + 1:
            emit("broadening_top", -1.0)
    if len(lows) >= 3:
        (ia, pa), (ib, pb), (ic, pc) = lows[-3], lows[-2], lows[-1]
        if _near(pa, pc, 0.40 * atr_now) and _near(pa, pb, 0.40 * atr_now):
            neck = max(h[ia:ic + 1])
            if c[i] > neck and i <= ic + PIVOT_RIGHT + 2:
                emit("triple_bottom", 1.0)
        if pb < pa and pb < pc and _near(pa, pc, 0.55 * atr_now):
            neck = max(h[ia:ic + 1])
            if c[i] > neck and i <= ic + PIVOT_RIGHT + 3:
                emit("inverse_head_and_shoulders", 1.0)
    if len(highs) >= 3 and len(lows) >= 3:
        hh = [p for _, p in highs[-3:]]
        ll = [p for _, p in lows[-3:]]
        if hh[0] > hh[1] > hh[2] and ll[0] < ll[1] < ll[2]:
            if c[i] > hh[-1]:
                emit("falling_wedge", 1.0)
            elif c[i] < ll[-1]:
                emit("falling_wedge", -1.0)
        if hh[0] < hh[1] < hh[2] and ll[0] > ll[1] > ll[2]:
            if c[i] < ll[-1]:
                emit("rising_wedge", -1.0)
            elif c[i] > hh[-1]:
                emit("rising_wedge", 1.0)
        if _near(hh[0], hh[1], 0.25 * atr_now) and _near(hh[1], hh[2], 0.25 * atr_now) and ll[0] < ll[1] < ll[2]:
            if c[i] > max(hh):
                emit("ascending_triangle", 1.0)
        if _near(ll[0], ll[1], 0.25 * atr_now) and _near(ll[1], ll[2], 0.25 * atr_now) and hh[0] > hh[1] > hh[2]:
            if c[i] < min(ll):
                emit("descending_triangle", -1.0)
        if hh[0] > hh[1] > hh[2] and ll[0] < ll[1] < ll[2] and abs((hh[0] - hh[2]) - (ll[2] - ll[0])) <= 0.8 * atr_now:
            if c[i] > hh[-1] or c[i] < ll[-1]:
                emit("symmetrical_triangle", 1.0 if c[i] > hh[-1] else -1.0)
        if _near(max(hh), min(hh), 0.45 * atr_now) and _near(max(ll), min(ll), 0.45 * atr_now):
            if c[i] > max(hh):
                emit("rectangle_break_up", 1.0)
            elif c[i] < min(ll):
                emit("rectangle_break_down", -1.0)
        span_h = max(hh) - min(hh)
        span_l = max(ll) - min(ll)
        if span_h <= 0.8 * atr_now and span_l <= 0.8 * atr_now and i >= 20:
            look = slice(max(0, i - 18), i - 5)
            prior_up = c[i - 5] > c[max(0, i - 18)] + atr_now
            prior_dn = c[i - 5] < c[max(0, i - 18)] - atr_now
            if prior_up and c[i] > max(h[look]):
                emit("bull_flag", 1.0)
                if span_h <= 0.45 * atr_now:
                    emit("bull_pennant", 1.0)
            if prior_dn and c[i] < min(l[look]):
                emit("bear_flag", -1.0)
                if span_l <= 0.45 * atr_now:
                    emit("bear_pennant", -1.0)
        if hh[0] < hh[1] < hh[2] and ll[0] < ll[1] < ll[2] and c[i] < ll[-1]:
            emit("channel_up_break", -1.0)
        if hh[0] > hh[1] > hh[2] and ll[0] > ll[1] > ll[2] and c[i] > hh[-1]:
            emit("channel_down_break", 1.0)
        if len(highs) >= 4 and len(lows) >= 4:
            if hh[0] < hh[1] and hh[1] > hh[2] and ll[0] > ll[1] and ll[1] < ll[2]:
                emit("diamond_top", -1.0)

    if i >= 12:
        left = c[i - 12: i - 7]
        midc = c[i - 7: i - 3]
        right = c[i - 3: i + 1]
        if np.min(midc) < np.min(left) - 0.2 * atr_now and np.min(midc) < np.min(right) - 0.2 * atr_now:
            if np.mean(right) > np.mean(midc) and c[i] > np.max(left):
                emit("rounding_bottom", 1.0)
                emit("cup_and_handle", 1.0)
        if np.max(midc) > np.max(left) + 0.2 * atr_now and np.max(midc) > np.max(right) + 0.2 * atr_now:
            if np.mean(right) < np.mean(midc) and c[i] < np.min(left):
                emit("rounding_top", -1.0)
        if _gap_down(bars, i - 2) and _gap_up(bars, i) and h[i - 1] < l[i] and h[i - 1] < l[i - 2]:
            emit("island_reversal_bull", 1.0)
        if _gap_up(bars, i - 2) and _gap_down(bars, i) and l[i - 1] > h[i] and l[i - 1] > h[i - 2]:
            emit("island_reversal_bear", -1.0)
        if i >= 6:
            drop = c[i - 3] - np.min(c[i - 6: i - 2])
            bounce = c[i] - np.min(c[i - 3: i + 1])
            if drop > 1.4 * atr_now and bounce > 0.9 * atr_now and c[i] > c[i - 1]:
                emit("v_bottom", 1.0)
            rise = np.max(c[i - 6: i - 2]) - c[i - 3]
            fade = np.max(c[i - 3: i + 1]) - c[i]
            if rise > 1.4 * atr_now and fade > 0.9 * atr_now and c[i] < c[i - 1]:
                emit("v_top", -1.0)

    # ── Indicator / regime transitions ───────────────────────────────────────
    if i >= 1:
        rsi, rsi_p = bars.rsi[i], bars.rsi[i - 1]
        if _finite(rsi, rsi_p):
            if rsi_p < 70.0 <= rsi:
                emit("rsi_overbought", -1.0)
            if rsi_p > 30.0 >= rsi:
                emit("rsi_oversold", 1.0)
        macd, sig = bars.macd[i], bars.macd_signal[i]
        macd_p, sig_p = bars.macd[i - 1], bars.macd_signal[i - 1]
        if _finite(macd, sig, macd_p, sig_p):
            if macd_p <= sig_p and macd > sig:
                emit("macd_bull_cross", 1.0)
            if macd_p >= sig_p and macd < sig:
                emit("macd_bear_cross", -1.0)
        hist, hist_p = bars.macd_hist[i], bars.macd_hist[i - 1]
        if i >= 4 and _finite(hist, hist_p, bars.macd_hist[i - 2], bars.macd_hist[i - 3]):
            hist2, hist3 = bars.macd_hist[i - 2], bars.macd_hist[i - 3]
            if hist3 >= hist2 >= hist_p and hist > hist_p and hist_p < 0.0:
                emit("macd_hist_turn_up", 1.0)
            if hist3 <= hist2 <= hist_p and hist < hist_p and hist_p > 0.0:
                emit("macd_hist_turn_down", -1.0)
        e50, e200 = bars.ema50[i], bars.ema200[i]
        e50_p, e200_p = bars.ema50[i - 1], bars.ema200[i - 1]
        if _finite(e50, e200, e50_p, e200_p):
            if e50_p <= e200_p and e50 > e200:
                emit("golden_cross", 1.0)
            if e50_p >= e200_p and e50 < e200:
                emit("death_cross", -1.0)
        e12, e26 = bars.ema12[i], bars.ema26[i]
        e12_p, e26_p = bars.ema12[i - 1], bars.ema26[i - 1]
        if _finite(e12, e26, e50, e12_p, e26_p, e50_p):
            stacked_now = e12 > e26 > e50
            stacked_prev = e12_p > e26_p > e50_p
            if stacked_now and not stacked_prev:
                emit("ema_stack_bull", 1.0)
            stacked_bear_now = e12 < e26 < e50
            stacked_bear_prev = e12_p < e26_p < e50_p
            if stacked_bear_now and not stacked_bear_prev:
                emit("ema_stack_bear", -1.0)
        width, width_p = bars.bb_width[i], bars.bb_width[i - 1]
        if _finite(width, width_p, bars.bb_up[i], bars.bb_dn[i]):
            look_w = bars.bb_width[max(0, i - 40): i + 1]
            look_w = look_w[np.isfinite(look_w)]
            if len(look_w) >= 20:
                q20 = float(np.quantile(look_w, 0.20))
                if width_p > q20 >= width:
                    emit("bb_squeeze_on", 1.0 if _uptrend(bars, i) else -1.0)
            if c[i - 1] <= bars.bb_up[i - 1] and c[i] > bars.bb_up[i]:
                emit("bb_break_up", 1.0)
            if c[i - 1] >= bars.bb_dn[i - 1] and c[i] < bars.bb_dn[i]:
                emit("bb_break_down", -1.0)
            if i >= 3 and np.all(c[i - 2: i + 1] > bars.bb_up[i - 2: i + 1]):
                prev_walk = i >= 4 and np.all(c[i - 3: i] > bars.bb_up[i - 3: i])
                if not prev_walk:
                    emit("bb_walk_up", 1.0)
            if i >= 3 and np.all(c[i - 2: i + 1] < bars.bb_dn[i - 2: i + 1]):
                prev_walk = i >= 4 and np.all(c[i - 3: i] < bars.bb_dn[i - 3: i])
                if not prev_walk:
                    emit("bb_walk_down", -1.0)
        k, d = bars.stoch_k[i], bars.stoch_d[i]
        k_p, d_p = bars.stoch_k[i - 1], bars.stoch_d[i - 1]
        if _finite(k, d, k_p, d_p):
            if k_p < 80.0 <= k:
                emit("stoch_overbought", -1.0)
            if k_p > 20.0 >= k:
                emit("stoch_oversold", 1.0)
            if k_p <= d_p and k > d and k < 30.0:
                emit("stoch_bull_cross", 1.0)
            if k_p >= d_p and k < d and k > 70.0:
                emit("stoch_bear_cross", -1.0)
        atr, atr_p = bars.atr[i], bars.atr[i - 1]
        if i >= 20 and _finite(atr, atr_p):
            atr_ma = float(np.nanmean(bars.atr[i - 20: i + 1]))
            if atr_ma > 0:
                if atr_p <= 1.35 * atr_ma < atr:
                    emit("atr_expansion", 1.0 if c[i] >= o[i] else -1.0)
                if atr_p >= 0.70 * atr_ma > atr:
                    emit("atr_compression")
        if _finite(bars.sma20[i], bars.sma20[i - 1]):
            if c[i - 1] <= bars.sma20[i - 1] and c[i] > bars.sma20[i] and l[i] <= bars.sma20[i]:
                emit("support_bounce", 1.0)
            if c[i - 1] >= bars.sma20[i - 1] and c[i] < bars.sma20[i] and h[i] >= bars.sma20[i]:
                emit("resistance_reject", -1.0)
            if _finite(bars.sma50[i], bars.sma50[i - 1]):
                if bars.sma20[i - 1] <= bars.sma50[i - 1] and bars.sma20[i] > bars.sma50[i]:
                    emit("trend_flip_bull", 1.0)
                if bars.sma20[i - 1] >= bars.sma50[i - 1] and bars.sma20[i] < bars.sma50[i]:
                    emit("trend_flip_bear", -1.0)

    if i >= 20 and _finite(bars.rsi[i]) and len(lows) >= 2 and len(highs) >= 2:
        (l1, p1), (l2, p2) = lows[-2], lows[-1]
        if p2 < p1 and _finite(bars.rsi[l1], bars.rsi[l2]) and bars.rsi[l2] > bars.rsi[l1] + 1.0:
            if i <= l2 + PIVOT_RIGHT + 1:
                emit("rsi_bull_div", 1.0)
        (h1, q1), (h2, q2) = highs[-2], highs[-1]
        if q2 > q1 and _finite(bars.rsi[h1], bars.rsi[h2]) and bars.rsi[h2] < bars.rsi[h1] - 1.0:
            if i <= h2 + PIVOT_RIGHT + 1:
                emit("rsi_bear_div", -1.0)

    # Deduplicate while keeping first (family-priority) hit.
    seen: set[str] = set()
    unique: list[tuple[str, float]] = []
    for name, pct in out:
        if name in seen:
            continue
        seen.add(name)
        unique.append((name, pct))
    return unique


def scan_ohlc(
    dates: list[datetime],
    open_: Iterable[float],
    high: Iterable[float],
    low: Iterable[float],
    close: Iterable[float],
    *,
    warmup: int = 30,
) -> list[dict]:
    """Walk the chart and emit one enriched row per detected figure."""
    bars = build_bars(open_, high, low, close)
    n = len(bars.c)
    start = min(max(warmup, 5), max(0, n - 1))
    rows: list[dict] = []
    for i in range(start, n):
        figures = figures_on_chart(bars, i)
        if not figures:
            continue
        dt = dates[i]
        close_i = float(bars.c[i])
        for name, pct in figures:
            rows.append(
                {
                    "date_dt": dt,
                    "value": close_i,
                    "pct_change": float(pct),
                    "event_type": name,
                }
            )
    return rows
