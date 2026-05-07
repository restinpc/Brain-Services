"""
model.py v2 — Service 43: Output Strategy as Feature Provider.

Главное отличие от v1: возвращает 8 named features вместо одного
{"output": magnitude}. Это позволяет PHP-нейронету обучать веса
независимо для каждой фичи и комбинировать их через свой
init_thresholds + reweight_modificator.

Совместимость:
  RETURN_MODE = "features"   — новое поведение (рекомендуется)
  RETURN_MODE = "scalar"     — старое поведение (для обратной совместимости)

Все индикаторы каузальны: trajectory[i] зависит только от rates[0..i].
Это позволяет batch_model() считать всю историю за O(N) вместо O(N²).
"""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

# Опциональные зависимости (graceful fallback при отсутствии)
try:
    from scipy.signal import lfilter as _sp_lfilter
    _HAS_SCIPY = True
except Exception:
    _sp_lfilter = None
    _HAS_SCIPY = False

try:
    from sklearn.mixture import GaussianMixture
    _HAS_SKLEARN = True
except Exception:
    GaussianMixture = None
    _HAS_SKLEARN = False

try:
    from hmmlearn.hmm import GaussianHMM
    _HAS_HMM = True
except Exception:
    GaussianHMM = None
    _HAS_HMM = False


# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ФРЕЙМВОРКА
# ══════════════════════════════════════════════════════════════════════════════

PRETEST_ALLOW_EMPTY = True
RATES_TABLE = os.getenv("RATES_TABLE", "brain_rates_eur_usd")

# v2: Этот сервис теперь self-contained, не зависит от внешнего dataset.
# Если нужен dataset для совместимости — оставлен пустой query.
DATASET_QUERY = "SELECT NULL AS date LIMIT 0"
DATASET_ENGINE = "vlad"
DATASET_TABLE = ""
DATASET_KEY = ""
FILTER_DATASET_BY_DATE = False

# Не используем context idx / weights table — у feature provider их нет
CTX_TABLE = None
WEIGHTS_TABLE = None

# Single var: все 8 features считаются за один проход; разные var-ы здесь
# не нужны (v1 переключал между стратегиями, v2 выдаёт все одновременно)
VAR_RANGE = [0]

# type=0 — оба mode (фреймворк всё равно сохранит свой ключ-кэш)
TYPES_RANGE = [0]

CACHE_DATE_FROM = os.getenv("CACHE_DATE_FROM", "2025-01-15")
USE_ML_VALUES = False

# ── Новые параметры v2 ──
RETURN_MODE = os.getenv("RETURN_MODE", "features")  # "features" | "scalar"

# Для совместимости с фреймворком (patch B) — рекомендуем выставить
# SIGNAL_AGGREGATION="weighted_mean" в model.py при RETURN_MODE="features"
SIGNAL_AGGREGATION = os.getenv("SIGNAL_AGGREGATION", "weighted_mean")
SIGNAL_MIN_THRESHOLD = float(os.getenv("SIGNAL_MIN_THRESHOLD", "0.1"))


# ══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ ПОЗИЦИИ (для scalar mode)
# ══════════════════════════════════════════════════════════════════════════════

FLAT, LONG, SHORT = 0, +1, -1


# ══════════════════════════════════════════════════════════════════════════════
# NUMPY-ИНДИКАТОРЫ (векторизованные, O(n))
# ══════════════════════════════════════════════════════════════════════════════

def _rolling_max_v(arr: np.ndarray, n: int) -> np.ndarray:
    if arr.size == 0 or n <= 1:
        return arr.astype(np.float64).copy()
    n = min(n, arr.size)
    out = np.empty(arr.size, dtype=np.float64)
    out[: n - 1] = np.maximum.accumulate(arr[: n - 1].astype(np.float64))
    windows = sliding_window_view(arr.astype(np.float64), n)
    out[n - 1 :] = windows.max(axis=1)
    return out


def _rolling_min_v(arr: np.ndarray, n: int) -> np.ndarray:
    if arr.size == 0 or n <= 1:
        return arr.astype(np.float64).copy()
    n = min(n, arr.size)
    out = np.empty(arr.size, dtype=np.float64)
    out[: n - 1] = np.minimum.accumulate(arr[: n - 1].astype(np.float64))
    windows = sliding_window_view(arr.astype(np.float64), n)
    out[n - 1 :] = windows.min(axis=1)
    return out


def _sma_v(arr: np.ndarray, n: int) -> np.ndarray:
    if arr.size == 0:
        return arr.astype(np.float64).copy()
    n = min(n, arr.size)
    csum = np.cumsum(arr.astype(np.float64))
    out = np.empty(arr.size, dtype=np.float64)
    idx = np.arange(arr.size) + 1
    out[: n] = csum[: n] / idx[: n]
    if arr.size > n:
        out[n:] = (csum[n:] - csum[: -n]) / n
    return out


def _wilder_avg(values: np.ndarray, n: int) -> np.ndarray:
    sz = values.size
    out = np.empty(sz, dtype=np.float64)
    if sz == 0:
        return out
    if _HAS_SCIPY and sz > 1:
        v = values.astype(np.float64)
        alpha = 1.0 / n
        b_filt = np.array([alpha])
        a_filt = np.array([1.0, -(1.0 - alpha)])
        zi = np.array([v[0]])
        out, _ = _sp_lfilter(b_filt, a_filt, v, zi=zi)
        return out
    alpha_keep = (n - 1) / n
    alpha_new = 1.0 / n
    acc = 0.0
    for i in range(sz):
        if i < n:
            acc = (acc * i + values[i]) / (i + 1)
        else:
            acc = acc * alpha_keep + values[i] * alpha_new
        out[i] = acc
    return out


def _rsi(close: np.ndarray, n: int = 14) -> np.ndarray:
    if close.size < 2:
        return np.full(close.size, 50.0, dtype=np.float64)
    diff = np.diff(close, prepend=close[0])
    up = np.where(diff > 0, diff, 0.0)
    dn = np.where(diff < 0, -diff, 0.0)
    avg_up = _wilder_avg(up, n)
    avg_dn = _wilder_avg(dn, n)
    out = np.full(close.size, 50.0, dtype=np.float64)
    nonzero = avg_dn > 0
    rs = np.where(nonzero, avg_up / np.where(nonzero, avg_dn, 1.0), 0.0)
    out = np.where(nonzero, 100.0 - 100.0 / (1.0 + rs), out)
    out = np.where((avg_dn == 0) & (avg_up > 0), 100.0, out)
    return out


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
         n: int = 14) -> np.ndarray:
    sz = close.size
    if sz == 0:
        return np.zeros(0, dtype=np.float64)
    tr = np.empty(sz, dtype=np.float64)
    tr[0] = high[0] - low[0]
    if sz > 1:
        prev_close = close[:-1]
        tr[1:] = np.maximum.reduce([
            high[1:] - low[1:],
            np.abs(high[1:] - prev_close),
            np.abs(low[1:] - prev_close),
        ])
    return _wilder_avg(tr, n)


def _rolling_std_v(arr: np.ndarray, n: int) -> np.ndarray:
    if arr.size == 0 or n <= 1:
        return np.zeros(arr.size, dtype=np.float64)
    n = min(n, arr.size)
    a = arr.astype(np.float64)
    out = np.zeros(arr.size, dtype=np.float64)
    mean = a[0]
    m2 = 0.0
    for i in range(1, n):
        x = a[i]
        delta = x - mean
        mean += delta / (i + 1)
        delta2 = x - mean
        m2 += delta * delta2
        out[i] = math.sqrt(m2 / (i + 1)) if i >= 1 else 0.0
    if arr.size >= n:
        windows = sliding_window_view(a, n)
        out[n - 1 :] = windows.std(axis=1)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# RATES → arrays
# ══════════════════════════════════════════════════════════════════════════════

def _rates_to_arrays(rates: Sequence[dict]) -> dict[str, np.ndarray]:
    n = len(rates)
    if n == 0:
        empty = np.zeros(0, dtype=np.float64)
        return {"open": empty, "close": empty, "low": empty, "high": empty}
    op = np.fromiter((float(r.get("open") or 0.0) for r in rates),
                     dtype=np.float64, count=n)
    cl = np.fromiter((float(r.get("close") or 0.0) for r in rates),
                     dtype=np.float64, count=n)
    lo = np.fromiter((float(r.get("min") or 0.0) for r in rates),
                     dtype=np.float64, count=n)
    hi = np.fromiter((float(r.get("max") or 0.0) for r in rates),
                     dtype=np.float64, count=n)
    return {"open": op, "close": cl, "low": lo, "high": hi}


# ══════════════════════════════════════════════════════════════════════════════
# CORE: вычисление всех 8 features за один проход
# ══════════════════════════════════════════════════════════════════════════════

def _compute_all_features(arr: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """
    Вычисляет 8 named features. Каждая фича — массив той же длины, что rates.
    Все каузальны: f[i] зависит только от arr[*][:i+1].
    """
    cl = arr["close"]
    hi = arr["high"]
    lo = arr["low"]
    sz = cl.size
    if sz == 0:
        return {k: np.zeros(0) for k in _FEATURE_NAMES}

    # ── ATR + std для нормализации ──
    atr = _atr(hi, lo, cl, n=14)
    atr_safe = np.where(atr > 0, atr, 1e-9)
    atr_mean = _sma_v(atr, n=100)
    atr_std = _rolling_std_v(atr, n=100)
    atr_std_safe = np.where(atr_std > 0, atr_std, 1e-9)

    # ── 1. Donchian: текущее положение от 20-bar high/low ──
    n_in = 20
    n_out = 10
    rmax_in = _rolling_max_v(hi, n_in)
    rmin_in = _rolling_min_v(lo, n_in)
    rmax_in_prev = np.concatenate(([np.inf], rmax_in[:-1]))
    rmin_in_prev = np.concatenate(([-np.inf], rmin_in[:-1]))

    # donchian_pos: расстояние close от середины канала / ATR.
    # На начальных барах rmax_in_prev=+inf, rmin_in_prev=-inf — игнорим.
    valid_chan = np.isfinite(rmax_in_prev) & np.isfinite(rmin_in_prev)
    donchian_pos = np.zeros(sz, dtype=np.float64)
    if valid_chan.any():
        mid_valid = (rmax_in_prev[valid_chan] + rmin_in_prev[valid_chan]) / 2.0
        donchian_pos[valid_chan] = (cl[valid_chan] - mid_valid) / atr_safe[valid_chan]
    donchian_pos = np.clip(donchian_pos, -3.0, 3.0)

    # ── 2. Donchian state machine ──
    # +1 если был breakout вверх и не было выхода через 10-bar low
    # -1 если был breakout вниз и не было выхода через 10-bar high
    # 0 в остальных случаях
    rmax_out = _rolling_max_v(hi, n_out)
    rmin_out = _rolling_min_v(lo, n_out)
    rmax_out_prev = np.concatenate(([np.inf], rmax_out[:-1]))
    rmin_out_prev = np.concatenate(([-np.inf], rmin_out[:-1]))

    donchian_state = np.zeros(sz, dtype=np.float64)
    state = 0
    for i in range(sz):
        if i < max(n_in, n_out):
            continue
        price = cl[i]
        if state == LONG and price < rmin_out_prev[i]:
            state = FLAT
        elif state == SHORT and price > rmax_out_prev[i]:
            state = FLAT
        if state == FLAT:
            if price > rmax_in_prev[i]:
                state = LONG
            elif price < rmin_in_prev[i]:
                state = SHORT
        donchian_state[i] = float(state)

    # ── 3. MA diff: (sma20 - sma50) / atr ──
    sma20 = _sma_v(cl, 20)
    sma50 = _sma_v(cl, 50)
    ma_diff = (sma20 - sma50) / atr_safe
    ma_diff = np.clip(ma_diff, -3.0, 3.0)

    # ── 4. RSI z-score: (rsi - 50) / 25 ──
    rsi = _rsi(cl, n=14)
    rsi_z = (rsi - 50.0) / 25.0
    rsi_z = np.clip(rsi_z, -2.0, 2.0)

    # ── 5. ATR z-score: vol regime indicator ──
    atr_z = (atr - atr_mean) / atr_std_safe
    atr_z = np.clip(atr_z, -2.0, 3.0)

    # ── 6. Momentum: 10-bar return / ATR ──
    mom_n = 10
    mom = np.zeros(sz, dtype=np.float64)
    if sz > mom_n:
        mom[mom_n:] = (cl[mom_n:] - cl[:-mom_n]) / atr_safe[mom_n:]
    momentum_n = np.clip(mom, -3.0, 3.0)

    # ── 7. Regime label (HMM/GMM/fallback) ──
    regime_label = _compute_regime_label(cl, sz)

    # ── 8. Breakout strength: расстояние от последнего экстремума ──
    breakout_strength = np.zeros(sz, dtype=np.float64)
    valid_b = np.isfinite(rmax_in_prev) & np.isfinite(rmin_in_prev)
    # Для long-breakout: (price - rmax_in_prev) / atr if positive
    # Для short-breakout: -(rmin_in_prev - price) / atr if positive
    above = np.where(valid_b, (cl - rmax_in_prev) / atr_safe, 0.0)
    below = np.where(valid_b, (rmin_in_prev - cl) / atr_safe, 0.0)
    above = np.where(above > 0, above, 0.0)
    below = np.where(below > 0, below, 0.0)
    # Знак — по тому, какое направление активно
    sign = np.where(above > 0, 1.0, np.where(below > 0, -1.0, 0.0))
    strength = np.maximum(above, below)
    breakout_strength = np.clip(sign * strength, -3.0, 3.0)

    return {
        "donchian_pos":      donchian_pos,
        "donchian_state":    donchian_state,
        "ma_diff":           ma_diff,
        "rsi_z":             rsi_z,
        "atr_z":             atr_z,
        "momentum_n":        momentum_n,
        "regime_label":      regime_label,
        "breakout_strength": breakout_strength,
    }


_FEATURE_NAMES = [
    "donchian_pos", "donchian_state", "ma_diff", "rsi_z",
    "atr_z", "momentum_n", "regime_label", "breakout_strength",
]


# ── HMM / GMM regime classifier ─────────────────────────────────────────────

_HMM_CACHE: dict[tuple, Any] = {}


def _compute_regime_label(close: np.ndarray, sz: int) -> np.ndarray:
    """
    Кластеризует историю log-returns на 3 режима. Возвращает label
    каждого бара в {-1, 0, +1} (медвежий / нейтральный / бычий).
    """
    out = np.zeros(sz, dtype=np.float64)
    if sz < 100 or not (_HAS_HMM or _HAS_SKLEARN):
        return out
    log_ret = np.diff(np.log(np.maximum(close, 1e-9)), prepend=0.0)
    win = 50  # окно для скользящих фич
    if sz <= win + 50:
        return out
    # Фичи: текущий return + rolling vol
    vol = _rolling_std_v(log_ret, n=20)
    X = np.column_stack([log_ret, vol])
    train = X[win:]
    if train.shape[0] < 50:
        return out

    n_states = 3
    cache_key = ("regime", sz, train.shape[0], n_states)
    model = _HMM_CACHE.get(cache_key)
    labels = None
    if model is None:
        if _HAS_HMM:
            try:
                hmm = GaussianHMM(n_components=n_states, covariance_type="diag",
                                  n_iter=50, random_state=42)
                hmm.fit(train)
                model = hmm
                _HMM_CACHE[cache_key] = hmm
            except Exception:
                pass
        if model is None and _HAS_SKLEARN:
            try:
                gm = GaussianMixture(n_components=n_states, covariance_type="diag",
                                     max_iter=100, random_state=42)
                gm.fit(train)
                model = gm
                _HMM_CACHE[cache_key] = gm
            except Exception:
                return out
    if model is None:
        return out
    try:
        labels = model.predict(X)
    except Exception:
        return out

    # Сопоставляем кластерам направление по среднему return
    cluster_dir = {}
    for k in range(n_states):
        mask = labels == k
        if mask.any():
            mr = float(log_ret[mask].mean())
            cluster_dir[k] = 1.0 if mr > 0 else (-1.0 if mr < 0 else 0.0)
        else:
            cluster_dir[k] = 0.0

    for i in range(sz):
        out[i] = cluster_dir.get(int(labels[i]), 0.0)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# CACHE
# ══════════════════════════════════════════════════════════════════════════════

_FEATURE_CACHE: dict[tuple, dict[str, np.ndarray]] = {}
_CACHE_MAX = 1024


def _hash_param(param: str) -> str:
    return hashlib.md5(param.encode("utf-8")).hexdigest()[:8] if param else "00"


def _compute_features_cached(
    marker: tuple, rates: Sequence[dict]
) -> dict[str, np.ndarray]:
    cached = _FEATURE_CACHE.get(marker)
    if cached is not None:
        return cached
    arr = _rates_to_arrays(rates)
    features = _compute_all_features(arr)
    if len(_FEATURE_CACHE) >= _CACHE_MAX:
        _FEATURE_CACHE.pop(next(iter(_FEATURE_CACHE)))
    _FEATURE_CACHE[marker] = features
    return features


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:
    """
    Возвращает либо набор named features ("features" mode),
    либо одно скалярное значение output ("scalar" mode).
    """
    if not rates:
        return {}

    last_dt = rates[-1].get("date")
    last_ts = int(last_dt.timestamp()) if isinstance(last_dt, datetime) else 0
    first_close = float(rates[0].get("close") or 0.0)
    last_close = float(rates[-1].get("close") or 0.0)
    fp = (round(first_close, 6), round(last_close, 6))
    marker = (len(rates), last_ts, _hash_param(param), fp)

    features = _compute_features_cached(marker, rates)
    if not features or features["donchian_pos"].size == 0:
        return {}

    # Берём значения последнего бара
    idx = features["donchian_pos"].size - 1
    last_values = {name: float(features[name][idx]) for name in _FEATURE_NAMES}

    if RETURN_MODE == "features":
        # Возвращаем все 8 features (кроме нулевых — экономия места в кеше)
        return {k: round(v, 4) for k, v in last_values.items() if abs(v) > 1e-6}

    # scalar mode (legacy): эмулируем старое поведение, выдавая один output
    # как взвешенную сумму с равными весами
    weighted_sum = sum(last_values.values()) / len(last_values)
    if abs(weighted_sum) < 0.1:
        return {}
    return {"output": round(weighted_sum, 4)}


def batch_model(
    rates: list[dict],
    dataset: list[dict],
    dates: list[datetime],
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[datetime, dict[str, float]]:
    """
    Пакетная версия: считает все 8 features за один проход по всему ряду,
    затем для каждой даты из dates берёт значение по индексу.
    O(N) вместо O(N²) при последовательных вызовах model().
    """
    if not rates or not dates:
        return {d: {} for d in dates}

    last_dt = rates[-1].get("date")
    last_ts = int(last_dt.timestamp()) if isinstance(last_dt, datetime) else 0
    first_close = float(rates[0].get("close") or 0.0)
    last_close = float(rates[-1].get("close") or 0.0)
    fp = (round(first_close, 6), round(last_close, 6))
    marker = (len(rates), last_ts, _hash_param(param), fp)

    features = _compute_features_cached(marker, rates)
    if not features or features["donchian_pos"].size == 0:
        return {d: {} for d in dates}

    rate_dates = [r["date"] for r in rates]
    results: dict[datetime, dict[str, float]] = {}
    for target in dates:
        idx = bisect.bisect_right(rate_dates, target) - 1
        if idx < 0:
            results[target] = {}
            continue
        last_values = {name: float(features[name][idx]) for name in _FEATURE_NAMES}
        if RETURN_MODE == "features":
            results[target] = {k: round(v, 4) for k, v in last_values.items()
                              if abs(v) > 1e-6}
        else:
            ws = sum(last_values.values()) / len(last_values)
            results[target] = {"output": round(ws, 4)} if abs(ws) >= 0.1 else {}

    return results
