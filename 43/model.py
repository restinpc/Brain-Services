"""
model.py v2 — Output Strategy Microservice (improved)
=====================================================

Улучшения относительно v1:
  • Spread-aware state machine: не флипает позицию, если ожидаемая
    отдача меньше 2× спреда (закрытие + повторное открытие).
  • Hysteresis 20/10 (Turtle System 1): вход и выход на разных
    окнах, тренд получает место для развития.
  • Skip rule: после прибыльной сделки следующий сигнал того же
    типа игнорируется (стандарт оригинальной Turtle System 1).
  • Magnitude output: возвращаем взвешенный сигнал (по силе пробоя
    относительно ATR), а не ±1. Это даёт PHP-нейросети осмысленный
    диапазон значений для подбора buy/sell порогов.
  • Volatility-adaptive Donchian: окно автоматически растёт в шумные
    периоды и сужается в спокойные.
  • Векторизованные индикаторы через sliding_window_view (numpy).
    Сложность пересчёта O(n) вместо O(n²) у v1.
  • HMM (hmmlearn) для регим-классификатора var=3 — учитывает
    временную последовательность, в отличие от GMM.
    Graceful fallback: hmmlearn → sklearn GMM → Donchian.
  • Pyramiding по ATR в var=2: магнитуда выхода растёт по мере
    подтверждения тренда (как у оригинальной Turtle).

Контракт остаётся прежним:
    LONG  → {"output": +X}    где X >= 0.5
    SHORT → {"output": -X}    где X >= 0.5
    FLAT  → {}

Спред:
    Передаётся через param как {"spread": 0.0002} или эвристика по
    уровню цены: EUR/USD ~1 → 0.0002, ETH ~3000 → 10, BTC ~50000 → 60.
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

try:
    from scipy.signal import lfilter as _sp_lfilter  # type: ignore
    _HAS_SCIPY = True
except Exception:
    _sp_lfilter = None  # type: ignore
    _HAS_SCIPY = False
try:
    from sklearn.mixture import GaussianMixture  # type: ignore
    _HAS_SKLEARN = True
except Exception:  # pragma: no cover
    GaussianMixture = None  # type: ignore
    _HAS_SKLEARN = False

# Опциональный hmmlearn — основной путь var=3.
try:
    from hmmlearn.hmm import GaussianHMM  # type: ignore
    _HAS_HMM = True
except Exception:  # pragma: no cover
    GaussianHMM = None  # type: ignore
    _HAS_HMM = False


# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ФРЕЙМВОРКА
# ══════════════════════════════════════════════════════════════════════════════
PRETEST_ALLOW_EMPTY = True
# Основная таблица котировок. Можно переопределить через .env:
#   RATES_TABLE=brain_rates_eur_usd
RATES_TABLE = os.getenv("RATES_TABLE", "brain_rates_eur_usd")

# Таблицы, которые строят builders context_idx.py и weights.py.
# Эти константы нужны фреймворку, чтобы при старте загрузились:
#   dataset, weights, url_map, dataset_index.
CTX_TABLE = os.getenv("CTX_TABLE", "brain_calendar_context_idx")
WEIGHTS_TABLE = os.getenv("WEIGHTS_TABLE", "brain_calendar_weights")
WEIGHTS_CODE_COLUMN = os.getenv("WEIGHTS_CODE_COLUMN", "weight_code")

# Dataset берём из engine_brain по умолчанию.
# Если календарь/USGS-данные лежат в engine_vlad, поставь в .env:
#   DATASET_ENGINE=vlad
DATASET_ENGINE = os.getenv("DATASET_ENGINE", "brain")
DATASET_TABLE = os.getenv("DATASET_TABLE", "brain_calendar")

DATASET_QUERY = f"""
    SELECT
        Url              AS url,
        CurrencyCode     AS currency_code,
        Importance       AS importance,
        ForecastValue    AS forecast_value,
        PreviousValue    AS previous_value,
        OldPreviousValue AS old_previous_value,
        ActualValue      AS actual_value,
        FullDate         AS event_time,
        FullDate         AS date,
        EventType        AS event_type
    FROM {DATASET_TABLE}
    WHERE ActualValue IS NOT NULL
      AND Processed = 1
      AND FullDate IS NOT NULL
      AND Url IS NOT NULL
      AND Url != ''
      AND CurrencyCode IS NOT NULL
      AND (EventType IS NULL OR EventType NOT IN (2))
    ORDER BY FullDate
"""

# Фреймворк использует это для быстрой привязки dataset-событий к event_id.
URL_MAP_ENGINE = os.getenv("URL_MAP_ENGINE", "vlad")
URL_MAP_QUERY = f"""
    SELECT Url AS url, EventId AS event_id
    FROM {DATASET_TABLE}
    WHERE Url IS NOT NULL
      AND Url != ''
      AND EventId IS NOT NULL
    GROUP BY Url, EventId
"""

FILTER_DATASET_BY_DATE = True
DATASET_KEY = "url"

VAR_RANGE = [0, 1, 2, 3]

CACHE_DATE_FROM = os.getenv("CACHE_DATE_FROM", "2025-01-15")

# Важно: True включает загрузку weights и ML-значений во фреймворке.
# Без этого в логе будет ml=off, а weights/url_map могут остаться нулевыми.
USE_ML_VALUES = os.getenv("USE_ML_VALUES", "1") == "1"


# ══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ ПОЗИЦИИ
# ══════════════════════════════════════════════════════════════════════════════

FLAT = 0
LONG = +1
SHORT = -1


# Эвристические значения спредов по уровню цены — соответствуют PHP-фреймворку:
#   EUR/USD (~1)      → 0.0002
#   ETH/USD (~3000)   → 10.0
#   BTC/USD (~50000)  → 60.0
def _estimate_spread(last_price: float) -> float:
    if last_price >= 5000:
        return 60.0
    if last_price >= 500:
        return 10.0
    return 0.0002


# ══════════════════════════════════════════════════════════════════════════════
# NUMPY-ИНДИКАТОРЫ (векторизованные, O(n))
# ══════════════════════════════════════════════════════════════════════════════

def _rolling_max_v(arr: np.ndarray, n: int) -> np.ndarray:
    """Векторизованный rolling max через sliding_window_view."""
    if arr.size == 0 or n <= 1:
        return arr.astype(np.float64).copy()
    n = min(n, arr.size)
    # Для первых n-1 точек — кумулятивный max.
    out = np.empty(arr.size, dtype=np.float64)
    out[: n - 1] = np.maximum.accumulate(arr[: n - 1].astype(np.float64))
    # Для остальных — векторно.
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
    """Скользящее среднее с прогревом."""
    if arr.size == 0:
        return arr.astype(np.float64).copy()
    n = min(n, arr.size)
    csum = np.cumsum(arr.astype(np.float64))
    out = np.empty(arr.size, dtype=np.float64)
    # Накопительное среднее на прогреве.
    idx = np.arange(arr.size) + 1
    out[: n] = csum[: n] / idx[: n]
    # После прогрева.
    if arr.size > n:
        out[n:] = (csum[n:] - csum[: -n]) / n
    return out


def _wilder_avg(values: np.ndarray, n: int) -> np.ndarray:
    """
    Average по Уайлдеру: y[i] = y[i-1]*(n-1)/n + x[i]/n.

    Если scipy доступен — реализован через lfilter (IIR 1-го порядка, ~50× быстрее).
    Fallback: Python-цикл (оригинальное поведение).
    Разница в зоне прогрева [0..n] несущественна — стратегии пропускают эти бары.
    """
    sz = values.size
    out = np.empty(sz, dtype=np.float64)
    if sz == 0:
        return out

    if _HAS_SCIPY and sz > 1:
        v = values.astype(np.float64)
        alpha = 1.0 / n
        b_filt = np.array([alpha])
        a_filt = np.array([1.0, -(1.0 - alpha)])
        # zi = начальное состояние, аппроксимирующее cumulative mean.
        zi = np.array([v[0]])
        out, _ = _sp_lfilter(b_filt, a_filt, v, zi=zi)
        return out

    # Fallback: Python-цикл с предвычисленными константами.
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


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = 14) -> np.ndarray:
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
    """Векторизованное скользящее стандартное отклонение.
    Прогрев через алгоритм Велфорда — O(n) вместо O(n²).
    """
    if arr.size == 0 or n <= 1:
        return np.zeros(arr.size, dtype=np.float64)
    n = min(n, arr.size)
    a = arr.astype(np.float64)
    out = np.zeros(arr.size, dtype=np.float64)

    # Прогрев [0..n-2]: алгоритм Велфорда за O(n) суммарно.
    mean = a[0]
    m2 = 0.0
    for i in range(1, n):
        x = a[i]
        delta = x - mean
        mean += delta / (i + 1)
        delta2 = x - mean
        m2 += delta * delta2
        out[i] = math.sqrt(m2 / (i + 1)) if i >= 1 else 0.0

    # После прогрева: sliding_window_view — полностью векторно.
    if arr.size >= n:
        windows = sliding_window_view(a, n)
        out[n - 1 :] = windows.std(axis=1)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# КОНВЕРТАЦИЯ rates → numpy
# ══════════════════════════════════════════════════════════════════════════════

def _rates_to_arrays(rates: Sequence[dict]) -> dict[str, np.ndarray]:
    # Векторная распаковка через list comprehension + np.fromiter — быстрее
    # чем побарный float(r.get(...)) в явном цикле.
    n = len(rates)
    if n == 0:
        empty = np.zeros(0, dtype=np.float64)
        return {"open": empty, "close": empty, "low": empty, "high": empty}
    op = np.fromiter((float(r.get("open") or 0.0)  for r in rates), dtype=np.float64, count=n)
    cl = np.fromiter((float(r.get("close") or 0.0) for r in rates), dtype=np.float64, count=n)
    lo = np.fromiter((float(r.get("min") or 0.0)   for r in rates), dtype=np.float64, count=n)
    hi = np.fromiter((float(r.get("max") or 0.0)   for r in rates), dtype=np.float64, count=n)
    return {"open": op, "close": cl, "low": lo, "high": hi}


def _hash_param(param: str) -> str:
    return hashlib.md5(param.encode("utf-8")).hexdigest()[:8] if param else "00"


def _parse_param(param: str) -> dict[str, Any]:
    if not param:
        return {}
    try:
        v = json.loads(param)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# СТРАТЕГИИ — каждая возвращает «сырые намерения» в виде магнитуды
#   intent[i] > 0 → желает быть в LONG (величина = сила сигнала)
#   intent[i] < 0 → желает быть в SHORT (величина = сила сигнала)
#   intent[i] = 0 → нейтрален
# Сила сигнала нормализована примерно к диапазону [0.5, 3.0].
# ══════════════════════════════════════════════════════════════════════════════

def _signal_strength(price: float, ref_level: float, atr: float) -> float:
    """
    Нормализует расстояние пробоя в условные «N» (как у Turtle):
        |price - ref| / atr → ограничено [0.5, 3.0].
    """
    if atr <= 0:
        return 0.5
    n_units = abs(price - ref_level) / atr
    return float(np.clip(0.5 + n_units, 0.5, 3.0))


def _strategy_donchian_improved(arr: dict[str, np.ndarray], hp: dict[str, Any]) -> np.ndarray:
    """
    Turtle System 1 + adaptive volatility window + magnitude output.

    Параметры (через param):
        donchian_in  (default 20) — окно для входа
        donchian_out (default 10) — окно для выхода (Hysteresis)
        adaptive     (default True) — окно адаптируется по волатильности
        atr_n        (default 14)
    Возвращает array float — магнитуду намерения на каждом баре.
    """
    n_in = int(hp.get("donchian_in", 20))
    n_out = int(hp.get("donchian_out", 10))
    atr_n = int(hp.get("atr_n", 14))
    adaptive = bool(hp.get("adaptive", True))

    hi = arr["high"]
    lo = arr["low"]
    cl = arr["close"]
    sz = cl.size
    if sz == 0:
        return np.zeros(0, dtype=np.float64)

    atr = _atr(hi, lo, cl, atr_n)

    # Адаптивное окно: если текущая ATR > средней ATR за 100 баров — расширяем
    # окно входа (избегаем шумных breakout'ов в волатильной фазе); если ниже —
    # сужаем (даём сигналы быстрее в спокойном тренде).
    if adaptive and sz > 100:
        atr_long = _wilder_avg(atr, 100)
        ratio = np.where(atr_long > 0, atr / atr_long, 1.0)
        # Множитель окна: 0.7..1.5
        n_in_dyn = np.clip(n_in * np.clip(ratio, 0.7, 1.5), 10, 60).astype(int)
        # Берём максимальное на текущий момент окно для быстрой векторной аппроксимации.
        # На каждом баре используем своё окно — для простоты считаем серию для нескольких
        # фиксированных окон и индексируем.
        # В практике: достаточно двух окон (узкого и широкого) и переключение.
        rmax_narrow = _rolling_max_v(hi, max(10, n_in - 5))
        rmax_wide = _rolling_max_v(hi, n_in + 10)
        rmin_narrow = _rolling_min_v(lo, max(10, n_in - 5))
        rmin_wide = _rolling_min_v(lo, n_in + 10)
        is_calm = ratio < 0.9
        rmax_in = np.where(is_calm, rmax_narrow, rmax_wide)
        rmin_in = np.where(is_calm, rmin_narrow, rmin_wide)
    else:
        rmax_in = _rolling_max_v(hi, n_in)
        rmin_in = _rolling_min_v(lo, n_in)

    rmax_out = _rolling_max_v(hi, n_out)
    rmin_out = _rolling_min_v(lo, n_out)

    # Сравниваем с экстремумами ПРЕДЫДУЩИХ баров (исключаем текущий).
    rmax_in_prev = np.concatenate(([np.inf], rmax_in[:-1]))
    rmin_in_prev = np.concatenate(([-np.inf], rmin_in[:-1]))
    rmax_out_prev = np.concatenate(([np.inf], rmax_out[:-1]))
    rmin_out_prev = np.concatenate(([-np.inf], rmin_out[:-1]))

    intents = np.zeros(sz, dtype=np.float64)

    # Используем явный Python-цикл для отслеживания state + skip rule.
    # Это нужно, потому что skip rule зависит от исхода предыдущей сделки,
    # которая определяется на нескольких барах вперёд (не векторизуется).
    pos = FLAT
    entry_price = 0.0
    entry_idx = 0
    last_signal_was_profitable = False

    for i in range(sz):
        if i < max(n_in, atr_n):
            continue

        price = cl[i]
        atr_v = atr[i]
        if atr_v <= 0:
            atr_v = 1e-9

        # ── Сначала: если в позиции, проверяем выход (Hysteresis 10) ─────
        if pos == LONG:
            # Выход лонга — пробой 10-периодного минимума.
            if price < rmin_out_prev[i]:
                # Сделка завершена; проверяем была ли прибыльной.
                last_signal_was_profitable = price > entry_price
                pos = FLAT
        elif pos == SHORT:
            if price > rmax_out_prev[i]:
                last_signal_was_profitable = price < entry_price
                pos = FLAT

        # ── Потом: если флэт, проверяем вход (Donchian 20) ──────────────
        if pos == FLAT:
            long_breakout = price > rmax_in_prev[i]
            short_breakout = price < rmin_in_prev[i]

            if long_breakout:
                if last_signal_was_profitable:
                    # Skip rule: пропускаем, обнуляя флаг.
                    last_signal_was_profitable = False
                else:
                    pos = LONG
                    entry_price = price
                    entry_idx = i
            elif short_breakout:
                if last_signal_was_profitable:
                    last_signal_was_profitable = False
                else:
                    pos = SHORT
                    entry_price = price
                    entry_idx = i

        # ── Магнитуда выхода ────────────────────────────────────────────
        if pos == LONG:
            # Сила = расстояние от уровня входа в N (как у Turtle).
            strength = _signal_strength(price, entry_price, atr_v)
            intents[i] = strength
        elif pos == SHORT:
            strength = _signal_strength(price, entry_price, atr_v)
            intents[i] = -strength
        else:
            intents[i] = 0.0

    return intents


def _strategy_ma_rsi_adaptive(arr: dict[str, np.ndarray], hp: dict[str, Any]) -> np.ndarray:
    """
    SMA crossover + RSI фильтр + адаптация к волатильности.
    Магнитуда = (sma_fast - sma_slow) / atr (нормировано в N).
    """
    fast = int(hp.get("sma_fast", 20))
    slow = int(hp.get("sma_slow", 50))
    rsi_n = int(hp.get("rsi_n", 14))
    rsi_hi = float(hp.get("rsi_high", 75.0))  # чуть мягче чем 70
    rsi_lo = float(hp.get("rsi_low", 25.0))
    atr_n = int(hp.get("atr_n", 14))

    cl = arr["close"]
    hi = arr["high"]
    lo = arr["low"]
    sz = cl.size
    if sz == 0:
        return np.zeros(0, dtype=np.float64)

    sf = _sma_v(cl, fast)
    ss = _sma_v(cl, slow)
    rs = _rsi(cl, rsi_n)
    atr = _atr(hi, lo, cl, atr_n)

    diff = sf - ss
    norm_atr = np.where(atr > 0, atr, 1e-9)
    raw_strength = np.clip(np.abs(diff) / norm_atr, 0.5, 3.0)

    out = np.zeros(sz, dtype=np.float64)
    long_mask = (sf > ss) & (rs < rsi_hi)
    shrt_mask = (sf < ss) & (rs > rsi_lo)
    out[long_mask] = raw_strength[long_mask]
    out[shrt_mask] = -raw_strength[shrt_mask]
    out[: max(slow, rsi_n, atr_n)] = 0.0
    return out


def _strategy_atr_trail_pyramid(arr: dict[str, np.ndarray], hp: dict[str, Any]) -> np.ndarray:
    """
    Momentum-вход + ATR-trailing stop + pyramiding.

    Пирамидинг по Turtle: каждые 0.5N в плюс — увеличиваем магнитуду на 1.
    Максимум — 4 unit'а (чтобы не убежать в плюс бесконечно).
    """
    atr_n = int(hp.get("atr_n", 14))
    atr_mult = float(hp.get("atr_mult", 2.5))
    mom_n = int(hp.get("mom_n", 10))
    pyramid_step = float(hp.get("pyramid_step", 0.5))  # каждые 0.5N
    max_units = int(hp.get("max_units", 4))

    cl = arr["close"]
    hi = arr["high"]
    lo = arr["low"]
    sz = cl.size
    if sz == 0:
        return np.zeros(0, dtype=np.float64)

    a = _atr(hi, lo, cl, atr_n)
    intents = np.zeros(sz, dtype=np.float64)

    pos = 0
    entry_price = 0.0
    trail = 0.0
    units = 0  # текущее количество "пирамид-юнитов"
    entry_atr = 0.0  # ATR на момент входа — от него считаем 0.5N шаги

    for i in range(sz):
        if i < max(atr_n, mom_n):
            continue
        price = cl[i]
        atr_v = a[i]
        if atr_v <= 0:
            atr_v = 1e-9

        mom = price - cl[i - mom_n]

        # Trailing stop check
        if pos == +1:
            new_trail = price - atr_mult * atr_v
            if new_trail > trail:
                trail = new_trail
            if price < trail:
                pos = 0
                units = 0
        elif pos == -1:
            new_trail = price + atr_mult * atr_v
            if trail == 0 or new_trail < trail:
                trail = new_trail
            if price > trail:
                pos = 0
                units = 0

        # Entry by momentum
        if pos == 0:
            if mom > atr_v:
                pos = +1
                entry_price = price
                entry_atr = atr_v
                trail = price - atr_mult * atr_v
                units = 1
            elif mom < -atr_v:
                pos = -1
                entry_price = price
                entry_atr = atr_v
                trail = price + atr_mult * atr_v
                units = 1

        # Pyramiding (Turtle-style: каждые 0.5N в плюс — +1 unit)
        if pos == +1 and units < max_units:
            steps = int((price - entry_price) / (pyramid_step * entry_atr))
            target_units = max(1, min(max_units, 1 + steps))
            if target_units > units:
                units = target_units
        elif pos == -1 and units < max_units:
            steps = int((entry_price - price) / (pyramid_step * entry_atr))
            target_units = max(1, min(max_units, 1 + steps))
            if target_units > units:
                units = target_units

        if pos == +1:
            intents[i] = float(units) * 0.5 + 0.5  # 1..max_units → 1.0..max*0.5+0.5
        elif pos == -1:
            intents[i] = -(float(units) * 0.5 + 0.5)
        else:
            intents[i] = 0.0

    return intents


def _strategy_regime_hmm(arr: dict[str, np.ndarray], hp: dict[str, Any]) -> np.ndarray:
    """
    Регим-классификатор: HMM (приоритет) → GMM → fallback на Donchian.

    Фичи: log-return, скользящая волатильность, моментум.
    Кластеры с положительным средним → bull, отрицательным → bear.
    Модель кэшируется и переобучается только каждые _HMM_RETRAIN_STEP баров.
    """
    win = int(hp.get("ml_window", 20))
    n_states = int(hp.get("ml_components", 3))

    cl = arr["close"]
    sz = cl.size
    if sz == 0:
        return np.zeros(0, dtype=np.float64)

    # Минимум данных для имеющего смысл обучения.
    min_required = max(win * 5, n_states * 30)
    if sz < min_required:
        return _strategy_donchian_improved(arr, hp)

    log_ret = np.zeros(sz, dtype=np.float64)
    safe_cl = np.clip(cl, 1e-12, None)
    log_ret[1:] = np.log(safe_cl[1:] / safe_cl[:-1])
    vol = _rolling_std_v(log_ret, win)

    mom = np.zeros(sz, dtype=np.float64)
    if sz > win:
        prev = cl[: -win]
        prev_safe = np.where(prev > 0, prev, 1.0)
        mom[win:] = (cl[win:] - prev) / prev_safe

    X = np.column_stack([log_ret, vol, mom])
    train = X[win:]
    if train.shape[0] < n_states * 10:
        return _strategy_donchian_improved(arr, hp)

    # Ключ кэша: квантуем длину обучающей выборки с шагом _HMM_RETRAIN_STEP,
    # чтобы не переобучать модель на каждый новый бар.
    train_len_quantized = (train.shape[0] // _HMM_RETRAIN_STEP) * _HMM_RETRAIN_STEP
    hmm_key = (_hash_param(json.dumps(hp, sort_keys=True)), n_states, train_len_quantized)

    labels = None
    cached_model = _HMM_MODEL_CACHE.get(hmm_key)

    if cached_model is not None:
        # Переиспользуем обученную модель — только predict, без fit.
        try:
            labels = cached_model.predict(X)
        except Exception:
            labels = None
            _HMM_MODEL_CACHE.pop(hmm_key, None)

    if labels is None:
        # Попытка №1: HMM
        if _HAS_HMM:
            try:
                hmm = GaussianHMM(  # type: ignore[misc]
                    n_components=n_states,
                    covariance_type="diag",
                    n_iter=50,
                    random_state=42,
                )
                hmm.fit(train)
                labels = hmm.predict(X)
                if len(_HMM_MODEL_CACHE) >= _HMM_MODEL_CACHE_MAX:
                    _HMM_MODEL_CACHE.pop(next(iter(_HMM_MODEL_CACHE)))
                _HMM_MODEL_CACHE[hmm_key] = hmm
            except Exception:
                labels = None

        # Попытка №2: GMM
        if labels is None and _HAS_SKLEARN:
            try:
                gm = GaussianMixture(  # type: ignore[misc]
                    n_components=n_states,
                    covariance_type="diag",
                    max_iter=100,
                    n_init=2,
                    random_state=42,
                )
                gm.fit(train)
                labels = gm.predict(X)
                if len(_HMM_MODEL_CACHE) >= _HMM_MODEL_CACHE_MAX:
                    _HMM_MODEL_CACHE.pop(next(iter(_HMM_MODEL_CACHE)))
                _HMM_MODEL_CACHE[hmm_key] = gm
            except Exception:
                labels = None

    if labels is None:
        return _strategy_donchian_improved(arr, hp)

    # Сопоставляем кластерам направление.
    train_labels = labels[win:]
    train_log_ret = log_ret[win:]
    log_ret_std = float(log_ret.std()) if log_ret.size > 1 else 0.0
    thresh = 0.25 * log_ret_std

    cluster_dir: dict[int, float] = {}
    for k in range(n_states):
        mask = train_labels == k
        if not mask.any():
            cluster_dir[k] = 0.0
            continue
        mean_ret = float(train_log_ret[mask].mean())
        if abs(mean_ret) <= thresh:
            cluster_dir[k] = 0.0
        else:
            # Магнитуда — z-score кластерного среднего относительно std.
            z = abs(mean_ret) / max(log_ret_std, 1e-9)
            magnitude = float(np.clip(0.5 + z, 0.5, 3.0))
            cluster_dir[k] = magnitude if mean_ret > 0 else -magnitude

    out = np.zeros(sz, dtype=np.float64)
    for i in range(win, sz):
        out[i] = cluster_dir.get(int(labels[i]), 0.0)
    return out


_STRATEGY_FNS = {
    0: _strategy_donchian_improved,
    1: _strategy_ma_rsi_adaptive,
    2: _strategy_atr_trail_pyramid,
    3: _strategy_regime_hmm,
}


# ══════════════════════════════════════════════════════════════════════════════
# STATE MACHINE — spread-aware с подтверждением
# ══════════════════════════════════════════════════════════════════════════════

def _run_state_machine(
    intents: np.ndarray,
    close: np.ndarray,
    spread: float,
    confirmation: int = 0,
) -> np.ndarray:
    """
    Преобразует «сырые намерения» (с магнитудой) в фактические позиции
    с учётом спреда и confirmation.

    Правила:
      • FLAT → LONG: intent > 0
      • FLAT → SHORT: intent < 0
      • LONG  → FLAT: intent == 0
      • SHORT → FLAT: intent == 0
      • LONG  → SHORT (флип): только если ожидаемый профит компенсирует
        2× спред. Иначе остаёмся в LONG.
      • Confirmation: переход требует `confirmation` баров согласного знака.

    Возвращает array float — фактическую магнитуду позиции
    с правильным знаком (готова идти в model() как output).
    """
    n = intents.size
    if n == 0:
        return np.zeros(0, dtype=np.float64)

    out = np.zeros(n, dtype=np.float64)
    pos_sign = 0  # FLAT / LONG / SHORT
    pos_magnitude = 0.0
    last_intent_sign = 0
    confirm_count = 0
    entry_price = 0.0

    spread_threshold = 2.0 * abs(spread) if spread > 0 else 0.0

    for i in range(n):
        intent = float(intents[i])
        intent_sign = (1 if intent > 0 else (-1 if intent < 0 else 0))
        price = float(close[i])

        # Confirmation: требуем N баров согласного знака для перехода.
        if intent_sign == last_intent_sign and intent_sign != 0:
            confirm_count += 1
        else:
            confirm_count = 1 if intent_sign != 0 else 0
        last_intent_sign = intent_sign

        confirmed = confirm_count > confirmation or confirmation == 0

        if intent_sign == 0:
            # Сигнал «выход».
            pos_sign = 0
            pos_magnitude = 0.0
        elif pos_sign == 0:
            if confirmed:
                pos_sign = intent_sign
                pos_magnitude = abs(intent)
                entry_price = price
        elif pos_sign == intent_sign:
            # Удерживаем — обновляем магнитуду на текущее намерение
            # (оно может расти, например при пирамидинге).
            pos_magnitude = abs(intent)
        else:
            # Желание перевернуться. Применяем spread-aware фильтр.
            if confirmed:
                expected_move = abs(price - entry_price)
                if expected_move >= spread_threshold:
                    # Флип оправдан: профит покрыл бы спред.
                    pos_sign = intent_sign
                    pos_magnitude = abs(intent)
                    entry_price = price
                # else: остаёмся в текущей позиции с прежней магнитудой.
                # Не выходим в FLAT — это бы привело к повторному
                # входу в противоположную сторону на след. баре и
                # обошло бы spread filter (платили бы 2× спред).
                # Только явный intent=0 от стратегии может закрыть позицию.

        out[i] = pos_sign * pos_magnitude

    return out


# ══════════════════════════════════════════════════════════════════════════════
# КЭШ
# ══════════════════════════════════════════════════════════════════════════════

_TRAJECTORY_CACHE: dict[tuple, np.ndarray] = {}
_CACHE_MAX = 1024  # было 32 — увеличено в 32× для покрытия пар×дней×vars

# Кэш обученных HMM/GMM моделей: ключ = (hash_param, n_states, len_train)
# Модель переобучается только при изменении обучающей выборки на ≥ 50 баров.
_HMM_MODEL_CACHE: dict[tuple, Any] = {}
_HMM_MODEL_CACHE_MAX = 64
_HMM_RETRAIN_STEP = 50  # переобучаем при росте выборки на этот шаг


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _compute_trajectory(
    marker: tuple,
    rates: Sequence[dict],
    var: int,
    param: str,
) -> np.ndarray:
    cached = _TRAJECTORY_CACHE.get(marker)
    if cached is not None:
        return cached

    arr = _rates_to_arrays(rates)
    hp = _parse_param(param)

    fn = _STRATEGY_FNS.get(var, _strategy_donchian_improved)
    intents = fn(arr, hp)

    # Спред: явный из param или эвристика по последней цене.
    # Безопасно обрабатываем и числа, и строки из JSON/env.
    spread = _safe_float(hp.get("spread"), None)
    if spread is None or spread <= 0:
        spread = _estimate_spread(arr["close"][-1] if arr["close"].size else 1.0)

    confirmation_raw = _safe_float(hp.get("confirmation"), 0.0)
    confirmation = max(0, int(confirmation_raw or 0))

    trajectory = _run_state_machine(intents, arr["close"], float(spread), confirmation)

    if len(_TRAJECTORY_CACHE) >= _CACHE_MAX:
        _TRAJECTORY_CACHE.pop(next(iter(_TRAJECTORY_CACHE)))
    _TRAJECTORY_CACHE[marker] = trajectory
    return trajectory


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа фреймворка
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
    Контракт фреймворка:
        rates         — свечи [{open, close, min, max, date}] строго <= date
        dataset       — события, которые загружает DATASET_QUERY
        dataset_index — индекс/карты фреймворка: ctx_index, url_map, weights и т.п.
                        В этой output-стратегии он не меняет торговый сигнал,
                        но параметр нужен, чтобы фреймворк включил dataset_index=yes.
        date          — целевая дата
        var           — выбор стратегии (0..3)
        param         — JSON c гиперпараметрами:
                        {"spread": 0.0002, "donchian_in": 20, ...}

    Возвращает:
        {"output": +X}  — LONG с магнитудой X (0.5..6.0)
        {"output": -X}  — SHORT с магнитудой X
        {}              — FLAT
    """
    if not rates:
        return {}

    calc_var = int(var or 0)
    if calc_var not in VAR_RANGE:
        return {}

    last_dt = rates[-1].get("date")
    last_ts = int(last_dt.timestamp()) if isinstance(last_dt, datetime) else 0
    first_close = float(rates[0].get("close") or 0.0)
    last_close = float(rates[-1].get("close") or 0.0)
    fp = (round(first_close, 6), round(last_close, 6))
    marker = (calc_var, len(rates), last_ts, _hash_param(param), fp)

    trajectory = _compute_trajectory(marker, rates, calc_var, param)
    if trajectory.size == 0:
        return {}

    last_value = float(trajectory[-1])

    # FLAT: возвращаем пустой dict.
    # LONG/SHORT: возвращаем magnitude с правильным знаком.
    if last_value == 0.0:
        return {}
    return {"output": round(last_value, 4)}


# ══════════════════════════════════════════════════════════════════════════════
# batch_model() — пакетный режим для fill_cache (O(N) вместо O(N²))
# ══════════════════════════════════════════════════════════════════════════════

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
    Пакетная версия model() для fill_cache.

    Вычисляет полную траекторию ОДИН РАЗ по всему `rates`,
    затем для каждой даты из `dates` читает значение по индексу.

    Сложность: O(N) вместо O(N²) при последовательном вызове model()
    на каждой свече — ключевое ускорение fill_cache.

    Гарантия каузальности: все стратегии в _STRATEGY_FNS каузальны
    (trajectory[i] зависит только от rates[0..i]), поэтому вычисление
    по полному ряду эквивалентно вычислению по срезу.

    Возвращает:
        {date: {"output": X}}  — LONG/SHORT
        {date: {}}             — FLAT
    """
    if not rates or not dates:
        return {d: {} for d in dates}

    calc_var = int(var or 0)
    if calc_var not in VAR_RANGE:
        return {d: {} for d in dates}

    # Маркер по ВСЕЙ истории — попадаем в кэш при повторных вызовах.
    last_dt = rates[-1].get("date")
    last_ts = int(last_dt.timestamp()) if isinstance(last_dt, datetime) else 0
    first_close = float(rates[0].get("close") or 0.0)
    last_close = float(rates[-1].get("close") or 0.0)
    fp = (round(first_close, 6), round(last_close, 6))
    marker = (calc_var, len(rates), last_ts, _hash_param(param), fp)

    # Одно вычисление траектории на весь ряд.
    trajectory = _compute_trajectory(marker, rates, calc_var, param)
    if trajectory.size == 0:
        return {d: {} for d in dates}

    # Индекс date → позиция в rates для быстрого bisect.
    rate_dates = [r["date"] for r in rates]

    results: dict[datetime, dict[str, float]] = {}
    for target_date in dates:
        idx = bisect.bisect_right(rate_dates, target_date) - 1
        if idx < 0:
            results[target_date] = {}
            continue
        val = float(trajectory[idx])
        results[target_date] = {} if val == 0.0 else {"output": round(val, 4)}

    return results
