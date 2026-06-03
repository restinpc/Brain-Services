"""
model.py — brain-trend-combo (сервис 51)

var = размер окна для поиска экстремумов:
    0 → окно 1  (adjacent-bar, плотные экстремумы ~4.7ч)
    1 → окно 3  (~10.8ч между экстремумами)
    2 → окно 5  (~17.2ч)
    3 → окно 7  (~23.6ч, крупные свинги)

type = формат возвращаемого сигнала:
    0 → bull_ratio (0..1, вероятность бычьей следующей свечи)
    1 → только надёжные паттерны (occurrence_count >= MIN_OCC), те же 0..1
    2 → центрированный сигнал (2*bull_ratio-1, от -1 до +1)

weight_code = f"{var}_{prefix}_{dir_bits}"
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import numpy as np

from brain_framework import get_service_config

# ── Константы (импортируются context_idx.py) ──────────────────────────────────
SERVICE_ID  = 51
RATES_TABLE = "brain_rates_eur_usd"

VAR_RANGE   = [0, 1, 2, 3]
TYPES_RANGE = [0, 1, 2]

# Минимальное кол-во вхождений паттерна для type=1
MIN_OCC = 30

# Окно поиска экстремумов по var
EXTREMA_WINDOWS: dict[int, int] = {0: 1, 1: 3, 2: 5, 3: 7}

# ── Таймфреймы ────────────────────────────────────────────────────────────────
TIMEFRAME_DEFS: list[tuple[str, int]] = [
    ("1h",   1   ),
    ("3h",   3   ),
    ("6h",   6   ),
    ("12h",  12  ),
    ("24h",  24  ),
    ("3d",   72  ),
    ("7d",   168 ),
    ("15d",  360 ),
    ("1m",   720 ),
    ("3m",   2160),
]

N_TF        = len(TIMEFRAME_DEFS)
N_ADD_TF    = N_TF - 1
MAX_PREFIX  = 1 << N_ADD_TF   # 512
MIN_CANDLES = 20


# ══════════════════════════════════════════════════════════════════════════════
# АГРЕГАЦИЯ OHLC
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_ohlc(
    ts: np.ndarray, opens: np.ndarray, closes: np.ndarray,
    highs: np.ndarray, lows: np.ndarray, period_hours: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    period_sec = period_hours * 3600
    groups  = ts // period_sec
    change  = np.concatenate(([True], np.diff(groups) != 0))
    starts  = np.where(change)[0]
    ends    = np.concatenate((starts[1:], [len(groups)]))
    agg_ts     = groups[starts] * period_sec
    agg_opens  = opens[starts]
    agg_closes = closes[ends - 1]
    agg_highs  = np.array([np.max(highs[s:e]) for s, e in zip(starts, ends)])
    agg_lows   = np.array([np.min(lows[s:e])  for s, e in zip(starts, ends)])
    return agg_ts, agg_opens, agg_closes, agg_highs, agg_lows


# ══════════════════════════════════════════════════════════════════════════════
# ПОИСК ЭКСТРЕМУМОВ (с вариантами окна)
# ══════════════════════════════════════════════════════════════════════════════

def find_all_extrema(
    highs: np.ndarray,
    lows:  np.ndarray,
    window: int = 1,
) -> tuple[np.ndarray, np.ndarray]:
    """
    window=1: adjacent-bar (векторизованный, быстрый).
    window=3,5,7: точка является экстремумом если она строго больше/меньше
                  всех соседей в радиусе window. Даёт более значимые свинги.
    """
    n = len(highs)
    if n < 2 * window + 1:
        return np.array([], dtype=np.intp), np.array([], dtype=np.intp)

    if window == 1:
        upper = np.where((highs[1:-1] > highs[:-2]) & (highs[1:-1] > highs[2:]))[0] + 1
        lower = np.where((lows[1:-1]  < lows[:-2])  & (lows[1:-1]  < lows[2:]))[0]  + 1
        return upper, lower

    upper_list, lower_list = [], []
    for i in range(window, n - window):
        h_i, l_i = highs[i], lows[i]
        if h_i > np.max(highs[i - window:i]) and h_i > np.max(highs[i + 1:i + window + 1]):
            upper_list.append(i)
        if l_i < np.min(lows[i - window:i]) and l_i < np.min(lows[i + 1:i + window + 1]):
            lower_list.append(i)
    return np.array(upper_list, dtype=np.intp), np.array(lower_list, dtype=np.intp)


def last_n_before(
    extrema_idx: np.ndarray, pos: int, n: int = 2,
) -> Optional[np.ndarray]:
    i = int(np.searchsorted(extrema_idx, pos, side="left"))
    return None if i < n else extrema_idx[i - n : i]


# ══════════════════════════════════════════════════════════════════════════════
# АЛГОРИТМ ТРЕНДА
# ══════════════════════════════════════════════════════════════════════════════

def trend_direction(
    ts: np.ndarray, highs: np.ndarray, lows: np.ndarray,
    upper_ext: np.ndarray, lower_ext: np.ndarray, pos: int,
) -> Optional[int]:
    """
    H1,H2 — последние 2 верхних экстремума, L1,L2 — нижних.
    slope_upper = (H2-H1)/(t2-t1), slope_lower = (L2-L1)/(t2-t1)
    mean_slope  = (slope_upper + slope_lower) / 2
    Возвращает 1 (вверх) или 0 (вниз), None если нет данных.
    """
    u = last_n_before(upper_ext, pos, 2)
    l = last_n_before(lower_ext, pos, 2)
    if u is None or l is None:
        return None
    u0, u1 = int(u[0]), int(u[1])
    l0, l1 = int(l[0]), int(l[1])
    dt_u = float(ts[u1] - ts[u0]); dt_l = float(ts[l1] - ts[l0])
    su = (highs[u1] - highs[u0]) / dt_u if dt_u > 0 else 0.0
    sl = (lows[l1]  - lows[l0])  / dt_l if dt_l > 0 else 0.0
    return 1 if (su + sl) * 0.5 >= 0.0 else 0


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ НАПРАВЛЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════

def compute_directions_at(
    ts: np.ndarray, opens: np.ndarray, closes: np.ndarray,
    highs: np.ndarray, lows: np.ndarray,
    window: int = 1,
) -> list[Optional[int]]:
    """Вычисляет направление тренда для всех 10 TF. window = var → EXTREMA_WINDOWS."""
    n = len(ts)
    if n < MIN_CANDLES:
        return [None] * N_TF

    directions: list[Optional[int]] = []

    ue, le = find_all_extrema(highs, lows, window)
    directions.append(trend_direction(ts, highs, lows, ue, le, n))

    for _, period_hours in TIMEFRAME_DEFS[1:]:
        agg_ts, _, _, agg_h, agg_l = aggregate_ohlc(ts, opens, closes, highs, lows, period_hours)
        m = len(agg_ts)
        if m < MIN_CANDLES:
            directions.append(None)
            continue
        ue_a, le_a = find_all_extrema(agg_h, agg_l, window)
        directions.append(trend_direction(agg_ts, agg_h, agg_l, ue_a, le_a, m))

    return directions


# ══════════════════════════════════════════════════════════════════════════════
# ФОРМИРОВАНИЕ WEIGHT CODES
# ══════════════════════════════════════════════════════════════════════════════

def build_weight_codes(
    directions: list[Optional[int]],
    var: int = 0,
) -> dict[str, float]:
    """
    weight_code = f"{var}_{prefix}_{dir_bits}"
    Возвращает {wc: 0.5} — neutral default, перезаписывается из ctx_index.
    """
    if directions[0] is None:
        return {}
    result: dict[str, float] = {}
    for prefix in range(MAX_PREFIX):
        active = [0] + [bit + 1 for bit in range(N_ADD_TF) if prefix & (1 << bit)]
        if any(directions[i] is None for i in active):
            continue
        dir_bits = "".join(str(directions[i]) for i in active)
        result[f"{var}_{prefix}_{dir_bits}"] = 0.5
    return result


# ══════════════════════════════════════════════════════════════════════════════
# model()
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],
    date:          datetime,
    *,
    type:          int  = 0,
    var:           int  = 0,
    param:         str  = "",
    dataset_index: dict | None = None,
    **kw,
) -> dict[str, float]:
    if not rates:
        return {}

    ts     = np.array([int(r["date"].timestamp())   for r in rates], dtype=np.int64)
    opens  = np.array([float(r.get("open")  or 0.0) for r in rates], dtype=np.float64)
    closes = np.array([float(r.get("close") or 0.0) for r in rates], dtype=np.float64)
    highs  = np.array([float(r.get("max")   or 0.0) for r in rates], dtype=np.float64)
    lows   = np.array([float(r.get("min")   or 0.0) for r in rates], dtype=np.float64)

    window     = EXTREMA_WINDOWS.get(var, 1)
    directions = compute_directions_at(ts, opens, closes, highs, lows, window)
    wc_base    = build_weight_codes(directions, var)
    if not wc_base:
        return {}

    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if not ctx_index:
        return wc_base   # нет индекса — возвращаем neutral 0.5

    reverse: dict[str, dict] = {
        info["weight_code"]: info
        for _, info in ctx_index.items()
        if info.get("weight_code")
    }

    result: dict[str, float] = {}
    for wc in wc_base:
        entry = reverse.get(wc)
        if not entry:
            continue
        occ       = int(entry.get("occurrence_count") or 0)
        bull_ratio = float(entry.get("bull_ratio") or 0.5)
        avg_t1     = float(entry.get("avg_t1") or 0.0)

        if occ == 0:
            continue

        if type == 0:
            # Сырой bull_ratio (вероятность бычьей следующей свечи)
            result[wc] = bull_ratio

        elif type == 1:
            # Только надёжные паттерны — отфильтрованы по MIN_OCC вхождений
            if occ >= MIN_OCC:
                result[wc] = bull_ratio

        elif type == 2:
            # Центрированный сигнал: 0=нейтраль, >0=бычий, <0=медвежий
            result[wc] = round(2.0 * bull_ratio - 1.0, 6)

    return result
