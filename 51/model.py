"""
model.py — brain-trend-combo (сервис 45)

Многотаймфреймовый индексатор трендовых комбинаций.

Алгоритм (один вызов model()):
  1. Часовые котировки до `date` конвертируются в numpy-массивы.
  2. Для каждого из 10 TF (1h, 3h, 6h, 12h, 24h, 3d, 7d, 15d, 1m, 3m)
     агрегируется OHLC и вычисляется направление тренда:
       - находим 2 последних верхних и 2 нижних экстремума;
       - upper slope = (H2-H1)/(t2-t1),  lower slope = (L2-L1)/(t2-t1);
       - mean_slope = (upper + lower) / 2;
       - direction = 1 (вверх) если mean_slope >= 0, иначе 0.
  3. prefix = bitmask из 9 бит (bit i → включён TF[i+1], i=0..8).
     prefix 0 = только 1h.  Всего 512 prefix-значений.
  4. direction_bits = строка бит в порядке активных TF, начиная с TF0.
     weight_code = f"{prefix}_{direction_bits}"
  5. /values возвращает все валидные prefix (≤512 ключей):
       key   = weight_code   (например "7_010" = 1h вниз, 3h вверх, 6h вниз)
       value = bull_ratio из ctx_index (вероятность ↑ следующей свечи),
               или 0.5 если индекс ещё не построен.

Инфраструктура (индекс) — в context_idx.py::build_index().
Конфигурация — в config.toml.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import numpy as np

from brain_framework import get_service_config

# ── Фиксированы в model.py чтобы context_idx.py мог их импортировать ─────────
# (фреймворк читает эти же значения из config.toml через _get(),
#  здесь они нужны только для context_idx.py::build_index)
SERVICE_ID  = 51                        # brain_models.id — ПОДСТАВИТЬ перед деплоем
RATES_TABLE = "brain_rates_eur_usd"     # базовая часовая таблица


# ══════════════════════════════════════════════════════════════════════════════
# ТАЙМФРЕЙМЫ
# ══════════════════════════════════════════════════════════════════════════════

# (label, period_hours)
# TF0 (1h) всегда в combo; биты 0..8 кодируют TF1..TF9
TIMEFRAME_DEFS: list[tuple[str, int]] = [
    ("1h",   1   ),   # idx 0 — базовый
    ("3h",   3   ),   # idx 1, bit 0
    ("6h",   6   ),   # idx 2, bit 1
    ("12h",  12  ),   # idx 3, bit 2
    ("24h",  24  ),   # idx 4, bit 3
    ("3d",   72  ),   # idx 5, bit 4
    ("7d",   168 ),   # idx 6, bit 5
    ("15d",  360 ),   # idx 7, bit 6
    ("1m",   720 ),   # idx 8, bit 7  (~30 дней)
    ("3m",   2160),   # idx 9, bit 8  (~90 дней)
]

N_TF       = len(TIMEFRAME_DEFS)           # 10
N_ADD_TF   = N_TF - 1                      # 9 дополнительных TF
MAX_PREFIX = 1 << N_ADD_TF                 # 512
MIN_CANDLES = 20   # минимум агрегированных свечей для нахождения 2 экстремумов


# ══════════════════════════════════════════════════════════════════════════════
# АГРЕГАЦИЯ OHLC
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_ohlc(
    ts:     np.ndarray,
    opens:  np.ndarray,
    closes: np.ndarray,
    highs:  np.ndarray,
    lows:   np.ndarray,
    period_hours: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Агрегирует часовые OHLC в таймфрейм period_hours.
    Выравнивание: group = unix_ts // (period_hours * 3600).
    Возвращает (timestamps, opens, closes, highs, lows).
    """
    period_sec = period_hours * 3600
    groups = ts // period_sec

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
# ПОИСК ЭКСТРЕМУМОВ
# ══════════════════════════════════════════════════════════════════════════════

def find_all_extrema(
    highs: np.ndarray,
    lows:  np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Находит все локальные максимумы (highs) и минимумы (lows)
    по критерию сравнения с соседними барами (adjacent-bar).
    Возвращает (upper_idx, lower_idx) — массивы позиций в порядке возрастания.
    """
    if len(highs) < 3:
        return np.array([], dtype=np.intp), np.array([], dtype=np.intp)

    upper = np.where((highs[1:-1] > highs[:-2]) & (highs[1:-1] > highs[2:]))[0] + 1
    lower = np.where((lows[1:-1]  < lows[:-2])  & (lows[1:-1]  < lows[2:]))[0]  + 1
    return upper, lower


def last_n_before(
    extrema_idx: np.ndarray,
    pos: int,
    n: int = 2,
) -> Optional[np.ndarray]:
    """
    Возвращает последние n экстремумов строго перед позицией pos
    в порядке возрастания времени. None если их меньше n.
    """
    i = int(np.searchsorted(extrema_idx, pos, side="left"))
    if i < n:
        return None
    return extrema_idx[i - n : i]


# ══════════════════════════════════════════════════════════════════════════════
# АЛГОРИТМ ТРЕНДА
# ══════════════════════════════════════════════════════════════════════════════

def trend_direction(
    ts:         np.ndarray,
    highs:      np.ndarray,
    lows:       np.ndarray,
    upper_ext:  np.ndarray,
    lower_ext:  np.ndarray,
    pos:        int,
) -> Optional[int]:
    """
    Вычисляет направление тренда на момент позиции pos-1.

    Алгоритм:
      H1, H2 — последние 2 верхних экстремума перед pos
      L1, L2 — последние 2 нижних  экстремума перед pos

      slope_upper = (H2.high - H1.high) / (H2.ts - H1.ts)
      slope_lower = (L2.low  - L1.low)  / (L2.ts - L1.ts)
      mean_slope  = (slope_upper + slope_lower) / 2

    Возвращает: 1 (вверх), 0 (вниз), None (нет данных).
    """
    u = last_n_before(upper_ext, pos, 2)
    l = last_n_before(lower_ext, pos, 2)
    if u is None or l is None:
        return None

    u0, u1 = int(u[0]), int(u[1])
    dt_u    = float(ts[u1] - ts[u0])
    slope_u = (highs[u1] - highs[u0]) / dt_u if dt_u > 0 else 0.0

    l0, l1 = int(l[0]), int(l[1])
    dt_l    = float(ts[l1] - ts[l0])
    slope_l = (lows[l1] - lows[l0]) / dt_l if dt_l > 0 else 0.0

    return 1 if (slope_u + slope_l) * 0.5 >= 0.0 else 0


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ НАПРАВЛЕНИЙ ДЛЯ ВСЕХ TF
# ══════════════════════════════════════════════════════════════════════════════

def compute_directions_at(
    ts:     np.ndarray,
    opens:  np.ndarray,
    closes: np.ndarray,
    highs:  np.ndarray,
    lows:   np.ndarray,
) -> list[Optional[int]]:
    """
    Вычисляет направление тренда для всех 10 TF на момент последней свечи.
    Возвращает список длиной N_TF: 1=вверх, 0=вниз, None=нет данных.
    """
    n = len(ts)
    if n < MIN_CANDLES:
        return [None] * N_TF

    directions: list[Optional[int]] = []

    # TF0: часовой
    ue, le = find_all_extrema(highs, lows)
    directions.append(trend_direction(ts, highs, lows, ue, le, n))

    # TF1..TF9: агрегированные
    for _, period_hours in TIMEFRAME_DEFS[1:]:
        agg_ts, _, _, agg_h, agg_l = aggregate_ohlc(
            ts, opens, closes, highs, lows, period_hours
        )
        m = len(agg_ts)
        if m < MIN_CANDLES:
            directions.append(None)
            continue
        ue_a, le_a = find_all_extrema(agg_h, agg_l)
        directions.append(trend_direction(agg_ts, agg_h, agg_l, ue_a, le_a, m))

    return directions


# ══════════════════════════════════════════════════════════════════════════════
# ФОРМИРОВАНИЕ WEIGHT CODES
# ══════════════════════════════════════════════════════════════════════════════

def build_weight_codes(
    directions: list[Optional[int]],
) -> dict[str, float]:
    """
    Для каждого валидного prefix (0..511) строит weight_code и
    возвращает {weight_code: 0.5} (neutral default).

    prefix = bitmask: bit i (0..8) → включён TF[i+1].
    prefix 0 → только TF0 (1h).

    weight_code = f"{prefix}_{direction_bits}"
    direction_bits = строка бит активных TF в порядке возрастания индекса.
    """
    if directions[0] is None:
        return {}

    result: dict[str, float] = {}
    for prefix in range(MAX_PREFIX):
        active = [0] + [bit + 1 for bit in range(N_ADD_TF) if prefix & (1 << bit)]

        if any(directions[i] is None for i in active):
            continue

        dir_bits    = "".join(str(directions[i]) for i in active)
        weight_code = f"{prefix}_{dir_bits}"
        result[weight_code] = 0.5   # будет перезаписан из ctx_index

    return result


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа фреймворка
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],   # не используется — сервис работает только на rates
    date:          datetime,
    *,
    type:          int  = 0,
    var:           int  = 0,
    param:         str  = "",
    dataset_index: dict | None = None,
    **kw,
) -> dict[str, float]:
    """
    Вычисляет трендовые комбинации для всех 10 TF на момент `date`.
    Возвращает {weight_code: bull_ratio} для каждого валидного prefix.

    bull_ratio — доля бычьих следующих свечей при данной комбинации
                 (из индексной таблицы vlad_trend_combo_svc45_index).
                 0.5 если индекс ещё не заполнен.
    """
    if not rates:
        return {}

    # ── Конвертация котировок ─────────────────────────────────────────────────
    ts     = np.array([int(r["date"].timestamp())    for r in rates], dtype=np.int64)
    opens  = np.array([float(r.get("open")  or 0.0)  for r in rates], dtype=np.float64)
    closes = np.array([float(r.get("close") or 0.0)  for r in rates], dtype=np.float64)
    highs  = np.array([float(r.get("max")   or 0.0)  for r in rates], dtype=np.float64)
    lows   = np.array([float(r.get("min")   or 0.0)  for r in rates], dtype=np.float64)

    # ── Направления ───────────────────────────────────────────────────────────
    directions = compute_directions_at(ts, opens, closes, highs, lows)

    # ── Базовые weight_codes ──────────────────────────────────────────────────
    result = build_weight_codes(directions)
    if not result:
        return {}

    # ── Обогащаем из ctx_index (исторический bull_ratio) ─────────────────────
    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if ctx_index:
        reverse: dict[str, dict] = {
            info["weight_code"]: info
            for _, info in ctx_index.items()
            if info.get("weight_code")
        }
        for wc in result:
            entry = reverse.get(wc)
            if entry and int(entry.get("occurrence_count") or 0) > 0:
                result[wc] = float(entry.get("bull_ratio") or 0.5)

    return result