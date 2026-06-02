"""
model.py — brain-extremum-levels (сервис 53)

9 вариантов (var=0..8) = 3 уровня order × 3 порога confirm_k:

  var │ order │ confirm_k │ Смысл
  ────┼───────┼───────────┼─────────────────────────────────────────
   0  │   2   │  0.10     │ микро-уровни, слабое подтверждение
   1  │   2   │  0.15     │ микро-уровни, стандарт
   2  │   2   │  0.25     │ микро-уровни, строгое подтверждение
   3  │   5   │  0.10     │ свинг-уровни, слабое подтверждение
   4  │   5   │  0.15     │ свинг-уровни, стандарт  ← был дефолт
   5  │   5   │  0.25     │ свинг-уровни, строгое подтверждение
   6  │  10   │  0.10     │ структурные уровни, слабое подтверждение
   7  │  10   │  0.15     │ структурные уровни, стандарт
   8  │  10   │  0.25     │ структурные уровни, строгое подтверждение

48 базовых weight_codes = 12 TF × 4 события (bo_bull/bo_bear/rb_bull/rb_bear)
Итого в индексе: 48 × 9 = 432 строки (weight_code × var)

type = метод измерения исхода (4 bull_ratio столбца на каждую строку):
  type=0 → bull_ratio_t0  ATR-based confirmation через 3 бара
  type=1 → bull_ratio_t1  фикс. N свечей вперёд (period × T1_MULT)
  type=2 → bull_ratio_t2  фикс. % движения (T2_PCT)
  type=3 → bull_ratio_t3  формирование нового экстремума
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

import numpy as np
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("brain-framework")

# ── Константы ─────────────────────────────────────────────────────────────────
SERVICE_ID   = 54
RATES_TABLE  = "brain_rates_eur_usd"

TIMEFRAMES   = [3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 20]
N_LEVELS     = 5     # последних уровней каждого типа
MIN_AGG_BARS = 10    # минимум агрегированных свечей
ATR_PERIOD   = 14    # период ATR на часовых барах
PROXIMITY_K  = 0.5   # |close − level| ≤ K × ATR → «рядом»

# 9 вариантов: var → (order, confirm_k)
VAR_CONFIGS: dict[int, tuple[int, float]] = {
    0: (2,  0.10),
    1: (2,  0.15),
    2: (2,  0.25),
    3: (5,  0.10),
    4: (5,  0.15),
    5: (5,  0.25),
    6: (10, 0.10),
    7: (10, 0.15),
    8: (10, 0.25),
}

# type → колонка bull_ratio
TYPE_COL: dict[int, str] = {
    0: "bull_ratio_t0",
    1: "bull_ratio_t1",
    2: "bull_ratio_t2",
    3: "bull_ratio_t3",
}


# ══════════════════════════════════════════════════════════════════════════════
# Агрегация и ATR
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_ohlc(
    ts: np.ndarray, opens: np.ndarray, closes: np.ndarray,
    highs: np.ndarray, lows: np.ndarray, period_hours: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    period_sec = period_hours * 3600
    groups     = ts // period_sec
    change     = np.concatenate(([True], np.diff(groups) != 0))
    starts     = np.where(change)[0]
    ends       = np.concatenate((starts[1:], [len(groups)]))
    return (
        groups[starts] * period_sec,
        opens[starts],
        closes[ends - 1],
        np.array([np.max(highs[s:e]) for s, e in zip(starts, ends)]),
        np.array([np.min(lows[s:e])  for s, e in zip(starts, ends)]),
    )


def compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                period: int = 14) -> np.ndarray:
    if len(highs) < 2:
        return np.full(len(highs), 1e-8)
    tr    = np.maximum(highs[1:] - lows[1:],
                       np.maximum(np.abs(highs[1:] - closes[:-1]),
                                  np.abs(lows[1:]  - closes[:-1])))
    atr   = np.empty(len(tr))
    atr[0] = tr[0]
    alpha  = 1.0 / period
    for i in range(1, len(tr)):
        atr[i] = alpha * tr[i] + (1.0 - alpha) * atr[i - 1]
    return np.concatenate(([atr[0]], atr))


# ══════════════════════════════════════════════════════════════════════════════
# Поиск уровней
# ══════════════════════════════════════════════════════════════════════════════

def find_extrema_levels(
    agg_highs: np.ndarray, agg_lows: np.ndarray,
    up_to: int, n: int = N_LEVELS, order: int = 2,
) -> tuple[list[float], list[float]]:
    """
    Возвращает (resistance_levels, support_levels).
    order — чувствительность: 2=микро, 5=свинг, 10=структурные.
    """
    from scipy.signal import argrelextrema

    m = up_to
    if m < order * 2 + 1:
        return [], []

    h = agg_highs[:m]
    l = agg_lows[:m]

    res_idx = argrelextrema(h, np.greater, order=order)[0]
    sup_idx = argrelextrema(l, np.less,    order=order)[0]

    resistance = [float(h[i]) for i in res_idx[-n:]]
    support    = [float(l[i]) for i in sup_idx[-n:]]
    return resistance, support


# ══════════════════════════════════════════════════════════════════════════════
# Детектор событий — параметризованный
# ══════════════════════════════════════════════════════════════════════════════

def detect_events(
    ts: np.ndarray, opens: np.ndarray, closes: np.ndarray,
    highs: np.ndarray, lows: np.ndarray,
    order: int, confirm_k: float,
) -> list[str]:
    """
    Детектирует события на последнем баре с заданными order и confirm_k.
    Возвращает список "tf{period}_{event}".
    """
    n = len(ts)
    if n < 2:
        return []

    atr_arr   = compute_atr(highs, lows, closes, ATR_PERIOD)
    atr       = float(atr_arr[-1]) or 1e-8
    proximity = PROXIMITY_K * atr
    confirm   = confirm_k   * atr

    curr_close = float(closes[-1])
    curr_high  = float(highs[-1])
    curr_low   = float(lows[-1])
    prev_close = float(closes[-2])

    events: list[str] = []

    for period in TIMEFRAMES:
        if n < period * MIN_AGG_BARS:
            continue

        _, _, _, agg_h, agg_l = aggregate_ohlc(ts, opens, closes, highs, lows, period)
        m = len(agg_h)
        if m < MIN_AGG_BARS:
            continue

        resistance, support = find_extrema_levels(agg_h, agg_l, m, order=order)
        pfx = f"tf{period}"

        for lvl in resistance:
            if abs(curr_close - lvl) > proximity * 2:
                continue
            if prev_close < lvl and curr_close > lvl + confirm:
                events.append(f"{pfx}_bo_bull"); break
            if curr_high >= lvl - proximity and curr_close < lvl - confirm:
                events.append(f"{pfx}_rb_bear"); break

        for lvl in support:
            if abs(curr_close - lvl) > proximity * 2:
                continue
            if prev_close > lvl and curr_close < lvl - confirm:
                events.append(f"{pfx}_bo_bear"); break
            if curr_low <= lvl + proximity and curr_close > lvl + confirm:
                events.append(f"{pfx}_rb_bull"); break

    return events


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка индекса из БД
# ══════════════════════════════════════════════════════════════════════════════

# Кеш индекса: (var, type_col) → lookup dict
# Сбрасывается при перезапуске процесса (раз в reload_interval)
_INDEX_CACHE: dict[tuple, dict[str, float]] = {}


def _load_index_from_db(var: int, type_col: str) -> dict[str, float]:
    """
    Читает bull_ratio из БД. Результат кешируется в памяти процесса.
    Кеш сбрасывается при перезапуске сервиса (раз в reload_interval).
    """
    cache_key = (var, type_col)
    if cache_key in _INDEX_CACHE:
        return _INDEX_CACHE[cache_key]

    idx_table = f"vlad_extremum_lvl_svc{SERVICE_ID}_index"
    import mysql.connector
    try:
        cfg = {
            "host":     os.getenv("DB_HOST",     "127.0.0.1"),
            "port":     int(os.getenv("DB_PORT", "3306")),
            "user":     os.getenv("DB_USER",     "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME",     "vlad"),
        }
        conn = mysql.connector.connect(**cfg)
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT weight_code, `{type_col}` FROM `{idx_table}` WHERE var = %s",
            (var,)
        )
        rows = cur.fetchall(); cur.close(); conn.close()
        result = {r["weight_code"]: float(r[type_col])
                  for r in rows if r[type_col] is not None}
        _INDEX_CACHE[cache_key] = result
        log.debug(f"[exl] loaded {len(result)} index entries var={var} {type_col}")
        return result
    except Exception as e:
        log.warning(f"[extremum-levels] _load_index_from_db error: {e}")
        return {}


def _build_lookup_from_ctx(ctx_index: dict, var: int, type_col: str) -> dict[str, float]:
    """Fallback: ctx_index от фреймворка."""
    lookup = {}
    for _, info in ctx_index.items():
        if info.get("var") != var:
            continue
        wc = info.get("weight_code")
        br = info.get(type_col)
        if wc and br is not None:
            lookup[wc] = float(br)
    return lookup


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа фреймворка
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],
    date:          datetime,
    *,
    type:          int  = 0,
    var:           int  = 4,   # default: свинг-уровни + стандарт
    param:         str  = "",
    dataset_index: dict | None = None,
    **kw,
) -> dict[str, float]:
    """
    Детектирует пробои/отскоки с параметрами из VAR_CONFIGS[var].
    Возвращает {weight_code: bull_ratio} для активных событий.
    """
    if not rates or len(rates) < MIN_AGG_BARS * max(TIMEFRAMES):
        return {}

    # ── Параметры для этого var ────────────────────────────────────────────
    if var not in VAR_CONFIGS:
        var = 4
    order, confirm_k = VAR_CONFIGS[var]

    # ── Котировки → numpy ──────────────────────────────────────────────────
    ts     = np.array([int(r["date"].timestamp())    for r in rates], dtype=np.int64)
    opens  = np.array([float(r.get("open")  or 0.0) for r in rates], dtype=np.float64)
    closes = np.array([float(r.get("close") or 0.0) for r in rates], dtype=np.float64)
    highs  = np.array([float(r.get("max")   or 0.0) for r in rates], dtype=np.float64)
    lows   = np.array([float(r.get("min")   or 0.0) for r in rates], dtype=np.float64)

    # ── Детектируем события для этого var ─────────────────────────────────
    active = detect_events(ts, opens, closes, highs, lows, order, confirm_k)
    if not active:
        return {}

    # ── Загружаем bull_ratio ───────────────────────────────────────────────
    type_col  = TYPE_COL.get(type, "bull_ratio_t0")
    ctx_index = (dataset_index or {}).get("ctx_index") or {}

    lookup = _load_index_from_db(var, type_col)
    if not lookup:
        lookup = _build_lookup_from_ctx(ctx_index, var, type_col)

    result = {wc: round(lookup.get(wc, 0.5), 6) for wc in active}
    log.debug(f"[exl] {date} var={var}(order={order},ck={confirm_k}) "
              f"type={type}: {len(result)} events")
    return result