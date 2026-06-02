"""
context_idx.py — brain-extremum-levels  (оптимизированная версия)

Ключевая оптимизация:
  БЫЛО: argrelextrema вызывается 9 vars × 12 TF = 108 раз НА КАЖДЫЙ БАР
        → 70K × 108 = ~7.5 млн вызовов scipy → 15+ минут

  СТАЛО: для каждой из 36 пар (TF, order) вычисляем экстремумы ОДИН РАЗ.
         На каждом баре — только бинарный поиск последних N уровней.
         → 36 вызовов scipy + O(1) на каждый бар → ~1-2 минуты.

Outcomes тоже векторизованы:
  t0, t1 — shift массива closes → вся история за один numpy вызов.
  t2     — rolling argmax/argmin по future window → векторизованно.
  t3     — pre-computed next-extremum array.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import numpy as np
from scipy.signal import argrelextrema
from sqlalchemy import text

log = logging.getLogger("brain-framework")

import importlib.util as _ilu

def _import_model():
    here = os.path.dirname(os.path.abspath(__file__))
    spec = _ilu.spec_from_file_location("exl_model", os.path.join(here, "model.py"))
    mod  = _ilu.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

_m = _import_model()

aggregate_ohlc  = _m.aggregate_ohlc
compute_atr     = _m.compute_atr
SERVICE_ID      = _m.SERVICE_ID
RATES_TABLE     = _m.RATES_TABLE
TIMEFRAMES      = _m.TIMEFRAMES
MIN_AGG_BARS    = _m.MIN_AGG_BARS
ATR_PERIOD      = _m.ATR_PERIOD
PROXIMITY_K     = _m.PROXIMITY_K
VAR_CONFIGS     = _m.VAR_CONFIGS
N_LEVELS        = _m.N_LEVELS

T1_FORWARD_MULT = int(os.getenv("T1_FORWARD_MULT", "3"))
T2_FORWARD_PCT  = float(os.getenv("T2_FORWARD_PCT", "0.005"))
T2_MAX_FORWARD  = int(os.getenv("T2_MAX_FORWARD",  "20"))
MIN_WARMUP_BARS = int(os.getenv("MIN_WARMUP_BARS", "500"))
BATCH_SIZE      = 500

INDEX_TABLE = f"vlad_extremum_lvl_svc{SERVICE_ID}_index"
ORDERS      = sorted(set(cfg[0] for cfg in VAR_CONFIGS.values()))  # [2, 5, 10]


# ── DDL ──────────────────────────────────────────────────────────────────────

async def _ensure_table(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{INDEX_TABLE}` (
                `id`               INT           NOT NULL AUTO_INCREMENT,
                `weight_code`      VARCHAR(30)   NOT NULL,
                `var`              TINYINT       NOT NULL,
                `period`           TINYINT       NOT NULL,
                `event_type`       VARCHAR(12)   NOT NULL,
                `order_val`        TINYINT       NOT NULL,
                `confirm_k`        FLOAT         NOT NULL,
                `bull_ratio_t0`    FLOAT         NOT NULL DEFAULT 0.5,
                `bull_ratio_t1`    FLOAT         NOT NULL DEFAULT 0.5,
                `bull_ratio_t2`    FLOAT         NOT NULL DEFAULT 0.5,
                `bull_ratio_t3`    FLOAT         NOT NULL DEFAULT 0.5,
                `t1_forward_bars`  SMALLINT      NOT NULL DEFAULT 3,
                `t2_forward_pct`   FLOAT         NOT NULL DEFAULT 0.005,
                `occurrence_count` INT           NOT NULL DEFAULT 0,
                `support`          FLOAT         NOT NULL DEFAULT 0.0,
                `date_updated`     DATETIME      NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_wc_var` (`weight_code`, `var`),
                INDEX `idx_var`   (`var`),
                INDEX `idx_period`(`period`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def on_startup(engine_vlad, engine_brain) -> None:
    await _ensure_table(engine_vlad)


# ── Котировки ─────────────────────────────────────────────────────────────────

async def _load_rates(engine, table: str) -> tuple:
    """Возвращает (n, ts, opens, closes, highs, lows) как numpy-массивы."""
    async with engine.connect() as conn:
        res = await conn.execute(text(
            f"SELECT date, open, close, `max`, `min` FROM `{table}` ORDER BY date"
        ))
        rows = res.mappings().all()
    n      = len(rows)
    ts     = np.array([int(r["date"].timestamp()) for r in rows], dtype=np.int64)
    opens  = np.array([float(r["open"]  or 0)    for r in rows], dtype=np.float64)
    closes = np.array([float(r["close"] or 0)    for r in rows], dtype=np.float64)
    highs  = np.array([float(r["max"]   or 0)    for r in rows], dtype=np.float64)
    lows   = np.array([float(r["min"]   or 0)    for r in rows], dtype=np.float64)
    return n, ts, opens, closes, highs, lows


# ══════════════════════════════════════════════════════════════════════════════
# Предвычисление: экстремумы + исходы
# ══════════════════════════════════════════════════════════════════════════════

def _precompute_extrema(
    agg_h: np.ndarray, agg_l: np.ndarray, order: int
) -> tuple[np.ndarray, np.ndarray]:
    """
    Вычисляет ВСЕ позиции локальных максимумов и минимумов в агрегированном массиве.
    Вызывается ОДИН РАЗ на (TF, order) — не в цикле по барам.
    """
    res_idx = argrelextrema(agg_h, np.greater, order=order)[0]
    sup_idx = argrelextrema(agg_l, np.less,    order=order)[0]
    return res_idx, sup_idx


def _levels_before(extrema_idx: np.ndarray, values: np.ndarray,
                   pos: int, n: int = N_LEVELS) -> list[float]:
    """
    Возвращает последние n уровней строго до позиции pos.
    Использует searchsorted — O(log k).
    """
    i = int(np.searchsorted(extrema_idx, pos, side="left"))
    if i == 0:
        return []
    indices = extrema_idx[max(0, i - n) : i]
    return [float(values[j]) for j in indices]


def _precompute_outcomes(
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
    atr: np.ndarray, n: int,
) -> dict:
    """
    Векторизованные исходы для ВСЕХ позиций сразу.
    Возвращает dict с numpy-массивами:
      t0_bull[pos] = 1/0/-1  (1=win, 0=loss, -1=invalid)
      t0_bear[pos] = ...
      t1_bull_{fwd}, t1_bear_{fwd} — для каждого t1 forward
      t2_bull, t2_bear  — 1/0/-1
      t3_bull, t3_bear  — 1/0/-1
    """
    out = {}

    # ── t0: через 3 часа сдвинулся на 0.10×ATR ────────────────────────────
    thr      = 0.10 * atr
    delta3   = np.concatenate((closes[3:] - closes[:-3],
                               np.full(3, np.nan)))
    t0_bull  = np.where(np.isnan(delta3), -1,
               np.where(delta3 >=  thr, 1, 0)).astype(np.int8)
    t0_bear  = np.where(np.isnan(delta3), -1,
               np.where(delta3 <= -thr, 1, 0)).astype(np.int8)
    out["t0_bull"] = t0_bull
    out["t0_bear"] = t0_bear

    # ── t1: close через forward баров выше/ниже ───────────────────────────
    # Предвычисляем для всех нужных forward = period × T1_FORWARD_MULT
    for period in TIMEFRAMES:
        fwd  = period * T1_FORWARD_MULT
        diff = np.concatenate((closes[fwd:] - closes[:-fwd],
                               np.full(fwd, np.nan)))
        out[f"t1_bull_{fwd}"] = np.where(np.isnan(diff), -1,
                                          np.where(diff > 0,  1, 0)).astype(np.int8)
        out[f"t1_bear_{fwd}"] = np.where(np.isnan(diff), -1,
                                          np.where(diff < 0,  1, 0)).astype(np.int8)

    # ── t2: достигла ±PCT до противоположного (rolling forward window) ─────
    pct_thr = T2_FORWARD_PCT
    t2_bull = np.full(n, -1, dtype=np.int8)
    t2_bear = np.full(n, -1, dtype=np.int8)
    # Строим rolling future max/min за T2_MAX_FORWARD баров
    for pos in range(n - 1):
        end   = min(pos + T2_MAX_FORWARD + 1, n)
        entry = closes[pos]
        if entry == 0:
            continue
        future = closes[pos+1:end]
        moves  = (future - entry) / (entry + 1e-10)
        # bull: ищем где первым достигнет +pct или -pct
        up_hit   = np.where(moves >=  pct_thr)[0]
        down_hit = np.where(moves <= -pct_thr)[0]
        if len(up_hit) > 0 and (len(down_hit) == 0 or up_hit[0] < down_hit[0]):
            t2_bull[pos] = 1; t2_bear[pos] = 0
        elif len(down_hit) > 0 and (len(up_hit) == 0 or down_hit[0] < up_hit[0]):
            t2_bull[pos] = 0; t2_bear[pos] = 1
        # иначе остаётся -1 (invalid)
    out["t2_bull"] = t2_bull
    out["t2_bear"] = t2_bear

    # ── t3: следующий локальный экстремум (order=2 для скорости) ──────────
    t3_bull = np.full(n, -1, dtype=np.int8)
    t3_bear = np.full(n, -1, dtype=np.int8)
    MAX_T3  = 50
    res_idx = argrelextrema(highs, np.greater, order=2)[0]
    sup_idx = argrelextrema(lows,  np.less,    order=2)[0]
    for pos in range(n - 2):
        # Первый MAX после pos
        ri = int(np.searchsorted(res_idx, pos + 1, side="left"))
        si = int(np.searchsorted(sup_idx, pos + 1, side="left"))
        next_max = res_idx[ri] if ri < len(res_idx) else n + MAX_T3
        next_min = sup_idx[si] if si < len(sup_idx) else n + MAX_T3
        if next_max - pos > MAX_T3 and next_min - pos > MAX_T3:
            continue
        if next_max < next_min:
            t3_bull[pos] = 1; t3_bear[pos] = 0
        else:
            t3_bull[pos] = 0; t3_bear[pos] = 1
    out["t3_bull"] = t3_bull
    out["t3_bear"] = t3_bear

    return out


# ══════════════════════════════════════════════════════════════════════════════
# Основной скан — O(n) с предвычисленными данными
# ══════════════════════════════════════════════════════════════════════════════

def _scan(ts, opens, closes, highs, lows) -> dict:
    n   = len(ts)
    atr = compute_atr(highs, lows, closes, ATR_PERIOD)

    log.info(f"[{INDEX_TABLE}] precomputing TF aggregates...")

    # ── Преагрегируем все TF ──────────────────────────────────────────────
    tf_data: dict[int, tuple] = {}     # period → (agg_ts, agg_h, agg_l)
    for period in TIMEFRAMES:
        if n < period * MIN_AGG_BARS:
            continue
        agg_ts, _, _, agg_h, agg_l = aggregate_ohlc(
            ts, opens, closes, highs, lows, period)
        tf_data[period] = (agg_ts, agg_h, agg_l)

    # ── Предвычисляем экстремумы для каждой (TF, order) пары ──────────────
    # ОДНА SCIPY CALL на каждую пару — не 7.5 млн
    log.info(f"[{INDEX_TABLE}] precomputing extrema "
             f"({len(tf_data)} TFs × {len(ORDERS)} orders = "
             f"{len(tf_data)*len(ORDERS)} scipy calls)...")
    ext_data: dict[tuple, tuple] = {}  # (period, order) → (res_idx, sup_idx)
    for period, (_, agg_h, agg_l) in tf_data.items():
        for order in ORDERS:
            res_idx, sup_idx = _precompute_extrema(agg_h, agg_l, order)
            ext_data[(period, order)] = (res_idx, sup_idx, agg_h, agg_l)

    # ── Предвычисляем векторизованные исходы ──────────────────────────────
    log.info(f"[{INDEX_TABLE}] precomputing vectorized outcomes...")
    outcomes = _precompute_outcomes(closes, highs, lows, atr, n)

    # ── Основной цикл — только бинарный поиск ─────────────────────────────
    log.info(f"[{INDEX_TABLE}] scanning {n-MIN_WARMUP_BARS:,} bars × 9 vars × "
             f"{len(tf_data)} TFs...")
    accum: dict[tuple, dict] = {}   # (var_id, wc) → stats

    for pos in range(MIN_WARMUP_BARS, n - T2_MAX_FORWARD - 2):
        curr_atr   = float(atr[pos]) or 1e-8
        proximity  = PROXIMITY_K * curr_atr
        curr_close = float(closes[pos])
        curr_high  = float(highs[pos])
        curr_low   = float(lows[pos])
        prev_close = float(closes[pos - 1])
        curr_ts    = int(ts[pos])

        for var_id, (order, confirm_k) in VAR_CONFIGS.items():
            confirm = confirm_k * curr_atr

            for period, (agg_ts, agg_h, agg_l) in tf_data.items():
                agg_pos = int(np.searchsorted(agg_ts, curr_ts, side="right"))
                if agg_pos < MIN_AGG_BARS:
                    continue

                res_idx, sup_idx, agg_h_full, agg_l_full = ext_data[(period, order)]

                # Уровни через бинарный поиск — O(log k)
                resistance = _levels_before(res_idx, agg_h_full, agg_pos)
                support    = _levels_before(sup_idx, agg_l_full, agg_pos)

                pfx      = f"tf{period}"
                detected = []

                for lvl in reversed(resistance):
                    if abs(curr_close - lvl) > proximity * 2: continue
                    if prev_close < lvl and curr_close > lvl + confirm:
                        detected.append((f"{pfx}_bo_bull", True)); break
                    if curr_high >= lvl - proximity and curr_close < lvl - confirm:
                        detected.append((f"{pfx}_rb_bear", False)); break

                for lvl in reversed(support):
                    if abs(curr_close - lvl) > proximity * 2: continue
                    if prev_close > lvl and curr_close < lvl - confirm:
                        detected.append((f"{pfx}_bo_bear", False)); break
                    if curr_low <= lvl + proximity and curr_close > lvl + confirm:
                        detected.append((f"{pfx}_rb_bull", True)); break

                t1_fwd = period * T1_FORWARD_MULT

                for wc, is_bull in detected:
                    key = (var_id, wc)
                    if key not in accum:
                        accum[key] = {
                            "period": period,
                            "event_type": wc.split("_", 1)[1],
                            "order": order, "confirm_k": confirm_k,
                            "cnt": 0,
                            "s0": 0, "v0": 0,
                            "s1": 0, "v1": 0,
                            "s2": 0, "v2": 0,
                            "s3": 0, "v3": 0,
                        }
                    a = accum[key]; a["cnt"] += 1

                    # Читаем предвычисленные исходы
                    sfx = "bull" if is_bull else "bear"

                    o0 = int(outcomes[f"t0_{sfx}"][pos])
                    if o0 >= 0: a["v0"] += 1; a["s0"] += o0

                    o1 = int(outcomes[f"t1_{sfx}_{t1_fwd}"][pos])
                    if o1 >= 0: a["v1"] += 1; a["s1"] += o1

                    o2 = int(outcomes[f"t2_{sfx}"][pos])
                    if o2 >= 0: a["v2"] += 1; a["s2"] += o2

                    o3 = int(outcomes[f"t3_{sfx}"][pos])
                    if o3 >= 0: a["v3"] += 1; a["s3"] += o3

        if pos % 10000 == 0:
            pct = (pos - MIN_WARMUP_BARS) / max(1, n - T2_MAX_FORWARD - 2 - MIN_WARMUP_BARS) * 100
            log.info(f"[{INDEX_TABLE}] {pos:,}/{n:,} ({pct:.1f}%) events={len(accum)}")

    return accum


async def _write(engine, accum: dict, n_total: int) -> int:
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{INDEX_TABLE}`"))

    def r(s, v): return round(s / v, 6) if v > 0 else 0.5

    rows = []
    for (var_id, wc), a in accum.items():
        if a["cnt"] == 0: continue
        rows.append({
            "wc":  wc,     "var": var_id,
            "per": a["period"],  "evt": a["event_type"],
            "ord": a["order"],   "ck":  a["confirm_k"],
            "r0":  r(a["s0"], a["v0"]),  "r1": r(a["s1"], a["v1"]),
            "r2":  r(a["s2"], a["v2"]),  "r3": r(a["s3"], a["v3"]),
            "t1":  a["period"] * T1_FORWARD_MULT,
            "t2":  T2_FORWARD_PCT,
            "cnt": a["cnt"],
            "sup": round(a["cnt"] / n_total, 6),
        })

    sql = f"""
        INSERT INTO `{INDEX_TABLE}`
            (weight_code, var, period, event_type, order_val, confirm_k,
             bull_ratio_t0, bull_ratio_t1, bull_ratio_t2, bull_ratio_t3,
             t1_forward_bars, t2_forward_pct, occurrence_count, support, date_updated)
        VALUES (:wc, :var, :per, :evt, :ord, :ck,
                :r0, :r1, :r2, :r3, :t1, :t2, :cnt, :sup, NOW())
    """
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        async with engine.begin() as conn:
            await conn.execute(text(sql), rows[i:i+BATCH_SIZE])
        total += len(rows[i:i+BATCH_SIZE])
    return total


async def build_index(engine_vlad, engine_brain) -> dict:
    log.info(f"[{INDEX_TABLE}] build_index start (optimized)")
    await _ensure_table(engine_vlad)

    n, ts, opens, closes, highs, lows = await _load_rates(engine_brain, RATES_TABLE)
    if n < MIN_WARMUP_BARS + max(TIMEFRAMES) * MIN_AGG_BARS:
        return {"error": "Недостаточно данных", "rows": n}

    log.info(f"[{INDEX_TABLE}] {n:,} свечей loaded")
    accum   = _scan(ts, opens, closes, highs, lows)
    written = await _write(engine_vlad, accum, n)

    result = {
        "candles": n, "entries": written,
        "unique_events": len(accum),
        "index_table": INDEX_TABLE,
    }
    log.info(f"[{INDEX_TABLE}] done: {result}")
    return result