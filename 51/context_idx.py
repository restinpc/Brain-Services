"""
context_idx.py — build_index для brain-trend-combo

Строит индекс для всех var-вариантов (0..3) в одной таблице.
weight_code = f"{var}_{prefix}_{dir_bits}"

var → размер окна для поиска экстремумов:
    0 → window=1  (adjacent-bar)
    1 → window=3
    2 → window=5
    3 → window=7
"""
from __future__ import annotations

import logging
from datetime import datetime

import numpy as np
from sqlalchemy import text

log = logging.getLogger("brain-framework")

import importlib.util as _ilu
import os as _os

def _import_model():
    here = _os.path.dirname(_os.path.abspath(__file__))
    spec = _ilu.spec_from_file_location("model", _os.path.join(here, "model.py"))
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_mdl = _import_model()

aggregate_ohlc   = _mdl.aggregate_ohlc
find_all_extrema = _mdl.find_all_extrema
trend_direction  = _mdl.trend_direction
TIMEFRAME_DEFS   = _mdl.TIMEFRAME_DEFS
N_TF             = _mdl.N_TF
N_ADD_TF         = _mdl.N_ADD_TF
MAX_PREFIX       = _mdl.MAX_PREFIX
MIN_CANDLES      = _mdl.MIN_CANDLES
SERVICE_ID       = _mdl.SERVICE_ID
RATES_TABLE      = _mdl.RATES_TABLE
EXTREMA_WINDOWS  = _mdl.EXTREMA_WINDOWS
VAR_RANGE        = _mdl.VAR_RANGE

INDEX_TABLE  = f"vlad_trend_combo_svc{SERVICE_ID}_index"
WARMUP_HOURS = 365 * 24
BATCH_SIZE   = 500


async def _ensure_index_table(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{INDEX_TABLE}` (
                `id`               INT               NOT NULL AUTO_INCREMENT,
                `weight_code`      VARCHAR(32)       NOT NULL,
                `var_id`           TINYINT UNSIGNED  NOT NULL DEFAULT 0,
                `combo_prefix`     SMALLINT UNSIGNED NOT NULL,
                `direction_bits`   VARCHAR(12)       NOT NULL,
                `occurrence_count` INT               NOT NULL DEFAULT 0,
                `bull_count`       INT               NOT NULL DEFAULT 0,
                `bull_ratio`       FLOAT             NOT NULL DEFAULT 0.5,
                `avg_t1`           FLOAT             NOT NULL DEFAULT 0.0,
                `date_added`       DATETIME          NULL,
                `date_updated`     DATETIME          NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_wc`      (`weight_code`),
                INDEX `idx_var_prefix`  (`var_id`, `combo_prefix`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def _load_rates(engine, table: str) -> list[dict]:
    async with engine.connect() as conn:
        res = await conn.execute(text(
            f"SELECT date, open, close, `max`, `min` FROM `{table}` ORDER BY date"
        ))
        return [
            {"date": r["date"], "open": float(r["open"] or 0), "close": float(r["close"] or 0),
             "max": float(r["max"] or 0), "min": float(r["min"] or 0)}
            for r in res.mappings().all()
        ]


async def _upsert_batch(engine, rows: list[dict]) -> None:
    if not rows:
        return
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{INDEX_TABLE}`
                (weight_code, var_id, combo_prefix, direction_bits,
                 occurrence_count, bull_count, bull_ratio, avg_t1,
                 date_added, date_updated)
            VALUES
                (:wc, :var_id, :prefix, :dir_bits,
                 :cnt, :bull, :br, :avg_t1,
                 NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                occurrence_count = VALUES(occurrence_count),
                bull_count       = VALUES(bull_count),
                bull_ratio       = VALUES(bull_ratio),
                avg_t1           = VALUES(avg_t1),
                date_updated     = NOW()
        """), rows)


def _compute_tf_prebuilds(ts, opens, closes, highs, lows, window: int) -> list[dict]:
    result = []
    ue, le = find_all_extrema(highs, lows, window)
    result.append({"ts": ts, "highs": highs, "lows": lows, "upper_ext": ue, "lower_ext": le})
    for _, period_hours in TIMEFRAME_DEFS[1:]:
        agg_ts, _, _, agg_h, agg_l = aggregate_ohlc(ts, opens, closes, highs, lows, period_hours)
        ue_a, le_a = find_all_extrema(agg_h, agg_l, window)
        result.append({"ts": agg_ts, "highs": agg_h, "lows": agg_l, "upper_ext": ue_a, "lower_ext": le_a})
    return result


def _directions_at_pos(prebuilds: list[dict], hour_pos: int) -> list[int | None]:
    directions: list[int | None] = []
    cur_ts = prebuilds[0]["ts"][hour_pos]
    for tf_idx, tf_data in enumerate(prebuilds):
        agg_ts = tf_data["ts"]
        pos = hour_pos if tf_idx == 0 else int(np.searchsorted(agg_ts, cur_ts, side="left"))
        if pos < MIN_CANDLES:
            directions.append(None)
            continue
        d = trend_direction(agg_ts, tf_data["highs"], tf_data["lows"],
                            tf_data["upper_ext"], tf_data["lower_ext"], pos)
        directions.append(d)
    return directions


async def build_index(engine_vlad, engine_brain) -> dict:
    log.info(f"[{INDEX_TABLE}] build_index start, var_range={VAR_RANGE}")

    await _ensure_index_table(engine_vlad)

    raw = await _load_rates(engine_brain, RATES_TABLE)
    if len(raw) < WARMUP_HOURS + MIN_CANDLES:
        return {"error": "Недостаточно данных", "rows": len(raw)}

    n      = len(raw)
    ts     = np.array([int(r["date"].timestamp()) for r in raw], dtype=np.int64)
    opens  = np.array([r["open"]  for r in raw], dtype=np.float64)
    closes = np.array([r["close"] for r in raw], dtype=np.float64)
    highs  = np.array([r["max"]   for r in raw], dtype=np.float64)
    lows   = np.array([r["min"]   for r in raw], dtype=np.float64)

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{INDEX_TABLE}`"))

    total_written = 0
    stats_per_var = {}

    for var_id in VAR_RANGE:
        window = EXTREMA_WINDOWS[var_id]
        log.info(f"[{INDEX_TABLE}] var={var_id} window={window}: building prebuilds...")

        prebuilds = _compute_tf_prebuilds(ts, opens, closes, highs, lows, window)
        log.info(f"[{INDEX_TABLE}] var={var_id}: prebuilds done")

        accum: dict[str, list] = {}
        processed = 0

        for hour_pos in range(WARMUP_HOURS, n - 1):
            directions = _directions_at_pos(prebuilds, hour_pos)
            if directions[0] is None:
                continue

            next_bull = 1 if closes[hour_pos + 1] > opens[hour_pos + 1] else 0
            next_t1   = float(closes[hour_pos + 1] - opens[hour_pos + 1])

            for prefix in range(MAX_PREFIX):
                active = [0] + [bit + 1 for bit in range(N_ADD_TF) if prefix & (1 << bit)]
                if any(directions[i] is None for i in active):
                    continue
                dir_bits    = "".join(str(directions[i]) for i in active)
                weight_code = f"{var_id}_{prefix}_{dir_bits}"

                if weight_code not in accum:
                    accum[weight_code] = [var_id, prefix, dir_bits, 0, 0, 0.0]
                entry = accum[weight_code]
                entry[3] += 1
                entry[4] += next_bull
                entry[5] += next_t1

            processed += 1
            if processed % 10000 == 0:
                pct = processed / (n - WARMUP_HOURS - 1) * 100
                log.info(f"[{INDEX_TABLE}] var={var_id}: {processed:,}/{n-WARMUP_HOURS-1:,} ({pct:.0f}%)")

        rows = []
        for wc, (vid, pfx, dbits, cnt, bull, sum_t1) in accum.items():
            if cnt == 0:
                continue
            rows.append({
                "wc": wc, "var_id": int(vid), "prefix": int(pfx), "dir_bits": dbits,
                "cnt": cnt, "bull": bull,
                "br": round(bull / cnt, 6),
                "avg_t1": round(sum_t1 / cnt, 6),
            })

        written = 0
        for i in range(0, len(rows), BATCH_SIZE):
            await _upsert_batch(engine_vlad, rows[i : i + BATCH_SIZE])
            written += len(rows[i : i + BATCH_SIZE])

        stats_per_var[var_id] = {"candles": processed, "entries": written, "window": window}
        total_written += written
        log.info(f"[{INDEX_TABLE}] var={var_id} done: {processed:,} candles, {written:,} entries")

    log.info(f"[{INDEX_TABLE}] ALL DONE: {total_written:,} total entries")
    return {
        "total_entries": total_written,
        "vars":          stats_per_var,
        "warmup_hours":  WARMUP_HOURS,
        "rates_table":   RATES_TABLE,
        "index_table":   INDEX_TABLE,
    }
