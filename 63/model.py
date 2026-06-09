"""
model.py — sasha_fred_vixcls (CBOE Volatility Index: VIX)
"""
from __future__ import annotations

from datetime import datetime

from brain_framework import get_service_config, run_standard_model


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config()
    parser_table = cfg["dataset"]["parser_table"]
    enriched_table = cfg["dataset"]["enriched_table"]

    async with engine_brain.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT date_iso, value
            FROM `{parser_table}`
            WHERE value IS NOT NULL
              AND date_iso IS NOT NULL
              AND date_iso >= '1971-01-01'
            ORDER BY date_iso
        """))
        source = res.fetchall()

    pcts = []
    draft_rows = []
    prev = None
    for date_iso, raw in source:
        v = float(raw)
        dt = datetime.combine(date_iso, datetime.min.time()) if not isinstance(date_iso, datetime) else date_iso
        if prev is not None and prev != 0:
            pct = ((v - prev) / prev) * 100.0
            pcts.append(pct)
            draft_rows.append({
                "date_dt": dt,
                "value": v,
                "pct_change": pct,
            })
        prev = v
    thresholds = _build_thresholds(pcts)
    rows = [
        {
            "date_dt": r["date_dt"],
            "value": r["value"],
            "pct_change": r["pct_change"],
            "event_type": _classify(r["pct_change"], thresholds),
        }
        for r in draft_rows
    ]

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`         BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`    DATETIME    NOT NULL,
                `value`      DOUBLE      NOT NULL,
                `pct_change` DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type` VARCHAR(32) NOT NULL,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 500):
            await conn.execute(text(f"""
                INSERT INTO `{enriched_table}`
                    (date_dt, value, pct_change, event_type)
                VALUES
                    (:date_dt, :value, :pct_change, :event_type)
            """), rows[i: i + 500])

    return {
        "source_rows": len(source),
        "enriched_rows": len(rows),
        "thresholds": thresholds,
    }


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return float(sorted_values[0])
    if p >= 1:
        return float(sorted_values[-1])
    idx = (len(sorted_values) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _build_thresholds(pcts: list[float]) -> dict:
    abs_vals = sorted(abs(x) for x in pcts if x != 0.0)
    pos_vals = sorted(x for x in pcts if x > 0.0)
    neg_abs_vals = sorted(abs(x) for x in pcts if x < 0.0)

    up = _percentile(pos_vals or abs_vals, 0.60)
    spike_up = _percentile(pos_vals or abs_vals, 0.90)
    down_abs = _percentile(neg_abs_vals or abs_vals, 0.60)
    spike_down_abs = _percentile(neg_abs_vals or abs_vals, 0.90)

    if spike_up < up:
        spike_up = up
    if spike_down_abs < down_abs:
        spike_down_abs = down_abs

    return {
        "up": up,
        "spike_up": spike_up,
        "down": -down_abs,
        "spike_down": -spike_down_abs,
    }


def _classify(pct: float, thresholds: dict) -> str:
    if pct >= float(thresholds["spike_up"]):
        return "vix_spike_up"
    if pct >= float(thresholds["up"]):
        return "vix_up"
    if pct <= float(thresholds["spike_down"]):
        return "vix_spike_down"
    if pct <= float(thresholds["down"]):
        return "vix_down"
    return "vix_flat"


def _apply_var(signed_t1: float, pct: float, var: int, ctx_info: dict) -> float:
    avg = float(ctx_info.get("avg_abs_pct_change") or 0.0)
    if var == 0:
        return signed_t1
    if var == 1:
        return signed_t1 if avg > 0 and abs(pct) >= avg else 0.0
    if var == 2:
        base = avg if avg > 0 else abs(pct)
        return (signed_t1 * min(abs(pct) / base, 3.0)) if base > 0 else 0.0
    if var == 3:
        return signed_t1 if pct > 0 else 0.0
    return 0.0


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    cfg = get_service_config()
    return run_standard_model(
        rates,
        dataset,
        date,
        type=type,
        var=var,
        dataset_index=dataset_index,
        shift_window=cfg["cache"]["shift_window"],
        apply_var_fn=_apply_var,
    )
