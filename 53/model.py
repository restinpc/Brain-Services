"""
model.py — sasha_fred_dgs10 (10-Year Treasury Yield)
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

    rows = []
    prev = None
    for date_iso, raw in source:
        v = float(raw)
        dt = datetime.combine(date_iso, datetime.min.time()) if not isinstance(date_iso, datetime) else date_iso
        if prev is not None and prev != 0:
            pct = ((v - prev) / prev) * 100.0
            rows.append({
                "date_dt": dt,
                "value": v,
                "pct_change": pct,
                "event_type": _classify(pct),
            })
        prev = v

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

    return {"source_rows": len(source), "enriched_rows": len(rows)}


def _classify(pct: float) -> str:
    # Для доходности UST 10Y используем более "узкие" пороги, чем для BTC.
    if pct >= 1.0:
        return "yield_spike_up"
    if pct >= 0.2:
        return "yield_up"
    if pct <= -1.0:
        return "yield_spike_down"
    if pct <= -0.2:
        return "yield_down"
    return "yield_flat"


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
