"""
model.py - vlad_fear_greed_index (Crypto Fear & Greed sentiment model)
"""
from __future__ import annotations

from datetime import datetime

from brain_framework import get_service_config, run_standard_model


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

    up = max(_percentile(pos_vals or abs_vals, 0.60), 0.8)
    shock_up = max(_percentile(pos_vals or abs_vals, 0.90), up)
    down_abs = max(_percentile(neg_abs_vals or abs_vals, 0.60), 0.8)
    shock_down_abs = max(_percentile(neg_abs_vals or abs_vals, 0.90), down_abs)
    calm = max(_percentile(abs_vals, 0.35), 0.2)

    return {
        "up": up,
        "shock_up": shock_up,
        "down": -down_abs,
        "shock_down": -shock_down_abs,
        "calm": calm,
    }


def _classify(row: dict, thresholds: dict) -> str:
    idx = float(row["raw_index"])
    pct = float(row["pct_change"])
    fear_gap = float(row["fear_gap"])
    greed_gap = float(row["greed_gap"])

    if idx <= 20 and pct <= float(thresholds["shock_down"]):
        return "fear_capitulation"
    if idx <= 25 and pct < 0:
        return "fear_deepening"
    if idx >= 80 and pct >= float(thresholds["shock_up"]):
        return "greed_euphoria_spike"
    if idx >= 75 and pct > 0:
        return "greed_expansion"
    if 45 <= idx <= 55 and abs(pct) <= float(thresholds["calm"]):
        return "neutral_balance"
    if pct >= float(thresholds["up"]) and greed_gap > fear_gap:
        return "sentiment_risk_on"
    if pct <= float(thresholds["down"]) and fear_gap >= greed_gap:
        return "sentiment_risk_off"
    return "sentiment_transition"


async def _load_source_rows(engine_vlad, engine_brain, parser_table: str) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT
            record_date,
            value,
            classification
        FROM `{parser_table}`
        WHERE record_date IS NOT NULL
          AND value IS NOT NULL
        ORDER BY record_date
    """)

    # Parser table is expected in vlad DB; fallback keeps deploy safer.
    for engine in (engine_vlad, engine_brain):
        try:
            async with engine.connect() as conn:
                res = await conn.execute(query)
                rows = res.fetchall()
                if rows:
                    return rows
        except Exception:
            continue

    return []


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config()
    parser_table = cfg["dataset"]["parser_table"]
    enriched_table = cfg["dataset"]["enriched_table"]

    source = await _load_source_rows(engine_vlad, engine_brain, parser_table)

    draft_rows = []
    pcts = []
    prev = None

    for record_date, raw_value, classification in source:
        idx = float(raw_value)
        if isinstance(record_date, datetime):
            dt = record_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            dt = datetime.combine(record_date, datetime.min.time())

        fear_gap = max(50.0 - idx, 0.0)
        greed_gap = max(idx - 50.0, 0.0)

        if prev is None:
            pct = 0.0
        elif prev != 0:
            pct = ((idx - prev) / prev) * 100.0
        else:
            pct = (idx - prev) * 100.0

        draft_rows.append(
            {
                "date_dt": dt,
                "value": idx,
                "raw_index": idx,
                "pct_change": pct,
                "fear_gap": fear_gap,
                "greed_gap": greed_gap,
                "classification_raw": str(classification or ""),
            }
        )
        pcts.append(pct)
        prev = idx

    thresholds = _build_thresholds(pcts)
    rows = [{**r, "event_type": _classify(r, thresholds)} for r in draft_rows]

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`                 BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`            DATETIME    NOT NULL,
                `value`              DOUBLE      NOT NULL,
                `raw_index`          DOUBLE      NOT NULL DEFAULT 0.0,
                `pct_change`         DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`         VARCHAR(64) NOT NULL,
                `fear_gap`           DOUBLE      NOT NULL DEFAULT 0.0,
                `greed_gap`          DOUBLE      NOT NULL DEFAULT 0.0,
                `classification_raw` VARCHAR(64) NOT NULL DEFAULT '',
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 500):
            await conn.execute(text(f"""
                INSERT INTO `{enriched_table}`
                    (date_dt, value, raw_index, pct_change, event_type, fear_gap, greed_gap, classification_raw)
                VALUES
                    (:date_dt, :value, :raw_index, :pct_change, :event_type, :fear_gap, :greed_gap, :classification_raw)
            """), rows[i: i + 500])

    return {
        "source_rows": len(source),
        "enriched_rows": len(rows),
        "thresholds": thresholds,
    }


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
