"""
model.py — vlad_coingecko_stablecoin_history (stablecoin depeg monitor)
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


def _build_thresholds(values: list[float], pcts: list[float]) -> dict:
    sorted_values = sorted(v for v in values if v >= 0.0)
    positive_pcts = sorted(x for x in pcts if x > 0.0)
    negative_abs_pcts = sorted(abs(x) for x in pcts if x < 0.0)

    baseline = max(_percentile(sorted_values, 0.60), 0.01)
    high = max(_percentile(sorted_values, 0.85), baseline * 1.5, 0.03)
    extreme = max(_percentile(sorted_values, 0.97), high * 1.4, 0.08)
    buildup = max(_percentile(positive_pcts, 0.70), 5.0)
    relief = max(_percentile(negative_abs_pcts, 0.70), 5.0)

    return {
        "baseline": baseline,
        "high": high,
        "extreme": extreme,
        "buildup": buildup,
        "relief": relief,
    }


def _classify(row: dict, thresholds: dict) -> str:
    max_abs_dev_pct = float(row["max_abs_dev_pct"])
    stress_share = float(row["stress_share"])
    stressed_count = int(row["stressed_count"])
    pct_change = float(row["pct_change"])

    if stress_share >= 0.35 or max_abs_dev_pct >= float(thresholds["extreme"]):
        if pct_change >= float(thresholds["buildup"]):
            return "depeg_systemic_accel"
        return "depeg_systemic"

    if max_abs_dev_pct >= float(thresholds["high"]) or stressed_count >= 2:
        return "depeg_broad"

    if max_abs_dev_pct >= float(thresholds["baseline"]):
        return "depeg_local"

    if pct_change <= -float(thresholds["relief"]) and max_abs_dev_pct < float(thresholds["baseline"]):
        return "repeg_relief"

    return "peg_stable"


async def _load_source_rows(engine_vlad, engine_brain, parser_table: str) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT
            record_date,
            symbol,
            price,
            peg_deviation,
            market_cap,
            volume_24h
        FROM `{parser_table}`
        WHERE record_date IS NOT NULL
          AND price IS NOT NULL
          AND peg_deviation IS NOT NULL
        ORDER BY record_date, symbol
    """)

    # Parser table name is vlad_*, but keeping fallback makes deploy safer.
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

    by_date: dict[datetime, dict] = {}
    for record_date, symbol, price, peg_deviation, market_cap, volume_24h in source:
        del symbol, price

        if isinstance(record_date, datetime):
            dt = record_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            dt = datetime.combine(record_date, datetime.min.time())

        abs_dev = abs(float(peg_deviation or 0.0))
        mc = float(market_cap or 0.0)
        vol = float(volume_24h or 0.0)

        item = by_date.setdefault(
            dt,
            {
                "coin_count": 0,
                "stressed_count": 0,
                "max_abs_dev": 0.0,
                "sum_abs_dev": 0.0,
                "total_mc": 0.0,
                "stressed_mc": 0.0,
                "total_vol": 0.0,
            },
        )
        item["coin_count"] += 1
        item["sum_abs_dev"] += abs_dev
        item["max_abs_dev"] = max(item["max_abs_dev"], abs_dev)
        item["total_vol"] += max(vol, 0.0)
        if mc > 0.0:
            item["total_mc"] += mc
        if abs_dev >= 0.001:  # 10 bps depeg threshold.
            item["stressed_count"] += 1
            if mc > 0.0:
                item["stressed_mc"] += mc

    ordered_dates = sorted(by_date.keys())
    draft_rows = []
    signal_values = []
    pcts = []
    prev_value = None

    for dt in ordered_dates:
        item = by_date[dt]
        coin_count = max(int(item["coin_count"]), 1)
        max_abs_dev_pct = float(item["max_abs_dev"]) * 100.0
        mean_abs_dev_pct = (float(item["sum_abs_dev"]) / coin_count) * 100.0
        stress_share = (float(item["stressed_mc"]) / float(item["total_mc"])) if float(item["total_mc"]) > 0.0 else 0.0
        liq_ratio = (float(item["total_vol"]) / float(item["total_mc"])) if float(item["total_mc"]) > 0.0 else 0.0

        # Composite stress score (percentage points): max depeg + systemic pressure.
        value = max_abs_dev_pct + (stress_share * 0.25) + (min(item["stressed_count"], 5) * 0.05)

        if prev_value is None:
            pct_change = 0.0
        elif prev_value > 0.0:
            pct_change = ((value - prev_value) / prev_value) * 100.0
        else:
            pct_change = (value - prev_value) * 100.0

        draft_rows.append(
            {
                "date_dt": dt,
                "value": value,
                "pct_change": pct_change,
                "max_abs_dev_pct": max_abs_dev_pct,
                "mean_abs_dev_pct": mean_abs_dev_pct,
                "stress_share": stress_share,
                "liq_ratio": liq_ratio,
                "coin_count": coin_count,
                "stressed_count": int(item["stressed_count"]),
            }
        )
        signal_values.append(value)
        pcts.append(pct_change)
        prev_value = value

    thresholds = _build_thresholds(signal_values, pcts)
    rows = []
    for row in draft_rows:
        rows.append(
            {
                **row,
                "event_type": _classify(row, thresholds),
            }
        )

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`               BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`          DATETIME    NOT NULL,
                `value`            DOUBLE      NOT NULL,
                `pct_change`       DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`       VARCHAR(64) NOT NULL,
                `max_abs_dev_pct`  DOUBLE      NOT NULL DEFAULT 0.0,
                `mean_abs_dev_pct` DOUBLE      NOT NULL DEFAULT 0.0,
                `stress_share`     DOUBLE      NOT NULL DEFAULT 0.0,
                `liq_ratio`        DOUBLE      NOT NULL DEFAULT 0.0,
                `coin_count`       INT         NOT NULL DEFAULT 0,
                `stressed_count`   INT         NOT NULL DEFAULT 0,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 500):
            await conn.execute(text(f"""
                INSERT INTO `{enriched_table}`
                    (date_dt, value, pct_change, event_type, max_abs_dev_pct, mean_abs_dev_pct,
                     stress_share, liq_ratio, coin_count, stressed_count)
                VALUES
                    (:date_dt, :value, :pct_change, :event_type, :max_abs_dev_pct, :mean_abs_dev_pct,
                     :stress_share, :liq_ratio, :coin_count, :stressed_count)
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
