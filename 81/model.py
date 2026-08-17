"""
model.py - sasha_us_treasury_debt_to_penny (U.S. Treasury debt fiscal regime model)
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


def _build_thresholds(
    debt_pcts: list[float],
    share_deltas: list[float],
    daily_changes_bn: list[float],
) -> dict:
    abs_pcts = sorted(abs(x) for x in debt_pcts if x != 0.0)
    pos_pcts = sorted(x for x in debt_pcts if x > 0.0)
    neg_abs_pcts = sorted(abs(x) for x in debt_pcts if x < 0.0)
    abs_share = sorted(abs(x) for x in share_deltas if x != 0.0)
    abs_bn = sorted(abs(x) for x in daily_changes_bn if x != 0.0)

    up = max(_percentile(pos_pcts or abs_pcts, 0.60), 0.001)
    surge = max(_percentile(pos_pcts or abs_pcts, 0.90), up)
    down_abs = max(_percentile(neg_abs_pcts or abs_pcts, 0.60), 0.001)
    relief_abs = max(_percentile(neg_abs_pcts or abs_pcts, 0.90), down_abs)
    share_shift = max(_percentile(abs_share, 0.75), 0.00005)
    bn_surge = max(_percentile(abs_bn, 0.85), 1.0)

    return {
        "up": up,
        "surge": surge,
        "down": -down_abs,
        "relief": -relief_abs,
        "share_shift": share_shift,
        "bn_surge": bn_surge,
    }


def _classify(row: dict, thresholds: dict) -> str:
    debt_pct = float(row["debt_pct_change"])
    public_delta = float(row["public_share_delta"])
    intragov_delta = float(row["intragov_share_delta"])
    daily_bn = float(row["daily_change_bn"])

    up = float(thresholds["up"])
    surge = float(thresholds["surge"])
    down = float(thresholds["down"])
    relief = float(thresholds["relief"])
    share_shift = float(thresholds["share_shift"])
    bn_surge = float(thresholds["bn_surge"])

    if debt_pct >= surge and daily_bn >= bn_surge:
        return "us_debt_surge_accel"
    if debt_pct >= surge:
        return "us_debt_surge"
    if debt_pct >= up:
        if public_delta >= share_shift and public_delta > intragov_delta:
            return "us_debt_public_drift"
        if intragov_delta >= share_shift and intragov_delta > public_delta:
            return "us_debt_intragov_drift"
        return "us_debt_buildup"
    if debt_pct <= relief:
        return "us_debt_relief_spike"
    if debt_pct <= down:
        return "us_debt_relief"
    return "us_debt_stable"


async def _load_source(engine_vlad, engine_brain, parser_table: str) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT
            date_iso,
            tot_pub_debt_out_amt,
            tot_pub_debt_out_amt_held_by_pub,
            govt_account_invest_hold_amt
        FROM `{parser_table}`
        WHERE date_iso IS NOT NULL
          AND tot_pub_debt_out_amt IS NOT NULL
        ORDER BY date_iso
    """)

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

    source = await _load_source(engine_vlad, engine_brain, parser_table)

    draft_rows = []
    debt_pcts = []
    share_deltas = []
    daily_changes_bn = []
    prev_total = None
    prev_public_share = None
    prev_intragov_share = None
    prev_score = None

    for date_iso, total_raw, held_public_raw, intragov_raw in source:
        dt = (
            date_iso.replace(hour=0, minute=0, second=0, microsecond=0)
            if isinstance(date_iso, datetime)
            else datetime.combine(date_iso, datetime.min.time())
        )

        total = float(total_raw)
        held_public = float(held_public_raw or 0.0)
        intragov = float(intragov_raw or 0.0)

        public_share = held_public / total if total > 0.0 else 0.0
        intragov_share = intragov / total if total > 0.0 else 0.0
        total_debt_trillions = total / 1e12

        if prev_total is None:
            debt_pct_change = 0.0
            daily_change_bn = 0.0
            public_share_delta = 0.0
            intragov_share_delta = 0.0
        else:
            debt_pct_change = ((total - prev_total) / prev_total) * 100.0 if prev_total != 0.0 else 0.0
            daily_change_bn = (total - prev_total) / 1e9
            public_share_delta = public_share - prev_public_share
            intragov_share_delta = intragov_share - prev_intragov_share

        # Composite fiscal-stress score: debt growth + composition shift.
        score = (
            abs(debt_pct_change) * 1000.0
            + abs(public_share_delta) * 500.0
            + abs(intragov_share_delta) * 500.0
        )

        if prev_score is None:
            pct_change = 0.0
        elif prev_score != 0.0:
            pct_change = ((score - prev_score) / prev_score) * 100.0
        else:
            pct_change = (score - prev_score) * 100.0

        draft_rows.append(
            {
                "date_dt": dt,
                "value": score,
                "pct_change": pct_change,
                "total_debt_trillions": total_debt_trillions,
                "debt_pct_change": debt_pct_change,
                "daily_change_bn": daily_change_bn,
                "public_share": public_share,
                "intragov_share": intragov_share,
                "public_share_delta": public_share_delta,
                "intragov_share_delta": intragov_share_delta,
            }
        )
        debt_pcts.append(debt_pct_change)
        share_deltas.extend([public_share_delta, intragov_share_delta])
        daily_changes_bn.append(daily_change_bn)

        prev_total = total
        prev_public_share = public_share
        prev_intragov_share = intragov_share
        prev_score = score

    thresholds = _build_thresholds(debt_pcts, share_deltas, daily_changes_bn)
    rows = [{**r, "event_type": _classify(r, thresholds)} for r in draft_rows]

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`                   BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`              DATETIME    NOT NULL,
                `value`                DOUBLE      NOT NULL,
                `pct_change`           DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`           VARCHAR(64) NOT NULL,
                `total_debt_trillions` DOUBLE      NOT NULL DEFAULT 0.0,
                `debt_pct_change`      DOUBLE      NOT NULL DEFAULT 0.0,
                `daily_change_bn`      DOUBLE      NOT NULL DEFAULT 0.0,
                `public_share`         DOUBLE      NOT NULL DEFAULT 0.0,
                `intragov_share`       DOUBLE      NOT NULL DEFAULT 0.0,
                `public_share_delta`   DOUBLE      NOT NULL DEFAULT 0.0,
                `intragov_share_delta` DOUBLE      NOT NULL DEFAULT 0.0,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 500):
            await conn.execute(
                text(f"""
                    INSERT INTO `{enriched_table}`
                        (date_dt, value, pct_change, event_type, total_debt_trillions,
                         debt_pct_change, daily_change_bn, public_share, intragov_share,
                         public_share_delta, intragov_share_delta)
                    VALUES
                        (:date_dt, :value, :pct_change, :event_type, :total_debt_trillions,
                         :debt_pct_change, :daily_change_bn, :public_share, :intragov_share,
                         :public_share_delta, :intragov_share_delta)
                """),
                rows[i : i + 500],
            )

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
