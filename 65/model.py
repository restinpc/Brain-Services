"""
model.py — vlad_treasury_nominal_yield (US Treasury curve regime model)
"""
from __future__ import annotations

import re
from datetime import datetime

from brain_framework import get_service_config, run_standard_model

_MATURITY_RE = re.compile(r"(\d+)\s*(MONTH|YEAR)", re.IGNORECASE)


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


def _maturity_years(column_name: str) -> float | None:
    token = str(column_name).upper().replace("_", "")
    match = _MATURITY_RE.search(token)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).upper()
    return amount / 12.0 if unit == "MONTH" else amount


def _pick_curve_columns(columns: list[str]) -> list[tuple[str, float]]:
    picked: list[tuple[str, float]] = []
    for col in columns:
        if col.lower() == "record_date":
            continue
        years = _maturity_years(col)
        if years is None:
            continue
        picked.append((col, years))
    picked.sort(key=lambda x: x[1])
    return picked


def _avg(nums: list[float]) -> float:
    return float(sum(nums) / len(nums)) if nums else 0.0


def _build_thresholds(values: list[float], pcts: list[float]) -> dict:
    abs_values = sorted(abs(v) for v in values)
    abs_pcts = sorted(abs(x) for x in pcts if x != 0.0)
    pos_pcts = sorted(x for x in pcts if x > 0.0)
    neg_abs_pcts = sorted(abs(x) for x in pcts if x < 0.0)

    regime = max(_percentile(abs_values, 0.65), 0.20)
    stress = max(_percentile(abs_values, 0.85), regime * 1.35)
    shock = max(_percentile(abs_values, 0.97), stress * 1.30)
    accel_up = max(_percentile(pos_pcts or abs_pcts, 0.70), 2.0)
    accel_down = max(_percentile(neg_abs_pcts or abs_pcts, 0.70), 2.0)

    return {
        "regime": regime,
        "stress": stress,
        "shock": shock,
        "accel_up": accel_up,
        "accel_down": accel_down,
    }


def _classify(row: dict, thresholds: dict) -> str:
    value = abs(float(row["value"]))
    pct = float(row["pct_change"])
    slope_10y_2y = float(row["slope_10y_2y"])
    inversion_gap = float(row["inversion_gap"])
    curvature = float(row["curvature"])

    if inversion_gap >= 1.0 and value >= float(thresholds["shock"]):
        return "curve_inversion_shock"
    if inversion_gap >= 0.25 and value >= float(thresholds["stress"]):
        return "curve_inversion_stress"
    if value >= float(thresholds["stress"]) and pct >= float(thresholds["accel_up"]):
        return "curve_stress_accel"
    if value >= float(thresholds["stress"]) and pct <= -float(thresholds["accel_down"]):
        return "curve_stress_relief"
    if value >= float(thresholds["regime"]) and slope_10y_2y > 0.25:
        return "curve_steepening"
    if value >= float(thresholds["regime"]) and slope_10y_2y < -0.25:
        return "curve_flattening"
    if abs(curvature) >= 0.50:
        return "curve_belly_shift"
    return "curve_neutral"


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config()
    parser_table = cfg["dataset"]["parser_table"]
    enriched_table = cfg["dataset"]["enriched_table"]

    async with engine_vlad.connect() as conn:
        cols_res = await conn.execute(text(f"SHOW COLUMNS FROM `{parser_table}`"))
        table_columns = [str(r[0]) for r in cols_res.fetchall()]
        curve_cols = _pick_curve_columns(table_columns)

        data_res = await conn.execute(text(f"""
            SELECT *
            FROM `{parser_table}`
            WHERE record_date IS NOT NULL
            ORDER BY record_date
        """))
        source = data_res.mappings().all()

    short_cols = [c for c, y in curve_cols if y <= 1.0]
    mid_cols = [c for c, y in curve_cols if 1.0 < y < 10.0]
    long_cols = [c for c, y in curve_cols if y >= 10.0]

    benchmark_2y = next((c for c, y in curve_cols if abs(y - 2.0) < 1e-9), None)
    benchmark_10y = next((c for c, y in curve_cols if abs(y - 10.0) < 1e-9), None)

    draft_rows = []
    values = []
    pcts = []
    prev_value = None

    for row in source:
        record_date = row.get("record_date")
        if record_date is None:
            continue

        dt = (
            record_date.replace(hour=0, minute=0, second=0, microsecond=0)
            if isinstance(record_date, datetime)
            else datetime.combine(record_date, datetime.min.time())
        )

        short_vals = [float(row[c]) for c in short_cols if row.get(c) is not None]
        mid_vals = [float(row[c]) for c in mid_cols if row.get(c) is not None]
        long_vals = [float(row[c]) for c in long_cols if row.get(c) is not None]

        if not short_vals and not mid_vals and not long_vals:
            continue

        short_rate = _avg(short_vals)
        mid_rate = _avg(mid_vals)
        long_rate = _avg(long_vals)

        y2 = float(row.get(benchmark_2y)) if benchmark_2y and row.get(benchmark_2y) is not None else mid_rate
        y10 = float(row.get(benchmark_10y)) if benchmark_10y and row.get(benchmark_10y) is not None else long_rate

        slope_10y_2y = y10 - y2
        inversion_gap = max(short_rate - long_rate, 0.0)
        curvature = long_rate - (2.0 * mid_rate) + short_rate

        # Composite curve-tension score in yield points.
        value = abs(slope_10y_2y) + (abs(curvature) * 0.5) + (inversion_gap * 1.2)

        if prev_value is None:
            pct_change = 0.0
        elif prev_value != 0:
            pct_change = ((value - prev_value) / prev_value) * 100.0
        else:
            pct_change = (value - prev_value) * 100.0

        out = {
            "date_dt": dt,
            "value": value,
            "pct_change": pct_change,
            "short_rate": short_rate,
            "mid_rate": mid_rate,
            "long_rate": long_rate,
            "slope_10y_2y": slope_10y_2y,
            "inversion_gap": inversion_gap,
            "curvature": curvature,
        }
        draft_rows.append(out)
        values.append(value)
        pcts.append(pct_change)
        prev_value = value

    thresholds = _build_thresholds(values, pcts)
    rows = [{**r, "event_type": _classify(r, thresholds)} for r in draft_rows]

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`            BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`       DATETIME    NOT NULL,
                `value`         DOUBLE      NOT NULL,
                `pct_change`    DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`    VARCHAR(64) NOT NULL,
                `short_rate`    DOUBLE      NOT NULL DEFAULT 0.0,
                `mid_rate`      DOUBLE      NOT NULL DEFAULT 0.0,
                `long_rate`     DOUBLE      NOT NULL DEFAULT 0.0,
                `slope_10y_2y`  DOUBLE      NOT NULL DEFAULT 0.0,
                `inversion_gap` DOUBLE      NOT NULL DEFAULT 0.0,
                `curvature`     DOUBLE      NOT NULL DEFAULT 0.0,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 500):
            await conn.execute(text(f"""
                INSERT INTO `{enriched_table}`
                    (date_dt, value, pct_change, event_type, short_rate, mid_rate, long_rate,
                     slope_10y_2y, inversion_gap, curvature)
                VALUES
                    (:date_dt, :value, :pct_change, :event_type, :short_rate, :mid_rate, :long_rate,
                     :slope_10y_2y, :inversion_gap, :curvature)
            """), rows[i:i + 500])

    return {
        "source_rows": len(source),
        "enriched_rows": len(rows),
        "curve_columns": [c for c, _ in curve_cols],
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
