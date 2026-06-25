"""
model.py - sasha_alpha_daily_eurusd_ind (Alpha Vantage EURUSD indicators)
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


def _build_thresholds(trend_values: list[float], band_values: list[float], pcts: list[float]) -> dict:
    abs_trend = sorted(abs(x) for x in trend_values if x != 0.0)
    bands = sorted(x for x in band_values if x > 0.0)
    abs_pcts = sorted(abs(x) for x in pcts if x != 0.0)

    trend = max(_percentile(abs_trend, 0.60), 0.0005)
    trend_strong = max(_percentile(abs_trend, 0.90), trend)
    band_high = max(_percentile(bands, 0.70), 0.002)
    band_low = min(_percentile(bands, 0.30), band_high)
    accel = max(_percentile(abs_pcts, 0.75), 0.4)

    return {
        "trend": trend,
        "trend_strong": trend_strong,
        "band_high": band_high,
        "band_low": band_low,
        "accel": accel,
    }


def _classify(row: dict, thresholds: dict) -> str:
    rsi = float(row["rsi_14"])
    trend = float(row["trend_strength"])
    band = float(row["band_width"])
    pct = float(row["pct_change"])

    trend_level = float(thresholds["trend"])
    trend_strong = float(thresholds["trend_strong"])
    band_high = float(thresholds["band_high"])
    band_low = float(thresholds["band_low"])
    accel = float(thresholds["accel"])

    if rsi >= 70.0 and trend >= trend_strong and pct >= accel:
        return "eurusd_overbought_breakout"
    if rsi <= 30.0 and trend <= -trend_strong and pct >= accel:
        return "eurusd_oversold_breakdown"
    if band >= band_high and abs(trend) < trend_level:
        return "eurusd_volatility_expansion"
    if band <= band_low and abs(trend) < trend_level:
        return "eurusd_volatility_squeeze"
    if trend >= trend_level and rsi >= 50.0:
        return "eurusd_bull_trend"
    if trend <= -trend_level and rsi <= 50.0:
        return "eurusd_bear_trend"
    return "eurusd_range"


async def _load_source(engine_vlad, engine_brain, parser_table: str) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT
            datetime_iso,
            sma_20,
            ema_12,
            ema_26,
            rsi_14,
            bb_middle,
            bb_upper,
            bb_lower
        FROM `{parser_table}`
        WHERE datetime_iso IS NOT NULL
          AND sma_20 IS NOT NULL
          AND ema_12 IS NOT NULL
          AND ema_26 IS NOT NULL
          AND rsi_14 IS NOT NULL
          AND bb_middle IS NOT NULL
          AND bb_upper IS NOT NULL
          AND bb_lower IS NOT NULL
        ORDER BY datetime_iso
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
    trend_values = []
    band_values = []
    pcts = []
    prev_score = None

    for date_iso, sma_20, ema_12, ema_26, rsi_14, bb_middle, bb_upper, bb_lower in source:
        dt = (
            date_iso.replace(hour=0, minute=0, second=0, microsecond=0)
            if isinstance(date_iso, datetime)
            else datetime.combine(date_iso, datetime.min.time())
        )

        sma = float(sma_20)
        e12 = float(ema_12)
        e26 = float(ema_26)
        rsi = float(rsi_14)
        bb_m = float(bb_middle)
        bb_u = float(bb_upper)
        bb_l = float(bb_lower)

        baseline = abs(sma) if sma != 0 else 1e-9
        trend_strength = (e12 - e26) / baseline
        band_width = (bb_u - bb_l) / (abs(bb_m) if bb_m != 0 else baseline)
        rsi_centered = (rsi - 50.0) / 50.0
        score = abs(trend_strength) + (band_width * 0.5) + (abs(rsi_centered) * 0.3)

        if prev_score is None:
            pct_change = 0.0
        elif prev_score != 0:
            pct_change = ((score - prev_score) / prev_score) * 100.0
        else:
            pct_change = (score - prev_score) * 100.0

        draft_rows.append(
            {
                "date_dt": dt,
                "value": score,
                "pct_change": pct_change,
                "sma_20": sma,
                "ema_12": e12,
                "ema_26": e26,
                "rsi_14": rsi,
                "band_width": band_width,
                "trend_strength": trend_strength,
            }
        )
        trend_values.append(trend_strength)
        band_values.append(band_width)
        pcts.append(pct_change)
        prev_score = score

    thresholds = _build_thresholds(trend_values, band_values, pcts)
    rows = [{**r, "event_type": _classify(r, thresholds)} for r in draft_rows]

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`             BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`        DATETIME    NOT NULL,
                `value`          DOUBLE      NOT NULL,
                `pct_change`     DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`     VARCHAR(64) NOT NULL,
                `sma_20`         DOUBLE      NOT NULL DEFAULT 0.0,
                `ema_12`         DOUBLE      NOT NULL DEFAULT 0.0,
                `ema_26`         DOUBLE      NOT NULL DEFAULT 0.0,
                `rsi_14`         DOUBLE      NOT NULL DEFAULT 0.0,
                `band_width`     DOUBLE      NOT NULL DEFAULT 0.0,
                `trend_strength` DOUBLE      NOT NULL DEFAULT 0.0,
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
                        (date_dt, value, pct_change, event_type, sma_20, ema_12, ema_26, rsi_14, band_width, trend_strength)
                    VALUES
                        (:date_dt, :value, :pct_change, :event_type, :sma_20, :ema_12, :ema_26, :rsi_14, :band_width, :trend_strength)
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
