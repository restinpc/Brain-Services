"""
model.py - Eurostat quarterly government-debt model.

The model joins debt as a percentage of GDP with debt in millions of euros.
param selects a Eurostat aggregate: eu27_2020 or ea20.
"""
from __future__ import annotations

from datetime import datetime

from brain_framework import get_service_config, run_standard_model


GEOS = {
    "eu27_2020": "EU27_2020",
    "ea20": "EA20",
}
PARAM_RANGE = list(GEOS)

_SELECTED_CACHE: dict[tuple[int, int, str], tuple[list[dict], list[int]]] = {}


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0.0:
        return float(sorted_values[0])
    if p >= 1.0:
        return float(sorted_values[-1])
    idx = (len(sorted_values) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _build_thresholds(
    ratio_deltas: list[float],
    amount_pcts: list[float],
) -> dict[str, float]:
    positive_ratio = sorted(value for value in ratio_deltas if value > 0.0)
    negative_ratio_abs = sorted(abs(value) for value in ratio_deltas if value < 0.0)
    positive_amount = sorted(value for value in amount_pcts if value > 0.0)
    negative_amount_abs = sorted(abs(value) for value in amount_pcts if value < 0.0)

    ratio_up = max(_percentile(positive_ratio, 0.60), 0.05)
    ratio_surge = max(_percentile(positive_ratio, 0.90), ratio_up)
    ratio_down_abs = max(_percentile(negative_ratio_abs, 0.60), 0.05)
    ratio_relief_abs = max(
        _percentile(negative_ratio_abs, 0.90),
        ratio_down_abs,
    )
    amount_up = max(_percentile(positive_amount, 0.60), 0.05)
    amount_surge = max(_percentile(positive_amount, 0.90), amount_up)
    amount_down_abs = max(_percentile(negative_amount_abs, 0.60), 0.05)

    return {
        "ratio_up": ratio_up,
        "ratio_surge": ratio_surge,
        "ratio_down": -ratio_down_abs,
        "ratio_relief": -ratio_relief_abs,
        "amount_up": amount_up,
        "amount_surge": amount_surge,
        "amount_down": -amount_down_abs,
    }


def _classify(row: dict, thresholds: dict[str, float]) -> str:
    ratio_delta = float(row["ratio_delta_pp"])
    amount_pct = float(row["amount_pct_change"])

    if (
        ratio_delta >= thresholds["ratio_surge"]
        and amount_pct >= thresholds["amount_surge"]
    ):
        return "euro_debt_surge"
    if ratio_delta >= thresholds["ratio_up"]:
        return "euro_debt_ratio_rising"
    if amount_pct >= thresholds["amount_up"]:
        return "euro_debt_nominal_buildup"
    if (
        ratio_delta <= thresholds["ratio_relief"]
        and amount_pct <= thresholds["amount_down"]
    ):
        return "euro_debt_relief"
    if ratio_delta <= thresholds["ratio_down"]:
        return "euro_debt_ratio_falling"
    if amount_pct <= thresholds["amount_down"]:
        return "euro_debt_nominal_contraction"
    return "euro_debt_stable"


async def _load_source(
    engine_vlad,
    engine_brain,
    table_name: str,
    unit: str,
) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT date_iso, geo, value
        FROM `{table_name}`
        WHERE date_iso IS NOT NULL
          AND geo IN ('EU27_2020', 'EA20')
          AND unit = :unit
          AND value IS NOT NULL
        ORDER BY geo, date_iso
    """)
    last_error = None
    table_found = False

    for engine in (engine_brain, engine_vlad):
        try:
            async with engine.connect() as conn:
                rows = (await conn.execute(query, {"unit": unit})).fetchall()
                table_found = True
                if rows:
                    return rows
        except Exception as exc:
            last_error = exc

    if table_found:
        return []
    if last_error is not None:
        raise last_error
    return []


def _as_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    return datetime.combine(value, datetime.min.time())


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config()
    parser_table = cfg["dataset"]["parser_table"]
    parser_table_2 = cfg["dataset"]["parser_table_2"]
    enriched_table = cfg["dataset"]["enriched_table"]

    ratio_source = await _load_source(
        engine_vlad,
        engine_brain,
        parser_table,
        "PC_GDP",
    )
    amount_source = await _load_source(
        engine_vlad,
        engine_brain,
        parser_table_2,
        "MIO_EUR",
    )

    ratios = {
        (date_iso, str(geo).upper()): float(value)
        for date_iso, geo, value in ratio_source
    }
    amounts = {
        (date_iso, str(geo).upper()): float(value)
        for date_iso, geo, value in amount_source
    }
    joined_keys = sorted(
        ratios.keys() & amounts.keys(),
        key=lambda item: (item[1], item[0]),
    )

    draft_by_geo: dict[str, list[dict]] = {geo: [] for geo in GEOS.values()}
    previous_by_geo: dict[str, tuple[float, float]] = {}
    ratio_deltas: list[float] = []
    amount_pcts: list[float] = []

    for date_iso, geo in joined_keys:
        debt_pc_gdp = ratios[(date_iso, geo)]
        debt_mio_eur = amounts[(date_iso, geo)]
        previous = previous_by_geo.get(geo)

        if previous is None:
            ratio_delta_pp = 0.0
            amount_pct_change = 0.0
            pct_change = 0.0
        else:
            previous_ratio, previous_amount = previous
            ratio_delta_pp = debt_pc_gdp - previous_ratio
            amount_pct_change = (
                ((debt_mio_eur - previous_amount) / previous_amount) * 100.0
                if previous_amount != 0.0
                else 0.0
            )
            pct_change = (
                ((debt_pc_gdp - previous_ratio) / previous_ratio) * 100.0
                if previous_ratio != 0.0
                else 0.0
            )

        draft_by_geo.setdefault(geo, []).append(
            {
                "date_dt": _as_datetime(date_iso),
                "value": debt_pc_gdp,
                "pct_change": pct_change,
                "geo": geo,
                "debt_pc_gdp": debt_pc_gdp,
                "debt_mio_eur": debt_mio_eur,
                "ratio_delta_pp": ratio_delta_pp,
                "amount_pct_change": amount_pct_change,
            }
        )
        if previous is not None:
            ratio_deltas.append(ratio_delta_pp)
            amount_pcts.append(amount_pct_change)
        previous_by_geo[geo] = (debt_pc_gdp, debt_mio_eur)

    thresholds = _build_thresholds(ratio_deltas, amount_pcts)
    rows = [
        {**row, "event_type": _classify(row, thresholds)}
        for geo_rows in draft_by_geo.values()
        for row in geo_rows
    ]
    rows.sort(key=lambda row: (row["date_dt"], row["geo"]))

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`                BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`           DATETIME    NOT NULL,
                `value`             DOUBLE      NOT NULL,
                `pct_change`        DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`        VARCHAR(64) NOT NULL,
                `geo`               VARCHAR(20) NOT NULL,
                `debt_pc_gdp`       DOUBLE      NOT NULL,
                `debt_mio_eur`      DOUBLE      NOT NULL,
                `ratio_delta_pp`    DOUBLE      NOT NULL DEFAULT 0.0,
                `amount_pct_change` DOUBLE      NOT NULL DEFAULT 0.0,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uq_geo_date` (`geo`, `date_dt`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_geo_date` (`geo`, `date_dt`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for index in range(0, len(rows), 500):
            await conn.execute(
                text(f"""
                    INSERT INTO `{enriched_table}`
                        (date_dt, value, pct_change, event_type, geo,
                         debt_pc_gdp, debt_mio_eur, ratio_delta_pp,
                         amount_pct_change)
                    VALUES
                        (:date_dt, :value, :pct_change, :event_type, :geo,
                         :debt_pc_gdp, :debt_mio_eur, :ratio_delta_pp,
                         :amount_pct_change)
                """),
                rows[index : index + 500],
            )

    _SELECTED_CACHE.clear()
    return {
        "source_rows": {
            "pc_gdp": len(ratio_source),
            "mio_eur": len(amount_source),
        },
        "joined_rows": len(rows),
        "thresholds": thresholds,
    }


def _select_geo(
    dataset: list[dict],
    dataset_index: dict | None,
    geo: str,
) -> tuple[list[dict], dict]:
    index = dict(dataset_index or {})
    full_dataset = index.get("full_dataset")
    source = full_dataset if isinstance(full_dataset, list) else dataset
    cache_key = (id(source), len(source), geo)
    cached = _SELECTED_CACHE.get(cache_key)

    if cached is None:
        selected = [
            row
            for row in source
            if str(row.get("geo") or "").strip().upper() == geo
        ]
        selected.sort(key=lambda row: row.get("date") or row.get("date_dt"))
        timestamps = [
            int((row.get("date") or row["date_dt"]).timestamp())
            for row in selected
        ]
        cached = (selected, timestamps)
        if len(_SELECTED_CACHE) >= 8:
            _SELECTED_CACHE.clear()
        _SELECTED_CACHE[cache_key] = cached

    selected, timestamps = cached
    index["full_dataset"] = selected
    index["dataset_timestamps"] = timestamps
    return selected, index


def _apply_var(signed_t1: float, pct: float, var: int, ctx_info: dict) -> float:
    avg = float(ctx_info.get("avg_abs_pct_change") or 0.0)
    if var == 0:
        return signed_t1
    if var == 1:
        return signed_t1 if avg > 0.0 and abs(pct) >= avg else 0.0
    if var == 2:
        base = avg if avg > 0.0 else abs(pct)
        return (
            signed_t1 * min(abs(pct) / base, 3.0)
            if base > 0.0
            else 0.0
        )
    if var == 3:
        return signed_t1 if pct > 0.0 else 0.0
    return 0.0


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    geo = GEOS.get(str(param or "").strip().lower())
    if geo is None:
        return {}

    selected_dataset, selected_index = _select_geo(
        dataset,
        dataset_index,
        geo,
    )
    if not selected_dataset:
        return {}

    cfg = get_service_config()
    ml_enabled = bool((cfg.get("ml") or {}).get("enabled", False))

    # In reverse-learning mode the public type/var values configure the
    # trainer (train mode and extremum interval).  The source model must only
    # provide the complete set of active codes, so use its unfiltered slot.
    source_type = 0 if ml_enabled else type
    source_var = 0 if ml_enabled else var

    return run_standard_model(
        rates,
        selected_dataset,
        date,
        type=source_type,
        var=source_var,
        dataset_index=selected_index,
        shift_window=cfg["cache"]["shift_window"],
        apply_var_fn=_apply_var,
    )
