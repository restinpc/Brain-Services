"""
model.py - ECB currency in circulation and M3 monetary-supply model.

param selects one normalized source:
  emission -> sasha_ecb_emission
  m3       -> sasha_ecb_m3
"""
from __future__ import annotations

from datetime import datetime

from brain_framework import get_service_config, run_standard_model


DATASETS = {
    "emission": "sasha_ecb_emission",
    "m3": "sasha_ecb_m3",
}
PARAM_RANGE = list(DATASETS)

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


def _build_thresholds(pcts: list[float]) -> dict[str, float]:
    abs_values = sorted(abs(v) for v in pcts if v != 0.0)
    positive = sorted(v for v in pcts if v > 0.0)
    negative_abs = sorted(abs(v) for v in pcts if v < 0.0)

    expansion = max(_percentile(positive or abs_values, 0.60), 0.01)
    expansion_surge = max(
        _percentile(positive or abs_values, 0.90),
        expansion,
    )
    contraction_abs = max(
        _percentile(negative_abs or abs_values, 0.60),
        0.01,
    )
    contraction_shock_abs = max(
        _percentile(negative_abs or abs_values, 0.90),
        contraction_abs,
    )
    stable = min(_percentile(abs_values, 0.30), expansion, contraction_abs)

    return {
        "expansion": expansion,
        "expansion_surge": expansion_surge,
        "contraction": -contraction_abs,
        "contraction_shock": -contraction_shock_abs,
        "stable": stable,
    }


def _classify(dataset_name: str, pct: float, thresholds: dict[str, float]) -> str:
    prefix = "ecb_currency" if dataset_name == "emission" else "ecb_m3"

    if pct >= thresholds["expansion_surge"]:
        regime = "expansion_surge"
    elif pct >= thresholds["expansion"]:
        regime = "expansion"
    elif pct <= thresholds["contraction_shock"]:
        regime = "contraction_shock"
    elif pct <= thresholds["contraction"]:
        regime = "contraction"
    elif abs(pct) <= thresholds["stable"]:
        regime = "stable"
    else:
        regime = "transition"

    return f"{prefix}_{regime}"


async def _load_source(engine_vlad, engine_brain, table_name: str) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT date_iso, value
        FROM `{table_name}`
        WHERE date_iso IS NOT NULL
          AND value IS NOT NULL
        ORDER BY date_iso
    """)
    last_error = None
    table_found = False

    for engine in (engine_brain, engine_vlad):
        try:
            async with engine.connect() as conn:
                rows = (await conn.execute(query)).fetchall()
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


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config()
    enriched_table = cfg["dataset"]["enriched_table"]
    source_tables = {
        "emission": cfg["dataset"].get("parser_table", DATASETS["emission"]),
        "m3": cfg["dataset"].get("parser_table_2", DATASETS["m3"]),
    }

    source_by_name = {
        name: await _load_source(engine_vlad, engine_brain, table_name)
        for name, table_name in source_tables.items()
    }

    rows: list[dict] = []
    thresholds_by_name: dict[str, dict[str, float]] = {}

    for dataset_name, source in source_by_name.items():
        draft_rows: list[dict] = []
        pcts: list[float] = []
        prev = None

        for date_iso, raw_value in source:
            value = float(raw_value)
            dt = (
                date_iso.replace(hour=0, minute=0, second=0, microsecond=0)
                if isinstance(date_iso, datetime)
                else datetime.combine(date_iso, datetime.min.time())
            )

            if prev is not None:
                pct_change = (
                    ((value - prev) / prev) * 100.0
                    if prev != 0.0
                    else (value - prev) * 100.0
                )
                draft_rows.append(
                    {
                        "date_dt": dt,
                        "value": value,
                        "pct_change": pct_change,
                        "dataset_name": dataset_name,
                    }
                )
                pcts.append(pct_change)
            prev = value

        thresholds = _build_thresholds(pcts)
        thresholds_by_name[dataset_name] = thresholds
        rows.extend(
            {
                **row,
                "event_type": _classify(
                    dataset_name,
                    float(row["pct_change"]),
                    thresholds,
                ),
            }
            for row in draft_rows
        )

    rows.sort(key=lambda row: (row["date_dt"], row["dataset_name"]))

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`           BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`      DATETIME    NOT NULL,
                `value`        DOUBLE      NOT NULL,
                `pct_change`   DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`   VARCHAR(64) NOT NULL,
                `dataset_name` VARCHAR(16) NOT NULL,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_dataset_date` (`dataset_name`, `date_dt`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 500):
            await conn.execute(
                text(f"""
                    INSERT INTO `{enriched_table}`
                        (date_dt, value, pct_change, event_type, dataset_name)
                    VALUES
                        (:date_dt, :value, :pct_change, :event_type, :dataset_name)
                """),
                rows[i : i + 500],
            )

    _SELECTED_CACHE.clear()
    return {
        "source_rows": {
            name: len(source)
            for name, source in source_by_name.items()
        },
        "enriched_rows": len(rows),
        "thresholds": thresholds_by_name,
    }


def _select_dataset(
    dataset: list[dict],
    dataset_index: dict | None,
    dataset_name: str,
) -> tuple[list[dict], dict]:
    index = dict(dataset_index or {})
    full_dataset = index.get("full_dataset")
    source = full_dataset if isinstance(full_dataset, list) else dataset
    cache_key = (id(source), len(source), dataset_name)
    cached = _SELECTED_CACHE.get(cache_key)

    if cached is None:
        selected = [
            row
            for row in source
            if str(row.get("dataset_name") or "").strip().lower() == dataset_name
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
    dataset_name = str(param or "").strip().lower()
    if dataset_name not in DATASETS:
        return {}

    selected_dataset, selected_index = _select_dataset(
        dataset,
        dataset_index,
        dataset_name,
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
