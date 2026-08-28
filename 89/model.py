"""
model.py - technical-analysis figure model with reverse-learning.

The source model only reports which figures from the fixed catalog are
visible on the current chart.  brain_framework then scores those events
with stored T1 (candle-sum) and extremum probability; reverse_learning
reweights the active codes on historical extrema.
"""
from __future__ import annotations

from datetime import datetime
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from brain_framework import get_service_config, run_standard_model
from patterns import FIGURE_CATALOG, scan_ohlc


RATES_SOURCES = {
    "brain_rates_eur_usd": "eur_h1",
    "brain_rates_eur_usd_day": "eur_d1",
    "brain_rates_btc_usd": "btc_h1",
    "brain_rates_btc_usd_day": "btc_d1",
    "brain_rates_eth_usd": "eth_h1",
    "brain_rates_eth_usd_day": "eth_d1",
}
SOURCE_TO_TABLE = {value: key for key, value in RATES_SOURCES.items()}

# selected rows + timestamps + compiled event index (shared across model/batch calls)
_SELECTED_CACHE: dict[tuple[int, int, str], list] = {}
_RESULT_CACHE: dict[tuple, dict] = {}
_RESULT_CACHE_MAX = 4096
_CFG_CACHE: dict | None = None


def _as_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    return datetime.combine(value, datetime.min.time())


async def _load_ohlc(engine_brain, engine_vlad, table_name: str) -> list:
    from sqlalchemy import text

    query = text(f"""
        SELECT date, open, close, `max`, `min`
        FROM `{table_name}`
        WHERE date IS NOT NULL
          AND open IS NOT NULL
          AND close IS NOT NULL
          AND `max` IS NOT NULL
          AND `min` IS NOT NULL
        ORDER BY date
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
    counts: dict[str, int] = {}
    figure_counts: dict[str, int] = {}
    rows: list[dict] = []

    for table_name, source_name in RATES_SOURCES.items():
        source = await _load_ohlc(engine_brain, engine_vlad, table_name)
        if not source:
            counts[source_name] = 0
            continue

        dates = [_as_datetime(row[0]) for row in source]
        opens = [float(row[1]) for row in source]
        closes = [float(row[2]) for row in source]
        highs = [float(row[3]) for row in source]
        lows = [float(row[4]) for row in source]
        detected = scan_ohlc(dates, opens, highs, lows, closes)
        for row in detected:
            row["source_name"] = source_name
            figure_counts[row["event_type"]] = figure_counts.get(row["event_type"], 0) + 1
        rows.extend(detected)
        counts[source_name] = len(detected)

    rows.sort(key=lambda row: (row["date_dt"], row["source_name"], row["event_type"]))

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`          BIGINT      NOT NULL AUTO_INCREMENT,
                `date_dt`     DATETIME    NOT NULL,
                `value`       DOUBLE      NOT NULL,
                `pct_change`  DOUBLE      NOT NULL DEFAULT 0.0,
                `event_type`  VARCHAR(64) NOT NULL,
                `source_name` VARCHAR(16) NOT NULL,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_source_date` (`source_name`, `date_dt`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for index in range(0, len(rows), 500):
            await conn.execute(
                text(f"""
                    INSERT INTO `{enriched_table}`
                        (date_dt, value, pct_change, event_type, source_name)
                    VALUES
                        (:date_dt, :value, :pct_change, :event_type, :source_name)
                """),
                rows[index : index + 500],
            )

    _SELECTED_CACHE.clear()
    _RESULT_CACHE.clear()
    return {
        "catalog_size": len(FIGURE_CATALOG),
        "source_rows": counts,
        "enriched_rows": len(rows),
        "figures_fired": len(figure_counts),
        "top_figures": dict(
            sorted(figure_counts.items(), key=lambda item: item[1], reverse=True)[:12]
        ),
    }


def _source_from_index(dataset_index: dict | None, param: str) -> str:
    requested = str(param or "").strip().lower()
    if requested in SOURCE_TO_TABLE:
        return requested
    table = str((dataset_index or {}).get("rates_table") or "")
    return RATES_SOURCES.get(table, "eur_h1")


def _service_cfg() -> dict:
    global _CFG_CACHE
    if _CFG_CACHE is None:
        _CFG_CACHE = get_service_config()
    return _CFG_CACHE


def _select_source(
    dataset: list[dict],
    dataset_index: dict | None,
    source_name: str,
) -> tuple[list[dict], dict]:
    index = dict(dataset_index or {})
    full_dataset = index.get("full_dataset")
    source = full_dataset if isinstance(full_dataset, list) else dataset
    cache_key = (id(source), len(source), source_name)
    cached = _SELECTED_CACHE.get(cache_key)

    if cached is None:
        selected = [
            row
            for row in source
            if str(row.get("source_name") or "").strip().lower() == source_name
        ]
        selected.sort(key=lambda row: row.get("date") or row.get("date_dt"))
        timestamps = [
            int((row.get("date") or row["date_dt"]).timestamp())
            for row in selected
        ]
        cached = [selected, timestamps, None]
        if len(_SELECTED_CACHE) >= 12:
            _SELECTED_CACHE.clear()
            _RESULT_CACHE.clear()
        _SELECTED_CACHE[cache_key] = cached

    selected, timestamps, events = cached
    index["full_dataset"] = selected
    index["dataset_timestamps"] = timestamps
    if events is not None:
        index["_standard_events_by_type"] = events
    index["_selected_cache_entry"] = cached
    return selected, index


def _persist_event_index(index: dict) -> None:
    entry = index.get("_selected_cache_entry")
    events = index.get("_standard_events_by_type")
    if isinstance(entry, list) and events is not None and entry[2] is None:
        entry[2] = events


def _run_source_model(
    rates,
    selected_dataset: list[dict],
    date,
    *,
    source_name: str,
    source_type: int,
    source_var: int,
    selected_index: dict,
    ml_enabled: bool,
    shift_window: int,
) -> dict:
    if ml_enabled:
        cache_key = (id(selected_dataset), date, source_name)
        cached = _RESULT_CACHE.get(cache_key)
        if cached is not None:
            return cached

    result = run_standard_model(
        rates,
        selected_dataset,
        date,
        type=source_type,
        var=source_var,
        dataset_index=selected_index,
        shift_window=shift_window,
        apply_var_fn=_apply_var,
    )
    _persist_event_index(selected_index)

    if ml_enabled:
        if len(_RESULT_CACHE) >= _RESULT_CACHE_MAX:
            _RESULT_CACHE.clear()
        _RESULT_CACHE[cache_key] = result
    return result


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
    source_name = _source_from_index(dataset_index, param)
    selected_dataset, selected_index = _select_source(
        dataset,
        dataset_index,
        source_name,
    )
    if not selected_dataset:
        return {}

    cfg = _service_cfg()
    ml_enabled = bool((cfg.get("ml") or {}).get("enabled", False))

    # In reverse-learning mode the public type/var values configure the
    # trainer (train mode and extremum interval).  The source model must only
    # provide the complete set of active codes, so use its unfiltered slot.
    return _run_source_model(
        rates,
        selected_dataset,
        date,
        source_name=source_name,
        source_type=0 if ml_enabled else type,
        source_var=0 if ml_enabled else var,
        selected_index=selected_index,
        ml_enabled=ml_enabled,
        shift_window=cfg["cache"]["shift_window"],
    )


def batch_model(rates, dataset, dates, *, type=0, var=0, param="", dataset_index=None):
    """Shared event index + result cache for fill_cache ML prewarm."""
    if not dates:
        return {}
    source_name = _source_from_index(dataset_index, param)
    selected_dataset, selected_index = _select_source(
        dataset,
        dataset_index,
        source_name,
    )
    if not selected_dataset:
        return {date: {} for date in dates}

    cfg = _service_cfg()
    ml_enabled = bool((cfg.get("ml") or {}).get("enabled", False))
    shift_window = cfg["cache"]["shift_window"]
    source_type = 0 if ml_enabled else type
    source_var = 0 if ml_enabled else var

    return {
        date: _run_source_model(
            rates,
            selected_dataset,
            date,
            source_name=source_name,
            source_type=source_type,
            source_var=source_var,
            selected_index=selected_index,
            ml_enabled=ml_enabled,
            shift_window=shift_window,
        )
        for date in dates
    }
