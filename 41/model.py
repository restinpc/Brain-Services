from __future__ import annotations
from datetime import datetime, timedelta


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())

# ── Конфиг ─────────────────────
WEIGHTS_TABLE          = "sasha_eq_weights"
CTX_TABLE              = "sasha_eq_context_idx"
CTX_KEY_COLUMNS        = ["id"]

DATASET_QUERY = """
    SELECT id, magnitude, tsunami, alert_level,
           depth_km, significance, felt_reports, event_time,
           event_time AS date
    FROM vlad_usgs_earthquakes
    WHERE event_time IS NOT NULL
    ORDER BY event_time
"""
DATASET_ENGINE         = "brain"
FILTER_DATASET_BY_DATE = True
SHIFT_WINDOW           = 24
RECURRING_MIN_COUNT    = 2
CACHE_DATE_FROM        = "2025-01-15"
VAR_RANGE              = [0, 1, 2, 3]

def _mag_class(mag: float) -> str:
    if mag < 5.5:
        return "small"
    if mag < 6.5:
        return "medium"
    return "large"

def _build_reverse(ctx_index: dict) -> dict:
    """(mag_class, tsunami, alert_level) → (ctx_id, info)"""
    result = {}
    for key, info in ctx_index.items():
        ctx_id = info.get("id")
        mc = info.get("mag_class")
        ts = info.get("tsunami")
        al = info.get("alert_level")
        if ctx_id and mc:
            lookup_key = (mc, int(ts or 0), str(al or "none").lower())
            result[lookup_key] = (ctx_id, info)
    return result

def _apply_var(t1: float, var: int, info: dict) -> float:
    if var == 0:
        return t1
    elif var == 1:
        if (info.get("avg_significance") or 0) < 400:
            return 0.0
        return t1
    elif var == 2:
        depth = info.get("avg_depth_km") or 50
        return t1 * (1.0 / (depth / 10.0 + 1.0))
    elif var == 3:
        if (info.get("felt_ratio") or 0) < 0.1:
            return 0.0
        return t1
    return 0.0

def model(
    rates,
    dataset,
    date,
    *,
    type=0,
    var=0,
    param="",
    dataset_index: dict | None = None,
    ctx=None,
):
    if not rates or not dataset:
        return {}

    reverse = None
    if dataset_index:
        reverse = dataset_index.get("ctx_reverse")
        if reverse is None:
            ds_ctx_index = dataset_index.get("ctx_index")
            if ds_ctx_index:
                reverse = _build_reverse(ds_ctx_index)
    if reverse is None and ctx is not None:
        reverse = _build_reverse(ctx.ctx_index)

    if not reverse:
        return {}

    du = timedelta(hours=1)
    prev = None
    modification = 1.0
    if ctx is not None:
        du = ctx.delta_unit
        prev = ctx.prev_candle
        modification = float(getattr(ctx, "modification", 1.0))

    np_rates = dataset_index.get("np_rates") if dataset_index else None
    np_view = None
    if np_rates is not None:
        dates_ns = np_rates.get("dates_ns")
        if dates_ns is not None:
            import numpy as _np_mod
            cut = int(_np_mod.searchsorted(dates_ns, _dt_to_ts(date), side="right"))
            np_view = {
                "dates_ns": dates_ns[:cut],
                "t1": np_rates["t1"][:cut],
                "ext_max": np_rates["ext_max"][:cut],
                "ext_min": np_rates["ext_min"][:cut],
                "cut": cut,
            }
            if cut > 0:
                is_bull = float(np_rates["close"][cut - 1]) > float(np_rates["open"][cut - 1])
            else:
                is_bull = True
            np_view["ext"] = np_view["ext_max"] if is_bull else np_view["ext_min"]

    rates_t1 = {}
    ext_max = set()
    ext_min = set()
    if np_view is None:
        rates_t1 = {
            r["date"]: float((r.get("close") or 0) - (r.get("open") or 0))
            for r in rates
        }
        for i in range(1, len(rates) - 1):
            h = float(rates[i].get("max") or 0)
            lo = float(rates[i].get("min") or 0)
            if h > float(rates[i - 1].get("max") or 0) and h > float(rates[i + 1].get("max") or 0):
                ext_max.add(rates[i]["date"])
            if lo < float(rates[i - 1].get("min") or 0) and lo < float(rates[i + 1].get("min") or 0):
                ext_min.add(rates[i]["date"])

    result = {}
    WINDOW_SEC = SHIFT_WINDOW * 3600

    for eq in dataset:
        eq_time = eq.get("event_time")
        if eq_time is None:
            continue

        diff_sec = (date - eq_time).total_seconds()
        if diff_sec < 0 or diff_sec > WINDOW_SEC:
            continue

        shift = int(diff_sec / 3600)

        mc = _mag_class(float(eq.get("magnitude") or 0))
        ts = int(eq.get("tsunami") or 0)
        al = str(eq.get("alert_level") or "none").lower()
        lookup = reverse.get((mc, ts, al))
        if lookup is None:
            continue

        ctx_id, info = lookup
        occ = info.get("occurrence_count", 0)

        if occ < RECURRING_MIN_COUNT and shift != 0:
            continue

        # Историческая дата — eq_time флорим до часа чтобы совпасть с котировками
        eq_time_h = eq_time.replace(minute=0, second=0, microsecond=0)
        t_date = eq_time_h + timedelta(hours=1) * shift
        if ctx is not None and du >= timedelta(days=1):
            t_date = t_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if t_date >= date:
            continue

        if np_view is not None:
            import numpy as _np_i
            t_ts = _dt_to_ts(t_date)
            idx = int(_np_i.searchsorted(np_view["dates_ns"], t_ts, side="left"))
            if idx >= np_view["cut"] or int(np_view["dates_ns"][idx]) != t_ts:
                continue
            t1 = float(np_view["t1"][idx])
            ext_hit = bool(np_view["ext"][idx])
        else:
            t1 = rates_t1.get(t_date, 0.0)
            if prev is not None:
                _, is_bull = prev
            else:
                last = rates[-1] if rates else None
                is_bull = (float(last.get("close") or 0) > float(last.get("open") or 0)) if last else True
            ext_set = ext_max if is_bull else ext_min
            ext_hit = t_date in ext_set

        weighted_t1 = _apply_var(t1, var, info)
        if weighted_t1 != 0.0 and type in (0, 1):
            wc = f"{ctx_id}_0_{shift}"
            result[wc] = result.get(wc, 0.0) + round(weighted_t1, 6)

        if type in (0, 2) and occ > 0:
            if ext_hit:
                ext = ((1.0 / occ) * 2 - 1) * modification
                if ext != 0:
                    wc = f"{ctx_id}_1_{shift}"
                    result[wc] = result.get(wc, 0.0) + round(ext, 6)

    return {k: v for k, v in result.items() if v != 0}