"""
model.py — FRED DFF (Effective Federal Funds Rate)

Датасет: sasha_fred_dff (id, date_iso, value, loaded_at).
Типы событий: rate_hike / rate_cut / rate_unchanged.

Код веса: {ctx_id}_{mode}_{shift}
  mode=0 -> T1
  mode=1 -> Extremum
  shift  -> 0..SHIFT_WINDOW дней
"""

from __future__ import annotations

from datetime import date as date_class
from datetime import datetime, time, timedelta


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


RATES_TABLE = "brain_rates_eur_usd"

WEIGHTS_TABLE = "sasha_fred_dff_weights"
CTX_TABLE = "sasha_fred_dff_context_idx"
CTX_KEY_COLUMNS = ["id"]

DATASET_QUERY = """
    SELECT
        id,
        DATE_FORMAT(date_iso, '%Y-%m-%d') AS date_iso,
        value,
        loaded_at,
        STR_TO_DATE(DATE_FORMAT(date_iso, '%Y-%m-%d'), '%Y-%m-%d') AS event_time,
        CAST('2000-01-01 00:00:00' AS DATETIME) AS date
    FROM sasha_fred_dff
    WHERE date_iso IS NOT NULL
      AND value IS NOT NULL
      AND STR_TO_DATE(DATE_FORMAT(date_iso, '%Y-%m-%d'), '%Y-%m-%d') IS NOT NULL
      AND STR_TO_DATE(DATE_FORMAT(date_iso, '%Y-%m-%d'), '%Y-%m-%d') >= '1970-01-01'
      AND STR_TO_DATE(DATE_FORMAT(date_iso, '%Y-%m-%d'), '%Y-%m-%d') <= '2099-12-31'
    ORDER BY date_iso
"""
DATASET_ENGINE = "brain"
FILTER_DATASET_BY_DATE = False
SHIFT_WINDOW = 30
CACHE_DATE_FROM = "2020-01-01"
VAR_RANGE = [0, 1, 2, 3]
REBUILD_INTERVAL = 7200

_MIN_OCCURRENCE = 2

USE_ML_VALUES = True

def _as_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date_class):
        return datetime.combine(value, time.min)
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        txt = txt.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(txt)
        except ValueError:
            try:
                parsed = datetime.strptime(txt, "%Y-%m-%d")
            except ValueError:
                return None
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)
        return parsed
    return None


def _build_reverse(ctx_index: dict) -> dict[str, tuple[int, dict]]:
    reverse: dict[str, tuple[int, dict]] = {}
    for _, info in ctx_index.items():
        ctx_id = info.get("id")
        event_type = str(info.get("event_type") or "").strip().lower()
        if ctx_id and event_type:
            reverse[event_type] = (int(ctx_id), info)
    return reverse


def _event_type(delta: float) -> str:
    if delta > 0:
        return "rate_hike"
    if delta < 0:
        return "rate_cut"
    return "rate_unchanged"


def _prepare_events(dataset: list[dict]) -> list[tuple[datetime, float, str]]:
    """
    Превращаем сырой ряд value в список событий изменения:
    (event_time, delta_value), где delta_value != 0.
    """
    events: list[tuple[datetime, float, str]] = []
    prev_value = None

    for row in dataset:
        dt = _as_datetime(row.get("event_time") or row.get("date_iso") or row.get("date"))
        if dt is None:
            continue
        try:
            value = float(row.get("value"))
        except (TypeError, ValueError):
            continue

        if prev_value is not None:
            delta = value - prev_value
            events.append((dt, delta, _event_type(delta)))
        prev_value = value

    return events


def _apply_var(signed_t1: float, delta: float, var: int, ctx_info: dict) -> float:
    avg_abs_change = float(ctx_info.get("avg_abs_change") or 0.0)

    if var == 0:
        return signed_t1
    if var == 1:
        if avg_abs_change <= 0:
            return 0.0
        return signed_t1 if abs(delta) >= avg_abs_change else 0.0
    if var == 2:
        base = avg_abs_change if avg_abs_change > 0 else abs(delta)
        if base <= 0:
            return 0.0
        scale = min(abs(delta) / base, 3.0)
        return signed_t1 * scale
    if var == 3:
        return signed_t1 if delta > 0 else 0.0
    return 0.0


def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:
    del param

    if not rates or not dataset:
        return {}

    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if not ctx_index:
        return {}
    reverse = _build_reverse(ctx_index)
    if not reverse:
        return {}

    last_candle = rates[-1]
    is_daily = last_candle["date"].hour == 0 and last_candle["date"].minute == 0
    is_bull = float(last_candle.get("close") or 0) > float(last_candle.get("open") or 0)

    np_rates = (dataset_index or {}).get("np_rates")
    np_view = None
    if np_rates is not None:
        dates_ns = np_rates.get("dates_ns")
        if dates_ns is not None:
            import numpy as _np

            cut = int(_np.searchsorted(dates_ns, _dt_to_ts(date), side="right"))
            if cut > 0:
                is_bull = float(np_rates["close"][cut - 1]) > float(np_rates["open"][cut - 1])
            ext_arr = np_rates["ext_max"][:cut] if is_bull else np_rates["ext_min"][:cut]
            np_view = {
                "dates_ns": dates_ns[:cut],
                "t1": np_rates["t1"][:cut],
                "ext": ext_arr,
                "cut": cut,
            }

    rates_t1 = {}
    ext_set = set()
    rates_t1_by_day = {}
    ext_by_day = {}
    if np_view is None:
        rates_t1 = {
            r["date"]: float((r.get("close") or 0) - (r.get("open") or 0))
            for r in rates
        }
        ext_max = set()
        ext_min = set()
        for i in range(1, len(rates) - 1):
            h_val = float(rates[i].get("max") or 0)
            l_val = float(rates[i].get("min") or 0)
            if h_val > float(rates[i - 1].get("max") or 0) and h_val > float(rates[i + 1].get("max") or 0):
                ext_max.add(rates[i]["date"])
            if l_val < float(rates[i - 1].get("min") or 0) and l_val < float(rates[i + 1].get("min") or 0):
                ext_min.add(rates[i]["date"])
        ext_set = ext_max if is_bull else ext_min
        if is_daily:
            # Daily tables can keep non-midnight timestamps; align lookups by date.
            rates_t1_by_day = {
                r["date"].date(): float((r.get("close") or 0) - (r.get("open") or 0))
                for r in rates
            }
            ext_by_day = {d.date(): True for d in ext_set}

    events = _prepare_events(dataset)
    if not events:
        return {}

    result: dict[str, float] = {}
    window_sec = SHIFT_WINDOW * 86400

    for event_time, delta, event_type in events:
        lookup = reverse.get(event_type)
        if lookup is None:
            continue
        ctx_id, ctx_info = lookup
        occurrence_count = int(ctx_info.get("occurrence_count") or 0)

        diff_sec = (date - event_time).total_seconds()
        if diff_sec < 0 or diff_sec > window_sec:
            continue

        shift = int(diff_sec // 86400)
        if occurrence_count < _MIN_OCCURRENCE and shift != 0:
            continue

        t_date = event_time + timedelta(days=shift)
        t_date = t_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if is_daily:
            t_date = t_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if t_date > date:
            continue

        if np_view is not None:
            import numpy as _np_i

            t_ts = _dt_to_ts(t_date)
            idx = int(_np_i.searchsorted(np_view["dates_ns"], t_ts, side="left"))
            if idx >= np_view["cut"] or int(np_view["dates_ns"][idx]) != t_ts:
                if not is_daily:
                    continue
                day_start_ts = _dt_to_ts(t_date.replace(hour=0, minute=0, second=0, microsecond=0))
                day_end_ts = day_start_ts + 86400
                left = int(_np_i.searchsorted(np_view["dates_ns"], day_start_ts, side="left"))
                right = int(_np_i.searchsorted(np_view["dates_ns"], day_end_ts, side="left"))
                if right <= left:
                    continue
                idx = right - 1
            t1 = float(np_view["t1"][idx])
            ext_hit = bool(np_view["ext"][idx])
        else:
            if is_daily:
                day_key = t_date.date()
                t1 = rates_t1_by_day.get(day_key, 0.0)
                ext_hit = bool(ext_by_day.get(day_key, False))
            else:
                t1 = rates_t1.get(t_date, 0.0)
                ext_hit = t_date in ext_set

        direction = 1.0 if delta > 0 else -1.0
        signed_t1 = t1 * direction
        weighted_t1 = _apply_var(signed_t1, delta, var, ctx_info)

        if weighted_t1 != 0.0 and type in (0, 1):
            wc = f"{ctx_id}_0_{shift}"
            result[wc] = result.get(wc, 0.0) + round(weighted_t1, 6)

        if type in (0, 2) and occurrence_count > 0 and ext_hit:
            ext = ((1.0 / occurrence_count) * 2 - 1) * direction
            if ext != 0.0:
                wc = f"{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + round(ext, 6)

    return {k: v for k, v in result.items() if v != 0.0}
