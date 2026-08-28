"""
model.py - Service 91: U.S. Treasury auction demand / absorption model.

Source
------
FiscalData `v1/accounting/od/auctions_query`, stored by parser/treasury_gov.py as
`vlad_tr_auctions_query`.

Economic idea
-------------
One completed Treasury auction is decomposed into independent contexts:
  * bid-to-cover demand;
  * primary-dealer absorption share;
  * indirect-bidder share;
  * accepted-book pricing dispersion;
  * offering/supply pressure;
  * original issue vs reopening.

No direction is hard-coded. brain_framework measures historical T1/extremum outcomes
and reverse_learning learns the sign/weight separately for each market/timeframe.

Causality
---------
* A result becomes visible only after auction_date + closing_time_comp + safety lag.
* All LOW/NORMAL/HIGH thresholds for an auction use PRIOR auctions of the SAME
  instrument only. The current auction is appended to rolling history afterwards.
* Same-timestamp auctions are classified as a batch, so they cannot set each
  other's thresholds.
* D1 automatically sees an intraday auction only on the next daily target timestamp
  because brain_framework filters dataset rows causally by `date_dt <= target_date`.
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import date as date_cls, datetime, time as time_cls, timedelta, timezone
import math
import re
from statistics import median
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from brain_framework import get_service_config, run_standard_model


_NY = ZoneInfo("America/New_York")
_UTC = timezone.utc
_NULL_STRINGS = {"", "null", "none", "nan", "n/a", "na", "-"}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in _NULL_STRINGS else text


def _to_float(value: Any) -> float | None:
    text = _clean(value)
    if not text:
        return None
    text = text.replace("$", "").replace(",", "").replace("%", "").strip()
    # Fractional coupons are not used by the selected features, but accepting
    # them here keeps numeric parsing defensive.
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_date(value: Any) -> date_cls | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date_cls):
        return value
    text = _clean(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue
    return None


def _parse_clock(value: Any) -> time_cls | None:
    """Parse FiscalData `closing_time_comp` variants (e.g. 11:30 AM, 13:00)."""
    if isinstance(value, time_cls):
        return value.replace(tzinfo=None)
    text = _clean(value).upper().replace("E.T.", "").replace("ET", "").strip()
    if not text:
        return None
    text = re.sub(r"\s+", " ", text)
    for fmt in ("%I:%M %p", "%I:%M:%S %p", "%H:%M", "%H:%M:%S", "%I %p"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _yes(value: Any) -> bool:
    return _clean(value).lower() in {"yes", "y", "true", "1"}


def _slug(value: Any) -> str:
    text = _clean(value).upper()
    text = re.sub(r"[^A-Z0-9]+", "_", text).strip("_")
    return text or "UNKNOWN"


def _canonical_instrument(row: dict) -> str:
    term = _clean(row.get("original_security_term")) or _clean(row.get("security_term"))
    term_slug = _slug(term)

    if _yes(row.get("cash_management_bill_cmb")):
        prefix = "CMB"
    elif _yes(row.get("inflation_index_security")) or _yes(row.get("tips")):
        prefix = "TIPS"
    elif _yes(row.get("floating_rate")):
        prefix = "FRN"
    else:
        prefix = _slug(row.get("security_type"))

    return f"{prefix}_{term_slug}"[:48]


def _auction_available_at(row: dict, safety_minutes: int) -> datetime | None:
    auction_day = _to_date(row.get("auction_date"))
    if auction_day is None:
        return None

    close_clock = _parse_clock(row.get("closing_time_comp"))
    if close_clock is None:
        # Historical rows may not have a close time. 18:00 New York is a
        # deliberately conservative same-day fallback: it avoids pretending the
        # result was available at midnight while retaining old auctions as analogs.
        close_clock = time_cls(18, 0)

    local_dt = datetime.combine(auction_day, close_clock).replace(tzinfo=_NY)
    available_local = local_dt + timedelta(minutes=max(0, int(safety_minutes)))
    return available_local.astimezone(_UTC).replace(tzinfo=None, microsecond=0)


def _ratio(numerator: Any, denominator: Any) -> float | None:
    n = _to_float(numerator)
    d = _to_float(denominator)
    if n is None or d is None or d <= 0.0:
        return None
    value = n / d
    return value if math.isfinite(value) and value >= 0.0 else None


def _dispersion(row: dict) -> float | None:
    # Notes/Bonds/TIPS -> yield; Bills -> discount rate; FRNs -> discount margin.
    for high_name, median_name in (
        ("high_yield", "avg_med_yield"),
        ("high_discnt_rate", "avg_med_discnt_rate"),
        ("high_discnt_margin", "avg_med_discnt_margin"),
        ("high_investment_rate", "avg_med_investment_rate"),
    ):
        high = _to_float(row.get(high_name))
        med = _to_float(row.get(median_name))
        if high is not None and med is not None:
            value = high - med
            if math.isfinite(value):
                return max(0.0, value)
    return None


def _features(row: dict) -> dict[str, float]:
    result: dict[str, float] = {}

    btc = _to_float(row.get("bid_to_cover_ratio"))
    if btc is not None and btc > 0.0:
        result["BTC"] = btc

    dealer = _ratio(row.get("primary_dealer_accepted"), row.get("comp_accepted"))
    if dealer is not None:
        result["DEALER"] = dealer

    indirect = _ratio(row.get("indirect_bidder_accepted"), row.get("comp_accepted"))
    if indirect is not None:
        result["INDIRECT"] = indirect

    dispersion = _dispersion(row)
    if dispersion is not None:
        result["DISP"] = dispersion

    supply = _to_float(row.get("offering_amt"))
    if supply is not None and supply > 0.0:
        result["SUPPLY"] = supply

    return result


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    q = min(1.0, max(0.0, float(q)))
    idx = (len(sorted_values) - 1) * q
    lo = int(math.floor(idx))
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _state(metric: str, current: float, prior: Iterable[float], q_low: float, q_high: float) -> tuple[str, float]:
    hist = sorted(float(v) for v in prior if v is not None and math.isfinite(float(v)))
    if not hist:
        return "NORMAL", 0.0

    low = _quantile(hist, q_low)
    high = _quantile(hist, q_high)

    if metric == "DISP":
        label = "NARROW" if current <= low else "WIDE" if current >= high else "NORMAL"
    elif metric == "SUPPLY":
        label = "SMALL" if current <= low else "LARGE" if current >= high else "NORMAL"
    else:
        label = "LOW" if current <= low else "HIGH" if current >= high else "NORMAL"

    med = float(median(hist))
    abs_dev = sorted(abs(v - med) for v in hist)
    mad = float(median(abs_dev)) if abs_dev else 0.0
    scale = max(mad * 1.4826, abs(med) * 0.01, 1e-9)
    surprise = max(-10.0, min(10.0, (float(current) - med) / scale))
    return label, surprise


def _result_completeness(row: dict) -> int:
    fields = (
        "bid_to_cover_ratio", "comp_accepted", "offering_amt",
        "primary_dealer_accepted", "indirect_bidder_accepted",
        "high_yield", "avg_med_yield", "high_discnt_rate", "avg_med_discnt_rate",
        "high_discnt_margin", "avg_med_discnt_margin", "pdf_filenm_comp_results",
    )
    return sum(1 for field in fields if _clean(row.get(field)))


def _dedupe_source(rows: list[dict]) -> list[dict]:
    """Keep the most complete row for each CUSIP + auction date."""
    chosen: dict[tuple[str, str], dict] = {}
    rank: dict[tuple[str, str], tuple[int, str, int]] = {}

    for row in rows:
        cusip = _clean(row.get("cusip"))
        auction_date = _clean(row.get("auction_date"))
        if not cusip or not auction_date:
            continue
        key = (cusip, auction_date)
        row_rank = (
            _result_completeness(row),
            _clean(row.get("record_date")),
            int(_to_float(row.get("id")) or 0),
        )
        if key not in chosen or row_rank > rank[key]:
            chosen[key] = row
            rank[key] = row_rank

    return list(chosen.values())


def build_enriched_rows(source_rows: list[dict], *, cfg: dict | None = None) -> tuple[list[dict], dict]:
    """Pure transformation used by enrich_dataset() and unit tests."""
    cfg = cfg or get_service_config()
    model_cfg = (cfg.get("model") or {}) if isinstance(cfg, dict) else {}
    history_window = max(4, int(model_cfg.get("history_window", 24)))
    history_min = max(2, int(model_cfg.get("history_min", 8)))
    q_low = float(model_cfg.get("quantile_low", 0.25))
    q_high = float(model_cfg.get("quantile_high", 0.75))
    safety_minutes = max(0, int(model_cfg.get("publication_safety_minutes", 30)))

    prepared = []
    skipped_incomplete = 0
    skipped_date = 0

    for raw in _dedupe_source(source_rows):
        row = {str(k).lower(): v for k, v in raw.items()}
        available_at = _auction_available_at(row, safety_minutes)
        if available_at is None:
            skipped_date += 1
            continue

        features = _features(row)
        # Require a genuine completed result, not just an announcement row.
        comp_accepted = _to_float(row.get("comp_accepted"))
        if "BTC" not in features or comp_accepted is None or comp_accepted <= 0.0:
            skipped_incomplete += 1
            continue

        prepared.append(
            {
                "row": row,
                "date_dt": available_at,
                "instrument": _canonical_instrument(row),
                "features": features,
            }
        )

    prepared.sort(
        key=lambda item: (
            item["date_dt"],
            item["instrument"],
            _clean(item["row"].get("cusip")),
        )
    )

    histories: dict[tuple[str, str], deque[float]] = defaultdict(
        lambda: deque(maxlen=history_window)
    )
    rows: list[dict] = []
    event_counts: dict[str, int] = defaultdict(int)
    auctions_used = 0

    # Process identical availability timestamps as a batch. Thresholds for every
    # event in the batch see only strictly earlier timestamps.
    i = 0
    while i < len(prepared):
        batch_time = prepared[i]["date_dt"]
        j = i + 1
        while j < len(prepared) and prepared[j]["date_dt"] == batch_time:
            j += 1
        batch = prepared[i:j]

        pending_history_updates: list[tuple[tuple[str, str], float]] = []

        for item in batch:
            row = item["row"]
            instrument = item["instrument"]
            features = item["features"]
            cusip = _clean(row.get("cusip"))
            auction_date = _clean(row.get("auction_date"))
            reopening = _yes(row.get("reopening"))

            btc_hist = histories[(instrument, "BTC")]
            base_surprise = 0.0
            if len(btc_hist) >= history_min:
                _base_state, base_surprise = _state("BTC", features["BTC"], btc_hist, q_low, q_high)

            base_type = f"AUC.{instrument}.BASE"
            rows.append(
                {
                    "date_dt": item["date_dt"],
                    "value": float(features["BTC"]),
                    "pct_change": float(base_surprise * 10.0),
                    "event_type": base_type,
                    "instrument": instrument,
                    "feature_name": "BASE",
                    "feature_state": "BASE",
                    "cusip": cusip,
                    "auction_date": _to_date(auction_date),
                    "reopening": 1 if reopening else 0,
                }
            )
            event_counts[base_type] += 1

            issue_state = "REOPEN" if reopening else "ORIGINAL"
            issue_type = f"AUC.{instrument}.ISSUE.{issue_state}"
            rows.append(
                {
                    "date_dt": item["date_dt"],
                    "value": 1.0,
                    "pct_change": 0.0,
                    "event_type": issue_type,
                    "instrument": instrument,
                    "feature_name": "ISSUE",
                    "feature_state": issue_state,
                    "cusip": cusip,
                    "auction_date": _to_date(auction_date),
                    "reopening": 1 if reopening else 0,
                }
            )
            event_counts[issue_type] += 1

            for metric, current in features.items():
                hist = histories[(instrument, metric)]
                if len(hist) >= history_min:
                    state, surprise = _state(metric, current, hist, q_low, q_high)
                    event_type = f"AUC.{instrument}.{metric}.{state}"
                    rows.append(
                        {
                            "date_dt": item["date_dt"],
                            "value": float(current),
                            "pct_change": float(surprise * 10.0),
                            "event_type": event_type,
                            "instrument": instrument,
                            "feature_name": metric,
                            "feature_state": state,
                            "cusip": cusip,
                            "auction_date": _to_date(auction_date),
                            "reopening": 1 if reopening else 0,
                        }
                    )
                    event_counts[event_type] += 1

                pending_history_updates.append(((instrument, metric), float(current)))

            auctions_used += 1

        for key, value in pending_history_updates:
            histories[key].append(value)
        i = j

    rows.sort(key=lambda r: (r["date_dt"], r["event_type"], r["cusip"]))
    return rows, {
        "source_rows": len(source_rows),
        "deduped_rows": len(prepared) + skipped_incomplete + skipped_date,
        "auctions_used": auctions_used,
        "enriched_rows": len(rows),
        "contexts": len(event_counts),
        "history_window": history_window,
        "history_min": history_min,
        "skipped_incomplete": skipped_incomplete,
        "skipped_bad_date": skipped_date,
        "top_contexts": dict(sorted(event_counts.items(), key=lambda kv: kv[1], reverse=True)[:12]),
    }


async def _load_source(engine_vlad, engine_brain, parser_table: str) -> list[dict]:
    from sqlalchemy import text

    # SELECT * deliberately: FiscalData's auctions schema is wide and has changed
    # over time. Feature extraction is alias-safe and simply ignores unavailable
    # optional fields. This also works with parser-created TEXT columns.
    query = text(f"""
        SELECT *
        FROM `{parser_table}`
        WHERE auction_date IS NOT NULL
          AND cusip IS NOT NULL
        ORDER BY auction_date, cusip, id
    """)

    last_error = None
    table_found = False
    for engine in (engine_vlad, engine_brain):
        try:
            async with engine.connect() as conn:
                result = await conn.execute(query)
                table_found = True
                mappings = [dict(r) for r in result.mappings().all()]
                if mappings:
                    return mappings
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
    parser_table = cfg["dataset"]["parser_table"]
    enriched_table = cfg["dataset"]["enriched_table"]

    source = await _load_source(engine_vlad, engine_brain, parser_table)
    rows, stats = build_enriched_rows(source, cfg=cfg)

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`            BIGINT       NOT NULL AUTO_INCREMENT,
                `date_dt`       DATETIME     NOT NULL,
                `value`         DOUBLE       NOT NULL DEFAULT 0.0,
                `pct_change`    DOUBLE       NOT NULL DEFAULT 0.0,
                `event_type`    VARCHAR(96)  NOT NULL,
                `instrument`    VARCHAR(48)  NOT NULL,
                `feature_name`  VARCHAR(16)  NOT NULL,
                `feature_state` VARCHAR(16)  NOT NULL,
                `cusip`         VARCHAR(16)  NOT NULL,
                `auction_date`  DATE         NULL,
                `reopening`     TINYINT      NOT NULL DEFAULT 0,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_instrument_date` (`instrument`, `date_dt`),
                INDEX `idx_cusip_auction` (`cusip`, `auction_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))

        insert_sql = text(f"""
            INSERT INTO `{enriched_table}`
                (date_dt, value, pct_change, event_type, instrument,
                 feature_name, feature_state, cusip, auction_date, reopening)
            VALUES
                (:date_dt, :value, :pct_change, :event_type, :instrument,
                 :feature_name, :feature_state, :cusip, :auction_date, :reopening)
        """)
        for start in range(0, len(rows), 500):
            await conn.execute(insert_sql, rows[start:start + 500])

    return stats


def _apply_var(signed_t1: float, pct: float, var: int, ctx_info: dict) -> float:
    # In production ML mode source_var is forced to 0. These variants remain
    # available for non-ML research/debug runs and match the modern services.
    avg = float(ctx_info.get("avg_abs_pct_change") or 0.0)
    if var == 0:
        return signed_t1
    if var == 1:
        return signed_t1 if avg > 0.0 and abs(pct) >= avg else 0.0
    if var == 2:
        base = avg if avg > 0.0 else abs(pct)
        return signed_t1 * min(abs(pct) / base, 3.0) if base > 0.0 else 0.0
    if var == 3:
        return signed_t1 if pct > 0.0 else 0.0
    return 0.0


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    cfg = get_service_config()
    ml_enabled = bool((cfg.get("ml") or {}).get("enabled", False))

    # Reverse-learning uses public type/var as trainer axes. The source model
    # should expose the full active auction-code set, not filter it by those axes.
    source_type = 0 if ml_enabled else int(type)
    source_var = 0 if ml_enabled else int(var)

    return run_standard_model(
        rates,
        dataset,
        date,
        type=source_type,
        var=source_var,
        dataset_index=dataset_index,
        shift_window=int(cfg["cache"]["shift_window"]),
        apply_var_fn=_apply_var,
        min_occurrence=2,
    )
