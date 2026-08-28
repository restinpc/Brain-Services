"""Service 95: causal macro-economic surprise model.

Source
------
``vlad_macro_calendar_events`` from TradingView Economic Calendar.

The model deliberately uses only fields whose historical interpretation is
suitable for a release-time backtest:

* ``datetime``  - scheduled/release timestamp stored by the fixed parser in UTC;
* ``Country`` + ``Title`` - indicator identity;
* ``Actual`` and ``Forecast`` - the release surprise;
* ``Importance`` - retained as metadata only.

``Previous`` is *not* used by the historical signal because old values may be
revised after the fact and the legacy table has no vintage/first-seen snapshot.
Future calendar rows with ``Actual IS NULL`` also produce no signal.

Causality
---------
For every Country+Title series the current ``Actual-Forecast`` is normalized
only against STRICTLY PRIOR releases of that exact series. Same-timestamp rows
cannot affect one another. The release becomes active only at the next H1
boundary (strictly next hour for exact-hour releases), so a 13:30 publication
is first usable at 14:00 and a 15:00 publication at 16:00.

Direction is never hand-coded. ``POS`` only means Actual > Forecast and ``NEG``
means Actual < Forecast; Brain historical outcomes / reverse-learning decide
whether that context historically precedes EUR/BTC/ETH strength or weakness.
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, date as date_cls
import hashlib
import math
import re
from typing import Any, Iterable

from brain_framework import get_service_config, run_standard_model


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

_SPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]+")
_STAGE_NOISE_RE = re.compile(r"\s+")

FAMILY_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("INFLATION", re.compile(r"\b(?:inflation|cpi|pce|ppi|consumer price|producer price|price index|prices)\b", re.I)),
    ("LABOR", re.compile(r"\b(?:payrolls?|non\s*farm|nonfarm|employment|unemployment|jobless|jobs?|jolts|claims|earnings|wages?|labor|labour|participation|claimant|weekly hours)\b", re.I)),
    ("GROWTH", re.compile(r"\b(?:gdp|economic growth|industrial production|factory orders|production|output)\b", re.I)),
    ("ACTIVITY", re.compile(r"\b(?:pmi|ism|business activity|new orders|capacity utilization|inventor(?:y|ies)|manufacturing|durable goods|machinery orders|leading index|industrial trends)\b", re.I)),
    ("CONSUMER", re.compile(r"\b(?:retail sales|consumer confidence|consumer sentiment|consumer credit|personal spending|personal income|household spending|vehicle sales|current conditions)\b", re.I)),
    ("HOUSING", re.compile(r"\b(?:home sales|housing|mortgage|building permits|housing starts|home price|nahb|house approvals|construction spending)\b", re.I)),
    ("TRADE", re.compile(r"\b(?:balance of trade|trade balance|exports?|imports?|current account|foreign investment|foreign securities|tic flows)\b", re.I)),
    ("MONEY_CREDIT", re.compile(r"\b(?:money supply|m[123]\b|bank lending|loans?|credit|fed balance sheet|foreign exchange reserves)\b", re.I)),
    ("ENERGY", re.compile(r"\b(?:eia|api crude|oil stocks?|gasoline|distillate|natural gas|rig count|refinery|crude oil)\b", re.I)),
    ("CENTRAL_BANK", re.compile(r"\b(?:fed |fomc|ecb|boe|boj|snb|rba|boc|interest rate|rate decision|deposit facility rate|monetary policy)\b", re.I)),
    ("SENTIMENT", re.compile(r"\b(?:sentiment|optimism|expectations|zew|ifo|economic confidence)\b", re.I)),
    ("FISCAL", re.compile(r"\b(?:budget|government debt|federal budget|treasury statement|public sector net borrowing)\b", re.I)),
]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if math.isfinite(v) else None
    s = _clean(value).replace("\u2212", "-").replace("%", "").replace(",", ".")
    if not s:
        return None
    try:
        v = float(s)
    except Exception:
        return None
    return v if math.isfinite(v) else None


def _coerce_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None, microsecond=0)
    if isinstance(value, date_cls):
        return datetime.combine(value, datetime.min.time())
    if value is None:
        return None
    s = str(value).strip().replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(s[:19]).replace(microsecond=0)
    except Exception:
        return None


def _next_hour_strict(dt: datetime) -> datetime:
    """First H1 boundary strictly after the release timestamp."""
    base = dt.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=1)


def _title_key(country: str, title: str) -> tuple[str, str]:
    country = _clean(country).upper()[:8]
    title_norm = _SPACE_RE.sub(" ", _clean(title)).strip()
    return country, title_norm.casefold()


def _family(title: str) -> str:
    for name, rx in FAMILY_RULES:
        if rx.search(title or ""):
            return name
    return "OTHER"


def _slug(title: str) -> str:
    raw = _NON_ALNUM_RE.sub("_", _clean(title).upper()).strip("_")
    raw = re.sub(r"_+", "_", raw)
    if len(raw) <= 48:
        return raw or "EVENT"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:7].upper()
    return f"{raw[:40].rstrip('_')}_{digest}"


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(float(x) for x in values)
    if len(vals) == 1:
        return vals[0]
    q = min(1.0, max(0.0, float(q)))
    pos = (len(vals) - 1) * q
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(vals) - 1)
    f = pos - lo
    return vals[lo] * (1.0 - f) + vals[hi] * f


def _robust_scale(prior_surprises: Iterable[float], eps: float) -> float:
    abs_vals = sorted(abs(float(x)) for x in prior_surprises if math.isfinite(float(x)))
    nonzero = [x for x in abs_vals if x > eps]
    if not nonzero:
        return 1.0
    # Median absolute forecast error keeps zero as the natural surprise centre.
    scale = _quantile(nonzero, 0.50)
    if scale <= eps:
        scale = _quantile(nonzero, 0.75)
    return max(float(scale), eps)


def _collapse_duplicates(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per release. Prefer complete Actual+Forecast, then highest id.

    The legacy dump contains ~2k duplicate (datetime,Country,Title) rows. A later
    duplicate must not create a second market event with the same price reaction.
    """
    best: dict[tuple[datetime, str, str], tuple[tuple[int, int, int], dict[str, Any]]] = {}
    for raw in rows:
        dt = _coerce_dt(raw.get("datetime") or raw.get("date_dt") or raw.get("date"))
        country = _clean(raw.get("Country") if "Country" in raw else raw.get("country")).upper()
        title = _clean(raw.get("Title") if "Title" in raw else raw.get("title"))
        if dt is None or not country or not title:
            continue
        actual = _safe_float(raw.get("Actual") if "Actual" in raw else raw.get("actual"))
        forecast = _safe_float(raw.get("Forecast") if "Forecast" in raw else raw.get("forecast"))
        previous = _safe_float(raw.get("Previous") if "Previous" in raw else raw.get("previous"))
        try:
            source_id = int(raw.get("id") or 0)
        except Exception:
            source_id = 0
        try:
            importance = int(raw.get("Importance") if "Importance" in raw else raw.get("importance"))
        except Exception:
            importance = -1
        item = {
            "source_id": source_id,
            "source_datetime": dt,
            "country": country[:8],
            "title": title[:180],
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
            "importance": importance,
        }
        completeness = int(actual is not None) + int(forecast is not None) + int(previous is not None)
        score = (int(actual is not None and forecast is not None), completeness, source_id)
        key = (dt, country, title.casefold())
        if key not in best or score > best[key][0]:
            best[key] = (score, item)
    return [v[1] for v in best.values()]


def build_enriched_rows(source_rows: Iterable[dict[str, Any]], *, cfg: dict[str, Any] | None = None) -> tuple[list[dict], dict]:
    """Pure causal transformation used by production enrichment and tests."""
    cfg = dict(cfg or {})
    min_history = max(2, int(cfg.get("min_history", 6)))
    history_window = max(min_history, int(cfg.get("history_window", 60)))
    big_q = float(cfg.get("big_quantile", 0.75))
    zero_eps = max(0.0, float(cfg.get("zero_epsilon", 1e-12)))
    strict_next_hour = bool(cfg.get("strict_next_hour", True))

    deduped = _collapse_duplicates(source_rows)
    prepared = [r for r in deduped if r["actual"] is not None and r["forecast"] is not None]
    prepared.sort(key=lambda r: (r["source_datetime"], r["country"], r["title"].casefold(), r["source_id"]))

    histories: dict[tuple[str, str], deque[float]] = defaultdict(lambda: deque(maxlen=history_window))
    rows: list[dict] = []
    skipped_warmup = 0
    skipped_missing = len(deduped) - len(prepared)
    event_counts: dict[str, int] = defaultdict(int)

    i = 0
    while i < len(prepared):
        batch_dt = prepared[i]["source_datetime"]
        j = i + 1
        while j < len(prepared) and prepared[j]["source_datetime"] == batch_dt:
            j += 1
        batch = prepared[i:j]
        pending: list[tuple[tuple[str, str], float]] = []

        for item in batch:
            key = _title_key(item["country"], item["title"])
            raw_surprise = float(item["actual"] - item["forecast"])
            hist = histories[key]

            if len(hist) >= min_history:
                prior = list(hist)
                scale = _robust_scale(prior, zero_eps)
                score = max(-10.0, min(10.0, raw_surprise / scale))
                abs_prior = [abs(x) for x in prior]
                big_threshold = _quantile(abs_prior, big_q)

                if abs(raw_surprise) <= zero_eps:
                    sign = "INLINE"
                    magnitude = "NORMAL"
                else:
                    sign = "POS" if raw_surprise > 0.0 else "NEG"
                    magnitude = "BIG" if big_threshold > zero_eps and abs(raw_surprise) >= big_threshold else "NORMAL"

                fam = _family(item["title"])
                slug = _slug(item["title"])
                # Primary identity deliberately excludes BIG/NORMAL. On the real
                # 2024-2026 dump, putting magnitude into every exact indicator key
                # fragmented the median context support to only ~3 occurrences.
                # Keep the exact indicator+sign dense, while BIG surprise pressure
                # is emitted as a separate broad family context below.
                event_type = f"MC.{item['country']}.{fam}.{slug}.{sign}"
                date_dt = _next_hour_strict(batch_dt) if strict_next_hour else batch_dt
                base_row = {
                    "source_id": item["source_id"],
                    "source_datetime": batch_dt,
                    "date_dt": date_dt,
                    "country": item["country"],
                    "title_key": f"{item['country']}|{item['title']}",
                    "title": item["title"],
                    "family": fam,
                    "importance": item["importance"],
                    "actual": item["actual"],
                    "forecast": item["forecast"],
                    "raw_surprise": raw_surprise,
                    "surprise_score": score,
                    "magnitude": magnitude,
                    "sign_state": sign,
                    "history_count": len(hist),
                    "event_type": event_type[:160],
                    "event_scope": "INDICATOR",
                    "value": raw_surprise,
                    "pct_change": score,
                }
                rows.append(base_row)
                event_counts[event_type[:160]] += 1

                # Magnitude companion: a BIG release gets a second exact-indicator
                # code. Crucially we do NOT aggregate BIG surprises across a broad
                # family such as LABOR, because Actual>Forecast means opposite
                # economics for payrolls vs unemployment. The primary sign context
                # remains dense; this optional companion lets reverse-learning test
                # whether unusually large misses add information for this same series.
                if magnitude == "BIG" and sign != "INLINE":
                    big_type = f"MCX.{item['country']}.{slug}.{sign}.BIG"
                    big_row = dict(base_row)
                    big_row.update({
                        "event_type": big_type[:160],
                        "event_scope": "INDICATOR_BIG",
                    })
                    rows.append(big_row)
                    event_counts[big_type[:160]] += 1
            else:
                skipped_warmup += 1

            pending.append((key, raw_surprise))

        # Strict causality: releases with the same timestamp become historical
        # only AFTER every current release at that timestamp has been classified.
        for key, surprise in pending:
            histories[key].append(surprise)
        i = j

    rows.sort(key=lambda r: (r["date_dt"], r["event_type"], r["source_id"]))
    return rows, {
        "source_rows": len(list(source_rows)) if isinstance(source_rows, list) else len(deduped),
        "deduped_releases": len(deduped),
        "complete_actual_forecast": len(prepared),
        "skipped_missing_actual_or_forecast": skipped_missing,
        "skipped_warmup": skipped_warmup,
        "enriched_rows": len(rows),
        "event_types": len(event_counts),
        "event_counts": dict(sorted(event_counts.items())),
        "previous_used": False,
        "availability_rule": "strict_next_hour" if strict_next_hour else "release_timestamp",
    }


# ---------------------------------------------------------------------------
# SQL enrichment
# ---------------------------------------------------------------------------

async def _load_source(engine, table: str) -> list[dict[str, Any]]:
    from sqlalchemy import text
    # The table has existed in different deployments under either the vlad or
    # brain connection. Try the configured engine first; caller falls back.
    sql = text(f"""
        SELECT id, `datetime`, Country, Title, Actual, Previous, Forecast, Importance
        FROM `{table}`
        WHERE `datetime` IS NOT NULL
          AND Country IS NOT NULL
          AND Title IS NOT NULL
        ORDER BY `datetime`, id
    """)
    async with engine.connect() as conn:
        return [dict(r) for r in (await conn.execute(sql)).mappings().all()]


async def _load_source_any(engine_vlad, engine_brain, table: str) -> list[dict[str, Any]]:
    errors = []
    for engine in (engine_vlad, engine_brain):
        try:
            return await _load_source(engine, table)
        except Exception as exc:
            errors.append(exc)
    if errors:
        raise errors[-1]
    return []


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config() or {}
    ds = cfg["dataset"]
    parser_table = ds["parser_table"]
    enriched = ds["enriched_table"]
    model_cfg = dict(cfg.get("model") or {})

    source = await _load_source_any(engine_vlad, engine_brain, parser_table)
    rows, stats = build_enriched_rows(source, cfg=model_cfg)

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched}` (
                id BIGINT NOT NULL AUTO_INCREMENT,
                source_id INT NOT NULL DEFAULT 0,
                source_datetime DATETIME NOT NULL,
                date_dt DATETIME NOT NULL,
                country VARCHAR(8) NOT NULL,
                title_key VARCHAR(220) NOT NULL,
                title VARCHAR(180) NOT NULL,
                family VARCHAR(24) NOT NULL,
                importance SMALLINT NOT NULL DEFAULT -1,
                actual DOUBLE NOT NULL,
                forecast DOUBLE NOT NULL,
                raw_surprise DOUBLE NOT NULL,
                surprise_score DOUBLE NOT NULL,
                magnitude VARCHAR(8) NOT NULL,
                sign_state VARCHAR(8) NOT NULL,
                history_count SMALLINT NOT NULL DEFAULT 0,
                event_type VARCHAR(160) NOT NULL,
                event_scope VARCHAR(16) NOT NULL DEFAULT 'INDICATOR',
                value DOUBLE NOT NULL DEFAULT 0.0,
                pct_change DOUBLE NOT NULL DEFAULT 0.0,
                PRIMARY KEY(id),
                UNIQUE KEY uq_source_event(source_id,event_type,date_dt),
                INDEX idx_date(date_dt),
                INDEX idx_event(event_type),
                INDEX idx_country_date(country,date_dt),
                INDEX idx_family_date(family,date_dt),
                INDEX idx_source_dt(source_datetime)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched}`"))
        if rows:
            ins = text(f"""
                INSERT INTO `{enriched}`
                    (source_id,source_datetime,date_dt,country,title_key,title,family,importance,
                     actual,forecast,raw_surprise,surprise_score,magnitude,sign_state,history_count,
                     event_type,event_scope,value,pct_change)
                VALUES
                    (:source_id,:source_datetime,:date_dt,:country,:title_key,:title,:family,:importance,
                     :actual,:forecast,:raw_surprise,:surprise_score,:magnitude,:sign_state,:history_count,
                     :event_type,:event_scope,:value,:pct_change)
            """)
            for i in range(0, len(rows), 1000):
                await conn.execute(ins, rows[i:i+1000])

    return stats


# ---------------------------------------------------------------------------
# Brain runtime
# ---------------------------------------------------------------------------


def _apply_var(signed_t1: float, pct: float, var: int, ctx_info: dict) -> float:
    avg = float(ctx_info.get("avg_abs_pct_change") or 0.0)
    magnitude = abs(float(pct or 0.0))
    if var == 0:  # baseline
        return signed_t1
    if var == 1:  # stronger-than-this-context's historical norm
        return signed_t1 if avg > 0.0 and magnitude >= avg else 0.0
    if var == 2:  # robust surprise-amplitude weighting
        base = avg if avg > 0.0 else magnitude
        return signed_t1 * min(magnitude / base, 3.0) if base > 0.0 else 0.0
    if var == 3:  # objectively large normalized forecast error
        return signed_t1 if magnitude >= 2.0 else 0.0
    return 0.0


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    cfg = get_service_config() or {}
    ml_enabled = bool((cfg.get("ml") or {}).get("enabled", False))
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
