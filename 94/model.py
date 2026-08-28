"""Service 94: Polymarket prediction-market probability pressure.

Design goals
------------
* Use only information that was observable at each historical timestamp.
* Treat ``price`` as the historical implied probability of the selected YES token.
* Ignore historical ``volume``, ``volume_24h``, ``liquidity``, ``spread`` and
  ``outcome_yes/outcome_no`` from the legacy table because Polymarket_direct.py
  copied the *current* Gamma market snapshot backwards over historical price
  rows and updates those columns again on later parser runs.
* Remove sports/weather noise before scanning the huge price table.
* Convert price history to sparse, interpretable probability-repricing events:
  H1/H6/H24 moves, threshold crossings, reversals and cross-market breadth.
* Do not hand-code LONG/SHORT for EUR/USD, BTC/USD or ETH/USD.  Brain's
  historical outcome / reverse-learning layer determines the price direction.

The enriched table is intentionally much smaller than the raw 80M+ price rows.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, date as date_cls
import math
import re
from typing import Any, Iterable

from brain_framework import get_service_config, run_standard_model


# ---------------------------------------------------------------------------
# Text / market classification
# ---------------------------------------------------------------------------

SPORTS_RE = re.compile(
    r"\b(?:nba|nfl|nhl|mlb|wnba|ufc|mma|atp|wta|itf|soccer|football|basketball|"
    r"baseball|hockey|tennis|cricket|rugby|golf|formula\s*1|f1|nascar|match|game|"
    r"vs\.?|score|goals?|sets?|touchdowns?|innings?|grand slam|champions league|"
    r"premier league|la liga|serie a|bundesliga)\b",
    re.I,
)
WEATHER_RE = re.compile(
    r"\b(?:temperature|rainfall|rain|snow|weather|celsius|fahrenheit|°c|°f|"
    r"hurricane|typhoon|wind speed|highest temperature|lowest temperature)\b",
    re.I,
)

FAMILY_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("BTC", re.compile(r"\b(?:bitcoin|btc)\b", re.I)),
    ("ETH", re.compile(r"\b(?:ethereum|ether|eth)\b", re.I)),
    ("CRYPTO", re.compile(
        r"\b(?:crypto(?:currency)?|solana|\bsol\b|xrp|ripple|dogecoin|doge|"
        r"stablecoin|usdt|tether|usdc|memecoin|altcoin|defi|blockchain)\b", re.I)),
    ("FED", re.compile(
        r"\b(?:federal reserve|\bfed\b|fomc|rate cut|rate hike|interest rates?|"
        r"fed funds|jerome powell|powell)\b", re.I)),
    ("MACRO", re.compile(
        r"\b(?:inflation|\bcpi\b|\bpce\b|gdp|recession|unemployment|jobs report|"
        r"nonfarm|payrolls?|consumer prices?|economic growth|us economy|u\.s\. economy|"
        r"treasury yields?|bond yields?|\bdxy\b|us dollar|u\.s\. dollar)\b", re.I)),
    ("REGULATION", re.compile(
        r"\b(?:\bsec\b|cftc|regulation|regulator|crypto law|crypto bill|etf approval|"
        r"spot etf|securities and exchange commission)\b", re.I)),
    ("GEOPOLITICS", re.compile(
        r"\b(?:israel|iran|ukraine|russia|china|taiwan|gaza|ceasefire|war\b|nato|"
        r"netanyahu|putin|zelensky|hamas|hezbollah|north korea|south china sea)\b", re.I)),
    ("POLITICS", re.compile(
        r"\b(?:trump|biden|white house|president|presidential|congress|senate|house of representatives|"
        r"government shutdown|election|tariffs?|democrat|republican)\b", re.I)),
    ("ENERGY", re.compile(
        r"\b(?:oil|brent|wti|opec|natural gas|crude|energy prices?)\b", re.I)),
    ("MARKETS", re.compile(
        r"\b(?:s&p\s*500|s&p500|sp500|nasdaq|dow jones|stock market|equities|"
        r"vix|volatility index|gold price|silver price)\b", re.I)),
]

BULLISH_Q_RE = re.compile(
    r"\b(?:above|over|reach|hit|at least|higher than|greater than|exceed|new high|all[- ]time high|ath)\b",
    re.I,
)
BEARISH_Q_RE = re.compile(
    r"\b(?:below|under|fall below|drop below|less than|lower than|crash|collapse)\b",
    re.I,
)

_NUMBER_RE = re.compile(r"(?<![A-Za-z])(?:\$|€|£)?\d+(?:[.,]\d+)?(?:\s?(?:k|m|b|bn|million|billion|%|°c|°f))?", re.I)
_DATE_RE = re.compile(
    r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:,\s*\d{4})?\b",
    re.I,
)
_SPACE_RE = re.compile(r"\s+")


def normalize_question(text: str) -> str:
    s = str(text or "").lower().strip()
    s = _DATE_RE.sub(" <date> ", s)
    s = _NUMBER_RE.sub(" <n> ", s)
    s = re.sub(r"[^a-z0-9<>%$€£\s]+", " ", s)
    return _SPACE_RE.sub(" ", s).strip()[:240]


def classify_market(question: str, tags: str | None = None) -> dict[str, str] | None:
    text = f"{question or ''} {tags or ''}".strip()
    if not text:
        return None
    if SPORTS_RE.search(text) or WEATHER_RE.search(text):
        return None

    family = None
    for name, rule in FAMILY_RULES:
        if rule.search(text):
            family = name
            break
    if family is None:
        return None

    orientation = "NEUTRAL"
    if family in {"BTC", "ETH", "CRYPTO"}:
        bull = bool(BULLISH_Q_RE.search(text))
        bear = bool(BEARISH_Q_RE.search(text))
        if bull and not bear:
            orientation = "BULLQ"
        elif bear and not bull:
            orientation = "BEARQ"

    return {
        "family": family,
        "orientation": orientation,
        "signature": normalize_question(question),
    }


# ---------------------------------------------------------------------------
# Pure event construction
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PricePoint:
    condition_id: str
    dt: datetime
    price: float
    family: str
    orientation: str
    signature: str
    end_date: date_cls | None = None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        try:
            v = float(str(value).replace(",", "."))
        except Exception:
            return None
    return v if math.isfinite(v) else None


def _coerce_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date_cls):
        return datetime.combine(value, datetime.min.time())
    if value is None:
        return None
    s = str(value).strip().replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(s[:19])
    except Exception:
        return None


def _coerce_date(value: Any) -> date_cls | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date_cls):
        return value
    if not value:
        return None
    try:
        return date_cls.fromisoformat(str(value)[:10])
    except Exception:
        return None


def prepare_points(rows: Iterable[dict[str, Any]]) -> list[PricePoint]:
    out: list[PricePoint] = []
    for row in rows:
        dt = _coerce_dt(row.get("price_timestamp") or row.get("date_dt") or row.get("date"))
        p = _safe_float(row.get("price"))
        if dt is None or p is None or not (0.0 <= p <= 1.0):
            continue
        family = str(row.get("family") or "").upper().strip()
        orientation = str(row.get("orientation") or "NEUTRAL").upper().strip()
        signature = str(row.get("signature") or "").strip()
        if not family or not signature:
            cls = classify_market(str(row.get("question") or ""), row.get("tags"))
            if not cls:
                continue
            family = cls["family"]
            orientation = cls["orientation"]
            signature = cls["signature"]
        out.append(PricePoint(
            condition_id=str(row.get("condition_id") or "")[:66],
            dt=dt,
            price=float(p),
            family=family,
            orientation=orientation,
            signature=signature,
            end_date=_coerce_date(row.get("end_date")),
        ))
    out.sort(key=lambda x: (x.condition_id, x.dt))
    return out


def _lag_price(times: list[datetime], prices: list[float], idx: int, hours: int, tolerance_hours: float) -> float | None:
    target = times[idx] - timedelta(hours=hours)
    j = bisect_right(times, target, hi=idx) - 1
    if j < 0:
        return None
    age = target - times[j]
    if age < timedelta(0) or age > timedelta(hours=tolerance_hours):
        return None
    return prices[j]


def _move_bucket(delta: float, cfg: dict[str, Any]) -> str:
    a = abs(delta)
    if a >= float(cfg["extreme_move"]):
        return "EXTREME"
    if a >= float(cfg["large_move"]):
        return "LARGE"
    return "MED"


def _level_bucket(p: float) -> str:
    if p <= 0.10: return "TAIL_LOW"
    if p <= 0.25: return "LOW"
    if p < 0.75: return "MID"
    if p < 0.90: return "HIGH"
    return "TAIL_HIGH"


def _days_bucket(dt: datetime, end_date: date_cls | None) -> str | None:
    if end_date is None:
        return None
    d = (end_date - dt.date()).days
    if d < 0: return "EXPIRED"
    if d <= 1: return "D1"
    if d <= 7: return "D7"
    if d <= 30: return "D30"
    return "FAR"


def _event(date_dt: datetime, event_type: str, score: float, *, family: str, feature: str,
           condition_id: str = "", signature: str = "", price: float | None = None,
           horizon: int = 0, orientation: str = "NEUTRAL", market_count: int = 1) -> dict[str, Any]:
    return {
        "date_dt": date_dt,
        "value": float(price if price is not None else score),
        "pct_change": float(score),
        "event_type": event_type[:120],
        "family": family[:20],
        "feature": feature[:24],
        "condition_id": condition_id[:66],
        "signature": signature[:240],
        "horizon_hours": int(horizon),
        "orientation": orientation[:12],
        "market_count": int(market_count),
    }


def build_events(rows: Iterable[dict[str, Any]], *, cfg: dict[str, Any] | None = None,
                 write_from: datetime | None = None, write_to: datetime | None = None) -> tuple[list[dict], dict]:
    """Build sparse causal Polymarket events from ordered historical probabilities."""
    cfg = dict(cfg or {})
    defaults = {
        "h1_min_move": 0.02, "h6_min_move": 0.04, "h24_min_move": 0.07,
        "large_move": 0.07, "extreme_move": 0.15,
        "pressure_small": 0.015, "pressure_large": 0.03,
        "breadth_threshold": 0.35, "shock_move": 0.05, "shock_share": 0.25,
        "uncertainty_delta": 0.05, "min_family_signatures": 3,
    }
    for k, v in defaults.items(): cfg.setdefault(k, v)

    points = prepare_points(rows)
    by_condition: dict[str, list[PricePoint]] = defaultdict(list)
    for p in points:
        by_condition[p.condition_id].append(p)

    events: list[dict] = []
    # family/hour/signature -> list of (price,d1,d6,d24)
    agg_raw: dict[tuple[datetime, str, str], list[tuple[float, float | None, float | None, float | None]]] = defaultdict(list)
    scanned = 0

    for cid, seq in by_condition.items():
        times = [p.dt for p in seq]
        prices = [p.price for p in seq]
        prev_level = _level_bucket(prices[0]) if prices else "MID"
        for i, p in enumerate(seq):
            scanned += 1
            p1 = _lag_price(times, prices, i, 1, 1.25)
            p6 = _lag_price(times, prices, i, 6, 1.75)
            p24 = _lag_price(times, prices, i, 24, 3.0)
            d1 = p.price - p1 if p1 is not None else None
            d6 = p.price - p6 if p6 is not None else None
            d24 = p.price - p24 if p24 is not None else None

            agg_raw[(p.dt, p.family, p.signature)].append((p.price, d1, d6, d24))
            should_write = (write_from is None or p.dt >= write_from) and (write_to is None or p.dt < write_to)
            if not should_write:
                prev_level = _level_bucket(p.price)
                continue

            for horizon, delta, minimum in (
                (1, d1, float(cfg["h1_min_move"])),
                (6, d6, float(cfg["h6_min_move"])),
                (24, d24, float(cfg["h24_min_move"])),
            ):
                if delta is None or abs(delta) < minimum:
                    continue
                direction = "UP" if delta > 0 else "DOWN"
                bucket = _move_bucket(delta, cfg)
                score = delta * 100.0
                base = f"PM.{p.family}.H{horizon}.{direction}.{bucket}"
                events.append(_event(p.dt, base, score, family=p.family, feature=f"H{horizon}",
                                     condition_id=cid, signature=p.signature, price=p.price,
                                     horizon=horizon, orientation=p.orientation))
                if p.orientation != "NEUTRAL" and p.family in {"BTC", "ETH", "CRYPTO"}:
                    events.append(_event(
                        p.dt, f"PM.{p.family}.ORIENT.{p.orientation}.H{horizon}.{direction}.{bucket}",
                        score, family=p.family, feature=f"ORIENT_H{horizon}", condition_id=cid,
                        signature=p.signature, price=p.price, horizon=horizon, orientation=p.orientation,
                    ))

            # Probability threshold crossing: emit only when the state changes.
            cur_level = _level_bucket(p.price)
            if i > 0 and cur_level != prev_level:
                score = (p.price - prices[i-1]) * 100.0
                events.append(_event(
                    p.dt, f"PM.{p.family}.LEVEL.ENTER.{cur_level}", score,
                    family=p.family, feature="LEVEL", condition_id=cid, signature=p.signature,
                    price=p.price, orientation=p.orientation,
                ))
            prev_level = cur_level

            # Short-term reversal: 1h move opposes broader 6h move.
            if d1 is not None and d6 is not None and abs(d1) >= float(cfg["h1_min_move"]) and abs(d6) >= float(cfg["h6_min_move"]):
                if d1 * d6 < 0:
                    direction = "UP" if d1 > 0 else "DOWN"
                    events.append(_event(
                        p.dt, f"PM.{p.family}.REVERSAL.TO_{direction}", d1 * 100.0,
                        family=p.family, feature="REVERSAL", condition_id=cid, signature=p.signature,
                        price=p.price, orientation=p.orientation,
                    ))

            end_bucket = _days_bucket(p.dt, p.end_date)
            if end_bucket in {"D1", "D7"} and d1 is not None and abs(d1) >= float(cfg["h1_min_move"]):
                events.append(_event(
                    p.dt, f"PM.{p.family}.NEAR_END.{end_bucket}.MOVE", d1 * 100.0,
                    family=p.family, feature="NEAR_END", condition_id=cid, signature=p.signature,
                    price=p.price, orientation=p.orientation,
                ))

    # Deduplicate contracts that represent the same question template before
    # computing family breadth. Each signature gets equal weight regardless of
    # how many threshold/outcome markets Polymarket created for that event.
    by_hour_family: dict[tuple[datetime, str], list[tuple[float, float | None, float | None, float | None]]] = defaultdict(list)
    for (dt, family, _sig), vals in agg_raw.items():
        # Mean within signature avoids a multi-outcome/range event dominating.
        def mean_idx(idx: int) -> float | None:
            vv = [x[idx] for x in vals if x[idx] is not None]
            return sum(vv)/len(vv) if vv else None
        by_hour_family[(dt, family)].append((mean_idx(0) or 0.0, mean_idx(1), mean_idx(2), mean_idx(3)))

    last_uncertainty: dict[str, tuple[datetime, float]] = {}
    aggregate_written = 0
    for (dt, family), vals in sorted(by_hour_family.items()):
        if write_from is not None and dt < write_from:  # still seed uncertainty history
            probs = [v[0] for v in vals]
            if probs:
                last_uncertainty[family] = (dt, sum(4*p*(1-p) for p in probs)/len(probs))
            continue
        if write_to is not None and dt >= write_to:
            continue
        n = len(vals)
        if n < int(cfg["min_family_signatures"]):
            continue

        d1s = [v[1] for v in vals if v[1] is not None]
        probs = [v[0] for v in vals]
        if d1s:
            mean_d1 = sum(d1s) / len(d1s)
            pos = sum(1 for x in d1s if x > 0)
            neg = sum(1 for x in d1s if x < 0)
            breadth = (pos - neg) / max(1, len(d1s))
            shock_share = sum(1 for x in d1s if abs(x) >= float(cfg["shock_move"])) / len(d1s)

            if abs(mean_d1) >= float(cfg["pressure_small"]):
                direction = "UP" if mean_d1 > 0 else "DOWN"
                size = "LARGE" if abs(mean_d1) >= float(cfg["pressure_large"]) else "MED"
                events.append(_event(dt, f"PM.{family}.PRESSURE.H1.{direction}.{size}", mean_d1*100.0,
                                     family=family, feature="PRESSURE", market_count=n))
                aggregate_written += 1
            if abs(breadth) >= float(cfg["breadth_threshold"]):
                direction = "UP" if breadth > 0 else "DOWN"
                events.append(_event(dt, f"PM.{family}.BREADTH.H1.{direction}", breadth*100.0,
                                     family=family, feature="BREADTH", market_count=n))
                aggregate_written += 1
            if shock_share >= float(cfg["shock_share"]):
                events.append(_event(dt, f"PM.{family}.SHOCK_CLUSTER.H1", shock_share*100.0,
                                     family=family, feature="SHOCK_CLUSTER", market_count=n))
                aggregate_written += 1

        if probs:
            uncertainty = sum(4*p*(1-p) for p in probs) / len(probs)
            prev = last_uncertainty.get(family)
            if prev is not None and 0 < (dt - prev[0]).total_seconds() <= 3*3600:
                du = uncertainty - prev[1]
                if abs(du) >= float(cfg["uncertainty_delta"]):
                    direction = "UP" if du > 0 else "DOWN"
                    events.append(_event(dt, f"PM.{family}.UNCERTAINTY.{direction}", du*100.0,
                                         family=family, feature="UNCERTAINTY", market_count=n))
                    aggregate_written += 1
            last_uncertainty[family] = (dt, uncertainty)

    # Stable ordering and exact duplicate suppression.
    unique: dict[tuple, dict] = {}
    for e in events:
        key = (e["date_dt"], e["event_type"], e["condition_id"], e["signature"])
        unique[key] = e
    out = sorted(unique.values(), key=lambda e: (e["date_dt"], e["event_type"], e["condition_id"]))
    return out, {
        "price_points_scanned": scanned,
        "conditions": len(by_condition),
        "events": len(out),
        "aggregate_events": aggregate_written,
        "families": sorted({p.family for p in points}),
    }


# ---------------------------------------------------------------------------
# SQL / enrichment
# ---------------------------------------------------------------------------

async def _table_exists(engine, name: str) -> bool:
    from sqlalchemy import text
    async with engine.connect() as conn:
        n = (await conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:t
        """), {"t": name})).scalar()
    return bool(int(n or 0))


async def _load_market_meta(engine, markets_table: str, parser_table: str) -> list[dict]:
    from sqlalchemy import text
    if await _table_exists(engine, markets_table):
        sql = text(f"""SELECT condition_id, question, end_date, tags, active, num_outcomes
                       FROM `{markets_table}` WHERE condition_id IS NOT NULL AND question IS NOT NULL""")
    else:
        sql = text(f"""SELECT condition_id, MAX(question) question, MAX(end_date) end_date,
                              MAX(tags) tags, MAX(active) active, MAX(num_outcomes) num_outcomes
                       FROM `{parser_table}`
                       WHERE condition_id IS NOT NULL AND question IS NOT NULL
                       GROUP BY condition_id""")
    async with engine.connect() as conn:
        return [dict(r) for r in (await conn.execute(sql)).mappings().all()]


async def _prepare_map_table(engine, map_table: str, markets: list[dict]) -> dict:
    from sqlalchemy import text
    selected = []
    rejected = 0
    for row in markets:
        cls = classify_market(str(row.get("question") or ""), row.get("tags"))
        if not cls:
            rejected += 1
            continue
        selected.append({
            "condition_id": str(row.get("condition_id") or "")[:66],
            "question": str(row.get("question") or "")[:160],
            "end_date": _coerce_date(row.get("end_date")),
            **cls,
        })

    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{map_table}` (
                condition_id VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                question VARCHAR(160) NULL,
                family VARCHAR(20) NOT NULL,
                orientation VARCHAR(12) NOT NULL,
                signature VARCHAR(240) NOT NULL,
                end_date DATE NULL,
                PRIMARY KEY(condition_id),
                INDEX idx_family(family)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{map_table}`"))
        if selected:
            sql = text(f"""INSERT INTO `{map_table}`
                (condition_id,question,family,orientation,signature,end_date)
                VALUES (:condition_id,:question,:family,:orientation,:signature,:end_date)""")
            for i in range(0, len(selected), 1000):
                await conn.execute(sql, selected[i:i+1000])
    fam_counts = defaultdict(int)
    for r in selected: fam_counts[r["family"]] += 1
    return {"markets_total": len(markets), "markets_selected": len(selected), "markets_rejected": rejected,
            "families": dict(sorted(fam_counts.items()))}


async def _source_max_ts(engine, prices_table: str, parser_table: str) -> datetime | None:
    from sqlalchemy import text
    table = prices_table if await _table_exists(engine, prices_table) else parser_table
    async with engine.connect() as conn:
        return (await conn.execute(text(f"SELECT MAX(price_timestamp) FROM `{table}`"))).scalar()


async def _load_price_chunk(engine, prices_table: str, parser_table: str, map_table: str,
                            start: datetime, end: datetime) -> list[dict]:
    from sqlalchemy import text
    table = prices_table if await _table_exists(engine, prices_table) else parser_table
    sql = text(f"""
        SELECT p.condition_id, p.price_timestamp, p.price,
               m.family, m.orientation, m.signature, m.end_date
        FROM `{table}` p
        JOIN `{map_table}` m ON m.condition_id = p.condition_id
        WHERE p.price_timestamp >= :start AND p.price_timestamp < :end
          AND p.price IS NOT NULL
        ORDER BY p.condition_id, p.price_timestamp
    """)
    async with engine.connect() as conn:
        return [dict(r) for r in (await conn.execute(sql, {"start": start, "end": end})).mappings().all()]


async def _ensure_enriched_tables(engine, enriched: str, state_table: str):
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched}` (
                id BIGINT NOT NULL AUTO_INCREMENT,
                date_dt DATETIME NOT NULL,
                value DOUBLE NOT NULL DEFAULT 0.0,
                pct_change DOUBLE NOT NULL DEFAULT 0.0,
                event_type VARCHAR(120) NOT NULL,
                family VARCHAR(20) NOT NULL,
                feature VARCHAR(24) NOT NULL,
                condition_id VARCHAR(66) NOT NULL DEFAULT '',
                signature VARCHAR(240) NOT NULL DEFAULT '',
                horizon_hours SMALLINT NOT NULL DEFAULT 0,
                orientation VARCHAR(12) NOT NULL DEFAULT 'NEUTRAL',
                market_count INT NOT NULL DEFAULT 1,
                PRIMARY KEY(id),
                UNIQUE KEY uq_event(date_dt,event_type,condition_id,signature(80)),
                INDEX idx_date(date_dt),
                INDEX idx_event(event_type),
                INDEX idx_family_date(family,date_dt)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{state_table}` (
                id TINYINT NOT NULL PRIMARY KEY,
                source_max_ts DATETIME NULL,
                rebuilt_at DATETIME NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text
    cfg = get_service_config()
    ds = cfg["dataset"]
    mc = dict(cfg.get("model") or {})
    parser_table = ds["parser_table"]
    markets_table = ds.get("markets_table", f"{parser_table}_markets")
    prices_table = ds.get("prices_table", f"{parser_table}_prices")
    enriched = ds["enriched_table"]
    map_table = ds["map_table"]
    state_table = ds["state_table"]

    await _ensure_enriched_tables(engine_vlad, enriched, state_table)
    markets = await _load_market_meta(engine_vlad, markets_table, parser_table)
    map_stats = await _prepare_map_table(engine_vlad, map_table, markets)
    source_max = await _source_max_ts(engine_vlad, prices_table, parser_table)
    if source_max is None:
        return {**map_stats, "mode": "empty", "events_written": 0}
    source_max = _coerce_dt(source_max)

    date_from = _coerce_dt((cfg.get("cache") or {}).get("date_from")) or datetime(2026, 3, 1)
    context_h = int(mc.get("context_hours", 30))
    chunk_days = max(1, int(mc.get("chunk_days", 7)))

    async with engine_vlad.connect() as conn:
        state = (await conn.execute(text(f"SELECT source_max_ts FROM `{state_table}` WHERE id=1"))).scalar()
        has_rows = (await conn.execute(text(f"SELECT 1 FROM `{enriched}` LIMIT 1"))).first() is not None

    if state is None or not has_rows:
        mode = "full"
        write_start = date_from
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{enriched}`"))
    else:
        mode = "incremental"
        prev = _coerce_dt(state) or date_from
        write_start = max(date_from, prev - timedelta(hours=context_h))
        # Recompute a causal overlap to absorb late source corrections.
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"DELETE FROM `{enriched}` WHERE date_dt >= :d"), {"d": write_start})

    total_source = total_events = total_agg = 0
    cur = write_start
    insert_sql = text(f"""INSERT INTO `{enriched}`
        (date_dt,value,pct_change,event_type,family,feature,condition_id,signature,horizon_hours,orientation,market_count)
        VALUES (:date_dt,:value,:pct_change,:event_type,:family,:feature,:condition_id,:signature,:horizon_hours,:orientation,:market_count)
        ON DUPLICATE KEY UPDATE value=VALUES(value),pct_change=VALUES(pct_change),market_count=VALUES(market_count)""")

    while cur <= source_max:
        write_end = min(source_max + timedelta(seconds=1), cur + timedelta(days=chunk_days))
        context_start = max(date_from, cur - timedelta(hours=context_h))
        raw = await _load_price_chunk(engine_vlad, prices_table, parser_table, map_table, context_start, write_end)
        total_source += len(raw)
        ev, st = build_events(raw, cfg=mc, write_from=cur, write_to=write_end)
        total_events += len(ev)
        total_agg += int(st.get("aggregate_events") or 0)
        if ev:
            async with engine_vlad.begin() as conn:
                for i in range(0, len(ev), 1000):
                    await conn.execute(insert_sql, ev[i:i+1000])
        cur = write_end

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""INSERT INTO `{state_table}` (id,source_max_ts,rebuilt_at)
            VALUES (1,:s,NOW()) ON DUPLICATE KEY UPDATE source_max_ts=VALUES(source_max_ts),rebuilt_at=NOW()"""), {"s": source_max})

    return {
        **map_stats,
        "mode": mode,
        "source_max_ts": source_max.isoformat(sep=" "),
        "price_rows_read_with_overlap": total_source,
        "events_written": total_events,
        "aggregate_events": total_agg,
        "lookahead_fields_ignored": ["volume", "volume_24h", "liquidity", "spread", "outcome_yes", "outcome_no", "loaded_at"],
    }


# ---------------------------------------------------------------------------
# Brain runtime
# ---------------------------------------------------------------------------

def _apply_var(signed_t1: float, pct: float, var: int, ctx_info: dict) -> float:
    avg = float(ctx_info.get("avg_abs_pct_change") or 0.0)
    if var == 0:
        return signed_t1
    if var == 1:  # stronger-than-normal repricing only
        return signed_t1 if avg > 0 and abs(pct) >= avg else 0.0
    if var == 2:  # magnitude-weighted
        base = avg if avg > 0 else abs(pct)
        return signed_t1 * min(abs(pct) / base, 3.0) if base > 0 else 0.0
    if var == 3:  # extreme tail of each context
        return signed_t1 if avg > 0 and abs(pct) >= 2.0 * avg else 0.0
    return 0.0


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    cfg = get_service_config()
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
