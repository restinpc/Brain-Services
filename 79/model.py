"""Semantic news event model for CNN/NYT/TWP/WSJ/TGD (services 79).

This model intentionally goes beyond the older NER-only idea.  NER tells us *who*
is mentioned; this model classifies *what happened*, to which market it is relevant,
how important/novel the event is and whether independent publishers confirm the same
story.  brain_framework then learns the historical T1/extremum reaction of the market
to those semantic event classes.

Services 79/80 share one materialized dataset: ``vlad_news_semantic_events``.
79 is the baseline, 80 enables Brain Framework ML/reverse-learning.  Enrichment is
incremental and protected by a MySQL advisory lock, so simultaneous rebuilds do not
scan the five news tables twice.
"""
from __future__ import annotations

import threading

import hashlib
import html
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from brain_framework import get_service_config, run_standard_model


# Process-local guard for batch-model derived caches.
_CACHE_LOCK = threading.RLock()
SOURCE_TABLES: dict[str, str] = {
    "cnn": "brain_cnn_news",
    "nyt": "brain_nyt_news",
    "twp": "brain_twp_news",
    "wsj": "brain_wsj_news",
    "tgd": "brain_tgd_news",
}
PAIRS: dict[int, tuple[str, str, str]] = {
    1: ("eur", "usd", "eurusd"),
    3: ("btc", "usd", "btcusd"),
    4: ("eth", "usd", "ethusd"),
}
RATES_TO_PAIR = {
    "brain_rates_eur_usd": 1, "brain_rates_eur_usd_day": 1,
    "brain_rates_btc_usd": 3, "brain_rates_btc_usd_day": 3,
    "brain_rates_eth_usd": 4, "brain_rates_eth_usd_day": 4,
}

RATES_TABLE = "brain_rates_eur_usd"
FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
SHIFT_WINDOW = 12
VAR_RANGE = list(range(8))
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = False
ENRICH_SCHEMA_VERSION = "1"
ENRICH_BATCH = 2000

_WORD_RE = re.compile(r"[a-z0-9][a-z0-9'%-]*", re.I)
_SPACE_RE = re.compile(r"\\s+")
STOP = {
    "the","a","an","and","or","of","to","in","on","for","with","at","by","from","as","is","are",
    "was","were","be","been","being","that","this","these","those","it","its","after","before","over",
    "under","into","about","amid","new","says","say","said","report","reports","live","latest","more",
    "how","why","what","who","when","where","could","would","may","might","will","can","not",
}

TOPIC_RULES: dict[str, tuple[str, ...]] = {
    "monetary_policy": ("federal reserve"," fed ","powell","ecb","european central bank","interest rate","rate cut","rate hike","central bank","monetary policy","quantitative easing","quantitative tightening"),
    "inflation": ("inflation","consumer price"," cpi ","producer price"," ppi ","price pressures","deflation"),
    "labor": ("payroll","jobs report","unemployment","jobless","labor market","labour market","wages","employment"),
    "growth": (" gdp ","gross domestic product","recession","economic growth","economic slowdown","manufacturing pmi","services pmi","retail sales"),
    "fiscal": ("budget deficit","government spending","fiscal","debt ceiling","treasury issuance","sovereign debt","tax cut","tax increase"),
    "banking": ("bank failure","banking crisis","bank run","bankruptcy","liquidity crisis","credit crunch","deposit outflow","capital requirement"),
    "markets": ("stocks","stock market","wall street","bond market","treasury yield","market selloff","market rally","s&p 500","nasdaq","dow jones","volatility"),
    "crypto": ("bitcoin"," btc ","ethereum"," ether "," eth ","cryptocurrency"," crypto ","blockchain","spot etf","stablecoin","defi"),
    "energy": ("oil price","crude oil","brent","wti","opec","natural gas","energy prices"),
    "geopolitics": ("war ","invasion","missile","military strike","ceasefire","sanctions","nuclear","conflict","attack","troops","iran","ukraine","russia","israel","gaza","china taiwan"),
    "trade": ("tariff","trade war","import duty","export ban","trade deal","trade agreement","customs duty"),
    "regulation": ("regulator","regulation","sec ","cftc","antitrust","lawsuit","court ruling","ban ","approval","approved"),
    "corporate": ("earnings","revenue","profit","merger","acquisition","takeover","ipo","layoffs","guidance","quarterly results"),
    "technology": ("artificial intelligence"," ai ","semiconductor","chipmaker","cyberattack","data breach","software","technology"),
    "health": ("pandemic","outbreak","virus","vaccine","covid","disease"),
    "disaster": ("earthquake","hurricane","wildfire","flood","storm","tsunami","explosion"),
}

EVENT_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("rate_hike", ("rate hike","raises rates","raised rates","hike rates","hawkish")),
    ("rate_cut", ("rate cut","cuts rates","cut rates","dovish")),
    ("etf_approval", ("etf approved","approves etf","etf approval","spot bitcoin etf approval","spot ether etf approval")),
    ("ban_crackdown", ("crackdown","bans ","ban on","outlaw","prohibit","restriction")),
    ("sanctions", ("sanctions","sanctioned")),
    ("tariff", ("tariff","import duty","trade war")),
    ("war_escalation", ("invasion","military strike","missile attack","war escalat","troops enter","airstrike")),
    ("ceasefire", ("ceasefire","peace deal","peace agreement")),
    ("hack", ("hack ","hacked","cyberattack","exploit","stolen crypto","data breach")),
    ("bank_failure", ("bank failure","bank collapsed","bank run","bankruptcy","insolvent")),
    ("earnings", ("earnings","quarterly results","revenue","profit")),
    ("acquisition", ("acquisition","acquires","takeover","merger")),
    ("jobs", ("payroll","jobs report","unemployment","jobless claims")),
    ("inflation_release", ("consumer price"," cpi ","producer price"," ppi ","inflation rate")),
    ("growth_release", (" gdp ","pmi ","retail sales","economic growth")),
    ("policy_guidance", ("signals","guidance","expects rates","rate outlook","policy outlook")),
]

POSITIVE = ("beats expectations","better than expected","stronger than expected","surges","rallies","rises","growth accelerates","record high","approval","approved","deal reached","ceasefire","peace deal","inflows","adoption","upgrade","profit rises")
NEGATIVE = ("misses expectations","worse than expected","weaker than expected","plunges","slumps","falls","recession","crisis","default","bankruptcy","hack","cyberattack","ban ","crackdown","war escalat","invasion","sanctions","outflows","downgrade","layoffs")
HAWKISH = ("hawkish","rate hike","raises rates","higher for longer","inflation remains high","tightening")
DOVISH = ("dovish","rate cut","cuts rates","easing","lower rates","quantitative easing")

ASSET_TERMS = {
    "usd": ("dollar"," usd ","federal reserve"," fed ","powell","u.s. economy","us economy","treasury yield","united states economy"),
    "eur": ("euro "," eur ","eurozone","european central bank"," ecb ","lagarde","european economy"),
    "btc": ("bitcoin"," btc ","spot bitcoin","bitcoin etf","crypto market","cryptocurrency"),
    "eth": ("ethereum"," ether "," eth ","spot ether","ethereum etf","defi"),
}
CRYPTO_POS = ("etf approval","approved etf","institutional adoption","inflows","adoption","legal tender","upgrade","staking approval")
CRYPTO_NEG = ("hack","exploit","outflows","ban ","crackdown","liquidation","fraud","exchange collapse","bankruptcy")

MARKET_TOPICS = {"monetary_policy","inflation","labor","growth","fiscal","banking","markets","crypto","energy","geopolitics","trade","regulation"}
MACRO_TOPICS = {"monetary_policy","inflation","labor","growth","fiscal","trade"}
RISK_TOPICS = {"crypto","geopolitics","banking","regulation","disaster","health"}


def _clean(value: Any) -> str:
    s = html.unescape(str(value or "")).lower().replace("\\x00", " ")
    return " " + _SPACE_RE.sub(" ", s).strip() + " "


def _contains_count(text: str, phrases: tuple[str, ...]) -> int:
    return sum(1 for p in phrases if p in text)


def _topic(title: str, body: str, feed: str) -> tuple[str, int]:
    scores: dict[str, int] = {}
    combined = title + " " + body
    for name, phrases in TOPIC_RULES.items():
        score = 2 * _contains_count(title, phrases) + _contains_count(combined, phrases)
        if name in feed:
            score += 1
        if score:
            scores[name] = score
    if not scores:
        return "other", 0
    name, score = max(scores.items(), key=lambda kv: (kv[1], kv[0]))
    return name, int(score)


def _event_class(text: str) -> str:
    for name, phrases in EVENT_RULES:
        if any(p in text for p in phrases):
            return name
    return "generic"


def _generic_polarity(text: str) -> int:
    pos = _contains_count(text, POSITIVE)
    neg = _contains_count(text, NEGATIVE)
    return 1 if pos > neg else (-1 if neg > pos else 0)


def _direct_relevance(asset: str, text: str) -> int:
    hits = _contains_count(text, ASSET_TERMS[asset])
    return min(hits, 3)


def _asset_bias(asset: str, text: str, topic: str, polarity: int) -> tuple[float, int]:
    direct = _direct_relevance(asset, text)
    bias = float(polarity * direct)

    # Central-bank semantics are directional for fiat currencies.
    if asset in ("usd", "eur") and direct:
        hawk = _contains_count(text, HAWKISH)
        dove = _contains_count(text, DOVISH)
        bias += float(hawk - dove) * 1.5

    if asset in ("btc", "eth") and direct:
        bias += float(_contains_count(text, CRYPTO_POS) - _contains_count(text, CRYPTO_NEG)) * 1.5

    # Broad risk regime fallback is deliberately weak. Historical outcomes still
    # decide whether the context has predictive value.
    if direct == 0:
        if topic in ("geopolitics", "banking", "disaster", "health") and polarity < 0:
            if asset == "usd": bias += 0.35
            elif asset in ("btc", "eth"): bias -= 0.35
            elif asset == "eur": bias -= 0.15
        elif topic in ("growth", "markets") and polarity > 0:
            if asset in ("btc", "eth"): bias += 0.25
            elif asset == "usd": bias -= 0.10
    return bias, direct


def _cluster_key(title: str) -> str:
    words = [w for w in _WORD_RE.findall(title) if len(w) >= 4 and w not in STOP and not w.isdigit()]
    # Long/informative title tokens are stable enough to collapse many repeated
    # wire headlines without requiring embeddings at runtime.
    ranked = sorted(set(words), key=lambda w: (-len(w), w))[:10]
    payload = "|".join(sorted(ranked)) or title[:160]
    return hashlib.sha1(payload.encode("utf-8", "ignore")).hexdigest()


def _importance(topic: str, topic_score: int, direct: int, event_class: str, title: str, feed: str) -> tuple[str, float]:
    score = 0.0
    if topic in MARKET_TOPICS: score += 1.0
    score += min(topic_score, 4) * 0.25
    score += min(direct, 3) * 0.55
    if event_class != "generic": score += 0.75
    if any(x in feed for x in ("business","markets","money","economy","politics","world")): score += 0.35
    if any(x in title for x in ("breaking","unexpected","surprise","emergency","record","crisis")): score += 0.50
    cls = "high" if score >= 2.75 else ("mid" if score >= 1.35 else "low")
    return cls, score


def _event_row(press: str, raw: dict[str, Any], pair_id: int) -> dict[str, Any] | None:
    date_dt = raw.get("date_dt")
    if not date_dt:
        return None
    if not isinstance(date_dt, datetime):
        try: date_dt = datetime.fromisoformat(str(date_dt)[:19])
        except ValueError: return None
    title_raw = str(raw.get("title") or "")
    body_raw = str(raw.get("text") or "")
    feed_raw = str(raw.get("feed") or "").lower()[:40]
    title = _clean(title_raw)
    body = _clean(body_raw[:12000])
    combined = title + " " + body

    topic, topic_score = _topic(title, body, feed_raw)
    event_class = _event_class(combined)
    polarity = _generic_polarity(combined)
    base, quote, pair_code = PAIRS[pair_id]
    base_bias, base_direct = _asset_bias(base, combined, topic, polarity)
    quote_bias, quote_direct = _asset_bias(quote, combined, topic, polarity)
    edge = base_bias - quote_bias
    direct = min(base_direct + quote_direct, 3)

    # A neutral semantic event must remain distinguishable from a negative one.
    direction = "up" if edge > 0.05 else ("down" if edge < -0.05 else "neutral")
    importance_class, importance_score = _importance(topic, topic_score, direct, event_class, title, feed_raw)
    relevance_class = "strong" if direct >= 2 else ("direct" if direct >= 1 else ("market" if topic in MARKET_TOPICS else "weak"))
    confidence = min(1.0, 0.15 + 0.12 * topic_score + 0.17 * direct + (0.15 if event_class != "generic" else 0.0))

    return {
        "date_dt": date_dt, "press": press, "source_news_id": int(raw["source_news_id"]),
        "feed": feed_raw, "pair_id": pair_id, "pair_code": pair_code,
        "cluster_key": _cluster_key(title_raw.lower()),
        "topic": topic, "event_class": event_class, "direction": direction,
        "importance_class": importance_class, "relevance_class": relevance_class,
        "importance_score": importance_score, "relevance_score": direct,
        "polarity": polarity, "edge": edge, "confidence": confidence,
        "novelty_class": "novel", "source_count": 1, "repeat_count": 1,
        # source/confirmation are useful context, but raw feed is kept as metadata
        # to avoid exploding contexts into hundreds of tiny RSS buckets.
        "event_type": f"{pair_code}|{press}|{topic}|{event_class}|{direction}|{importance_class}|solo",
    }


async def _ensure_tables(engine_vlad, table: str, meta: str) -> None:
    from sqlalchemy import text
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `date_dt` DATETIME NOT NULL,
              `press` VARCHAR(3) NOT NULL,
              `source_news_id` INT NOT NULL,
              `feed` VARCHAR(40) NOT NULL DEFAULT '',
              `pair_id` TINYINT NOT NULL,
              `pair_code` VARCHAR(8) NOT NULL,
              `cluster_key` CHAR(40) NOT NULL,
              `topic` VARCHAR(32) NOT NULL,
              `event_class` VARCHAR(32) NOT NULL,
              `direction` VARCHAR(8) NOT NULL,
              `importance_class` VARCHAR(8) NOT NULL,
              `relevance_class` VARCHAR(8) NOT NULL,
              `importance_score` DOUBLE NOT NULL DEFAULT 0,
              `relevance_score` TINYINT NOT NULL DEFAULT 0,
              `polarity` TINYINT NOT NULL DEFAULT 0,
              `edge` DOUBLE NOT NULL DEFAULT 0,
              `confidence` DOUBLE NOT NULL DEFAULT 0,
              `novelty_class` VARCHAR(8) NOT NULL DEFAULT 'novel',
              `source_count` TINYINT NOT NULL DEFAULT 1,
              `repeat_count` INT NOT NULL DEFAULT 1,
              `event_type` VARCHAR(180) NOT NULL,
              `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              UNIQUE KEY `uk_source_pair` (`press`,`source_news_id`,`pair_id`),
              KEY `idx_date` (`date_dt`), KEY `idx_pair_date` (`pair_id`,`date_dt`),
              KEY `idx_cluster` (`cluster_key`), KEY `idx_event_type` (`event_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='shared semantic news events for Brain services 79/80'
        """))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{meta}` (
              `meta_key` VARCHAR(64) NOT NULL, `meta_value` VARCHAR(255) NOT NULL,
              `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`meta_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def _meta_get(engine, table: str, key: str) -> str | None:
    from sqlalchemy import text
    async with engine.connect() as conn:
        row = (await conn.execute(text(f"SELECT meta_value FROM `{table}` WHERE meta_key=:k"), {"k":key})).fetchone()
    return str(row[0]) if row else None


async def _meta_set(engine, table: str, key: str, value: Any) -> None:
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.execute(text(f"INSERT INTO `{table}`(meta_key,meta_value) VALUES(:k,:v) ON DUPLICATE KEY UPDATE meta_value=VALUES(meta_value)"), {"k":key,"v":str(value)})


async def _fetch_batch(engine_brain, source_table: str, last_id: int) -> list[dict[str, Any]]:
    from sqlalchemy import text
    async with engine_brain.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT id AS source_news_id, title, text, date AS date_dt, feed
            FROM `{source_table}`
            WHERE id > :last_id AND date IS NOT NULL
            ORDER BY id ASC LIMIT {ENRICH_BATCH}
        """), {"last_id": int(last_id)})
        return [dict(r) for r in res.mappings().all()]


async def _upsert_rows(engine_vlad, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows: return
    from sqlalchemy import text
    sql = text(f"""
        INSERT INTO `{table}` (
          date_dt,press,source_news_id,feed,pair_id,pair_code,cluster_key,topic,event_class,
          direction,importance_class,relevance_class,importance_score,relevance_score,polarity,
          edge,confidence,novelty_class,source_count,repeat_count,event_type
        ) VALUES (
          :date_dt,:press,:source_news_id,:feed,:pair_id,:pair_code,:cluster_key,:topic,:event_class,
          :direction,:importance_class,:relevance_class,:importance_score,:relevance_score,:polarity,
          :edge,:confidence,:novelty_class,:source_count,:repeat_count,:event_type
        ) ON DUPLICATE KEY UPDATE
          date_dt=VALUES(date_dt),feed=VALUES(feed),cluster_key=VALUES(cluster_key),topic=VALUES(topic),
          event_class=VALUES(event_class),direction=VALUES(direction),importance_class=VALUES(importance_class),
          relevance_class=VALUES(relevance_class),importance_score=VALUES(importance_score),
          relevance_score=VALUES(relevance_score),polarity=VALUES(polarity),edge=VALUES(edge),
          confidence=VALUES(confidence)
    """)
    async with engine_vlad.begin() as conn:
        for i in range(0, len(rows), 600):
            await conn.execute(sql, rows[i:i+600])


async def _refresh_clusters(engine_vlad, table: str, cluster_keys: set[str]) -> None:
    """Refresh novelty/cross-source metadata only for clusters touched by the batch."""
    if not cluster_keys: return
    from sqlalchemy import text
    keys = list(cluster_keys)
    for start in range(0, len(keys), 500):
        chunk = keys[start:start+500]
        ph = ",".join(f":k{i}" for i in range(len(chunk)))
        params = {f"k{i}":v for i,v in enumerate(chunk)}
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT cluster_key, COUNT(DISTINCT CONCAT(press,':',source_news_id)) AS repeats,
                       COUNT(DISTINCT press) AS sources, MIN(date_dt) AS first_dt
                FROM `{table}` WHERE pair_id=1 AND cluster_key IN ({ph})
                GROUP BY cluster_key
            """), params)
            stats = [dict(r) for r in res.mappings().all()]
        async with engine_vlad.begin() as conn:
            for s in stats:
                confirm = "multi" if int(s["sources"] or 0) >= 2 else "solo"
                await conn.execute(text(f"""
                    UPDATE `{table}`
                    SET source_count=:sources, repeat_count=:repeats,
                        novelty_class=CASE WHEN date_dt=:first_dt THEN 'novel' ELSE 'repeat' END,
                        event_type=CONCAT(pair_code,'|',press,'|',topic,'|',event_class,'|',direction,'|',importance_class,'|',:confirm)
                    WHERE cluster_key=:cluster
                """), {"sources":int(s["sources"] or 1),"repeats":int(s["repeats"] or 1),"first_dt":s["first_dt"],"confirm":confirm,"cluster":s["cluster_key"]})


async def _enrich_unlocked(engine_vlad, engine_brain) -> dict[str, Any]:
    cfg = get_service_config() or {}
    dcfg = cfg.get("dataset") or {}
    table = str(dcfg.get("enriched_table") or "vlad_news_semantic_events")
    meta = f"{table}_meta"
    await _ensure_tables(engine_vlad, table, meta)

    version = await _meta_get(engine_vlad, meta, "schema_version")
    full = version != ENRICH_SCHEMA_VERSION
    if full:
        from sqlalchemy import text
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{table}`"))
        for press in SOURCE_TABLES:
            await _meta_set(engine_vlad, meta, f"last_{press}_id", 0)
        await _meta_set(engine_vlad, meta, "schema_version", ENRICH_SCHEMA_VERSION)

    result: dict[str, Any] = {"mode":"full" if full else "incremental", "rows_written":0, "articles":0, "sources":{}}
    for press, source_table in SOURCE_TABLES.items():
        try: last_id = int(await _meta_get(engine_vlad, meta, f"last_{press}_id") or 0)
        except (TypeError, ValueError): last_id = 0
        source_articles = source_rows = 0
        while True:
            batch = await _fetch_batch(engine_brain, source_table, last_id)
            if not batch: break
            out: list[dict[str, Any]] = []
            touched: set[str] = set()
            for raw in batch:
                source_articles += 1
                for pair_id in PAIRS:
                    row = _event_row(press, raw, pair_id)
                    if row:
                        out.append(row); touched.add(row["cluster_key"])
                last_id = max(last_id, int(raw["source_news_id"]))
            await _upsert_rows(engine_vlad, table, out)
            await _refresh_clusters(engine_vlad, table, touched)
            await _meta_set(engine_vlad, meta, f"last_{press}_id", last_id)
            source_rows += len(out)
            if len(batch) < ENRICH_BATCH: break
        result["articles"] += source_articles
        result["rows_written"] += source_rows
        result["sources"][press] = {"articles":source_articles,"rows":source_rows,"last_id":last_id}
    if result["articles"] == 0: result["mode"] = "noop"
    return result


async def enrich_dataset(engine_vlad, engine_brain) -> dict[str, Any]:
    from sqlalchemy import text
    cfg = get_service_config() or {}
    table = str(((cfg.get("dataset") or {}).get("enriched_table")) or "vlad_news_semantic_events")
    lock_name = f"news_semantic_enrich:{table}"[:64]
    async with engine_vlad.connect() as conn:
        got = (await conn.execute(text("SELECT GET_LOCK(:n,600)"), {"n":lock_name})).scalar()
        if int(got or 0) != 1:
            return {
                "mode": "locked",
                "reason": "another news service is enriching the shared dataset",
                "lock_name": lock_name,
            }
        try:
            return await _enrich_unlocked(engine_vlad, engine_brain)
        finally:
            try: await conn.execute(text("SELECT RELEASE_LOCK(:n)"), {"n":lock_name})
            except Exception: pass


def _pair_id(dataset_index: dict | None) -> int:
    table = str((dataset_index or {}).get("rates_table") or RATES_TABLE)
    return int(RATES_TO_PAIR.get(table, 1))


def _allowed(row: dict[str, Any], var: int) -> bool:
    topic = str(row.get("topic") or "other")
    relevance = int(row.get("relevance_score") or 0)
    importance = str(row.get("importance_class") or "low")
    sources = int(row.get("source_count") or 1)
    novelty = str(row.get("novelty_class") or "novel")
    if var == 0: return True
    if var == 1: return topic in MARKET_TOPICS or relevance >= 1
    if var == 2: return relevance >= 2
    if var == 3: return importance == "high"
    if var == 4: return sources >= 2
    if var == 5: return novelty == "novel"
    if var == 6: return topic in MACRO_TOPICS
    if var == 7: return topic in RISK_TOPICS
    return False


def _event_parser(pair_id: int, var: int):
    def parse(row: dict[str, Any]):
        try:
            if int(row.get("pair_id") or 0) != pair_id or not _allowed(row, var): return None
        except (TypeError, ValueError): return None
        dt = row.get("date_dt") or row.get("date")
        if isinstance(dt, str):
            try: dt = datetime.fromisoformat(dt[:19])
            except ValueError: return None
        if not isinstance(dt, datetime): return None
        return dt, float(row.get("edge") or 0.0), str(row.get("event_type") or "")
    return parse


def _apply_var(stored_t1: float, analog_edge: float, var: int, ctx_info: dict) -> float:
    if var == 3:
        # High-importance mode also lets stronger historical semantic edges carry
        # somewhat more weight without allowing a single headline to dominate.
        scale = min(max(abs(float(analog_edge)), 0.35), 2.5)
        return float(stored_t1) * scale
    return float(stored_t1)


def _ml_signal_fn(calc_type:int, ctx_id:int, shift:int, mode0_value:float, has_extremum_hits:bool, current_edge:float, outcomes:int, direction:float, ctx_info:dict):
    if calc_type not in (3,4): return None
    result: dict[str,float] = {}
    if mode0_value != 0.0: result[f"{ctx_id}_0_{shift}"] = round(float(mode0_value),6)
    if has_extremum_hits: result[f"{ctx_id}_1_{shift}"] = 1.0 if direction >= 0 else -1.0
    return result


def model(rates:list[dict], dataset:list[dict], date:datetime, *, type:int=0, var:int=0, param:str="", dataset_index:dict|None=None) -> dict[str,float]:
    if not dataset or not date: return {}
    pair_id = _pair_id(dataset_index)
    local_index = dict(dataset_index or {})
    # Causal guard: never let the event selector see news published after target.
    local_index["full_dataset"] = dataset
    return run_standard_model(
        rates, dataset, date, type=int(type), var=int(var), dataset_index=local_index,
        shift_window=SHIFT_WINDOW, apply_var_fn=_apply_var, min_occurrence=3,
        get_event_fn=_event_parser(pair_id, int(var)), signal_fn=_ml_signal_fn,
        rare_event_fn=lambda _ctx: True, rare_occurrence_max=0,
    )
