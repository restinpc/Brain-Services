"""
Service 93 — Unified Crypto News Algorithmic ML
================================================

Four raw sources:
    CoinDesk + Investing + TradingView + Binance Square

Core design follows service 80:
    causal ingestion time
      -> bilingual canonical normalization
      -> frozen TF-IDF word/bigram representation
      -> MiniBatchNMF broad latent topics
      -> TruncatedSVD semantic subspace
      -> topic-local MiniBatchKMeans event clusters
      -> causal novelty + provider-aware confirmation
      -> Brain historical T1/extremum reaction + reverse learning.

Important differences from service 80:
* confirmation is counted by the REAL origin provider, not by aggregator channel;
* the same provider syndicated through Investing/TradingView/Binance does not
  become an independent second trading event;
* Binance Square unknown-user noise and stale historical backfills are filtered;
* event time is Brain ingestion time (created_at/inserted_at), never publisher time;
* BTC/ETH relevance and channel/source quality remain causal row features and
  never alter the frozen semantic event identity.

Anti-look-ahead guarantees:
1. NLP artifact fits only rows with available_at < fixed train_cutoff.
2. Frozen artifact is never refit on later rows unless configuration signature changes.
3. Novelty/confirmation inspect only rows strictly earlier in chronological order.
4. Adding future rows cannot rewrite historical enriched rows.
5. Market prices/T1 are never inputs to TF-IDF/NMF/SVD/KMeans.
"""
from __future__ import annotations

import hashlib
import html
import io
import math
import re
import bisect
import threading
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from brain_framework import get_service_config

# ---------------------------------------------------------------------------
# Framework contract
# ---------------------------------------------------------------------------

SERVICE_ID = 93
PORT = 8955
PRETEST_ALLOW_EMPTY = True
RATES_TABLE = "brain_rates_eur_usd"
FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
SHIFT_WINDOW = 12
VAR_RANGE = list(range(16))
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = True

ENRICH_SCHEMA_VERSION = "4"
ALGO_VERSION = "crypto4-tfidf-nmf-svd-hierkmeans-provider-causal-v4"

SOURCES: dict[str, dict[str, str]] = {
    "cd": {"table": "vlad_coindesk_crypto_news", "channel": "COINDESK", "date_col": "created_at"},
    "iv": {"table": "vlad_investing_crypto_news", "channel": "INVESTING", "date_col": "created_at"},
    "tv": {"table": "vlad_tradingview_crypto", "channel": "TRADINGVIEW", "date_col": "created_at"},
    "bn": {"table": "vlad_binance_news", "channel": "BINANCE", "date_col": "inserted_at"},
}

DEFAULT_TRAIN_CUTOFF = "2026-05-01 00:00:00"
DEFAULT_SAMPLE_PER_SOURCE = 3_000
DEFAULT_MAX_FEATURES = 8_000
DEFAULT_NMF_TOPICS = 20
DEFAULT_SVD_COMPONENTS = 40
DEFAULT_TARGET_EVENT_DOCS = 180
DEFAULT_MIN_EVENT_SUPPORT = 40
DEFAULT_MAX_LOCAL_CLUSTERS = 20
DEFAULT_TEXT_CHARS = 1_800
DEFAULT_PERIOD_DAYS = 10
DEFAULT_NOVELTY_DAYS = 7
DEFAULT_CONFIRM_HOURS = 12
DEFAULT_CONFIRM_SIM = 0.76
DEFAULT_DUPLICATE_MINUTES = 90
DEFAULT_DUPLICATE_SIM = 0.90
DEFAULT_FAST_CONFIRM_MINUTES = 120
DEFAULT_MAX_SOURCE_AGE_DAYS = 2

TOPIC_FOCUS_MIN = 0.84
CLUSTER_SIM_MIN = 0.66
NOVELTY_MIN = 0.32

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_WS_RE = re.compile(r"\s+")
_NUMBER_RE = re.compile(r"(?<!\w)(\d{2,8}(?:[.,]\d+)?)\b")
_BOILERPLATE_RE = re.compile(
    r"(?:\bcontinue reading\b|\bread (?:the )?full (?:article|story)\b|"
    r"\bread article\b|\bclick here\b|\bsign up\b|\bsubscribe\b|"
    r"\bfollow\b|\breport\b|\bblock user\b)", re.I,
)

BILINGUAL_STOP = [
    "the","and","for","are","was","were","with","from","into","after","before","over","under",
    "about","their","there","this","that","which","while","will","would","could","should","have",
    "has","had","been","said","says","according","report","reports","news","today","market","markets",
    "crypto","cryptocurrency","of","to","in","on","as","at","by","an","a","is","it","its","or",
    "что","как","для","это","после","перед","при","или","его","ее","их","уже","еще","также",
    "рынок","рынка","рынке","крипто","криптовалют","криптовалюты","сообщил","сообщила","сообщили",
    "заявил","заявила","года","году","год","данным","около","более","менее","будет","могут","может",
    "на","по","из","от","до","за","и","в","с","о","не","но","а","у","к",
]

# These anchors are not event labels. They only normalize obvious cross-language
# entities/actions so an English and Russian description of the same story can
# occupy nearby TF-IDF/SVD geometry.
ANCHOR_PATTERNS: dict[str, tuple[str, ...]] = {
    "BTC": (r"\bbitcoin\b", r"\bbtc\b", r"биткоин\w*"),
    "ETH": (r"\bethereum\b", r"\bether\b", r"\beth\b", r"эфириум\w*", r"\bэфир\b"),
    "ETF": (r"\betf\b", r"биржев\w* фонд", r"спотов\w* фонд"),
    "SEC": (r"\bsec\b", r"securities and exchange commission", r"комисси\w* по ценным бумаг"),
    "FED": (r"\bfed\b", r"federal reserve", r"\bфрс\b", r"федеральн\w* резерв"),
    "HACK": (r"\bhack(?:ed|ing)?\b", r"exploit", r"breach", r"взлом", r"эксплойт", r"уязвимост"),
    "LIQUIDATION": (r"liquidat", r"ликвидац"),
    "APPROVE": (r"approv", r"одобр", r"разрешил", r"разрешила"),
    "FILE": (r"\bfiling\b", r"\bfiled\b", r"\bfiles\b", r"s-1", r"19b-4", r"подал\w* заяв", r"подан\w* заяв"),
    "REJECT": (r"reject", r"deny", r"отклони", r"отказал"),
    "BAN": (r"\bban(?:ned)?\b", r"запрет"),
    "BUY": (r"\bbuy(?:s|ing)?\b", r"purchas", r"acquir", r"купил", r"покупк", r"приобрел", r"приобрела"),
    "SELL": (r"\bsell(?:s|ing)?\b", r"\bsold\b", r"продал", r"продаж"),
    "INFLOW": (r"\binflow", r"приток"),
    "OUTFLOW": (r"\boutflow", r"отток"),
    "LIST": (r"\blisting\b", r"\blisted\b", r"листинг"),
    "DELIST": (r"delist", r"делист"),
    "WHALE": (r"\bwhale", r"whale alert", r"\bкит\w*"),
    "STABLECOIN": (r"stablecoin", r"стейблкоин"),
    "REGULATION": (r"regulat", r"legislation", r"законопроект", r"регулирован"),
    "DERIVATIVES": (r"funding rate", r"open interest", r"futures", r"options", r"дериватив", r"фьючерс", r"опцион"),
    "BINANCE": (r"\bbinance\b",),
    "COINBASE": (r"\bcoinbase\b",),
    "BLACKROCK": (r"\bblackrock\b",),
    "GRAYSCALE": (r"\bgrayscale\b",),
    "FIDELITY": (r"\bfidelity\b",),
    "TETHER": (r"\btether\b", r"\busdt\b"),
    "CIRCLE": (r"\bcircle\b", r"\busdc\b"),
    "STRATEGY": (r"\bmicrostrategy\b", r"\bstrategy\b"),
    "TRUMP": (r"\btrump\b", r"трамп"),
}
_COMPILED_ANCHORS = {k: tuple(re.compile(p, re.I) for p in v) for k, v in ANCHOR_PATTERNS.items()}

PROVIDER_ALIASES = {
    "coindesk":"COINDESK", "investing.com":"INVESTING", "bits.media":"BITS_MEDIA",
    "happy coin news":"HAPPY_COIN", "getblock magazine":"GETBLOCK", "getblock":"GETBLOCK",
    "ifx":"IFX", "beincrypto":"BEINCRYPTO", "beincrypto global":"BEINCRYPTO",
    "reuters":"REUTERS", "chainwire":"CHAINWIRE", "forklog":"FORKLOG",
    "рбк крипто":"RBC_CRYPTO", "рбк инвестиции":"RBC_INVEST", "coindar":"COINDAR",
    "cointelegraph":"COINTELEGRAPH", "tradingview":"TRADINGVIEW", "binance news":"BINANCE_NEWS",
    "blockbeats_en":"BLOCKBEATS", "律动blockbeats":"BLOCKBEATS", "foresight_news":"FORESIGHT",
    "u.today":"UTODAY", "panews":"PANEWS", "bsc news":"BSC_NEWS", "dl news":"DL_NEWS",
    "cryptopotato":"CRYPTOPOTATO", "cryptonewscom":"CRYPTONEWS", "blockonomi":"BLOCKONOMI",
    "cryptopolitan":"CRYPTOPOLITAN", "odaily星球日报":"ODAILY", "深潮 techflow":"TECHFLOW",
    "coinpedia fintech news":"COINPEDIA", "bitcoinworld":"BITCOINWORLD",
}
ORIGIN_MARKERS = (
    (r"\(cointelegraph\)", "COINTELEGRAPH"), (r"according to cointelegraph", "COINTELEGRAPH"),
    (r"blockbeats news", "BLOCKBEATS"), (r"according to blockbeats", "BLOCKBEATS"),
    (r"foresight news", "FORESIGHT"), (r"according to reuters", "REUTERS"),
    (r"according to bloomberg", "BLOOMBERG"), (r"whale alert", "WHALE_ALERT"),
)


def _nlp_cfg() -> dict[str, Any]:
    cfg = get_service_config() or {}
    raw = cfg.get("nlp") or {}
    cutoff_s = str(raw.get("train_cutoff") or DEFAULT_TRAIN_CUTOFF)
    try:
        cutoff = datetime.fromisoformat(cutoff_s[:19])
    except ValueError:
        cutoff = datetime.fromisoformat(DEFAULT_TRAIN_CUTOFF)
    return {
        "train_cutoff": cutoff,
        "sample_per_source": max(2_000, int(raw.get("sample_per_source", DEFAULT_SAMPLE_PER_SOURCE))),
        "max_features": max(5_000, int(raw.get("max_features", DEFAULT_MAX_FEATURES))),
        "nmf_topics": max(8, int(raw.get("nmf_topics", DEFAULT_NMF_TOPICS))),
        "svd_components": max(24, int(raw.get("svd_components", DEFAULT_SVD_COMPONENTS))),
        "target_event_docs": max(80, int(raw.get("target_event_docs", DEFAULT_TARGET_EVENT_DOCS))),
        "min_event_support": max(20, int(raw.get("min_event_support", DEFAULT_MIN_EVENT_SUPPORT))),
        "max_local_clusters": max(2, int(raw.get("max_local_clusters", DEFAULT_MAX_LOCAL_CLUSTERS))),
        "text_chars": max(300, int(raw.get("text_chars", DEFAULT_TEXT_CHARS))),
        "period_days": max(1, int(raw.get("period_days", DEFAULT_PERIOD_DAYS))),
        "novelty_days": max(1, int(raw.get("novelty_days", DEFAULT_NOVELTY_DAYS))),
        "confirm_hours": max(1, int(raw.get("confirmation_hours", DEFAULT_CONFIRM_HOURS))),
        "confirm_similarity": float(raw.get("confirmation_similarity", DEFAULT_CONFIRM_SIM)),
        "duplicate_minutes": max(1, int(raw.get("duplicate_minutes", DEFAULT_DUPLICATE_MINUTES))),
        "duplicate_similarity": float(raw.get("duplicate_similarity", DEFAULT_DUPLICATE_SIM)),
        "fast_confirm_minutes": max(1, int(raw.get("fast_confirmation_minutes", DEFAULT_FAST_CONFIRM_MINUTES))),
        "max_source_age_days": max(0, int(raw.get("max_source_age_days", DEFAULT_MAX_SOURCE_AGE_DAYS))),
    }


def _signature(cfg: dict[str, Any]) -> str:
    payload = "|".join([
        ALGO_VERSION, cfg["train_cutoff"].isoformat(sep=" "), str(cfg["sample_per_source"]),
        str(cfg["max_features"]), str(cfg["nmf_topics"]), str(cfg["svd_components"]),
        str(cfg["target_event_docs"]), str(cfg["min_event_support"]), str(cfg["max_local_clusters"]),
        str(cfg["text_chars"]), str(cfg["novelty_days"]), str(cfg["confirm_hours"]),
        f"{cfg['confirm_similarity']:.6f}", str(cfg["duplicate_minutes"]),
        f"{cfg['duplicate_similarity']:.6f}", str(cfg["fast_confirm_minutes"]), str(cfg["max_source_age_days"]),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _clean_news_text(value: str) -> str:
    value = html.unescape(str(value or "")).replace("\x00", " ")
    value = _URL_RE.sub(" ", value)
    value = _BOILERPLATE_RE.sub(" ", value)
    return _WS_RE.sub(" ", value).strip()


def _anchors(text: str) -> set[str]:
    """Fast bilingual canonical anchors; used only for normalization/dedup."""
    low=text.lower(); words=set(re.findall(r"[a-zа-яё0-9]+",low,re.I)); out=set()
    if "bitcoin" in words or "btc" in words or "биткоин" in low: out.add("BTC")
    if "ethereum" in words or "ether" in words or "eth" in words or "эфириум" in low or " эфир " in f" {low} ": out.add("ETH")
    if "etf" in words or "биржев" in low and "фонд" in low or "спотов" in low and "фонд" in low: out.add("ETF")
    if "sec" in words or "securities and exchange commission" in low or ("комисси" in low and "ценн" in low): out.add("SEC")
    if "fed" in words or "federal reserve" in low or "фрс" in words or ("федеральн" in low and "резерв" in low): out.add("FED")
    if any(x in low for x in ("hack","exploit","breach","взлом","эксплойт","уязвимост")): out.add("HACK")
    if "liquidat" in low or "ликвидац" in low: out.add("LIQUIDATION")
    if "approv" in low or "одобр" in low or "разрешил" in low: out.add("APPROVE")
    if "filing" in low or "filed" in words or "files" in words or "s-1" in low or "19b-4" in low or ("заяв" in low and ("подал" in low or "подан" in low)): out.add("FILE")
    if "reject" in low or "deny" in low or "отклони" in low or "отказал" in low: out.add("REJECT")
    if "ban" in words or "banned" in words or "запрет" in low: out.add("BAN")
    if any(x in low for x in (" buying "," buys ","purchase","acquir","купил","покупк","приобрел","приобрела")): out.add("BUY")
    if any(x in low for x in (" selling "," sells "," sold ","продал","продаж")): out.add("SELL")
    if "inflow" in low or "приток" in low: out.add("INFLOW")
    if "outflow" in low or "отток" in low: out.add("OUTFLOW")
    if "delist" in low or "делист" in low: out.add("DELIST")
    elif "listing" in low or "listed" in words or "листинг" in low: out.add("LIST")
    if "whale" in low or "кит" in low: out.add("WHALE")
    if "stablecoin" in low or "стейблкоин" in low: out.add("STABLECOIN")
    if "regulat" in low or "legislation" in low or "законопроект" in low or "регулирован" in low: out.add("REGULATION")
    if "funding rate" in low or "open interest" in low or "futures" in low or "options" in low or "дериватив" in low or "фьючерс" in low or "опцион" in low: out.add("DERIVATIVES")
    for token,name in (("binance","BINANCE"),("coinbase","COINBASE"),("blackrock","BLACKROCK"),("grayscale","GRAYSCALE"),("fidelity","FIDELITY"),("tether","TETHER"),("circle","CIRCLE"),("microstrategy","STRATEGY"),("trump","TRUMP")):
        if token in words: out.add(name)
    if "usdt" in words: out.add("TETHER")
    if "usdc" in words: out.add("CIRCLE")
    if "трамп" in words: out.add("TRUMP")
    return out


def _number_anchors(text: str) -> set[str]:
    out: set[str] = set()
    for m in _NUMBER_RE.finditer(text):
        try:
            v = float(m.group(1).replace(",", "."))
        except ValueError:
            continue
        if v >= 10:
            out.add(str(int(round(v))))
        if len(out) >= 5:
            break
    return out


def _document_text(row: dict[str, Any], text_chars: int) -> str:
    title=_clean_news_text(str(row.get("title") or "")); body=_clean_news_text(str(row.get("text") or "")[:text_chars])
    combined=f"{title} {body}"
    anchors=set(row.get("anchors") or ()) or _anchors(combined)
    nums=set(row.get("number_anchors") or ()) or _number_anchors(combined)
    canon=" ".join(f"canon_{a.lower()}" for a in sorted(anchors)); numtxt=" ".join(f"num_{n}" for n in sorted(nums))
    return _WS_RE.sub(" ",f"{title} {title} {combined} {canon} {canon} {numtxt}").strip()


def _provider_code(provider: str) -> str:
    key = _clean_news_text(provider).lower()
    if key in PROVIDER_ALIASES:
        return PROVIDER_ALIASES[key]
    key2 = re.sub(r"[^a-zа-яё0-9]+", "", key)
    for raw, code in PROVIDER_ALIASES.items():
        if re.sub(r"[^a-zа-яё0-9]+", "", raw.lower()) == key2:
            return code
    return "OTHER_MEDIA"


def _clean_binance_author(raw: str) -> tuple[str, bool]:
    s = _clean_news_text(raw)
    verified = "verified binance official account" in s.lower()
    s = re.sub(r"\d+(?:s|m|h|d|w)(?:・Verified Binance official account)?(?:Follow.*)?$", "", s, flags=re.I)
    s = re.sub(r"(?:Follow|View Hidden Replies|Report|Block User).*$", "", s, flags=re.I).strip(" ・")
    return (s or "Binance Square"), verified


def _origin_provider(text: str, fallback: str) -> str:
    low = text.lower()
    for pat, code in ORIGIN_MARKERS:
        if re.search(pat, low, re.I):
            return code
    return _provider_code(fallback)


def _source_class(channel: str, provider: str, raw_author: str = "", author_verified: Any = None) -> tuple[str, str, int]:
    if channel == "COINDESK":
        return "DIRECT", "COINDESK", 3
    if channel in {"INVESTING", "TRADINGVIEW"}:
        code = _provider_code(provider)
        return ("KNOWN_MEDIA", code, 2) if code != "OTHER_MEDIA" else ("AGGREGATED_MEDIA", code, 1)
    author, old_verified = _clean_binance_author(raw_author or provider)
    verified = old_verified or str(author_verified).lower() in {"1", "true", "yes"}
    code = _provider_code(author)
    if author.lower().startswith("binance news") or verified:
        return "OFFICIAL", "BINANCE_NEWS", 3
    if code != "OTHER_MEDIA":
        return "KNOWN_MEDIA", code, 2
    return "USER", "BINANCE_USER", 0


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "": return None
    if isinstance(value, datetime): return value.replace(tzinfo=None)
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try: return datetime.strptime(s[:26], fmt)
        except ValueError: pass
    try: return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception: return None


def _source_absolute_date(label: str) -> datetime | None:
    s = _clean_news_text(label).lower().replace("г.", "").replace("г", "")
    if not re.search(r"\b20\d{2}\b", s):
        return None
    months = {"янв":1,"фев":2,"мар":3,"апр":4,"мая":5,"май":5,"июн":6,"июл":7,"авг":8,"сен":9,"окт":10,"ноя":11,"дек":12,
              "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    m = re.search(r"(\d{1,2})\s+([a-zа-яё.]+)\s+(20\d{2})", s, re.I)
    if m:
        mon = months.get(m.group(2).strip(".")[:3])
        if mon:
            try: return datetime(int(m.group(3)), mon, int(m.group(1)))
            except ValueError: return None
    return _parse_dt(label)


def _prepare_source_row(row: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any] | None:
    dt = _parse_dt(row.get("date_dt"))
    if dt is None: return None
    title = _clean_news_text(row.get("title") or "")
    body = _clean_news_text(row.get("text") or "")
    if len(title) < 10 and len(body) < 60: return None
    channel = str(row.get("channel") or "")
    raw_provider = _clean_news_text(row.get("provider") or "")
    source_class, provider, tier = _source_class(channel, raw_provider, row.get("raw_author") or "", row.get("author_verified"))
    combined = f"{title} {body[:3000]}"
    if channel == "BINANCE":
        origin = _origin_provider(combined, raw_provider)
        if origin == "OTHER_MEDIA" and provider != "OTHER_MEDIA": origin = provider
    else:
        origin = provider
    anchors = _anchors(combined)
    crypto = bool({"BTC","ETH","ETF","HACK","LIQUIDATION","STABLECOIN","DERIVATIVES"} & anchors) or bool(re.search(r"crypto|bitcoin|ethereum|blockchain|token|крипто|биткоин|эфириум|блокчейн", combined, re.I))
    if not crypto: return None
    if source_class == "USER" and not ({"BTC","ETH","HACK","LIQUIDATION","ETF","REGULATION"} & anchors):
        return None
    src_abs = _source_absolute_date(str(row.get("published_label") or ""))
    if src_abs is not None and dt - src_abs > timedelta(days=int(cfg["max_source_age_days"])):
        return None
    out = dict(row)
    out.update({
        "date_dt": dt, "title": title, "text": body, "provider": provider,
        "origin_provider": origin, "source_class": source_class, "source_tier": tier,
        "btc_relevant": int("BTC" in anchors), "eth_relevant": int("ETH" in anchors),
        "anchors": anchors, "number_anchors": _number_anchors(combined),
    })
    return out


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b: return 0.0
    return len(a & b) / float(len(a | b))


def _effective_similarity(vec_sim: float, anchors_a: set[str], anchors_b: set[str], nums_a: set[str], nums_b: set[str]) -> float:
    """Semantic SVD similarity plus conservative cross-language anchor bridge."""
    sim = max(-1.0, min(1.0, float(vec_sim)))
    aj = _jaccard(anchors_a, anchors_b)
    shared = anchors_a & anchors_b
    # Require multiple matching semantic anchors before overriding weak lexical
    # similarity. This avoids merging every generic BTC market article together.
    if len(shared) >= 3 and aj >= 0.50:
        sim = max(sim, min(0.90, 0.72 + 0.18 * aj))
    elif len(shared) >= 2 and ({"ETF","HACK","LIQUIDATION","APPROVE","FILE","REJECT"} & shared):
        sim = max(sim, 0.78)
    if nums_a and nums_b and nums_a & nums_b and len(shared) >= 2:
        sim = max(sim, 0.82)
    return sim


def _require_nlp_stack():
    try:
        import joblib  # noqa: F401
        from sklearn.cluster import MiniBatchKMeans  # noqa: F401
        from sklearn.decomposition import MiniBatchNMF, TruncatedSVD  # noqa: F401
        from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: F401
        from sklearn.svm import LinearSVC  # noqa: F401
    except Exception as exc:
        raise RuntimeError("Service 93 requires scikit-learn + joblib") from exc

# ---------------------------------------------------------------------------
# Database schema / metadata / shared artifact
# ---------------------------------------------------------------------------

async def _ensure_tables(engine_vlad, enriched_table: str, meta_table: str, artifact_table: str) -> None:
    from sqlalchemy import text
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `date_dt` DATETIME NOT NULL,
                `press` VARCHAR(3) NOT NULL,
                `source_id` BIGINT NOT NULL,
                `channel` VARCHAR(20) NOT NULL,
                `provider` VARCHAR(48) NOT NULL,
                `source_class` VARCHAR(24) NOT NULL,
                `source_tier` TINYINT NOT NULL DEFAULT 0,
                `event_type` VARCHAR(64) NOT NULL,
                `pct_change` DOUBLE NOT NULL DEFAULT 0,
                `cluster_id` SMALLINT NOT NULL,
                `topic_id` SMALLINT NOT NULL,
                `topic_focus` DOUBLE NOT NULL DEFAULT 0,
                `cluster_similarity` DOUBLE NOT NULL DEFAULT 0,
                `novelty` DOUBLE NOT NULL DEFAULT 0,
                `confirmation_count` TINYINT NOT NULL DEFAULT 1,
                `channel_count` TINYINT NOT NULL DEFAULT 1,
                `fast_confirmation` TINYINT NOT NULL DEFAULT 0,
                `btc_relevant` TINYINT NOT NULL DEFAULT 0,
                `eth_relevant` TINYINT NOT NULL DEFAULT 0,
                `quality_score` DOUBLE NOT NULL DEFAULT 0,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_source` (`press`,`source_id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_cluster_date` (`cluster_id`,`date_dt`),
                INDEX `idx_provider_date` (`provider`,`date_dt`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='service93 frozen algorithmic crypto-news events, provider-aware'
        """))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{meta_table}` (
                `meta_key` VARCHAR(64) NOT NULL, `meta_value` VARCHAR(255) NOT NULL,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`meta_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{artifact_table}` (
                `artifact_key` VARCHAR(32) NOT NULL, `signature` CHAR(64) NOT NULL,
                `sha256` CHAR(64) NOT NULL, `blob_data` LONGBLOB NOT NULL,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`artifact_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def _meta_get(engine_vlad, table: str, key: str) -> str | None:
    from sqlalchemy import text
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(
            text(f"SELECT meta_value FROM `{table}` WHERE meta_key=:k"), {"k": key}
        )).fetchone()
    return str(row[0]) if row else None


async def _meta_set_many(engine_vlad, table: str, values: dict[str, Any]) -> None:
    if not values:
        return
    from sqlalchemy import text
    rows = [{"k": str(k), "v": str(v)} for k, v in values.items()]
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{table}` (`meta_key`, `meta_value`)
            VALUES (:k, :v)
            ON DUPLICATE KEY UPDATE meta_value=VALUES(meta_value)
        """), rows)


async def _artifact_load(engine_vlad, table: str, signature: str):
    _require_nlp_stack()
    import joblib
    from sqlalchemy import text

    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text(f"""
            SELECT signature, sha256, blob_data
            FROM `{table}`
            WHERE artifact_key='main'
        """))).fetchone()
    if not row or str(row[0]) != signature:
        return None
    blob = bytes(row[2])
    if hashlib.sha256(blob).hexdigest() != str(row[1]):
        raise RuntimeError("Service 93 crypto-news NLP artifact checksum mismatch")
    return joblib.load(io.BytesIO(blob))


async def _artifact_store(engine_vlad, table: str, signature: str, artifact: dict[str, Any]) -> int:
    _require_nlp_stack()
    import joblib
    from sqlalchemy import text

    bio = io.BytesIO()
    joblib.dump(artifact, bio, compress=3)
    blob = bio.getvalue()
    digest = hashlib.sha256(blob).hexdigest()
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{table}` (`artifact_key`, `signature`, `sha256`, `blob_data`)
            VALUES ('main', :sig, :sha, :blob)
            ON DUPLICATE KEY UPDATE
                signature=VALUES(signature), sha256=VALUES(sha256), blob_data=VALUES(blob_data)
        """), {"sig": signature, "sha": digest, "blob": blob})
    return len(blob)


# ---------------------------------------------------------------------------
# Source queries
# ---------------------------------------------------------------------------

def _source_sql(spec_key: str, *, where: str = "1=1") -> str:
    spec = SOURCES[spec_key]; table=spec["table"]
    if spec_key == "cd":
        return f"""SELECT id AS source_id,title,description AS text,source AS provider,
            published_at AS published_label,created_at AS date_dt,'' AS raw_author,NULL AS author_verified,
            'COINDESK' AS channel,'cd' AS press FROM `{table}` WHERE {where}"""
    if spec_key == "iv":
        return f"""SELECT id AS source_id,title,description AS text,source AS provider,
            published_at AS published_label,created_at AS date_dt,'' AS raw_author,NULL AS author_verified,
            'INVESTING' AS channel,'iv' AS press FROM `{table}` WHERE {where}"""
    if spec_key == "tv":
        return f"""SELECT id AS source_id,title,description AS text,source AS provider,
            '' AS published_label,created_at AS date_dt,'' AS raw_author,NULL AS author_verified,
            'TRADINGVIEW' AS channel,'tv' AS press FROM `{table}` WHERE {where}"""
    return f"""SELECT id AS source_id,title,COALESCE(NULLIF(full_text,''),preview,'') AS text,author AS provider,
        date AS published_label,inserted_at AS date_dt,author AS raw_author,NULL AS author_verified,
        'BINANCE' AS channel,'bn' AS press FROM `{table}` WHERE {where}"""


async def _query_source(engine, spec_key: str, where: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    from sqlalchemy import text
    sql = text(_source_sql(spec_key, where=where) + " ORDER BY date_dt, source_id")
    async with engine.connect() as conn:
        rows = (await conn.execute(sql, params or {})).mappings().all()
    return [dict(r) for r in rows]


async def _training_sample(engine_brain, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cutoff=cfg["train_cutoff"]; limit=int(cfg["sample_per_source"])
    for press,spec in SOURCES.items():
        raw = await _query_source(engine_brain, press, f"{spec['date_col']} IS NOT NULL AND {spec['date_col']} < :cutoff", {"cutoff": cutoff})
        prepared=[x for r in raw if (x:=_prepare_source_row(r,cfg)) is not None]
        if len(prepared)>limit:
            stride=len(prepared)/float(limit)
            prepared=[prepared[min(len(prepared)-1,int(i*stride))] for i in range(limit)]
        out.extend(prepared)
    out.sort(key=lambda r:(r["date_dt"],r["press"],int(r["source_id"])))
    return out


async def _source_bounds(engine_brain) -> tuple[datetime | None, datetime | None]:
    from sqlalchemy import text
    mins=[]; maxs=[]
    async with engine_brain.connect() as conn:
        for spec in SOURCES.values():
            col=spec["date_col"]; table=spec["table"]
            row=(await conn.execute(text(f"SELECT MIN(`{col}`),MAX(`{col}`) FROM `{table}` WHERE `{col}` IS NOT NULL"))).fetchone()
            if row and isinstance(row[0],datetime): mins.append(row[0])
            if row and isinstance(row[1],datetime): maxs.append(row[1])
    return (min(mins) if mins else None, max(maxs) if maxs else None)


async def _source_max_ids(engine_brain) -> dict[str,int]:
    from sqlalchemy import text
    out={}
    async with engine_brain.connect() as conn:
        for press,spec in SOURCES.items():
            out[press]=int((await conn.execute(text(f"SELECT COALESCE(MAX(id),0) FROM `{spec['table']}`"))).scalar() or 0)
    return out


async def _changed_dates(engine_brain,last_ids:dict[str,int]) -> list[datetime]:
    from sqlalchemy import text
    dates=[]
    async with engine_brain.connect() as conn:
        for press,spec in SOURCES.items():
            col=spec["date_col"]; table=spec["table"]
            res=await conn.execute(text(f"SELECT `{col}` FROM `{table}` WHERE id>:id AND `{col}` IS NOT NULL ORDER BY id"),{"id":int(last_ids.get(press,0))})
            dates.extend(r[0] for r in res.fetchall() if isinstance(r[0],datetime))
    return sorted(dates)


async def _fetch_period(engine_brain,start:datetime,end:datetime) -> list[dict[str,Any]]:
    cfg=_nlp_cfg(); out=[]
    for press,spec in SOURCES.items():
        col=spec["date_col"]
        raw=await _query_source(engine_brain,press,f"{col} >= :start AND {col} < :end",{"start":start,"end":end})
        out.extend(x for r in raw if (x:=_prepare_source_row(r,cfg)) is not None)
    out.sort(key=lambda r:(r["date_dt"],r["press"],int(r["source_id"])))
    return out


# ---------------------------------------------------------------------------
# NLP fit / transform
# ---------------------------------------------------------------------------


def _fit_artifact(sample_rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    _require_nlp_stack()
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.decomposition import MiniBatchNMF, TruncatedSVD
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.svm import LinearSVC
    from sklearn.preprocessing import normalize

    texts = [_document_text(r, cfg["text_chars"]) for r in sample_rows]
    texts = [t for t in texts if len(t) >= 12]
    if len(texts) < 500:
        raise RuntimeError(f"Too few pre-cutoff news documents for NLP fit: {len(texts)}")

    vectorizer = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        stop_words=BILINGUAL_STOP,
        ngram_range=(1, 2),
        min_df=5,
        max_df=0.97,
        max_features=int(cfg["max_features"]),
        sublinear_tf=True,
        norm="l2",
        dtype=np.float32,
    )
    X = vectorizer.fit_transform(texts)

    # NMF decides the broad semantic family.  It is NOT used as the only event
    # geometry anymore: that was the source of overly broad global clusters.
    n_topics = min(int(cfg["nmf_topics"]), max(8, X.shape[1] // 100))
    nmf = MiniBatchNMF(
        n_components=n_topics,
        init="nndsvda",
        random_state=93,
        batch_size=2048,
        max_iter=80,
        max_no_improvement=12,
        tol=1e-4,
    )
    # NMF discovers broad latent topics on the frozen training prefix.
    W_fit = nmf.fit_transform(X).astype(np.float32, copy=False)
    dominant_topics = np.argmax(W_fit, axis=1).astype(np.int32, copy=False)

    # Freeze a fast linear surrogate for the NMF topic assignment. NMF remains
    # the unsupervised teacher; LinearSVC only learns to reproduce those labels.
    # Runtime inference is then row-wise, deterministic and prefix-invariant.
    topic_classifier = LinearSVC(C=1.0, random_state=93, dual="auto", max_iter=3000)
    topic_classifier.fit(X, dominant_topics)
    train_topic_pred = topic_classifier.predict(X).astype(np.int32, copy=False)
    training_topic_accuracy = float(np.mean(train_topic_pred == dominant_topics))

    # SVD keeps much more lexical/semantic detail than the NMF topic affinity
    # mixture and is therefore used for local event clustering + similarity.
    svd_components = min(int(cfg["svd_components"]), max(24, X.shape[1] - 1))
    svd = TruncatedSVD(n_components=svd_components, n_iter=7, random_state=79)
    Z = svd.fit_transform(X).astype(np.float32, copy=False)
    Zn = normalize(Z, norm="l2", copy=False)

    topic_models: dict[int, dict[str, Any]] = {}
    total_events = 0
    target_docs = int(cfg["target_event_docs"])
    min_support = int(cfg["min_event_support"])
    max_local = int(cfg["max_local_clusters"])

    for topic_id in range(n_topics):
        idx = np.flatnonzero(dominant_topics == topic_id)
        n = int(len(idx))
        if n == 0:
            continue

        # Allocate event count by topic population, not globally.  This makes an
        # event cluster semantically nested inside one broad NMF topic.
        raw_k = max(1, int(round(n / float(target_docs))))
        raw_k = min(raw_k, max(1, n // max(1, min_support)), max_local)

        if raw_k == 1:
            raw_labels = np.zeros(n, dtype=np.int32)
            raw_centers = normalize(Zn[idx].mean(axis=0, keepdims=True), norm="l2").astype(np.float32)
            kmeans = None
        else:
            kmeans = MiniBatchKMeans(
                n_clusters=raw_k,
                random_state=9300 + topic_id,
                batch_size=min(2048, max(256, n)),
                n_init=3,
                max_iter=120,
                max_no_improvement=15,
                reassignment_ratio=0.005,
            )
            raw_labels = kmeans.fit_predict(Zn[idx]).astype(np.int32, copy=False)
            raw_centers = normalize(kmeans.cluster_centers_.astype(np.float32), norm="l2")

        # KMeans can create a few tiny clusters despite a sensible target size.
        # Merge every undersupported raw cluster into the nearest healthy one.
        counts = np.bincount(raw_labels, minlength=raw_k)
        healthy = np.flatnonzero(counts >= min_support)
        if len(healthy) == 0:
            healthy = np.asarray([int(np.argmax(counts))], dtype=np.int32)
        label_map = np.full(raw_k, -1, dtype=np.int32)
        for final_id, raw_id in enumerate(healthy):
            label_map[int(raw_id)] = int(final_id)
        for raw_id in range(raw_k):
            if label_map[raw_id] >= 0:
                continue
            nearest_healthy = int(healthy[np.argmax(raw_centers[healthy] @ raw_centers[raw_id])])
            label_map[raw_id] = label_map[nearest_healthy]

        final_labels = label_map[raw_labels]
        final_k = int(final_labels.max()) + 1
        final_centers = np.zeros((final_k, Zn.shape[1]), dtype=np.float32)
        for event_id in range(final_k):
            members = idx[final_labels == event_id]
            final_centers[event_id] = normalize(
                Zn[members].mean(axis=0, keepdims=True), norm="l2"
            )[0]

        topic_models[int(topic_id)] = {
            "kmeans": kmeans,
            "label_map": label_map,
            "centers_norm": final_centers,
            "n_events": final_k,
            "training_documents": n,
        }
        total_events += final_k

    return {
        "algo_version": ALGO_VERSION,
        "train_cutoff": cfg["train_cutoff"].isoformat(sep=" "),
        "vectorizer": vectorizer,
        "nmf": nmf,
        "topic_classifier": topic_classifier,
        "training_topic_accuracy": training_topic_accuracy,
        "svd": svd,
        "topic_models": topic_models,
        "n_topics": int(n_topics),
        "n_events": int(total_events),
        # Compatibility alias used by enrichment status/older monitoring.
        "n_clusters": int(total_events),
        "training_documents": int(len(texts)),
        "vocabulary_size": int(X.shape[1]),
    }



def _transform_rows(rows: list[dict[str, Any]], artifact: dict[str, Any], cfg: dict[str, Any]):
    if not rows:
        return (
            np.zeros((0, 1), dtype=np.float32),
            np.zeros(0, dtype=np.int32),
            np.zeros(0, dtype=np.float32),
            np.zeros(0, dtype=np.int32),
            np.zeros(0, dtype=np.float32),
        )
    from sklearn.preprocessing import normalize

    texts = [_document_text(r, cfg["text_chars"]) for r in rows]
    X = artifact["vectorizer"].transform(texts)
    # Frozen linear surrogate of NMF topic labels. Decision margin is used as
    # topic-focus; it depends only on this row and the frozen pre-cutoff model.
    clf = artifact["topic_classifier"]
    scores = np.asarray(clf.decision_function(X), dtype=np.float32)
    if scores.ndim == 1:
        scores = np.column_stack((-scores, scores)).astype(np.float32, copy=False)
    top_ids = np.argmax(scores, axis=1).astype(np.int32, copy=False)
    part = np.partition(scores, -2, axis=1)
    margin = part[:, -1] - part[:, -2]
    topic_focus = (1.0 / (1.0 + np.exp(-margin))).astype(np.float32, copy=False)

    Z = artifact["svd"].transform(X).astype(np.float32, copy=False)
    Zn = normalize(Z, norm="l2", copy=False)
    labels = np.zeros(len(rows), dtype=np.int32)
    cluster_sim = np.zeros(len(rows), dtype=np.float32)

    for topic_id in np.unique(top_ids):
        idx = np.flatnonzero(top_ids == topic_id)
        model_info = artifact["topic_models"].get(int(topic_id))
        if model_info is None:
            continue
        kmeans = model_info["kmeans"]
        if kmeans is None:
            raw = np.zeros(len(idx), dtype=np.int32)
        else:
            raw = kmeans.predict(Zn[idx]).astype(np.int32, copy=False)
        final = np.asarray(model_info["label_map"], dtype=np.int32)[raw]
        labels[idx] = final
        centers = np.asarray(model_info["centers_norm"], dtype=np.float32)
        cluster_sim[idx] = np.einsum("ij,ij->i", Zn[idx], centers[final]).astype(np.float32, copy=False)

    # Return normalized SVD vectors: novelty/confirmation now use the richer
    # semantic subspace instead of coarse NMF topic-mixture vectors.
    return Zn, labels, topic_focus.astype(np.float32), top_ids, cluster_sim


# ---------------------------------------------------------------------------
# Causal novelty + cross-source confirmation
# ---------------------------------------------------------------------------


def _quality(topic_focus: float, cluster_sim: float, novelty: float, confirmation_count: int) -> float:
    confirmation_boost=1.0+0.20*math.log2(max(1,int(confirmation_count)))
    q=((0.40+0.60*max(0.0,min(1.0,topic_focus)))
       *(0.40+0.60*max(0.0,min(1.0,cluster_sim)))
       *(0.55+0.45*max(0.0,min(1.0,novelty)))
       *confirmation_boost)
    return float(max(0.05,min(3.0,q)))


def _enrich_rows(rows:list[dict[str,Any]],artifact:dict[str,Any],cfg:dict[str,Any],history:dict[int,deque],*,write_from:datetime|None=None,write_to:datetime|None=None) -> list[dict[str,Any]]:
    if not rows: return []
    Zn,labels,topic_focus,top_ids,cluster_sim=_transform_rows(rows,artifact,cfg)
    novelty_h=timedelta(days=int(cfg["novelty_days"]))
    confirm_h=timedelta(hours=int(cfg["confirm_hours"]))
    duplicate_h=timedelta(minutes=int(cfg["duplicate_minutes"]))
    fast_h=timedelta(minutes=int(cfg["fast_confirm_minutes"]))
    threshold=float(cfg["confirm_similarity"]); dup_threshold=float(cfg["duplicate_similarity"])
    out=[]
    for i,row in enumerate(rows):
        dt=row.get("date_dt")
        if not isinstance(dt,datetime): continue
        topic_id=int(top_ids[i]); cluster_id=int(labels[i]); vector=np.asarray(Zn[i],dtype=np.float32)
        bucket=history[topic_id]; min_dt=dt-novelty_h
        while bucket and bucket[0]["dt"]<min_dt: bucket.popleft()
        current_provider=str(row.get("origin_provider") or row.get("provider") or "OTHER_MEDIA")
        current_channel=str(row.get("channel") or "UNKNOWN")
        cur_a=set(row.get("anchors") or ()); cur_n=set(row.get("number_anchors") or ())
        max_sim=0.0; providers={current_provider}; channels={current_channel}; fast=False; duplicate=False
        for prev in bucket:
            raw_sim=float(np.dot(vector,prev["vec"]))
            sim=_effective_similarity(raw_sim,cur_a,prev["anchors"],cur_n,prev["numbers"])
            if sim>max_sim: max_sim=sim
            age=dt-prev["dt"]
            if age<=confirm_h and sim>=threshold:
                # Same origin provider through another aggregator is syndication,
                # not independent confirmation.
                if prev["provider"]==current_provider:
                    if age<=duplicate_h and sim>=dup_threshold:
                        duplicate=True
                    channels.add(prev["channel"])
                else:
                    providers.add(prev["provider"]); channels.add(prev["channel"])
                    if age<=fast_h: fast=True
        novelty=1.0 if not bucket else max(0.0,min(1.0,1.0-max_sim))
        focus=float(topic_focus[i]); c_sim=max(0.0,min(1.0,float(cluster_sim[i])))
        confirmation_count=max(1,len(providers)); channel_count=max(1,len(channels))
        event_type=f"t{topic_id:02d}|e{cluster_id:02d}"
        quality=_quality(focus,c_sim,novelty,confirmation_count)
        should_write=(not duplicate and (write_from is None or dt>=write_from) and (write_to is None or dt<write_to))
        if should_write:
            out.append({
                "date_dt":dt,"press":str(row.get("press") or "")[:3],"source_id":int(row.get("source_id") or 0),
                "channel":current_channel[:20],"provider":current_provider[:48],"source_class":str(row.get("source_class") or "")[:24],
                "source_tier":int(row.get("source_tier") or 0),"event_type":event_type,"pct_change":quality,
                "cluster_id":cluster_id,"topic_id":topic_id,"topic_focus":focus,"cluster_similarity":c_sim,
                "novelty":novelty,"confirmation_count":confirmation_count,"channel_count":channel_count,
                "fast_confirmation":int(fast),"btc_relevant":int(row.get("btc_relevant") or 0),
                "eth_relevant":int(row.get("eth_relevant") or 0),"quality_score":quality,
            })
        # History is updated after current feature computation. Duplicates are kept
        # in history so later independent providers can still see all propagation
        # channels, but they never become an additional trading event themselves.
        bucket.append({"dt":dt,"provider":current_provider,"channel":current_channel,"vec":vector.copy(),
                       "anchors":cur_a,"numbers":cur_n})
    return out


async def _upsert(engine_vlad, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows: return 0
    from sqlalchemy import text
    sql=text(f"""INSERT INTO `{table}` (
        date_dt,press,source_id,channel,provider,source_class,source_tier,event_type,pct_change,
        cluster_id,topic_id,topic_focus,cluster_similarity,novelty,confirmation_count,channel_count,
        fast_confirmation,btc_relevant,eth_relevant,quality_score)
        VALUES (:date_dt,:press,:source_id,:channel,:provider,:source_class,:source_tier,:event_type,:pct_change,
        :cluster_id,:topic_id,:topic_focus,:cluster_similarity,:novelty,:confirmation_count,:channel_count,
        :fast_confirmation,:btc_relevant,:eth_relevant,:quality_score)
        ON DUPLICATE KEY UPDATE date_dt=VALUES(date_dt),channel=VALUES(channel),provider=VALUES(provider),
        source_class=VALUES(source_class),source_tier=VALUES(source_tier),event_type=VALUES(event_type),pct_change=VALUES(pct_change),
        cluster_id=VALUES(cluster_id),topic_id=VALUES(topic_id),topic_focus=VALUES(topic_focus),cluster_similarity=VALUES(cluster_similarity),
        novelty=VALUES(novelty),confirmation_count=VALUES(confirmation_count),channel_count=VALUES(channel_count),
        fast_confirmation=VALUES(fast_confirmation),btc_relevant=VALUES(btc_relevant),eth_relevant=VALUES(eth_relevant),quality_score=VALUES(quality_score)""")
    written=0
    async with engine_vlad.begin() as conn:
        for start in range(0,len(rows),1000):
            batch=rows[start:start+1000]; await conn.execute(sql,batch); written+=len(batch)
    return written


def _periods(start: datetime, end: datetime, days: int):
    cur = start
    step = timedelta(days=max(1, days))
    while cur < end:
        nxt = min(end, cur + step)
        yield cur, nxt
        cur = nxt


def _affected_windows(changed_dates: list[datetime], cfg: dict[str, Any]):
    if not changed_dates:
        return []
    horizon = timedelta(days=int(cfg["novelty_days"]))
    raw = [(dt - horizon, dt, dt + horizon + timedelta(seconds=1)) for dt in changed_dates]
    raw.sort(key=lambda x: x[0])
    merged: list[list[datetime]] = []
    for context_start, write_start, end in raw:
        if not merged or context_start > merged[-1][2]:
            merged.append([context_start, write_start, end])
        else:
            merged[-1][0] = min(merged[-1][0], context_start)
            merged[-1][1] = min(merged[-1][1], write_start)
            merged[-1][2] = max(merged[-1][2], end)
    return [tuple(x) for x in merged]


# ---------------------------------------------------------------------------
# Shared Brain context indexes / weights
# ---------------------------------------------------------------------------

async def _table_exists(engine_vlad, table_name: str) -> bool:
    from sqlalchemy import text
    async with engine_vlad.connect() as conn:
        value = (await conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tbl
        """), {"tbl": table_name})).scalar()
    return bool(int(value or 0))


async def _table_has_rows(engine_vlad, table_name: str) -> bool:
    from sqlalchemy import text
    try:
        async with engine_vlad.connect() as conn:
            row = (await conn.execute(text(f"SELECT 1 FROM `{table_name}` LIMIT 1"))).fetchone()
        return row is not None
    except Exception:
        return False


async def _ensure_shared_weights(
    engine_vlad,
    enriched_table: str,
    *,
    reset: bool = False,
) -> int:
    """Materialize standard ctx/mode/shift codes once for both services."""
    from sqlalchemy import text

    indexes_table = f"{enriched_table}_indexes"
    weights_table = f"{enriched_table}_weights"
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{weights_table}` (
                id          INT         NOT NULL AUTO_INCREMENT,
                weight_code VARCHAR(40) NOT NULL,
                ctx_id      BIGINT      NOT NULL,
                mode        TINYINT     NOT NULL,
                shift       SMALLINT    NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY uk_wc (weight_code),
                INDEX idx_ctx_id (ctx_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        if reset:
            await conn.execute(text(f"TRUNCATE TABLE `{weights_table}`"))

    async with engine_vlad.connect() as conn:
        rows = (await conn.execute(text(f"""
            SELECT idx.id, COALESCE(agg.cnt, 0) AS occ
            FROM `{indexes_table}` idx
            LEFT JOIN (
                SELECT event_type, COUNT(*) AS cnt
                FROM `{enriched_table}`
                GROUP BY event_type
            ) agg ON agg.event_type = idx.event_type
            WHERE idx.mask_id = 1
            ORDER BY idx.id
        """))).fetchall()

    payload = [
        {
            "wc": f"{int(ctx_id)}_{mode}_{shift}",
            "ctx_id": int(ctx_id),
            "mode": mode,
            "shift": shift,
        }
        for ctx_id, occ in rows
        for mode in (0, 1)
        for shift in range(0, (SHIFT_WINDOW if int(occ or 0) >= 2 else 0) + 1)
    ]
    if payload:
        async with engine_vlad.begin() as conn:
            sql = text(f"""
                INSERT IGNORE INTO `{weights_table}`
                    (weight_code, ctx_id, mode, shift)
                VALUES (:wc, :ctx_id, :mode, :shift)
            """)
            for start in range(0, len(payload), 1000):
                await conn.execute(sql, payload[start:start + 1000])
    return len(payload)


async def _sync_shared_brain_indexes(
    engine_vlad,
    enriched_table: str,
    *,
    mode: str,
    index_from: datetime | None,
) -> dict[str, Any]:
    """Keep shared event_type indexes stable and avoid 79/80 duplicate rebuilds.

    A full NLP rebuild creates a fresh index universe. Incremental enrichment only
    appends newly observed event_type combinations from the affected date onward;
    old unused contexts may remain, which intentionally preserves ctx ids and
    therefore cache/ML code identity.
    """
    from dataset_indexer import build_indexes, parse_indexes

    mask_table = f"{enriched_table}_mask"
    indexes_table = f"{enriched_table}_indexes"
    weights_table = f"{enriched_table}_weights"
    mask_ok = (await _table_exists(engine_vlad, mask_table)) and (await _table_has_rows(engine_vlad, mask_table))
    indexes_ok = (await _table_exists(engine_vlad, indexes_table)) and (await _table_has_rows(engine_vlad, indexes_table))
    weights_ok = (await _table_exists(engine_vlad, weights_table)) and (await _table_has_rows(engine_vlad, weights_table))

    full_index = mode == "full" or not (mask_ok and indexes_ok)
    indexed = False
    if full_index:
        await build_indexes(engine_vlad, enriched_table, ["event_type"])
        ok = await parse_indexes(engine_vlad, enriched_table, "date_dt", date=None)
        if not ok:
            raise RuntimeError(f"Could not build shared indexes for {enriched_table}")
        indexed = True
    elif mode == "incremental" and index_from is not None:
        ok = await parse_indexes(engine_vlad, enriched_table, "date_dt", date=index_from)
        if not ok:
            raise RuntimeError(f"Could not increment shared indexes for {enriched_table}")
        indexed = True

    # If an old deployment left indexes but no weights, repair automatically.
    weight_rows = 0
    if indexed or not weights_ok:
        weight_rows = await _ensure_shared_weights(
            engine_vlad, enriched_table, reset=full_index,
        )

    return {
        "mode": "full" if full_index else ("incremental" if indexed else "noop"),
        "mask_table": mask_table,
        "indexes_table": indexes_table,
        "weights_table": weights_table,
        "weight_codes_seen": weight_rows,
    }


# ---------------------------------------------------------------------------
# Shared enrichment entry point
# ---------------------------------------------------------------------------

async def _enrich_unlocked(engine_vlad, engine_brain) -> dict[str, Any]:
    from sqlalchemy import text

    cfg = _nlp_cfg()
    service_cfg = get_service_config() or {}
    dcfg = service_cfg.get("dataset") or {}
    enriched_table = str(dcfg.get("enriched_table") or "vlad_crypto_news_algo_events_s93")
    meta_table = f"{enriched_table}_meta"
    artifact_table = f"{enriched_table}_artifact"
    signature = _signature(cfg)

    await _ensure_tables(engine_vlad, enriched_table, meta_table, artifact_table)

    stored_schema = await _meta_get(engine_vlad, meta_table, "schema_version")
    stored_sig = await _meta_get(engine_vlad, meta_table, "algo_signature")
    full_rebuild = stored_schema != ENRICH_SCHEMA_VERSION or stored_sig != signature

    artifact = await _artifact_load(engine_vlad, artifact_table, signature)
    fitted = False
    artifact_bytes = 0
    if artifact is None:
        sample = await _training_sample(engine_brain, cfg)
        artifact = _fit_artifact(sample, cfg)
        artifact_bytes = await _artifact_store(engine_vlad, artifact_table, signature, artifact)
        fitted = True
        full_rebuild = True

    source_max = await _source_max_ids(engine_brain)
    last_ids = {
        press: int(await _meta_get(engine_vlad, meta_table, f"last_id_{press}") or 0)
        for press in SOURCES
    }

    if not full_rebuild and all(source_max[p] <= last_ids[p] for p in SOURCES):
        return {
            "mode": "noop",
            "artifact_fitted": fitted,
            "artifact_bytes": artifact_bytes,
            "training_documents": int(artifact.get("training_documents", 0)),
            "vocabulary_size": int(artifact.get("vocabulary_size", 0)),
            "topics": int(artifact.get("n_topics", 0)),
            "clusters": int(artifact.get("n_clusters", 0)),
            "events_written": 0,
            "source_max_ids": source_max,
            "index_from": None,
        }

    events_written = 0
    source_rows_seen = 0

    if full_rebuild:
        # Do not truncate until a valid frozen artifact exists.
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))

        mn, mx = await _source_bounds(engine_brain)
        if mn is not None and mx is not None:
            history: dict[int, deque] = defaultdict(deque)
            rebuild_end = mx + timedelta(seconds=1)
            for start, end in _periods(mn, rebuild_end, int(cfg["period_days"])):
                rows = await _fetch_period(engine_brain, start, end)
                source_rows_seen += len(rows)
                enriched = _enrich_rows(rows, artifact, cfg, history)
                events_written += await _upsert(engine_vlad, enriched_table, enriched)

        await _meta_set_many(engine_vlad, meta_table, {
            "schema_version": ENRICH_SCHEMA_VERSION,
            "algo_signature": signature,
            **{f"last_id_{p}": source_max[p] for p in SOURCES},
        })
        return {
            "mode": "full",
            "artifact_fitted": fitted,
            "artifact_bytes": artifact_bytes,
            "training_documents": int(artifact.get("training_documents", 0)),
            "vocabulary_size": int(artifact.get("vocabulary_size", 0)),
            "topics": int(artifact.get("n_topics", 0)),
            "clusters": int(artifact.get("n_clusters", 0)),
            "source_rows_seen": source_rows_seen,
            "events_written": events_written,
            "source_max_ids": source_max,
            "index_from": None,
        }

    # Incremental: delayed/old inserts can change novelty/confirmation for the
    # following seven days. Recompute only those causally affected windows.
    changed_dates = await _changed_dates(engine_brain, last_ids)
    windows = _affected_windows(changed_dates, cfg)
    for context_start, write_start, end in windows:
        rows = await _fetch_period(engine_brain, context_start, end)
        source_rows_seen += len(rows)
        history: dict[int, deque] = defaultdict(deque)
        enriched = _enrich_rows(
            rows, artifact, cfg, history,
            write_from=write_start,
            write_to=end,
        )
        events_written += await _upsert(engine_vlad, enriched_table, enriched)

    # Advance watermarks only after every affected window committed.
    await _meta_set_many(engine_vlad, meta_table, {
        "schema_version": ENRICH_SCHEMA_VERSION,
        "algo_signature": signature,
        **{f"last_id_{p}": source_max[p] for p in SOURCES},
    })
    return {
        "mode": "incremental",
        "artifact_fitted": fitted,
        "training_documents": int(artifact.get("training_documents", 0)),
        "vocabulary_size": int(artifact.get("vocabulary_size", 0)),
        "topics": int(artifact.get("n_topics", 0)),
        "clusters": int(artifact.get("n_clusters", 0)),
        "changed_news": len(changed_dates),
        "recomputed_windows": len(windows),
        "source_rows_seen": source_rows_seen,
        "events_written": events_written,
        "source_max_ids": source_max,
        "index_from": min(changed_dates).isoformat(sep=" ") if changed_dates else None,
    }


async def enrich_dataset(engine_vlad, engine_brain) -> dict[str, Any]:
    """Build/update the unified frozen crypto-news dataset for service 93."""
    from sqlalchemy import text

    cfg = get_service_config() or {}
    dcfg = cfg.get("dataset") or {}
    enriched_table = str(dcfg.get("enriched_table") or "vlad_crypto_news_algo_events_s93")
    lock_name = f"crypto_news_algo:{enriched_table}"[:64]

    async with engine_vlad.connect() as lock_conn:
        acquired = (await lock_conn.execute(
            text("SELECT GET_LOCK(:name, 1800)"), {"name": lock_name}
        )).scalar()
        if int(acquired or 0) != 1:
            raise RuntimeError(f"Could not acquire shared news enrichment lock: {lock_name}")
        try:
            result = await _enrich_unlocked(engine_vlad, engine_brain)
            index_from_raw = result.get("index_from") if isinstance(result, dict) else None
            index_from = None
            if index_from_raw:
                try:
                    index_from = datetime.fromisoformat(str(index_from_raw)[:19])
                except ValueError:
                    index_from = None
            index_stats = await _sync_shared_brain_indexes(
                engine_vlad, enriched_table,
                mode=str((result or {}).get("mode") or "noop"),
                index_from=index_from,
            )
            result["shared_index"] = index_stats
            return result
        finally:
            try:
                await lock_conn.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name})
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Runtime Brain model: indexed O(current-events × analogs), not O(all-news)
# ---------------------------------------------------------------------------

def _var_allows_row(row: dict[str, Any], var: int) -> bool:
    try:
        focus=float(row.get("topic_focus") or 0.0); fit=float(row.get("cluster_similarity") or 0.0)
        novelty=float(row.get("novelty") or 0.0); confirm=int(row.get("confirmation_count") or 1)
        fast=bool(int(row.get("fast_confirmation") or 0)); btc=bool(int(row.get("btc_relevant") or 0)); eth=bool(int(row.get("eth_relevant") or 0))
        tier=int(row.get("source_tier") or 0); channel=str(row.get("channel") or "").upper()
    except (TypeError,ValueError): return var in (0,7)
    if var==0: return True
    if var==1: return focus>=TOPIC_FOCUS_MIN
    if var==2: return fit>=CLUSTER_SIM_MIN
    if var==3: return novelty>=NOVELTY_MIN
    if var==4: return confirm>=2
    if var==5: return confirm>=3
    if var==6: return novelty>=NOVELTY_MIN and confirm>=2
    if var==7: return True
    if var==8: return btc
    if var==9: return eth
    if var==10: return fast and confirm>=2
    if var==11: return tier>=2
    if var==12: return channel=="COINDESK"
    if var==13: return channel=="INVESTING"
    if var==14: return channel=="TRADINGVIEW"
    if var==15: return channel=="BINANCE"
    return False


def _frame_date(dt: datetime, is_daily: bool) -> datetime:
    if is_daily:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(minute=0, second=0, microsecond=0)


def _ctx_reverse(ctx_index: dict) -> dict[str, tuple[int, dict]]:
    return {
        str(info.get("event_type") or "").strip().lower(): (int(info["id"]), info)
        for info in (ctx_index or {}).values()
        if info.get("id") and info.get("event_type")
    }


def _dataset_token(source, dataset_index: dict) -> tuple:
    if not source:
        return (0, 0, 0)
    last = source[-1].get("date") or source[-1].get("date_dt")
    return (id(source), len(source), int(last.timestamp()) if isinstance(last, datetime) else 0)


def _current_rows(source, target_date: datetime, dataset_index: dict, is_daily: bool):
    if not source:
        return []
    horizon = timedelta(days=1) if is_daily else timedelta(hours=SHIFT_WINDOW)
    left_ts = int((target_date - horizon).timestamp())
    right_ts = int(target_date.timestamp())
    ts_arr = dataset_index.get("dataset_timestamps")
    if ts_arr is not None and len(ts_arr) == len(source):
        lo = int(np.searchsorted(ts_arr, left_ts, side="left"))
        hi = int(np.searchsorted(ts_arr, right_ts, side="right"))
        return source[lo:hi]

    dates = dataset_index.get("dates") or []
    if dates and len(dates) == len(source):
        lo = bisect.bisect_left(dates, target_date - horizon)
        hi = bisect.bisect_right(dates, target_date)
        return source[lo:hi]

    # Defensive fallback; expected only in standalone unit tests.
    return [
        r for r in source
        if isinstance((r.get("date") or r.get("date_dt")), datetime)
        and target_date - horizon <= (r.get("date") or r.get("date_dt")) <= target_date
    ]


def _previous_direction(np_rates: dict | None, target_date: datetime) -> tuple[bool, str]:
    if not np_rates:
        return False, "ext_min"
    dates_ns = np_rates.get("dates_ns")
    opens = np_rates.get("open")
    closes = np_rates.get("close")
    if dates_ns is None or opens is None or closes is None or len(dates_ns) == 0:
        return False, "ext_min"
    cut = int(np.searchsorted(dates_ns, int(target_date.timestamp()), side="left"))
    if cut <= 0:
        return False, "ext_min"
    idx = cut - 1
    predict_max = float(closes[idx]) > float(opens[idx])
    return predict_max, "ext_max" if predict_max else "ext_min"


def _aggregate_analogs_by_var(
    analog_rows: list[dict[str, Any]],
    analog_dates: list[datetime],
    target_date: datetime,
    current_event_time: datetime,
    shift: int,
    *,
    is_daily: bool,
    np_rates: dict,
    ext_name: str,
) -> dict[int, tuple[float, float, int, float, float, float]]:
    """Aggregate historical outcomes once while maintaining all var subsets."""
    end = bisect.bisect_left(analog_dates, target_date)
    stats = {v: [0.0, 0.0, 0, 0.0, 0.0, 0.0] for v in VAR_RANGE}
    if end <= 0:
        return {v: tuple(x) for v, x in stats.items()}

    dates_ns = np_rates.get("dates_ns") if np_rates else None
    t1_arr = np_rates.get("t1") if np_rates else None
    ext_arr = np_rates.get(ext_name) if np_rates else None
    if dates_ns is None or t1_arr is None or ext_arr is None:
        return {v: tuple(x) for v, x in stats.items()}

    unit = timedelta(days=1) if is_daily else timedelta(hours=1)
    for j in range(end):
        analog_dt = analog_dates[j]
        if analog_dt == current_event_time:
            continue
        outcome_time = analog_dt + unit * int(shift)
        frame = _frame_date(outcome_time, is_daily)
        if frame + unit > target_date:
            continue
        ts = int(frame.timestamp())
        idx = int(np.searchsorted(dates_ns, ts, side="left"))
        if idx >= len(dates_ns) or int(dates_ns[idx]) != ts:
            continue

        analog = analog_rows[j]
        stored_t1 = float(t1_arr[idx])
        hit = 1.0 if bool(ext_arr[idx]) else 0.0
        quality = float(analog.get("quality_score") or 1.0)
        q_weight = min(max(quality / 0.65, 0.25), 2.5)

        for var in VAR_RANGE:
            if not _var_allows_row(analog, var):
                continue
            st = stats[var]
            st[0] += stored_t1
            st[1] += hit
            st[2] += 1
            st[3] += stored_t1 * q_weight
            st[4] += hit * q_weight
            st[5] += q_weight

    return {v: tuple(x) for v, x in stats.items()}


def _mode1_score(hits: float, total: float, predict_max: bool) -> float:
    if total <= 0:
        return 0.0
    score = ((hits / total) * 2.0) - 1.0
    return -score if predict_max else score


def _add_code(target: dict[str, float], code: str, value: float) -> None:
    value = float(value)
    if value != 0.0:
        target[code] = target.get(code, 0.0) + value


def _compute_all_slots_for_date(
    dataset: list[dict],
    target_date: datetime,
    dataset_index: dict,
) -> dict[tuple[int, int], dict[str, float]]:
    outputs = {(t, v): {} for t in TYPES_RANGE for v in VAR_RANGE}
    if target_date < _nlp_cfg()["train_cutoff"]:
        return outputs

    di = dataset_index or {}
    source = di.get("full_dataset") if di.get("full_dataset") is not None else dataset
    if not source:
        return outputs
    by_key = di.get("by_key") or {}
    key_dates = di.get("key_dates") or {}
    ctx_reverse = _ctx_reverse(di.get("ctx_index") or {})
    np_rates = di.get("np_rates")
    if not by_key or not key_dates or not ctx_reverse or not np_rates:
        return outputs

    is_daily = bool(di.get("is_daily"))
    unit_seconds = 86400 if is_daily else 3600
    predict_max, ext_name = _previous_direction(np_rates, target_date)
    current = _current_rows(source, target_date, di, is_daily)

    for row in current:
        event_dt = row.get("date") or row.get("date_dt")
        if not isinstance(event_dt, datetime) or event_dt > target_date:
            continue
        event_type = str(row.get("event_type") or "").strip().lower()
        ctx = ctx_reverse.get(event_type)
        if ctx is None:
            continue
        ctx_id, _ctx_info = ctx

        seconds = max(0.0, (target_date - event_dt).total_seconds())
        shift = int(seconds // unit_seconds)
        if not is_daily and shift > SHIFT_WINDOW:
            continue
        if is_daily and shift > 1:
            continue

        analog_rows = by_key.get(event_type) or []
        analog_dates = key_dates.get(event_type) or []
        if len(analog_rows) < 3:
            continue

        by_var = _aggregate_analogs_by_var(
            analog_rows, analog_dates, target_date, event_dt, shift,
            is_daily=is_daily, np_rates=np_rates, ext_name=ext_name,
        )

        for var in VAR_RANGE:
            if not _var_allows_row(row, var):
                continue
            raw_t1, raw_hits, n, w_t1, w_hits, w_total = by_var[var]
            if n < 3:
                continue
            raw_mode1 = _mode1_score(raw_hits, float(n), predict_max)
            weighted_mode1 = _mode1_score(w_hits, w_total, predict_max)
            mode0 = w_t1 if var == 7 else raw_t1
            mode1 = weighted_mode1 if var == 7 else raw_mode1
            code0 = f"{ctx_id}_0_{shift}"
            code1 = f"{ctx_id}_1_{shift}"

            if 0 in TYPES_RANGE:
                _add_code(outputs[(0, var)], code0, round(mode0, 6))
                _add_code(outputs[(0, var)], code1, round(mode1, 6))
            if 1 in TYPES_RANGE:
                _add_code(outputs[(1, var)], code0, round(mode0, 6))
            if 2 in TYPES_RANGE:
                _add_code(outputs[(2, var)], code1, round(mode1, 6))
            # Type 3/4: expose the same active-code universe used by the existing
            # reverse-learning services.  In ML mode only keys matter; weights are
            # learned by Brain's ReverseStore.
            for calc_type in (3, 4):
                if calc_type not in TYPES_RANGE:
                    continue
                if mode0 != 0.0:
                    _add_code(outputs[(calc_type, var)], code0, round(mode0, 6))
                if raw_hits > 0.0:
                    _add_code(outputs[(calc_type, var)], code1, 1.0)

    return outputs


def _runtime_cache_token(dataset: list[dict], dataset_index: dict, target_dates: tuple[int, ...] | None = None) -> tuple:
    source = dataset_index.get("full_dataset") if dataset_index.get("full_dataset") is not None else dataset
    np_rates = dataset_index.get("np_rates") or {}
    dates_ns = np_rates.get("dates_ns")
    rates_tail = int(dates_ns[-1]) if dates_ns is not None and len(dates_ns) else 0
    base = (
        _dataset_token(source, dataset_index),
        str(dataset_index.get("rates_table") or ""),
        bool(dataset_index.get("is_daily")),
        rates_tail,
    )
    return base + ((target_dates,) if target_dates is not None else ())


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
    if not dataset or not date or dataset_index is None:
        return {}
    calc_type, calc_var = int(type), int(var)
    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {}

    di = dict(dataset_index)
    # Use the framework's full immutable dataset only with our own explicit
    # <= target_date bounds. This avoids per-call prefix construction and cannot
    # introduce future news into current/analog selection.
    token = _runtime_cache_token(dataset, di) + (int(date.timestamp()),)
    with _CACHE_LOCK:
        cached = _SINGLE_CACHE.get(token)
        if cached is not None:
            _SINGLE_CACHE.move_to_end(token)
            return dict(cached.get((calc_type, calc_var), {}))

    all_slots = _compute_all_slots_for_date(dataset, date, di)
    with _CACHE_LOCK:
        _SINGLE_CACHE[token] = all_slots
        _SINGLE_CACHE.move_to_end(token)
        while len(_SINGLE_CACHE) > _SINGLE_CACHE_MAX:
            _SINGLE_CACHE.popitem(last=False)
    return dict(all_slots.get((calc_type, calc_var), {}))


def batch_model(
    rates: list[dict],
    dataset: list[dict],
    dates: list[datetime],
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[datetime, dict[str, float]]:
    """Compute all type/var slots in one chronological pass and reuse them.

    Brain Framework invokes batch_model once per slot.  The first invocation for
    a date batch computes the whole 5×16 slot cube; the remaining slot calls are
    cache lookups.  This is critical for ~872k news rows.
    """
    if not dates or not dataset or dataset_index is None:
        return {d: {} for d in dates}
    calc_type, calc_var = int(type), int(var)
    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {d: {} for d in dates}

    di = dict(dataset_index)
    date_token = tuple(int(d.timestamp()) for d in dates)
    key = _runtime_cache_token(dataset, di, date_token)

    with _CACHE_LOCK:
        cached = _BATCH_CACHE.get(key)
        if cached is not None:
            _BATCH_CACHE.move_to_end(key)
            selected = cached.get((calc_type, calc_var), {})
            return {d: dict(selected.get(d, {})) for d in dates}

    cube: dict[tuple[int, int], dict[datetime, dict[str, float]]] = {
        (t, v): {} for t in TYPES_RANGE for v in VAR_RANGE
    }
    for dt in dates:
        slots = _compute_all_slots_for_date(dataset, dt, di)
        for slot, value in slots.items():
            cube[slot][dt] = value

    with _CACHE_LOCK:
        _BATCH_CACHE[key] = cube
        _BATCH_CACHE.move_to_end(key)
        while len(_BATCH_CACHE) > _BATCH_CACHE_MAX:
            _BATCH_CACHE.popitem(last=False)

    selected = cube.get((calc_type, calc_var), {})
    return {d: dict(selected.get(d, {})) for d in dates}
