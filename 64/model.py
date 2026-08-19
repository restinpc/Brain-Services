"""
Service 64 — Structured Financial News H1 + Reverse Learning
============================================================

Replacement for the experimental service-81-style news digestion model.

The service builds its own deterministic structured event dataset directly from
the existing raw news tables:
    brain_cnn_news
    brain_nyt_news
    brain_twp_news
    brain_wsj_news
    brain_tgd_news

No LLM is used.

Each article is converted into low-cardinality financial event attributes:
family, actor/actor class, action, object, semantic orientation, certainty,
importance, magnitude, surprise hints and asset relevance.

A second strictly chronological pass builds story lifecycle features:
FIRST/FOLLOWUP/CONFIRMED/MULTI_SOURCE/SATURATED, 1h/4h article and source
counts, velocity, novelty, and 180-day expectedness/rarity.  The pass never
updates a historical row using later articles.

The enriched table is created and maintained by enrich_dataset(), therefore
Brain Framework /rebuild_index runs the whole raw->structured pipeline before
building its _mask/_indexes/_weights tables.

VAR semantics
-------------
0 PRICE_CONTROL       closed H1 market state only
1 STRUCTURED_EVENT    family + actor + action + object + relevance
2 SURPRISE            structured event + surprise/magnitude/expectedness
3 STORY               structured event + causal story lifecycle/narrative
4 EVENT_REACTION      structured event + first closed H1 price reaction
5 ALL                 structured + surprise + story + reaction

TYPE semantics remain the shared reverse-learning modes 0..4.
No event action is hard-coded as bullish/bearish for the traded market.
"""

from __future__ import annotations

import bisect
import hashlib
import html
import math
import re
import threading
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta
from typing import Any

import numpy as np


# -----------------------------------------------------------------------------
# Brain framework contract
# -----------------------------------------------------------------------------

SERVICE_ID = 64
PORT = 8926
NODE_NAME = "brain-news-structured-h1-ml-s64"
SERVICE_TEXT = "Structured financial news events + causal story lifecycle + H1 reverse-learning"

RATES_TABLE = "brain_rates_eur_usd"

ENRICHED_TABLE = "vlad_news_structured_events_v2"
DATASET_INDEX_FIELDS = ["event_type"]
DATASET_DATE_FIELD = "date_dt"
DATASET_ENGINE = "vlad"
DATASET_KEY = "event_type"

FILTER_DATASET_BY_DATE = True
MODEL_CAN_FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
PRETEST_ALLOW_EMPTY = True

CACHE_DATE_FROM = "2025-01-15"
MODEL_START = datetime(2025, 1, 15, 0, 0, 0)

# We keep 180 days only as causal warm-up for rarity/expectedness before
# MODEL_START. Rows earlier than this cannot affect the model.
SOURCE_WARM_FROM = MODEL_START - timedelta(days=180)

SHIFT_WINDOW = 4
VAR_RANGE = [0, 1, 2, 3, 4, 5]
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = True

ML_TARGET_PRECISION = 0.62
ML_MAX_ITER = 8
ML_STEP = 0.04
ML_EXTREMUM_LIMIT = 48
ML_ACTIVE_TAIL = 0
ML_PRECISION_METRIC = "mean"

REVERSE_MAX_ABS_WEIGHT = 32.0
REVERSE_AMP_MIN = 0.25
REVERSE_AMP_MAX = 8.0
REVERSE_AMP_LOOKBACK = 24
REVERSE_FIXED_EXTREMUM_INTERVAL = 3

FAST_HOURS = 4
NARRATIVE_HOURS = 24
STORY_MATCH_HOURS = 72
EXPECTEDNESS_DAYS = 180
ENRICH_BATCH = 3000
SCHEMA_VERSION = "structured-fin-events-v2.4"

SOURCE_TABLES: dict[str, str] = {
    "cnn": "brain_cnn_news",
    "nyt": "brain_nyt_news",
    "twp": "brain_twp_news",
    "wsj": "brain_wsj_news",
    "tgd": "brain_tgd_news",
}

RATES_TO_PAIR = {
    "brain_rates_eur_usd": 1,
    "brain_rates_eur_usd_day": 1,
    "brain_rates_btc_usd": 3,
    "brain_rates_btc_usd_day": 3,
    "brain_rates_eth_usd": 4,
    "brain_rates_eth_usd_day": 4,
}

VAR_LABELS = {
    0: "PRICE_CONTROL",
    1: "STRUCTURED_EVENT",
    2: "SURPRISE",
    3: "STORY",
    4: "EVENT_REACTION",
    5: "ALL",
}

# -----------------------------------------------------------------------------
# Service-local bounded reverse policy
# -----------------------------------------------------------------------------
def _install_service64_reverse_policy() -> None:
    try:
        import reverse_learning as rl  # type: ignore
    except Exception:
        # Allows model.py to be imported in isolated unit tests without shared/.
        return

    if getattr(rl, "_S64_STRUCT_POLICY_INSTALLED", False):
        return

    # ---- 1) var must describe the economic hypothesis, not change extrema. ----
    orig_back = rl.collect_extremums_back
    orig_forward = rl.collect_extremums_forward

    def collect_back_fixed(np_simple_rates, control_date, *, limit=50, extremum_interval=3):
        return orig_back(
            np_simple_rates,
            control_date,
            limit=limit,
            extremum_interval=REVERSE_FIXED_EXTREMUM_INTERVAL,
        )

    def collect_forward_fixed(np_simple_rates, control_date, *, limit=50, extremum_interval=3):
        return orig_forward(
            np_simple_rates,
            control_date,
            limit=limit,
            extremum_interval=REVERSE_FIXED_EXTREMUM_INTERVAL,
        )

    rl.collect_extremums_back = collect_back_fixed
    rl.collect_extremums_forward = collect_forward_fixed

    # ---- 2) amplitude modes use local range units instead of raw price units. ----
    # This preserves the useful lesson of service 58 (magnitude matters) while
    # making a 2-ATR move comparable between EUR, BTC and ETH.
    def diff_amp_normalized(
        np_simple_rates: dict | None,
        date_a: datetime,
        date_b: datetime | None,
    ) -> float:
        if not np_simple_rates:
            return 1.0
        dates_ns = np_simple_rates.get("dates_ns")
        close = np_simple_rates.get("close")
        ranges = np_simple_rates.get("ranges")
        if dates_ns is None or close is None or len(close) == 0:
            return 1.0

        n = len(close)
        ia = min(
            int(np.searchsorted(dates_ns, np.int64(int(date_a.timestamp())), side="left")),
            n - 1,
        )
        if ia < 0:
            return 1.0

        if date_b is None:
            # No known neighbour: do not inject abs(price) as an amplitude label.
            # A neutral 1 range-unit fallback is safer and cross-asset stable.
            raw_move = 0.0
        else:
            ib = min(
                int(np.searchsorted(dates_ns, np.int64(int(date_b.timestamp())), side="left")),
                n - 1,
            )
            raw_move = abs(float(close[ia]) - float(close[ib]))

        start = max(0, ia - REVERSE_AMP_LOOKBACK + 1)
        if ranges is not None and len(ranges) >= ia + 1:
            local = np.asarray(ranges[start : ia + 1], dtype=np.float64)
            local = local[np.isfinite(local) & (local > 0)]
            scale = float(np.median(local)) if local.size else 0.0
        else:
            scale = 0.0

        if not math.isfinite(scale) or scale <= 1e-12:
            px = abs(float(close[ia]))
            scale = max(px * 1e-4, 1e-9)

        if not math.isfinite(raw_move) or raw_move <= 0:
            amp = 1.0
        else:
            amp = raw_move / scale

        return float(min(max(amp, REVERSE_AMP_MIN), REVERSE_AMP_MAX))

    rl._diff_amp = diff_amp_normalized

    # ---- 3) bound reverse magnitude by UNIFORM scaling. -----------------------
    # Uniform positive scaling leaves reverse-learning precision invariant:
    #   sum(w)*sign / sum(abs(w))
    # and preserves every relative weight/sign.  It only removes meaningless
    # absolute explosions such as 1e30..1e60 seen in old services.
    def _bound_array(arr):
        a = np.asarray(arr, dtype=np.float64)
        if a.size == 0:
            return a
        if not np.all(np.isfinite(a)):
            a = np.nan_to_num(
                a,
                nan=0.0,
                posinf=REVERSE_MAX_ABS_WEIGHT,
                neginf=-REVERSE_MAX_ABS_WEIGHT,
            )
        mx = float(np.max(np.abs(a))) if a.size else 0.0
        if mx > REVERSE_MAX_ABS_WEIGHT:
            a = a.copy()
            a *= REVERSE_MAX_ABS_WEIGHT / mx
        return a

    def _bound_dict(weights: dict[str, float]) -> dict[str, float]:
        if not weights:
            return {}
        clean: dict[str, float] = {}
        mx = 0.0
        for key, value in weights.items():
            try:
                v = float(value)
            except (TypeError, ValueError):
                continue
            if math.isnan(v):
                v = 0.0
            elif math.isinf(v):
                v = math.copysign(REVERSE_MAX_ABS_WEIGHT, v)
            clean[str(key)] = v
            mx = max(mx, abs(v))
        if mx > REVERSE_MAX_ABS_WEIGHT and mx > 0:
            scale = REVERSE_MAX_ABS_WEIGHT / mx
            clean = {k: v * scale for k, v in clean.items()}
        return clean

    if hasattr(rl, "_rebalance_prefix_fast"):
        orig_prefix = rl._rebalance_prefix_fast

        def bounded_prefix(*args, **kwargs):
            arr, precision, iterations = orig_prefix(*args, **kwargs)
            return _bound_array(arr), precision, iterations

        rl._rebalance_prefix_fast = bounded_prefix

    if hasattr(rl, "_rebalance_weights_py"):
        orig_rebalance_py = rl._rebalance_weights_py

        def bounded_rebalance_py(*args, **kwargs):
            weights, precision, iterations = orig_rebalance_py(*args, **kwargs)
            return _bound_dict(weights), precision, iterations

        rl._rebalance_weights_py = bounded_rebalance_py

    if hasattr(rl, "train_prebuilt_records"):
        orig_train_prebuilt = rl.train_prebuilt_records

        async def bounded_train_prebuilt(*args, **kwargs):
            universe, precision, iterations, ext_count, from_cache = await orig_train_prebuilt(*args, **kwargs)
            return _bound_dict(universe), precision, iterations, ext_count, from_cache

        rl.train_prebuilt_records = bounded_train_prebuilt

    # Existing rows are unlikely for a brand-new service, but normalise them too
    # so a restart can never resurrect an old runaway universe.
    if hasattr(rl, "ReverseStore") and hasattr(rl.ReverseStore, "load_universe"):
        orig_load_universe = rl.ReverseStore.load_universe

        async def bounded_load_universe(self, *args, **kwargs):
            loaded = await orig_load_universe(self, *args, **kwargs)
            if not loaded:
                return loaded
            universe, precision = loaded
            bounded = _bound_dict(universe)
            # Keep the in-memory copy bounded as well.
            return bounded, precision

        rl.ReverseStore.load_universe = bounded_load_universe

    rl._S64_STRUCT_POLICY_INSTALLED = True


_install_service64_reverse_policy()

# -----------------------------------------------------------------------------
# Deterministic text parser — no LLM
# -----------------------------------------------------------------------------

_SPACE_RE = re.compile(r"\s+")
_WORD_RE = re.compile(r"[a-z0-9][a-z0-9'\-]{2,}", re.I)
_TERM_RE_CACHE: dict[str, re.Pattern[str]] = {}

STOP = {
    "the","and","for","with","from","that","this","into","after","before","about",
    "amid","over","under","more","than","says","said","say","new","latest","live",
    "report","reports","will","would","could","should","may","might","its","their",
    "they","them","his","her","who","what","when","where","why","how","are","was",
    "were","been","being","have","has","had","not","but","out","off","you","your",
}

FED_CONTEXT_TERMS = (
    "federal reserve", "fomc", "jerome powell", "powell",
    "fed chair", "fed officials", "fed governor", "fed meeting",
    "fed policy", "fed rate", "fed rates",
)

SEC_CONTEXT_TERMS = (
    "securities", "regulator", "regulation", "etf", "crypto",
    "bitcoin", "ethereum", "coinbase", "binance", "filing",
    "filings", "lawsuit", "charges", "charged", "enforcement", "sues", "sued",
    "approve", "approved", "approves", "approval", "reject", "rejected", "rejects",
)

ETHER_CONTEXT_TERMS = (
    "crypto", "cryptocurrency", "ethereum", "etf", "token", "blockchain",
)

ETH_ALIAS_CONTEXT_TERMS = (
    "crypto", "cryptocurrency", "ethereum", "ether", "blockchain", "token",
    "wallet", "smart contract", "defi", "staking", "stablecoin", "binance",
    "coinbase", "kraken", "crypto exchange", "eth/usd", "ethusd", "eth/usdt",
)

ETH_PAIR_ALIASES = ("eth/usd", "ethusd", "eth/usdt", "ethusdt")

USD_MARKET_TERMS = (
    "u.s. dollar", "us dollar", "usd", "dollar index", "dxy",
    "dollar rises", "dollar falls", "dollar gains", "dollar drops",
    "dollar weakens", "dollar strengthens",
)

EUR_MARKET_TERMS = (
    "european central bank", "ecb", "lagarde", "eurozone", "euro area",
    "eur", "eur/usd", "euro rises", "euro falls", "euro gains",
    "euro weakens", "euro strengthens",
)

CRYPTO_SECURITY_TERMS = (
    "crypto", "cryptocurrency", "bitcoin", "btc", "ethereum",
    "blockchain", "defi", "binance", "coinbase", "kraken",
    "stablecoin", "usdt", "usdc",
)

FAMILY_RULES = [
    ("etf", (
        "spot bitcoin etf","bitcoin etf","btc etf","spot ether etf",
        "spot ethereum etf","ethereum etf","ether etf","eth etf",
        "exchange-traded fund","etf",
    )),
    ("monetary_policy", (
        *FED_CONTEXT_TERMS, "european central bank", "ecb", "lagarde",
        "interest rate","rate cut","rate hike","central bank",
        "monetary policy","quantitative easing","quantitative tightening",
    )),
    ("inflation", (
        "inflation","consumer price"," cpi ","producer price"," ppi ","core pce",
        "price pressures","deflation",
    )),
    ("labor", (
        "nonfarm payroll","payrolls","jobs report","unemployment","jobless claims",
        "employment","labor market","labour market","wages",
    )),
    ("growth", (
        " gdp ","gross domestic product","retail sales","manufacturing pmi",
        "services pmi","economic growth","economic slowdown","recession",
    )),
    ("crypto_regulation", (
        "crypto regulation", "cryptocurrency regulation", "crypto regulator",
        "crypto rules", "cryptocurrency rules", "crypto enforcement",
        "digital asset regulation", "digital asset rules",
    )),
    ("exchange", (
        "binance","coinbase","kraken","bitfinex","okx","bybit","crypto exchange",
    )),
    ("security", (
        " hack ","hacked","cyberattack","exploit","exploited","stolen funds",
        "stolen crypto","security breach","data breach",
    )),
    ("stablecoin", (
        "stablecoin","tether","usdt","usd coin","usdc",
    )),
    ("protocol", (
        "bitcoin network","ethereum","blockchain","protocol upgrade","hard fork",
        "staking"," defi ",
    )),
    ("banking", (
        "bank failure","bank run","banking crisis","liquidity crisis","credit crunch",
        "deposit outflow","bankruptcy","insolvent",
    )),
    ("trade", (
        "tariff","trade war","import duty","export ban","trade agreement","trade deal",
    )),
    ("geopolitics", (
        "war","invasion","missile","airstrike","military strike","ceasefire",
        "sanctions","troops","nuclear","conflict",
    )),
    ("energy", (
        "oil price","crude oil","brent"," wti ","opec","natural gas",
    )),
    ("fiscal", (
        "debt ceiling","budget deficit","treasury issuance","government spending",
        "fiscal policy",
    )),
    ("corporate", (
        "earnings","revenue","profit","acquisition","merger","takeover","layoffs",
        "guidance","quarterly results",
    )),
    ("markets", (
        "stock market","wall street","s&p 500","nasdaq","dow jones","bond market",
        "treasury yield","market selloff","market rally","volatility",
    )),
]

ACTION_RULES = [
    ("RATE_HIKE", ("rate hike","raises rates","raised rates","hikes rates","hawkish")),
    ("RATE_CUT", ("rate cut","cuts rates","cut rates","dovish","easing cycle")),
    ("RATE_HOLD", ("holds rates","keeps rates unchanged","leaves rates unchanged","holds steady")),
    ("APPROVE", (" approved "," approves "," approval "," greenlights "," cleared ")),
    ("REJECT", (" rejected "," rejects "," denied "," blocks "," refused ")),
    ("DELAY", (" delays "," delayed "," postpones "," postponed "," extends review")),
    ("FILE", (" files for "," filed for "," filing "," submits "," submitted ")),
    ("INFLOW", (" inflow"," inflows","funds flow into")),
    ("OUTFLOW", (" outflow"," outflows","withdrawals surge")),
    ("HACK", (" hack "," hacked "," cyberattack "," exploit "," exploited ")),
    ("HALT", ("halts withdrawals","suspends withdrawals","trading halt","halts trading","suspends trading")),
    ("RESUME", ("resumes withdrawals","restores withdrawals","resumes trading")),
    ("BAN", (" bans "," ban on "," crackdown","prohibits","outlaws")),
    ("SANCTION", (" sanctions "," sanctioned ")),
    ("CEASEFIRE", ("ceasefire","peace agreement","peace deal")),
    ("ESCALATE", ("escalates","invasion","military strike","missile attack","airstrike")),
    ("BANKRUPTCY", ("bankruptcy","insolvent","collapse","files for chapter 11")),
    ("DELIST", (" delists "," delisting "," delisted ")),
    ("LIST", (" lists "," listing "," listed on ")),
    ("ACQUIRE", (" acquisition "," acquires "," merger "," takeover ")),
    ("LAUNCH", (" launches "," launch of "," rolls out ")),
    ("UPGRADE", (" upgrade "," upgrades "," upgraded ")),
    ("DOWNGRADE", (" downgrade "," downgrades "," downgraded ")),
]

OBJECT_RULES = [
    ("BTC_ETF", ("spot bitcoin etf","bitcoin etf","btc etf")),
    ("ETH_ETF", ("spot ether etf","spot ethereum etf","ether etf","ethereum etf","eth etf")),
    ("INTEREST_RATE", ("interest rate","fed funds rate","deposit rate","policy rate")),
    ("CPI", ("consumer price"," cpi ")),
    ("PPI", ("producer price"," ppi ")),
    ("PCE", (" pce ","personal consumption expenditures")),
    ("JOBS", ("nonfarm payroll","payrolls","jobs report","unemployment","jobless claims")),
    ("GDP", (" gdp ","gross domestic product")),
    ("BITCOIN", ("bitcoin"," btc ")),
    ("ETHEREUM", ("ethereum",)),
    ("STABLECOIN", ("stablecoin","tether"," usdt ","usd coin"," usdc ")),
    ("EXCHANGE", ("binance","coinbase","kraken","crypto exchange")),
    ("BANK", (" bank ","banking sector")),
    ("OIL", ("crude oil","brent"," wti ","oil price")),
    ("TARIFFS", ("tariff","import duty")),
    ("SANCTIONS", ("sanctions","sanctioned")),
]

ACTOR_RULES = [
    ("ECB", "CENTRAL_BANK_EU", ("european central bank"," ecb ","christine lagarde","lagarde")),
    ("CFTC", "REGULATOR_US", (" cftc ",)),
    ("US_TREASURY", "GOVERNMENT_US", ("u.s. treasury","us treasury","treasury department")),
    ("BLS", "STATISTICS_US", ("bureau of labor statistics"," bls ")),
    ("BEA", "STATISTICS_US", ("bureau of economic analysis"," bea ")),
    ("BINANCE", "CRYPTO_EXCHANGE", ("binance",)),
    ("COINBASE", "CRYPTO_EXCHANGE", ("coinbase",)),
    ("KRAKEN", "CRYPTO_EXCHANGE", ("kraken",)),
    ("BLACKROCK", "ETF_ISSUER", ("blackrock","ishares")),
    ("FIDELITY", "ETF_ISSUER", ("fidelity",)),
    ("GRAYSCALE", "ETF_ISSUER", ("grayscale",)),
    ("TETHER", "STABLECOIN_ISSUER", ("tether",)),
    ("CIRCLE", "STABLECOIN_ISSUER", (
        "circle internet financial", "circle stablecoin", "circle usdc",
        "usd coin", "usdc",
    )),
    ("OPEC", "ENERGY_ORG", ("opec",)),
]

POSITIVE_TONE = (
    "beats expectations","better than expected","stronger than expected","surges",
    "rallies","record high","deal reached","ceasefire","inflows","adoption","upgrade",
    "profit rises","recovery","reopens","resumes",
)
NEGATIVE_TONE = (
    "misses expectations","worse than expected","weaker than expected","plunges",
    "slumps","recession","crisis","default","bankruptcy","hack","cyberattack","ban ",
    "crackdown","invasion","sanctions","outflows","downgrade","layoffs","collapse",
)

_NUM = r"(-?\d+(?:\.\d+)?)"
_SURPRISE_PATTERNS: list[tuple[re.Pattern, bool]] = [
    (re.compile(rf"(?:actual|came in at|rose to|fell to|at)\s+{_NUM}\s*%?.{{0,80}}?(?:expected|forecast|estimate(?:d)?(?: at| of)?)\s+{_NUM}\s*%?", re.I), False),
    (re.compile(rf"{_NUM}\s*%?\s+(?:vs\.?|versus)\s+(?:an?\s+)?(?:expected|forecast|estimate(?:d)?)\s+{_NUM}\s*%?", re.I), False),
    (re.compile(rf"(?:expected|forecast|estimate(?:d)?(?: at| of)?)\s+{_NUM}\s*%?.{{0,80}}?(?:actual|came in at|rose to|fell to)\s+{_NUM}\s*%?", re.I), True),
]
_BPS_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:basis points|bps|bp)\b", re.I)
_PCT_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*%", re.I)
_MAGNITUDE_CONTEXT = (
    "rate", "rates", "interest rate", "inflation", "consumer price", "producer price",
    "price", "prices", "cpi", "ppi", "pce", "jobs", "jobless", "employment",
    "unemployment", "payroll", "payrolls", "wage", "wages", "gdp",
    "gross domestic product", "growth", "yield", "yields", "revenue", "profit",
    "earnings", "sales", "tariff", "tariffs", "deficit", "debt", "budget",
    "forecast", "expected", "estimate", "actual", "market share", "inflow",
    "inflows", "outflow", "outflows", "bitcoin", "btc", "ethereum", "eth",
    "ether", "crypto", "stock", "stocks", "bond", "bonds", "oil", "gas",
)


def _clean(v: Any) -> str:
    s = html.unescape(str(v or "")).lower().replace("\x00", " ")
    return " " + _SPACE_RE.sub(" ", s).strip() + " "


def _term_pattern(term: str) -> re.Pattern[str]:
    """Compile an exact token/phrase matcher instead of using substrings."""
    normalized = _SPACE_RE.sub(" ", str(term or "").strip().lower())
    pattern = _TERM_RE_CACHE.get(normalized)
    if pattern is None:
        body = r"\s+".join(re.escape(part) for part in normalized.split(" "))
        pattern = re.compile(rf"(?<![a-z0-9]){body}(?![a-z0-9])", re.I)
        _TERM_RE_CACHE[normalized] = pattern
    return pattern


def _contains(text: str, phrases: tuple[str, ...]) -> bool:
    return any(_term_pattern(p).search(text) is not None for p in phrases if str(p).strip())


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _as_dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v.replace(tzinfo=None) if v.tzinfo else v
    if v is None:
        return None
    try:
        d = datetime.fromisoformat(str(v)[:19])
        return d.replace(tzinfo=None) if d.tzinfo else d
    except Exception:
        return None


def _find_rule(text: str, rules, default: str) -> str:
    for name, phrases in rules:
        if _contains(text, phrases):
            return name
    return default


def _actor(text: str) -> tuple[str, str]:
    if _contains(text, FED_CONTEXT_TERMS):
        return "FED", "CENTRAL_BANK_US"

    if _contains(text, ("securities and exchange commission",)):
        return "SEC", "REGULATOR_US"

    if _contains(text, ("sec",)) and _contains(text, SEC_CONTEXT_TERMS):
        return "SEC", "REGULATOR_US"

    if _contains(text, ("circle",)) and _contains(text, ("stablecoin", "usdc", "usd coin")):
        return "CIRCLE", "STABLECOIN_ISSUER"

    for actor, actor_class, phrases in ACTOR_RULES:
        if _contains(text, phrases):
            return actor, actor_class
    return "OTHER", "OTHER"


def _object(text: str) -> str:
    obj = _find_rule(text, OBJECT_RULES, "OTHER")
    if obj == "OTHER" and _has_eth_alias_context(text):
        return "ETHEREUM"
    return obj


def _has_eth_alias_context(text: str) -> bool:
    return (
        _contains(text, ETH_PAIR_ALIASES)
        or (_contains(text, ("ether",)) and _contains(text, ETHER_CONTEXT_TERMS))
        or (_contains(text, ("eth",)) and _contains(text, ETH_ALIAS_CONTEXT_TERMS))
    )


def _orientation(action: str) -> str:
    if action == "RATE_HIKE":
        return "TIGHTEN"
    if action == "RATE_CUT":
        return "EASE"
    if action in {"APPROVE","RESUME","CEASEFIRE","UPGRADE"}:
        return "ENABLE"
    if action in {"REJECT","BAN","HALT","HACK","BANKRUPTCY","SANCTION","DOWNGRADE","DELIST"}:
        return "RESTRICT"
    if action == "ESCALATE":
        return "ESCALATE"
    return "NEUTRAL"


def _tone(text: str) -> str:
    p = sum(1 for x in POSITIVE_TONE if x in text)
    n = sum(1 for x in NEGATIVE_TONE if x in text)
    return "POS" if p > n else ("NEG" if n > p else "NEU")


def _certainty(text: str) -> str:
    if _contains(text, (
        "rumor", "rumour", "reportedly", "sources say",
        "might approve", "might reject", "might cut", "might raise", "might launch",
        "could approve", "could reject", "could cut", "could raise", "could launch",
        "may be", "may have", "may seek", "may consider", "may plan",
    )):
        return "RUMOR"
    if _contains(text, (
        "plans to", "proposes", "proposal", "considering", "seeks approval", "files for",
    )):
        return "PLANNED"
    if _contains(text, (
        "confirmed", "official", "announced", "approved", "completed", "effective immediately",
    )):
        return "CONFIRMED"
    return "REPORTED"


def _surprise(text: str) -> tuple[str, float | None]:
    for pat, reverse in _SURPRISE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        try:
            a = float(m.group(1))
            b = float(m.group(2))
            actual, forecast = (b, a) if reverse else (a, b)
            delta = actual - forecast
        except Exception:
            continue
        if abs(delta) <= 1e-12:
            return "INLINE", 0.0
        return ("ABOVE" if delta > 0 else "BELOW"), delta

    if _contains(text, ("beats expectations","above expectations","better than expected","stronger than expected")):
        return "BEAT", None
    if _contains(text, ("misses expectations","below expectations","worse than expected","weaker than expected")):
        return "MISS", None
    return "NONE", None


def _has_magnitude_context(text: str, start: int, end: int) -> bool:
    # A percentage is meaningful only when a nearby phrase identifies a
    # financial/economic measure. This prevents arbitrary percentages elsewhere
    # in an article from becoming event magnitude.
    window = text[max(0, start - 96):min(len(text), end + 96)]
    return _contains(window, _MAGNITUDE_CONTEXT)


def _magnitude(text: str) -> str:
    # Basis points are themselves an unambiguous financial unit.
    bps_vals = [float(m.group(1)) for m in _BPS_RE.finditer(text)]
    if bps_vals:
        v = max(abs(x) for x in bps_vals)
        return "H" if v >= 75 else ("M" if v >= 25 else "L")

    vals = [
        float(m.group(1))
        for m in _PCT_RE.finditer(text)
        if _has_magnitude_context(text, m.start(), m.end())
    ]
    if vals:
        v = max(abs(x) for x in vals)
        return "H" if v >= 5.0 else ("M" if v >= 1.0 else "L")
    return "U"


def _story_terms(title: str, family: str, actor_class: str, action: str, obj: str) -> str:
    words = [
        w.lower() for w in _WORD_RE.findall(title)
        if len(w) >= 4 and w.lower() not in STOP and not w.isdigit()
    ]
    ranked = sorted(set(words), key=lambda x: (-len(x), x))[:12]
    if not ranked:
        ranked = [x.lower() for x in (family, actor_class, action, obj) if x not in ("other","OTHER")]
    return " ".join(ranked[:12])


def _relevance(text: str, family: str, actor_class: str, obj: str) -> dict[str, int]:
    r = {"usd": 0, "eur": 0, "btc": 0, "eth": 0}

    if (
        _contains(text, FED_CONTEXT_TERMS)
        or _contains(text, ("u.s. economy", "us economy"))
        or _contains(text, USD_MARKET_TERMS)
    ):
        r["usd"] = 3
    if _contains(text, EUR_MARKET_TERMS):
        r["eur"] = 3

    if _contains(text, ("bitcoin", "btc")):
        r["btc"] = 3
    if _contains(text, ("ethereum",)):
        r["eth"] = 3

    if _has_eth_alias_context(text):
        r["eth"] = 3

    if obj == "BTC_ETF":
        r["btc"] = 3
    elif obj == "ETH_ETF":
        r["eth"] = 3

    # These families and actors are crypto-specific by definition. They affect
    # both crypto pairs even when the headline names an exchange, stablecoin or
    # protocol rather than Bitcoin/Ethereum directly.
    if (
        family in {"crypto_regulation", "exchange", "stablecoin", "protocol"}
        or actor_class in {"CRYPTO_EXCHANGE", "STABLECOIN_ISSUER"}
    ):
        r["btc"] = max(r["btc"], 2)
        r["eth"] = max(r["eth"], 2)

    # Security is broad; project it to crypto only in explicit crypto context.
    if family == "security" and (
        _contains(text, CRYPTO_SECURITY_TERMS) or _has_eth_alias_context(text)
    ):
        r["btc"] = max(r["btc"], 2)
        r["eth"] = max(r["eth"], 2)

    if family in {"monetary_policy","inflation","labor","growth","fiscal","trade"}:
        r["usd"] = max(r["usd"], 2)
        r["eur"] = max(r["eur"], 1)

    if family in {"geopolitics","banking","energy","markets"}:
        r["usd"] = max(r["usd"], 1)
        r["eur"] = max(r["eur"], 1)

    if actor_class == "CENTRAL_BANK_US":
        r["usd"] = 3
    elif actor_class == "CENTRAL_BANK_EU":
        r["eur"] = 3

    return r


def _importance(family: str, action: str, actor_class: str, surprise: str, magnitude: str, title: str) -> str:
    points = 0
    points += 1 if family != "other" else 0
    points += 2 if action != "OTHER" else 0
    points += 1 if actor_class != "OTHER" else 0
    points += 2 if surprise != "NONE" else 0
    points += 2 if magnitude == "H" else (1 if magnitude == "M" else 0)
    points += 1 if _contains(title, ("breaking","unexpected","emergency","record","crisis")) else 0
    return "H" if points >= 6 else ("M" if points >= 3 else "L")


def _extract_static(press: str, raw: dict[str, Any]) -> dict[str, Any] | None:
    dt = _as_dt(raw.get("date_dt"))
    if dt is None or dt < SOURCE_WARM_FROM:
        return None

    title_raw = str(raw.get("title") or "")
    body_raw = str(raw.get("text") or "")[:8000]
    feed = str(raw.get("feed") or "").lower()[:64]

    title = _clean(title_raw)
    lead_raw = body_raw[:1800]
    signal_text = title + " " + _clean(lead_raw)
    full_text = title + " " + _clean(body_raw)

    family = _find_rule(signal_text, FAMILY_RULES, "other")
    action = _find_rule(signal_text, ACTION_RULES, "OTHER")
    obj = _object(signal_text)
    actor, actor_class = _actor(signal_text)
    orientation = _orientation(action)
    tone = _tone(signal_text)
    certainty = _certainty(signal_text)
    surprise_class, surprise_value = _surprise(full_text)
    magnitude = _magnitude(full_text)
    relevance = _relevance(signal_text, family, actor_class, obj)

    # Runtime ignores zero-relevance events, so keeping them would only pollute
    # story/signature statistics and derived indexes.
    if not any(relevance.values()):
        return None

    importance = _importance(family, action, actor_class, surprise_class, magnitude, title)

    signature = f"{family}|{actor_class}|{action}|{obj}"
    terms = _story_terms(title_raw, family, actor_class, action, obj)
    unexpected_hint = int(
        surprise_class != "NONE"
        or _contains(full_text, ("unexpected","unexpectedly","surprise","shocks markets"))
    )

    return {
        "date_dt": dt,
        "press": press,
        "source_news_id": int(raw["source_news_id"]),
        "feed": feed,
        "family": family,
        "actor": actor,
        "actor_class": actor_class,
        "action": action,
        "object_type": obj,
        "orientation": orientation,
        "tone_class": tone,
        "certainty": certainty,
        "importance_class": importance,
        "magnitude_class": magnitude,
        "surprise_class": surprise_class,
        "surprise_value": surprise_value,
        "unexpected_hint": unexpected_hint,
        "event_signature": signature,
        "event_type": signature,
        "story_terms": terms,
        "relevance_usd": relevance["usd"],
        "relevance_eur": relevance["eur"],
        "relevance_btc": relevance["btc"],
        "relevance_eth": relevance["eth"],
        "pct_change": 0.0,
    }

def _price_state(target: datetime, dataset_index: dict[str, Any]) -> dict[str, Any] | None:
    npr = dataset_index.get("np_rates")
    if not npr:
        return None
    dns = npr.get("dates_ns")
    close = npr.get("close")
    open_ = npr.get("open")
    high = npr.get("max")
    low = npr.get("min")
    ranges = npr.get("ranges")
    if dns is None or close is None or open_ is None or len(dns) < 25:
        return None

    cut = int(np.searchsorted(dns, int(target.timestamp()), side="left"))
    i = cut - 1
    if i < 24:
        return None

    # Reject stale Friday/holiday state being carried into an unrelated target.
    last_ts = int(dns[i])
    age_sec = int(target.timestamp()) - last_ts
    if age_sec < 0 or age_sec > 2 * 3600:
        return None

    c0 = float(close[i])
    c1 = float(close[i - 1])
    c2 = float(close[i - 2])
    c4 = float(close[i - 4])
    o0 = float(open_[i])
    if min(abs(c0), abs(c1), abs(c2), abs(c4), abs(o0)) <= 1e-12:
        return None

    ret1 = c0 / c1 - 1.0
    ret2 = c0 / c2 - 1.0
    ret4 = c0 / c4 - 1.0
    body = c0 / o0 - 1.0

    if ranges is not None and len(ranges) > i:
        local_ranges = np.asarray(ranges[max(0, i - 23) : i + 1], dtype=np.float64)
        local_close = np.asarray(close[max(0, i - 23) : i + 1], dtype=np.float64)
        good = np.isfinite(local_ranges) & np.isfinite(local_close) & (np.abs(local_close) > 1e-12)
        range_pct = np.abs(local_ranges[good] / local_close[good]) if np.any(good) else np.array([], dtype=np.float64)
    elif high is not None and low is not None:
        hh = np.asarray(high[max(0, i - 23) : i + 1], dtype=np.float64)
        ll = np.asarray(low[max(0, i - 23) : i + 1], dtype=np.float64)
        cc = np.asarray(close[max(0, i - 23) : i + 1], dtype=np.float64)
        good = np.isfinite(hh) & np.isfinite(ll) & np.isfinite(cc) & (np.abs(cc) > 1e-12)
        range_pct = np.abs((hh[good] - ll[good]) / cc[good]) if np.any(good) else np.array([], dtype=np.float64)
    else:
        range_pct = np.array([], dtype=np.float64)

    scale24 = float(np.median(range_pct)) if range_pct.size else 0.0
    if not math.isfinite(scale24) or scale24 <= 1e-8:
        # Fallback uses only historical close-to-close returns.
        hist = np.asarray(close[max(0, i - 24) : i + 1], dtype=np.float64)
        rr = np.abs(hist[1:] / hist[:-1] - 1.0)
        rr = rr[np.isfinite(rr) & (rr > 0)]
        scale24 = float(np.median(rr)) if rr.size else 1e-4
    scale24 = max(scale24, 1e-8)

    z1 = ret1 / scale24
    if abs(z1) < 0.25:
        direction = "F"
    else:
        direction = "U" if z1 > 0 else "D"

    az = abs(z1)
    if az < 0.35:
        shock = "F"      # essentially flat
    elif az < 1.0:
        shock = "N"      # normal
    elif az < 2.0:
        shock = "S"      # strong
    else:
        shock = "E"      # extreme

    if abs(body / scale24) < 0.20:
        body_dir = "F"
    else:
        body_dir = "U" if body > 0 else "D"

    z4 = ret4 / max(scale24 * 2.0, 1e-8)
    if abs(z4) < 0.35:
        trend4 = "F"
    else:
        trend4 = "U" if z4 > 0 else "D"

    # Recent-volatility regime relative to the same causal 24h scale.
    if range_pct.size >= 6:
        scale6 = float(np.median(range_pct[-6:]))
        vr = scale6 / scale24 if scale24 > 0 else 1.0
    else:
        vr = 1.0
    vol_regime = "H" if vr >= 1.5 else ("L" if vr <= 0.67 else "N")

    h = target.hour
    session = "00_07" if h < 8 else ("08_15" if h < 16 else "16_23")
    weekend = target.weekday() >= 5

    return {
        "direction": direction,
        "shock": shock,
        "body": body_dir,
        "trend4": trend4,
        "vol": vol_regime,
        "session": session,
        "weekend": weekend,
        "ret1": ret1,
        "ret2": ret2,
        "ret4": ret4,
        "z1": z1,
        "scale24": scale24,
    }

# -----------------------------------------------------------------------------
# Enrichment tables and /rebuild_index integration
# -----------------------------------------------------------------------------

META_TABLE = f"{ENRICHED_TABLE}_meta"


async def _ensure_tables(engine_vlad) -> None:
    from sqlalchemy import text
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{ENRICHED_TABLE}` (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `date_dt` DATETIME NOT NULL,
                `press` VARCHAR(3) NOT NULL,
                `source_news_id` INT NOT NULL,
                `feed` VARCHAR(64) NOT NULL DEFAULT '',

                `family` VARCHAR(32) NOT NULL DEFAULT 'other',
                `actor` VARCHAR(48) NOT NULL DEFAULT 'OTHER',
                `actor_class` VARCHAR(32) NOT NULL DEFAULT 'OTHER',
                `action` VARCHAR(32) NOT NULL DEFAULT 'OTHER',
                `object_type` VARCHAR(32) NOT NULL DEFAULT 'OTHER',
                `orientation` VARCHAR(24) NOT NULL DEFAULT 'NEUTRAL',
                `tone_class` VARCHAR(8) NOT NULL DEFAULT 'NEU',
                `certainty` VARCHAR(16) NOT NULL DEFAULT 'REPORTED',

                `importance_class` CHAR(1) NOT NULL DEFAULT 'L',
                `magnitude_class` CHAR(1) NOT NULL DEFAULT 'U',
                `surprise_class` VARCHAR(16) NOT NULL DEFAULT 'NONE',
                `surprise_value` DOUBLE NULL,
                `unexpected_hint` TINYINT NOT NULL DEFAULT 0,

                `event_signature` VARCHAR(160) NOT NULL,
                `event_type` VARCHAR(160) NOT NULL,
                `story_terms` VARCHAR(255) NOT NULL DEFAULT '',

                `story_key` CHAR(40) NOT NULL DEFAULT '',
                `story_stage` VARCHAR(16) NOT NULL DEFAULT 'FIRST',
                `novelty_class` CHAR(1) NOT NULL DEFAULT 'N',
                `expectedness_class` VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',

                `article_count_1h` SMALLINT NOT NULL DEFAULT 1,
                `article_count_4h` SMALLINT NOT NULL DEFAULT 1,
                `source_count_1h` TINYINT NOT NULL DEFAULT 1,
                `source_count_4h` TINYINT NOT NULL DEFAULT 1,
                `velocity_class` CHAR(1) NOT NULL DEFAULT 'L',

                `prior_signature_count_180d` INT NOT NULL DEFAULT 0,
                `prior_signature_gap_hours` DOUBLE NULL,

                `relevance_usd` TINYINT NOT NULL DEFAULT 0,
                `relevance_eur` TINYINT NOT NULL DEFAULT 0,
                `relevance_btc` TINYINT NOT NULL DEFAULT 0,
                `relevance_eth` TINYINT NOT NULL DEFAULT 0,

                `pct_change` DOUBLE NOT NULL DEFAULT 0.0,

                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,

                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_source` (`press`,`source_news_id`),
                KEY `idx_date` (`date_dt`),
                KEY `idx_event_type` (`event_type`),
                KEY `idx_signature_date` (`event_signature`,`date_dt`),
                KEY `idx_story_date` (`story_key`,`date_dt`),
                KEY `idx_family_date` (`family`,`date_dt`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{META_TABLE}` (
                `meta_key` VARCHAR(64) NOT NULL,
                `meta_value` VARCHAR(255) NOT NULL,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`meta_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def _meta_get(engine_vlad, key: str) -> str | None:
    from sqlalchemy import text
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(
            text(f"SELECT meta_value FROM `{META_TABLE}` WHERE meta_key=:k"),
            {"k": key},
        )).fetchone()
    return str(row[0]) if row else None


async def _meta_set(engine_vlad, key: str, value: Any) -> None:
    from sqlalchemy import text
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{META_TABLE}`(meta_key,meta_value)
            VALUES(:k,:v)
            ON DUPLICATE KEY UPDATE meta_value=VALUES(meta_value)
        """), {"k": key, "v": str(value)})


async def _fetch_raw(engine_brain, table: str, last_id: int) -> list[dict[str, Any]]:
    from sqlalchemy import text
    async with engine_brain.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT
                id AS source_news_id,
                title,
                text,
                date AS date_dt,
                feed
            FROM `{table}`
            WHERE id > :last_id
              AND date IS NOT NULL
              AND date >= :source_from
              AND title IS NOT NULL
              AND title <> ''
            ORDER BY id ASC
            LIMIT {ENRICH_BATCH}
        """), {"last_id": int(last_id), "source_from": SOURCE_WARM_FROM})
        return [dict(r) for r in res.mappings().all()]


async def _source_max_id(engine_brain, table: str) -> int:
    from sqlalchemy import text
    async with engine_brain.connect() as conn:
        value = (await conn.execute(text(f"SELECT COALESCE(MAX(id),0) FROM `{table}`"))).scalar()
    return int(value or 0)


async def _upsert_static(engine_vlad, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    from sqlalchemy import text
    sql = text(f"""
        INSERT INTO `{ENRICHED_TABLE}` (
            date_dt,press,source_news_id,feed,
            family,actor,actor_class,action,object_type,orientation,tone_class,certainty,
            importance_class,magnitude_class,surprise_class,surprise_value,unexpected_hint,
            event_signature,event_type,story_terms,
            relevance_usd,relevance_eur,relevance_btc,relevance_eth,pct_change
        ) VALUES (
            :date_dt,:press,:source_news_id,:feed,
            :family,:actor,:actor_class,:action,:object_type,:orientation,:tone_class,:certainty,
            :importance_class,:magnitude_class,:surprise_class,:surprise_value,:unexpected_hint,
            :event_signature,:event_type,:story_terms,
            :relevance_usd,:relevance_eur,:relevance_btc,:relevance_eth,:pct_change
        )
        ON DUPLICATE KEY UPDATE
            date_dt=VALUES(date_dt), feed=VALUES(feed),
            family=VALUES(family), actor=VALUES(actor), actor_class=VALUES(actor_class),
            action=VALUES(action), object_type=VALUES(object_type),
            orientation=VALUES(orientation), tone_class=VALUES(tone_class),
            certainty=VALUES(certainty), importance_class=VALUES(importance_class),
            magnitude_class=VALUES(magnitude_class), surprise_class=VALUES(surprise_class),
            surprise_value=VALUES(surprise_value), unexpected_hint=VALUES(unexpected_hint),
            event_signature=VALUES(event_signature), event_type=VALUES(event_type),
            story_terms=VALUES(story_terms),
            relevance_usd=VALUES(relevance_usd), relevance_eur=VALUES(relevance_eur),
            relevance_btc=VALUES(relevance_btc), relevance_eth=VALUES(relevance_eth),
            pct_change=VALUES(pct_change)
    """)
    async with engine_vlad.begin() as conn:
        for i in range(0, len(rows), 800):
            await conn.execute(sql, rows[i:i+800])


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    u = len(a | b)
    return (len(a & b) / u) if u else 0.0


def _story_score(row: dict[str, Any], story: dict[str, Any], terms: set[str]) -> float:
    score = _jaccard(terms, story["terms"])
    if row["family"] != "other" and row["family"] == story["family"]:
        score += 0.12
    if row["action"] != "OTHER" and row["action"] == story["action"]:
        score += 0.10
    if row["object_type"] != "OTHER" and row["object_type"] == story["object_type"]:
        score += 0.10
    if row["actor_class"] != "OTHER" and row["actor_class"] == story["actor_class"]:
        score += 0.06
    return score


def _velocity_class(
    count1: int,
    sources1: int,
    prev_count1: int,
    prev_sources1: int,
    prior_events4: int,
) -> str:
    """Classify publication acceleration using two equal causal windows."""
    if prior_events4 == 0:
        return "L"

    article_growth = count1 > prev_count1
    source_growth = sources1 > prev_sources1
    high_article_growth = count1 >= max(3, 2 * max(prev_count1, 1))
    high_source_growth = sources1 >= max(2, 2 * max(prev_sources1, 1))

    if (article_growth and high_article_growth) or (source_growth and high_source_growth):
        return "H"
    if article_growth or source_growth:
        return "M"
    return "L"


async def _causal_rebuild(engine_vlad, recompute_from: datetime) -> int:
    """Rebuild lifecycle fields chronologically; no future row can update an older row."""
    from sqlalchemy import text

    warm_start = max(SOURCE_WARM_FROM, recompute_from - timedelta(days=EXPECTEDNESS_DAYS))

    stories: dict[str, dict[str, Any]] = {}
    term_index: dict[str, set[str]] = defaultdict(set)
    expiry_queue: deque[tuple[datetime, str]] = deque()
    signature_history: dict[str, deque[datetime]] = defaultdict(deque)

    cursor_dt = warm_start
    cursor_id = 0
    updates: list[dict[str, Any]] = []
    updated = 0

    async def flush() -> None:
        nonlocal updates, updated
        if not updates:
            return
        sql = text(f"""
            UPDATE `{ENRICHED_TABLE}`
            SET story_key=:story_key,
                story_stage=:story_stage,
                novelty_class=:novelty_class,
                expectedness_class=:expectedness_class,
                article_count_1h=:article_count_1h,
                article_count_4h=:article_count_4h,
                source_count_1h=:source_count_1h,
                source_count_4h=:source_count_4h,
                velocity_class=:velocity_class,
                prior_signature_count_180d=:prior_signature_count_180d,
                prior_signature_gap_hours=:prior_signature_gap_hours
            WHERE id=:id
        """)
        async with engine_vlad.begin() as conn:
            await conn.execute(sql, updates)
        updated += len(updates)
        updates = []

    while True:
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT
                    id,date_dt,press,source_news_id,
                    family,actor_class,action,object_type,
                    event_signature,story_terms,
                    surprise_class,unexpected_hint
                FROM `{ENRICHED_TABLE}`
                WHERE date_dt >= :warm_start
                  AND (
                        date_dt > :cursor_dt
                        OR (date_dt = :cursor_dt AND id > :cursor_id)
                  )
                ORDER BY date_dt ASC,id ASC
                LIMIT 5000
            """), {
                "warm_start": warm_start,
                "cursor_dt": cursor_dt,
                "cursor_id": cursor_id,
            })
            rows = [dict(r) for r in res.mappings().all()]

        if not rows:
            break

        for row in rows:
            dt = _as_dt(row["date_dt"])
            if dt is None:
                continue

            # Expire story candidates older than 72h.
            cutoff_story = dt - timedelta(hours=STORY_MATCH_HOURS)
            while expiry_queue and expiry_queue[0][0] < cutoff_story:
                old_dt, old_key = expiry_queue.popleft()
                st_old = stories.get(old_key)
                if st_old is None or st_old["last_dt"] != old_dt:
                    continue
                for term in st_old["terms"]:
                    bucket = term_index.get(term)
                    if bucket:
                        bucket.discard(old_key)
                        if not bucket:
                            term_index.pop(term, None)
                stories.pop(old_key, None)

            # Strictly prior 180-day signature history.
            signature = str(row.get("event_signature") or "other")
            hist = signature_history[signature]
            cutoff_hist = dt - timedelta(days=EXPECTEDNESS_DAYS)
            while hist and hist[0] < cutoff_hist:
                hist.popleft()

            prior_count = len(hist)
            gap_hours = ((dt - hist[-1]).total_seconds() / 3600.0) if hist else None

            if gap_hours is None or gap_hours >= 24:
                novelty = "N"
            elif gap_hours >= 6:
                novelty = "F"
            else:
                novelty = "R"

            if int(row.get("unexpected_hint") or 0):
                expectedness = "UNEXPECTED"
            elif prior_count <= 1:
                expectedness = "RARE"
            elif prior_count >= 20:
                expectedness = "COMMON"
            else:
                expectedness = "NORMAL"

            # Add current event only after computing its prior state.
            hist.append(dt)

            terms = set(str(row.get("story_terms") or "").split())
            candidate_keys: set[str] = set()
            for term in terms:
                candidate_keys.update(term_index.get(term, ()))

            best_key = None
            best_score = 0.0
            for key in candidate_keys:
                st = stories.get(key)
                if st is None:
                    continue
                score = _story_score(row, st, terms)
                if score > best_score:
                    best_score = score
                    best_key = key

            if best_key is None or best_score < 0.48:
                story_key = hashlib.sha1(
                    f"{row['press']}:{row['source_news_id']}".encode("utf-8")
                ).hexdigest()
                st = {
                    "last_dt": dt,
                    "terms": set(terms),
                    "family": row["family"],
                    "action": row["action"],
                    "object_type": row["object_type"],
                    "actor_class": row["actor_class"],
                    "events": deque(),
                }
                stories[story_key] = st
                for term in terms:
                    term_index[term].add(story_key)
            else:
                story_key = best_key
                st = stories[story_key]
                old_terms = set(st["terms"])
                merged = sorted(old_terms | terms, key=lambda x: (-len(x), x))[:16]
                new_terms = set(merged)
                if new_terms != old_terms:
                    for term in old_terms:
                        bucket = term_index.get(term)
                        if bucket:
                            bucket.discard(story_key)
                    for term in new_terms:
                        term_index[term].add(story_key)
                    st["terms"] = new_terms
                st["last_dt"] = dt

            expiry_queue.append((dt, story_key))

            events: deque[tuple[datetime, str]] = st["events"]
            cutoff4 = dt - timedelta(hours=4)
            while events and events[0][0] < cutoff4:
                events.popleft()

            # Counts before current event determine FIRST/FOLLOWUP causally.
            prior_events4 = len(events)
            events.append((dt, str(row["press"])))

            events1 = [e for e in events if e[0] >= dt - timedelta(hours=1)]
            count1 = len(events1)
            count4 = len(events)
            sources1 = len({e[1] for e in events1})
            sources4 = len({e[1] for e in events})

            if prior_events4 == 0:
                stage = "FIRST"
            elif count4 >= 6 or sources4 >= 4:
                stage = "SATURATED"
            elif sources4 >= 3:
                stage = "MULTI_SOURCE"
            elif sources4 >= 2:
                stage = "CONFIRMED"
            else:
                stage = "FOLLOWUP"

            # Compare equal one-hour windows. A lone first publication is never
            # a burst; M/H require an actual increase in article or source count
            # over the immediately preceding hour.
            previous1 = [
                e for e in events
                if dt - timedelta(hours=2) <= e[0] < dt - timedelta(hours=1)
            ]
            prev_count1 = len(previous1)
            prev_sources1 = len({e[1] for e in previous1})

            velocity_class = _velocity_class(
                count1,
                sources1,
                prev_count1,
                prev_sources1,
                prior_events4,
            )

            if dt >= recompute_from:
                updates.append({
                    "id": int(row["id"]),
                    "story_key": story_key,
                    "story_stage": stage,
                    "novelty_class": novelty,
                    "expectedness_class": expectedness,
                    "article_count_1h": count1,
                    "article_count_4h": count4,
                    "source_count_1h": sources1,
                    "source_count_4h": sources4,
                    "velocity_class": velocity_class,
                    "prior_signature_count_180d": prior_count,
                    "prior_signature_gap_hours": gap_hours,
                })

            if len(updates) >= 1500:
                await flush()

            cursor_dt = dt
            cursor_id = int(row["id"])

    await flush()
    return updated


async def _truncate_if_exists(engine_vlad, table_name: str) -> bool:
    """Safely truncate a service-local table if it exists."""
    from sqlalchemy import text

    async with engine_vlad.connect() as conn:
        exists = (await conn.execute(
            text("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = :t
            """),
            {"t": table_name},
        )).scalar()

    if not int(exists or 0):
        return False

    # table_name is an internal constant, never user input.
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{table_name}`"))
    return True


async def _build_structured_news(engine_vlad, engine_brain, *, force_full: bool = False) -> dict[str, Any]:
    from sqlalchemy import text

    await _ensure_tables(engine_vlad)
    version = await _meta_get(engine_vlad, "schema_version")
    full = force_full or version != SCHEMA_VERSION

    reset_tables: list[str] = []

    if full:
        # Service 64 keeps the same port/URL while the model semantics are being
        # replaced.  Old values/reverse universes use the same params_hash
        # (type,var,param) and MUST NOT survive into the new algorithm.
        #
        # This branch executes only on a structured schema-version change/full
        # rebuild, not on ordinary incremental rebuilds.
        # Values cache lives in engine_cache / SUPER_* and must be cleared once
        # on the cache-writer through /clear_cache?also_backtest=true. Only the
        # reverse-learning tables below belong to engine_vlad.
        for stale_table in (
            "vlad_reverse_universe_svc8926",
            "vlad_reverse_jobs_svc8926",
        ):
            if await _truncate_if_exists(engine_vlad, stale_table):
                reset_tables.append(stale_table)

        # Recreate the structured/derived tables on a schema migration so a
        # partially deployed older prototype cannot leave incompatible columns
        # or stale context ids behind.
        async with engine_vlad.begin() as conn:
            for derived in (
                f"{ENRICHED_TABLE}_weights",
                f"{ENRICHED_TABLE}_indexes",
                f"{ENRICHED_TABLE}_mask",
                ENRICHED_TABLE,
            ):
                await conn.execute(text(f"DROP TABLE IF EXISTS `{derived}`"))

        await _ensure_tables(engine_vlad)

        for press in SOURCE_TABLES:
            await _meta_set(engine_vlad, f"last_{press}_id", 0)

    total = 0
    min_new_date: datetime | None = None
    max_new_date: datetime | None = None
    sources: dict[str, Any] = {}

    for press, source_table in SOURCE_TABLES.items():
        try:
            last_id = int(await _meta_get(engine_vlad, f"last_{press}_id") or 0)
        except Exception:
            last_id = 0

        count = 0
        while True:
            batch = await _fetch_raw(engine_brain, source_table, last_id)
            if not batch:
                break

            out: list[dict[str, Any]] = []
            for raw in batch:
                parsed = _extract_static(press, raw)
                if parsed is not None:
                    out.append(parsed)
                    dt = parsed["date_dt"]
                    min_new_date = dt if min_new_date is None or dt < min_new_date else min_new_date
                    max_new_date = dt if max_new_date is None or dt > max_new_date else max_new_date
                last_id = max(last_id, int(raw["source_news_id"]))

            await _upsert_static(engine_vlad, out)
            count += len(out)
            total += len(out)
            await _meta_set(engine_vlad, f"last_{press}_id", last_id)

            if len(batch) < ENRICH_BATCH:
                break

        # Advance through any source rows older than SOURCE_WARM_FROM so they are
        # not rescanned on every incremental rebuild.
        max_source_id = await _source_max_id(engine_brain, source_table)
        if max_source_id > last_id:
            last_id = max_source_id
            await _meta_set(engine_vlad, f"last_{press}_id", last_id)

        sources[press] = {"rows": count, "last_id": last_id}

    causal_updated = 0
    if full:
        causal_updated = await _causal_rebuild(engine_vlad, SOURCE_WARM_FROM)
    elif min_new_date is not None:
        causal_updated = await _causal_rebuild(engine_vlad, min_new_date)

    # Schema version is the commit marker. Record it only after ingestion and
    # the full causal pass have both completed successfully, so an interrupted
    # build is retried as full on the next /rebuild_index.
    if full:
        await _meta_set(engine_vlad, "schema_version", SCHEMA_VERSION)

    return {
        "mode": "full" if full else ("incremental" if total else "noop"),
        "table": ENRICHED_TABLE,
        "articles": total,
        "causal_rows_updated": causal_updated,
        "min_new_date": str(min_new_date) if min_new_date else None,
        "max_new_date": str(max_new_date) if max_new_date else None,
        "sources": sources,
        "reset_tables": reset_tables,
    }


async def enrich_dataset(engine_vlad, engine_brain):
    """Called automatically as step 0 of Brain Framework /rebuild_index."""
    from sqlalchemy import text

    lock_name = "brain_news_structured_v2_s64"
    async with engine_vlad.connect() as conn:
        got = (await conn.execute(text("SELECT GET_LOCK(:n,600)"), {"n": lock_name})).scalar()
        if int(got or 0) != 1:
            return {"mode": "locked", "reason": "another node is rebuilding structured news"}
        try:
            result = await _build_structured_news(engine_vlad, engine_brain)
            clear_runtime_caches()
            return result
        finally:
            try:
                await conn.execute(text("SELECT RELEASE_LOCK(:n)"), {"n": lock_name})
            except Exception:
                pass

# -----------------------------------------------------------------------------
# Runtime news state
# -----------------------------------------------------------------------------

def _pair_id(di: dict[str, Any]) -> int:
    return int(RATES_TO_PAIR.get(str(di.get("rates_table") or RATES_TABLE), 1))


def _rows_window(source: list[dict[str, Any]], target: datetime, di: dict[str, Any], hours_back: int, *, end_before_hours: int = 0) -> list[dict[str, Any]]:
    start = target - timedelta(hours=hours_back)
    end = target - timedelta(hours=end_before_hours)

    ts = di.get("dataset_timestamps")
    if ts is not None and len(ts) == len(source):
        l = int(np.searchsorted(ts, int(start.timestamp()), side="left"))
        r = int(np.searchsorted(ts, int(end.timestamp()), side="left"))
        return source[l:r]

    dates = di.get("dates") or []
    if dates and len(dates) == len(source):
        l = bisect.bisect_left(dates, start)
        r = bisect.bisect_left(dates, end)
        return source[l:r]

    out = []
    for row in source:
        dt = _as_dt(row.get("date") or row.get("date_dt"))
        if dt is not None and start <= dt < end:
            out.append(row)
    return out


def _pair_rel(row: dict[str, Any], pair_id: int) -> tuple[int, int]:
    if pair_id == 1:
        return _safe_int(row.get("relevance_eur")), _safe_int(row.get("relevance_usd"))
    if pair_id == 3:
        return _safe_int(row.get("relevance_btc")), _safe_int(row.get("relevance_usd"))
    if pair_id == 4:
        return _safe_int(row.get("relevance_eth")), _safe_int(row.get("relevance_usd"))
    return 0, 0


def _impact_score(row: dict[str, Any], pair_id: int, target: datetime) -> float:
    dt = _as_dt(row.get("date") or row.get("date_dt"))
    if dt is None or dt >= target:
        return -1.0

    base_rel, quote_rel = _pair_rel(row, pair_id)
    rel = max(base_rel, quote_rel)
    if rel <= 0:
        return -1.0

    age_h = max(0.0, (target - dt).total_seconds() / 3600.0)
    recency = 2.0 ** (-age_h / 2.0)

    imp = {"H": 3.0, "M": 2.0, "L": 1.0}.get(str(row.get("importance_class") or "L"), 1.0)
    src = max(_safe_int(row.get("source_count_4h"), 1), 1)
    vel = {"H": 1.35, "M": 1.15, "L": 1.0}.get(str(row.get("velocity_class") or "L"), 1.0)
    surpr = 1.30 if str(row.get("surprise_class") or "NONE") != "NONE" else 1.0
    rare = 1.20 if str(row.get("expectedness_class") or "") in ("RARE","UNEXPECTED") else 1.0

    return recency * (0.75 + 0.55 * rel) * imp * min(1.0 + 0.10 * (src - 1), 1.4) * vel * surpr * rare


def _count_bucket(n: int) -> str:
    return "6P" if n >= 6 else ("35" if n >= 3 else ("2" if n == 2 else "1"))


def _rel_bucket(n: int) -> str:
    return "H" if n >= 3 else ("M" if n >= 2 else ("L" if n >= 1 else "0"))


def _event_codes(row: dict[str, Any], pair_id: int) -> list[str]:
    family = str(row.get("family") or "other")
    actor = str(row.get("actor") or "OTHER")
    actor_class = str(row.get("actor_class") or "OTHER")
    action = str(row.get("action") or "OTHER")
    obj = str(row.get("object_type") or "OTHER")
    ori = str(row.get("orientation") or "NEUTRAL")
    cert = str(row.get("certainty") or "REPORTED")
    tone = str(row.get("tone_class") or "NEU")
    imp = str(row.get("importance_class") or "L")
    base_rel, quote_rel = _pair_rel(row, pair_id)

    codes = [
        f"E.F.{family}",
        f"E.ORI.{ori}",
        f"E.CERT.{cert}",
        f"E.IMP.{imp}",
        f"E.TONE.{tone}",
        f"E.REL.B.{_rel_bucket(base_rel)}",
        f"E.REL.Q.{_rel_bucket(quote_rel)}",
    ]

    if actor_class != "OTHER":
        codes.append(f"E.AC.{actor_class}")
    if actor != "OTHER":
        codes.append(f"E.ACTOR.{actor}")
    if action != "OTHER":
        codes.append(f"E.ACT.{action}")
    if obj != "OTHER":
        codes.append(f"E.OBJ.{obj}")

    if family != "other" and action != "OTHER":
        codes.append(f"E.FA.{family}.{action}")
    if actor_class != "OTHER" and action != "OTHER":
        codes.append(f"E.AA.{actor_class}.{action}")
    if action != "OTHER" and obj != "OTHER":
        codes.append(f"E.AO.{action}.{obj}")

    return codes


def _surprise_codes(row: dict[str, Any]) -> list[str]:
    return [
        f"S.SUR.{str(row.get('surprise_class') or 'NONE')}",
        f"S.MAG.{str(row.get('magnitude_class') or 'U')}",
        f"S.EXP.{str(row.get('expectedness_class') or 'UNKNOWN')}",
    ]


def _story_codes(row: dict[str, Any]) -> list[str]:
    return [
        f"ST.STAGE.{str(row.get('story_stage') or 'FIRST')}",
        f"ST.NOV.{str(row.get('novelty_class') or 'N')}",
        f"ST.EXP.{str(row.get('expectedness_class') or 'UNKNOWN')}",
        f"ST.VEL.{str(row.get('velocity_class') or 'L')}",
        f"ST.SRC4.{_count_bucket(_safe_int(row.get('source_count_4h'),1))}",
        f"ST.CNT4.{_count_bucket(_safe_int(row.get('article_count_4h'),1))}",
    ]


def _price_codes(price: dict[str, Any]) -> list[str]:
    return [
        f"P.DIR.{price['direction']}",
        f"P.SHK.{price['shock']}",
        f"P.BDY.{price['body']}",
        f"P.TR4.{price['trend4']}",
        f"P.VOL.{price['vol']}",
        f"P.SES.{price['session']}",
        f"P.DAY.{'WE' if price['weekend'] else 'WD'}",
    ]


def _top_story_rows(
    ranked_articles: list[tuple[float, dict[str, Any]]],
    limit: int = 2,
) -> list[dict[str, Any]]:
    """Keep the highest-scoring article from each distinct causal story."""
    best_by_story: dict[str, tuple[float, dict[str, Any]]] = {}

    for score, row in ranked_articles:
        story_key = str(row.get("story_key") or "").strip()
        if not story_key:
            story_key = (
                f"row:{row.get('press', '')}:"
                f"{row.get('source_news_id', row.get('id', ''))}"
            )

        old = best_by_story.get(story_key)
        if old is None or score > old[0]:
            best_by_story[story_key] = (score, row)

    ranked_stories = sorted(
        best_by_story.values(),
        key=lambda x: (-x[0], _safe_int(x[1].get("id"), 0)),
    )
    return [row for _, row in ranked_stories[:max(0, int(limit))]]


def _build_news_state(source: list[dict[str, Any]], target: datetime, di: dict[str, Any], pair_id: int) -> dict[str, Any]:
    fast = _rows_window(source, target, di, FAST_HOURS)
    slow = _rows_window(source, target, di, NARRATIVE_HOURS, end_before_hours=FAST_HOURS)

    ranked_articles: list[tuple[float, dict[str, Any]]] = []
    for row in fast:
        score = _impact_score(row, pair_id, target)
        if score > 0:
            ranked_articles.append((score, row))
    ranked_articles.sort(key=lambda x: (-x[0], _safe_int(x[1].get("id"), 0)))

    # Two distinct top stories keep multiple simultaneous narratives without
    # letting duplicate coverage consume both slots. Codes remain factorized.
    top_rows = _top_story_rows(ranked_articles, 2)

    narrative_scores: dict[str, float] = defaultdict(float)
    narrative_sources: dict[str, set[str]] = defaultdict(set)
    for row in slow:
        base_rel, quote_rel = _pair_rel(row, pair_id)
        rel = max(base_rel, quote_rel)
        if rel <= 0:
            continue
        family = str(row.get("family") or "other")
        if family == "other":
            continue
        dt = _as_dt(row.get("date") or row.get("date_dt"))
        if dt is None:
            continue
        age_h = max(FAST_HOURS, (target - dt).total_seconds() / 3600.0)
        narrative_scores[family] += rel * (2.0 ** (-age_h / 12.0))
        press = str(row.get("press") or "")
        if press:
            narrative_sources[family].add(press)

    narrative = None
    if narrative_scores:
        ordered = sorted(narrative_scores.items(), key=lambda kv: (-kv[1], kv[0]))
        total = sum(v for _, v in ordered)
        fam, score = ordered[0]
        dominance = score / total if total > 0 else 0.0
        if dominance >= 0.20 and len(narrative_sources.get(fam, set())) >= 2:
            narrative = fam

    orientations = {str(r.get("orientation") or "NEUTRAL") for r in top_rows}
    nonneutral = {x for x in orientations if x != "NEUTRAL"}

    return {
        "rows": top_rows,
        "has_news": bool(top_rows),
        "count4": len(ranked_articles),
        "narrative": narrative,
        "orientation_conflict": len(nonneutral) >= 2,
    }


def _codes_for_var(snapshot: dict[str, Any], var: int) -> dict[str, float]:
    price = snapshot.get("price")
    news = snapshot.get("news") or {}

    if price is None:
        return {}

    p_codes = _price_codes(price)

    if var == 0:
        return {c: 1.0 for c in p_codes}

    rows = list(news.get("rows") or [])
    if not rows:
        return {}

    pair_id = int(snapshot.get("pair_id") or 1)
    event_codes: list[str] = []
    for row in rows:
        event_codes.extend(_event_codes(row, pair_id))

    # Deduplicate while preserving deterministic order.
    event_codes = list(dict.fromkeys(event_codes))
    event_codes.append(f"N.CNT4.{_count_bucket(_safe_int(news.get('count4'),1))}")

    if news.get("orientation_conflict"):
        event_codes.append("N.ORI.CONFLICT")
    if news.get("narrative"):
        event_codes.append(f"N.NARR.{news['narrative']}")

    if var == 1:
        return {c: 1.0 for c in event_codes}

    if var == 2:
        selected = [
            r for r in rows
            if str(r.get("surprise_class") or "NONE") != "NONE"
            or str(r.get("expectedness_class") or "") in ("RARE","UNEXPECTED")
            or str(r.get("magnitude_class") or "U") in ("M","H")
        ]
        if not selected:
            return {}
        codes = list(event_codes)
        for row in selected:
            codes.extend(_surprise_codes(row))
        return {c: 1.0 for c in dict.fromkeys(codes)}

    if var == 3:
        codes = list(event_codes)
        for row in rows:
            codes.extend(_story_codes(row))
        return {c: 1.0 for c in dict.fromkeys(codes)}

    if var == 4:
        codes = list(event_codes) + p_codes
        top = rows[0]
        family = str(top.get("family") or "other")
        action = str(top.get("action") or "OTHER")
        ori = str(top.get("orientation") or "NEUTRAL")
        stage = str(top.get("story_stage") or "FIRST")
        codes += [
            f"X.F.{family}.D.{price['direction']}",
            f"X.ORI.{ori}.D.{price['direction']}",
            f"X.ST.{stage}.S.{price['shock']}",
        ]
        if action != "OTHER":
            codes.append(f"X.ACT.{action}.D.{price['direction']}")
        return {c: 1.0 for c in dict.fromkeys(codes)}

    if var == 5:
        codes = list(event_codes) + p_codes
        for row in rows:
            codes.extend(_surprise_codes(row))
            codes.extend(_story_codes(row))
        top = rows[0]
        codes += [
            f"X.F.{str(top.get('family') or 'other')}.D.{price['direction']}",
            f"X.ORI.{str(top.get('orientation') or 'NEUTRAL')}.D.{price['direction']}",
            f"X.ST.{str(top.get('story_stage') or 'FIRST')}.S.{price['shock']}",
        ]
        action = str(top.get("action") or "OTHER")
        if action != "OTHER":
            codes.append(f"X.ACT.{action}.D.{price['direction']}")
        return {c: 1.0 for c in dict.fromkeys(codes)}

    return {}


# -----------------------------------------------------------------------------
# Calculation-preserving runtime caches
# -----------------------------------------------------------------------------

_NEWS_LOCK = threading.RLock()
_NEWS_CACHE: "OrderedDict[tuple, dict[str, Any]]" = OrderedDict()
_NEWS_CACHE_MAX = 65536

_PRICE_LOCK = threading.RLock()
_PRICE_CACHE: "OrderedDict[tuple, dict[str, Any] | None]" = OrderedDict()
_PRICE_CACHE_MAX = 65536

_SNAPSHOT_LOCK = threading.RLock()
_SNAPSHOT_CACHE: "OrderedDict[tuple, dict[str, Any]]" = OrderedDict()
_SNAPSHOT_CACHE_MAX = 65536

_BATCH_LOCK = threading.RLock()
_BATCH_CACHE: "OrderedDict[tuple, dict[datetime, dict[str, Any]]]" = OrderedDict()
_BATCH_CACHE_MAX = 32


def _news_key(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any], pair_id: int) -> tuple:
    source = di.get("full_dataset") or dataset
    return (
        id(source),
        id(di.get("dataset_timestamps")),
        id(di.get("dates")),
        len(source),
        pair_id,
        int(target.timestamp()),
    )


def _price_key(target: datetime, di: dict[str, Any]) -> tuple:
    npr = di.get("np_rates") or {}
    dns = npr.get("dates_ns")
    return (
        id(dns),
        id(npr.get("open")),
        id(npr.get("close")),
        id(npr.get("max")),
        id(npr.get("min")),
        id(npr.get("ranges")),
        len(dns) if dns is not None else 0,
        int(target.timestamp()),
    )


def _news_cached(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any], pair_id: int) -> dict[str, Any]:
    key = _news_key(dataset, target, di, pair_id)
    with _NEWS_LOCK:
        hit = _NEWS_CACHE.get(key)
        if hit is not None:
            _NEWS_CACHE.move_to_end(key)
            return hit

    source = di.get("full_dataset") or dataset
    built = _build_news_state(source, target, di, pair_id)

    with _NEWS_LOCK:
        old = _NEWS_CACHE.get(key)
        if old is not None:
            _NEWS_CACHE.move_to_end(key)
            return old
        _NEWS_CACHE[key] = built
        while len(_NEWS_CACHE) > _NEWS_CACHE_MAX:
            _NEWS_CACHE.popitem(last=False)
    return built


def _price_cached(target: datetime, di: dict[str, Any]) -> dict[str, Any] | None:
    key = _price_key(target, di)
    sentinel = object()
    with _PRICE_LOCK:
        hit = _PRICE_CACHE.get(key, sentinel)
        if hit is not sentinel:
            _PRICE_CACHE.move_to_end(key)
            return hit

    built = _price_state(target, di)

    with _PRICE_LOCK:
        if key in _PRICE_CACHE:
            _PRICE_CACHE.move_to_end(key)
            return _PRICE_CACHE[key]
        _PRICE_CACHE[key] = built
        while len(_PRICE_CACHE) > _PRICE_CACHE_MAX:
            _PRICE_CACHE.popitem(last=False)
    return built


def _snapshot_key(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any], pair_id: int) -> tuple:
    return (
        _news_key(dataset, target, di, pair_id),
        _price_key(target, di),
    )


def _snapshot_cached(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any]) -> dict[str, Any]:
    pair_id = _pair_id(di)
    key = _snapshot_key(dataset, target, di, pair_id)
    with _SNAPSHOT_LOCK:
        hit = _SNAPSHOT_CACHE.get(key)
        if hit is not None:
            _SNAPSHOT_CACHE.move_to_end(key)
            return hit

    built = {
        "pair_id": pair_id,
        "price": _price_cached(target, di),
        "news": _news_cached(dataset, target, di, pair_id),
    }

    with _SNAPSHOT_LOCK:
        old = _SNAPSHOT_CACHE.get(key)
        if old is not None:
            _SNAPSHOT_CACHE.move_to_end(key)
            return old
        _SNAPSHOT_CACHE[key] = built
        while len(_SNAPSHOT_CACHE) > _SNAPSHOT_CACHE_MAX:
            _SNAPSHOT_CACHE.popitem(last=False)
    return built


def _batch_key(dataset: list[dict[str, Any]], dates: list[datetime], di: dict[str, Any]) -> tuple | None:
    if not dates:
        return None
    source = di.get("full_dataset") or dataset
    npr = di.get("np_rates") or {}
    dates_ns = npr.get("dates_ns")
    return (
        id(source),
        id(di.get("dataset_timestamps")),
        id(di.get("dates")),
        len(source),
        id(dates_ns),
        id(npr.get("open")),
        id(npr.get("close")),
        id(npr.get("max")),
        id(npr.get("min")),
        id(npr.get("ranges")),
        len(dates_ns) if dates_ns is not None else 0,
        _pair_id(di),
        tuple(dates),
    )


def _batch_snapshots(dataset: list[dict[str, Any]], dates: list[datetime], di: dict[str, Any]) -> dict[datetime, dict[str, Any]]:
    key = _batch_key(dataset, dates, di)
    if key is None:
        return {}

    with _BATCH_LOCK:
        hit = _BATCH_CACHE.get(key)
        if hit is not None:
            _BATCH_CACHE.move_to_end(key)
            return hit

    built = {d: _snapshot_cached(dataset, d, di) for d in dates}

    with _BATCH_LOCK:
        old = _BATCH_CACHE.get(key)
        if old is not None:
            _BATCH_CACHE.move_to_end(key)
            return old
        _BATCH_CACHE[key] = built
        while len(_BATCH_CACHE) > _BATCH_CACHE_MAX:
            _BATCH_CACHE.popitem(last=False)
    return built


def clear_runtime_caches() -> None:
    with _BATCH_LOCK:
        _BATCH_CACHE.clear()
    with _SNAPSHOT_LOCK:
        _SNAPSHOT_CACHE.clear()
    with _NEWS_LOCK:
        _NEWS_CACHE.clear()
    with _PRICE_LOCK:
        _PRICE_CACHE.clear()


# -----------------------------------------------------------------------------
# Public model API
# -----------------------------------------------------------------------------

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
    del rates, param

    if not dataset or date is None or dataset_index is None:
        return {}
    if date < MODEL_START:
        return {}

    t = int(type)
    v = int(var)
    if t not in TYPES_RANGE or v not in VAR_RANGE:
        return {}

    di = dict(dataset_index)
    if bool(di.get("is_daily")):
        return {}
    di.setdefault("full_dataset", dataset)

    return _codes_for_var(_snapshot_cached(dataset, date, di), v)


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
    del rates, param

    if not dates:
        return {}
    if not dataset or dataset_index is None:
        return {d: {} for d in dates}

    t = int(type)
    v = int(var)
    if t not in TYPES_RANGE or v not in VAR_RANGE:
        return {d: {} for d in dates}

    di = dict(dataset_index)
    if bool(di.get("is_daily")):
        return {d: {} for d in dates}
    di.setdefault("full_dataset", dataset)

    valid = [d for d in dates if d >= MODEL_START]
    snaps = _batch_snapshots(dataset, valid, di) if valid else {}
    return {d: (_codes_for_var(snaps[d], v) if d in snaps else {}) for d in dates}
