"""
Service 81 — H1 News Digestion + Reverse Learning
=================================================

Purpose
-------
A new hourly model built on the already prepared ``vlad_news_algo_events``
dataset used by service 80.  Unlike service 80, this model does NOT ask only
"what happened after the same semantic event in the past?".  It describes the
point-in-time market/news state with a small hierarchy of reusable feature
codes and lets Brain reverse-learning decide which states correspond to future
buy/sell extrema.

Design lessons intentionally carried over from services 58/80 and the research
review:
  * H1 only.  Do not compete for the first seconds/minutes after a release.
    The model trades the market's *digestion* of information on later H1 bars.
  * strict point-in-time news: only rows with publication ``date_dt < target``;
  * no use of stored ``novelty``/``confirmation_count`` because those fields may
    have been calculated with information that arrived later.  Confirmation and
    novelty are rebuilt causally from rows that were already available;
  * frozen NLP cutoff: pre-cutoff semantic rows are never used as active reverse
    codes, preventing a frozen artifact trained through the cutoff from leaking
    backwards into earlier reverse-training extrema;
  * price-only control var so we can prove that news adds edge beyond H1
    momentum/reversal alone;
  * first closed H1 reaction, volatility regime and shock are explicit features;
  * continuation / underreaction / overreaction are separate hypotheses;
  * reverse types 2/3/4 use ATR/range-normalised extremum amplitude instead of
    raw BTC/EUR/ETH price units;
  * reverse extremum registration interval is fixed at 3 bars for every var so
    ``var`` changes only the economic hypothesis, not the label definition;
  * service-local bounded reverse policy prevents the 1e30..1e60 weight runaway
    seen in older reverse models.  Uniform rescaling preserves reverse precision
    and all within-universe weight ratios.

VAR semantics
-------------
0 PRICE_CONTROL       : closed-price state only, no news (placebo/control)
1 NEWS_ONLY           : causal news state only
2 NEWS_REACTION       : news + first closed H1 reaction (main general model)
3 HIGH_IMPACT         : only high-impact news states
4 UNDERREACTION       : strong news + weak/normal first price response
5 CONTINUATION        : meaningful news + directional, non-extreme response
6 OVERREACTION        : high-impact news + extreme first response; reverse learns
                        continuation versus fade, we do not hard-code the side

TYPE semantics are intentionally left to shared reverse_learning.py:
0 backward/sign, 1 forward/sign, 2 backward/amplitude,
3 forward/amplitude, 4 forward-strict/amplitude.
The active feature set itself does not change with type.
"""
from __future__ import annotations

import bisect
import math
import threading
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from typing import Any

import numpy as np


# -----------------------------------------------------------------------------
# Brain framework contract
# -----------------------------------------------------------------------------

SERVICE_ID = 64
PORT = 8926
NODE_NAME = "brain-news-digestion-h1-ml-s64"
SERVICE_TEXT = "H1 causal news digestion + market reaction + bounded reverse learning"

RATES_TABLE = "brain_rates_eur_usd"

# Use only the columns that model 81 actually needs.  date_dt is the information
# availability timestamp for this model; reference/economic dates are never used
# as substitutes for publication time.
DATASET_QUERY = """
    SELECT
        date_dt AS date,
        date_dt,
        press,
        event_type,
        topic_id,
        cluster_id,
        topic_focus,
        cluster_similarity,
        quality_score
    FROM vlad_news_algo_events
    WHERE date_dt IS NOT NULL
    ORDER BY date_dt
"""
DATASET_ENGINE = "vlad"
DATASET_KEY = "event_type"
FILTER_DATASET_BY_DATE = True
MODEL_CAN_FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
PRETEST_ALLOW_EMPTY = True

CACHE_DATE_FROM = "2025-01-15"
MODEL_START = datetime(2025, 1, 15, 0, 0, 0)

# Semantic horizon of the H1 model.  The reverse layer has its own extremum
# history window; these four hours are the direct post-news digestion horizon.
SHIFT_WINDOW = 4
VAR_RANGE = list(range(7))
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = True

# Fallback ML settings when config.toml does not override them.  0.95 was far too
# aggressive for noisy market data and was one cause of excessive reweighting.
ML_TARGET_PRECISION = 0.62
ML_MAX_ITER = 8
ML_STEP = 0.04
ML_EXTREMUM_LIMIT = 48
ML_ACTIVE_TAIL = 0
ML_PRECISION_METRIC = "mean"


# -----------------------------------------------------------------------------
# Frozen thresholds derived before/at the service-80 NLP cutoff, not from future
# H1 performance.  Service-80 diagnostics had median topic_focus ~= .358 and
# median cluster_similarity ~= .723; 0.36/0.70 were its runtime thresholds.
# -----------------------------------------------------------------------------

TOPIC_FOCUS_MID = 0.36
TOPIC_FOCUS_HIGH = 0.58
CLUSTER_SIM_MID = 0.70
CLUSTER_SIM_HIGH = 0.80

FAST_NEWS_HOURS = 4
FAST_1H_HOURS = 1
SLOW_NEWS_HOURS = 48
NOVEL_GAP_HOURS = 24
FRESH_GAP_HOURS = 6
MIN_CAUSAL_CLUSTER_SUPPORT = 60

# The model emits only compact categorical codes.  Reverse weights are bounded
# separately below.
REVERSE_MAX_ABS_WEIGHT = 32.0
REVERSE_AMP_MIN = 0.25
REVERSE_AMP_MAX = 8.0
REVERSE_AMP_LOOKBACK = 24
REVERSE_FIXED_EXTREMUM_INTERVAL = 3


VAR_LABELS = {
    0: "PRICE_CONTROL",
    1: "NEWS_ONLY",
    2: "NEWS_REACTION",
    3: "HIGH_IMPACT",
    4: "UNDERREACTION",
    5: "CONTINUATION",
    6: "OVERREACTION",
}


# -----------------------------------------------------------------------------
# Service-local reverse policy
# -----------------------------------------------------------------------------
# server.py adds ../shared to sys.path before importing model.py.  Therefore the
# import below resolves the same reverse_learning module that brain_framework
# will use a moment later.  Monkey-patches affect only service 81's process; no
# files in shared/ are modified and services 58/78/80 keep their old behaviour.
# -----------------------------------------------------------------------------

def _install_service81_reverse_policy() -> None:
    try:
        import reverse_learning as rl  # type: ignore
    except Exception:
        # Allows model.py to be imported in isolated unit tests without shared/.
        return

    if getattr(rl, "_S81_POLICY_INSTALLED", False):
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
            if not math.isfinite(v):
                v = math.copysign(REVERSE_MAX_ABS_WEIGHT, v) if v else 0.0
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

    rl._S81_POLICY_INSTALLED = True


_install_service81_reverse_policy()


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------

def _as_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value[:19])
            return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
        except ValueError:
            return None
    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


def _safe_int(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bucket3(value: float, mid: float, high: float) -> str:
    if value >= high:
        return "H"
    if value >= mid:
        return "M"
    return "L"


def _count_bucket(n: int) -> str:
    if n >= 4:
        return "4P"
    if n >= 2:
        return "23"
    return "1"


def _age_bucket(hours: float) -> str:
    if hours <= 0.5:
        return "30M"
    if hours <= 1.0:
        return "1H"
    if hours <= 2.0:
        return "2H"
    return "4H"


def _event_key(row: dict[str, Any]) -> str:
    et = str(row.get("event_type") or "").strip()
    if et:
        return et
    t = _safe_int(row.get("topic_id"), -1)
    c = _safe_int(row.get("cluster_id"), -1)
    return f"t{t:02d}|e{c:03d}"


def _topic_id(row: dict[str, Any]) -> int:
    t = _safe_int(row.get("topic_id"), -1)
    if t >= 0:
        return t
    et = _event_key(row).lower()
    try:
        if et.startswith("t") and "|" in et:
            return int(et[1 : et.index("|")])
    except Exception:
        pass
    return -1


def _cluster_id(row: dict[str, Any]) -> int:
    c = _safe_int(row.get("cluster_id"), -1)
    if c >= 0:
        return c
    et = _event_key(row).lower()
    try:
        pos = et.find("|e")
        if pos >= 0:
            return int(et[pos + 2 :])
    except Exception:
        pass
    return -1


def _quality(row: dict[str, Any]) -> float:
    # Keep the same broad safe range used by service 80; quality is used only as
    # a ranking/impact feature, never as an externally signed trading direction.
    q = _safe_float(row.get("quality_score"), 1.0)
    return min(max(q, 0.25), 2.5)


# -----------------------------------------------------------------------------
# Price state — ONLY candles strictly before target are visible.
# -----------------------------------------------------------------------------

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
# Causal news state
# -----------------------------------------------------------------------------

def _rows_window(
    source: list[dict[str, Any]],
    target: datetime,
    dataset_index: dict[str, Any],
    hours_back: int,
    *,
    end_before_hours: int = 0,
) -> list[dict[str, Any]]:
    start = target - timedelta(hours=hours_back)
    end = target - timedelta(hours=end_before_hours)
    timestamps = dataset_index.get("dataset_timestamps")
    if timestamps is not None and len(timestamps) == len(source):
        l = int(np.searchsorted(timestamps, int(start.timestamp()), side="left"))
        # STRICTLY before end: publication exactly at target is not assumed
        # tradable at the target's opening instant.
        r = int(np.searchsorted(timestamps, int(end.timestamp()), side="left"))
        return source[l:r]

    dates = dataset_index.get("dates") or []
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


def _previous_event_gap_hours(
    event_key: str,
    row_dt: datetime,
    dataset_index: dict[str, Any],
) -> float:
    key_dates = dataset_index.get("key_dates") or {}
    dates = key_dates.get(event_key) or key_dates.get(event_key.lower()) or []
    if not dates:
        return float("inf")
    pos = bisect.bisect_left(dates, row_dt)
    if pos <= 0:
        return float("inf")
    prev = dates[pos - 1]
    return max(0.0, (row_dt - prev).total_seconds() / 3600.0)


def _causal_support(event_key: str, target: datetime, dataset_index: dict[str, Any]) -> int:
    key_dates = dataset_index.get("key_dates") or {}
    dates = key_dates.get(event_key) or key_dates.get(event_key.lower()) or []
    return bisect.bisect_left(dates, target) if dates else 0


def _news_state(
    source: list[dict[str, Any]],
    target: datetime,
    dataset_index: dict[str, Any],
) -> dict[str, Any]:
    fast4 = _rows_window(source, target, dataset_index, FAST_NEWS_HOURS)
    fast1 = _rows_window(source, target, dataset_index, FAST_1H_HOURS)
    slow = _rows_window(
        source,
        target,
        dataset_index,
        SLOW_NEWS_HOURS,
        end_before_hours=FAST_NEWS_HOURS,
    )

    result: dict[str, Any] = {
        "has_fast": bool(fast4),
        "fast_rows": len(fast4),
        "fast1_rows": len(fast1),
        "slow_rows": len(slow),
        "codes": [],
        "top_topics": [],
        "top_event": None,
        "impact": "L",
        "attention": "L",
        "confirmation": "1",
        "novelty": "R",
        "age": "4H",
        "focus": "L",
        "similarity": "L",
        "quality": "L",
        "slow_topic": None,
    }
    if not fast4:
        return result

    # Group same semantic event context inside the 4h pulse.  Repeated articles
    # from one source count as attention, but confirmation is distinct sources.
    stories: dict[str, dict[str, Any]] = {}
    all_sources: set[str] = set()
    newest_dt: datetime | None = None

    for row in fast4:
        dt = _as_dt(row.get("date") or row.get("date_dt"))
        if dt is None or dt >= target:
            continue
        key = _event_key(row)
        if not key:
            continue
        topic = _topic_id(row)
        cluster = _cluster_id(row)
        press = str(row.get("press") or "").strip().lower()
        if press:
            all_sources.add(press)

        st = stories.get(key)
        if st is None:
            st = {
                "key": key,
                "topic": topic,
                "cluster": cluster,
                "rows": 0,
                "sources": set(),
                "first": dt,
                "last": dt,
                "focus": 0.0,
                "sim": 0.0,
                "quality": 0.25,
                "prev_gap": _previous_event_gap_hours(key, dt, dataset_index),
            }
            stories[key] = st
        st["rows"] += 1
        if press:
            st["sources"].add(press)
        st["first"] = min(st["first"], dt)
        st["last"] = max(st["last"], dt)
        st["focus"] = max(st["focus"], _safe_float(row.get("topic_focus"), 0.0))
        st["sim"] = max(st["sim"], _safe_float(row.get("cluster_similarity"), 0.0))
        st["quality"] = max(st["quality"], _quality(row))
        if newest_dt is None or dt > newest_dt:
            newest_dt = dt

    if not stories or newest_dt is None:
        return result

    # Rank each semantic story using only information already known by target.
    # The score is used only to choose top hierarchical feature codes; it is not
    # a trading direction or target.
    topic_scores: dict[int, float] = defaultdict(float)
    ranked_stories = []
    max_focus = 0.0
    max_sim = 0.0
    max_quality = 0.25
    max_confirm = 1
    any_novel = False
    any_fresh = False

    for st in stories.values():
        age_h = max(0.0, (target - st["last"]).total_seconds() / 3600.0)
        recency = 2.0 ** (-age_h / 2.0)  # half-life 2h inside the fast pulse
        confirms = max(1, len(st["sources"]))
        novelty_bonus = 1.20 if st["prev_gap"] >= NOVEL_GAP_HOURS else (
            1.08 if st["prev_gap"] >= FRESH_GAP_HOURS else 1.0
        )
        score = (
            recency
            * (0.55 + min(st["focus"], 1.0))
            * (0.55 + min(st["sim"], 1.0))
            * min(max(st["quality"], 0.5), 2.0)
            * (1.0 + 0.12 * min(confirms - 1, 3))
            * novelty_bonus
        )
        st["score"] = score
        st["support"] = _causal_support(st["key"], target, dataset_index)
        ranked_stories.append(st)
        if st["topic"] >= 0:
            topic_scores[st["topic"]] += score
        max_focus = max(max_focus, st["focus"])
        max_sim = max(max_sim, st["sim"])
        max_quality = max(max_quality, st["quality"])
        max_confirm = max(max_confirm, confirms)
        any_novel = any_novel or st["prev_gap"] >= NOVEL_GAP_HOURS
        any_fresh = any_fresh or st["prev_gap"] >= FRESH_GAP_HOURS

    ranked_stories.sort(key=lambda x: (-x["score"], x["key"]))
    top_event = ranked_stories[0]
    top_topics = [t for t, _ in sorted(topic_scores.items(), key=lambda kv: (-kv[1], kv[0]))[:2]]

    # Attention is a point-in-time burst relative to the immediately preceding
    # 4..48h baseline.  This adapts to different eras/source densities without
    # using future averages.
    baseline_per_hour = max(len(slow) / max(SLOW_NEWS_HOURS - FAST_NEWS_HOURS, 1), 0.25)
    burst_ratio = len(fast1) / baseline_per_hour if baseline_per_hour > 0 else 0.0
    if (burst_ratio >= 1.75 and len(fast1) >= 2) or (
        len(top_event["sources"]) >= 3 and len(stories) >= 2
    ):
        attention = "H"
    elif burst_ratio >= 1.20 or len(top_event["sources"]) >= 2:
        attention = "M"
    else:
        attention = "L"

    focus_bucket = _bucket3(max_focus, TOPIC_FOCUS_MID, TOPIC_FOCUS_HIGH)
    sim_bucket = _bucket3(max_sim, CLUSTER_SIM_MID, CLUSTER_SIM_HIGH)
    quality_bucket = _bucket3(max_quality, 0.85, 1.50)
    confirm_bucket = "3P" if max_confirm >= 3 else ("2" if max_confirm >= 2 else "1")
    novelty_bucket = "N" if any_novel else ("F" if any_fresh else "R")

    points = 0
    points += 2 if attention == "H" else (1 if attention == "M" else 0)
    points += 2 if confirm_bucket == "3P" else (1 if confirm_bucket == "2" else 0)
    points += 2 if focus_bucket == "H" else (1 if focus_bucket == "M" else 0)
    points += 2 if sim_bucket == "H" else (1 if sim_bucket == "M" else 0)
    points += 1 if novelty_bucket in ("N", "F") else 0
    points += 1 if quality_bucket == "H" else 0
    impact = "H" if points >= 6 else ("M" if points >= 3 else "L")

    newest_age_h = max(0.0, (target - newest_dt).total_seconds() / 3600.0)

    # Slow 4..48h information regime.  Deduplicate by event context first, then
    # require topic concentration; otherwise broad daily news flow becomes noise.
    slow_seen: dict[str, dict[str, Any]] = {}
    slow_topic_scores: dict[int, float] = defaultdict(float)
    slow_topic_sources: dict[int, set[str]] = defaultdict(set)
    for row in slow:
        dt = _as_dt(row.get("date") or row.get("date_dt"))
        if dt is None or dt >= target:
            continue
        key = _event_key(row)
        topic = _topic_id(row)
        if not key or topic < 0:
            continue
        age_h = max(FAST_NEWS_HOURS, (target - dt).total_seconds() / 3600.0)
        score = (2.0 ** (-age_h / 18.0)) * (0.35 + _safe_float(row.get("topic_focus"), 0.0))
        prev = slow_seen.get(key)
        # One semantic story contributes once; keep its strongest/most recent row.
        if prev is None or score > prev["score"]:
            slow_seen[key] = {"topic": topic, "score": score}
        press = str(row.get("press") or "").strip().lower()
        if press:
            slow_topic_sources[topic].add(press)

    for item in slow_seen.values():
        slow_topic_scores[item["topic"]] += item["score"]

    slow_topic = None
    if slow_topic_scores:
        ordered = sorted(slow_topic_scores.items(), key=lambda kv: (-kv[1], kv[0]))
        total_score = sum(v for _, v in ordered)
        t0, s0 = ordered[0]
        dominance = s0 / total_score if total_score > 0 else 0.0
        # Need both meaningful concentration and cross-source presence.
        if dominance >= 0.18 and len(slow_topic_sources.get(t0, set())) >= 2:
            slow_topic = t0

    codes: list[str] = [
        f"N.ATT.{attention}",
        f"N.IMP.{impact}",
        f"N.AGE.{_age_bucket(newest_age_h)}",
        f"N.CFM.{confirm_bucket}",
        f"N.NOV.{novelty_bucket}",
        f"N.FOC.{focus_bucket}",
        f"N.SIM.{sim_bucket}",
        f"N.QLT.{quality_bucket}",
        f"N.STY.{_count_bucket(len(stories))}",
    ]
    for topic in top_topics:
        codes.append(f"N.T.{topic}")

    # Hierarchical cluster code only after enough PRIOR support.  This prevents a
    # rare cluster from fragmenting the reverse universe and is strictly causal.
    if (
        top_event["support"] >= MIN_CAUSAL_CLUSTER_SUPPORT
        and top_event["topic"] >= 0
        and top_event["cluster"] >= 0
        and top_event["focus"] >= TOPIC_FOCUS_MID
        and top_event["sim"] >= CLUSTER_SIM_MID
    ):
        codes.append(f"N.E.{top_event['topic']}.{top_event['cluster']}")

    if slow_topic is not None:
        codes.append(f"N.SLOW.T.{slow_topic}")

    result.update(
        {
            "codes": codes,
            "top_topics": top_topics,
            "top_event": top_event,
            "impact": impact,
            "attention": attention,
            "confirmation": confirm_bucket,
            "novelty": novelty_bucket,
            "age": _age_bucket(newest_age_h),
            "focus": focus_bucket,
            "similarity": sim_bucket,
            "quality": quality_bucket,
            "slow_topic": slow_topic,
            "story_count": len(stories),
            "source_count": len(all_sources),
            "burst_ratio": burst_ratio,
        }
    )
    return result


# -----------------------------------------------------------------------------
# Feature composition per var
# -----------------------------------------------------------------------------

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


def _interaction_codes(news: dict[str, Any], price: dict[str, Any]) -> list[str]:
    out = [
        f"X.I{news['impact']}.D{price['direction']}",
        f"X.A{news['attention']}.S{price['shock']}",
        f"X.C{news['confirmation']}.D{price['direction']}",
    ]
    for topic in news.get("top_topics", [])[:1]:
        out.append(f"X.T{topic}.D{price['direction']}")
    return out


def _codes_for_var(snapshot: dict[str, Any], var: int) -> dict[str, float]:
    price = snapshot.get("price")
    news = snapshot.get("news")
    if price is None:
        return {}

    p_codes = _price_codes(price)

    # PRICE_CONTROL: no news whatsoever.  This is a required benchmark, not a
    # production recommendation by itself.
    if var == 0:
        return {code: 1.0 for code in p_codes}

    if not news or not news.get("has_fast"):
        return {}

    n_codes = list(news.get("codes") or [])

    # NEWS_ONLY: exact same 4h availability window as the main model, but no
    # price-state feature.  If var2 cannot beat this + var0 independently, the
    # supposed interaction edge is questionable.
    if var == 1:
        return {code: 1.0 for code in n_codes}

    impact = str(news.get("impact") or "L")
    attention = str(news.get("attention") or "L")
    direction = str(price.get("direction") or "F")
    shock = str(price.get("shock") or "F")
    meaningful_news = impact in ("M", "H")
    strong_news = impact == "H"

    # Main general interaction model.
    if var == 2:
        if not meaningful_news:
            return {}
        codes = n_codes + p_codes + _interaction_codes(news, price)
        return {code: 1.0 for code in codes}

    # High-impact sparse model.  No direction is hard-coded; reverse decides.
    if var == 3:
        if not strong_news:
            return {}
        codes = n_codes + p_codes + _interaction_codes(news, price)
        codes += ["HIMP.ACTIVE", f"HIMP.SHK.{shock}"]
        return {code: 1.0 for code in codes}

    # Underreaction: strong information state but first closed reaction has not
    # escaped normal noise yet.
    if var == 4:
        if not strong_news or shock not in ("F", "N"):
            return {}
        codes = n_codes + p_codes
        codes += [
            "UR.ACTIVE",
            f"UR.DIR.{direction}",
            f"UR.ATT.{attention}",
        ]
        for topic in news.get("top_topics", [])[:1]:
            codes.append(f"UR.T.{topic}")
        return {code: 1.0 for code in codes}

    # Continuation candidate: a meaningful event and a directional but not yet
    # extreme first response.  Reverse can still assign an opposite sign if the
    # historical continuation hypothesis is wrong for a given state.
    if var == 5:
        if not meaningful_news or direction not in ("U", "D") or shock not in ("N", "S"):
            return {}
        codes = n_codes + p_codes
        codes += [
            "CONT.ACTIVE",
            f"CONT.DIR.{direction}",
            f"CONT.IMP.{impact}",
        ]
        for topic in news.get("top_topics", [])[:1]:
            codes.append(f"CONT.T{topic}.D{direction}")
        return {code: 1.0 for code in codes}

    # Overreaction/fade candidate: do NOT say "fade" in the label itself.  An
    # extreme move can continue; reverse must learn the side from history.
    if var == 6:
        if not strong_news or direction not in ("U", "D") or shock != "E":
            return {}
        codes = n_codes + p_codes
        codes += [
            "OVR.ACTIVE",
            f"OVR.DIR.{direction}",
            f"OVR.ATT.{attention}",
        ]
        for topic in news.get("top_topics", [])[:1]:
            codes.append(f"OVR.T{topic}.D{direction}")
        return {code: 1.0 for code in codes}

    return {}


# -----------------------------------------------------------------------------
# FAST CACHE v2
# Calculation-preserving execution cache.
#
# News state does not depend on pair/rates, therefore it is shared between
# EUR/USD, BTC/USD and ETH/USD.
#
# Price state depends on np_rates, therefore it has a separate per-market cache.
#
# IMPORTANT:
#   _news_state() and _price_state() are NOT changed.
#   Only their already-calculated return values are reused.
# -----------------------------------------------------------------------------

_NEWS_STATE_LOCK = threading.RLock()
_NEWS_STATE_CACHE: "OrderedDict[tuple, dict[str, Any]]" = OrderedDict()

# Enough for the complete H1 period from 2025-01-15 with margin.
_NEWS_STATE_CACHE_MAX = 32768


_PRICE_STATE_LOCK = threading.RLock()
_PRICE_STATE_CACHE: "OrderedDict[tuple, dict[str, Any] | None]" = OrderedDict()

# 3 markets × ~14-16k H1 dates + margin.
_PRICE_STATE_CACHE_MAX = 65536


def _news_cache_key(
    dataset: list[dict[str, Any]],
    target: datetime,
    di: dict[str, Any],
) -> tuple:
    source = di.get("full_dataset") or dataset

    return (
        id(source),
        id(di.get("dataset_timestamps")),
        id(di.get("key_dates")),
        len(source),
        int(target.timestamp()),
    )


def _price_cache_key(
    target: datetime,
    di: dict[str, Any],
) -> tuple:
    npr = di.get("np_rates") or {}
    dates_ns = npr.get("dates_ns")

    return (
        id(dates_ns),
        id(npr.get("open")),
        id(npr.get("close")),
        id(npr.get("max")),
        id(npr.get("min")),
        id(npr.get("ranges")),
        len(dates_ns) if dates_ns is not None else 0,
        int(target.timestamp()),
    )


def _news_state_cached(
    dataset: list[dict[str, Any]],
    target: datetime,
    di: dict[str, Any],
) -> dict[str, Any]:

    key = _news_cache_key(dataset, target, di)

    with _NEWS_STATE_LOCK:
        cached = _NEWS_STATE_CACHE.get(key)

        if cached is not None:
            _NEWS_STATE_CACHE.move_to_end(key)
            return cached

    source = di.get("full_dataset") or dataset

    if source:
        built = _news_state(source, target, di)
    else:
        built = {
            "has_fast": False,
            "codes": [],
        }

    with _NEWS_STATE_LOCK:
        existing = _NEWS_STATE_CACHE.get(key)

        if existing is not None:
            _NEWS_STATE_CACHE.move_to_end(key)
            return existing

        _NEWS_STATE_CACHE[key] = built

        while len(_NEWS_STATE_CACHE) > _NEWS_STATE_CACHE_MAX:
            _NEWS_STATE_CACHE.popitem(last=False)

    return built


def _price_state_cached(
    target: datetime,
    di: dict[str, Any],
) -> dict[str, Any] | None:

    key = _price_cache_key(target, di)

    # None является допустимым cached result, поэтому обычный .get()
    # здесь использовать нельзя.
    sentinel = object()

    with _PRICE_STATE_LOCK:
        cached = _PRICE_STATE_CACHE.get(key, sentinel)

        if cached is not sentinel:
            _PRICE_STATE_CACHE.move_to_end(key)
            return cached

    built = _price_state(target, di)

    with _PRICE_STATE_LOCK:
        if key in _PRICE_STATE_CACHE:
            _PRICE_STATE_CACHE.move_to_end(key)
            return _PRICE_STATE_CACHE[key]

        _PRICE_STATE_CACHE[key] = built

        while len(_PRICE_STATE_CACHE) > _PRICE_STATE_CACHE_MAX:
            _PRICE_STATE_CACHE.popitem(last=False)

    return built


# -----------------------------------------------------------------------------
# Snapshot caches — the expensive point-in-time state is independent of type/var
# and should not be rebuilt 35 times during fill_cache.
# -----------------------------------------------------------------------------

_SNAPSHOT_LOCK = threading.RLock()
_SNAPSHOT_CACHE: "OrderedDict[tuple, dict[str, Any]]" = OrderedDict()
_SNAPSHOT_CACHE_MAX = 65536

_BATCH_LOCK = threading.RLock()
_BATCH_CACHE: "OrderedDict[tuple, dict[datetime, dict[str, Any]]]" = OrderedDict()
_BATCH_CACHE_MAX = 32


def _snapshot_key(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any]) -> tuple:
    source = di.get("full_dataset") or dataset
    npr = di.get("np_rates") or {}
    return (
        id(source),
        id(di.get("dataset_timestamps")),
        id(npr.get("dates_ns")),
        int(target.timestamp()),
    )


def _build_snapshot(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any]) -> dict[str, Any]:
    price = _price_state_cached(
        target,
        di,
    )

    news = _news_state_cached(
        dataset,
        target,
        di,
    )

    return {"price": price, "news": news}


def _snapshot_cached(dataset: list[dict[str, Any]], target: datetime, di: dict[str, Any]) -> dict[str, Any]:
    key = _snapshot_key(dataset, target, di)
    with _SNAPSHOT_LOCK:
        cached = _SNAPSHOT_CACHE.get(key)
        if cached is not None:
            _SNAPSHOT_CACHE.move_to_end(key)
            return cached

    built = _build_snapshot(dataset, target, di)
    with _SNAPSHOT_LOCK:
        existing = _SNAPSHOT_CACHE.get(key)
        if existing is not None:
            _SNAPSHOT_CACHE.move_to_end(key)
            return existing
        _SNAPSHOT_CACHE[key] = built
        while len(_SNAPSHOT_CACHE) > _SNAPSHOT_CACHE_MAX:
            _SNAPSHOT_CACHE.popitem(last=False)
    return built


def _batch_key(
    dataset: list[dict[str, Any]],
    dates: list[datetime],
    di: dict[str, Any],
) -> tuple | None:

    if not dates:
        return None

    source = di.get("full_dataset") or dataset
    npr = di.get("np_rates") or {}

    return (
        id(source),
        id(di.get("dataset_timestamps")),
        id(npr.get("dates_ns")),
        tuple(dates),
    )


def _batch_snapshots(dataset: list[dict[str, Any]], dates: list[datetime], di: dict[str, Any]) -> dict[datetime, dict[str, Any]]:
    key = _batch_key(dataset, dates, di)
    if key is None:
        return {}
    with _BATCH_LOCK:
        cached = _BATCH_CACHE.get(key)
        if cached is not None:
            _BATCH_CACHE.move_to_end(key)
            return cached

    built = {d: _snapshot_cached(dataset, d, di) for d in dates}
    with _BATCH_LOCK:
        existing = _BATCH_CACHE.get(key)
        if existing is not None:
            _BATCH_CACHE.move_to_end(key)
            return existing
        _BATCH_CACHE[key] = built
        while len(_BATCH_CACHE) > _BATCH_CACHE_MAX:
            _BATCH_CACHE.popitem(last=False)
    return built


def clear_runtime_caches() -> None:
    """
    Clears only service-local execution caches.

    Does NOT touch:
      reverse universe
      DB
      dataset
      weights
      model logic
    """

    with _SNAPSHOT_LOCK:
        _SNAPSHOT_CACHE.clear()

    with _BATCH_LOCK:
        _BATCH_CACHE.clear()

    with _NEWS_STATE_LOCK:
        _NEWS_STATE_CACHE.clear()

    with _PRICE_STATE_LOCK:
        _PRICE_STATE_CACHE.clear()


# -----------------------------------------------------------------------------
# Public Brain model API
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
        # Critical: the NLP artifact was frozen at MODEL_START, therefore reverse
        # extrema before the cutoff must not receive semantic feature codes.
        return {}

    t = int(type)
    v = int(var)
    if t not in TYPES_RANGE or v not in VAR_RANGE:
        return {}

    di = dict(dataset_index)
    if bool(di.get("is_daily")):
        return {}  # model 81 is intentionally H1-only
    di.setdefault("full_dataset", dataset)

    snapshot = _snapshot_cached(dataset, date, di)
    return _codes_for_var(snapshot, v)


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

    valid_dates = [d for d in dates if d >= MODEL_START]
    snapshots = _batch_snapshots(dataset, valid_dates, di) if valid_dates else {}
    return {
        d: (_codes_for_var(snapshots[d], v) if d in snapshots else {})
        for d in dates
    }


async def enrich_dataset(engine_vlad, engine_brain):
    """Service 64 consumes the shared service-80 enriched dataset as-is.
    """
    del engine_vlad, engine_brain

    # Dataset/reload boundary:
    # old point-in-time snapshots must not survive a refreshed dataset.
    clear_runtime_caches()

    return {
        "mode": "noop",
        "source": "vlad_news_algo_events",
        "reason": "service64 reuses frozen service80 NLP dataset",
    }
