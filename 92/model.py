"""
Service 92 - Bybit TradeGPT hourly market-context model.

The model intentionally separates two information layers:
  1) TradeGPT's own directional opinion (text classification);
  2) numeric Bybit context exposed in the answer (funding, long/short,
     capital flow, sentiment and Bollinger width).

All rolling regimes are causal: an observation is classified only against
STRICTLY PRIOR observations for the same asset and metric.
"""
from __future__ import annotations

import math
import re
from collections import defaultdict, deque
from datetime import datetime
from statistics import median
from typing import Any, Iterable

from brain_framework import get_service_config, run_standard_model


_FAILURE_MARKERS = (
    "you've hit today's limit",
    "you have hit today's limit",
    "still working on the last question",
    "please ask again in a bit",
)

_UNSOLICITED_SIGNAL_MARKERS = (
    "contract pair:",
    "entry price:",
    "контрактная пара:",
    "входная цена:",
    "tradegpt opening signal",
    "tradegpt открытие сигнала",
)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return v if math.isfinite(v) else None
    text = _clean(value).replace(",", "").replace("$", "")
    if not text:
        return None
    try:
        v = float(text)
    except ValueError:
        return None
    return v if math.isfinite(v) else None


def _first_number(text: str, patterns: Iterable[str], *, lo: float | None = None, hi: float | None = None) -> float | None:
    for pattern in patterns:
        for m in re.finditer(pattern, text, re.I | re.S):
            v = _to_float(m.group(1))
            if v is None:
                continue
            if lo is not None and v < lo:
                continue
            if hi is not None and v > hi:
                continue
            return v
    return None


def _direction(text: str, horizon: str | None = None) -> str | None:
    """Return BULLISH / BEARISH / NEUTRAL, preferring structured V2 output."""
    upper = text.upper()
    if horizon:
        m = re.search(rf"OUTLOOK[_\s-]*{re.escape(horizon.upper())}\s*[=:]\s*(BULLISH|BEARISH|LONG|SHORT|NEUTRAL)", upper)
        if m:
            token = m.group(1)
            return "BULLISH" if token in {"BULLISH", "LONG"} else "BEARISH" if token in {"BEARISH", "SHORT"} else "NEUTRAL"
        # Legacy `trend now` answers have no explicit horizon. Do not silently
        # clone that generic opinion into both 1H and 24H contexts.
        return None

    m = re.search(r"\bOUTLOOK\s*[=:]\s*(BULLISH|BEARISH|LONG|SHORT|NEUTRAL)\b", upper)
    if m:
        token = m.group(1)
        return "BULLISH" if token in {"BULLISH", "LONG"} else "BEARISH" if token in {"BEARISH", "SHORT"} else "NEUTRAL"

    lower = text.lower()
    # Restrict legacy inference to the conclusion/tail, where TradeGPT usually
    # states the final direction. This avoids treating a conflicting indicator
    # mentioned earlier as the bot's final opinion.
    if "### conclusion" in lower:
        tail = lower.split("### conclusion", 1)[1]
    elif "conclusion:" in lower:
        tail = lower.split("conclusion:", 1)[1]
    else:
        tail = lower[-1200:]

    bearish_patterns = (
        r"current trend(?:\s+for\s+\w+)?\s+(?:is|remains)\s+(?:a\s+)?(?:short|bearish)",
        r"overall (?:market )?(?:trend|analysis|outlook).*?\b(?:short|bearish)\b",
        r"trend classification as\s+short",
        r"points? (?:toward|towards) (?:a )?(?:short|bearish)",
        r"bearish (?:trend|outlook|sentiment)",
    )
    bullish_patterns = (
        r"current trend(?:\s+for\s+\w+)?\s+(?:is|remains)\s+(?:a\s+)?(?:long|bullish)",
        r"overall (?:market )?(?:trend|analysis|outlook).*?\b(?:long|bullish)\b",
        r"trend classification as\s+long",
        r"points? (?:toward|towards) (?:a )?(?:long|bullish)",
        r"bullish (?:trend|outlook|sentiment)",
    )
    has_bear = any(re.search(p, tail, re.I | re.S) for p in bearish_patterns)
    has_bull = any(re.search(p, tail, re.I | re.S) for p in bullish_patterns)
    if has_bear and not has_bull:
        return "BEARISH"
    if has_bull and not has_bear:
        return "BULLISH"
    if re.search(r"\b(neutral|mixed|sideways|no clear direction)\b", tail, re.I):
        return "NEUTRAL"
    return None


def _extract_features(text: str) -> dict[str, float]:
    # Numeric patterns deliberately have sanity ranges. The legacy prose can be
    # inconsistent, so a value parsed outside an economically plausible range
    # is rejected rather than silently poisoning the dataset.
    long_short = _first_number(
        text,
        (
            r"long/short ratio[^\n\r0-9]{0,120}([0-9]+(?:\.[0-9]+)?)\s*:\s*1",
            r"long[- ]to[- ]short ratio[^\n\r0-9]{0,120}([0-9]+(?:\.[0-9]+)?)\s*:\s*1",
        ),
        lo=0.05,
        hi=20.0,
    )

    funding = _first_number(
        text,
        (
            r"funding rate[^\n\r]{0,160}?([-+]?0\.\d+)",
            r"funding rate[^\n\r]{0,160}?([-+]?\d+\.\d+[eE][-+]?\d+)",
        ),
        lo=-0.02,
        hi=0.02,
    )

    inflow = _first_number(
        text,
        (
            r"(?:capital\s+)?inflow[^\n\r$0-9]{0,100}\$?([0-9][0-9,]*(?:\.\d+)?)",
            r"(?:24\s*h|24-hour)[^\n\r]{0,100}?inflow[^\n\r$0-9]{0,80}\$?([0-9][0-9,]*(?:\.\d+)?)",
        ),
        lo=0.0,
    )
    outflow = _first_number(
        text,
        (
            r"(?:capital\s+)?outflow[^\n\r$0-9]{0,100}\$?([0-9][0-9,]*(?:\.\d+)?)",
            r"(?:24\s*h|24-hour)[^\n\r]{0,100}?outflow[^\n\r$0-9]{0,80}\$?([0-9][0-9,]*(?:\.\d+)?)",
        ),
        lo=0.0,
    )

    sentiment = _first_number(
        text,
        (
            r"Market Sentiment Index[^\n\r0-9]{0,120}([0-9]+(?:\.\d+)?)",
            r"sentiment index[^\n\r0-9]{0,120}([0-9]+(?:\.\d+)?)",
        ),
        lo=0.0,
        hi=100.0,
    )

    support = _first_number(
        text,
        (
            r"BOLL(?:inger)?\s+support\s+price[^\n\r$0-9]{0,100}\$?([0-9][0-9,]*(?:\.\d+)?)",
            r"support\s+(?:level|price)[^\n\r$0-9]{0,160}\$?([0-9][0-9,]*(?:\.\d+)?)",
            r"support\s+level\s+at\s+\$?([0-9][0-9,]*(?:\.\d+)?)",
        ),
        lo=0.0,
    )
    resistance = _first_number(
        text,
        (
            r"BOLL(?:inger)?\s+resistance\s+price[^\n\r$0-9]{0,100}\$?([0-9][0-9,]*(?:\.\d+)?)",
            r"resistance\s+(?:level|price)[^\n\r$0-9]{0,160}\$?([0-9][0-9,]*(?:\.\d+)?)",
            r"resistance\s+level\s+at\s+\$?([0-9][0-9,]*(?:\.\d+)?)",
        ),
        lo=0.0,
    )

    result: dict[str, float] = {}
    if long_short is not None:
        result["LONG_SHORT"] = long_short
    if funding is not None:
        result["FUNDING"] = funding
    if inflow is not None and outflow is not None and inflow + outflow > 0.0:
        # Scale-free signed flow imbalance in [-1, +1].
        result["NET_FLOW"] = (inflow - outflow) / (inflow + outflow)
    if sentiment is not None:
        result["SENTIMENT"] = sentiment
    if support is not None and resistance is not None and resistance > support > 0.0:
        mid = (support + resistance) / 2.0
        if mid > 0.0:
            result["BOLL_WIDTH"] = (resistance - support) / mid
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


def _rolling_state(metric: str, current: float, prior: Iterable[float], q_low: float, q_high: float) -> tuple[str, float]:
    hist = sorted(float(v) for v in prior if v is not None and math.isfinite(float(v)))
    if not hist:
        return "NORMAL", 0.0

    low = _quantile(hist, q_low)
    high = _quantile(hist, q_high)
    if current <= low:
        state = "LOW"
    elif current >= high:
        state = "HIGH"
    else:
        state = "NORMAL"

    med = float(median(hist))
    deviations = sorted(abs(v - med) for v in hist)
    mad = float(median(deviations)) if deviations else 0.0
    scale = max(mad * 1.4826, abs(med) * 0.01, 1e-12)
    surprise = max(-10.0, min(10.0, (float(current) - med) / scale))
    return state, surprise


def _sentiment_state(value: float) -> str:
    if value <= 25.0:
        return "EXTREME_FEAR"
    if value < 45.0:
        return "FEAR"
    if value <= 55.0:
        return "NEUTRAL"
    if value < 75.0:
        return "GREED"
    return "EXTREME_GREED"


def _is_valid_response(text: str, cfg: dict, feature_count: int, opinion: str | None) -> bool:
    model_cfg = cfg.get("model") or {}
    min_chars = int(model_cfg.get("min_response_chars", 500))
    min_features = int(model_cfg.get("min_numeric_features", 3))
    lower = text.lower().strip()
    if not lower or any(marker in lower for marker in _FAILURE_MARKERS):
        return False
    # Telegram TradeGPT may push unrelated automatic trade signals while the
    # collector is waiting for a Q&A response. Those messages are not answers
    # to our scheduled prompt and must never become model contexts.
    if any(marker in lower for marker in _UNSOLICITED_SIGNAL_MARKERS):
        return False
    # Keep a strongly structured future prompt even when it is intentionally
    # concise; legacy prose still needs the normal minimum length.
    structured = "OUTLOOK_1H=" in text.upper() or "OUTLOOK_24H=" in text.upper()
    if len(text) < min_chars and not (structured and len(text) >= 120):
        return False
    if not structured and feature_count < min_features:
        return False
    return opinion is not None or feature_count >= min_features


def build_enriched_rows(source_rows: list[dict], *, cfg: dict | None = None) -> tuple[list[dict], dict]:
    """Pure causal transformation used by enrichment and unit tests."""
    cfg = cfg or get_service_config()
    model_cfg = cfg.get("model") or {}
    history_window = max(24, int(model_cfg.get("history_window", 168)))
    history_min = max(8, int(model_cfg.get("history_min", 24)))
    q_low = float(model_cfg.get("quantile_low", 0.25))
    q_high = float(model_cfg.get("quantile_high", 0.75))
    emit_flip = bool(model_cfg.get("emit_opinion_flip", True))

    prepared: list[dict] = []
    skipped_invalid = 0

    for raw in source_rows:
        asset = _clean(raw.get("asset")).upper()
        text = _clean(raw.get("raw_response"))
        dt = raw.get("created_at")
        if asset not in {"BTC", "ETH"} or not isinstance(dt, datetime):
            skipped_invalid += 1
            continue

        features = _extract_features(text)
        generic_opinion = _direction(text)
        opinion_1h = _direction(text, "1H")
        opinion_24h = _direction(text, "24H")
        opinion_for_quality = opinion_1h or generic_opinion or opinion_24h

        if not _is_valid_response(text, cfg, len(features), opinion_for_quality):
            skipped_invalid += 1
            continue

        prepared.append(
            {
                "source_id": int(raw.get("id") or 0),
                "asset": asset,
                "date_dt": dt.replace(microsecond=0),
                "features": features,
                "opinion": generic_opinion,
                "opinion_1h": opinion_1h,
                "opinion_24h": opinion_24h,
            }
        )

    prepared.sort(key=lambda r: (r["date_dt"], r["asset"], r["source_id"]))

    histories: dict[tuple[str, str], deque[float]] = defaultdict(lambda: deque(maxlen=history_window))
    previous_opinion: dict[str, str] = {}
    rows: list[dict] = []
    event_counts: dict[str, int] = defaultdict(int)

    i = 0
    while i < len(prepared):
        batch_time = prepared[i]["date_dt"]
        j = i + 1
        while j < len(prepared) and prepared[j]["date_dt"] == batch_time:
            j += 1
        batch = prepared[i:j]
        pending_updates: list[tuple[tuple[str, str], float]] = []

        for item in batch:
            asset = item["asset"]
            source_id = item["source_id"]

            # Legacy/current generic opinion.
            if item["opinion"]:
                state = item["opinion"]
                event_type = f"TG.{asset}.OPINION.{state}"
                rows.append({
                    "source_id": source_id,
                    "asset": asset,
                    "date_dt": item["date_dt"],
                    "value": 1.0 if state == "BULLISH" else -1.0 if state == "BEARISH" else 0.0,
                    "pct_change": 0.0,
                    "event_type": event_type,
                    "feature_name": "OPINION",
                    "feature_state": state,
                })
                event_counts[event_type] += 1

                prev = previous_opinion.get(asset)
                if emit_flip and prev and prev != state and state in {"BULLISH", "BEARISH"}:
                    flip_type = f"TG.{asset}.OPINION_FLIP.TO_{state}"
                    rows.append({
                        "source_id": source_id,
                        "asset": asset,
                        "date_dt": item["date_dt"],
                        "value": 1.0 if state == "BULLISH" else -1.0,
                        "pct_change": 0.0,
                        "event_type": flip_type,
                        "feature_name": "OPINION_FLIP",
                        "feature_state": f"TO_{state}",
                    })
                    event_counts[flip_type] += 1
                previous_opinion[asset] = state

            # Future V2 prompt can carry horizon-specific opinions. These are
            # separate codes so reverse-learning can decide whether they add alpha.
            for horizon, state in (("1H", item["opinion_1h"]), ("24H", item["opinion_24h"])):
                if not state:
                    continue
                event_type = f"TG.{asset}.OUTLOOK_{horizon}.{state}"
                rows.append({
                    "source_id": source_id,
                    "asset": asset,
                    "date_dt": item["date_dt"],
                    "value": 1.0 if state == "BULLISH" else -1.0 if state == "BEARISH" else 0.0,
                    "pct_change": 0.0,
                    "event_type": event_type,
                    "feature_name": f"OUTLOOK_{horizon}",
                    "feature_state": state,
                })
                event_counts[event_type] += 1

            # Sentiment has an intrinsic 0..100 scale, so use stable semantic
            # bands rather than future/global sample quantiles.
            if "SENTIMENT" in item["features"]:
                value = item["features"]["SENTIMENT"]
                state = _sentiment_state(value)
                event_type = f"TG.{asset}.SENTIMENT.{state}"
                rows.append({
                    "source_id": source_id,
                    "asset": asset,
                    "date_dt": item["date_dt"],
                    "value": value,
                    "pct_change": 0.0,
                    "event_type": event_type,
                    "feature_name": "SENTIMENT",
                    "feature_state": state,
                })
                event_counts[event_type] += 1

            # Remaining continuous metrics use prior-only rolling regimes.
            for metric in ("LONG_SHORT", "FUNDING", "NET_FLOW", "BOLL_WIDTH"):
                if metric not in item["features"]:
                    continue
                value = float(item["features"][metric])
                key = (asset, metric)
                hist = histories[key]
                if len(hist) >= history_min:
                    state, surprise = _rolling_state(metric, value, hist, q_low, q_high)
                    event_type = f"TG.{asset}.{metric}.{state}"
                    rows.append({
                        "source_id": source_id,
                        "asset": asset,
                        "date_dt": item["date_dt"],
                        "value": value,
                        "pct_change": surprise * 10.0,
                        "event_type": event_type,
                        "feature_name": metric,
                        "feature_state": state,
                    })
                    event_counts[event_type] += 1

                # Sign contexts have fixed causal semantics and can be emitted
                # even during warm-up.
                if metric in {"FUNDING", "NET_FLOW"}:
                    eps = 1e-12
                    sign_state = "POS" if value > eps else "NEG" if value < -eps else "ZERO"
                    sign_type = f"TG.{asset}.{metric}_SIGN.{sign_state}"
                    rows.append({
                        "source_id": source_id,
                        "asset": asset,
                        "date_dt": item["date_dt"],
                        "value": value,
                        "pct_change": 0.0,
                        "event_type": sign_type,
                        "feature_name": f"{metric}_SIGN",
                        "feature_state": sign_state,
                    })
                    event_counts[sign_type] += 1

                pending_updates.append((key, value))

        # Same-timestamp rows cannot influence each other's rolling thresholds.
        for key, value in pending_updates:
            histories[key].append(value)
        i = j

    stats = {
        "source_rows": len(source_rows),
        "valid_responses": len(prepared),
        "skipped_invalid": skipped_invalid,
        "enriched_rows": len(rows),
        "event_types": len(event_counts),
        "event_counts": dict(sorted(event_counts.items())),
    }
    return rows, stats


async def _load_table(engine, table_name: str, default_asset: str) -> list[dict]:
    from sqlalchemy import text

    q = text(f"""
        SELECT id, asset, raw_response, created_at
        FROM `{table_name}`
        WHERE created_at IS NOT NULL
          AND raw_response IS NOT NULL
        ORDER BY created_at, id
    """)
    async with engine.connect() as conn:
        res = await conn.execute(q)
        cols = list(res.keys())
        rows = []
        for values in res.fetchall():
            row = dict(zip(cols, values))
            if not _clean(row.get("asset")):
                row["asset"] = default_asset
            rows.append(row)
        return rows


async def _load_sources(engine_vlad, engine_brain, table1: str, table2: str) -> list[dict]:
    all_rows: list[dict] = []
    for table_name, default_asset in ((table1, "BTC"), (table2, "ETH")):
        loaded = None
        for engine in (engine_vlad, engine_brain):
            try:
                loaded = await _load_table(engine, table_name, default_asset)
                if loaded:
                    break
            except Exception:
                loaded = None
        if loaded:
            all_rows.extend(loaded)
    return all_rows


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    from sqlalchemy import text

    cfg = get_service_config()
    ds = cfg["dataset"]
    parser_table = ds["parser_table"]
    parser_table_2 = ds["parser_table_2"]
    enriched_table = ds["enriched_table"]

    source = await _load_sources(engine_vlad, engine_brain, parser_table, parser_table_2)
    rows, stats = build_enriched_rows(source, cfg=cfg)

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`            BIGINT       NOT NULL AUTO_INCREMENT,
                `source_id`     BIGINT       NOT NULL,
                `asset`         VARCHAR(8)   NOT NULL,
                `date_dt`       DATETIME     NOT NULL,
                `value`         DOUBLE       NOT NULL DEFAULT 0.0,
                `pct_change`    DOUBLE       NOT NULL DEFAULT 0.0,
                `event_type`    VARCHAR(96)  NOT NULL,
                `feature_name`  VARCHAR(32)  NOT NULL,
                `feature_state` VARCHAR(32)  NOT NULL,
                PRIMARY KEY (`id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_asset_date` (`asset`, `date_dt`),
                INDEX `idx_source` (`asset`, `source_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        for i in range(0, len(rows), 1000):
            await conn.execute(
                text(f"""
                    INSERT INTO `{enriched_table}`
                        (source_id, asset, date_dt, value, pct_change,
                         event_type, feature_name, feature_state)
                    VALUES
                        (:source_id, :asset, :date_dt, :value, :pct_change,
                         :event_type, :feature_name, :feature_state)
                """),
                rows[i:i + 1000],
            )

    return stats


def _apply_var(signed_t1: float, pct: float, var: int, ctx_info: dict) -> float:
    avg = float(ctx_info.get("avg_abs_pct_change") or 0.0)
    if var == 0:
        return signed_t1
    if var == 1:
        return signed_t1 if avg > 0 and abs(pct) >= avg else 0.0
    if var == 2:
        base = avg if avg > 0 else abs(pct)
        return signed_t1 * min(abs(pct) / base, 3.0) if base > 0 else 0.0
    if var == 3:
        return signed_t1 if pct > 0 else 0.0
    return 0.0


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    cfg = get_service_config()
    ml_enabled = bool((cfg.get("ml") or {}).get("enabled", False))
    source_type = 0 if ml_enabled else type
    source_var = 0 if ml_enabled else var

    return run_standard_model(
        rates,
        dataset,
        date,
        type=source_type,
        var=source_var,
        dataset_index=dataset_index,
        shift_window=cfg["cache"]["shift_window"],
        apply_var_fn=_apply_var,
    )
