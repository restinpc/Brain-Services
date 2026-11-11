"""Service 100: hourly GDELT observations, causal news contexts and Brain ML.

Provider categories describe news, never the direction of a traded instrument.
Only available_at determines historical availability; missing timestamps are
excluded. Completed hours use the same end-minus-one-second contract as 99.
The database path streams raw rows and stages bounded batches before replacing
the enriched table in one transaction. Source tables are read-only.
"""
from __future__ import annotations

from collections import Counter, deque
from datetime import datetime, timedelta, timezone
import json
import math
import re

from brain_framework import get_service_config
from news_history import history_for, invalidate

HOUR = timedelta(hours=1)
DEFAULT_FAMILIES = {
    "monetary": ("ECON_INTEREST_RATES", "ECON_CENTRALBANK", "EPU_CATS_MONETARY_POLICY",
                 "EPU_POLICY_MONETARY_POLICY", "EPU_POLICY_CENTRAL_BANK", "WB_444", "WB_1235"),
    "inflation": ("ECON_INFLATION", "WB_442"),
    "labor": ("ECON_UNEMPLOYMENT", "ECON_UNIONS", "WB_697", "WB_855", "WB_2745"),
    "growth": ("EPU_ECONOMY", "WB_471", "WB_439"),
    "trade": ("ECON_TRADE", "WB_698", "WB_775", "EPU_CATS_TRADE_POLICY"),
    "energy": ("ECON_OILPRICE", "ECON_GASOLINEPRICE", "ECON_HEATINGOIL", "WB_507", "WB_525"),
    "fiscal": ("ECON_TAXATION", "ECON_DEBT", "WB_445", "WB_1104", "EPU_CATS_FISCAL_POLICY"),
    "finance": ("ECON_STOCKMARKET", "ECON_WORLDCURRENCIES", "ECON_CURRENCY_EXCHANGE_RATE",
                "WB_318", "WB_1234"),
}
DEFAULTS = {
    "availability_mode": "provider_time", "publication_lag_minutes": 0,
    "neutral_tone": 1.0, "strong_tone": 5.0, "min_mention_confidence": 40.0,
    "baseline_days": 30, "baseline_min_hours": 24,
    "volume_elevated": 2.0, "volume_extreme": 4.0,
    "min_analogs": 8, "history_days": 730, "extremum_prior": 4.0,
}
STREAMS = {"event": "event", "mention": "mention", "article_metadata": "gkg"}


def _number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _datetime(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _json(value, kind):
    if isinstance(value, (str, bytes, bytearray)):
        try:
            value = json.loads(value)
        except (ValueError, TypeError, UnicodeError):
            return kind()
    return value if isinstance(value, kind) else kind()


def _settings(cfg=None):
    settings = {**DEFAULTS, **(cfg or {})}
    if settings["availability_mode"] not in ("provider_time", "first_seen"):
        raise ValueError("availability_mode must be provider_time or first_seen")
    for key in ("neutral_tone", "strong_tone", "min_mention_confidence", "baseline_days",
                "baseline_min_hours", "volume_elevated", "volume_extreme", "min_analogs",
                "history_days", "extremum_prior", "publication_lag_minutes"):
        value = _number(settings[key])
        if value is None or value < 0:
            raise ValueError(f"{key} must be nonnegative and finite")
        settings[key] = value
    if not 0 <= settings["neutral_tone"] < settings["strong_tone"] <= 100:
        raise ValueError("tone thresholds must satisfy 0 <= neutral < strong <= 100")
    if settings["min_mention_confidence"] > 100:
        raise ValueError("min_mention_confidence must be <= 100")
    if not 1 < settings["volume_elevated"] < settings["volume_extreme"]:
        raise ValueError("volume thresholds must satisfy 1 < elevated < extreme")
    if (settings["baseline_days"] <= 0 or settings["history_days"] <= 0
            or settings["min_analogs"] < 2 or settings["baseline_min_hours"] < 1
            or settings["baseline_min_hours"] > settings["baseline_days"] * 24):
        raise ValueError("invalid history/baseline size")
    for key in ("min_analogs", "baseline_min_hours"):
        if not settings[key].is_integer():
            raise ValueError(f"{key} must be an integer")
        settings[key] = int(settings[key])
    families = {**DEFAULT_FAMILIES, **settings.get("theme_families", {})}
    for name, prefixes in families.items():
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,23}", name):
            raise ValueError("invalid theme family name")
        if (not isinstance(prefixes, (list, tuple))
                or any(not isinstance(p, str) or not re.fullmatch(r"[A-Z0-9_]+", p) for p in prefixes)):
            raise ValueError("theme prefixes must be lists of uppercase category codes")
    settings["theme_families"] = families
    return settings


def _effective_time(row, cfg):
    available = _datetime(row.get("available_at"))
    if available is None:
        return None
    available += timedelta(minutes=cfg["publication_lag_minutes"])
    if cfg["availability_mode"] == "first_seen":
        loaded = _datetime(row.get("loaded_at"))
        if loaded is None:
            return None
        available = max(available, loaded)
    return available


def _observation(row, cfg, stats):
    stats["source_rows"] += 1
    if row.get("provider") != "gdelt" or row.get("record_type") not in STREAMS:
        stats["other_rows"] += 1
        return None
    available = _effective_time(row, cfg)
    if available is None:
        stats["missing_availability"] += 1
        return None
    stream = STREAMS[row["record_type"]]
    identity = row.get("event_id") if stream == "event" else row.get("publication_id")
    if not identity:
        stats["missing_identity"] += 1
        return None
    raw = _json(row.get("raw_payload"), dict)
    if stream == "mention":
        confidence = _number(raw.get("Confidence"))
        if confidence is None or not cfg["min_mention_confidence"] <= confidence <= 100:
            stats["low_confidence_mentions"] += 1
            return None
    # Ties have a deterministic rule, independent of SQL/input iteration order.
    key = str(row.get("record_key") or json.dumps(raw, sort_keys=True, default=str))
    observation = {"available": available, "stream": stream, "identity": str(identity),
                   "key": key, "raw": raw, "row": row}
    observation["features"] = tuple(_features(observation, cfg))
    # GCAM, full raw GKG fields and character offsets can be very large. Keep
    # only the finite feature list while choosing hourly publication snapshots.
    del observation["raw"], observation["row"]
    return observation


def _tone_state(value, cfg):
    value = _number(value)
    if value is None or abs(value) > 100:
        return "unknown", 1.0
    strength = min(6.0, 1.0 + abs(value) / cfg["strong_tone"])
    if abs(value) <= cfg["neutral_tone"]:
        return "neutral", strength
    sign = "positive" if value > 0 else "negative"
    return ("strong_" + sign if abs(value) >= cfg["strong_tone"] else sign), strength


def _features(observation, cfg):
    stream, raw, row = observation["stream"], observation["raw"], observation["row"]
    if stream == "event":
        root = str(raw.get("EventRootCode") or "").strip()
        if root.isdigit() and 1 <= int(root) <= 20:
            severity = _number(raw.get("GoldsteinScale"))
            score = 1.0 + min(abs(severity), 10.0) / 5.0 if severity is not None else 1.0
            yield f"gdelt_event_root_{int(root):02d}", score
        quad = str(raw.get("QuadClass") or "").strip()
        if quad in ("1", "2", "3", "4"):
            yield f"gdelt_event_quad_{quad}", 1.0
        state, score = _tone_state(raw.get("AvgTone"), cfg)
        yield f"gdelt_event_tone_{state}", score
        return
    if stream == "mention":
        state, score = _tone_state(raw.get("MentionDocTone"), cfg)
        yield f"gdelt_mention_tone_{state}", score
        return
    typed = _json(row.get("v2_tone"), dict)
    tone = typed.get("tone")
    if tone is None:
        tone = str(raw.get("V2Tone") or "").split(",")[0]
    state, score = _tone_state(tone, cfg)
    yield f"gdelt_gkg_tone_{state}", score
    themes = {t.upper() for t in _json(row.get("themes"), list) if isinstance(t, str)}
    themes.update(t["theme"].upper() for t in _json(row.get("v2_themes"), list)
                  if isinstance(t, dict) and isinstance(t.get("theme"), str))
    themes.update(t.upper() for t in str(raw.get("Themes") or "").split(";") if t)
    themes.update(t.split(",")[0].upper() for t in str(raw.get("V2Themes") or "").split(";") if t)
    for family, prefixes in sorted(cfg["theme_families"].items()):
        if any(t == p or t.startswith(p + "_") for p in prefixes for t in themes):
            yield f"gdelt_gkg_theme_{family}_{state}", score


class NewsEnricher:
    """One hour of raw observations plus past volume counts; bounded memory."""

    def __init__(self, cfg=None):
        self.cfg = _settings(cfg)
        self.stats = Counter()
        self.bucket = None
        self.latest = {}
        self.last_time = None
        self.baselines = {s: deque() for s in STREAMS.values()}
        self.contexts = set()

    def add(self, row):
        observation = _observation(row, self.cfg, self.stats)
        return self.push(observation) if observation is not None else []

    def push(self, observation):
        available = observation["available"]
        if self.last_time is not None and available < self.last_time:
            raise ValueError("GDELT observations must be ordered by effective availability")
        self.last_time = available
        bucket = available.replace(minute=0, second=0, microsecond=0)
        output = self.finish() if self.bucket is not None and bucket != self.bucket else []
        self.bucket = bucket
        key = (observation["stream"], observation["identity"])
        previous = self.latest.get(key)
        if previous is not None:
            self.stats["duplicate_observations"] += 1
        if previous is None or (available, observation["key"]) > (previous["available"], previous["key"]):
            self.latest[key] = observation
        return output

    def finish(self):
        if not self.latest:
            return []
        features, counts, times = {}, Counter(), {}
        end = self.bucket + HOUR

        def emit(name, score, source_time, count=1):
            old = features.get(name)
            features[name] = {
                "date_dt": end - timedelta(seconds=1), "event_type": name,
                "value": max(score, old["value"]) if old else score,
                "pct_change": max(score, old["pct_change"]) if old else score,
                "source_time": max(source_time, old["source_time"]) if old else source_time,
                "observation_count": count + (old["observation_count"] if old else 0),
            }

        for observation in self.latest.values():
            stream, available = observation["stream"], observation["available"]
            counts[stream] += 1
            times[stream] = max(available, times.get(stream, available))
            for name, score in observation["features"]:
                emit(name, score, available)
        for stream, count in counts.items():
            baseline = self.baselines[stream]
            cutoff = self.bucket - timedelta(days=self.cfg["baseline_days"])
            while baseline and baseline[0][0] < cutoff:
                baseline.popleft()
            if len(baseline) >= self.cfg["baseline_min_hours"]:
                # Missing hours are unknown coverage, never assumed zero news.
                mean = sum(n for _, n in baseline) / len(baseline)
                ratio = count / mean
                state = ("extreme" if ratio >= self.cfg["volume_extreme"] else
                         "elevated" if ratio >= self.cfg["volume_elevated"] else
                         "quiet" if ratio < 0.5 else "normal")
                emit(f"gdelt_{stream}_volume_{state}", min(6.0, max(0.25, ratio)), times[stream], count)
            baseline.append((self.bucket, count))
        rows = [features[key] for key in sorted(features)]
        self.contexts.update(features)
        self.stats.update(enriched_rows=len(rows), observed_hours=1,
                          unique_observations=sum(counts.values()))
        self.latest.clear()
        return rows

    def summary(self):
        return {**self.stats, "contexts": len(self.contexts),
                "availability_mode": self.cfg["availability_mode"]}


def build_enriched_rows(source, *, cfg=None):
    """Pure offline path: sort an iterable, then use the production aggregator."""
    builder = NewsEnricher(cfg)
    observations = [o for row in source if (o := _observation(row, builder.cfg, builder.stats)) is not None]
    observations.sort(key=lambda o: (o["available"], o["key"]))
    output = []
    for observation in observations:
        output.extend(builder.push(observation))
    output.extend(builder.finish())
    return output, builder.summary()


def _identifier(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,51}", name):
        raise ValueError("SQL table names must be safe identifiers of at most 52 characters")
    return name


async def enrich_dataset(engine_vlad, engine_brain):
    from sqlalchemy import text

    cfg = get_service_config()
    ds = cfg["dataset"]
    source, enriched = _identifier(ds["parser_table"]), _identifier(ds["enriched_table"])
    if source == enriched:
        raise ValueError("Source and enriched tables must differ")
    source_engine = ds.get("source_engine", "vlad")
    if source_engine not in ("brain", "vlad"):
        raise ValueError("source_engine must be brain or vlad")
    engine = engine_brain if source_engine == "brain" else engine_vlad
    builder = NewsEnricher(cfg.get("model"))
    settings = builder.cfg
    order = "available_at"
    conditions = "provider='gdelt' AND available_at IS NOT NULL"
    if settings["availability_mode"] == "first_seen":
        conditions += " AND loaded_at IS NOT NULL"
        order = "GREATEST(TIMESTAMPADD(MICROSECOND, :lag_microseconds, available_at), loaded_at)"
    query = text(f"""
        SELECT record_key, provider, record_type, publication_id, event_id,
               available_at, loaded_at, themes, v2_themes, v2_tone, raw_payload
        FROM `{source}` WHERE {conditions} ORDER BY {order}
    """)
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched}` (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                date_dt DATETIME NOT NULL, event_type VARCHAR(96) NOT NULL,
                value DOUBLE NOT NULL, pct_change DOUBLE NOT NULL,
                source_time DATETIME(6) NOT NULL, observation_count INT NOT NULL,
                UNIQUE KEY uq_event (date_dt, event_type), INDEX idx_event (event_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
    stage = enriched + "_stage"
    async with engine_vlad.begin() as dest:
        await dest.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{stage}`"))
        await dest.execute(text(f"CREATE TEMPORARY TABLE `{stage}` LIKE `{enriched}`"))
        try:
            insert = text(f"""
                INSERT INTO `{stage}`
                    (date_dt, event_type, value, pct_change, source_time, observation_count)
                VALUES (:date_dt, :event_type, :value, :pct_change, :source_time, :observation_count)
            """)
            pending = []
            async with engine.connect() as conn:
                result = await conn.stream(query, {"lag_microseconds": round(settings["publication_lag_minutes"] * 60_000_000)})
                try:
                    async for row in result.mappings():
                        pending.extend(builder.add(dict(row)))
                        if len(pending) >= 1000:
                            await dest.execute(insert, pending)
                            pending.clear()
                finally:
                    await result.close()
            pending.extend(builder.finish())
            if pending:
                await dest.execute(insert, pending)
            if not builder.stats["enriched_rows"]:
                raise ValueError("No usable GDELT observations; existing enriched data was preserved")
            # Both DML statements roll back together. Context and weight tables
            # are owned by Brain and retain their IDs across rebuilds.
            await dest.execute(text(f"DELETE FROM `{enriched}`"))
            await dest.execute(text(f"""
                INSERT INTO `{enriched}`
                    (date_dt, event_type, value, pct_change, source_time, observation_count)
                SELECT date_dt, event_type, value, pct_change, source_time, observation_count
                FROM `{stage}` ORDER BY date_dt, event_type
            """))
        finally:
            await dest.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{stage}`"))
    invalidate()
    return builder.summary()


def _runtime(dataset, dataset_index):
    index = dataset_index or {}
    source = index.get("full_dataset")
    if source is None:
        source = dataset
    daily = bool(index.get("is_daily", str(index.get("rates_table", "")).endswith("_day")))
    return history_for(source, daily=daily), index


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    del rates, param
    cfg = get_service_config() or {}
    settings = _settings(cfg.get("model"))
    history, index = _runtime(dataset, dataset_index)
    ml = bool((cfg.get("ml") or {}).get("enabled", False))
    return history.values_at(date, index, settings, calc_type=0 if ml else type, var=0 if ml else var)


def batch_model(rates, dataset, dates, *, type=0, var=0, param="", dataset_index=None):
    del rates, param
    cfg = get_service_config() or {}
    settings = _settings(cfg.get("model"))
    history, index = _runtime(dataset, dataset_index)
    ml = bool((cfg.get("ml") or {}).get("enabled", False))
    return {date: history.values_at(date, index, settings,
                                  calc_type=0 if ml else type, var=0 if ml else var)
            for date in dates}
