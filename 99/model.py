"""USGS event regimes; storage and pure, reproducible feature construction.

No market direction is inferred from earthquake severity. PAGER, significance,
felt, tsunami and status-derived severity are not historical predictors here:
the source does not preserve their release vintages. Magnitude/coordinates can
also be revised: event_time mode is explicitly an estimated historical replay.

Rows represent a completed UTC hour and carry date_dt = hour end minus 1 second
to match Brain's [target - timeframe, target) current-event contract. An event
at 12:30 with a 60-minute delay first contributes at 14:00, never at 13:00.
"""
from __future__ import annotations

from bisect import bisect_left
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
import math
import re

from brain_framework import get_service_config
from event_history import history_for, invalidate

HOUR = timedelta(hours=1)
DEFAULTS = {
    "min_magnitude": 4.5, "availability_mode": "event_time",
    "publication_lag_minutes": 60, "cluster_hours": 72,
    "cluster_radius_km": 300.0, "escalation_magnitude": 0.3,
    "baseline_days": 30, "baseline_min_days": 7,
    "activity_elevated": 2.0, "activity_extreme": 4.0,
    "min_analogs": 8, "history_days": 730, "extremum_prior": 4.0,
}


def _settings(cfg=None):
    settings = {**DEFAULTS, **(cfg or {})}
    if settings["availability_mode"] not in ("event_time", "first_seen"):
        raise ValueError("availability_mode must be event_time or first_seen")
    for key in ("cluster_hours", "cluster_radius_km", "baseline_days",
                "baseline_min_days", "min_analogs", "history_days"):
        value = float(settings[key])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{key} must be positive and finite")
    for key in ("publication_lag_minutes", "escalation_magnitude", "extremum_prior"):
        value = float(settings[key])
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{key} must be nonnegative and finite")
    if not 4.5 <= float(settings["min_magnitude"]) <= 10.0:
        raise ValueError("min_magnitude must be between 4.5 and 10")
    if float(settings["baseline_min_days"]) > float(settings["baseline_days"]):
        raise ValueError("baseline_min_days cannot exceed baseline_days")
    if not 1 < float(settings["activity_elevated"]) < float(settings["activity_extreme"]):
        raise ValueError("activity thresholds must satisfy 1 < elevated < extreme")
    return settings


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


def _hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)


def _magnitude(value):
    return "m75" if value >= 7.5 else "m65" if value >= 6.5 else "m55" if value >= 5.5 else "m45"


def _depth(value):
    if value is None:
        return "unknown"
    return "shallow" if value < 70 else "intermediate" if value < 300 else "deep"


def _region(lat, lon):
    if lat is None or lon is None:
        return "unknown"
    # Coarse geographic cells, not country borders or an exposure map.
    belt = "n" if lat >= 30 else "s" if lat < -30 else "t"
    sector = int(((lon + 180) % 360) // 60)
    return f"{belt}{sector}"


def _distance_km(a, b):
    lat1, lat2 = math.radians(a["latitude"]), math.radians(b["latitude"])
    dlat = lat2 - lat1
    dlon = math.radians(b["longitude"] - a["longitude"])
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(min(1.0, max(0.0, h))))


def _prepare(source, cfg):
    stats = Counter(source_rows=len(source))
    candidates = []
    for row in source:
        dt = _datetime(row.get("event_time"))
        mag = _number(row.get("magnitude"))
        status = str(row.get("status") or "").lower()
        kind = str(row.get("event_type") or "earthquake").lower()
        if (dt is None or mag is None or not cfg["min_magnitude"] <= mag <= 10
                or status == "deleted" or kind != "earthquake"):
            stats["invalid_rows"] += 1
            continue
        loaded = _datetime(row.get("loaded_at"))
        if cfg["availability_mode"] == "first_seen" and loaded is None:
            stats["missing_loaded_at"] += 1
            continue
        available = dt + timedelta(minutes=float(cfg["publication_lag_minutes"]))
        if cfg["availability_mode"] == "first_seen":
            available = max(available, loaded)
        lat, lon, depth = (_number(row.get(key)) for key in ("latitude", "longitude", "depth_km"))
        lat = lat if lat is not None and -90 <= lat <= 90 else None
        lon = lon if lon is not None and -180 <= lon <= 180 else None
        depth = depth if depth is not None and -10 <= depth <= 800 else None
        if lat is None or lon is None:
            lat = lon = None
        event_id = str(row.get("usgs_id") or "").strip()
        # Stable fallback when a migrated source lacks a USGS id.
        key = event_id or (dt, lat, lon, mag, depth)
        candidates.append({
            "key": key, "event_time": dt, "available": available,
            "loaded_at": loaded, "magnitude": mag, "latitude": lat,
            "longitude": lon, "depth_km": depth,
        })
    # Keep the earliest observed copy, never prefer a later revision by magnitude.
    candidates.sort(key=lambda x: (x["loaded_at"] or x["available"], x["event_time"], str(x["key"])))
    seen, records = set(), []
    for record in candidates:
        if record["key"] in seen:
            stats["duplicate_rows"] += 1
            continue
        seen.add(record["key"])
        records.append(record)
    records.sort(key=lambda x: (x["available"], x["event_time"], str(x["key"])))
    stats["valid_events"] = len(records)
    return records, stats


def build_enriched_rows(source, *, cfg=None):
    """Build features from past observations only; no DB, rates or wall clock."""
    cfg = _settings(cfg)
    records, stats = _prepare(list(source), cfg)
    if not records:
        return [], dict(stats)
    times = [r["available"] for r in records]
    window = timedelta(hours=float(cfg["cluster_hours"]))
    baseline = timedelta(days=float(cfg["baseline_days"]))
    min_baseline = timedelta(days=float(cfg["baseline_min_days"]))
    recent = deque()
    output, pos = [], 0
    bucket, last_bucket = _hour(times[0]), _hour(times[-1])
    while bucket <= last_bucket:
        end = bucket + HOUR
        features = {}

        def emit(event_type, score, source_time, count=1):
            # A swarm cannot multiply the same context within one candle.
            old = features.get(event_type)
            features[event_type] = {
                "date_dt": end - timedelta(seconds=1), "event_type": event_type,
                "value": max(score, old["value"]) if old else score,
                "pct_change": max(score, old["pct_change"]) if old else score,
                "source_time": max(source_time, old["source_time"]) if old else source_time,
                "event_count": (old["event_count"] if old else 0) + count,
            }

        while pos < len(records) and records[pos]["available"] < end:
            eq = records[pos]
            pos += 1
            while recent and recent[0]["available"] < eq["available"] - window:
                recent.popleft()
            mag = _magnitude(eq["magnitude"])
            score = min(6.0, max(1.0, 1.0 + eq["magnitude"] - 4.5))
            emit(f"eq_mag_{mag}", score, eq["event_time"])
            emit(f"eq_depth_{mag}_{_depth(eq['depth_km'])}", score, eq["event_time"])
            region = _region(eq["latitude"], eq["longitude"])
            emit(f"eq_geo_{mag}_{region}", score, eq["event_time"])
            # Equal-time records cannot turn one another into a previous shock.
            if region != "unknown":
                neighbors = [p for p in recent if p["available"] < eq["available"]
                             and timedelta(0) < eq["event_time"] - p["event_time"] <= window
                             and p["latitude"] is not None
                             and _distance_km(p, eq) <= float(cfg["cluster_radius_km"])]
                state = "fresh"
                if neighbors:
                    prior_max = max(p["magnitude"] for p in neighbors)
                    state = "escalation" if eq["magnitude"] >= prior_max + float(cfg["escalation_magnitude"]) else "repeat"
                emit(f"eq_sequence_{mag}_{state}", score, eq["event_time"])
            recent.append(eq)

        # Activity relative to an earlier window, excluding the measured 6/24 h.
        # All bisect cutoffs are < end; future appends cannot change this hour.
        for hours in (6, 24):
            start = end - timedelta(hours=hours)
            base_start = max(times[0], start - baseline)
            span = start - base_start
            if span < min_baseline:
                continue
            base_count = bisect_left(times, start) - bisect_left(times, base_start)
            if base_count == 0:
                continue
            count = pos - bisect_left(times, start)
            expected = base_count * hours * 3600 / span.total_seconds()
            ratio = count / max(1.0, expected)
            state = ("extreme" if ratio >= cfg["activity_extreme"] else
                     "elevated" if ratio >= cfg["activity_elevated"] else
                     "quiet" if ratio < 0.5 else "normal")
            emit(f"eq_activity_{hours}h_{state}", min(6.0, max(0.25, ratio)),
                 records[pos - 1]["event_time"], count=count)
        output.extend(features[key] for key in sorted(features))
        bucket = end
    stats.update(enriched_rows=len(output), contexts=len({r["event_type"] for r in output}))
    return output, dict(stats)


def _identifier(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name):
        raise ValueError("Invalid SQL table identifier")
    return name



async def _model99_checked_execute(conn, statement, params=None):
    """Execute one MySQL statement and fail on any server warning."""
    result = await conn.execute(statement, params or {})
    warnings = (await conn.exec_driver_sql("SHOW WARNINGS")).mappings().all()
    if warnings:
        raise RuntimeError(f"Model99 SQL warnings: {warnings[:5]!r}")
    return result


async def _model99_create_target(conn, statement, table):
    """Accept only MySQL's known IF NOT EXISTS note; reject every other warning."""
    result = await conn.execute(statement)
    warnings = (await conn.exec_driver_sql("SHOW WARNINGS")).mappings().all()
    for warning in warnings:
        if int(warning["Code"]) == 1050 and str(warning["Message"]) == f"Table '{table}' already exists":
            continue
        raise RuntimeError(f"Unexpected Model99 target DDL warning: {warning!r}")
    return result


async def _model99_insert_stage_batch(conn, table, rows, offset):
    from sqlalchemy import text

    columns = ("date_dt", "event_type", "value", "pct_change", "source_time", "event_count")
    values = []
    params = {}
    for row_index, row in enumerate(rows):
        names = []
        for column in columns:
            key = f"r{offset + row_index}_{column}"
            names.append(":" + key)
            params[key] = row[column]
        values.append("(" + ",".join(names) + ")")
    cols = ",".join(f"`{column}`" for column in columns)
    statement = text(f"INSERT INTO `{table}` ({cols}) VALUES " + ",".join(values))
    return await _model99_checked_execute(conn, statement, params)


async def _model99_validate_target(conn, table):
    from sqlalchemy import text

    meta = (await conn.execute(text("""
        SELECT ENGINE FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table
    """), {"table": table})).mappings().one_or_none()
    if not meta or str(meta["ENGINE"]).upper() != "INNODB":
        raise RuntimeError("Model99 enriched target must exist and use InnoDB")
    columns = (await conn.exec_driver_sql(f"SHOW COLUMNS FROM `{table}`")).mappings().all()
    by_name = {r["Field"]: r for r in columns}
    expected = {"id": "bigint", "date_dt": "datetime", "event_type": "varchar(96)",
                "value": "double", "pct_change": "double", "source_time": "datetime", "event_count": "int"}
    for name, typ in expected.items():
        if name not in by_name or str(by_name[name]["Type"]).lower() != typ or by_name[name]["Null"] != "NO":
            raise RuntimeError(f"Model99 target schema mismatch for {name}: {by_name.get(name)!r}")
    if "auto_increment" not in str(by_name["id"]["Extra"]).lower():
        raise RuntimeError("Model99 target id must be AUTO_INCREMENT")
    indexes = (await conn.exec_driver_sql(f"SHOW INDEX FROM `{table}`")).mappings().all()
    unique_keys = {}
    for row in indexes:
        if int(row["Non_unique"]) == 0:
            unique_keys.setdefault(row["Key_name"], []).append((int(row["Seq_in_index"]), row["Column_name"]))
    if not any([column for _, column in sorted(parts)] == ["date_dt", "event_type"] for parts in unique_keys.values()):
        raise RuntimeError("Model99 target lacks the exact unique natural key (date_dt,event_type)")
    if [column for _, column in sorted(unique_keys.get("PRIMARY", []))] != ["id"]:
        raise RuntimeError("Model99 target primary key must be id")


async def _model99_publish_delta(conn, target_name, stage_name):
    from sqlalchemy import text

    target, stage = f"`{target_name}`", f"`{stage_name}`"
    columns = ("date_dt", "event_type", "value", "pct_change", "source_time", "event_count")
    payload = columns[2:]
    stage_count = int((await conn.execute(text(f"SELECT COUNT(*) FROM {stage}"))).scalar_one())
    if stage_count <= 0:
        raise ValueError("Refusing to publish an empty Model99 snapshot")
    invalid = int((await conn.execute(text(f"SELECT COUNT(*) FROM {stage} WHERE date_dt IS NULL OR event_type IS NULL"))).scalar_one())
    if invalid:
        raise RuntimeError(f"Model99 stage has {invalid} invalid natural keys")

    join = "t.`date_dt`=s.`date_dt` AND t.`event_type`=s.`event_type`"
    different = " OR ".join(f"NOT (t.`{column}` <=> s.`{column}`)" for column in payload)
    old_fields = ",".join(f"t.`{column}` AS old_{column}" for column in columns)
    new_fields = ",".join(f"s.`{column}` AS new_{column}" for column in payload)
    changed_rows = (await conn.execute(text(f"""
        SELECT t.`id`, {old_fields}, {new_fields}
        FROM {target} AS t JOIN {stage} AS s ON {join}
        WHERE {different}
    """))).mappings().all()
    deleted_rows = (await conn.execute(text(f"""
        SELECT t.`id`, {old_fields}
        FROM {target} AS t LEFT JOIN {stage} AS s ON {join}
        WHERE s.`date_dt` IS NULL
    """))).mappings().all()
    inserted_rows = (await conn.execute(text(f"""
        SELECT {','.join(f's.`{column}` AS `{column}`' for column in columns)}
        FROM {stage} AS s LEFT JOIN {target} AS t ON {join}
        WHERE t.`id` IS NULL
    """))).mappings().all()

    updated_count = 0
    for offset in range(0, len(changed_rows), 100):
        batch = changed_rows[offset:offset + 100]
        params, ids, predicates = {}, [], []
        for row_index, row in enumerate(batch):
            id_key = f"u{row_index}_id"
            params[id_key] = row["id"]
            ids.append(":" + id_key)
            old_terms = [f"`id`=:{id_key}"]
            for column in columns:
                key = f"u{row_index}_old_{column}"
                params[key] = row[f"old_{column}"]
                if column != "id":
                    old_terms.append(f"`{column}` <=> :{key}")
            predicates.append("(" + " AND ".join(old_terms) + ")")
        assignments = []
        for column in payload:
            branches = []
            for row_index, row in enumerate(batch):
                branches.append(f"WHEN :u{row_index}_id THEN :u{row_index}_new_{column}")
                params[f"u{row_index}_new_{column}"] = row[f"new_{column}"]
            assignments.append(f"`{column}`=CASE `id` " + " ".join(branches) + f" ELSE `{column}` END")
        statement = text(
            f"UPDATE {target} SET {','.join(assignments)} "
            f"WHERE `id` IN ({','.join(ids)}) AND ({' OR '.join(predicates)})"
        )
        result = await _model99_checked_execute(conn, statement, params)
        if int(result.rowcount) != len(batch):
            raise RuntimeError(f"Model99 update CAS mismatch expected={len(batch)} actual={result.rowcount}")
        updated_count += int(result.rowcount)

    inserted_count = 0
    for offset in range(0, len(inserted_rows), 500):
        batch = [dict(row) for row in inserted_rows[offset:offset + 500]]
        result = await _model99_insert_stage_batch(conn, target_name, batch, offset)
        inserted_count += int(result.rowcount)
    if inserted_count != len(inserted_rows):
        raise RuntimeError(f"Model99 insert count mismatch expected={len(inserted_rows)} actual={inserted_count}")

    deleted_count = 0
    for offset in range(0, len(deleted_rows), 100):
        batch = deleted_rows[offset:offset + 100]
        params, ids, predicates = {}, [], []
        for row_index, row in enumerate(batch):
            id_key = f"d{row_index}_id"
            params[id_key] = row["id"]
            ids.append(":" + id_key)
            old_terms = [f"`id`=:{id_key}"]
            for column in columns:
                key = f"d{row_index}_old_{column}"
                params[key] = row[f"old_{column}"]
                if column != "id":
                    old_terms.append(f"`{column}` <=> :{key}")
            predicates.append("(" + " AND ".join(old_terms) + ")")
        result = await _model99_checked_execute(conn, text(
            f"DELETE FROM {target} WHERE `id` IN ({','.join(ids)}) AND ({' OR '.join(predicates)})"
        ), params)
        if int(result.rowcount) != len(batch):
            raise RuntimeError(f"Model99 delete CAS mismatch expected={len(batch)} actual={result.rowcount}")
        deleted_count += int(result.rowcount)

    target_count = int((await conn.execute(text(f"SELECT COUNT(*) FROM {target}"))).scalar_one())
    mismatch = int((await conn.execute(text(f"""
        SELECT COUNT(*) FROM {target} AS t LEFT JOIN {stage} AS s ON {join}
        WHERE s.`date_dt` IS NULL OR {different}
    """))).scalar_one())
    if target_count != stage_count or mismatch:
        raise RuntimeError(f"Model99 publication mismatch target={target_count} stage={stage_count} rows={mismatch}")
    return {"updated": updated_count, "inserted": inserted_count,
            "deleted": deleted_count, "rows": target_count}
async def enrich_dataset(engine_vlad, engine_brain):
    from sqlalchemy import text

    cfg = get_service_config()
    ds = cfg["dataset"]
    source_table = _identifier(ds["parser_table"])
    enriched = _identifier(ds["enriched_table"])
    if source_table == enriched:
        raise ValueError("Source and enriched tables must differ")
    source_engine = ds.get("source_engine", "brain")
    if source_engine not in ("brain", "vlad"):
        raise ValueError("source_engine must be brain or vlad")
    engine = engine_brain if source_engine == "brain" else engine_vlad
    async with engine.connect() as conn:
        result = await conn.execute(text(f"""
            SELECT usgs_id, event_time, magnitude, latitude, longitude,
                   depth_km, event_type, status, loaded_at
            FROM `{source_table}` ORDER BY event_time, id
        """))
        source = [dict(row) for row in result.mappings().all()]
    rows, stats = build_enriched_rows(source, cfg=cfg.get("model"))
    if not rows:
        raise ValueError("No usable USGS events; existing enriched data was preserved")
    async with engine_vlad.begin() as conn:
        await _model99_create_target(conn, text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched}` (
                id BIGINT NOT NULL AUTO_INCREMENT,
                date_dt DATETIME NOT NULL,
                event_type VARCHAR(96) NOT NULL,
                value DOUBLE NOT NULL,
                pct_change DOUBLE NOT NULL,
                source_time DATETIME NOT NULL,
                event_count INT NOT NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uq_event (date_dt, event_type),
                INDEX idx_event (event_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """), enriched)
    # Stage a complete source-derived snapshot, then publish only changed natural keys
    # in one transaction. Context/weight IDs and their framework path stay untouched.
    import secrets
    stage_name = _identifier("tmp_m99_stage_" + secrets.token_hex(8))
    async with engine_vlad.connect() as conn:
        try:
            await _model99_validate_target(conn, enriched)
            await conn.commit()
            await _model99_checked_execute(conn, text(f"CREATE TEMPORARY TABLE `{stage_name}` LIKE `{enriched}`"))
            await conn.commit()
            for offset in range(0, len(rows), 500):
                async with conn.begin():
                    await _model99_insert_stage_batch(conn, stage_name, rows[offset:offset + 500], offset)
            async with conn.begin():
                delta = await _model99_publish_delta(conn, enriched, stage_name)
        finally:
            if conn.in_transaction():
                await conn.rollback()
            await conn.exec_driver_sql(f"DROP TEMPORARY TABLE IF EXISTS `{stage_name}`")
    invalidate()
    return {**stats, **delta, "availability_mode": _settings(cfg.get("model"))["availability_mode"]}


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
    # As in service 95, type/var select reverse-learning modes when ML is on.
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
