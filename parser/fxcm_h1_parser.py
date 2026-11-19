#!/usr/bin/env python3
"""FXCM weekly EUR/USD H1 Bid/Ask archive loader for Brain's registered parser.

The FXCM public archive contains broker-source Bid/Ask OHLC, not global FX
trades. This parser writes only those observations and never infers volume.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shlex
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from model100_ingest import (
    aggregate_bid_ask_h1,
    decode_dukascopy_bi5,
    parse_fxcm_h1_csv,
)

UTC = timezone.utc
PARSER_VERSION = "fxcm-h1-brain-v1"
SCHEMA_VERSION = "model100-fxcm-h1-v1"
TABLE = "vlad_model100_fxcm_h1"
STATE = "vlad_model100_fxcm_h1_state"
BATCHES = "vlad_model100_fxcm_h1_batches"
BASE = "https://candledata.fxcorporate.com/H1/EURUSD/{year}/{week}.csv.gz"
ARCHIVE_DIR = Path("/home/model100-data/fxcm_h1")
DUKASCOPY_BASE = "https://datafeed.dukascopy.com/datafeed/EURUSD/{year}/{month0:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
DUKASCOPY_PARSER_VERSION = "dukascopy-h1-brain-v1"
DUKASCOPY_SCHEMA_VERSION = "model100-fx-bidask-h1-v1"
DUKASCOPY_SOURCE_KEY = "DUKASCOPY_EURUSD_H1"
DUKASCOPY_LOCK_NAME = "brain:model100:dukascopy:eurusd:h1"


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def iter_weeks(start: date, end: date):
    cur = monday(start)
    while cur < end:
        iso = cur.isocalendar()
        yield iso.year, iso.week
        cur += timedelta(days=7)


def dukascopy_payload_to_rows(payload: bytes, hour_start: datetime,
                               archive_uri: str, archive_sha256: str) -> list[dict]:
    """Normalize one Dukascopy UTC-hour BI5 file to source-labelled H1 rows."""
    if len(payload) > 2_000_000:
        raise ValueError("Dukascopy hourly archive exceeds 2 MB bound")
    ticks = decode_dukascopy_bi5(payload, hour_start, period="hour")
    start = hour_start.astimezone(UTC)
    end = start + timedelta(hours=1)
    if any(tick.event_at < start or tick.event_at >= end for tick in ticks):
        raise ValueError("tick timestamp falls outside its UTC H1 window")
    rows = aggregate_bid_ask_h1(
        ticks, provider="DUKASCOPY", source_archive_uri=archive_uri,
        source_archive_sha256=archive_sha256,
    )
    for row in rows:
        row["parser_version"] = DUKASCOPY_PARSER_VERSION
        row["schema_version"] = DUKASCOPY_SCHEMA_VERSION
    return rows


def fetch_dukascopy_hour(hour_start: datetime):
    """Fetch one public BI5 hour with bounded bytes and ordinary transient retries."""
    hour_start = hour_start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    url = DUKASCOPY_BASE.format(year=hour_start.year, month0=hour_start.month - 1,
                                day=hour_start.day, hour=hour_start.hour)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Brain-Model100/1.0 historical quote ingestion",
        "Accept": "application/octet-stream",
    })
    last_error = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                payload = response.read(2_000_001)
                if len(payload) > 2_000_000:
                    raise ValueError("Dukascopy hourly archive exceeds 2 MB bound")
                return payload, response.geturl(), int(response.status)
        except urllib.error.HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                raise
            retry_after = exc.headers.get("Retry-After", "")
            try:
                pause = min(max(float(retry_after), 1.0), 30.0) if retry_after else (2 ** attempt)
            except ValueError:
                pause = 2 ** attempt
            time.sleep(pause)
            last_error = exc
        except urllib.error.URLError as exc:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
            last_error = exc
    raise last_error or RuntimeError("Dukascopy download failed without an error detail")


def _dukascopy_expected_closed(hour_start: datetime) -> bool:
    """Conservative weekend closure window in UTC; uncertain hours remain errors."""
    weekday, hour = hour_start.weekday(), hour_start.hour
    return weekday == 5 or (weekday == 6 and hour < 21) or (weekday == 4 and hour >= 22)


def _dukascopy_detail(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _read_dukascopy_cursor(db):
    cur = db.cursor()
    cur.execute(f"SELECT last_detail FROM `{STATE}` WHERE source_key=%s", (DUKASCOPY_SOURCE_KEY,))
    found = cur.fetchone()
    cur.close()
    detail = _dukascopy_detail(found[0]) if found else {}
    stamp = detail.get("last_completed_hour_utc")
    if not stamp:
        return None, detail
    value = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    return value.astimezone(UTC), detail


def _commit_dukascopy_hour(db, hour_start, row, detail, status="SUCCESS"):
    """Commit one candle and its restart cursor atomically; empty closed hours advance too."""
    now = datetime.now(UTC).replace(tzinfo=None)
    cur = db.cursor()
    try:
        if row:
            cur.execute(f"""INSERT INTO `{TABLE}`
              (provider,instrument,market_type,event_time_utc,window_end_utc,feature_ready_at_utc,
               bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,
               spread_open,spread_close,quote_tick_count,executed_volume,volume_type,
               source_archive_uri,source_archive_sha256,source_archive_available_at_utc,
               brain_first_received_at_utc,ingested_at_utc,parser_version,schema_version,quality_status,asof_status)
              VALUES (%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,'NONE',%s,%s,NULL,%s,%s,%s,%s,'VALIDATED','HISTORICAL_REPLAY')
              ON DUPLICATE KEY UPDATE
               bid_open=VALUES(bid_open),bid_high=VALUES(bid_high),bid_low=VALUES(bid_low),bid_close=VALUES(bid_close),
               ask_open=VALUES(ask_open),ask_high=VALUES(ask_high),ask_low=VALUES(ask_low),ask_close=VALUES(ask_close),
               spread_open=VALUES(spread_open),spread_close=VALUES(spread_close),quote_tick_count=VALUES(quote_tick_count),
               source_archive_uri=VALUES(source_archive_uri),source_archive_sha256=VALUES(source_archive_sha256),
               ingested_at_utc=VALUES(ingested_at_utc),parser_version=VALUES(parser_version),schema_version=VALUES(schema_version),
               quality_status='VALIDATED',asof_status='HISTORICAL_REPLAY'""",
              ("DUKASCOPY", "EURUSD", "SPOT_BID_ASK_H1", row["event_time_utc"], row["window_end_utc"],
               row["bid_open"], row["bid_high"], row["bid_low"], row["bid_close"],
               row["ask_open"], row["ask_high"], row["ask_low"], row["ask_close"],
               row["spread_open"], row["spread_close"], row["quote_tick_count"],
               row["source_archive_uri"], row["source_archive_sha256"], now, now,
               DUKASCOPY_PARSER_VERSION, DUKASCOPY_SCHEMA_VERSION))
        detail = dict(detail)
        detail["last_completed_hour_utc"] = hour_start.isoformat().replace("+00:00", "Z")
        detail["last_http_status"] = detail.get("http_status")
        cur.execute(f"""INSERT INTO `{STATE}`
          (source_key,last_successful_week_start,last_success_at_utc,last_status,last_detail,updated_at_utc)
          VALUES (%s,%s,%s,%s,%s,%s)
          ON DUPLICATE KEY UPDATE last_successful_week_start=VALUES(last_successful_week_start),
          last_success_at_utc=VALUES(last_success_at_utc),last_status=VALUES(last_status),
          last_detail=VALUES(last_detail),updated_at_utc=VALUES(updated_at_utc)""",
          (DUKASCOPY_SOURCE_KEY, hour_start.date(), now, status, json.dumps(detail), now))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        cur.close()


def _record_dukascopy_error(db, detail):
    cursor, previous = _read_dukascopy_cursor(db)
    merged = dict(previous)
    merged.update(detail)
    if cursor:
        merged["last_completed_hour_utc"] = cursor.isoformat().replace("+00:00", "Z")
    now = datetime.now(UTC).replace(tzinfo=None)
    cur = db.cursor()
    cur.execute(f"""INSERT INTO `{STATE}`
      (source_key,last_successful_week_start,last_success_at_utc,last_status,last_detail,updated_at_utc)
      VALUES (%s,%s,NULL,'PARTIAL_SOURCE_ACCESS',%s,%s)
      ON DUPLICATE KEY UPDATE last_status='PARTIAL_SOURCE_ACCESS',last_detail=VALUES(last_detail),updated_at_utc=VALUES(updated_at_utc)""",
      (DUKASCOPY_SOURCE_KEY, cursor.date() if cursor else None, json.dumps(merged), now))
    db.commit(); cur.close()


def run_dukascopy(db, table: str, args, *, dry_run=False, fetcher=None, now=None) -> dict:
    if dry_run:
        return _run_dukascopy_locked(db, table, args, dry_run=True, fetcher=fetcher, now=now)
    if db is None:
        raise ValueError("database connection required outside --dry-run")
    lock_cursor = db.cursor()
    acquired = False
    try:
        lock_cursor.execute("SELECT GET_LOCK(%s, 0)", (DUKASCOPY_LOCK_NAME,))
        result = lock_cursor.fetchone()
        acquired = bool(result and result[0] == 1)
    finally:
        lock_cursor.close()
    if not acquired:
        return {"status": "ALREADY_RUNNING", "source": "DUKASCOPY", "target": TABLE}
    try:
        return _run_dukascopy_locked(db, table, args, dry_run=False, fetcher=fetcher, now=now)
    finally:
        release = db.cursor()
        try:
            release.execute("SELECT RELEASE_LOCK(%s)", (DUKASCOPY_LOCK_NAME,))
            release.fetchone()
        finally:
            release.close()


def _run_dukascopy_locked(db, table: str, args, *, dry_run=False, fetcher=None, now=None) -> dict:
    if table != TABLE:
        raise ValueError(f"registered target must be {TABLE}")
    if args.end <= args.start:
        raise ValueError("--end must be later than --start")
    if not 1 <= args.max_hours <= 240:
        raise ValueError("--max-hours must be 1..240")
    fetcher = fetcher or fetch_dukascopy_hour
    current = (now or datetime.now(UTC)).astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    end = min(current, datetime.combine(args.end, datetime.min.time(), tzinfo=UTC))
    if dry_run:
        start = datetime.combine(args.start, datetime.min.time(), tzinfo=UTC)
    else:
        if db is None:
            raise ValueError("database connection required outside --dry-run")
        ensure_schema(db)
        cursor, _ = _read_dukascopy_cursor(db)
        if cursor:
            start = cursor + timedelta(hours=1)
        else:
            cur = db.cursor()
            cur.execute(f"SELECT 1 FROM `{TABLE}` WHERE provider='DUKASCOPY' AND instrument='EURUSD' LIMIT 1")
            existing_dukascopy_rows = cur.fetchone()
            if existing_dukascopy_rows:
                cur.close()
                return {"status": "CURSOR_STATE_MISSING", "source": "DUKASCOPY", "target": TABLE,
                        "error": "DUKASCOPY_ROWS_EXIST_WITHOUT_RESTART_CURSOR"}
            cur.execute(f"SELECT MAX(event_time_utc) FROM `{TABLE}` WHERE provider='FXCM' AND instrument='EURUSD'")
            last_fxcm = cur.fetchone()[0]
            cur.close()
            start = (last_fxcm.replace(tzinfo=UTC) + timedelta(hours=1)) if last_fxcm else datetime.combine(args.start, datetime.min.time(), tzinfo=UTC)
    start = start.replace(minute=0, second=0, microsecond=0)
    hours = []
    probe = start
    while probe < end and len(hours) < args.max_hours:
        hours.append(probe)
        probe += timedelta(hours=1)

    counts = {"rows_upserted": 0, "hours_processed": 0, "empty_closed_hours": 0,
              "http_403": 0, "http_404": 0, "http_429": 0, "http_5xx": 0}
    last_event = None
    for hour_start in hours:
        try:
            payload, uri, http_status = fetcher(hour_start)
            digest = hashlib.sha256(payload).hexdigest()
            rows = dukascopy_payload_to_rows(payload, hour_start, uri, digest) if payload else []
            if not rows and not _dukascopy_expected_closed(hour_start):
                raise ValueError("EMPTY_TICK_FILE_DURING_EXPECTED_FX_SESSION")
            row = rows[0] if rows else None
            if len(rows) > 1:
                raise ValueError("ONE_HOURLY_ARCHIVE_PRODUCED_MULTIPLE_H1_ROWS")
            detail = {"source": "DUKASCOPY", "http_status": http_status,
                      "archive_uri": uri, "archive_sha256": digest,
                      "tick_count": row["quote_tick_count"] if row else 0}
            if dry_run:
                counts["rows_upserted"] += len(rows)
            else:
                _commit_dukascopy_hour(db, hour_start, row, detail)
                counts["rows_upserted"] += len(rows)
            counts["hours_processed"] += 1
            counts["empty_closed_hours"] += int(row is None)
            last_event = row["event_time_utc"].isoformat(sep=" ") if row else last_event
        except urllib.error.HTTPError as exc:
            field = "http_403" if exc.code == 403 else "http_429" if exc.code == 429 else "http_5xx" if exc.code >= 500 else "http_404" if exc.code == 404 else None
            if exc.code == 404 and _dukascopy_expected_closed(hour_start):
                detail = {"source": "DUKASCOPY", "http_status": 404, "empty_reason": "EXPECTED_MARKET_CLOSED"}
                if not dry_run:
                    _commit_dukascopy_hour(db, hour_start, None, detail)
                counts["hours_processed"] += 1
                counts["empty_closed_hours"] += 1
                continue
            if field:
                counts[field] += 1
            detail = {"source": "DUKASCOPY", "error": f"HTTP_{exc.code}", "http_status": exc.code,
                      "failed_hour_utc": hour_start.isoformat()}
            if db is not None:
                _record_dukascopy_error(db, detail)
            return {"status": "PARTIAL_SOURCE_ACCESS", "target": TABLE, **counts,
                    "last_event_utc": last_event, "failed_hour_utc": hour_start.isoformat(),
                    "error": detail["error"]}
        except Exception as exc:
            detail = {"source": "DUKASCOPY", "error": type(exc).__name__,
                      "error_detail": str(exc)[:200], "failed_hour_utc": hour_start.isoformat()}
            if db is not None:
                _record_dukascopy_error(db, detail)
            return {"status": "PARTIAL_SOURCE_ACCESS", "target": TABLE, **counts,
                    "last_event_utc": last_event, "failed_hour_utc": hour_start.isoformat(),
                    "error": detail["error"], "error_detail": detail["error_detail"]}
        time.sleep(0.25)
    return {"status": "SUCCESS" if counts["hours_processed"] else "NO_NEW_CLOSED_HOURS",
            "source": "DUKASCOPY", "target": TABLE, "start": start.isoformat(),
            "end_exclusive": end.isoformat(), "last_event_utc": last_event, **counts}


def connect(host: str, port: int, user: str, password: str, database: str):
    import mysql.connector
    return mysql.connector.connect(host=host, port=port, user=user,
                                   password=password, database=database,
                                   connection_timeout=15, autocommit=False)


def ensure_schema(db):
    cur = db.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{TABLE}` (
      `provider` VARCHAR(24) NOT NULL,
      `instrument` VARCHAR(12) NOT NULL,
      `market_type` VARCHAR(32) NOT NULL,
      `event_time_utc` DATETIME NOT NULL,
      `window_end_utc` DATETIME NOT NULL,
      `feature_ready_at_utc` DATETIME NULL,
      `bid_open` DECIMAL(14,7) NOT NULL, `bid_high` DECIMAL(14,7) NOT NULL,
      `bid_low` DECIMAL(14,7) NOT NULL, `bid_close` DECIMAL(14,7) NOT NULL,
      `ask_open` DECIMAL(14,7) NOT NULL, `ask_high` DECIMAL(14,7) NOT NULL,
      `ask_low` DECIMAL(14,7) NOT NULL, `ask_close` DECIMAL(14,7) NOT NULL,
      `spread_open` DECIMAL(14,7) NOT NULL, `spread_close` DECIMAL(14,7) NOT NULL,
      `quote_tick_count` BIGINT UNSIGNED NULL,
      `executed_volume` DECIMAL(24,8) NULL,
      `volume_type` VARCHAR(24) NOT NULL,
      `source_archive_uri` VARCHAR(512) NOT NULL,
      `source_archive_sha256` CHAR(64) NOT NULL,
      `source_archive_available_at_utc` DATETIME NULL,
      `brain_first_received_at_utc` DATETIME NOT NULL,
      `ingested_at_utc` DATETIME NOT NULL,
      `parser_version` VARCHAR(32) NOT NULL,
      `schema_version` VARCHAR(32) NOT NULL,
      `quality_status` VARCHAR(24) NOT NULL,
      `asof_status` VARCHAR(24) NOT NULL,
      PRIMARY KEY (`provider`,`instrument`,`event_time_utc`),
      KEY `ix_window_end` (`window_end_utc`),
      KEY `ix_asof` (`asof_status`,`event_time_utc`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{STATE}` (
      `source_key` VARCHAR(48) NOT NULL PRIMARY KEY,
      `last_successful_week_start` DATE NULL,
      `last_success_at_utc` DATETIME NULL,
      `last_status` VARCHAR(24) NOT NULL,
      `last_detail` JSON NULL,
      `updated_at_utc` DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{BATCHES}` (
      `iso_year` SMALLINT UNSIGNED NOT NULL, `iso_week` TINYINT UNSIGNED NOT NULL,
      `archive_uri` VARCHAR(512) NOT NULL, `http_status` SMALLINT NULL,
      `archive_sha256` CHAR(64) NULL, `byte_count` INT UNSIGNED NULL,
      `record_count` INT UNSIGNED NOT NULL DEFAULT 0, `batch_status` VARCHAR(24) NOT NULL,
      `last_attempt_at_utc` DATETIME NOT NULL, `last_success_at_utc` DATETIME NULL,
      PRIMARY KEY (`iso_year`,`iso_week`), KEY `ix_batch_status_attempt` (`batch_status`,`last_attempt_at_utc`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    db.commit()
    cur.close()


def _download(url: str, dest: Path, cap: int = 2_000_000):
    req = urllib.request.Request(url, headers={"User-Agent": "Brain-Model100/1.0 personal historical research"})
    tmp = dest.with_suffix(dest.suffix + ".part")
    digest = hashlib.sha256()
    size = 0
    try:
        with urllib.request.urlopen(req, timeout=45) as response, tmp.open("wb") as f:
            last_modified = response.headers.get("Last-Modified")
            for chunk in iter(lambda: response.read(128 * 1024), b""):
                size += len(chunk)
                if size > cap:
                    raise ValueError("FXCM weekly archive exceeds 2 MB bound")
                digest.update(chunk)
                f.write(chunk)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(dest)
        available = parsedate_to_datetime(last_modified).astimezone(UTC).replace(tzinfo=None) if last_modified else None
        return size, digest.hexdigest(), available
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def weekly_data(year: int, week: int, refresh: bool = False):
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    name = f"EURUSD_H1_{year}_W{week:02d}.csv.gz"
    path = ARCHIVE_DIR / name
    url = BASE.format(year=year, week=week)
    if path.exists() and not refresh:
        raw = path.read_bytes()
        return raw, hashlib.sha256(raw).hexdigest(), None, url, "CACHED"
    size, sha, available = _download(url, path)
    raw = path.read_bytes()
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError(f"archive integrity mismatch: {name}")
    return raw, sha, available, url, "DOWNLOADED"


def upsert_rows(db, rows, archive_uri, archive_sha, archive_available, received_at):
    sql = f"""INSERT INTO `{TABLE}`
      (provider,instrument,market_type,event_time_utc,window_end_utc,feature_ready_at_utc,
       bid_open,bid_high,bid_low,bid_close,ask_open,ask_high,ask_low,ask_close,
       spread_open,spread_close,quote_tick_count,executed_volume,volume_type,
       source_archive_uri,source_archive_sha256,source_archive_available_at_utc,
       brain_first_received_at_utc,ingested_at_utc,parser_version,schema_version,quality_status,asof_status)
      VALUES (%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,'NONE',%s,%s,%s,%s,%s,'fxcm-h1-brain-v1','model100-fxcm-h1-v1','VALIDATED','HISTORICAL_REPLAY')
      ON DUPLICATE KEY UPDATE
       bid_open=VALUES(bid_open),bid_high=VALUES(bid_high),bid_low=VALUES(bid_low),bid_close=VALUES(bid_close),
       ask_open=VALUES(ask_open),ask_high=VALUES(ask_high),ask_low=VALUES(ask_low),ask_close=VALUES(ask_close),
       spread_open=VALUES(spread_open),spread_close=VALUES(spread_close),
       source_archive_sha256=VALUES(source_archive_sha256),
       source_archive_available_at_utc=VALUES(source_archive_available_at_utc),
       ingested_at_utc=VALUES(ingested_at_utc),quality_status='VALIDATED',asof_status='HISTORICAL_REPLAY'"""
    values = []
    for r in rows:
        stamp = datetime.fromisoformat(r["source_event_at"].replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)
        end = datetime.fromisoformat(r["window_end_at"].replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)
        vals = [r[f"{side}_{key}"] for side in ("BID", "ASK") for key in ("OPEN", "HIGH", "LOW", "CLOSE")]
        values.append(("FXCM", "EURUSD", "SPOT_BID_ASK_H1", stamp, end,
                       *vals, r["SPOT_SPREAD_OPEN"], r["SPOT_SPREAD_CLOSE"],
                       archive_uri, archive_sha, archive_available, received_at, received_at))
    cur = db.cursor()
    cur.executemany(sql, values)
    db.commit()
    cur.close()
    return len(values)


def run(db, table: str, args) -> dict:
    if table != TABLE:
        raise ValueError(f"registered target must be {TABLE}")
    if args.end <= args.start:
        raise ValueError("--end must be later than --start")
    if getattr(args, "source", "fxcm") == "dukascopy":
        return run_dukascopy(db, table, args)
    if getattr(args, "source", "fxcm") != "fxcm":
        raise ValueError("unsupported source")
    ensure_schema(db)
    received = datetime.now(UTC).replace(tzinfo=None)
    start = args.start
    cur = db.cursor()
    cur.execute(f"SELECT iso_year,iso_week,batch_status,last_attempt_at_utc FROM `{BATCHES}`")
    prior = {(int(y), int(w)): (str(status), attempted) for y, w, status, attempted in cur.fetchall()}
    cur.close()
    manifest = []
    loaded = 0
    now = datetime.now(UTC).replace(tzinfo=None)
    all_weeks = list(iter_weeks(start, args.end))
    refresh_weeks = set(all_weeks[-2:])
    attempted_count = 0
    for year, week in all_weeks:
        prev = prior.get((year, week))
        is_refresh = (year, week) in refresh_weeks
        if prev and prev[0] == "SUCCESS" and not is_refresh:
            continue
        if prev and prev[0] != "SUCCESS" and prev[1] and (now - prev[1]).total_seconds() < 86400:
            continue
        if attempted_count >= args.max_weeks:
            break
        attempted_count += 1
        url = BASE.format(year=year, week=week)
        try:
            raw, sha, available, url, status = weekly_data(year, week, refresh=is_refresh)
        except urllib.error.HTTPError as exc:
            result = f"HTTP_{exc.code}"
            cur = db.cursor()
            cur.execute(f"""INSERT INTO `{BATCHES}`
              (iso_year,iso_week,archive_uri,http_status,record_count,batch_status,last_attempt_at_utc)
              VALUES (%s,%s,%s,%s,0,%s,%s)
              ON DUPLICATE KEY UPDATE archive_uri=VALUES(archive_uri),http_status=VALUES(http_status),
              batch_status=VALUES(batch_status),last_attempt_at_utc=VALUES(last_attempt_at_utc)""",
              (year, week, url, exc.code, result, now))
            db.commit(); cur.close()
            prior[(year, week)] = (result, now)
            manifest.append({"week": f"{year}-W{week:02d}", "status": result})
            # FXCM archives can be absent or denied at isolated historical
            # weeks. Record the exact HTTP outcome and continue so later
            # cached/published weeks still enter Brain.
            if exc.code in {403, 404, 429, 500, 502, 503, 504}:
                continue
            raise
        content = gzip.decompress(raw).decode("utf-8-sig")
        rows = parse_fxcm_h1_csv(content)
        clipped = []
        for row in rows:
            stamp = datetime.fromisoformat(row["source_event_at"].replace("Z", "+00:00")).date()
            if start <= stamp < args.end:
                clipped.append(row)
        count = upsert_rows(db, clipped, url, sha, available, received)
        loaded += count
        manifest.append({"week": f"{year}-W{week:02d}", "status": status,
                         "sha256": sha, "bytes": len(raw), "rows": count})
        cur = db.cursor()
        cur.execute(f"""INSERT INTO `{BATCHES}`
          (iso_year,iso_week,archive_uri,http_status,archive_sha256,byte_count,record_count,batch_status,last_attempt_at_utc,last_success_at_utc)
          VALUES (%s,%s,%s,200,%s,%s,%s,'SUCCESS',%s,%s)
          ON DUPLICATE KEY UPDATE archive_uri=VALUES(archive_uri),http_status=200,
          archive_sha256=VALUES(archive_sha256),byte_count=VALUES(byte_count),record_count=VALUES(record_count),
          batch_status='SUCCESS',last_attempt_at_utc=VALUES(last_attempt_at_utc),last_success_at_utc=VALUES(last_success_at_utc)""",
          (year, week, url, sha, len(raw), count, received, received))
        db.commit(); cur.close()
        prior[(year, week)] = ("SUCCESS", received)
        cur = db.cursor()
        cur.execute(f"""INSERT INTO `{STATE}`
          (source_key,last_successful_week_start,last_success_at_utc,last_status,last_detail,updated_at_utc)
          VALUES ('FXCM_EURUSD_H1',%s,%s,'SUCCESS',%s,%s)
          ON DUPLICATE KEY UPDATE last_successful_week_start=VALUES(last_successful_week_start),
          last_success_at_utc=VALUES(last_success_at_utc),last_status='SUCCESS',
          last_detail=VALUES(last_detail),updated_at_utc=VALUES(updated_at_utc)""",
          (date.fromisocalendar(year, week, 1), received, json.dumps(manifest), received))
        db.commit()
        cur.close()
        if status == "DOWNLOADED":
            time.sleep(0.25)
    unavailable = [(year, week, status) for (year, week), (status, _) in prior.items()
                   if status != "SUCCESS"]
    overall_status = "PARTIAL_SOURCE_ACCESS" if unavailable else "SUCCESS"
    cur = db.cursor()
    cur.execute(f"""INSERT INTO `{STATE}`
      (source_key,last_successful_week_start,last_success_at_utc,last_status,last_detail,updated_at_utc)
      VALUES ('FXCM_EURUSD_H1',NULL,NULL,%s,%s,%s)
      ON DUPLICATE KEY UPDATE last_status=VALUES(last_status),
      last_detail=VALUES(last_detail),updated_at_utc=VALUES(updated_at_utc)""",
      (overall_status, json.dumps(manifest), datetime.now(UTC).replace(tzinfo=None)))
    db.commit(); cur.close()
    return {"status": overall_status, "target": TABLE, "start": start.isoformat(),
            "end_exclusive": args.end.isoformat(), "weeks_processed": len(manifest),
            "rows_upserted": loaded, "source_gap_weeks": len(unavailable),
            "http_404": sum(status == "HTTP_404" for _, _, status in unavailable),
            "http_403": sum(status == "HTTP_403" for _, _, status in unavailable),
            "archive_directory": str(ARCHIVE_DIR)}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) < 6:
        raise SystemExit("usage: parser.py TABLE HOST PORT USER PASSWORD DATABASE [--start DATE --end DATE --max-weeks N]")
    table, host, port, user, password, database = argv[:6]
    # parser.php passes the registry's argument text as one argv element.
    tail = []
    for item in argv[6:]:
        tail.extend(shlex.split(item))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=date.fromisoformat, default=date(2025, 8, 1))
    p.add_argument("--end", type=date.fromisoformat, default=datetime.now(UTC).date() + timedelta(days=1))
    p.add_argument("--max-weeks", type=int, default=90)
    p.add_argument("--source", choices=("fxcm", "dukascopy"), default="fxcm")
    p.add_argument("--max-hours", type=int, default=24)
    p.add_argument("--dry-run", action="store_true", help="fetch and validate data without database writes")
    args = p.parse_args(tail)
    if not 1 <= args.max_weeks <= 200:
        raise SystemExit("--max-weeks must be 1..200")
    if args.dry_run and args.source != "dukascopy":
        raise SystemExit("--dry-run is available only with --source dukascopy")
    if args.dry_run:
        print(json.dumps(run_dukascopy(None, table, args, dry_run=True), sort_keys=True))
        return
    db = connect(host, int(port), user, password, database)
    try:
        print(json.dumps(run(db, table, args), sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
