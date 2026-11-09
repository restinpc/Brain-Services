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

from model100_ingest import parse_fxcm_h1_csv

UTC = timezone.utc
PARSER_VERSION = "fxcm-h1-brain-v1"
SCHEMA_VERSION = "model100-fxcm-h1-v1"
TABLE = "vlad_model100_fxcm_h1"
STATE = "vlad_model100_fxcm_h1_state"
BATCHES = "vlad_model100_fxcm_h1_batches"
BASE = "https://candledata.fxcorporate.com/H1/EURUSD/{year}/{week}.csv.gz"
ARCHIVE_DIR = Path("/home/model100-data/fxcm_h1")


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def iter_weeks(start: date, end: date):
    cur = monday(start)
    while cur < end:
        iso = cur.isocalendar()
        yield iso.year, iso.week
        cur += timedelta(days=7)


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
    args = p.parse_args(tail)
    if not 1 <= args.max_weeks <= 200:
        raise SystemExit("--max-weeks must be 1..200")
    db = connect(host, int(port), user, password, database)
    try:
        print(json.dumps(run(db, table, args), sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
