#!/usr/bin/env python3
"""Download and ingest official Binance Spot EURUSDT H1 kline archives.

Volume units are EUR (base asset) and USDT (quote asset). Taker-buy volume is
the aggressive buyer side; sell aggressor volume is total minus taker-buy.
This is Binance Spot EURUSDT activity, not global EUR/USD volume.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shlex
import sys
import time
import re
import urllib.error
import urllib.request
import urllib.parse
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

UTC = timezone.utc
PARSER_VERSION = "binance-eurusdt-h1-v2"
SCHEMA_VERSION = "model100-binance-h1-v1"
TABLE = "vlad_model100_binance_eurusdt_h1"
STATE = "vlad_model100_binance_eurusdt_state"
BASE = "https://data.binance.vision/data/spot/monthly/klines/EURUSDT/1h/EURUSDT-1h-{year}-{month:02d}.zip"
DAILY_BASE = "https://data.binance.vision/data/spot/daily/klines/EURUSDT/1h/EURUSDT-1h-{day}.zip"
ARCHIVE_DIR = Path("/home/model100-data/binance_eurusdt/archives")


def utc_datetime(micros: int) -> datetime:
    return datetime.fromtimestamp(micros / 1_000_000, UTC).replace(tzinfo=None)


def parse_kline_csv(payload: bytes, archive_sha256: str, archive_uri: str):
    """Parse official 12-field Spot kline CSV; 2025+ timestamps are µs."""
    rows = []
    text = payload.decode("utf-8-sig")
    for line_no, row in enumerate(csv.reader(io.StringIO(text)), 1):
        if not row:
            continue
        if row[0].lower() in {"open_time", "open time"}:
            continue
        if len(row) != 12:
            raise ValueError(f"line {line_no}: expected 12 columns, got {len(row)}")
        stamp_raw, op, hi, lo, cl, base_vol, close_raw, quote_vol, trades, taker_base, taker_quote, _ = row
        stamp_raw, close_raw = int(stamp_raw), int(close_raw)
        # Binance documents Spot timestamps as microseconds beginning 2025-01-01.
        # Detect by scale as a safety check; never silently treat µs as ms.
        divisor = 1_000_000 if stamp_raw >= 10**14 else 1000
        event_time = datetime.fromtimestamp(stamp_raw / divisor, UTC).replace(tzinfo=None)
        close_divisor = 1_000_000 if close_raw >= 10**14 else 1000
        close_time = datetime.fromtimestamp(close_raw / close_divisor, UTC).replace(tzinfo=None)
        o, h, l, c = map(float, (op, hi, lo, cl))
        volume, qvolume, buy, qbuy = map(float, (base_vol, quote_vol, taker_base, taker_quote))
        count = int(trades)
        if not (l > 0 and l <= min(o, c) <= max(o, c) <= h and volume >= 0 and qvolume >= 0):
            raise ValueError(f"line {line_no}: invalid OHLC/volume")
        if not (0 <= buy <= volume + max(1e-10, volume * 1e-10)):
            raise ValueError(f"line {line_no}: taker-buy base volume outside total")
        if not (0 <= qbuy <= qvolume + max(1e-10, qvolume * 1e-10)):
            raise ValueError(f"line {line_no}: taker-buy quote volume outside total")
        if event_time.minute or event_time.second or event_time.microsecond:
            raise ValueError(f"line {line_no}: non-hourly open timestamp")
        if close_time <= event_time:
            raise ValueError(f"line {line_no}: close time not after open")
        sell = max(0.0, volume - buy)
        rows.append({
            "event_time": event_time,
            "window_end": event_time.replace(minute=0, second=0, microsecond=0) + __import__("datetime").timedelta(hours=1),
            "open": o, "high": h, "low": l, "close": c,
            "base_volume_eur": volume, "quote_volume_usdt": qvolume,
            "trade_count": count, "aggressor_buy_eur": buy,
            "aggressor_sell_eur": sell, "signed_delta_eur": buy - sell,
            "aggressor_buy_usdt": qbuy, "aggressor_sell_usdt": max(0.0, qvolume - qbuy),
            "source_archive_sha256": archive_sha256, "source_archive_uri": archive_uri,
        })
    if len({r["event_time"] for r in rows}) != len(rows):
        raise ValueError("duplicate kline open timestamps")
    return rows


def _request(url: str, headers=None, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "Brain-Model100/1.0", **(headers or {})})
    return urllib.request.urlopen(req, timeout=timeout)


def _download_ranged(url: str, dest: Path, retries=3):
    """Range-based resumable download; useful with slow archive gateways."""
    head_req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Brain-Model100/1.0"})
    head = urllib.request.urlopen(head_req, timeout=30)
    try:
        size = int(head.headers.get("Content-Length", "0"))
        accept_ranges = head.headers.get("Accept-Ranges", "").lower() == "bytes"
    finally:
        head.close()
    if size <= 0 or size > 20_000_000:
        raise ValueError(f"unexpected archive size: {size}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    offset = tmp.stat().st_size if tmp.exists() else 0
    if offset > size:
        tmp.unlink(); offset = 0
    mode = "ab" if offset else "wb"
    with tmp.open(mode) as out:
        if accept_ranges:
            for start in range(offset, size, 8192):
                end = min(start + 8191, size - 1)
                last = None
                for attempt in range(retries):
                    try:
                        with _request(url, {"Range": f"bytes={start}-{end}"}, timeout=35) as response:
                            expected_len = end - start + 1
                            content_range = response.headers.get("Content-Range", "")
                            data = response.read(expected_len + 1)
                            if response.status != 206 or len(data) != expected_len or not content_range.startswith(f"bytes {start}-{end}/"):
                                raise IOError("archive server returned invalid range data")
                        out.write(data); out.flush(); os.fsync(out.fileno())
                        last = None
                        break
                    except Exception as exc:
                        last = exc
                        time.sleep(min(attempt + 1, 3))
                if last:
                    raise last
        else:
            if offset:
                tmp.unlink(missing_ok=True)
                return _download_ranged(url, dest, retries)
            with _request(url, timeout=60) as response:
                while True:
                    chunk = response.read(64 * 1024)
                    if not chunk: break
                    out.write(chunk)
                out.flush(); os.fsync(out.fileno())
    if tmp.stat().st_size != size:
        raise IOError(f"truncated archive: expected {size}, got {tmp.stat().st_size}")
    return tmp


def download_archive(year: int, month: int, archive_dir: Path = ARCHIVE_DIR, retries=3):
    archive_dir.mkdir(parents=True, exist_ok=True)
    name = f"EURUSDT-1h-{year}-{month:02d}.zip"
    url = BASE.format(year=year, month=month)
    dest = archive_dir / name
    checksum_dest = archive_dir / f"{name}.CHECKSUM"
    if not dest.exists():
        tmp = _download_ranged(url, dest, retries)
        with zipfile.ZipFile(tmp) as zf:
            bad = zf.testzip()
            if bad:
                raise ValueError(f"ZIP CRC failure: {bad}")
        tmp.replace(dest)
    # Official SHA-256 sidecar, compared against the downloaded archive.
    with _request(url + ".CHECKSUM") as response:
        checksum_text = response.read(4096).decode("ascii").strip()
    expected = checksum_text.split()[0].lower()
    actual = hashlib.sha256(dest.read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError(f"official checksum mismatch: {name}")
    with zipfile.ZipFile(dest) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(f"expected one CSV in archive, found {len(csv_names)}")
        payload = zf.read(csv_names[0])
    return dest, url, actual, parse_kline_csv(payload, actual, url)


def download_daily_archive(day: date, archive_dir: Path = ARCHIVE_DIR, retries=3):
    """Download a published completed UTC day, retaining the same CSV schema."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    name = f"EURUSDT-1h-{day.isoformat()}.zip"
    url = DAILY_BASE.format(day=day.isoformat())
    dest = archive_dir / name
    if not dest.exists():
        tmp = _download_ranged(url, dest, retries)
        with zipfile.ZipFile(tmp) as zf:
            bad = zf.testzip()
            if bad: raise ValueError(f"ZIP CRC failure: {bad}")
        tmp.replace(dest)
    with _request(url + ".CHECKSUM") as response:
        expected = response.read(4096).decode("ascii").strip().split()[0].lower()
    actual = hashlib.sha256(dest.read_bytes()).hexdigest()
    if actual != expected: raise ValueError(f"official checksum mismatch: {name}")
    with zipfile.ZipFile(dest) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1: raise ValueError(f"expected one CSV, found {len(names)}")
        rows = parse_kline_csv(zf.read(names[0]), actual, url)
    return dest, url, actual, rows


def ensure_schema(db):
    cur = db.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{TABLE}` (
      provider VARCHAR(24) NOT NULL, instrument VARCHAR(32) NOT NULL,
      market_type VARCHAR(32) NOT NULL, event_time_utc DATETIME NOT NULL,
      window_end_utc DATETIME NOT NULL, open_price DECIMAL(24,10) NOT NULL,
      high_price DECIMAL(24,10) NOT NULL, low_price DECIMAL(24,10) NOT NULL,
      close_price DECIMAL(24,10) NOT NULL,
      base_volume_eur DECIMAL(28,12) NOT NULL, quote_volume_usdt DECIMAL(28,12) NOT NULL,
      trade_count BIGINT UNSIGNED NOT NULL, aggressor_buy_volume_eur DECIMAL(28,12) NOT NULL,
      aggressor_sell_volume_eur DECIMAL(28,12) NOT NULL, signed_trade_delta_eur DECIMAL(28,12) NOT NULL,
      aggressor_buy_volume_usdt DECIMAL(28,12) NOT NULL, aggressor_sell_volume_usdt DECIMAL(28,12) NOT NULL,
      source_archive_uri VARCHAR(512) NOT NULL, source_archive_sha256 CHAR(64) NOT NULL,
      source_archive_available_at_utc DATETIME NULL, feature_ready_at_utc DATETIME NOT NULL,
      brain_first_received_at_utc DATETIME NOT NULL, ingested_at_utc DATETIME NOT NULL,
      parser_version VARCHAR(32) NOT NULL, schema_version VARCHAR(32) NOT NULL,
      quality_status VARCHAR(24) NOT NULL, asof_status VARCHAR(24) NOT NULL,
      PRIMARY KEY(provider,instrument,event_time_utc), KEY ix_window_end(window_end_utc),
      KEY ix_asof(asof_status,event_time_utc)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{STATE}` (
      source_key VARCHAR(48) NOT NULL PRIMARY KEY, last_archive VARCHAR(32) NULL,
      last_sha256 CHAR(64) NULL, last_rows INT UNSIGNED NOT NULL DEFAULT 0,
      last_success_at_utc DATETIME NULL, status VARCHAR(24) NOT NULL,
      updated_at_utc DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    db.commit()
    cur.close()


def write_rows(db, rows, archive_sha, archive_uri, received, source_available=None,
               asof_status="HISTORICAL_REPLAY"):
    sql = f"""INSERT INTO `{TABLE}`
      (provider,instrument,market_type,event_time_utc,window_end_utc,open_price,high_price,low_price,close_price,
       base_volume_eur,quote_volume_usdt,trade_count,aggressor_buy_volume_eur,aggressor_sell_volume_eur,
       signed_trade_delta_eur,aggressor_buy_volume_usdt,aggressor_sell_volume_usdt,source_archive_uri,
       source_archive_sha256,source_archive_available_at_utc,feature_ready_at_utc,brain_first_received_at_utc,
       ingested_at_utc,parser_version,schema_version,quality_status,asof_status)
      VALUES (%s,%s,'SPOT_EURUSDT',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'VALIDATED',%s)
      ON DUPLICATE KEY UPDATE open_price=VALUES(open_price),high_price=VALUES(high_price),low_price=VALUES(low_price),
       close_price=VALUES(close_price),base_volume_eur=VALUES(base_volume_eur),quote_volume_usdt=VALUES(quote_volume_usdt),
       trade_count=VALUES(trade_count),aggressor_buy_volume_eur=VALUES(aggressor_buy_volume_eur),
       aggressor_sell_volume_eur=VALUES(aggressor_sell_volume_eur),signed_trade_delta_eur=VALUES(signed_trade_delta_eur),
       aggressor_buy_volume_usdt=VALUES(aggressor_buy_volume_usdt),aggressor_sell_volume_usdt=VALUES(aggressor_sell_volume_usdt),
       source_archive_uri=VALUES(source_archive_uri),source_archive_sha256=VALUES(source_archive_sha256),
       feature_ready_at_utc=VALUES(feature_ready_at_utc),ingested_at_utc=VALUES(ingested_at_utc),
       source_archive_available_at_utc=VALUES(source_archive_available_at_utc),
       quality_status='VALIDATED',asof_status=VALUES(asof_status)"""
    values = [("BINANCE", "EURUSDT", r["event_time"], r["window_end"], r["open"], r["high"], r["low"], r["close"],
               r["base_volume_eur"], r["quote_volume_usdt"], r["trade_count"], r["aggressor_buy_eur"],
               r["aggressor_sell_eur"], r["signed_delta_eur"], r["aggressor_buy_usdt"], r["aggressor_sell_usdt"],
               archive_uri, archive_sha, source_available, received, received, received,
               PARSER_VERSION, SCHEMA_VERSION, asof_status) for r in rows]
    cur = db.cursor()
    cur.executemany(sql, values)
    db.commit()
    cur.close()
    return len(values)


def download_closed_klines(db, max_hours: int):
    """Fetch only closed UTC H1 bars from the official Spot REST API."""
    if not 1 <= max_hours <= 1000:
        raise ValueError("--api-hours must be between 1 and 1000")
    now = datetime.now(UTC)
    closed_before = now.replace(minute=0, second=0, microsecond=0)
    end_ms = int(closed_before.timestamp() * 1000) - 1
    cur = db.cursor()
    cur.execute(f"SELECT MAX(event_time_utc) FROM `{TABLE}`")
    latest = cur.fetchone()[0]
    cur.close()
    if latest:
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=UTC)
        start_ms = int((latest.timestamp() + 3600) * 1000)
    else:
        start_ms = end_ms - max_hours * 3_600_000
    if start_ms > end_ms:
        return {"status": "UP_TO_DATE", "rows": 0, "uri": None}
    start_ms = max(start_ms, end_ms - max_hours * 3_600_000)
    query = urllib.parse.urlencode({"symbol": "EURUSDT", "interval": "1h", "timeZone": "0",
                                    "startTime": start_ms, "endTime": end_ms,
                                    "limit": min(max_hours, 1000)})
    uri = "https://api.binance.com/api/v3/klines?" + query
    last_error = None
    for attempt in range(3):
        try:
            with _request(uri, timeout=30) as response:
                payload = response.read(2_000_001)
            if len(payload) > 2_000_000:
                raise ValueError("REST response exceeded 2 MB limit")
            decoded = json.loads(payload)
            if not isinstance(decoded, list):
                raise ValueError("unexpected Binance kline response")
            out = io.StringIO()
            writer = csv.writer(out, lineterminator="\n")
            for row in decoded:
                if not isinstance(row, list) or len(row) != 12:
                    raise ValueError("REST kline row must contain 12 fields")
                writer.writerow(row)
            sha = hashlib.sha256(payload).hexdigest()
            rows = parse_kline_csv(out.getvalue().encode(), sha, uri)
            rows = [r for r in rows if r["window_end"].replace(tzinfo=UTC) <= closed_before]
            return {"status": "SUCCESS", "rows": rows, "sha256": sha, "uri": uri,
                    "available_at": now.replace(tzinfo=None)}
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504}:
                raise
        except (TimeoutError, urllib.error.URLError) as exc:
            last_error = exc
        if attempt < 2:
            time.sleep(1 + attempt * 2)
    raise RuntimeError(f"Binance REST request failed after retries: {last_error}")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) < 6:
        raise SystemExit("usage: parser.py TABLE HOST PORT USER PASSWORD DATABASE [--start YYYY-MM --end YYYY-MM --max-months N]")
    table, host, port, user, password, database = argv[:6]
    tail = []
    for item in argv[6:]:
        tail.extend(shlex.split(item))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", default="2025-08")
    p.add_argument("--end", default=(datetime.now(UTC).replace(day=1) - __import__("datetime").timedelta(days=1)).strftime("%Y-%m"))
    p.add_argument("--max-months", type=int, default=24)
    p.add_argument("--daily-days", type=int, default=2)
    p.add_argument("--incremental-only", action="store_true")
    p.add_argument("--api-hours", type=int, default=48)
    args = p.parse_args(tail)
    if (table != TABLE or not 1 <= args.max_months <= 36 or not 0 <= args.daily_days <= 7
            or not 1 <= args.api_hours <= 1000):
        raise SystemExit("invalid target table or --max-months")
    sy, sm = map(int, args.start.split("-")); ey, em = map(int, args.end.split("-"))
    months = []
    while (sy, sm) <= (ey, em):
        months.append((sy, sm)); sm += 1
        if sm == 13: sy, sm = sy + 1, 1
    if len(months) > args.max_months:
        months = months[-args.max_months:]
    import mysql.connector
    db = mysql.connector.connect(host=host, port=int(port), user=user, password=password,
                                 database=database, connection_timeout=15, autocommit=False)
    try:
        ensure_schema(db)
        received = datetime.now(UTC).replace(tzinfo=None)
        results = []
        for year, month in ([] if args.incremental_only else months):
            try:
                path, uri, sha, rows = download_archive(year, month)
                count = write_rows(db, rows, sha, uri, received)
                cur = db.cursor()
                cur.execute(f"""INSERT INTO `{STATE}` VALUES ('EURUSDT_1H',%s,%s,%s,%s,'SUCCESS',UTC_TIMESTAMP())
                    ON DUPLICATE KEY UPDATE last_archive=VALUES(last_archive),last_sha256=VALUES(last_sha256),
                    last_rows=VALUES(last_rows),last_success_at_utc=VALUES(last_success_at_utc),status='SUCCESS',updated_at_utc=UTC_TIMESTAMP()""",
                    (f"{year}-{month:02d}", sha, count, received))
                db.commit(); cur.close()
                results.append({"month": f"{year}-{month:02d}", "status": "SUCCESS", "rows": count, "sha256": sha, "archive": str(path)})
            except urllib.error.HTTPError as exc:
                results.append({"month": f"{year}-{month:02d}", "status": f"HTTP_{exc.code}"})
            except Exception as exc:
                results.append({"month": f"{year}-{month:02d}", "status": "ERROR", "error": str(exc)[:240]})
        for days_back in range(0 if args.incremental_only else args.daily_days, 0, -1):
            day = datetime.now(UTC).date() - __import__("datetime").timedelta(days=days_back)
            try:
                path, uri, sha, rows = download_daily_archive(day)
                count = write_rows(db, rows, sha, uri, received)
                results.append({"day": day.isoformat(), "status": "SUCCESS", "rows": count, "sha256": sha, "archive": str(path)})
            except urllib.error.HTTPError as exc:
                results.append({"day": day.isoformat(), "status": f"HTTP_{exc.code}"})
            except Exception as exc:
                results.append({"day": day.isoformat(), "status": "ERROR", "error": str(exc)[:240]})
        try:
            api = download_closed_klines(db, args.api_hours)
            if api["status"] == "SUCCESS":
                certified_open = api["available_at"].replace(minute=0, second=0, microsecond=0) - __import__("datetime").timedelta(hours=1)
                current_rows = [r for r in api["rows"] if r["event_time"] == certified_open]
                historical_rows = [r for r in api["rows"] if r["event_time"] != certified_open]
                count = write_rows(db, historical_rows, api["sha256"], api["uri"], api["available_at"],
                                   source_available=api["available_at"], asof_status="SOURCE_HISTORICAL")
                count += write_rows(db, current_rows, api["sha256"], api["uri"], api["available_at"],
                                    source_available=api["available_at"], asof_status="CERTIFIED_ASOF")
                api = {k: v for k, v in api.items() if k != "rows"}
                api["rows"] = count
                api["available_at"] = api["available_at"].isoformat(timespec="seconds") + "Z"
            results.append({"source": "spot_rest_api", **api})
        except Exception as exc:
            results.append({"source": "spot_rest_api", "status": "ERROR", "error": str(exc)[:240]})
        print(json.dumps({"target": TABLE, "parser_version": PARSER_VERSION, "results": results}, sort_keys=True))
        failures = [r for r in results if r.get("status") not in {"SUCCESS", "UP_TO_DATE"}]
        if failures:
            raise RuntimeError(f"{len(failures)} Binance archive batch(es) did not complete successfully")
    finally:
        db.close()


if __name__ == "__main__":
    main()
