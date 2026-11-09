#!/usr/bin/env python3
"""Download and ingest CFTC TFF Futures Only Euro FX positioning archives."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import shlex
import os
import tempfile
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from calendar import monthcalendar, SUNDAY
from datetime import time as dtime, timezone as tz

UTC = timezone.utc
PARSER_VERSION = "cftc-tff-euro-fx-v1"
SCHEMA_VERSION = "model100-cftc-tff-v1"
TABLE = "vlad_model100_cftc_tff_euro_fx"
STATE = "vlad_model100_cftc_tff_state"
ARCHIVE_DIR = Path("/home/model100-data/cftc")
BASE = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
MARKET = "EURO FX - CHICAGO MERCANTILE EXCHANGE"
CONTRACT_MARKET_CODE = "099741"


def _parse_int(value):
    value = (value or "").strip()
    if not value or value == ".": return None
    return int(value.replace(",", ""))


def _parse_float(value):
    value = (value or "").strip()
    if not value or value == ".": return None
    return float(value.replace(",", ""))


def expected_release_at(report_date: date) -> datetime:
    """CFTC states routine weekly releases Friday 15:30 ET; this is inferred.

    Special holiday publication calendars can differ, so consumers must retain
    the SOURCE_HISTORICAL flag and must not treat this computed time as verified.
    """
    release_day = report_date + timedelta(days=3)
    # US DST since 2007: second Sunday in March to first Sunday in November.
    cal = monthcalendar(release_day.year, 3)
    second_sunday = [week[SUNDAY] for week in cal if week[SUNDAY]][1]
    cal = monthcalendar(release_day.year, 11)
    first_sunday = [week[SUNDAY] for week in cal if week[SUNDAY]][0]
    dst_start = date(release_day.year, 3, second_sunday)
    dst_end = date(release_day.year, 11, first_sunday)
    offset = timedelta(hours=-4 if dst_start <= release_day < dst_end else -5)
    eastern = tz(offset)
    return datetime.combine(release_day, dtime(15, 30), tzinfo=eastern).astimezone(UTC).replace(tzinfo=None)


def parse_tff_zip(payload: bytes, year: int, archive_sha256: str):
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        candidates = [n for n in archive.namelist() if n.lower().endswith(".txt")]
        if len(candidates) != 1:
            raise ValueError(f"expected one .txt member, found {len(candidates)}")
        text = archive.read(candidates[0]).decode("utf-8-sig")
    rows = []
    for line_no, r in enumerate(csv.DictReader(io.StringIO(text)), 2):
        market = (r.get("Market_and_Exchange_Names") or "").strip().upper()
        code = (r.get("CFTC_Contract_Market_Code") or "").strip()
        if market != MARKET or code != CONTRACT_MARKET_CODE or (r.get("FutOnly_or_Combined") or "").strip() != "FutOnly":
            continue
        report_date = date.fromisoformat(r["Report_Date_as_YYYY-MM-DD"])
        if report_date.year != year:
            raise ValueError(f"line {line_no}: report date outside archive year")
        numeric_fields = (
            "Open_Interest_All", "Dealer_Positions_Long_All", "Dealer_Positions_Short_All", "Dealer_Positions_Spread_All",
            "Asset_Mgr_Positions_Long_All", "Asset_Mgr_Positions_Short_All", "Asset_Mgr_Positions_Spread_All",
            "Lev_Money_Positions_Long_All", "Lev_Money_Positions_Short_All", "Lev_Money_Positions_Spread_All",
            "Other_Rept_Positions_Long_All", "Other_Rept_Positions_Short_All", "Other_Rept_Positions_Spread_All",
            "Tot_Rept_Positions_Long_All", "Tot_Rept_Positions_Short_All", "NonRept_Positions_Long_All", "NonRept_Positions_Short_All",
            "Change_in_Open_Interest_All", "Change_in_Lev_Money_Long_All", "Change_in_Lev_Money_Short_All",
        )
        values = {key: _parse_int(r.get(key)) for key in numeric_fields}
        if values["Open_Interest_All"] is None or values["Open_Interest_All"] <= 0:
            raise ValueError(f"line {line_no}: invalid open interest")
        rows.append({
            "report_date": report_date,
            "as_of_date": datetime.strptime(r["As_of_Date_In_Form_YYMMDD"], "%y%m%d").date(),
            "release_at_inferred_utc": expected_release_at(report_date),
            "provider_market_name": r["Market_and_Exchange_Names"].strip(),
            "market_code": code,
            "commodity_code": (r.get("CFTC_Commodity_Code") or "").strip(),
            "contract_units": (r.get("Contract_Units") or "").strip(),
            "values": values,
            "net_dealer": _parse_int(r.get("Dealer_Positions_Long_All")) - _parse_int(r.get("Dealer_Positions_Short_All")) if _parse_int(r.get("Dealer_Positions_Long_All")) is not None and _parse_int(r.get("Dealer_Positions_Short_All")) is not None else None,
            "net_asset_manager": _parse_int(r.get("Asset_Mgr_Positions_Long_All")) - _parse_int(r.get("Asset_Mgr_Positions_Short_All")) if _parse_int(r.get("Asset_Mgr_Positions_Long_All")) is not None and _parse_int(r.get("Asset_Mgr_Positions_Short_All")) is not None else None,
            "net_leveraged_money": _parse_int(r.get("Lev_Money_Positions_Long_All")) - _parse_int(r.get("Lev_Money_Positions_Short_All")) if _parse_int(r.get("Lev_Money_Positions_Long_All")) is not None and _parse_int(r.get("Lev_Money_Positions_Short_All")) is not None else None,
            "source_archive_sha256": archive_sha256,
        })
    if len({r["report_date"] for r in rows}) != len(rows):
        raise ValueError("duplicate Euro FX report dates")
    return sorted(rows, key=lambda r: r["report_date"])


def preserve_archive(path, uri):
    """Immutable bytes; observed time is never advertised as publication time."""
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    folder = ARCHIVE_DIR / 'originals'
    folder.mkdir(mode=0o750, parents=True, exist_ok=True)
    for dest, body in [(folder / (digest + '.zip'), payload),
                       (folder / (digest + '.json'), json.dumps({
                           'sha256': digest, 'source_url': uri, 'bytes': len(payload),
                           'snapshot_preserved_at_utc': datetime.now(UTC).isoformat(),
                           'historical_publication_at_utc': None,
                           'preservation_clock_is_not_first_publication': True
                       }, sort_keys=True).encode())]:
        if dest.exists():
            if dest.suffix == '.zip' and hashlib.sha256(dest.read_bytes()).hexdigest() != digest:
                raise ValueError('CFTC_IMMUTABLE_ORIGINAL_SHA_MISMATCH')
            continue
        fd, name = tempfile.mkstemp(prefix=digest + '.', suffix='.part', dir=folder)
        try:
            with os.fdopen(fd, 'wb') as out:
                out.write(body); out.flush(); os.fsync(out.fileno())
            os.chmod(name, 0o640); os.replace(name, dest)
        finally:
            Path(name).unlink(missing_ok=True)
    return digest


def fetch_year(year: int, force=False):
    if not 1986 <= year <= datetime.now(UTC).year:
        raise ValueError("year outside CFTC data range")
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    path = ARCHIVE_DIR / f"fut_fin_txt_{year}.zip"
    uri = BASE.format(year=year)
    if force or not path.exists():
        if path.exists():
            preserve_archive(path, uri)
        req = urllib.request.Request(uri, headers={"User-Agent": "Brain-Model100/1.0", "Accept": "application/zip"})
        error = None
        for attempt in range(4):
            try:
                tmp = path.with_suffix(".zip.part")
                with urllib.request.urlopen(req, timeout=60) as response, tmp.open("wb") as out:
                    size = 0
                    while True:
                        chunk = response.read(128 * 1024)
                        if not chunk: break
                        size += len(chunk)
                        if size > 20_000_000: raise ValueError("CFTC annual archive exceeds 20 MB")
                        out.write(chunk)
                    out.flush()
                with zipfile.ZipFile(tmp) as archive:
                    bad = archive.testzip()
                    if bad: raise ValueError(f"ZIP CRC failed: {bad}")
                preserve_archive(tmp, uri)
                tmp.replace(path)
                error = None; break
            except Exception as exc:
                error = exc; time.sleep(2 ** attempt)
        if error: raise error
    payload = path.read_bytes()
    with zipfile.ZipFile(path) as archive:
        if archive.testzip(): raise ValueError("cached CFTC archive CRC failure")
    sha = preserve_archive(path, uri)
    return path, uri, sha, parse_tff_zip(payload, year, sha)


def ensure_schema(db):
    cur = db.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{TABLE}` (
      provider VARCHAR(24) NOT NULL, instrument VARCHAR(24) NOT NULL,
      report_type VARCHAR(24) NOT NULL, report_date DATE NOT NULL,
      positions_as_of_date DATE NOT NULL, expected_release_at_utc DATETIME NOT NULL,
      source_available_at_utc DATETIME NULL,
      open_interest BIGINT UNSIGNED NOT NULL,
      dealer_long BIGINT NULL,dealer_short BIGINT NULL,dealer_spread BIGINT NULL,
      asset_manager_long BIGINT NULL,asset_manager_short BIGINT NULL,asset_manager_spread BIGINT NULL,
      leveraged_money_long BIGINT NULL,leveraged_money_short BIGINT NULL,leveraged_money_spread BIGINT NULL,
      other_reportable_long BIGINT NULL,other_reportable_short BIGINT NULL,other_reportable_spread BIGINT NULL,
      total_reportable_long BIGINT NULL,total_reportable_short BIGINT NULL,
      nonreportable_long BIGINT NULL,nonreportable_short BIGINT NULL,
      change_open_interest BIGINT NULL,change_leveraged_long BIGINT NULL,change_leveraged_short BIGINT NULL,
      net_dealer BIGINT NULL,net_asset_manager BIGINT NULL,net_leveraged_money BIGINT NULL,
      market_code VARCHAR(12) NOT NULL,commodity_code VARCHAR(8) NOT NULL,contract_units VARCHAR(64) NOT NULL,
      source_archive_uri VARCHAR(512) NOT NULL,source_archive_sha256 CHAR(64) NOT NULL,
      brain_first_received_at_utc DATETIME NOT NULL,ingested_at_utc DATETIME NOT NULL,
      parser_version VARCHAR(32) NOT NULL,schema_version VARCHAR(32) NOT NULL,
      quality_status VARCHAR(24) NOT NULL,asof_status VARCHAR(24) NOT NULL,
      PRIMARY KEY(provider,instrument,report_type,report_date),KEY ix_report_available(expected_release_at_utc,report_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{STATE}` (
      source_key VARCHAR(48) NOT NULL PRIMARY KEY,last_year SMALLINT NULL,last_sha256 CHAR(64) NULL,
      last_rows INT UNSIGNED NOT NULL DEFAULT 0,last_success_at_utc DATETIME NULL,
      status VARCHAR(24) NOT NULL,updated_at_utc DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    db.commit();cur.close()


def write_rows(db, rows, uri, sha, received):
    sql = f"""INSERT INTO `{TABLE}`
      (provider,instrument,report_type,report_date,positions_as_of_date,expected_release_at_utc,source_available_at_utc,
       open_interest,dealer_long,dealer_short,dealer_spread,asset_manager_long,asset_manager_short,asset_manager_spread,
       leveraged_money_long,leveraged_money_short,leveraged_money_spread,other_reportable_long,other_reportable_short,other_reportable_spread,
       total_reportable_long,total_reportable_short,nonreportable_long,nonreportable_short,change_open_interest,change_leveraged_long,change_leveraged_short,
       net_dealer,net_asset_manager,net_leveraged_money,market_code,commodity_code,contract_units,source_archive_uri,source_archive_sha256,
       brain_first_received_at_utc,ingested_at_utc,parser_version,schema_version,quality_status,asof_status)
      VALUES ({','.join(['%s'] * 41)})
      ON DUPLICATE KEY UPDATE positions_as_of_date=VALUES(positions_as_of_date),expected_release_at_utc=VALUES(expected_release_at_utc),
       open_interest=VALUES(open_interest),dealer_long=VALUES(dealer_long),dealer_short=VALUES(dealer_short),dealer_spread=VALUES(dealer_spread),
       asset_manager_long=VALUES(asset_manager_long),asset_manager_short=VALUES(asset_manager_short),asset_manager_spread=VALUES(asset_manager_spread),
       leveraged_money_long=VALUES(leveraged_money_long),leveraged_money_short=VALUES(leveraged_money_short),leveraged_money_spread=VALUES(leveraged_money_spread),
       other_reportable_long=VALUES(other_reportable_long),other_reportable_short=VALUES(other_reportable_short),other_reportable_spread=VALUES(other_reportable_spread),
       total_reportable_long=VALUES(total_reportable_long),total_reportable_short=VALUES(total_reportable_short),
       nonreportable_long=VALUES(nonreportable_long),nonreportable_short=VALUES(nonreportable_short),
       change_open_interest=VALUES(change_open_interest),change_leveraged_long=VALUES(change_leveraged_long),change_leveraged_short=VALUES(change_leveraged_short),
       net_dealer=VALUES(net_dealer),net_asset_manager=VALUES(net_asset_manager),net_leveraged_money=VALUES(net_leveraged_money),
       source_archive_sha256=VALUES(source_archive_sha256),ingested_at_utc=VALUES(ingested_at_utc)"""
    value_fields = ["Open_Interest_All","Dealer_Positions_Long_All","Dealer_Positions_Short_All","Dealer_Positions_Spread_All",
      "Asset_Mgr_Positions_Long_All","Asset_Mgr_Positions_Short_All","Asset_Mgr_Positions_Spread_All",
      "Lev_Money_Positions_Long_All","Lev_Money_Positions_Short_All","Lev_Money_Positions_Spread_All",
      "Other_Rept_Positions_Long_All","Other_Rept_Positions_Short_All","Other_Rept_Positions_Spread_All",
      "Tot_Rept_Positions_Long_All","Tot_Rept_Positions_Short_All","NonRept_Positions_Long_All","NonRept_Positions_Short_All",
      "Change_in_Open_Interest_All","Change_in_Lev_Money_Long_All","Change_in_Lev_Money_Short_All"]
    all_rows = []
    for r in rows:
        v = r["values"]
        vals = [v.get(k) for k in value_fields]
        all_rows.append(("CFTC","CME_6E","TFF_FUTURES_ONLY",r["report_date"],r["as_of_date"],r["release_at_inferred_utc"],None,*vals,
          r["net_dealer"],r["net_asset_manager"],r["net_leveraged_money"],r["market_code"],r["commodity_code"],r["contract_units"],
          uri,sha,received,received,PARSER_VERSION,SCHEMA_VERSION,"VALIDATED","SOURCE_HISTORICAL"))
    cur=db.cursor();cur.executemany(sql,all_rows);db.commit();cur.close();return len(all_rows)


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    if len(argv)<6: raise SystemExit("usage: parser.py TABLE HOST PORT USER PASSWORD DATABASE [--start-year Y --end-year Y]")
    table,host,port,user,password,database=argv[:6];tail=[]
    for item in argv[6:]:tail.extend(shlex.split(item))
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--start-year",type=int,default=2025);p.add_argument("--end-year",type=int,default=datetime.now(UTC).year)
    args=p.parse_args(tail)
    if table!=TABLE or not 1986<=args.start_year<=args.end_year<=datetime.now(UTC).year: raise SystemExit("invalid target or years")
    import mysql.connector
    db=mysql.connector.connect(host=host,port=int(port),user=user,password=password,database=database,connection_timeout=15,autocommit=False)
    try:
        ensure_schema(db);received=datetime.now(UTC).replace(tzinfo=None);out=[]
        for year in range(args.start_year,args.end_year+1):
            try:
                refresh_current_year = year == datetime.now(UTC).year
                path,uri,sha,rows=fetch_year(year,force=refresh_current_year);n=write_rows(db,rows,uri,sha,received)
                cur=db.cursor();cur.execute(f"""INSERT INTO `{STATE}` VALUES ('TFF_EURO_FX',%s,%s,%s,%s,'SUCCESS',UTC_TIMESTAMP())
                  ON DUPLICATE KEY UPDATE last_year=VALUES(last_year),last_sha256=VALUES(last_sha256),last_rows=VALUES(last_rows),last_success_at_utc=VALUES(last_success_at_utc),status='SUCCESS',updated_at_utc=UTC_TIMESTAMP()""",(year,sha,n,received));db.commit();cur.close()
                out.append({"year":year,"status":"SUCCESS","rows":n,"refreshed":refresh_current_year,"sha256":sha,"archive":str(path)})
            except Exception as exc:out.append({"year":year,"status":"ERROR","error":str(exc)[:220]})
        print(json.dumps({"target":TABLE,"results":out},sort_keys=True))
        failures=[r for r in out if r.get("status")!="SUCCESS"]
        if failures: raise RuntimeError(f"{len(failures)} CFTC year batch(es) did not complete successfully")
    finally:db.close()


if __name__=="__main__":main()
