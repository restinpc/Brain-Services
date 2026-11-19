#!/usr/bin/env python3
"""B1 collector for ECB MID originals and official ifo vintages.

All historical ifo cells remain DAY_ONLY until their original release date/time
is independently matched. MID RSS publication timestamps are accepted only
when they contain an explicit UTC offset. Raw source bodies are immutable and
content addressed; database rows retain every changed digest.
"""
from __future__ import annotations

import hashlib
import http.client
import io
import math
import json
import os
import re
import shlex
import sys
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

UTC = timezone.utc
TABLE = "vlad_model100_primary_source_records"
PARSER_VERSION = "model100-primary-sources-v5"
RAW_ROOT = Path("/home/model100-data/primary-source-raw")
MID_URL = "https://mid.ecb.europa.eu/rss/mid.xml"
IFO_URL = "https://www.ifo.de/sites/default/files/facts/vintage/Germany-ifo-vintage.xlsx"
GDP_NOW_URL = "https://www.atlantafed.org/-/media/Project/Atlanta/FRBA/Documents/research-and-data/data/gdpnow/GDPTrackingModelDataAndForecasts.xlsx"
GDP_NOW_CACHE = Path("/home/model100-data/primary-source-cache/GDPTrackingModelDataAndForecasts.xlsx")
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

# Normal scheduling is source-specific. The registered parser is invoked by
# Brain every minute; the scheduler below runs the live ECB feed every five
# minutes and at most one other due collector per invocation. Full collectors
# remain idempotent and are throttled independently by their own state rows.
SOURCE_INTERVALS_SECONDS = {
    "ECB_MID": 300,
    "GDP_NOW": 3600,
    "IFO_VINTAGE": 21600,
    "IFO_RELEASES": 3600,
    "ECB_HIST": 86400,
    "FED_FOMC": 86400,
    "FED_SPEECHES_ARCHIVE": 86400,
    "FED_POLICY_ARCHIVE": 86400,
    "BLS_RELEASES": 3600,
    "BEA_RELEASES": 86400,
    "EUROSTAT_FLASH": 86400,
    "DEST_ATIS_CPI": 86400,
    "ALFRED": 86400,
    "ADP_NER": 604800,
    "ZEW": 86400,
    "SP_GLOBAL_FLASH_PMI": 604800,
    "ISM_PMI": 604800,
    "CLEVELAND_NOW": 86400,
    "CENSUS_RETAIL": 86400,
    "ECB_EDP_VINTAGE": 86400,
    "WHITEHOUSE_TRADE": 300,
    "USTR_TRADE": 86400,
    "NYFED_NOW": 86400,
    "ECB_SPF": 21600,
    "NYFED_SME": 86400,
}
SOURCE_PRIORITY = (
    "ECB_HIST", "FED_FOMC", "FED_SPEECHES_ARCHIVE", "FED_POLICY_ARCHIVE", "BLS_RELEASES", "BEA_RELEASES", "EUROSTAT_FLASH",
    "DEST_ATIS_CPI", "ALFRED", "ADP_NER", "IFO_VINTAGE", "IFO_RELEASES", "GDP_NOW",
    "ZEW", "SP_GLOBAL_FLASH_PMI", "ISM_PMI", "CLEVELAND_NOW",
    "CENSUS_RETAIL", "ECB_EDP_VINTAGE", "WHITEHOUSE_TRADE", "USTR_TRADE", "NYFED_NOW", "ECB_SPF", "NYFED_SME",
)
SOURCE_DATA_IDS = {"GDP_NOW": "GDP_NOW_ATL","FED_SPEECHES_ARCHIVE":"FED_FOMC","FED_POLICY_ARCHIVE":"FED_FOMC","IFO_RELEASES":"IFO_VINTAGE"}


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def parse_explicit_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC).replace(tzinfo=None)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value.strip())
            if parsed.tzinfo is None:
                return None
            return parsed.astimezone(UTC).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            return None


class IncompleteSourceResponse(TimeoutError):
    """Transport evidence only: partial bytes must never become release data."""
    def __init__(self, response, body, error):
        super().__init__(f'INCOMPLETE_SOURCE_RESPONSE:{type(error).__name__}:HTTP={response.status}:bytes={len(body)}')
        self.response_status = int(response.status)
        self.partial_body = body
        self.response_headers = {k.lower(): response.headers.get(k) for k in
                                 ('Content-Type','Content-Length','ETag','Last-Modified','Retry-After')
                                 if response.headers.get(k) is not None}


# Reuse the egress proxy already configured by native PHP news collectors.
# Only network-failing public origins use it; there is no fallback on HTTP403.
NATIVE_EGRESS_PROXY = 'http://10.10.11.1:3128'
PROXY_ORIGINS = frozenset(('www.newyorkfed.org','www.clevelandfed.org',
                          'www.atlantafed.org','www.zew.de','download.zew.de'))


def official_source_proxy(url):
    parsed = urllib.parse.urlparse(url)
    return NATIVE_EGRESS_PROXY if parsed.scheme == 'https' and parsed.hostname in PROXY_ORIGINS else None


def download(url: str, timeout: int = 25, cap: int = 12_000_000, conditional_headers=None, method="GET"):
    headers = {"User-Agent": "Brain-Model100/1.0 research collector"}
    if urllib.parse.urlparse(url).hostname == 'fraser.stlouisfed.org':
        # FRASER transport requires the client-library product token. Retain
        # the collector identity; no browser impersonation or auth workaround.
        headers['User-Agent'] = 'Python-urllib/' + str(sys.version_info.major) + '.' + str(sys.version_info.minor) + ' Brain-Model100/1.0 research collector'
    headers.update(conditional_headers or {})
    req = urllib.request.Request(url, headers=headers, method=method)
    proxy = official_source_proxy(url)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({'https': proxy})).open if proxy else urllib.request.urlopen
    transport = 'B1_NATIVE_EGRESS_PROXY' if proxy else 'B1_DIRECT_HTTP'
    try:
        response_context = opener(req, timeout=timeout)
    except urllib.error.HTTPError as error:
        if error.code == 304:
            return b"", {"status": 304, "content_type": error.headers.get("Content-Type"),
                          "network_response_bytes": 0,
                          "etag": error.headers.get("ETag"), "last_modified": error.headers.get("Last-Modified"),
                          "url": url, "transport_path": transport, "request_method": method, "received_at_utc": utcnow().replace(tzinfo=UTC).isoformat()}
        raise
    with response_context as response:
        chunks, size = [], 0
        while True:
            try:
                reader = getattr(response, 'read1', response.read)
                chunk = reader(128 * 1024)
            except (OSError, http.client.IncompleteRead) as error:
                raise IncompleteSourceResponse(response, b''.join(chunks) + getattr(error, 'partial', b''), error) from error
            if not chunk:
                break
            size += len(chunk)
            if size > cap:
                raise ValueError("SOURCE_RESPONSE_SIZE_LIMIT")
            chunks.append(chunk)
        body = b"".join(chunks)
        return body, {
            "status": int(response.status),
            "network_response_bytes": len(body),
            "content_type": response.headers.get("Content-Type"),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "url": response.geturl(),
            "transport_path": transport,
            "request_method": method,
            "content_length": response.headers.get('Content-Length'),
            "received_at_utc": utcnow().replace(tzinfo=UTC).isoformat(),
        }


def download_large_xlsx(url: str, timeout: int = 20, cap: int = 15_000_000, conditional_headers=None):
    """Use the host's existing curl/HTTP2 path for large official workbooks."""
    with tempfile.TemporaryDirectory(prefix="model100-source-") as folder:
        body_path = Path(folder) / "source.xlsx"
        header_path = Path(folder) / "headers.txt"
        command = ["curl", "--connect-timeout", "10", "--max-time", str(timeout), "-L", "-sS"]
        proxy = official_source_proxy(url)
        if proxy:
            command.extend(['--proxy', proxy])
        for key, flag in (("If-None-Match", "If-None-Match"), ("If-Modified-Since", "If-Modified-Since")):
            value = (conditional_headers or {}).get(key)
            if value:
                command.extend(["-H", f"{flag}: {value}"])
        command.extend([
            "-D", str(header_path), "-o", str(body_path), "-w", "%{http_code}\n%{content_type}\n%{url_effective}\n", url])
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout + 5)
        if result.returncode:
            raise TimeoutError("GDP_NOW_CURL_FAILED:" + str(result.returncode))
        lines = result.stdout.splitlines()
        if len(lines) < 3 or not body_path.is_file():
            raise ValueError("GDP_NOW_CURL_METADATA_MISSING")
        status = int(lines[-3])
        final_url = lines[-1]
        header_blocks = re.split(r"\r?\n\r?\n", header_path.read_text(errors="replace"))
        headers = {}
        for block in reversed(header_blocks):
            if re.search(r"^HTTP/", block):
                for line in block.splitlines()[1:]:
                    if ":" in line:
                        key, value = line.split(":", 1)
                        headers[key.strip().lower()] = value.strip()
                break
        if status == 304:
            return b"", {"status": 304, "content_type": lines[-2] or headers.get("content-type"),
                          "network_response_bytes": 0,
                          "etag": headers.get("etag"), "last_modified": headers.get("last-modified"),
                          "url": final_url, "transport_path": "B1_NATIVE_EGRESS_PROXY" if proxy else "B1_DIRECT_HTTP", "received_at_utc": utcnow().replace(tzinfo=UTC).isoformat()}
        if status != 200:
            raise urllib.error.HTTPError(url, status, "GDPNow workbook HTTP status", None, None)
        if body_path.stat().st_size > cap:
            raise ValueError("GDP_NOW_WORKBOOK_SIZE_LIMIT")
        body = body_path.read_bytes()
        return body, {"status": status, "network_response_bytes": len(body),
                      "content_type": lines[-2] or headers.get("content-type"),
                      "etag": headers.get("etag"), "last_modified": headers.get("last-modified"),
                      "url": final_url, "transport_path": "B1_NATIVE_EGRESS_PROXY" if proxy else "B1_DIRECT_HTTP", "received_at_utc": utcnow().replace(tzinfo=UTC).isoformat()}


def conditional_fetch(db, source_id: str, record_key: str, url: str, *, large: bool = False,
                      timeout: int = 25, cap: int = 12_000_000):
    cur = db.cursor()
    cur.execute(f"SELECT extra_json,raw_blob_path,content_type,content_sha256 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",
                (source_id, record_key))
    prior = cur.fetchone()
    cur.close()
    conditional = {}
    if prior:
        try:
            extra = json.loads(prior[0] or "{}")
        except (TypeError, ValueError):
            extra = {}
        if extra.get("etag"):
            conditional["If-None-Match"] = extra["etag"]
        elif extra.get("last_modified"):
            conditional["If-Modified-Since"] = extra["last_modified"]
    if large and prior and extra.get('etag') and Path(prior[1]).is_file():
        # Some archive servers ignore conditional GET but implement validators
        # on HEAD. Prove the same representation without downloading 11MB.
        _, head = download(url, timeout=min(timeout,12), cap=0,
                           conditional_headers=conditional, method='HEAD')
        length = head.get('content_length')
        if head['status'] == 304 or (head.get('etag') == extra['etag']
                and (length is None or int(length) == Path(prior[1]).stat().st_size)):
            cached = Path(prior[1]).read_bytes()
            if hashlib.sha256(cached).hexdigest() != prior[3]:
                raise ValueError('CACHED_ORIGINAL_SHA_MISMATCH')
            head.update(content_type=head.get('content_type') or prior[2],
                        original_http_status=200, network_response_bytes=0,
                        fetch_status='HTTP_304_CACHE' if head['status']==304 else 'HEAD_VALIDATED_CACHE')
            return cached, head
    if large:
        body, meta = download_large_xlsx(url, timeout=timeout, cap=cap, conditional_headers=conditional)
    else:
        body, meta = download(url, timeout=timeout, cap=cap, conditional_headers=conditional)
    meta.setdefault("network_response_bytes", len(body))
    if meta["status"] == 304:
        if not prior:
            raise ValueError("HTTP_304_WITHOUT_CACHED_ORIGINAL")
        cached_path = Path(prior[1])
        body = cached_path.read_bytes()
        if hashlib.sha256(body).hexdigest() != prior[3]:
            raise ValueError("CACHED_ORIGINAL_SHA_MISMATCH")
        meta["content_type"] = meta.get("content_type") or prior[2]
        meta["etag"] = meta.get("etag") or extra.get("etag")
        meta["last_modified"] = meta.get("last_modified") or extra.get("last_modified")
        meta["original_http_status"] = 200
        meta["fetch_status"] = "HTTP_304_CACHE"
    return body, meta


def preserve(source_id: str, body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    dest = RAW_ROOT / source_id / (digest + ".bin")
    dest.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    if dest.exists():
        if hashlib.sha256(dest.read_bytes()).hexdigest() != digest:
            raise ValueError("RAW_BLOB_HASH_MISMATCH")
    else:
        # A killed prior process may leave .part files. Unique temporary names
        # let retries finish without trusting or deleting an incomplete body.
        fd, temporary = tempfile.mkstemp(prefix=digest + ".", suffix=".part", dir=dest.parent)
        tmp = Path(temporary)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            if hashlib.sha256(tmp.read_bytes()).hexdigest() != digest:
                raise ValueError("RAW_BLOB_HASH_MISMATCH")
            os.chmod(tmp, 0o640)
            os.replace(tmp, dest)
        finally:
            tmp.unlink(missing_ok=True)
    return digest, str(dest)


def ensure_schema(db) -> None:
    cur = db.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS `{TABLE}` (
      `source_id` varchar(40) NOT NULL,
      `record_key` varchar(220) NOT NULL,
      `record_kind` varchar(24) NOT NULL,
      `title` varchar(500) DEFAULT NULL,
      `event_type` varchar(96) DEFAULT NULL,
      `region` varchar(48) DEFAULT NULL,
      `observation_period_start` date DEFAULT NULL,
      `vintage_month` date DEFAULT NULL,
      `numeric_value` decimal(20,8) DEFAULT NULL,
      `unit` varchar(48) DEFAULT NULL,
      `source_published_at_utc` datetime DEFAULT NULL,
      `first_received_at_utc` datetime NOT NULL,
      `last_seen_at_utc` datetime NOT NULL,
      `reference_timezone` varchar(48) DEFAULT NULL,
      `release_clock_evidence` varchar(32) NOT NULL,
      `asof_class` varchar(40) NOT NULL,
      `canonical_url` varchar(1024) NOT NULL,
      `content_type` varchar(160) DEFAULT NULL,
      `http_status` smallint unsigned NOT NULL,
      `content_sha256` char(64) NOT NULL,
      `raw_blob_path` varchar(512) NOT NULL,
      `parser_version` varchar(48) NOT NULL,
      `extra_json` json DEFAULT NULL,
      PRIMARY KEY (`source_id`,`record_key`,`content_sha256`),
      KEY `ix_model100_asof` (`asof_class`,`source_published_at_utc`,`first_received_at_utc`),
      KEY `ix_model100_vintage` (`source_id`,`vintage_month`,`observation_period_start`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""")
    db.commit()
    cur.close()


def store(db, row: dict) -> None:
    try:
        from model100_semantics import economic_identity
    except ImportError:
        from work.model100_semantics import economic_identity
    identity = economic_identity(row)
    if identity:
        extra = row.get('extra_json') or '{}'
        extra = json.loads(extra) if isinstance(extra, str) else dict(extra)
        row = {**row, 'extra_json': json.dumps({**extra, **identity}, ensure_ascii=False)}
    columns = ("source_id", "record_key", "record_kind", "title", "event_type", "region",
               "observation_period_start", "vintage_month", "numeric_value", "unit",
               "source_published_at_utc", "first_received_at_utc", "last_seen_at_utc",
               "reference_timezone", "release_clock_evidence", "asof_class", "canonical_url",
               "content_type", "http_status", "content_sha256", "raw_blob_path", "parser_version", "extra_json")
    values = [row.get(k) for k in columns]
    placeholders = ",".join(["%s"] * len(columns))
    # Preserve original HTTP status and first receipt time. Reprocessing the
    # same immutable body may promote previously unknown release-time evidence
    # or a generic title as parsing improves, but never replace evidence with
    # DAY_ONLY or a later 304 status.
    update = (
        "last_seen_at_utc=VALUES(last_seen_at_utc),"
        "source_published_at_utc=COALESCE(VALUES(source_published_at_utc),source_published_at_utc),"
        "release_clock_evidence=IF(VALUES(source_published_at_utc) IS NOT NULL,"
        "VALUES(release_clock_evidence),release_clock_evidence),"
        "asof_class=IF(record_kind IN ('FEED','RUN_CONTROL') OR VALUES(asof_class)='UNUSABLE',VALUES(asof_class),"
        "IF(asof_class='CERTIFIED_LIVE' AND VALUES(asof_class)='HISTORICAL_DOCUMENTED' "
        "AND TIMESTAMPDIFF(SECOND,source_published_at_utc,first_received_at_utc) BETWEEN 0 AND 300,asof_class,"
        "IF(VALUES(source_published_at_utc) IS NOT NULL,VALUES(asof_class),asof_class))),"
        "reference_timezone=COALESCE(VALUES(reference_timezone),reference_timezone),"
        "title=IF((title IS NULL OR LOWER(title) IN ('view','read more','browse page')) "
        "AND VALUES(title) IS NOT NULL,VALUES(title),title),"
        "parser_version=VALUES(parser_version),extra_json=VALUES(extra_json)"
    )
    cur = db.cursor()
    cur.execute(f"INSERT INTO `{TABLE}` ({','.join('`'+k+'`' for k in columns)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update}", values)
    db.commit()
    cur.close()


def base_row(source_id: str, key: str, kind: str, url: str, body: bytes, meta: dict,
             first_received: datetime, published: datetime | None, evidence: str, asof: str,
             title: str | None = None, event_type: str | None = None, **extra) -> dict:
    digest, path = preserve(source_id, body)
    received_at = parse_explicit_datetime(meta.get("received_at_utc")) or first_received
    extra_json = {"etag": meta.get("etag"), "last_modified": meta.get("last_modified"),
                  "last_http_status": meta.get("original_http_status", meta.get("status")),
                  "last_fetch_http_status": meta.get("status"),
                  "fetch_status": meta.get("fetch_status", "HTTP_" + str(meta.get("status", "UNKNOWN")))}
    extra_json.update(extra.get("extra_json", {}))
    if meta.get("transport_path"):
        extra_json["transport_path"] = meta["transport_path"]
    if meta.get("request_method"):
        extra_json["http_request_method"] = meta["request_method"]
    return {
        "source_id": source_id, "record_key": key, "record_kind": kind, "title": title,
        "event_type": event_type, "region": extra.get("region"),
        "observation_period_start": extra.get("observation_period_start"),
        "vintage_month": extra.get("vintage_month"), "numeric_value": extra.get("numeric_value"),
        "unit": extra.get("unit"), "source_published_at_utc": published,
        "first_received_at_utc": received_at, "last_seen_at_utc": received_at,
        "reference_timezone": extra.get("reference_timezone"), "release_clock_evidence": evidence,
        "asof_class": asof, "canonical_url": meta.get("url", url), "content_type": meta.get("content_type"),
        "http_status": meta.get("original_http_status", meta.get("status", 200)),
        "content_sha256": digest, "raw_blob_path": path,
        "parser_version": PARSER_VERSION, "extra_json": json.dumps(extra_json, ensure_ascii=False),
    }


def xml_text(element: ET.Element, local_name: str) -> str | None:
    for node in element.iter():
        if node.tag.rsplit("}", 1)[-1] == local_name and node.text:
            return node.text.strip()
    return None


def reclassify_delayed_mid_rows(db, now: datetime) -> int:
    """Downgrade previously cached MID items that were first received late.

    These rows came from the historical RSS window and cannot be certified as
    live observations. Reuse the immutable row body/SHA metadata; do not issue
    any additional requests.
    """
    fields = ("source_id", "record_key", "record_kind", "title", "event_type", "region",
              "observation_period_start", "vintage_month", "numeric_value", "unit",
              "source_published_at_utc", "first_received_at_utc", "last_seen_at_utc",
              "reference_timezone", "release_clock_evidence", "asof_class", "canonical_url",
              "content_type", "http_status", "content_sha256", "raw_blob_path", "parser_version", "extra_json")
    cur = db.cursor()
    cur.execute(f"SELECT {','.join('`'+field+'`' for field in fields)} FROM `{TABLE}` "
                "WHERE source_id='ECB_MID' AND record_kind IN ('RELEASE','FEED') "
                "AND asof_class='CERTIFIED_LIVE'")
    rows = cur.fetchall()
    cur.close()
    changed = 0
    for values in rows:
        row = dict(zip(fields, values))
        if row['record_kind'] == 'FEED':
            ex=json.loads(row.get('extra_json') or '{}')
            row['extra_json']=json.dumps({**ex,'previous_asof_class':row['asof_class'],
                                         'quality_correction':'FEED_SNAPSHOT_IS_NOT_A_CERTIFIED_RELEASE'})
            row['asof_class']='LIVE_SHADOW'
            store(db,row)
            changed+=1
            continue
        published = row["source_published_at_utc"]
        received = row["first_received_at_utc"]
        if published is None:
            continue
        if isinstance(published, str):
            published = datetime.fromisoformat(published)
        if isinstance(received, str):
            received = datetime.fromisoformat(received)
        if 0 <= (received - published).total_seconds() <= 300:
            continue
        row["asof_class"] = "HISTORICAL_ARCHIVE"
        row["last_seen_at_utc"] = now
        store(db, row)
        changed += 1
    return changed


def collect_mid(db, now: datetime) -> dict:
    body, meta = conditional_fetch(db, "ECB_MID", "rss:latest", MID_URL)
    feed_sha, feed_path = preserve("ECB_MID", body)
    root = ET.fromstring(body)
    items = root.findall(".//item")
    feed_meta = meta
    store(db, base_row("ECB_MID", "rss:latest", "FEED", MID_URL, body, feed_meta,
                       now, None, "MID_STRUCTURED", "LIVE_SHADOW",
                       title="ECB MID RSS snapshot", reference_timezone="UTC",
                       extra_json={"entry_count": len(items), "raw_blob_path": feed_path}))
    if meta.get("status") == 304:
        reclassified = reclassify_delayed_mid_rows(db, now)
        return {"source": "ECB_MID", "feed_status": meta["status"], "feed_sha256": feed_sha,
                "feed_bytes": meta.get("network_response_bytes", 0), "entry_count": len(items),
                "release_docs_saved": 0, "decision_docs": 0,
                "reclassified_delayed_rows": reclassified, "status": "UNCHANGED_304",
                "rss_timezone_explicit": all(
                    parse_explicit_datetime(xml_text(i, "pubDate")) is not None for i in items)}
    stored = 0
    decisions = 0
    for item in items:
        title = xml_text(item, "title") or ""
        link = xml_text(item, "link") or ""
        published_text = xml_text(item, "pubDate")
        published = parse_explicit_datetime(published_text)
        if not link.startswith("https://mid.ecb.europa.eu/rel/"):
            continue
        time.sleep(0.15)
        try:
            xml, doc_meta = conditional_fetch(db, "ECB_MID", "mid:" + urllib.parse.urlparse(link).path.rsplit("/", 1)[-1],
                                               link, timeout=18, cap=3_000_000)
        except urllib.error.HTTPError as error:
            empty = b""
            doc_meta = {"status": error.code, "content_type": error.headers.get("Content-Type"), "url": link}
            key = "item:" + hashlib.sha256(link.encode()).hexdigest()
            store(db, base_row("ECB_MID", key, "HTTP_ERROR", link, empty, doc_meta, now, published,
                               "MID_STRUCTURED" if published else "UNKNOWN", "UNUSABLE", title=title,
                               reference_timezone="UTC" if published else None,
                               extra_json={"source_pubdate": published_text, "error": "HTTP_ERROR"}))
            stored += 1
            continue
        doc_root = ET.fromstring(xml)
        release_type = xml_text(doc_root, "releaseType") or xml_text(doc_root, "releaseTypeCode")
        release_time_text = xml_text(doc_root, "releaseDateTime")
        publication_time_text = xml_text(doc_root, "publicationDateTime")
        release_time = parse_explicit_datetime(release_time_text)
        publication_time = parse_explicit_datetime(publication_time_text) or published
        is_decision = release_type == "PressReleaseMonPolDecision"
        received_time = parse_explicit_datetime(doc_meta.get("received_at_utc"))
        if received_time is None:
            received_time = now
        published_time = publication_time
        live_age = ((received_time - published_time).total_seconds()
                    if published_time is not None else None)
        item_asof = ("CERTIFIED_LIVE" if live_age is not None and 0 <= live_age <= 300
                     else "HISTORICAL_ARCHIVE")
        key = "mid:" + urllib.parse.urlparse(link).path.rsplit("/", 1)[-1]
        if is_decision:
            try:
                import model100_primary_mid_recovery as mid_recovery
            except ImportError:
                from work import model100_primary_mid_recovery as mid_recovery
            mid_recovery.store_decision(db,link,xml,doc_meta,now,
                                        publication_hint=publication_time,asof=item_asof)
            stored+=1;decisions+=1
            continue
        store(db, base_row("ECB_MID", key, "RELEASE", link, xml, doc_meta, now, publication_time,
                           "MID_STRUCTURED" if publication_time else "UNKNOWN",
                           item_asof, title=title, event_type=release_type,
                           region="EA", reference_timezone="UTC" if publication_time else None,
                           extra_json={"source_pubdate": published_text,
                                       "releaseDateTime_raw": release_time_text,
                                       "publicationDateTime_raw": publication_time_text,
                                       "releaseDateTime_utc": release_time.isoformat() if release_time else None,
                                       "publicationDateTime_utc": publication_time.isoformat() if publication_time else None,
                                       "decision_event": is_decision}))
        stored += 1
        decisions += int(is_decision)
    return {"source": "ECB_MID", "feed_status": meta["status"], "feed_sha256": feed_sha,
            "feed_bytes": len(body), "entry_count": len(items), "release_docs_saved": stored,
            "decision_docs": decisions, "rss_timezone_explicit": all(
                parse_explicit_datetime(xml_text(i, "pubDate")) is not None for i in items)}


def col_index(ref: str) -> int:
    letters = re.match(r"[A-Z]+", ref).group(0)
    result = 0
    for char in letters:
        result = result * 26 + ord(char) - 64
    return result - 1


def excel_sheets(zf: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {r.attrib["Id"]: r.attrib["Target"] for r in rels}
    result = []
    for sheet in workbook.findall("m:sheets/m:sheet", NS):
        target = rel_map[sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]]
        result.append((sheet.attrib["name"], "xl/" + target.lstrip("/")))
    return result


def excel_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    return ["".join(t.text or "" for t in item.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"))
            for item in ET.fromstring(zf.read("xl/sharedStrings.xml"))]


def excel_cell_value(cell: ET.Element, strings: list[str]) -> str:
    value = cell.find("m:v", NS)
    text = value.text if value is not None and value.text else ""
    if cell.get("t") == "s" and text:
        return strings[int(text)]
    return text


def excel_serial_date(value: str) -> date:
    return (datetime(1899, 12, 30) + timedelta(days=float(value))).date()


def quarter_start(quarter_end: date) -> date:
    month = ((quarter_end.month - 1) // 3) * 3 + 1
    return date(quarter_end.year, month, 1)


def parse_ifo_rows(body: bytes, asof_date: date | None = None):
    asof_date=asof_date or datetime.now(UTC).date()
    zf = zipfile.ZipFile(io.BytesIO(body))
    strings = excel_shared_strings(zf)
    rows = []
    for sheet_name, path in excel_sheets(zf):
        root = ET.fromstring(zf.read(path))
        sheet_rows = root.findall("m:sheetData/m:row", NS)
        if not sheet_rows:
            continue
        headers = {}
        for cell in sheet_rows[0].findall("m:c", NS):
            text = excel_cell_value(cell, strings)
            if not text:
                continue
            value = text
            headers[col_index(cell.get("r", "A1"))] = value
        vintages = {i: value for i, value in headers.items() if re.fullmatch(r"v\d{4}m\d{2}", value)
                    and int(value[1:5]) >= 2025 and 1<=int(value[6:8])<=12
                    and (int(value[1:5]),int(value[6:8]))<=(asof_date.year,asof_date.month)}
        for row in sheet_rows[1:]:
            values = {}
            for cell in row.findall("m:c", NS):
                raw = excel_cell_value(cell, strings)
                if not raw:
                    continue
                values[col_index(cell.get("r", "A1"))] = raw
            period = values.get(0, "")
            match = re.fullmatch(r"([A-Za-z]+)\s+(\d{4})", period)
            if not match:
                continue
            month_names = {"january": 1, "february": 2, "march": 3, "april": 4,
                           "may": 5, "june": 6, "july": 7, "august": 8,
                           "september": 9, "october": 10, "november": 11, "december": 12}
            month = month_names.get(match.group(1).lower())
            if not month:
                continue
            period_date = f"{int(match.group(2)):04d}-{month:02d}-01"
            for col, vintage in vintages.items():
                if period_date>f'{int(vintage[1:5]):04d}-{int(vintage[6:8]):02d}-01':
                    continue
                value = values.get(col)
                if value in (None, ""):
                    continue
                rows.append((sheet_name, period_date, vintage,
                             int(vintage[1:5]), int(vintage[6:8]), float(value)))
    return rows


def numeric_version_count(db, source, kind):
    """Count economic value versions, not raw-document byte revisions."""
    cur = db.cursor()
    try:
        cur.execute(f"SELECT COUNT(DISTINCT record_key,numeric_value) FROM `{TABLE}` WHERE source_id=%s AND record_kind=%s AND numeric_value IS NOT NULL AND asof_class<>'UNUSABLE'", (source, kind))
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        cur.close()


def collect_ifo(db, now: datetime) -> dict:
    before = numeric_version_count(db, "IFO_VINTAGE", "VINTAGE_VALUE")
    body, meta = conditional_fetch(db, "IFO_VINTAGE", "workbook:latest", IFO_URL,
                                   timeout=30, cap=5_000_000)
    digest, path = preserve("IFO_VINTAGE", body)
    store(db, base_row("IFO_VINTAGE", "workbook:latest", "ORIGINAL_XLSX", IFO_URL, body, meta,
                       now, None, "DAY_ONLY", "DAY_ONLY", title="Germany ifo vintage workbook",
                       reference_timezone="Europe/Berlin",
                       extra_json={"raw_blob_path": path, "observation_definition": "reference month; vintage column is publication month"}))
    rows = parse_ifo_rows(body,now.date())
    for sheet, period, vintage, year, month, value in rows:
        store(db, base_row("IFO_VINTAGE", f"{sheet}:{period}:{vintage}", "VINTAGE_VALUE", IFO_URL,
                           body, meta, now, None, "DAY_ONLY", "DAY_ONLY",
                           title=f"ifo {sheet} vintage {vintage}", event_type="BUSINESS_CLIMATE",
                           region="DE", observation_period_start=period,
                           vintage_month=f"{year:04d}-{month:02d}-01", numeric_value=value,
                           unit="INDEX", reference_timezone="Europe/Berlin",
                           extra_json={"series": sheet, "vintage_code": vintage,
                                       "release_date_status": "UNMATCHED_DAY_ONLY"}))
    new_values = max(0, numeric_version_count(db, "IFO_VINTAGE", "VINTAGE_VALUE") - before)
    vintage_codes = sorted({r[2] for r in rows})
    periods = sorted({r[1] for r in rows})
    return {"source": "IFO_VINTAGE", "status": "SUCCESS_NEW_DATA" if new_values else "SUCCESS_UNCHANGED",
            "new_value_versions": new_values, "http_status": meta["status"], "content_type": meta["content_type"],
            "bytes": len(body), "sha256": digest, "vintage_codes": vintage_codes,
            "reference_period_start": periods[0] if periods else None,
            "reference_period_end": periods[-1] if periods else None, "value_rows": len(rows),
            "asof_class": "DAY_ONLY"}


def parse_gdpnow_archives(body: bytes, start_date: date, end_date: date):
    zf = zipfile.ZipFile(io.BytesIO(body))
    strings = excel_shared_strings(zf)
    sheets = dict(excel_sheets(zf))
    if "TrackingArchives" not in sheets:
        raise ValueError("GDP_NOW_TRACKING_ARCHIVES_MISSING")
    root = ET.fromstring(zf.read(sheets["TrackingArchives"]))
    rows = root.findall("m:sheetData/m:row", NS)
    if len(rows) < 2:
        raise ValueError("GDP_NOW_ARCHIVE_EMPTY")
    headers = {}
    for cell in rows[0].findall("m:c", NS):
        value = excel_cell_value(cell, strings)
        if value:
            headers[col_index(cell.get("r", "A1"))] = value
    by_name = {v: k for k, v in headers.items()}
    if "Forecast Date" not in by_name or "Quarter being forecasted" not in by_name or "GDP Nowcast" not in by_name:
        raise ValueError("GDP_NOW_ARCHIVE_REQUIRED_COLUMNS_MISSING")
    output = []
    for row in rows[1:]:
        cells = {col_index(cell.get("r", "A1")): excel_cell_value(cell, strings)
                 for cell in row.findall("m:c", NS)}
        forecast_raw = cells.get(by_name["Forecast Date"], "")
        quarter_raw = cells.get(by_name["Quarter being forecasted"], "")
        value_raw = cells.get(by_name["GDP Nowcast"], "")
        if not forecast_raw or not quarter_raw or not value_raw or value_raw.startswith("#"):
            continue
        forecast_date = excel_serial_date(forecast_raw)
        if forecast_date < start_date or forecast_date > end_date:
            continue
        quarter_date = excel_serial_date(quarter_raw)
        output.append((forecast_date, quarter_date, float(value_raw)))
    output.sort()
    if len({r[0] for r in output}) != len(output):
        raise ValueError("GDP_NOW_DUPLICATE_FORECAST_DATE")
    return output


def parse_gdpnow_current(body: bytes, start_date: date, end_date: date):
    zf=zipfile.ZipFile(io.BytesIO(body));strings=excel_shared_strings(zf);sheets=dict(excel_sheets(zf))
    if 'CurrentQtrEvolution' not in sheets:return []
    rows=ET.fromstring(zf.read(sheets['CurrentQtrEvolution'])).findall('m:sheetData/m:row',NS)
    cells=[{col_index(c.get('r','A1')):excel_cell_value(c,strings) for c in r.findall('m:c',NS)} for r in rows]
    if not cells or cells[0].get(0)!='Date' or not cells[0].get(2,'').startswith('GDP'):raise ValueError('GDP_NOW_CURRENT_HEADER')
    label=next((r.get(1,'') for r in cells if 'Initial GDPNow' in r.get(1,'')), '')
    match=re.search(r'Initial GDPNow (20\d{2}|\d{2}):Q([1-4]) forecast',label)
    if not match:raise ValueError('GDP_NOW_CURRENT_TARGET_QUARTER_UNKNOWN')
    year=int(match[1]);year=year if year>=2000 else 2000+year;quarter=date(year,3*int(match[2])-2,1)
    result=[]
    for r in cells[1:]:
        if not r.get(0) or not r.get(2):continue
        day=excel_serial_date(r[0]);value=float(r[2])
        if not start_date<=day<=end_date:continue
        if not math.isfinite(value) or abs(value)>100:raise ValueError('GDP_NOW_CURRENT_VALUE_RANGE')
        result.append((day,quarter,value))
    if len({r[0] for r in result})!=len(result):raise ValueError('GDP_NOW_CURRENT_DUPLICATE_DAY')
    return sorted(result)


def collect_gdpnow(db, now: datetime) -> dict:
    before = numeric_version_count(db, "GDP_NOW_ATL", "FORECAST_VINTAGE")
    network_fallback = None
    try:
        body, meta = conditional_fetch(db, "GDP_NOW_ATL", "trackingarchives:latest", GDP_NOW_URL,
                                       large=True, timeout=20, cap=15_000_000)
    except urllib.error.HTTPError:
        raise
    except (TimeoutError, urllib.error.URLError, subprocess.SubprocessError) as error:
        if not GDP_NOW_CACHE.is_file():
            raise
        network_fallback = {"status": "NO_HTTP_STATUS", "error": type(error).__name__ + ":" + str(error)[:200]}
        body = GDP_NOW_CACHE.read_bytes()
        meta = {"status": 200, "original_http_status": 200, "fetch_status": "CACHED_AFTER_NETWORK_ERROR",
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "etag": None, "last_modified": None, "url": GDP_NOW_URL,
                "fetch_error": network_fallback["error"]}
        payload = json.dumps({"source": "GDP_NOW_ATL", "url": GDP_NOW_URL,
                              "observed_at_utc": now.isoformat(), **network_fallback},
                             sort_keys=True).encode("utf-8")
        status_sha, status_path = preserve("GDP_NOW_ATL", payload)
        status_row = base_row("GDP_NOW_ATL", "fetch-status:trackingarchives:latest:network",
                              "FETCH_STATUS", GDP_NOW_URL, payload,
                              {"status": 0, "content_type": "application/json", "url": GDP_NOW_URL},
                              now, None, "OBSERVED", "FETCH_STATUS",
                              title="GDPNow workbook network fetch fallback",
                              event_type="FETCH_STATUS", extra_json=network_fallback)
        status_row.update(content_sha256=status_sha, raw_blob_path=status_path,
                          parser_version=PARSER_VERSION)
        store(db, status_row)
    # Validate before replacing the last good cache or admitting an original.
    with zipfile.ZipFile(io.BytesIO(body)) as workbook:
        if workbook.testzip():
            raise ValueError('GDP_NOW_WORKBOOK_CRC_FAILURE')
    archived=parse_gdpnow_archives(body,date(2025,1,1),now.date())
    current=parse_gdpnow_current(body,date(2025,1,1),now.date())
    merged={r[0]:r for r in archived}
    for r in current:
        if r[0] in merged and (merged[r[0]][1]!=r[1] or not math.isclose(merged[r[0]][2],r[2],abs_tol=1e-10,rel_tol=0)):raise ValueError('GDP_NOW_CURRENT_ARCHIVE_CONFLICT')
        merged[r[0]]=r
    parsed=sorted(merged.values())
    if not parsed:
        raise ValueError('GDP_NOW_NO_VALIDATED_FORECASTS')
    if network_fallback is None:
        if meta["status"] != 304 and meta.get('fetch_status') != 'HEAD_VALIDATED_CACHE':
            GDP_NOW_CACHE.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
            temp_cache = GDP_NOW_CACHE.with_suffix(".part")
            with temp_cache.open("wb") as stream:
                stream.write(body); stream.flush(); os.fsync(stream.fileno())
            os.chmod(temp_cache, 0o640); os.replace(temp_cache, GDP_NOW_CACHE)
    digest, path = preserve("GDP_NOW_ATL", body)
    store(db, base_row("GDP_NOW_ATL", "trackingarchives:latest", "ORIGINAL_XLSX", GDP_NOW_URL,
                       body, meta, now, None, "DAY_ONLY", "DAY_ONLY",
                       title="Atlanta Fed GDPNow TrackingArchives workbook", reference_timezone="America/New_York",
                       extra_json={"raw_blob_path": path, "used_sheet": "TrackingArchives+CurrentQtrEvolution",
                                   "excluded_sheets": ["TrackRecord"], "date_range_start": "2025-01-01"}))
    previous_by_quarter = {}
    for forecast_date, quarter_date, value in parsed:
        prior = previous_by_quarter.get(quarter_date)
        store(db, base_row("GDP_NOW_ATL", "GDPNow:" + forecast_date.isoformat(), "FORECAST_VINTAGE",
                           GDP_NOW_URL, body, meta, now, None, "DAY_ONLY", "DAY_ONLY",
                           title="Atlanta Fed GDPNow forecast", event_type="GDP_NOWCAST", region="US",
                           observation_period_start=quarter_start(quarter_date).isoformat(),
                           vintage_month=forecast_date.replace(day=1).isoformat(), numeric_value=value,
                           unit="PERCENT_SAAR", reference_timezone="America/New_York",
                           extra_json={"forecast_date": forecast_date.isoformat(),
                                       "change_from_prior_same_quarter": round(value - prior, 8) if prior is not None else None,
                                       "target_quarter_end": quarter_date.isoformat(),
                                       "forecast_date_is_not_release_time": True}))
        previous_by_quarter[quarter_date] = value
    new_values = max(0, numeric_version_count(db, "GDP_NOW_ATL", "FORECAST_VINTAGE") - before)
    return {"source": "GDP_NOW_ATL", "status": "NETWORK_TIMEOUT" if network_fallback else "SUCCESS_NEW_DATA" if new_values else "SUCCESS_UNCHANGED",
            "new_value_versions": new_values, "http_status": meta["status"], "content_type": meta["content_type"],
            "bytes": len(body), "sha256": digest, "forecast_rows": len(parsed),
            "fetch_status": meta.get("fetch_status", "HTTP_" + str(meta["status"])),
            "network_status": network_fallback["status"] if network_fallback else meta["status"],
            "network_error": network_fallback["error"] if network_fallback else None,
            "forecast_date_start": parsed[0][0].isoformat() if parsed else None,
            "forecast_date_end": parsed[-1][0].isoformat() if parsed else None,
            "target_quarter_start": quarter_start(parsed[0][1]).isoformat() if parsed else None,
            "target_quarter_end": parsed[-1][1].isoformat() if parsed else None,
            "asof_class": "DAY_ONLY", "trackrecord_excluded": True}


def _consume_force_archive_once(db, nonce: str, now: datetime) -> None:
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise ValueError("FORCE_ARCHIVE_NONCE_INVALID")
    key = "control:force-archive-once:" + nonce
    cur = db.cursor()
    cur.execute(f"SELECT content_sha256 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s LIMIT 1",
                ("MODEL100_CONTROL", key))
    prior = cur.fetchone()
    cur.close()
    if prior:
        raise ValueError("FORCE_ARCHIVE_NONCE_ALREADY_USED")
    payload = json.dumps({"action": "force-archive-once", "nonce": nonce,
                          "sources": ["BEA_RELEASES", "EUROSTAT_FLASH"],
                          "requested_at_utc": now.isoformat()}, sort_keys=True).encode()
    digest, path = preserve("MODEL100_CONTROL", payload)
    row = base_row("MODEL100_CONTROL", key, "RUN_CONTROL",
                   "brain://model100/archive-force-control", payload,
                   {"status": 0, "content_type": "application/json",
                    "fetch_status": "INTERNAL_CONTROL", "url": "brain://model100/archive-force-control"},
                   now, None, "OBSERVED", "RUN_CONTROL", title="One-time archive pass control",
                   event_type="FORCE_ARCHIVE_ONCE", extra_json={"nonce": nonce})
    row.update(content_sha256=digest, raw_blob_path=path, parser_version=PARSER_VERSION)
    store(db, row)


def _consume_bls_status_seed_once(db, nonce: str, now: datetime) -> None:
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise ValueError("STATUS_SEED_NONCE_INVALID")
    key = "control:known-not-published-once:BLS_RELEASES:" + nonce
    cur = db.cursor()
    cur.execute(f"SELECT content_sha256 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s LIMIT 1",
                ("MODEL100_CONTROL", key))
    prior = cur.fetchone()
    cur.close()
    if prior:
        raise ValueError("STATUS_SEED_NONCE_ALREADY_USED")
    payload = json.dumps({"action": "seed-known-not-published-once",
                          "source": "BLS_RELEASES", "nonce": nonce,
                          "requested_at_utc": now.isoformat()}, sort_keys=True).encode()
    digest, path = preserve("MODEL100_CONTROL", payload)
    row = base_row("MODEL100_CONTROL", key, "RUN_CONTROL",
                   "brain://model100/known-not-published-seed", payload,
                   {"status": 0, "content_type": "application/json",
                    "fetch_status": "INTERNAL_CONTROL", "url": "brain://model100/known-not-published-seed"},
                   now, None, "OBSERVED", "RUN_CONTROL",
                   title="One-time BLS nonpublication status seed",
                   event_type="STATUS_SEED_ONCE", extra_json={"nonce": nonce})
    row.update(content_sha256=digest, raw_blob_path=path, parser_version=PARSER_VERSION)
    store(db, row)


def _consume_source_force_once(db, source_id: str, nonce: str, now: datetime) -> None:
    if source_id not in SOURCE_PRIORITY or source_id == "ECB_MID":
        raise ValueError("FORCE_SOURCE_NOT_ALLOWLISTED")
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise ValueError("FORCE_SOURCE_NONCE_INVALID")
    key = f"control:force-source-once:{source_id}:{nonce}"
    cur = db.cursor()
    cur.execute(f"SELECT content_sha256 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s LIMIT 1",
                ("MODEL100_CONTROL", key))
    prior = cur.fetchone()
    cur.close()
    if prior:
        raise ValueError("FORCE_SOURCE_NONCE_ALREADY_USED")
    payload = json.dumps({"action": "force-source-once", "source": source_id,
                          "nonce": nonce, "requested_at_utc": now.isoformat()}, sort_keys=True).encode()
    digest, path = preserve("MODEL100_CONTROL", payload)
    url = "brain://model100/source-force-control"
    row = base_row("MODEL100_CONTROL", key, "RUN_CONTROL", url, payload,
                   {"status": 0, "content_type": "application/json",
                    "fetch_status": "INTERNAL_CONTROL", "url": url},
                   now, None, "OBSERVED", "RUN_CONTROL", title="One-time source run control",
                   event_type="FORCE_SOURCE_ONCE", extra_json={"source": source_id,
                                                               "nonce": nonce,
                                                               "http_attempted": False})
    row.update(content_sha256=digest, raw_blob_path=path, parser_version=PARSER_VERSION)
    store(db, row)


def _schedule_state(db, source_id: str, now: datetime, interval_seconds: int,
                    outcome: str = "PARTIAL_COVERAGE", schedule_id: str | None = None, execution_mode: str = "NORMAL") -> datetime | None:
    """Read/update a stable per-source scheduler row in the canonical B1 table."""
    schedule_id = schedule_id or source_id
    if outcome in ('BACKFILL_INCOMPLETE','RATE_LIMITED'):
        interval_seconds = min(interval_seconds, 300)
    key = "control:source-schedule:" + schedule_id
    cur = db.cursor()
    cur.execute(f"SELECT last_seen_at_utc,extra_json FROM `{TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",
                (source_id, key))
    result = cur.fetchone()
    last_seen = result[0] if result else None
    cur.close()
    prior_state=(result[1] if isinstance(result[1],dict) else json.loads(result[1] or '{}')) if result and len(result)>1 else {}
    failures=int(prior_state.get('consecutive_network_failures',0))+1 if outcome=='NETWORK_TIMEOUT' else 0
    if failures:
        interval_seconds=min(86400,900*2**min(failures-1,7))
    payload = json.dumps({"source_id": source_id, "schedule_id": schedule_id,
                          "kind": "source-schedule-state"},
                         sort_keys=True).encode("utf-8")
    digest, path = preserve(source_id, payload)
    meta = {"status": 0, "content_type": "application/json",
            "fetch_status": "INTERNAL_CONTROL",
            "url": "brain://model100/source-scheduler"}
    row = base_row(source_id, key, "RUN_CONTROL", meta["url"], payload, meta,
                   now, None, "OBSERVED", "RUN_CONTROL", title="Source scheduler state",
                   event_type="SOURCE_SCHEDULE", extra_json={
                       "interval_seconds": interval_seconds, "last_outcome": outcome,
                       "last_check_started_at_utc":now.replace(tzinfo=UTC).isoformat(),
                       "execution_mode":execution_mode,
                       "normal_recovery_verification_due":execution_mode in ('CONTROLLED_SOURCE','CONTROLLED_ARCHIVE') and outcome in ('SUCCESS_NEW_DATA','SUCCESS_UNCHANGED') and prior_state.get('last_outcome') in ('NETWORK_TIMEOUT','BLOCKED_ACCESS','PARSE_ERROR'),
                       "last_normal_check_utc":now.isoformat() if execution_mode=="NORMAL" else prior_state.get("last_normal_check_utc"),
                       "last_controlled_check_utc":now.isoformat() if execution_mode!="NORMAL" else prior_state.get("last_controlled_check_utc"),
                       "last_successful_check_utc":now.isoformat() if outcome in ('SUCCESS_NEW_DATA','SUCCESS_UNCHANGED') else prior_state.get('last_successful_check_utc'),
                       "last_unchanged_check_utc":now.isoformat() if outcome=='SUCCESS_UNCHANGED' else prior_state.get('last_unchanged_check_utc'),
                       "last_new_data_utc":now.isoformat() if outcome=='SUCCESS_NEW_DATA' else prior_state.get('last_new_data_utc'),
                       "consecutive_network_failures":failures,
                       "schedule_id": schedule_id})
    row.update(content_sha256=digest, raw_blob_path=path, parser_version=PARSER_VERSION)
    store(db, row)
    return last_seen


def _schedule_last(db, source_id: str, schedule_id: str | None = None):
    cur = db.cursor()
    try:
        cur.execute(f"SELECT last_seen_at_utc,extra_json FROM `{TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",
                    (source_id, "control:source-schedule:" + (schedule_id or source_id)))
        result = cur.fetchone()
        if result and len(result)>1:
            metadata=result[1] if isinstance(result[1],dict) else json.loads(result[1] or '{}')
            precise=parse_explicit_datetime(metadata.get('last_check_started_at_utc'))
            if precise:return precise
        return result[0] if result else None
    finally:
        cur.close()


def _exception_outcome(error) -> str:
    if isinstance(error, urllib.error.HTTPError):
        return ('RATE_LIMITED' if error.code == 429 else
                'BLOCKED_ACCESS' if error.code in (401, 403, 407) else
                'PARTIAL_COVERAGE')
    if isinstance(error, (TimeoutError, urllib.error.URLError, OSError)):
        return 'NETWORK_TIMEOUT'
    return 'PARSE_ERROR'


def _collector_outcome(result) -> str:
    if not isinstance(result, dict):
        return "PARSE_ERROR"
    explicit = result.get('status') or result.get('classification')
    if explicit in {'SUCCESS_NEW_DATA', 'SUCCESS_UNCHANGED', 'PARTIAL_COVERAGE',
                    'RATE_LIMITED', 'BLOCKED_ACCESS', 'REQUIRES_API_KEY',
                    'BLOCKED_LICENSE', 'NETWORK_TIMEOUT', 'PARSE_ERROR', 'BACKFILL_INCOMPLETE'}:
        return explicit
    if explicit == 'UNCHANGED_304' or result.get('fetch_status') == 'HTTP_304_CACHE':
        return 'SUCCESS_UNCHANGED'
    if result.get("rate_limited"):
        return "RATE_LIMITED"
    if result.get("blocked"):
        return "BLOCKED_ACCESS"
    if result.get("fetch_status") == "CACHED_AFTER_NETWORK_ERROR":
        return "NETWORK_TIMEOUT"
    if result.get('archive_error') and not (result.get('current_page') or {}).get('forecast_rows'):
        if result.get('archive_http_status') in (401,403,407):return 'BLOCKED_ACCESS'
        if result.get('archive_http_status')==429:return 'RATE_LIMITED'
        if 'timeout' in result['archive_error'].lower() or 'urlerror' in result['archive_error'].lower():
            return 'NETWORK_TIMEOUT'
    if result.get("errors"):
        statuses = json.dumps(result['errors']).lower()
        if '429' in statuses or 'rate_limited' in statuses:
            return 'RATE_LIMITED'
        if '403' in statuses or 'blocked_access' in statuses:
            return 'BLOCKED_ACCESS'
        if 'timeout' in statuses or 'timed out' in statuses or 'urlerror' in statuses:
            return 'NETWORK_TIMEOUT'
        return 'PARTIAL_COVERAGE'
    return "PARTIAL_COVERAGE"


def _release_window_interval(source_id, now, interval):
    if source_id in ('GDP_NOW','CLEVELAND_NOW','NYFED_NOW','NYFED_SME'):
        from zoneinfo import ZoneInfo
        local=now.replace(tzinfo=UTC).astimezone(ZoneInfo('America/New_York'))
        # Polling heuristics only; never publication-time evidence.
        if local.weekday()<5:
            if source_id=='GDP_NOW' and (local.hour,local.minute)>=(8,25) and local.hour<17:
                return min(interval,300)
            if source_id=='CLEVELAND_NOW' and 9<=local.hour<13:
                return min(interval,3600)
            if source_id=='NYFED_NOW' and local.weekday()==4 and 11<=local.hour<14:
                return min(interval,900)
            if source_id=='NYFED_SME' and 14<=local.hour<17:
                return min(interval,3600)
    if source_id=='ZEW':
        from zoneinfo import ZoneInfo
        local=now.replace(tzinfo=UTC).astimezone(ZoneInfo('Europe/Berlin'))
        if local.weekday()<5 and (local.hour,local.minute)>=(10,45) and local.hour<12:
            return min(interval,300)
    if source_id == 'IFO_RELEASES':
        from zoneinfo import ZoneInfo
        local=now.replace(tzinfo=UTC).astimezone(ZoneInfo('Europe/Berlin'))
        if local.weekday()<5 and (local.hour,local.minute)>=(10,15) and local.hour<12:
            return min(interval,300)
    # Eurostat flash publishes at 11:00 Europe/Luxembourg. During that
    # window the completed collector refreshes only recent calendar months;
    # the archived-month checkpoint is never reset.
    if source_id == 'EUROSTAT_FLASH':
        from zoneinfo import ZoneInfo
        local = now.replace(tzinfo=UTC).astimezone(ZoneInfo('Europe/Luxembourg'))
        if local.weekday() < 5 and (local.hour, local.minute) >= (10, 55) and local.hour < 13:
            return min(interval, 300)
    if source_id in ('ALFRED','BLS_RELEASES'):
        from zoneinfo import ZoneInfo
        local = now.replace(tzinfo=UTC).astimezone(ZoneInfo('America/New_York'))
        if local.weekday() < 5 and (local.hour, local.minute) >= (8, 10) and local.hour < 11:
            # Only vintage-date lists are fetched when their SHA is unchanged.
            # Changed series alone enter the bounded two-series backfill lane.
            return min(interval, 300)
    return interval


def _due_sources(db, now: datetime, force_source: str | None = None):
    try:
        import model100_primary_archive as archive
        import model100_primary_p1 as p1
        import model100_primary_alfred as alfred
        import model100_primary_bls_fraser as bls_fraser
        import model100_primary_fed_speeches as fed_speeches
        import model100_primary_ifo_releases as ifo_releases
        import model100_primary_ecb_mirrors as ecb_mirrors
        import model100_primary_spf as spf
        import model100_primary_leading_archives as leading_archives
        import model100_primary_zew as zew
        import model100_primary_sme as sme
    except ImportError:
        from work import model100_primary_archive as archive
        from work import model100_primary_p1 as p1
        from work import model100_primary_alfred as alfred
        from work import model100_primary_bls_fraser as bls_fraser
        from work import model100_primary_fed_speeches as fed_speeches
        from work import model100_primary_ifo_releases as ifo_releases
        from work import model100_primary_ecb_mirrors as ecb_mirrors
        from work import model100_primary_spf as spf
        from work import model100_primary_leading_archives as leading_archives
        from work import model100_primary_zew as zew
        from work import model100_primary_sme as sme
    callbacks = {
        "GDP_NOW": collect_gdpnow,
        "IFO_VINTAGE": collect_ifo,
        "IFO_RELEASES": ifo_releases.collect,
        "ECB_HIST": ecb_mirrors.collect_decisions_live,
        "ECB_SPF": spf.collect,
        "FED_FOMC": archive.collect_fed_fomc,
        "FED_SPEECHES_ARCHIVE": fed_speeches.collect_archive,
        "FED_POLICY_ARCHIVE": lambda db,at:fed_speeches.collect_archive(db,at,policy=True),
        "BLS_RELEASES": bls_fraser.collect,
        "BEA_RELEASES": archive.collect_bea,
        "EUROSTAT_FLASH": archive.collect_eurostat_flash,
        "DEST_ATIS_CPI": archive.collect_destatis_preliminary,
        "ALFRED": alfred.collect,
        "ADP_NER": lambda db, at: alfred.collect(db, at, adp_only=True),
        "ZEW": lambda db, at: zew.collect(db, at),
        "NYFED_SME": lambda db, at: sme.collect(db, at),
        "SP_GLOBAL_FLASH_PMI": lambda db, at: p1.collect_source(db, "SP_GLOBAL_FLASH_PMI", at),
        "ISM_PMI": lambda db, at: p1.collect_source(db, "ISM_PMI", at),
        "CLEVELAND_NOW": lambda db, at: leading_archives.collect(db, "CLEVELAND_NOW", at),
        "CENSUS_RETAIL": lambda db, at: p1.collect_source(db, "CENSUS_RETAIL", at),
        "ECB_EDP_VINTAGE": lambda db, at: p1.collect_source(db, "ECB_EDP_VINTAGE", at),
        "WHITEHOUSE_TRADE": lambda db, at: p1.collect_source(db, "WHITEHOUSE_TRADE", at),
        "USTR_TRADE": lambda db, at: p1.collect_source(db, "USTR_TRADE", at),
        "NYFED_NOW": lambda db, at: leading_archives.collect(db, "NYFED_NOW", at),
    }
    if force_source:
        if force_source not in callbacks:
            raise ValueError("FORCE_SOURCE_NOT_ALLOWLISTED")
        return [(datetime.min, force_source, SOURCE_INTERVALS_SECONDS[force_source],
                 callbacks[force_source])]
    due = []
    for source_id, callback in callbacks.items():
        interval = SOURCE_INTERVALS_SECONDS[source_id]
        state={}
        key = "control:source-schedule:" + source_id
        cur = db.cursor()
        cur.execute(f"SELECT last_seen_at_utc,extra_json FROM `{TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",
                    (source_id, key))
        result = cur.fetchone()
        last_seen = result[0] if result else None
        if result and len(result) > 1:
            state = result[1] if isinstance(result[1], dict) else json.loads(result[1] or '{}')
            if state.get('last_outcome') == 'NETWORK_TIMEOUT':
                interval=max(60,int(state.get('interval_seconds',interval)))
            elif state.get('last_outcome') in ('BACKFILL_INCOMPLETE','RATE_LIMITED'):
                interval = min(interval, max(60, int(state.get('interval_seconds', interval))))
        controlled = state.get('execution_mode') in ('CONTROLLED_SOURCE', 'CONTROLLED_ARCHIVE')
        if controlled:
            # A diagnostic one-shot must not postpone the normal source tick.
            prior_normal = state.get('last_normal_check_utc')
            last_seen = datetime.fromisoformat(prior_normal).replace(tzinfo=None) if prior_normal else None
            if state.get('normal_recovery_verification_due'):
                last_seen = None
        if last_seen is None and not controlled:
            # Adopt existing per-source fetch/document evidence as the initial
            # checkpoint so deployment does not immediately repeat today's
            # archive crawl or re-hit a source that just returned 403/429.
            source_data_id = SOURCE_DATA_IDS.get(source_id, source_id)
            cur.execute(f"SELECT MAX(last_seen_at_utc) FROM `{TABLE}` WHERE source_id=%s",
                        (source_data_id,))
            result = cur.fetchone()
            last_seen = result[0] if result else None
        cur.close()
        if state.get('last_outcome')!='NETWORK_TIMEOUT':
            interval = _release_window_interval(source_id, now, interval)
        if last_seen is None or (now - last_seen).total_seconds() >= interval:
            due.append((last_seen or datetime.min, source_id, interval, callback))
    priority = {source_id: rank for rank, source_id in enumerate(SOURCE_PRIORITY)}
    due.sort(key=lambda item: (0 if item[1] == 'WHITEHOUSE_TRADE' or
                              (item[1] in ('EUROSTAT_FLASH','ALFRED') and
                              _release_window_interval(item[1], now, 86400) == 300) else 1,
                              item[0], priority.get(item[1], len(priority))))
    return due


def _source_poll_time(now, last, interval):
    """Absorb subsecond cron jitter without polling before the source interval."""
    if last is not None:
        remaining = interval - (now - last).total_seconds()
        if 0 < remaining < 1:
            time.sleep(remaining)
            return utcnow()
    return now


def _cold_source_has_poll_window(source_id, seconds_remaining):
    # Measured B1 cold runs: Cleveland~85s, NYFed~25s. Leave margin;
    # defer these runs rather than hold the writer across the next live poll.
    budget = {'CLEVELAND_NOW': 110, 'NYFED_NOW': 40, 'GDP_NOW': 60, 'ZEW':25, 'NYFED_SME':20}.get(source_id, 0)
    return seconds_remaining >= budget if budget else True


def run(db, table: str, run_mode: str | None = None) -> dict:
    if table != TABLE:
        raise ValueError("REGISTERED_TARGET_MISMATCH")
    force_match = re.fullmatch(r"force-archive-once:([0-9a-f]{32})", run_mode or "")
    seed_match = re.fullmatch(r"seed-known-not-published-once:([0-9a-f]{32})", run_mode or "")
    source_match = re.fullmatch(r"force-source-once:([A-Z0-9_]+):([0-9a-f]{32})", run_mode or "")
    if run_mode not in (None, "normal") and not force_match and not seed_match and not source_match:
        raise ValueError("PARSER_RUN_MODE_INVALID")
    ensure_schema(db)
    now = utcnow()
    if seed_match:
        nonce = seed_match.group(1)
        _consume_bls_status_seed_once(db, nonce, now)
        try:
            import model100_primary_archive as archive
        except ImportError:
            from work import model100_primary_archive as archive
        count = archive._store_bls_not_published(db, now)
        return {"table": table, "mode": "seed-known-not-published-once",
                "source": "BLS_RELEASES", "rows_stored": count,
                "nonce": nonce, "inserted_or_seen_at_utc": now.isoformat()}
    if source_match:
        source_id, nonce = source_match.groups()
        _consume_source_force_once(db, source_id, nonce, now)
        _, _, interval, collector = _due_sources(db, now, force_source=source_id)[0]
        errors = []
        try:
            result = collector(db, now)
            _schedule_state(db, source_id, now, interval, _collector_outcome(result), execution_mode="CONTROLLED_SOURCE")
            return {"table": table, "mode": "force-source-once", "source": source_id,
                    "nonce": nonce, "inserted_or_seen_at_utc": now.isoformat(),
                    "collector": result, "errors": errors}
        except urllib.error.HTTPError as error:
            errors.append({"collector": source_id, "http_status": error.code,
                           "status": _exception_outcome(error)})
            _schedule_state(db, source_id, now, interval, _exception_outcome(error), execution_mode="CONTROLLED_SOURCE")
        except Exception as error:
            errors.append({"collector": source_id,
                           "error": type(error).__name__ + ":" + str(error)[:200]})
            _schedule_state(db, source_id, now, interval, _exception_outcome(error), execution_mode="CONTROLLED_SOURCE")
        return {"table": table, "mode": "force-source-once", "source": source_id,
                "nonce": nonce, "inserted_or_seen_at_utc": now.isoformat(), "errors": errors}
    if force_match:
        nonce = force_match.group(1)
        _consume_force_archive_once(db, nonce, now)
        try:
            import model100_primary_archive as archive
        except ImportError:
            from work import model100_primary_archive as archive
        results, errors = [], []
        for collector in (archive.collect_bea, archive.collect_eurostat_flash):
            try:
                results.append(collector(db, now))
            except urllib.error.HTTPError as error:
                errors.append({"collector": collector.__name__, "http_status": error.code,
                               "status": _exception_outcome(error)})
            except Exception as error:
                errors.append({"collector": collector.__name__,
                               "error": type(error).__name__ + ":" + str(error)[:200]})
        return {"table": table, "mode": "force-archive-once", "nonce": nonce,
                "inserted_or_seen_at_utc": now.isoformat(), "collectors": results, "errors": errors}
    results, errors = [], []
    mid_last = _schedule_last(db, 'ECB_MID')
    now = _source_poll_time(now, mid_last, SOURCE_INTERVALS_SECONDS['ECB_MID'])
    next_mid_due = (now if mid_last is None or (now-mid_last).total_seconds()>=300 else mid_last) + timedelta(seconds=300)
    if mid_last is None or (now - mid_last).total_seconds() >= SOURCE_INTERVALS_SECONDS['ECB_MID']:
        try:
            mid_result = collect_mid(db, now)
            results.append(mid_result)
            _schedule_state(db, "ECB_MID", now, SOURCE_INTERVALS_SECONDS["ECB_MID"], _collector_outcome(mid_result))
        except urllib.error.HTTPError as error:
            errors.append({"collector": "collect_mid", "http_status": error.code,
                           "status": _exception_outcome(error)})
            _schedule_state(db, "ECB_MID", now, SOURCE_INTERVALS_SECONDS["ECB_MID"], _exception_outcome(error))
        except Exception as error:
            errors.append({"collector": "collect_mid", "error": type(error).__name__ + ":" + str(error)[:200]})
            _schedule_state(db, "ECB_MID", now, SOURCE_INTERVALS_SECONDS["ECB_MID"], _exception_outcome(error))
    else:
        results.append({'source': 'ECB_MID', 'status': 'NOT_DUE',
                        'next_check_at_utc': (mid_last + timedelta(seconds=300)).isoformat()})

    # Destatis RSS has its own precise interval/state; never run its history
    # crawler on this live lane.
    try:
        try:
            import model100_primary_archive as archive
        except ImportError:
            from work import model100_primary_archive as archive
        results.append(archive.collect_destatis_live(db, now))
    except Exception as error:
        errors.append({'collector': 'DEST_ATIS_LIVE', 'error': type(error).__name__ + ':' + str(error)[:200]})
        _schedule_state(db, 'DEST_ATIS_CPI', now, 300, _exception_outcome(error), schedule_id='DEST_ATIS_LIVE')

    # Poll the official Federal Reserve monetary RSS every five minutes. The
    # annual archive/backfill collector remains on its own daily schedule.
    live_last = _schedule_last(db,'FED_FOMC','FED_FOMC_LIVE')
    if live_last is None or (now - live_last).total_seconds() >= 300:
        try:
            import model100_primary_archive as archive
        except ImportError:
            from work import model100_primary_archive as archive
        try:
            live_result = archive.collect_fed_fomc_live(db, now)
            results.append(live_result)
            live_outcome = _collector_outcome(live_result)
            _schedule_state(db, "FED_FOMC", now, 300, live_outcome,
                            schedule_id="FED_FOMC_LIVE")
        except urllib.error.HTTPError as error:
            errors.append({"collector": "FED_FOMC_LIVE", "http_status": error.code,
                           "status": _exception_outcome(error)})
            _schedule_state(db, "FED_FOMC", now, 300, _exception_outcome(error),
                            schedule_id="FED_FOMC_LIVE")
        except Exception as error:
            errors.append({"collector": "FED_FOMC_LIVE",
                           "error": type(error).__name__ + ":" + str(error)[:200]})
            _schedule_state(db, "FED_FOMC", now, 300, _exception_outcome(error),
                            schedule_id="FED_FOMC_LIVE")

    # BEA's official RSS can update between archive crawls. Poll only the feed
    # on this five-minute lane; article originals are fetched only if its ETag
    # changes. The schedule/archive collector remains daily and checkpointed.
    bea_live_last = _schedule_last(db,'BEA_RELEASES','BEA_RELEASES_LIVE')
    if bea_live_last is None or (now - bea_live_last).total_seconds() >= 300:
        try:
            import model100_primary_archive as archive
        except ImportError:
            from work import model100_primary_archive as archive
        try:
            bea_live_result = archive.collect_bea_live(db, now)
            results.append(bea_live_result)
            bea_live_outcome = _collector_outcome(bea_live_result)
            _schedule_state(db, "BEA_RELEASES", now, 300, bea_live_outcome,
                            schedule_id="BEA_RELEASES_LIVE")
        except urllib.error.HTTPError as error:
            errors.append({"collector": "BEA_RELEASES_LIVE", "http_status": error.code,
                           "status": _exception_outcome(error)})
            _schedule_state(db, "BEA_RELEASES", now, 300, _exception_outcome(error),
                            schedule_id="BEA_RELEASES_LIVE")
        except Exception as error:
            errors.append({"collector": "BEA_RELEASES_LIVE",
                           "error": type(error).__name__ + ":" + str(error)[:200]})
            _schedule_state(db, "BEA_RELEASES", now, 300, _exception_outcome(error),
                            schedule_id="BEA_RELEASES_LIVE")

    due = _due_sources(db, now)
    eligible = []
    remaining = (next_mid_due - utcnow()).total_seconds()
    for item in due:
        if _cold_source_has_poll_window(item[1], remaining):
            eligible.append(item)
        else:
            results.append({'source': item[1], 'status': 'DEFERRED_FOR_LIVE_POLL',
                            'next_live_poll_at_utc': next_mid_due.isoformat(),
                            'http_attempted': False})
    due = eligible
    if due:
        _, source_id, interval, collector = due[0]
        try:
            result = collector(db, now)
            results.append(result)
            outcome = _collector_outcome(result)
            _schedule_state(db, source_id, now, interval, outcome, execution_mode="CONTROLLED_ARCHIVE" if force_match else "NORMAL")
        except urllib.error.HTTPError as error:
            errors.append({"collector": source_id, "http_status": error.code,
                           "status": _exception_outcome(error)})
            _schedule_state(db, source_id, now, interval, _exception_outcome(error))
        except Exception as error:
            errors.append({"collector": source_id, "error": type(error).__name__ + ":" + str(error)[:200]})
            _schedule_state(db, source_id, now, interval, _exception_outcome(error))
    else:
        results.append({"phase": "source_schedule", "status": "NO_SOURCE_DUE"})
    return {"table": table, "inserted_or_seen_at_utc": now.isoformat(), "collectors": results, "errors": errors}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) < 6:
        raise SystemExit("usage: model100_primary_sources.py TABLE HOST PORT USER PASSWORD DATABASE")
    table, host, port, user, password, database = argv[:6]
    run_mode = argv[6] if len(argv) >= 7 else None
    import mysql.connector
    db = mysql.connector.connect(host=host, port=int(port), user=user, password=password,
                                 database=database, connection_timeout=15, autocommit=False)
    try:
        result=run(db,table,run_mode)
        if run_mode in (None,'normal'):
            import model100_primary_mid_recovery as mid_recovery
            result['mid_recent_recovery']=mid_recovery.recover(db,utcnow())
            import model100_primary_fed_speeches as fed_speeches
            result['fed_speech_live']=fed_speeches.collect_live(db,utcnow())
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
