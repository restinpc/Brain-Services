#!/usr/bin/env python3
"""Official P1 Model100 source collectors, sharing the B1 primary-source store.

This module captures official index documents and linked original artifacts,
records access/licensing probes without attempting workarounds, and only emits
dated observations when the original artifact itself supplies the dates.
"""
from __future__ import annotations

import hashlib
import html
import csv
import io
import json
import math
import re
import time
import urllib.error
import urllib.parse
import zipfile
from decimal import Decimal, ROUND_HALF_UP
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path

try:
    import model100_primary_sources as common
    import model100_primary_archive as archive
except ImportError:
    from work import model100_primary_sources as common
    from work import model100_primary_archive as archive

TABLE = common.TABLE
VERSION = "model100-primary-p1-v10"
START = date(2025, 1, 1)
WHITEHOUSE_TRADE_PATTERN = r'\b(?:tariffs?|trade|duties|imports?|exports?|reciprocal|sanctions?|de minimis|steel|alumin(?:um|ium)|section\s*(?:232|301))\b|China|European Union'
NOW = timezone.utc
CENSUS_MARTS_API = "https://api.census.gov/data/timeseries/eits/marts"
CENSUS_MARTS_FIELDS = "cell_value,data_type_code,time_slot_id,category_code,seasonally_adj"
CLEVELAND_CURRENT_PAGE = "https://www.clevelandfed.org/indicators-and-data/inflation-nowcasting"

P1_SOURCES = {
    "ZEW": [
        "https://www.zew.de/en/zew/news/2025-release-dates-for-zew-indicator-of-economic-sentiment-fixed",
        "https://www.zew.de/en/press/latest-press-releases/2026-release-dates-for-zew-indicator-of-economic-sentiment-fixed",
        "https://www.zew.de/en/publications/zew-expertises-research-reports/research-reports/business-cycle/zew-financial-market-survey",
    ],
    "SP_GLOBAL_FLASH_PMI": [
        "https://pmi.spglobal.com/Public/Release/PressReleases?language=en",
        "https://pmi.spglobal.com/Public/Release/ReleaseDates?language=en",
    ],
    "ISM_PMI": ["https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/"],
    "CLEVELAND_NOW": ["https://www.clevelandfed.org/-/media/files/webcharts/inflationcharting/inflationcharting.zip?hash=5B270BF76C60D2F77BBAE9FF8E36C007&sc_lang=en"],
    "CENSUS_RETAIL": [
        "https://www.census.gov/retail/marts/historic_releases.html",
        "https://www.census.gov/economic-indicators/calendar-listview-2025.html",
        "https://www.census.gov/economic-indicators/calendar-listview-2026.html",
    ],
    "ECB_EDP_VINTAGE": [
        "https://data.ecb.europa.eu/help/api/data",
        "https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.MRR_FR.LEV?startPeriod=2025-01-01&includeHistory=true",
        "https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25.WT?startPeriod=2025-01-01&includeHistory=true",
    ],
    "WHITEHOUSE_TRADE": [
        "https://www.whitehouse.gov/presidential-actions/executive-orders/?s=tariffs",
        "https://www.whitehouse.gov/presidential-actions/executive-orders/?s=trade",
        "https://www.whitehouse.gov/presidential-actions/executive-orders/",
    ],
    "USTR_TRADE": [
        "https://www.ustr.gov/about/policy-offices/press-office/press-releases/2025",
        "https://www.ustr.gov/about/policy-offices/press-office/press-releases/2026",
        "https://ustr.gov/archive/Meta_Content/RSS/ustr_press_releases_10475.xml",
    ],
    "NYFED_NOW": ["https://www.newyorkfed.org/research/policy/nowcast/"],
}

LICENSE_PROBES = {
    "SP_GLOBAL_FLASH_PMI": "https://pmi.spglobal.com/Public/Release/PressReleases?language=en",
    "ISM_PMI": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/",
}


def _now():
    return datetime.now(NOW).replace(tzinfo=None)


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self.href = None
        self.text = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag.lower() == "a" and a.get("href"):
            self.href, self.text = a["href"], []

    def handle_data(self, data):
        val = " ".join(data.split())
        if self.href is not None and val:
            self.text.append(val)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self.href is not None:
            self.rows.append((self.href, " ".join(self.text)))
            self.href, self.text = None, []


def _text(body: bytes):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace"))).split())


def _record_status(db, source, key, url, now, status, error="", headers=None):
    payload = json.dumps({"url": url, "http_status": status, "error": error[:250],
                          "headers": headers or {}, "observed_at_utc": now.isoformat()},
                         sort_keys=True).encode()
    digest, path = common.preserve(source, payload)
    meta = {"status": status or 0, "content_type": "application/json", "url": url}
    row = common.base_row(source, "fetch-status:" + key, "FETCH_STATUS", url,
                          payload, meta, now, None, "OBSERVED", "FETCH_STATUS",
                          title=f"fetch {status or 'NO_STATUS'}", event_type="FETCH_STATUS",
                          extra_json={"error": error[:250], "response_headers": headers or {}})
    row.update(content_sha256=digest, raw_blob_path=path, parser_version=VERSION)
    common.store(db, row)


def _save(db, source, key, url, body, meta, now, *, kind="ORIGINAL_INDEX", title=None,
          event="ARCHIVE_INDEX", region=None, asof="DAY_ONLY", evidence="DAY_ONLY",
          extra=None, published_at=None):
    row = common.base_row(source, key, kind, url, body, meta, now, published_at,
                          evidence, asof, title=title or key, event_type=event,
                          region=region, extra_json=extra or {})
    row["parser_version"] = VERSION
    common.store(db, row)


def _fetch_page(db, source, key, url, now, cap=10_000_000, timeout=25):
    # Honor durable per-URL transport backoff, including controlled source
    # reruns. A deferred check is not a new HTTP response.
    cur=db.cursor()
    cur.execute(f"SELECT last_seen_at_utc,http_status,extra_json FROM `{TABLE}` WHERE source_id=%s AND canonical_url=%s AND record_kind='FETCH_STATUS' ORDER BY last_seen_at_utc DESC LIMIT 1",(source,url))
    failed=cur.fetchone();cur.close()
    if failed:
        seen,status,raw_extra=failed;ex=json.loads(raw_extra or '{}')
        wait=86400 if status in (401,403,407) else 300 if status==429 else 86400 if status==404 else 0
        value=(ex.get('response_headers') or {}).get('retry-after')
        deadline=seen+timedelta(seconds=wait)
        if status==429 and value:
            try:deadline=seen+timedelta(seconds=max(0,int(value)))
            except (ValueError,TypeError):deadline=common.parse_explicit_datetime(value) or deadline
        if now<deadline:
            return None,{'status':status or None,'url':url,'error':str(ex.get('error',''))+':RETRY_DEFERRED',
                         'http_attempted':False,'next_retry_at_utc':deadline.isoformat()}
    try:
        body, meta = common.conditional_fetch(db, source, key, url, timeout=timeout, cap=cap)
        return body, meta
    except Exception as exc:
        status = exc.code if isinstance(exc, urllib.error.HTTPError) else getattr(exc, 'response_status', None)
        hdr = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else getattr(exc, 'response_headers', {})
        response_body = getattr(exc, 'partial_body', b'')
        if response_body:
            _save(db, source, 'incomplete-response:' + key, url, response_body,
                  {'status': status, 'content_type': hdr.get('content-type'), 'url': url}, now,
                  kind='INCOMPLETE_RESPONSE', title='Incomplete response; not a release original',
                  event='INCOMPLETE_TRANSPORT', asof='FETCH_STATUS', evidence='OBSERVED',
                  extra={'response_complete': False, 'error': str(exc), 'response_bytes': len(response_body)})
        if isinstance(exc, urllib.error.HTTPError):
            try:
                response_body = exc.read(2_000_000)
            except Exception:
                response_body = b""
            if response_body:
                meta = {"status": status, "content_type": hdr.get("content-type"), "url": url}
                _save(db, source, "blocked-response:" + key, url, response_body, meta, now,
                      kind="BLOCKED_ACCESS_RESPONSE", title=f"HTTP {status} response",
                      event="ACCESS_ERROR_RESPONSE", asof="FETCH_STATUS", evidence="OBSERVED",
                      extra={"response_bytes": len(response_body), "response_headers": hdr})
        _record_status(db, source, key, url, now, status, f"{type(exc).__name__}:{exc}", hdr)
        return None, {"status": status, "url": url, "content_type": hdr.get("content-type"),
                      "response_bytes": len(response_body) if response_body else None,
                      "sha256": hashlib.sha256(response_body).hexdigest() if response_body else None,
                      "error": type(exc).__name__ + ":" + str(exc)[:150]}


def _extract_date(title, url):
    haystack = title + " " + urllib.parse.unquote(url)
    for pattern in (r"\b(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})\b",
                    r"\b(\d{1,2})[-_/](\d{1,2})[-_/](20\d{2})\b"):
        m = re.search(pattern, haystack)
        if not m:
            continue
        try:
            if m.group(1).startswith("20"):
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            else:
                d = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            if START <= d <= date.today():
                return d
        except ValueError:
            pass
    m = re.search(r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})\b", haystack, re.I)
    if m:
        try:
            d = date(int(m.group(3)), datetime.strptime(m.group(1), "%B").month, int(m.group(2)))
            if START <= d <= date.today():
                return d
        except ValueError:
            pass
    return None


def _extract_published_date(body):
    """Read only an explicit publication date from an official HTML document."""
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
    patterns = (
        r'<meta[^>]+(?:property|name)=["\'](?:article:published_time|parsely-pub-date|date)["\'][^>]+content=["\']([^"\']+)',
        r'"datePublished"\s*:\s*"([^"\']+)"',
        r'<time[^>]+datetime=["\']([^"\']+)',
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if not match:
            continue
        try:
            value = match.group(1).replace("Z", "+00:00")
            published = datetime.fromisoformat(value).date()
            if START <= published <= date.today():
                return published
        except ValueError:
            continue
    return None


def _extract_published_datetime(body):
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
    patterns = (
        r'<meta[^>]+(?:property|name)=["\'](?:article:published_time|parsely-pub-date|date)["\'][^>]+content=["\']([^"\']+)',
        r'"datePublished"\s*:\s*"([^"\']+)"',
        r'<time[^>]+datetime=["\']([^"\']+)',
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if not match:
            continue
        try:
            value = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
                if START <= value.date() <= date.today():
                    return value
        except ValueError:
            continue
    return None


def _modified_clock_evidence(body):
    text=body.decode('utf-8','replace')
    match=re.search(r'"dateModified"\s*:\s*"([^"\']+)"',text,re.I)
    modified=common.parse_explicit_datetime(match.group(1)) if match else None
    return {'source_modified_at_utc':modified.isoformat() if modified else None,
            'current_html_is_not_verified_first_publication_text':True}


def _parse_ustr_rss(body):
    root = ET.fromstring(body)
    rows = []
    for item in root.iter():
        if item.tag.rsplit("}", 1)[-1].lower() != "item":
            continue
        fields = {child.tag.rsplit("}", 1)[-1].lower(): (child.text or "").strip()
                  for child in item}
        title, url = fields.get("title"), fields.get("link")
        raw_date = fields.get("pubdate") or fields.get("published") or fields.get("date")
        if not title or not url or not raw_date:
            continue
        try:
            published = parsedate_to_datetime(raw_date)
        except (TypeError, ValueError, OverflowError):
            continue
        if published.tzinfo is None:
            continue
        published = published.astimezone(timezone.utc).replace(tzinfo=None)
        if START <= published.date() <= date.today():
            rows.append({"title": html.unescape(title), "url": url,
                         "published_at": published})
    return rows


def _parse_nyfed_nowcast(body):
    text = _text(body)
    release_match = re.search(
        r"Latest Release:\s*(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2}),\s+(20\d{2})",
        text, re.I)
    release_date = None
    if release_match:
        try:
            month = None
            for fmt in ("%B", "%b"):
                try:
                    month = datetime.strptime(release_match.group(1), fmt).month
                    break
                except ValueError:
                    pass
            release_date = date(int(release_match.group(3)), month,
                                int(release_match.group(2)))
        except ValueError:
            pass
    pattern = r"(?:The )?(?:New York Fed )?Staff Nowcast for\s+(20\d{2}:Q[1-4])\s+is\s+([+\-]?\d+(?:\.\d+)?)%"
    values = {}
    for match in re.finditer(pattern, text, re.I):
        q_year, quarter = map(int, re.fullmatch(r"(20\d{2}):Q([1-4])", match.group(1)).groups())
        values[f"{q_year}:Q{quarter}"] = {
            "observation_period": date(q_year, (quarter - 1) * 3 + 1, 1),
            "value": float(match.group(2)),
        }
    return {"release_date": release_date, "values": values}


def _store_nyfed_nowcast(db, url, body, meta, now):
    parsed = _parse_nyfed_nowcast(body)
    saved = 0
    for quarter, item in parsed["values"].items():
        release_date = parsed["release_date"]
        key = f"current:{quarter}:{release_date.isoformat() if release_date else 'day-only'}"
        row = common.base_row(
            "NYFED_NOW", key, "LIVE_NOWCAST", url, body, meta, now, None,
            "DAY_ONLY", "DAY_ONLY", title=f"New York Fed Staff Nowcast {quarter}",
            event_type="GDP_NOWCAST", region="UNITED_STATES",
            observation_period_start=item["observation_period"].isoformat(),
            vintage_month=release_date.replace(day=1).isoformat() if release_date else None,
            numeric_value=item["value"], unit="PERCENT_QOQ_SAAR",
            reference_timezone="America/New_York",
            extra_json={"quarter": quarter, "release_date": release_date.isoformat() if release_date else None,
                        "release_hour_known": False,
                        "availability_rule": "official page says Friday at/shortly after 12:45 ET; not used as actual timestamp",
                        "historical_daily_vintages_not_in_current_page": True})
        row["parser_version"] = VERSION
        common.store(db, row)
        saved += 1
    return {"release_date": parsed["release_date"].isoformat() if parsed["release_date"] else None,
            "forecast_rows": saved}


def _collect_ustr_trade(db, now):
    source = "USTR_TRADE"
    result = {"source": source, "indices": 0, "documents": 0,
              "rss_items": 0, "errors": [], "backfill_start": START.isoformat()}
    candidates = {}
    for i, url in enumerate(P1_SOURCES[source]):
        key = f"index:{i}:{hashlib.sha256(url.encode()).hexdigest()[:12]}"
        body, meta = _fetch_page(db, source, key, url, now, cap=12_000_000)
        if body is None:
            result["errors"].append({"url": url, "status": meta.get("error")})
            continue
        result["indices"] += 1
        _save(db, source, key, url, body, meta, now, title="Official USTR archive/feed",
              extra={"index_url": url, "http_status": meta.get("status")})
        if url.lower().endswith(".xml"):
            try:
                items = _parse_ustr_rss(body)
                result["rss_items"] += len(items)
                for item in items:
                    if re.search(r"trade|tariff|china|europe|eu|import|export|301|usmca", item["title"], re.I):
                        candidates[item["url"]] = (item["title"], item["published_at"].date(), item["published_at"])
            except ET.ParseError as exc:
                result["errors"].append({"url": url, "status": f"XML_PARSE:{exc}"})
            continue
        parser = archive.LinkParser()
        parser.feed(body.decode("utf-8", "replace"))
        for href, title, context_date in parser.links:
            if not re.search(r"trade|tariff|china|europe|eu|import|export|301|usmca", title, re.I):
                continue
            target = urllib.parse.urljoin(url, href).split("#", 1)[0]
            if not target.startswith("https://") or (urllib.parse.urlparse(target).hostname or "").lower() not in ("ustr.gov", "www.ustr.gov"):
                continue
            published_day = context_date or _extract_date(title, target)
            if published_day and START <= published_day <= date.today():
                candidates[target] = (title, published_day, None)
    result["candidates"] = len(candidates)
    for url, (title, published_day, published_at) in sorted(candidates.items(), key=lambda kv: (kv[1][1], kv[0])):
        key = "document:" + hashlib.sha256(url.encode()).hexdigest()[:32]
        cur = db.cursor()
        cur.execute(f"SELECT 1 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s LIMIT 1", (source, key))
        exists = cur.fetchone() is not None
        cur.close()
        if exists:
            continue
        body, meta = _fetch_page(db, source, key, url, now, cap=12_000_000)
        if body is None:
            result["errors"].append({"url": url, "status": meta.get("error")})
            continue
        published_at = published_at or _extract_published_datetime(body)
        if published_at:
            published_day = published_at.date()
        _save(db, source, key, url, body, meta, now, kind="ORIGINAL_DOCUMENT",
              title=title, event="USTR_TRADE_POLICY_RELEASE", region="UNITED_STATES",
              asof="HISTORICAL_REPLAY",
              evidence="DOCUMENTED_PUBLISHED_AT" if published_at else "DAY_ONLY",
              published_at=published_at,
              extra={"document_date": published_day.isoformat(),
                     "publication_clock": "documented" if published_at else "DAY_ONLY",
                     "source_timezone": "UTC" if published_at else "America/New_York",
                     "original_document": True, "backfill_from": START.isoformat()})
        result["documents"] += 1
        time.sleep(.25)
    return result


def _collect_source(db, source, urls, now):
    result = {"source": source, "indices": 0, "documents": 0, "dated_documents": 0,
              "new_document_versions":0,
              "linked_artifacts": 0, "probe_evidence": [], "blocked": [], "errors": []}
    links = {}
    for n, url in enumerate(urls):
        key = f"index:{n}:{hashlib.sha256(url.encode()).hexdigest()[:12]}"
        body, meta = _fetch_page(db, source, key, url, now)
        if body is None:
            if meta.get("status") in (401, 403, 407):
                result["blocked"].append({**meta, "http_status": meta["status"], "classification": "BLOCKED_ACCESS"})
            else:
                result["errors"].append({"url": url, "status": meta.get("error")})
            continue
        result["indices"] += 1
        result["probe_evidence"].append({"url": meta.get("url", url),
            "http_status": meta.get("status"), "content_type": meta.get("content_type"),
            "response_bytes": meta.get("network_response_bytes", len(body)),
            "cached_body_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "etag": meta.get("etag"), "last_modified": meta.get("last_modified")})
        _save(db, source, key, url, body, meta, now, title="Official source index",
              extra={"index_url": url, "http_status": meta.get("status")})
        if source == "NYFED_NOW":
            result["current_nowcast"] = _store_nyfed_nowcast(db, url, body, meta, now)
        page = Links()
        page.feed(body.decode("utf-8", "replace"))
        if source == 'WHITEHOUSE_TRADE':
            page_numbers=[int(v) for v in re.findall(r'/presidential-actions/page/(\d+)/',body.decode('utf-8','replace'))]
            result['archive_max_page']=max(page_numbers,default=1)
        for href, title in page.rows:
            target = urllib.parse.urljoin(url, href).split("#", 1)[0]
            if not target.startswith(("https://", "http://")):
                continue
            host = urllib.parse.urlparse(target).hostname or ""
            if not any(x in host for x in ("zew.de", "pmi.spglobal.com", "ismworld.org", "clevelandfed.org", "census.gov", "ecb.europa.eu", "ec.europa.eu", "whitehouse.gov", "ustr.gov", "newyorkfed.org")):
                continue
            if target.rstrip("/") == url.rstrip("/"):
                continue
            d = _extract_date(title, target)
            # Keep index pages and dated records; source adapter refines scope.
            if source == "WHITEHOUSE_TRADE" and not re.search(WHITEHOUSE_TRADE_PATTERN, title, re.I):
                continue
            if source == "USTR_TRADE" and not re.search(r"trade|tariff|china|european union|europe|eu", title, re.I):
                continue
            if source == "SP_GLOBAL_FLASH_PMI" and not re.search(r"flash|eurozone|united states|pmi", title, re.I):
                continue
            if source == "ZEW" and not re.search(r"ZEW|economic sentiment|financial market|data|download", title, re.I):
                continue
            if source == "ISM_PMI" and not re.search(r"PMI|manufacturing|services|report", title, re.I):
                continue
            if source == "CLEVELAND_NOW" and not re.search(r"data|download|csv|xlsx|inflation|nowcast", title + " " + target, re.I):
                continue
            if source == "CENSUS_RETAIL" and not re.search(r"retail|marts|advance|2025|2026|xls|pdf", title + " " + target, re.I):
                continue
            if source == "NYFED_NOW" and not re.search(r"histor|archive|data|download|xlsx|excel|nowcast", title + " " + target, re.I):
                continue
            links[target] = (title or "Official source document", d)
        time.sleep(.2)
    result["candidates"] = len(links)
    for url, (title, d) in sorted(links.items()):
        # Avoid recursively fetching static navigation links and undated generic pages.
        path = urllib.parse.urlparse(url).path.lower()
        whitehouse_action = (source == "WHITEHOUSE_TRADE" and
                             re.search(r"/presidential-actions/20\d{2}/\d{2}/", path))
        if not d and not whitehouse_action and not path.endswith((".pdf", ".xlsx", ".xls", ".csv", ".xml")):
            continue
        key = "document:" + hashlib.sha256(url.encode()).hexdigest()[:32]
        body, meta = _fetch_page(db, source, key, url, now, cap=20_000_000)
        if body is None:
            if meta.get("status") in (401, 403, 407):
                result["blocked"].append({**meta, "http_status": meta["status"], "classification": "BLOCKED_ACCESS"})
            else:
                result["errors"].append({"url": url, "status": meta.get("error")})
            continue
        published_at = _extract_published_datetime(body) if source == "WHITEHOUSE_TRADE" else None
        if source == "WHITEHOUSE_TRADE" and d is None:
            d = published_at.date() if published_at else _extract_published_date(body)
        cur=db.cursor()
        cur.execute(f"SELECT 1 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s AND content_sha256=%s LIMIT 1",
                    (source,key,hashlib.sha256(body).hexdigest()))
        already_stored=cur.fetchone() is not None;cur.close()
        result['new_document_versions']+=int(not already_stored)
        _save(db, source, key, url, body, meta, now, kind="ORIGINAL_DOCUMENT",
              title=title, event="SOURCE_DOCUMENT",
              asof="HISTORICAL_REPLAY" if source == "WHITEHOUSE_TRADE" else ("DAY_ONLY" if d else "UNDATED_ARCHIVE"),
              evidence="DOCUMENTED_PUBLISHED_AT" if published_at else "DAY_ONLY",
              published_at=published_at,
              region=_region(source), extra={"document_date": d.isoformat() if d else None,
                    "historical_backfill": bool(d), "publication_clock": "DOCUMENTED" if published_at else "DAY_ONLY",
                    "source_timezone": _timezone(source), "http_status": meta.get("status"),
                    **(_modified_clock_evidence(body) if source=='WHITEHOUSE_TRADE' else {})})
        if source == "CLEVELAND_NOW" and path.endswith(".csv"):
            result["historical_chart_values"] = result.get("historical_chart_values", 0) + _store_cleveland_export(
                db, url, body, meta, now)
        result["documents"] += 1
        result["dated_documents"] += bool(d)
        result["linked_artifacts"] += path.endswith((".pdf", ".xlsx", ".xls", ".csv", ".xml"))
        time.sleep(.2)
    return result


def _collect_zew_monthly_tables(db, now):
    """Retain official monthly ZEW table PDFs; release-day/hour stays unknown."""
    months = []
    y, m = 2025, 1
    while (y, m) <= (now.year, now.month):
        months.append((y, m))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    result = {"source": "ZEW", "documents": 0, "errors": [], "attempted": 0,
              "first_month": months[0][0] * 100 + months[0][1],
              "last_month": months[-1][0] * 100 + months[-1][1]}
    consecutive_timeouts = 0
    for year, month in months:
        url = f"https://download.zew.de/e_{month:02d}_{year}_Tabelle.pdf"
        key = f"monthly-table:{year}-{month:02d}"
        body, meta = _fetch_page(db, "ZEW", key, url, now, cap=8_000_000, timeout=8)
        result["attempted"] += 1
        if body is None:
            result["errors"].append({"url": url, "status": meta.get("error"),
                                     "http_status": meta.get("status")})
            consecutive_timeouts = consecutive_timeouts + 1 if meta.get("status") is None else 0
            if consecutive_timeouts >= 2:
                result["stopped_after_consecutive_timeouts"] = True
                break
            continue
        consecutive_timeouts = 0
        digest = hashlib.sha256(body).hexdigest()
        if meta.get("content_type") and "pdf" not in meta["content_type"].lower():
            result["errors"].append({"url": url, "status": "UNEXPECTED_CONTENT_TYPE",
                                     "content_type": meta["content_type"], "sha256": digest,
                                     "bytes": len(body)})
            continue
        _save(db, "ZEW", key, url, body, meta, now, kind="ORIGINAL_DOCUMENT",
              title=f"ZEW Financial Market Survey table {year}-{month:02d}",
              event="ZEW_INDICATOR_RELEASE", region="EURO_AREA_OR_GERMANY",
              asof="HISTORICAL_ARCHIVE", evidence="DAY_ONLY",
              extra={"release_month": f"{year}-{month:02d}",
                     "source_published_at_unknown": True,
                     "scheduled_clock_local": "11:05",
                     "scheduled_timezone": "Europe/Berlin",
                     "scheduled_clock_is_not_actual_publication": True,
                     "response_bytes": len(body), "sha256": digest})
        result["documents"] += 1
        result.setdefault("manifest", []).append({"url": url, "http_status": meta.get("status"),
            "content_type": meta.get("content_type"), "response_bytes": len(body), "sha256": digest})
        time.sleep(.3)
    return result


def _region(source):
    return {"ZEW": "EURO_AREA", "SP_GLOBAL_FLASH_PMI": "EURO_AREA_OR_US",
            "ISM_PMI": "UNITED_STATES", "CLEVELAND_NOW": "UNITED_STATES",
            "CENSUS_RETAIL": "UNITED_STATES", "ECB_EDP_VINTAGE": "EURO_AREA",
            "WHITEHOUSE_TRADE": "UNITED_STATES", "USTR_TRADE": "UNITED_STATES",
            "NYFED_NOW": "UNITED_STATES"}.get(source)


def _collect_census_retail_api_control(db, now):
    """Authenticated official Census MARTS API; values stay control-only.

    Census API responses contain the latest/revised series state, not archived
    real-time vintages. Preserve successful samples and never join them to H1.
    """
    source = "CENSUS_RETAIL"
    result = {"source": source, "api": CENSUS_MARTS_API,
              "samples": [], "latest_revised_control_rows": 0, "errors": []}
    try:
        from model100_primary_alfred import protected_key
    except ImportError:
        from work.model100_primary_alfred import protected_key
    token = protected_key('CENSUS_API_KEY', r'[a-fA-F0-9]{40}')
    if not token:
        result.update(status='REQUIRES_API_KEY',
                      requirement='Census-specific CENSUS_API_KEY in existing protected environment/config',
                      policy_url='https://www.census.gov/library/video/2026/adrm/requesting-a-census-data-api-key.html')
        return result
    for period in ("2025-08", "2026-09", "2026-10"):
        query = urllib.parse.urlencode({"get": CENSUS_MARTS_FIELDS, "time": period})
        url = CENSUS_MARTS_API + "?" + query
        # The credential-bearing transport URL never enters records, errors,
        # source manifests or returned results.
        try:
            body, meta = common.download(url + '&key=' + token, timeout=8, cap=2_000_000)
            meta['url'] = url
            if token.encode() in body:
                raise ValueError('CENSUS_RESPONSE_CONTAINS_CREDENTIAL')
            meta.update(response_bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
        except Exception as error:
            status = error.code if isinstance(error, urllib.error.HTTPError) else None
            _record_status(db, source, 'api-latest-control:' + period, url, now,
                           status, type(error).__name__)
            body, meta = None, {'status': status, 'error': type(error).__name__}
        sample = {"period_query": period, "url": url, "http_status": meta.get("status"),
                  "content_type": meta.get("content_type"),
                  "response_bytes": meta.get("response_bytes"), "sha256": meta.get("sha256")}
        if body is None:
            sample["classification"] = "BLOCKED_ACCESS" if meta.get("status") in (401, 403, 407) else "NO_HTTP_STATUS"
            result["samples"].append(sample)
            result["errors"].append({"period": period, "status": meta.get("error")})
            continue
        try:
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, list) or not payload or not isinstance(payload[0], list):
                raise ValueError("CENSUS_API_UNEXPECTED_JSON_SHAPE")
        except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
            sample["classification"] = "UNPARSEABLE_RESPONSE"
            result["samples"].append(sample)
            result["errors"].append({"period": period, "status": str(exc)[:120]})
            continue
        _save(db, source, "api-latest-control:" + period, url, body, meta, now,
              kind="LATEST_REVISED_CONTROL", title="Census MARTS latest API series control",
              event="CENSUS_RETAIL_LATEST_REVISED_CONTROL", region="UNITED_STATES",
              asof="LATEST_REVISED_CONTROL", evidence="DAY_ONLY",
              extra={"query_period": period, "response_bytes": len(body),
                     "sha256": hashlib.sha256(body).hexdigest(),
                     "api_values_are_latest_revised_not_first_vintage": True,
                     "source_published_at_utc": None, "api_key_supplied": True})
        sample["classification"] = "LATEST_REVISED_CONTROL_ONLY"
        sample["response_bytes"] = len(body)
        sample["sha256"] = hashlib.sha256(body).hexdigest()
        result["samples"].append(sample)
        result["latest_revised_control_rows"] += len(payload) - 1
    return result


def _timezone(source):
    return {"ZEW": "Europe/Berlin", "SP_GLOBAL_FLASH_PMI": "UTC",
            "ISM_PMI": "America/New_York", "CLEVELAND_NOW": "America/New_York",
            "CENSUS_RETAIL": "America/New_York", "ECB_EDP_VINTAGE": "Europe/Luxembourg",
            "NYFED_NOW": "America/New_York"}.get(source)


def _parse_cleveland_csv(body: bytes):
    """Parse only unambiguous dated wide chart exports; never reconstruct history."""
    csv_payloads = []
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive_zip:
            csv_payloads = [archive_zip.read(name) for name in archive_zip.namelist()
                            if name.lower().endswith(".csv")]
    except zipfile.BadZipFile:
        csv_payloads = [body]
    try:
        sources = []
        for payload in csv_payloads:
            text = payload.decode("utf-8-sig", "replace")
            try:
                dialect = csv.Sniffer().sniff(text[:8192], delimiters=",;\t")
                rows = list(csv.DictReader(io.StringIO(text), dialect=dialect))
            except csv.Error:
                rows = list(csv.DictReader(io.StringIO(text)))
            if rows:
                sources.append(rows)
    except (UnicodeError, csv.Error):
        return []
    observations = []
    for rows in sources:
        headers = list(rows[0])
        date_header = next((h for h in headers if h and re.search(r"^(date|as.?of|snapshot.?date)$", h.strip(), re.I)), None)
        if not date_header:
            continue
        for row in rows:
            raw_date = (row.get(date_header) or "").strip()
            try:
                snapshot = date.fromisoformat(raw_date[:10])
            except ValueError:
                snapshot = None
                for fmt in ("%m/%d/%Y", "%m/%d/%y"):
                    try:
                        snapshot = datetime.strptime(raw_date, fmt).date()
                        break
                    except ValueError:
                        pass
            if not snapshot or not START <= snapshot <= date.today():
                continue
            for header in headers:
                if not header or header == date_header or not re.search(r"CPI|PCE|inflation", header, re.I):
                    continue
                try:
                    value = float((row.get(header) or "").strip().replace(",", ""))
                except ValueError:
                    continue
                if not math.isfinite(value):
                    continue
                target = re.search(r"(20\d{2})[- /](0?[1-9]|1[0-2])", header)
                if not target:
                    continue
                observations.append({"snapshot_date": snapshot,
                                     "target_month": date(int(target.group(1)), int(target.group(2)), 1),
                                     "metric": header.strip(), "value": value})
    return observations


def _store_cleveland_export(db, url, body, meta, now):
    parsed = _parse_cleveland_csv(body)
    for item in parsed:
        digest = hashlib.sha256(item["metric"].encode()).hexdigest()[:12]
        key = "chart:" + item["snapshot_date"].isoformat() + ":" + digest
        row = common.base_row("CLEVELAND_NOW", key, "FORECAST_VINTAGE", url, body, meta,
            now, None, "DAY_ONLY", "DAY_ONLY", title=item["metric"],
            event_type="CLEVELAND_INFLATION_NOWCAST", region="UNITED_STATES",
            observation_period_start=item["target_month"].isoformat(),
            numeric_value=item["value"], unit="SOURCE_REPORTED_UNIT",
            extra_json={"snapshot_date": item["snapshot_date"].isoformat(),
                        "snapshot_date_is_not_publication_time": True,
                        "source_timezone": "America/New_York", "original_export_url": url})
        row["parser_version"] = VERSION
        common.store(db, row)
    return len(parsed)


class TableRows(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables, self.table, self.row, self.cell = [], None, None, None

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "table":
            self.table = []
        elif tag == "tr" and self.table is not None:
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            value = " ".join(data.split())
            if value:
                self.cell.append(value)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("td", "th") and self.cell is not None:
            self.row.append(" ".join(self.cell))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def _parse_cleveland_current_page(body, now):
    parser = TableRows()
    parser.feed(body.decode("utf-8", "replace"))
    kinds = (("CLEVELAND_MOM", "PERCENT_MOM_SA"),
             ("CLEVELAND_YOY", "PERCENT_YOY"),
             ("CLEVELAND_QOQ_SAAR", "PERCENT_QOQ_SAAR"))
    out = []
    for table_index, (event_type, unit) in enumerate(kinds):
        if table_index >= len(parser.tables):
            break
        for cells in parser.tables[table_index]:
            if len(cells) < 6:
                continue
            label = cells[0].strip()
            try:
                quarter = re.fullmatch(r"(20\d{2}):Q([1-4])", label)
                if quarter:
                    year, q = map(int, quarter.groups())
                    period = date(year, (q - 1) * 3 + 1, 1)
                else:
                    period = datetime.strptime(label, "%B %Y").date().replace(day=1)
                updated = re.match(r"(\d{1,2})/(\d{1,2})", cells[5])
                updated_date = date(now.year, int(updated.group(1)), int(updated.group(2))) if updated else None
                if updated_date and updated_date > now.date():
                    updated_date = updated_date.replace(year=now.year - 1)
            except (ValueError, AttributeError):
                continue
            for metric, raw in zip(("CPI", "CORE_CPI", "PCE", "CORE_PCE"), cells[1:5]):
                try:
                    value = float(raw.replace(",", ""))
                except ValueError:
                    continue
                out.append({"metric": metric, "event_type": event_type,
                            "observation_period": period, "updated_date": updated_date,
                            "value": value, "unit": unit})
    return out


def _collect_cleveland_current_page(db, now):
    source, url = "CLEVELAND_NOW", CLEVELAND_CURRENT_PAGE
    body, meta = _fetch_page(db, source, "official-current-nowcast-page", url, now,
                             cap=8_000_000, timeout=25)
    if body is None:
        return {"http_status": meta.get("status"), "error": meta.get("error"), "values": 0}
    _save(db, source, "official-current-nowcast-page", url, body, meta, now,
          title="Cleveland Fed current inflation nowcasts", event="CURRENT_NOWCAST_PAGE",
          region="UNITED_STATES", asof="DAY_ONLY", evidence="DAY_ONLY",
          extra={"page_exposes_update_date_without_publication_hour": True})
    parsed = _parse_cleveland_current_page(body, now)
    for item in parsed:
        updated = item["updated_date"]
        key = (f"daily:{item['event_type']}:{item['metric']}:{item['observation_period'].isoformat()}:"
               f"{updated.isoformat() if updated else 'unknown'}")
        row = common.base_row(
            source, key, "LIVE_NOWCAST", url, body, meta, now, None,
            "DAY_ONLY", "DAY_ONLY", title=f"Cleveland {item['metric']} {item['event_type']} nowcast",
            event_type=item["event_type"], region="UNITED_STATES",
            observation_period_start=item["observation_period"].isoformat(),
            vintage_month=updated.replace(day=1).isoformat() if updated else None,
            numeric_value=item["value"], unit=item["unit"], reference_timezone="America/New_York",
            extra_json={"metric": item["metric"], "updated_date": updated.isoformat() if updated else None,
                        "value_class": "current_page_latest_state_not_historical_snapshot",
                        "publication_hour_known": False})
        row["parser_version"] = VERSION
        common.store(db, row)
    return {"http_status": meta.get("status"), "response_bytes": meta.get("network_response_bytes", len(body)),
            "sha256": hashlib.sha256(body).hexdigest(), "values": len(parsed),
            "historical_snapshots": False}


def _license_probe(db, source, url, now):
    try:
        body, meta = common.conditional_fetch(db, source, "license-probe", url, timeout=15, cap=2_000_000)
        result = {"http_status": meta.get("status"), "content_type": meta.get("content_type"),
                  "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "url": url}
        _save(db, source, "access-probe", url, body, meta, now, title="Public access and license probe",
              event="ACCESS_LICENSE_PROBE", asof="OBSERVED", evidence="OBSERVED",
              extra={**result, "license_verified": False})
        result["classification"] = "ACCESSIBLE_LICENSE_UNCONFIRMED"
    except Exception as exc:
        status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
        if isinstance(exc, urllib.error.HTTPError):
            try:
                response_body = exc.read(2_000_000)
            except Exception:
                response_body = b""
            if response_body:
                meta = {"status": status, "content_type": headers.get("content-type"), "url": url}
                _save(db, source, "blocked-response:license-probe", url, response_body,
                      meta, now, kind="BLOCKED_ACCESS_RESPONSE", title=f"HTTP {status} response",
                      event="ACCESS_ERROR_RESPONSE", asof="FETCH_STATUS", evidence="OBSERVED",
                      extra={"response_bytes": len(response_body), "response_headers": headers})
        _record_status(db, source, "license-probe", url, now, status, f"{type(exc).__name__}:{exc}", headers)
        result = {"http_status": status, "url": url,
                  "classification": "BLOCKED_ACCESS" if status in (401, 403, 407) else "UNAVAILABLE",
                  "reason": f"{type(exc).__name__}:{str(exc)[:150]}"}
    return result


def collect_alfred_vintages(db, now):
    """Fetch public ALFRED metadata/website release archive; no private API key."""
    source = "ALFRED"
    urls = ["https://alfred.stlouisfed.org/release?rid=10",  # CPI
            "https://alfred.stlouisfed.org/release?rid=53",  # GDP
            "https://alfred.stlouisfed.org/release?rid=54",  # PCE price index
            "https://alfred.stlouisfed.org/release?rid=50",  # Employment situation
            "https://alfred.stlouisfed.org/release/downloaddates?rid=10",
            "https://alfred.stlouisfed.org/release/downloaddates?rid=53",
            "https://alfred.stlouisfed.org/release/downloaddates?rid=54",
            "https://alfred.stlouisfed.org/release/downloaddates?rid=50"]
    return _collect_source(db, source, urls, now)


def _ecb_value_key(url, item):
    return (url,item.get('dataflow'),json.dumps(item.get('dimensions') or {},sort_keys=True),
            str(item.get('observation_period') or item.get('observation_period_start')),
            item.get('updated_at_raw'),str(Decimal(str(item['value'])).quantize(Decimal('.00000001'),rounding=ROUND_HALF_UP)))


def ecb_original_response_links(records):
    """Verify a derived value against preserved SDMX bodies, without network I/O."""
    originals={}
    for row in records:
        legacy_xml=(row['record_kind']=='ORIGINAL_INDEX' and row['canonical_url'].startswith('https://data-api.ecb.europa.eu/service/data/'))
        if (row['record_kind']!='ORIGINAL_XML' and not legacy_xml) or int(row['http_status'] or 0) not in (200,304):continue
        body=Path(row['raw_blob_path']).read_bytes()
        if hashlib.sha256(body).hexdigest()!=row['content_sha256']:raise ValueError('ECB_SDMX_ORIGINAL_SHA_MISMATCH')
        for item in parse_ecb_sdmx_vintages(body,row['canonical_url']):
            key=_ecb_value_key(row['canonical_url'],item)
            originals.setdefault(key,[]).append({'sha256':row['content_sha256'],'raw_blob_path':row['raw_blob_path']})
    result={}
    for row in records:
        if row.get('numeric_value') is None:continue
        ex=json.loads(row['extra_json']) if isinstance(row['extra_json'],str) else row['extra_json']
        item={**ex,'observation_period_start':row['observation_period_start'],'value':row['numeric_value']}
        matches=originals.get(_ecb_value_key(row['canonical_url'],item),[])
        result[(row['record_key'],row['content_sha256'])]=list({m['sha256']:m for m in matches}.values())
    return result


def _repair_ecb_original_links(db):
    cur=db.cursor();cur.execute(f"SELECT * FROM `{TABLE}` WHERE source_id='ECB_EDP_VINTAGE'")
    fields=[f[0] for f in cur.description];records=[dict(zip(fields,v)) for v in cur.fetchall()];cur.close()
    links=ecb_original_response_links(records);fixed=0
    for row in records:
        if row.get('numeric_value') is None:continue
        ex=json.loads(row['extra_json'] or '{}')
        if ex.get('source_response_sha256') or ex.get('source_response_sha256_candidates'):continue
        matches=links.get((row['record_key'],row['content_sha256']),[])
        if not matches:continue
        ex.update(derived_record_sha_is_not_http_body_sha=True,
                  source_response_link_basis='SDMX_VALUE_AND_VINTAGE_CONTENT_MATCH',
                  source_response_sha256_candidates=[m['sha256'] for m in matches])
        if len(matches)==1:
            ex.update(source_response_sha256=matches[0]['sha256'],source_response_raw_blob_path=matches[0]['raw_blob_path'])
        row['extra_json']=json.dumps(ex);common.store(db,row);fixed+=1
    return fixed


def collect_ecb_history_api(db, now):
    source = "ECB_EDP_VINTAGE"
    result = {"source": source, "indices": 0, "vintage_rows": 0,
              "records_by_url": [], "errors": [],'legacy_original_links_corrected':_repair_ecb_original_links(db)}
    for n, url in enumerate(P1_SOURCES[source]):
        if not url.startswith("https://data-api.ecb.europa.eu/service/data/"):
            # Help pages are documentation probes, not series history; don't
            # add their repeated SSL timeout to the daily data collector.
            continue
        key = f"index:{n}:{hashlib.sha256(url.encode()).hexdigest()[:12]}"
        body, meta = _fetch_page(db, source, key, url, now, cap=20_000_000)
        if body is None:
            result["errors"].append({"url": url, "status": meta.get("error"),
                                     "http_status": meta.get("status")})
            continue
        result["indices"] += 1
        sha = hashlib.sha256(body).hexdigest()
        _,source_response_path=common.preserve(source,body)
        kind = "ORIGINAL_XML" if "xml" in (meta.get("content_type") or "").lower() else "ORIGINAL_DOCUMENT"
        _save(db, source, key, url, body, meta, now, kind=kind,
              title="ECB Data API history=" + ("true" if "includeHistory=true" in url else "metadata"),
              event="ECB_API_HISTORY_OR_METADATA", asof="HISTORICAL_ARCHIVE",
              evidence="API_RESPONSE", region="EURO_AREA",
              extra={"include_history": "includeHistory=true" in url,
                     "response_bytes": meta.get("network_response_bytes", len(body)),
                     "cached_body_bytes": len(body), "response_sha256": sha})
        values = parse_ecb_sdmx_vintages(body, url)
        for item in values:
            raw = json.dumps(item, ensure_ascii=False, sort_keys=True).encode("utf-8")
            dimensions = item["dimensions"]
            dims_key = ".".join(f"{k}={v}" for k, v in sorted(dimensions.items()))
            row_key = (f"vintage:{item['dataflow']}:{dims_key}:{item['observation_period']}"
                       f":{item['updated_at_raw']}:{item['value']}")
            updated_at = item["updated_at_utc"]
            evidence = "API_VINTAGE_TIMESTAMP" if updated_at else "DAY_ONLY"
            meta_row = {"status": 200, "content_type": "application/json", "url": url,
                        "received_at_utc": meta.get("received_at_utc"),
                        "fetch_status": "PARSED_API_VINTAGE"}
            row = common.base_row(
                source, row_key, "VINTAGE_VALUE", url, raw, meta_row, now, None,
                evidence, "HISTORICAL_VINTAGE", title=item["title"],
                event_type="ECB_API_VINTAGE", region="EURO_AREA",
                observation_period_start=item["observation_period"],
                numeric_value=item["value"], unit=item.get("unit"),
                extra_json={**item, "source_updated_at_utc": updated_at,
                            "source_published_at_utc": None,
                            "source_response_sha256":sha,
                            "source_response_raw_blob_path":source_response_path,
                            "derived_record_sha_is_not_http_body_sha":True,
                            "api_fetch_http_status": meta.get("status"),
                            "api_fetch_status": meta.get("fetch_status", "HTTP_" + str(meta.get("status"))),
                            "available_at_interpretation": "API_VINTAGE_TIME_ONLY"})
            row["parser_version"] = VERSION
            common.store(db, row)
            result["vintage_rows"] += 1
        result["records_by_url"].append({"url": url, "http_status": meta.get("status"),
                                         "response_bytes": meta.get("network_response_bytes", len(body)),
                                         "cached_body_bytes": len(body), "sha256": sha,
                                         "vintage_rows": len(values)})
        time.sleep(.25)
    return result


def parse_ecb_sdmx_vintages(body: bytes, url: str):
    """Parse ECB SDMX-ML GenericData; keep update time distinct from publication time."""
    root = ET.fromstring(body)
    local = lambda tag: tag.rsplit("}", 1)[-1]
    dataflow = "unknown"
    m = re.search(r"/service/data/([^/]+)/", urllib.parse.urlparse(url).path)
    if m:
        dataflow = m.group(1)
    observations = []
    for dataset in (node for node in root.iter() if local(node.tag) == "DataSet"):
        updated_raw = dataset.attrib.get("validFromDate")
        updated_utc = None
        if updated_raw:
            try:
                parsed = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    updated_utc = parsed.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
            except ValueError:
                pass
        for series in (node for node in dataset.iter() if local(node.tag) == "Series"):
            dimensions = {}
            series_title = None
            unit = None
            for section in series:
                section_name = local(section.tag)
                if section_name not in ("SeriesKey", "Attributes"):
                    continue
                for node in section:
                    if local(node.tag) != "Value":
                        continue
                    identifier, value = node.attrib.get("id"), node.attrib.get("value")
                    if not identifier or not value:
                        continue
                    if section_name == "SeriesKey":
                        dimensions[identifier] = value
                    elif identifier in ("TITLE_COMPL", "TITLE"):
                        series_title = value
                    elif identifier == "UNIT":
                        unit = value
            for obs in (node for node in series.iter() if local(node.tag) == "Obs"):
                period = next((x.attrib.get("value") for x in obs if local(x.tag) == "ObsDimension"), None)
                value_raw = next((x.attrib.get("value") for x in obs if local(x.tag) == "ObsValue"), None)
                try:
                    value = float(value_raw)
                except (TypeError, ValueError):
                    continue
                if not period or not re.match(r"^20\d\d-\d\d(?:-\d\d)?$", period):
                    continue
                observation_period = period[:10] + ("-01" if len(period) == 7 else "")
                obs_attrs = {x.attrib.get("id"): x.attrib.get("value")
                             for attrs in obs if local(attrs.tag) == "Attributes"
                             for x in attrs if local(x.tag) == "Value"}
                observations.append({"dataflow": dataflow, "dimensions": dimensions.copy(),
                    "observation_period": observation_period, "period_raw": period,
                    "value": value, "title": series_title or dataflow, "unit": unit,
                    "updated_at_raw": updated_raw or "DAY_ONLY", "updated_at_utc": updated_utc,
                    "obs_status": obs_attrs.get("OBS_STATUS"), "obs_conf": obs_attrs.get("OBS_CONF"),
                    "source_url": url})
    return observations


def _repair_whitehouse_cached_clock(db):
    cur=db.cursor()
    cur.execute(f"SELECT * FROM `{TABLE}` WHERE source_id='WHITEHOUSE_TRADE' AND record_kind='ORIGINAL_DOCUMENT'")
    fields=[field[0] for field in cur.description];values=cur.fetchall();cur.close()
    for values_row in values:
        row=dict(zip(fields,values_row));body=Path(row['raw_blob_path']).read_bytes()
        if hashlib.sha256(body).hexdigest()!=row['content_sha256']:
            raise ValueError('WHITEHOUSE_CACHED_ORIGINAL_SHA_MISMATCH')
        ex=json.loads(row['extra_json'] or '{}')
        row['extra_json']=json.dumps({**ex,**_modified_clock_evidence(body)})
        common.store(db,row)
    return len(values)


def _collect_whitehouse_trade(db, now):
    source='WHITEHOUSE_TRADE';key='control:whitehouse-archive-pages'
    cur=db.cursor()
    cur.execute(f"SELECT extra_json FROM `{TABLE}` WHERE source_id=%s AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",(source,key))
    prior=cur.fetchone();cur.close()
    state=json.loads(prior[0]) if prior else {}
    if state.get('metadata_revision')!=1:
        _repair_whitehouse_cached_clock(db)
    complete=state.get('state')=='COMPLETE'
    if state.get('scope_revision')!=2:
        complete=False
        state['next_page']=1
    last_sweep=state.get('last_full_archive_completed_utc')
    if complete and last_sweep and (now-datetime.fromisoformat(last_sweep)).total_seconds()>=7*86400:
        complete=False
        state['next_page']=1
    failed=set(state.get('failed_pages',[]))
    page=1 if complete else int(state.get('next_page',1))
    url='https://www.whitehouse.gov/presidential-actions/' + (f'page/{page}/' if page>1 else '')
    last_recovery=state.get('last_recovery_check_utc')
    recovery_page=min(failed) if complete and failed and (not last_recovery or (now-datetime.fromisoformat(last_recovery)).total_seconds()>=86400) else None
    urls=[url]
    if recovery_page and recovery_page!=1:urls.append(f'https://www.whitehouse.gov/presidential-actions/page/{recovery_page}/')
    result=_collect_source(db,source,urls,now)
    result.update(lane='checkpointed-policy-archive',archive_page=page)
    if not result['indices']:
        result['status']=common._collector_outcome(result)
        return result
    maximum=max(page,int(result.get('archive_max_page',page)),int(state.get('last_page_seen',1)))
    if result['errors'] or result['blocked']:
        if not recovery_page:failed.add(page)
    else: failed.discard(page)
    if recovery_page and not result['errors'] and not result['blocked']:failed.discard(recovery_page)
    # Historical page traversal is independent of missing-document recovery.
    # Last-page success proves traversal, never proves calendar completeness.
    next_page=page+1
    done=page>=maximum or complete
    updated={'state':'COMPLETE' if done else 'INCOMPLETE','next_page':1 if done else next_page,
             'metadata_revision':1,
             'scope_revision':2,
             'last_recovery_check_utc':now.isoformat() if recovery_page else last_recovery,
             'last_page_seen':maximum,'last_checked_utc':now.isoformat(),
             'last_full_archive_completed_utc':(now.isoformat() if done and not complete else last_sweep),
             'failed_pages':sorted(failed),'history_start':START.isoformat(),
             'scope':'all presidential actions filtered for trade significance',
             'calendar_completeness_verified':False}
    payload=b'{"checkpoint_schema":1,"source":"WHITEHOUSE_TRADE"}'
    row=common.base_row(source,key,'RUN_CONTROL','brain://model100/whitehouse-archive-pages',payload,
        {'status':0,'content_type':'application/json'},now,None,'OBSERVED','RUN_CONTROL',
        event_type='BACKFILL_CHECKPOINT',extra_json=updated)
    common.store(db,row)
    result['checkpoint']=updated
    result['status']=('BACKFILL_INCOMPLETE' if not done else 'PARTIAL_COVERAGE' if failed else
                      'SUCCESS_NEW_DATA' if result['new_document_versions'] else 'SUCCESS_UNCHANGED')
    return result


def collect_source(db, source, now=None):
    """Run one independently scheduled P1 source without coupling its peers."""
    now = now or _now()
    if source not in P1_SOURCES:
        raise ValueError("UNKNOWN_P1_SOURCE")
    if source in LICENSE_PROBES:
        probe = _license_probe(db, source, LICENSE_PROBES[source], now)
        return {"source": source, "license_probe": probe, "documents": 0,
                "classification": probe["classification"]}
    if source == "ECB_EDP_VINTAGE":
        return collect_ecb_history_api(db, now)
    if source == "USTR_TRADE":
        return _collect_ustr_trade(db, now)
    if source == 'WHITEHOUSE_TRADE':
        return _collect_whitehouse_trade(db,now)
    if source == "CENSUS_RETAIL":
        control = _collect_census_retail_api_control(db, now)
        indices = _collect_source(db, source, P1_SOURCES[source], now)
        return {"source": source, "api_control": control, "index_checks": indices,
                "classification": 'REQUIRES_API_KEY' if control.get('status')=='REQUIRES_API_KEY' else 'PARTIAL_COVERAGE',
                "errors": control["errors"] + indices.get("errors", [])}
    if source == "ZEW":
        tables = _collect_zew_monthly_tables(db, now)
        indices = (None if tables.get("stopped_after_consecutive_timeouts")
                   else _collect_source(db, source, P1_SOURCES[source], now))
        tables["indices"] = indices
        return tables
    if source == "CLEVELAND_NOW":
        url = P1_SOURCES[source][0]
        current_page = _collect_cleveland_current_page(db, now)
        # This is a bundled historical archive; allow a longer transfer window
        # than the lightweight index polls, while remaining bounded.
        try:
            body, meta = common.conditional_fetch(db, source, "official-chart-export", url,
                                                 timeout=60, cap=20_000_000)
        except Exception as exc:
            status = exc.code if isinstance(exc, urllib.error.HTTPError) else getattr(exc, 'response_status', 0)
            response_body = getattr(exc, 'partial_body', b'')
            headers = getattr(exc, 'response_headers', {})
            if isinstance(exc, urllib.error.HTTPError):
                headers = {k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])}
                try:
                    response_body = exc.read(2_000_000)
                except Exception:
                    pass
            failure_meta = {"status": status, "url": url,
                            "content_type": headers.get("content-type"),
                            "fetch_status": "INCOMPLETE_RESPONSE" if isinstance(exc, common.IncompleteSourceResponse) else "HTTP_" + str(status) if status else "NO_HTTP_STATUS"}
            _save(db, source, "official-chart-export:fetch-status", url, response_body,
                  failure_meta, now, kind="FETCH_STATUS", title="Cleveland Fed archive fetch status",
                  event="SOURCE_FETCH_STATUS", asof="FETCH_STATUS", evidence="FETCH_STATUS",
                  extra={"error_type": type(exc).__name__, "error": str(exc)[:240],
                         "http_status_present": bool(status), "http_headers": headers,
                         "response_complete": False, "response_bytes": len(response_body)})
            return {"source": source, "current_page": current_page,
                    "archive_http_status": status or None,
                    "archive_error": f"{type(exc).__name__}:{str(exc)[:160]}",
                    "historical_snapshots": False}
        _save(db, source, "official-chart-export", url, body, meta, now,
              kind="ORIGINAL_ARCHIVE", title="Cleveland Fed official inflation chart export",
              event="OFFICIAL_CHART_EXPORT", asof="DAY_ONLY", evidence="DAY_ONLY",
              extra={"archive_format": "ZIP", "publication_clock": "DAY_ONLY",
                     "source_page": "https://www.clevelandfed.org/en/center-for-inflation-research/inflation-charting"})
        count = _store_cleveland_export(db, url, body, meta, now)
        return {"source": source, "current_page": current_page, "http_status": meta.get("status"),
                "response_bytes": meta.get("network_response_bytes", len(body)),
                "sha256": hashlib.sha256(body).hexdigest(), "historical_chart_values": count,
                "publication_clock": "DAY_ONLY"}
    return _collect_source(db, source, P1_SOURCES[source], now)


def collect_p1(db, now=None):
    now = now or _now()
    results = []
    # P1 sources in the supplied order. PMI collectors are probes only until
    # access and rights have been positively confirmed.
    for source in ("ZEW", "SP_GLOBAL_FLASH_PMI", "ISM_PMI", "CLEVELAND_NOW",
                   "CENSUS_RETAIL", "ECB_EDP_VINTAGE", "WHITEHOUSE_TRADE",
                   "USTR_TRADE", "NYFED_NOW"):
        results.append(collect_source(db, source, now))
    return results


def main(argv=None):
    import sys
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) < 6:
        raise SystemExit("usage: model100_primary_p1.py TABLE HOST PORT USER PASSWORD DATABASE")
    table, host, port, user, password, database = argv[:6]
    if table != TABLE:
        raise SystemExit("REGISTERED_TARGET_MISMATCH")
    import mysql.connector
    db = mysql.connector.connect(host=host, port=int(port), user=user, password=password,
                                 database=database, connection_timeout=15, autocommit=False)
    try:
        common.ensure_schema(db)
        print(json.dumps(collect_p1(db), ensure_ascii=False, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
