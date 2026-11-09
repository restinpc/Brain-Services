#!/usr/bin/env python3
"""Incremental archive collectors for Model100 official release documents.

Uses the shared B1 table and content-addressed raw store from
model100_primary_sources.py. Historical source dates are kept distinct from
the time this B1 collector first received the archived file.
"""
from __future__ import annotations

import hashlib
import html
import json
import re
import time
from fractions import Fraction
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, date, time as dtime, timezone, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import model100_primary_sources as common
except ImportError:
    from work import model100_primary_sources as common

TABLE = common.TABLE
PARSER_VERSION = "model100-primary-archive-v8"
BACKFILL_START = date(2025, 1, 1)
NOW = timezone.utc

ECB_INDEX = "https://www.ecb.europa.eu/press/govcdec/mopo/html/index.en.html"
FED_INDEX = "https://www.federalreserve.gov/newsevents/pressreleases/{year}-press-fomc.htm"
FED_MONETARY_RSS = "https://www.federalreserve.gov/feeds/press_monetary.xml"
FED_MEETING_SCHEDULE = 'https://www.federalreserve.gov/newsevents/pressreleases/monetary20240809a.htm'
BEA_SCHEDULE = "https://www.bea.gov/news/schedule/full-{year}"
BEA_SCHEDULE_JSON = "https://apps.bea.gov/API/signup/release_dates.json"
BEA_RSS = "https://apps.bea.gov/rss/rss.xml"
EUROSTAT_SEARCH_PORTLET = "estatsearchportlet_WAR_estatsearchportlet_INSTANCE_bHVzuvn1SZ8J"
BLS_PROBES = {
    "cpi": "https://www.bls.gov/bls/news-release/cpi.htm",
    "employment": "https://www.bls.gov/bls/news-release/empsit.htm",
    "calendar": "https://www.bls.gov/schedule/news_release/bls.ics",
    "cpi_2025_sample": "https://www.bls.gov/news.release/archives/cpi_09112025.htm",
    # BLS v1 is an official no-registration endpoint. These series are stored
    # only as latest-revised controls because the API does not expose release
    # vintages or publication timestamps.
    "api_cpi": "https://api.bls.gov/publicAPI/v1/timeseries/data/CUSR0000SA0",
    "api_employment": "https://api.bls.gov/publicAPI/v1/timeseries/data/CES0000000001",
    "api_ppi": "https://api.bls.gov/publicAPI/v1/timeseries/data/WPUFD4",
}
BLS_KNOWN_NOT_PUBLISHED = [
    {"series": "CPI", "observation_period": "2025-10-01",
     "status": "NOT_PUBLISHED", "reason": "BLS October 2025 shutdown-related nonpublication per supplied specification"},
    {"series": "EMPLOYMENT_SITUATION", "observation_period": "2025-10-01",
     "status": "NOT_PUBLISHED", "reason": "BLS October 2025 shutdown-related nonpublication per supplied specification"},
]


def _utcnow() -> datetime:
    return datetime.now(NOW).replace(tzinfo=None)


def _utc_aware(value: datetime) -> datetime:
    """Interpret collector datetimes as UTC and make comparisons explicit."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _read(url: str, timeout: int = 20, cap: int = 8_000_000):
    return common.download(url, timeout=timeout, cap=cap)


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self._href = None
        self._text = []
        self._last_date = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag.lower() == "a" and attrs.get("href"):
            self._href = attrs["href"]
            self._text = []

    def handle_data(self, data):
        s = " ".join(data.split())
        m = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", s)
        if not m:
            m = re.search(r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(20\d{2})\b", s, re.I)
        if m:
            try:
                if "/" in m.group(0):
                    self._last_date = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
                else:
                    self._last_date = date(int(m.group(3)), datetime.strptime(m.group(1), "%B").month, int(m.group(2)))
            except ValueError:
                pass
        if self._href is not None and s:
            self._text.append(s)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, " ".join(self._text).strip(), self._last_date))
            self._href = None
            self._text = []


class DocumentHeading(HTMLParser):
    """Read H1 or HTML title when an index link contains no useful heading."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._tag = None
        self._parts = {"h1": [], "title": []}

    def handle_starttag(self, tag, attrs):
        if tag in self._parts and self._tag is None:
            self._tag = tag

    def handle_endtag(self, tag):
        if tag == self._tag:
            self._tag = None

    def handle_data(self, data):
        if self._tag and data.strip():
            self._parts[self._tag].append(data.strip())

    def heading(self):
        for key in ("h1", "title"):
            value = " ".join(self._parts[key]).strip()
            if value:
                return re.sub(r"\s+", " ", value)
        return None


class TableRows(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self._row = None
        self._cell = None
        self._link = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        t = tag.lower()
        if t == "tr":
            self._row = {"cells": [], "links": []}
        elif t in ("td", "th") and self._row is not None:
            self._cell = []
        elif t == "a" and self._row is not None and attrs.get("href"):
            self._link = (attrs["href"], [])
        elif t == 'time' and self._row is not None and attrs.get('datetime'):
            self._row['published_at'] = common.parse_explicit_datetime(attrs['datetime'])

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)
        if self._link is not None:
            self._link[1].append(data)

    def handle_endtag(self, tag):
        t = tag.lower()
        if t in ("td", "th") and self._row is not None and self._cell is not None:
            self._row["cells"].append(" ".join(" ".join(self._cell).split()))
            self._cell = None
        elif t == "a" and self._row is not None and self._link is not None:
            self._row["links"].append((self._link[0], " ".join(" ".join(self._link[1]).split())))
            self._link = None
        elif t == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


def _store_document(db, source: str, key: str, url: str, body: bytes, meta: dict,
                    now: datetime, title: str, event_type: str, *,
                    pub_at: datetime | None = None, evidence: str = "DAY_ONLY",
                    asof: str = "DAY_ONLY", region: str | None = None,
                    extra: dict | None = None):
    row = common.base_row(source, key, "ORIGINAL_DOCUMENT", url, body, meta, now,
                          pub_at, evidence, asof, title=title, event_type=event_type,
                          region=region,
                          extra_json={"document_date": (extra or {}).get("document_date"),
                                      "historical_backfill": True,
                                      **(extra or {})})
    row["parser_version"] = PARSER_VERSION
    common.store(db, row)


def _save_index(db, source: str, key: str, url: str, body: bytes, meta: dict, now: datetime):
    row = common.base_row(source, key, "ORIGINAL_INDEX", url, body, meta, now,
                          None, "DAY_ONLY", "DAY_ONLY",
                          title=key, event_type="ARCHIVE_INDEX",
                          extra_json={"index_fetch_utc": now.isoformat()})
    row["parser_version"] = PARSER_VERSION
    common.store(db, row)


def _record_fetch_status(db, source: str, key: str, url: str, now: datetime,
                        status: int | None, error: str, headers: dict | None = None):
    payload = json.dumps({"url": url, "http_status": status, "error": error,
                          "headers": headers or {}, "observed_at_utc": now.isoformat()},
                         sort_keys=True, ensure_ascii=False).encode("utf-8")
    digest, path = common.preserve(source, payload)
    meta = {"status": status or 0, "content_type": "application/json", "url": url}
    row = common.base_row(source, "fetch-status:" + key, "FETCH_STATUS", url,
                          payload, meta, now, None, "OBSERVED", "FETCH_STATUS",
                          title=f"Fetch status {status or 'NO_HTTP_STATUS'}",
                          event_type="FETCH_STATUS",
                          extra_json={"error": error, "response_headers": headers or {}})
    row["content_sha256"] = digest
    row["raw_blob_path"] = path
    row["parser_version"] = PARSER_VERSION
    common.store(db, row)


def _parse_date_from_ecb_url(url: str):
    m = re.search(r"ecb\.mp(\d{2})(\d{2})(\d{2})", url)
    if not m:
        return None
    try:
        return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _fed_release_date(path: str):
    match = re.search(r"(\d{4})(\d{2})(\d{2})[a-z]?\.htm$", path)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def parse_fed_monetary_rss(body: bytes, now: datetime):
    """Parse the official Fed monetary RSS without treating naive dates as UTC."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    items = []
    for item in (node for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "item"):
        fields = {node.tag.rsplit("}", 1)[-1]: (node.text or "").strip() for node in item}
        url = fields.get("link", "")
        if not url.startswith("https://www.federalreserve.gov/newsevents/pressreleases/"):
            continue
        path = urllib.parse.urlparse(url).path
        if not path.endswith(".htm"):
            continue
        title = fields.get("title", "").strip()
        if not title or not re.search(r"FOMC|monetary policy|federal reserve issues", title, re.I):
            continue
        raw_date = fields.get("pubDate", "")
        try:
            published = parsedate_to_datetime(raw_date)
        except (TypeError, ValueError, OverflowError):
            continue
        if published.tzinfo is None:
            continue
        published = published.astimezone(timezone.utc)
        release_date = _fed_release_date(path) or published.date()
        if not BACKFILL_START <= release_date <= now.date():
            continue
        items.append({"url": url.split("?", 1)[0], "title": title,
                      "published_at": published.replace(tzinfo=None),
                      "pub_date_raw": raw_date, "release_date": release_date})
    return sorted({item["url"]: item for item in items}.values(),
                  key=lambda item: (item["published_at"], item["url"]))


def _release_clock_evidence(body: bytes, release_date: date | None, tz_name: str):
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = " ".join(text.split())
    patterns = (
        r"(?:(?:Embargoed\s+until\s+)?For\s+release|Embargoed\s+until\s+release)\s+(?:at\s+)?(\d{1,2}:\d{2})\s*([ap])\.?m\.?\s*(EDT|EST|ET)",
        r"(?:(?:Embargoed\s+until\s+)?For\s+release|Embargoed\s+until\s+release)\s+(?:at\s+)?(\d{1,2}:\d{2})\s*(EDT|EST|ET)",
    )
    for i, pattern in enumerate(patterns):
        m = re.search(pattern, text, re.I)
        if not m or not release_date:
            continue
        try:
            clock = m.group(1)
            hour, minute = map(int, clock.split(":"))
            if i == 0:
                if m.group(2).lower() == "p" and hour != 12:
                    hour += 12
                elif m.group(2).lower() == "a" and hour == 12:
                    hour = 0
                zone_token = m.group(3).upper()
            else:
                zone_token = m.group(2).upper()
            zone = ZoneInfo("America/New_York") if zone_token == "ET" else timezone.utc
            if zone_token in ("EST", "EDT"):
                zone = timezone.utc
                offset = -5 if zone_token == "EST" else -4
                local = datetime.combine(release_date, dtime(hour, minute), tzinfo=timezone.utc)
                utc_value = local - timedelta(hours=offset)
                return utc_value.replace(tzinfo=None), "DOCUMENTED_CLOCK", zone_token
            local = datetime.combine(release_date, dtime(hour, minute), tzinfo=zone)
            return local.astimezone(timezone.utc).replace(tzinfo=None), "DOCUMENTED_CLOCK", tz_name
        except (ValueError, KeyError):
            pass
    return None, "DAY_ONLY", None


def _store_bea_trade_values(db, url: str, title: str, body: bytes, meta: dict,
                            publication_date: date, published_at: datetime | None,
                            clock_evidence: str, now: datetime, asof_override=None):
    """Normalize headline BEA trade values from their archived original HTML."""
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = " ".join(text.split())
    match = re.search(
        r"(?:U\.S\.\s+)?International Trade in Goods and Services,\s*"
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(?:and Annual\s+)?(20\d{2})",
        title, re.I)
    if not match:
        return 0
    try:
        observation = date(int(match.group(2)), datetime.strptime(match.group(1), "%B").month, 1)
    except ValueError:
        return 0
    metrics = (
        ("TRADE_DEFICIT", "Deficit"),
        ("TRADE_EXPORTS", "Exports"),
        ("TRADE_IMPORTS", "Imports"),
    )
    parsed = []
    for metric, label in metrics:
        value = re.search(rf"\b{label}:\s*\$([\d,]+(?:\.\d+)?)\s*Billion\b", text, re.I)
        if value:
            parsed.append((metric, observation, float(value.group(1).replace(",", "")), "FIRST_RELEASE"))
    revised = re.search(
        r"(?:up|down)\s+\$[\d,.]+\s+Billion\s+from\s+\$([\d,.]+)\s+Billion\s+in\s+"
        r"(January|February|March|April|May|June|July|August|September|October|November|December),?\s+revised",
        text, re.I)
    if revised:
        try:
            revised_month = datetime.strptime(revised.group(2), "%B").month
            revised_period = date(observation.year - int(revised_month >= observation.month), revised_month, 1)
            parsed.append(("TRADE_DEFICIT", revised_period,
                           float(revised.group(1).replace(",", "")), "REVISED_PRIOR_PERIOD"))
        except ValueError:
            pass
    asof = asof_override or ("HISTORICAL_DOCUMENTED" if published_at and clock_evidence == "DOCUMENTED_CLOCK" else "DAY_ONLY")
    for metric, period, value, value_kind in parsed:
        key = f"value:{period.isoformat()}:{metric}:{publication_date.isoformat()}:{value_kind}"
        row = common.base_row(
            "BEA_RELEASES", key, "FIRST_ESTIMATE" if value_kind == "FIRST_RELEASE" else "REVISION",
            url, body, meta, now, published_at,
            clock_evidence if published_at else "DAY_ONLY", asof,
            title=title, event_type="BEA_" + metric, region="UNITED_STATES",
            observation_period_start=period.isoformat(),
            vintage_month=publication_date.replace(day=1).isoformat(),
            numeric_value=value, unit="USD_BILLION",
            extra_json={"value_kind": value_kind, "source_metric": metric,
                        "publication_date": publication_date.isoformat(),
                        "publication_timezone": "America/New_York",
                        "revision_scope": "prior_observation" if value_kind == "REVISED_PRIOR_PERIOD" else None})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
    return len(parsed)


def _store_bea_macro_values(db, url: str, title: str, body: bytes, meta: dict,
                            publication_date: date, published_at: datetime | None,
                            clock_evidence: str, now: datetime, asof_override=None):
    """Extract GDP and PCE headline values while keeping estimate vintages explicit."""
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = " ".join(text.split())
    values = []
    gdp = re.search(
        r"Real gross domestic product\s*\(GDP\)\s+(?:increased|decreased) at an annual rate of\s+"
        r"(\d+(?:\.\d+)?)\s+percent in the\s+(first|second|third|fourth)\s+quarter of\s+(20\d{2})",
        text, re.I)
    if not gdp:
        gdp = re.search(
            r"Real GDP (?:increased|decreased) at an annual rate of\s+(\d+(?:\.\d+)?)\s+percent\s+"
            r"in the\s+(first|second|third|fourth)\s+quarter of\s+(20\d{2})",
            text, re.I)
    if gdp:
        quarter_month = {"first": 1, "second": 4, "third": 7, "fourth": 10}[gdp.group(2).lower()]
        observation = date(int(gdp.group(3)), quarter_month, 1)
        stage_match = re.search(r"\b(Initial|Advance|Second|Third|Updated) Estimate\b", title + " " + text[:700], re.I)
        stage = stage_match.group(1).upper() if stage_match else "UNSPECIFIED"
        values.append(("BEA_GDP_REAL_QOQ_ANNUALIZED", observation,
                       float(gdp.group(1)) * (-1 if 'decreased' in gdp.group(0).lower() else 1), "PERCENT_ANNUALIZED", stage))

    pce_section = re.search(r"From the same month one year ago,\s+(.*?)(?:Personal Income and Related Measures|$)",
                            text, re.I)
    if pce_section:
        section = pce_section.group(1)
        pce_period = re.search(
            r"Personal Income and Outlays,?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})",
            title, re.I)
        pce = re.search(
            r"the PCE price index\s+(?:for\s+)?(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
            r"(?:increased|was)\s+(\d+(?:\.\d+)?)\s+percent",
            "From the same month one year ago, " + section, re.I)
        if pce:
            try:
                observation = date(int(pce_period.group(2)) if pce_period else publication_date.year,
                                   datetime.strptime(pce.group(1), "%B").month, 1)
                values.append(("BEA_PCE_PRICE_YOY", observation, float(pce.group(2)),
                               "PERCENT_YOY", "MONTHLY_RELEASE"))
            except ValueError:
                pass
        core = re.search(
            r"Excluding food and energy,\s+the PCE price index increased\s+(\d+(?:\.\d+)?)\s+percent from one year ago",
            section, re.I)
        if core and pce:
            values.append(("BEA_CORE_PCE_PRICE_YOY", observation, float(core.group(1)),
                           "PERCENT_YOY", "MONTHLY_RELEASE"))
        # The shutdown-delayed October/November 2025 release contains two
        # actual observation months. Never substitute its January 2026 date.
        paired = re.search(r'Personal Income and Outlays,?\s+(October) and (November)\s+(2025)',title,re.I)
        headline = re.search(r'the PCE price index increased\s+(\d+(?:\.\d+)?) percent in October, followed by an increase of\s+(\d+(?:\.\d+)?) percent in November',section,re.I)
        paired_core = re.search(r'Excluding food and energy,\s+the PCE price index also increased\s+(\d+(?:\.\d+)?) percent followed by an increase of\s+(\d+(?:\.\d+)?) percent',section,re.I)
        if paired and headline and paired_core:
            for index, month in enumerate((10,11),1):
                for metric, match in (('BEA_PCE_PRICE_YOY',headline),('BEA_CORE_PCE_PRICE_YOY',paired_core)):
                    values.append((metric,date(2025,month,1),float(match.group(index)),
                                   'PERCENT_YOY','MONTHLY_RELEASE'))

    asof = asof_override or ("HISTORICAL_DOCUMENTED" if published_at and clock_evidence == "DOCUMENTED_CLOCK" else "DAY_ONLY")
    for metric, observation, value, unit, stage in values:
        key = f"value:{observation.isoformat()}:{metric}:{publication_date.isoformat()}:{stage}"
        row = common.base_row(
            "BEA_RELEASES", key, "RELEASE_VALUE", url, body, meta, now, published_at,
            clock_evidence if published_at else "DAY_ONLY", asof,
            title=title, event_type=metric, region="UNITED_STATES",
            observation_period_start=observation.isoformat(),
            vintage_month=publication_date.replace(day=1).isoformat(),
            numeric_value=value, unit=unit,
            extra_json={"value_kind": "RELEASE_ACTUAL", "estimate_stage": stage,
                        "publication_date": publication_date.isoformat(),
                        "publication_timezone": "America/New_York"})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
    return len(values)


def parse_bea_rss_items(body: bytes, now: datetime):
    """Read current BEA releases only when the official RSS pubDate has a timezone."""
    root = ET.fromstring(body)
    out = {}
    for item in root.findall(".//item"):
        title = " ".join((item.findtext("title") or "").split())
        url = (item.findtext("link") or "").strip()
        raw_pub = (item.findtext("pubDate") or "").strip()
        if not title or not url or not raw_pub:
            continue
        if not re.search(r"^(?:U\.S\.\s+International Trade in Goods and Services,|Personal Income and Outlays,|Gross Domestic Product,|GDP\s*\()", title, re.I):
            continue
        if re.search(r"\b(?:by (?:County|State|Industry|Metropolitan)|Puerto Rico)\b", title, re.I):
            continue
        host = (urllib.parse.urlparse(url).hostname or "").lower()
        if host not in ("www.bea.gov", "bea.gov", "apps.bea.gov"):
            continue
        try:
            parsed = parsedate_to_datetime(raw_pub)
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed is None or parsed.tzinfo is None:
            continue
        published_aware = parsed.astimezone(timezone.utc)
        if published_aware.date() < BACKFILL_START or published_aware > _utc_aware(now):
            continue
        published = published_aware.replace(tzinfo=None)
        out[url] = {"title": title, "url": url, "published_at": published,
                    "pub_date_raw": raw_pub}
    return sorted(out.values(), key=lambda x: (x["published_at"], x["url"]))


def _download_original(db, source: str, url: str, title: str, event_type: str,
                       release_date: date | None, now: datetime, region: str,
                       source_timezone: str, extra: dict | None = None,
                       published_hint: datetime | None = None):
    if release_date and release_date < BACKFILL_START:
        return "OUT_OF_SCOPE"
    if release_date and release_date > now.date():
        return "FUTURE_EXCLUDED"
    key = urllib.parse.urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    if not key:
        key = hashlib.sha256(url.encode()).hexdigest()[:24]
    try:
        # The persisted ORIGINAL_DOCUMENT key is the canonical slug (without
        # a document: prefix); use that same key to recover its ETag/body.
        body, meta = common.conditional_fetch(db, source, key, url, timeout=20, cap=8_000_000)
    except urllib.error.HTTPError as e:
        response_headers = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        response_body = e.read(2_000_000)
        if response_body:
            blocked_meta = {"status": e.code, "content_type": response_headers.get("content-type"),
                            "url": url}
            _store_document(db, source, "blocked:" + key, url, response_body, blocked_meta, now,
                            title, "BLOCKED_ACCESS_RESPONSE", region=region,
                            extra={"document_date": release_date.isoformat() if release_date else None,
                                   "error_status": e.code})
        _record_fetch_status(db, source, key, url, now, e.code, str(e), response_headers)
        return "BLOCKED_ACCESS" if e.code in (401, 403, 407) else f"HTTP_{e.code}"
    except Exception as e:
        _record_fetch_status(db, source, key, url, now, None,
                             type(e).__name__ + ":" + str(e)[:150])
        return type(e).__name__ + ":" + str(e)[:100]
    if not body:
        return "EMPTY_BODY"
    heading = DocumentHeading()
    heading.feed(body.decode("utf-8", "replace"))
    parsed_title = heading.heading()
    if parsed_title and (not title or title.strip().lower() in ("view", "read more", "browse page")):
        title = parsed_title
    pub, evidence, actual_zone = _release_clock_evidence(body, release_date, source_timezone)
    if published_hint is not None:
        if pub is not None and _utc_aware(pub) != _utc_aware(published_hint):
            extra={**(extra or {}),'publication_clock_conflict':True,
                   'article_clock_utc':pub.isoformat(),'index_or_rss_clock_utc':published_hint.isoformat()}
            pub,evidence=None,'CONFLICTING_CLOCKS'
        else:
            pub, evidence, actual_zone = published_hint, "DOCUMENTED_CLOCK", source_timezone
    asof = "HISTORICAL_DOCUMENTED" if pub else "DAY_ONLY"
    received=common.parse_explicit_datetime(meta.get('received_at_utc')) or now
    if pub and published_hint and (extra or {}).get('live_poll') and 0<=(received-pub).total_seconds()<=300:
        asof='CERTIFIED_LIVE'
    _store_document(db, source, key, url, body, meta, now, title, event_type,
                    pub_at=pub, evidence=evidence, asof=asof, region=region,
                    extra={"document_date": release_date.isoformat() if release_date else None,
                           "source_timezone": actual_zone or source_timezone,
                           **(extra or {})})
    if source == "FED_FOMC" and release_date:
        _store_fed_target_range(db, url, title, body, meta, release_date,
                                pub, evidence, asof, now)
    if source == "BEA_RELEASES" and release_date:
        _store_bea_trade_values(db, url, title, body, meta, release_date,
                                pub, evidence, now, asof_override=asof)
        _store_bea_macro_values(db, url, title, body, meta, release_date,
                                pub, evidence, now, asof_override=asof)
    time.sleep(0.2)
    return "STORED"


def collect_ecb_history(db, now: datetime):
    source = "ECB_HIST"
    result = {"source": source, "start": BACKFILL_START.isoformat(), "indices": 0,
              "documents": 0, "errors": []}
    try:
        body, meta = common.conditional_fetch(db, source, "index:decisions", ECB_INDEX, timeout=20, cap=8_000_000)
    except Exception as e:
        status = e.code if isinstance(e, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
        _record_fetch_status(db, source, "index:decisions", ECB_INDEX, now, status, str(e), headers)
        result["errors"].append(f"INDEX:{type(e).__name__}:{str(e)[:100]}")
        return result
    result["index_http_status"] = meta.get("status")
    result["index_bytes"] = len(body)
    result["index_sha256"] = hashlib.sha256(body).hexdigest()
    _save_index(db, source, "index:decisions", ECB_INDEX, body, meta, now)
    page = LinkParser()
    page.feed(body.decode("utf-8", "replace"))
    candidates = {}
    for href, title, _ in page.links:
        url = urllib.parse.urljoin(ECB_INDEX, href)
        if "ecb.europa.eu/press/pr/date/" not in url or "ecb.mp" not in url:
            continue
        d = _parse_date_from_ecb_url(url)
        if d and BACKFILL_START <= d <= now.date():
            candidates[url.split("?", 1)[0]] = (title or "ECB monetary policy decision", d)
    result["candidates"] = len(candidates)
    for url, (title, d) in sorted(candidates.items()):
        state = _download_original(db, source, url, title, "MONETARY_POLICY_DECISION",
                                   d, now, "EURO_AREA", "Europe/Luxembourg",
                                   {"decision_scheduled_local": "14:15"})
        if state in ("STORED", "UNCHANGED_304"):
            result["documents"] += 1
        elif state not in ("OUT_OF_SCOPE", "FUTURE_EXCLUDED"):
            result["errors"].append({"url": url, "status": state})
    return result


def parse_fed_meeting_schedule(body, today):
    text = ' '.join(html.unescape(re.sub(r'<[^>]+>', ' ', body.decode('utf-8','replace'))).split())
    dates = set()
    for section in re.finditer(r'For\s+(2025|2026):\s*(.*?)(?=For\s+20\d{2}:|The Committee releases|$)', text, re.S):
        year = int(section.group(1))
        for meeting in re.finditer(r'Wednesday,\s+([A-Z][a-z]+)\s+(\d{1,2})(?:,\s*(20\d{2}))?', section.group(2)):
            day = date(int(meeting.group(3) or year), datetime.strptime(meeting.group(1),'%B').month,int(meeting.group(2)))
            if BACKFILL_START <= day <= today:
                dates.add(day)
    return sorted(dates)


def collect_fed_fomc(db, now: datetime):
    source = "FED_FOMC"
    result = {"source": source, "start": BACKFILL_START.isoformat(), "indices": 0,
              "documents": 0, "errors": []}
    candidates = {}
    try:
        schedule_body,schedule_meta=common.conditional_fetch(db,source,'official-meeting-schedule',FED_MEETING_SCHEDULE,timeout=15)
        _save_index(db,source,'official-meeting-schedule',FED_MEETING_SCHEDULE,schedule_body,schedule_meta,now)
        expected=parse_fed_meeting_schedule(schedule_body,now.date())
        result['expected_decisions']=len(expected)
        for day in expected:
            scheduled=datetime.combine(day,dtime(14,0),tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc)
            row=common.base_row(source,'scheduled-decision:'+day.isoformat(),'SCHEDULED_RELEASE',
                FED_MEETING_SCHEDULE,schedule_body,schedule_meta,now,None,'SCHEDULED','SCHEDULED',
                title='FOMC scheduled policy decision',event_type='FOMC_DECISION_SCHEDULE',region='UNITED_STATES',
                extra_json={'meeting_date':day.isoformat(),'scheduled_at_utc':scheduled.isoformat(),
                            'scheduled_clock_source':'official 2025/2026 meeting schedule announcement',
                            'schedule_does_not_prove_actual_publication':True})
            common.store(db,row)
    except Exception as exc:
        result['errors'].append({'url':FED_MEETING_SCHEDULE,'status':type(exc).__name__})
    for year in (2025, 2026):
        url = FED_INDEX.format(year=year)
        try:
            body, meta = common.conditional_fetch(db, source, f"index:{year}", url, timeout=20, cap=4_000_000)
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, source, f"index:{year}", url, now, status, str(e), headers)
            result["errors"].append({"url": url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
            continue
        result["indices"] += 1
        _save_index(db, source, f"index:{year}", url, body, meta, now)
        page = LinkParser()
        page.feed(body.decode("utf-8", "replace"))
        for href, title, page_date in page.links:
            target = urllib.parse.urljoin(url, href)
            path = urllib.parse.urlparse(target).path
            if not path.endswith(".htm") or "/newsevents/pressreleases/" not in path:
                continue
            if re.search(r"\d{4}-press-fomc\.htm$", path):
                continue
            # Annual indexes include statements, projections and other FOMC
            # releases; the official page category labels them Monetary Policy.
            if not title or title.lower() in ("view", "read more"):
                continue
            d = _fed_release_date(path) or page_date
            if d and BACKFILL_START <= d <= now.date():
                candidates[target.split("?", 1)[0]] = (title, d)
    result["candidates"] = len(candidates)
    for url, (title, d) in sorted(candidates.items()):
        state = _download_original(db, source, url, title, "FOMC_RELEASE", d, now,
                                   "UNITED_STATES", "America/New_York")
        if state in ("STORED", "UNCHANGED_304"):
            result["documents"] += 1
        elif state not in ("OUT_OF_SCOPE", "FUTURE_EXCLUDED"):
            result["errors"].append({"url": url, "status": state})
    return result


def collect_fed_fomc_live(db, now: datetime):
    """Five-minute feed lane; archive index/backfill remains on its daily lane."""
    source = "FED_FOMC"
    result = {"source": source, "lane": "live-rss", "feed_url": FED_MONETARY_RSS,
              "rss_items": 0, "documents": 0, "errors": []}
    try:
        body, meta = common.conditional_fetch(
            db, source, "rss:press-monetary", FED_MONETARY_RSS,
            timeout=20, cap=2_000_000)
    except Exception as exc:
        status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
        _record_fetch_status(db, source, "rss:press-monetary", FED_MONETARY_RSS,
                             now, status, str(exc), headers)
        result["errors"].append({"url": FED_MONETARY_RSS,
                                 "status": f"{type(exc).__name__}:{str(exc)[:100]}"})
        return result
    _save_index(db, source, "rss:press-monetary", FED_MONETARY_RSS, body, meta, now)
    result["feed_http_status"] = meta.get("status")
    result["feed_bytes"] = meta.get("network_response_bytes", len(body))
    result["feed_sha256"] = hashlib.sha256(body).hexdigest()
    if meta.get("status") == 304:
        result["status"] = "UNCHANGED_304"
    items = parse_fed_monetary_rss(body, now)
    result["rss_items"] = len(items)
    cur=db.cursor()
    cur.execute(f"SELECT DISTINCT canonical_url FROM `{TABLE}` WHERE source_id='FED_FOMC' AND record_kind='ORIGINAL_DOCUMENT' AND http_status=200")
    known={r[0] for r in cur.fetchall()};cur.close()
    for item in items:
        if meta.get('status')==304 and item['url'] in known:continue
        if result['documents']>=6:
            result['status']='BACKFILL_INCOMPLETE';break
        state = _download_original(
            db, source, item["url"], item["title"], "FOMC_RELEASE",
            item["release_date"], now, "UNITED_STATES", "America/New_York",
            {"timestamp_source": "official Federal Reserve monetary RSS pubDate",
             "live_poll":True,
             "rss_pub_date_raw": item["pub_date_raw"],
             "rss_feed_url": FED_MONETARY_RSS,
             "rss_feed_sha256": result["feed_sha256"]},
            published_hint=item["published_at"])
        if state in ("STORED", "UNCHANGED_304"):
            result["documents"] += 1
            result['status']='SUCCESS_NEW_DATA' if state=='STORED' else 'SUCCESS_UNCHANGED'
        elif state not in ("OUT_OF_SCOPE", "FUTURE_EXCLUDED"):
            result["errors"].append({"url": item["url"], "status": state})
    if result['errors']:
        result['status']=common._collector_outcome({'errors':result['errors']})
    return result


def collect_bea_schedule_archive(db, now: datetime):
    source = "BEA_RELEASES"
    result = {"source": source, "start": BACKFILL_START.isoformat(), "indices": 0,
              "documents": 0, "rss_items": 0, "rss_documents": 0, "errors": []}
    candidates = {}
    for year in (2025, 2026):
        url = BEA_SCHEDULE.format(year=year)
        try:
            body, meta = common.conditional_fetch(db, source, f"schedule:{year}", url, timeout=25, cap=10_000_000)
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, source, f"schedule:{year}", url, now, status, str(e), headers)
            result["errors"].append({"url": url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
            continue
        result["indices"] += 1
        _save_index(db, source, f"schedule:{year}", url, body, meta, now)
        rows = TableRows()
        rows.feed(body.decode("utf-8", "replace"))
        for row in rows.rows:
            cells = " | ".join(row["cells"])
            m = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\b", cells, re.I)
            if not m:
                continue
            try:
                d = date(year, datetime.strptime(m.group(1), "%B").month, int(m.group(2)))
            except ValueError:
                continue
            if not BACKFILL_START <= d <= now.date():
                continue
            for href, text in row["links"]:
                target = urllib.parse.urljoin(url, href)
                if "bea.gov/news" in target and (".pdf" not in target.lower()):
                    candidates[target] = (text or cells, d, cells)
    result["candidates"] = len(candidates)
    for url, (title, d, schedule_row) in sorted(candidates.items()):
        state = _download_original(db, source, url, title, "BEA_RELEASE", d, now,
                                   "UNITED_STATES", "America/New_York",
                                   {"schedule_row": schedule_row, "schedule_only_clock": "08:30 ET"})
        if state in ("STORED", "UNCHANGED_304"):
            result["documents"] += 1
        elif state not in ("OUT_OF_SCOPE", "FUTURE_EXCLUDED"):
            result["errors"].append({"url": url, "status": state})
    # BEA renders the 2026 HTML calendar client-side, leaving no server-side
    # rows for an HTML-only parser. Its own linked machine-readable release
    # calendar is the authoritative fallback for scheduled timestamps.
    try:
        schedule_body, schedule_meta = common.conditional_fetch(
            db, source, "schedule:release-dates-json", BEA_SCHEDULE_JSON,
            timeout=25, cap=2_000_000)
        schedule = json.loads(schedule_body.decode("utf-8"))
        _save_index(db, source, "schedule:release-dates-json", BEA_SCHEDULE_JSON,
                    schedule_body, schedule_meta, now)
        for release_name, details in schedule.items():
            if not isinstance(details, dict) or not isinstance(details.get("release_dates"), list):
                continue
            if not re.search(r"Gross Domestic Product|Personal Income and Outlays|International Trade", release_name, re.I):
                continue
            for raw_clock in details["release_dates"]:
                try:
                    scheduled = datetime.fromisoformat(raw_clock.replace("Z", "+00:00"))
                except (AttributeError, ValueError):
                    continue
                if scheduled.date() < BACKFILL_START or scheduled > _utc_aware(now):
                    continue
                key = "scheduled:" + re.sub(r"[^a-z0-9]+", "-", release_name.lower()).strip("-") + ":" + scheduled.isoformat()
                event_body = json.dumps({"series": release_name, "scheduled_at": raw_clock},
                                        ensure_ascii=False, sort_keys=True).encode("utf-8")
                row = common.base_row(
                    source, key, "SCHEDULED_RELEASE", BEA_SCHEDULE_JSON, event_body,
                    {"status": 200, "content_type": "application/json", "url": BEA_SCHEDULE_JSON},
                    now, None, "SCHEDULED", "SCHEDULED", title=release_name,
                    event_type="BEA_RELEASE_SCHEDULE", region="UNITED_STATES",
                    extra_json={"scheduled_at_utc": scheduled.astimezone(timezone.utc).isoformat(),
                                "schedule_timezone": "UTC", "schedule_source_url": BEA_SCHEDULE_JSON})
                row["parser_version"] = PARSER_VERSION
                common.store(db, row)
                result["scheduled_releases"] = result.get("scheduled_releases", 0) + 1
    except Exception as e:
        status = e.code if isinstance(e, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
        _record_fetch_status(db, source, "schedule:release-dates-json", BEA_SCHEDULE_JSON,
                             now, status, str(e), headers)
        result["errors"].append({"url": BEA_SCHEDULE_JSON, "status": f"{type(e).__name__}:{str(e)[:100]}"})

    # BEA's official RSS exposes release-specific publication timestamps and
    # links that the JavaScript-rendered 2026 calendar does not provide to an
    # HTML table parser. Retain the complete feed and each original article.
    try:
        rss_body, rss_meta = common.conditional_fetch(
            db, source, "rss:release-feed", BEA_RSS, timeout=25, cap=2_000_000)
        _save_index(db, source, "rss:release-feed", BEA_RSS, rss_body, rss_meta, now)
        items = parse_bea_rss_items(rss_body, now)
        rss_sha = hashlib.sha256(rss_body).hexdigest()
        result["rss_items"] = len(items)
        for item in items:
            state = _download_original(
                db, source, item["url"], item["title"], "BEA_RELEASE",
                item["published_at"].date(), now, "UNITED_STATES", "America/New_York",
                {"timestamp_source": "BEA official RSS pubDate",
                 "rss_pub_date_raw": item["pub_date_raw"],
                 "rss_feed_url": BEA_RSS, "rss_feed_sha256": rss_sha},
                published_hint=item["published_at"])
            if state in ("STORED", "UNCHANGED_304"):
                result["rss_documents"] += 1
            elif state not in ("OUT_OF_SCOPE", "FUTURE_EXCLUDED"):
                result["errors"].append({"url": item["url"], "status": state})
    except Exception as e:
        status = e.code if isinstance(e, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
        _record_fetch_status(db, source, "rss:release-feed", BEA_RSS,
                             now, status, str(e), headers)
        result["errors"].append({"url": BEA_RSS, "status": f"{type(e).__name__}:{str(e)[:100]}"})
    return result


def _bea_archive_state(db, product):
    cur=db.cursor()
    try:
        cur.execute(f"SELECT extra_json FROM `{TABLE}` WHERE source_id='BEA_RELEASES' AND record_key=%s ORDER BY last_seen_at_utc DESC LIMIT 1",('control:bea-product:'+product,))
        row=cur.fetchone()
        return json.loads(row[0]) if row and row[0] else {}
    finally:
        cur.close()


def _save_bea_archive_state(db, product, state, now):
    payload=json.dumps({'product':product,'state_schema':1},sort_keys=True).encode()
    row=common.base_row('BEA_RELEASES','control:bea-product:'+product,'RUN_CONTROL',
        'brain://model100/bea-product-backfill',payload,{'status':0,'content_type':'application/json'},
        now,None,'OBSERVED','RUN_CONTROL',event_type='BACKFILL_CHECKPOINT',extra_json=state)
    row['parser_version']=PARSER_VERSION
    common.store(db,row)


def _repair_bea_cached_values(db, now):
    marker = _bea_archive_state(db, 'parser-repair-v4')
    if marker.get('state') == 'COMPLETE':
        return {'state':'ALREADY_COMPLETE'}
    fields = ('source_id','record_key','record_kind','title','event_type','region',
              'observation_period_start','vintage_month','numeric_value','unit',
              'source_published_at_utc','first_received_at_utc','last_seen_at_utc',
              'reference_timezone','release_clock_evidence','asof_class','canonical_url',
              'content_type','http_status','content_sha256','raw_blob_path','parser_version','extra_json')
    cur=db.cursor()
    cur.execute(f"SELECT {','.join(fields)} FROM `{TABLE}` WHERE source_id='BEA_RELEASES' AND numeric_value IS NOT NULL AND observation_period_start>DATE(source_published_at_utc) AND asof_class<>'UNUSABLE'")
    invalid=cur.fetchall()
    cur.execute(f"SELECT title,first_received_at_utc,source_published_at_utc,canonical_url,content_type,http_status,content_sha256,raw_blob_path,release_clock_evidence,extra_json FROM `{TABLE}` WHERE source_id='BEA_RELEASES' AND record_kind='ORIGINAL_DOCUMENT' AND http_status=200")
    documents=cur.fetchall();cur.close()
    for values in invalid:
        row=dict(zip(fields,values));ex=json.loads(row['extra_json'] or '{}')
        row['extra_json']=json.dumps({**ex,'quarantine_reason':'ACTUAL_OBSERVATION_AFTER_PUBLICATION',
                                    'previous_asof_class':row['asof_class'],
                                    'correction_parser':'BEA_CACHED_REPAIR_V2'})
        row['asof_class']='UNUSABLE';row['parser_version']=PARSER_VERSION
        common.store(db,row)
    count=0
    for title,received,pub,url,ctype,status,digest,path,evidence,raw_extra in documents:
        body=Path(path).read_bytes()
        if hashlib.sha256(body).hexdigest()!=digest:
            raise ValueError('BEA_CACHED_ORIGINAL_SHA_MISMATCH')
        ex=json.loads(raw_extra or '{}')
        day=pub.date() if pub else date.fromisoformat(ex['document_date']) if ex.get('document_date') else None
        if not day: continue
        heading=DocumentHeading();heading.feed(body.decode('utf-8','replace'))
        title=heading.heading() or title
        meta={'status':status,'url':url,'content_type':ctype,
              'received_at_utc':received.replace(tzinfo=timezone.utc).isoformat(),
              'fetch_status':'CACHED_ORIGINAL_REPROCESS'}
        count+=_store_bea_trade_values(db,url,title,body,meta,day,pub,evidence,now)
        count+=_store_bea_macro_values(db,url,title,body,meta,day,pub,evidence,now)
    result={'state':'COMPLETE','originals_reprocessed':len(documents),
            'values_reparsed':count,'invalid_rows_quarantined':len(invalid),
            'completed_at_utc':now.isoformat()}
    _save_bea_archive_state(db,'parser-repair-v4',result,now)
    return result


def _collect_bea_county_current(db, now):
    """Recover county releases omitted from the archive's product taxonomy."""
    index='https://www.bea.gov/data/gdp/gdp-by-county'
    body,meta,links=_index_links(db,'BEA_RELEASES','county-current-index',index,now,timeout=12)
    text=' '.join(html.unescape(re.sub(r'<[^>]+>',' ',body.decode('utf-8','replace'))).split())
    clock=re.search(r'Current release:\s*([A-Z][a-z]+\s+\d{1,2},\s*20\d{2})',text,re.I)
    if not clock:
        raise ValueError('BEA_COUNTY_CURRENT_RELEASE_DATE_MISSING')
    day=datetime.strptime(clock.group(1),'%B %d, %Y').date()
    if not BACKFILL_START<=day<=now.date():
        return {'documents':0,'status':'OUT_OF_SCOPE'}
    candidates={}
    for href,title,_ in links:
        url=urllib.parse.urljoin(index,href)
        parsed=urllib.parse.urlparse(url)
        if parsed.hostname not in ('bea.gov','www.bea.gov'):
            continue
        if re.fullmatch(r'/news/20\d{2}/gross-domestic-product-county-and-personal-income-county-\d{4}',parsed.path):
            candidates[url]=(title if 'county' in title.lower() else
                'Gross Domestic Product by County and Personal Income by County, '+parsed.path.rsplit('-',1)[-1])
    if not candidates:
        raise ValueError('BEA_COUNTY_CURRENT_ORIGINAL_LINK_MISSING')
    cur=db.cursor()
    cur.execute(f"SELECT DISTINCT canonical_url FROM `{TABLE}` WHERE source_id='BEA_RELEASES' AND record_kind='ORIGINAL_DOCUMENT' AND http_status=200")
    known={row[0] for row in cur.fetchall()};cur.close()
    result={'documents':0,'errors':[],'index_url':index}
    for url,title in sorted(candidates.items())[:2]:
        if url in known:continue
        outcome=_download_original(db,'BEA_RELEASES',url,title,'BEA_RELEASE',day,now,
            'UNITED_STATES','America/New_York',{'discovery_index_url':index,
            'discovery_index_sha256':hashlib.sha256(body).hexdigest(),
            'release_date_evidence':'official Current release date; exact hour must come from original'})
        if outcome in ('STORED','UNCHANGED_304'):result['documents']+=1
        else:result['errors'].append({'url':url,'status':outcome})
    return result


def collect_bea(db, now: datetime):
    """Bounded product archive chunk; live RSS is scheduled independently."""
    repair=_repair_bea_cached_values(db,now)
    products=('451','456','476','496','461','1276')  # Broad GDP / national GDP / PCE / trade / state / county.
    states={product:_bea_archive_state(db,product) for product in products}
    incomplete=[product for product in products if states[product].get('state')!='COMPLETE']
    if incomplete:
        eligible=[p for p in incomplete if not states[p].get('retry_after_utc') or now>=datetime.fromisoformat(states[p]['retry_after_utc'])]
        if not eligible:
            return {'source':'BEA_RELEASES','status':'PARTIAL_COVERAGE','remaining_products':incomplete,
                    'next_retry_at_utc':min(states[p]['retry_after_utc'] for p in incomplete),'errors':[]}
        product=eligible[0]
    else:
        # Periodic latest-page discovery after the one-time archive is complete.
        product=min(products,key=lambda p:states[p].get('last_checked_utc',''))
    state=states[product]
    page=int(state.get('next_page',0)) if incomplete else 0
    queue=list(state.get('pending',[])) if incomplete else []
    result={'source':'BEA_RELEASES','lane':'product-archive-chunk','product':product,
            'page':page,'documents':0,'errors':[],'max_document_attempts':6,'cached_repair':repair}
    try:
        if product=='1276':
            county=_collect_bea_county_current(db,now)
            result['county_current']=county
            result['documents']+=county.get('documents',0)
            result['errors'].extend(county.get('errors',[]))
        if not queue:
            url='https://www.bea.gov/news/archive?'+urllib.parse.urlencode({
                'field_related_product_target_id':product,'created_1':'All','page':page})
            body,meta=common.conditional_fetch(db,'BEA_RELEASES',f'product-archive:{product}:{page}',url,timeout=15,cap=8_000_000)
            _save_index(db,'BEA_RELEASES',f'product-archive:{product}:{page}',url,body,meta,now)
            parser=TableRows();parser.feed(body.decode('utf-8','replace'))
            dated=[]
            for row in parser.rows:
                published=row.get('published_at')
                if not published: continue
                for href,title in row['links']:
                    target=urllib.parse.urljoin(url,href)
                    if re.search(r'/news/20\d{2}/',target) and (urllib.parse.urlparse(target).hostname or '') in ('www.bea.gov','bea.gov'):
                        dated.append((published,target,title))
            if not dated:
                raise ValueError('BEA_ARCHIVE_HAS_NO_DATED_RELEASES')
            if any(dated[n][0]<dated[n+1][0] for n in range(len(dated)-1)):
                raise ValueError('BEA_ARCHIVE_SORT_ORDER_UNKNOWN')
            queue=[{'published_at':d.isoformat(),'url':url,'title':title} for d,url,title in dated if BACKFILL_START<=d.date()<=now.date()]
            state={**state,'pending':queue,'next_page':page,'page_sha256':hashlib.sha256(body).hexdigest(),
                   'last_checked_utc':now.isoformat(),'reached_history_start':min(d[0].date() for d in dated)<BACKFILL_START,
                   'state':'INCOMPLETE' if incomplete else 'COMPLETE'}
            _save_bea_archive_state(db,product,state,now)
        cur=db.cursor()
        cur.execute(f"SELECT DISTINCT canonical_url FROM `{TABLE}` WHERE source_id='BEA_RELEASES' AND record_kind='ORIGINAL_DOCUMENT' AND http_status=200")
        known={row[0] for row in cur.fetchall()};cur.close()
        attempts=0
        while queue and attempts<6:
            item=queue[0]
            if item['url'] in known:
                queue.pop(0)
                state['pending']=queue
                _save_bea_archive_state(db,product,state,now)
                continue
            attempts+=1
            published=datetime.fromisoformat(item['published_at'])
            outcome=_download_original(db,'BEA_RELEASES',item['url'],item['title'],'BEA_RELEASE',
                published.date(),now,'UNITED_STATES','America/New_York',
                {'timestamp_source':'official BEA archive time datetime'},published_hint=published)
            if outcome not in ('STORED','UNCHANGED_304'):
                result['errors'].append({'url':item['url'],'status':outcome})
                state['retry_after_utc']=(now+timedelta(hours=24 if outcome=='BLOCKED_ACCESS' else 1)).isoformat()
                state['last_error']=outcome
                _save_bea_archive_state(db,product,state,now)
                break
            result['documents']+=1;known.add(item['url']);queue.pop(0)
            state['pending']=queue
            _save_bea_archive_state(db,product,state,now)
        if not queue:
            state['next_page']=page+1
            state['state']='COMPLETE' if state.get('reached_history_start') or not incomplete else 'INCOMPLETE'
            state['pending']=[]
            _save_bea_archive_state(db,product,state,now)
        states[product]=state
        result['pending_documents']=len(queue)
        result['remaining_products']=[p for p in products if states[p].get('state')!='COMPLETE']
        result['status']='PARTIAL_COVERAGE' if result['errors'] else ('BACKFILL_INCOMPLETE' if result['remaining_products'] else ('SUCCESS_NEW_DATA' if result['documents'] else 'SUCCESS_UNCHANGED'))
        if result['errors'] and any(p!=product and states[p].get('state')!='COMPLETE' for p in products):
            result['status']='BACKFILL_INCOMPLETE'
    except Exception as exc:
        result['errors'].append({'status':type(exc).__name__,'detail':str(exc)[:160]})
        result['status']='RATE_LIMITED' if isinstance(exc,urllib.error.HTTPError) and exc.code==429 else ('BLOCKED_ACCESS' if isinstance(exc,urllib.error.HTTPError) and exc.code==403 else 'PARSE_ERROR' if isinstance(exc,ValueError) else 'NETWORK_TIMEOUT')
    return result


def collect_bea_live(db, now: datetime):
    """Lightweight five-minute BEA RSS lane; fetch article bodies only on feed change."""
    source = "BEA_RELEASES"
    result = {"source": source, "lane": "live-rss", "feed_url": BEA_RSS,
              "rss_items": 0, "documents": 0, "errors": []}
    try:
        body, meta = common.conditional_fetch(
            db, source, "rss:release-feed", BEA_RSS, timeout=20, cap=2_000_000)
    except Exception as exc:
        status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
        _record_fetch_status(db, source, "rss:release-feed", BEA_RSS,
                             now, status, str(exc), headers)
        result["errors"].append({"url": BEA_RSS,
                                 "status": f"{type(exc).__name__}:{str(exc)[:100]}"})
        return result
    _save_index(db, source, "rss:release-feed", BEA_RSS, body, meta, now)
    result["feed_http_status"] = meta.get("status")
    result["feed_bytes"] = meta.get("network_response_bytes", len(body))
    result["feed_sha256"] = hashlib.sha256(body).hexdigest()
    if meta.get("status") == 304:
        result["status"] = "UNCHANGED_304"
    items = parse_bea_rss_items(body, now)
    result["rss_items"] = len(items)
    cur=db.cursor()
    cur.execute(f"SELECT DISTINCT canonical_url FROM `{TABLE}` WHERE source_id='BEA_RELEASES' AND record_kind='ORIGINAL_DOCUMENT' AND http_status=200")
    known={row[0] for row in cur.fetchall()};cur.close()
    for item in items:
        if meta.get('status') == 304 and item['url'] in known:
            continue
        if result['documents']>=6:
            result['status']='BACKFILL_INCOMPLETE'
            break
        state = _download_original(
            db, source, item["url"], item["title"], "BEA_RELEASE",
            item["published_at"].date(), now, "UNITED_STATES", "America/New_York",
            {"timestamp_source": "BEA official RSS pubDate",
             "live_poll":True,
             "rss_pub_date_raw": item["pub_date_raw"],
             "rss_feed_url": BEA_RSS,
             "rss_feed_sha256": result["feed_sha256"]},
            published_hint=item["published_at"])
        if state in ("STORED", "UNCHANGED_304"):
            result["documents"] += 1
            result['status']='SUCCESS_NEW_DATA' if state=='STORED' else 'SUCCESS_UNCHANGED'
        elif state not in ("OUT_OF_SCOPE", "FUTURE_EXCLUDED"):
            result["errors"].append({"url": item["url"], "status": state})
    if result['errors']:
        result['status']=common._collector_outcome({'errors':result['errors']})
    return result


def collect_bls_access(db, now: datetime):
    source = "BLS_RELEASES"
    result = {"source": source, "classification": "BLOCKED_ACCESS", "probes": [],
              "known_not_published": BLS_KNOWN_NOT_PUBLISHED}
    result["not_published_rows_stored"] = _store_bls_not_published(db, now)
    for name, url in BLS_PROBES.items():
        try:
            body, meta = _read(url, timeout=12, cap=4_000_000)
            digest = hashlib.sha256(body).hexdigest()
            result["probes"].append({"name": name, "http_status": meta.get("status"),
                                     "content_type": meta.get("content_type"),
                                     "bytes": len(body), "sha256": digest, "url": url})
            # Only archive bodies returned successfully are retained. No access
            # workaround or alternate host is attempted.
            if meta.get("status") == 200:
                _store_document(db, source, "probe:" + name, url, body, meta, now,
                                name, "ACCESS_PROBE", evidence="DAY_ONLY",
                                asof="HISTORICAL_ARCHIVE_PROBE", region="UNITED_STATES",
                                extra={"probe_only": True})
                if name.startswith("api_"):
                    result.setdefault("latest_revised_control_rows", 0)
                    result["latest_revised_control_rows"] += _store_bls_api_control(
                        db, name, url, body, meta, now)
        except urllib.error.HTTPError as e:
            response_headers = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
            response_body = e.read(2_000_000)
            if response_body:
                meta = {"status": e.code, "content_type": response_headers.get("content-type"), "url": url}
                _store_document(db, source, "blocked:" + name, url, response_body, meta, now,
                                name, "BLOCKED_ACCESS_RESPONSE", evidence="OBSERVED",
                                asof="FETCH_STATUS", region="UNITED_STATES",
                                extra={"probe_only": True, "http_status": e.code})
            _record_fetch_status(db, source, name, url, now, e.code, str(e), response_headers)
            result["probes"].append({"name": name, "http_status": e.code, "url": url,
                                     "content_type": response_headers.get("content-type"),
                                     "response_bytes": len(response_body),
                                     "sha256": hashlib.sha256(response_body).hexdigest() if response_body else None,
                                     "status": "BLOCKED_ACCESS" if e.code in (401, 403, 407) else "HTTP_ERROR"})
        except Exception as e:
            _record_fetch_status(db, source, name, url, now, None,
                                 type(e).__name__ + ":" + str(e)[:150])
            result["probes"].append({"name": name, "http_status": None, "url": url,
                                     "status": f"{type(e).__name__}:{str(e)[:100]}"})
    if any(p.get("http_status") in (401, 403, 407) for p in result["probes"]):
        result["classification"] = "BLOCKED_ACCESS"
    elif any(p.get("http_status") == 200 for p in result["probes"]):
        result["classification"] = "PARTIAL_ACCESS"
    else:
        result["classification"] = "NO_HTTP_STATUS"
    return result


def _parse_bls_api_series(body: bytes):
    """Parse BLS v1 monthly observations without implying a release vintage."""
    payload = json.loads(body.decode("utf-8"))
    if payload.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError("BLS_API_REQUEST_NOT_SUCCEEDED")
    out = []
    for result in payload.get("Results", []):
        for series in result.get("series", []):
            series_id = series.get("seriesID")
            for item in series.get("data", []):
                period = item.get("period", "")
                if not re.fullmatch(r"M(?:0[1-9]|1[0-2])", period):
                    continue
                year = int(item["year"])
                month = int(period[1:])
                observation = date(year, month, 1)
                if observation < BACKFILL_START:
                    continue
                try:
                    value = float(item["value"])
                except (KeyError, TypeError, ValueError):
                    continue
                notes = [f.get("text") for f in item.get("footnotes", []) if f.get("text")]
                out.append({"series_id": series_id, "observation_period": observation,
                            "period": period, "value": value, "footnotes": notes})
    return out


def _store_bls_api_control(db, probe_name, url, body, meta, now):
    labels = {
        "api_cpi": ("CPI", "CUSR0000SA0", "index"),
        "api_employment": ("NFP", "CES0000000001", "thousands of employees"),
        "api_ppi": ("PPI_FINAL_DEMAND", "WPUFD4", "index"),
    }
    event_type, expected_series, unit = labels[probe_name]
    rows = 0
    for item in _parse_bls_api_series(body):
        if item["series_id"] != expected_series:
            continue
        period = item["observation_period"]
        key = f"latest-revised:{expected_series}:{period.isoformat()}"
        row = common.base_row(
            "BLS_RELEASES", key, "LATEST_REVISED_CONTROL", url, body, meta,
            now, None, "DAY_ONLY", "LATEST_REVISED_CONTROL",
            title=f"BLS {event_type} latest revised control", event_type=event_type,
            region="UNITED_STATES", observation_period_start=period.isoformat(),
            numeric_value=item["value"], unit=unit,
            reference_timezone="America/New_York",
            extra_json={"series_id": expected_series, "source_api": "BLS Public Data API v1",
                        "value_class": "latest_revised_not_first_vintage",
                        "footnotes": item["footnotes"], "api_has_no_publication_timestamp": True})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
        rows += 1
    return rows


def _store_bls_not_published(db, now: datetime) -> int:
    """Persist specification-confirmed cancelled releases without implying a fetch."""
    source = "BLS_RELEASES"
    url = BLS_PROBES["calendar"]
    for item in BLS_KNOWN_NOT_PUBLISHED:
        payload = json.dumps({"source": source, "series": item["series"],
                              "observation_period": item["observation_period"],
                              "status": "NOT_PUBLISHED", "reason": item["reason"],
                              "basis": "SUPPLIED_RESEARCH_SPECIFICATION",
                              "source_url": url}, sort_keys=True).encode("utf-8")
        meta = {"status": 0, "content_type": "application/json", "url": url,
                "fetch_status": "NOT_PUBLISHED_SPEC_EVIDENCE"}
        row = common.base_row(
            source, "not-published:" + item["series"] + ":" + item["observation_period"],
            "RELEASE_STATUS", url, payload, meta, now, None, "NOT_PUBLISHED",
            "NOT_PUBLISHED", title=item["series"] + " October 2025 release not published",
            event_type=item["series"], region="UNITED_STATES",
            observation_period_start=item["observation_period"],
            extra_json={"status": "NOT_PUBLISHED", "reason": item["reason"],
                        "basis": "SUPPLIED_RESEARCH_SPECIFICATION",
                        "http_attempted_for_this_status": False})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
    return len(BLS_KNOWN_NOT_PUBLISHED)


def _week_mondays(start: date, end: date):
    current = start - timedelta(days=start.weekday())
    while current <= end:
        yield current
        current += timedelta(days=7)


def _eurostat_release_date(url: str):
    m = re.search(r"/w/2-(\d{2})(\d{2})(20\d{2})-(?:ap|wd)(?:/|$)", urllib.parse.urlparse(url).path)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _parse_date_from_eurostat_code(code: str):
    match = re.fullmatch(r"2-(\d{2})(\d{2})(20\d{2})-ap", code)
    if not match:
        return None
    try:
        return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    except ValueError:
        return None


def _eurostat_published_at(body: bytes):
    text = body.decode("utf-8", "replace")
    match = re.search(r"[\"']datePublished[\"']\s*:\s*[\"']([^\"']+)", text, re.I)
    if not match:
        return None
    try:
        value = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
    except ValueError:
        return None
    if value.tzinfo is None:
        return None
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _store_eurostat_hicp_value(db, url: str, title: str, body: bytes,
                               meta: dict, release_date: date, now: datetime):
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = " ".join(text.split())
    match = re.search(
        r"Euro area annual inflation is expected to\s+(?:be\s+)?(\d+(?:\.\d+)?)%\s+in\s+([A-Z][a-z]+)\s+(20\d{2})",
        text, re.I)
    if not match:
        return False
    value = float(match.group(1))
    try:
        observation = date(int(match.group(3)), datetime.strptime(match.group(2), "%B").month, 1)
    except ValueError:
        return False
    slug = urllib.parse.urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    key = f"flash:{slug}:EA_HICP_YOY_FIRST"
    published = _eurostat_published_at(body)
    evidence = "DOCUMENTED_PUBLISHED_AT" if published else "DAY_ONLY"
    row = common.base_row(
        "EUROSTAT_FLASH", key, "FIRST_ESTIMATE", url, body, meta, now,
        published, evidence, "HISTORICAL_ARCHIVE", title=title,
        event_type="FLASH_HICP_YOY", region="EURO_AREA",
        observation_period_start=observation.isoformat(),
        vintage_month=release_date.replace(day=1).isoformat(),
        numeric_value=value, unit="PERCENT_YOY",
        extra_json={"value_kind": "FLASH_FIRST_ESTIMATE",
                    "release_document_date": release_date.isoformat(),
                    "publication_timezone": "Europe/Luxembourg",
                    "publication_clock": "original schema datePublished" if published else "not recovered; DAY_ONLY",
                    "publication_time_source": "JSON-LD datePublished" if published else None,
                    "metric": "EA_ALL_ITEMS_HICP_YOY"})
    row["parser_version"] = PARSER_VERSION
    common.store(db, row)
    return True


def _parse_fed_target_range(body: bytes):
    """Extract the published FOMC target range without inferring a change."""
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = text.translate(str.maketrans({"‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-"}))
    text = " ".join(text.split())
    number = r"\d+(?:-\d+/\d+|/\d+)?"
    match = re.search(
        rf"target range for the federal funds rate\s+(?:at|to|by\s+\d+(?:/\d+)?\s+percentage point\s+to)\s+({number})\s+to\s+({number})\s+percent\b",
        text, re.I)
    if not match:
        return None

    def as_percent(raw):
        if "-" in raw:
            whole, fraction = raw.split("-", 1)
            return float(Fraction(int(whole), 1) + Fraction(fraction))
        if "/" in raw:
            return float(Fraction(raw))
        return float(raw)

    try:
        lower, upper = (as_percent(match.group(1)), as_percent(match.group(2)))
    except (ValueError, ZeroDivisionError):
        return None
    if lower >= upper:
        return None
    action = re.search(r'Committee\s+decided\s+to\s+(maintain|lower|raise)\b', text, re.I)
    return {"lower": lower, "upper": upper, "text": match.group(0),
            'decision_action': {'maintain':'HOLD','lower':'CUT','raise':'HIKE'}.get(action.group(1).lower()) if action else 'UNKNOWN'}


def _store_fed_target_range(db, url: str, title: str, body: bytes,
                            meta: dict, release_date: date, published_at: datetime | None,
                            evidence: str, asof: str, now: datetime):
    if not re.search(r"FOMC statement|statement of the federal open market committee", title, re.I):
        return 0
    target = _parse_fed_target_range(body)
    if not target:
        return 0
    count = 0
    for bound in ("lower", "upper"):
        value = target[bound]
        row = common.base_row(
            "FED_FOMC", f"value:{release_date.isoformat()}:FED_FUNDS_TARGET_{bound.upper()}",
            "RELEASE_VALUE", url, body, meta, now, published_at,
            evidence if published_at else "DAY_ONLY", asof,
            title=title, event_type=f"FED_FUNDS_TARGET_{bound.upper()}",
            region="UNITED_STATES", observation_period_start=release_date.isoformat(),
            vintage_month=release_date.replace(day=1).isoformat(),
            numeric_value=value, unit="PERCENT",
            extra_json={"value_kind": "FOMC_TARGET_RANGE_BOUND",
                        "target_bound": bound.upper(),
                        "target_range_text": target["text"],
                        "decision_action": target['decision_action'],
                        "rate_change_bp_not_inferred": True,
                        "publication_timezone": "America/New_York"})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
        count += 1
    return count


def _reprocess_cached_eurostat_documents(db, now: datetime) -> dict:
    """Re-run value extraction over immutable originals without network I/O."""
    cur = db.cursor()
    cur.execute(f"SELECT record_key,content_sha256 FROM `{TABLE}` "
                "WHERE source_id='EUROSTAT_FLASH' AND record_kind='FIRST_ESTIMATE'")
    existing_values = {(row[0], row[1]) for row in cur.fetchall()}
    cur.execute(f"SELECT record_key,title,first_received_at_utc,canonical_url,content_type,"
                "http_status,content_sha256,raw_blob_path FROM `" + TABLE + "` "
                "WHERE source_id='EUROSTAT_FLASH' AND record_kind='ORIGINAL_DOCUMENT' "
                "AND record_key LIKE 'document:%'")
    documents = cur.fetchall()
    cur.close()
    result = {"cached_documents_checked": 0, "cached_first_estimates_added": 0, "errors": []}
    for key, title, received, url, content_type, status, digest, raw_path in documents:
        slug = urllib.parse.urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
        value_key = f"flash:{slug}:EA_HICP_YOY_FIRST"
        if (value_key, digest) in existing_values:
            continue
        release_date = _eurostat_release_date(url)
        if not release_date or release_date < BACKFILL_START:
            continue
        try:
            body = Path(raw_path).read_bytes()
            if hashlib.sha256(body).hexdigest() != digest:
                raise ValueError("CACHED_ORIGINAL_SHA_MISMATCH")
            received_text = (received.replace(tzinfo=timezone.utc).isoformat()
                             if isinstance(received, datetime) else str(received).replace(" ", "T") + "+00:00")
            meta = {"status": status, "original_http_status": status,
                    "content_type": content_type, "url": url,
                    "received_at_utc": received_text,
                    "fetch_status": "CACHED_ORIGINAL_REPROCESS"}
            result["cached_documents_checked"] += 1
            if _store_eurostat_hicp_value(db, url, title, body, meta, release_date, now):
                result["cached_first_estimates_added"] += 1
        except Exception as exc:
            result["errors"].append({"record_key": key,
                                     "error": type(exc).__name__ + ":" + str(exc)[:160]})
    return result


def _eurostat_month_targets(backfill_start: date, current_month: date,
                            completed_calendar_months: set[date],
                            failed_search_months: set[date]) -> list[date]:
    previous_month = (current_month.replace(day=1) - timedelta(days=1)).replace(day=1)
    all_months = []
    month_cursor = backfill_start.replace(day=1)
    while month_cursor <= current_month:
        all_months.append(month_cursor)
        month_cursor = (month_cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    missing_months = [month for month in all_months if month not in completed_calendar_months]
    resume_months = sorted(set(missing_months) | failed_search_months)
    # Historical recovery consumes one checkpointed window. Recent refresh is
    # a separate phase once archive gaps are closed, avoiding duplicate live
    # requests on every historical chunk.
    month_targets = {resume_months[0]} if resume_months else {previous_month, current_month}
    return sorted(month for month in month_targets
                  if backfill_start.replace(day=1) <= month <= current_month)


def _eurostat_missing_release_months(db):
    cur = db.cursor()
    try:
        cur.execute(f"""SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(s.extra_json,'$.calendar_date'))
            FROM `{TABLE}` s WHERE s.source_id='EUROSTAT_FLASH' AND s.record_kind='SCHEDULED_RELEASE'
            AND s.observation_period_start IS NOT NULL AND NOT EXISTS
            (SELECT 1 FROM `{TABLE}` v WHERE v.source_id=s.source_id AND v.record_kind='FIRST_ESTIMATE'
             AND v.observation_period_start=s.observation_period_start)""")
        return {date.fromisoformat(row[0]).replace(day=1) for row in cur.fetchall() if row[0]}
    finally:
        cur.close()


def _eurostat_value_version_count(db):
    return common.numeric_version_count(db, 'EUROSTAT_FLASH', 'FIRST_ESTIMATE')


def _eurostat_completion_status(result, new_values):
    if result['rate_limited']:
        return 'RATE_LIMITED'
    if result['errors']:
        return 'PARTIAL_COVERAGE'
    if result['remaining_calendar_months'] or result['remaining_release_months']:
        return 'BACKFILL_INCOMPLETE'
    return 'SUCCESS_NEW_DATA' if new_values else 'SUCCESS_UNCHANGED'


def collect_eurostat_flash(db, now: datetime):
    source = "EUROSTAT_FLASH"
    prior_value_versions = _eurostat_value_version_count(db)
    result = {"source": source, "start": BACKFILL_START.isoformat(),
              "calendar_months": 0, "scheduled_events": 0,
              "documents": 0, "first_estimates": 0,
              "rate_limited": False, "errors": []}
    cur = db.cursor()
    cur.execute(f"SELECT last_seen_at_utc,extra_json FROM `{TABLE}` WHERE source_id=%s AND record_kind='FETCH_STATUS' AND http_status=429 ORDER BY last_seen_at_utc DESC LIMIT 1", (source,))
    limited = cur.fetchone()
    cur.close()
    if limited:
        last_limited, raw_extra = limited
        rate_extra = raw_extra if isinstance(raw_extra, dict) else json.loads(raw_extra or '{}')
        headers = rate_extra.get('response_headers') or {}
        wait = headers.get('retry-after') or headers.get('x-rate-limit-retry-after-seconds') or '60'
        try:
            retry_at = last_limited + timedelta(seconds=max(0, int(wait)))
        except (ValueError, TypeError):
            retry_at = parsedate_to_datetime(wait).astimezone(timezone.utc).replace(tzinfo=None)
        if now.replace(tzinfo=None) < retry_at:
            return {**result, 'status':'RATE_LIMITED', 'rate_limited':True,
                    'next_retry_at_utc':retry_at.isoformat()}
    current_month = _utc_aware(now).date().replace(day=1)
    # Treat the saved ORIGINAL_INDEX rows as the historical backfill checkpoint.
    # Resume the oldest missing calendar month one at a time, then refresh only
    # the latest two months. This avoids replaying thirteen archive months on
    # every routine update while keeping failed windows resumable.
    cur = db.cursor()
    cur.execute(f"SELECT record_key FROM `{TABLE}` WHERE source_id=%s AND record_kind='ORIGINAL_INDEX' AND record_key LIKE 'calendar-month:%'",
                (source,))
    completed_calendar_months = set()
    for row in cur.fetchall():
        key = row[0]
        try:
            completed_calendar_months.add(date.fromisoformat(key.rsplit(":", 1)[1]))
        except (TypeError, ValueError):
            continue
    cur.execute(f"SELECT record_key,last_seen_at_utc FROM `{TABLE}` WHERE source_id=%s AND record_kind='FETCH_STATUS' AND record_key LIKE 'fetch-status:search:flash:%'",
                (source,))
    failed_search_months = set()
    for row in cur.fetchall():
        key, failed_at = row[0], row[1]
        m = re.search(r"fetch-status:search:flash:(\d{4}-\d{2}-\d{2})$", key)
        if not m:
            continue
        try:
            search_day = date.fromisoformat(m.group(1))
        except ValueError:
            continue
        cur.execute(f"SELECT MAX(last_seen_at_utc) FROM `{TABLE}` WHERE source_id=%s AND record_kind='ORIGINAL_INDEX' AND record_key=%s",
                    (source, "search:flash:" + search_day.isoformat()))
        indexed = cur.fetchone()
        if not indexed or indexed[0] is None or failed_at > indexed[0]:
            failed_search_months.add(search_day.replace(day=1))
    cur.close()
    failed_search_months.update(_eurostat_missing_release_months(db))
    month_targets = _eurostat_month_targets(BACKFILL_START, current_month,
                                            completed_calendar_months, failed_search_months)
    seen_event_ids = set()
    processed_flash_dates = set()
    for month_start in month_targets:
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        start = datetime.combine(month_start, dtime.min, tzinfo=timezone.utc)
        end = datetime.combine(next_month, dtime.min, tzinfo=timezone.utc)
        params = urllib.parse.urlencode({
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "theme": 0, "category": 0, "keywords": "",
            "isEuroindicator": "true", "authorInclude": "", "authorExclude": "",
        })
        url = "https://ec.europa.eu/eurostat/o/calendars/eventsJson?" + params
        month_key = f"calendar-month:{month_start.isoformat()}"
        try:
            time.sleep(1.0)
            body, meta = common.conditional_fetch(db, source, month_key, url, timeout=20, cap=4_000_000)
            events = json.loads(body.decode("utf-8"))
            if not isinstance(events, list):
                raise ValueError("Eurostat calendar JSON is not an event list")
            _save_index(db, source, month_key, url, body, meta, now)
            completed_calendar_months.add(month_start)
            result["calendar_months"] += 1
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, source, month_key, url, now, status, str(e), headers)
            result["errors"].append({"month": month_start.isoformat(), "status": f"{type(e).__name__}:{str(e)[:100]}"})
            if status == 429:
                result["rate_limited"] = True
                result["retry_after_seconds"] = (headers.get("x-rate-limit-retry-after-seconds")
                                                  or headers.get("retry-after"))
                break
            continue
        for event in events:
            title = str(event.get("title") or "")
            if "flash estimate inflation euro area" not in title.lower():
                continue
            try:
                scheduled = datetime.fromisoformat(str(event["start"]).replace("Z", "+00:00"))
            except (KeyError, ValueError):
                continue
            event_date = scheduled.date()
            if event_date < BACKFILL_START or scheduled < start or scheduled >= end or scheduled > _utc_aware(now):
                continue
            record_key = "scheduled:" + str(event.get("recordid") or scheduled.isoformat())
            if record_key in seen_event_ids:
                continue
            seen_event_ids.add(record_key)
            try:
                observation_period = datetime.strptime(str(event.get("period", "")), "%B %Y").date().replace(day=1)
            except ValueError:
                observation_period = None
            event_body = json.dumps(event, ensure_ascii=False, sort_keys=True).encode("utf-8")
            event_url = url
            local_date = scheduled.astimezone(ZoneInfo("Europe/Luxembourg")).date()
            row = common.base_row(
                source, record_key, "SCHEDULED_RELEASE", event_url, event_body,
                {"status": 200, "content_type": "application/json", "url": url},
                now, None, "SCHEDULED", "SCHEDULED", title=title,
                event_type="FLASH_HICP_RELEASE", region="EURO_AREA",
                observation_period_start=observation_period.isoformat() if observation_period else None,
                extra_json={"calendar_date": local_date.isoformat(),
                            "source_timezone": "Europe/Luxembourg",
                            "calendar_start_semantics": "event date only; not an actual publication timestamp",
                            "source_calendar_url": url,
                            "original_calendar_recordid": event.get("recordid"),
                            "dataset_codes": event.get("datasetCodes"),
                            "preliminary": event.get("preliminary")})
            row["parser_version"] = PARSER_VERSION
            common.store(db, row)
            result["scheduled_events"] += 1
            if local_date in processed_flash_dates:
                continue
            processed_flash_dates.add(local_date)
            search_params = urllib.parse.urlencode({
                "_" + EUROSTAT_SEARCH_PORTLET + "_collection": "CAT_PREREL",
                "_" + EUROSTAT_SEARCH_PORTLET + "_publicationDate":
                    f">{local_date.isoformat()}T00:00:00,<{local_date.isoformat()}T23:59:59",
            })
            search_url = ("https://ec.europa.eu/eurostat/web/main/search/-/search/"
                          + EUROSTAT_SEARCH_PORTLET + "?" + search_params)
            search_key = "search:flash:" + local_date.isoformat()
            try:
                time.sleep(1.0)
                search_body, search_meta = common.conditional_fetch(
                    db, source, search_key, search_url, timeout=25, cap=5_000_000)
                _save_index(db, source, search_key, search_url, search_body, search_meta, now)
                search_page = LinkParser()
                search_page.feed(search_body.decode("utf-8", "replace"))
                article_links = {}
                for href, article_title, _ in search_page.links:
                    target = urllib.parse.urljoin(search_url, href)
                    code_match = re.search(r"[?&]code=(2-\d{8}-ap)(?:&|$)", target)
                    if not code_match:
                        continue
                    code = code_match.group(1)
                    article_date = _parse_date_from_eurostat_code(code)
                    if article_date == local_date:
                        canonical_article_url = (
                            "https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/" + code)
                        article_links[canonical_article_url] = article_title or title
                if not article_links:
                    # Official Euro-indicator identifiers encode the dated
                    # release. A calendar-confirmed past HICP flash can be
                    # fetched directly when the search index omits its link.
                    code = '2-' + local_date.strftime('%d%m%Y') + '-ap'
                    article_links['https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/' + code] = title
                for article_url, article_title in sorted(article_links.items()):
                    time.sleep(1.5)
                    article_code = urllib.parse.urlparse(article_url).path.rstrip("/").rsplit("/", 1)[-1]
                    try:
                        article_body, article_meta = common.conditional_fetch(
                            db, source, "document:" + article_code,
                            article_url, timeout=25, cap=8_000_000)
                        canonical_url = article_meta.get("url") or article_url
                        release_date = _eurostat_release_date(canonical_url) or local_date
                        published = _eurostat_published_at(article_body)
                        _store_document(
                            db, source, "document:" + article_code,
                            canonical_url, article_body, article_meta, now, article_title,
                            "EURO_INDICATORS_FLASH", pub_at=published,
                            evidence="DOCUMENTED_PUBLISHED_AT" if published else "DAY_ONLY",
                            asof="HISTORICAL_ARCHIVE", region="EURO_AREA",
                            extra={"document_date": release_date.isoformat(),
                                   "publication_timezone": "UTC" if published else "Europe/Luxembourg",
                                   "publication_time_source": "JSON-LD datePublished" if published else None,
                                   "request_url": article_url,
                                   "calendar_date": local_date.isoformat()})
                        result["documents"] += 1
                        if _store_eurostat_hicp_value(
                                db, canonical_url, article_title, article_body,
                                article_meta, release_date, now):
                            result["first_estimates"] += 1
                        else:
                            result['errors'].append({'url':article_url, 'status':'REQUIRED_FIRST_VALUE_NOT_PARSED'})
                    except Exception as e:
                        status = e.code if isinstance(e, urllib.error.HTTPError) else None
                        headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
                        _record_fetch_status(db, source, "article:" + article_url, article_url,
                                             now, status, str(e), headers)
                        result["errors"].append({"url": article_url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
                        if status == 429:
                            result["rate_limited"] = True
                            result["retry_after_seconds"] = (headers.get("x-rate-limit-retry-after-seconds")
                                                              or headers.get("retry-after"))
                            break
                if result["rate_limited"]:
                    break
            except Exception as e:
                status = e.code if isinstance(e, urllib.error.HTTPError) else None
                headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
                _record_fetch_status(db, source, search_key, search_url, now, status, str(e), headers)
                result["errors"].append({"url": search_url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
                if status == 429:
                    result["rate_limited"] = True
                    result["retry_after_seconds"] = (headers.get("x-rate-limit-retry-after-seconds")
                                                      or headers.get("retry-after"))
            if result["rate_limited"]:
                break
            time.sleep(1.5)
        if result["rate_limited"]:
            break
        time.sleep(1.5)
        month_start = next_month
    cached_values = _reprocess_cached_eurostat_documents(db, now)
    result.update({k: v for k, v in cached_values.items() if k != "errors"})
    result["errors"].extend(cached_values["errors"])
    month = BACKFILL_START.replace(day=1)
    remaining = []
    while month <= current_month:
        if month not in completed_calendar_months:
            remaining.append(month.isoformat())
        month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    result['remaining_calendar_months'] = remaining
    release_remaining = sorted(month.isoformat() for month in _eurostat_missing_release_months(db))
    result['remaining_release_months'] = release_remaining
    result['new_value_versions'] = max(0, _eurostat_value_version_count(db) - prior_value_versions)
    result['status'] = _eurostat_completion_status(result, result['new_value_versions'])
    return result


DESTATIS_INDEXES = (
    "https://www.destatis.de/EN/Themes/Economy/Prices/Consumer-Price-Index/_node.html",
    "https://www.destatis.de/EN/Press/press_node_2.html",
    "https://www.destatis.de/SiteGlobals/Forms/Suche/Presse/EN/Pressesuche_Formular_2.html?dateOfIssue_dt=lastyear&resultsPerPage=30&cl2Taxonomies_Themen_0=preise&cl2Taxonomies_Themen_1=verbraucherpreisindex",
)
DESTATIS_RSS_URL = "https://www.destatis.de/SiteGlobals/Functions/RSSFeed/DE/RSSNewsfeed/Aktuell.xml?nn=241288"
_DE_MONTHS = {"januar": 1, "februar": 2, "märz": 3, "maerz": 3,
              "april": 4, "mai": 5, "juni": 6, "juli": 7, "august": 8,
              "september": 9, "oktober": 10, "november": 11, "dezember": 12}


def _parse_destatis_rss(body):
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
        if BACKFILL_START <= published.date() <= date.today():
            rows.append({"title": html.unescape(title), "url": url,
                         "description": html.unescape(fields.get("description", "")),
                         "published_at": published})
    return rows


def _destatis_german_period(text):
    match = re.search(r"\b(Januar|Februar|März|Maerz|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(20\d{2})\b", text, re.I)
    if not match:
        return None
    return date(int(match.group(2)), _DE_MONTHS[match.group(1).lower()], 1)


def _parse_destatis_current_page(body):
    """Extract only the official current-page provisional headline, not an archive vintage."""
    class SummaryParser(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.items = []
            self.anchor = None

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag.lower() == "a" and attrs.get("href"):
                self.anchor = {"href": attrs["href"], "title": attrs.get("title", ""), "text": []}

        def handle_data(self, data):
            if self.anchor is not None:
                self.anchor["text"].append(data)

        def handle_endtag(self, tag):
            if tag.lower() == "a" and self.anchor is not None:
                self.items.append(self.anchor)
                self.anchor = None

    parser = SummaryParser()
    parser.feed(body.decode("utf-8", "replace"))
    out = {}
    for item in parser.items:
        href, title = item["href"], item["title"]
        text = " ".join(" ".join(item["text"]).split())
        match = re.search(r"(?:^|/)EN/Press/(20\d{2})/(\d{2})/PE\d{2}_\d+_611\.html", href)
        value = re.search(r"([+\-]?\d+(?:\.\d+)?)\s*%", title + " " + text)
        if not match or not value or not re.search(r"inflation|Teuerungsrate|Verbraucherpreis", title + " " + text, re.I):
            continue
        label = title + " " + text
        period_match = re.search(r"(?:in|for)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})", label, re.I)
        if not period_match or not re.search(r"expected|provisional|preliminary", label, re.I):
            continue
        period = date(int(period_match.group(2)), datetime.strptime(period_match.group(1), "%B").month, 1)
        if period < BACKFILL_START.replace(day=1) or period > date.today().replace(day=1):
            continue
        out[period.isoformat()] = {"value": float(value.group(1)), "observation_period": period,
                                  "article_url": urllib.parse.urljoin("https://www.destatis.de/", href),
                                  "title": title or text, "release_month_path": f"{match.group(1)}-{match.group(2)}"}
    return list(out.values())


def _store_destatis_current_page_values(db, body, meta, now):
    rows = _parse_destatis_current_page(body)
    for item in rows:
        key = f"current-page-provisional:{item['observation_period'].isoformat()}"
        row = common.base_row(
            "DEST_ATIS_CPI", key, "CURRENT_PAGE_ESTIMATE", meta.get("url", DESTATIS_INDEXES[0]),
            body, meta, now, None, "DAY_ONLY", "DAY_ONLY", title=item["title"],
            event_type="CPI_YOY_PRELIM", region="GERMANY",
            observation_period_start=item["observation_period"].isoformat(),
            numeric_value=item["value"], unit="PERCENT_YOY", reference_timezone="Europe/Berlin",
            extra_json={"value_class": "current_official_index_summary_not_original_release",
                        "original_release_url": item["article_url"],
                        "release_month_path": item["release_month_path"],
                        "source_published_at_unknown": True,
                        "not_a_historical_first_vintage": True})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
    return len(rows)


def _collect_destatis_rss(db, now):
    source, url = "DEST_ATIS_CPI", DESTATIS_RSS_URL
    result = {"http_status": None, "feed_items": 0, "candidate_articles": 0,
              "documents": 0, "first_estimate_values": 0, "errors": []}
    try:
        body, meta = common.conditional_fetch(db, source, "official-german-press-rss", url,
                                              timeout=20, cap=8_000_000)
    except Exception as exc:
        status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
        _record_fetch_status(db, source, "official-german-press-rss", url, now, status, str(exc), headers)
        result["http_status"] = status
        result["errors"].append(f"{type(exc).__name__}:{str(exc)[:120]}")
        return result
    result["http_status"] = meta.get("status")
    _save_index(db, source, "official-german-press-rss", url, body, meta, now)
    try:
        items = _parse_destatis_rss(body)
    except ET.ParseError as exc:
        result["errors"].append(f"XML_PARSE:{exc}")
        return result
    result["feed_items"] = len(items)
    candidates = [item for item in items
                  if re.search(r"inflation|consumer price|verbraucherpreis|teuerungsrate|vpi|hvpi", item["title"] + " " + item["description"], re.I)]
    result["candidate_articles"] = len(candidates)
    for item in candidates:
        key = "rss-document:" + hashlib.sha256(item["url"].encode()).hexdigest()[:32]
        cur = db.cursor()
        cur.execute(f"SELECT 1 FROM `{TABLE}` WHERE source_id=%s AND record_key=%s LIMIT 1", (source, key))
        exists = cur.fetchone() is not None
        cur.close()
        if exists:
            continue
        try:
            article, article_meta = common.conditional_fetch(db, source, key, item["url"],
                                                             timeout=20, cap=8_000_000)
            release_date = item["published_at"].date()
            _store_document(db, source, key, item["url"], article, article_meta, now,
                            item["title"], "DEST_ATIS_CPI_RELEASE",
                            pub_at=item["published_at"], evidence="DOCUMENTED_PUBLISHED_AT",
                            asof="HISTORICAL_REPLAY", region="GERMANY",
                            extra={"document_date": release_date.isoformat(),
                                   "publication_timezone": "UTC_from_RSS_offset",
                                   "rss_url": url, "original_document": True})
            result["documents"] += 1
            text_body = html.unescape(re.sub(r"<[^>]+>", " ", article.decode("utf-8", "replace")))
            text_body = " ".join(text_body.split())
            period = _destatis_german_period(item["title"] + " " + text_body)
            preliminary = re.search(r"(?:Inflationsrate|Verbraucherpreise).*?(?:voraussichtlich|vorläufig|vorlaeufig).*?([+\-]?\d+(?:[,.]\d+)?)\s*%", text_body, re.I)
            if not period:
                period = _destatis_german_period(item["title"] + " " + item["description"])
            if period and preliminary:
                value = float(preliminary.group(1).replace(",", "."))
                value_key = "preliminary:" + hashlib.sha256(item["url"].encode()).hexdigest()[:24] + ":CPI_YOY_PRELIM"
                row = common.base_row(source, value_key, "FIRST_ESTIMATE", item["url"], article,
                    article_meta, now, item["published_at"], "DOCUMENTED_PUBLISHED_AT",
                    "HISTORICAL_REPLAY", title=item["title"], event_type="CPI_YOY_PRELIM",
                    region="GERMANY", observation_period_start=period.isoformat(),
                    vintage_month=release_date.replace(day=1).isoformat(),
                    numeric_value=value, unit="PERCENT_YOY", reference_timezone="Europe/Berlin",
                    extra_json={"value_kind": "PRELIMINARY", "rss_pubdate_utc": item["published_at"].isoformat(),
                                "source_timezone": "RSS_explicit_offset", "historical_vintage": True})
                row["parser_version"] = PARSER_VERSION
                common.store(db, row)
                result["first_estimate_values"] += 1
        except Exception as exc:
            status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
            _record_fetch_status(db, source, key, item["url"], now, status, str(exc), headers)
            result["errors"].append({"url": item["url"], "status": f"{type(exc).__name__}:{str(exc)[:100]}"})
        time.sleep(.25)
    return result


def _destatis_article_links(body: bytes, page_url: str):
    """Destatis relative URLs use the document base; 'More' inherits its heading."""
    class Articles(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.base = page_url
            self.heading = []
            self.in_heading = False
            self.last_heading = ""
            self.anchor = None
            self.items = []

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == "base" and attrs.get("href"):
                self.base = urllib.parse.urljoin(page_url, attrs["href"])
            if tag in ("h1", "h2", "h3", "h4"):
                self.in_heading, self.heading = True, []
            if tag == "a" and attrs.get("href"):
                self.anchor = [attrs["href"], attrs.get("title", ""), []]

        def handle_data(self, data):
            if self.in_heading:
                self.heading.append(data)
            if self.anchor is not None:
                self.anchor[2].append(data)

        def handle_endtag(self, tag):
            if tag in ("h1", "h2", "h3", "h4"):
                self.last_heading = " ".join(" ".join(self.heading).split())
                self.in_heading = False
            if tag == "a" and self.anchor is not None:
                href, title, text = self.anchor
                label = " ".join((title + " " + " ".join(text)).split())
                if not re.search(r"inflation|consumer price|hicp", label, re.I):
                    label = self.last_heading + " " + label
                target = urllib.parse.urljoin(self.base, href).split("?", 1)[0]
                parsed = urllib.parse.urlparse(target)
                if (parsed.hostname == "www.destatis.de" and
                        re.fullmatch(r"/EN/Press/20\d{2}/\d{2}/PE\d{2}_\d+_611\.html", parsed.path)
                        and re.search(r"inflation|consumer price|hicp", label, re.I)):
                    self.items.append((target, label.strip()))
                self.anchor = None
    parser = Articles()
    parser.feed(body.decode("utf-8", "replace"))
    return list(dict(parser.items).items())


def _destatis_blocked_until(db):
    cur = db.cursor()
    cur.execute(f"SELECT f.canonical_url,f.last_seen_at_utc,f.extra_json,f.http_status FROM `{TABLE}` f WHERE f.source_id=%s AND f.record_kind='FETCH_STATUS' AND f.http_status IN (401,403,429) AND NOT EXISTS (SELECT 1 FROM `{TABLE}` s WHERE s.source_id=f.source_id AND s.canonical_url=f.canonical_url AND s.http_status=200 AND s.record_kind IN ('ORIGINAL_INDEX','ORIGINAL_DOCUMENT') AND s.last_seen_at_utc>f.last_seen_at_utc) ORDER BY f.last_seen_at_utc DESC LIMIT 500", ('DEST_ATIS_CPI',))
    failures = cur.fetchall()
    cur.close()
    blocked = {}
    for url, last, raw_extra, status in failures:
        extra = raw_extra if isinstance(raw_extra, dict) else json.loads(raw_extra or '{}')
        wait = (extra.get('response_headers') or {}).get('retry-after')
        retry_at = last + timedelta(seconds=300 if int(status) == 429 else 86400)
        if wait:
            try:
                requested = last + timedelta(seconds=max(0, int(wait)))
            except (ValueError, TypeError):
                try:
                    requested = parsedate_to_datetime(wait).astimezone(timezone.utc).replace(tzinfo=None)
                except (ValueError, TypeError, OverflowError):
                    requested = retry_at
            retry_at = max(retry_at, requested)
        blocked[url] = max(blocked.get(url, datetime.min), retry_at)
    return blocked


def collect_destatis_live(db, now):
    """Independent five-minute RSS lane; never crawl historical indexes here."""
    source, schedule = 'DEST_ATIS_CPI', 'DEST_ATIS_LIVE'
    last = common._schedule_last(db, source, schedule)
    now = common._source_poll_time(now, last, 300)
    if last is not None and (now - last).total_seconds() < 300:
        return {'source': schedule, 'status': 'NOT_DUE'}
    retry_at = _destatis_blocked_until(db).get(DESTATIS_RSS_URL)
    if retry_at and now < retry_at:
        return {'source': schedule, 'status': 'RETRY_DEFERRED', 'next_retry_at_utc': retry_at.isoformat()}
    result = _collect_destatis_rss(db, now)
    result['source'] = schedule
    if not result.get('errors'):
        result['status'] = 'SUCCESS_NEW_DATA' if result.get('first_estimate_values') else 'SUCCESS_UNCHANGED'
        if result.get('candidate_articles') and not result.get('documents'):
            result['status'] = 'SUCCESS_UNCHANGED'
    common._schedule_state(db, source, now, 300, common._collector_outcome(result), schedule_id=schedule)
    return result


def _destatis_release_date(body: bytes):
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = " ".join(text.split())
    m = re.search(r"Press release No\.\s*\d+ of\s+(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})", text, re.I)
    if m:
        try:
            return date(int(m.group(3)), datetime.strptime(m.group(2), "%B").month, int(m.group(1)))
        except ValueError:
            pass
    return None


def _store_destatis_values(db, url: str, title: str, body: bytes,
                           meta: dict, release_date: date, now: datetime):
    text = html.unescape(re.sub(r"<[^>]+>", " ", body.decode("utf-8", "replace")))
    text = " ".join(text.split())
    source_slug = urllib.parse.urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    values = []
    for indicator, heading in (("CPI", "Consumer price index"), ("HICP", "Harmonised index of consumer prices")):
        pattern = re.escape(heading) + r",\s*([A-Z][a-z]+)\s+(20\d{2}):\s*([+\-]?\d+(?:\.\d+)?)%\s+on the same month a year earlier(?:\s*\(([^)]+)\))?"
        m = re.search(pattern, text, re.I)
        if not m:
            continue
        try:
            observation = date(int(m.group(2)), datetime.strptime(m.group(1), "%B").month, 1)
        except ValueError:
            continue
        edition = "PRELIMINARY" if (m.group(4) or "").lower().strip() == "provisional" else "FINAL"
        suffix = "PRELIM" if edition == "PRELIMINARY" else "FINAL"
        values.append((indicator + "_YOY_" + suffix, observation, float(m.group(3)), edition, "PERCENT_YOY"))
        mom = re.match(r"\s*([+\-]?\d+(?:\.\d+)?)%\s+on the previous month", text[m.end():], re.I)
        if mom:
            values.append((indicator + "_MOM_" + suffix, observation, float(mom.group(1)), edition, "PERCENT_MOM"))
    for metric, observation, value, edition, unit in values:
        row = common.base_row(
            "DEST_ATIS_CPI", f"{'preliminary' if edition == 'PRELIMINARY' else 'final'}:{source_slug}:{metric}",
            "FIRST_ESTIMATE" if edition == "PRELIMINARY" else "FINAL_ESTIMATE",
            url, body, meta, now, None, "DAY_ONLY", "DAY_ONLY", title=title,
            event_type=metric, region="GERMANY",
            observation_period_start=observation.isoformat(),
            vintage_month=release_date.replace(day=1).isoformat(),
            numeric_value=value, unit=unit,
            extra_json={"value_kind": edition,
                        "release_document_date": release_date.isoformat(),
                        "publication_timezone": "Europe/Berlin",
                        "publication_clock": "not recovered from original; DAY_ONLY"})
        row["parser_version"] = PARSER_VERSION
        common.store(db, row)
    return len(values)


def collect_destatis_preliminary(db, now: datetime):
    source = "DEST_ATIS_CPI"
    result = {"source": source, "start": BACKFILL_START.isoformat(),
              "indexes": 0, "article_candidates": 0, "documents": 0,
              "first_estimate_values": 0, "errors": []}
    rss = collect_destatis_live(db, now)
    rss.setdefault('documents', 0)
    rss.setdefault('first_estimate_values', 0)
    rss.setdefault('errors', [])
    result["rss"] = rss
    result["documents"] += rss["documents"]
    result["first_estimate_values"] += rss["first_estimate_values"]
    result["errors"].extend(rss["errors"])
    candidates = {}
    # Reparse verified stored originals after parser upgrades without another
    # network request. A transient index failure must not discard known links.
    cur = db.cursor()
    cur.execute(f"SELECT canonical_url,raw_blob_path,content_sha256,title,content_type FROM `{TABLE}` WHERE source_id=%s AND record_kind IN ('ORIGINAL_INDEX','ORIGINAL_DOCUMENT') ORDER BY last_seen_at_utc DESC LIMIT 100", (source,))
    cached = cur.fetchall()
    cur.close()
    blocked_until = _destatis_blocked_until(db)
    seen_sha = set()
    for cached_url, raw_path, sha, cached_title, content_type in cached:
        if sha in seen_sha:
            continue
        seen_sha.add(sha)
        try:
            cached_body = Path(raw_path).read_bytes()
            if hashlib.sha256(cached_body).hexdigest() != sha:
                raise ValueError("CACHED_ORIGINAL_SHA_MISMATCH")
            for target, title in _destatis_article_links(cached_body, cached_url):
                candidates[target] = (title, None)
            release_day = _destatis_release_date(cached_body)
            if release_day and BACKFILL_START <= release_day <= now.date():
                result["cached_values_reparsed"] = result.get("cached_values_reparsed", 0) + _store_destatis_values(
                    db, cached_url, cached_title or "Destatis CPI release", cached_body,
                    {"status": 200, "content_type": content_type, "url": cached_url,
                     "fetch_status": "VERIFIED_STORED_ORIGINAL_REPARSE"}, release_day, now)
        except (OSError, ValueError) as error:
            result["errors"].append({"url": cached_url, "status": str(error)[:120]})
    for idx, url in enumerate(DESTATIS_INDEXES):
        key = "index:" + str(idx)
        if blocked_until.get(url, datetime.min) > now:
            result["errors"].append({"url": url, "status": "BLOCKED_ACCESS_RETRY_DEFERRED", "retry_at_utc": blocked_until[url].isoformat()})
            continue
        try:
            body, meta = common.conditional_fetch(db, source, key, url,
                                                  timeout=20, cap=8_000_000)
            _save_index(db, source, key, url, body, meta, now)
            result["indexes"] += 1
            # Both the CPI topic page and the official press index may expose
            # the current estimate; index availability varies at the origin.
            result["current_page_estimates"] = result.get("current_page_estimates", 0) + _store_destatis_current_page_values(
                db, body, meta, now)
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, source, key, url, now, status, str(e), headers)
            result["errors"].append({"url": url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
            time.sleep(0.25)
            continue
        for target, title in _destatis_article_links(body, url):
            candidates[target] = (title or "Destatis CPI release", None)
        time.sleep(0.2)
    result["article_candidates"] = len(candidates)
    for url, (title, _) in sorted(candidates.items()):
        slug = urllib.parse.urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
        if blocked_until.get(url, datetime.min) > now:
            result["errors"].append({"url": url, "status": "BLOCKED_ACCESS_RETRY_DEFERRED", "retry_at_utc": blocked_until[url].isoformat()})
            continue
        try:
            body, meta = common.conditional_fetch(db, source, "document:" + slug, url,
                                                  timeout=20, cap=8_000_000)
            release_date = _destatis_release_date(body)
            if not release_date or release_date < BACKFILL_START or release_date > now.date():
                result["errors"].append({"url": url, "status": "DATE_UNRESOLVED_OR_OUT_OF_SCOPE"})
                continue
            _store_document(db, source, slug, url, body, meta, now, title,
                            "DEST_ATIS_CPI_RELEASE", evidence="DAY_ONLY",
                            asof="DAY_ONLY", region="GERMANY",
                            extra={"document_date": release_date.isoformat(),
                                   "publication_timezone": "Europe/Berlin",
                                   "expected_preliminary_indicator": True})
            result["documents"] += 1
            result["first_estimate_values"] += _store_destatis_values(
                db, url, title, body, meta, release_date, now)
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, source, "article:" + slug, url, now, status, str(e), headers)
            result["errors"].append({"url": url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
        time.sleep(0.25)
    return result


ADP_PRIMARY_INDEXES = (
    "https://adpemploymentreport.com/Default.aspx",
    "https://mediacenter.adp.com/workforce-data-releases",
    "https://mediacenter.adp.com/workforce-data-releases?o=25",
    "https://mediacenter.adp.com/workforce-data-releases?o=50",
    "https://mediacenter.adp.com/workforce-data-releases?o=75",
    "https://mediacenter.adp.com/workforce-data-releases?o=100",
)
ALFRED_ADP_RELEASE = "https://alfred.stlouisfed.org/release?rid=194"
ALFRED_ADP_DATES = "https://alfred.stlouisfed.org/release/downloaddates?rid=194"
ALFRED_ADP_CALENDAR = "https://alfred.stlouisfed.org/releases/calendar?rid=194&y={year}"
ALFRED_CORE_RELEASES = {
    "CPI": 10,
    "GDP": 53,
    "PCE_PRICE_INDEX": 54,
    "EMPLOYMENT_SITUATION": 50,
}


def _alfred_event_date(url: str, fallback: date | None):
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    rd = (query.get("rd") or [None])[0]
    if rd:
        try:
            return date.fromisoformat(rd)
        except ValueError:
            return fallback
    return fallback


def _index_links(db, source: str, key: str, url: str, now: datetime,
                 cap: int = 8_000_000, timeout: int = 18):
    body, meta = common.conditional_fetch(db, source, key, url, timeout=timeout, cap=cap)
    _save_index(db, source, key, url, body, meta, now)
    parser = LinkParser()
    parser.feed(body.decode("utf-8", "replace"))
    return body, meta, parser.links


def _parse_adp_ner(body: bytes):
    text = html.unescape(re.sub(r"<(script|style)[^>]*>.*?</\1>", " ",
                               body.decode("utf-8", "replace"), flags=re.I | re.S))
    text = " ".join(re.sub(r"<[^>]+>", " ", text).split())
    title_match = re.search(r"<title[^>]*>(.*?)</title", body.decode("utf-8", "replace"), re.I | re.S)
    title = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", title_match.group(1))).split()) if title_match else "ADP National Employment Report"
    published = re.search(r"ROSELAND,\s*N\.J\.?[, ]+(?:–|-|—)?\s*([A-Z][a-z]+\s+\d{1,2},\s+20\d{2})", text, re.I)
    published_day = None
    if published:
        try:
            published_day = datetime.strptime(published.group(1), "%B %d, %Y").date()
        except ValueError:
            pass
    headline = re.search(r"private sector employment\s+(increased|decreased)\s+by\s+([\d,]+)\s+jobs\s+in\s+([A-Z][a-z]+)", text, re.I)
    value = None
    observation = None
    if headline:
        month = None
        try:
            month = datetime.strptime(headline.group(3), "%B").month
        except ValueError:
            pass
        year_match = re.search(r"in\s+" + re.escape(headline.group(3)) + r"\s+(20\d{2})", text, re.I)
        year = int(year_match.group(1)) if year_match else (published_day.year if published_day else None)
        if month and year:
            if not year_match and published_day and month > published_day.month:
                year -= 1
            observation = date(year, month, 1)
            magnitude = int(headline.group(2).replace(",", ""))
            value = magnitude if headline.group(1).lower() == "increased" else -magnitude
    return {"title": title, "published_day": published_day,
            "observation_period": observation, "value": value}


def _store_adp_ner_value(db, url, title, body, meta, parsed, now):
    observation = parsed["observation_period"]
    key = f"adp-ner-first:{observation.isoformat()}"
    row = common.base_row(
        "ADP_NER", key, "FIRST_ESTIMATE", url, body, meta, now, None,
        "DAY_ONLY", "HISTORICAL_REPLAY", title=title,
        event_type="PRIVATE_PAYROLLS", region="UNITED_STATES",
        observation_period_start=observation.isoformat(),
        numeric_value=parsed["value"], unit="JOBS",
        reference_timezone="America/New_York",
        extra_json={"value_class": "original_monthly_adp_ner_headline",
                    "source_publication_day": parsed["published_day"].isoformat() if parsed["published_day"] else None,
                    "source_published_at_unknown": True,
                    "availability_clock": "DAY_ONLY; exact hour not inferred"})
    row["parser_version"] = PARSER_VERSION
    common.store(db, row)


def collect_adp_alfred(db, now: datetime):
    result = {"source": "ADP_NER", "alfred_source": "ALFRED",
              "start": BACKFILL_START.isoformat(), "calendars": 0,
              "release_pages": 0, "release_date_files": 0,
              "primary_indexes": [], "errors": []}
    result["public_report_app_assets"] = []
    # Preserve both official ALFRED release-date downloads. They disclose the
    # source's retrospective date methodology and provide a coverage checklist.
    try:
        _, _, dates_links = _index_links(db, "ALFRED", "adp:release-dates-page",
                                         ALFRED_ADP_DATES, now, timeout=8)
        result["release_date_files"] = 0
        for href, title, _ in dates_links:
            target = urllib.parse.urljoin(ALFRED_ADP_DATES, href)
            if not re.search(r"release_dates_194\.(?:xlsx|txt)$", urllib.parse.urlparse(target).path, re.I):
                continue
            body, meta = common.conditional_fetch(db, "ALFRED", "adp:" + title.lower().replace(" ", "-"),
                                                  target, timeout=20, cap=4_000_000)
            key = urllib.parse.urlparse(target).path.rstrip("/").rsplit("/", 1)[-1]
            _store_document(db, "ALFRED", key, target, body, meta, now, title,
                            "ADP_RELEASE_DATE_CATALOG", None,
                            region="UNITED_STATES",
                            extra={"release_id": 194,
                                   "source_timezone": "America/Chicago",
                                   "backfill_start": BACKFILL_START.isoformat()})
            result["release_date_files"] += 1
    except Exception as e:
        status = e.code if isinstance(e, urllib.error.HTTPError) else None
        headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
        _record_fetch_status(db, "ALFRED", "adp:release-dates-page", ALFRED_ADP_DATES,
                             now, status, str(e), headers)
        result["errors"].append({"url": ALFRED_ADP_DATES, "status": f"{type(e).__name__}:{str(e)[:100]}"})
    for year in (2025, 2026):
        url = ALFRED_ADP_CALENDAR.format(year=year)
        try:
            _, _, events = _index_links(db, "ALFRED", f"adp:calendar:{year}", url, now, timeout=8)
            result["calendars"] += 1
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, "ALFRED", f"adp:calendar:{year}", url, now, status, str(e), headers)
            result["errors"].append({"url": url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
            continue
        candidates = {}
        for href, title, page_date in events:
            if "ADP National Employment Report" not in title:
                continue
            target = urllib.parse.urljoin(url, href)
            d = _alfred_event_date(target, page_date)
            if not d or d < BACKFILL_START or d > now.date():
                continue
            candidates[d] = target
        for d, url in sorted(candidates.items()):
            event_url = url
            if not re.search(r"rd=\d{4}-\d{2}-\d{2}", event_url):
                event_url = f"https://alfred.stlouisfed.org/release?rd={d.isoformat()}&rid=194"
            key = "adp-release:" + d.isoformat()
            try:
                body, meta = common.conditional_fetch(db, "ALFRED", key, event_url,
                                                      timeout=20, cap=8_000_000)
                _store_document(db, "ALFRED", key, event_url, body, meta, now,
                                "ADP National Employment Report",
                                "ADP_HISTORICAL_RELEASE_VINTAGE", d,
                                evidence="SCHEDULE_ONLY", asof="HISTORICAL_VINTAGE",
                                region="UNITED_STATES",
                                extra={"release_id": 194,
                                       "source_timezone": "America/Chicago",
                                       "release_date_from": "ALFRED release calendar",
                                       "scheduled_local_clock": "07:15 AM",
                                       "scheduled_timezone_status": "not independently confirmed"})
                result["release_pages"] += 1
            except Exception as e:
                status = e.code if isinstance(e, urllib.error.HTTPError) else None
                headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
                _record_fetch_status(db, "ALFRED", key, event_url, now, status, str(e), headers)
                result["errors"].append({"url": event_url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
            time.sleep(0.35)
    # Primary ADP publication endpoints are attempted once per run; if blocked
    # or unavailable, the ALFRED release archive above remains separately
    # attributable and is not mislabeled as the original ADP delivery.
    candidates = {}
    for url in ADP_PRIMARY_INDEXES:
        try:
            index_key = "primary-index:" + urllib.parse.urlparse(url).netloc + ":" + hashlib.sha256(url.encode()).hexdigest()[:12]
            body, meta, links = _index_links(db, "ADP_NER", index_key,
                                             url, now, cap=5_000_000)
            result["primary_indexes"].append({"url": url, "status": meta["status"],
                                              "content_type": meta.get("content_type"),
                                              "bytes": len(body),
                                              "sha256": hashlib.sha256(body).hexdigest(),
                                              "link_count": len(links)})
            if (urllib.parse.urlparse(url).hostname or "").lower() == "adpemploymentreport.com":
                # This official site is a thin public shell. Fetch only its
                # referenced static bundle to discover published API routes;
                # do not execute scripts or bypass auth/access controls.
                script_paths = re.findall(r'<script[^>]+src=["\']([^"\']+\.js)["\']',
                                          body.decode("utf-8", "replace"), re.I)
                for script_path in script_paths[:4]:
                    script_url = urllib.parse.urljoin(url, script_path)
                    if (urllib.parse.urlparse(script_url).hostname or "").lower() != "adpemploymentreport.com":
                        continue
                    try:
                        script_body, script_meta = common.conditional_fetch(
                            db, "ADP_NER", "public-app-script:" + hashlib.sha256(script_url.encode()).hexdigest()[:20],
                            script_url, timeout=12, cap=12_000_000)
                        _save_index(db, "ADP_NER", "public-app-script:" + hashlib.sha256(script_url.encode()).hexdigest()[:20],
                                    script_url, script_body, script_meta, now)
                        routes = sorted(set(re.findall(r"(?:https?://[^\s\"'<>]+|/api/[A-Za-z0-9_./?=&%-]+)",
                                                      script_body.decode("utf-8", "replace"))))
                        result["public_report_app_assets"].append({"url": script_url,
                            "http_status": script_meta.get("status"), "content_type": script_meta.get("content_type"),
                            "bytes": len(script_body), "sha256": hashlib.sha256(script_body).hexdigest(),
                            "discovered_public_route_count": len(routes)})
                    except Exception as asset_error:
                        status = asset_error.code if isinstance(asset_error, urllib.error.HTTPError) else None
                        headers = {k.lower(): v for k, v in asset_error.headers.items()} if isinstance(asset_error, urllib.error.HTTPError) and asset_error.headers else {}
                        _record_fetch_status(db, "ADP_NER", "public-app-script:" + hashlib.sha256(script_url.encode()).hexdigest()[:20],
                                             script_url, now, status, str(asset_error), headers)
                        result["errors"].append({"url": script_url,
                            "status": f"{type(asset_error).__name__}:{str(asset_error)[:100]}"})
            for href, title, page_date in links:
                if "ADP National Employment Report" not in title or "Preliminary Estimate" in title:
                    continue
                target = urllib.parse.urljoin(url, href).split("#", 1)[0]
                if (urllib.parse.urlparse(target).hostname or "").lower() not in ("mediacenter.adp.com", "www.mediacenter.adp.com"):
                    continue
                candidates[target] = title
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, "ADP_NER", "primary-index:" + urllib.parse.urlparse(url).netloc,
                                 url, now, status, str(e), headers)
            result["primary_indexes"].append({"url": url, "status": status,
                                              "classification": "BLOCKED_ACCESS" if status in (401, 403, 407) else "NO_HTTP_STATUS"})
        time.sleep(0.35)
    result["monthly_report_candidates"] = len(candidates)
    result["monthly_report_documents"] = 0
    result["first_estimate_values"] = 0
    for url, title in sorted(candidates.items()):
        slug = urllib.parse.urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
        key = "monthly-ner:" + hashlib.sha256(url.encode()).hexdigest()[:28]
        try:
            body, meta = common.conditional_fetch(db, "ADP_NER", key, url, timeout=12, cap=8_000_000)
            parsed = _parse_adp_ner(body)
            day = parsed["published_day"]
            if not day or day < BACKFILL_START or day > now.date():
                continue
            _store_document(db, "ADP_NER", key, url, body, meta, now, title or parsed["title"],
                            "ADP_NATIONAL_EMPLOYMENT_REPORT", evidence="DAY_ONLY",
                            asof="HISTORICAL_REPLAY", region="UNITED_STATES",
                            extra={"document_date": day.isoformat(), "publication_clock": "DAY_ONLY",
                                   "original_monthly_release": True, "source_timezone": "America/New_York"})
            result["monthly_report_documents"] += 1
            if parsed["observation_period"] and parsed["value"] is not None:
                _store_adp_ner_value(db, url, title, body, meta, parsed, now)
                result["first_estimate_values"] += 1
        except Exception as e:
            status = e.code if isinstance(e, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in e.headers.items()} if isinstance(e, urllib.error.HTTPError) and e.headers else {}
            _record_fetch_status(db, "ADP_NER", key, url, now, status, str(e), headers)
            result["errors"].append({"url": url, "status": f"{type(e).__name__}:{str(e)[:100]}"})
        time.sleep(0.25)
    return result


def collect_alfred_core_vintages(db, now: datetime):
    """Capture official ALFRED release/vintage pages for core macro releases.

    The public API requires an API key, so this uses ALFRED's public release
    calendars and per-release-date web pages. These document/vintage snapshots
    are day-only and are not a substitute for an intraday release timestamp.
    """
    source = "ALFRED"
    result = {"source": source, "start": BACKFILL_START.isoformat(),
              "release_families": {}, "vintage_pages": 0, "errors": []}
    unique_events = {}
    for family, rid in ALFRED_CORE_RELEASES.items():
        family_result = {"release_id": rid, "calendar_pages": 0, "candidates": 0}
        for year in (2025, 2026):
            url = f"https://alfred.stlouisfed.org/releases/calendar?rid={rid}&y={year}"
            key = f"core:{family}:calendar:{year}"
            try:
                body, meta, links = _index_links(db, source, key, url, now, timeout=8)
                family_result["calendar_pages"] += 1
            except Exception as exc:
                status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
                headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
                _record_fetch_status(db, source, key, url, now, status, f"{type(exc).__name__}:{exc}", headers)
                result["errors"].append({"family": family, "url": url,
                                         "status": f"{type(exc).__name__}:{str(exc)[:120]}"})
                continue
            for href, title, context_date in links:
                target = urllib.parse.urljoin(url, href)
                q = urllib.parse.parse_qs(urllib.parse.urlparse(target).query)
                if (q.get("rid") or [None])[0] != str(rid):
                    continue
                event_date = _alfred_event_date(target, context_date)
                if not event_date or event_date < BACKFILL_START or event_date.year != year or event_date > now.date():
                    continue
                # Follow only official release detail pages. The release date
                # remains a vintage date; the page may describe when data first
                # became available and does not prove its publication hour.
                if "/release?" not in target:
                    continue
                normalized = urllib.parse.urlunparse(urllib.parse.urlparse(target)._replace(fragment=""))
                unique_events[normalized] = (family, rid, event_date, title or family + " vintage")
                family_result["candidates"] += 1
            time.sleep(.2)
        result["release_families"][family] = family_result
    result["candidate_vintage_pages"] = len(unique_events)
    for url, (family, rid, vintage_date, title) in sorted(unique_events.items()):
        try:
            body, meta = common.conditional_fetch(db, source,
                f"vintage:{rid}:{vintage_date.isoformat()}", url, timeout=8, cap=8_000_000)
            _store_document(db, source, f"vintage:{rid}:{vintage_date.isoformat()}", url,
                            body, meta, now, title, "HISTORICAL_VINTAGE",
                            region="UNITED_STATES",
                            extra={"release_id": rid, "release_family": family,
                                   "vintage_date": vintage_date.isoformat(),
                                   "vintage_date_is_not_publication_time": True,
                                   "source_timezone": "unknown", "api_observations_not_fetched": True})
            result["vintage_pages"] += 1
        except Exception as exc:
            status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
            headers = {k.lower(): v for k, v in exc.headers.items()} if isinstance(exc, urllib.error.HTTPError) and exc.headers else {}
            _record_fetch_status(db, source, f"vintage:{rid}:{vintage_date.isoformat()}",
                                 url, now, status, f"{type(exc).__name__}:{exc}", headers)
            result["errors"].append({"family": family, "date": vintage_date.isoformat(),
                                     "url": url, "status": f"{type(exc).__name__}:{str(exc)[:120]}"})
        time.sleep(.2)
    return result


COLLECTORS = (collect_ecb_history, collect_fed_fomc, collect_bls_access,
              collect_bea, collect_eurostat_flash, collect_destatis_preliminary,
              collect_alfred_core_vintages, collect_adp_alfred)


def run(db, table: str):
    if table != TABLE:
        raise ValueError("REGISTERED_TARGET_MISMATCH")
    common.ensure_schema(db)
    now = _utcnow()
    results, errors = [], []
    for collector in COLLECTORS:
        try:
            results.append(collector(db, now))
        except Exception as e:
            errors.append({"collector": collector.__name__,
                           "error": type(e).__name__ + ":" + str(e)[:200]})
    return {"table": table, "inserted_or_seen_at_utc": now.isoformat(),
            "backfill_start": BACKFILL_START.isoformat(), "collectors": results, "errors": errors}


def main(argv=None):
    import sys
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) < 6:
        raise SystemExit("usage: model100_primary_archive.py TABLE HOST PORT USER PASSWORD DATABASE")
    table, host, port, user, password, database = argv[:6]
    import mysql.connector
    db = mysql.connector.connect(host=host, port=int(port), user=user, password=password,
                                 database=database, connection_timeout=15, autocommit=False)
    try:
        print(json.dumps(run(db, table), ensure_ascii=False, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
