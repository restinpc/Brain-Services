#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import random
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode

from dotenv import load_dotenv
import mysql.connector

load_dotenv()

# ---------- CONFIG ----------
SETTINGS = {
    "base_page": "https://ru.investing.com/economic-calendar/",
    "api_occ": "https://endpoints.investing.com/pd-instruments/v1/calendars/economic/events/occurrences",
    "domain_id": 7,
    "limit": 500,
    "countries": "5,72,35,4,6,25,12,37,17,11,19,14,10,22,39,36,43",
}

TABLE_NAME = "vlad_investing_calendar"

IMPORTANCE_MAP = {"low": 1, "medium": 2, "high": 3}

START_FALLBACK = date(1970, 1, 1)
END_DATE_UTC = datetime.now(timezone.utc).date()
LOOKBACK_DAYS = 7


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ---------- DATE HELPERS (без pandas) ----------
def add_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 12:
        return date(y + 1, 1, 1)
    return date(y, m + 1, 1)


def month_ranges(start_d: date, end_d: date) -> List[Tuple[date, date]]:
    cur = date(start_d.year, start_d.month, 1)
    out: List[Tuple[date, date]] = []
    while cur <= end_d:
        nm = add_month(cur)
        last_day = nm - timedelta(days=1)
        out.append((max(start_d, cur), min(end_d, last_day)))
        cur = nm
    return out


# ---------- DB ----------
class DB:
    def get_db_connection(self):
        return mysql.connector.connect(
            host=os.getenv("DB_HOST", ""),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", ""),
        )

    def ensure_table(self) -> None:
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
                    occurrence_id BIGINT PRIMARY KEY,
                    occurrence_time_utc DATETIME NULL,
                    event_id INT NULL,

                    currency VARCHAR(8) NULL,
                    importance TINYINT NULL,
                    event_name VARCHAR(255) NULL,

                    actual VARCHAR(64) NULL,
                    forecast VARCHAR(64) NULL,
                    previous VARCHAR(64) NULL,

                    country_id INT NULL,
                    category VARCHAR(64) NULL,
                    source VARCHAR(255) NULL,
                    page_link VARCHAR(255) NULL,

                    unit VARCHAR(16) NULL,
                    reference_period VARCHAR(32) NULL,

                    preliminary BOOLEAN NULL,
                    precision_value INT NULL,
                    previous_revised_from VARCHAR(64) NULL,

                    actual_to_forecast VARCHAR(16) NULL,
                    revised_to_previous VARCHAR(16) NULL,

                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    INDEX idx_time (occurrence_time_utc),
                    INDEX idx_event (event_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            conn.commit()

    def get_max_time(self) -> Optional[datetime]:
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(occurrence_time_utc) FROM `{TABLE_NAME}`")
            (mx,) = cur.fetchone()
            return mx

    def insert_ignore_batch(self, batch: List[Dict[str, Any]]) -> int:
        if not batch:
            return 0

        sql = f"""
            INSERT IGNORE INTO `{TABLE_NAME}` (
                occurrence_id, occurrence_time_utc, event_id,
                currency, importance, event_name,
                actual, forecast, previous,
                country_id, category, source, page_link,
                unit, reference_period,
                preliminary, precision_value, previous_revised_from,
                actual_to_forecast, revised_to_previous
            ) VALUES (
                %(occurrence_id)s, %(occurrence_time_utc)s, %(event_id)s,
                %(currency)s, %(importance)s, %(event_name)s,
                %(actual)s, %(forecast)s, %(previous)s,
                %(country_id)s, %(category)s, %(source)s, %(page_link)s,
                %(unit)s, %(reference_period)s,
                %(preliminary)s, %(precision_value)s, %(previous_revised_from)s,
                %(actual_to_forecast)s, %(revised_to_previous)s
            )
        """
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.executemany(sql, batch)
            conn.commit()
            return cur.rowcount


# ---------- VALUE HELPERS ----------
def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def safe_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "1", "yes"}:
        return True
    if s in {"false", "0", "no"}:
        return False
    return None


def safe_str(v: Any, max_len: int) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    return s[:max_len]


def parse_occurrence_time_utc_to_mysql_dt(iso_z: Any) -> Optional[datetime]:
    """
    ISO '...Z' -> naive UTC datetime для MySQL DATETIME: 'YYYY-MM-DD HH:MM:SS'
    """
    if not iso_z:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_z).replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


# ---------- FETCH ----------
def build_occ_url(start_d: date, end_d: date, limit: int, cursor: Optional[str]) -> str:
    params = {
        "domain_id": str(SETTINGS["domain_id"]),
        "limit": str(limit),
        "start_date": f"{start_d.isoformat()}T00:00:00Z",
        "end_date": f"{end_d.isoformat()}T23:59:59Z",
        "country_ids": SETTINGS["countries"],
    }
    if cursor:
        params["cursor"] = cursor
    return f"{SETTINGS['api_occ']}?{urlencode(params)}"


def request_json_with_retries(context, url: str, headers: Dict[str, str], tries: int = 4) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            resp = context.request.get(url, headers=headers, timeout=60000)
            if resp.status == 200:
                return resp.json()

            if resp.status in {429, 500, 502, 503, 504}:
                wait = min(10, 0.7 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.6)
                log(f"HTTP {resp.status} retry {attempt}/{tries}, sleep {wait:.1f}s")
                time.sleep(wait)
                continue

            raise RuntimeError(f"HTTP {resp.status}: {resp.text()[:300]}")
        except Exception as e:
            last_err = e
            wait = min(10, 0.7 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.6)
            log(f"Request error retry {attempt}/{tries}: {e}, sleep {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(f"Failed after {tries} tries: {last_err}")


def fetch_all_pages_for_range(context, start_d: date, end_d: date, limit: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    headers = {
        "domain-id": str(SETTINGS["domain_id"]),
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru-RU,ru;q=0.9,en;q=0.8",
        "referer": SETTINGS["base_page"],
        "origin": "https://ru.investing.com",
    }

    all_occ: List[Dict[str, Any]] = []
    all_events: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page_num = 0

    while True:
        page_num += 1
        url = build_occ_url(start_d, end_d, limit=limit, cursor=cursor)
        data = request_json_with_retries(context, url, headers=headers)

        occ = data.get("occurrences", []) or []
        events = data.get("events", []) or []
        cursor = data.get("next_page_cursor")

        all_occ.extend(occ)
        all_events.extend(events)

        log(f"  page {page_num}: occurrences={len(occ)} events={len(events)} cursor={'yes' if cursor else 'no'}")

        if not cursor:
            break

        time.sleep(random.uniform(0.3, 0.9))

    return all_occ, all_events


def occurrence_to_db_row(o: Dict[str, Any], event_map: Dict[int, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    occ_id = safe_int(o.get("occurrence_id"))
    if occ_id is None:
        return None

    eid = safe_int(o.get("event_id"))
    ev = event_map.get(eid, {}) if eid is not None else {}

    name = ev.get("event_translated") or ev.get("long_name") or ev.get("short_name")
    importance = IMPORTANCE_MAP.get(str(ev.get("importance", "")).lower(), 1)

    return {
        "occurrence_id": occ_id,
        "occurrence_time_utc": parse_occurrence_time_utc_to_mysql_dt(o.get("occurrence_time")),
        "event_id": eid,

        "currency": safe_str(ev.get("currency"), 8),
        "importance": importance,
        "event_name": safe_str(name, 255),

        "actual": safe_str(o.get("actual"), 64),
        "forecast": safe_str(o.get("forecast"), 64),
        "previous": safe_str(o.get("previous"), 64),

        "country_id": safe_int(ev.get("country_id")),
        "category": safe_str(ev.get("category"), 64),
        "source": safe_str(ev.get("source"), 255),
        "page_link": safe_str(ev.get("page_link"), 255),

        "unit": safe_str(o.get("unit"), 16),
        "reference_period": safe_str(o.get("reference_period"), 32),

        "preliminary": safe_bool(o.get("preliminary")),
        "precision_value": safe_int(o.get("precision")),
        "previous_revised_from": safe_str(o.get("previous_revised_from"), 64),

        "actual_to_forecast": safe_str(o.get("actual_to_forecast"), 16),
        "revised_to_previous": safe_str(o.get("revised_to_previous"), 16),
    }


# ---------- MAIN ----------
def main() -> int:
    db = DB()
    db.ensure_table()

    last_time = db.get_max_time()
    if last_time:
        start_d = last_time.date() - timedelta(days=LOOKBACK_DAYS)
        if start_d < START_FALLBACK:
            start_d = START_FALLBACK
        log(f"Incremental start: {start_d.isoformat()} (lookback={LOOKBACK_DAYS}d)")
    else:
        start_d = START_FALLBACK
        log(f"DB empty. Full start: {start_d.isoformat()}")

    end_d = END_DATE_UTC
    log(f"End date (today UTC): {end_d.isoformat()}")
    log(f"DB={os.getenv('DB_NAME','')} Table={TABLE_NAME}")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit("Install: pip install playwright mysql-connector-python python-dotenv && playwright install chromium")

    inserted_total = 0
    seen_total = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            locale="ru-RU",
            timezone_id="UTC",
        )
        page = context.new_page()

        log("Opening calendar page...")
        page.goto(SETTINGS["base_page"], timeout=60000)
        page.wait_for_timeout(1500)

        ranges = month_ranges(start_d, end_d)
        log(f"Month chunks: {len(ranges)}")

        event_map: Dict[int, Dict[str, Any]] = {}

        for i, (sd, ed) in enumerate(ranges, 1):
            log(f"[{i}/{len(ranges)}] Fetching {sd.isoformat()} .. {ed.isoformat()}")
            occ, events = fetch_all_pages_for_range(context, sd, ed, limit=SETTINGS["limit"])

            for e in events:
                eid = safe_int(e.get("event_id"))
                if eid is not None:
                    event_map[eid] = e

            if not occ:
                continue

            batch: List[Dict[str, Any]] = []
            for o in occ:
                row = occurrence_to_db_row(o, event_map)
                if row:
                    batch.append(row)

            seen_total += len(batch)
            inserted = db.insert_ignore_batch(batch)
            inserted_total += inserted
            log(f"  rows seen={len(batch)} inserted_new={inserted}")

            time.sleep(random.uniform(0.15, 0.5))

        browser.close()

    log(f"Done. Seen={seen_total} Inserted_new={inserted_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
