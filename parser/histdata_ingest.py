"""Small, restart-safe HistData.com monthly EUR/USD tick archive downloader.

HistData quotes use a fixed EST clock (no daylight-saving adjustment). The
numeric Volume field in these quote exports is commonly zero and is not
executed trade volume; this module intentionally does not emit volume features.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html.parser
import io
import json
import os
from pathlib import Path
import re
import time
import zipfile

BASE = "https://www.histdata.com"
FORM_URL = BASE + "/download-free-forex-historical-data/?/ascii/tick-data-quotes/eurusd/{year}/{month:02d}"
PARSER_VERSION = "histdata-eurusd-tick-v1"


class _HiddenForm(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_form = False
        self.inputs: dict[str, str] = {}

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "form" and a.get("id") == "file_down" and a.get("action") == "/get.php":
            self.in_form = True
        elif tag == "form" and self.in_form:
            self.in_form = False
        elif tag == "input" and self.in_form and a.get("name"):
            self.inputs[a["name"]] = a.get("value", "")

    def handle_endtag(self, tag):
        if tag == "form":
            self.in_form = False


def download_month(year: int, month: int, destination: Path, *, session=None,
                   max_bytes: int = 100 * 1024 * 1024, retries: int = 3) -> dict:
    if year < 2000 or not 1 <= month <= 12:
        raise ValueError("invalid month")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if session is None:
        import requests
        sess = requests.Session()
    else:
        sess = session
    page_url = FORM_URL.format(year=year, month=month)
    last_error = None
    for attempt in range(retries):
        tmp = destination.with_suffix(destination.suffix + ".part")
        try:
            page = sess.get(page_url, timeout=(15, 60))
            page.raise_for_status()
            form = _HiddenForm()
            form.feed(page.text)
            fields = form.inputs
            if not fields or fields.get("fxpair") != "EURUSD" or fields.get("timeframe") != "T":
                raise ValueError("HistData page did not return the expected EURUSD tick form")
            response = sess.post(BASE + "/get.php", data=fields,
                headers={"Referer": page_url, "Origin": BASE}, timeout=(15, 120), stream=True)
            response.raise_for_status()
            if "zip" not in response.headers.get("content-type", "").lower() and ".zip" not in response.headers.get("content-disposition", "").lower():
                raise ValueError("HistData response is not a ZIP archive")
            digest = hashlib.sha256()
            size = 0
            with tmp.open("wb") as out:
                for block in response.iter_content(1024 * 1024):
                    if not block:
                        continue
                    size += len(block)
                    if size > max_bytes:
                        raise ValueError(f"archive exceeds configured cap ({max_bytes} bytes)")
                    out.write(block)
                    digest.update(block)
                out.flush()
                os.fsync(out.fileno())
            validate_archive(tmp, year, month)
            os.replace(tmp, destination)
            return {"provider": "HistData", "instrument": "EURUSD", "year": year,
                    "month": month, "path": str(destination), "bytes": size,
                    "sha256": digest.hexdigest(), "parser_version": PARSER_VERSION,
                    "asof_status": "SOURCE_HISTORICAL", "volume_type": "UNAVAILABLE"}
        except Exception as exc:  # retain the verified prior archive; restart the bounded month on retry
            last_error = exc
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))
    raise RuntimeError(f"HistData {year}-{month:02d} download failed after {retries} tries: {last_error}")


def validate_archive(path: Path, year: int, month: int) -> dict:
    with zipfile.ZipFile(path) as zf:
        members = [n for n in zf.namelist() if not n.endswith("/")]
        if len(members) != 1 or not re.fullmatch(r"DAT_ASCII_EURUSD_T_\d{6}\.csv", Path(members[0]).name):
            raise ValueError("unexpected HistData ZIP member")
        count = 0
        previous = None
        with zf.open(members[0]) as binary:
            text = io.TextIOWrapper(binary, encoding="ascii", newline="")
            for row in csv.reader(text, delimiter=","):
                if len(row) != 4:
                    raise ValueError("expected timestamp,bid,ask,volume")
                stamp, bid_text, ask_text, _volume = row
                parsed = time.strptime(stamp[:15], "%Y%m%d %H%M%S")
                if parsed.tm_year != year or parsed.tm_mon != month:
                    raise ValueError("record timestamp outside requested HistData month")
                bid, ask = float(bid_text), float(ask_text)
                if not (0 < bid <= ask < 1000):
                    raise ValueError("invalid HistData bid/ask")
                if previous is not None and stamp < previous:
                    raise ValueError("out-of-order HistData timestamp")
                previous = stamp
                count += 1
        if not count:
            raise ValueError("empty HistData archive")
        return {"rows": count, "member": members[0]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("year", type=int)
    p.add_argument("month", type=int)
    p.add_argument("destination", type=Path)
    p.add_argument("--max-bytes", type=int, default=100 * 1024 * 1024)
    args = p.parse_args()
    print(json.dumps(download_month(args.year, args.month, args.destination,
                                   max_bytes=args.max_bytes), sort_keys=True))


if __name__ == "__main__":
    main()
