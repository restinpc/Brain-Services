"""
Таблица: vlad_usgs_earthquakes

Запуск:
  python USGS_earthquakes.py vlad_usgs_earthquakes [host] [port] [user] [password] [database]
"""

import os, sys, argparse, json, time, random, traceback
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "usgs_earthquakes")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="USGS_earthquakes.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="USGS Earthquakes Direct → MySQL")
parser.add_argument("table_name")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}
DATASETS = {"vlad_usgs_earthquakes": {"description": "USGS earthquakes M4.5+ (full history + incremental)"}}
BACKFILL_START_YEAR = 2020  # С 2020 года (можно поменять на 2000 или 1970)
MIN_MAGNITUDE = 4.5

def _sf(v):
    if v is None or v == "": return None
    try: return float(str(v).replace(",", ""))
    except: return None

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "BrainProject/1.0 (earthquake-parser)", "Accept": "application/json"})
    return s


class USGSCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                usgs_id VARCHAR(30) NOT NULL COMMENT 'USGS event ID (us7000xxxx)',
                magnitude FLOAT NOT NULL,
                mag_type VARCHAR(10) COMMENT 'ml/mb/mw/ms',
                place VARCHAR(300),
                latitude DECIMAL(10,6), longitude DECIMAL(10,6), depth_km FLOAT,
                event_time DATETIME NOT NULL COMMENT 'UTC',
                event_timestamp BIGINT COMMENT 'Unix ms',
                tsunami TINYINT(1) DEFAULT 0,
                felt_reports INT COMMENT 'Кол-во DYFI felt reports',
                cdi FLOAT COMMENT 'Community Decimal Intensity',
                mmi FLOAT COMMENT 'Modified Mercalli Intensity',
                alert_level VARCHAR(10) COMMENT 'green/yellow/orange/red (PAGER)',
                significance INT COMMENT 'USGS significance score 0-1000+',
                event_type VARCHAR(30) DEFAULT 'earthquake',
                status VARCHAR(20) COMMENT 'automatic/reviewed/deleted',
                url VARCHAR(500) COMMENT 'USGS event page URL',
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_usgs_id (usgs_id),
                INDEX idx_magnitude (magnitude DESC),
                INDEX idx_time (event_time),
                INDEX idx_alert (alert_level)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='USGS earthquakes M4.5+ (direct FDSNWS API, backfill + incremental)';
        """)
        conn.commit(); c.close(); conn.close()

    def _parse_features(self, features):
        rows = []
        for f in features:
            p = f.get("properties", {})
            g = f.get("geometry", {})
            coords = g.get("coordinates", [None, None, None]) if g else [None, None, None]
            ts = p.get("time")
            dt = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S") if ts else None
            rows.append((
                str(f.get("id", p.get("code", "")))[:30],
                _sf(p.get("mag")),
                str(p.get("magType", ""))[:10],
                str(p.get("place", ""))[:300],
                _sf(coords[1]) if len(coords) > 1 else None,
                _sf(coords[0]) if len(coords) > 0 else None,
                _sf(coords[2]) if len(coords) > 2 else None,
                dt, ts,
                1 if p.get("tsunami") else 0,
                int(p["felt"]) if p.get("felt") else None,
                _sf(p.get("cdi")), _sf(p.get("mmi")),
                str(p.get("alert", ""))[:10] or None,
                int(p["sig"]) if p.get("sig") else None,
                str(p.get("type", "earthquake"))[:30],
                str(p.get("status", ""))[:20],
                str(p.get("url", ""))[:500],
            ))
        return rows

    def _insert_rows(self, rows):
        if not rows: return 0
        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (usgs_id, magnitude, mag_type, place, latitude, longitude, depth_km,
             event_time, event_timestamp, tsunami, felt_reports, cdi, mmi,
             alert_level, significance, event_type, status, url)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        for i in range(0, len(rows), 500):
            c.executemany(sql, rows[i:i+500])
        conn.commit(); n = c.rowcount; c.close(); conn.close()
        return n

    def process(self):
        self.ensure_table()
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"SELECT MAX(event_time) FROM `{self.table_name}`")
        last_time = c.fetchone()[0]
        c.close(); conn.close()

        if last_time is None:
            # BACKFILL: по годам
            print(f"    BACKFILL: {BACKFILL_START_YEAR}–{datetime.utcnow().year}")
            total_inserted = 0
            for year in range(BACKFILL_START_YEAR, datetime.utcnow().year + 1):
                start = f"{year}-01-01"
                end = f"{year + 1}-01-01" if year < datetime.utcnow().year else datetime.utcnow().strftime("%Y-%m-%d")
                url = (f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
                       f"format=geojson&minmagnitude={MIN_MAGNITUDE}&starttime={start}&endtime={end}&limit=20000")
                print(f"      {year}: ", end="", flush=True)
                resp = self.session.get(url, timeout=60)
                if resp.status_code != 200:
                    print(f"HTTP {resp.status_code}"); continue
                data = resp.json()
                features = data.get("features", [])
                rows = self._parse_features(features)
                n = self._insert_rows(rows)
                total_inserted += n
                print(f"{len(features)} events → {n} new")
                time.sleep(random.uniform(1.5, 3.0))
            print(f"    BACKFILL: {total_inserted} total inserted")
        else:
            # INCREMENTAL: feed (последний месяц) + query (от last_time - 2 дня)
            print(f"    INCREMENTAL от {last_time}")
            # Fast path: month feed
            resp = self.session.get(
                f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{MIN_MAGNITUDE}_month.geojson",
                timeout=30
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])
            rows = self._parse_features(features)
            n = self._insert_rows(rows)
            print(f"    Feed: {len(features)} events → {n} new")


def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица. Допустимые:"); [print(f"  - {n}") for n in DATASETS]; sys.exit(1)
    print(f" USGS Earthquake Direct Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}"); print("=" * 60)
    USGSCollector(args.table_name).process()
    print("=" * 60); print(" ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n Прервано"); sys.exit(1)
    except Exception as e: print(f"\n {e!r}"); send_error_trace(e); sys.exit(1)
