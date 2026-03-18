"""
Таблица: vlad_gdelt_geo_events (геолоцированные события)

Запуск:
  python GDELT_events.py vlad_gdelt_geo_events [host] [port] [user] [password] [database]
"""

import os, sys, argparse, json, time, random, traceback, hashlib
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "gdelt_events")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="GDELT_events.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="GDELT GEO Events → MySQL")
parser.add_argument("table_name")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}
DATASETS = {"vlad_gdelt_geo_events": {"description": "GDELT geo-located events"}}

# Темы без сильных пересечений
GDELT_QUERIES = [
    {"query": "PROTEST",          "category": "protest"},
    {"query": "STRIKE",           "category": "protest"},
    {"query": "ARMEDCONFLICT",    "category": "conflict"},
    {"query": "MILITARY",         "category": "conflict"},
    {"query": "SANCTIONS",        "category": "economic"},
    {"query": "NATURAL_DISASTER", "category": "natural"},
    {"query": "ELECTION",         "category": "political"},
    {"query": "POLITICAL_TURMOIL","category": "political"},
]

def _sf(v):
    if v is None or v == "": return None
    try: return float(str(v).replace(",", ""))
    except: return None

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "BrainProject/1.0 (gdelt-parser)", "Accept": "application/json"})
    return s


class GDELTCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_hash VARCHAR(40) NOT NULL,
                name TEXT,
                category VARCHAR(30),
                latitude DECIMAL(10,6), longitude DECIMAL(10,6),
                country VARCHAR(100),
                mention_count INT,
                source_url TEXT,
                shareimage TEXT,
                tone FLOAT,
                raw_json TEXT,
                snapshot_hour DATETIME NOT NULL,
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_hash_hour (event_hash, snapshot_hour),
                INDEX idx_category (category),
                INDEX idx_snapshot (snapshot_at),
                INDEX idx_mentions (mention_count DESC)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='GDELT geo-located events from GKG v1 (MAXROWS=2500)';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()
        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        all_rows = []
        for q in GDELT_QUERIES:
            url = "https://api.gdeltproject.org/api/v1/gkg_geojson"
            params = {
                "QUERY": q["query"],
                "TIMESPAN": "1440",
                "MAXROWS": "2500"          # ← ВЕРНУЛИ ПОЛНЫЙ ОБЪЁМ
            }
            print(f"   🌍 GDELT GKG v1 [{q['query']}] → {q['category']}...", end=" ", flush=True)

            try:
                resp = self.session.get(url, params=params, timeout=40)
                if resp.status_code != 200:
                    print(f"HTTP {resp.status_code}")
                    print(f"   🔗 {resp.url}")
                    continue

                data = resp.json()
                features = data.get("features", [])
                print(f"{len(features)} событий")

                for f in features:
                    props = f.get("properties", {})
                    coords = f.get("geometry", {}).get("coordinates", [None, None])
                    name = props.get("name", "")[:2000]
                    lon = _sf(coords[0]) if len(coords) > 0 else None
                    lat = _sf(coords[1]) if len(coords) > 1 else None

                    hash_input = f"{name[:100]}|{lat}|{lon}|{q['category']}"
                    event_hash = hashlib.sha1(hash_input.encode()).hexdigest()[:40]

                    country = name.split(",")[-1].strip()[:100] if "," in name else ""
                    mention_count = 1
                    source_url = str(props.get("url", ""))[:2000] or None
                    shareimage = None
                    tone = _sf(props.get("urltone"))

                    raw_json = json.dumps(props, ensure_ascii=False, default=str)[:3000]

                    all_rows.append((
                        event_hash, name, q["category"],
                        lat, lon, country, mention_count,
                        source_url, shareimage, tone, raw_json,
                        snapshot_hour, snapshot_at
                    ))
            except Exception as e:
                print(f"ERROR: {e}")
            time.sleep(random.uniform(1.5, 3))

        if not all_rows:
            print("   ⚠️ Нет данных")
            return

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (event_hash, name, category, latitude, longitude, country,
             mention_count, source_url, shareimage, tone, raw_json, snapshot_hour, snapshot_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        c.executemany(sql, all_rows)
        conn.commit(); n = c.rowcount; c.close(); conn.close()

        print(f"   ✅ Записано {n} уникальных событий")
        cats = {}
        for r in all_rows:
            cats[r[2]] = cats.get(r[2], 0) + 1
        for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"      {cat:12s}: {cnt}")


def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица."); sys.exit(1)
    print(f"🚀 GDELT GEO Events Direct Collector (GKG v1 + MAXROWS=2500)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Таблица: {args.table_name}"); print("=" * 60)
    GDELTCollector(args.table_name).process()
    print("=" * 60); print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n🛑 Прервано"); sys.exit(1)
    except Exception as e: print(f"\n❌ {e!r}"); send_error_trace(e); sys.exit(1)