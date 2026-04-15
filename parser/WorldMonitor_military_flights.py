"""
Таблица: vlad_wm_military_flights

Запуск:
  python WorldMonitor_military_flights.py vlad_wm_military_flights [host] [port] [user] [password] [database]
"""

import os, sys, argparse, json, time, random, traceback
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "wm_military_flights")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")
WM_API_BASE = os.getenv("WM_API_BASE", "https://api.worldmonitor.app")

def send_error_trace(exc, script_name="WorldMonitor_military_flights.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="WorldMonitor Military Flights (ADS-B) → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}
DATASETS = {"vlad_wm_military_flights": {"description": "Military ADS-B flights with classification"}}

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json", "Origin": "https://worldmonitor.app", "Referer": "https://worldmonitor.app/"})
    return s

def wm_get(session, path, params=None, timeout=30):
    url = f"{WM_API_BASE}{path}"
    for attempt in range(3):
        try:
            if attempt > 0: time.sleep(1.5**attempt + random.uniform(0.2, 0.8))
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429: time.sleep(int(resp.headers.get("Retry-After", 10))); continue
            resp.raise_for_status(); return resp.json()
        except Exception as e: print(f"    {path}: {e}")
    return None

def _sf(v):
    if v is None or v == "": return None
    try: return float(str(v).replace(",", ""))
    except: return None
def _si(v):
    if v is None or v == "": return None
    try: return int(float(str(v).replace(",", "")))
    except: return None


class MilitaryFlightsCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                icao_hex VARCHAR(10) COMMENT 'ICAO 24-bit hex',
                callsign VARCHAR(20),
                country VARCHAR(100) COMMENT 'Страна регистрации',
                aircraft_type VARCHAR(100) COMMENT 'Тип ВС',
                mission_type VARCHAR(50) COMMENT 'tanker/awacs/fighter/isr/transport/heli/unknown',
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                altitude_ft INT,
                speed_kts INT,
                heading DECIMAL(6,2),
                on_ground TINYINT(1) DEFAULT 0,
                squawk VARCHAR(10),
                theater VARCHAR(50) COMMENT 'Ближайший theater (если определён)',
                confidence VARCHAR(20) COMMENT 'Уверенность классификации',
                raw_json TEXT,
                snapshot_hour DATETIME NOT NULL COMMENT 'Для дедупликации',
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_hex_hour (icao_hex, snapshot_hour),
                INDEX idx_callsign (callsign),
                INDEX idx_mission (mission_type),
                INDEX idx_country (country),
                INDEX idx_theater (theater),
                INDEX idx_snapshot (snapshot_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='WorldMonitor: military ADS-B flights with classification';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()

        data = wm_get(self.session, "/api/military-flights")
        if not data:
            print("    Нет данных military flights")
            return

        flights = data.get("flights", [])
        stats = data.get("stats", {})
        fetched_at = data.get("fetchedAt", "")
        audit = data.get("classificationAudit", {})

        if not flights:
            print("    Нет полётов в ответе")
            return

        print(f"    Получено {len(flights)} военных полётов")
        if stats:
            print(f"      Stats: {json.dumps(stats, default=str)[:200]}")

        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (icao_hex, callsign, country, aircraft_type, mission_type,
             latitude, longitude, altitude_ft, speed_kts, heading, on_ground, squawk,
             theater, confidence, raw_json, snapshot_hour, snapshot_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        rows = []
        for f in flights:
            rows.append((
                str(f.get("hex", f.get("icao", "")))[:10],
                str(f.get("callsign", f.get("flight", "")))[:20].strip(),
                f.get("country", f.get("operator", f.get("flag", "")))[:100],
                f.get("type", f.get("aircraftType", f.get("t", "")))[:100],
                f.get("missionType", f.get("mission", f.get("classification", "unknown")))[:50],
                _sf(f.get("lat", f.get("latitude"))),
                _sf(f.get("lon", f.get("lng", f.get("longitude")))),
                _si(f.get("alt", f.get("altitude", f.get("alt_baro")))),
                _si(f.get("spd", f.get("speed", f.get("gs")))),
                _sf(f.get("hdg", f.get("heading", f.get("track")))),
                1 if f.get("onGround", f.get("on_ground", False)) else 0,
                str(f.get("squawk", ""))[:10],
                f.get("theater", f.get("region"))[:50] if f.get("theater", f.get("region")) else None,
                f.get("confidence", f.get("identMethod"))[:20] if f.get("confidence", f.get("identMethod")) else None,
                json.dumps(f, ensure_ascii=False, default=str)[:3000],
                snapshot_hour,
                snapshot_at,
            ))

        for i in range(0, len(rows), 500):
            c.executemany(sql, rows[i:i+500])
        conn.commit()
        inserted = c.rowcount
        c.close(); conn.close()

        print(f"    Записано {len(rows)} полётов")

        # Статистика по типам миссий
        mission_counts = {}
        for f in flights:
            mt = f.get("missionType", f.get("mission", f.get("classification", "unknown")))
            mission_counts[mt] = mission_counts.get(mt, 0) + 1
        print("    По типам миссий:")
        for mt, cnt in sorted(mission_counts.items(), key=lambda x: -x[1]):
            print(f"      {mt:15s}: {cnt}")


def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица. Допустимые:")
        for n in DATASETS: print(f"  - {n}")
        sys.exit(1)
    print(f" WorldMonitor Military Flights Collector (ADS-B)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}"); print("=" * 60)
    MilitaryFlightsCollector(args.table_name).process()
    print("=" * 60); print(" ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n Прервано"); sys.exit(1)
    except Exception as e: print(f"\n Критическая ошибка: {e!r}"); send_error_trace(e); sys.exit(1)
