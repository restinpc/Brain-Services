"""
Таблица: vlad_wm_ais_snapshot

Запуск:
  python WorldMonitor_ais_maritime.py vlad_wm_ais_snapshot [host] [port] [user] [password] [database]
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
NODE_NAME = os.getenv("NODE_NAME", "wm_ais_maritime")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")
WM_API_BASE = os.getenv("WM_API_BASE", "https://api.worldmonitor.app")

def send_error_trace(exc, script_name="WorldMonitor_ais_maritime.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="WorldMonitor AIS Maritime Snapshot → MySQL")
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
DATASETS = {"vlad_wm_ais_snapshot": {"description": "AIS maritime snapshot: disruptions + chokepoints + naval"}}

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json", "Origin": "https://worldmonitor.app", "Referer": "https://worldmonitor.app/"})
    return s

def wm_get(session, path, params=None, timeout=30):
    url = f"{WM_API_BASE}{path}"
    for attempt in range(3):
        try:
            if attempt > 0: time.sleep(1.5 ** attempt + random.uniform(0.2, 0.8))
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


class AISCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sequence_id BIGINT COMMENT 'AIS sequence number',
                api_timestamp VARCHAR(50) COMMENT 'Timestamp из API',
                ais_status VARCHAR(50) COMMENT 'connected/degraded/offline',
                disruption_count INT COMMENT 'Кол-во disruptions',
                disruptions_json LONGTEXT COMMENT 'Полный JSON disruptions',
                density_json LONGTEXT COMMENT 'Vessel density grid',
                candidate_count INT COMMENT 'Кол-во naval candidates',
                candidates_json LONGTEXT COMMENT 'Полный JSON candidateReports',
                total_vessels INT COMMENT 'Общее кол-во судов',
                snapshot_hour DATETIME NOT NULL COMMENT 'Для дедупликации',
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_snapshot_hour (snapshot_hour),
                INDEX idx_sequence (sequence_id),
                INDEX idx_snapshot (snapshot_at),
                INDEX idx_disruptions (disruption_count DESC)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='WorldMonitor: AIS maritime snapshot (chokepoints + naval)';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()

        data = wm_get(self.session, "/api/ais-snapshot")
        if not data:
            print("    Нет данных от AIS snapshot")
            return

        # Ответ: {sequence, timestamp, status, disruptions, density, candidateReports}
        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        disruptions = data.get("disruptions", [])
        if isinstance(disruptions, dict): disruptions = disruptions.get("events", disruptions.get("data", []))
        candidates = data.get("candidateReports", data.get("candidates", []))
        if isinstance(candidates, dict): candidates = candidates.get("reports", candidates.get("data", []))
        density = data.get("density", {})

        # Подсчёт судов из density grid
        total_vessels = 0
        if isinstance(density, dict):
            for cell in density.values() if isinstance(density, dict) else []:
                if isinstance(cell, (int, float)):
                    total_vessels += int(cell)
        elif isinstance(density, list):
            for cell in density:
                if isinstance(cell, dict):
                    total_vessels += int(cell.get("count", cell.get("vessels", 0)))

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (sequence_id, api_timestamp, ais_status, disruption_count, disruptions_json,
             density_json, candidate_count, candidates_json, total_vessels, snapshot_hour, snapshot_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        row = (
            _si(data.get("sequence")),
            str(data.get("timestamp", ""))[:50],
            str(data.get("status", "unknown"))[:50],
            len(disruptions) if isinstance(disruptions, list) else 0,
            json.dumps(disruptions, ensure_ascii=False, default=str)[:50000],
            json.dumps(density, ensure_ascii=False, default=str)[:50000],
            len(candidates) if isinstance(candidates, list) else 0,
            json.dumps(candidates, ensure_ascii=False, default=str)[:50000],
            total_vessels if total_vessels > 0 else None,
            snapshot_hour,
            snapshot_at,
        )

        c.execute(sql, row)
        conn.commit(); c.close(); conn.close()

        print(f"    AIS snapshot записан:")
        print(f"      Sequence: {data.get('sequence')}")
        print(f"      Status: {data.get('status')}")
        print(f"      Disruptions: {len(disruptions) if isinstance(disruptions, list) else '?'}")
        print(f"      Naval candidates: {len(candidates) if isinstance(candidates, list) else '?'}")

        # Детали disruptions
        if isinstance(disruptions, list):
            for d in disruptions[:5]:
                name = d.get("chokepoint", d.get("name", d.get("region", "?")))
                score = d.get("severity", d.get("score", d.get("level", "?")))
                print(f"       {name}: severity={score}")


def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица. Допустимые:")
        for n in DATASETS: print(f"  - {n}")
        sys.exit(1)
    print(f" WorldMonitor AIS Maritime Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}"); print("=" * 60)
    AISCollector(args.table_name).process()
    print("=" * 60); print(" ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n Прервано"); sys.exit(1)
    except Exception as e: print(f"\n Критическая ошибка: {e!r}"); send_error_trace(e); sys.exit(1)
