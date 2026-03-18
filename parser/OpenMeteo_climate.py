"""
WM мониторит 15 conflict-prone зон, вычисляет 30-day baseline из ERA5, сравнивает с текущими.
Severity: >5°C = Extreme, >3°C = Moderate.

Таблица: vlad_openmeteo_climate

Запуск:
  python OpenMeteo_climate.py vlad_openmeteo_climate [host] [port] [user] [password] [database]
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
NODE_NAME = os.getenv("NODE_NAME", "openmeteo_climate")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="OpenMeteo_climate.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="Open-Meteo ERA5 Climate Anomalies → MySQL")
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
DATASETS = {"vlad_openmeteo_climate": {"description": "Open-Meteo ERA5 climate anomalies (15 conflict zones)"}}

# WorldMonitor's 15 conflict-prone monitoring zones (extracted from source)
ZONES = [
    {"name": "Ukraine", "lat": 48.4, "lon": 35.0},
    {"name": "Gaza/Israel", "lat": 31.4, "lon": 34.4},
    {"name": "Syria", "lat": 33.5, "lon": 36.3},
    {"name": "Yemen", "lat": 15.4, "lon": 44.2},
    {"name": "Sudan", "lat": 15.6, "lon": 32.5},
    {"name": "Horn of Africa", "lat": 5.0, "lon": 42.0},
    {"name": "Sahel (Mali/Burkina)", "lat": 14.0, "lon": -2.0},
    {"name": "Myanmar", "lat": 20.0, "lon": 96.5},
    {"name": "Afghanistan", "lat": 34.0, "lon": 69.0},
    {"name": "Taiwan Strait", "lat": 24.0, "lon": 119.5},
    {"name": "Persian Gulf", "lat": 27.0, "lon": 51.0},
    {"name": "Nigeria", "lat": 9.0, "lon": 7.5},
    {"name": "Ethiopia", "lat": 9.0, "lon": 38.7},
    {"name": "Pakistan", "lat": 30.4, "lon": 69.3},
    {"name": "Black Sea", "lat": 43.0, "lon": 34.0},
]

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "BrainProject/1.0", "Accept": "application/json"})
    return s


class ClimateCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                zone_name VARCHAR(50) NOT NULL,
                latitude DECIMAL(8,4), longitude DECIMAL(8,4),
                baseline_temp_avg FLOAT COMMENT '30-day avg temperature baseline (°C)',
                current_temp FLOAT COMMENT 'Последний день temperature (°C)',
                temp_deviation FLOAT COMMENT 'current - baseline (°C)',
                baseline_precip_avg FLOAT COMMENT '30-day avg precipitation baseline (mm)',
                current_precip FLOAT COMMENT 'Последний день precipitation (mm)',
                precip_deviation FLOAT COMMENT 'current - baseline (mm)',
                severity VARCHAR(20) COMMENT 'normal/moderate/extreme',
                snapshot_hour DATETIME NOT NULL,
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_zone_hour (zone_name, snapshot_hour),
                INDEX idx_severity (severity),
                INDEX idx_deviation (temp_deviation DESC)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Open-Meteo ERA5 climate anomalies for 15 conflict-prone zones';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()
        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        # ERA5 data has ~5 day lag
        end_date = (now - timedelta(days=5)).strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=35)).strftime("%Y-%m-%d")  # 30 days for baseline

        rows = []
        for zone in ZONES:
            print(f"   🌡️ {zone['name']:20s}", end=" ", flush=True)
            try:
                resp = self.session.get(
                    "https://archive-api.open-meteo.com/v1/era5",
                    params={
                        "latitude": zone["lat"], "longitude": zone["lon"],
                        "start_date": start_date, "end_date": end_date,
                        "daily": "temperature_2m_mean,precipitation_sum",
                        "timezone": "UTC",
                    },
                    timeout=15
                )
                if resp.status_code != 200:
                    print(f"HTTP {resp.status_code}"); continue

                data = resp.json()
                daily = data.get("daily", {})
                temps = daily.get("temperature_2m_mean", [])
                precips = daily.get("precipitation_sum", [])

                if len(temps) < 7:
                    print("insufficient data"); continue

                # Baseline: first 25 days, current: last 5 days average
                baseline_temps = [t for t in temps[:-5] if t is not None]
                current_temps = [t for t in temps[-5:] if t is not None]
                baseline_precips = [p for p in precips[:-5] if p is not None]
                current_precips = [p for p in precips[-5:] if p is not None]

                b_temp = sum(baseline_temps) / len(baseline_temps) if baseline_temps else None
                c_temp = sum(current_temps) / len(current_temps) if current_temps else None
                b_prec = sum(baseline_precips) / len(baseline_precips) if baseline_precips else None
                c_prec = sum(current_precips) / len(current_precips) if current_precips else None

                temp_dev = round(c_temp - b_temp, 2) if (c_temp is not None and b_temp is not None) else None
                prec_dev = round(c_prec - b_prec, 2) if (c_prec is not None and b_prec is not None) else None

                # WorldMonitor severity: >5°C or >80mm = extreme, >3°C or >40mm = moderate
                severity = "normal"
                if temp_dev is not None:
                    if abs(temp_dev) > 5: severity = "extreme"
                    elif abs(temp_dev) > 3: severity = "moderate"
                if prec_dev is not None and prec_dev > 80: severity = "extreme"
                elif prec_dev is not None and prec_dev > 40 and severity != "extreme": severity = "moderate"

                rows.append((
                    zone["name"], zone["lat"], zone["lon"],
                    round(b_temp, 2) if b_temp else None,
                    round(c_temp, 2) if c_temp else None,
                    temp_dev,
                    round(b_prec, 2) if b_prec else None,
                    round(c_prec, 2) if c_prec else None,
                    prec_dev, severity,
                    snapshot_hour, snapshot_at,
                ))

                icon = "🔴" if severity == "extreme" else ("🟡" if severity == "moderate" else "🟢")
                dev_str = f"{temp_dev:+.1f}°C" if temp_dev else "N/A"
                print(f"{icon} {dev_str} ({severity})")

            except Exception as e:
                print(f"ERROR: {e}")
            time.sleep(random.uniform(0.5, 1.5))

        if not rows:
            print("   ⚠️ Нет данных"); return

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (zone_name, latitude, longitude, baseline_temp_avg, current_temp, temp_deviation,
             baseline_precip_avg, current_precip, precip_deviation, severity, snapshot_hour, snapshot_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        c.executemany(sql, rows)
        conn.commit(); n = c.rowcount; c.close(); conn.close()
        print(f"   ✅ Записано {n} зон")


def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица."); sys.exit(1)
    print(f"🚀 Open-Meteo ERA5 Climate Anomaly Collector (15 zones)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Таблица: {args.table_name}"); print("=" * 60)
    ClimateCollector(args.table_name).process()
    print("=" * 60); print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n🛑 Прервано"); sys.exit(1)
    except Exception as e: print(f"\n❌ {e!r}"); send_error_trace(e); sys.exit(1)
