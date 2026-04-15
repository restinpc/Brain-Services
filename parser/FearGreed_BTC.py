"""
7 сигналов для composite BUY/CASH verdict:
  1. Fear & Greed Index → api.alternative.me/fng
  2. BTC Hashrate → mempool.space/api/v1/mining/hashrate
  3. BTC SMA50/SMA200 → вычисляется из Yahoo Finance
  4. Mayer Multiple → BTC price / SMA200
  5. VIX → Yahoo Finance (уже есть в TradingView_1.py)
  6. QQQ/XLP ratio → Yahoo Finance (уже есть)
  7. JPY carry → Yahoo Finance (уже есть)

Таблицы:
  vlad_fear_greed_index    — полная история с 2018 (backfill) + daily incremental
  vlad_btc_hashrate        — hashrate + difficulty snapshot

Запуск:
  python FearGreed_BTC.py vlad_fear_greed_index [host] [port] [user] [password] [database]
  python FearGreed_BTC.py vlad_btc_hashrate [host] [port] [user] [password] [database]
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
NODE_NAME = os.getenv("NODE_NAME", "fear_greed_btc")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="FearGreed_BTC.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="Fear & Greed + BTC Technical → MySQL")
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
DATASETS = {
    "vlad_fear_greed_index": {"description": "Crypto Fear & Greed Index (full history + daily)"},
    "vlad_btc_hashrate": {"description": "BTC hashrate + difficulty (mempool.space)"},
}

def _sf(v):
    if v is None or v == "": return None
    try: return float(str(v).replace(",", ""))
    except: return None

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"})
    return s


class FearGreedCollector:
    """api.alternative.me/fng — полная история с 2018-02-01"""
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                record_date DATE NOT NULL,
                value INT NOT NULL COMMENT 'Fear & Greed score (0=Extreme Fear, 100=Extreme Greed)',
                classification VARCHAR(30) COMMENT 'Extreme Fear / Fear / Neutral / Greed / Extreme Greed',
                timestamp_unix BIGINT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_date (record_date),
                INDEX idx_value (value)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Crypto Fear & Greed Index (alternative.me) — full history since 2018';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()

        # Определяем: backfill или incremental
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"SELECT MAX(record_date) FROM `{self.table_name}`")
        last_date = c.fetchone()[0]
        c.close(); conn.close()

        if last_date is None:
            # BACKFILL: вся история (limit=0 = все данные)
            print("    BACKFILL: загрузка всей истории с 2018...")
            url = "https://api.alternative.me/fng/?limit=0&format=json"
        else:
            # INCREMENTAL: последние 10 дней (перекрытие для safety)
            days_diff = (datetime.utcnow().date() - last_date).days + 5
            print(f"    INCREMENTAL: последние {days_diff} дней (от {last_date})")
            url = f"https://api.alternative.me/fng/?limit={days_diff}&format=json"

        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        entries = data.get("data", [])

        if not entries:
            print("    Нет данных"); return

        print(f"    Получено {len(entries)} записей")

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (record_date, value, classification, timestamp_unix)
            VALUES (%s, %s, %s, %s)"""

        rows = []
        for e in entries:
            ts = int(e.get("timestamp", 0))
            dt = datetime.utcfromtimestamp(ts).date() if ts > 0 else None
            if dt is None: continue
            rows.append((dt, int(e.get("value", 0)), e.get("value_classification", ""), ts))

        c.executemany(sql, rows)
        conn.commit(); inserted = c.rowcount; c.close(); conn.close()

        print(f"    Записано {inserted} новых дней")
        # Последние 3 дня
        for e in entries[:3]:
            ts = int(e.get("timestamp", 0))
            dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            print(f"      {dt}: {e['value']} ({e['value_classification']})")


class BTCHashrateCollector:
    """mempool.space/api — BTC mining hashrate + difficulty"""

    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        # ... (твой CREATE TABLE без изменений)
        conn = self.get_db_connection();
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                current_hashrate DECIMAL(30,2),
                current_hashrate_eh DECIMAL(20,4),
                current_difficulty DECIMAL(30,2),
                hashrate_data_json LONGTEXT,
                difficulty_data_json LONGTEXT,
                period VARCHAR(10) DEFAULT '3m',
                snapshot_hour DATETIME NOT NULL,
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_hour (snapshot_hour),
                INDEX idx_snapshot (snapshot_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='BTC hashrate + difficulty (mempool.space)';
        """)
        conn.commit();
        c.close();
        conn.close()

    def process(self):
        self.ensure_table()
        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0)

        try:
            # 1. Hashrate 3m
            resp = self.session.get("https://mempool.space/api/v1/mining/hashrate/3m", timeout=20)
            resp.raise_for_status()
            data = resp.json()

            current_hr = data.get("currentHashrate", 0)
            current_diff = data.get("currentDifficulty", 0)
            hr_eh = round(current_hr / 1e18, 4) if current_hr else 0.0

            # 2. Difficulty adjustments
            diff_resp = self.session.get("https://mempool.space/api/v1/mining/difficulty-adjustments/6", timeout=15)
            diff_resp.raise_for_status()
            diff_data = diff_resp.json()

            # Сохраняем только последние 30 точек hashrate и 10 корректировок
            hr_json = json.dumps(data.get("hashrates", [])[-30:], default=str)[:50000]
            diff_json = json.dumps(diff_data[:10] if isinstance(diff_data, list) else diff_data, default=str)[:10000]

            conn = self.get_db_connection()
            c = conn.cursor()
            sql = f"""INSERT IGNORE INTO `{self.table_name}`
                (current_hashrate, current_hashrate_eh, current_difficulty,
                 hashrate_data_json, difficulty_data_json, period, snapshot_hour, snapshot_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

            c.execute(sql, (current_hr, hr_eh, current_diff, hr_json, diff_json, "3m", snapshot_hour, snapshot_at))
            conn.commit()
            inserted = c.rowcount
            c.close();
            conn.close()

            print(f"    BTC Hashrate: {hr_eh:.2f} EH/s | Difficulty: {current_diff:,.0f} | Записано: {inserted}")

        except requests.exceptions.RequestException as e:
            print(f"    Ошибка API mempool.space: {e}")
            raise
        except Exception as e:
            print(f"    Неизвестная ошибка в hashrate: {e}")
            raise


def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица. Допустимые:"); [print(f"  - {n}") for n in DATASETS]; sys.exit(1)
    ds = DATASETS[args.table_name]
    print(f" {ds['description']}")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}"); print("=" * 60)
    if args.table_name == "vlad_fear_greed_index": FearGreedCollector(args.table_name).process()
    elif args.table_name == "vlad_btc_hashrate": BTCHashrateCollector(args.table_name).process()
    print("=" * 60); print(" ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n Прервано"); sys.exit(1)
    except Exception as e: print(f"\n {e!r}"); send_error_trace(e); sys.exit(1)
