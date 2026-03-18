"""
Таблица: vlad_coingecko_stablecoin_history

Запуск:
  python CoinGecko_stablecoin_history.py vlad_coingecko_stablecoin_history [host] [port] [user] [password] [database]
"""

import os, sys, argparse, time, random, traceback
from datetime import datetime, date, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "coingecko_stablecoin")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")
CG_API_KEY = os.getenv("COINGECKO_API_KEY", "")
CG_BASE = "https://api.coingecko.com/api/v3"

STABLECOINS = {
    "tether":       "USDT",
    "usd-coin":     "USDC",
    "dai":          "DAI",
    "first-digital-usd": "FDUSD",
    "ethena-usde":  "USDe",
}
BACKFILL_DAYS = 365

def send_error_trace(exc, script_name="CoinGecko_stablecoin_history.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="CoinGecko Stablecoin History → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}
DATASETS = {"vlad_coingecko_stablecoin_history": {"description": "CoinGecko stablecoin peg history"}}


class CoinGeckoCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        headers = {"Accept": "application/json"}
        if CG_API_KEY:
            headers["x-cg-demo-api-key"] = CG_API_KEY
        self.session.headers.update(headers)

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                coin_id VARCHAR(50) NOT NULL COMMENT 'CoinGecko ID',
                symbol VARCHAR(10) NOT NULL,
                record_date DATE NOT NULL,
                price DECIMAL(20,8) NOT NULL,
                peg_deviation DECIMAL(10,8) COMMENT 'price - 1.0',
                market_cap BIGINT,
                volume_24h BIGINT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_coin_date (coin_id, record_date),
                INDEX idx_symbol (symbol),
                INDEX idx_date (record_date),
                INDEX idx_deviation (peg_deviation)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='CoinGecko: stablecoin peg history (daily)';
        """)
        conn.commit(); c.close(); conn.close()

    def get_last_date(self, coin_id):
        try:
            conn = self.get_db_connection(); c = conn.cursor()
            c.execute("SHOW TABLES LIKE %s", (self.table_name,))
            if not c.fetchone(): c.close(); conn.close(); return None
            c.execute(f"SELECT MAX(record_date) FROM `{self.table_name}` WHERE coin_id = %s", (coin_id,))
            row = c.fetchone(); c.close(); conn.close()
            return row[0] if row and row[0] else None
        except: return None

    def fetch_market_chart(self, coin_id, from_ts, to_ts):
        """
        GET /coins/{id}/market_chart/range?vs_currency=usd&from=X&to=Y
        Возвращает daily data для периодов > 90 дней.
        """
        url = f"{CG_BASE}/coins/{coin_id}/market_chart/range"
        params = {"vs_currency": "usd", "from": int(from_ts), "to": int(to_ts)}

        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    print(f"   ⏳ Rate-limit, ждём {wait}s...")
                    time.sleep(wait + random.uniform(5, 15))
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                print(f"   ⚠️ Попытка {attempt+1}: {e}")
                time.sleep(5 * (attempt + 1))
        return None

    def insert_rows(self, rows):
        if not rows: return 0
        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}` 
            (coin_id, symbol, record_date, price, peg_deviation, market_cap, volume_24h)
            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        c.executemany(sql, rows)
        n = c.rowcount; conn.commit(); c.close(); conn.close()
        return n

    def process(self):
        self.ensure_table()
        now = datetime.utcnow()
        total_inserted = 0

        for coin_id, symbol in STABLECOINS.items():
            last_date = self.get_last_date(coin_id)

            if last_date is None:
                # BACKFILL
                start = now - timedelta(days=BACKFILL_DAYS)
                mode = "BACKFILL"
            else:
                # INCREMENTAL (перекрытие 2 дня)
                start = datetime.combine(last_date - timedelta(days=2), datetime.min.time())
                mode = "INCREMENTAL"

            from_ts = start.timestamp()
            to_ts = now.timestamp()

            print(f"\n   {'📦' if mode == 'BACKFILL' else '🔄'} {symbol} ({coin_id}): {mode} {start.date()} → {now.date()}")

            data = self.fetch_market_chart(coin_id, from_ts, to_ts)
            if not data or "prices" not in data:
                print(f"      ⚠️ Нет данных для {symbol}")
                time.sleep(random.uniform(5, 10))
                continue

            prices = data["prices"]  # [[timestamp_ms, price], ...]
            market_caps = data.get("market_caps", [])
            volumes = data.get("total_volumes", [])

            # Индексируем market_caps и volumes по дате для join
            mc_map = {}
            for item in market_caps:
                d = datetime.utcfromtimestamp(item[0] / 1000).date()
                mc_map[d] = int(item[1]) if item[1] else None

            vol_map = {}
            for item in volumes:
                d = datetime.utcfromtimestamp(item[0] / 1000).date()
                vol_map[d] = int(item[1]) if item[1] else None

            rows = []
            for ts_ms, price in prices:
                d = datetime.utcfromtimestamp(ts_ms / 1000).date()
                deviation = round(price - 1.0, 8) if price else None
                rows.append((
                    coin_id, symbol, d, price, deviation,
                    mc_map.get(d), vol_map.get(d),
                ))

            n = self.insert_rows(rows)
            total_inserted += n
            print(f"      ✅ {n} новых из {len(prices)} точек")

            # CoinGecko rate limit: 10-30 req/min
            time.sleep(random.uniform(3, 6))

        print(f"\n📊 Всего вставлено: {total_inserted}")


def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица. Допустимые:")
        for n in DATASETS: print(f"  - {n}")
        sys.exit(1)
    print(f"🚀 CoinGecko Stablecoin History (backfill + incremental)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 {args.table_name}"); print("=" * 60)
    CoinGeckoCollector(args.table_name).process()
    print("=" * 60); print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n🛑 Прервано"); sys.exit(1)
    except Exception as e: print(f"\n❌ Критическая ошибка: {e!r}"); send_error_trace(e); sys.exit(1)
