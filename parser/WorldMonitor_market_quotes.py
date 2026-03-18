"""
Таблица: vlad_wm_market_quotes

Запуск:
  python WorldMonitor_market_quotes.py vlad_wm_market_quotes [host] [port] [user] [password] [database]
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
NODE_NAME = os.getenv("NODE_NAME", "wm_market_quotes")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")
WM_API_BASE = os.getenv("WM_API_BASE", "https://api.worldmonitor.app")

def send_error_trace(exc, script_name="WorldMonitor_market_quotes.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="WorldMonitor Market Quotes (Finnhub) → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны все параметры подключения к БД"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}

DATASETS = {"vlad_wm_market_quotes": {"description": "WorldMonitor: Finnhub market quotes (snapshot)"}}

# Тикеры по батчам (Finnhub rate-limit ~10 за раз)
SYMBOL_BATCHES = [
    ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "JPM", "V", "WMT"],
    ["BRK.B", "UNH", "XOM", "JNJ", "PG", "MA", "HD", "COST", "ABBV", "BAC"],
]

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Origin": "https://worldmonitor.app",
        "Referer": "https://worldmonitor.app/",
    })
    return s

def wm_get(session, path, params=None, timeout=30):
    url = f"{WM_API_BASE}{path}"
    for attempt in range(3):
        try:
            if attempt > 0: time.sleep(1.5 ** attempt + random.uniform(0.2, 0.8))
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"   ⏳ Rate-limit, ждём {wait}s...")
                time.sleep(wait + random.uniform(0, 2)); continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            print(f"   ⚠️ HTTP {resp.status_code} для {path}")
        except Exception as e:
            print(f"   ⚠️ {e}")
    return None


class QuotesCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                current_price DECIMAL(20,6),
                change_amount DECIMAL(20,6),
                change_pct DECIMAL(10,4),
                high_price DECIMAL(20,6),
                low_price DECIMAL(20,6),
                open_price DECIMAL(20,6),
                prev_close DECIMAL(20,6),
                timestamp_unix BIGINT COMMENT 'Finnhub timestamp',
                raw_json TEXT COMMENT 'Полный JSON квоты',
                snapshot_hour DATETIME NOT NULL COMMENT 'Для дедупликации: один snapshot в час',
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_symbol_hour (symbol, snapshot_hour),
                INDEX idx_symbol (symbol),
                INDEX idx_snapshot (snapshot_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='WorldMonitor: Finnhub market quotes';
        """)
        conn.commit(); c.close(); conn.close()

    def process(self):
        self.ensure_table()
        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        all_quotes = []

        for batch in SYMBOL_BATCHES:
            symbols_str = ",".join(batch)
            print(f"   📊 Запрос: {symbols_str[:60]}...")
            data = wm_get(self.session, "/api/market/v1/list-market-quotes", params={"symbols": symbols_str})

            if not data:
                print(f"   ⚠️ Нет ответа для батча")
                continue

            quotes = data.get("quotes", [])
            skipped = data.get("finnhubSkipped", [])
            rate_limited = data.get("rateLimited", False)

            if rate_limited:
                print(f"   ⏳ Finnhub rate-limited, пауза 15s...")
                time.sleep(15)

            if skipped:
                print(f"   ⚠️ Пропущены (Finnhub): {skipped}")

            all_quotes.extend(quotes)
            time.sleep(random.uniform(1.0, 2.0))  # Между батчами

        if not all_quotes:
            print("   ⚠️ Нет котировок")
            return

        print(f"   📈 Получено {len(all_quotes)} котировок")

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (symbol, current_price, change_amount, change_pct, high_price, low_price,
             open_price, prev_close, timestamp_unix, raw_json, snapshot_hour, snapshot_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        rows = []
        for q in all_quotes:
            # Finnhub quote format: c=current, d=change, dp=change%, h=high, l=low, o=open, pc=prev_close, t=timestamp
            symbol = q.get("symbol", q.get("ticker", ""))
            rows.append((
                symbol,
                _sf(q.get("c", q.get("current", q.get("price")))),
                _sf(q.get("d", q.get("change"))),
                _sf(q.get("dp", q.get("changePct", q.get("change_percent")))),
                _sf(q.get("h", q.get("high"))),
                _sf(q.get("l", q.get("low"))),
                _sf(q.get("o", q.get("open"))),
                _sf(q.get("pc", q.get("previousClose", q.get("prev_close")))),
                _si(q.get("t", q.get("timestamp"))),
                json.dumps(q, ensure_ascii=False, default=str)[:2000],
                snapshot_hour,
                snapshot_at,
            ))

        c.executemany(sql, rows)
        conn.commit()
        inserted = c.rowcount
        c.close(); conn.close()

        print(f"   ✅ Записано {inserted} котировок")
        # Показать топ-5
        for q in all_quotes[:5]:
            sym = q.get("symbol", q.get("ticker", "?"))
            price = q.get("c", q.get("price", "?"))
            dp = q.get("dp", q.get("changePct", "?"))
            print(f"      {sym:8s}  ${price}  ({dp}%)")


def _sf(v):
    if v is None or v == "": return None
    try: return float(str(v).replace(",", ""))
    except: return None

def _si(v):
    if v is None or v == "": return None
    try: return int(float(str(v).replace(",", "")))
    except: return None


def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица '{args.table_name}'. Допустимые:")
        for n in DATASETS: print(f"  - {n}")
        sys.exit(1)

    print(f"🚀 WorldMonitor Market Quotes Collector (Finnhub)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Таблица: {args.table_name}")
    print("=" * 60)

    QuotesCollector(args.table_name).process()

    print("=" * 60)
    print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА")


if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n🛑 Прервано пользователем"); sys.exit(1)
    except Exception as e: print(f"\n❌ Критическая ошибка: {e!r}"); send_error_trace(e); sys.exit(1)
