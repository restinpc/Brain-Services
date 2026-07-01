"""
Описание: Finnhub — snapshot рыночных котировок по списку тикеров.
Таблица: vlad_wm_market_quotes

Важно:
  В .env должен быть ключ Finnhub:
    FINNHUB_API_KEY=your_key_here

Запуск:
  python WorldMonitor_market_quotes.py vlad_wm_market_quotes [host] [port] [user] [password] [database]

Примеры:
  python WorldMonitor_market_quotes.py vlad_wm_market_quotes
  python WorldMonitor_market_quotes.py vlad_wm_market_quotes 127.0.0.1 3306 root password brain
"""

import os
import sys
import argparse
import json
import time
import random
import traceback
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "wm_market_quotes")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# Старый endpoint api.worldmonitor.app начал отдавать HTTP 401 без авторизации.
# Поэтому берём котировки напрямую из Finnhub.
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
FINNHUB_API_BASE = os.getenv("FINNHUB_API_BASE", "https://finnhub.io/api/v1").rstrip("/")

# Пауза между запросами, чтобы не упираться в лимиты free-tier.
FINNHUB_REQUEST_DELAY = float(os.getenv("FINNHUB_REQUEST_DELAY", "1.1"))


def send_error_trace(exc, script_name="WorldMonitor_market_quotes.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
            timeout=10,
        )
    except Exception:
        pass


parser = argparse.ArgumentParser(description="Finnhub Market Quotes → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("Ошибка: не указаны все параметры подключения к БД")
    print("Использование:")
    print("  python WorldMonitor_market_quotes.py vlad_wm_market_quotes")
    print("  python WorldMonitor_market_quotes.py vlad_wm_market_quotes 127.0.0.1 3306 root password brain")
    sys.exit(1)

if not FINNHUB_API_KEY:
    print("Ошибка: не найден FINNHUB_API_KEY в .env")
    print("Добавь строку в .env:")
    print("  FINNHUB_API_KEY=your_key_here")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

DATASETS = {
    "vlad_wm_market_quotes": {
        "description": "Finnhub market quotes snapshot",
    }
}

# Тикеры. Finnhub /quote принимает один symbol за запрос.
SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "JPM", "V", "WMT",
    "BRK.B", "UNH", "XOM", "JNJ", "PG", "MA", "HD", "COST", "ABBV", "BAC",
]


def build_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Brain-Services/1.0",
        "Accept": "application/json",
        "X-Finnhub-Token": FINNHUB_API_KEY,
    })
    return session


def finnhub_get_quote(session, symbol, timeout=30):
    url = f"{FINNHUB_API_BASE}/quote"
    params = {"symbol": symbol}

    for attempt in range(1, 4):
        try:
            if attempt > 1:
                time.sleep((1.7 ** attempt) + random.uniform(0.2, 0.8))

            response = session.get(url, params=params, timeout=timeout)

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 15))
                print(f"   Rate-limit Finnhub для {symbol}, ждём {wait}s...")
                time.sleep(wait + random.uniform(0, 2))
                continue

            if response.status_code in (401, 403):
                print(f"   HTTP {response.status_code} для {symbol}: проверь FINNHUB_API_KEY")
                return None

            response.raise_for_status()
            data = response.json()

            # Finnhub иногда возвращает {'error': '...'} вместо обычной котировки.
            if isinstance(data, dict) and data.get("error"):
                print(f"   Ошибка Finnhub для {symbol}: {data.get('error')}")
                return None

            if not isinstance(data, dict):
                print(f"   Неожиданный ответ Finnhub для {symbol}: {type(data).__name__}")
                return None

            # Для невалидного/недоступного тикера Finnhub может вернуть нули.
            if all(_sf(data.get(k)) in (None, 0.0) for k in ["c", "h", "l", "o", "pc"]):
                print(f"   Нет данных для {symbol}")
                return None

            data["symbol"] = symbol
            return data

        except requests.exceptions.Timeout:
            print(f"   Timeout для {symbol}, попытка {attempt}/3")
        except requests.exceptions.RequestException as exc:
            print(f"   Ошибка запроса для {symbol}: {exc}")
        except ValueError as exc:
            print(f"   Ошибка JSON для {symbol}: {exc}")

    return None


class QuotesCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection()
        c = conn.cursor()
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
            COMMENT='Finnhub market quotes';
        """)
        conn.commit()
        c.close()
        conn.close()

    def process(self):
        self.ensure_table()

        now = datetime.now(timezone.utc)
        snapshot_at = now.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0, tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        all_quotes = []

        print(f"   Запрос котировок: {len(SYMBOLS)} тикеров")
        for idx, symbol in enumerate(SYMBOLS, start=1):
            print(f"   [{idx}/{len(SYMBOLS)}] {symbol}...", end=" ")
            quote = finnhub_get_quote(self.session, symbol)
            if quote:
                all_quotes.append(quote)
                price = quote.get("c")
                dp = quote.get("dp")
                print(f"OK ${price} ({dp}%)")
            else:
                print("пропущен")

            if idx < len(SYMBOLS):
                time.sleep(FINNHUB_REQUEST_DELAY)

        if not all_quotes:
            print("   Нет котировок")
            return

        print(f"   Получено {len(all_quotes)} котировок")

        conn = self.get_db_connection()
        c = conn.cursor()
        sql = f"""
            INSERT INTO `{self.table_name}`
            (symbol, current_price, change_amount, change_pct, high_price, low_price,
             open_price, prev_close, timestamp_unix, raw_json, snapshot_hour, snapshot_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                current_price = VALUES(current_price),
                change_amount = VALUES(change_amount),
                change_pct = VALUES(change_pct),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                open_price = VALUES(open_price),
                prev_close = VALUES(prev_close),
                timestamp_unix = VALUES(timestamp_unix),
                raw_json = VALUES(raw_json),
                snapshot_at = VALUES(snapshot_at),
                loaded_at = CURRENT_TIMESTAMP
        """

        rows = []
        for q in all_quotes:
            symbol = q.get("symbol", "")
            rows.append((
                symbol,
                _sf(q.get("c")),
                _sf(q.get("d")),
                _sf(q.get("dp")),
                _sf(q.get("h")),
                _sf(q.get("l")),
                _sf(q.get("o")),
                _sf(q.get("pc")),
                _si(q.get("t")),
                json.dumps(q, ensure_ascii=False, default=str)[:2000],
                snapshot_hour,
                snapshot_at,
            ))

        c.executemany(sql, rows)
        conn.commit()
        affected = c.rowcount
        c.close()
        conn.close()

        print(f"   Обновлено/записано строк: {affected}")
        print("   Пример:")
        for q in all_quotes[:5]:
            sym = q.get("symbol", "?")
            price = q.get("c", "?")
            dp = q.get("dp", "?")
            print(f"     {sym:8s} ${price} ({dp}%)")


def _sf(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _si(v):
    if v is None or v == "":
        return None
    try:
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def main():
    if args.table_name not in DATASETS:
        print(f"Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATASETS:
            print(f"  - {name}")
        sys.exit(1)

    print("Finnhub Market Quotes Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"Таблица: {args.table_name}")
    print("=" * 60)

    QuotesCollector(args.table_name).process()

    print("=" * 60)
    print("ЗАГРУЗКА ЗАВЕРШЕНА")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nКритическая ошибка: {e!r}")
        traceback.print_exc()
        send_error_trace(e)
        sys.exit(1)
