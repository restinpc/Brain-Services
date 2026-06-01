"""
Описание: Парсинг биржевых стаканов (Order Book) с Binance
Запуск:   python binance_orderbook.py <table_name> [host] [port] [user] [password] [database] [tickers_json]
Пример:   python binance_orderbook.py sasha_binance_orderbook ["BTCUSDT","ETHUSDT","SOLUSDT"]
"""

# ── 1. ИМПОРТЫ ─────────────────────────────────────────────────────────────────
import os
import sys
import json
import argparse
import traceback
import requests
import mysql.connector
import time
from dotenv import load_dotenv
from datetime import datetime

# ── 2. КОНФИГ ─────────────────────────────────────────────────────────────────
load_dotenv()

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "binance_orderbook")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

# Отдельная HTTP-сессия для Binance без прокси из окружения
# (иначе requests может пытаться использовать SOCKS и падать без PySocks).
BINANCE_HTTP = requests.Session()
BINANCE_HTTP.trust_env = False

# ── 3. ТРАССИРОВКА ОШИБОК ─────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "binance_orderbook.py"):
    import threading
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"

    def _send():
        try:
            requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
        except:
            pass

    threading.Thread(target=_send, daemon=True).start()

# ── 4. АРГУМЕНТЫ ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Binance Order Book Parser → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument(
    "extra",
    nargs="*",
    help='[host] [port] [user] [password] [database] ["SYMBOL1","SYMBOL2"]'
)
args = parser.parse_args()

raw_extra = args.extra
tickers_arg = None

if raw_extra and raw_extra[-1].strip().startswith("["):
    tickers_arg = raw_extra[-1]
    db_tokens = raw_extra[:-1]
else:
    db_tokens = raw_extra

if len(db_tokens) not in (0, 5):
    print("[ERROR] Параметры БД нужно передавать либо все 5, либо не передавать")
    sys.exit(1)

if len(db_tokens) == 5:
    args.host, args.port, args.user, args.password, args.database = db_tokens
else:
    args.host = os.getenv("DB_HOST")
    args.port = os.getenv("DB_PORT", "3306")
    args.user = os.getenv("DB_USER")
    args.password = os.getenv("DB_PASSWORD")
    args.database = os.getenv("DB_NAME")

if not all([args.host, args.user, args.password, args.database]):
    print("[ERROR] Не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host":     args.host,
    "port":     int(args.port),
    "user":     args.user,
    "password": args.password,
    "database": args.database,
}

# ── 5. ТАБЛИЦЫ ───────────────────────────────────────────────────────────────
DATASETS = {
    "sasha_binance_orderbook": {
        "description": "Биржевые стаканы (Order Book) с Binance",
        "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"],
        "depth": 50,                    # количество уровней (максимум 1000)
        "interval_seconds": 60          # рекомендуемый интервал между запусками
    },
}

# ── 6. СОЗДАНИЕ ТАБЛИЦЫ ───────────────────────────────────────────────────────
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            date_iso        DATE         NOT NULL,
            symbol          VARCHAR(20)  NOT NULL,
            timestamp       BIGINT       NOT NULL,
            side            ENUM('bid', 'ask') NOT NULL,
            price           DOUBLE       NOT NULL,
            quantity        DOUBLE       NOT NULL,
            loaded_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_order (date_iso, symbol, timestamp, side, price),
            INDEX idx_symbol_ts (symbol, timestamp),
            INDEX idx_date_symbol (date_iso, symbol),
            INDEX idx_price (price)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

# ── 7. ЗАГРУЗКА ДОПОЛНИТЕЛЬНЫХ ТИКЕРОВ ─────────────────────────────────────
def add_symbols_from_argument(table_name: str, tickers_arg: str):
    if not tickers_arg:
        return

    try:
        symbols = json.loads(tickers_arg)
        if not isinstance(symbols, list):
            symbols = [tickers_arg]
    except:
        # Попытка разобрать как простую строку
        symbols = [s.strip() for s in tickers_arg.strip("[]").split(",")]

    config = DATASETS[table_name]
    existing = {s.upper() for s in config.get("symbols", [])}
    new_symbols = [s.strip().upper() for s in symbols if s.strip() and s.strip().upper() not in existing]

    if new_symbols:
        config["symbols"].extend(new_symbols)
        print(f"[OK] Добавлено тикеров: {len(new_symbols)} -> {new_symbols}")

# ── 8. ПОЛУЧЕНИЕ СТАКАНА ─────────────────────────────────────────────────────
def fetch_orderbook(symbol: str, depth: int = 50):
    """Получает snapshot стакана с Binance"""
    try:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={depth}"
        response = BINANCE_HTTP.get(url, timeout=15)

        if response.status_code != 200:
            print(f"[ERROR] {symbol} HTTP {response.status_code}: {response.text[:200]}")
            return None

        data = response.json()
        now = datetime.now()
        
        return {
            "date_iso": now.date().isoformat(),
            "symbol": symbol,
            "timestamp": int(now.timestamp()),
            "bids": data.get("bids", []),
            "asks": data.get("asks", [])
        }
    except Exception as e:
        print(f"[ERROR] Ошибка при запросе стакана {symbol}: {e}")
        return None

# ── 9. ЗАПИСЬ В БД ───────────────────────────────────────────────────────────
def save_orderbook(table_name: str, records: list):
    if not records:
        print("[WARN] Нет данных для записи")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    rows = []
    for rec in records:
        for side, levels in [("bid", rec["bids"]), ("ask", rec["asks"])]:
            for price_str, qty_str in levels:
                try:
                    rows.append((
                        rec["date_iso"],
                        rec["symbol"],
                        rec["timestamp"],
                        side,
                        float(price_str),
                        float(qty_str)
                    ))
                except (ValueError, TypeError):
                    continue

    if rows:
        sql = f"""
            INSERT IGNORE INTO `{table_name}` 
            (date_iso, symbol, timestamp, side, price, quantity)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        c.executemany(sql, rows)
        conn.commit()
        print(f"[OK] Записано {c.rowcount} уровней стакана из {len(rows)}")
    else:
        print("[WARN] Нет валидных уровней для записи")

    c.close()
    conn.close()

# ── 10. ОСНОВНАЯ ЛОГИКА ───────────────────────────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]

    ensure_table(table_name)
    add_symbols_from_argument(table_name, tickers_arg)

    print("Binance Order Book Parser")
    print(f"   Таблица: {table_name}")
    print(f"   Тикеры: {config['symbols']}")
    print(f"   Глубина: {config['depth']} уровней")

    records = []
    for symbol in config["symbols"]:
        ob = fetch_orderbook(symbol, config["depth"])
        if ob:
            records.append(ob)
        time.sleep(0.25)  # небольшая задержка между запросами

    if not records:
        print("[ERROR] Не удалось получить ни одного стакана")
        return

    save_orderbook(table_name, records)

# ── 11. ТОЧКА ВХОДА ───────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"[ERROR] Неизвестная таблица '{args.table_name}'. Доступно:")
        for name in DATASETS:
            print(f"  - {name}")
        sys.exit(1)

    process(args.table_name)
    print("DONE")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n[INFO] Прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)