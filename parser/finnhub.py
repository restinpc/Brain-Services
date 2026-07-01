"""
Описание: Данные о курсе акций и рыночной капитализации
           для NVDA, AAPL, MSFT, AMZN, GOOGL из Finnhub API
Запуск:   python finnhub.py <table_name> [host] [port] [user] [password] [database] [TSLA,META, ...]
Ограничение: 60 запросов/минуту
Строка в .env FINNHUB_API_KEY=
"""

#  1. ИМПОРТЫ 
import os
import sys
import json
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime

#  2. КОНФИГ 
load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "finnhub")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")
TICKERS_TABLE_NAME = "sasha_add_tikers_for_finnhub"

#  3. ТРАССИРОВКА ОШИБОК 
def send_error_trace(exc: Exception, script_name: str = "finnhub.py"):
    """
    Отправляет трассировку в фоновом потоке — не блокирует основной процесс.
    """
    import threading
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"

    def _send():
        try:
            requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
        except:
            pass

    threading.Thread(target=_send, daemon=True).start()

#  4. АРГУМЕНТЫ 
parser = argparse.ArgumentParser(description="Finnhub Stock Parser → MySQL")
parser.add_argument("table_name",  help="Имя целевой таблицы в БД")
parser.add_argument(
    "extra",
    nargs="*",
    help='После table_name можно передать host port user password database, а последним JSON-массив тикеров'
)
args = parser.parse_args()

raw_extra = args.extra
args.tickers = None

db_tokens = raw_extra
if raw_extra and raw_extra[-1].strip().startswith("["):
    args.tickers = raw_extra[-1]
    db_tokens = raw_extra[:-1]

if any(token.strip().startswith("[") for token in db_tokens):
    print(" Ошибка: JSON-массив тикеров должен быть последним аргументом")
    print("   Ожидается: [host] [port] [user] [password] [database] [tickers_json]")
    sys.exit(1)

if len(db_tokens) not in (0, 5):
    print(" Ошибка: параметры БД нужно передавать либо все 5, либо не передавать вовсе")
    print("   Формат 1: <table_name> [host] [port] [user] [password] [database] [tickers_json]")
    print("   Формат 2: <table_name> [tickers_json]  (если DB_* уже заданы в .env)")
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
    print(" Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

if not FINNHUB_API_KEY:
    print(" Ошибка: FINNHUB_API_KEY не указан в .env")
    sys.exit(1)

DB_CONFIG = {
    "host":     args.host,
    "port":     int(args.port),
    "user":     args.user,
    "password": args.password,
    "database": args.database,
}

#  5. ТАБЛИЦЫ (table_name → конфиг запроса) 
DATASETS = {
    "sasha_finnhub_stock_prices": {
        "description": "Дневной курс акций (поле value = текущая цена)",
        "metric": "price",
        "symbols": ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL"]
    },
    "sasha_finnhub_stock_marketcaps": {
        "description": "Рыночная капитализация компаний (поле value = marketCapitalization)",
        "metric": "marketcap",
        "symbols": ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL"]
    },
}

#  6. УПРАВЛЕНИЕ СПИСКОМ ТИКЕРОВ 
def ensure_tickers_table():
    """
    Создаёт таблицу для хранения дополнительных тикеров, если её ещё нет.
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TICKERS_TABLE_NAME}` (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            table_name  VARCHAR(128) NOT NULL,
            symbol      VARCHAR(10)  NOT NULL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_table_symbol (table_name, symbol),
            INDEX idx_table_name (table_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Дополнительные тикеры для Finnhub parser';
    """)
    conn.commit()
    c.close()
    conn.close()


def load_saved_symbols_global() -> list:
    """
    Загружает глобальный список сохранённых тикеров
    (без привязки к конкретной целевой таблице).
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(
        f"SELECT DISTINCT symbol FROM `{TICKERS_TABLE_NAME}` ORDER BY symbol ASC"
    )
    rows = c.fetchall()
    c.close()
    conn.close()
    return [row[0].strip().upper() for row in rows if row and row[0]]


def apply_saved_symbols_to_all_tables():
    """
    Применяет глобальные сохранённые в БД тикеры ко всем таблицам DATASETS.
    """
    extra_symbols = load_saved_symbols_global()
    for dataset_name in DATASETS:
        merged = []
        seen = set()
        for symbol in DATASETS[dataset_name].get("symbols", []) + extra_symbols:
            if not isinstance(symbol, str):
                continue
            symbol = symbol.strip().upper()
            if symbol and symbol not in seen:
                merged.append(symbol)
                seen.add(symbol)
        DATASETS[dataset_name]["symbols"] = merged


def add_symbols_from_argument(table_name: str, tickers_arg: str, limit: int = 40):
    """
    Добавляет новые тикеры (JSON-массив строк) в symbols для указанной таблицы.
    Если итоговое количество тикеров > limit, ничего не добавляет.
    """
    if not tickers_arg:
        return

    tickers_json = None
    try:
        tickers_json = json.loads(tickers_arg)
    except json.JSONDecodeError:
        # PowerShell может передать массив как [TSLA,META] без кавычек.
        raw = tickers_arg.strip()
        if raw.startswith("[") and raw.endswith("]"):
            inner = raw[1:-1].strip()
            if inner:
                tickers_json = [token.strip() for token in inner.split(",")]
            else:
                tickers_json = []
        else:
            print(' Аргумент тикеров должен быть JSON-массивом, например: ["TSLA","META"]')
            return

    if not isinstance(tickers_json, list):
        print(" Аргумент тикеров должен быть JSON-массивом строк")
        return

    parsed = []
    for token in tickers_json:
        if not isinstance(token, str):
            continue
        symbol = token.strip().upper()
        if symbol:
            parsed.append(symbol)

    if not parsed:
        print("  JSON-массив тикеров передан, но валидные тикеры не распознаны")
        return

    current_symbols = DATASETS[table_name].get("symbols", [])
    existing = set()
    for dataset_name in DATASETS:
        for s in DATASETS[dataset_name].get("symbols", []):
            if isinstance(s, str) and s.strip():
                existing.add(s.strip().upper())
    new_unique = []
    seen_new = set()
    for symbol in parsed:
        if symbol in existing or symbol in seen_new:
            continue
        new_unique.append(symbol)
        seen_new.add(symbol)

    if not new_unique:
        print("ℹ  Все переданные тикеры уже есть в списке")
        return

    total_after_add = len(current_symbols) + len(new_unique)
    if total_after_add > limit:
        print(f" Нельзя добавить тикеры: будет {total_after_add}, лимит {limit}")
        print(f"   Текущее количество: {len(current_symbols)}")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.executemany(
        f"""
        INSERT INTO `{TICKERS_TABLE_NAME}` (table_name, symbol)
        SELECT %s, %s
        FROM DUAL
        WHERE NOT EXISTS (
            SELECT 1
            FROM `{TICKERS_TABLE_NAME}`
            WHERE symbol = %s
        )
        """,
        [(table_name, symbol, symbol) for symbol in new_unique]
    )
    conn.commit()
    c.close()
    conn.close()

    for dataset_name in DATASETS:
        merged = []
        seen = set()
        for symbol in DATASETS[dataset_name].get("symbols", []) + new_unique:
            if not isinstance(symbol, str):
                continue
            symbol = symbol.strip().upper()
            if symbol and symbol not in seen:
                merged.append(symbol)
                seen.add(symbol)
        DATASETS[dataset_name]["symbols"] = merged

#  7. СОЗДАНИЕ ТАБЛИЦЫ 
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            date_iso   DATE        NOT NULL,
            symbol     VARCHAR(10) NOT NULL,
            value      DOUBLE,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date_symbol (date_iso, symbol),
            INDEX idx_date_symbol (date_iso, symbol),
            INDEX idx_symbol (symbol)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

#  8. ПОСЛЕДНЯЯ ДАТА В БД (для логов) 
def get_latest_date(table_name: str):
    """
    Возвращает максимальную дату из таблицы или None если таблица пуста.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(date_iso) FROM `{table_name}`")
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None
    except:
        return None

#  9. ПОЛУЧЕНИЕ ДАННЫХ 
def fetch_data(config: dict) -> list:
    """
    Запрос к Finnhub. Возвращает список готовых кортежей:
    [(date_iso, symbol, value), ...]
    """
    symbols = config.get("symbols", [])
    metric = config.get("metric")
    rows = []
    today = datetime.now().date().isoformat()

    for symbol in symbols:
        try:
            if metric == "price":
                # Курс акций — реал-тайм / последняя цена
                url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}"
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    print(f" {symbol} quote HTTP {response.status_code}")
                    continue
                data = response.json()
                price = data.get("c")  # текущая цена
                if price is None or price <= 0:
                    print(f"  {symbol} — некорректная цена")
                    continue
                rows.append((today, symbol, float(price)))

            elif metric == "marketcap":
                # Рыночная капитализация
                url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    print(f" {symbol} profile2 HTTP {response.status_code}")
                    continue
                data = response.json()
                mcap = data.get("marketCapitalization")
                if mcap is None or mcap <= 0:
                    print(f"  {symbol} — некорректная капитализация")
                    continue
                rows.append((today, symbol, float(mcap)))

        except Exception as e:
            print(f" Ошибка запроса для {symbol}: {e}")
            continue

    return rows

#  10. ЗАПИСЬ В БД 
def save_rows(table_name: str, rows: list):
    if not rows:
        print("  Нет данных для записи")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    sql = f"""
        INSERT IGNORE INTO `{table_name}` (date_iso, symbol, value)
        VALUES (%s, %s, %s)
    """
    # INSERT IGNORE — пропускает дубли по (date_iso + symbol)
    c.executemany(sql, rows)
    conn.commit()

    print(f" Записано {c.rowcount} новых строк из {len(rows)} total")
    c.close()
    conn.close()

#  11. ОСНОВНАЯ ЛОГИКА 
def process(table_name: str):
    config = DATASETS[table_name]

    ensure_table(table_name)

    latest = get_latest_date(table_name)
    print(f" Последняя дата в БД: {latest or 'таблица пуста'}")

    raw = fetch_data(config)
    if not raw:
        print("  Данных нет")
        return

    rows = raw
    print(f" Строк для записи: {len(rows)}")
    save_rows(table_name, rows)

#  12. ТОЧКА ВХОДА 
def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATASETS:
            print(f"  - {name}")
        sys.exit(1)

    print(f" Finnhub Parser")
    print(f"   База: {args.host}:{args.port}/{args.database}")
    print(f"   Таблица: {args.table_name}")
    print(f"   API-ключ: {' есть' if FINNHUB_API_KEY else ' нет'}")

    ensure_tickers_table()
    apply_saved_symbols_to_all_tables()
    add_symbols_from_argument(args.table_name, args.tickers)
    process(args.table_name)

    print(" ГОТОВО")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n Прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\n Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)