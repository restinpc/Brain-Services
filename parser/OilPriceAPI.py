"""
OilPriceAPI Parser
Запуск:   python OilPriceAPI.py <table_name> [host] [port] [user] [password] [database]
Строка в .env: OILPRICE_API_KEY=...
Доступные table_name:
    sasha_oilpriceapi_brent
    sasha_oilpriceapi_wti
    sasha_oilpriceapi_urals
    sasha_oilpriceapi_dubai
    sasha_oilpriceapi_opec
"""

import os
import sys
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, date

#  1. КОНФИГ
load_dotenv()

OILPRICE_API_KEY = (
    os.getenv("OILPRICE_API_KEY")
    or os.getenv("OILPRICEAPI_API_KEY")
    or os.getenv("OIL_PRICE_API_KEY")
)
OILPRICE_BASE_URL = os.getenv("OILPRICE_API_BASE_URL", "https://api.oilpriceapi.com/v1").rstrip("/")

_HANDLER = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL = f"{_HANDLER}/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "OilPriceAPI")
EMAIL = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

#  2. ТРАССИРОВКА ОШИБОК
def send_error_trace(exc: Exception, script_name: str = "OilPriceAPI.py"):
    """
    Отправляет трассировку в фоновом потоке — не блокирует основной процесс.
    """
    import threading

    logs = (
        f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}"
        f"\n\nTraceback:\n{traceback.format_exc()}"
    )

    def _send():
        try:
            requests.post(
                TRACE_URL,
                data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
                timeout=10,
            )
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True).start()


#  3. АРГУМЕНТЫ
parser = argparse.ArgumentParser(description="OilPriceAPI Parser -> MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

if not OILPRICE_API_KEY:
    print("Ошибка: не найден OILPRICE_API_KEY (или OILPRICEAPI_API_KEY) в .env")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

#  4. ДАТАСЕТЫ
DATASETS = {
    "sasha_oilpriceapi_brent": {
        "description": "Brent crude price",
        "code_candidates": ["BRENT_CRUDE_USD", "BRENT_USD"],
    },
    "sasha_oilpriceapi_wti": {
        "description": "WTI crude price",
        "code_candidates": ["WTI_USD"],
    },
    "sasha_oilpriceapi_urals": {
        "description": "Urals crude price",
        "code_candidates": ["URALS_USD"],
    },
    "sasha_oilpriceapi_dubai": {
        "description": "Dubai crude price",
        "code_candidates": ["DUBAI_USD"],
    },
    "sasha_oilpriceapi_opec": {
        "description": "OPEC basket crude price",
        "code_candidates": ["OPEC_BASKET_USD"],
    },
}


#  5. БАЗА
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            date_iso        DATE NOT NULL,
            commodity_code  VARCHAR(64) NOT NULL,
            value           DOUBLE,
            loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date_code (date_iso, commodity_code),
            INDEX idx_date (date_iso),
            INDEX idx_code (commodity_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
        """
    )
    conn.commit()
    c.close()
    conn.close()


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
    except Exception:
        return None


def _parse_date_any(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(int(value)).date().isoformat()
        except Exception:
            return None
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    raw = raw.replace("Z", "+00:00")
    formats = (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    )
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(raw).date().isoformat()
    except Exception:
        return None


def _extract_rows_from_payload(payload, preferred_code: str) -> list:
    """
    Пытается нормализовать разные схемы ответа OilPriceAPI.
    Возвращает [(date_iso, commodity_code, value), ...].
    """
    rows = []
    seen = set()
    today = date.today().isoformat()

    def _add_row(dt_raw, code_raw, value_raw):
        dt = _parse_date_any(dt_raw) or today
        code = str(code_raw or preferred_code).strip().upper()
        try:
            value = float(value_raw)
        except (TypeError, ValueError):
            return

        key = (dt, code)
        if key in seen:
            return
        seen.add(key)
        rows.append((dt, code, value))

    def _walk(obj):
        if isinstance(obj, dict):
            has_price = any(k in obj for k in ("price", "value", "close", "last"))
            if has_price:
                _add_row(
                    obj.get("date") or obj.get("datetime") or obj.get("timestamp") or obj.get("updated_at"),
                    obj.get("code") or obj.get("commodity_code") or obj.get("symbol"),
                    obj.get("price") if "price" in obj else obj.get("value") if "value" in obj else obj.get("close") if "close" in obj else obj.get("last"),
                )
            for nested_value in obj.values():
                if isinstance(nested_value, (dict, list)):
                    _walk(nested_value)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(payload)
    return rows


def _request_oilprice(endpoint: str, code: str):
    url = f"{OILPRICE_BASE_URL}{endpoint}"
    headers = {"Authorization": f"Token {OILPRICE_API_KEY}"}
    params = {"by_code": code}
    response = requests.get(url, params=params, headers=headers, timeout=30)

    if response.status_code != 200:
        return None

    try:
        return response.json()
    except ValueError:
        return None


def fetch_data(config: dict) -> list:
    """
    Пытаемся получить историю через past_month/past_week.
    Если недоступно на тарифе — используем latest.
    """
    endpoints = ["/prices/past_month", "/prices/past_week", "/prices/latest"]

    for code in config.get("code_candidates", []):
        for endpoint in endpoints:
            payload = _request_oilprice(endpoint, code)
            if not payload:
                continue

            rows = _extract_rows_from_payload(payload, code)
            if rows:
                print(f"Получено строк: {len(rows)} (code={code}, endpoint={endpoint})")
                return rows

    return []


def save_rows(table_name: str, rows: list):
    if not rows:
        print("Нет новых данных для записи")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    sql = f"""
        INSERT IGNORE INTO `{table_name}` (date_iso, commodity_code, value)
        VALUES (%s, %s, %s)
    """
    c.executemany(sql, rows)
    conn.commit()
    print(f"Записано {c.rowcount} новых строк")
    c.close()
    conn.close()


#  6. ОСНОВНАЯ ЛОГИКА
def process(table_name: str):
    config = DATASETS[table_name]
    ensure_table(table_name)

    latest = get_latest_date(table_name)
    print(f"Последняя дата в БД: {latest or 'таблица пуста'}")

    raw_rows = fetch_data(config)
    if not raw_rows:
        print("Данных от OilPriceAPI нет")
        return

    rows = []
    for dt, code, value in raw_rows:
        if latest and dt <= str(latest):
            continue
        rows.append((dt, code, value))

    print(f"Новых строк после фильтра: {len(rows)}")
    save_rows(table_name, rows)


#  7. ТОЧКА ВХОДА
def main():
    if args.table_name not in DATASETS:
        print(f"Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name, cfg in DATASETS.items():
            codes = ", ".join(cfg.get("code_candidates", []))
            print(f"  - {name} -> {codes}")
        sys.exit(1)

    print("OilPriceAPI Parser")
    print(f"  База: {args.host}:{args.port}/{args.database}")
    print(f"  Таблица: {args.table_name}")
    print("=" * 70)

    process(args.table_name)

    print("=" * 70)
    print("ГОТОВО")


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
        send_error_trace(e)
        sys.exit(1)
