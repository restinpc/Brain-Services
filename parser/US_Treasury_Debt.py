"""
U.S. Treasury Fiscal Data Parser — Debt to the Penny
Запуск:   python US_Treasury_Debt.py sasha_us_treasury_debt_to_penny [host] [port] [user] [password] [database]
"""

# ── 1. ИМПОРТЫ ─────────────────────────────────────────────────────────────────
import os
import sys
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime

# ── 2. КОНФИГ ─────────────────────────────────────────────────────────────────
load_dotenv()

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "us_treasury_debt_parser")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

# ── 3. ТРАССИРОВКА ОШИБОК ─────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "US_Treasury_Debt.py"):
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

# ── 4. АРГУМЕНТЫ ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="U.S. Treasury Debt to the Penny → MySQL")
parser.add_argument("table_name",  help="Имя целевой таблицы в БД")
parser.add_argument("host",        nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port",        nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user",        nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password",    nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database",    nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host":     args.host,
    "port":     int(args.port),
    "user":     args.user,
    "password": args.password,
    "database": args.database,
}

# ── 5. ТАБЛИЦЫ ─────────────────────────────────
DATASETS = {
    "sasha_us_treasury_debt_to_penny": {
        "description": "U.S. Treasury Fiscal Data - Debt to the Penny (ежедневный госдолг США)",
        "endpoint": "v2/accounting/od/debt_to_penny",
        "fields": "record_date,tot_pub_debt_out_amt,debt_held_public_amt,intragov_hold_amt",
        "sort": "-record_date",
        "date_field": "record_date",
        "value_field": "tot_pub_debt_out_amt",
    },
}

# ── 6. СОЗДАНИЕ ТАБЛИЦЫ ────────────────────────────────────────────────────────
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id                             INT AUTO_INCREMENT PRIMARY KEY,
            date_iso                       DATE        NOT NULL,
            tot_pub_debt_out_amt           DECIMAL(20,2) NULL,
            tot_pub_debt_out_amt_held_by_pub DECIMAL(20,2) NULL,
            govt_account_invest_hold_amt   DECIMAL(20,2) NULL,
            loaded_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date (date_iso),
            INDEX idx_date (date_iso)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()
    print(f"Таблица {table_name} проверена/создана")

# ── 7. ПОСЛЕДНЯЯ ДАТА В БД ───────────────────────────────────────────────────
def get_latest_date(table_name: str):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(date_iso) FROM `{table_name}`")
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print(f"Ошибка получения последней даты: {e}")
        return None

# ── 8. ПОЛУЧЕНИЕ ДАННЫХ ИЗ API ───────────────────────────────────────────────
def fetch_data(config: dict, latest_date=None) -> list:
    base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    url = f"{base_url}{config['endpoint']}"

    params = {
        "fields": config["fields"],
        "sort": config["sort"],
        "format": "json",
        "page[size]": 10000,
    }

    if latest_date:
        params["filter"] = f"record_date:gt:{latest_date}"

    try:
        print(f"Запрос к API: {url}")
        try:
            response = requests.get(url, params=params, timeout=60)
        except Exception as e:
            # Частая проблема на Windows: SOCKS proxy задан в env, но socks-зависимости не установлены.
            if "Missing dependencies for SOCKS support" in str(e):
                session = requests.Session()
                session.trust_env = False
                response = session.get(url, params=params, timeout=60)
            else:
                raise
        
        if response.status_code != 200:
            print(f"HTTP {response.status_code}: {response.text[:500]}")
            return []

        data = response.json()
        records = data.get("data", [])
        print(f"Получено {len(records)} записей")
        return records

    except Exception as e:
        print(f"Ошибка запроса к API: {e}")
        return []

# ── 9. ЗАПИСЬ В БД ────────────────────────────────────────────────────────────
def parse_amount(raw_value):
    if raw_value in (None, "", ".", "null"):
        return None
    return float(raw_value)

def save_rows(table_name: str, rows: list):
    if not rows:
        print("Нет новых данных для записи")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    sql = f"""
        INSERT IGNORE INTO `{table_name}` 
        (date_iso, tot_pub_debt_out_amt, tot_pub_debt_out_amt_held_by_pub, govt_account_invest_hold_amt)
        VALUES (%s, %s, %s, %s)
    """
    c.executemany(sql, rows)
    conn.commit()

    print(f"Записано {c.rowcount} новых строк")
    c.close()
    conn.close()

# ── 10. ОСНОВНАЯ ЛОГИКА ────────────────────────────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]

    ensure_table(table_name)

    latest = get_latest_date(table_name)
    print(f"Последняя дата в БД: {latest or 'таблица пуста'}")

    raw = fetch_data(config, latest)
    if not raw:
        print("Данных от API нет")
        return

    rows = []
    value_field = config["value_field"]
    date_field = config["date_field"]

    for item in raw:
        date_str = item.get(date_field)
        value = item.get(value_field)
        held_by_pub = item.get("debt_held_public_amt") or item.get("tot_pub_debt_out_amt_held_by_pub")
        intragov = item.get("intragov_hold_amt") or item.get("govt_account_invest_hold_amt")

        if not date_str or value in (None, "", "."):
            continue

        if latest and date_str <= str(latest):
            continue

        try:
            rows.append((
                date_str,
                parse_amount(value),
                parse_amount(held_by_pub),
                parse_amount(intragov)
            ))
        except (ValueError, TypeError):
            continue

    print(f"Новых строк после фильтра: {len(rows)}")
    save_rows(table_name, rows)

# ── 11. ТОЧКА ВХОДА ────────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name, cfg in DATASETS.items():
            print(f"  - {name} → {cfg['description']}")
        sys.exit(1)

    print("U.S. Treasury Debt to the Penny Parser")
    print(f"   База: {args.host}:{args.port}/{args.database}")
    print(f"   Таблица: {args.table_name}")
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