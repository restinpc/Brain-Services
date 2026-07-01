"""
Описание: Dune Analytics on-chain метрики BTC/ETH
Запуск:   python dune.py <table_name> [host] [port] [user] [password] [database]
Строка в .env: DUNE_API_KEY=
"""

#  1. ИМПОРТЫ 
import os
import sys
import argparse
import traceback
import requests
import mysql.connector
import time
from dotenv import load_dotenv
from datetime import datetime

#  2. КОНФИГ 
load_dotenv()

# Windows-консоль может быть не в UTF-8; не падаем на emoji.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "dune")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")

if not DUNE_API_KEY:
    print(" Ошибка: DUNE_API_KEY не найден в .env")
    sys.exit(1)

#  3. ТРАССИРОВКА ОШИБОК 
def send_error_trace(exc: Exception, script_name: str = "dune.py"):
    import threading
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"

    def _send():
        try:
            requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
        except:
            pass
    threading.Thread(target=_send, daemon=True).start()

#  4. АРГУМЕНТЫ 
parser = argparse.ArgumentParser(description="Dune Analytics on-chain parser → MySQL")
parser.add_argument("table_name",  help="Имя целевой таблицы в БД")
parser.add_argument("host",        nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port",        nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user",        nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password",    nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database",    nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host":     args.host,
    "port":     int(args.port),
    "user":     args.user,
    "password": args.password,
    "database": args.database,
}

#  5. ТАБЛИЦЫ + SQL-запросы к Dune 
DATASETS = {
    "sasha_btc_daily_tx_count": {
        "description": "BTC — Daily Transaction Count(количество транзакций в день)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                count(*) as value
            FROM bitcoin.transactions 
            WHERE block_time >= now() - interval '90' day 
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_btc_daily_active_addresses": {
        "description": "BTC — Daily Active Addresses(примерное количество активных адресов в день)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                APPROX_DISTINCT(address) as value
            FROM bitcoin.inputs
            WHERE block_time >= now() - interval '90' day 
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_btc_daily_volume_btc": {
        "description": "BTC — Daily On-chain Volume(объём перемещённого BTC в день)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                SUM(output_value) / 100000000.0 as value
            FROM bitcoin.transactions 
            WHERE block_time >= now() - interval '90' day 
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_eth_daily_tx_count": {
        "description": "ETH — Daily Transaction Count(количество транзакций в день)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                count(*) as value
            FROM ethereum.transactions 
            WHERE block_time >= now() - interval '90' day 
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_eth_daily_active_addresses": {
        "description": "ETH — Daily Active Addresses(количество активных адресов в день)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                APPROX_DISTINCT("from") as value
            FROM ethereum.transactions 
            WHERE block_time >= now() - interval '90' day 
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_eth_daily_gas_used": {
        "description": "ETH — Daily Gas Used(объём использованного газа в день)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                SUM(gas_used) as value
            FROM ethereum.transactions 
            WHERE block_time >= now() - interval '90' day 
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_eth_daily_eth_burned": {
        "description": "ETH — Daily ETH Burned (количество сожжённого ETH в день по EIP-1559)",
        "sql": """
            SELECT 
                date_trunc('day', block_time) as date_iso,
                SUM(tx_fee_breakdown['base_fee']) as value
            FROM gas.fees
            WHERE blockchain = 'ethereum'
              AND block_date >= current_date - interval '90' day
            GROUP BY 1 
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_eth_daily_fee_usd": {
        "description": "ETH — Daily Transaction Fees (USD) (сумма комиссий в USD в день)",
        "sql": """
            SELECT
                date_trunc('day', block_time) AS date_iso,
                SUM(tx_fee_usd) AS value
            FROM gas.fees
            WHERE blockchain = 'ethereum'
              AND block_date >= current_date - interval '90' day
            GROUP BY 1
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
    "sasha_btc_daily_fee_btc": {
        "description": "BTC — Daily Transaction Fees (BTC) (сумма комиссий в BTC в день)",
        "sql": """
            WITH max_bt AS (
                SELECT MAX(block_time) AS max_block_time
                FROM bitcoin.transactions
            )
            SELECT
                date_trunc('day', t.block_time) AS date_iso,
                SUM(t.fee) / 100000000.0 AS value
            FROM bitcoin.transactions t
            CROSS JOIN max_bt m
            WHERE m.max_block_time IS NOT NULL
              AND t.block_time >= m.max_block_time - interval '90' day
            GROUP BY 1
            ORDER BY 1
        """,
        "date_col": "date_iso",
        "value_col": "value"
    },
}

#  6. СОЗДАНИЕ ТАБЛИЦЫ 
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            date_iso   DATE        NOT NULL,
            value      DOUBLE,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date (date_iso),
            INDEX idx_date (date_iso)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

#  7. ПОСЛЕДНЯЯ ДАТА В БД 
def get_latest_date(table_name: str):
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

#  8. ЗАПРОС К DUNE API (execute + poll) 
def fetch_dune_data(sql: str) -> list:
    headers = {
        "X-DUNE-API-KEY": DUNE_API_KEY,
        "Accept": "application/json",
    }
    
    try:
        resp = requests.post(
            "https://api.dune.com/api/v1/sql/execute",
            json={"sql": sql},
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        execution_id = resp.json()["execution_id"]
    except Exception as e:
        print(f" Ошибка запуска Dune query: {e}")
        return []

    for _ in range(120):
        try:
            status_resp = requests.get(
                f"https://api.dune.com/api/v1/execution/{execution_id}/status",
                headers=headers,
                timeout=10
            )
            status_resp.raise_for_status()
            status_json = status_resp.json()
            state = status_json.get("state")

            if not state:
                # В редких случаях API может вернуть служебный/ошибочный payload без state.
                # Не падаем по KeyError, пробуем подождать и повторить.
                msg = status_json.get("error") or status_json.get("message") or str(status_json)
                print(f" Неожиданный ответ статуса Dune: {msg}")
                time.sleep(2)
                continue

            # Free tier допускает 1 одновременный интерактивный запрос.
            if status_json.get("max_inflight_interactive_reached"):
                time.sleep(2)
                continue

            if state == "QUERY_STATE_COMPLETED":
                break
            if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                print(f" Dune query failed: {state}")
                return []
        except Exception as e:
            print(f" Ошибка статуса: {e}")
            return []
        time.sleep(2)

    try:
        result_resp = requests.get(
            f"https://api.dune.com/api/v1/execution/{execution_id}/results",
            headers=headers,
            timeout=30
        )
        result_resp.raise_for_status()
        data = result_resp.json()
        return data.get("result", {}).get("rows", [])
    except Exception as e:
        print(f" Ошибка получения результатов: {e}")
        return []

#  9. ЗАПИСЬ В БД 
def save_rows(table_name: str, rows: list):
    if not rows:
        print("  Нет новых данных")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    sql = f"""
        INSERT IGNORE INTO `{table_name}` (date_iso, value)
        VALUES (%s, %s)
    """
    c.executemany(sql, rows)
    conn.commit()

    print(f" Записано {c.rowcount} новых строк")
    c.close()
    conn.close()

#  10. ОСНОВНАЯ ЛОГИКА 
def process(table_name: str):
    config = DATASETS[table_name]

    ensure_table(table_name)

    latest = get_latest_date(table_name)
    print(f" Последняя дата в БД: {latest or 'таблица пуста'}")

    print(" Выполняем SQL-запрос в Dune...")
    raw = fetch_dune_data(config["sql"])
    if not raw:
        print("  Данных не получено")
        return

    rows = []
    for item in raw:
        date_val = item.get(config["date_col"])
        value_val = item.get(config["value_col"])

        if not date_val or value_val is None:
            continue

        if isinstance(date_val, str):
            date_str = date_val[:10]
        else:
            date_str = str(date_val)[:10]

        if latest and date_str <= str(latest):
            continue

        try:
            rows.append((date_str, float(value_val)))
        except (ValueError, TypeError):
            continue

    print(f" Новых строк: {len(rows)}")
    save_rows(table_name, rows)

#  11. ТОЧКА ВХОДА 
def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name, cfg in DATASETS.items():
            print(f"  - {name} → {cfg['description']}")
        sys.exit(1)

    print(f" Dune On-Chain Parser")
    print(f"   База: {args.host}:{args.port}/{args.database}")
    print(f"   Таблица: {args.table_name} — {DATASETS[args.table_name]['description']}")
    print("=" * 70)

    process(args.table_name)

    print("=" * 70)
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