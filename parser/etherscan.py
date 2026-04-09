"""
Описание: Сетевые on-chain метрики Etherscan используя бесплатный API
Запуск:   python etherscan.py <table_name> [host] [port] [user] [password] [database]
Строка в .env ETHERSCAN_API_KEY=
"""

import os
import sys
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime

# ── 1. КОНФИГ ─────────────────────────────────────────────────────────────────
load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "etherscan")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    print("❌ Ошибка: ETHERSCAN_API_KEY не указан в .env файле")
    sys.exit(1)

# ── 2. ТРАССИРОВКА ОШИБОК ─────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "etherscan.py"):
    import threading
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"

    def _send():
        try:
            requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
        except:
            pass
    threading.Thread(target=_send, daemon=True).start()

# ── 3. АРГУМЕНТЫ ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Сетевые on-chain метрики Etherscan используя бесплатный API")
parser.add_argument("table_name",  help="Имя целевой таблицы в БД")
parser.add_argument("host",        nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port",        nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user",        nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password",    nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database",    nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host":     args.host,
    "port":     int(args.port),
    "user":     args.user,
    "password": args.password,
    "database": args.database,
}

# ── 4. ДОСТУПНЫЕ МЕТРИКИ ─────────────────
DATASETS = {
    "sasha_eth_gas_safe_daily": {
        "description": "Safe (Low) Gas Price в Gwei",
        "module": "gastracker",
        "action": "gasoracle",
        "field": "SafeGasPrice",
    },
    "sasha_eth_gas_propose_daily": {
        "description": "Propose (Average) Gas Price в Gwei",
        "module": "gastracker",
        "action": "gasoracle",
        "field": "ProposeGasPrice",
    },
    "sasha_eth_gas_fast_daily": {
        "description": "Fast (High) Gas Price в Gwei",
        "module": "gastracker",
        "action": "gasoracle",
        "field": "FastGasPrice",
    },
    "sasha_eth_price_usd_daily": {
        "description": "Цена ETH в USD",
        "module": "stats",
        "action": "ethprice",
        "field": "ethusd",
    },
    "sasha_eth_price_btc_daily": {
        "description": "Цена ETH в BTC (для пары BTC/ETH)",
        "module": "stats",
        "action": "ethprice",
        "field": "ethbtc",
    },
    "sasha_eth_supply2_daily": {
        "description": "Supply ETH2 (учитывает staking, burning по EIP-1559 и withdrawals)",
        "module": "stats",
        "action": "ethsupply2",
        "field": None,           
    },
    "sasha_eth_latest_block_daily": {
        "description": "Номер последнего блока сети Ethereum",
        "module": "proxy",
        "action": "eth_blockNumber",
        "field": None,           
    },
}

# ── 5. СОЗДАНИЕ ТАБЛИЦЫ ────────────────────────────────────────────────────────
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            date_iso   DATE        NOT NULL,
            value      DOUBLE      NULL,
            loaded_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date (date_iso),
            INDEX idx_date (date_iso)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

# ── 6. ПОСЛЕДНЯЯ ДАТА В БД ─────────────────────────────────────────────────────
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
        print(f"⚠️ Не удалось получить последнюю дату: {e}")
        return None

# ── 7. ЗАПРОС К ETHERSCAN API ─────────────────────────────────────────────────
def fetch_data(config: dict):
    try:
        params = {
            "chainid": "1",
            "module": config["module"],
            "action": config["action"],
            "apikey": ETHERSCAN_API_KEY,
        }

        response = requests.get("https://api.etherscan.io/v2/api", params=params, timeout=25)

        if response.status_code != 200:
            print(f"❌ HTTP ошибка: {response.status_code}")
            return None

        data = response.json()

        # proxy/eth_blockNumber в V2 возвращает jsonrpc-формат без status/message
        if config["action"] == "eth_blockNumber" and "result" in data:
            return data.get("result")

        if data.get("status") != "1":
            print(f"❌ Etherscan API ошибка: {data.get('message', 'Unknown')} | {data.get('result', '')}")
            return None

        return data.get("result")

    except Exception as e:
        print(f"❌ Ошибка при запросе к Etherscan: {e}")
        return None

# ── 8. ОБРАБОТКА И СОХРАНЕНИЕ ─────────────────────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]
    ensure_table(table_name)

    latest = get_latest_date(table_name)
    today = datetime.now().date().isoformat()

    if latest and today <= str(latest):
        print(f"⏭️  Данные за {today} уже существуют в таблице")
        return

    result = fetch_data(config)
    if not result:
        print("⚠️  Данные от Etherscan не получены")
        return

    # Извлекаем конкретное поле из result, если API вернул словарь.
    raw_value = result
    if isinstance(result, dict):
        field_name = config.get("field")
        if field_name:
            raw_value = result.get(field_name)
        elif config["action"] == "ethsupply2":
            # Для ehsupply2 в качестве базовой метрики берем общий supply.
            raw_value = result.get("EthSupply")
        else:
            print(f"❌ Неизвестно, какое поле извлекать из ответа: {result}")
            return

    # Конвертация значения в число
    try:
        if config["action"] == "eth_blockNumber" and isinstance(raw_value, str) and raw_value.startswith("0x"):
            value = int(raw_value, 16)                 # hex → decimal
        else:
            value = float(raw_value)                   # для supply2, gas, price и т.д.
    except (ValueError, TypeError) as e:
        print(f"❌ Не удалось преобразовать значение в число: {e}")
        print(f"   Получено: {raw_value}")
        return

    # Запись в базу
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    sql = f"INSERT IGNORE INTO `{table_name}` (date_iso, value) VALUES (%s, %s)"
    c.execute(sql, (today, value))
    conn.commit()
    c.close()
    conn.close()

    print(f"✅ Успешно записано")


# ── 9. MAIN ───────────────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица: {args.table_name}")
        print("\nДоступные таблицы:")
        for name, cfg in DATASETS.items():
            print(f"   • {name}  →  {cfg['description']}")
        sys.exit(1)

    print(f"🚀 Etherscan Network Metrics Parser (free API)")
    print(f"   Таблица: {args.table_name}")
    print(f"   База: {args.host}:{args.port}/{args.database}")

    process(args.table_name)

    print("🏁 Парсер завершил работу")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)