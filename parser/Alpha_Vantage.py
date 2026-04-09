"""
Описание: Alpha Vantage — дневные технические индикаторы 
          Для EUR/USD, BTC/USD, ETH/USD
Ограничения: 5 запросов в минуту, 25 запросов в день.
Запуск:   python Alpha_Vantage.py <table_name> [host] [port] [user] [password] [database]
Строка в .env: ALPHA_VANTAGE_API_KEY=
"""

import os
import sys
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Конфигурация ─────────────────────────────────────────────────────────────
ALPHA_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
if not ALPHA_API_KEY:
    print("❌ ALPHA_VANTAGE_API_KEY не найден в .env")
    sys.exit(1)

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "Alpha_Vantage")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

# Free tier Alpha Vantage: до 5 запросов/мин.
_LAST_AV_CALL_TS = 0.0
_MIN_AV_INTERVAL_SEC = 12.2

# Аргументы командной строки
parser = argparse.ArgumentParser(description="Alpha Vantage Daily Indicators")
parser.add_argument("table_name", help="Имя целевой таблицы")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

# ── Доступные таблицы ─────────────────────
DATASETS = {
    "sasha_alpha_daily_eurusd_ind": {
        "description": "Дневные технические индикаторы EUR/USD",
        "type": "forex",
        "from_currency": "EUR",
        "to_currency": "USD",
        "function_base": "FX_DAILY",
        "indicators": ["SMA", "EMA", "RSI", "BBANDS"]
    },
    "sasha_alpha_daily_btcusd_ind": {
        "description": "Дневные технические индикаторы BTC/USD",
        "type": "crypto",
        "symbol": "BTC",
        "market": "USD",
        "function_base": "DIGITAL_CURRENCY_DAILY",
        "indicators": ["SMA", "EMA", "RSI", "BBANDS"]
    },
    "sasha_alpha_daily_ethusd_ind": {
        "description": "Дневные технические индикаторы ETH/USD",
        "type": "crypto",
        "symbol": "ETH",
        "market": "USD",
        "function_base": "DIGITAL_CURRENCY_DAILY",
        "indicators": ["SMA", "EMA", "RSI", "BBANDS"]
    },
}

# ── Создание таблицы ─────────────────────────────────────
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            datetime_iso    DATE        NOT NULL,   -- только дата для daily
            sma_20          DOUBLE,
            ema_12          DOUBLE,
            ema_26          DOUBLE,
            rsi_14          DOUBLE,
            bb_middle       DOUBLE,
            bb_upper        DOUBLE,
            bb_lower        DOUBLE,
            loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date (datetime_iso),
            INDEX idx_date (datetime_iso)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

# ── Последняя дата в БД ──────────────────────────────────────────────────────
def get_latest_date(table_name: str):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(datetime_iso) FROM `{table_name}`")
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None
    except:
        return None

# ── Отправка ошибки ─────────────────────────────────────
def send_error_trace(exc: Exception):
    import threading
    logs = f"Node: {NODE_NAME}\nScript: Alpha_Vantage.py\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"

    def _send():
        try:
            requests.post(TRACE_URL, data={
                "url": "Alpha_Vantage",
                "node": NODE_NAME,
                "email": EMAIL,
                "logs": logs
            }, timeout=10)
        except:
            pass
    threading.Thread(target=_send, daemon=True).start()

# ── Запрос данных из Alpha Vantage ──────────────────
def fetch_indicator(config: dict, indicator: str = None, time_period_override: int = None):
    try:
        global _LAST_AV_CALL_TS
        base_url = "https://www.alphavantage.co/query"
        params = {
            "apikey": ALPHA_API_KEY,
            "outputsize": "compact", 
        }

        if indicator:
            params["function"] = indicator
        else:
            params["function"] = config["function_base"]

        # Параметры для индикаторов
        if indicator:
            
            if config["type"] == "forex":
                params["symbol"] = f"{config['from_currency']}{config['to_currency']}"
            else:  # crypto
                params["symbol"] = config["symbol"]
                params["market"] = config["market"]

            params["interval"] = "daily"
            params["series_type"] = "close"
            if time_period_override is not None:
                params["time_period"] = time_period_override
            elif indicator in ["SMA", "EMA", "BBANDS"]:
                params["time_period"] = 20
            elif indicator == "RSI":
                params["time_period"] = 14
        else:
            if config["type"] == "forex":
                params["from_symbol"] = config["from_currency"]
                params["to_symbol"]   = config["to_currency"]
            else:
                params["symbol"] = config["symbol"]
                params["market"] = config["market"]

        # Соблюдаем лимит, ретраи при throttling.
        data = {}
        for attempt in range(1, 4):
            now = time.time()
            sleep_for = _MIN_AV_INTERVAL_SEC - (now - _LAST_AV_CALL_TS)
            if sleep_for > 0:
                time.sleep(sleep_for)

            response = requests.get(base_url, params=params, timeout=30)
            _LAST_AV_CALL_TS = time.time()
            data = response.json()

            if "Note" in data or "Information" in data:
                msg = data.get("Note") or data.get("Information")
                print(f"⚠️ Alpha Vantage throttle/info (attempt {attempt}/3): {msg}")
                if attempt < 3:
                    time.sleep(_MIN_AV_INTERVAL_SEC)
                    continue
            break

        if "Error Message" in data or "Note" in data or "Information" in data:
            print(f"⚠️ Alpha Vantage: {data.get('Error Message') or data.get('Note') or data.get('Information')}")
            return {}

        # Находим временной ряд
        for key in data:
            if any(k in key for k in ["Time Series", "SMA", "EMA", "RSI", "MACD", "BBANDS"]):
                return data[key]
        return {}

    except Exception as e:
        print(f"❌ Ошибка при запросе {indicator or 'base'}: {e}")
        return {}

# ── Основная обработка ───────────────────────────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]
    ensure_table(table_name)

    latest = get_latest_date(table_name)
    print(f"📅 Последняя дата в БД: {latest or 'таблица пуста'}")

    # Качаем каждый индикатор отдельно
    indicator_data = {}
    indicators_to_fetch = config.get("indicators", [])
    for ind in indicators_to_fetch:
        print(f"📥 Загрузка индикатора {ind}...")
        if ind == "EMA":
            ema12 = fetch_indicator(config, "EMA", time_period_override=12)
            ema26 = fetch_indicator(config, "EMA", time_period_override=26)
            if ema12:
                indicator_data["EMA12"] = ema12
            if ema26:
                indicator_data["EMA26"] = ema26
        else:
            ind_data = fetch_indicator(config, ind)
            if ind_data:
                indicator_data[ind] = ind_data

    if not indicator_data:
        print("⚠️ Не удалось получить данные индикаторов")
        return

    # Объединяем данные по датам
    rows = []
    # Берём даты из первого доступного индикатора
    sample_key = next(iter(indicator_data.values()))
    for dt_str in sample_key.keys():
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d").date()  # только дата

            if latest and dt <= latest:
                continue

            row = [dt]

            # SMA 20
            row.append(float(indicator_data.get("SMA", {}).get(dt_str, {}).get("SMA", None) or 0))
            # EMA 12 и EMA 26
            ema12 = float(indicator_data.get("EMA12", {}).get(dt_str, {}).get("EMA", 0) or 0)
            ema26 = float(indicator_data.get("EMA26", {}).get(dt_str, {}).get("EMA", 0) or 0)
            row.append(ema12)
            row.append(ema26)
            # RSI
            row.append(float(indicator_data.get("RSI", {}).get(dt_str, {}).get("RSI", 0) or 0))
            # Bollinger Bands
            bb = indicator_data.get("BBANDS", {}).get(dt_str, {})
            row.append(float(bb.get("Real Middle Band", 0) or 0))
            row.append(float(bb.get("Real Upper Band", 0) or 0))
            row.append(float(bb.get("Real Lower Band", 0) or 0))

            rows.append(tuple(row))

        except Exception:
            continue

    print(f"🆕 Найдено новых дней: {len(rows)}")

    if not rows:
        print("✅ Нет новых данных")
        return

    # Запись в БД
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    sql = f"""
        INSERT IGNORE INTO `{table_name}` 
        (datetime_iso, sma_20, ema_12, ema_26, rsi_14, bb_middle, bb_upper, bb_lower)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    c.executemany(sql, rows)
    conn.commit()
    print(f"✅ Записано {c.rowcount} новых строк")
    c.close()
    conn.close()

# ── Запуск ───────────────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица '{args.table_name}'.")
        print("Доступные:")
        for name in DATASETS:
            print(f"  - {name}")
        sys.exit(1)

    print(f"🚀 Alpha Vantage Daily Indicators (Free Tier Only)")
    print(f"   Таблица: {args.table_name}")
    print("=" * 65)

    try:
        process(args.table_name)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)

    print("=" * 65)
    print("🏁 ГОТОВО")


if __name__ == "__main__":
    main()