"""
FRED API Parser
Запуск:   python FRED.py <table_name> [host] [port] [user] [password] [database]
Доступные table_name:
    fred_dff
    fred_dgs10
    fred_vixcls
    fred_dexuseu
    fred_t10yie
    fred_cbbtcusd
"""

# ── 1. ИМПОРТЫ ─────────────────────────────────────────────────────────────────
import os
import sys
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ── 2. КОНФИГ ─────────────────────────────────────────────────────────────────
load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "FRED")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

TABLE_PREFIX = "sasha_"

# ── 3. ТРАССИРОВКА ОШИБОК ─────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "fred_parser.py"):
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
parser = argparse.ArgumentParser(description="FRED Parser → MySQL")
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

# ── 5. ТАБЛИЦЫ ─────────────────────────────────
DATASETS = {
    "fred_dff": {
        "series_id": "DFF",
        "description": "Effective Federal Funds Rate — Эффективная ставка по федеральным фондам"
    },
    "fred_dgs10": {
        "series_id": "DGS10",
        "description": "10-Year Treasury — Доходность 10-летних казначейских облигаций"
    },
    "fred_vixcls": {
        "series_id": "VIXCLS",
        "description": "Risk index — Индекс волатильности VIX"
    },
    "fred_dexuseu": {
        "series_id": "DEXUSEU",
        "description": "EUR/USD — Номинальный спот"
    },
    "fred_t10yie": {
        "series_id": "T10YIE",
        "description": "10-Year Breakeven Inflation Rate — 10-летний безубыточный уровень инфляции"
    },
    "fred_cbbtcusd": {
        "series_id": "CBBTCUSD",
        "description": "BTC — Цена Bitcoin от Coinbase (USD)"
    },
}

def get_full_table_name(short_name: str) -> str:
    return f"{TABLE_PREFIX}{short_name}"

# ── 6. СОЗДАНИЕ ТАБЛИЦЫ ────────────────────────────────────────────────────────
def ensure_table(short_name: str):
    full_table = get_full_table_name(short_name)
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{full_table}` (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            date_iso   DATE        NOT NULL,
            value      DOUBLE,
            loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date (date_iso),
            INDEX idx_date (date_iso)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[short_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

# ── 7. ПОСЛЕДНЯЯ ДАТА В БД (для инкрементальной загрузки) ─────────────────────
def get_latest_date(short_name: str):
    full_table = get_full_table_name(short_name)
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(date_iso) FROM `{full_table}`")
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None
    except:
        return None

# ── 8. ПОЛУЧЕНИЕ ДАННЫХ ───────────────────────────────────────────────────────
def fetch_data(config: dict, observation_start: str = None) -> list:
    """
    Запрос к FRED API. Поддерживает observation_start для инкрементальной загрузки.
    """
    try:
        params = {
            "series_id": config["series_id"],
            "api_key": FRED_API_KEY,
            "file_type": "json",
        }
        if observation_start:
            params["observation_start"] = observation_start

        response = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params=params,
            timeout=30,
        )

        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code} (series: {config['series_id']})")
            print(response.text[:500])
            return []

        data = response.json()
        observations = data.get("observations", [])
        print(f"📥 Получено наблюдений от FRED: {len(observations)}")
        return observations

    except Exception as e:
        print(f"❌ Ошибка запроса к FRED: {e}")
        return []

# ── 9. ЗАПИСЬ В БД ────────────────────────────────────────────────────────────
def save_rows(short_name: str, rows: list):
    if not rows:
        print("⚠️  Нет новых данных")
        return

    full_table = get_full_table_name(short_name)
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    sql = f"""
        INSERT IGNORE INTO `{full_table}` (date_iso, value)
        VALUES (%s, %s)
    """
    c.executemany(sql, rows)
    conn.commit()
    print(f"✅ Записано {c.rowcount} новых строк в {full_table}")
    c.close()
    conn.close()

# ── 10. ОСНОВНАЯ ЛОГИКА ────────────────────────────────────────────────────────
def process(short_name: str):
    config = DATASETS[short_name]
    ensure_table(short_name)

    latest = get_latest_date(short_name)
    print(f"📅 Последняя дата в БД: {latest or 'таблица пуста'}")

    observation_start = None
    if latest:
        try:
            start_date = latest + timedelta(days=1)
            observation_start = start_date.strftime("%Y-%m-%d")
        except:
            pass

    raw = fetch_data(config, observation_start)
    if not raw:
        print("⚠️  Данных от FRED нет")
        return

    rows = []
    for item in raw:
        date = item.get("date")
        value = item.get("value")
        if not date or value in (None, ".", "", "NA"):
            continue
        try:
            rows.append((date, float(value)))
        except (ValueError, TypeError):
            continue

    print(f"🆕 Новых строк после фильтра: {len(rows)}")
    save_rows(short_name, rows)

# ── 11. ТОЧКА ВХОДА ────────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name, cfg in DATASETS.items():
            print(f"  - {name} → {cfg['description']}")
        sys.exit(1)

    print(f"🚀 FRED Parser")
    print(f"   База: {args.host}:{args.port}/{args.database}")
    print(f"   Таблица: {args.table_name} ({DATASETS[args.table_name]['series_id']})")
    print("=" * 70)

    process(args.table_name)

    print("=" * 70)
    print("🏁 ГОТОВО")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)