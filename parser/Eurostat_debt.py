"""
Eurostat Parser — Government Gross Debt (ЕС)
Запуск:   python Eurostat_debt.py <table_name> [host] [port] [user] [password] [database]
"""

# ── 1. ИМПОРТЫ ─────────────────────────────────────────────────────────────────
import os
import sys
import argparse
import time
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv

# ── 2. КОНФИГ ─────────────────────────────────────────────────────────────────
load_dotenv()

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "Eurostat_debt")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

# ── 3. ТРАССИРОВКА ОШИБОК ─────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "Eurostat_debt.py"):
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
parser = argparse.ArgumentParser(description="Eurostat Government Debt → MySQL")
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
    "sasha_eurostat_debt_pc_gdp": {
        "description": "Eurostat - Quarterly gross debt in % of GDP",
        "dataset": "gov_10q_ggdebt",
        "params": {
            "lang": "EN",
            "format": "JSON",
            "freq": "Q",
            "sector": "S13",
            "na_item": "GD",
            "unit": "PC_GDP",     
            # "geo": "EU27_2020,EA20,DE,FR,IT"  # при необходимости фильтр по странам
        },
        "date_field": "time",           # формат YYYY-Qn
        "geo_field": "geo",
    },
    "sasha_eurostat_debt_mio_eur": {
        "description": "Eurostat - Quarterly gross debt in millions of EUR",
        "dataset": "gov_10q_ggdebt",
        "params": {
            "lang": "EN",
            "format": "JSON",
            "freq": "Q",
            "sector": "S13",
            "na_item": "GD",
            "unit": "MIO_EUR",
            # "geo": "EU27_2020,EA20,DE,FR,IT"  # при необходимости фильтр по странам
        },
        "date_field": "time",
        "geo_field": "geo",
    },
}

# ── 6. СОЗДАНИЕ ТАБЛИЦЫ ────────────────────────────────────────────────────────
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor(buffered=True)
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            date_iso        DATE        NOT NULL,   -- преобразованный квартал в дату (последний день квартала)
            geo             VARCHAR(20) NOT NULL,
            unit            VARCHAR(20) NOT NULL DEFAULT 'PC_GDP',
            value           DOUBLE      NULL,       -- % от ВВП или абсолютное значение
            loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date_geo_unit (date_iso, geo, unit),
            INDEX idx_date (date_iso),
            INDEX idx_geo (geo),
            INDEX idx_unit (unit)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    # На случай если таблица уже существовала в старой схеме без unit.
    c.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE 'unit'")
    if not c.fetchone():
        c.execute(f"ALTER TABLE `{table_name}` ADD COLUMN unit VARCHAR(20) NOT NULL DEFAULT 'PC_GDP' AFTER geo")
    c.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'uq_date_geo_unit'")
    if not c.fetchone():
        c.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'uq_date_geo'")
        if c.fetchone():
            c.execute(f"ALTER TABLE `{table_name}` DROP INDEX uq_date_geo")
        c.execute(f"ALTER TABLE `{table_name}` ADD UNIQUE KEY uq_date_geo_unit (date_iso, geo, unit)")
    conn.commit()
    c.close()
    conn.close()
    print(f"Таблица {table_name} проверена/создана")

# ── 7. ПОСЛЕДНЯЯ ДАТА В БД ───────────────────────────────────────────────────
def get_latest_date(table_name: str, unit: str):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(date_iso) FROM `{table_name}` WHERE unit = %s", (unit,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print(f"Ошибка получения последней даты: {e}")
        return None

# ── 8. ПОЛУЧЕНИЕ ДАННЫХ ИЗ EUROSTAT API ─────────────────────────────────────
def fetch_data(config: dict, params: dict) -> dict:
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    url = f"{base_url}{config['dataset']}"

    response = None
    try:
        print(f"Запрос к Eurostat API: {url} (unit={params.get('unit')})")
        for attempt in range(1, 4):
            try:
                response = requests.get(url, params=params, timeout=60)
                if response.status_code in (429, 500, 502, 503, 504) and attempt < 3:
                    time.sleep(attempt * 2)
                    continue
                break
            except Exception as e:
                # Частая проблема на Windows: SOCKS proxy задан в env, но socks-зависимости не установлены.
                if "Missing dependencies for SOCKS support" in str(e):
                    session = requests.Session()
                    session.trust_env = False
                    response = session.get(url, params=params, timeout=60)
                    break
                if attempt == 3:
                    raise
                time.sleep(attempt * 2)
        
        if not response or response.status_code != 200:
            status = response.status_code if response else "no_response"
            body_preview = response.text[:500] if response else "empty response"
            print(f"HTTP {status}: {body_preview}")
            return {}

        data = response.json()
        if "value" not in data or "dimension" not in data:
            print(f"HTTP {response.status_code}: {response.text[:500]}")
            return {}

        print("Ответ Eurostat получен, начинаю парсинг JSON-stat")
        return data

    except Exception as e:
        print(f"Ошибка запроса к Eurostat: {e}")
        return {}


def quarter_to_date(quarter_key: str):
    if not quarter_key or "-Q" not in quarter_key:
        return None
    try:
        year_str, quarter_num_str = quarter_key.split("-Q", 1)
        year = int(year_str)
        quarter_num = int(quarter_num_str)
        quarter_end_map = {
            1: (3, 31),
            2: (6, 30),
            3: (9, 30),
            4: (12, 31),
        }
        month, day = quarter_end_map[quarter_num]
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, KeyError):
        return None


def decode_jsonstat_rows(raw: dict, config: dict, latest_date=None, selected_unit="PC_GDP"):
    dim_ids = raw.get("id", [])
    dim_sizes = raw.get("size", [])
    dimensions = raw.get("dimension", {})
    values = raw.get("value", {})

    if not dim_ids or not dim_sizes or len(dim_ids) != len(dim_sizes):
        print("Некорректная структура JSON-stat: id/size")
        return []

    labels_by_dim = {}
    for dim_id in dim_ids:
        dim_data = dimensions.get(dim_id, {})
        cat_index = dim_data.get("category", {}).get("index", {})
        if isinstance(cat_index, dict):
            ordered_labels = [name for name, _ in sorted(cat_index.items(), key=lambda x: x[1])]
        elif isinstance(cat_index, list):
            ordered_labels = cat_index
        else:
            ordered_labels = []
        labels_by_dim[dim_id] = ordered_labels

    rows = []
    latest_as_str = str(latest_date) if latest_date else None
    geo_field = config["geo_field"]
    date_field = config["date_field"]

    for flat_idx_raw, raw_value in values.items():
        if raw_value in (None, "", ".", "null"):
            continue

        try:
            flat_idx = int(flat_idx_raw)
        except (TypeError, ValueError):
            continue

        coords = []
        remainder = flat_idx
        for size in reversed(dim_sizes):
            coords.append(remainder % size)
            remainder //= size
        coords.reverse()

        dim_value_map = {}
        is_valid = True
        for dim_id, pos in zip(dim_ids, coords):
            labels = labels_by_dim.get(dim_id, [])
            if pos >= len(labels):
                is_valid = False
                break
            dim_value_map[dim_id] = labels[pos]
        if not is_valid:
            continue

        quarter_key = dim_value_map.get(date_field)
        geo = dim_value_map.get(geo_field)
        unit = dim_value_map.get("unit", selected_unit)
        date_iso = quarter_to_date(quarter_key)

        if not date_iso or not geo:
            continue
        if latest_as_str and date_iso <= latest_as_str:
            continue
        if unit != selected_unit:
            continue

        try:
            value = float(raw_value)
        except (ValueError, TypeError):
            continue

        rows.append((date_iso, geo, unit, value))

    return rows

# ── 9. ЗАПИСЬ В БД ────────────────────────────────────────────────────────────
def save_rows(table_name: str, rows: list):
    if not rows:
        print("Нет новых данных для записи")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    sql = f"""
        INSERT INTO `{table_name}` (date_iso, geo, unit, value)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            value = VALUES(value),
            loaded_at = CURRENT_TIMESTAMP
    """
    c.executemany(sql, rows)
    conn.commit()

    print(f"Записано {c.rowcount} новых строк")
    c.close()
    conn.close()

# ── 10. ОСНОВНАЯ ЛОГИКА (обработка JSON-stat) ────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]
    params = dict(config["params"])

    ensure_table(table_name)

    latest = get_latest_date(table_name, params["unit"])
    print(f"Последняя дата в БД (unit={params['unit']}): {latest or 'таблица пуста'}")

    raw = fetch_data(config, params)
    if not raw:
        print("Данных от API нет")
        return

    rows = decode_jsonstat_rows(raw, config, latest, selected_unit=params["unit"])
    print(f"Новых строк после фильтра: {len(rows)}")
    save_rows(table_name, rows)

# ── 11. ТОЧКА ВХОДА ────────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name, cfg in DATASETS.items():
            print(f"  - {name} → {cfg['description']}")
        sys.exit(1)

    print("Eurostat Government Debt Parser")
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