"""
Описание: IMF Data Parser
Запуск:   python IMF.py sasha_imf_international_reserves [host] [port] [user] [password] [database]
"""

#  1. ИМПОРТЫ 
import os
import sys
import argparse
import traceback
import requests
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, date
import pandas as pd
import io
import time
import re

#  2. КОНФИГ 
load_dotenv()

# включаем UTF-8, чтобы не падать на emoji.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "IMF")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

#  3. ТРАССИРОВКА ОШИБОК 
def send_error_trace(exc: Exception, script_name: str = "IMF.py"):
    import threading
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"

    def _send():
        try:
            requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
        except:
            pass

    threading.Thread(target=_send, daemon=True).start()

#  4. АРГУМЕНТЫ 
parser = argparse.ArgumentParser(description="IMF Data Parser → MySQL (SDMX 2.1 API)")
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

#  5. ТАБЛИЦЫ (table_name → конфиг запроса к IMF SDMX API) 
# Ключ = имя таблицы в БД
DATASETS = {
    "sasha_imf_international_reserves": {
        "description": "IRFCL - Международные резервы и ликвидность иностранной валюты",
        "dataflow": "IRFCL",
        "key": "...",
        "require_daily_frequency": True,
        "usd_eur_filter_columns": ["CURRENCY", "INDICATOR", "FULL_DESCRIPTION", "SERIES_NAME"],
        "default_start_date": "2015-01-01",
    },
}

#  6. СОЗДАНИЕ ТАБЛИЦЫ (универсальная под несколько серий) 
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            date_iso    DATE        NOT NULL,
            series_key  VARCHAR(255) NOT NULL,
            value       DOUBLE,
            loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_date_series (date_iso, series_key),
            INDEX idx_date (date_iso),
            INDEX idx_series (series_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)
    conn.commit()
    c.close()
    conn.close()

#  7. ПОСЛЕДНЯЯ ДАТА В БД (для инкрементальной загрузки) 
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

#  8. ПОЛУЧЕНИЕ ДАННЫХ 
def _parse_time_period(raw_value) -> date | None:
    """
    IMF может отдавать TIME_PERIOD в форматах:
      - 2024-03-31
      - 2024-M01
      - 2024-Q1
      - 2024
    """
    if pd.isna(raw_value):
        return None

    value = str(raw_value).strip()
    if not value:
        return None

    monthly = re.match(r"^(\d{4})-M(\d{2})$", value)
    if monthly:
        y, m = monthly.groups()
        return date(int(y), int(m), 1)

    quarterly = re.match(r"^(\d{4})-Q([1-4])$", value)
    if quarterly:
        y, q = quarterly.groups()
        month = (int(q) - 1) * 3 + 1
        return date(int(y), month, 1)

    yearly = re.match(r"^(\d{4})$", value)
    if yearly:
        return date(int(value), 1, 1)

    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _contains_usd_eur(value) -> bool:
    if pd.isna(value):
        return False
    text = str(value).upper()
    return re.search(r"(^|[^A-Z0-9])(USD|EUR)([^A-Z0-9]|$)|US[_\s-]*DOLLAR|EURO", text) is not None


def fetch_data(config: dict, start_date: str = None) -> list:
    """
    Возвращает список кортежей: (date_iso, series_key, value)
    """
    try:
        base_url = "https://api.imf.org/external/sdmx/2.1/Data"
        dataflow_candidates = [config["dataflow"], *config.get("fallback_dataflows", [])]

        params = {}
        if start_date:
            params["startPeriod"] = start_date

        headers = {"Accept": "text/csv"}
        response = None

        for dataflow in dataflow_candidates:
            url = f"{base_url}/{dataflow}/{config.get('key', '...')}"
            print(f" Запрос: {url} | params={params}")

            # IMF API иногда отдает chunked-ошибки; ретраим с небольшим backoff
            for attempt in range(1, 4):
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=90)
                    break
                except requests.exceptions.RequestException as req_exc:
                    if attempt == 3:
                        print(f" Ошибка запроса ({dataflow}) после {attempt} попыток: {req_exc}")
                    else:
                        wait_s = attempt * 2
                        print(f" Сбой сети ({dataflow}), попытка {attempt}/3, повтор через {wait_s}с...")
                        time.sleep(wait_s)

            if response is None:
                continue

            if response.status_code == 403:
                print(f" HTTP 403 для dataflow={dataflow}, пробуем следующий candidate...")
                continue

            if response.status_code != 200:
                print(f" HTTP {response.status_code} ({dataflow}): {response.text[:500]}")
                continue

            break

        if response is None or response.status_code != 200:
            return []

        # Читаем CSV прямо в pandas
        df = pd.read_csv(io.StringIO(response.text), low_memory=False)

        if df.empty or 'TIME_PERIOD' not in df.columns or 'OBS_VALUE' not in df.columns:
            print("  Нет колонок TIME_PERIOD / OBS_VALUE")
            return []

        # Приводим данные
        df['date_iso'] = df['TIME_PERIOD'].apply(_parse_time_period)
        df['value'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
        df = df.dropna(subset=['date_iso', 'value'])

        # Оставляем только данные с частотой до 24 часов (daily)
        if config.get("require_daily_frequency", False):
            if "FREQUENCY" not in df.columns:
                print("  В ответе нет колонки FREQUENCY, daily-фильтр не может быть применен")
                return []
            before_daily = len(df)
            df = df[df["FREQUENCY"].astype(str).str.upper() == "D"]
            print(f"⏱ Фильтр daily (FREQUENCY=D): {before_daily} → {len(df)}")

        # Оставляем только ряды, относящиеся к USD/EUR
        filter_cols = config.get("usd_eur_filter_columns", [])
        existing_filter_cols = [c for c in filter_cols if c in df.columns]
        if not existing_filter_cols:
            print("  Не найдены колонки для USD/EUR фильтрации")
            return []

        usd_eur_mask = pd.Series(False, index=df.index)
        for col in existing_filter_cols:
            usd_eur_mask = usd_eur_mask | df[col].apply(_contains_usd_eur)

        before_fx = len(df)
        df = df[usd_eur_mask]
        print(f" Фильтр USD/EUR: {before_fx} → {len(df)}")

        # Формируем series_key из ключевых измерений; не включаем справочные текстовые поля
        ignored_cols = {
            'TIME_PERIOD', 'OBS_VALUE', 'date_iso', 'value',
            'FULL_DESCRIPTION', 'PUBLISHER', 'DEPARTMENT', 'CONTACT_POINT',
            'TOPIC', 'TOPIC_DATASET', 'KEYWORDS', 'KEYWORDS_DATASET',
            'LANGUAGE', 'PUBLICATION_DATE', 'UPDATE_DATE', 'METHODOLOGY',
            'METHODOLOGY_NOTES', 'ACCESS_SHARING_LEVEL', 'ACCESS_SHARING_NOTES',
            'SECURITY_CLASSIFICATION', 'SOURCE', 'SHORT_SOURCE_CITATION',
            'FULL_SOURCE_CITATION', 'LICENSE', 'SUGGESTED_CITATION',
            'KEY_INDICATOR', 'SERIES_NAME'
        }
        dim_cols = [c for c in df.columns if c not in ignored_cols]
        if dim_cols:
            def _mk_series_key(row):
                parts = []
                for v in row:
                    if pd.isna(v):
                        continue
                    s = str(v).strip()
                    if s:
                        parts.append(s)
                return ".".join(parts) if parts else config['dataflow']

            df['series_key'] = df[dim_cols].apply(_mk_series_key, axis=1)
        else:
            df['series_key'] = config['dataflow']

        # Опциональный фильтр по нужным сериям (например SDR/XDR)
        series_regex = config.get("series_regex")
        if series_regex:
            before = len(df)
            df = df[df['series_key'].str.contains(series_regex, case=False, na=False)]
            print(f" Фильтр series_regex='{series_regex}': {before} → {len(df)}")

        # Оставляем только нужное
        result = df[['date_iso', 'series_key', 'value']].dropna(subset=['date_iso', 'value']).values.tolist()
        print(f" Получено строк: {len(result)}")
        return result

    except Exception as e:
        print(f" Ошибка запроса: {e}")
        return []

#  9. ЗАПИСЬ В БД 
def save_rows(table_name: str, rows: list):
    if not rows:
        print("  Нет данных для записи")
        return

    sql = f"""
        INSERT IGNORE INTO `{table_name}` (date_iso, series_key, value)
        VALUES (%s, %s, %s)
    """
    chunk_size = 5000
    total_inserted = 0
    total_chunks = (len(rows) + chunk_size - 1) // chunk_size

    for idx in range(0, len(rows), chunk_size):
        chunk = rows[idx: idx + chunk_size]
        chunk_no = idx // chunk_size + 1
        saved = False

        for attempt in range(1, 4):
            conn = None
            c = None
            try:
                conn = mysql.connector.connect(**DB_CONFIG)
                c = conn.cursor()
                c.executemany(sql, chunk)
                conn.commit()
                total_inserted += c.rowcount
                saved = True
                break
            except mysql.connector.Error as db_exc:
                if attempt == 3:
                    print(f" Batch {chunk_no}/{total_chunks} не записан после 3 попыток: {db_exc}")
                else:
                    wait_s = attempt * 2
                    print(f" Ошибка БД на batch {chunk_no}/{total_chunks}: {db_exc}. Повтор через {wait_s}с...")
                    time.sleep(wait_s)
            finally:
                try:
                    if c:
                        c.close()
                    if conn and conn.is_connected():
                        conn.close()
                except Exception:
                    pass

        if not saved:
            # Продолжаем со следующими батчами, чтобы сохранить максимум возможного.
            continue

    print(f" Записано {total_inserted} новых строк из {len(rows)} total")

#  10. ОСНОВНАЯ ЛОГИКА 
def process(table_name: str):
    config = DATASETS[table_name]

    ensure_table(table_name)

    latest = get_latest_date(table_name)
    print(f" Последняя дата в БД: {latest or 'таблица пуста'}")

    # Если есть последняя дата — качаем только новые
    start_date = None
    if latest:
        start_date = (latest + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        start_date = config.get("default_start_date")
        if start_date:
            print(f" Первичная загрузка ограничена: startPeriod={start_date}")

    raw = fetch_data(config, start_date)
    if not raw:
        print("  Данных нет")
        return

    # raw уже содержит (date_iso, series_key, value)
    print(f" Новых строк: {len(raw)}")
    save_rows(table_name, raw)

#  11. ТОЧКА ВХОДА 
def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATASETS:
            print(f"  - {name}")
        sys.exit(1)

    print(f" IMF Parser")
    print(f"   База: {args.host}:{args.port}/{args.database}")
    print(f"   Таблица: {args.table_name} → {DATASETS[args.table_name]['description']}")
    print("=" * 80)

    process(args.table_name)

    print("=" * 80)
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