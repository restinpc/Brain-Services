import requests
import json
import os
import sys
import argparse
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text, MetaData, Table, select
from sqlalchemy.types import String, Text, Date, Float
from datetime import datetime, date
from dotenv import load_dotenv
import requests
import traceback
import os

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "BEA_Data_Loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# Загружаем .env по умолчанию (на случай, если аргументы не переданы)
load_dotenv()

# === Настройка аргументов командной строки ===
parser = argparse.ArgumentParser(description="Загрузчик данных BEA в MySQL")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("login", nargs="?", default=os.getenv("DB_USER"), help="Логин к БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль к БД")
parser.add_argument("db_name", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

# Приоритет: аргументы > .env
BEA_API_KEY = os.getenv("BEA_API_KEY")
BASE_API_URL = "https://apps.bea.gov/api/data"

# Используем аргументы, если они заданы
DB_HOST = args.host
DB_PORT = int(args.port)
DB_USER = args.login
DB_PASS = args.password
DB_NAME = args.db_name

if not all([DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME]):
    print("❌ Ошибка: Не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

DB_CONNECTION_STR = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Словарь настроек
DATASETS = {
    "Macro_USA_PCE_Inflation": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T20804", "Frequency": "M", "Year": "2023,2024,2025"},
        "LineFilter": "1",
        "Description": "US Personal Consumption Expenditures (PCE) Price Index"
    },
    "Macro_USA_GDP_Growth": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10101", "Frequency": "Q", "Year": "ALL"},
        "LineFilter": "1",
        "Description": "US Real Gross Domestic Product (GDP)"
    },
    "Macro_USA_Trade_Balance": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10105", "Frequency": "Q", "Year": "2020,2021,2022,2023,2024,2025"},
        "FilterFunc": lambda df: df[df['LineDescription'].str.contains("Net exports", case=False, na=False)],
        "Description": "US Net Exports of Goods and Services"
    }
}

def send_error_trace(exc: Exception, script_name: str = "bea_loader.py"):
    """Отправляет информацию об ошибке на удалённый сервер"""
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )

    payload = {
        "url": "cli_script",
        "node": NODE_NAME,
        "email": EMAIL,
        "logs": logs,
    }

    print(f"\n📤 [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"✅ [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")

def fetch_bea_data(dataset_key, config):
    print(f"🚀 Скачивание: {dataset_key}...")
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": config["Dataset"],
        "ResultFormat": "JSON"
    }
    params.update(config["Params"])

    try:
        response = requests.get(BASE_API_URL, params=params, timeout=30)
        if response.status_code != 200:
            print(f"   ⚠️ HTTP Error: {response.status_code}")
            return None
        data = response.json()
        if "Error" in data.get("BEAAPI", {}):
            err = data['BEAAPI']['Error']
            print(f"   ⚠️ Ошибка API BEA: {err.get('APIErrorDescription', err)}")
            return None
        results = data.get('BEAAPI', {}).get('Results', {})
        if 'Data' in results:
            raw_data = results['Data']
            print(f"   ✓ Получено строк: {len(raw_data)}")
            return raw_data
        else:
            print(f"   ⚠️ Данные пусты")
            return None
    except Exception as e:
        print(f"   ❌ Ошибка соединения: {e}")
        return None

def get_sqlalchemy_engine():
    return create_engine(DB_CONNECTION_STR, pool_recycle=3600)


def prepare_dataframe(df, config):
    """Подготовка DataFrame к загрузке"""
    if df.empty:
        return df

    # Создаем копию, чтобы избежать предупреждений
    df = df.copy()

    # 1. Фильтрация
    if "LineFilter" in config:
        df = df[df['LineNumber'] == config["LineFilter"]]
    elif "FilterFunc" in config:
        df = config["FilterFunc"](df)

    if df.empty:
        return df

    # 2. Очистка
    if 'DataValue' in df.columns:
        df['value_clean'] = df['DataValue'].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')

    def parse_date(row):
        tp = str(row.get('TimePeriod', ''))
        year = int(tp[:4])
        if 'Q' in tp:
            q = int(tp.split('Q')[1])
            return datetime(year, (q - 1) * 3 + 1, 1).date()
        elif 'M' in tp:
            m = int(tp.split('M')[1])
            return datetime(year, m, 1).date()
        return datetime(year, 1, 1).date()

    if 'TimePeriod' in df.columns:
        df['date_iso'] = df.apply(parse_date, axis=1)

    # 3. Подготовка колонок
    cols_to_keep = ['date_iso', 'value_clean', 'LineDescription', 'SeriesCode', 'TimePeriod']
    df_final = df[[c for c in cols_to_keep if c in df.columns]].copy()
    df_final['loaded_at'] = datetime.now()

    return df_final


def get_latest_date_from_db(table_name, engine):
    """Получает последнюю дату из таблицы в БД"""
    try:
        with engine.connect() as conn:
            # Проверяем, существует ли таблица
            result = conn.execute(
                text(f"SHOW TABLES LIKE '{table_name}'")
            )
            if not result.fetchone():
                return None

            # Получаем максимальную дату
            result = conn.execute(
                text(f"SELECT MAX(date_iso) as latest_date FROM `{table_name}`")
            )
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print(f"   ⚠️ Ошибка при получении даты из БД: {e}")
        return None


def process_and_load_incremental(dataset_key, config):
    """Загрузка только новых данных (инкрементально)"""
    print(f"\n📊 Обработка: {dataset_key}")

    # Получаем свежие данные
    raw_data = fetch_bea_data(dataset_key, config)
    if not raw_data:
        print(f"   ⚠️ Не удалось получить данные")
        return

    # Подготавливаем DataFrame
    df = pd.DataFrame(raw_data)
    df_new = prepare_dataframe(df, config)

    if df_new.empty:
        print(f"   ⚠️ Нет данных после фильтрации")
        return

    table_name = dataset_key.lower()
    engine = get_sqlalchemy_engine()

    try:
        # Получаем последнюю дату из БД
        latest_date_in_db = get_latest_date_from_db(table_name, engine)

        if latest_date_in_db:
            print(f"   📅 Последняя дата в БД: {latest_date_in_db}")

            # Фильтруем только данные новее последней даты
            df_to_load = df_new[df_new['date_iso'] > latest_date_in_db].copy()

            if df_to_load.empty:
                print(f"   ✅ Новых данных нет (все данные актуальны)")
                return
            else:
                print(f"   🔄 Найдено {len(df_to_load)} новых строк для загрузки")
                # Загружаем только новые строки
                df_to_load.to_sql(
                    table_name,
                    engine,
                    if_exists='append',
                    index=False,
                    dtype={
                        'LineDescription': Text(),
                        'SeriesCode': String(50),
                        'value_clean': Float(),
                        'date_iso': Date()
                    }
                )
                print(f"   ✅ Загружено новых строк: {len(df_to_load)}")
        else:
            # Таблица не существует или пуста - создаем с нуля
            print(f"   📝 Таблица не существует, создаем новую")
            df_new.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                dtype={
                    'LineDescription': Text(),
                    'SeriesCode': String(50),
                    'value_clean': Float(),
                    'date_iso': Date()
                }
            )
            print(f"   ✅ Создана новая таблица с {len(df_new)} строками")

        # Добавляем/обновляем комментарий к таблице
        with engine.connect() as conn:
            safe_comment = config.get('Description', '').replace("'", "''")
            sql_comment = text(f"ALTER TABLE `{table_name}` COMMENT = '{safe_comment}'")
            conn.execute(sql_comment)
            conn.commit()

    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


def main():
    print(f"База данных: {DB_HOST}:{DB_PORT}")
    if not BEA_API_KEY:
        print("❌ Ошибка: Не указан BEA_API_KEY")
        return

    print("\n=== ЗАГРУЗКА ДАННЫХ В SQL (ИНКРЕМЕНТАЛЬНАЯ) ===")

    for key, config in DATASETS.items():
        process_and_load_incremental(key, config)

    print("\n🏁 ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        # sys.exit() вызывает SystemExit — не считаем это ошибкой
        pass
    except KeyboardInterrupt:
        print("\n🛑 Скрипт прерван пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка при выполнении скрипта: {e!r}")
        send_error_trace(e)
        sys.exit(1)