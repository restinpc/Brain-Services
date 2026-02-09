import requests
import json
import os
import time
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
# ИМПОРТИРУЕМ ПРАВИЛЬНЫЕ ТИПЫ
from sqlalchemy.types import String, Text, Date, Float
from datetime import datetime
from dotenv import load_dotenv

# ========== КОНФИГ ==========
load_dotenv()

BEA_API_KEY = os.getenv("BEA_API_KEY")
BASE_API_URL = "https://apps.bea.gov/api/data"
OUTPUT_DIR = "bea_datasets_json"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

DB_CONNECTION_STR = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Словарь настроек
DATASETS = {
    "Macro_USA_PCE_Inflation": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T20804", "Frequency": "M", "Year": "2023,2024,2025"},
        "LineFilter": "1",
        "Description": "US Personal Consumption Expenditures (PCE) Price Index. The primary inflation gauge used by the Federal Reserve."
    },
    "Macro_USA_GDP_Growth": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10101", "Frequency": "Q", "Year": "ALL"},
        "LineFilter": "1",
        "Description": "US Real Gross Domestic Product (GDP) - Percent Change From Preceding Period. Main indicator of economic health."
    },
    "Macro_USA_Trade_Balance": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10105", "Frequency": "Q", "Year": "2020,2021,2022,2023,2024,2025"},
        "FilterFunc": lambda df: df[df['LineDescription'].str.contains("Net exports", case=False, na=False)],
        "Description": "US Net Exports of Goods and Services (Trade Balance). Negative value indicates a trade deficit."
    }
}


# ========== ФУНКЦИИ СКАЧИВАНИЯ (Без изменений) ==========

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


def save_json(name, data, description):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    filepath = os.path.join(OUTPUT_DIR, f"{name}.json")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({"meta": {"description": description, "source": "BEA.gov"}, "data": data}, f, ensure_ascii=False,
                  indent=2)
    print(f"   💾 Сохранено в JSON")


# ========== ИСПРАВЛЕННАЯ ЗАГРУЗКА В SQL ==========

def get_sqlalchemy_engine():
    return create_engine(DB_CONNECTION_STR, pool_recycle=3600)


def process_and_load(dataset_key, config):
    filepath = os.path.join(OUTPUT_DIR, f"{dataset_key}.json")
    if not os.path.exists(filepath):
        print(f"⚠️ Файл {filepath} не найден, пропускаем.")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        content = json.load(f)
        raw_data = content.get('data', [])
        meta_desc = content.get('meta', {}).get('description', '')

    if not raw_data: return

    df = pd.DataFrame(raw_data)

    # 1. Фильтрация
    if "LineFilter" in config:
        df = df[df['LineNumber'] == config["LineFilter"]]
    elif "FilterFunc" in config:
        df = config["FilterFunc"](df)

    if df.empty:
        print(f"   ⚠️ Данных нет после фильтрации")
        return

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

    table_name = dataset_key.lower()
    engine = get_sqlalchemy_engine()

    try:
        # === ИСПРАВЛЕНИЕ ЗДЕСЬ ===
        # Используем классы типов (String, Text, Date), а не text()
        df_final.to_sql(table_name, engine, if_exists='replace', index=False, dtype={
            'LineDescription': Text(),  # <--- Правильный тип
            'SeriesCode': String(50),  # <--- Правильный тип
            'value_clean': Float(),
            'date_iso': Date()
        })

        # Добавляем комментарий к таблице
        safe_comment = meta_desc.replace("'", "''")
        with engine.connect() as conn:
            sql_comment = text(f"ALTER TABLE `{table_name}` COMMENT = '{safe_comment}'")
            conn.execute(sql_comment)
            conn.commit()

        print(f"   ✅ Загружено в БД: `{table_name}` ({len(df_final)} строк)")
        print(f"      📝 Комментарий: {meta_desc[:50]}...")

    except Exception as e:
        print(f"   ❌ Ошибка SQL: {e}")


# ========== MAIN ==========

def main():
    print(f"База данных: {DB_HOST}:{DB_PORT}")
    if not BEA_API_KEY:
        print("❌ Ошибка: Не указан BEA_API_KEY")
        return

    # Скачивание (файлы уже есть - пропустит или перезапишет быстро)
    print("\n=== ЭТАП 1: СКАЧИВАНИЕ ===")
    for key, config in DATASETS.items():
        data = fetch_bea_data(key, config)
        if data: save_json(key, data, config["Description"])
        time.sleep(0.1)

    # Загрузка
    print("\n=== ЭТАП 2: ЗАГРУЗКА В SQL ===")
    for key, config in DATASETS.items():
        process_and_load(key, config)

    print("\n🏁 ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ")


if __name__ == "__main__":
    main()
