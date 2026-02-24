#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import requests
import json
import pandas as pd
from sqlalchemy import create_engine, text, String, Float, Date, Text
from datetime import datetime, date
import traceback
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "bea_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "BEA.py"):
    logs = f"Node: {NODE_NAME}\\nScript: {script_name}\\nException: {repr(exc)}\\n\\nTraceback:\\n{traceback.format_exc()}"
    payload = {"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}
    try:
        requests.post(TRACE_URL, data=payload, timeout=10)
    except:
        pass


parser = argparse.ArgumentParser(description="Загрузка данных BEA API в SQL (ТОЛЬКО одна таблица)")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

BEA_API_KEY = os.getenv("BEA_API_KEY")
BASE_API_URL = "https://apps.bea.gov/api/data"
DB_CONNECTION_STR = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"

# ✅ ИСПРАВЛЕННЫЕ конфигурации
DATASETS = {
    "vlad_macro_usa_pce_inflation": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T20804", "Frequency": "M", "Year": "2023,2024,2025"},
        "LineFilter": "1",
        "Description": "US Personal Consumption Expenditures (PCE) Price Index"
    },
    "vlad_macro_usa_gdp_growth": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10101", "Frequency": "Q", "Year": "ALL"},
        "LineFilter": "1",
        "Description": "US Real Gross Domestic Product (GDP)"
    },
    "vlad_macro_usa_trade_balance": {
        "Dataset": "NIPA",  # ✅ Используем NIPA, а не ITA
        "Params": {"TableName": "T10101", "Frequency": "Q", "Year": "ALL"},  # ✅ Правильная таблица GDP с Net Exports
        "FilterFunc": lambda df: df[df['LineDescription'].str.contains("Net exports", case=False, na=False)],
        "Description": "US Net Exports of Goods and Services (из T10101)"
    }
}


def fetch_bea_data(config):
    print(f"🚀 Скачивание данных из BEA API...")
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": config["Dataset"],
        "ResultFormat": "JSON"
    }
    params.update(config["Params"])

    # 🔍 DEBUG: показываем точные параметры
    print(f"📋 Параметры запроса: {params}")

    try:
        response = requests.get(BASE_API_URL, params=params, timeout=30)
        print(f"📡 HTTP статус: {response.status_code}")

        if response.status_code != 200:
            print(f"⚠️ HTTP Error: {response.status_code}")
            print(f"🔍 Ответ: {response.text[:500]}")
            return None

        data = response.json()
        if "Error" in data.get("BEAAPI", {}):
            err = data['BEAAPI']['Error']
            error_msg = err.get('APIErrorDescription', str(err))
            print(f"⚠️ Ошибка API BEA: {error_msg}")
            return None

        results = data.get('BEAAPI', {}).get('Results', {})
        if 'Data' in results:
            raw_data = results['Data']
            print(f"✓ Получено строк: {len(raw_data)}")
            return raw_data
        else:
            print("⚠️ Данные пусты")
            print(f"🔍 Results: {results}")
            return None
    except Exception as e:
        print(f"❌ Ошибка соединения: {e}")
        return None


def prepare_dataframe(df, config):
    if df.empty:
        return df

    df = df.copy()

    # ✅ Применяем фильтр
    if "LineFilter" in config:
        df = df[df['LineNumber'] == config["LineFilter"]]
        print(f"🔍 После LineFilter: {len(df)} строк")
    elif "FilterFunc" in config:
        before = len(df)
        df = config["FilterFunc"](df)
        print(f"🔍 После текстового фильтра: {len(df)} строк (было {before})")

    if df.empty:
        print("⚠️ Нет данных после фильтрации")
        return df

    # ✅ Очистка значений
    if 'DataValue' in df.columns:
        df['value_clean'] = (df['DataValue'].astype(str)
                             .str.replace(',', '')
                             .str.replace('$', '')
                             .apply(pd.to_numeric, errors='coerce'))
        print(f"📊 Диапазон value_clean: {df['value_clean'].min():.0f} .. {df['value_clean'].max():.0f}")

    def parse_date(row):
        tp = str(row.get('TimePeriod', ''))
        if not tp:
            return None
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
        print(f"📅 Диапазон дат: {df['date_iso'].min()} .. {df['date_iso'].max()}")

    # ✅ Сохраняем нужные колонки
    cols_to_keep = ['date_iso', 'value_clean', 'LineDescription', 'SeriesCode', 'TimePeriod']
    df_final = df[[c for c in cols_to_keep if c in df.columns]].copy()
    df_final = df_final.dropna(subset=['date_iso', 'value_clean'])
    df_final['loaded_at'] = datetime.now()

    print(f"✅ Финальный DataFrame: {len(df_final)} строк")
    return df_final


def get_latest_date_from_db(table_name, engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
            if not result.fetchone():
                return None
            result = conn.execute(text(f"SELECT MAX(date_iso) FROM `{table_name}`"))
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except:
        return None


def process_and_load_incremental(table_name, config):
    print(f"\n📊 Обработка ТОЛЬКО таблицы: {table_name}")
    raw_data = fetch_bea_data(config)
    if not raw_data:
        print("⚠️ Не удалось получить данные")
        return

    df = pd.DataFrame(raw_data)

    # 🔥 СПЕЦИАЛЬНАЯ ЛОГИКА для trade_balance
    if table_name == "vlad_macro_usa_trade_balance":
        print("🔄 Вычисляем Net Exports = Exports - Imports...")

        def parse_date(tp):
            tp = str(tp)
            if not tp or pd.isna(tp):
                return None
            year = int(tp[:4])
            if 'Q' in tp:
                q = int(tp.split('Q')[1])
                return datetime(year, (q - 1) * 3 + 1, 1).date()
            return datetime(year, 1, 1).date()

        # Находим Exports и Imports
        exports = df[df['LineDescription'].str.contains('exports', case=False, na=False)].copy()
        imports_ = df[df['LineDescription'].str.contains('imports', case=False, na=False)].copy()

        print(f"📊 Exports строк: {len(exports)}, Imports строк: {len(imports_)}")

        if exports.empty or imports_.empty:
            print("⚠️ Не найдены exports/imports")
            return

        # Парсим DataValue
        exports['value_clean'] = (exports['DataValue'].astype(str)
                                  .str.replace(',', '').str.replace('$', '')
                                  .apply(pd.to_numeric, errors='coerce'))
        imports_['value_clean'] = (imports_['DataValue'].astype(str)
                                   .str.replace(',', '').str.replace('$', '')
                                   .apply(pd.to_numeric, errors='coerce'))

        # Объединяем по TimePeriod
        merged = pd.merge(
            exports[['TimePeriod', 'value_clean']].rename(columns={'value_clean': 'exp'}),
            imports_[['TimePeriod', 'value_clean']].rename(columns={'value_clean': 'imp'}),
            on='TimePeriod', how='inner'
        )

        # Net Exports = Exports - Imports
        merged['value_clean'] = merged['exp'] - merged['imp']
        merged['LineDescription'] = 'Net exports of goods and services'
        merged['SeriesCode'] = 'A191RC0'
        merged['date_iso'] = merged['TimePeriod'].apply(parse_date)
        merged['loaded_at'] = datetime.now()

        # Финальные колонки
        df_new = merged[['date_iso', 'value_clean', 'LineDescription', 'SeriesCode', 'TimePeriod']].copy()
        df_new = df_new.dropna(subset=['date_iso', 'value_clean'])

        print(f"✅ Trade Balance: {len(df_new)} кварталов")
        print(f"📊 Диапазон: {df_new['date_iso'].min()} .. {df_new['date_iso'].max()}")
        print(f"💰 Мин/Макс: {df_new['value_clean'].min():.0f} .. {df_new['value_clean'].max():.0f}")

    else:
        # Обычная логика для PCE/GDP
        df_new = prepare_dataframe(df, config)

    if df_new.empty:
        print("⚠️ Нет данных после обработки")
        return

    # Загрузка в БД (без изменений)
    engine = create_engine(DB_CONNECTION_STR, pool_recycle=3600)
    latest_date_in_db = get_latest_date_from_db(table_name, engine)

    if latest_date_in_db:
        print(f"📅 Последняя дата в БД: {latest_date_in_db}")
        df_to_load = df_new[df_new['date_iso'] > latest_date_in_db].copy()
        if df_to_load.empty:
            print("✅ Новых данных нет")
            engine.dispose()
            return
        print(f"🔄 Найдено {len(df_to_load)} новых строк")
    else:
        print("📝 Таблица не существует — создаём новую")
        df_to_load = df_new

    try:
        df_to_load.to_sql(
            table_name, engine, if_exists='append' if latest_date_in_db else 'replace',
            index=False,
            dtype={'LineDescription': Text(500), 'SeriesCode': String(50), 'value_clean': Float(), 'date_iso': Date()},
            method='multi'
        )
        with engine.connect() as conn:
            safe_comment = config.get('Description', '').replace("'", "''")
            conn.execute(text(f"ALTER TABLE `{table_name}` COMMENT = '{safe_comment}'"))
            conn.commit()
        print(f"✅ Загружено {len(df_to_load)} строк в '{table_name}'")
    except Exception as e:
        print(f"❌ Ошибка записи: {e}")
        traceback.print_exc()
    finally:
        engine.dispose()


def main():
    if args.table_name not in DATASETS:
        print(f"❌ Ошибка: неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATASETS.keys():
            print(f"  - {name}")
        sys.exit(1)

    if not BEA_API_KEY:
        print("❌ Ошибка: не указан BEA_API_KEY в .env")
        sys.exit(1)

    print(f"🚀 BEA COLLECTOR (ТОЛЬКО: {args.table_name})")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 ЦЕЛЕВАЯ ТАБЛИЦА: {args.table_name}")
    print("=" * 60)

    config = DATASETS[args.table_name]
    process_and_load_incremental(args.table_name, config)

    print("=" * 60)
    print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА (только одна таблица обработана)")


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
