#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BEA collector → MySQL.

Запуск:
  python BEA.py <table_name> [host] [port] [user] [password] [database]

Главное исправление:
  - данные BEA могут пересматриваться за уже существующий квартал;
  - поэтому нельзя грузить только date_iso > MAX(date_iso);
  - теперь скрипт вставляет новые строки и обновляет изменившиеся значения за те же даты.
"""

import os
import sys
import argparse
import requests
import pandas as pd
from sqlalchemy import create_engine, text, String, Float, Date, Text, DateTime
from datetime import datetime
import traceback
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "bea_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "BEA.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}
    try:
        requests.post(TRACE_URL, data=payload, timeout=10)
    except Exception:
        pass


parser = argparse.ArgumentParser(description="Загрузка данных BEA API в SQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
parser.add_argument(
    "--replace-all",
    action="store_true",
    help="Полностью пересоздать таблицу текущими данными BEA вместо upsert",
)
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

BEA_API_KEY = os.getenv("BEA_API_KEY")
BASE_API_URL = "https://apps.bea.gov/api/data"
DB_CONNECTION_STR = (
    f"mysql+mysqlconnector://{args.user}:{args.password}@"
    f"{args.host}:{args.port}/{args.database}"
)

# Конфигурации BEA.
# Важно: vlad_macro_usa_trade_balance теперь берётся из Table 1.1.5 / T10105,
# где Net exports of goods and services дан как уровень в млрд долларов SAAR.
DATASETS = {
    "vlad_macro_usa_pce_inflation": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T20804", "Frequency": "M", "Year": "ALL"},
        "LineFilter": "1",
        "Description": "US Personal Consumption Expenditures (PCE) Price Index",
    },
    "vlad_macro_usa_gdp_growth": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10101", "Frequency": "Q", "Year": "ALL"},
        "LineFilter": "1",
        "Description": "US Real Gross Domestic Product (GDP), percent change",
    },
    "vlad_macro_usa_trade_balance": {
        "Dataset": "NIPA",
        "Params": {"TableName": "T10105", "Frequency": "Q", "Year": "ALL"},
        "LineFilter": "14",
        "Description": "US Net Exports of Goods and Services, Table 1.1.5",
    },
}


def fetch_bea_data(config):
    print(" Скачивание данных из BEA API...")
    params = {
        "UserID": BEA_API_KEY,
        "method": "GetData",
        "datasetname": config["Dataset"],
        "ResultFormat": "JSON",
    }
    params.update(config["Params"])

    safe_params = dict(params)
    if safe_params.get("UserID"):
        safe_params["UserID"] = "***"
    print(f" Параметры запроса: {safe_params}")

    try:
        response = requests.get(BASE_API_URL, params=params, timeout=45)
        print(f" HTTP статус: {response.status_code}")

        if response.status_code != 200:
            print(f" HTTP Error: {response.status_code}")
            print(f" Ответ: {response.text[:500]}")
            return None

        data = response.json()
        if "Error" in data.get("BEAAPI", {}):
            err = data["BEAAPI"]["Error"]
            error_msg = err.get("APIErrorDescription", str(err))
            print(f" Ошибка API BEA: {error_msg}")
            return None

        results = data.get("BEAAPI", {}).get("Results", {})
        raw_data = results.get("Data")
        if raw_data:
            print(f" Получено строк: {len(raw_data)}")
            return raw_data

        print(" Данные пусты")
        print(f" Results: {results}")
        return None
    except Exception as e:
        print(f" Ошибка соединения: {e}")
        return None


def parse_bea_date(time_period):
    tp = str(time_period or "").strip()
    if not tp or tp.lower() == "nan":
        return None
    year = int(tp[:4])
    if "Q" in tp:
        q = int(tp.split("Q", 1)[1])
        return datetime(year, (q - 1) * 3 + 1, 1).date()
    if "M" in tp:
        m = int(tp.split("M", 1)[1])
        return datetime(year, m, 1).date()
    return datetime(year, 1, 1).date()


def clean_number(series):
    return (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(" ", "", regex=False)
        .replace({"--": None, "...": None, "nan": None, "None": None})
        .apply(pd.to_numeric, errors="coerce")
    )


def prepare_dataframe(df, config):
    if df.empty:
        return df

    df = df.copy()

    if "LineFilter" in config:
        before = len(df)
        df = df[df["LineNumber"].astype(str) == str(config["LineFilter"])].copy()
        print(f" После LineFilter={config['LineFilter']}: {len(df)} строк (было {before})")
    elif "FilterFunc" in config:
        before = len(df)
        df = config["FilterFunc"](df).copy()
        print(f" После текстового фильтра: {len(df)} строк (было {before})")

    if df.empty:
        print(" Нет данных после фильтрации")
        return df

    df["value_clean"] = clean_number(df["DataValue"])
    df["date_iso"] = df["TimePeriod"].apply(parse_bea_date)
    df["loaded_at"] = datetime.now()

    cols_to_keep = ["date_iso", "value_clean", "LineDescription", "SeriesCode", "TimePeriod", "loaded_at"]
    df_final = df[[c for c in cols_to_keep if c in df.columns]].copy()
    df_final = df_final.dropna(subset=["date_iso", "value_clean"])

    # Нормализуем служебные поля.
    if "SeriesCode" not in df_final.columns:
        df_final["SeriesCode"] = ""
    df_final["SeriesCode"] = df_final["SeriesCode"].fillna("").astype(str).str[:50]
    df_final["LineDescription"] = df_final["LineDescription"].fillna("").astype(str)
    df_final["TimePeriod"] = df_final["TimePeriod"].fillna("").astype(str).str[:20]

    print(f" Финальный DataFrame: {len(df_final)} строк")
    print(f" Диапазон дат: {df_final['date_iso'].min()} .. {df_final['date_iso'].max()}")
    print(f" Мин/Макс: {df_final['value_clean'].min():.3f} .. {df_final['value_clean'].max():.3f}")
    return df_final


def table_exists(engine, table_name):
    with engine.connect() as conn:
        result = conn.execute(text("SHOW TABLES LIKE :table_name"), {"table_name": table_name})
        return result.fetchone() is not None


def ensure_table_exists(table_name, engine, config):
    if not table_exists(engine, table_name):
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE `{table_name}` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date_iso DATE NOT NULL,
                    value_clean DOUBLE,
                    LineDescription TEXT,
                    SeriesCode VARCHAR(50),
                    TimePeriod VARCHAR(20),
                    loaded_at DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_date_iso (date_iso),
                    INDEX idx_series (SeriesCode)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            safe_comment = config.get("Description", "").replace("'", "''")
            conn.execute(text(f"ALTER TABLE `{table_name}` COMMENT = '{safe_comment}'"))
        print(f" Таблица '{table_name}' создана")
        return

    # Добавляем недостающие колонки для старых таблиц.
    required_columns = {
        "date_iso": "DATE",
        "value_clean": "DOUBLE",
        "LineDescription": "TEXT",
        "SeriesCode": "VARCHAR(50)",
        "TimePeriod": "VARCHAR(20)",
        "loaded_at": "DATETIME",
    }
    with engine.begin() as conn:
        existing_cols = {
            row[0]
            for row in conn.execute(text(f"SHOW COLUMNS FROM `{table_name}`")).fetchall()
        }
        for col, col_type in required_columns.items():
            if col not in existing_cols:
                conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {col_type}"))
                print(f" Добавлена колонка `{col}`")
        safe_comment = config.get("Description", "").replace("'", "''")
        conn.execute(text(f"ALTER TABLE `{table_name}` COMMENT = '{safe_comment}'"))


def get_latest_date_from_db(table_name, engine):
    if not table_exists(engine, table_name):
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT MAX(date_iso) FROM `{table_name}`")).fetchone()
            return row[0] if row and row[0] else None
    except Exception:
        return None


def load_with_revision_updates(table_name, engine, df_new):
    """
    Вставляет новые даты и обновляет строки, если BEA пересмотрела значение
    за уже существующий квартал/месяц.
    """
    if df_new.empty:
        print(" Нет данных для загрузки")
        return 0, 0, 0

    df_new = df_new.copy()
    df_new["date_iso"] = pd.to_datetime(df_new["date_iso"]).dt.date
    df_new["loaded_at"] = datetime.now()

    with engine.begin() as conn:
        existing_rows = conn.execute(text(f"""
            SELECT date_iso, COALESCE(SeriesCode, '') AS SeriesCode, value_clean
            FROM `{table_name}`
        """)).fetchall()

        existing = {}
        for row in existing_rows:
            existing[(row[0], row[1] or "")] = float(row[2]) if row[2] is not None else None

        rows_to_insert = []
        rows_to_update = []
        unchanged = 0

        for _, row in df_new.iterrows():
            key = (row["date_iso"], row.get("SeriesCode", "") or "")
            new_value = float(row["value_clean"])
            old_value = existing.get(key)

            payload = {
                "date_iso": row["date_iso"],
                "value_clean": new_value,
                "LineDescription": str(row.get("LineDescription", "")),
                "SeriesCode": str(row.get("SeriesCode", ""))[:50],
                "TimePeriod": str(row.get("TimePeriod", ""))[:20],
                "loaded_at": row["loaded_at"].to_pydatetime() if hasattr(row["loaded_at"], "to_pydatetime") else row["loaded_at"],
            }

            if key not in existing:
                rows_to_insert.append(payload)
            elif old_value is None or abs(old_value - new_value) > 1e-9:
                rows_to_update.append(payload)
            else:
                unchanged += 1

        delete_sql = text(f"""
            DELETE FROM `{table_name}`
            WHERE date_iso = :date_iso AND COALESCE(SeriesCode, '') = :SeriesCode
        """)
        insert_sql = text(f"""
            INSERT INTO `{table_name}`
                (date_iso, value_clean, LineDescription, SeriesCode, TimePeriod, loaded_at)
            VALUES
                (:date_iso, :value_clean, :LineDescription, :SeriesCode, :TimePeriod, :loaded_at)
        """)

        for payload in rows_to_update:
            conn.execute(delete_sql, payload)
        if rows_to_update:
            conn.execute(insert_sql, rows_to_update)
        if rows_to_insert:
            conn.execute(insert_sql, rows_to_insert)

    return len(rows_to_insert), len(rows_to_update), unchanged


def replace_all(table_name, engine, df_new, config):
    df_new.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "LineDescription": Text(500),
            "SeriesCode": String(50),
            "value_clean": Float(),
            "date_iso": Date(),
            "TimePeriod": String(20),
            "loaded_at": DateTime(),
        },
        method="multi",
        chunksize=1000,
    )
    with engine.begin() as conn:
        safe_comment = config.get("Description", "").replace("'", "''")
        conn.execute(text(f"ALTER TABLE `{table_name}` COMMENT = '{safe_comment}'"))
    print(f" Таблица полностью обновлена: {len(df_new)} строк")


def process_and_load(table_name, config):
    print(f"\n Обработка ТОЛЬКО таблицы: {table_name}")
    raw_data = fetch_bea_data(config)
    if not raw_data:
        print(" Не удалось получить данные")
        return

    df = pd.DataFrame(raw_data)
    df_new = prepare_dataframe(df, config)

    if df_new.empty:
        print(" Нет данных после обработки")
        return

    engine = create_engine(DB_CONNECTION_STR, pool_recycle=3600)
    try:
        latest_date_in_db = get_latest_date_from_db(table_name, engine)
        if latest_date_in_db:
            print(f" Последняя дата в БД: {latest_date_in_db}")
        else:
            print(" Таблица пустая или отсутствует")

        if args.replace_all:
            replace_all(table_name, engine, df_new, config)
            return

        ensure_table_exists(table_name, engine, config)
        inserted, updated, unchanged = load_with_revision_updates(table_name, engine, df_new)

        print(f" Новые строки: {inserted}")
        print(f" Обновлено ревизий BEA: {updated}")
        print(f" Без изменений: {unchanged}")

        if inserted == 0 and updated == 0:
            print(" Новых дат и ревизий нет")
        else:
            print(f" Загружено/обновлено строк: {inserted + updated}")
    except Exception as e:
        print(f" Ошибка записи: {e}")
        traceback.print_exc()
    finally:
        engine.dispose()


def main():
    if args.table_name not in DATASETS:
        print(f" Ошибка: неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATASETS.keys():
            print(f"  - {name}")
        sys.exit(1)

    if not BEA_API_KEY:
        print(" Ошибка: не указан BEA_API_KEY в .env")
        sys.exit(1)

    print(f" BEA COLLECTOR (ТОЛЬКО: {args.table_name})")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" ЦЕЛЕВАЯ ТАБЛИЦА: {args.table_name}")
    print("=" * 60)

    config = DATASETS[args.table_name]
    process_and_load(args.table_name, config)

    print("=" * 60)
    print(" ЗАГРУЗКА ЗАВЕРШЕНА (только одна таблица обработана)")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
