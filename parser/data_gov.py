#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import time
import requests
import xml.etree.ElementTree as ET
import mysql.connector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, date
import traceback
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "data_gov_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "data_gov.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}
    try:
        requests.post(TRACE_URL, data=payload, timeout=10)
    except:
        pass


parser = argparse.ArgumentParser(description="Загрузчик данных Treasury.gov в MySQL (ТОЛЬКО одна таблица)")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database,
}

BASE_TREASURY_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"

# ЕДИНСТВЕННЫЙ источник конфигурации — строго для одной таблицы
DATA_TYPES_CONFIG = {
    "vlad_treasury_nominal_yield": {
        "code": "daily_treasury_yield_curve",
        "start_year": 1990,
        "description": "Номинальная кривая доходности"
    },
    "vlad_treasury_real_yield": {
        "code": "daily_treasury_real_yield_curve",
        "start_year": 2003,
        "description": "Реальная доходность"
    }
}


def clean_tag(tag):
    return tag.split('}')[-1] if '}' in tag else tag


def get_latest_date_from_db(table_name: str) -> date | None:
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            return None
        cursor.execute(f"SELECT MAX(record_date) FROM `{table_name}`")
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except Exception as e:
        print(f" Ошибка БД при получении даты: {e}")
        return None
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


def download_and_parse_xml(data_type_code: str, year: int):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    headers = {'User-Agent': 'Mozilla/5.0'}
    params = {'data': data_type_code, 'field_tdr_date_value': year}

    try:
        response = session.get(BASE_TREASURY_URL, params=params, headers=headers, timeout=30)
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            print(f" HTTP {response.status_code} для {year}")
            return []
        root = ET.fromstring(response.text)
    except Exception as e:
        print(f" Ошибка XML за {year}: {e}")
        return []

    ns = {'atom': 'http://www.w3.org/2005/Atom', 'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'}
    entries = root.findall('atom:entry', ns)
    data_rows = []

    for entry in entries:
        content = entry.find('atom:content', ns)
        properties = content.find('m:properties', ns)
        if properties is None:
            continue
        row_data = {}
        for prop in properties:
            col_name = clean_tag(prop.tag)
            col_value = prop.text
            if col_name == 'Id':
                continue
            if col_name == 'NEW_DATE' and col_value:
                row_data['record_date'] = col_value.split('T')[0]
            else:
                row_data[col_name] = float(col_value) if col_value and col_value.strip() else None
        if 'record_date' in row_data:
            data_rows.append(row_data)
    return data_rows


def save_to_db_incremental(data, table_name, table_comment=""):
    if not data:
        return
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sample_row = data[0]
        columns_in_file = [k for k in sample_row.keys() if k != 'record_date']
        columns_def = ["`record_date` DATE NOT NULL PRIMARY KEY COMMENT 'Дата публикации'"]
        for key in sorted(columns_in_file):
            columns_def.append(f"`{key}` FLOAT NULL")
        sql_create = (
            f"CREATE TABLE IF NOT EXISTS `{table_name}` "
            f"({', '.join(columns_def)}) "
            f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
            f"COMMENT='{table_comment}';"
        )
        cursor.execute(sql_create)
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        db_columns = [row[0] for row in cursor.fetchall()]
        valid_keys = [col for col in db_columns if col != 'record_date']
        cols_str = ", ".join([f"`{c}`" for c in ['record_date'] + valid_keys])
        placeholders = ", ".join(["%s"] * (len(valid_keys) + 1))
        sql = f"INSERT IGNORE INTO `{table_name}` ({cols_str}) VALUES ({placeholders})"
        values = [[row.get('record_date')] + [row.get(k) for k in valid_keys] for row in data]
        cursor.executemany(sql, values)
        conn.commit()
        print(f" DB: Вставлено {cursor.rowcount} новых строк в {table_name}")
    except mysql.connector.Error as err:
        print(f" DB Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


def process_data_type(table_name: str, config: dict, current_year: int):
    latest_date = get_latest_date_from_db(table_name)
    start_year = config['start_year']
    effective_start = max(start_year, latest_date.year - 1) if latest_date else start_year
    print(f" Диапазон загрузки: {effective_start} – {current_year}")
    all_data = []
    for year in range(effective_start, current_year + 1):
        print(f" Запрос данных за {year}...")
        parsed = download_and_parse_xml(config['code'], year)
        if parsed:
            all_data.extend(parsed)
        time.sleep(0.5)
    if all_data and latest_date:
        filtered = [r for r in all_data if datetime.strptime(r['record_date'], "%Y-%m-%d").date() > latest_date]
        print(f" Отфильтровано: {len(filtered)} новых записей из {len(all_data)}")
        all_data = filtered
    if all_data:
        save_to_db_incremental(all_data, table_name, config['description'])
    else:
        print(" Новых данных нет")


def main():
    #  КРИТИЧЕСКАЯ ПРОВЕРКА: только одна таблица, никаких циклов!
    if args.table_name not in DATA_TYPES_CONFIG:
        print(f" Ошибка: неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATA_TYPES_CONFIG.keys():
            print(f"  - {name}")
        sys.exit(1)

    print(f" TREASURY.GOV COLLECTOR (ТОЛЬКО: {args.table_name})")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" ЦЕЛЕВАЯ ТАБЛИЦА: {args.table_name}")
    print("=" * 60)

    config = DATA_TYPES_CONFIG[args.table_name]
    current_year = datetime.now().year
    process_data_type(args.table_name, config, current_year)
    print("=" * 60)
    print(" ЗАГРУЗКА ЗАВЕРШЕНА")


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