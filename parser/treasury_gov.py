#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import requests
import json
import time
from urllib.parse import urljoin
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import traceback
from dotenv import load_dotenv

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "treasurygov_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception, script_name: str = "treasury_gov.py"):
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
        import requests as req
        response = req.post(TRACE_URL, data=payload, timeout=10)
        print(f"✅ [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")

# === Аргументы командной строки + .env fallback ===
parser = argparse.ArgumentParser(description="U.S. Treasury Fiscal Data API → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database,
}

BASE_API_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"

# === ТОЧНЫЙ СПИСОК ЭНДПОИНТОВ И ТАБЛИЦ ===
RAW_DATASETS = {
        "Daily_Treasury_Statement_All": "v1/accounting/dts/dts_all",
        "Debt_to_the_Penny": "v2/accounting/od/debt_to_penny",
        "Debt_Outstanding": "v2/accounting/od/debt_outstanding",
        "Average_Interest_Rates": "v2/accounting/od/avg_interest_rates",
        "FRN_Daily_Indexes": "v1/accounting/od/frn_daily_indexes",
        "TIPS_CPI_Data": "v1/accounting/od/tips_cpi_data",
        "Gold_Reserve": "v2/accounting/od/gold_reserve",
        "Auctions_Query": "v1/accounting/od/auctions_query",
        "Upcoming_Auctions": "v1/accounting/od/upcoming_auctions",
        "Record_Setting_Auction": "v2/accounting/od/record_setting_auction",
        "Buybacks_Operations": "v1/accounting/od/buybacks_operations",
        "Buybacks_Security_Details": "v1/accounting/od/buybacks_security_details",
        "MSPD_Table_1": "v1/debt/mspd/mspd_table_1",
        "MSPD_Table_2": "v1/debt/mspd/mspd_table_2",
        "MSPD_Table_3": "v1/debt/mspd/mspd_table_3",
        "MSPD_Table_3_Market": "v1/debt/mspd/mspd_table_3_market",
        "MSPD_Table_3_NonMarket": "v1/debt/mspd/mspd_table_3_nonmarket",
        "MSPD_Table_4": "v1/debt/mspd/mspd_table_4",
        "MSPD_Table_5": "v1/debt/mspd/mspd_table_5",
        "Interest_Expense": "v2/accounting/od/interest_expense",
        "Interest_Uninvested": "v2/accounting/od/interest_uninvested",
        "Federal_Maturity_Rates": "v1/accounting/od/federal_maturity_rates",
        "Receipts_by_Department": "v1/accounting/od/receipts_by_department",
    }

    # Создаём DATASETS с префиксом vlad_
    DATASETS = {}
    for key, endpoint in RAW_DATASETS.items():
        table_name = f"vlad_tr_{key.lower()}"
        DATASETS[table_name] = endpoint

PAGE_SIZE = 5000
MAX_RETRIES = 3

class TreasuryCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def get_last_record_date(self) -> str | None:
        """Возвращает самую свежую дату из таблицы по колонке с датой."""
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES LIKE %s", (self.table_name,))
                if not cursor.fetchone():
                    return None
                for date_col in ['record_date', 'record_date_time', 'date', 'as_of_date']:
                    try:
                        cursor.execute(f"SELECT MAX(`{date_col}`) FROM `{self.table_name}`")
                        row = cursor.fetchone()
                        if row and row[0]:
                            return str(row[0])
                    except Error:
                        continue
                return None
        except Exception as e:
            print(f"   ⚠️ Ошибка при получении последней даты из {self.table_name}: {e}")
            return None

    def fetch_all_pages(self, endpoint: str, last_date: str | None = None) -> list[dict]:
        full_url = urljoin(BASE_API_URL, endpoint)
        all_data = []
        page = 1
        while True:
            params = {'page[number]': page, 'page[size]': PAGE_SIZE}
            attempts = 0
            while attempts < MAX_RETRIES:
                try:
                    resp = self.session.get(full_url, params=params, timeout=60)
                    if resp.status_code == 429:
                        time.sleep(5 * (attempts + 1))
                        attempts += 1
                        continue
                    resp.raise_for_status()
                    break
                except Exception as e:
                    attempts += 1
                    if attempts >= MAX_RETRIES:
                        raise e
                    time.sleep(3 * attempts)
            data = resp.json()
            page_data = data.get('data', [])
            if not page_data:
                break
            if last_date:
                filtered = []
                for row in page_data:
                    row_date = None
                    for key in ['record_date', 'record_date_time', 'date', 'as_of_date']:
                        if key in row and row[key]:
                            row_date = str(row[key])[:10]
                            break
                    if row_date and row_date > last_date:
                        filtered.append(row)
                page_data = filtered
            all_data.extend(page_data)
            print(f"   ...страница {page}, всего строк: {len(all_data)}", end='\r')
            page += 1
            if len(page_data) < PAGE_SIZE:
                break
        print()
        return all_data

    def create_table_from_sample(self, sample_row: dict):
        columns_def = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
        for key in sample_row.keys():
            safe_key = self.sanitize_column_name(key)
            columns_def.append(f"`{safe_key}` TEXT NULL")
        sql = f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                {', '.join(columns_def)},
                `loaded_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()

    def sanitize_column_name(self, name: str) -> str:
        return name.replace('-', '_').replace('.', '_').replace('/', '_').lower()

    def insert_batch(self, data: list[dict]):
        if not data:
            return 0
        sample = data[0]
        keys = list(sample.keys())
        safe_keys = [self.sanitize_column_name(k) for k in keys]
        placeholders = ", ".join(["%s"] * len(keys))
        cols_str = ", ".join([f"`{k}`" for k in safe_keys])
        sql = f"INSERT IGNORE INTO `{self.table_name}` ({cols_str}) VALUES ({placeholders})"
        batch_size = 1000
        total_inserted = 0
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                values = []
                for row in batch:
                    vals = []
                    for k in keys:
                        v = row.get(k)
                        if isinstance(v, (dict, list)):
                            v = json.dumps(v, ensure_ascii=False)
                        elif v in ("", "null", None):
                            v = None
                        vals.append(v)
                    values.append(tuple(vals))
                cursor.executemany(sql, values)
                total_inserted += cursor.rowcount
            conn.commit()
        return total_inserted

    def process_dataset(self, endpoint: str):
        print(f"\n=== Обработка таблицы: {self.table_name} ===")
        last_date = self.get_last_record_date()
        if last_date:
            print(f"   📅 Последняя запись: {last_date} → загружаем только новее")
        try:
            all_data = self.fetch_all_pages(endpoint, last_date)
        except Exception as e:
            print(f"   ❌ Ошибка загрузки: {e}")
            return
        if not all_data:
            print("   ⚠️ Нет новых данных")
            return
        self.create_table_from_sample(all_data[0])
        inserted = self.insert_batch(all_data)
        print(f"   ✅ Вставлено новых записей: {inserted}")

def main():
    print(f"Запуск Treasury.gov Collector (MySQL Mode)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    # Находим эндпоинт по имени таблицы
    if args.table_name not in DATASETS:
        print(f"❌ Ошибка: неизвестное имя таблицы '{args.table_name}'. Допустимые значения:")
        for name in DATASETS.keys():
            print(f"  - {name}")
        sys.exit(1)
    endpoint = DATASETS[args.table_name]
    collector = TreasuryCollector(args.table_name)
    collector.process_dataset(endpoint)
    print("\n🏁 Завершено!")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)