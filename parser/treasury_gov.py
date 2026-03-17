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

# Колонки с датами — для определения поля фильтрации
DATE_COLUMNS = ['record_date', 'record_date_time', 'date', 'as_of_date']


class TreasuryCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def get_last_record_date(self) -> tuple:
        """
        Возвращает (last_date, date_column_name).
        Нужно имя колонки чтобы передать серверу в filter=.
        """
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES LIKE %s", (self.table_name,))
                if not cursor.fetchone():
                    return None, None
                for date_col in DATE_COLUMNS:
                    try:
                        cursor.execute(f"SELECT MAX(`{date_col}`) FROM `{self.table_name}`")
                        row = cursor.fetchone()
                        if row and row[0]:
                            return str(row[0])[:10], date_col
                    except Error:
                        continue
                return None, None
        except Exception as e:
            print(f"   ⚠️ Ошибка при получении последней даты из {self.table_name}: {e}")
            return None, None

    def detect_date_column(self, endpoint: str) -> str:
        """Определяет имя колонки с датой, запросив 1 строку из API."""
        full_url = urljoin(BASE_API_URL, endpoint)
        try:
            resp = self.session.get(full_url, params={'page[size]': 1}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get('data', [])
            if rows:
                for col in DATE_COLUMNS:
                    if col in rows[0]:
                        return col
        except Exception:
            pass
        return 'record_date'

    def fetch_all_pages(self, endpoint: str, last_date: str = None) -> list:
        """
        Загружает данные с СЕРВЕРНОЙ фильтрацией.

        FIX: ранее парсер загружал ВСЕ страницы с 2005 года и фильтровал в Python.
        API возвращает данные по умолчанию ASC → страница 1 = данные 2005 →
        все отбрасываются фильтром (row_date <= last_date) →
        len(filtered) == 0 < PAGE_SIZE → break →
        парсер останавливался на 1й странице, никогда не дойдя до новых данных.

        Теперь: параметр filter=record_date:gt:YYYY-MM-DD передаётся серверу.
        API сам возвращает только строки новее last_date. Парсер получает
        ТОЛЬКО новые данные, пагинация работает корректно.
        """
        full_url = urljoin(BASE_API_URL, endpoint)
        all_data = []
        page = 1

        # Определяем колонку с датой для серверного фильтра
        date_col = self.detect_date_column(endpoint) if last_date else 'record_date'

        while True:
            params = {
                'page[number]': page,
                'page[size]': PAGE_SIZE,
                'sort': date_col,
            }

            # Серверная фильтрация — API отдаёт только строки новее last_date
            if last_date:
                params['filter'] = f'{date_col}:gt:{last_date}'

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

            all_data.extend(page_data)

            total_count = data.get('meta', {}).get('total-count', '?')
            print(f"   ...страница {page}, строк: {len(all_data)}/{total_count}", end='\r')

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

    def insert_batch(self, data: list):
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
        last_date, date_col = self.get_last_record_date()
        if last_date:
            print(f"   📅 Последняя запись: {last_date} (колонка: {date_col}) → загружаем только новее")
            print(f"   🔍 Серверный фильтр: filter={date_col}:gt:{last_date}")
        else:
            print(f"   📦 Таблица пуста — полная загрузка")
        try:
            all_data = self.fetch_all_pages(endpoint, last_date)
        except Exception as e:
            print(f"   ❌ Ошибка загрузки: {e}")
            return
        if not all_data:
            print("   ⚠️ Нет новых данных")
            return
        print(f"   📊 Получено {len(all_data)} новых строк")
        self.create_table_from_sample(all_data[0])
        inserted = self.insert_batch(all_data)
        print(f"   ✅ Вставлено новых записей: {inserted}")


def main():
    print(f"Запуск Treasury.gov Collector (MySQL Mode)")
    print(f"База: {args.host}:{args.port}/{args.database}")
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
