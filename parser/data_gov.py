import os
import time
import requests
import xml.etree.ElementTree as ET
import mysql.connector
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from datetime import datetime

# --- КОНФИГУРАЦИЯ ---
load_dotenv()

BASE_TREASURY_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
SAVE_DIR = "data_gov_datasets"

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'user': os.getenv('DB_USER', 'vlad'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'vlad'),
    'port': int(os.getenv('DB_PORT', 3306))
}

# Оставляем только 100% рабочие категории
DATA_TYPES_CONFIG = {
    "Nominal_Yield": {
        "code": "daily_treasury_yield_curve",
        "start_year": 1990,
        "description": "Номинальная кривая доходности (Nominal Yield Curve). Основной индикатор."
    },
    "Real_Yield": {
        "code": "daily_treasury_real_yield_curve",
        "start_year": 2003,
        "description": "Реальная доходность (Real Yield) с поправкой на инфляцию."
    }
}


# --- СКАЧИВАНИЕ ---
def download_treasury_data(data_type_name, data_type_code, year):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    params = {'data': data_type_code, 'field_tdr_date_value': year}
    headers = {'User-Agent': 'Mozilla/5.0'}
    filename = f"Treasury_{data_type_name}_{year}.xml"
    filepath = os.path.join(SAVE_DIR, filename)

    print(f"📥 Скачивание {data_type_name} за {year}...", end=" ")

    try:
        response = session.get(BASE_TREASURY_URL, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print("[OK]")
            return filepath
        elif response.status_code == 404:
            print("[SKIP] Нет данных")
            return None
        else:
            print(f"[ERR] {response.status_code}")
            return None
    except Exception as e:
        print(f"[ERR] {e}")
        return None


# --- ПАРСИНГ ---
def clean_tag(tag):
    return tag.split('}')[-1] if '}' in tag else tag


def parse_xml_file(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except:
        return []

    ns = {'atom': 'http://www.w3.org/2005/Atom', 'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'}
    data_rows = []
    entries = root.findall('atom:entry', ns)

    if not entries: return []

    for entry in entries:
        content = entry.find('atom:content', ns)
        properties = content.find('m:properties', ns)
        row_data = {}

        for prop in properties:
            col_name = clean_tag(prop.tag)
            col_value = prop.text
            if col_name == 'Id': continue
            if col_name == 'NEW_DATE':
                if col_value: row_data['record_date'] = col_value.split('T')[0]
            else:
                if col_value:
                    try:
                        row_data[col_name] = float(col_value)
                    except:
                        row_data[col_name] = None
                else:
                    row_data[col_name] = None

        if 'record_date' in row_data:
            data_rows.append(row_data)

    return data_rows


# --- ЗАГРУЗКА (БЕЗ ALTER TABLE) ---
def save_to_db_simple(data, table_name, table_comment=""):
    if not data: return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Сначала узнаем, какие колонки УЖЕ ЕСТЬ в таблице (если она существует)
        # Если таблицы нет, мы её создадим по образцу текущего файла

        # Попытка создать таблицу
        sample_row = data[0]
        columns_in_file = [k for k in sample_row.keys() if k != 'record_date']

        columns_def = ["`record_date` DATE NOT NULL PRIMARY KEY COMMENT 'Дата публикации'"]
        for key in sorted(columns_in_file):
            columns_def.append(f"`{key}` FLOAT NULL")

        sql_create = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns_def)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{table_comment}';"
        cursor.execute(sql_create)

        # 2. Получаем список РЕАЛЬНЫХ колонок в базе
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        db_columns = [row[0] for row in cursor.fetchall()]  # ['record_date', 'BC_1MONTH', ...]

        # 3. Фильтруем данные: оставляем только те ключи, которые есть в БД
        # Это предотвращает ошибку "Unknown column", если в файле появились новые поля
        valid_keys = [col for col in db_columns if col != 'record_date']

        # Подготовка INSERT
        placeholders = ", ".join(["%s"] * (len(valid_keys) + 1))
        cols_str = ", ".join([f"`{c}`" for c in ['record_date'] + valid_keys])

        sql = f"REPLACE INTO `{table_name}` ({cols_str}) VALUES ({placeholders})"

        values = []
        for row in data:
            # Формируем список значений строго в порядке valid_keys
            row_vals = [row.get('record_date')]
            for k in valid_keys:
                row_vals.append(row.get(k))  # Если ключа нет в файле (старый год), вернется None -> NULL
            values.append(row_vals)

        cursor.executemany(sql, values)
        conn.commit()
        print(f"   ✅ DB: Загружено {len(values)} строк в {table_name}")

    except mysql.connector.Error as err:
        print(f"   ❌ DB Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


# --- MAIN ---
def main():
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    print(f"🚀 TREASURY COLLECTOR (LITE)")
    current_year = datetime.now().year

    for name, config in DATA_TYPES_CONFIG.items():
        start_year = config['start_year']
        code_url = config['code']
        comment = config.get('description', '')

        print(f"\n=== Категория: {name} (с {start_year} года) ===")
        table_name = f"vlad_treasury_{name.lower()}"

        for year in range(start_year, current_year + 1):
            file_path = download_treasury_data(name, code_url, year)
            if file_path:
                parsed_data = parse_xml_file(file_path)
                if parsed_data:
                    save_to_db_simple(parsed_data, table_name, comment)
                else:
                    print(f"   ⚠️ Файл пуст")
            time.sleep(0.5)

    print("\n🏁 ГОТОВО!")


if __name__ == "__main__":
    main()
