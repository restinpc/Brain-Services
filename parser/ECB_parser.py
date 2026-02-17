#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import time
import zipfile
import io
import traceback
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import feedparser
from dateutil import parser as date_parser
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import tempfile

# Новая библиотека для PDF
from pypdf import PdfReader

load_dotenv()

# Создаём рабочую директорию в домашней папке
WORK_DIR = os.path.join(os.path.expanduser("~"), ".ecb_parser")
os.makedirs(WORK_DIR, exist_ok=True)
print(f"📁 Рабочая директория: {WORK_DIR}")

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "ecb_parser")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

BASE_URL_RSS = "https://www.ecb.europa.eu/home/html/rss.en.html"
ZIP_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
CSV_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.csv"


def send_error_trace(exc: Exception):
    logs = f"Node: {NODE_NAME}\nScript: ECB_parser.py\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
                      timeout=10)
    except:
        pass


def download_and_read_zip_csv(url):
    """
    Скачивает ZIP-архив по URL, извлекает из него CSV-файл
    и возвращает DataFrame. Сохраняет файл в рабочую директорию.
    """
    local_zip = os.path.join(WORK_DIR, "eurofxref-hist.zip")
    csv_filename_in_zip = "eurofxref-hist.csv"

    try:
        print(f"1. Скачиваю архив из: {url}")
        response = requests.get(url, timeout=15, stream=True)
        response.raise_for_status()

        with open(local_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"   Архив сохранён как: {local_zip}")

        print(f"2. Извлекаю '{csv_filename_in_zip}' из архива...")
        with zipfile.ZipFile(local_zip, 'r') as zf:
            if csv_filename_in_zip not in zf.namelist():
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise Exception("В архиве не найдено CSV файлов.")
                csv_filename_in_zip = csv_files[0]
                print(f"   Найден CSV файл: {csv_filename_in_zip}")

            with zf.open(csv_filename_in_zip) as csv_file:
                df = pd.read_csv(csv_file)

        num_rows = df.shape[0]
        print(f"   ✅ CSV загружен, строк: {num_rows}")
        print(f"   Последняя дата: {df['Date'].max()}")

        if df['Date'].max() < '2026-01-01':
            raise ValueError(f"Данные старые! Max дата {df['Date'].max()}")

        return df

    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при скачивании: {e}")
        raise
    except zipfile.BadZipFile:
        print("❌ Ошибка: скачанный файл не является ZIP архивом или повреждён.")
        raise
    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
        raise


def download_and_read_zip_csv_memory(url):
    """
    Скачивает ZIP-архив по URL и читает CSV напрямую из памяти
    """
    csv_filename_in_zip = "eurofxref-hist.csv"

    try:
        print(f"1. Скачиваю архив из: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        print("2. Читаю ZIP архив из памяти...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            if csv_filename_in_zip not in zf.namelist():
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise Exception("В архиве не найдено CSV файлов.")
                csv_filename_in_zip = csv_files[0]
                print(f"   Найден CSV файл: {csv_filename_in_zip}")

            with zf.open(csv_filename_in_zip) as csv_file:
                df = pd.read_csv(csv_file)

        num_rows = df.shape[0]
        print(f"   ✅ CSV загружен, строк: {num_rows}")
        print(f"   Последняя дата: {df['Date'].max()}")

        if df['Date'].max() < '2026-01-01':
            raise ValueError(f"Данные старые! Max дата {df['Date'].max()}")

        return df

    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при скачивании: {e}")
        raise
    except zipfile.BadZipFile:
        print("❌ Ошибка: скачанный файл не является ZIP архивом или повреждён.")
        raise
    except Exception as e:
        print(f"❌ Произошла ошибка: {e}")
        raise


def extract_text_from_pdf(pdf_url):
    """
    Скачивает PDF по URL и извлекает текст с помощью pypdf.
    Возвращает строку текста или None при ошибке.
    """
    try:
        print(f"      → Скачиваем PDF: {pdf_url}")
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()

        reader = PdfReader(io.BytesIO(response.content))
        text = ""

        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n\n"

        if not text.strip():
            print("      → Текст не извлечён (возможно, PDF-скан или защита)")
            return None

        print(f"      → Извлечено ~{len(text):,} символов")
        return text[:1000000]  # ограничим размер

    except Exception as e:
        print(f"      → Ошибка pypdf: {e}")
        return None


parser = argparse.ArgumentParser(description="ECB Parser: rates из ZIP/CSV + items с полным текстом (включая PDF)")
parser.add_argument("table_name", help="Префикс таблиц (vlad, vlad_ecb_rates, vlad_ecb_items и т.д.)")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database,
}


class ECBParser:
    def __init__(self, prefix: str):
        clean = prefix.split('_ecb_')[0].rstrip('_') if '_ecb_' in prefix else prefix
        self.prefix = clean or "vlad"

        p = prefix.lower()
        if any(w in p for w in ['rates', 'exchange', 'fxref', 'currency']):
            self.mode = "rates"
        elif 'items' in p:
            self.mode = "items"
        else:
            self.mode = "all"

        self.items_table = f"{self.prefix}_ecb_items" if self.mode in ("all", "items") else None
        self.rates_table = f"{self.prefix}_ecb_exchange_rates" if self.mode in ("all", "rates") else None

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        self.init_db()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def init_db(self):
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            if self.rates_table:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{self.rates_table}` (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        currency CHAR(3) NOT NULL,
                        rate_date DATE NOT NULL,
                        rate DECIMAL(22,12) NOT NULL,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_rate (currency, rate_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                print(f"   → Таблица {self.rates_table} ")

            if self.items_table:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{self.items_table}` (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        feed_url VARCHAR(512) NOT NULL,
                        guid VARCHAR(512) NOT NULL UNIQUE,
                        feed_type VARCHAR(50),
                        title VARCHAR(1024),
                        link VARCHAR(1024),
                        published_at DATETIME,
                        description LONGTEXT,
                        full_text LONGTEXT,
                        scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                print(f"   → Таблица {self.items_table} (с автоинкрементом id)")

            conn.commit()
            print(f"✅ Таблицы готовы (режим {self.mode})")

    def run_rates(self):
        print("\n📊 Скачиваем полную историю курсов из eurofxref-hist.zip...")
        try:
            df = download_and_read_zip_csv(ZIP_URL)

            print("\n3. Преобразую данные для загрузки в БД...")
            df_melted = df.melt(id_vars=['Date'], var_name='currency', value_name='rate')
            df_melted['rate_date'] = pd.to_datetime(df_melted['Date'])
            df_melted = df_melted.drop('Date', axis=1)
            df_melted = df_melted.dropna(subset=['rate'])

            print(f"   Всего записей для загрузки: {len(df_melted):,}")
            print(f"   Диапазон дат: {df_melted['rate_date'].min()} → {df_melted['rate_date'].max()}")
            print(f"   Уникальных валют: {df_melted['currency'].nunique()}")

            print("\n4. Загружаю данные в БД...")
            batch_size = 10000
            total_inserted = 0
            total_updated = 0

            with self.get_db_connection() as conn:
                cursor = conn.cursor()

                for i in range(0, len(df_melted), batch_size):
                    batch = df_melted.iloc[i:i + batch_size]
                    values = [
                        (row['currency'], row['rate_date'].strftime('%Y-%m-%d'), float(row['rate']))
                        for _, row in batch.iterrows()
                    ]

                    cursor.executemany(f"""
                        INSERT INTO `{self.rates_table}` (currency, rate_date, rate)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            rate = VALUES(rate),
                            updated_at = CURRENT_TIMESTAMP
                    """, values)

                    conn.commit()
                    total_inserted += len(batch)
                    print(f"      Загружено {total_inserted:,} / {len(df_melted):,} записей...")

            print(f"\n✅ Успешно загружено {total_inserted:,} записей в {self.rates_table}")

        except Exception as e:
            print(f"❌ Ошибка в run_rates: {e}")
            traceback.print_exc()
            raise

    def fetch_rss_feeds(self):
        print(f"\n📡 Сканируем RSS-страницу → {BASE_URL_RSS}")
        resp = self.session.get(BASE_URL_RSS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        feeds = []

        language_titles = {
            "Български", "Čeština", "Dansk", "Deutsch", "Eλληνικά", "English", "Español",
            "Eesti keel", "Suomi", "Français", "Gaeilge", "Hrvatski", "Magyar", "Italiano",
            "Lietuvių", "Latviešu", "Malti", "Nederlands", "Polski", "Português", "Română",
            "Slovenčina", "Slovenščina", "Svenska"
        }

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            title = a.get_text(strip=True) or "ECB Feed"

            if not (
                    href.startswith("/rss/fxref-") or
                    "/rss/" in href and href.endswith((".html", ".rss", ".xml")) or
                    href.endswith((".rss", ".xml"))
            ):
                continue

            if re.match(r'^/rss\.[a-z]{2,3}\.html?$', href) or title in language_titles:
                continue

            if any(x in href.lower() for x in ["hist", "90d", "archive", ".zip", "pdf"]):
                continue

            full_url = urljoin("https://www.ecb.europa.eu", href)
            feeds.append((full_url, title))

        feeds = list(dict.fromkeys(feeds))
        print(f" Найдено {len(feeds)} реальных RSS-фидов")
        for url, t in feeds[:10]:
            print(f"   - {t}: {url}")
        return feeds

    def run_items(self):
        print("\n📰 Собираем RSS-статьи...")
        feeds = self.fetch_rss_feeds()
        count_new = 0

        for feed_url, title in feeds:
            print(f"\n   ↓ Обрабатываем фид: {title}")
            try:
                r = self.session.get(feed_url, timeout=45)
                r.raise_for_status()
                d = feedparser.parse(r.text)

                feed_type = self._get_feed_type(feed_url)

                with self.get_db_connection() as conn:
                    cursor = conn.cursor()

                    for entry in d.entries:
                        try:
                            guid = entry.get('id') or entry.get('guid') or entry.get('link')
                            if not guid: continue

                            published = None
                            for f in ['published', 'updated', 'dc_date', 'pubDate']:
                                if entry.get(f):
                                    try:
                                        published = date_parser.parse(entry.get(f))
                                        break
                                    except:
                                        continue

                            desc = entry.get('summary') or entry.get('description') or ""
                            if isinstance(desc, dict) and 'value' in desc:
                                desc = desc['value']

                            link = entry.get('link')
                            full_text = None

                            if link:
                                # Если ссылка ведёт на PDF — используем pypdf
                                if link.lower().endswith('.pdf'):
                                    print(f"      → PDF-файл: {link}")
                                    full_text = extract_text_from_pdf(link)
                                else:
                                    # Обычная HTML-страница
                                    try:
                                        html_r = self.session.get(link, timeout=30)
                                        html_r.raise_for_status()
                                        soup = BeautifulSoup(html_r.text, 'html.parser')

                                        for tag in soup(['header', 'footer', 'nav', 'aside', 'script', 'style', 'form']):
                                            tag.decompose()

                                        content = soup.find('main') or soup.find('article') or \
                                                  soup.find('div', class_=['content', 'article', 'rte', 'ecb-article'])
                                        if content:
                                            full_text = content.get_text(separator='\n', strip=True)
                                        else:
                                            full_text = soup.get_text(separator='\n', strip=True)[:200000]

                                    except Exception as e:
                                        print(f"        Не удалось спарсить статью {link}: {e}")

                            # Если полный текст не получен, используем описание
                            if not full_text:
                                full_text = desc

                            cursor.execute(f"""
                                INSERT INTO `{self.items_table}` 
                                (feed_url, guid, feed_type, title, link, published_at, description, full_text)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    title=VALUES(title),
                                    published_at=VALUES(published_at),
                                    description=VALUES(description),
                                    full_text=VALUES(full_text),
                                    scraped_at=NOW()
                            """, (
                                feed_url, guid, feed_type, entry.get('title'), link, published, desc[:50000],
                                full_text
                            ))

                            if cursor.rowcount != 0:
                                count_new += 1

                        except Exception as e:
                            continue

                    conn.commit()
                    print(f"      ✅ Обработано {len(d.entries)} записей")

            except Exception as e:
                print(f"      ❌ Ошибка: {e}")
            time.sleep(1.5)

        print(f"\n✅ Добавлено/обновлено {count_new} статей в {self.items_table}")

    def _get_feed_type(self, url: str) -> str:
        u = url.lower()
        if 'fxref' in u: return 'exchange_rate'
        if any(x in u for x in ['press', 'pressreleases']): return 'press_release'
        if 'speech' in u or '/key/' in u: return 'speech'
        if 'blog' in u: return 'blog'
        if 'statpress' in u: return 'statistical_release'
        return 'other'

    def run(self):
        print(f"\n🚀 ECB Parser запущен | префикс: {self.prefix} | режим: {self.mode.upper()}")
        if self.mode in ("all", "rates"):
            self.run_rates()
        if self.mode in ("all", "items"):
            self.run_items()
        print("\n🏁 Завершено!")


if __name__ == "__main__":
    try:
        ECBParser(args.table_name).run()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        send_error_trace(e)
        sys.exit(1)