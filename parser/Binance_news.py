#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import time
import random
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, List, Optional
import traceback
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "investing_crypto_news_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "investing_crypto_news.py"):
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
        import requests
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"✅ [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")


# === Аргументы командной строки ===
parser = argparse.ArgumentParser(description="Investing.com Crypto News → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны все параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database,
}

# ---------- CONFIG ----------
SETTINGS = {
    "base_page": "https://ru.investing.com/news/cryptocurrency-news",
    "domain_id": 7,
}
LOOKBACK_DAYS = 7
MAX_PAGES = 100  # Предотвращение бесконечного цикла


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ---------- DB ----------
class DB:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self) -> None:
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                    article_id BIGINT PRIMARY KEY,
                    publish_time_utc DATETIME NULL,
                    title VARCHAR(255) NULL,
                    summary TEXT NULL,
                    link VARCHAR(255) NULL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_time (publish_time_utc)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            conn.commit()

    def get_max_time(self) -> Optional[datetime]:
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(publish_time_utc) FROM `{self.table_name}`")
            (mx,) = cur.fetchone()
            return mx

    def upsert_single(self, row: Dict[str, Any]) -> bool:
        """Возвращает True, если строка была вставлена или обновлена."""
        update_fields = ["publish_time_utc", "title", "summary", "link"]
        set_clause = ", ".join([f"{col} = VALUES({col})" for col in update_fields])

        sql = f"""
        INSERT INTO `{self.table_name}` (
            article_id, publish_time_utc, title, summary, link
        ) VALUES (
            %(article_id)s, %(publish_time_utc)s, %(title)s, %(summary)s, %(link)s
        )
        ON DUPLICATE KEY UPDATE {set_clause}
        """

        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, row)
            affected = cursor.rowcount
            conn.commit()
            return affected > 0


# ---------- VALUE HELPERS ----------
def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def safe_str(v: Any, max_len: int) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    return s[:max_len]


def parse_publish_time(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    now = datetime.now(timezone.utc)
    date_str = date_str.strip().lower().replace('&nbsp;-&nbsp;', '').replace('•', '').strip()

    num_match = re.search(r'(\d+)', date_str)
    if not num_match:
        return None
    num = int(num_match.group(1))

    if 'минут' in date_str:
        return now - timedelta(minutes=num)
    elif 'час' in date_str:
        return now - timedelta(hours=num)
    elif 'день' in date_str or 'дня' in date_str or 'дней' in date_str:
        return now - timedelta(days=num)
    elif 'недел' in date_str:
        return now - timedelta(weeks=num)
    elif 'вчера' in date_str:
        return now - timedelta(days=1)
    else:
        # Абсолютная дата, например "17 февр. 2026 г."
        months_ru = {
            'янв': 1, 'февр': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
            'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12
        }
        parts = re.split(r'\s+', date_str)
        if len(parts) < 2:
            return None
        day_str = parts[0]
        mon_str = parts[1].rstrip('.')
        year_str = parts[2] if len(parts) > 2 else str(now.year)

        try:
            day = int(day_str)
            mon = months_ru.get(mon_str)
            year = int(year_str.rstrip('г.'))
            if mon is None:
                return None
            dt = datetime(year, mon, day, tzinfo=timezone.utc)
            return dt.replace(tzinfo=None)
        except ValueError:
            return None


# ---------- FETCH ----------
def fetch_page(context, page_num: int) -> List[Dict[str, Any]]:
    base = SETTINGS["base_page"]
    url = base if page_num == 1 else f"{base}/{page_num}"
    log(f"Fetching page {page_num}: {url}")
    page = context.new_page()
    page.goto(url, timeout=60000)
    page.wait_for_load_state('networkidle')
    time.sleep(2)  # Additional wait
    content = page.content()
    page.close()

    soup = BeautifulSoup(content, 'html.parser')
    articles = soup.find_all('div', class_='articleItem')
    rows: List[Dict[str, Any]] = []
    for art in articles:
        title_elem = art.find('a', class_='title')
        if not title_elem:
            continue
        title = safe_str(title_elem.text, 255)
        link = safe_str('https://ru.investing.com' + title_elem.get('href', ''), 255)

        # Извлечение article_id из link
        id_match = re.search(r'article-(\d+)', link)
        article_id = safe_int(id_match.group(1)) if id_match else None
        if not article_id:
            continue

        date_elem = art.find('span', class_='date')
        date_str = date_elem.text.strip() if date_elem else ''
        publish_time = parse_publish_time(date_str)

        summary_elem = art.find('p', class_='summary')
        summary = safe_str(summary_elem.text, 2000) if summary_elem else None

        rows.append({
            "article_id": article_id,
            "publish_time_utc": publish_time,
            "title": title,
            "summary": summary,
            "link": link,
        })
    return rows


# ---------- MAIN ----------
def main() -> int:
    db = DB(args.table_name)
    db.ensure_table()
    last_time = db.get_max_time()
    if last_time:
        start_time = last_time - timedelta(days=LOOKBACK_DAYS)
        log(f"Incremental start: lookback={LOOKBACK_DAYS}d, skip if <= {last_time}")
    else:
        start_time = datetime.min
        log(f"DB empty. Full scrape")

    log(f"DB={args.database} Table={args.table_name}")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit(
            "Install: pip install playwright beautifulsoup4 mysql-connector-python python-dotenv re && playwright install chromium")

    processed_total = 0
    seen_total = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            locale="ru-RU",
            timezone_id="UTC",
        )
        page_num = 1
        while page_num <= MAX_PAGES:
            rows = fetch_page(context, page_num)
            if not rows:
                log(f"No more articles on page {page_num}, stopping")
                break
            batch_seen = 0
            batch_processed = 0
            stop = False
            for row in rows:
                if row['publish_time_utc'] is None or row['publish_time_utc'] <= start_time:
                    log(f"Reached old article at {row['publish_time_utc']}, stopping")
                    stop = True
                    break
                batch_seen += 1
                if db.upsert_single(row):
                    batch_processed += 1
            seen_total += batch_seen
            processed_total += batch_processed
            log(f"Page {page_num}: seen={batch_seen} processed={batch_processed}")
            if stop or batch_seen == 0:
                break
            page_num += 1
            time.sleep(random.uniform(1.0, 3.0))  # Анти-бан пауза
        browser.close()

    log(f"Done. Seen={seen_total} Processed (new/updated)={processed_total}")
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)