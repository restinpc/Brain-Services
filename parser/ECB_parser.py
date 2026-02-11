#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import time
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import traceback
from dotenv import load_dotenv

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "ecb_parser_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "ecb_parser.py"):
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


# === Аргументы командной строки + .env fallback ===
parser = argparse.ArgumentParser(description="ECB RSS Feeds Parser → MySQL (no local files)")
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

BASE_URL = "https://www.ecb.europa.eu/home/html/rss.en.html"


class ECBCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.init_db()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def init_db(self):
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        feed_url VARCHAR(255) NOT NULL,
                        feed_title VARCHAR(255),
                        saved_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        xml_content LONGTEXT,
                        UNIQUE KEY unique_feed_url (feed_url)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn.commit()
                print(f"✅ Таблица `{self.table_name}` готова.")
        except Error as err:
            print(f"❌ Ошибка БД при инициализации: {err}")

    def fetch_and_parse_feeds(self):
        from playwright.sync_api import sync_playwright
        print(f"Поиск RSS лент на {BASE_URL}...")
        feeds = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                locale="en-US",
                timezone_id="UTC"
            )
            page = context.new_page()
            page.goto(BASE_URL, timeout=30000)
            page.wait_for_timeout(1000)
            links = page.query_selector_all("a[href]")
            for link in links:
                href = link.get_attribute("href")
                title = link.text_content().strip() or "ECB Feed"
                if not href:
                    continue
                if "/rss/" in href or href.endswith(".xml") or href.endswith(".rss"):
                    if "fxref" in href:
                        continue
                    from urllib.parse import urljoin
                    full_url = urljoin("https://www.ecb.europa.eu", href)
                    feeds.append((full_url, title))
            browser.close()
        unique_feeds = list(set(feeds))
        print(f"Найдено {len(unique_feeds)} лент.")
        return unique_feeds

    def download_feeds(self):
        feeds = self.fetch_and_parse_feeds()
        if not feeds:
            print("Ленты не найдены.")
            return
        new_count = 0
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                for url, title in feeds:
                    try:
                        import requests
                        resp = requests.get(url, timeout=30)
                        if resp.status_code != 200:
                            continue
                        content = resp.text
                        cursor.execute(f"""
                            INSERT INTO `{self.table_name}` (feed_url, feed_title, xml_content)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                xml_content = VALUES(xml_content),
                                saved_at = NOW()
                        """, (url, title, content))
                        if cursor.rowcount > 0:
                            new_count += 1
                            print(f"Обновлено: {title}")
                    except Exception as e:
                        print(f"Ошибка при обработке {url}: {e}")
                conn.commit()
                print(f"Цикл завершен. Обновлено записей: {new_count}")
        except Error as err:
            print(f"Ошибка БД: {err}")


def main():
    print(f"Запуск ECB Collector (MySQL Mode, без файлов)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print("=" * 40)

    collector = ECBCollector(args.table_name)
    collector.download_feeds()
    print("\n🏁 ЗАГРУЗКА ЗАВЕРШЕНА")


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