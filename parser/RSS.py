#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import time
import json
from datetime import datetime
import feedparser
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import mysql.connector
from mysql.connector import Error
import traceback
from dotenv import load_dotenv

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "rss_fed_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception, script_name: str = "RSS.py"):
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
    print(f"\n [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f" [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f" [POST] Не удалось отправить отчёт: {e}")

# === Аргументы командной строки + .env fallback ===
parser = argparse.ArgumentParser(description="Federal Reserve RSS Parser → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database,
}

FEEDS_URL = "https://www.federalreserve.gov/feeds/feeds.htm"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

IGNORE_KEYWORDS = [
    "Data Download", "Inspector General", "Supervision", "Reporting Forms",
    "Board Meetings", "Charge-Off", "Legal Developments", "Enforcement Actions"
]

class RSSCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
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
                        feed_title VARCHAR(255),
                        feed_url VARCHAR(255),
                        entry_title VARCHAR(255),
                        entry_link VARCHAR(500),
                        entry_guid VARCHAR(190) UNIQUE,
                        entry_description LONGTEXT,
                        full_text LONGTEXT,
                        published VARCHAR(100),
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_feed (feed_url),
                        INDEX idx_published (published)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn.commit()
                print(f" Таблица `{self.table_name}` готова.")
        except Error as err:
            print(f" Ошибка БД при старте: {err}")

    def clean_html_content(self, soup):
        for tag in soup.select('script, style, nav, header, footer, aside, .header, .footer, .breadcrumb, .social-share'):
            tag.decompose()
        return soup

    def get_full_text_playwright(self, url: str) -> str:
        """Использует Playwright для извлечения текста с JS-сайтов."""
        from playwright.sync_api import sync_playwright
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    locale="en-US",
                    timezone_id="UTC"
                )
                page = context.new_page()
                page.goto(url, timeout=30000)
                page.wait_for_timeout(1000)
                # Удаляем мусор
                page.evaluate("""() => {
                    const trash = document.querySelectorAll('nav, header, footer, .header, .footer, .breadcrumb, .social-share');
                    trash.forEach(el => el.remove());
                }""")
                text = ""
                selectors = ["#article", "#content .col-md-8", "#content", ".data-article"]
                for sel in selectors:
                    els = page.query_selector_all(sel)
                    for el in els:
                        t = el.text_content().strip()
                        if len(t) > 50:
                            text += t + "\n"
                    if len(text) > 100:
                        break
                if not text:
                    body = page.query_selector("body")
                    if body:
                        text = body.text_content().strip()
                browser.close()
                return text if len(text) > 50 else ""
        except Exception as e:
            print(f"   [Playwright Error] {e}")
            return ""

    def get_full_text(self, url: str) -> str:
        """Сначала requests, если неудача — Playwright."""
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                self.clean_html_content(soup)
                content = None
                for sel in ['div#content', 'div#article', 'div.col-md-8', 'main']:
                    found = soup.select_one(sel)
                    if found:
                        content = found
                        break
                if content:
                    text = content.get_text(separator=' ', strip=True)
                    if "Skip to main content" not in text and len(text) > 200:
                        return text
        except Exception:
            pass
        print(f"   -> Переход на Playwright для: {url[-30:]}")
        return self.get_full_text_playwright(url)

    def process_feed(self, feed_url: str, feed_name: str):
        try:
            feed = feedparser.parse(feed_url)
            new_count = 0
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                for entry in feed.entries[:10]:  # только последние 10
                    entry_guid = (entry.get('id') or entry.link)[:190]
                    if not entry_guid:
                        continue
                    cursor.execute(f"SELECT id FROM `{self.table_name}` WHERE entry_guid = %s", (entry_guid,))
                    if cursor.fetchone():
                        continue
                    print(f" [{feed_name}] Новая статья: {entry.title[:50]}...")
                    full_text = ""
                    if 'link' in entry:
                        full_text = self.get_full_text(entry.link)
                    description = entry.get('description', '')
                    if not full_text:
                        full_text = BeautifulSoup(description, 'html.parser').get_text(strip=True)
                    published = entry.get('published') or entry.get('updated') or datetime.now().isoformat()
                    cursor.execute(f"""
                        INSERT INTO `{self.table_name}` 
                        (feed_title, feed_url, entry_title, entry_link, entry_guid,
                         entry_description, full_text, published)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (feed_name, feed_url, entry.title, entry.link, entry_guid,
                          description, full_text, published))
                    new_count += 1
                if new_count > 0:
                    conn.commit()
                    print(f" -> Сохранено {new_count} записей.")
        except Exception as e:
            print(f"Ошибка фида {feed_name}: {e}")

    def run_cycle(self):
        print("Сканирование списка фидов...")
        try:
            resp = self.session.get(FEEDS_URL, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            feeds = []
            seen = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                name = a.text.strip()
                if any(ignored in name for ignored in IGNORE_KEYWORDS):
                    continue
                if href.endswith('.xml'):
                    full_url = urljoin("https://www.federalreserve.gov", href)
                    if full_url not in seen:
                        seen.add(full_url)
                        feeds.append({'url': full_url, 'name': name})
            print(f"Отобрано {len(feeds)} полезных лент (пресс-релизы, речи).")
            for feed in feeds:
                self.process_feed(feed['url'], feed['name'])
        except Exception as e:
            print(f"Сбой цикла: {e}")

def main():
    collector = RSSCollector(args.table_name)
    collector.run_cycle()
    print("\n ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n Прервано пользователем")
    except Exception as e:
        print(f"\n Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)