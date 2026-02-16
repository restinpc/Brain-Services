#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер новостей Binance Square → MySQL
"""
import os
import sys
import argparse
import time
import random
import re
import traceback
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

import mysql.connector
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import tempfile

# Создаем папку для временных файлов в домашней директории
TEMP_DIR = os.path.join(os.path.expanduser("~"), ".playwright-tmp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Перенаправляем все временные файлы туда
os.environ["PLAYWRIGHT_TMPDIR"] = TEMP_DIR
os.environ["TMPDIR"] = TEMP_DIR
os.environ["TEMP"] = TEMP_DIR
os.environ["TMP"] = TEMP_DIR
tempfile.tempdir = TEMP_DIR

print(f"Временная директория Playwright: {TEMP_DIR}")

load_dotenv()

# === РЕШЕНИЕ ПРОБЛЕМЫ С ВРЕМЕННОЙ ДИРЕКТОРИЕЙ ===
# Создаем временную папку в домашней директории пользователя
HOME_TEMP = os.path.join(os.path.expanduser("~"), ".playwright-tmp")
os.makedirs(HOME_TEMP, exist_ok=True)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = HOME_TEMP
os.environ["PLAYWRIGHT_TMPDIR"] = HOME_TEMP

# Также устанавливаем глобальную временную директорию Python
tempfile.tempdir = HOME_TEMP

# === Настройка временной директории ===
# Создаем папку для временных файлов Playwright в текущей директории
PLAYWRIGHT_TEMP_DIR = os.path.join(os.getcwd(), ".playwright-tmp")
os.makedirs(PLAYWRIGHT_TEMP_DIR, exist_ok=True)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = PLAYWRIGHT_TEMP_DIR

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "binance_news_parser")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception, script_name: str = "Binance_news.py"):
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
parser = argparse.ArgumentParser(description="Binance Square News → MySQL")
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
    "base_url": "https://www.binance.com/en/square/news/all",
    "scrolls": 3,                       # количество прокруток главной страницы
    "pause_after_click": 5.0,            # пауза после клика (сек)
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "locale": "en-US",
    "timeout": 60000,
}

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
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    link VARCHAR(255) NOT NULL UNIQUE,
                    title TEXT,
                    full_text TEXT,
                    preview TEXT,
                    date VARCHAR(32),
                    author VARCHAR(100),
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_date (date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            conn.commit()

    def upsert_single(self, row: Dict[str, Any]) -> bool:
        """
        Вставляет или обновляет запись. Возвращает True, если запись была добавлена или обновлена.
        """
        sql = f"""
        INSERT INTO `{self.table_name}` (link, title, full_text, preview, date, author)
        VALUES (%(link)s, %(title)s, %(full_text)s, %(preview)s, %(date)s, %(author)s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            full_text = VALUES(full_text),
            preview = VALUES(preview),
            date = VALUES(date),
            author = VALUES(author)
        """
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, row)
            affected = cursor.rowcount   # 1 – вставка, 2 – обновление
            conn.commit()
            return affected > 0

# ---------- Парсинг ----------
def extract_full_text(page) -> tuple[str, str, str]:
    """
    Извлекает полный текст, автора и дату со страницы новости.
    Возвращает (full_text, author, date_str).
    """
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    # 1. Полный текст
    selectors = [
        "div[class*='post-content']",
        "div[class*='article-body']",
        "div[class*='content-body']",
        "div[class*='body']",
        "article",
        "div[class*='text']",
        "div[class*='prose']",
        "main p",
    ]
    full_text = ""
    for sel in selectors:
        block = soup.select_one(sel)
        if block:
            # Удаляем мусор
            for unwanted in block.select("script, style, header, footer, nav, button, .login, .signup"):
                unwanted.decompose()
            full_text = block.get_text(separator="\n", strip=True)
            break

    if not full_text:
        # fallback: все параграфы
        paragraphs = soup.find_all("p")
        full_text = "\n".join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30])

    full_text = re.sub(r'\s+', ' ', full_text).strip()[:10000]  # ограничение 10k символов

    # 2. Автор
    author = "Binance Square"
    author_elem = soup.find(class_=re.compile(r"author|username|creator|byline"))
    if author_elem:
        author = author_elem.get_text(strip=True)
    else:
        match = re.search(r'By\s+([A-Za-z\s]+?)(?:\s+on|\s+·)', html, re.I)
        if match:
            author = match.group(1).strip()

    # 3. Дата
    date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        dt = time_tag["datetime"]
        date_str = dt[:10] + " " + dt[11:16] if "T" in dt else dt

    return full_text, author, date_str

def collect_links(page, max_scrolls: int) -> Dict[str, tuple[str, str]]:
    log(f"Скроллим {max_scrolls} раз...")
    for i in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(SETTINGS["pause_after_click"] + random.uniform(-0.5, 1.5))
        log(f"  скролл {i+1}/{max_scrolls}")

    page.wait_for_timeout(5000)  # дополнительная пауза для догрузки
    html = page.content()

    # Ищем ссылки вида /en/square/post/ID
    post_links = re.findall(r'href="([^"]*/square/post/(\d+))"[^>]*>([^<]+)</a>', html, re.I | re.S)
    unique = {}
    for link_part, post_id, title in post_links:
        full_link = f"https://www.binance.com{link_part}" if link_part.startswith('/') else link_part
        clean_title = re.sub(r'\s+', ' ', title.strip())
        if post_id not in unique and len(clean_title) > 15:
            unique[post_id] = (full_link, clean_title)

    log(f"Найдено {len(unique)} уникальных новостей")
    return unique

# ---------- MAIN ----------
def main() -> int:
    # Простое решение для ошибки с временной папкой
    import os
    os.environ['PLAYWRIGHT_TMPDIR'] = '/tmp'

    db = DB(args.table_name)
    db.ensure_table()
    log(f"Целевая таблица: {args.database}.{args.table_name}")
    log(f"Количество прокруток: {SETTINGS['scrolls']}")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit("Установите playwright: pip install playwright && playwright install chromium")

    processed_total = 0
    seen_total = 0

    with sync_playwright() as p:
        # Создаем постоянный контекст с пользовательской директорией
        user_data_dir = os.path.join(PLAYWRIGHT_TEMP_DIR, "profile")
        context = p.chromium.launch_persistent_context(
            user_data_dir,
            headless=True,
            args=[
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ],
            user_agent=SETTINGS["user_agent"],
            locale=SETTINGS["locale"],
            timezone_id="UTC",
        )
        page = context.new_page()

        log(f"Открываю {SETTINGS['base_url']}")
        page.goto(SETTINGS["base_url"], timeout=SETTINGS["timeout"])
        log("Ожидание 10 сек для загрузки / капчи...")
        page.wait_for_timeout(10000)

        # Сбор ссылок
        links_map = collect_links(page, SETTINGS["scrolls"])

        # Обрабатываем ВСЕ собранные ссылки (без ограничения)
        for i, (post_id, (link, title)) in enumerate(links_map.items(), 1):
            log(f"[{i}/{len(links_map)}] Обработка: {title[:70]}...")
            try:
                page.goto(link, timeout=30000)
                page.wait_for_timeout(SETTINGS["pause_after_click"] * 1000)

                full_text, author, date_str = extract_full_text(page)

                # Подготовка записи
                row = {
                    "link": link,
                    "title": title,
                    "full_text": full_text,
                    "preview": (full_text[:300] + "...") if full_text else title[:200] + "...",
                    "date": date_str,
                    "author": author,
                }

                # Сохраняем
                if db.upsert_single(row):
                    processed_total += 1

                seen_total += 1
                log(f"  ✓ добавлено/обновлено, всего обработано: {processed_total}")

                # Возврат на главную
                page.go_back(timeout=15000)
                page.wait_for_timeout(2000 + random.uniform(0, 2))

            except Exception as e:
                log(f"  ✗ Ошибка при обработке: {e}")
                try:
                    page.go_back()
                except:
                    pass

        context.close()

    # Итоговая статистика
    with db.get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{args.table_name}` WHERE full_text IS NOT NULL AND full_text != ''")
        count_with_text = cur.fetchone()[0]
        log(f"Всего записей с текстом: {count_with_text}")

    log(f"Готово. Просмотрено={seen_total}, Добавлено/обновлено={processed_total}")
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