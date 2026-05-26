#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузчик новостей CoinDesk.com (крипто) в MySQL.

Почему без Playwright:
  CoinDesk публикует официальный RSS-feed, поэтому для регулярной загрузки
  новостей не нужно открывать тяжелую JS-страницу браузером. Это устраняет
  зависания Page.goto/Page.screenshot и снижает нагрузку на сервер.

Запуск:
  python coindesk_crypto_news.py vlad_coindesk_crypto_news [host] [port] [user] [password] [database]

Опциональные переменные .env:
  COINDESK_RSS_URL=https://www.coindesk.com/arc/outboundfeeds/rss/
  COINDESK_REQUEST_TIMEOUT=30
"""

import os
import sys
import re
import html
import argparse
import datetime as dt
import traceback
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

# ----------------------------------------------------------------------
# Конфигурация отправки ошибок
# ----------------------------------------------------------------------
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "vlad_coindesk_crypto_news")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# ----------------------------------------------------------------------
# Параметры источника
# ----------------------------------------------------------------------
BASE_URL = "https://www.coindesk.com"
RSS_URL = os.getenv("COINDESK_RSS_URL", "https://www.coindesk.com/arc/outboundfeeds/rss/")
REQUEST_TIMEOUT = int(os.getenv("COINDESK_REQUEST_TIMEOUT", "30"))
DEFAULT_LIMIT = int(os.getenv("COINDESK_MAX_ITEMS", "0"))  # 0 = все элементы RSS


def send_error_trace(exc: Exception, script_name: str = "coindesk_crypto_news.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME, "email": ALERT_EMAIL, "logs": logs},
            timeout=10,
        )
    except Exception:
        pass


# ----------------------------------------------------------------------
# Аргументы командной строки
# ----------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Загрузчик новостей CoinDesk.com (RSS) в MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
parser.add_argument(
    "--max-clicks",
    type=int,
    default=None,
    help="Совместимость со старым запуском: теперь ограничивает число RSS-новостей, 0/не указано = все",
)
parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Максимум RSS-новостей за один запуск, 0/не указано = все",
)
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

if not re.fullmatch(r"[A-Za-z0-9_]+", args.table_name):
    print(" Ошибка: имя таблицы может содержать только латинские буквы, цифры и подчёркивание")
    sys.exit(1)

TABLE = f"`{args.table_name}`"

SQLALCHEMY_URL = URL.create(
    "mysql+mysqlconnector",
    username=args.user,
    password=args.password,
    host=args.host,
    port=int(args.port),
    database=args.database,
)
engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600, pool_pre_ping=True, future=True)


# ----------------------------------------------------------------------
# Вспомогательные функции
# ----------------------------------------------------------------------
def clean_text(value):
    if value is None:
        return ""
    value = html.unescape(str(value))
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def strip_cdata(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_datetime(value):
    if not value:
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    except Exception:
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def first_text(element, names):
    for name in names:
        found = element.find(name)
        if found is not None and found.text:
            return found.text
    # fallback для namespaced tags: сравниваем только локальную часть имени
    wanted = {n.split("}")[-1].split(":")[-1] for n in names}
    for child in list(element):
        local = child.tag.split("}")[-1].split(":")[-1]
        if local in wanted and child.text:
            return child.text
    return ""


def first_attr(element, tag_names, attr_names):
    wanted_tags = {n.split("}")[-1].split(":")[-1] for n in tag_names}
    wanted_attrs = {n.split("}")[-1].split(":")[-1] for n in attr_names}
    for child in list(element):
        local = child.tag.split("}")[-1].split(":")[-1]
        if local not in wanted_tags:
            continue
        for attr, value in child.attrib.items():
            attr_local = attr.split("}")[-1].split(":")[-1]
            if attr_local in wanted_attrs and value:
                return value
    return ""


def normalize_link(link):
    link = strip_cdata(link)
    if not link:
        return ""
    return urljoin(BASE_URL, link)


# ----------------------------------------------------------------------
# Таблица
# ----------------------------------------------------------------------
def ensure_table_exists(table_name):
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = :db AND table_name = :tbl
                """
            ),
            {"db": args.database, "tbl": table_name},
        )
        table_exists = result.scalar() > 0

        if not table_exists:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {TABLE} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        datetime DATETIME,
                        title TEXT,
                        source VARCHAR(255),
                        category VARCHAR(255),
                        description TEXT,
                        link VARCHAR(500),
                        published_at VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_link (link)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                    """
                )
            )
            print(f" Таблица '{table_name}' создана с автоинкрементом")
            return

        result = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.statistics
                WHERE table_schema = :db
                  AND table_name = :tbl
                  AND column_name = 'link'
                  AND non_unique = 0
                """
            ),
            {"db": args.database, "tbl": table_name},
        )
        has_unique = result.scalar() > 0

    if not has_unique:
        print(f" Внимание: в таблице '{table_name}' нет уникального индекса на поле link")
        print("Рекомендуется выполнить:")
        print(f"ALTER TABLE {TABLE} MODIFY link VARCHAR(500), ADD UNIQUE INDEX unique_link (link);")


def load_existing_links():
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT link FROM {TABLE}")).fetchall()
            return {row[0] for row in rows if row[0]}
    except Exception as e:
        print(f"  Ошибка при получении существующих ссылок: {e}")
        return set()


# ----------------------------------------------------------------------
# RSS
# ----------------------------------------------------------------------
def fetch_rss_xml():
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BrainServices/1.0; +https://brain-project.online)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Cache-Control": "no-cache",
    }
    response = requests.get(RSS_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.content


def parse_rss_items(xml_bytes, limit=0):
    root = ET.fromstring(xml_bytes)
    channel = root.find("channel")
    if channel is None:
        # Atom fallback
        items = root.findall("{http://www.w3.org/2005/Atom}entry") or root.findall("entry")
    else:
        items = channel.findall("item")

    parsed = []
    for item in items:
        title = clean_text(first_text(item, ["title", "{http://www.w3.org/2005/Atom}title"]))
        link = normalize_link(first_text(item, ["link", "{http://www.w3.org/2005/Atom}link"]))
        if not link:
            link = normalize_link(first_attr(item, ["link"], ["href"]))
        description = clean_text(
            first_text(
                item,
                [
                    "description",
                    "summary",
                    "{http://www.w3.org/2005/Atom}summary",
                    "{http://purl.org/rss/1.0/modules/content/}encoded",
                ],
            )
        )
        published_raw = clean_text(
            first_text(
                item,
                [
                    "pubDate",
                    "published",
                    "updated",
                    "{http://www.w3.org/2005/Atom}published",
                    "{http://www.w3.org/2005/Atom}updated",
                ],
            )
        )
        category = clean_text(first_text(item, ["category"])) or "Crypto News"

        if not title or not link:
            continue

        parsed.append(
            {
                "datetime": parse_datetime(published_raw),
                "title": title[:2000],
                "source": "CoinDesk",
                "category": category[:255],
                "description": description[:5000],
                "link": link[:500],
                "published_at": published_raw[:100],
            }
        )

        if limit and len(parsed) >= limit:
            break

    return parsed


def insert_news(rows, existing_links):
    new_rows = [row for row in rows if row["link"] not in existing_links]
    if not new_rows:
        return 0

    sql = text(
        f"""
        INSERT IGNORE INTO {TABLE}
            (`datetime`, title, source, category, description, link, published_at)
        VALUES
            (:datetime, :title, :source, :category, :description, :link, :published_at)
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql, new_rows)

    inserted = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else len(new_rows)
    existing_links.update(row["link"] for row in new_rows)
    return inserted


def parse_and_save_incrementally(table_name, limit=0):
    print("[*] Запуск парсера CoinDesk.com через RSS, без Playwright...")
    ensure_table_exists(table_name)

    print("[*] Загрузка существующих ссылок из БД...")
    existing_links = load_existing_links()
    print(f"  В БД уже есть {len(existing_links)} новостей")

    print(f"[*] Загрузка RSS: {RSS_URL}")
    xml_bytes = fetch_rss_xml()
    news = parse_rss_items(xml_bytes, limit=limit)

    print(f"  Получено из RSS: {len(news)}")
    if not news:
        print("  Новостей в RSS не найдено")
        return 0

    inserted = insert_news(news, existing_links)
    print(f"  Добавлено в БД: {inserted}")

    print("  Последние элементы RSS:")
    for item in news[:5]:
        date_label = item["published_at"] or str(item["datetime"])
        print(f"   - {date_label} | {item['title'][:90]}")

    try:
        with engine.connect() as conn:
            max_id = conn.execute(text(f"SELECT MAX(id) FROM {TABLE}")).scalar()
            print(f"  Последний ID в таблице: {max_id}")
    except Exception:
        pass

    return inserted


# ----------------------------------------------------------------------
# Основная логика
# ----------------------------------------------------------------------
def main():
    # Совместимость: --max-clicks теперь работает как лимит RSS-элементов,
    # потому что браузерные клики больше не используются.
    limit = args.limit
    if limit is None:
        limit = args.max_clicks
    if limit is None:
        limit = DEFAULT_LIMIT
    if limit < 0:
        limit = 0

    print(" Загрузчик новостей CoinDesk.com (крипто)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Целевая таблица: {args.table_name}")
    print(" Режим: RSS без Playwright")
    print(f" Лимит RSS-новостей: {'все элементы RSS' if not limit else limit}")
    print("=" * 60)

    total_added = parse_and_save_incrementally(args.table_name, limit=limit)

    print("=" * 60)
    print(" Загрузка завершена")
    if total_added == 0:
        print("ℹ Новых новостей нет или все элементы RSS уже были в БД")
    else:
        print(f" Успешно добавлено {total_added} новых записей")


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
        traceback.print_exc()
        send_error_trace(e)
        sys.exit(1)
