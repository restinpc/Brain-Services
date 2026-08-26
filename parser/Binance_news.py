import os
import sys
import argparse
import time
import random
import re
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import mysql.connector
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

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
    payload = {"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}
    print(f"\n[POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"[POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"[POST] Не удалось отправить отчёт: {e}")


parser = argparse.ArgumentParser(description="Binance Square News → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("Ошибка: не указаны все параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

SETTINGS = {
    # Основной путь: внутренние read-only JSON endpoints Binance Square.
    # Они не являются официальным публичным API, поэтому ниже оставлен DOM fallback.
    "article_api": "https://www.binance.com/bapi/composite/v3/friendly/pgc/content/article/list",
    "news_api": "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/news/list",
    "base_url": "https://www.binance.com/en/square/news/all",
    "pages_latest": 3,
    "pages_news": 3,
    "pages_trending": 1,
    "page_size": 20,
    "request_delay": 0.55,
    "timeout": 30,
    "scrolls": 3,
    "pause_after_scroll": 2.5,
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    ),
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


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
        # Обновляем старую строку, если Binance позже дополнил title/content/author/date.
        sql = f"""
        INSERT INTO `{self.table_name}` (link, title, full_text, preview, date, author)
        VALUES (%(link)s, %(title)s, %(full_text)s, %(preview)s, %(date)s, %(author)s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            full_text = CASE
                WHEN CHAR_LENGTH(VALUES(full_text)) > CHAR_LENGTH(COALESCE(full_text, ''))
                THEN VALUES(full_text) ELSE full_text END,
            preview = CASE
                WHEN CHAR_LENGTH(VALUES(preview)) > CHAR_LENGTH(COALESCE(preview, ''))
                THEN VALUES(preview) ELSE preview END,
            date = COALESCE(NULLIF(VALUES(date), ''), date),
            author = COALESCE(NULLIF(VALUES(author), ''), author)
        """
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, row)
            affected = cursor.rowcount
            conn.commit()
            return affected > 0


def _strip_html(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if "<" in text and ">" in text:
        text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _format_timestamp(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        value = value.strip()
        if re.fullmatch(r"\d{10,13}", value):
            try:
                value = int(value)
            except ValueError:
                pass
        else:
            # ISO-like timestamps are already good enough for storage.
            m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})", value)
            return f"{m.group(1)} {m.group(2)}" if m else value[:32]
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:  # milliseconds
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OSError, OverflowError):
            return ""
    return ""


def _walk(obj: Any) -> Iterable[Dict[str, Any]]:
    """Находит post-like dict независимо от того, как Binance вложил data/list/items."""
    if isinstance(obj, dict):
        keys = set(obj.keys())
        looks_like_post = (
            ("id" in keys or "postId" in keys or "articleId" in keys)
            and ("title" in keys or "content" in keys or "webLink" in keys)
            and ("date" in keys or "createTime" in keys or "publishTime" in keys or "cardType" in keys)
        )
        if looks_like_post:
            yield obj
        for value in obj.values():
            yield from _walk(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk(value)


def _post_id(post: Dict[str, Any]) -> str:
    for key in ("id", "postId", "articleId", "contentId"):
        val = post.get(key)
        if val not in (None, ""):
            return str(val)
    return ""


def normalize_api_post(post: Dict[str, Any]) -> Optional[Dict[str, str]]:
    pid = _post_id(post)
    if not pid:
        return None

    title = _strip_html(post.get("title") or post.get("subTitle") or "")
    content = _strip_html(
        post.get("content")
        or post.get("body")
        or post.get("summary")
        or post.get("description")
        or ""
    )
    if not title and not content:
        return None

    link = str(post.get("webLink") or post.get("shareUrl") or post.get("url") or "").strip()
    if not link:
        link = f"https://www.binance.com/en/square/post/{pid}"
    elif link.startswith("/"):
        link = "https://www.binance.com" + link

    author = _strip_html(
        post.get("authorName")
        or post.get("author")
        or post.get("nickName")
        or post.get("nickname")
        or "Binance Square"
    )
    if len(author) > 100:
        author = author[:100]

    raw_date = (
        post.get("date")
        or post.get("createTime")
        or post.get("publishTime")
        or post.get("createdAt")
        or post.get("releaseTime")
    )
    date_str = _format_timestamp(raw_date)

    full_text = content[:10000]
    preview_source = full_text or title
    preview = (preview_source[:300] + "...") if len(preview_source) > 300 else preview_source

    return {
        "link": link[:255],
        "title": title,
        "full_text": full_text,
        "preview": preview,
        "date": date_str,
        "author": author or "Binance Square",
    }


def fetch_json(session: requests.Session, url: str, params: Dict[str, Any]) -> Optional[Any]:
    try:
        r = session.get(url, params=params, timeout=SETTINGS["timeout"])
        if r.status_code != 200:
            log(f"API HTTP {r.status_code}: {r.url}")
            return None
        ctype = r.headers.get("content-type", "")
        if "json" not in ctype.lower() and not r.text.lstrip().startswith(("{", "[")):
            log(f"API вернул не JSON ({ctype}): {r.url}")
            return None
        data = r.json()
        # Binance обычно code=000000/0; не блокируем неизвестные успешные варианты.
        code = data.get("code") if isinstance(data, dict) else None
        if code not in (None, 0, "0", "000000"):
            log(f"API code={code}: {data.get('message') or data.get('msg') or ''}")
        return data
    except Exception as e:
        log(f"API ошибка: {e}")
        return None


def collect_from_api() -> Dict[str, Dict[str, str]]:
    headers = {
        "User-Agent": SETTINGS["user_agent"],
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.binance.com/en/square/news/all",
        "Accept-Language": "en-US,en;q=0.9",
    }
    session = requests.Session()
    session.headers.update(headers)
    unique: Dict[str, Dict[str, str]] = {}

    jobs: List[Tuple[str, Dict[str, Any], int, str]] = [
        (SETTINGS["article_api"], {"pageSize": SETTINGS["page_size"], "type": 2}, SETTINGS["pages_latest"], "latest"),
        (SETTINGS["news_api"], {"pageSize": SETTINGS["page_size"]}, SETTINGS["pages_news"], "news"),
        (SETTINGS["article_api"], {"pageSize": SETTINGS["page_size"], "type": 1}, SETTINGS["pages_trending"], "trending"),
    ]

    for url, base_params, pages, label in jobs:
        empty_pages = 0
        for page_index in range(1, pages + 1):
            params = dict(base_params)
            params["pageIndex"] = page_index
            data = fetch_json(session, url, params)
            posts = list(_walk(data)) if data is not None else []
            accepted = 0
            for post in posts:
                row = normalize_api_post(post)
                if not row:
                    continue
                # URL является устойчивым dedup key и совместим со старой таблицей.
                unique[row["link"]] = row
                accepted += 1
            log(f"API {label} page {page_index}: найдено {accepted}")
            if accepted == 0:
                empty_pages += 1
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0
            time.sleep(SETTINGS["request_delay"])

    log(f"API: {len(unique)} уникальных новостей")
    return unique


def collect_links_dom(page, max_scrolls: int) -> Dict[str, Tuple[str, str]]:
    """Fallback: DOM, без хрупкой regex по HTML."""
    log(f"DOM fallback: скроллим {max_scrolls} раз...")
    for i in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(SETTINGS["pause_after_scroll"] + random.uniform(0, 0.8))
        log(f"  скролл {i + 1}/{max_scrolls}")

    page.wait_for_timeout(3000)
    log(f"DOM URL: {page.url}")
    log(f"DOM title: {page.title()[:160]}")

    anchors = page.locator('a[href*="/square/post/"]')
    count = anchors.count()
    log(f"DOM ссылок /square/post/: {count}")

    unique: Dict[str, Tuple[str, str]] = {}
    for i in range(count):
        a = anchors.nth(i)
        try:
            href = a.get_attribute("href") or ""
            m = re.search(r"/square/post/(\d+)", href)
            if not m:
                continue
            pid = m.group(1)
            title = re.sub(r"\s+", " ", (a.inner_text(timeout=1500) or "").strip())
            if len(title) < 8:
                title = (a.get_attribute("aria-label") or a.get_attribute("title") or "").strip()
            if len(title) < 8:
                continue
            link = "https://www.binance.com" + href if href.startswith("/") else href
            unique[pid] = (link, title)
        except Exception:
            continue

    if not unique:
        try:
            body = re.sub(r"\s+", " ", page.locator("body").inner_text(timeout=3000))[:500]
            log(f"DOM diagnostic body: {body}")
        except Exception:
            pass
    log(f"DOM: найдено {len(unique)} уникальных новостей")
    return unique


def extract_full_text(page) -> Tuple[str, str, str]:
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "div[class*='post-content']", "div[class*='article-body']",
        "div[class*='content-body']", "article", "main p",
    ]
    full_text = ""
    for sel in selectors:
        block = soup.select_one(sel)
        if block:
            for unwanted in block.select("script, style, header, footer, nav, button"):
                unwanted.decompose()
            full_text = block.get_text(separator="\n", strip=True)
            break
    if not full_text:
        paragraphs = soup.find_all("p")
        full_text = "\n".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)
    full_text = re.sub(r"\s+", " ", full_text).strip()[:10000]

    author = "Binance Square"
    author_elem = soup.find(class_=re.compile(r"author|username|creator|byline", re.I))
    if author_elem:
        author = author_elem.get_text(" ", strip=True)[:100]

    date_str = ""
    time_tag = soup.find("time")
    if time_tag:
        date_str = _format_timestamp(time_tag.get("datetime") or time_tag.get_text(strip=True))
    return full_text, author, date_str


def collect_from_dom_fallback() -> Dict[str, Dict[str, str]]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("Playwright не установлен; DOM fallback пропущен")
        return {}

    rows: Dict[str, Dict[str, str]] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=SETTINGS["user_agent"], locale="en-US", timezone_id="UTC")
        page = context.new_page()
        log(f"DOM fallback: открываю {SETTINGS['base_url']}")
        page.goto(SETTINGS["base_url"], wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)
        links = collect_links_dom(page, SETTINGS["scrolls"])
        for idx, (_pid, (link, title)) in enumerate(links.items(), 1):
            try:
                page.goto(link, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1800)
                full_text, author, date_str = extract_full_text(page)
                preview_source = full_text or title
                rows[link] = {
                    "link": link[:255],
                    "title": title,
                    "full_text": full_text,
                    "preview": (preview_source[:300] + "...") if len(preview_source) > 300 else preview_source,
                    "date": date_str,
                    "author": author,
                }
                if idx % 10 == 0:
                    log(f"DOM обработано: {idx}/{len(links)}")
            except Exception as e:
                log(f"DOM ошибка {link}: {e}")
        browser.close()
    return rows


def main() -> int:
    db = DB(args.table_name)
    db.ensure_table()
    log(f"Целевая таблица: {args.database}.{args.table_name}")

    rows = collect_from_api()
    mode = "API"
    if not rows:
        log("API не вернул новостей — запускаю Playwright DOM fallback")
        rows = collect_from_dom_fallback()
        mode = "DOM"

    changed = 0
    for row in rows.values():
        if db.upsert_single(row):
            changed += 1

    with db.get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{args.table_name}` WHERE full_text IS NOT NULL AND full_text != ''")
        count_with_text = cur.fetchone()[0]

    log(f"Режим={mode}; Получено={len(rows)}; Добавлено/обновлено={changed}")
    log(f"Всего записей с текстом: {count_with_text}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\nКритическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
