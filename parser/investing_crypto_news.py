"""
Запуск:
  python investing_crypto_news_loader.py vlad_investing_crypto_news [host] [port] [user] [password] [database]
"""

import os
import sys
import argparse
import datetime
import traceback
import asyncio
import random
import pandas as pd
from sqlalchemy import create_engine, text
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import tempfile
import shutil
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urlunparse

# Playwright temp dir
TEMP_DIR = os.path.join(os.path.expanduser("~"), ".playwright-tmp")
os.makedirs(TEMP_DIR, exist_ok=True)
os.environ["PLAYWRIGHT_TMPDIR"] = TEMP_DIR
os.environ["TMPDIR"] = TEMP_DIR
os.environ["TEMP"] = TEMP_DIR
os.environ["TMP"] = TEMP_DIR
tempfile.tempdir = TEMP_DIR

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "vlad_investing_crypto_news")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

BASE_URL = "https://ru.investing.com/news/cryptocurrency-news"
INVESTING_CRYPTO_RSS = "https://www.investing.com/rss/news_301.rss"
HTTP_PROXY_HOST = os.getenv("HTTP_PROXY_HOST", "10.10.11.1")
HTTP_PROXY_PORT = int(os.getenv("HTTP_PROXY_PORT", "3128"))
HTTP_PROXY_URL = f"http://{HTTP_PROXY_HOST}:{HTTP_PROXY_PORT}" if HTTP_PROXY_HOST else None
MAX_PAGES = 999
MAX_CONSECUTIVE_FAILURES = 15  # Больше попыток — Cloudflare иногда пропускает
CONTEXT_REFRESH_EVERY = 5  # Чаще обновляем context

# Разные User-Agent для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


def send_error_trace(exc: Exception, script_name: str = "investing_crypto_news_loader.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": ALERT_EMAIL, "logs": logs}, timeout=10)
    except:
        pass


def should_stop_on_http_status(status: int) -> bool:
    """Do not retry access-denied or rate-limited responses in the same run."""
    return status in (403, 429, 503)


parser = argparse.ArgumentParser(description="Investing.com crypto news → MySQL")
parser.add_argument("table_name", help="Имя таблицы")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
parser.add_argument("--max-pages", type=int, default=None)
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: параметры БД"); sys.exit(1)

SQLALCHEMY_URL = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)


def ensure_table_exists(table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = '{args.database}' AND table_name = '{table_name}'
        """))
        if result.scalar() == 0:
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    datetime DATETIME,
                    title TEXT,
                    source VARCHAR(255),
                    description TEXT,
                    link VARCHAR(500),
                    published_at VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_link (link)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """))
            conn.commit()
            print(f" Таблица '{table_name}' создана")


async def human_like_behavior(page):
    """Имитация человеческого поведения — scroll + mouse move"""
    try:
        # Случайный скролл вниз
        scroll_y = random.randint(300, 800)
        await page.evaluate(f"window.scrollBy(0, {scroll_y})")
        await asyncio.sleep(random.uniform(0.3, 0.8))

        # Движение мыши
        await page.mouse.move(
            random.randint(100, 800),
            random.randint(100, 600)
        )
        await asyncio.sleep(random.uniform(0.2, 0.5))
    except:
        pass


async def create_context(browser):
    """Создаёт новый browser context с рандомным User-Agent"""
    ua = random.choice(USER_AGENTS)
    context = await browser.new_context(
        user_agent=ua,
        viewport={'width': random.choice([1366, 1920, 1536, 1440]), 'height': random.choice([768, 1080, 864, 900])},
        locale='ru-RU',
        timezone_id='Europe/Moscow',
        java_script_enabled=True,
    )
    page = await context.new_page()

    # Скрываем webdriver
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)

    return context, page


def canonical_investing_link(link):
    parts = urlparse(link)
    if parts.netloc.lower() in ("www.investing.com", "investing.com"):
        parts = parts._replace(netloc="ru.investing.com")
    return urlunparse(parts)


def fetch_crypto_rss():
    proxies = {"http": HTTP_PROXY_URL, "https": HTTP_PROXY_URL} if HTTP_PROXY_URL else None
    response = requests.get(INVESTING_CRYPTO_RSS, proxies=proxies, timeout=(10, 30),
                            headers={"User-Agent": USER_AGENTS[0], "Accept": "application/rss+xml, application/xml;q=0.9, */*"})
    response.raise_for_status()
    root = ET.fromstring(response.content)
    items = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        if not link.startswith("http"):
            link = "https://www.investing.com" + (link if link.startswith("/") else "/" + link)
        items.append({
            "datetime": datetime.datetime.now(),
            "title": title,
            "source": (item.findtext("source") or "Investing.com").strip(),
            "description": (item.findtext("description") or "").strip(),
            "link": canonical_investing_link(link),
            "published_at": (item.findtext("pubDate") or "").strip(),
        })
    return items


async def parse_and_save_incrementally(table_name, max_pages=None):
    print("[*] Запуск парсера Investing.com (v2 — anti-detect)...")
    ensure_table_exists(table_name)

    # Загружаем существующие ссылки
    print("[*] Загрузка существующих ссылок из БД...")
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(f"SELECT link FROM {table_name}", conn)
            existing_links = set(existing['link'].tolist()) if not existing.empty else set()
        print(f"    В БД уже есть {len(existing_links)} новостей")
    except:
        existing_links = set()

    print(f"[*] Получение официальной RSS-ленты Investing.com: {INVESTING_CRYPTO_RSS}")
    rss_news = fetch_crypto_rss()
    rss_frame = pd.DataFrame(rss_news)
    if not rss_frame.empty:
        rss_frame = rss_frame.drop_duplicates(subset=["link"])
        new_frame = rss_frame[~rss_frame["link"].isin(existing_links)]
        if not new_frame.empty:
            new_frame.to_sql(name=table_name, con=engine, if_exists="append", index=False, chunksize=100, method="multi")
        total_added = len(new_frame)
    else:
        total_added = 0
    print(f" Получено из RSS: {len(rss_news)}")
    print(" Обработано страниц: 1")
    print(f" Спарсено новостей: {len(rss_news)}")
    print(f" Добавлено в БД: {total_added}")
    print(" Загрузка завершена")
    return total_added

    async with async_playwright() as p:
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--disable-background-timer-throttling',
        ]
        try:
            browser = await p.chromium.launch(headless=True, args=browser_args)
        except Exception as launch_error:
            if 'Executable doesn\'t exist' not in str(launch_error):
                raise
            system_chromium = (shutil.which('chromium-browser')
                               or shutil.which('chromium')
                               or shutil.which('google-chrome'))
            if not system_chromium:
                raise
            print(f'[WARN] Playwright browser bundle missing; using system Chromium: {system_chromium}')
            browser = await p.chromium.launch(
                headless=True, executable_path=system_chromium, args=browser_args)

        context, page = await create_context(browser)

        total_parsed = 0
        total_added = 0
        page_num = 1
        max_limit = max_pages if max_pages else MAX_PAGES
        consecutive_failures = 0

        while page_num <= max_limit and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
            try:
                # Обновляем context каждые N страниц (сброс cookies, fingerprint)
                if page_num > 1 and (page_num - 1) % CONTEXT_REFRESH_EVERY == 0:
                    await context.close()
                    print(f"    Обновление browser context (каждые {CONTEXT_REFRESH_EVERY} стр)")
                    await asyncio.sleep(random.uniform(3, 6))
                    context, page = await create_context(browser)

                url = BASE_URL if page_num == 1 else f"{BASE_URL}/{page_num}"
                print(f"\n Страница {page_num}: {url}")

                #  FIX #1: domcontentloaded вместо load/networkidle 
                # networkidle ждёт пока ВСЕ сетевые запросы завершатся — 
                # на Investing.com это часто не происходит из-за рекламных скриптов,
                # tracking pixels, websocket connections → timeout 30 сек.
                # domcontentloaded срабатывает как только HTML распарсен — 
                # новости уже в DOM, JS-трекеры нам не нужны.
                response = await page.goto(url, timeout=30000, wait_until='domcontentloaded')

                # Do not retry access-denied or rate-limited responses by
                # rotating browser contexts. Let the next scheduled run retry.
                if response and response.status >= 400:
                    consecutive_failures += 1
                    status = response.status
                    print(f"    HTTP {status} (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")

                    if should_stop_on_http_status(status):
                        print(f"    SOURCE_FETCH_STOPPED_HTTP_{status}; повтор будет в следующем штатном запуске")
                        break
                    else:
                        page_num += 1
                        await asyncio.sleep(random.uniform(5, 10))
                        continue

                #  FIX #2: ждём появления статей, не networkidle 
                try:
                    await page.wait_for_selector('article[data-test="article-item"], article', timeout=10000)
                except:
                    pass  # Может не быть — проверим ниже

                #  FIX #3: Human-like behavior 
                await human_like_behavior(page)
                await asyncio.sleep(random.uniform(1, 2))

                # Cookie-баннер (только первая страница)
                if page_num == 1:
                    try:
                        btn = page.locator('button:has-text("I Accept"), button:has-text("Accept"), #onetrust-accept-btn-handler')
                        if await btn.is_visible(timeout=3000):
                            await btn.click()
                            await asyncio.sleep(1)
                    except:
                        pass

                # Ищем статьи
                selector = 'article[data-test="article-item"]'
                count = await page.locator(selector).count()

                if count == 0:
                    for alt in ['[data-test="article-item"]', 'article', '.news-analysis-v2_article__wW0pT']:
                        count = await page.locator(alt).count()
                        if count >= 3:
                            selector = alt
                            break

                if count == 0:
                    consecutive_failures += 1
                    print(f"    Новости не найдены (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")

                    # Проверяем: может это Cloudflare challenge page?
                    page_content = await page.content()
                    is_cf_block = "Cloudflare" in page_content or "cf-browser-verification" in page_content or "challenge-platform" in page_content

                    if is_cf_block or consecutive_failures >= 3:
                        try:
                            await context.close()
                        except:
                            pass
                        wait = random.uniform(20, 40)
                        print(f"    {'Cloudflare challenge' if is_cf_block else 'Серия неудач'} — пауза {wait:.0f}с + новый context")
                        await asyncio.sleep(wait)
                        context, page = await create_context(browser)
                        # Retry ту же страницу
                        continue

                    page_num += 1
                    await asyncio.sleep(random.uniform(5, 10))
                    continue

                # Успех — сбрасываем failures
                print(f"    Найдено новостей: {count}")
                consecutive_failures = 0

                news_items = await page.locator(selector).all()
                page_news = []

                for idx, item in enumerate(news_items, 1):
                    try:
                        title = href = None

                        for link_sel in ['[data-test="article-title-link"]', 'a[href*="/article-"]', 'a[href*="/news/"]', 'a']:
                            try:
                                elem = item.locator(link_sel).first
                                title = await elem.inner_text(timeout=3000)
                                href = await elem.get_attribute('href')
                                if title and href and len(title.strip()) >= 10:
                                    break
                            except:
                                continue

                        if not title or not href or len(title.strip()) < 10:
                            continue

                        title = title.strip()
                        href = href.strip()
                        if not href.startswith('http'):
                            href = f"https://ru.investing.com{href}"

                        # Дата
                        published_at = None
                        try:
                            published_at = await item.locator('[data-test="article-publish-date"], time, span[class*="date"]').first.inner_text(timeout=2000)
                        except:
                            pass

                        # Источник
                        source = "Investing.com"
                        try:
                            source = await item.locator('[data-test="news-provider-name"]').first.inner_text(timeout=2000)
                        except:
                            pass

                        # Описание
                        description = None
                        try:
                            description = await item.locator('[data-test="article-description"]').first.inner_text(timeout=2000)
                        except:
                            pass

                        page_news.append({
                            'datetime': datetime.datetime.now(),
                            'title': title,
                            'source': source.strip() if source else 'Investing.com',
                            'description': description.strip() if description else '',
                            'link': href,
                            'published_at': published_at.strip() if published_at else ''
                        })
                    except:
                        continue

                # Сохраняем
                if page_news:
                    df_page = pd.DataFrame(page_news)
                    df_new = df_page[~df_page['link'].isin(existing_links)]

                    if not df_new.empty:
                        try:
                            df_new.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=100, method='multi')
                            existing_links.update(df_new['link'].tolist())
                            total_added += len(df_new)
                            print(f"    Добавлено: {len(df_new)} из {len(page_news)} (новые)")
                        except Exception as e:
                            print(f"    Ошибка записи: {e}")
                    else:
                        print(f"   ℹ Все {len(page_news)} уже в БД")
                        # Если 3 страницы подряд без новых — мы догнали до existing данных
                        # Можно остановиться если нужен только incremental

                    total_parsed += len(page_news)

                page_num += 1

                #  FIX #5: рандомная пауза 3–7 сек (не 2) 
                await asyncio.sleep(random.uniform(3, 7))

            except Exception as e:
                consecutive_failures += 1
                err_short = str(e)[:80]
                print(f"    Ошибка: {err_short} (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")

                # При timeout — пауза + новый context
                if "timeout" in str(e).lower() or "Timeout" in str(e):
                    await asyncio.sleep(random.uniform(8, 15))
                    if consecutive_failures >= 2:
                        try:
                            await context.close()
                        except:
                            pass
                        context, page = await create_context(browser)
                        print(f"    Новый context после timeout")

                page_num += 1
                continue

        await browser.close()

        print(f"\n{'=' * 60}")
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(f" Остановлено: {MAX_CONSECUTIVE_FAILURES} неудач подряд")
        print(f" Обработано страниц: {page_num - 1}")
        print(f" Спарсено новостей: {total_parsed}")
        print(f" Добавлено в БД: {total_added}")

        try:
            with engine.connect() as conn:
                max_id = conn.execute(text(f"SELECT MAX(id) FROM {table_name}")).scalar()
                print(f" Последний ID: {max_id}")
        except:
            pass

        return total_added


def main():
    print(f" Investing.com Crypto News (v2 — anti-detect)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}")
    print(f" Лимит: {args.max_pages or MAX_PAGES} страниц")
    print("=" * 60)

    total = asyncio.run(parse_and_save_incrementally(args.table_name, max_pages=args.max_pages))

    print("=" * 60)
    print(" Завершено")
    if total == 0:
        print("ℹ Все новости уже в БД")
    else:
        print(f" Добавлено {total} новых")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n Прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\n {e!r}")
        send_error_trace(e)
        sys.exit(1)
