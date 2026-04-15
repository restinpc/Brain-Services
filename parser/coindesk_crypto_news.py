#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import traceback
import asyncio
import pandas as pd
from sqlalchemy import create_engine, text
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import tempfile

# ----------------------------------------------------------------------
# Настройка временной директории для Playwright
# ----------------------------------------------------------------------
TEMP_DIR = os.path.join(os.path.expanduser("~"), ".playwright-tmp")
os.makedirs(TEMP_DIR, exist_ok=True)
os.environ["PLAYWRIGHT_TMPDIR"] = TEMP_DIR
os.environ["TMPDIR"] = TEMP_DIR
os.environ["TEMP"] = TEMP_DIR
os.environ["TMP"] = TEMP_DIR
tempfile.tempdir = TEMP_DIR
print(f"Временная директория Playwright: {TEMP_DIR}")

load_dotenv()

# ----------------------------------------------------------------------
# Конфигурация отправки ошибок
# ----------------------------------------------------------------------
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "vlad_coindesk_crypto_news")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# ----------------------------------------------------------------------
# Параметры парсинга
# ----------------------------------------------------------------------
BASE_URL = "https://www.coindesk.com/latest-crypto-news"
MAX_CLICKS = 20  # лимит кликов "More Stories"

# ----------------------------------------------------------------------
# Селекторы для элементов на странице (можно легко обновить)
# ----------------------------------------------------------------------
SELECTORS = {
    'news_container': 'div[class*="flex gap-4"]',  # контейнер новости (уточнить!)
    'title_link': 'a[class*="content-card-title"]',  # ссылка с заголовком
    'category': 'a[class*="font-title"]',  # категория
    'description': 'p[class*="font-body"]',  # описание
    'date': 'span[class*="font-metadata"]',  # дата
    'more_button': 'button:has-text("More Stories")',  # кнопка "More Stories"
    'cookie_accept': 'button:has-text("Accept"), button:has-text("I Accept"), button:has-text("Got It")'
}


# ----------------------------------------------------------------------
# Функция отправки трейса об ошибке
# ----------------------------------------------------------------------
def send_error_trace(exc: Exception, script_name: str = "coindesk_crypto_news.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {
        "url": "cli_script",
        "node": NODE_NAME,
        "email": ALERT_EMAIL,
        "logs": logs
    }
    try:
        requests.post(TRACE_URL, data=payload, timeout=10)
    except Exception:
        pass


# ----------------------------------------------------------------------
# Парсинг аргументов командной строки
# ----------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Загрузчик новостей CoinDesk.com (крипто) в MySQL"
)
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
parser.add_argument("--max-clicks", type=int, default=None,
                    help="Максимум кликов на 'More Stories' (по умолчанию: все доступные)")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

# ----------------------------------------------------------------------
# Формируем строку подключения SQLAlchemy
# ----------------------------------------------------------------------
SQLALCHEMY_URL = (
    f"mysql+mysqlconnector://{args.user}:{args.password}@"
    f"{args.host}:{args.port}/{args.database}"
)
engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)


# ----------------------------------------------------------------------
# Асинхронный парсинг с бесконечным скроллом и загрузкой в БД
# ----------------------------------------------------------------------
async def parse_and_save_incrementally(table_name, max_clicks=None):
    print("[*] Запуск парсера CoinDesk.com (headless mode)...")
    ensure_table_exists(table_name)

    print("[*] Загрузка существующих ссылок из БД...")
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(f"SELECT link FROM {table_name}", conn)
            existing_links = set(existing['link'].tolist()) if not existing.empty else set()
        print(f"  В БД уже есть {len(existing_links)} новостей")
    except Exception as e:
        print(f"  Ошибка при получении существующих ссылок: {e}")
        existing_links = set()

    async with async_playwright() as p:
        # Запускаем браузер (headless=False для отладки, потом можно сменить на True)
        browser = await p.chromium.launch(
            headless=True,  # при отладке видим окно, потом можно True
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
        )
        page = await context.new_page()
        # Скрываем автоматизацию
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        total_parsed = 0
        total_added = 0
        click_num = 0
        max_limit = max_clicks if max_clicks else MAX_CLICKS
        consecutive_failures = 0
        MAX_CONSECUTIVE_FAILURES = 10

        try:
            print(f"\n Загрузка главной страницы: {BASE_URL}")
            # Ждём только загрузки DOM, не всех ресурсов (это быстро)
            await page.goto(BASE_URL, wait_until='domcontentloaded', timeout=120000)
            print("  DOM загружен. Ожидаем 30 секунд для подгрузки динамического контента...")
            await asyncio.sleep(30)

            # Закрываем cookie-баннер если есть
            try:
                accept_btn = page.locator(SELECTORS['cookie_accept'])
                if await accept_btn.is_visible(timeout=3000):
                    await accept_btn.click()
                    print("  Cookie-баннер закрыт")
                    await asyncio.sleep(1)
            except:
                pass

            # Основной цикл подгрузки новостей
            while click_num < max_limit and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
                # Парсим текущие новости (не дожидаясь специально)
                news_items = await page.locator(SELECTORS['news_container']).all()
                print(f"   Найдено элементов новостей: {len(news_items)}")

                page_news = []
                for idx, item in enumerate(news_items, 1):
                    try:
                        # Заголовок и ссылка
                        title_elem = item.locator(SELECTORS['title_link'])
                        title = await title_elem.inner_text(timeout=1000) if await title_elem.count() > 0 else None
                        href = await title_elem.get_attribute('href', timeout=1000) if title else None

                        if not title or not href:
                            continue
                        title = title.strip()
                        if len(title) < 10:
                            continue
                        if not href.startswith('http'):
                            href = f"https://www.coindesk.com{href}"

                        # Категория
                        cat_elem = item.locator(SELECTORS['category'])
                        category = await cat_elem.inner_text(timeout=1000) if await cat_elem.count() > 0 else "News"

                        # Описание
                        desc_elem = item.locator(SELECTORS['description'])
                        description = await desc_elem.inner_text(timeout=1000) if await desc_elem.count() > 0 else ""

                        # Дата
                        date_elem = item.locator(SELECTORS['date'])
                        published_at = await date_elem.inner_text(timeout=1000) if await date_elem.count() > 0 else ""

                        dt = datetime.datetime.now()  # можно заменить на парсинг даты

                        page_news.append({
                            'datetime': dt,
                            'title': title,
                            'source': 'CoinDesk',
                            'category': category.strip(),
                            'description': description.strip(),
                            'link': href,
                            'published_at': published_at.strip()
                        })
                    except Exception as e:
                        print(f"  Ошибка парсинга элемента {idx}: {e}")
                        continue

                # Сохраняем новые записи в БД
                if page_news:
                    df_page = pd.DataFrame(page_news)
                    df_new = df_page[~df_page['link'].isin(existing_links)]
                    if not df_new.empty:
                        try:
                            df_new.to_sql(
                                name=table_name,
                                con=engine,
                                if_exists='append',
                                index=False,
                                chunksize=100,
                                method='multi'
                            )
                            existing_links.update(df_new['link'].tolist())
                            total_added += len(df_new)
                            print(f"  Добавлено в БД: {len(df_new)} из {len(page_news)} (новые) на клике {click_num}")
                        except Exception as e:
                            print(f"  Ошибка записи в БД: {e}")
                    else:
                        print(f" ℹ Все {len(page_news)} новостей уже есть в БД на клике {click_num}")
                    total_parsed += len(page_news)
                    consecutive_failures = 0

                # Кликаем на "More Stories"
                try:
                    load_more_btn = page.locator(SELECTORS['more_button'])
                    # Проверяем видимость кнопки
                    if not await load_more_btn.is_visible(timeout=5000):
                        print(" ℹ Кнопка 'More Stories' больше не видна. Остановка.")
                        break
                    await load_more_btn.click()

                    # Ждём фиксированное время для загрузки новых статей
                    await asyncio.sleep(5)

                    click_num += 1
                    print(f"  Клик на 'More Stories' {click_num}/{max_limit}")
                except Exception as e:
                    consecutive_failures += 1
                    print(f"  Ошибка клика (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {e}")
                    # Делаем скриншот при ошибке клика
                    await page.screenshot(path=f"error_click_{click_num}.png")
                    await asyncio.sleep(2)
                    continue

        except Exception as e:
            print(f"  Критическая ошибка в цикле: {e}")
            await page.screenshot(path="critical_error.png")
            send_error_trace(e)

        await browser.close()

        # Итоговая статистика
        print(f"\n{'=' * 60}")
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(f" Остановлено: {MAX_CONSECUTIVE_FAILURES} последовательных неудач")
        elif click_num >= max_limit:
            print(f"ℹ Достигнут лимит кликов: {max_limit}")
        else:
            print(f" Все новости загружены")
        print(f" Обработано кликов: {click_num}")
        print(f" Спарсено новостей: {total_parsed}")
        print(f" Добавлено в БД: {total_added}")
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
                max_id = result.scalar()
                print(f" Последний ID в таблице: {max_id}")
        except:
            pass
        return total_added


# ----------------------------------------------------------------------
# Функция для создания таблицы
# ----------------------------------------------------------------------
def ensure_table_exists(table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{args.database}' AND table_name = '{table_name}'
        """))
        table_exists = result.scalar() > 0

    if not table_exists:
        create_query = text(f"""
        CREATE TABLE {table_name} (
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
        """)
        with engine.connect() as conn:
            conn.execute(create_query)
            conn.commit()
        print(f" Таблица '{table_name}' создана с автоинкрементом")
    else:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.statistics
                WHERE table_schema = '{args.database}'
                AND table_name = '{table_name}'
                AND column_name = 'link'
                AND non_unique = 0
            """))
            has_unique = result.scalar() > 0
        if not has_unique:
            print(f" Внимание: в таблице '{table_name}' нет уникального индекса на поле link")
            print("Рекомендуется добавить уникальный индекс командой:")
            print(f"ALTER TABLE {table_name} MODIFY link VARCHAR(500), ADD UNIQUE INDEX unique_link (link);")


# ----------------------------------------------------------------------
# Основная логика
# ----------------------------------------------------------------------
def main():
    print(f" Загрузчик новостей CoinDesk.com (крипто)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Целевая таблица: {args.table_name}")
    if args.max_clicks:
        print(f" Лимит кликов: {args.max_clicks}")
    else:
        print(f" Лимит кликов: все доступные (макс. {MAX_CLICKS})")
    print("=" * 60)
    total_added = asyncio.run(
        parse_and_save_incrementally(args.table_name, max_clicks=args.max_clicks)
    )
    print("=" * 60)
    print(" Загрузка завершена")
    if total_added == 0:
        print("ℹ Все новости уже были в БД")
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
        send_error_trace(e)
        sys.exit(1)
