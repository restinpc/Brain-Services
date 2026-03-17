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
import os
import tempfile

# Создаём папку для временных файлов Playwright в домашней директории
TEMP_DIR = os.path.join(os.path.expanduser("~"), ".playwright-tmp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Перенаправляем все переменные окружения, влияющие на временные файлы
os.environ["PLAYWRIGHT_TMPDIR"] = TEMP_DIR
os.environ["TMPDIR"] = TEMP_DIR
os.environ["TEMP"] = TEMP_DIR
os.environ["TMP"] = TEMP_DIR

# Также говорим модулю tempfile использовать эту папку
tempfile.tempdir = TEMP_DIR

print(f"Временная директория Playwright: {TEMP_DIR}")
load_dotenv()

# ----------------------------------------------------------------------
# Конфигурация отправки ошибок (аналог trace.php)
# ----------------------------------------------------------------------
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "vlad_investing_crypto_news")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# ----------------------------------------------------------------------
# Параметры парсинга Investing.com
# ----------------------------------------------------------------------
BASE_URL = "https://ru.investing.com/news/cryptocurrency-news"
MAX_PAGES = 999  # Защита от бесконечного цикла (практически unlimited)


# ----------------------------------------------------------------------
# Функция отправки трейса об ошибке
# ----------------------------------------------------------------------
def send_error_trace(exc: Exception, script_name: str = "investing_crypto_news_loader.py"):
    """
    Отправляет информацию об исключении на сервер трейсов.
    """
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
        # Если не удалось отправить трейс – игнорируем
        pass


# ----------------------------------------------------------------------
# Парсинг аргументов командной строки
# ----------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Загрузчик новостей Investing.com (крипто, русский язык) в MySQL"
)
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
parser.add_argument("--max-pages", type=int, default=None,
                    help="Максимум страниц для парсинга (по умолчанию: все доступные)")

args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
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
# Асинхронный парсинг с постраничной загрузкой в БД
# ----------------------------------------------------------------------
async def parse_and_save_incrementally(table_name, max_pages=None):
    """
    Парсит страницы и сразу сохраняет в БД (постранично)
    Останавливается при достижении последней страницы или max_pages
    """
    print("[*] Запуск парсера Investing.com (headless mode)...")

    # Создаём таблицу если её нет
    ensure_table_exists(table_name)

    # Получаем список существующих ссылок ОДИН РАЗ в начале
    print("[*] Загрузка существующих ссылок из БД...")
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(f"SELECT link FROM {table_name}", conn)
            existing_links = set(existing['link'].tolist()) if not existing.empty else set()
        print(f"   ✓ В БД уже есть {len(existing_links)} новостей")
    except Exception as e:
        print(f"   ⚠️ Ошибка при получении существующих ссылок: {e}")
        existing_links = set()

    async with async_playwright() as p:
        # Headless-режим с stealth настройками
        browser = await p.chromium.launch(
            headless=True,
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
            locale='ru-RU',
            timezone_id='Europe/Moscow',
        )

        page = await context.new_page()

        # Скрываем webdriver
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        total_parsed = 0
        total_added = 0
        page_num = 1
        max_limit = max_pages if max_pages else MAX_PAGES
        consecutive_failures = 0  # Счётчик последовательных неудач
        MAX_CONSECUTIVE_FAILURES = 100  # Остановка после 10 неудач подряд

        while page_num <= max_limit and consecutive_failures < MAX_CONSECUTIVE_FAILURES:
            try:
                # Формируем URL
                if page_num == 1:
                    url = BASE_URL
                else:
                    url = f"{BASE_URL}/{page_num}"

                print(f"\n📄 Страница {page_num}: {url}")

                await page.goto(url, timeout=100000)
                await page.wait_for_load_state('networkidle')
                await asyncio.sleep(2)

                # Закрываем cookie-баннер на первой странице
                if page_num == 1:
                    try:
                        accept_btn = page.locator('button:has-text("I Accept"), button:has-text("Accept")')
                        if await accept_btn.is_visible(timeout=3000):
                            await accept_btn.click()
                            print("   ✓ Cookie-баннер закрыт")
                            await asyncio.sleep(1)
                    except:
                        pass

                # Проверяем наличие новостей
                selector = 'article[data-test="article-item"]'
                count = await page.locator(selector).count()

                if count == 0:
                    # Пробуем альтернативные селекторы
                    alternatives = [
                        '[data-test="article-item"]',
                        'article',
                        '.news-analysis-v2_article__wW0pT',
                    ]

                    for alt_selector in alternatives:
                        count = await page.locator(alt_selector).count()
                        if count >= 5:
                            selector = alt_selector
                            break

                    if count == 0:
                        consecutive_failures += 1
                        print(f"   ⚠️  Новости не найдены (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                        page_num += 1
                        await asyncio.sleep(2)
                        continue

                print(f"   ✓ Найдено новостей: {count}")

                news_items = await page.locator(selector).all()
                page_news = []

                for idx, item in enumerate(news_items, 1):
                    try:
                        # === ЗАГОЛОВОК И ССЫЛКА ===
                        title = None
                        href = None

                        # Пробуем data-test
                        try:
                            title_elem = item.locator('[data-test="article-title-link"]')
                            title = await title_elem.inner_text()
                            href = await title_elem.get_attribute('href')
                        except:

                            try:
                                link_elem = item.locator('a[href*="/article-"]').first
                                title = await link_elem.inner_text()
                                href = await link_elem.get_attribute('href')
                            except:

                                try:
                                    link_elem = item.locator('a').first
                                    title = await link_elem.inner_text()
                                    href = await link_elem.get_attribute('href')
                                except:
                                    continue

                        if title:
                            title = title.strip()
                        if href:
                            href = href.strip()

                        # Проверяем что не пустые после очистки
                        if not title or not href:
                            continue

                        if len(title) < 10:
                            continue

                        # Полный URL
                        if not href.startswith('http'):
                            href = f"https://ru.investing.com{href}"

                        title = title.strip()
                        if len(title) < 10:
                            continue

                        # Полный URL
                        if href and not href.startswith('http'):
                            href = f"https://ru.investing.com{href}"

                        # === ДАТА ===
                        published_at = None
                        try:
                            date_elem = item.locator('[data-test="article-publish-date"]')
                            published_at = await date_elem.inner_text()
                        except:
                            try:
                                date_elem = item.locator('time, span[class*="date"]').first
                                published_at = await date_elem.inner_text()
                            except:
                                pass

                        # Конвертируем в datetime (если нужно для БД)
                        dt = datetime.datetime.now()  # По умолчанию
                        if published_at:
                            # Здесь можно добавить парсинг "4 часа назад" -> datetime
                            # Пока оставляем как строку
                            pass

                        # === ИСТОЧНИК ===
                        source = "Investing.com"
                        try:
                            source_elem = item.locator('[data-test="news-provider-name"]')
                            source = await source_elem.inner_text()
                        except:
                            pass

                        # === ОПИСАНИЕ ===
                        description = None
                        try:
                            desc_elem = item.locator('[data-test="article-description"]')
                            description = await desc_elem.inner_text()
                        except:
                            pass

                        page_news.append({
                            'datetime': dt,
                            'title': title,
                            'source': source.strip(),
                            'description': description.strip() if description else '',
                            'link': href,
                            'published_at': published_at.strip() if published_at else ''
                        })

                    except Exception as e:
                        print(f"   ⚠️  Ошибка парсинга элемента {idx}: {e}")
                        continue

                # === СРАЗУ СОХРАНЯЕМ В БД ===
                if page_news:
                    df_page = pd.DataFrame(page_news)

                    # Фильтруем новые (которых нет в existing_links)
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

                            # Добавляем новые ссылки в set, чтобы не дублировать на следующих страницах
                            existing_links.update(df_new['link'].tolist())

                            total_added += len(df_new)
                            print(f"   ✅ Добавлено в БД: {len(df_new)} из {len(page_news)} (новые)")
                        except Exception as e:
                            print(f"   ❌ Ошибка записи в БД: {e}")
                            # Не прерываем парсинг, идём дальше
                    else:
                        print(f"   ℹ️  Все {len(page_news)} новостей уже есть в БД")

                    total_parsed += len(page_news)
                    consecutive_failures = 0  # Успешная страница - сбрасываем счётчик

                # Проверяем есть ли следующая страница
                try:
                    next_exists = await page.locator(f'a[href*="/news/cryptocurrency-news/{page_num + 1}"]').count()
                    if next_exists == 0:
                        print(f"   ℹ️  Последняя страница (нет ссылки на страницу {page_num + 1})")
                        break
                except:
                    # Если не можем проверить наличие следующей - считаем неудачей
                    consecutive_failures += 1
                    print(
                        f"   ⚠️  Не удалось проверить наличие следующей страницы (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")

                page_num += 1
                await asyncio.sleep(2)  # Пауза между страницами

            except Exception as e:
                consecutive_failures += 1
                print(
                    f"   ❌ Ошибка на странице {page_num}: {e} (неудача {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                # Не прерываем весь процесс, переходим к следующей странице
                page_num += 1
                await asyncio.sleep(2)
                continue

        await browser.close()

        print(f"\n{'=' * 60}")

        # Причина остановки
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(f"⚠️  Остановлено: {MAX_CONSECUTIVE_FAILURES} последовательных неудач")
        elif page_num > max_limit:
            print(f"ℹ️  Достигнут лимит страниц: {max_limit}")
        else:
            print(f"✅ Достигнута последняя страница")

        print(f"📄 Обработано страниц: {page_num - 1}")
        print(f"📊 Спарсено новостей: {total_parsed}")
        print(f"💾 Добавлено в БД: {total_added}")

        # Показываем последний ID
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
                max_id = result.scalar()
                print(f"🔢 Последний ID в таблице: {max_id}")
        except:
            pass

        return total_added


# ----------------------------------------------------------------------
# Функция для создания таблицы с автоинкрементным ID
# ----------------------------------------------------------------------
def ensure_table_exists(table_name):
    """
    Проверяет существование таблицы и создаёт её при необходимости
    """
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
        print(f"✅ Таблица '{table_name}' создана с автоинкрементом")
    else:
        # Проверяем наличие уникального индекса
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
            print(f"⚠️ Внимание: в таблице '{table_name}' нет уникального индекса на поле link")
            print("Рекомендуется добавить уникальный индекс командой:")
            print(f"ALTER TABLE {table_name} MODIFY link VARCHAR(500), ADD UNIQUE INDEX unique_link (link);")


# ----------------------------------------------------------------------
# Основная логика
# ----------------------------------------------------------------------
def main():
    print(f"🚀 Загрузчик новостей Investing.com (крипто, RU)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Целевая таблица: {args.table_name}")
    if args.max_pages:
        print(f"📄 Лимит страниц: {args.max_pages}")
    else:
        print(f"📄 Лимит страниц: все доступные (макс. {MAX_PAGES})")
    print("=" * 60)

    # Запускаем парсинг с постраничной загрузкой
    total_added = asyncio.run(
        parse_and_save_incrementally(args.table_name, max_pages=args.max_pages)
    )

    print("=" * 60)
    print("🏁 Загрузка завершена")

    if total_added == 0:
        print("ℹ️  Все новости уже были в БД")
    else:
        print(f"✨ Успешно добавлено {total_added} новых записей")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
