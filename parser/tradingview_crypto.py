#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import traceback
import pandas as pd
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------------------------------------
# Конфигурация отправки ошибок (аналог trace.php)
# ----------------------------------------------------------------------
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "vlad_crypto_news_loader")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# ----------------------------------------------------------------------
# Параметры TradingView API (константы)
# ----------------------------------------------------------------------
LIST_URL = "https://news-mediator.tradingview.com/news-flow/v2/news"
STORY_URL = "https://news-mediator.tradingview.com/public/news/v1/story"


# ----------------------------------------------------------------------
# Функция отправки трейса об ошибке
# ----------------------------------------------------------------------
def send_error_trace(exc: Exception, script_name: str = "vlad_crypto_news.py"):
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
    description="Загрузчик новостей TradingView (крипто, русский язык) в MySQL"
)
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))

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
# Вспомогательная функция для извлечения текста из AST‑описания
# ----------------------------------------------------------------------
def extract_text(node):
    if not isinstance(node, dict):
        return ''
    if node.get('type') == 'text':
        return node.get('text', '') + ' '
    text = ''
    children = node.get('children', [])
    for child in children:
        if isinstance(child, str):
            text += child + ' '
        else:
            text += extract_text(child)
    if node.get('type') in ['p', 'li', 'h1', 'h2', 'h3']:
        text += '\n'
    return text.strip()


# ----------------------------------------------------------------------
# Получение и парсинг новостей с TradingView
# ----------------------------------------------------------------------
def get_news():
    print("[*] Получение новостей с TradingView API...")
    params = {
        'filter': ['lang:ru', 'market:crypto'],
        'client': 'screener',
        'streaming': 'true',
        'user_prostatus': 'non_pro'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Referer': 'https://ru.tradingview.com/',
        'Origin': 'https://ru.tradingview.com'
    }

    try:
        r = crequests.get(LIST_URL, params=params, headers=headers,
                          impersonate="chrome110", timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f" Ошибка запроса списка: {e}")
        return None

    if 'items' not in data or not data['items']:
        print(" Нет данных в ответе API.")
        return None

    news_list = []
    for item in data['items']:
        try:
            item_id = item['id']
            title = item['title']
            source = item.get('provider', {}).get('name', '')
            published = item.get('published')
            dt = datetime.datetime.fromtimestamp(published) if published else datetime.datetime.now()
            story_path = item.get('storyPath', '')
            link = f"https://ru.tradingview.com{story_path}" if story_path else ''

            # Получаем полный текст новости
            description = ''
            story_params = {
                'id': item_id,
                'lang': 'ru',
                'user_prostatus': 'non_pro'
            }
            try:
                story_r = crequests.get(STORY_URL, params=story_params,
                                        headers=headers, impersonate="chrome110", timeout=10)
                story_r.raise_for_status()
                story_data = story_r.json()
                ast_desc = story_data.get('ast_description')
                if ast_desc:
                    description = extract_text(ast_desc)
                if not description:
                    description = story_data.get('short_description', '')
            except Exception as e:
                print(f" Ошибка при получении истории {item_id}: {e}")

            news_list.append({
                'datetime': dt,
                'title': title,
                'source': source,
                'description': description,
                'link': link
            })
        except KeyError as e:
            print(f"Пропуск элемента, отсутствует ключ: {e}")
            continue

    df = pd.DataFrame(news_list)
    return df


# ----------------------------------------------------------------------
# Функция для создания таблицы с автоинкрементным ID, если она не существует
# ----------------------------------------------------------------------
def ensure_table_exists(table_name):
    # Проверяем существование таблицы
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = '{args.database}' AND table_name = '{table_name}'
        """))
        table_exists = result.scalar() > 0

    if not table_exists:
        # Создаем новую таблицу с уникальным индексом на VARCHAR поле
        create_query = text(f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            datetime DATETIME,
            title TEXT,
            source VARCHAR(255),
            description TEXT,
            link VARCHAR(500),  -- Используем VARCHAR вместо TEXT для индекса
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_link (link)  -- Теперь работает, т.к. link VARCHAR
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        with engine.connect() as conn:
            conn.execute(create_query)
            conn.commit()
        print(f" Таблица '{table_name}' создана")
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
            print(f" Внимание: в таблице '{table_name}' нет уникального индекса на поле link")
            print("Рекомендуется добавить уникальный индекс командой:")
            print(f"ALTER TABLE {table_name} MODIFY link VARCHAR(500), ADD UNIQUE INDEX unique_link (link);")


# ----------------------------------------------------------------------
# Сохранение данных в MySQL через SQLAlchemy (только новая версия)
# ----------------------------------------------------------------------
def save_data(df, table_name):
    if df.empty:
        return

    ensure_table_exists(table_name)

    # Получаем существующие ссылки
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(f"SELECT link FROM {table_name}", conn)
            existing_links = set(existing['link'].tolist()) if not existing.empty else set()
    except Exception as e:
        print(f" Ошибка при получении существующих ссылок: {e}")
        existing_links = set()

    # Фильтруем новые
    df_new = df[~df['link'].isin(existing_links)]

    if df_new.empty:
        print("ℹ Нет новых новостей для добавления")
        return

    try:
        df_new.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000,
            method='multi'
        )
        print(f" Добавлено {len(df_new)} новых строк из {len(df)} полученных")

        # Показываем последние добавленные ID
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
            max_id = result.scalar()
            print(f" Последний ID в таблице: {max_id}")

    except Exception as e:
        print(f" Ошибка записи: {e}")
        sys.exit(1)


# ----------------------------------------------------------------------
# Основная логика
# ----------------------------------------------------------------------
def main():
    print(f" Загрузчик новостей TradingView (крипто, RU)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Целевая таблица: {args.table_name}")
    print("=" * 60)

    df_news = get_news()
    if df_news is None:
        print(" Нет новостей")
        return

    save_data(df_news, args.table_name)
    print("=" * 60)
    print(" Загрузка завершена")


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