import asyncio
import argparse
import datetime
import logging
import os
import re
import signal
import sys
import traceback
from pathlib import Path

import qrcode
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Text, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError
from telethon import TelegramClient, events

# ==================== НАСТРОЙКИ ====================
load_dotenv()

API_ID = int(os.getenv("API_ID", 32475085))
API_HASH = os.getenv("API_HASH", "2152018f5ca2fa91b67c5dbf589f89e7")
PHONE = os.getenv("PHONE", "+79330941672")

TARGET_BOT = "@Bybit_TradeGPT_bot"
QUERIES = ['/gpt ETH trend now', '/gpt BTC trend now']

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 3600))
RESPONSE_TIMEOUT = int(os.getenv("RESPONSE_TIMEOUT", 30))
MAX_WAIT = int(os.getenv("MAX_WAIT", 120))

SESSION_FILE = os.getenv("SESSION_FILE", "test_session")

TRACE_URL = os.getenv("TRACE_URL", "https://server.brain-project.online/trace.php")
NODE_NAME = os.getenv("NODE_NAME", "bybit_trend_bot")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "your@email.com")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
shutdown = asyncio.Event()


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def send_error_trace(exc: Exception, script_name: str = "Bybit_Tg_Bot.py"):
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


# ==================== ПАРСЕРЫ ====================

def extract_asset(query: str) -> str:
    """Извлекает тикер из запроса"""
    for token in query.upper().split():
        if token in ('BTC', 'ETH'):
            return token
    return 'UNKNOWN'


# ==================== РАБОТА С БОТОМ ====================

async def collect_response(bot_id: int, query: str) -> str:
    """Отправляет запрос и собирает все сообщения ответа"""
    queue: asyncio.Queue[str] = asyncio.Queue()

    async def handler(event):
        if event.message.text:
            queue.put_nowait(event.message.text)

    client.add_event_handler(handler, events.NewMessage(from_users=bot_id))

    try:
        log.info(f"→ {query}")
        await client.send_message(TARGET_BOT, query)

        parts, deadline = [], asyncio.get_event_loop().time() + MAX_WAIT

        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.warning("⏰ Таймаут сбора ответа")
                break
            try:
                text = await asyncio.wait_for(
                    queue.get(),
                    timeout=min(RESPONSE_TIMEOUT, remaining)
                )
                parts.append(text)
            except asyncio.TimeoutError:
                break

        return '\n\n'.join(parts)
    finally:
        client.remove_event_handler(handler, events.NewMessage)

# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================

def ensure_table_exists(engine, table_name):
    """
    Создаёт таблицу с минимальной структурой:
    id (автоинкремент), asset (валюта), raw_response (текст ответа), created_at.
    """
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('asset', String(20), nullable=False),          # Извлечённый актив
        Column('raw_response', Text, nullable=False),         # Полный ответ бота
        Column('created_at', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
        mysql_engine='InnoDB',
        mysql_default_charset='utf8mb4',
    )
    try:
        metadata.create_all(engine)
        log.info(f"✅ Таблица '{table_name}' проверена/создана")
    except SQLAlchemyError as e:
        log.error(f"❌ Ошибка создания таблицы: {e}")
        raise


def save_record(engine, table_name, asset, raw_response):
    """
    Сохраняет запись в БД. Возвращает ID или None при ошибке.
    """
    insert_sql = text(f"""
        INSERT INTO {table_name} (asset, raw_response)
        VALUES (:asset, :raw_response)
    """)
    try:
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {
                'asset': asset,
                'raw_response': raw_response
            })
            inserted_id = result.lastrowid
            log.debug(f"Запись добавлена, ID: {inserted_id}")
            return inserted_id
    except SQLAlchemyError as e:
        log.error(f"❌ Ошибка вставки в БД: {e}")
        send_error_trace(e, "save_record")
        return None


# ==================== ОСНОВНАЯ ЛОГИКА ====================

async def process_query(query: str, bot_id: int, engine, table_name):
    """Получает ответ, извлекает asset и сохраняет в БД"""
    try:
        raw = await collect_response(bot_id, query)
        if not raw.strip():
            log.warning("⚠ Пустой ответ")
            return None

        asset = extract_asset(query)
        inserted_id = save_record(engine, table_name, asset, raw)

        if inserted_id:
            log.info(f"✓ [ID {inserted_id}] {asset}: сохранено {len(raw)} симв.")
        else:
            log.info(f"⚠ {asset}: не сохранено")

        return inserted_id
    except Exception as e:
        log.error(f"❌ Ошибка '{query}': {e}", exc_info=True)
        send_error_trace(e, "process_query")
        return None


async def run_cycle(bot_id: int, engine, table_name):
    """Один цикл опроса всех запросов"""
    log.info("🔄 Запуск цикла опроса")
    for query in QUERIES:
        await process_query(query, bot_id, engine, table_name)
        await asyncio.sleep(5)
    log.info(f"⏳ Следующий цикл через {POLL_INTERVAL} сек")


async def scheduler(bot_id: int, engine, table_name):
    """Планировщик"""
    while not shutdown.is_set():
        await run_cycle(bot_id, engine, table_name)
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass


# ==================== ТОЧКА ВХОДА ====================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Сбор ответов от @Bybit_TradeGPT_bot с сохранением в MySQL"
    )
    parser.add_argument("table_name", help="Имя целевой таблицы в БД")
    parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
    parser.add_argument("user", nargs="?", default=os.getenv("DB_USER", "root"))
    parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME", "test"))
    parser.add_argument("--poll-interval", type=int, default=POLL_INTERVAL,
                        help="Интервал опроса в секундах")
    parser.add_argument("--queries", nargs="+", default=QUERIES,
                        help="Список запросов для отправки")
    return parser.parse_args()


async def main_async(args):
    db_url = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}?auth_plugin=mysql_native_password"
    engine = create_engine(db_url, pool_recycle=3600)

    ensure_table_exists(engine, args.table_name)

    await client.connect()
    if not await client.is_user_authorized():
        log.error("❌ Нет активной сессии. Сначала авторизуйтесь вручную или удалите сессионный файл и запустите с QR-кодом.")
        return
    else:
        me = await client.get_me()
        log.info(f"✅ Сессия загружена: {me.username or me.first_name}")

    me = await client.get_me()
    log.info(f"✅ Залогинен: {me.username or me.first_name}")

    bot_entity = await client.get_entity(TARGET_BOT)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

    log.info(f"🤖 Бот запущен. Опрос каждые {args.poll_interval} сек")
    await scheduler(bot_entity.id, engine, args.table_name)

    await client.disconnect()
    log.info("👋 Соединение закрыто")


def main():
    args = parse_args()
    global POLL_INTERVAL, QUERIES
    POLL_INTERVAL = args.poll_interval
    QUERIES = args.queries

    log.info("=" * 60)
    log.info("🚀 Запуск сборщика ответов Bybit")
    log.info(f"База: {args.host}:{args.port}/{args.database}")
    log.info(f"Таблица: {args.table_name}")
    log.info(f"Запросы: {QUERIES}")
    log.info(f"Интервал опроса: {POLL_INTERVAL} сек")
    log.info("=" * 60)

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.info("🛑 Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        log.critical(f"❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)


if __name__ == "__main__":
    main()