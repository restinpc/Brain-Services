import asyncio
import argparse
import logging
import os
import signal
import sys
import traceback
import json
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from telethon import TelegramClient, events
from telethon import connection

# ==================== НАСТРОЙКИ ====================
load_dotenv()

API_ID   = int(os.getenv("API_ID",   32475085))
API_HASH = os.getenv("API_HASH",     "2152018f5ca2fa91b67c5dbf589f89e7")

SESSIONS = [
    os.getenv("SESSION_FILE_1", "/brain/Brain-Services/parser/session_1"),
    os.getenv("SESSION_FILE_2", "/brain/Brain-Services/parser/session_2"),
    os.getenv("SESSION_FILE_3", "/brain/Brain-Services/parser/session_3"),
]

# ============= НАСТРОЙКИ MTProto ПРОКСИ =============
MTPROTO_HOST   = "77.110.121.105"      # IP
MTPROTO_PORT   = 8443                     # порт прокси
MTPROTO_SECRET = "22029ceb10bf65d1e998a57a698afd1a"      # hex-строка
# ========================================================================

# Формируем прокси только если хост указан
if MTPROTO_HOST and MTPROTO_HOST != "":
    MTPROTO_PROXY = (MTPROTO_HOST, MTPROTO_PORT, MTPROTO_SECRET)
else:
    MTPROTO_PROXY = None

# Квоты по активу: сколько запросов каждый аккаунт отправляет за цикл (24 запуска)
QUOTAS = {
    "BTC": [
        int(os.getenv("QUOTA_BTC_1", 20)),
        int(os.getenv("QUOTA_BTC_2", 4)),
        int(os.getenv("QUOTA_BTC_3", 0)),
    ],
    "ETH": [
        int(os.getenv("QUOTA_ETH_1", 0)),
        int(os.getenv("QUOTA_ETH_2", 16)),
        int(os.getenv("QUOTA_ETH_3", 8)),
    ],
}

COUNTER_FILES = {
    "BTC": os.getenv("COUNTER_FILE_BTC", "/brain/Brain-Services/parser/counter_btc.json"),
    "ETH": os.getenv("COUNTER_FILE_ETH", "/brain/Brain-Services/parser/counter_eth.json"),
}

TARGET_BOT       = "@Bybit_TradeGPT_bot"
RESPONSE_TIMEOUT = int(os.getenv("RESPONSE_TIMEOUT", 30))
MAX_WAIT         = int(os.getenv("MAX_WAIT", 120))

TRACE_URL   = os.getenv("TRACE_URL",   "https://server.brain-project.online/trace.php")
NODE_NAME   = os.getenv("NODE_NAME",   "bybit_trend_bot")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

shutdown = asyncio.Event()


# ==================== СЧЁТЧИК ====================

def load_counter(asset: str) -> int:
    path = COUNTER_FILES[asset]
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return int(data.get("total_requests", 0))
    except Exception as e:
        log.warning(f"Не удалось прочитать счётчик {path}, сбрасываю: {e}")
        return 0


def save_counter(asset: str, value: int):
    path = COUNTER_FILES[asset]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({"total_requests": value}, f, indent=2)


def pick_account(asset: str, request_index: int) -> int:
    cumulative = 0
    for idx, quota in enumerate(QUOTAS[asset]):
        cumulative += quota
        if request_index < cumulative:
            return idx
    raise ValueError(
        f"Запрос #{request_index} выходит за пределы суммарной квоты "
        f"{sum(QUOTAS[asset])} для {asset}"
    )


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def send_error_trace(exc: Exception, script_name: str = "Bybit_Tg_Bot.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    try:
        requests.post(TRACE_URL, data={
            "url": "cli_script",
            "node": NODE_NAME,
            "email": ALERT_EMAIL,
            "logs": logs
        }, timeout=10)
    except Exception:
        pass


def extract_asset(query: str) -> str:
    for token in query.upper().split():
        if token in ('BTC', 'ETH'):
            return token
    return None


# ==================== РАБОТА С БОТОМ ====================

async def collect_response(client: TelegramClient, bot_id: int, query: str) -> str:
    queue: asyncio.Queue = asyncio.Queue()

    async def handler(event):
        if event.message.text:
            queue.put_nowait(event.message.text)

    client.add_event_handler(handler, events.NewMessage(from_users=bot_id))

    try:
        log.info(f"-> {query}")
        await client.send_message(TARGET_BOT, query)

        parts = []
        deadline = asyncio.get_event_loop().time() + MAX_WAIT

        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.warning("Таймаут сбора ответа")
                break
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=min(RESPONSE_TIMEOUT, remaining))
                parts.append(msg)
            except asyncio.TimeoutError:
                break

        return '\n\n'.join(parts)
    finally:
        client.remove_event_handler(handler, events.NewMessage)


# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================

def ensure_table_exists(engine, table_name, database_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = '{database_name}' AND table_name = '{table_name}'
        """))
        table_exists = result.scalar() > 0

    if not table_exists:
        with engine.connect() as conn:
            conn.execute(text(f"""
            CREATE TABLE {table_name} (
                id           INT AUTO_INCREMENT PRIMARY KEY,
                asset        VARCHAR(20)  NOT NULL,
                raw_response LONGTEXT     NOT NULL,
                created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """))
            conn.commit()
        log.info(f"Таблица '{table_name}' создана")
    else:
        log.info(f"Таблица '{table_name}' уже существует")


def save_record(engine, table_name, asset, raw_response):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"INSERT INTO {table_name} (asset, raw_response) VALUES (:asset, :raw_response)"),
                {'asset': asset, 'raw_response': raw_response}
            )
            conn.commit()
            return result.lastrowid
    except SQLAlchemyError as e:
        log.error(f"Ошибка вставки в БД: {e}")
        send_error_trace(e, "save_record")
        return None


# ==================== ОСНОВНАЯ ЛОГИКА ====================

async def run_query(asset: str, query: str, engine, table_name: str):
    total_quota = sum(QUOTAS[asset])
    total_sent  = load_counter(asset)

    log.info(f"Счётчик {asset}: {total_sent}/{total_quota} в текущем цикле")

    if total_sent >= total_quota:
        log.error(
            f"Квота {asset} исчерпана ({total_sent}/{total_quota}). "
            "Счётчик должен был сброситься — проверь логику."
        )
        sys.exit(1)

    acc_idx = pick_account(asset, total_sent)
    log.info(
        f"Запрос {total_sent + 1}/{total_quota} через аккаунт #{acc_idx + 1} "
        f"(квота акк. {QUOTAS[asset][acc_idx]}): {query}"
    )

    # Создаём клиент с поддержкой MTProto прокси (если настроен)
    if MTPROTO_PROXY:
        log.info(f"Используем MTProto прокси: {MTPROTO_HOST}:{MTPROTO_PORT}")
        client = TelegramClient(
            SESSIONS[acc_idx],
            API_ID,
            API_HASH,
            connection=connection.ConnectionTcpMTProxyRandomizedIntermediate,
            proxy=MTPROTO_PROXY
        )
    else:
        log.info("Работаем без прокси (прямое подключение)")
        client = TelegramClient(SESSIONS[acc_idx], API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(
                f"Нет активной сессии для аккаунта #{acc_idx + 1}: {SESSIONS[acc_idx]}"
            )
        me = await client.get_me()
        log.info(f"Аккаунт #{acc_idx + 1} подключён: {me.username or me.first_name}")

        bot_entity = await client.get_entity(TARGET_BOT)

        raw = await collect_response(client, bot_entity.id, query)
        if not raw.strip():
            log.warning("Пустой ответ от бота")
        else:
            inserted_id = save_record(engine, table_name, asset, raw)
            if inserted_id:
                log.info(f"[ID {inserted_id}] {asset}: сохранено {len(raw)} симв.")
            else:
                log.warning(f"{asset}: не сохранено в БД")

    finally:
        await client.disconnect()
        log.info("Соединение закрыто")

    # Обновляем счётчик
    new_total = total_sent + 1
    if new_total >= total_quota:
        log.info(f"Цикл {asset} завершён ({new_total}/{total_quota}). Сбрасываю счётчик.")
        save_counter(asset, 0)
    else:
        log.info(f"Счётчик {asset} обновлён: {new_total}/{total_quota}")
        save_counter(asset, new_total)


# ==================== ТОЧКА ВХОДА ====================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Почасовой сборщик ответов @Bybit_TradeGPT_bot.\n"
            "Один запуск = один запрос. Актив определяется из текста запроса (BTC/ETH).\n"
            "Аккаунт выбирается автоматически по счётчику.\n"
            "\n"
            "Примеры:\n"
            "  python Bybit_Tg_Bot.py vlad_bybit_data_bot localhost 3306 root pass db -- /gpt BTC trend now\n"
            "  python Bybit_Tg_Bot.py vlad_bybit_data_bot_ETH localhost 3306 root pass db -- /gpt ETH trend now"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("table_name")
    parser.add_argument("host",     nargs="?", default=os.getenv("DB_HOST",     "localhost"))
    parser.add_argument("port",     nargs="?", default=os.getenv("DB_PORT",     "3306"))
    parser.add_argument("user",     nargs="?", default=os.getenv("DB_USER",     "root"))
    parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME",     "test"))
    parser.add_argument("query",    nargs=argparse.REMAINDER)
    return parser.parse_args()


async def main_async(args):
    raw_query = args.query
    if raw_query and raw_query[0] == '--':
        raw_query = raw_query[1:]

    if not raw_query:
        log.error("Не указан запрос. Передайте его после '--'.")
        sys.exit(1)

    query_str = ' '.join(raw_query)

    asset = extract_asset(query_str)
    if asset is None:
        log.error(f"Не удалось определить актив (BTC/ETH) из запроса: '{query_str}'")
        sys.exit(1)

    db_url = (
        f"mysql+mysqlconnector://{args.user}:{args.password}"
        f"@{args.host}:{args.port}/{args.database}"
    )
    engine = create_engine(
        db_url,
        pool_recycle=3600,
        connect_args={"auth_plugin": "caching_sha2_password"}
    )

    ensure_table_exists(engine, args.table_name, args.database)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

    await run_query(asset, query_str, engine, args.table_name)


def main():
    args = parse_args()

    raw_query = args.query
    if raw_query and raw_query[0] == '--':
        raw_query = raw_query[1:]

    query_str    = ' '.join(raw_query) if raw_query else ''
    asset        = extract_asset(query_str) or 'UNKNOWN'
    total_sent   = load_counter(asset) if asset != 'UNKNOWN' else 0
    total_quota  = sum(QUOTAS[asset]) if asset != 'UNKNOWN' else 0

    log.info("=" * 60)
    log.info(f"Почасовой сборщик Bybit | {asset}")
    log.info(f"База:          {args.host}:{args.port}/{args.database}")
    log.info(f"Таблица:       {args.table_name}")
    log.info(f"Запрос:        {query_str}")
    log.info(f"Счётчик:       {total_sent}/{total_quota} в текущем цикле")
    if asset != 'UNKNOWN':
        log.info(f"Квоты акк.:    {QUOTAS[asset]}")
        log.info(f"Файл счётчика: {COUNTER_FILES[asset]}")
    if MTPROTO_PROXY:
        log.info(f"MTProto прокси: {MTPROTO_HOST}:{MTPROTO_PORT}")
    else:
        log.info("MTProto прокси: не используется")
    log.info("=" * 60)

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.info("Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        log.critical(f"Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
