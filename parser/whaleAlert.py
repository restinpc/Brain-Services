import os
import sys
import re
import argparse
import asyncio
import traceback
import hashlib
from datetime import datetime, timedelta, UTC

import requests
import mysql.connector
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

_HANDLER = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL = f"{_HANDLER}/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "whale_parser")
EMAIL = os.getenv("ALERT_EMAIL", "твоя почта")
HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
PROXY_SERVER = os.getenv("PLAYWRIGHT_PROXY_SERVER", "").strip()
PROXY_USERNAME = os.getenv("PLAYWRIGHT_PROXY_USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PLAYWRIGHT_PROXY_PASSWORD", "").strip()


def send_error_trace(exc: Exception, script_name: str = "whaleAlert_clean.py"):
    import threading

    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )

    def _send():
        try:
            requests.post(
                TRACE_URL,
                data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
                timeout=10,
            )
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True).start()


parser = argparse.ArgumentParser(description="Whale Alert Parser (лента) -> MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("[ERROR] Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

DATASETS = {"sasha_whale_btc_eth_transactions": {"type": "transactions", "url": "https://whale-alert.io/"}}


def _browser_launch_kwargs() -> dict:
    kwargs = {"headless": HEADLESS}
    if PROXY_SERVER:
        proxy = {"server": PROXY_SERVER}
        if PROXY_USERNAME:
            proxy["username"] = PROXY_USERNAME
        if PROXY_PASSWORD:
            proxy["password"] = PROXY_PASSWORD
        kwargs["proxy"] = proxy
    return kwargs


def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id              INT AUTO_INCREMENT PRIMARY KEY,
            event_time      DATETIME        NOT NULL,
            symbol          VARCHAR(16)     NOT NULL,
            amount          BIGINT          NOT NULL,
            usd_value       BIGINT          NOT NULL,
            from_addr       TEXT,
            to_addr         TEXT,
            tx_hash         CHAR(64)        NOT NULL,
            parsed_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_tx_hash (tx_hash),
            INDEX idx_event_time (event_time),
            INDEX idx_parsed (parsed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    c.execute(f"SHOW COLUMNS FROM `{table_name}`")
    cols = {row[0] for row in c.fetchall()}

    if "event_time" not in cols:
        c.execute(f"ALTER TABLE `{table_name}` ADD COLUMN event_time DATETIME NULL AFTER id")
    if "tx_hash" not in cols:
        c.execute(f"ALTER TABLE `{table_name}` ADD COLUMN tx_hash CHAR(64) NULL AFTER to_addr")
    if "time_ago" in cols:
        c.execute(f"UPDATE `{table_name}` SET event_time = parsed_at WHERE event_time IS NULL")
        c.execute(f"ALTER TABLE `{table_name}` DROP COLUMN time_ago")

    c.execute(f"UPDATE `{table_name}` SET event_time = parsed_at WHERE event_time IS NULL")
    c.execute(f"ALTER TABLE `{table_name}` MODIFY COLUMN event_time DATETIME NOT NULL")
    c.execute(
        f"""
        UPDATE `{table_name}`
        SET tx_hash = SHA2(
            CONCAT_WS('|',
                symbol,
                CAST(amount AS CHAR),
                CAST(usd_value AS CHAR),
                COALESCE(from_addr, ''),
                COALESCE(to_addr, '')
            ),
            256
        )
        WHERE tx_hash IS NULL OR tx_hash = ''
        """
    )
    c.execute(f"ALTER TABLE `{table_name}` MODIFY COLUMN tx_hash CHAR(64) NOT NULL")

    try:
        c.execute(f"CREATE UNIQUE INDEX uq_tx_hash ON `{table_name}` (tx_hash)")
    except Exception:
        pass

    conn.commit()
    c.close()
    conn.close()
    print(f"[OK] Таблица `{table_name}` готова")


def _age_to_seconds(age_text: str):
    m = re.search(
        r"(?P<num>\d+)\s*(?P<unit>sec|secs|second|seconds|min|mins|minute|minutes|hour|hours|day|days)",
        age_text.lower().strip(),
    )
    if not m:
        return None
    n = int(m.group("num"))
    unit = m.group("unit")
    if unit.startswith("sec"):
        return n
    if unit.startswith("min"):
        return n * 60
    if unit.startswith("hour"):
        return n * 3600
    if unit.startswith("day"):
        return n * 86400
    return None


def _build_tx_hash(symbol: str, amount: int, usd_value: int, from_addr: str, to_addr: str) -> str:
    raw = f"{symbol}|{amount}|{usd_value}|{from_addr.strip().lower()}|{to_addr.strip().lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def fetch_transactions() -> list:
    print("[INFO] Загружаем whale-alert.io через браузер...")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**_browser_launch_kwargs())
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        await page.goto("https://whale-alert.io/", wait_until="networkidle", timeout=90000)
        await page.wait_for_timeout(5000)
        body_text = await page.inner_text("body")
        await browser.close()

    normalized = " ".join(body_text.split())
    pattern = re.compile(
        r"(?P<time>\d+\s*(?:sec|secs?|second|seconds|min|mins?|minute|minutes|hour|hours?|day|days?)\s*ago)\s*"
        r"(?P<amount>[\d,]+)\s*#(?P<symbol>[A-Z0-9]+)\s*\(\s*(?P<usd>[\d,]+)\s*USD\)\s*"
        r"transferred\s+from\s+(?P<from>.*?)\s+to\s+(?P<to>.*?)(?="
        r"\d+\s*(?:sec|secs?|second|seconds|min|mins?|minute|minutes|hour|hours?|day|days?)\s*ago|$)",
        re.IGNORECASE,
    )

    rows = []
    for m in pattern.finditer(normalized):
        age_seconds = _age_to_seconds(m.group("time").strip())
        if age_seconds is None:
            continue
        symbol = m.group("symbol").upper()
        amount = int(m.group("amount").replace(",", ""))
        usd_value = int(m.group("usd").replace(",", ""))
        from_addr = m.group("from").strip()[:500]
        to_addr = m.group("to").strip()[:500]
        tx_hash = _build_tx_hash(symbol, amount, usd_value, from_addr, to_addr)
        rows.append((age_seconds, symbol, amount, usd_value, from_addr, to_addr, tx_hash))

    print(f"[OK] Найдено {len(rows)} транзакций в ленте (все активы)")
    return rows


def save_rows(table_name: str, rows: list):
    if not rows:
        print("[WARN] Нет данных для сохранения")
        return

    now_utc = datetime.now(UTC).replace(tzinfo=None)
    parsed_at = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    prepared = []
    for age_seconds, symbol, amount, usd_value, from_addr, to_addr, tx_hash in rows:
        event_time = (now_utc - timedelta(seconds=int(age_seconds))).strftime("%Y-%m-%d %H:%M:%S")
        prepared.append((event_time, symbol, amount, usd_value, from_addr, to_addr, tx_hash, parsed_at))

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.executemany(
        f"""
        INSERT IGNORE INTO `{table_name}`
        (event_time, symbol, amount, usd_value, from_addr, to_addr, tx_hash, parsed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        prepared,
    )
    conn.commit()
    print(f"[OK] Записано {c.rowcount} новых записей в `{table_name}`")
    c.close()
    conn.close()


def process(table_name: str):
    ensure_table(table_name)
    print("[INFO] Парсим ленту транзакций (все активы)...")
    rows = asyncio.run(fetch_transactions())
    save_rows(table_name, rows)


def main():
    if args.table_name not in DATASETS:
        print(f"[ERROR] Неизвестная таблица. Доступны: {list(DATASETS.keys())}")
        sys.exit(1)
    print("Whale Alert Parser (чистый, без дублей кода)")
    print(f"   Таблица: {args.table_name}")
    print("=" * 70)
    process(args.table_name)
    print("=" * 70)
    print("DONE")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
