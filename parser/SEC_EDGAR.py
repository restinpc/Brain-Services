"""
Описание: SEC EDGAR Parser
Фокус на BTC/ETH
Запуск:   python SEC_EDGAR.py <table_name>
"""

import os
import sys
import argparse
import traceback
import requests
import mysql.connector
import time
import re
import hashlib
from datetime import datetime
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "SEC_EDGAR")
EMAIL      = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

# ── RATE LIMITER (≤ 9 запросов в секунду) ─────────────────────────────────────
class RateLimiter:
    def __init__(self, max_per_second: float = 9.0):
        self.max_per_second = max_per_second
        self.min_interval = 1.0 / max_per_second
        self.last_call = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()

rate_limiter = RateLimiter(max_per_second=9.0)

# ── ТРАССИРОВКА ОШИБОК ───────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "SEC_EDGAR.py"):
    import threading
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    def _send():
        try:
            requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
        except:
            pass
    threading.Thread(target=_send, daemon=True).start()

# ── АРГУМЕНТЫ ────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="EDGAR Parser с парсингом содержимого документов")
parser.add_argument("table_name",  help="Имя таблицы в БД")
parser.add_argument("host",        nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port",        nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user",        nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password",    nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database",    nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры БД")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host, "port": int(args.port),
    "user": args.user, "password": args.password,
    "database": args.database,
}

# ── DATASETS (можно добавлять новые CIK) ─────────────────────────────────────
DATASETS = {
    "sec_mstr_filings":      {"description": "MicroStrategy (BTC)",                     "cik": "0001050446"},
    "sec_coinbase_filings":  {"description": "Coinbase Global",                        "cik": "0001679788"},
    "sec_ibit_filings":      {"description": "iShares Bitcoin Trust (BlackRock)",      "cik": "0001980994"},
    "sec_arkb_filings":      {"description": "ARK 21Shares Bitcoin ETF",               "cik": "0001869699"},
    "sec_grayscale_eth_filings": {"description": "Grayscale Ethereum Trust",           "cik": "0001725210"},
}

# ── СОЗДАНИЕ ТАБЛИЦЫ (с полным текстом и ключами) ────────────────────────────
def ensure_table(table_name: str):
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id                INT AUTO_INCREMENT PRIMARY KEY,
            cik               VARCHAR(12),
            company_name      VARCHAR(255),
            ticker            VARCHAR(32),
            filing_date       DATE        NOT NULL,
            report_date       DATE NULL,
            acceptance_datetime DATETIME NULL,
            form              VARCHAR(20) NOT NULL,
            is_amendment      TINYINT(1) DEFAULT 0,
            accession_number  VARCHAR(64) NOT NULL,
            primary_document  VARCHAR(255),
            doc_url           VARCHAR(700),
            doc_hash          CHAR(64),
            content           MEDIUMTEXT,
            content_word_count INT DEFAULT 0,
            has_bitcoin       TINYINT(1) DEFAULT 0,
            has_ethereum      TINYINT(1) DEFAULT 0,
            bitcoin_mentions  INT DEFAULT 0,
            ethereum_mentions INT DEFAULT 0,
            item_8k           VARCHAR(255) DEFAULT NULL,   -- для 8-K: список Item'ов
            event_type        VARCHAR(64) DEFAULT 'other',
            event_severity    SMALLINT DEFAULT 0,
            crypto_relevance_score SMALLINT DEFAULT 0,
            loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_accession (accession_number),
            INDEX idx_filing_date (filing_date),
            INDEX idx_acceptance_datetime (acceptance_datetime),
            INDEX idx_event_type (event_type),
            INDEX idx_bitcoin (has_bitcoin),
            INDEX idx_ethereum (has_ethereum)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]} — с парсингом содержимого';
    """)

    # Миграция для уже существующих таблиц.
    required_columns = {
        "cik": "VARCHAR(12)",
        "company_name": "VARCHAR(255)",
        "ticker": "VARCHAR(32)",
        "report_date": "DATE NULL",
        "acceptance_datetime": "DATETIME NULL",
        "is_amendment": "TINYINT(1) DEFAULT 0",
        "doc_url": "VARCHAR(700)",
        "doc_hash": "CHAR(64)",
        "content_word_count": "INT DEFAULT 0",
        "event_type": "VARCHAR(64) DEFAULT 'other'",
        "event_severity": "SMALLINT DEFAULT 0",
        "crypto_relevance_score": "SMALLINT DEFAULT 0",
    }
    for col_name, col_type in required_columns.items():
        c.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE %s", (col_name,))
        if c.fetchone() is None:
            c.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}")

    required_indexes = {
        "idx_acceptance_datetime": "(`acceptance_datetime`)",
        "idx_event_type": "(`event_type`)",
    }
    for idx_name, idx_cols in required_indexes.items():
        c.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = %s", (idx_name,))
        if c.fetchone() is None:
            c.execute(f"ALTER TABLE `{table_name}` ADD INDEX `{idx_name}` {idx_cols}")

    conn.commit()
    c.close()
    conn.close()

# ── ВОДОРАЗДЕЛ ИНКРЕМЕНТА ──────────────────────────────────────────────────────
def get_latest_watermark(table_name: str):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(acceptance_datetime), MAX(filing_date) FROM `{table_name}`")
        row = c.fetchone()
        c.close()
        conn.close()
        if not row:
            return None, None
        return row[0], row[1]
    except:
        return None, None


def parse_acceptance_datetime(value: str):
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None

# ── СКАЧИВАНИЕ ДОКУМЕНТА ─────────────────────────────────────────────────────
def download_document(cik_clean: str, accession: str, primary_doc: str) -> str:
    if not primary_doc:
        return ""

    # cik без ведущих нулей
    url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{accession.replace('-', '')}/{primary_doc}"
    headers = {"User-Agent": f"EDGARTradingParser/1.0 ({EMAIL})"}

    rate_limiter.wait()

    try:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code != 200:
            print(f"   ⚠️ Не удалось скачать документ: HTTP {resp.status_code}")
            return ""

        # Парсим HTML или TXT
        if primary_doc.lower().endswith(('.htm', '.html')):
            if BeautifulSoup is not None:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for tag in soup(["script", "style", "header", "footer", "nav"]):
                    tag.decompose()
                text = soup.get_text(separator=" ", strip=True)
            else:
                # Fallback без bs4: грубое удаление HTML тегов.
                text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", resp.text)
                text = re.sub(r"(?s)<[^>]+>", " ", text)
        else:
            text = resp.text

        # Очистка
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:500000]  # ограничиваем размер для БД (при необходимости увеличь)

    except Exception as e:
        print(f"   ❌ Ошибка скачивания: {e}")
        return ""

# ── ПАРСИНГ КЛЮЧЕВОЙ ИНФОРМАЦИИ ─────────────────────────────────────────────
def classify_event(form: str, text_lower: str) -> tuple[str, int]:
    form_u = (form or "").upper()
    if form_u == "8-K":
        if re.search(r"private placement|public offering|convertible|notes?", text_lower):
            return "financing", 4
        if re.search(r"risk factors|impairment|investigation|litigation", text_lower):
            return "risk_disclosure", 4
        return "material_event", 3
    if form_u in ("10-Q", "10-K"):
        return "periodic_report", 2
    if form_u.startswith("S-") or form_u.startswith("424"):
        return "securities_offering", 4
    if form_u.startswith("SC 13") or form_u == "FORM 4":
        return "ownership_change", 3
    return "other", 1


def parse_content(text: str, form: str) -> dict:
    if not text:
        return {
            "has_bitcoin": 0, "has_ethereum": 0, "bitcoin_mentions": 0,
            "ethereum_mentions": 0, "item_8k": None, "content_word_count": 0,
            "event_type": "other", "event_severity": 0, "crypto_relevance_score": 0
        }

    text_lower = text.lower()

    btc_count = len(re.findall(r'\bbitcoin\b|\bbtc\b', text_lower))
    eth_count = len(re.findall(r'\bethereum\b|\beth\b', text_lower))

    has_btc = 1 if btc_count > 0 else 0
    has_eth = 1 if eth_count > 0 else 0

    item_8k = None
    if form == "8-K":
        items = re.findall(r'Item\s+\d+\.\d+', text, re.IGNORECASE)
        if items:
            item_8k = ", ".join(sorted(set(items)))[:255]

    event_type, event_severity = classify_event(form, text_lower)
    words = len(text.split())
    crypto_relevance_score = min(100, btc_count * 4 + eth_count * 4 + (15 if form == "8-K" else 0))

    return {
        "has_bitcoin": has_btc,
        "has_ethereum": has_eth,
        "bitcoin_mentions": btc_count,
        "ethereum_mentions": eth_count,
        "item_8k": item_8k,
        "content_word_count": words,
        "event_type": event_type,
        "event_severity": event_severity,
        "crypto_relevance_score": crypto_relevance_score,
    }

# ── FETCH METADATA ───────────────────────────────────────────────────────────
def fetch_data(config: dict) -> list:
    cik = config.get("cik")
    if not cik:
        return []

    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {"User-Agent": f"EDGARTradingParser/1.0 ({EMAIL})"}

    rate_limiter.wait()

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"❌ HTTP {response.status_code} для CIK {cik}")
            return []

        data = response.json()
        recent = data.get("filings", {}).get("recent", {})
        company_name = data.get("name", "")
        tickers = data.get("tickers", []) or []
        ticker = tickers[0] if tickers else ""

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accs = recent.get("accessionNumber", [])
        docs = recent.get("primaryDocument", [])
        report_dates = recent.get("reportDate", [])
        acceptance_times = recent.get("acceptanceDateTime", [])

        rows = []
        for i in range(len(forms)):
            form = forms[i] if i < len(forms) else ""
            rows.append({
                "filing_date": dates[i] if i < len(dates) else None,
                "report_date": report_dates[i] if i < len(report_dates) else None,
                "acceptance_datetime": parse_acceptance_datetime(acceptance_times[i] if i < len(acceptance_times) else None),
                "form": form,
                "is_amendment": 1 if str(form).endswith("/A") else 0,
                "accession_number": accs[i],
                "primary_document": docs[i] if i < len(docs) else "",
                "cik_clean": cik.lstrip("0"),
                "cik": cik,
                "company_name": company_name,
                "ticker": ticker,
            })
        return rows
    except Exception as e:
        print(f"❌ Ошибка metadata: {e}")
        return []

# ── ЗАПИСЬ В БД ──────────────────────────────────────────────────────────────
def save_rows(table_name: str, rows: list):
    if not rows:
        print("⚠️ Нет данных для записи")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()

    sql = f"""
        INSERT IGNORE INTO `{table_name}` 
        (
            cik, company_name, ticker,
            filing_date, report_date, acceptance_datetime, form, is_amendment,
            accession_number, primary_document, doc_url, doc_hash, content, content_word_count,
            has_bitcoin, has_ethereum, bitcoin_mentions, ethereum_mentions,
            item_8k, event_type, event_severity, crypto_relevance_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    c.executemany(sql, rows)
    conn.commit()
    print(f"✅ Записано {c.rowcount} новых записей")
    c.close()
    conn.close()

# ── ОСНОВНАЯ ЛОГИКА ──────────────────────────────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]
    ensure_table(table_name)
    latest_acceptance, latest_date = get_latest_watermark(table_name)
    print(f"📅 Последняя дата в БД: {latest_date or 'пуста'}")
    print(f"⏱️ Последний acceptance_datetime: {latest_acceptance or 'нет'}")

    raw = fetch_data(config)
    if not raw:
        print("⚠️ Нет метаданных")
        return

    filtered = []
    latest_str = latest_date.isoformat() if latest_date else None
    latest_acceptance_str = str(latest_acceptance) if latest_acceptance else None

    for item in raw:
        date = item.get("filing_date")
        acceptance_dt = item.get("acceptance_datetime")
        if not date:
            continue
        # Основной инкремент по acceptance_datetime, fallback по filing_date.
        if latest_acceptance_str and acceptance_dt and acceptance_dt <= latest_acceptance_str:
            continue
        if (not latest_acceptance_str) and latest_str and date <= latest_str:
            continue

        print(f"   → Обрабатываем {item['form']} от {date} (accession: {item['accession_number']})")

        content = download_document(item["cik_clean"], item["accession_number"], item["primary_document"])
        parsed = parse_content(content, item["form"])
        doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{item['cik_clean']}/{item['accession_number'].replace('-', '')}/{item['primary_document']}"
        ) if item["primary_document"] else None
        doc_hash = hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest() if content else None

        filtered.append((
            item.get("cik"),
            item.get("company_name"),
            item.get("ticker"),
            date,
            item.get("report_date"),
            acceptance_dt,
            item["form"],
            item.get("is_amendment", 0),
            item["accession_number"],
            item["primary_document"],
            doc_url,
            doc_hash,
            content,
            parsed["content_word_count"],
            parsed["has_bitcoin"],
            parsed["has_ethereum"],
            parsed["bitcoin_mentions"],
            parsed["ethereum_mentions"],
            parsed["item_8k"],
            parsed["event_type"],
            parsed["event_severity"],
            parsed["crypto_relevance_score"],
        ))

    print(f"🆕 Новых обработанных filings: {len(filtered)}")
    save_rows(table_name, filtered)

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print("❌ Неизвестная таблица. Доступные:")
        for k, v in DATASETS.items():
            print(f"   {k} — {v['description']}")
        sys.exit(1)

    print("🚀 EDGAR Parser с парсингом содержимого (BTC/ETH keys)")
    print(f"   Таблица: {args.table_name}")
    print("=" * 80)

    process(args.table_name)

    print("=" * 80)
    print("🏁 ГОТОВО")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        send_error_trace(e)
        sys.exit(1)