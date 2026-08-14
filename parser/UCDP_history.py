"""
UCDP GED Collector — загружает все события 1989–2024 + candidate.
Запуск: python UCDP_history.py <table_name> [host] [port] [user] [password] [database]
Пример: python UCDP_history.py vlad_ucdp 127.0.0.1 3306 root password brain
"""

import os, sys, argparse, time, random, traceback
from datetime import date
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError, ConnectionError as RequestsConnectionError
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "ucdp_history")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")
UCDP_TOKEN = os.getenv("UCDP_TOKEN", "")
UCDP_API_BASE = "https://ucdpapi.pcr.uu.se/api"
UCDP_GED_VERSION_OVERRIDE = os.getenv("UCDP_GED_VERSION", "").strip()
UCDP_CANDIDATE_VERSION_OVERRIDE = os.getenv("UCDP_CANDIDATE_VERSION", "").strip()
MAX_PAGE_RETRIES = 5
RETRY_BACKOFF_BASE = 10

def send_error_trace(exc, script_name="UCDP_history.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except:
        pass

# Аргументы командной строки — как в Alpha_Vantage.py:
# python UCDP_history.py <table_name> [host] [port] [user] [password] [database]
parser = argparse.ArgumentParser(description="UCDP GED Events → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="MySQL host")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="MySQL port")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="MySQL user")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="MySQL password")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="MySQL database name")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    print("\nИспользование:")
    print("  python UCDP_history.py <table_name> [host] [port] [user] [password] [database]")
    print("  python UCDP_history.py vlad_ucdp")
    print("  python UCDP_history.py vlad_ucdp 127.0.0.1 3306 root password brain")
    print("\nИли через .env файл:")
    print("  DB_HOST=127.0.0.1")
    print("  DB_PORT=3306")
    print("  DB_USER=root")
    print("  DB_PASSWORD=yourpass")
    print("  DB_NAME=brain")
    sys.exit(1)

DB_HOST = args.host
DB_PORT = args.port
DB_USER = args.user
DB_PASSWORD = args.password
DB_DATABASE = args.database
TABLE_NAME = args.table_name

if not UCDP_TOKEN:
    print("❌ Ошибка: не указан UCDP_TOKEN в .env")
    print("   Получить: https://ucdp.uu.se/apidocs/")
    sys.exit(1)

DB_CONFIG = {
    'host': DB_HOST,
    'port': int(DB_PORT),
    'user': DB_USER,
    'password': DB_PASSWORD,
    'database': DB_DATABASE
}

class UCDPCollector:
    def __init__(self):
        self.table_name = TABLE_NAME
        self.session = self._make_session()

    def _make_session(self):
        s = requests.Session()
        retry = Retry(total=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504],
                      allowed_methods=["GET"], raise_on_status=False)
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.headers.update({"x-ucdp-access-token": UCDP_TOKEN})
        return s

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def fetch_page(self, version, page=0, pagesize=1000, year_filter=None):
        url = f"{UCDP_API_BASE}/gedevents/{version}"
        params = {"pagesize": pagesize, "page": page}
        if year_filter:
            params["StartDate"] = f"{year_filter}-01-01"
            params["EndDate"] = f"{year_filter}-12-31"
        resp = self.session.get(url, params=params, timeout=90)
        if resp.status_code == 429:
            wait = 30 + random.uniform(5, 15)
            print(f"\n   ⏳ Rate limit (429), ждём {wait:.0f}с...")
            time.sleep(wait)
            return self.fetch_page(version, page, pagesize, year_filter)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(f"Неожиданный тип ответа на стр.{page}")
        return data

    def fetch_all_events(self, version, year_filter=None):
        all_events = []
        page = 0
        while True:
            for attempt in range(1, MAX_PAGE_RETRIES + 1):
                try:
                    data = self.fetch_page(version, page=page, year_filter=year_filter)
                    break
                except (ChunkedEncodingError, RequestsConnectionError, OSError) as e:
                    if attempt == MAX_PAGE_RETRIES:
                        raise
                    wait = RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(2, 8)
                    print(f"\n⚠️ Сетевой сбой на стр.{page+1} (попытка {attempt}/{MAX_PAGE_RETRIES}): {e!r}")
                    print(f"🔄 Пересоздаём сессию, ждём {wait:.0f}с...")
                    self.session = self._make_session()
                    time.sleep(wait)

            results = data.get("Result", [])
            total_pages = data.get("TotalPages", 0)
            total_count = data.get("TotalCount", 0)

            valid_events = [e for e in results if isinstance(e, dict)]
            all_events.extend(valid_events)

            current_page = data.get("CurrentPage", page)
            print(f"📄 Стр. {current_page+1}/{total_pages}: +{len(valid_events)}, всего {len(all_events)}/{total_count} ", end="\r")

            if current_page + 1 >= total_pages:
                break
            page += 1
            time.sleep(random.uniform(0.3, 0.8))

        print(f"\n✅ Загружено {len(all_events)} событий")
        return all_events

    def ensure_table(self):
        conn = self.get_db_connection()
        c = conn.cursor()

        # Проверяем, нет ли старой таблицы с iso3
        c.execute(f"SHOW TABLES LIKE '{self.table_name}'")
        if c.fetchone():
            c.execute(f"SHOW COLUMNS FROM `{self.table_name}` LIKE 'iso3'")
            if c.fetchone():
                print(f"🗑️ Обнаружена старая таблица с полем iso3. Удаляем...")
                c.execute(f"DROP TABLE IF EXISTS `{self.table_name}`")
                print(f"✅ Старая таблица удалена")

        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ucdp_id INT NOT NULL COMMENT 'UCDP event ID',
                relid VARCHAR(50),
                year INT NOT NULL,
                event_type VARCHAR(100),
                type_of_violence TINYINT,
                conflict_name VARCHAR(500),
                dyad_name VARCHAR(500),
                side_a VARCHAR(300),
                side_b VARCHAR(300),
                country VARCHAR(100),
                country_id INT COMMENT 'UCDP/G&W country code',
                region VARCHAR(100),
                adm_1 VARCHAR(200),
                adm_2 VARCHAR(200),
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                geo_precision TINYINT,
                date_start DATE,
                date_end DATE,
                deaths_a INT DEFAULT 0,
                deaths_b INT DEFAULT 0,
                deaths_civilians INT DEFAULT 0,
                deaths_unknown INT DEFAULT 0,
                best_estimate INT DEFAULT 0,
                low_estimate INT DEFAULT 0,
                high_estimate INT DEFAULT 0,
                source_article TEXT,
                source_office VARCHAR(500),
                source_date VARCHAR(100),
                where_description TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_ucdp_id (ucdp_id),
                INDEX idx_year (year),
                INDEX idx_country (country_id),
                INDEX idx_type (type_of_violence),
                INDEX idx_date (date_start),
                INDEX idx_deaths (best_estimate DESC)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='UCDP GED: georeferenced conflict events 1989-present'
        """)
        conn.commit()
        c.close()
        conn.close()

    def get_last_year(self):
        try:
            conn = self.get_db_connection()
            c = conn.cursor()
            c.execute("SHOW TABLES LIKE %s", (self.table_name,))
            if not c.fetchone():
                c.close(); conn.close(); return None
            c.execute(f"SELECT MAX(year) FROM `{self.table_name}`")
            row = c.fetchone()
            c.close(); conn.close()
            return row[0] if row and row[0] else None
        except:
            return None

    def get_row_count(self):
        try:
            conn = self.get_db_connection()
            c = conn.cursor()
            c.execute(f"SELECT COUNT(*) FROM `{self.table_name}`")
            cnt = c.fetchone()[0]
            c.close(); conn.close()
            return cnt
        except:
            return 0

    def get_last_date(self):
        try:
            conn = self.get_db_connection()
            c = conn.cursor()
            c.execute(f"SELECT MAX(date_start) FROM `{self.table_name}`")
            row = c.fetchone()
            c.close()
            conn.close()
            return row[0] if row and row[0] else None
        except Exception:
            return None

    def version_exists(self, version):
        url = f"{UCDP_API_BASE}/gedevents/{version}"
        resp = self.session.get(url, params={"pagesize": 1, "page": 0}, timeout=90)
        if resp.status_code in (401, 403):
            resp.raise_for_status()
        return resp.status_code == 200

    def discover_versions(self):
        current_year = date.today().year

        if UCDP_GED_VERSION_OVERRIDE:
            ged_version = UCDP_GED_VERSION_OVERRIDE
        else:
            ged_version = None
            for year in range(current_year, current_year - 3, -1):
                candidate = f"{year % 100}.1"
                if self.version_exists(candidate):
                    ged_version = candidate
                    break
            if not ged_version:
                raise RuntimeError("Не удалось определить актуальную версию UCDP GED")

        if UCDP_CANDIDATE_VERSION_OVERRIDE:
            candidate_version = UCDP_CANDIDATE_VERSION_OVERRIDE
        else:
            candidate_version = None
            for year in range(current_year, current_year - 2, -1):
                max_month = date.today().month if year == current_year else 12
                for month in range(max_month, 0, -1):
                    candidate = f"{year % 100}.0.{month}"
                    if self.version_exists(candidate):
                        candidate_version = candidate
                        break
                if candidate_version:
                    break
            if not candidate_version:
                raise RuntimeError("Не удалось определить актуальную версию UCDP Candidate")

        return ged_version, candidate_version

    def insert_events(self, events):
        if not events:
            return 0
        columns = [
            "ucdp_id", "relid", "year", "event_type", "type_of_violence",
            "conflict_name", "dyad_name", "side_a", "side_b",
            "country", "country_id", "region", "adm_1", "adm_2",
            "latitude", "longitude", "geo_precision",
            "date_start", "date_end",
            "deaths_a", "deaths_b", "deaths_civilians", "deaths_unknown",
            "best_estimate", "low_estimate", "high_estimate",
            "source_article", "source_office", "source_date", "where_description",
        ]
        conn = self.get_db_connection()
        c = conn.cursor()
        cols_str = ", ".join(f"`{col}`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_columns = [col for col in columns if col != "ucdp_id"]
        update_clause = ", ".join(f"`{col}` = VALUES(`{col}`)" for col in update_columns)
        sql = (
            f"INSERT INTO `{self.table_name}` ({cols_str}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

        def si(v):
            if v is None or v == "": return None
            try: return int(v)
            except: return None

        def sf(v):
            if v is None or v == "": return None
            try: return float(v)
            except: return None

        TYPE_MAP = {1: "State-based", 2: "Non-state", 3: "One-sided violence"}

        total = 0
        for i in range(0, len(events), 1000):
            batch = events[i:i+1000]
            rows = []
            for e in batch:
                if not isinstance(e, dict): continue
                tov = si(e.get("type_of_violence"))
                rows.append((
                    si(e.get("id")),
                    (e.get("relid", "") or "")[:50],
                    si(e.get("year")),
                    TYPE_MAP.get(tov, str(tov) if tov is not None else None),
                    tov,
                    (e.get("conflict_name", "") or "")[:500],
                    (e.get("dyad_name", "") or "")[:500],
                    (e.get("side_a", "") or "")[:300],
                    (e.get("side_b", "") or "")[:300],
                    e.get("country"),
                    si(e.get("country_id")),
                    e.get("region"),
                    (e.get("adm_1", "") or "")[:200],
                    (e.get("adm_2", "") or "")[:200],
                    sf(e.get("latitude")),
                    sf(e.get("longitude")),
                    si(e.get("where_prec")),
                    e.get("date_start"),
                    e.get("date_end"),
                    si(e.get("deaths_a", 0)),
                    si(e.get("deaths_b", 0)),
                    si(e.get("deaths_civilians", 0)),
                    si(e.get("deaths_unknown", 0)),
                    si(e.get("best", 0)),
                    si(e.get("low", 0)),
                    si(e.get("high", 0)),
                    (e.get("source_article", "") or "")[:5000],
                    (e.get("source_office", "") or "")[:500],
                    str(e.get("source_date") or "")[:100],
                    (e.get("where_description", "") or "")[:2000],
                ))
            if rows:
                c.executemany(sql, rows)
                total += c.rowcount if c.rowcount >= 0 else len(rows)

        conn.commit()
        c.close()
        conn.close()
        return total

    def process(self):
        self.ensure_table()
        last_year = self.get_last_year()
        last_date = self.get_last_date()
        current_count = self.get_row_count()
        ged_version, latest_candidate_version = self.discover_versions()
        candidate_year, _, candidate_month = map(int, latest_candidate_version.split("."))
        candidate_year += 2000

        print(f"\n🔎 Актуальная GED: {ged_version}")
        print(f"🔎 Актуальная Candidate: {latest_candidate_version}")

        if last_year is None:
            # === BACKFILL: весь датасет одним проходом ===
            print(f"\n🔄 BACKFILL MODE: UCDP GED v{ged_version}")
            print("   Скачиваем весь датасет за один проход...\n")
            events = self.fetch_all_events(ged_version)
            n = self.insert_events(events)
            print(f"\n✅ BACKFILL завершён: вставлено {n} записей")
        else:
            # === INCREMENTAL ===
            print(f"\n📊 INCREMENTAL MODE")
            print(f"   Последний год в БД: {last_year}")
            print(f"   Текущих записей: {current_count}\n")

            current_year = date.today().year
            for year in range(max(last_year - 1, 1989), current_year):
                print(f"    GED v{ged_version}, год {year}...")
                events = self.fetch_all_events(ged_version, year_filter=year)
                n = self.insert_events(events)
                if n > 0:
                    print(f"      {n} вставлено/обновлено")
                time.sleep(random.uniform(0.5, 1.5))

        if last_date and last_date.year == candidate_year:
            first_candidate_month = max(1, last_date.month - 1)
        else:
            first_candidate_month = 1

        for month in range(first_candidate_month, candidate_month + 1):
            version = f"{candidate_year % 100}.0.{month}"
            print(f"\n📥 Candidate v{version}...")
            events = self.fetch_all_events(version)
            n = self.insert_events(events)
            print(f"      {n} вставлено/обновлено из Candidate")

        final_count = self.get_row_count()
        print(f"\n📊 ИТОГО в таблице {self.table_name}: {final_count} записей")

def main():
    print(f"🚀 UCDP GED Collector")
    print(f"📁 База: {DB_HOST}:{DB_PORT}/{DB_DATABASE}")
    print(f"📋 Таблица: {TABLE_NAME}")
    print("=" * 60)
    UCDPCollector().process()
    print("=" * 60)
    print("✅ ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e!r}")
        traceback.print_exc()
        send_error_trace(e)
        sys.exit(1)
