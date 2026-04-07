"""
UCDP GED Collector — загружает все события 1989–2024 + candidate.
Таблица: vlad_ucdp
"""

import os, sys, argparse, json, time, random, traceback
from datetime import datetime, date

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError, ConnectionError as RequestsConnectionError
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "ucdp_history")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

UCDP_TOKEN = os.getenv("UCDP_TOKEN", "")
UCDP_API_BASE = "https://ucdpapi.pcr.uu.se/api"
UCDP_GED_VERSION = "25.1"
UCDP_CANDIDATE_VERSION = "26.0.1"

MAX_PAGE_RETRIES = 5
RETRY_BACKOFF_BASE = 10

def send_error_trace(exc, script_name="UCDP_history.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except:
        pass

# Поддерживаем оба формата: и старый (позиционные), и новый (с флагами)
parser = argparse.ArgumentParser(description="UCDP GED Events (full history + incremental) → MySQL")
parser.add_argument("table_name", nargs="?", help="Имя целевой таблицы (опционально, будет vlad_ucdp)")
parser.add_argument("host", nargs="?", help="MySQL host")
parser.add_argument("port", nargs="?", help="MySQL port")
parser.add_argument("user", nargs="?", help="MySQL user")
parser.add_argument("password", nargs="?", help="MySQL password")
parser.add_argument("database", nargs="?", help="MySQL database name")

# Добавляем флаги как альтернативный вариант
parser.add_argument("--host", dest="host_flag", help="MySQL host")
parser.add_argument("--port", dest="port_flag", help="MySQL port")
parser.add_argument("--user", dest="user_flag", help="MySQL user")
parser.add_argument("--password", dest="password_flag", help="MySQL password")
parser.add_argument("--database", dest="database_flag", help="MySQL database name")

args = parser.parse_args()

# ФИКСИРОВАННОЕ ИМЯ ТАБЛИЦЫ (игнорируем то, что передали в аргументах)
TABLE_NAME = "vlad_ucdp"

# Определяем параметры подключения:
# Сначала пробуем взять из флагов, потом из позиционных аргументов, потом из .env
DB_HOST = args.host_flag or args.host or os.getenv("DB_HOST")
DB_PORT = args.port_flag or args.port or os.getenv("DB_PORT", "3306")
DB_USER = args.user_flag or args.user or os.getenv("DB_USER")
DB_PASSWORD = args.password_flag or args.password or os.getenv("DB_PASSWORD")
DB_DATABASE = args.database_flag or args.database or os.getenv("DB_NAME")

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE]):
    print("❌ Ошибка: не указаны параметры подключения")
    print("\nИспользование (позиционные аргументы):")
    print("  python UCDP_history.py vlad_ucdp 127.0.0.1 3306 root password brain")
    print("\nИли (с флагами):")
    print("  python UCDP_history.py --host 127.0.0.1 --user root --password password --database brain")
    print("\nИли через .env файл:")
    print("  DB_HOST=127.0.0.1")
    print("  DB_USER=root")
    print("  DB_PASSWORD=password")
    print("  DB_NAME=brain")
    sys.exit(1)

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

print(f"🔌 Подключение к БД: {DB_HOST}:{DB_PORT}/{DB_DATABASE} as {DB_USER}")


class UCDPCollector:
    def __init__(self):
        self.table_name = TABLE_NAME
        self.session = self._make_session()

    def _make_session(self):
        s = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.headers.update({"x-ucdp-access-token": UCDP_TOKEN})
        return s

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def fetch_page(self, version, page=0, pagesize=1000, year_filter=None):
        url = f"{UCDP_API_BASE}/gedevents/{version}"
        params = {"pagesize": pagesize, "page": page}
        if year_filter:
            params["year"] = year_filter

        resp = self.session.get(url, params=params, timeout=90)
        if resp.status_code == 429:
            wait = 30 + random.uniform(5, 15)
            print(f"\n   ⏳ Rate limit (429), ждём {wait:.0f}с...")
            time.sleep(wait)
            return self.fetch_page(version, page, pagesize, year_filter)
        resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(
                f"Неожиданный тип ответа на стр.{page}: "
                f"{type(data).__name__} вместо dict. "
                f"Содержимое (первые 200 символов): {str(data)[:200]}"
            )
        return data

    def fetch_all_events(self, version, year_filter=None):
        all_events = []
        page = 0
        total_pages = None

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

            if not isinstance(results, list):
                print(f"\n⚠️ Стр.{page+1}: 'Result' не список ({type(results).__name__}), пропускаем")
                break

            valid_events = []
            for idx, e in enumerate(results):
                if isinstance(e, dict):
                    valid_events.append(e)
                else:
                    print(f"\n⚠️ Стр.{page+1}, элемент {idx}: ожидался dict, получен {type(e).__name__} = {e!r}")

            if not valid_events and results:
                print(f"\n⚠️ Стр.{page+1}: все {len(results)} элементов отфильтрованы как невалидные")
                break

            if not valid_events:
                break

            all_events.extend(valid_events)
            current_page = data.get("CurrentPage", page)
            print(
                f"📥 Стр. {current_page+1}/{total_pages}: "
                f"+{len(valid_events)}, всего {len(all_events)}/{total_count} ",
                end="\r"
            )

            if current_page + 1 >= total_pages:
                break

            page += 1
            time.sleep(random.uniform(0.3, 1.0))

        print()
        return all_events

    def ensure_table(self):
        conn = self.get_db_connection()
        c = conn.cursor()
        
        # Проверяем, существует ли таблица со старой схемой (с iso3)
        c.execute(f"SHOW TABLES LIKE '{self.table_name}'")
        if c.fetchone():
            # Проверяем, есть ли поле iso3
            c.execute(f"SHOW COLUMNS FROM `{self.table_name}` LIKE 'iso3'")
            if c.fetchone():
                print(f"⚠️ Обнаружена старая таблица с полем iso3. Удаляем...")
                c.execute(f"DROP TABLE IF EXISTS `{self.table_name}`")
                print(f"✅ Старая таблица удалена")
        
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ucdp_id INT NOT NULL COMMENT 'UCDP event ID',
                relid VARCHAR(50) COMMENT 'UCDP relational key',
                year INT NOT NULL,
                event_type VARCHAR(100) COMMENT 'type_of_violence text',
                type_of_violence TINYINT COMMENT '1=state,2=non-state,3=one-sided',
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
                best_estimate INT DEFAULT 0 COMMENT 'Best fatality estimate',
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
                c.close()
                conn.close()
                return None
            c.execute(f"SELECT MAX(year) FROM `{self.table_name}`")
            row = c.fetchone()
            c.close()
            conn.close()
            return row[0] if row and row[0] else None
        except:
            return None

    def get_row_count(self):
        try:
            conn = self.get_db_connection()
            c = conn.cursor()
            c.execute(f"SELECT COUNT(*) FROM `{self.table_name}`")
            cnt = c.fetchone()[0]
            c.close()
            conn.close()
            return cnt
        except:
            return 0

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
        sql = f"INSERT IGNORE INTO `{self.table_name}` ({cols_str}) VALUES ({placeholders})"

        def sf(v):
            if v is None or v == "":
                return None
            try:
                return float(v)
            except:
                return None

        def si(v):
            if v is None or v == "":
                return None
            try:
                return int(v)
            except:
                return None

        TYPE_MAP = {1: "State-based", 2: "Non-state", 3: "One-sided violence"}

        total = 0
        skipped = 0
        for i in range(0, len(events), 1000):
            batch = events[i:i+1000]
            rows = []
            for e in batch:
                if not isinstance(e, dict):
                    print(f"\n⚠️ insert_events: пропускаем элемент типа {type(e).__name__}: {e!r}")
                    skipped += 1
                    continue

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
                    e.get("source_date"),
                    (e.get("where_description", "") or "")[:2000],
                ))

            if not rows:
                continue

            c.executemany(sql, rows)
            total += c.rowcount if c.rowcount >= 0 else len(rows)

        conn.commit()
        c.close()
        conn.close()
        if skipped:
            print(f"\n⚠️ Пропущено невалидных элементов: {skipped}")
        return total

    def process(self):
        self.ensure_table()

        last_year = self.get_last_year()
        current_count = self.get_row_count()

        if last_year is None:
            # Полная загрузка 1989–2024
            print(f"\n📦 BACKFILL MODE: UCDP GED v{UCDP_GED_VERSION} (1989–2024)")
            print(f"   Таблица пуста, начинаем полную загрузку\n")

            total_inserted = 0
            for year in range(1989, 2025):
                print(f"\n📅 Год {year}:")
                events = self.fetch_all_events(UCDP_GED_VERSION, year_filter=year)
                if events:
                    n = self.insert_events(events)
                    total_inserted += n
                    print(f"   ✅ {year}: {n} новых из {len(events)}")
                else:
                    print(f"   ⚠️ {year}: нет данных")
                time.sleep(random.uniform(1, 3))

            print(f"\n🏁 BACKFILL GED завершён: {total_inserted} записей")

            # Candidate events
            print(f"\n📦 Загрузка UCDP Candidate v{UCDP_CANDIDATE_VERSION} (2025+)")
            events = self.fetch_all_events(UCDP_CANDIDATE_VERSION)
            if events:
                n = self.insert_events(events)
                print(f"   ✅ Candidate: {n} новых из {len(events)}")

        else:
            # Инкрементальный режим
            print(f"\n🔄 INCREMENTAL MODE")
            print(f"   Последний год в БД: {last_year}")
            print(f"   Текущих записей: {current_count}\n")

            current_year = date.today().year
            for year in range(max(last_year - 1, 1989), current_year):
                print(f"   📥 GED v{UCDP_GED_VERSION}, год {year}...")
                events = self.fetch_all_events(UCDP_GED_VERSION, year_filter=year)
                if events:
                    n = self.insert_events(events)
                    if n > 0:
                        print(f"      +{n} новых")
                time.sleep(random.uniform(0.5, 1.5))

            print(f"   📥 Candidate v{UCDP_CANDIDATE_VERSION}...")
            events = self.fetch_all_events(UCDP_CANDIDATE_VERSION)
            if events:
                n = self.insert_events(events)
                print(f"      +{n} новых из candidate")

        final_count = self.get_row_count()
        print(f"\n📊 Всего записей в {self.table_name}: {final_count}")


def main():
    print(f"🚀 UCDP GED Collector (backfill + incremental)")
    print(f"🎯 Таблица: {TABLE_NAME}")
    print("=" * 60)
    UCDPCollector().process()
    print("=" * 60)
    print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n🛑 Прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        traceback.print_exc()
        send_error_trace(e)
        sys.exit(1)
