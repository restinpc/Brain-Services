"""
Таблица: vlad_gdelt_geo_events (геолоцированные события)

Запуск:
  python GDELT_events.py vlad_gdelt_geo_events [host] [port] [user] [password] [database]

Storage-v2:
  - raw_json, shareimage, mention_count больше не хранятся, потому что дублируют
    разложенные поля или не несут полезной вариативности.
  - При первом запуске существующая таблица автоматически мигрирует:
      DROP raw_json/shareimage/mention_count,
      DROP слабые индексы idx_mentions/idx_category,
      сжатие TEXT/VARCHAR/CHAR типов.
  - Дальше новые строки пишутся сразу в компактный формат.

Переменные окружения:
  GDELT_AUTO_MIGRATE=1              включить авто-миграцию существующей таблицы
  GDELT_OPTIMIZE_AFTER_MIGRATION=1  выполнить OPTIMIZE TABLE после ALTER/DROP
"""

import os, sys, argparse, time, random, traceback, hashlib
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "gdelt_events")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

AUTO_MIGRATE = os.getenv("GDELT_AUTO_MIGRATE", "1") != "0"
OPTIMIZE_AFTER_MIGRATION = os.getenv("GDELT_OPTIMIZE_AFTER_MIGRATION", "1") != "0"


def send_error_trace(exc, script_name="GDELT_events.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except Exception:
        pass


parser = argparse.ArgumentParser(description="GDELT GEO Events → MySQL compact storage")
parser.add_argument("table_name")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

DATASETS = {"vlad_gdelt_geo_events": {"description": "GDELT geo-located events"}}

# Темы без сильных пересечений
GDELT_QUERIES = [
    {"query": "PROTEST", "category": "protest"},
    {"query": "STRIKE", "category": "protest"},
    {"query": "ARMEDCONFLICT", "category": "conflict"},
    {"query": "MILITARY", "category": "conflict"},
    {"query": "SANCTIONS", "category": "economic"},
    {"query": "NATURAL_DISASTER", "category": "natural"},
    {"query": "ELECTION", "category": "political"},
    {"query": "POLITICAL_TURMOIL", "category": "political"},
]


def _sf(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _safe_identifier(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": "BrainProject/1.0 (gdelt-parser)", "Accept": "application/json"})
    return s


class GDELTCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute("SET SESSION innodb_lock_wait_timeout = 600;")
        c.execute("SET SESSION lock_wait_timeout = 600;")
        return conn, c

    def _column_exists(self, cursor, column_name):
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (self.table_name, column_name),
        )
        return cursor.fetchone() is not None

    def _index_exists(self, cursor, index_name):
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND INDEX_NAME = %s
            LIMIT 1
            """,
            (self.table_name, index_name),
        )
        return cursor.fetchone() is not None

    def _table_exists(self, cursor):
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            LIMIT 1
            """,
            (self.table_name,),
        )
        return cursor.fetchone() is not None

    def _safe_execute(self, cursor, sql, label):
        try:
            print(f"    {label}...")
            cursor.execute(sql)
            return True
        except mysql.connector.Error as e:
            print(f"    ⚠️ {label}: {e}")
            return False

    def ensure_table(self):
        conn, c = self.get_db_connection()
        changed = False
        try:
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {_safe_identifier(self.table_name)} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    event_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                    name VARCHAR(128),
                    category VARCHAR(16),
                    latitude DECIMAL(10,6),
                    longitude DECIMAL(10,6),
                    country VARCHAR(64),
                    source_url VARCHAR(512),
                    tone FLOAT,
                    snapshot_hour DATETIME NOT NULL,
                    snapshot_at DATETIME NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_hash_hour (event_hash, snapshot_hour),
                    INDEX idx_snapshot (snapshot_at)
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                COMMENT='GDELT geo-located events compact storage';
            """)
            conn.commit()

            if AUTO_MIGRATE:
                changed = self.migrate_existing_table(c)
                conn.commit()

            if changed and OPTIMIZE_AFTER_MIGRATION:
                # ALTER/DROP могут быть INSTANT в MySQL 8 и не всегда сразу отдают место ОС.
                # OPTIMIZE пересобирает таблицу и физически уплотняет .ibd.
                self._safe_execute(c, f"OPTIMIZE TABLE {_safe_identifier(self.table_name)}", "OPTIMIZE TABLE для физического освобождения места")
                self._safe_execute(c, f"ANALYZE TABLE {_safe_identifier(self.table_name)}", "ANALYZE TABLE")
                conn.commit()
        finally:
            c.close()
            conn.close()

    def migrate_existing_table(self, c):
        if not self._table_exists(c):
            return False

        print("  Проверяю compact-миграцию существующей GDELT-таблицы...")
        changed = False

        # Сначала удаляем индексы, которые либо относятся к удаляемым полям,
        # либо имеют очень низкую селективность и раздувают index_length.
        for idx in ("idx_mentions", "idx_category"):
            if self._index_exists(c, idx):
                if self._safe_execute(c, f"ALTER TABLE {_safe_identifier(self.table_name)} DROP INDEX {_safe_identifier(idx)}", f"DROP INDEX {idx}"):
                    changed = True

        # Удаляем поля-дубликаты/мусор. raw_json дублирует name/source_url/tone/category,
        # shareimage по аудиту NULL, mention_count по аудиту практически константа.
        for col in ("raw_json", "shareimage", "mention_count"):
            if self._column_exists(c, col):
                if self._safe_execute(c, f"ALTER TABLE {_safe_identifier(self.table_name)} DROP COLUMN {_safe_identifier(col)}", f"DROP COLUMN {col}"):
                    changed = True

        # Сужаем типы. Ставим умеренные лимиты, чтобы не резать данные.
        desired_types = {
            "event_hash": "CHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
            "name": "VARCHAR(128) NULL",
            "category": "VARCHAR(16) NULL",
            "country": "VARCHAR(64) NULL",
            "source_url": "VARCHAR(512) NULL",
        }

        for col, definition in desired_types.items():
            if not self._column_exists(c, col):
                continue
            sql = f"ALTER TABLE {_safe_identifier(self.table_name)} MODIFY COLUMN {_safe_identifier(col)} {definition}"
            if self._safe_execute(c, sql, f"MODIFY {col} -> {definition}"):
                changed = True

        # На всякий случай создаём нужные индексы, если таблица старая/неполная.
        if not self._index_exists(c, "uq_hash_hour"):
            if self._safe_execute(c, f"ALTER TABLE {_safe_identifier(self.table_name)} ADD UNIQUE KEY uq_hash_hour (event_hash, snapshot_hour)", "ADD UNIQUE uq_hash_hour"):
                changed = True
        if not self._index_exists(c, "idx_snapshot"):
            if self._safe_execute(c, f"ALTER TABLE {_safe_identifier(self.table_name)} ADD INDEX idx_snapshot (snapshot_at)", "ADD INDEX idx_snapshot"):
                changed = True

        if changed:
            print("  GDELT compact-миграция выполнена")
        else:
            print("  GDELT таблица уже в compact-формате")
        return changed

    def process(self):
        self.ensure_table()
        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        all_rows = []
        for q in GDELT_QUERIES:
            url = "https://api.gdeltproject.org/api/v1/gkg_geojson"
            params = {
                "QUERY": q["query"],
                "TIMESPAN": "1440",
                "MAXROWS": "2500",
            }
            print(f"    GDELT GKG v1 [{q['query']}] → {q['category']}...", end=" ", flush=True)

            try:
                resp = self.session.get(url, params=params, timeout=40)
                if resp.status_code != 200:
                    print(f"HTTP {resp.status_code}")
                    print(f"    {resp.url}")
                    continue

                data = resp.json()
                features = data.get("features", [])
                print(f"{len(features)} событий")

                for f in features:
                    props = f.get("properties", {})
                    coords = f.get("geometry", {}).get("coordinates", [None, None])
                    name = str(props.get("name", ""))[:128]
                    lon = _sf(coords[0]) if len(coords) > 0 else None
                    lat = _sf(coords[1]) if len(coords) > 1 else None

                    hash_input = f"{name[:100]}|{lat}|{lon}|{q['category']}"
                    event_hash = hashlib.sha1(hash_input.encode()).hexdigest()[:40]

                    country = name.split(",")[-1].strip()[:64] if "," in name else ""
                    source_url = str(props.get("url", ""))[:512] or None
                    tone = _sf(props.get("urltone"))

                    all_rows.append((
                        event_hash, name, q["category"],
                        lat, lon, country,
                        source_url, tone,
                        snapshot_hour, snapshot_at,
                    ))
            except Exception as e:
                print(f"ERROR: {e}")
            time.sleep(random.uniform(1.5, 3))

        if not all_rows:
            print("    Нет данных")
            return

        conn, c = self.get_db_connection()
        try:
            sql = f"""INSERT IGNORE INTO {_safe_identifier(self.table_name)}
                (event_hash, name, category, latitude, longitude, country,
                 source_url, tone, snapshot_hour, snapshot_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            c.executemany(sql, all_rows)
            conn.commit()
            n = c.rowcount
        finally:
            c.close()
            conn.close()

        print(f"    Записано {n} уникальных событий")
        cats = {}
        for r in all_rows:
            cats[r[2]] = cats.get(r[2], 0) + 1
        for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"      {cat:12s}: {cnt}")


def main():
    if args.table_name not in DATASETS:
        print(" Неизвестная таблица.")
        sys.exit(1)
    print(" GDELT GEO Events Direct Collector — compact storage v2")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}")
    print("=" * 60)
    GDELTCollector(args.table_name).process()
    print("=" * 60)
    print(" ЗАГРУЗКА ЗАВЕРШЕНА")


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
