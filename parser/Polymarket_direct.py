"""
Запуск:
  python Polymarket_direct.py vlad_polymarket [host] [port] [user] [password] [database]

Что делает версия storage-v2:
  1) При старте создаёт компактные таблицы:
       <table>_markets — метаданные рынка, 1 строка на condition_id
       <table>_prices  — история цен, много строк на condition_id + время
  2) Если старая тяжёлая таблица <table> ещё существует как BASE TABLE,
     переносит данные батчами в новые таблицы, проверяет количество строк,
     удаляет старую таблицу и создаёт совместимое VIEW <table>.
  3) Дальше пишет новые данные только в compact-схему.

Переменные окружения:
  POLYMARKET_AUTO_MIGRATE=1              включить авто-миграцию старой таблицы
  POLYMARKET_DROP_LEGACY_AFTER_MIGRATION=1 удалить старую таблицу после успешного переноса
  POLYMARKET_CREATE_COMPAT_VIEW=1        создать VIEW со старым именем таблицы
  POLYMARKET_MIGRATION_BATCH_SIZE=100000 размер батча при переносе истории цен
"""
import os
import sys
import argparse
import json
import time
import random
import traceback
from datetime import datetime, timezone, date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "polymarket_history")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

AUTO_MIGRATE = os.getenv("POLYMARKET_AUTO_MIGRATE", "1") != "0"
DROP_LEGACY_AFTER_MIGRATION = os.getenv("POLYMARKET_DROP_LEGACY_AFTER_MIGRATION", "1") != "0"
CREATE_COMPAT_VIEW = os.getenv("POLYMARKET_CREATE_COMPAT_VIEW", "1") != "0"
MIGRATION_BATCH_SIZE = int(os.getenv("POLYMARKET_MIGRATION_BATCH_SIZE", "100000"))


def send_error_trace(exc, script_name="Polymarket_direct.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except Exception:
        pass


#
# Аргументы
#

parser = argparse.ArgumentParser(description="Polymarket History → MySQL compact storage")
parser.add_argument("table_name", help="Базовое имя таблицы/VIEW (например: vlad_polymarket)")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))

args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Не указаны параметры подключения к базе")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

TODAY = date.today()


#
# Утилиты
#

def _sf(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _parse_end_date(s):
    """Возвращает date-объект для колонки DATE, или None если не распарсить."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return dt.date()
    except Exception:
        return None


def _parse_dt_safe(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _safe_identifier(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _bool_to_int(v):
    if v is None:
        return None
    return 1 if bool(v) else 0


def build_session():
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=1.8, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return s


class PolymarketHistoryCollector:
    def __init__(self, table_name):
        self.base_table = table_name
        self.markets_table = f"{table_name}_markets"
        self.prices_table = f"{table_name}_prices"
        self.session = build_session()

    def get_db_connection(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute("SET SESSION innodb_lock_wait_timeout = 600;")
        c.execute("SET SESSION lock_wait_timeout = 600;")
        return conn, c

    def _table_type(self, cursor, table_name):
        cursor.execute(
            """
            SELECT TABLE_TYPE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            """,
            (table_name,),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _column_exists(self, cursor, table_name, column_name):
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (table_name, column_name),
        )
        return cursor.fetchone() is not None

    def _index_exists(self, cursor, table_name, index_name):
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND INDEX_NAME = %s
            LIMIT 1
            """,
            (table_name, index_name),
        )
        return cursor.fetchone() is not None

    def ensure_table(self):
        conn, c = self.get_db_connection()
        try:
            self._ensure_compact_tables(c)
            conn.commit()

            if AUTO_MIGRATE:
                self._migrate_legacy_table_if_needed(conn, c)

            if CREATE_COMPAT_VIEW:
                self._ensure_compat_view(conn, c)

            conn.commit()
        finally:
            c.close()
            conn.close()

    def _ensure_compact_tables(self, c):
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {_safe_identifier(self.markets_table)} (
            condition_id VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            question VARCHAR(160) NULL,
            end_date DATE NULL,
            tags VARCHAR(255) NULL,
            active TINYINT(1) NULL,
            num_outcomes TINYINT UNSIGNED NULL,
            loaded_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (condition_id)
        ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
          COMMENT='Polymarket — compact market metadata';
        """)

        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {_safe_identifier(self.prices_table)} (
            id INT NOT NULL AUTO_INCREMENT,
            condition_id VARCHAR(66) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            price_timestamp DATETIME NOT NULL,
            price DECIMAL(5,4) NOT NULL,
            volume DECIMAL(14,2) NULL,
            volume_24h DECIMAL(14,2) NULL,
            liquidity DECIMAL(14,2) NULL,
            spread DECIMAL(5,4) NULL,
            outcome_yes DECIMAL(5,4) NULL,
            outcome_no DECIMAL(5,4) NULL,
            loaded_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_cond_ts (condition_id, price_timestamp),
            KEY idx_timestamp (price_timestamp)
        ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
          COMMENT='Polymarket — compact price history';
        """)

    def _legacy_price_select_exprs(self, c):
        """Возвращает SELECT-выражения для переноса из старой широкой таблицы."""
        t = self.base_table
        optional = {
            "volume": "NULL",
            "volume_24h": "NULL",
            "liquidity": "NULL",
            "spread": "NULL",
            "outcome_yes": "NULL",
            "outcome_no": "NULL",
            "loaded_at": "CURRENT_TIMESTAMP",
        }
        exprs = {}
        for col, fallback in optional.items():
            exprs[col] = f"o.{_safe_identifier(col)}" if self._column_exists(c, t, col) else fallback
        return exprs

    def _migrate_legacy_table_if_needed(self, conn, c):
        legacy_type = self._table_type(c, self.base_table)
        if legacy_type != "BASE TABLE":
            return

        # Если это уже не старая широкая таблица, а пользователь назвал base == prices/markets, не трогаем.
        required = ["condition_id", "price_timestamp", "price"]
        if not all(self._column_exists(c, self.base_table, col) for col in required):
            print(f"  ⚠️ {self.base_table}: не похожа на старую Polymarket-таблицу, авто-миграция пропущена")
            return

        print(f"  Найдена старая широкая таблица {self.base_table}; начинаю миграцию в compact-схему")
        c.execute(f"SELECT COUNT(*) FROM {_safe_identifier(self.base_table)}")
        old_count = int(c.fetchone()[0] or 0)
        print(f"    старых строк: {old_count:,}")

        # 1 строка метаданных на condition_id. Все поля остаются, но больше не повторяются в истории цен.
        has_question = self._column_exists(c, self.base_table, "question")
        has_end_date = self._column_exists(c, self.base_table, "end_date")
        has_tags = self._column_exists(c, self.base_table, "tags")
        has_active = self._column_exists(c, self.base_table, "active")
        has_num_outcomes = self._column_exists(c, self.base_table, "num_outcomes")
        has_loaded_at = self._column_exists(c, self.base_table, "loaded_at")

        question_expr = "LEFT(MAX(o.`question`), 160)" if has_question else "NULL"
        end_date_expr = "MAX(o.`end_date`)" if has_end_date else "NULL"
        tags_expr = "LEFT(MAX(o.`tags`), 255)" if has_tags else "NULL"
        active_expr = "MAX(o.`active`)" if has_active else "NULL"
        num_expr = "MAX(o.`num_outcomes`)" if has_num_outcomes else "NULL"
        loaded_expr = "MAX(o.`loaded_at`)" if has_loaded_at else "CURRENT_TIMESTAMP"

        print("    переношу market metadata...")
        c.execute(f"""
            INSERT INTO {_safe_identifier(self.markets_table)}
                (condition_id, question, end_date, tags, active, num_outcomes, loaded_at)
            SELECT
                LEFT(o.`condition_id`, 66) AS condition_id,
                {question_expr} AS question,
                {end_date_expr} AS end_date,
                {tags_expr} AS tags,
                {active_expr} AS active,
                {num_expr} AS num_outcomes,
                {loaded_expr} AS loaded_at
            FROM {_safe_identifier(self.base_table)} o
            GROUP BY LEFT(o.`condition_id`, 66)
            ON DUPLICATE KEY UPDATE
                question = VALUES(question),
                end_date = VALUES(end_date),
                tags = VALUES(tags),
                active = VALUES(active),
                num_outcomes = VALUES(num_outcomes),
                loaded_at = VALUES(loaded_at)
        """)
        conn.commit()

        c.execute(f"SELECT MIN(id), MAX(id) FROM {_safe_identifier(self.base_table)}")
        min_id, max_id = c.fetchone()
        if min_id is None or max_id is None:
            print("    старая таблица пустая")
        else:
            exprs = self._legacy_price_select_exprs(c)
            current = int(min_id)
            max_id = int(max_id)
            batch = max(1, MIGRATION_BATCH_SIZE)
            copied_total = 0
            print(f"    переношу price history батчами по {batch:,} id...")

            while current <= max_id:
                end_id = min(current + batch - 1, max_id)
                c.execute(f"""
                    INSERT IGNORE INTO {_safe_identifier(self.prices_table)}
                        (id, condition_id, price_timestamp, price,
                         volume, volume_24h, liquidity, spread, outcome_yes, outcome_no, loaded_at)
                    SELECT
                        o.`id`,
                        LEFT(o.`condition_id`, 66),
                        o.`price_timestamp`,
                        o.`price`,
                        {exprs['volume']},
                        {exprs['volume_24h']},
                        {exprs['liquidity']},
                        {exprs['spread']},
                        {exprs['outcome_yes']},
                        {exprs['outcome_no']},
                        {exprs['loaded_at']}
                    FROM {_safe_identifier(self.base_table)} o
                    WHERE o.`id` BETWEEN %s AND %s
                """, (current, end_id))
                copied_total += c.rowcount if c.rowcount is not None else 0
                conn.commit()
                if current == min_id or end_id == max_id or copied_total % (batch * 10) < batch:
                    print(f"      id {current:,}..{end_id:,}, inserted/ignored affected≈{copied_total:,}")
                current = end_id + 1

        c.execute(f"SELECT COUNT(*) FROM {_safe_identifier(self.prices_table)}")
        new_count = int(c.fetchone()[0] or 0)
        print(f"    compact price rows now: {new_count:,}")

        if new_count < old_count:
            raise RuntimeError(
                f"Миграция небезопасна: в compact-таблице строк меньше, чем в старой ({new_count:,} < {old_count:,}). Старую таблицу не удаляю."
            )

        if DROP_LEGACY_AFTER_MIGRATION:
            print(f"    проверка пройдена, удаляю старую тяжёлую таблицу {self.base_table} для освобождения места")
            c.execute(f"DROP TABLE {_safe_identifier(self.base_table)}")
            conn.commit()
        else:
            print("    POLYMARKET_DROP_LEGACY_AFTER_MIGRATION=0, старая таблица оставлена; место на диске пока не освободится")

    def _ensure_compat_view(self, conn, c):
        base_type = self._table_type(c, self.base_table)
        if base_type == "BASE TABLE":
            # Нельзя создать VIEW поверх существующей таблицы.
            return

        # VIEW сохраняет старое имя и старый набор полей для SELECT-совместимости.
        c.execute(f"DROP VIEW IF EXISTS {_safe_identifier(self.base_table)}")
        c.execute(f"""
            CREATE VIEW {_safe_identifier(self.base_table)} AS
            SELECT
                p.id AS id,
                p.condition_id AS condition_id,
                m.question AS question,
                p.price_timestamp AS price_timestamp,
                p.price AS price,
                p.loaded_at AS loaded_at,
                p.volume AS volume,
                p.volume_24h AS volume_24h,
                p.liquidity AS liquidity,
                m.end_date AS end_date,
                m.tags AS tags,
                p.outcome_yes AS outcome_yes,
                p.outcome_no AS outcome_no,
                p.spread AS spread,
                m.num_outcomes AS num_outcomes,
                m.active AS active
            FROM {_safe_identifier(self.prices_table)} p
            LEFT JOIN {_safe_identifier(self.markets_table)} m
                ON m.condition_id = p.condition_id
        """)
        conn.commit()
        print(f"  Совместимое VIEW создано: {self.base_table}")

    def get_last_timestamp(self, condition_id):
        conn, c = self.get_db_connection()
        try:
            c.execute(
                f"SELECT MAX(price_timestamp) FROM {_safe_identifier(self.prices_table)} WHERE condition_id = %s",
                (condition_id,),
            )
            row = c.fetchone()
            return int(row[0].timestamp()) if row and row[0] else None
        except Exception:
            return None
        finally:
            c.close()
            conn.close()

    def fetch_all_markets(self):
        all_markets = []

        # Активные
        print("  Gamma API → активные рынки...")
        offset = 0
        while True:
            try:
                r = self.session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "limit": "100",
                        "offset": str(offset),
                        "order": "volume24hr",
                        "ascending": "false",
                    },
                    timeout=30,
                )
                if r.status_code != 200:
                    break
                data = r.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets:
                    break
                all_markets.extend(markets)
                print(f" ...активных: {len(all_markets):,}", end="\r")
                offset += len(markets)
                if len(markets) < 100:
                    break
                time.sleep(random.uniform(1.2, 2.2))
            except Exception as e:
                print(f"\n  Ошибка активных: {e}")
                break

        print(f"\n  Gamma API → закрытые рынки (volume ≥ $50K, liquidity ≥ $500, срок ≥ 7 дней)...")
        offset = 0
        closed_fetched = 0
        closed_passed = 0
        CLOSED_MIN_VOLUME = 50_000
        CLOSED_MIN_LIQUIDITY = 500
        CLOSED_MIN_DAYS = 7

        while True:
            try:
                r = self.session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={
                        "active": "false",
                        "closed": "true",
                        "limit": "100",
                        "offset": str(offset),
                        "order": "volume",
                        "ascending": "false",
                    },
                    timeout=30,
                )
                if r.status_code != 200:
                    break
                data = r.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets:
                    break

                page_passed = 0
                max_vol_on_page = 0
                for m in markets:
                    vol = _sf(m.get("volume") or m.get("volumeNum"))
                    if vol is not None:
                        max_vol_on_page = max(max_vol_on_page, vol)

                    if vol is None or vol < CLOSED_MIN_VOLUME:
                        continue

                    liq = _sf(m.get("liquidity") or m.get("liquidityNum"))
                    if liq is not None and liq < CLOSED_MIN_LIQUIDITY:
                        continue

                    start_dt = (
                        _parse_dt_safe(m.get("startDate"))
                        or _parse_dt_safe(m.get("createdAt"))
                        or _parse_dt_safe(m.get("created_at"))
                    )
                    end_dt = _parse_dt_safe(m.get("endDate")) or _parse_dt_safe(m.get("resolutionDate"))
                    if start_dt and end_dt:
                        if (end_dt - start_dt).days < CLOSED_MIN_DAYS:
                            continue
                    else:
                        continue

                    all_markets.append(m)
                    page_passed += 1

                closed_fetched += len(markets)
                closed_passed += page_passed
                print(f" ...закрытых проверено: {closed_fetched:,}  прошло фильтр: {closed_passed:,}", end="\r")

                if max_vol_on_page < CLOSED_MIN_VOLUME:
                    print(f"\n   → макс. volume страницы ${max_vol_on_page:,.0f} < порога, стоп")
                    break

                offset += len(markets)
                if len(markets) < 100:
                    break
                time.sleep(random.uniform(1.2, 2.2))
            except Exception as e:
                print(f"\n  Ошибка закрытых: {e}")
                break

        print(f"\n  Всего рынков: {len(all_markets):,}")
        return all_markets

    def fetch_price_history(self, token_id, start_ts=None):
        params = {"market": token_id, "interval": "max", "fidelity": 60}
        if start_ts:
            params.pop("interval", None)
            params["startTs"] = start_ts
            params["endTs"] = int(time.time())
        try:
            r = self.session.get("https://clob.polymarket.com/prices-history", params=params, timeout=35)
            if r.status_code == 200:
                return r.json().get("history", [])
        except Exception as e:
            print(f"   → ошибка истории для {token_id}: {e}")
        return []

    def extract_market_meta(self, m):
        cid = m.get("condition_id") or m.get("conditionId") or ""
        if not cid:
            return None

        tokens = m.get("tokens", [])
        clob_raw = m.get("clobTokenIds")
        clob_ids = []

        tid = yes_p = no_p = None

        if isinstance(tokens, list) and tokens:
            tid = tokens[0].get("token_id", "")
            yes_p = _sf(tokens[0].get("price"))
            if len(tokens) >= 2:
                no_p = _sf(tokens[1].get("price"))

        if not tid and clob_raw:
            if isinstance(clob_raw, str):
                try:
                    clob_ids = json.loads(clob_raw)
                except Exception:
                    clob_ids = []
            elif isinstance(clob_raw, list):
                clob_ids = clob_raw
            if isinstance(clob_ids, list) and clob_ids:
                tid = str(clob_ids[0])

        if not tid:
            return None

        outcomes = m.get("outcomePrices", [])
        if isinstance(outcomes, list):
            if len(outcomes) >= 1 and yes_p is None:
                yes_p = _sf(outcomes[0])
            if len(outcomes) >= 2 and no_p is None:
                no_p = _sf(outcomes[1])

        best_bid = _sf(m.get("bestBid"))
        best_ask = _sf(m.get("bestAsk"))
        spread = round(best_ask - best_bid, 4) if best_bid is not None and best_ask is not None else None

        tags_raw = m.get("tags", [])
        if isinstance(tags_raw, list):
            parts = []
            for t in tags_raw[:15]:
                label = (t.get("label") or t.get("name") or t.get("slug")) if isinstance(t, dict) else str(t)
                if label:
                    parts.append(str(label))
            tags_str = ",".join(parts)
        else:
            tags_str = str(tags_raw) if tags_raw else ""

        if isinstance(tokens, list) and len(tokens) > 0:
            num_outcomes = len(tokens)
        elif isinstance(clob_ids, list) and len(clob_ids) > 0:
            num_outcomes = len(clob_ids)
        else:
            num_outcomes = 2

        end_date = _parse_end_date(m.get("endDate") or m.get("end_date_iso") or m.get("resolutionDate"))
        if end_date is not None:
            active = 0 if end_date < TODAY else 1
        else:
            active = 1 if m.get("active", True) else 0

        return {
            "condition_id": str(cid)[:66],
            "_token_id": tid,
            "question": (m.get("question") or m.get("title") or "").strip()[:160],
            "volume": _sf(m.get("volume") or m.get("volumeNum")),
            "volume_24h": _sf(m.get("volume24hr") or m.get("volume_24h")),
            "liquidity": _sf(m.get("liquidity") or m.get("liquidityNum")),
            "end_date": end_date,
            "tags": tags_str[:255] if tags_str else None,
            "outcome_yes": yes_p,
            "outcome_no": no_p,
            "spread": spread,
            "num_outcomes": min(int(num_outcomes or 0), 255),
            "active": active,
        }

    def _upsert_market_metadata_batch(self, cursor, metas):
        if not metas:
            return
        rows = [
            (
                m["condition_id"],
                m["question"],
                m["end_date"],
                m["tags"],
                m["active"],
                m["num_outcomes"],
            )
            for m in metas
        ]
        sql = f"""
        INSERT INTO {_safe_identifier(self.markets_table)}
            (condition_id, question, end_date, tags, active, num_outcomes)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            question = VALUES(question),
            end_date = VALUES(end_date),
            tags = VALUES(tags),
            active = VALUES(active),
            num_outcomes = VALUES(num_outcomes),
            loaded_at = CURRENT_TIMESTAMP
        """
        cursor.executemany(sql, rows)

    def process(self):
        self.ensure_table()

        print("  Загрузка списка рынков...")
        markets = self.fetch_all_markets()
        if not markets:
            print("  Не удалось загрузить рынки")
            return

        market_metas = [meta for m in markets if (meta := self.extract_market_meta(m))]
        print(f"  Рынков с token_id: {len(market_metas):,}")

        if not market_metas:
            print("   → нет рынков с валидным token_id")
            return

        total_points = total_new = total_updated = 0
        conn, c = self.get_db_connection()

        market_sql = f"""
        INSERT INTO {_safe_identifier(self.markets_table)}
            (condition_id, question, end_date, tags, active, num_outcomes)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            question = VALUES(question),
            end_date = VALUES(end_date),
            tags = VALUES(tags),
            active = VALUES(active),
            num_outcomes = VALUES(num_outcomes),
            loaded_at = CURRENT_TIMESTAMP
        """

        price_sql = f"""
        INSERT INTO {_safe_identifier(self.prices_table)}
        (condition_id, price_timestamp, price,
         volume, volume_24h, liquidity, spread, outcome_yes, outcome_no)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            volume = VALUES(volume),
            volume_24h = VALUES(volume_24h),
            liquidity = VALUES(liquidity),
            spread = VALUES(spread),
            outcome_yes = VALUES(outcome_yes),
            outcome_no = VALUES(outcome_no),
            loaded_at = CURRENT_TIMESTAMP
        """

        SUBBATCH_SIZE = 500
        COMMIT_EVERY_MARKETS = 5
        FORCE_COMMIT_IF_ROWS_GT = 1000

        try:
            # Сначала обновляем metadata по рынкам один раз, чтобы не повторять её в каждой price-строке.
            market_rows = [
                (m["condition_id"], m["question"], m["end_date"], m["tags"], m["active"], m["num_outcomes"])
                for m in market_metas
            ]
            for start in range(0, len(market_rows), 1000):
                c.executemany(market_sql, market_rows[start:start + 1000])
                conn.commit()

            for i, meta in enumerate(market_metas):
                cid = meta["condition_id"]
                tid = meta["_token_id"]

                last_ts = self.get_last_timestamp(cid)
                start_ts = last_ts + 1 if last_ts else None
                history = self.fetch_price_history(tid, start_ts)

                if not history:
                    if (i + 1) % 200 == 0:
                        pct = round((i + 1) / len(market_metas) * 100, 1)
                        print(f" ...{i+1:,}/{len(market_metas):,} ({pct}%)  обработано: {total_new:,}", end="\r")
                    time.sleep(random.uniform(1.2, 2.2))
                    continue

                rows = []
                for p in history:
                    ts = p.get("t", 0)
                    price = p.get("p", 0)
                    if ts > 0:
                        dt = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:00")
                        rows.append((
                            cid, dt, price,
                            meta["volume"], meta["volume_24h"], meta["liquidity"],
                            meta["spread"], meta["outcome_yes"], meta["outcome_no"],
                        ))

                if rows:
                    for start in range(0, len(rows), SUBBATCH_SIZE):
                        sub_rows = rows[start:start + SUBBATCH_SIZE]
                        for attempt in range(5):
                            try:
                                c.executemany(price_sql, sub_rows)
                                rc = c.rowcount
                                total_new += len(sub_rows)
                                total_updated += rc
                                total_points += len(sub_rows)
                                break
                            except mysql.connector.Error as err:
                                if err.errno == 1213 and attempt < 4:
                                    conn.rollback()
                                    time.sleep(random.uniform(0.5, 1.5) * (attempt + 1))
                                    continue
                                print(f"   Ошибка рынка {i+1} (попытка {attempt+1}): {err}")
                                conn.rollback()
                                break

                if (i + 1) % COMMIT_EVERY_MARKETS == 0 or len(rows) > FORCE_COMMIT_IF_ROWS_GT:
                    conn.commit()
                    pct = round((i + 1) / len(market_metas) * 100, 1)
                    print(f" ...{i+1:,}/{len(market_metas):,} ({pct}%)  строк в БД: {total_points:,}  affected: {total_updated:,}", end="\r")

                time.sleep(random.uniform(1.4, 2.4))

            conn.commit()

        except Exception as e:
            print(f"\n Критическая ошибка: {e}")
            conn.rollback()
            raise

        finally:
            c.close()
            conn.close()

        print(f"\n\n  Завершено: строк обработано {total_points:,}  affected rows {total_updated:,}")


#
# Запуск
#

def main():
    print(" Polymarket History Collector — compact storage v2")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"Базовое имя: {args.table_name}")
    print(f"Новые таблицы: {args.table_name}_markets, {args.table_name}_prices")
    print("=" * 70)

    collector = PolymarketHistoryCollector(args.table_name)
    collector.process()

    print("=" * 70)
    print(" Завершено")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n Критическая ошибка: {e}")
        send_error_trace(e)
        sys.exit(1)
