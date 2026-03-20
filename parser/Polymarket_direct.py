"""
Запуск:
  python Polymarket_direct.py vlad_polymarket [host] [port] [user] [password] [database]
"""
import os
import sys
import argparse
import json
import time
import random
import traceback
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "polymarket_history")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="Polymarket_direct.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except:
        pass

# ────────────────────────────────────────────────
# Аргументы
# ────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Polymarket History → MySQL")
parser.add_argument("table_name", help="Имя таблицы (например: vlad_polymarket)")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))

args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Не указаны параметры подключения к базе")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database
}

# ────────────────────────────────────────────────
# Утилиты
# ────────────────────────────────────────────────

def _sf(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ""))
    except:
        return None

def _parse_end_date(s):
    """Возвращает date-объект для колонки DATE, или None если не распарсить."""
    if not s:
        return None
    try:
        from datetime import date
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return dt.date()
    except:
        return None

def build_session():
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=1.8, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return s

# ────────────────────────────────────────────────
# Класс коллектора
# ────────────────────────────────────────────────

class PolymarketHistoryCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute("SET SESSION innodb_lock_wait_timeout = 600;")
        return conn, c

    def ensure_table(self):
        conn, c = self.get_db_connection()

        c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            condition_id        VARCHAR(100) NOT NULL,
            question            VARCHAR(500),
            price_timestamp     DATETIME NOT NULL,
            price               DECIMAL(5,4) NOT NULL,
            loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_cond_ts (condition_id, price_timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Polymarket — история цен';
        """)

        columns = {
            # DECIMAL(14,2): до $99T — более чем достаточно, 7 байт вместо 9
            "volume":        "DECIMAL(14,2)  COMMENT 'Общий объём торгов (USDC)'",
            "volume_24h":    "DECIMAL(14,2)  COMMENT 'Объём за 24 часа'",
            "liquidity":     "DECIMAL(14,2)  COMMENT 'Текущая ликвидность'",
            # DATE (3 байта) вместо VARCHAR(50) (~25 байт) + нормальные date-операции
            "end_date":      "DATE           COMMENT 'Дата разрешения/экспирации'",
            "tags":          "VARCHAR(500)   COMMENT 'Теги через запятую'",
            # DECIMAL(5,4): цена 0.0000–0.9999, 3 байта вместо 4 у FLOAT
            "outcome_yes":   "DECIMAL(5,4)   COMMENT 'Текущая цена YES'",
            "outcome_no":    "DECIMAL(5,4)   COMMENT 'Текущая цена NO'",
            "spread":        "DECIMAL(5,4)   COMMENT 'Спред bid-ask'",
            "num_outcomes":  "INT            COMMENT 'Количество исходов'",
            "active":        "TINYINT(1)     COMMENT '1 = активен, 0 = закрыт'",
        }

        c.execute(f"SHOW COLUMNS FROM `{self.table_name}`")
        existing = {row[0] for row in c.fetchall()}

        added = False
        for col, definition in columns.items():
            if col not in existing:
                print(f"   → Добавляю колонку: {col}")
                try:
                    c.execute(f"ALTER TABLE `{self.table_name}` ADD COLUMN `{col}` {definition}")
                    added = True
                except Exception as e:
                    print(f"   Ошибка при добавлении {col}: {e}")

        indexes = [
            "idx_timestamp (price_timestamp)",
            "idx_price (price)",
            "idx_volume (volume DESC)",
            "idx_active (active)"
        ]
        for idx_def in indexes:
            try:
                c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX {idx_def}")
            except:
                pass

        if added:
            print("   Миграция завершена")
        else:
            print("   Все колонки уже существуют")

        conn.commit()
        c.close()
        conn.close()

    def get_last_timestamp(self, condition_id):
        conn, c = self.get_db_connection()
        try:
            c.execute(f"SELECT MAX(price_timestamp) FROM `{self.table_name}` WHERE condition_id = %s", (condition_id,))
            row = c.fetchone()
            return int(row[0].timestamp()) if row and row[0] else None
        except:
            return None
        finally:
            c.close()
            conn.close()

    def fetch_all_markets(self):
        all_markets = []

        # Активные
        print(" 📡 Gamma API → активные рынки...")
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
                        "ascending": "false"
                    },
                    timeout=30
                )
                if r.status_code != 200: break
                data = r.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets: break
                all_markets.extend(markets)
                print(f" ...активных: {len(all_markets):,}", end="\r")
                offset += len(markets)
                if len(markets) < 100: break
                time.sleep(random.uniform(1.2, 2.2))
            except Exception as e:
                print(f"\n ⚠️ Ошибка активных: {e}")
                break

        # Закрытые рынки — только качественные для бэктестинга
        # Критерии: volume >= $50K, ликвидность >= $500, срок жизни >= 7 дней
        # API отдаёт по volume DESC → как только max volume страницы < $50K — стоп
        print(f"\n 📡 Gamma API → закрытые рынки (volume ≥ $50K, liquidity ≥ $500, срок ≥ 7 дней)...")
        offset = 0
        closed_fetched = 0
        closed_passed  = 0
        CLOSED_MIN_VOLUME    = 50_000   # минимальный total volume в USDC
        CLOSED_MIN_LIQUIDITY = 500      # минимальная ликвидность — ниже = цена шумит от одной сделки
        CLOSED_MIN_DAYS      = 7        # минимальная длина жизни рынка (168 часовых точек)

        def _parse_dt_safe(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
            except:
                return None

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
                        "ascending": "false"
                    },
                    timeout=30
                )
                if r.status_code != 200: break
                data = r.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets: break

                page_passed = 0
                max_vol_on_page = 0
                for m in markets:
                    vol = _sf(m.get("volume") or m.get("volumeNum"))
                    if vol is not None:
                        max_vol_on_page = max(max_vol_on_page, vol)

                    # Фильтр 1: volume
                    if vol is None or vol < CLOSED_MIN_VOLUME:
                        continue

                    # Фильтр 2: ликвидность — защита от шумных малоликвидных рынков
                    liq = _sf(m.get("liquidity") or m.get("liquidityNum"))
                    if liq is not None and liq < CLOSED_MIN_LIQUIDITY:
                        continue

                    # Фильтр 3: длина жизни — надёжный парсинг через несколько полей
                    start_dt = (_parse_dt_safe(m.get("startDate"))
                                or _parse_dt_safe(m.get("createdAt"))
                                or _parse_dt_safe(m.get("created_at")))
                    end_dt   = (_parse_dt_safe(m.get("endDate"))
                                or _parse_dt_safe(m.get("resolutionDate")))
                    if start_dt and end_dt:
                        if (end_dt - start_dt).days < CLOSED_MIN_DAYS:
                            continue
                    else:
                        # Даты не распарсились — пропускаем, не берём сомнительные рынки
                        continue

                    all_markets.append(m)
                    page_passed += 1

                closed_fetched += len(markets)
                closed_passed  += page_passed
                print(f" ...закрытых проверено: {closed_fetched:,}  прошло фильтр: {closed_passed:,}", end="\r")

                # API DESC по volume — если максимум страницы уже ниже порога, дальше нет смысла
                if max_vol_on_page < CLOSED_MIN_VOLUME:
                    print(f"\n   → макс. volume страницы ${max_vol_on_page:,.0f} < порога, стоп")
                    break

                offset += len(markets)
                if len(markets) < 100: break
                time.sleep(random.uniform(1.2, 2.2))
            except Exception as e:
                print(f"\n ⚠️ Ошибка закрытых: {e}")
                break

        print(f"\n 📋 Всего рынков: {len(all_markets):,}")
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

        tid = yes_p = no_p = None

        if isinstance(tokens, list) and tokens:
            tid = tokens[0].get("token_id", "")
            yes_p = _sf(tokens[0].get("price"))
            if len(tokens) >= 2:
                no_p = _sf(tokens[1].get("price"))

        if not tid and clob_raw:
            clob_ids = clob_raw
            if isinstance(clob_raw, str):
                try:
                    clob_ids = json.loads(clob_raw)
                except:
                    clob_ids = []
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
        tags_str = ",".join(str(t.get("label", t) if isinstance(t, dict) else t) for t in tags_raw[:15])

        num_outcomes = len(tokens) if isinstance(tokens, list) else (len(clob_ids) if 'clob_ids' in locals() and isinstance(clob_ids, list) else None)

        return {
            "condition_id": cid[:100],
            "_token_id":    tid,          # только для API-запроса, в БД не пишем
            "question":     (m.get("question") or m.get("title") or "").strip()[:2000],
            "volume":       _sf(m.get("volume") or m.get("volumeNum")),
            "volume_24h":   _sf(m.get("volume24hr") or m.get("volume_24h")),
            "liquidity":    _sf(m.get("liquidity") or m.get("liquidityNum")),
            "end_date":     _parse_end_date(m.get("endDate") or m.get("end_date_iso") or m.get("resolutionDate")),
            "tags":         tags_str[:500] or None,
            "outcome_yes":  yes_p,
            "outcome_no":   no_p,
            "spread":       spread,
            "num_outcomes": num_outcomes,
            "active":       1 if m.get("active", True) else 0,
        }

    def process(self):
        self.ensure_table()

        print(" 📡 Загрузка списка рынков...")
        markets = self.fetch_all_markets()
        if not markets:
            print(" ⚠️ Не удалось загрузить рынки")
            return

        market_metas = [meta for m in markets if (meta := self.extract_market_meta(m))]

        print(f" 🎯 Рынков с token_id: {len(market_metas):,}")

        if not market_metas:
            print("   → нет рынков с валидным token_id")
            return

        total_points = total_new = 0
        conn, c = self.get_db_connection()

        sql = f"""
        INSERT IGNORE INTO `{self.table_name}`
        (condition_id, question, price_timestamp, price,
         volume, volume_24h, liquidity, end_date, tags,
         outcome_yes, outcome_no, spread, num_outcomes, active)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        SUBBATCH_SIZE = 500
        COMMIT_EVERY_MARKETS = 5
        FORCE_COMMIT_IF_ROWS_GT = 1000

        try:
            for i, meta in enumerate(market_metas):
                cid = meta["condition_id"]
                tid = meta["_token_id"]  # используем только для запроса к API

                last_ts = self.get_last_timestamp(cid)
                start_ts = last_ts + 1 if last_ts else None

                history = self.fetch_price_history(tid, start_ts)

                if not history:
                    if (i + 1) % 200 == 0:
                        pct = round((i + 1) / len(market_metas) * 100, 1)
                        print(f" ...{i+1:,}/{len(market_metas):,} ({pct}%)  новых: {total_new:,}", end="\r")
                    time.sleep(random.uniform(1.2, 2.2))
                    continue

                rows = []
                for p in history:
                    ts = p.get("t", 0)
                    price = p.get("p", 0)
                    if ts > 0:
                        # Округляем до минуты — API возвращает случайные секунды
                        dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:00")
                        rows.append((
                            cid, meta["question"], dt, price,
                            meta["volume"], meta["volume_24h"], meta["liquidity"],
                            meta["end_date"], meta["tags"],
                            meta["outcome_yes"], meta["outcome_no"], meta["spread"],
                            meta["num_outcomes"], meta["active"]
                        ))

                if rows:
                    for start in range(0, len(rows), SUBBATCH_SIZE):
                        sub_rows = rows[start:start + SUBBATCH_SIZE]
                        try:
                            c.executemany(sql, sub_rows)
                            inserted = c.rowcount
                            total_new += inserted
                            total_points += len(sub_rows)
                        except mysql.connector.Error as err:
                            print(f"   Ошибка вставки подбатча рынка {i+1} (строки {start}-{start+len(sub_rows)}): {err}")
                            conn.rollback()
                            break

                if (i + 1) % COMMIT_EVERY_MARKETS == 0 or len(rows) > FORCE_COMMIT_IF_ROWS_GT:
                    conn.commit()
                    pct = round((i + 1) / len(market_metas) * 100, 1)
                    print(f" ...{i+1:,}/{len(market_metas):,} ({pct}%)  новых: {total_new:,}  всего: {total_points:,}", end="\r")

                time.sleep(random.uniform(1.4, 2.4))

            conn.commit()

        except Exception as e:
            print(f"\n❌ Критическая ошибка: {e}")
            conn.rollback()
            raise

        finally:
            c.close()
            conn.close()

        final_pct = round(total_new / total_points * 100, 1) if total_points > 0 else 0
        print(f"\n\n ✅ Завершено: загружено {total_points:,} точек, новых — {total_new:,} ({final_pct}%)")

# ────────────────────────────────────────────────
# Запуск
# ────────────────────────────────────────────────

def main():
    print(f"🚀 Polymarket History Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"Таблица: {args.table_name}")
    print("=" * 70)

    collector = PolymarketHistoryCollector(args.table_name)
    collector.process()

    print("=" * 70)
    print("🏁 Завершено")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        send_error_trace(e)
        sys.exit(1)
