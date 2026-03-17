"""
Polymarket History Collector
Загружает список рынков (активные + топ-5000 закрытых по volume)
и делает incremental backfill ценовой истории через /prices-history

Таблица: vlad_polymarket_history

Запуск:
  python Polymarket_direct.py vlad_polymarket_history [host] [port] [user] [password] [database]
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
parser.add_argument("table_name", help="Должно быть: vlad_polymarket_history")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))

args = parser.parse_args()

if args.table_name != "vlad_polymarket_history":
    print("❌ Ожидалось: vlad_polymarket_history")
    sys.exit(1)

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

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return s

# ────────────────────────────────────────────────
# Класс коллектора истории
# ────────────────────────────────────────────────

class PolymarketHistoryCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection()
        c = conn.cursor()

        # Базовая структура
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            condition_id        VARCHAR(100) NOT NULL,
            token_id            VARCHAR(100) NOT NULL,
            question            TEXT,
            price_timestamp     DATETIME NOT NULL,
            price               FLOAT NOT NULL,
            loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_cond_ts (condition_id, price_timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Добавляем все остальные колонки, если их нет
        columns = {
            "volume":       "DECIMAL(20,2)      COMMENT 'Общий объём торгов (USDC)'",
            "volume_24h":   "DECIMAL(20,2)      COMMENT 'Объём за 24ч'",
            "liquidity":    "DECIMAL(20,2)      COMMENT 'Ликвидность'",
            "end_date":     "VARCHAR(50)        COMMENT 'Дата экспирации'",
            "slug":         "VARCHAR(255)       COMMENT 'Slug рынка'",
            "tags":         "VARCHAR(500)       COMMENT 'Теги через запятую'",
            "outcome_yes":  "FLOAT              COMMENT 'Текущая цена YES'",
            "outcome_no":   "FLOAT              COMMENT 'Текущая цена NO'",
            "spread":       "FLOAT              COMMENT 'Спред bid-ask'",
            "num_outcomes": "INT                COMMENT 'Количество исходов'",
            "active":       "TINYINT(1)         COMMENT 'Активен (1) / закрыт (0)'",
            "raw_json":     "TEXT               COMMENT 'Полный JSON рынка'"
        }

        c.execute(f"SHOW COLUMNS FROM `{self.table_name}`")
        existing = {row[0] for row in c.fetchall()}

        for col, definition in columns.items():
            if col not in existing:
                print(f"   → Добавляю колонку: {col}")
                try:
                    c.execute(f"ALTER TABLE `{self.table_name}` ADD COLUMN `{col}` {definition}")
                except Exception as e:
                    print(f"   Не удалось добавить {col}: {e}")

        # Индексы
        try:
            c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX idx_token        (token_id)")
            c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX idx_timestamp    (price_timestamp)")
            c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX idx_price        (price)")
            c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX idx_volume       (volume DESC)")
            c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX idx_slug         (slug)")
            c.execute(f"ALTER TABLE `{self.table_name}` ADD INDEX idx_active       (active)")
        except:
            pass

        conn.commit()
        c.close()
        conn.close()
        print("   Таблица готова")

    def get_last_timestamp(self, condition_id):
        try:
            conn = self.get_db_connection()
            c = conn.cursor()
            c.execute(f"SELECT MAX(price_timestamp) FROM `{self.table_name}` WHERE condition_id = %s", (condition_id,))
            row = c.fetchone()
            c.close()
            conn.close()
            return int(row[0].timestamp()) if row and row[0] else None
        except:
            return None

    def fetch_all_markets(self):
        all_markets = []

        # Активные рынки
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
                    timeout=20
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
                time.sleep(random.uniform(0.4, 0.9))
            except Exception as e:
                print(f"\n ⚠️ Gamma active error: {e}")
                break

        # Топ-5000 закрытых по volume
        print(f"\n 📡 Gamma API → топ-5000 закрытых...")
        offset = 0
        closed_count = 0
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
                    timeout=20
                )
                if r.status_code != 200:
                    break
                data = r.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets:
                    break
                all_markets.extend(markets)
                closed_count += len(markets)
                print(f" ...закрытых: {closed_count:,}", end="\r")
                offset += len(markets)
                if len(markets) < 100 or closed_count >= 5000:
                    break
                time.sleep(random.uniform(0.4, 0.9))
            except Exception as e:
                print(f"\n ⚠️ Gamma closed error: {e}")
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
            r = self.session.get("https://clob.polymarket.com/prices-history", params=params, timeout=25)
            if r.status_code == 200:
                return r.json().get("history", [])
        except:
            pass
        return []

    def extract_market_meta(self, market):
        cid = market.get("condition_id") or market.get("conditionId") or ""
        if not cid:
            return None

        tokens = market.get("tokens", [])
        clob_raw = market.get("clobTokenIds")

        tid = ""
        yes_p = no_p = None

        # CLOB формат
        if isinstance(tokens, list) and tokens:
            tid = tokens[0].get("token_id", "")
            yes_p = _sf(tokens[0].get("price"))
            if len(tokens) >= 2:
                no_p = _sf(tokens[1].get("price"))

        # Gamma формат (list или строка JSON)
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

        # outcomePrices как fallback для цен
        outcomes = market.get("outcomePrices", [])
        if isinstance(outcomes, list):
            if len(outcomes) >= 1 and yes_p is None:
                yes_p = _sf(outcomes[0])
            if len(outcomes) >= 2 and no_p is None:
                no_p = _sf(outcomes[1])

        best_bid  = _sf(market.get("bestBid"))
        best_ask  = _sf(market.get("bestAsk"))
        spread    = round(best_ask - best_bid, 4) if best_bid is not None and best_ask is not None else None

        tags_raw = market.get("tags", [])
        tags_str = ",".join(str(t.get("label", t) if isinstance(t, dict) else t) for t in tags_raw[:15])

        num_out = len(tokens) if isinstance(tokens, list) else (len(clob_ids) if 'clob_ids' in locals() and isinstance(clob_ids, list) else None)

        return {
            "condition_id": cid[:100],
            "token_id":     tid[:100],
            "question":     (market.get("question") or "").strip()[:2000],
            "volume":       _sf(market.get("volume") or market.get("volumeNum")),
            "volume_24h":   _sf(market.get("volume24hr") or market.get("volume_24h")),
            "liquidity":    _sf(market.get("liquidity") or market.get("liquidityNum")),
            "end_date":     str(market.get("endDate") or market.get("end_date_iso") or "")[:50],
            "slug":         str(market.get("slug") or market.get("market_slug") or "")[:255],
            "tags":         tags_str[:500] or None,
            "outcome_yes":  yes_p,
            "outcome_no":   no_p,
            "spread":       spread,
            "num_outcomes": num_out,
            "active":       1 if market.get("active", True) else 0,
            "raw_json":     json.dumps(market, ensure_ascii=False, default=str)[:8000]
        }

    def process(self):
        self.ensure_table()

        print(" 📡 Загрузка списка рынков...")
        markets = self.fetch_all_markets()
        if not markets:
            print(" ⚠️ Не удалось загрузить рынки")
            return

        market_metas = []
        for m in markets:
            meta = self.extract_market_meta(m)
            if meta:
                market_metas.append(meta)

        print(f" 🎯 Рынков с token_id: {len(market_metas):,}")

        if not market_metas:
            print("   → нет рынков с валидным token_id")
            return

        total_points = total_new = 0
        conn = self.get_db_connection()
        c = conn.cursor()

        sql = f"""
        INSERT IGNORE INTO `{self.table_name}`
        (condition_id, token_id, question, price_timestamp, price,
         volume, volume_24h, liquidity, end_date, slug, tags,
         outcome_yes, outcome_no, spread, num_outcomes, active, raw_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        BATCH_SIZE = 50

        for i, meta in enumerate(market_metas):
            cid = meta["condition_id"]
            tid = meta["token_id"]

            last_ts = self.get_last_timestamp(cid)
            start_ts = last_ts + 1 if last_ts else None

            history = self.fetch_price_history(tid, start_ts=start_ts)

            if not history:
                if (i + 1) % 200 == 0:
                    print(f" ...{i+1:,}/{len(market_metas):,}  (новых точек: {total_new:,})", end="\r")
                time.sleep(random.uniform(0.6, 1.3))
                continue

            rows = []
            for p in history:
                ts = p.get("t", 0)
                price = p.get("p", 0)
                if ts > 0:
                    dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append((
                        cid, tid, meta["question"], dt, price,
                        meta["volume"], meta["volume_24h"], meta["liquidity"],
                        meta["end_date"], meta["slug"], meta["tags"],
                        meta["outcome_yes"], meta["outcome_no"], meta["spread"],
                        meta["num_outcomes"], meta["active"], meta["raw_json"]
                    ))

            if rows:
                c.executemany(sql, rows)
                total_new += c.rowcount
                total_points += len(rows)

            if (i + 1) % BATCH_SIZE == 0:
                conn.commit()
                print(f" ...{i+1:,}/{len(market_metas):,}  новых точек: {total_new:,}  всего: {total_points:,}", end="\r")

            time.sleep(random.uniform(0.8, 1.4))

        conn.commit()
        c.close()
        conn.close()

        print(f"\n\n ✅ Итог: загружено {total_points:,} ценовых точек, из них новых — {total_new:,}")

# ────────────────────────────────────────────────
# Запуск
# ────────────────────────────────────────────────

def main():
    print(f"🚀 Polymarket History Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"Таблица: {args.table_name}")
    print("=" * 60)

    collector = PolymarketHistoryCollector(args.table_name)
    collector.process()

    print("=" * 60)
    print("🏁 Завершено")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        send_error_trace(e)
        sys.exit(1)
