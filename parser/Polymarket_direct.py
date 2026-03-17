"""
WorldMonitor фильтрует рынки по тегам: politics, geopolitics, elections, ukraine, china, middle-east, iran
Таблица: vlad_polymarket_history
Запуск:
  python Polymarket_direct.py vlad_polymarket_history [host] [port] [user] [password] [database]
"""
import os, sys, argparse, json, time, random, traceback
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "polymarket_direct")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc, script_name="Polymarket_direct.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except:
        pass

parser = argparse.ArgumentParser(description="Polymarket Direct → MySQL")
parser.add_argument("table_name")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database
}

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

class PolymarketHistoryCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection()
        c = conn.cursor()

        # Создаём таблицу, если её нет
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                condition_id VARCHAR(100) NOT NULL,
                token_id VARCHAR(100) NOT NULL,
                question TEXT,
                price_timestamp DATETIME NOT NULL,
                price FLOAT NOT NULL,
                volume DECIMAL(20,2),
                volume_24h DECIMAL(20,2),
                liquidity DECIMAL(20,2),
                end_date VARCHAR(50),
                slug VARCHAR(255),
                tags VARCHAR(500),
                outcome_yes FLOAT,
                outcome_no FLOAT,
                spread FLOAT,
                num_outcomes INT,
                active TINYINT(1),
                raw_json TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_cond_ts (condition_id, price_timestamp),
                INDEX idx_token (token_id),
                INDEX idx_timestamp (price_timestamp),
                INDEX idx_price (price),
                INDEX idx_volume (volume DESC),
                INDEX idx_slug (slug),
                INDEX idx_active (active)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Polymarket: полная ценовая история (hourly backfill)';
        """)

        # === Миграция колонок (на случай старой таблицы) ===
        columns_to_add = [
            ("volume", "DECIMAL(20,2)"),
            ("volume_24h", "DECIMAL(20,2)"),
            ("liquidity", "DECIMAL(20,2)"),
            ("outcome_yes", "FLOAT"),
            ("outcome_no", "FLOAT"),
            ("spread", "FLOAT"),
            ("num_outcomes", "INT"),
            ("active", "TINYINT(1)"),
            ("raw_json", "TEXT"),
            ("tags", "VARCHAR(500)"),
        ]

        for col, typ in columns_to_add:
            try:
                c.execute(f"ALTER TABLE `{self.table_name}` ADD COLUMN IF NOT EXISTS `{col}` {typ}")
            except:
                pass  # колонка уже есть или ошибка MySQL < 8.0

        conn.commit()
        c.close()
        conn.close()

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
        # Активные
        print(" 📡 Gamma API: активные рынки...")
        offset = 0
        while True:
            try:
                resp = self.session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"active": "true", "closed": "false", "limit": "100", "offset": str(offset), "order": "volume24hr", "ascending": "false"},
                    timeout=20
                )
                if resp.status_code != 200: break
                data = resp.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets: break
                all_markets.extend(markets)
                print(f" ...активных: {len(all_markets)}", end="\r")
                offset += len(markets)
                if len(markets) < 100: break
                time.sleep(random.uniform(0.3, 0.6))
            except Exception as e:
                print(f" ⚠️ Gamma error: {e}")
                break

        # ТОП-5000 закрытых
        print(f"\n 📡 Gamma API: недавно закрытые рынки...")
        offset = 0
        closed_count = 0
        while True:
            try:
                resp = self.session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"active": "false", "closed": "true", "limit": "100", "offset": str(offset), "order": "volume", "ascending": "false"},
                    timeout=20
                )
                if resp.status_code != 200: break
                data = resp.json()
                markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets: break
                all_markets.extend(markets)
                closed_count += len(markets)
                print(f" ...закрытых: {closed_count}", end="\r")
                offset += len(markets)
                if len(markets) < 100 or closed_count >= 5000:
                    break
                time.sleep(random.uniform(0.3, 0.6))
            except Exception as e:
                print(f" ⚠️ {e}")
                break

        print(f"\n 📋 Итого рынков: {len(all_markets)} (активных + топ-5000 закрытых)")
        return all_markets

    def fetch_price_history(self, token_id, start_ts=None):
        params = {"market": token_id, "interval": "max", "fidelity": 60}
        if start_ts:
            params.pop("interval", None)
            params["startTs"] = start_ts
            params["endTs"] = int(time.time())
        try:
            resp = self.session.get("https://clob.polymarket.com/prices-history", params=params, timeout=20)
            if resp.status_code == 200:
                return resp.json().get("history", [])
        except:
            pass
        return []

    def extract_market_meta(self, m):
        cid = m.get("condition_id", m.get("conditionId", ""))
        if not cid:
            return None

        tokens = m.get("tokens", [])
        clob_raw = m.get("clobTokenIds")
        tid = ""
        yes_p = no_p = None

        if isinstance(tokens, list) and tokens:
            tid = tokens[0].get("token_id", "")
            yes_p = _sf(tokens[0].get("price"))
            if len(tokens) >= 2:
                no_p = _sf(tokens[1].get("price"))

        if not tid:
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
        spread = round(best_ask - best_bid, 4) if best_bid and best_ask else None

        tags_raw = m.get("tags", [])
        tags_str = ",".join(str(t.get("label", t) if isinstance(t, dict) else t) for t in (tags_raw or [])[:15])

        num_outcomes = len(tokens) if isinstance(tokens, list) and tokens else (len(clob_ids) if isinstance(clob_ids, list) and clob_ids else None)

        return {
            "condition_id": cid[:100],
            "token_id": tid[:100],
            "question": m.get("question", "")[:2000],
            "volume": _sf(m.get("volume", m.get("volumeNum"))),
            "volume_24h": _sf(m.get("volume24hr", m.get("volume_24h"))),
            "liquidity": _sf(m.get("liquidity", m.get("liquidityNum"))),
            "end_date": str(m.get("endDate", m.get("end_date_iso", "")))[:50],
            "slug": str(m.get("market_slug", m.get("slug", "")))[:255],
            "tags": tags_str[:500] if tags_str else None,
            "outcome_yes": yes_p,
            "outcome_no": no_p,
            "spread": spread,
            "num_outcomes": num_outcomes,
            "active": 1 if m.get("active", True) else 0,
            "raw_json": json.dumps(m, ensure_ascii=False, default=str)[:5000],
        }

    def process(self):
        self.ensure_table()
        print(" 📡 Загрузка списка рынков...")
        markets = self.fetch_all_markets()
        if not markets:
            print(" ⚠️ Нет рынков")
            return

        market_metas = [meta for m in markets if (meta := self.extract_market_meta(m))]
        print(f" 🎯 Рынков с token_id: {len(market_metas)}")

        total_points = total_new = 0
        conn = self.get_db_connection()
        c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (condition_id, token_id, question, price_timestamp, price,
             volume, volume_24h, liquidity, end_date, slug, tags,
             outcome_yes, outcome_no, spread, num_outcomes, active, raw_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i, meta in enumerate(market_metas):
            cid = meta["condition_id"]
            tid = meta["token_id"]
            last_ts = self.get_last_timestamp(cid)
            start_ts = last_ts + 1 if last_ts else None
            history = self.fetch_price_history(tid, start_ts=start_ts)

            if not history:
                if (i + 1) % 100 == 0:
                    print(f" ...{i+1}/{len(market_metas)} рынков, {total_new} новых точек", end="\r")
                time.sleep(random.uniform(0.3, 0.5))
                continue

            rows = []
            for point in history:
                ts = point.get("t", 0)
                price = point.get("p", 0)
                if ts > 0:
                    dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append((
                        cid, tid, meta["question"], dt, price,
                        meta["volume"], meta["volume_24h"], meta["liquidity"],
                        meta["end_date"], meta["slug"], meta["tags"],
                        meta["outcome_yes"], meta["outcome_no"], meta["spread"],
                        meta["num_outcomes"], meta["active"], meta["raw_json"],
                    ))

            if rows:
                c.executemany(sql, rows)
                total_new += c.rowcount
                total_points += len(rows)

            if (i + 1) % 50 == 0:
                conn.commit()
                print(f" ...{i+1}/{len(market_metas)} рынков, {total_new} новых точек, {total_points} total", end="\r")

            time.sleep(random.uniform(0.8, 1.2))

        conn.commit()
        c.close()
        conn.close()
        print(f"\n ✅ Загружено {total_points} ценовых точек, {total_new} новых")

def main():
    print(f"🚀 Polymarket Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Таблица: {args.table_name}")
    print("=" * 60)

    if args.table_name == "vlad_polymarket_history":
        PolymarketHistoryCollector(args.table_name).process()

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
        print(f"\n❌ {e!r}")
        send_error_trace(e)
        sys.exit(1)
