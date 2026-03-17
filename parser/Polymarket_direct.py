"""
WorldMonitor фильтрует рынки по тегам: politics, geopolitics, elections, ukraine, china, middle-east, iran

Таблица: vlad_polymarket_direct

Запуск:
  python Polymarket_direct.py vlad_polymarket_direct [host] [port] [user] [password] [database]
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
    try: requests.post(TRACE_URL, data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}, timeout=10)
    except: pass

parser = argparse.ArgumentParser(description="Polymarket Direct → MySQL")
parser.add_argument("table_name")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения"); sys.exit(1)

DB_CONFIG = {'host': args.host, 'port': int(args.port), 'user': args.user, 'password': args.password, 'database': args.database}
DATASETS = {
    "vlad_polymarket_direct": {"description": "Polymarket — текущие snapshots всех рынков"},
    "vlad_polymarket_history": {"description": "Polymarket — полная история цен (backfill через /prices-history)"},
}

# === WorldMonitor фильтры (из scripts/data/prediction-tags.json) ===
GEO_TAGS = {"politics", "geopolitics", "elections", "world", "ukraine", "china", "middle-east",
            "europe", "economy", "fed", "inflation", "iran", "taiwan", "russia", "israel",
            "ai", "crypto", "trade", "tariffs", "recession", "war", "nato", "nuclear",
            "sanctions", "oil", "opec", "military"}

EXCLUDE_KEYWORDS = {
    "nba", "nfl", "mlb", "nhl", "fifa", "world cup", "super bowl", "championship",
    "playoffs", "oscar", "grammy", "emmy", "box office", "movie", "album", "song",
    "streamer", "influencer", "celebrity", "kardashian", "bachelor", "reality tv",
    "mvp", "touchdown", "home run", "goal scorer", "academy award", "bafta",
    "golden globe", "cannes", "sundance", "documentary", "feature film", "tv series",
    "season finale", "ufc", "boxing", "tennis", "golf", "formula 1", "cricket",
    "premier league", "champions league", "la liga", "bundesliga", "serie a",
    "ligue 1", "olympics", "paralympics", "wrestling", "nascar"
}

def _sf(v):
    if v is None or v == "": return None
    try: return float(str(v).replace(",", ""))
    except: return None

def build_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return s

def is_geopolitical(market):
    """Воспроизведение WM-фильтрации: только геополитические/экономические рынки"""
    title = str(market.get("question", market.get("title", ""))).lower()
    tags = [str(t).lower() for t in (market.get("tags", []) or [])]
    slug = str(market.get("slug", market.get("market_slug", ""))).lower()

    # Exclude sports/entertainment
    for kw in EXCLUDE_KEYWORDS:
        if kw in title: return False

    # Include if tags match
    for t in tags:
        if t in GEO_TAGS: return True

    # Include if title contains geo keywords
    for kw in GEO_TAGS:
        if kw in title or kw in slug: return True

    return False


class PolymarketDirectCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                condition_id VARCHAR(100) COMMENT 'Polymarket condition ID (дедупликация)',
                question TEXT NOT NULL COMMENT 'Вопрос: Will X happen?',
                probability FLOAT COMMENT 'Вероятность YES (0.0-1.0)',
                volume DECIMAL(20,2) COMMENT 'Общий объём торгов (USDC)',
                volume_24h DECIMAL(20,2) COMMENT 'Объём за 24ч',
                liquidity DECIMAL(20,2) COMMENT 'Ликвидность в orderbook',
                end_date VARCHAR(50) COMMENT 'Дата разрешения',
                slug VARCHAR(255),
                tags VARCHAR(500),
                outcome_yes FLOAT, outcome_no FLOAT,
                spread FLOAT COMMENT 'Bid-ask spread',
                num_outcomes INT COMMENT 'Кол-во исходов (обычно 2)',
                raw_json TEXT,
                snapshot_hour DATETIME NOT NULL,
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_cond_hour (condition_id, snapshot_hour),
                INDEX idx_probability (probability DESC),
                INDEX idx_volume (volume DESC),
                INDEX idx_snapshot (snapshot_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Polymarket prediction markets (direct CLOB API, hourly snapshots)';
        """)
        conn.commit(); c.close(); conn.close()

    def fetch_markets(self):
        """CLOB API с пагинацией — забираем ВСЕ активные рынки"""
        all_markets = []
        next_cursor = None

        print("   📡 CLOB API (clob.polymarket.com) — полная загрузка...")

        while True:
            try:
                url = "https://clob.polymarket.com/markets"
                params = {"limit": 500}  # Max per page
                if next_cursor:
                    params["next_cursor"] = next_cursor

                resp = self.session.get(url, params=params, timeout=20)
                if resp.status_code != 200:
                    print(f"   ⚠️ HTTP {resp.status_code}")
                    break

                data = resp.json()

                # CLOB API returns: list or {"data": [...], "next_cursor": "..."}
                if isinstance(data, list):
                    markets = data
                    next_cursor = None
                elif isinstance(data, dict):
                    markets = data.get("data", data.get("markets", []))
                    next_cursor = data.get("next_cursor")
                else:
                    break

                if not markets:
                    break

                all_markets.extend(markets)
                print(f"   ...загружено {len(all_markets)} рынков", end="\r")

                # Если нет next_cursor или получили меньше лимита — конец
                if not next_cursor or next_cursor == "LTE" or len(markets) < 100:
                    break

                time.sleep(random.uniform(0.3, 0.8))

            except Exception as e:
                print(f"   ⚠️ CLOB error: {e}")
                break

        print(f"\n   ✅ CLOB: {len(all_markets)} рынков всего")

        # Fallback: Gamma API если CLOB пустой
        if not all_markets:
            try:
                print("   📡 Fallback: Gamma API...")
                resp = self.session.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"active": "true", "closed": "false", "limit": "200", "order": "volume24hr", "ascending": "false"},
                    timeout=15
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list): all_markets = data
                    elif isinstance(data, dict): all_markets = data.get("data", data.get("events", []))
                    print(f"   ✅ Gamma: {len(all_markets)} events")
            except Exception as e:
                print(f"   ⚠️ Gamma failed: {e}")

        return all_markets

    def process(self):
        self.ensure_table()
        raw_markets = self.fetch_markets()

        if not raw_markets:
            print("   ⚠️ Нет данных от всех tier")
            return

        # Flatten events → markets (Gamma/Strapi return events with nested markets)
        markets = []
        for item in raw_markets:
            if "markets" in item and isinstance(item["markets"], list):
                for m in item["markets"]:
                    m["_event_title"] = item.get("title", item.get("question", ""))
                    m["_event_slug"] = item.get("slug", "")
                    m["_event_tags"] = item.get("tags", [])
                    markets.append(m)
            else:
                markets.append(item)

        # Записываем ВСЕ рынки — модель сама найдёт корреляции
        print(f"   🔮 {len(markets)} рынков всего (без фильтра)")

        if not markets:
            print("   ⚠️ Нет рынков")
            return

        geo_markets = markets  # Все рынки

        now = datetime.utcnow()
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (condition_id, question, probability, volume, volume_24h, liquidity, end_date,
             slug, tags, outcome_yes, outcome_no, spread, num_outcomes, raw_json, snapshot_hour, snapshot_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        rows = []
        for m in geo_markets:
            cid = str(m.get("condition_id", m.get("conditionId", m.get("id", ""))))[:100]
            question = m.get("question", m.get("_event_title", m.get("title", "")))

            # Extract prices
            outcomes = m.get("outcomePrices", m.get("outcomes", []))
            yes_p = no_p = None
            if isinstance(outcomes, list):
                if len(outcomes) >= 1:
                    if isinstance(outcomes[0], str): yes_p = _sf(outcomes[0])
                    elif isinstance(outcomes[0], dict): yes_p = _sf(outcomes[0].get("price"))
                if len(outcomes) >= 2:
                    if isinstance(outcomes[1], str): no_p = _sf(outcomes[1])
                    elif isinstance(outcomes[1], dict): no_p = _sf(outcomes[1].get("price"))

            prob = _sf(m.get("lastTradePrice", m.get("outcomePrices", [None])[0] if isinstance(m.get("outcomePrices"), list) and m.get("outcomePrices") else None))
            if prob is None and yes_p is not None: prob = yes_p

            # Spread
            best_bid = _sf(m.get("bestBid"))
            best_ask = _sf(m.get("bestAsk"))
            spread = None
            if best_bid and best_ask: spread = round(best_ask - best_bid, 4)

            # Tags
            tags_raw = m.get("_event_tags", m.get("tags", []))
            tags_str = ",".join(str(t.get("label", t) if isinstance(t, dict) else t) for t in (tags_raw or [])[:10])

            rows.append((
                cid if cid else None,
                str(question)[:2000],
                prob,
                _sf(m.get("volume", m.get("volumeNum"))),
                _sf(m.get("volume24hr", m.get("volume_24h"))),
                _sf(m.get("liquidity", m.get("liquidityNum"))),
                str(m.get("endDate", m.get("end_date_iso", m.get("resolutionDate", ""))))[:50],
                str(m.get("_event_slug", m.get("slug", "")))[:255],
                tags_str[:500] if tags_str else None,
                yes_p, no_p, spread,
                len(outcomes) if isinstance(outcomes, list) else None,
                json.dumps(m, ensure_ascii=False, default=str)[:5000],
                snapshot_hour, snapshot_at,
            ))

        c.executemany(sql, rows)
        conn.commit(); inserted = c.rowcount; c.close(); conn.close()

        print(f"   ✅ Записано {inserted} prediction markets")
        # TOP-5
        sorted_m = sorted(geo_markets, key=lambda x: float(x.get("volume", x.get("volumeNum", 0)) or 0), reverse=True)
        for m in sorted_m[:5]:
            q = m.get("question", m.get("_event_title", "?"))[:55]
            vol = m.get("volume", m.get("volumeNum", 0))
            print(f"      ${vol:>12}  {q}")


class PolymarketHistoryCollector:
    """
    Backfill полной ценовой истории через /prices-history.
    Таблица vlad_polymarket_history содержит ВСЕ поля из vlad_polymarket_direct
    + price_timestamp для каждой часовой точки.

    Алгоритм:
      1. Загрузить список всех рынков (CLOB /markets с пагинацией)
      2. Для каждого рынка:
         - Сохранить текущие метаданные (volume, liquidity, tags, slug, end_date...)
         - Проверить последний timestamp в БД
         - Запросить /prices-history?market={token_id}&interval=max&fidelity=60
         - Записать каждую ценовую точку с полными метаданными рынка
      3. INSERT IGNORE по (condition_id, price_timestamp) — дедупликация
    """
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self): return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection(); c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                condition_id VARCHAR(100) NOT NULL COMMENT 'Market condition ID',
                token_id VARCHAR(100) NOT NULL COMMENT 'CLOB token ID (YES outcome)',
                question TEXT COMMENT 'Market question',
                price_timestamp DATETIME NOT NULL COMMENT 'UTC timestamp ценовой точки',
                price FLOAT NOT NULL COMMENT 'Цена YES токена (≈ probability)',
                volume DECIMAL(20,2) COMMENT 'Общий объём торгов (USDC) на момент snapshot',
                volume_24h DECIMAL(20,2) COMMENT 'Объём за 24ч',
                liquidity DECIMAL(20,2) COMMENT 'Ликвидность в orderbook',
                end_date VARCHAR(50) COMMENT 'Дата разрешения рынка',
                slug VARCHAR(255) COMMENT 'URL slug рынка',
                tags VARCHAR(500) COMMENT 'Теги рынка',
                outcome_yes FLOAT COMMENT 'Текущая цена YES (на момент загрузки)',
                outcome_no FLOAT COMMENT 'Текущая цена NO (на момент загрузки)',
                spread FLOAT COMMENT 'Bid-ask spread',
                num_outcomes INT COMMENT 'Кол-во исходов',
                active TINYINT(1) COMMENT 'Рынок активен (1) или закрыт (0)',
                raw_json TEXT COMMENT 'Полный JSON рынка с CLOB API',
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_cond_ts (condition_id, price_timestamp),
                INDEX idx_token (token_id),
                INDEX idx_timestamp (price_timestamp),
                INDEX idx_price (price),
                INDEX idx_volume (volume DESC),
                INDEX idx_slug (slug),
                INDEX idx_active (active)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Polymarket: полная ценовая история с метаданными (hourly, backfill)';
        """)
        conn.commit(); c.close(); conn.close()

    def get_last_timestamp(self, condition_id):
        """Последний timestamp для этого condition_id в БД."""
        try:
            conn = self.get_db_connection(); c = conn.cursor()
            c.execute(f"SELECT MAX(price_timestamp) FROM `{self.table_name}` WHERE condition_id = %s", (condition_id,))
            row = c.fetchone(); c.close(); conn.close()
            if row and row[0]:
                return int(row[0].timestamp())
            return None
        except: return None

    def fetch_all_markets(self):
        """
        Загружает рынки через Gamma API (metadata-rich, с фильтрами).
        CLOB /markets отдаёт 300k+ архивных — слишком много.
        Gamma API позволяет фильтровать по active/closed и сортировать по volume.
        
        Стратегия: 
          1. Активные рынки (accepting_orders) — все
          2. Недавно закрытые (за последний год) — для исторического контекста
        """
        all_markets = []

        # === Tier 1: Gamma API — активные рынки (сортировка по volume) ===
        print("   📡 Gamma API: активные рынки...")
        offset = 0
        while True:
            try:
                resp = self.session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "limit": "100",
                        "offset": str(offset),
                        "order": "volume24hr",
                        "ascending": "false",
                    },
                    timeout=20
                )
                if resp.status_code != 200:
                    print(f"   ⚠️ Gamma HTTP {resp.status_code}, пробуем CLOB fallback")
                    break
                data = resp.json()
                if isinstance(data, list): markets = data
                elif isinstance(data, dict): markets = data.get("data", data.get("markets", []))
                else: break
                if not markets: break
                all_markets.extend(markets)
                print(f"   ...активных: {len(all_markets)}", end="\r")
                offset += len(markets)
                if len(markets) < 100: break
                time.sleep(random.uniform(0.3, 0.6))
            except Exception as e:
                print(f"   ⚠️ Gamma error: {e}")
                break

        # === Tier 2: Gamma — недавно закрытые (за последний год) ===
        print(f"\n   📡 Gamma API: недавно закрытые рынки...")
        offset = 0
        closed_count = 0
        while True:
            try:
                resp = self.session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={
                        "active": "false",
                        "closed": "true",
                        "limit": "100",
                        "offset": str(offset),
                        "order": "volume",
                        "ascending": "false",
                    },
                    timeout=20
                )
                if resp.status_code != 200: break
                data = resp.json()
                if isinstance(data, list): markets = data
                elif isinstance(data, dict): markets = data.get("data", data.get("markets", []))
                else: break
                if not markets: break
                all_markets.extend(markets)
                closed_count += len(markets)
                print(f"   ...закрытых: {closed_count}", end="\r")
                offset += len(markets)
                # Берём максимум 5000 закрытых (самых крупных по volume)
                if len(markets) < 100 or closed_count >= 5000: break
                time.sleep(random.uniform(0.3, 0.6))
            except Exception as e:
                print(f"   ⚠️ {e}")
                break

        print(f"\n   📋 Итого рынков: {len(all_markets)} (активных + топ закрытых)")

        # === Fallback: CLOB API если Gamma не дал результатов ===
        if len(all_markets) < 50:
            print("   📡 Fallback: CLOB API (только active)...")
            next_cursor = None
            while True:
                try:
                    params = {"limit": 500, "active": "true"}
                    if next_cursor: params["next_cursor"] = next_cursor
                    resp = self.session.get("https://clob.polymarket.com/markets", params=params, timeout=20)
                    if resp.status_code != 200: break
                    data = resp.json()
                    if isinstance(data, list): markets = data; next_cursor = None
                    elif isinstance(data, dict): markets = data.get("data", []); next_cursor = data.get("next_cursor")
                    else: break
                    if not markets: break
                    all_markets.extend(markets)
                    print(f"   ...CLOB: {len(all_markets)}", end="\r")
                    if not next_cursor or next_cursor == "LTE" or len(markets) < 100: break
                    time.sleep(random.uniform(0.3, 0.8))
                except Exception as e:
                    print(f"   ⚠️ {e}"); break
            print(f"\n   📋 CLOB fallback: {len(all_markets)} рынков")

        return all_markets

    def fetch_price_history(self, token_id, start_ts=None):
        """GET /prices-history — возвращает [{"t": unix_ts, "p": price}, ...]"""
        params = {"market": token_id, "interval": "max", "fidelity": 60}
        if start_ts:
            params.pop("interval", None)
            params["startTs"] = start_ts
            params["endTs"] = int(time.time())
        try:
            resp = self.session.get("https://clob.polymarket.com/prices-history", params=params, timeout=20)
            if resp.status_code == 200:
                return resp.json().get("history", [])
        except: pass
        return []

    def extract_market_meta(self, m):
        """Извлекает все метаданные рынка в dict (как в vlad_polymarket_direct)."""
        cid = m.get("condition_id", m.get("conditionId", ""))
        if not cid:
            return None

        # Сначала пробуем CLOB-формат (tokens array)
        tokens = m.get("tokens", [])
        tid = ""
        yes_p = no_p = None

        if isinstance(tokens, list) and tokens:
            tid = tokens[0].get("token_id", "")
            yes_p = _sf(tokens[0].get("price"))
            if len(tokens) >= 2:
                no_p = _sf(tokens[1].get("price"))

        # Fallback: Gamma API формат (clobTokenIds)
        if not tid:
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, list) and clob_ids:
                tid = str(clob_ids[0])

        if not tid:
            return None

        # Outcomes from outcomePrices
        outcomes = m.get("outcomePrices", [])
        if isinstance(outcomes, list):
            if len(outcomes) >= 1 and yes_p is None:
                yes_p = _sf(outcomes[0])
            if len(outcomes) >= 2 and no_p is None:
                no_p = _sf(outcomes[1])

        # Spread
        best_bid = _sf(m.get("bestBid"))
        best_ask = _sf(m.get("bestAsk"))
        spread = round(best_ask - best_bid, 4) if best_bid and best_ask else None

        # Tags
        tags_raw = m.get("tags", [])
        tags_str = ",".join(str(t.get("label", t) if isinstance(t, dict) else t) for t in (tags_raw or [])[:15])

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
            "num_outcomes": len(tokens) if isinstance(tokens, list) else None,
            "active": 1 if m.get("active", True) else 0,
            "raw_json": json.dumps(m, ensure_ascii=False, default=str)[:5000],
        }

    def process(self):
        self.ensure_table()

        # 1. Загрузить все рынки с полными метаданными
        print("   📡 Загрузка списка рынков...")
        markets = self.fetch_all_markets()
        if not markets:
            print("   ⚠️ Нет рынков"); return

        # 2. Извлечь метаданные каждого рынка
        market_metas = []
        for m in markets:
            meta = self.extract_market_meta(m)
            if meta:
                market_metas.append(meta)

        print(f"   🎯 Рынков с token_id: {len(market_metas)}")

        # 3. Для каждого рынка — скачать историю цен + записать с метаданными
        total_points = 0
        total_new = 0
        conn = self.get_db_connection(); c = conn.cursor()
        sql = f"""INSERT IGNORE INTO `{self.table_name}`
            (condition_id, token_id, question, price_timestamp, price,
             volume, volume_24h, liquidity, end_date, slug, tags,
             outcome_yes, outcome_no, spread, num_outcomes, active, raw_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i, meta in enumerate(market_metas):
            cid = meta["condition_id"]
            tid = meta["token_id"]

            # Incremental: проверяем последний timestamp
            last_ts = self.get_last_timestamp(cid)
            start_ts = last_ts + 1 if last_ts else None

            history = self.fetch_price_history(tid, start_ts=start_ts)

            if not history:
                if (i + 1) % 100 == 0:
                    print(f"   ...{i+1}/{len(market_metas)} рынков, {total_new} новых точек", end="\r")
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

            # Commit каждые 50 рынков
            if (i + 1) % 50 == 0:
                conn.commit()
                print(f"   ...{i+1}/{len(market_metas)} рынков, {total_new} новых точек, {total_points} total", end="\r")

            # Rate limit: 60 req/min (Polymarket CLOB). 1 req/sec = безопасно
            time.sleep(random.uniform(0.8, 1.2))

        conn.commit(); c.close(); conn.close()

        print(f"\n   ✅ Загружено {total_points} ценовых точек, {total_new} новых")


def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица. Допустимые:"); [print(f"  - {n}") for n in DATASETS]; sys.exit(1)
    print(f"🚀 Polymarket Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 Таблица: {args.table_name}"); print("=" * 60)

    if args.table_name == "vlad_polymarket_direct":
        PolymarketDirectCollector(args.table_name).process()
    elif args.table_name == "vlad_polymarket_history":
        PolymarketHistoryCollector(args.table_name).process()

    print("=" * 60); print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    try: main()
    except SystemExit: raise
    except KeyboardInterrupt: print("\n🛑 Прервано"); sys.exit(1)
    except Exception as e: print(f"\n❌ {e!r}"); send_error_trace(e); sys.exit(1)
