"""
Описание: Часовые и дневные котировки 16 активов к USD, затем синтетические кроссы.
          Порядок всегда один: сначала реальные котировки пишутся в БД,
          и только после этого кроссы собираются из уже лежащих там ног.

          Фиат (пары 0, 1, 4–10): Yahoo Finance + MOEX ISS для USD/RUB.
          Крипта (пары 2, 3, 11–15): Coinbase USD, запасной Binance USDT.
          Кроссы: A/B = (A в USD) / (B в USD), 16 активов → C(16,2) = 120 пар.

Запуск:
  python SashaRates.py sasha_rates                     # всё: фиат → крипта → кроссы
  python SashaRates.py sasha_quotes_fx                 # только фиат
  python SashaRates.py sasha_quotes_crypto             # только крипта
  python SashaRates.py sasha_quotes_cross              # только кроссы (ноги уже в БД)
  python SashaRates.py sasha_quotes_cross_fiat         # фиат×фиат
  python SashaRates.py sasha_quotes_cross_crypto       # крипта×крипта
  python SashaRates.py sasha_quotes_cross_mixed        # фиат×крипта
  python SashaRates.py sasha_rates_gbp_usd
  python SashaRates.py sasha_rates_eur_btc_day [host] [port] [user] [password] [database]
БД: DB_* из .env, если пусто — MASTER_*.
QUOTES_FULL_RELOAD=1 — перекачать/пересчитать всю историю заново, а не только хвост.
"""

import os
import sys
import time
import argparse
import traceback
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def _env(*names, default=None):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


_HANDLER = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL = f"{_HANDLER}/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "SashaRates")
EMAIL = os.getenv("ALERT_EMAIL", "samuray150305@gmail.com")

REQUEST_PAUSE = float(os.getenv("QUOTES_PAUSE", os.getenv("QUOTES_FX_PAUSE", "0.35")))
FULL_RELOAD = os.getenv("QUOTES_FULL_RELOAD", "0").strip().lower() in {"1", "true", "yes", "on"}
YAHOO_UA = os.getenv(
    "QUOTES_YAHOO_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)
COINBASE_BASE = os.getenv("COINBASE_API_BASE", "https://api.exchange.coinbase.com").rstrip("/")
BINANCE_BASE = os.getenv("BINANCE_API_BASE", "https://api.binance.com").rstrip("/")

ALL_TABLE = "sasha_rates"
ALIAS_ALL = {"sasha_rates", "sasha_quotes"}
ALIAS_FX = "sasha_quotes_fx"
ALIAS_CRYPTO = "sasha_quotes_crypto"
ALIAS_CROSS = "sasha_quotes_cross"
GROUP_TABLES = {
    "sasha_quotes_cross_fiat": "fiat",
    "sasha_quotes_cross_crypto": "crypto",
    "sasha_quotes_cross_mixed": "mixed",
}

# Сколько последних баров перечитывать у кросса при инкрементальном запуске:
# нужно, чтобы пересчитался t1 у прежней последней строки.
OVERLAP_BARS = 3


# ── Реальные пары ────────────────────────────────────────────────────────────

FX_PAIRS = [
    {
        "pair_id": 0,
        "code": "usd_rub",
        "label": "USD/RUB",
        "yahoo": "RUB=X",
        "moex": "USD000UTSTOM",
        "prefer": "moex",
        # Биржевые торги USD/RUB на MOEX прерывались (нет данных с 2024-06 по 2026-02),
        # поэтому пробелы закрываются котировками Yahoo.
        "merge": True,
    },
    {
        "pair_id": 1,
        "code": "eur_usd",
        "label": "EUR/USD",
        "yahoo": "EURUSD=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 4,
        "code": "gbp_usd",
        "label": "GBP/USD",
        "yahoo": "GBPUSD=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 5,
        "code": "usd_cny",
        "label": "USD/CNY",
        "yahoo": "CNY=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 6,
        "code": "usd_jpy",
        "label": "USD/JPY",
        "yahoo": "JPY=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 7,
        "code": "usd_aed",
        "label": "USD/AED",
        "yahoo": "AED=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 8,
        "code": "usd_inr",
        "label": "USD/INR",
        "yahoo": "INR=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 9,
        "code": "aud_usd",
        "label": "AUD/USD",
        "yahoo": "AUDUSD=X",
        "prefer": "yahoo",
    },
    {
        "pair_id": 10,
        "code": "usd_chf",
        "label": "USD/CHF",
        "yahoo": "CHF=X",
        "prefer": "yahoo",
    },
]

CRYPTO_PAIRS = [
    {"pair_id": 2, "code": "btc_usd", "label": "BTC/USD", "coinbase": "BTC-USD", "binance": "BTCUSDT"},
    {"pair_id": 3, "code": "eth_usd", "label": "ETH/USD", "coinbase": "ETH-USD", "binance": "ETHUSDT"},
    {"pair_id": 11, "code": "bch_usd", "label": "BCH/USD", "coinbase": "BCH-USD", "binance": "BCHUSDT"},
    {"pair_id": 12, "code": "sol_usd", "label": "SOL/USD", "coinbase": "SOL-USD", "binance": "SOLUSDT"},
    {"pair_id": 13, "code": "xrp_usd", "label": "XRP/USD", "coinbase": "XRP-USD", "binance": "XRPUSDT"},
    {"pair_id": 14, "code": "dash_usd", "label": "DASH/USD", "coinbase": "DASH-USD", "binance": "DASHUSDT"},
    {"pair_id": 15, "code": "bnb_usd", "label": "BNB/USD", "coinbase": "BNB-USD", "binance": "BNBUSDT"},
]


def _real_table(code: str, day: bool) -> str:
    return f"sasha_rates_{code}" + ("_day" if day else "")


def _build_real_datasets(pairs: list, kind: str) -> dict:
    datasets = {}
    for spec in pairs:
        for day in (False, True):
            datasets[_real_table(spec["code"], day)] = {
                **spec,
                "kind": kind,
                "day": day,
                "description": f"{spec['label']} {'daily' if day else 'hourly'} OHLC",
            }
    return datasets


FX_DATASETS = _build_real_datasets(FX_PAIRS, "fx")
CRYPTO_DATASETS = _build_real_datasets(CRYPTO_PAIRS, "crypto")
REAL_DATASETS = {**FX_DATASETS, **CRYPTO_DATASETS}


# ── Кроссы ───────────────────────────────────────────────────────────────────
# Актив → (таблица к USD, инвертировать ли котировку).
# invert=True означает, что таблица хранит «сколько актива за 1 USD»,
# то есть цену актива в USD надо получать как 1/quote с обменом max/min.

ASSETS = {
    "eur":  {"kind": "fiat",   "table": "sasha_rates_eur_usd",  "invert": False},
    "gbp":  {"kind": "fiat",   "table": "sasha_rates_gbp_usd",  "invert": False},
    "aud":  {"kind": "fiat",   "table": "sasha_rates_aud_usd",  "invert": False},
    "chf":  {"kind": "fiat",   "table": "sasha_rates_usd_chf",  "invert": True},
    "cny":  {"kind": "fiat",   "table": "sasha_rates_usd_cny",  "invert": True},
    "jpy":  {"kind": "fiat",   "table": "sasha_rates_usd_jpy",  "invert": True},
    "rub":  {"kind": "fiat",   "table": "sasha_rates_usd_rub",  "invert": True},
    "inr":  {"kind": "fiat",   "table": "sasha_rates_usd_inr",  "invert": True},
    "aed":  {"kind": "fiat",   "table": "sasha_rates_usd_aed",  "invert": True},
    "btc":  {"kind": "crypto", "table": "sasha_rates_btc_usd",  "invert": False},
    "eth":  {"kind": "crypto", "table": "sasha_rates_eth_usd",  "invert": False},
    "bch":  {"kind": "crypto", "table": "sasha_rates_bch_usd",  "invert": False},
    "sol":  {"kind": "crypto", "table": "sasha_rates_sol_usd",  "invert": False},
    "xrp":  {"kind": "crypto", "table": "sasha_rates_xrp_usd",  "invert": False},
    "dash": {"kind": "crypto", "table": "sasha_rates_dash_usd", "invert": False},
    "bnb":  {"kind": "crypto", "table": "sasha_rates_bnb_usd",  "invert": False},
}

# Порядок задаёт, кто становится базой кросса: EUR раньше BTC → EUR/BTC.
PRECEDENCE = ["eur", "gbp", "aud", "chf", "cny", "jpy", "rub", "inr", "aed",
              "btc", "eth", "bch", "sol", "xrp", "dash", "bnb"]


def _cross_table(base: str, quote: str, day: bool) -> str:
    return f"sasha_rates_{base}_{quote}" + ("_day" if day else "")


def _build_cross_datasets() -> dict:
    datasets = {}
    for i, base in enumerate(PRECEDENCE):
        for quote in PRECEDENCE[i + 1:]:
            kinds = {ASSETS[base]["kind"], ASSETS[quote]["kind"]}
            group = "mixed" if len(kinds) == 2 else kinds.pop()
            for day in (False, True):
                datasets[_cross_table(base, quote, day)] = {
                    "kind": "cross",
                    "base": base,
                    "quote": quote,
                    "group": group,
                    "day": day,
                    "label": f"{base.upper()}/{quote.upper()}",
                    "description": (
                        f"{base.upper()}/{quote.upper()} "
                        f"{'daily' if day else 'hourly'} synthetic cross via USD"
                    ),
                }
    return datasets


CROSS_DATASETS = _build_cross_datasets()
DATASETS = {**REAL_DATASETS, **CROSS_DATASETS}


def send_error_trace(exc: Exception, script_name: str = "SashaRates.py"):
    import threading

    logs = (
        f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\n"
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


parser = argparse.ArgumentParser(description="Sasha rates parser → MySQL (real quotes, then crosses)")
parser.add_argument("table_name", help=f"Таблица sasha_rates_* или {ALL_TABLE}")
parser.add_argument("host", nargs="?", default=_env("DB_HOST", "MASTER_HOST"))
parser.add_argument("port", nargs="?", default=_env("DB_PORT", "MASTER_PORT", default="3306"))
parser.add_argument("user", nargs="?", default=_env("DB_USER", "MASTER_USER"))
parser.add_argument("password", nargs="?", default=_env("DB_PASSWORD", "MASTER_PASSWORD"))
parser.add_argument("database", nargs="?", default=_env("DB_NAME", "MASTER_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("Ошибка: не указаны параметры подключения к БД (DB_* или MASTER_* в .env)")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}

HTTP = requests.Session()
HTTP.trust_env = False
HTTP.headers.update({"User-Agent": YAHOO_UA, "Accept": "application/json"})
retry = Retry(
    total=3,
    backoff_factor=1.2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
HTTP.mount("https://", HTTPAdapter(max_retries=retry))

_LEG_CACHE = {}
_MISSING_LEGS = set()


def _sleep():
    if REQUEST_PAUSE > 0:
        time.sleep(REQUEST_PAUSE)


def _http_get(url, params=None, timeout=30):
    _sleep()
    response = HTTP.get(url, params=params, timeout=timeout)
    if response.status_code == 429:
        wait = int(response.headers.get("Retry-After", "8"))
        print(f"   rate-limit {url}, ждём {wait}s")
        time.sleep(wait)
        response = HTTP.get(url, params=params, timeout=timeout)
    return response


def ensure_table(table_name: str):
    comment = DATASETS[table_name]["description"].replace("'", "")
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    try:
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id         INT AUTO_INCREMENT PRIMARY KEY,
                date       DATETIME    NOT NULL,
                open       DOUBLE,
                close      DOUBLE,
                `max`      DOUBLE,
                `min`      DOUBLE,
                t1         DOUBLE,
                loaded_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_date (date),
                INDEX idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='{comment}'
            """
        )
        conn.commit()
    except mysql.connector.Error as exc:
        if exc.errno == 1142:
            print(f"  CREATE запрещён для `{table_name}`, пишем в уже существующую таблицу")
        else:
            raise
    finally:
        c.close()
        conn.close()


def get_latest_date(table_name: str):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        c = conn.cursor()
        c.execute(f"SELECT MAX(`date`) FROM `{table_name}`")
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def save_rows(table_name: str, rows: list, *, count_written: bool = False):
    if not rows:
        print("  Нет новых данных для записи")
        return
    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    sql = f"""
        INSERT INTO `{table_name}` (`date`, `open`, `close`, `max`, `min`, `t1`)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `open`=VALUES(`open`),
            `close`=VALUES(`close`),
            `max`=VALUES(`max`),
            `min`=VALUES(`min`),
            `t1`=VALUES(`t1`)
    """
    c.executemany(sql, rows)
    conn.commit()
    if count_written:
        print(f"  Записано {len(rows)} строк")
    else:
        print(f"  Записано/обновлено {c.rowcount} строк")
    c.close()
    conn.close()


def _bar(dt, open_, high, low, close):
    try:
        o = float(open_)
        h = float(high)
        l = float(low)
        c = float(close)
    except (TypeError, ValueError):
        return None
    if min(o, h, l, c) <= 0:
        return None
    # Дневные бары Yahoo местами противоречивы: close выходит за max/min.
    # Расширяем границы до тела свечи, иначе поиск фигур получит невозможный бар.
    h = max(h, o, c)
    l = min(l, o, c)
    return (dt, o, c, h, l)


def _with_t1(bars: list) -> list:
    """Добавляет t1 = close следующего бара минус close текущего.

    Так устроены эталонные brain_rates_*: t1 — outcome следующего интервала,
    а не тело текущей свечи. У последнего бара продолжения ещё нет, поэтому 0.0;
    оно будет перезаписано следующим запуском, окно которого перекрывает хвост.
    """
    result = []
    for i, (dt, o, c, h, l) in enumerate(bars):
        next_close = bars[i + 1][2] if i + 1 < len(bars) else None
        result.append((dt, o, c, h, l, (next_close - c) if next_close is not None else 0.0))
    return result


# ── Источники фиата ──────────────────────────────────────────────────────────

def _yahoo_daily_bars(timestamps, opens, highs, lows, closes, gmt_offset, cutoff) -> list:
    """Собирает дневные FX-бары из снимков Yahoo.

    В дневных FX-барах Yahoo настоящие только max/min: open и close — это один
    и тот же снимок цены на момент метки (тело свечи выходило 0.02-0.07 от
    диапазона против 0.5 у здоровых серий). Сама метка равна началу торговой
    сессии в таймзоне биржи, для FX это 23:00 UTC предыдущих суток — что
    подтверждается совпадением max/min именно с этой сессией.

    Отсюда реконструкция: open сессии — снимок её собственной метки, close —
    снимок метки следующей сессии. У текущей незакрытой сессии close берётся из
    её же поля, там лежит последний тик.
    """
    limit = min(len(opens), len(highs), len(lows), len(closes))
    sessions: dict = {}
    for i, ts in enumerate(timestamps):
        if i >= limit or opens[i] is None:
            continue
        dt = datetime.utcfromtimestamp(int(ts) + gmt_offset).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        item = sessions.get(dt)
        if item is None:
            sessions[dt] = {
                "open": opens[i], "close": closes[i],
                "high": highs[i], "low": lows[i],
            }
            continue
        # Текущую сессию Yahoo отдаёт двумя строками: метка начала сессии и
        # метка последнего тика. Склеиваем их в один бар.
        if closes[i] is not None:
            item["close"] = closes[i]
        if highs[i] is not None:
            item["high"] = highs[i] if item["high"] is None else max(item["high"], highs[i])
        if lows[i] is not None:
            item["low"] = lows[i] if item["low"] is None else min(item["low"], lows[i])

    order = sorted(sessions)
    rows = []
    for idx, dt in enumerate(order):
        if cutoff and dt < cutoff:
            continue
        item = sessions[dt]
        close = sessions[order[idx + 1]]["open"] if idx + 1 < len(order) else item["close"]
        bar = _bar(dt, item["open"], item["high"], item["low"], close)
        if bar:
            rows.append(bar)
    return rows


def fetch_yahoo(symbol: str, day: bool, latest) -> list:
    interval = "1d" if day else "1h"
    params = {"interval": interval, "includePrePost": "false"}
    if latest is None:
        if day:
            # range=max для 1d молча деградирует до месячных свечей, period1/period2 — нет
            params["period1"] = 0
            params["period2"] = int(time.time())
        else:
            params["range"] = "2y"  # часовые Yahoo не отдаёт глубже 730 дней
    else:
        params["range"] = "3mo" if day else "10d"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    response = _http_get(url, params=params)
    if response.status_code != 200:
        print(f"   Yahoo HTTP {response.status_code} для {symbol}")
        return []
    try:
        result = (response.json().get("chart") or {}).get("result") or []
    except ValueError:
        print(f"   Yahoo: не JSON для {symbol}")
        return []
    if not result:
        print(f"   Yahoo: пустой result для {symbol}")
        return []
    payload = result[0]
    timestamps = payload.get("timestamp") or []
    # Дневные бары Yahoo помечает началом торговой сессии в таймзоне биржи
    # (для FX это Europe/London, то есть 23:00 UTC предыдущих суток). Без
    # поправки вся дневная история фиата сдвинута на день назад относительно
    # крипты, которая живёт по UTC.
    gmt_offset = int((payload.get("meta") or {}).get("gmtoffset") or 0)
    quote = ((payload.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    cutoff = None
    if latest:
        cutoff = latest - timedelta(days=3 if day else 1)

    if day:
        rows = _yahoo_daily_bars(timestamps, opens, highs, lows, closes, gmt_offset, cutoff)
    else:
        rows = []
        for i, ts in enumerate(timestamps):
            # текущий незакрытый бар Yahoo помечает временем последнего тика:
            # без округления каждый запуск добавлял бы лишнюю строку внутри часа
            dt = datetime.utcfromtimestamp(int(ts)).replace(minute=0, second=0, microsecond=0)
            if cutoff and dt < cutoff:
                continue
            if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes):
                continue
            bar = _bar(dt, opens[i], highs[i], lows[i], closes[i])
            if bar:
                rows.append(bar)
    print(f"   Yahoo {symbol} {interval}: {len(rows)} баров")
    return rows


def fetch_moex(secid: str, day: bool, latest) -> list:
    interval = 24 if day else 60
    rows = []
    start = 0
    cutoff = None
    if latest:
        cutoff = latest - timedelta(days=7 if day else 2)
    while True:
        url = (
            "https://iss.moex.com/iss/engines/currency/markets/selt/"
            f"boards/CETS/securities/{secid}/candles.json"
        )
        params = {"interval": interval, "start": start}
        if cutoff:
            params["from"] = cutoff.strftime("%Y-%m-%d")
        else:
            params["from"] = "2014-01-01"
        response = _http_get(url, params=params)
        if response.status_code != 200:
            print(f"   MOEX HTTP {response.status_code} для {secid}")
            break
        try:
            payload = response.json().get("candles") or {}
        except ValueError:
            print(f"   MOEX: не JSON для {secid}")
            break
        columns = payload.get("columns") or []
        data = payload.get("data") or []
        if not data:
            break
        idx = {name: i for i, name in enumerate(columns)}
        for item in data:
            begin = item[idx.get("begin", 6)]
            try:
                dt = datetime.strptime(str(begin)[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if day:
                dt = dt.replace(hour=0, minute=0, second=0)
            if cutoff and dt < cutoff:
                continue
            bar = _bar(
                dt,
                item[idx.get("open", 0)],
                item[idx.get("high", 2)],
                item[idx.get("low", 3)],
                item[idx.get("close", 1)],
            )
            if bar:
                rows.append(bar)
        start += len(data)
        if cutoff and len(data) < 50:
            break
        if len(data) < 100:
            break
    print(f"   MOEX {secid} interval={interval}: {len(rows)} баров")
    return rows


def _fetch_fx_source(source: str, config: dict, since) -> list:
    try:
        if source == "moex":
            if not config.get("moex"):
                return []
            return fetch_moex(config["moex"], config["day"], since)
        return fetch_yahoo(config["yahoo"], config["day"], since)
    except Exception as exc:
        print(f"   источник {source} упал: {exc!r}")
        return []


# ── Источники крипты ─────────────────────────────────────────────────────────

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_coinbase(product: str, day: bool, latest) -> list:
    granularity = 86400 if day else 3600
    period = timedelta(seconds=granularity)
    now = datetime.utcnow().replace(microsecond=0)
    if latest:
        start = latest - (3 * period)
    else:
        start = now - timedelta(days=3650 if day else 730)
    rows = []
    cursor = start
    max_batch = timedelta(seconds=granularity * 280)
    while cursor < now:
        end = min(cursor + max_batch, now)
        url = f"{COINBASE_BASE}/products/{product}/candles"
        response = _http_get(
            url,
            params={
                "granularity": granularity,
                "start": _iso(cursor),
                "end": _iso(end),
            },
        )
        if response.status_code != 200:
            print(f"   Coinbase HTTP {response.status_code} для {product}")
            break
        try:
            batch = response.json()
        except ValueError:
            print(f"   Coinbase: не JSON для {product}")
            break
        if not isinstance(batch, list) or not batch:
            cursor = end + period
            continue
        for item in batch:
            # [time, low, high, open, close, volume]
            dt = datetime.utcfromtimestamp(int(item[0])).replace(microsecond=0)
            if day:
                dt = dt.replace(hour=0, minute=0, second=0)
            bar = _bar(dt, item[3], item[2], item[1], item[4])
            if bar:
                rows.append(bar)
        cursor = end + period
    print(f"   Coinbase {product} gran={granularity}: {len(rows)} баров")
    return rows


def fetch_binance(symbol: str, day: bool, latest) -> list:
    interval = "1d" if day else "1h"
    period_ms = (86400 if day else 3600) * 1000
    start_ms = 0
    if latest:
        start_ms = int((latest - timedelta(days=3 if day else 1)).timestamp() * 1000)
    rows = []
    while True:
        params = {"symbol": symbol, "interval": interval, "limit": 1000}
        if start_ms:
            params["startTime"] = start_ms
        response = _http_get(f"{BINANCE_BASE}/api/v3/klines", params=params)
        if response.status_code != 200:
            print(f"   Binance HTTP {response.status_code} для {symbol}")
            break
        try:
            batch = response.json()
        except ValueError:
            print(f"   Binance: не JSON для {symbol}")
            break
        if not isinstance(batch, list) or not batch:
            break
        for item in batch:
            dt = datetime.utcfromtimestamp(int(item[0]) / 1000).replace(microsecond=0)
            if day:
                dt = dt.replace(hour=0, minute=0, second=0)
            bar = _bar(dt, item[1], item[2], item[3], item[4])
            if bar:
                rows.append(bar)
        last_open = int(batch[-1][0])
        if len(batch) < 1000:
            break
        start_ms = last_open + period_ms
        if latest is None and len(rows) > 20000:
            break
    print(f"   Binance {symbol} {interval}: {len(rows)} баров")
    return rows


# ── Обработка таблиц ─────────────────────────────────────────────────────────

def process_fx(table_name: str):
    config = FX_DATASETS[table_name]
    ensure_table(table_name)
    latest = get_latest_date(table_name)
    since = None if FULL_RELOAD else latest
    print(f"  {config['label']}  last={latest or 'пусто'}  tf={'day' if config['day'] else 'hour'}"
          f"{'  full-reload' if FULL_RELOAD else ''}")

    prefer = config.get("prefer") or "yahoo"
    sources = ["moex", "yahoo"] if prefer == "moex" else ["yahoo", "moex"]

    uniq = {}
    if config.get("merge"):
        # Идём от запасного источника к приоритетному: на общих датах
        # приоритетный перезапишет запасной, а тот закроет пробелы.
        for source in reversed(sources):
            for row in _fetch_fx_source(source, config, since):
                uniq[row[0]] = row
    else:
        for source in sources:
            rows = _fetch_fx_source(source, config, since)
            if rows:
                for row in rows:
                    uniq[row[0]] = row
                break

    if not uniq:
        print("  Данных нет")
        return
    save_rows(table_name, _with_t1([uniq[k] for k in sorted(uniq)]))


def process_crypto(table_name: str):
    config = CRYPTO_DATASETS[table_name]
    ensure_table(table_name)
    latest = get_latest_date(table_name)
    since = None if FULL_RELOAD else latest
    print(f"  {config['label']}  last={latest or 'пусто'}  tf={'day' if config['day'] else 'hour'}"
          f"{'  full-reload' if FULL_RELOAD else ''}")

    rows = []
    try:
        rows = fetch_coinbase(config["coinbase"], config["day"], since)
    except Exception as exc:
        print(f"   Coinbase упал: {exc!r}")
        rows = []
    source = "coinbase"
    if not rows:
        print("   fallback → Binance USDT")
        try:
            rows = fetch_binance(config["binance"], config["day"], since)
            source = "binance"
        except Exception as exc:
            print(f"   Binance упал: {exc!r}")
            rows = []
    if not rows:
        print("  Данных нет")
        return
    uniq = {}
    for row in rows:
        uniq[row[0]] = row
    print(f"   источник: {source}")
    save_rows(table_name, _with_t1([uniq[k] for k in sorted(uniq)]))


def load_leg(code: str, day: bool, cutoff):
    """Цена одного актива в USD по датам: {date: (open, close, max, min)}."""
    key = (code, day, cutoff)
    if key in _LEG_CACHE:
        return _LEG_CACHE[key]

    spec = ASSETS[code]
    table = spec["table"] + ("_day" if day else "")
    query = f"SELECT `date`, `open`, `close`, `max`, `min` FROM `{table}`"
    params = ()
    if cutoff is not None:
        query += " WHERE `date` >= %s"
        params = (cutoff,)
    query += " ORDER BY `date`"

    conn = mysql.connector.connect(**DB_CONFIG)
    c = conn.cursor()
    try:
        c.execute(query, params)
        raw = c.fetchall()
    except mysql.connector.Error as exc:
        if exc.errno == 1146:
            if table not in _MISSING_LEGS:
                print(f"   нет таблицы-источника `{table}`")
                _MISSING_LEGS.add(table)
            raw = []
        else:
            raise
    finally:
        c.close()
        conn.close()

    invert = spec["invert"]
    bars = {}
    for dt, o, c_, mx, mn in raw:
        if None in (o, c_, mx, mn):
            continue
        o, c_, mx, mn = float(o), float(c_), float(mx), float(mn)
        if min(o, c_, mx, mn) <= 0:
            continue
        if invert:
            # 1/x меняет порядок: максимум котировки даёт минимум цены
            o, c_, mx, mn = 1.0 / o, 1.0 / c_, 1.0 / mn, 1.0 / mx
        bars[dt] = (o, c_, mx, mn)

    _LEG_CACHE[key] = bars
    return bars


def build_cross(base_bars: dict, quote_bars: dict) -> list:
    """A/B по совпадающим меткам времени.

    max/min кросса точно не выводятся из экстремумов ног: неизвестно, в какой
    момент внутри бара они достигнуты. Берём внешнюю границу отношения —
    max(A)/min(B) и min(A)/max(B); она гарантированно содержит open и close.
    """
    dates = base_bars.keys() & quote_bars.keys()
    bars = []
    for dt in sorted(dates):
        a_open, a_close, a_max, a_min = base_bars[dt]
        b_open, b_close, b_max, b_min = quote_bars[dt]
        open_ = a_open / b_open
        close = a_close / b_close
        high = max(a_max / b_min, open_, close)
        low = min(a_min / b_max, open_, close)
        bars.append((dt, open_, close, high, low))
    return bars


def process_cross(table_name: str):
    config = CROSS_DATASETS[table_name]
    base, quote, day = config["base"], config["quote"], config["day"]

    latest = get_latest_date(table_name)
    cutoff = None
    if latest is not None and not FULL_RELOAD:
        step = timedelta(days=1) if day else timedelta(hours=1)
        cutoff = latest - OVERLAP_BARS * step
    print(f"  {config['label']}  last={latest or 'пусто'}  tf={'day' if day else 'hour'}"
          f"{'  full-reload' if FULL_RELOAD else ''}")

    base_bars = load_leg(base, day, cutoff)
    quote_bars = load_leg(quote, day, cutoff)
    if not base_bars or not quote_bars:
        print(f"  Нет данных по ногам: {base.upper()}={len(base_bars)} {quote.upper()}={len(quote_bars)}")
        return

    bars = build_cross(base_bars, quote_bars)
    if not bars:
        print("  Совпадающих меток времени нет")
        return
    print(f"   ноги: {len(base_bars)} × {len(quote_bars)} → пересечение {len(bars)} баров")

    ensure_table(table_name)
    save_rows(table_name, _with_t1(bars), count_written=True)


def process(table_name: str):
    kind = DATASETS[table_name]["kind"]
    if kind == "fx":
        process_fx(table_name)
    elif kind == "crypto":
        process_crypto(table_name)
    else:
        process_cross(table_name)


def _jobs(argument: str):
    """Список таблиц в порядке: фиат → крипта → кроссы."""
    if argument in ALIAS_ALL:
        return list(FX_DATASETS) + list(CRYPTO_DATASETS) + list(CROSS_DATASETS)
    if argument == ALIAS_FX:
        return list(FX_DATASETS)
    if argument == ALIAS_CRYPTO:
        return list(CRYPTO_DATASETS)
    if argument == ALIAS_CROSS:
        return list(CROSS_DATASETS)
    if argument in GROUP_TABLES:
        group = GROUP_TABLES[argument]
        return [name for name, cfg in CROSS_DATASETS.items() if cfg["group"] == group]
    if argument in DATASETS:
        return [argument]
    return None


def _print_help(unknown: str):
    print(f"Неизвестная таблица '{unknown}'. Допустимые:")
    print(f"  - {ALL_TABLE} / sasha_quotes → все реальные котировки, затем все кроссы")
    print(f"  - {ALIAS_FX} → фиат 0,1,4–10 (hour+day)")
    print(f"  - {ALIAS_CRYPTO} → крипта 2,3,11–15 (hour+day)")
    print(f"  - {ALIAS_CROSS} → все кроссы ({len(CROSS_DATASETS) // 2} пар, hour+day)")
    for group_table, group in GROUP_TABLES.items():
        count = sum(1 for cfg in CROSS_DATASETS.values() if cfg["group"] == group and not cfg["day"])
        print(f"  - {group_table} → {group}, {count} пар (hour+day)")
    print("  Отдельная таблица: sasha_rates_<pair> или sasha_rates_<pair>_day")


def _phase_title(table_name: str) -> str | None:
    kind = DATASETS[table_name]["kind"]
    if kind == "fx":
        return "Реальные котировки (фиат)"
    if kind == "crypto":
        return "Реальные котировки (крипта)"
    return "Кроссы"


def main():
    names = _jobs(args.table_name)
    if names is None:
        _print_help(args.table_name)
        sys.exit(1)

    print("Sasha Rates Parser")
    print(f"  База: {args.host}:{args.port}/{args.database}")
    print(f"  Таблиц: {len(names)}")
    print("=" * 70)
    current_phase = None
    for name in names:
        phase = _phase_title(name)
        if phase != current_phase:
            current_phase = phase
            print(f"\n=== {phase} ===")
        print(f"\n→ {name}")
        process(name)
    print("=" * 70)
    print("ГОТОВО")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nКритическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
