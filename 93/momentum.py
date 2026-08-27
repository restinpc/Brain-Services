"""
Слой данных модели 91: momentum последней свечки по всем инструментам.

Вселенная — 17 активов: 16 из исходной нумерации ТЗ плюс доллар, у которого
своей «ноги» нет, потому что он присутствует во всех базовых таблицах.
Каждая неупорядоченная пара активов — это одна таблица sasha_rates_*:
16 базовых (X к USD) и 120 кроссов, итого 136 пар в двух таймфреймах.

Таблицы не грузятся целиком: /values всегда работает с одной целевой парой,
а это лишь 30 инструментов из 136. Поэтому серии подтягиваются лениво по
запросу и кешируются в numpy-массивах.
"""

from __future__ import annotations

import calendar
import math
import os
import sys
import threading
import time
from datetime import datetime

import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ── Вселенная активов ────────────────────────────────────────────────────────
# id 0..15 — нумерация из ТЗ (те же pair_id, что в parsers/SashaRates.py).
# id 16 — доллар: в ТЗ он неявный, но для маски <id1>_<id2> ему нужен номер.
USD_ID = 16

ASSET_CODES = {
    0: "rub", 1: "eur", 2: "btc", 3: "eth", 4: "gbp", 5: "cny", 6: "jpy",
    7: "aed", 8: "inr", 9: "aud", 10: "chf", 11: "bch", 12: "sol",
    13: "xrp", 14: "dash", 15: "bnb", USD_ID: "usd",
}
ASSET_IDS = {code: asset_id for asset_id, code in ASSET_CODES.items()}

# Целевые инструменты. Ключ — pair из /values (нумерация фреймворка:
# 1=EUR/USD, 3=BTC/USD, 4=ETH/USD), значение — пара id активов.
TARGET_PAIRS = {
    1: (ASSET_IDS["eur"], USD_ID),
    3: (ASSET_IDS["btc"], USD_ID),
    4: (ASSET_IDS["eth"], USD_ID),
}
DEFAULT_TARGET = 1

_DEFAULT_PREFIX = "sasha_rates_"

# ── Состояние ────────────────────────────────────────────────────────────────
_LOCK = threading.RLock()
_ORIENTATION: dict[tuple[int, int], tuple[str, int, int]] = {}
_PAIRS_BY_ASSET: dict[int, list[tuple[int, int]]] = {}
_SERIES: dict[tuple[tuple[int, int], bool], tuple[np.ndarray, np.ndarray]] = {}
_DISCOVERED_AT = 0.0
_HISTORY_FROM: datetime | None = None


def configure(history_from: datetime | None) -> None:
    """Ограничивает глубину подгружаемых серий (обычно cache.date_from минус запас)."""
    global _HISTORY_FROM
    with _LOCK:
        _HISTORY_FROM = history_from


_ENGINE = None


def _env(*names, default=None):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _model_section() -> dict:
    try:
        from brain_framework import get_service_config
        return dict((get_service_config() or {}).get("model") or {})
    except Exception:
        return {}


def table_prefix() -> str:
    raw = (
        os.getenv("SASHA_RATES_PREFIX")
        or _model_section().get("quotes_prefix")
        or _DEFAULT_PREFIX
    )
    raw = str(raw).strip() or _DEFAULT_PREFIX
    return raw if raw.endswith("_") else raw + "_"


def _quotes_creds() -> dict:
    """Креды только для sasha_rates_*. brain_rates_* модель не читает.

    Приоритет: SASHA_DB_* (явная база котировок) → [model].quotes_engine
    (vlad=DB_*, brain=MASTER_*) → DB_*. На MASTER без SASHA_DB_* не падаем:
    там лежат чужие brain_rates_eur/btc/eth.
    """
    if os.getenv("SASHA_DB_HOST") or os.getenv("SASHA_DB_NAME"):
        return {
            "user": _env("SASHA_DB_USER", "DB_USER", "MASTER_USER"),
            "password": _env("SASHA_DB_PASSWORD", "DB_PASSWORD", "MASTER_PASSWORD") or "",
            "host": _env("SASHA_DB_HOST", "DB_HOST", "MASTER_HOST", default="127.0.0.1"),
            "port": _env("SASHA_DB_PORT", "DB_PORT", "MASTER_PORT", default="3306"),
            "name": _env("SASHA_DB_NAME", "DB_NAME", "MASTER_NAME"),
        }
    which = str(
        os.getenv("SASHA_QUOTES_ENGINE") or _model_section().get("quotes_engine") or "vlad"
    ).lower()
    if which in ("brain", "master"):
        return {
            "user": _env("MASTER_USER"),
            "password": _env("MASTER_PASSWORD") or "",
            "host": _env("MASTER_HOST", default="127.0.0.1"),
            "port": _env("MASTER_PORT", default="3306"),
            "name": _env("MASTER_NAME", default="brain"),
        }
    return {
        "user": _env("DB_USER"),
        "password": _env("DB_PASSWORD") or "",
        "host": _env("DB_HOST", default="127.0.0.1"),
        "port": _env("DB_PORT", default="3306"),
        "name": _env("DB_NAME"),
    }


def engine():
    """Синхронный движок к БД котировок sasha_rates_*.

    model() фреймворк вызывает синхронно, поэтому async-движки из
    enrich_dataset здесь не подходят. Драйвер pymysql уже есть в окружении
    сервисов: от него зависит aiomysql, на котором работает сам фреймворк.
    """
    global _ENGINE
    with _LOCK:
        if _ENGINE is None:
            creds = _quotes_creds()
            url = (
                f"mysql+pymysql://{creds['user']}:"
                f"{quote_plus(creds['password'])}@"
                f"{creds['host']}:{creds['port']}/"
                f"{creds['name']}?charset=utf8mb4"
            )
            _ENGINE = create_engine(
                url, pool_pre_ping=True, pool_recycle=1800, pool_size=2, max_overflow=3
            )
        return _ENGINE


def to_epoch(value: datetime) -> int:
    """Метки котировок наивные и лежат в UTC, поэтому не local time.

    datetime.timestamp() приводил бы их по таймзоне сервиса и сдвигал возраст
    последнего бара на величину этой таймзоны.
    """
    return calendar.timegm(value.timetuple())


def pair_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a <= b else (b, a)


def mask(key: tuple[int, int]) -> str:
    return f"{key[0]}_{key[1]}"


# ── Обнаружение таблиц ───────────────────────────────────────────────────────

def discover(force: bool = False) -> dict:
    """Сопоставляет пары активов с реально существующими таблицами.

    Ориентация таблицы определяется по её имени: sasha_rates_eur_btc значит,
    что база — EUR, а котируемый — BTC. Это важно для знака momentum.
    """
    global _DISCOVERED_AT
    with _LOCK:
        if _ORIENTATION and not force:
            return {"pairs": len(_ORIENTATION), "cached": True}

        with engine().connect() as conn:
            prefix = table_prefix()
            rows = conn.execute(text(f"SHOW TABLES LIKE '{prefix}%'")).fetchall()
        existing = {row[0] for row in rows}

        orientation: dict[tuple[int, int], tuple[str, int, int]] = {}
        missing: list[str] = []
        ids = sorted(ASSET_CODES)
        for i, first in enumerate(ids):
            for second in ids[i + 1:]:
                code_a = ASSET_CODES[first]
                code_b = ASSET_CODES[second]
                direct = f"{prefix}{code_a}_{code_b}"
                inverse = f"{prefix}{code_b}_{code_a}"
                if direct in existing:
                    orientation[(first, second)] = (direct, first, second)
                elif inverse in existing:
                    orientation[(first, second)] = (inverse, second, first)
                else:
                    missing.append(f"{code_a}/{code_b}")

        by_asset: dict[int, list[tuple[int, int]]] = {a: [] for a in ids}
        for key in orientation:
            by_asset[key[0]].append(key)
            by_asset[key[1]].append(key)

        _ORIENTATION.clear()
        _ORIENTATION.update(orientation)
        _PAIRS_BY_ASSET.clear()
        _PAIRS_BY_ASSET.update({a: sorted(v) for a, v in by_asset.items()})
        _SERIES.clear()
        _DISCOVERED_AT = time.time()

        return {
            "pairs": len(orientation),
            "missing": len(missing),
            "missing_preview": missing[:10],
            "cached": False,
        }


def pairs_with(asset_id: int) -> list[tuple[int, int]]:
    discover()
    return _PAIRS_BY_ASSET.get(asset_id, [])


def orientation(key: tuple[int, int]) -> tuple[str, int, int] | None:
    discover()
    return _ORIENTATION.get(key)


# ── Серии momentum ───────────────────────────────────────────────────────────

def _load_series(key: tuple[int, int], day: bool) -> tuple[np.ndarray, np.ndarray]:
    info = orientation(key)
    if info is None:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float64)

    table = info[0] + ("_day" if day else "")
    query = f"SELECT `date`, `open`, `close` FROM `{table}`"
    params: dict = {}
    if _HISTORY_FROM is not None:
        query += " WHERE `date` >= :history_from"
        params["history_from"] = _HISTORY_FROM
    query += " ORDER BY `date`"

    try:
        with engine().connect() as conn:
            rows = conn.execute(text(query), params).fetchall()
    except Exception:
        rows = []

    stamps = np.empty(len(rows), dtype=np.int64)
    values = np.empty(len(rows), dtype=np.float64)
    count = 0
    for date_value, open_value, close_value in rows:
        if not isinstance(date_value, datetime) or open_value is None or close_value is None:
            continue
        open_float = float(open_value)
        close_float = float(close_value)
        if open_float <= 0.0 or close_float <= 0.0:
            continue
        stamps[count] = to_epoch(date_value)
        # Логарифмическая доходность в базисных пунктах. Знак совпадает с
        # close-open, но величина сравнима между парами любого масштаба и
        # строго антисимметрична при инверсии пары — что и требуется для
        # знака второй ноги. Плюс она выживает округление кеша до 4 знаков,
        # в отличие от сырой разницы (у EUR/BTC это порядок 1e-8).
        values[count] = 10000.0 * math.log(close_float / open_float)
        count += 1

    return stamps[:count].copy(), values[:count].copy()


def series(key: tuple[int, int], day: bool) -> tuple[np.ndarray, np.ndarray]:
    cache_key = (key, bool(day))
    with _LOCK:
        cached = _SERIES.get(cache_key)
    if cached is not None:
        return cached

    loaded = _load_series(key, bool(day))
    with _LOCK:
        _SERIES[cache_key] = loaded
    return loaded


def invalidate_series() -> int:
    """Сбрасывает кеш серий — вызывается после подкачки новых котировок."""
    with _LOCK:
        dropped = len(_SERIES)
        _SERIES.clear()
    return dropped


def bar_momentum(key: tuple[int, int], day: bool, cutoff_ts: int) -> tuple[float, int] | None:
    """Momentum последней свечки с меткой <= cutoff_ts, в базисных пунктах."""
    stamps, values = series(key, day)
    if stamps.size == 0:
        return None
    index = int(np.searchsorted(stamps, cutoff_ts, side="right")) - 1
    if index < 0:
        return None
    return float(values[index]), int(stamps[index])


def asset_momentum(
    key: tuple[int, int], day: bool, cutoff_ts: int, asset_id: int
) -> tuple[float, int] | None:
    """Momentum конкретного актива внутри пары.

    Если актив стоит в знаменателе таблицы, знак инвертируется: рост EUR/BTC
    означает падение BTC относительно EUR.
    """
    info = orientation(key)
    if info is None or asset_id not in (info[1], info[2]):
        return None
    found = bar_momentum(key, day, cutoff_ts)
    if found is None:
        return None
    value, stamp = found
    return (value if asset_id == info[1] else -value), stamp


# ── Обновление котировок ─────────────────────────────────────────────────────

PARSER_JOBS = (
    ("SashaRates.py", "sasha_rates"),
)


def parsers_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", "parsers"))


def parser_python(override: str = "") -> str:
    """Интерпретатор для запуска парсеров.

    У парсеров свой набор зависимостей (mysql-connector), которого в окружении
    сервисов может не быть, поэтому предпочитаем .venv проекта, если он есть.
    """
    if override:
        return override
    root = os.path.dirname(parsers_dir())
    for relative in ((".venv", "Scripts", "python.exe"), (".venv", "bin", "python")):
        candidate = os.path.join(root, *relative)
        if os.path.exists(candidate):
            return candidate
    return sys.executable


def last_bar_age_minutes(day: bool = False) -> float | None:
    """Возраст самой свежей свечки среди базовых таблиц, в минутах."""
    discover()
    stamps: list[int] = []
    for asset_id in ASSET_CODES:
        if asset_id == USD_ID:
            continue
        key = pair_key(asset_id, USD_ID)
        found = bar_momentum(key, day, int(time.time()))
        if found is not None:
            stamps.append(found[1])
    if not stamps:
        return None
    return (time.time() - max(stamps)) / 60.0


def quotes_stale(day: bool, slack_minutes: float) -> bool:
    """Отстали ли котировки от текущего бара больше, чем на допуск.

    Метка бара — это его начало, поэтому у дневной свечки возраст доходит до
    суток даже при полностью свежих данных: допуск считается от длительности
    бара, иначе дневной таймфрейм выглядел бы просроченным всегда.
    """
    age = last_bar_age_minutes(day)
    if age is None:
        return False
    return age > (1440.0 if day else 60.0) + slack_minutes
