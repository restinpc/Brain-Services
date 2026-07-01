"""
model.py — brain-extremum-graph (сервис 54)

Алгоритм:
  1. Ждём формирования нового экстремума (с мин. интервалом = avg_candles)
  2. Округляем уровень до 3 значащих цифр → ключ в графе
  3. Идём по графу вероятностей var шагов (var=0..4 → steps=2,4,6,8,10)
     Всегда переходим в наиболее вероятный следующий уровень
  4. Проверяем: шаг 1 и шаг var совпадают по направлению?
     → LONG если оба UP, SHORT если оба DOWN
  5. Возвращаем {"signal": bull_ratio}
     bull_ratio > 0.5 → long сигнал
     bull_ratio < 0.5 → short сигнал
     {}              → нет сигнала

type:
  type=0 → закрытие через avg_candles баров (bull_ratio_t0)
  type=1 → закрытие при формировании нового экстремума (bull_ratio_t1)

var (чётные шаги: 2,4,6,8,10):
  var=0 → 2 шага,   var=1 → 4 шага
  var=2 → 6 шагов,  var=3 → 8 шагов
  var=4 → 10 шагов

Единственный output key: "signal" — нейросеть использует один вход,
bull_ratio кодирует и направление и уверенность.
"""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime
from typing import Optional

import numpy as np
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("brain-framework")

# ── Константы ─────────────────────────────────────────────────────────────────
SERVICE_ID  = 55
RATES_TABLE = "brain_rates_eur_usd"     # базовый (для фреймворка)

OUTPUT_KEY  = "signal"                  # фиксированный output ключ

VAR_STEPS: dict[int, int] = {           # var → кол-во шагов по графу
    0: 2, 1: 4, 2: 6, 3: 8, 4: 10,
}

SIG_DIGITS = 3   # значащих цифры при округлении уровней

# Таблицы в БД
GRAPH_TABLE = f"vlad_extremum_graph_svc{SERVICE_ID}"
STATS_TABLE = f"vlad_extremum_stats_svc{SERVICE_ID}"
INDEX_TABLE = f"vlad_extremum_lvl_svc{SERVICE_ID}_index"

# Кеши в памяти (сбрасываются при рестарте сервиса / каждый reload_interval)
_GRAPH_CACHE: dict[int, dict] = {}        # pair_id → {level: [(to, prob, dir),...]}
_STATS_CACHE: dict[int, dict] = {}        # pair_id → {avg_candles: int, ...}
_INDEX_CACHE: dict[tuple, dict] = {}      # (pair_id, var, type) → {bull_ratio}
_LAST_SIGNAL_TS: dict[int, int] = {}      # pair_id → unix timestamp последнего сигнала

# Ставка за соседних конкурентов (RATES_TABLES для context_idx.py)
RATES_TABLES: dict[int, str] = {
    1: "brain_rates_eur_usd",
    3: "brain_rates_btc_usd",
    4: "brain_rates_eth_usd",
}


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def round_to_level(price: float, sig: int = SIG_DIGITS) -> int:
    """
    Округляет цену до 3 значащих цифр → целочисленный ключ узла графа.
    BTC 93211 → 932, EUR 1.20165 → 120, ETH 3521 → 352.
    """
    if price <= 0:
        return 0
    magnitude = 10 ** (math.floor(math.log10(price)) - sig + 1)
    return int(price / magnitude)


def _detect_pair_id(rates: list) -> int:
    """Определяет пару по last_close."""
    if not rates:
        return 1
    c = float(rates[-1].get("close") or rates[-1].get("t1") or 0)
    if c > 10_000:
        return 3   # BTC
    if c > 100:
        return 4   # ETH
    return 1       # EUR


def _get_db_cfg(db_name: str = None) -> dict:
    db = db_name or os.getenv("DB_NAME", "vlad")
    return {
        "host":     os.getenv("DB_HOST",     "127.0.0.1"),
        "port":     int(os.getenv("DB_PORT", "3306")),
        "user":     os.getenv("DB_USER",     "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": db,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка графа, статистики, индекса из БД
# ══════════════════════════════════════════════════════════════════════════════

def _load_graph(pair_id: int) -> dict:
    """
    Загружает граф переходов из vlad_extremum_graph_svc54.
    Возвращает {from_level: [(to_level, probability, direction), ...]} — отсортировано по prob desc.
    """
    if pair_id in _GRAPH_CACHE:
        return _GRAPH_CACHE[pair_id]

    import mysql.connector
    graph: dict[int, list] = {}
    try:
        conn = mysql.connector.connect(**_get_db_cfg())
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT from_level, to_level, probability, direction "
            f"FROM `{GRAPH_TABLE}` WHERE pair = %s ORDER BY from_level, probability DESC",
            (pair_id,)
        )
        for r in cur.fetchall():
            f = int(r["from_level"])
            if f not in graph:
                graph[f] = []
            graph[f].append((int(r["to_level"]), float(r["probability"]), int(r["direction"])))
        cur.close(); conn.close()
        _GRAPH_CACHE[pair_id] = graph
        log.debug(f"[graph] pair={pair_id} loaded {len(graph)} nodes")
    except Exception as e:
        log.warning(f"[graph] _load_graph pair={pair_id}: {e}")
    return graph


def _load_stats(pair_id: int) -> dict:
    """Загружает статистику (avg_candles и др.) из vlad_extremum_stats_svc54."""
    if pair_id in _STATS_CACHE:
        return _STATS_CACHE[pair_id]

    import mysql.connector
    stats = {"avg_candles": 50, "min_candles": 10}
    try:
        conn = mysql.connector.connect(**_get_db_cfg())
        cur  = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM `{STATS_TABLE}` WHERE pair = %s", (pair_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            stats = {k: v for k, v in row.items()}
    except Exception as e:
        log.warning(f"[graph] _load_stats pair={pair_id}: {e}")
    _STATS_CACHE[pair_id] = stats
    return stats


def _load_bull_ratio(pair_id: int, var: int, type_id: int) -> float:
    """Загружает bull_ratio из индексной таблицы для (pair, var, type)."""
    cache_key = (pair_id, var, type_id)
    if cache_key in _INDEX_CACHE:
        return _INDEX_CACHE[cache_key]

    import mysql.connector
    br = 0.5
    col = "bull_ratio_t0" if type_id == 0 else "bull_ratio_t1"
    try:
        conn = mysql.connector.connect(**_get_db_cfg())
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT `{col}` FROM `{INDEX_TABLE}` "
            f"WHERE pair = %s AND var = %s LIMIT 1",
            (pair_id, var)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row[col] is not None:
            br = float(row[col])
    except Exception as e:
        log.warning(f"[graph] _load_bull_ratio: {e}")
    _INDEX_CACHE[cache_key] = br
    return br


# ══════════════════════════════════════════════════════════════════════════════
# Ядро алгоритма
# ══════════════════════════════════════════════════════════════════════════════

def _detect_new_extremum(
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
    avg_candles: int,
) -> Optional[dict]:
    """
    Проверяет: сформировался ли новый экстремум на последнем баре?
    Окно поиска = min(avg_candles // 2, 30) баров.
    Возвращает {'level': int, 'direction': int(+1=MAX/-1=MIN)} или None.
    """
    n   = len(closes)
    win = max(5, min(avg_candles // 2, 30))

    if n < win + 2:
        return None

    last_h = float(highs[-1])
    last_l = float(lows[-1])

    is_max = last_h >= float(np.max(highs[-win - 1:-1]))
    is_min = last_l <= float(np.min(lows[-win - 1:-1]))

    if not is_max and not is_min:
        return None

    price = last_h if is_max else last_l
    return {
        "level":     round_to_level(price),
        "direction": 1 if is_max else -1,
        "price":     price,
    }


def _walk_graph(
    graph: dict, start_level: int, steps: int,
) -> Optional[list]:
    """
    Идёт по графу from start_level за steps шагов, выбирая наиболее вероятный переход.
    Возвращает [{'from', 'to', 'dir', 'prob'}, ...] или None если путь обрывается.
    """
    path    = []
    current = start_level

    for _ in range(steps):
        nexts = graph.get(current)
        if not nexts:
            return None   # тупик — узел не встречался в истории
        best = nexts[0]   # уже отсортировано по prob desc
        path.append({
            "from": current,
            "to":   best[0],
            "dir":  best[2],
            "prob": best[1],
        })
        current = best[0]

    return path


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа фреймворка
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],
    date:          datetime,
    *,
    type:          int  = 0,
    var:           int  = 2,     # default: 6 шагов
    param:         str  = "",
    dataset_index: dict | None = None,
    **kw,
) -> dict[str, float]:
    """
    Возвращает {"signal": bull_ratio} при срабатывании сигнала.
    Пустой {} = нет сигнала на этом баре.

    bull_ratio > 0.5 → long сигнал с вероятностью bull_ratio
    bull_ratio < 0.5 → short сигнал с вероятностью (1 - bull_ratio)
    """
    if not rates:
        return {}

    # ── Определяем пару ────────────────────────────────────────────────────
    pair_id    = _detect_pair_id(rates)
    steps      = VAR_STEPS.get(var, 6)
    current_ts = int(rates[-1]["date"].timestamp()) \
                 if hasattr(rates[-1]["date"], "timestamp") \
                 else int(rates[-1]["date"])

    # ── Загружаем статистику пары ──────────────────────────────────────────
    stats       = _load_stats(pair_id)
    avg_candles = int(stats.get("avg_candles") or 50)

    # ── Мин. интервал между сигналами (avg_candles баров) ────────────────
    # Проверяем только если elapsed >= 0 (даты строго после последнего сигнала).
    # Pretest передаёт даты в произвольном порядке — отрицательный elapsed
    # означает что тест-дата раньше последнего сигнала, блокировать не надо.
    if pair_id in _LAST_SIGNAL_TS:
        elapsed = (current_ts - _LAST_SIGNAL_TS[pair_id]) // 3600
        if 0 <= elapsed < avg_candles:
            return {}

    # ── numpy-массивы (только хвост нужного размера) ──────────────────────
    tail = max(avg_candles * 2, 100)
    r    = rates[-tail:]
    closes = np.array([float(x.get("close") or 0) for x in r], dtype=np.float64)
    highs  = np.array([float(x.get("max")   or 0) for x in r], dtype=np.float64)
    lows   = np.array([float(x.get("min")   or 0) for x in r], dtype=np.float64)

    # ── Детектируем новый экстремум ────────────────────────────────────────
    ext = _detect_new_extremum(closes, highs, lows, avg_candles)
    if ext is None:
        return {}

    # ── Загружаем граф ─────────────────────────────────────────────────────
    graph = _load_graph(pair_id)
    if not graph:
        return {}

    # ── Идём по графу ─────────────────────────────────────────────────────
    path = _walk_graph(graph, ext["level"], steps)
    if not path:
        return {}

    # ── Проверяем условие сигнала: шаг 1 == шаг var по направлению ────────
    step1_dir = path[0]["dir"]
    stepN_dir = path[-1]["dir"]
    if step1_dir != stepN_dir:
        return {}

    signal_dir = step1_dir   # +1 = LONG, -1 = SHORT

    # ── Получаем bull_ratio из индекса ─────────────────────────────────────
    bull_ratio = _load_bull_ratio(pair_id, var, type)

    # Кодируем направление: long = bull_ratio, short = 1 - bull_ratio
    if signal_dir < 0:
        bull_ratio = 1.0 - bull_ratio

    _LAST_SIGNAL_TS[pair_id] = current_ts

    log.info(f"[graph] pair={pair_id} var={var}({steps}steps) "
             f"level={ext['level']} dir={'LONG' if signal_dir > 0 else 'SHORT'} "
             f"bull_ratio={bull_ratio:.4f}")

    return {OUTPUT_KEY: round(bull_ratio, 6)}