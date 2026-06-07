"""
context_idx.py — brain-extremum-graph v2 (сервис 59)

Строит три таблицы:

1. vlad_extremum_graph_svc59   — граф переходов между округлёнными уровнями
   (pair, from_level, to_level) → transition_count, probability, direction

2. vlad_extremum_stats_svc59  — статистика по паре
   pair → avg_candles (⌊среднее⌋), min_candles, total_extremums

3. vlad_extremum_lvl_svc59_index — индекс для Brain Framework
   Содержит weight_code="output" — единственный выходной ключ сервиса.
   bull_ratio вычисляется live в model() через walk_type0/walk_type1,
   индекс служит только для регистрации weight_code во фреймворке.

Алгоритм:
  Для каждой пары (EUR/BTC/ETH):
    1. Загружаем часовые котировки
    2. argrelextrema(order=5) → все экстремумы
    3. Фильтруем подряд идущие одного направления (оставляем выраженный)
    4. Округляем: 93211 → 932, 1.20165 → 120, 3521 → 352
    5. avg_candles = ⌊mean(gap между соседними экстремумами)⌋
    6. Строим матрицу переходов → нормируем в вероятности
    7. Записываем в vlad_extremum_graph_svc59
    8. Записываем stats
    9. Регистрируем weight_code="output" в индексе
"""
from __future__ import annotations

import logging
import math
import os

import numpy as np
from scipy.signal import argrelextrema
from sqlalchemy import text

log = logging.getLogger("brain-framework")

# ── Импорт констант из model.py ───────────────────────────────────────────────
import importlib.util as _ilu

def _import_model():
    here = os.path.dirname(os.path.abspath(__file__))
    spec = _ilu.spec_from_file_location("egr_model", os.path.join(here, "model.py"))
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_m = _import_model()

SERVICE_ID     = _m.SERVICE_ID       # 59
RATES_TABLES   = _m.RATES_TABLES
SIG_DIGITS     = _m.SIG_DIGITS
round_to_level = _m.round_to_level
OUTPUT_KEY     = _m.OUTPUT_KEY

GRAPH_TABLE = _m.GRAPH_TABLE   # vlad_extremum_graph_svc59
STATS_TABLE = _m.STATS_TABLE   # vlad_extremum_stats_svc59
INDEX_TABLE = _m.INDEX_TABLE   # vlad_extremum_lvl_svc59_index

# Параметры построения графа
GRAPH_ORDER     = 5     # порядок argrelextrema для нахождения уровней
MIN_TRANSITIONS = 2     # минимум переходов (a→b) для включения в граф
BATCH_SIZE      = 500


# ── DDL ───────────────────────────────────────────────────────────────────────

async def _create_tables(engine) -> None:
    async with engine.begin() as conn:

        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{GRAPH_TABLE}` (
                `id`               INT     NOT NULL AUTO_INCREMENT,
                `pair`             TINYINT NOT NULL,
                `from_level`       INT     NOT NULL,
                `to_level`         INT     NOT NULL,
                `direction`        TINYINT NOT NULL COMMENT '+1=up -1=down',
                `transition_count` INT     NOT NULL DEFAULT 1,
                `probability`      FLOAT   NOT NULL DEFAULT 0.0,
                `date_updated`     DATETIME NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_edge` (`pair`, `from_level`, `to_level`),
                INDEX `idx_pair_from` (`pair`, `from_level`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{STATS_TABLE}` (
                `pair`             TINYINT  NOT NULL,
                `avg_candles`      INT      NOT NULL DEFAULT 50,
                `min_candles`      INT      NOT NULL DEFAULT 10,
                `total_extremums`  INT      NOT NULL DEFAULT 0,
                `sig_digits`       TINYINT  NOT NULL DEFAULT 3,
                `date_updated`     DATETIME NULL,
                PRIMARY KEY (`pair`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{INDEX_TABLE}` (
                `id`               INT          NOT NULL AUTO_INCREMENT,
                `weight_code`      VARCHAR(20)  NOT NULL DEFAULT 'output',
                `pair`             TINYINT      NOT NULL,
                `bull_ratio`       FLOAT        NOT NULL DEFAULT 0.5,
                `occurrence_count` INT          NOT NULL DEFAULT 0,
                `date_updated`     DATETIME NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_pair_wc` (`pair`, `weight_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def on_startup(engine_vlad, engine_brain) -> None:
    await _create_tables(engine_vlad)


# ── Загрузка котировок ────────────────────────────────────────────────────────

async def _load_rates(engine, table: str) -> tuple:
    async with engine.connect() as conn:
        res = await conn.execute(text(
            f"SELECT date, open, close, `max`, `min` FROM `{table}` ORDER BY date"
        ))
        rows = res.mappings().all()
    n      = len(rows)
    closes = np.array([float(r["close"] or 0) for r in rows], dtype=np.float64)
    highs  = np.array([float(r["max"]   or 0) for r in rows], dtype=np.float64)
    lows   = np.array([float(r["min"]   or 0) for r in rows], dtype=np.float64)
    return n, closes, highs, lows


# ── Построение графа ──────────────────────────────────────────────────────────

def _build_graph_and_stats(
    closes: np.ndarray,
    highs:  np.ndarray,
    lows:   np.ndarray,
) -> tuple[list, dict, dict]:
    """
    1. Находит все локальные MAX/MIN (argrelextrema, order=GRAPH_ORDER)
    2. Удаляет подряд идущие одного направления (оставляет выраженный)
    3. Округляет уровни до SIG_DIGITS значащих цифр
    4. Строит матрицу переходов и нормирует в вероятности

    Возвращает:
      extremums — [(bar_pos, rounded_level, direction +1/-1), ...]
      graph     — {from_level: [(to_level, count, direction), ...]}
      stats     — {'avg_candles': int, 'min_candles': int, 'total': int}
    """
    n = len(closes)

    res_idx = argrelextrema(highs, np.greater, order=GRAPH_ORDER)[0]
    sup_idx = argrelextrema(lows,  np.less,    order=GRAPH_ORDER)[0]

    # Объединяем MAX и MIN, сортируем по позиции
    all_ext = sorted(
        [(int(i), float(highs[i]), +1) for i in res_idx] +
        [(int(i), float(lows[i]),  -1) for i in sup_idx],
        key=lambda x: x[0]
    )

    # Фильтруем подряд идущие одного направления — оставляем более выраженный
    filtered: list[tuple] = []
    for pos, price, direction in all_ext:
        if filtered and filtered[-1][2] == direction:
            prev = filtered[-1]
            if (direction == +1 and price > prev[1]) or \
               (direction == -1 and price < prev[1]):
                filtered[-1] = (pos, price, direction)
        else:
            filtered.append((pos, price, direction))

    if len(filtered) < 3:
        return [], {}, {}

    # Округляем уровни
    extremums = [(pos, round_to_level(price), direction)
                 for pos, price, direction in filtered]

    # Интервалы между экстремумами → avg_candles
    gaps = [extremums[i+1][0] - extremums[i][0]
            for i in range(len(extremums) - 1)]
    avg_candles = max(5, math.floor(sum(gaps) / len(gaps)))
    min_candles = max(3, min(gaps))

    # Матрица переходов (from_level, to_level) → count
    transitions: dict[tuple[int, int], int] = {}
    for i in range(len(extremums) - 1):
        f = extremums[i][1]
        t = extremums[i+1][1]
        transitions[(f, t)] = transitions.get((f, t), 0) + 1

    # Фильтруем редкие переходы
    transitions = {k: v for k, v in transitions.items() if v >= MIN_TRANSITIONS}

    # Нормируем в вероятности
    from_totals: dict[int, int] = {}
    for (f, _), cnt in transitions.items():
        from_totals[f] = from_totals.get(f, 0) + cnt

    graph: dict[int, list] = {}
    for (f, t), cnt in transitions.items():
        prob      = cnt / from_totals[f]
        direction = +1 if t > f else -1
        if f not in graph:
            graph[f] = []
        graph[f].append((t, cnt, direction, round(prob, 6)))

    stats = {
        "avg_candles": avg_candles,
        "min_candles": min_candles,
        "total":       len(extremums),
    }
    return extremums, graph, stats


# ── Запись в БД ───────────────────────────────────────────────────────────────

async def _write_graph(engine, pair_id: int, graph: dict) -> int:
    """Записывает граф в vlad_extremum_graph_svc59 (DELETE + INSERT)."""
    async with engine.begin() as conn:
        await conn.execute(text(
            f"DELETE FROM `{GRAPH_TABLE}` WHERE pair = :pair"
        ), {"pair": pair_id})

    rows = []
    for from_lvl, nexts in graph.items():
        for to_lvl, cnt, direction, prob in nexts:
            rows.append({
                "pair": pair_id,
                "from": from_lvl,
                "to":   to_lvl,
                "dir":  direction,
                "cnt":  cnt,
                "prob": prob,
            })

    if not rows:
        return 0

    sql = f"""
        INSERT INTO `{GRAPH_TABLE}`
            (pair, from_level, to_level, direction, transition_count, probability, date_updated)
        VALUES (:pair, :from, :to, :dir, :cnt, :prob, NOW())
    """
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        async with engine.begin() as conn:
            await conn.execute(text(sql), rows[i:i+BATCH_SIZE])
        total += len(rows[i:i+BATCH_SIZE])
    return total


async def _write_stats(engine, pair_id: int, stats: dict) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{STATS_TABLE}`
                (pair, avg_candles, min_candles, total_extremums, sig_digits, date_updated)
            VALUES (:pair, :avg, :mn, :tot, :sig, NOW())
            ON DUPLICATE KEY UPDATE
                avg_candles     = VALUES(avg_candles),
                min_candles     = VALUES(min_candles),
                total_extremums = VALUES(total_extremums),
                date_updated    = NOW()
        """), {
            "pair": pair_id,
            "avg":  stats["avg_candles"],
            "mn":   stats["min_candles"],
            "tot":  stats["total"],
            "sig":  SIG_DIGITS,
        })


async def _write_index(engine, pair_id: int, n_extremums: int) -> None:
    """
    Регистрирует weight_code='output' в индексной таблице.
    bull_ratio=0.5 — placeholder, реальное значение вычисляется live в model().
    """
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{INDEX_TABLE}`
                (weight_code, pair, bull_ratio, occurrence_count, date_updated)
            VALUES ('output', :pair, 0.5, :cnt, NOW())
            ON DUPLICATE KEY UPDATE
                occurrence_count = VALUES(occurrence_count),
                date_updated     = NOW()
        """), {"pair": pair_id, "cnt": n_extremums})


# ── Точка входа ───────────────────────────────────────────────────────────────

async def build_index(engine_vlad, engine_brain) -> dict:
    log.info(f"[{INDEX_TABLE}] build_index start")
    await _create_tables(engine_vlad)

    result: dict = {"pairs": {}, "index_table": INDEX_TABLE, "graph_table": GRAPH_TABLE}

    for pair_id, table in RATES_TABLES.items():
        log.info(f"[{INDEX_TABLE}] pair={pair_id} ({table})...")

        try:
            n, closes, highs, lows = await _load_rates(engine_brain, table)
        except Exception as e:
            log.warning(f"[{INDEX_TABLE}] Cannot load {table}: {e}")
            continue

        if n < 500:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: only {n} bars, skip")
            continue

        log.info(f"[{INDEX_TABLE}] pair={pair_id}: {n:,} bars → building graph...")

        extremums, graph, stats = _build_graph_and_stats(closes, highs, lows)
        if not graph:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: empty graph, skip")
            continue

        log.info(
            f"[{INDEX_TABLE}] pair={pair_id}: "
            f"{len(extremums)} extremums | "
            f"{len(graph)} nodes | "
            f"avg_candles={stats['avg_candles']}"
        )

        n_edges = await _write_graph(engine_vlad, pair_id, graph)
        await _write_stats(engine_vlad, pair_id, stats)
        await _write_index(engine_vlad, pair_id, len(extremums))

        result["pairs"][pair_id] = {
            "extremums":   len(extremums),
            "graph_nodes": len(graph),
            "graph_edges": n_edges,
            "avg_candles": stats["avg_candles"],
        }
        log.info(f"[{INDEX_TABLE}] pair={pair_id} done: {result['pairs'][pair_id]}")

    log.info(f"[{INDEX_TABLE}] build_index complete")
    return result
