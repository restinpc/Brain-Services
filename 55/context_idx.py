"""
context_idx.py — brain-extremum-graph

Строит три таблицы:

1. vlad_extremum_graph_svc54   — граф переходов между уровнями
   (pair, from_level, to_level) → probability, direction, transition_count

2. vlad_extremum_stats_svc54  — статистика по паре
   pair → avg_candles (⌊среднее⌋), min_candles, total_extremums, sig_digits

3. vlad_extremum_lvl_svc54_index — точность стратегии по (pair, var)
   pair, var → bull_ratio_t0 (exit через avg_candles баров)
             → bull_ratio_t1 (exit при следующем экстремуме)
             → occurrence_count (кол-во сигналов в истории)

Алгоритм:
  Для каждой пары (EUR/BTC/ETH):
    1. Загружаем часовые котировки
    2. argrelextrema(order=5) → полный список экстремумов
    3. Округляем до 3 значащих цифр → ключи узлов графа
    4. Вычисляем avg_candles = ⌊mean(gap между соседними экстремумами)⌋
    5. Строим матрицу переходов → нормируем в вероятности
    6. Симулируем стратегию на истории → bull_ratio_t0, bull_ratio_t1
    7. Записываем всё в БД
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
    spec = _ilu.spec_from_file_location("graph_model", os.path.join(here, "model.py"))
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_m = _import_model()

SERVICE_ID     = _m.SERVICE_ID
RATES_TABLES   = _m.RATES_TABLES
VAR_STEPS      = _m.VAR_STEPS
SIG_DIGITS     = _m.SIG_DIGITS
round_to_level = _m.round_to_level
_walk_graph    = _m._walk_graph

GRAPH_TABLE = _m.GRAPH_TABLE
STATS_TABLE = _m.STATS_TABLE
INDEX_TABLE = _m.INDEX_TABLE

# Порядок для scipy при построении графа (фиксированный, не зависит от var)
GRAPH_ORDER      = 5
MIN_TRANSITIONS  = 2    # минимум переходов (a, b) чтобы включить в граф
T2_MAX_FORWARD   = 20   # max баров ожидания для t1 при отсутствии экстремума
BATCH_SIZE       = 500


# ── DDL ──────────────────────────────────────────────────────────────────────

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
                `pair`             TINYINT NOT NULL,
                `avg_candles`      INT     NOT NULL DEFAULT 50,
                `min_candles`      INT     NOT NULL DEFAULT 10,
                `total_extremums`  INT     NOT NULL DEFAULT 0,
                `sig_digits`       TINYINT NOT NULL DEFAULT 3,
                `date_updated`     DATETIME NULL,
                PRIMARY KEY (`pair`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{INDEX_TABLE}` (
                `id`               INT     NOT NULL AUTO_INCREMENT,
                `weight_code`      VARCHAR(20) NOT NULL DEFAULT 'signal',
                `pair`             TINYINT NOT NULL,
                `var`              TINYINT NOT NULL,
                `steps`            TINYINT NOT NULL,
                `bull_ratio_t0`    FLOAT   NOT NULL DEFAULT 0.5,
                `bull_ratio_t1`    FLOAT   NOT NULL DEFAULT 0.5,
                `occurrence_count` INT     NOT NULL DEFAULT 0,
                `support`          FLOAT   NOT NULL DEFAULT 0.0,
                `date_updated`     DATETIME NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_pair_var` (`pair`, `var`),
                INDEX `idx_pair` (`pair`)
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
    ts     = np.array([int(r["date"].timestamp()) for r in rows], dtype=np.int64)
    closes = np.array([float(r["close"] or 0)     for r in rows], dtype=np.float64)
    highs  = np.array([float(r["max"]   or 0)     for r in rows], dtype=np.float64)
    lows   = np.array([float(r["min"]   or 0)     for r in rows], dtype=np.float64)
    return n, ts, closes, highs, lows


# ══════════════════════════════════════════════════════════════════════════════
# Основные вычисления
# ══════════════════════════════════════════════════════════════════════════════

def _compute_extremums_and_graph(
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray
) -> tuple[list, dict, dict]:
    """
    1. Находит все локальные MAX/MIN (order=GRAPH_ORDER)
    2. Округляет до SIG_DIGITS значащих цифр
    3. Строит граф переходов

    Возвращает:
      extremums: [(bar_pos, rounded_level, direction+1/-1), ...]
      graph:     {from_level: [(to_level, count), ...]}
      stats:     {'avg_candles': int, 'min_candles': int, 'total': int}
    """
    n       = len(closes)
    res_idx = argrelextrema(highs, np.greater, order=GRAPH_ORDER)[0]
    sup_idx = argrelextrema(lows,  np.less,    order=GRAPH_ORDER)[0]

    # Объединяем и сортируем по позиции
    all_ext = sorted(
        [(int(i), float(highs[i]), +1) for i in res_idx] +
        [(int(i), float(lows[i]),  -1) for i in sup_idx],
        key=lambda x: x[0]
    )

    # Удаляем подряд идущие одного направления (оставляем более выраженный)
    filtered = []
    for pos, price, direction in all_ext:
        if filtered and filtered[-1][2] == direction:
            # Оставляем более выраженный (max из MAX, min из MIN)
            prev = filtered[-1]
            if direction == +1 and price > prev[1]:
                filtered[-1] = (pos, price, direction)
            elif direction == -1 and price < prev[1]:
                filtered[-1] = (pos, price, direction)
        else:
            filtered.append((pos, price, direction))

    if len(filtered) < 3:
        return [], {}, {}

    # Округляем уровни
    extremums = [(pos, round_to_level(price), direction)
                 for pos, price, direction in filtered]

    # Статистика интервалов
    gaps = [extremums[i+1][0] - extremums[i][0]
            for i in range(len(extremums) - 1)]
    avg_candles = max(5,  math.floor(sum(gaps) / len(gaps)))
    min_candles = max(3,  min(gaps))

    # Граф переходов
    transitions: dict[tuple, int] = {}
    for i in range(len(extremums) - 1):
        f = extremums[i][1]
        t = extremums[i+1][1]
        transitions[(f, t)] = transitions.get((f, t), 0) + 1

    # Фильтруем редкие переходы
    transitions = {k: v for k, v in transitions.items() if v >= MIN_TRANSITIONS}

    # Нормируем в вероятности
    from_totals: dict[int, int] = {}
    for (f, t), cnt in transitions.items():
        from_totals[f] = from_totals.get(f, 0) + cnt

    graph: dict[int, list] = {}
    for (f, t), cnt in transitions.items():
        prob = cnt / from_totals[f]
        direction = +1 if t > f else -1
        if f not in graph:
            graph[f] = []
        graph[f].append((t, prob, direction))

    # Сортируем по вероятности
    for f in graph:
        graph[f].sort(key=lambda x: -x[1])

    stats = {
        "avg_candles": avg_candles,
        "min_candles": min_candles,
        "total":       len(extremums),
    }
    return extremums, graph, stats


def _compute_accuracy(
    extremums: list, graph: dict,
    closes: np.ndarray, avg_candles: int,
) -> dict[int, dict]:
    """
    Симулирует стратегию на исторических данных.
    Для каждого var (→ steps) считает bull_ratio_t0 и bull_ratio_t1.

    Возвращает {var_id: {bull_t0, bull_t1, n_signals}}
    """
    n     = len(closes)
    n_ext = len(extremums)
    res: dict[int, dict] = {}

    for var_id, steps in VAR_STEPS.items():
        n_bull_t0 = n_bear_t0 = n_valid_t0 = 0
        n_bull_t1 = n_bear_t1 = n_valid_t1 = 0
        last_signal_pos = -avg_candles  # начальное значение

        for i in range(1, n_ext):
            pos, level, direction = extremums[i]
            prev_pos              = extremums[i-1][0]

            # Мин. интервал
            if pos - last_signal_pos < avg_candles:
                continue

            # Идём по графу
            path = _walk_graph(graph, level, steps)
            if not path:
                continue

            # Сигнал: шаг 1 == шаг var по направлению
            step1_dir = path[0]["dir"]
            stepN_dir = path[-1]["dir"]
            if step1_dir != stepN_dir:
                continue

            signal_dir = step1_dir   # +1=LONG, -1=SHORT
            last_signal_pos = pos

            # ── Исход t0: через avg_candles баров ───────────────────────
            t0 = pos + avg_candles
            if t0 < n:
                delta   = float(closes[t0] - closes[pos])
                success = (delta > 0) if signal_dir > 0 else (delta < 0)
                n_valid_t0 += 1
                if success:
                    n_bull_t0 += 1

            # ── Исход t1: следующий экстремум ───────────────────────────
            if i + 1 < n_ext:
                next_level = extremums[i+1][1]
                next_dir   = +1 if next_level > level else -1
                success    = (next_dir == signal_dir)
                n_valid_t1 += 1
                if success:
                    n_bull_t1 += 1

        def safe_br(num, den): return round(num / den, 6) if den > 0 else 0.5

        res[var_id] = {
            "bull_t0":    safe_br(n_bull_t0,  n_valid_t0),
            "bull_t1":    safe_br(n_bull_t1,  n_valid_t1),
            "n_signals":  n_valid_t0,
        }

    return res


# ══════════════════════════════════════════════════════════════════════════════
# Запись в БД
# ══════════════════════════════════════════════════════════════════════════════

async def _write_graph(engine, pair_id: int, graph: dict) -> int:
    """Записывает граф в vlad_extremum_graph_svc54."""
    rows = []
    for from_lvl, nexts in graph.items():
        total_cnt = sum(cnt for _, cnt, _ in
                        [(n[0], round(n[1] * 1000), n[2]) for n in nexts])  # восстанавливаем count
        for to_lvl, prob, direction in nexts:
            rows.append({
                "pair":  pair_id,
                "from":  from_lvl,
                "to":    to_lvl,
                "dir":   direction,
                "cnt":   max(1, round(prob * 1000)),   # approx count
                "prob":  round(prob, 6),
            })

    if not rows:
        return 0

    # Удаляем старые данные этой пары
    async with engine.begin() as conn:
        await conn.execute(text(
            f"DELETE FROM `{GRAPH_TABLE}` WHERE pair = :pair"
        ), {"pair": pair_id})

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
            INSERT INTO `{STATS_TABLE}` (pair, avg_candles, min_candles, total_extremums, sig_digits, date_updated)
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


async def _write_index(engine, pair_id: int, accuracy: dict, n_bars: int) -> None:
    for var_id, acc in accuracy.items():
        async with engine.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO `{INDEX_TABLE}`
                    (weight_code, pair, var, steps, bull_ratio_t0, bull_ratio_t1,
                     occurrence_count, support, date_updated)
                VALUES ('signal', :pair, :var, :steps, :br0, :br1, :cnt, :sup, NOW())
                ON DUPLICATE KEY UPDATE
                    bull_ratio_t0    = VALUES(bull_ratio_t0),
                    bull_ratio_t1    = VALUES(bull_ratio_t1),
                    occurrence_count = VALUES(occurrence_count),
                    support          = VALUES(support),
                    date_updated     = NOW()
            """), {
                "pair":  pair_id,
                "var":   var_id,
                "steps": VAR_STEPS[var_id],
                "br0":   acc["bull_t0"],
                "br1":   acc["bull_t1"],
                "cnt":   acc["n_signals"],
                "sup":   round(acc["n_signals"] / max(n_bars, 1), 6),
            })


# ── Точка входа ───────────────────────────────────────────────────────────────

async def build_index(engine_vlad, engine_brain) -> dict:
    log.info(f"[{INDEX_TABLE}] build_index start")
    await _create_tables(engine_vlad)

    result = {"pairs": {}}

    for pair_id, table in RATES_TABLES.items():
        log.info(f"[{INDEX_TABLE}] pair={pair_id} ({table})...")

        try:
            n, ts, closes, highs, lows = await _load_rates(engine_brain, table)
        except Exception as e:
            log.warning(f"[{INDEX_TABLE}] Cannot load {table}: {e}")
            continue

        if n < 500:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: only {n} bars, skip")
            continue

        log.info(f"[{INDEX_TABLE}] pair={pair_id}: {n:,} bars, computing graph...")

        # Строим граф и статистику
        extremums, graph, stats = _compute_extremums_and_graph(closes, highs, lows)
        if not graph:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: empty graph")
            continue

        log.info(f"[{INDEX_TABLE}] pair={pair_id}: "
                 f"{len(extremums)} extremums, "
                 f"{len(graph)} graph nodes, "
                 f"avg_candles={stats['avg_candles']}")

        # Вычисляем точность стратегии
        log.info(f"[{INDEX_TABLE}] pair={pair_id}: simulating strategy...")
        accuracy = _compute_accuracy(extremums, graph, closes, stats["avg_candles"])

        # Пишем в БД
        n_edges = await _write_graph(engine_vlad, pair_id, graph)
        await _write_stats(engine_vlad, pair_id, stats)
        await _write_index(engine_vlad, pair_id, accuracy, n)

        pair_result = {
            "extremums":   len(extremums),
            "graph_nodes": len(graph),
            "graph_edges": n_edges,
            "avg_candles": stats["avg_candles"],
            "accuracy":    {
                f"var{v}": {
                    "t0": round(a["bull_t0"], 3),
                    "t1": round(a["bull_t1"], 3),
                    "n":  a["n_signals"],
                }
                for v, a in accuracy.items()
            },
        }
        result["pairs"][pair_id] = pair_result
        log.info(f"[{INDEX_TABLE}] pair={pair_id} done: {pair_result['accuracy']}")

    result["index_table"] = INDEX_TABLE
    result["graph_table"] = GRAPH_TABLE
    log.info(f"[{INDEX_TABLE}] build_index complete: {result}")
    return result
