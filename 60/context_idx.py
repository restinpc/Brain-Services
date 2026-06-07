"""
context_idx.py — brain-candle-graph (сервис 60)

Строит три таблицы:

1. vlad_candle_graph_svc60   — граф переходов между уровнями
   (pair, from_level, to_level) → transition_count, probability

2. vlad_candle_stats_svc60   — статистика по паре
   pair → total_candles, graph_nodes

3. vlad_candle_lvl_svc60_index — индекс для Brain Framework
   weight_code="output" на каждую пару

────────────────────────────────────────────────────────────────────
Главное отличие от сервиса 59 (extremum-graph):

  59: берём только экстремумы (argrelextrema)
      7000–10000 узлов на 120K баров
      переход = extremum_i → extremum_{i+1}

  60: берём ВСЕ свечи
      ~120K переходов на 120K баров
      переход = round(close_i) → round(close_{i+1})
      пропускаем self-transitions (from == to) — шум

Граф получается гораздо плотнее по рёбрам но компактнее по узлам
(уровни часто повторяются), что даёт иную структуру вероятностей.
"""
from __future__ import annotations

import logging
import os

import numpy as np
from sqlalchemy import text

log = logging.getLogger("brain-framework")

import importlib.util as _ilu

def _import_model():
    here = os.path.dirname(os.path.abspath(__file__))
    spec = _ilu.spec_from_file_location("candle_model", os.path.join(here, "model.py"))
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_m = _import_model()

SERVICE_ID     = _m.SERVICE_ID       # 60
RATES_TABLES   = _m.RATES_TABLES
SIG_DIGITS     = _m.SIG_DIGITS
round_to_level = _m.round_to_level

GRAPH_TABLE = _m.GRAPH_TABLE   # vlad_candle_graph_svc60
STATS_TABLE = _m.STATS_TABLE   # vlad_candle_stats_svc60
INDEX_TABLE = _m.INDEX_TABLE   # vlad_candle_lvl_svc60_index

MIN_TRANSITIONS = 3    # минимум переходов (a→b) для включения в граф
BATCH_SIZE      = 500

# Глубина истории для построения графа.
# Берём только последние N лет чтобы:
#   1. Избежать алиасинга уровней: $10, $100, $1000, $10000 → одинаковые level-ключи
#      (round_to_level нормирует к первым 3 цифрам, разные ценовые эпохи сливаются)
#   2. Работать только с релевантным режимом цен
# 3 года = ~26K баров на пару → быстрый rebuild и актуальный граф
LOOKBACK_YEARS  = 3


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
                `pair`          TINYINT  NOT NULL,
                `total_candles` INT      NOT NULL DEFAULT 0,
                `graph_nodes`   INT      NOT NULL DEFAULT 0,
                `graph_edges`   INT      NOT NULL DEFAULT 0,
                `sig_digits`    TINYINT  NOT NULL DEFAULT 3,
                `date_updated`  DATETIME NULL,
                PRIMARY KEY (`pair`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{INDEX_TABLE}` (
                `id`               INT         NOT NULL AUTO_INCREMENT,
                `weight_code`      VARCHAR(20) NOT NULL DEFAULT 'output',
                `pair`             TINYINT     NOT NULL,
                `bull_ratio`       FLOAT       NOT NULL DEFAULT 0.5,
                `occurrence_count` INT         NOT NULL DEFAULT 0,
                `date_updated`     DATETIME NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_pair_wc` (`pair`, `weight_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def on_startup(engine_vlad, engine_brain) -> None:
    await _create_tables(engine_vlad)


# ── Загрузка котировок ────────────────────────────────────────────────────────

async def _load_rates(engine, table: str, lookback_years: int = LOOKBACK_YEARS) -> tuple[int, np.ndarray]:
    """
    Загружает close-цены только за последние lookback_years лет.

    Фильтр по дате критически важен:
      - Без фильтра: полная история BTC от $3 до $100K → алиасинг уровней
        ($10, $100, $1000, $10000 → все дают одинаковый level-ключ)
      - С фильтром 3 года: один ценовой режим → корректный граф
    """
    async with engine.connect() as conn:
        res = await conn.execute(text(
            f"SELECT close FROM `{table}` "
            f"WHERE date >= DATE_SUB(NOW(), INTERVAL :years YEAR) "
            f"ORDER BY date"
        ), {"years": lookback_years})
        rows = res.mappings().all()
    closes = np.array([float(r["close"] or 0) for r in rows], dtype=np.float64)
    return len(closes), closes


# ── Построение графа из всех свечей ──────────────────────────────────────────

def _vectorized_levels(prices: np.ndarray, sig: int = SIG_DIGITS) -> np.ndarray:
    """
    Векторизованный аналог round_to_level для numpy-массива.
    93211 → 932, 1.2016 → 120, 3521 → 352 (те же результаты, но в 50× быстрее).
    """
    safe = np.where(prices > 0, prices, 1e-10)
    magnitudes = 10.0 ** (np.floor(np.log10(safe)) - sig + 1)
    return (prices / magnitudes).astype(np.int64)


def _build_candle_graph(closes: np.ndarray) -> tuple[dict, dict]:
    """
    Строит граф переходов из всех последовательных пар свечей.

    Для каждой пары (close_i, close_{i+1}):
      from_level = round_to_level(close_i)
      to_level   = round_to_level(close_{i+1})

    Self-transitions (from == to) пропускаются:
      цена не вышла за пределы одного округлённого уровня → не несёт информации.

    Возвращает:
      transitions — {(from_level, to_level): count}
      stats       — {'total_candles': int, 'skipped_self': int, 'after_filter': int}
    """
    n = len(closes)
    if n < 2:
        return {}, {}

    # Vectorized: вычисляем все уровни за один numpy-проход
    levels = _vectorized_levels(closes)
    f_arr  = levels[:-1]   # from
    t_arr  = levels[1:]    # to

    # Маска: не-self переходы с корректными уровнями
    mask         = (f_arr != t_arr) & (f_arr > 0) & (t_arr > 0)
    skipped_self = int((f_arr == t_arr).sum())

    valid_pairs = np.stack([f_arr[mask], t_arr[mask]], axis=1)
    if len(valid_pairs) == 0:
        return {}, {"total_candles": n, "skipped_self": skipped_self, "after_filter": 0}

    # np.unique с return_counts → подсчёт каждой уникальной пары
    unique_pairs, counts = np.unique(valid_pairs, axis=0, return_counts=True)

    # Фильтруем редкие переходы и строим dict
    transitions = {
        (int(f), int(t)): int(c)
        for (f, t), c in zip(unique_pairs, counts)
        if c >= MIN_TRANSITIONS
    }

    stats = {
        "total_candles":  n,
        "skipped_self":   skipped_self,
        "after_filter":   len(transitions),
    }
    return transitions, stats


def _normalize_graph(transitions: dict) -> dict[int, list]:
    """
    Нормирует transition counts в вероятности.
    Возвращает {from_level: [(to_level, count, direction, probability), ...]}.
    """
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

    # Сортируем по вероятности desc
    for f in graph:
        graph[f].sort(key=lambda x: -x[3])

    return graph


# ── Запись в БД ───────────────────────────────────────────────────────────────

async def _write_graph(engine, pair_id: int, graph: dict) -> int:
    """Записывает граф. Один DELETE + все INSERTы в одной транзакции."""
    rows = []
    for from_lvl, nexts in graph.items():
        for to_lvl, cnt, direction, prob in nexts:
            rows.append({
                "pair": pair_id, "from": from_lvl, "to": to_lvl,
                "dir": direction, "cnt": cnt, "prob": prob,
            })

    sql = f"""
        INSERT INTO `{GRAPH_TABLE}`
            (pair, from_level, to_level, direction, transition_count, probability, date_updated)
        VALUES (:pair, :from, :to, :dir, :cnt, :prob, NOW())
    """
    async with engine.begin() as conn:   # одна транзакция на всё
        await conn.execute(text(
            f"DELETE FROM `{GRAPH_TABLE}` WHERE pair = :pair"
        ), {"pair": pair_id})

        for i in range(0, len(rows), BATCH_SIZE):
            if rows[i:i + BATCH_SIZE]:
                await conn.execute(text(sql), rows[i:i + BATCH_SIZE])

    return len(rows)


async def _write_stats(engine, pair_id: int,
                       stats: dict, n_nodes: int, n_edges: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{STATS_TABLE}`
                (pair, total_candles, graph_nodes, graph_edges, sig_digits, date_updated)
            VALUES (:pair, :tc, :gn, :ge, :sig, NOW())
            ON DUPLICATE KEY UPDATE
                total_candles = VALUES(total_candles),
                graph_nodes   = VALUES(graph_nodes),
                graph_edges   = VALUES(graph_edges),
                date_updated  = NOW()
        """), {
            "pair": pair_id,
            "tc":   stats["total_candles"],
            "gn":   n_nodes,
            "ge":   n_edges,
            "sig":  SIG_DIGITS,
        })


async def _write_index(engine, pair_id: int, total_candles: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{INDEX_TABLE}`
                (weight_code, pair, bull_ratio, occurrence_count, date_updated)
            VALUES ('output', :pair, 0.5, :cnt, NOW())
            ON DUPLICATE KEY UPDATE
                occurrence_count = VALUES(occurrence_count),
                date_updated     = NOW()
        """), {"pair": pair_id, "cnt": total_candles})


# ── Точка входа ───────────────────────────────────────────────────────────────

async def build_index(engine_vlad, engine_brain) -> dict:
    log.info(f"[{INDEX_TABLE}] build_index start")
    await _create_tables(engine_vlad)

    result: dict = {"pairs": {}, "graph_table": GRAPH_TABLE}

    for pair_id, table in RATES_TABLES.items():
        log.info(f"[{INDEX_TABLE}] pair={pair_id} ({table})...")

        try:
            n, closes = await _load_rates(engine_brain, table)
        except Exception as e:
            log.warning(f"[{INDEX_TABLE}] Cannot load {table}: {e}")
            continue

        if n < 100:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: only {n} bars, skip")
            continue

        log.info(f"[{INDEX_TABLE}] pair={pair_id}: {n:,} candles → building graph...")

        # Строим граф из всех свечей
        transitions, stats = _build_candle_graph(closes)
        if not transitions:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: empty transitions")
            continue

        graph = _normalize_graph(transitions)
        n_nodes = len(graph)

        log.info(
            f"[{INDEX_TABLE}] pair={pair_id}: "
            f"{n:,} candles | "
            f"{stats['skipped_self']:,} self-transitions skipped | "
            f"{stats['after_filter']:,} edges after filter | "
            f"{n_nodes} nodes"
        )

        n_edges = await _write_graph(engine_vlad, pair_id, graph)
        await _write_stats(engine_vlad, pair_id, stats, n_nodes, n_edges)
        await _write_index(engine_vlad, pair_id, n)

        result["pairs"][pair_id] = {
            "total_candles":    n,
            "skipped_self":     stats["skipped_self"],
            "graph_nodes":      n_nodes,
            "graph_edges":      n_edges,
        }
        log.info(f"[{INDEX_TABLE}] pair={pair_id} done: {result['pairs'][pair_id]}")

    log.info(f"[{INDEX_TABLE}] build_index complete")
    return result
