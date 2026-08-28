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
from typing import Optional

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
level_to_price = _m.level_to_price
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


# ── Извлечение подтверждённых экстремумов (используется и графом, и подбором sig) ──

def _extract_confirmed_extrema(
    highs: np.ndarray,
    lows:  np.ndarray,
    order: int = GRAPH_ORDER,
) -> list[tuple[int, float, int]]:
    """
    1. Находит все локальные MAX/MIN (argrelextrema, order=GRAPH_ORDER)
    2. Outside-bar guard: убирает бары, одновременно MAX и MIN
    3. Граничный фильтр: убирает экстремумы без полных order соседей с краёв

    Возвращает [(bar_pos, price, direction +1/-1), ...] отсортированный по позиции.
    Вынесено в отдельную функцию, чтобы _choose_sig_digits и _build_graph_and_stats
    использовали ИДЕНТИЧНУЮ детекцию экстремумов (независимо от sig_digits —
    округление применяется ПОСЛЕ, экстремумы детектируются на сырых ценах).
    """
    n = len(highs)
    res_idx = argrelextrema(highs, np.greater, order=order)[0]
    sup_idx = argrelextrema(lows,  np.less,    order=order)[0]

    all_ext = sorted(
        [(int(i), float(highs[i]), +1) for i in res_idx] +
        [(int(i), float(lows[i]),  -1) for i in sup_idx],
        key=lambda x: x[0]
    )

    # Outside-bar guard (см. обоснование в _build_graph_and_stats)
    _pos_seen: set[int] = set()
    _pos_dup:  set[int] = set()
    for pos, _, _ in all_ext:
        if pos in _pos_seen:
            _pos_dup.add(pos)
        _pos_seen.add(pos)
    all_ext = [e for e in all_ext if e[0] not in _pos_dup]

    # Граничный фильтр
    all_ext = [e for e in all_ext if order <= e[0] < n - order]

    return all_ext


def _build_transitions(
    extremums: list[tuple[int, int, int]],
    min_transitions: int = MIN_TRANSITIONS,
) -> dict[tuple[int, int], int]:
    """Строит матрицу переходов (from_level, to_level) → count из ОКРУГЛЁННЫХ
    экстремумов [(pos, level, direction), ...]. Self-transitions пропускаются."""
    transitions: dict[tuple[int, int], int] = {}
    for i in range(len(extremums) - 1):
        f = extremums[i][1]
        t = extremums[i + 1][1]
        if f == t:
            continue
        transitions[(f, t)] = transitions.get((f, t), 0) + 1
    return {k: v for k, v in transitions.items() if v >= min_transitions}


def _normalize_transitions(transitions: dict[tuple[int, int], int]) -> dict[int, list]:
    """Нормирует transitions в {from_level: [(to_level, count, direction, prob), ...]}."""
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
    return graph


def _dict_graph_to_extremgraph(graph: dict[int, list]) -> "_m.ExtremGraph":
    """Конвертирует {from_level: [(to_level, count, ...)]} в ExtremGraph
    (структура из model.py), чтобы walk_type0/walk_type1 работали ИДЕНТИЧНО
    тому, как они работают в живой модели — без повторной реализации обхода."""
    eg = _m.ExtremGraph()
    for from_lvl, nexts in graph.items():
        from_id = eg.add_node(from_lvl)
        for to_lvl, cnt, direction, prob in nexts:
            to_id = eg.add_node(to_lvl)
            eg.get_node(from_id).add_relation(to_id, cnt)
    return eg


# ── Адаптивный подбор sig_digits per pair ────────────────────────────────────────

SIG_CANDIDATES   = (2, 3, 4, 5)   # перебираемые значения значащих цифр
MIN_SIG_SUPPORT  = 5.0            # мин. среднее число визитов на уровень (анти-оверфит)
SIG_VAL_FRACTION = 0.35           # доля экстремумов на конец истории под валидацию
SIG_VAL_HOLDS    = (6, 12, 24, 48)  # hold-периоды (часы) для усреднения Sharpe


def _eval_sig_candidate(
    closes: np.ndarray,
    val_ext: list[tuple[int, float, int]],
    graph:   dict[int, list],
    sig_digits: int,
    order: int,
    holds: tuple[int, ...],
) -> Optional[float]:
    """
    Прогоняет валидационные экстремумы через РЕАЛЬНЫЙ walk_type0 И
    РЕАЛЬНЫЙ compute_bull_ratio (оба — из model.py, вызываются напрямую,
    не дублируются здесь) для нескольких hold-периодов, усредняя Sharpe.

    Использование _m.compute_bull_ratio напрямую (вместо копии его логики)
    гарантирует, что подбор sig_digits оценивает ТОЧНО ТУ ЖЕ функцию,
    которая работает в live model() — включая round_to_level/level_to_price
    фикс глобальной монотонности. Раньше здесь была inline-копия старой
    (проблемной) формулы сравнения уровней; расхождение между копией и
    оригиналом — именно тот класс багов, который мы уже чинили между
    model.py и context_idx.py для детекции экстремумов.

    Это калька с live model(): entry = idx + order (задержка подтверждения,
    без look-ahead).
    Возвращает None если меньше половины holds дали ≥15 сделок (мало данных).
    """
    eg = _dict_graph_to_extremgraph(graph)
    n_total = len(closes)
    sharpes: list[float] = []

    for hold in holds:
        trades = []
        for idx, price, direction in val_ext:
            entry = idx + order
            exitb = entry + hold
            if exitb >= n_total:
                continue
            level = round_to_level(price, sig_digits)
            if level <= 0:
                continue
            node = eg.get_node_by_value(level) or eg.find_nearest(level)
            if node is None:
                continue
            predicted = _m.walk_type0(eg, node.id)
            current_close = closes[entry]
            bull_ratio = _m.compute_bull_ratio(predicted, current_close, direction, sig_digits)
            if bull_ratio is None:
                continue
            sgn = +1 if bull_ratio > 0.5 else -1
            ret = (closes[exitb] - closes[entry]) / closes[entry] * 100
            trades.append(ret * sgn)

        if len(trades) < 15:
            continue
        arr = np.array(trades)
        sharpes.append(float(arr.mean() / arr.std()) if arr.std() > 0 else 0.0)

    if len(sharpes) < max(1, len(holds) // 2):
        return None
    return float(np.mean(sharpes))


def _choose_sig_digits(
    closes: np.ndarray,
    highs:  np.ndarray,
    lows:   np.ndarray,
    order:  int = GRAPH_ORDER,
    candidates:  tuple[int, ...] = SIG_CANDIDATES,
    min_support: float = MIN_SIG_SUPPORT,
    val_frac:    float = SIG_VAL_FRACTION,
    holds:       tuple[int, ...] = SIG_VAL_HOLDS,
    default_sig: int = SIG_DIGITS,
) -> tuple[int, dict]:
    """
    Подбирает per-pair sig_digits через walk-forward внутреннюю валидацию
    (используется ТОЛЬКО историческими данными — production-realistic,
    никакого знания о "будущем" тестовом периоде).

    Почему это нужно:
    SIG_DIGITS=3 — общий дефолт для всех пар. На реальных данных это
    приводит к двум противоположным проблемам в зависимости от пары:
      • Слишком ГРУБО относительно объёма данных → переобучение графа
        (мало визитов на уровень, конкретные рёбра держатся на 1-2 совпадениях)
      • Слишком ТОЧНО → граф не успевает накопить статистику, шумные предсказания

    Алгоритм:
      1. support = len(ВСЕ_confirmed_extrema) / unique_levels(ВСЕ) —
         оценивается на ПОЛНОМ доступном датасете, т.к. именно с таким
         объёмом данных будет построен финальный production-граф
         (build_index использует 100% данных, не train/val срез).
         support < min_support → кандидат отбрасывается (анти-оверфит гейт).
      2. Для Sharpe-оценки ОТДЕЛЬНО делим extrema на train((1-val_frac))/val
         по позиции — здесь срез необходим, иначе оценка производительности
         была бы look-ahead (тестировали бы на данных, по которым строили граф).
      3. Строим граф на train-срезе, конвертируем в ExtremGraph, прогоняем val
         через РЕАЛЬНЫЙ walk_type0 на нескольких hold-периодах (см. SIG_VAL_HOLDS),
         усредняем Sharpe.
      4. Возвращаем sig с лучшим средним Sharpe среди прошедших гейт;
         если ни один не прошёл — default_sig.

    ВАЖНО (исправление): support ранее считался на train-срезе (65% данных),
    что искусственно занижало оценку и отсекало валидные sig — например,
    ETH sig=3 показывал support=7.2 на полных данных (прошёл бы гейт), но
    только 4.7 на 65%-срезе (не проходил). Теперь support считается на 100%
    данных (реалистично отражает production-граф), а train/val split
    используется ТОЛЬКО для честной (без look-ahead) оценки Sharpe.

    Финальный production-граф строится на 100% данных (build_index ниже)
    с выбранным sig_digits.
    """
    all_ext = _extract_confirmed_extrema(highs, lows, order)
    if len(all_ext) < 100:
        return default_sig, {"reason": "insufficient_extrema", "n": len(all_ext)}

    split = int(len(all_ext) * (1 - val_frac))
    train_ext, val_ext = all_ext[:split], all_ext[split:]
    if len(val_ext) < 50:
        return default_sig, {"reason": "insufficient_val", "n_val": len(val_ext)}

    diag: dict = {}
    best_sig, best_score = default_sig, float("-inf")
    valid_found = False

    for sig in candidates:
        # Support на ПОЛНОМ датасете — отражает реальный production-граф
        all_levels = [round_to_level(p, sig) for _, p, _ in all_ext]
        n_unique   = len(set(all_levels))
        support    = len(all_levels) / n_unique if n_unique else 0.0
        diag[sig]  = {"support": round(support, 1), "valid": support >= min_support}

        if support < min_support:
            continue   # риск оверфита — слишком мало визитов на уровень

        # Sharpe-оценка — ТОЛЬКО на train-срезе, чтобы val остался honest hold-out
        train_rounded = [(pos, round_to_level(price, sig), direction)
                          for pos, price, direction in train_ext]
        transitions = _build_transitions(train_rounded, MIN_TRANSITIONS)
        if not transitions:
            continue
        graph = _normalize_transitions(transitions)

        score = _eval_sig_candidate(closes, val_ext, graph, sig, order, holds)
        if score is None:
            continue

        diag[sig]["avg_sharpe"] = round(score, 4)
        valid_found = True
        if score > best_score:
            best_score, best_sig = score, sig

    if not valid_found:
        diag["fallback"] = default_sig
        return default_sig, diag
    return best_sig, diag


# ── Построение графа ──────────────────────────────────────────────────────────

def _build_graph_and_stats(
    closes: np.ndarray,
    highs:  np.ndarray,
    lows:   np.ndarray,
    sig_digits: int = SIG_DIGITS,
) -> tuple[list, dict, dict]:
    """
    1. Находит все локальные MAX/MIN (argrelextrema, order=GRAPH_ORDER)
    2. Удаляет outside-bar и граничные экстремумы
    3. Округляет уровни до sig_digits значащих цифр (per-pair, см. _choose_sig_digits)
    4. Строит матрицу переходов и нормирует в вероятности

    Возвращает:
      extremums — [(bar_pos, rounded_level, direction +1/-1), ...]
      graph     — {from_level: [(to_level, count, direction), ...]}
      stats     — {'avg_candles': int, 'min_candles': int, 'total': int}
    """
    n = len(closes)

    all_ext = _extract_confirmed_extrema(highs, lows, GRAPH_ORDER)

    # Используем все подтверждённые экстремумы без дополнительной фильтрации
    # подряд идущих одного направления. Причина (Вариант A):
    #   context_idx ранее удалял "менее выраженный" из двух подряд идущих MAX/MAX,
    #   но live-детектор model.py видит только прошлое и не знает, будет ли следующий
    #   экстремум того же направления. Это давало ~12-13% сигналов на несуществующих
    #   в графе узлах. Убирая фильтр из обоих мест, граф и детектор полностью
    #   согласованы без look-ahead.
    filtered = list(all_ext)

    if len(filtered) < 3:
        return [], {}, {}

    # Округляем уровни (per-pair sig_digits — НЕ модульная константа)
    extremums = [(pos, round_to_level(price, sig_digits), direction)
                 for pos, price, direction in filtered]

    # Интервалы между экстремумами → avg_candles
    gaps = [extremums[i+1][0] - extremums[i][0]
            for i in range(len(extremums) - 1)]
    avg_candles = max(5, math.floor(sum(gaps) / len(gaps)))
    min_candles = max(3, min(gaps))

    # Self-transitions (f == t) пропускаются: одинаковый уровень не несёт
    # информации о направлении и даёт bull_ratio=0.5 (нейтральный шум).
    transitions = _build_transitions(extremums, MIN_TRANSITIONS)
    graph       = _normalize_transitions(transitions)

    stats = {
        "avg_candles": avg_candles,
        "min_candles": min_candles,
        "total":       len(extremums),
    }
    return extremums, graph, stats


# ── Запись в БД ───────────────────────────────────────────────────────────────

async def _write_graph(engine, pair_id: int, graph: dict) -> int:
    """
    Записывает граф атомарно: DELETE + INSERT в ОДНОЙ транзакции.
    Если INSERT упадёт, граф пары не будет очищен (rollback).
    """
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
    async with engine.begin() as conn:          # ← ОДНА транзакция
        await conn.execute(text(
            f"DELETE FROM `{GRAPH_TABLE}` WHERE pair = :pair"
        ), {"pair": pair_id})
        for i in range(0, len(rows), BATCH_SIZE):
            await conn.execute(text(sql), rows[i:i+BATCH_SIZE])
            total += len(rows[i:i+BATCH_SIZE])
    return total


async def _write_stats(engine, pair_id: int, stats: dict, sig_digits: int) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{STATS_TABLE}`
                (pair, avg_candles, min_candles, total_extremums, sig_digits, date_updated)
            VALUES (:pair, :avg, :mn, :tot, :sig, NOW())
            ON DUPLICATE KEY UPDATE
                avg_candles     = VALUES(avg_candles),
                min_candles     = VALUES(min_candles),
                total_extremums = VALUES(total_extremums),
                sig_digits      = VALUES(sig_digits),
                date_updated    = NOW()
        """), {
            "pair": pair_id,
            "avg":  stats["avg_candles"],
            "mn":   stats["min_candles"],
            "tot":  stats["total"],
            "sig":  sig_digits,
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


async def _clear_pair(engine, pair_id: int) -> None:
    """Атомарно удаляет все данные пары, не оставляя устаревшую статистику."""
    async with engine.begin() as conn:
        await conn.execute(text(f"DELETE FROM `{GRAPH_TABLE}` WHERE pair=:pair"), {"pair": pair_id})
        await conn.execute(text(f"DELETE FROM `{STATS_TABLE}` WHERE pair=:pair"), {"pair": pair_id})
        await conn.execute(text(f"DELETE FROM `{INDEX_TABLE}` WHERE pair=:pair"), {"pair": pair_id})
    _m.invalidate_pair_cache(pair_id)


async def _publish_pair(engine, pair_id: int, graph: dict, stats: dict,
                        sig_digits: int, n_extremums: int) -> int:
    """Публикует граф, статистику и индекс одной транзакцией."""
    rows = []
    for from_lvl, nexts in graph.items():
        for to_lvl, cnt, direction, prob in nexts:
            rows.append({"pair": pair_id, "from": from_lvl, "to": to_lvl,
                         "dir": direction, "cnt": cnt, "prob": prob})
    sql = f"""INSERT INTO `{GRAPH_TABLE}`
        (pair, from_level, to_level, direction, transition_count, probability, date_updated)
        VALUES (:pair,:from,:to,:dir,:cnt,:prob,NOW())"""
    async with engine.begin() as conn:
        await conn.execute(text(f"DELETE FROM `{GRAPH_TABLE}` WHERE pair=:pair"), {"pair": pair_id})
        for i in range(0, len(rows), BATCH_SIZE):
            await conn.execute(text(sql), rows[i:i+BATCH_SIZE])
        await conn.execute(text(f"""INSERT INTO `{STATS_TABLE}`
            (pair, avg_candles, min_candles, total_extremums, sig_digits, date_updated)
            VALUES (:pair,:avg,:mn,:tot,:sig,NOW())
            ON DUPLICATE KEY UPDATE avg_candles=VALUES(avg_candles),
            min_candles=VALUES(min_candles), total_extremums=VALUES(total_extremums),
            sig_digits=VALUES(sig_digits), date_updated=NOW()"""),
            {"pair":pair_id,"avg":stats["avg_candles"],"mn":stats["min_candles"],
             "tot":stats["total"],"sig":sig_digits})
        await conn.execute(text(f"""INSERT INTO `{INDEX_TABLE}`
            (weight_code,pair,bull_ratio,occurrence_count,date_updated)
            VALUES ('output',:pair,0.5,:cnt,NOW())
            ON DUPLICATE KEY UPDATE occurrence_count=VALUES(occurrence_count),date_updated=NOW()"""),
            {"pair":pair_id,"cnt":n_extremums})
    _m.invalidate_pair_cache(pair_id)
    return len(rows)


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
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: only {n} bars — очищаем stale данные")
            await _clear_pair(engine_vlad, pair_id)
            continue

        log.info(f"[{INDEX_TABLE}] pair={pair_id}: {n:,} bars → building graph...")

        # ── Подбираем sig_digits для этой пары (walk-forward на истории) ──────
        chosen_sig, sig_diag = _choose_sig_digits(closes, highs, lows)
        log.info(f"[{INDEX_TABLE}] pair={pair_id}: sig_digits={chosen_sig} (diag={sig_diag})")

        extremums, graph, stats = _build_graph_and_stats(closes, highs, lows, chosen_sig)
        if not graph:
            log.warning(f"[{INDEX_TABLE}] pair={pair_id}: empty graph — очищаем старые данные")
            await _clear_pair(engine_vlad, pair_id)
            continue

        log.info(
            f"[{INDEX_TABLE}] pair={pair_id}: "
            f"{len(extremums)} extremums | "
            f"{len(graph)} nodes | "
            f"avg_candles={stats['avg_candles']} | sig_digits={chosen_sig}"
        )

        n_edges = await _publish_pair(
            engine_vlad, pair_id, graph, stats, chosen_sig, len(extremums)
        )

        result["pairs"][pair_id] = {
            "extremums":   len(extremums),
            "graph_nodes": len(graph),
            "graph_edges": n_edges,
            "avg_candles": stats["avg_candles"],
            "sig_digits":  chosen_sig,
        }
        log.info(f"[{INDEX_TABLE}] pair={pair_id} done: {result['pairs'][pair_id]}")

    log.info(f"[{INDEX_TABLE}] build_index complete")
    return result
