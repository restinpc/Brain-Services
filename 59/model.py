"""
model.py — brain-extremum-graph (сервис 55)  v2

Граф вероятностей переходов между экстремальными уровнями.
Структуры данных ExtremNode / ExtremGraph + 2 алгоритма обхода.

────────────────────────────────────────────────────────────────────
Общий алгоритм:
  1. Парсим экстремумы, округляем до 3 значащих цифр
  2. Загружаем граф переходов (vlad_extremum_graph_svc55)
  3. Находим узел для текущего экстремума (или ближайший)
  4. Обходим граф одним из 2 методов (type)
  5. Сравниваем предсказанное значение с close + тип экстремума
  6. → {"output": bull_ratio}   bull_ratio>0.5=рост, <0.5=падение

────────────────────────────────────────────────────────────────────
type=0  Жадный путь (1 нитка):
  Идём следуя наибольшей вероятности перехода.
  Стоп: пустой relations (тупик) → mean последних 2 узлов
         цикл → mean всех узлов петли
  Сравниваем с close + тип экстремума → output

type=1  Квантовый обход (дерево):
  Разворачиваем граф в дерево, рекурсивно обходим все ветви.
  Тупик:  branch_signal = mean(последние 2 узла)   × P(нитки)
  Петля:  branch_signal = mean(все узлы петли)       × P(нитки)
  output = сумма branch_signal по всем конечным ветвям
  Сравниваем с close + тип экстремума → bull_ratio

────────────────────────────────────────────────────────────────────
var → глубина ограничения дерева для type=1:
  var=0 → max_depth=10   var=1 → 20   var=2 → 30
  var=3 → 40             var=4 → 50

Единственный выходной ключ: "output"
"""
from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime
from typing import Optional

import numpy as np
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("brain-framework")

# ── Константы ──────────────────────────────────────────────────────────────
SERVICE_ID  = 59
RATES_TABLE = "brain_rates_eur_usd"
OUTPUT_KEY  = "output"

SIG_DIGITS  = 3   # значащих цифр при округлении уровней
GRAPH_TABLE = f"vlad_extremum_graph_svc{SERVICE_ID}"
STATS_TABLE = f"vlad_extremum_stats_svc{SERVICE_ID}"
INDEX_TABLE = f"vlad_extremum_lvl_svc{SERVICE_ID}_index"

# var → max_depth для tree walk (type=1)
VAR_DEPTH: dict[int, int] = {0: 10, 1: 20, 2: 30, 3: 40, 4: 50}

RATES_TABLES: dict[int, str] = {
    1: "brain_rates_eur_usd",
    3: "brain_rates_btc_usd",
    4: "brain_rates_eth_usd",
}

# Масштаб преобразования deviation → bull_ratio
SIGNAL_SCALE = 5.0   # deviation * scale, clamped to [0, 0.45]

# Кеши
_GRAPH_CACHE: dict[int, "ExtremGraph"] = {}
_GRAPH_TTL:   dict[int, float] = {}
_STATS_CACHE: dict[int, dict]  = {}
GRAPH_CACHE_TTL = 3600.0   # обновляем граф раз в час

_LAST_SIGNAL_TS: dict[int, int] = {}   # pair_id → ts последнего сигнала


# ══════════════════════════════════════════════════════════════════════════════
# Структуры данных графа (перевод псевдокода на Python)
# ══════════════════════════════════════════════════════════════════════════════

class ExtremNode:
    """
    Узел графа экстремумов.

    Поля:
        id    — уникальный числовой идентификатор внутри графа
        value — округлённое значение уровня (int, e.g. 932 для BTC 93211)
        relations — словарь исходящих переходов {node_id: count}
    """

    def __init__(self, node_id: int, value: int) -> None:
        self.id:        int = node_id
        self.value:     int = value
        self.relations: dict[int, int] = {}   # {node_id: count}

    def add_relation(self, node_id: int, count: int = 1) -> None:
        """Добавляет (или увеличивает) счётчик перехода к node_id."""
        self.relations[node_id] = self.relations.get(node_id, 0) + count

    def get_relation(self, node_id: int) -> int:
        """Возвращает сырое кол-во переходов к node_id (0 если нет)."""
        return self.relations.get(node_id, 0)

    def get_relations(self) -> dict[int, float]:
        """
        Возвращает нормализованные вероятности переходов.
        {node_id: probability}  sum = 1.0
        """
        total = sum(self.relations.values())
        if total == 0:
            return {}
        return {nid: cnt / total for nid, cnt in self.relations.items()}

    def __repr__(self) -> str:
        return f"ExtremNode(id={self.id}, value={self.value}, rels={len(self.relations)})"


class ExtremGraph:
    """
    Граф переходов между уровнями экстремумов.

    nodes — все узлы по их id
    """

    def __init__(self) -> None:
        self.nodes:     list[ExtremNode] = []
        self._by_value: dict[int, int] = {}   # value → node_id

    def add_node(self, value: int) -> int:
        """
        Добавляет узел с данным value. Если уже существует — возвращает id.
        Возвращает node_id.
        """
        if value in self._by_value:
            return self._by_value[value]
        node_id = len(self.nodes)
        node    = ExtremNode(node_id, value)
        self.nodes.append(node)
        self._by_value[value] = node_id
        return node_id

    def get_node(self, node_id: int) -> Optional[ExtremNode]:
        """Возвращает узел по его id или None."""
        if 0 <= node_id < len(self.nodes):
            return self.nodes[node_id]
        return None

    def get_node_by_value(self, value: int) -> Optional[ExtremNode]:
        """Возвращает узел по значению уровня или None."""
        nid = self._by_value.get(value)
        return self.nodes[nid] if nid is not None else None

    def find_nearest(self, value: int) -> Optional[ExtremNode]:
        """Возвращает узел с ближайшим value (для уровней не в графе)."""
        if not self.nodes:
            return None
        return min(self.nodes, key=lambda n: abs(n.value - value))

    def __len__(self) -> int:
        return len(self.nodes)

    def __repr__(self) -> str:
        return f"ExtremGraph({len(self.nodes)} nodes)"


# ══════════════════════════════════════════════════════════════════════════════
# Алгоритм type=0: Жадный путь (1 нитка)
# ══════════════════════════════════════════════════════════════════════════════

def walk_type0(graph: ExtremGraph, start_id: int) -> float:
    """
    Идём по графу следуя наибольшей вероятности на каждом шаге.

    Остановка:
      тупик  → среднее арифметическое ПОСЛЕДНИХ 2 узлов
      петля  → среднее арифметическое ВСЕХ узлов петли

    Возвращает предсказанное значение уровня (float).
    """
    path:    list[int] = [start_id]
    visited: dict[int, int] = {start_id: 0}   # node_id → позиция в path

    current_id = start_id

    while True:
        node = graph.get_node(current_id)
        if node is None:
            break

        rels = node.get_relations()   # {node_id: prob}

        if not rels:
            # Тупик: mean последних 2 узлов
            last_n = path[-2:] if len(path) >= 2 else path
            vals   = [graph.get_node(nid).value for nid in last_n
                      if graph.get_node(nid) is not None]
            return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)

        next_id = max(rels, key=rels.get)   # наиболее вероятный переход

        if next_id in visited:
            # Петля: mean всех узлов в цикле
            loop_start = visited[next_id]
            loop_nodes = path[loop_start:]
            vals = [graph.get_node(nid).value for nid in loop_nodes
                    if graph.get_node(nid) is not None]
            return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)

        visited[next_id] = len(path)
        path.append(next_id)
        current_id = next_id


# ══════════════════════════════════════════════════════════════════════════════
# Алгоритм type=1: Квантовый обход (дерево)
# ══════════════════════════════════════════════════════════════════════════════

def walk_type1(graph: ExtremGraph, start_id: int, max_depth: int = 20) -> float:
    """
    Разворачиваем граф в дерево, обходим все ветви рекурсивно.

    Для каждой конечной ветви:
      branch_signal = mean(узлы ветви) × произведение вероятностей по нитке

    Тупик:  mean(последние 2 узла) × P
    Петля:  mean(все узлы петли)   × P

    Итог = линейная сумма branch_signal по всем конечным ветвям.
    Интерпретация: ожидаемый уровень цены с учётом всех вероятных путей.
    """
    total_signal: list[float] = [0.0]   # через список чтобы изменять в closure

    def _terminal(path: list[int], prob: float) -> None:
        """Добавляет сигнал конечной ветви к общей сумме."""
        last_n = path[-2:] if len(path) >= 2 else path
        vals   = [graph.get_node(nid).value for nid in last_n
                  if graph.get_node(nid) is not None]
        if vals:
            mean_val = sum(vals) / len(vals)
            total_signal[0] += mean_val * prob

    def _recurse(
        node_id:    int,
        path:       list[int],
        path_set:   frozenset[int],
        prob:       float,
        depth:      int,
    ) -> None:
        if depth > max_depth or prob < 1e-9:
            _terminal(path, prob)
            return

        node = graph.get_node(node_id)
        if node is None:
            _terminal(path, prob)
            return

        rels = node.get_relations()
        if not rels:
            # Тупик
            _terminal(path, prob)
            return

        for next_id, next_prob in rels.items():
            branch_prob = prob * next_prob

            if next_id in path_set:
                # Петля: mean всех узлов ЦИКЛА (без повторного включения начала)
                # path.index(next_id) → позиция первого появления (начало цикла)
                # path[loop_start:] = все узлы цикла, без дубля начала
                loop_start = path.index(next_id)
                loop_nodes = path[loop_start:]   # НЕ добавляем next_id ещё раз
                vals = [graph.get_node(n).value for n in loop_nodes
                        if graph.get_node(n) is not None]
                if vals:
                    total_signal[0] += (sum(vals) / len(vals)) * branch_prob
            else:
                new_path    = path + [next_id]
                new_path_set = path_set | {next_id}
                _recurse(next_id, new_path, new_path_set, branch_prob, depth + 1)

    _recurse(start_id, [start_id], frozenset([start_id]), 1.0, 0)
    return total_signal[0]


# ══════════════════════════════════════════════════════════════════════════════
# Преобразование предсказанного значения → bull_ratio
# ══════════════════════════════════════════════════════════════════════════════

def compute_bull_ratio(
    predicted_value:     float,
    current_close:       float,
    last_ext_direction:  int,
) -> Optional[float]:
    """
    Сравниваем предсказанный уровень с текущей ценой и типом экстремума.

    ВАЖНО: predicted_value — целочисленный уровень (e.g. 932 для BTC 93211).
    current_close — сырая цена из котировок (e.g. 93211).
    Перед сравнением current_close конвертируется в тот же целочисленный масштаб.

    Сигнал на РОСТ:
      predicted > close_level  AND  last_ext_direction == -1 (последний был MIN)
      → bull_ratio = 0.5 + modification

    Сигнал на ПАДЕНИЕ:
      predicted < close_level  AND  last_ext_direction == +1 (последний был MAX)
      → bull_ratio = 0.5 - modification

    Противоречие → None

    modification = min(|predicted - close_level| / close_level × SIGNAL_SCALE, 0.45)
    """
    # Конвертируем close в тот же целочисленный масштаб что у узлов графа
    close_level = float(round_to_level(current_close))
    if close_level <= 0:
        return None

    pred_dir = +1 if predicted_value > close_level else -1

    deviation    = abs(predicted_value - close_level) / close_level
    modification = min(deviation * SIGNAL_SCALE, 0.45)

    # Рост: граф ведёт вверх + последний экстремум MIN (отскок от дна)
    if pred_dir > 0 and last_ext_direction < 0:
        return 0.5 + modification

    # Падение: граф ведёт вниз + последний экстремум MAX (откат от пика)
    if pred_dir < 0 and last_ext_direction > 0:
        return 0.5 - modification

    return None   # противоречивый сигнал


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка из БД
# ══════════════════════════════════════════════════════════════════════════════

def round_to_level(price: float, sig: int = SIG_DIGITS) -> int:
    if price <= 0:
        return 0
    magnitude = 10 ** (math.floor(math.log10(price)) - sig + 1)
    return int(price / magnitude)


def _detect_pair_id(rates: list) -> int:
    if not rates:
        return 1
    c = float(rates[-1].get("close") or rates[-1].get("t1") or 0)
    if c > 10_000: return 3
    if c > 100:    return 4
    return 1


def _db_cfg(db: str = None) -> dict:
    return {
        "host":     os.getenv("DB_HOST",     "127.0.0.1"),
        "port":     int(os.getenv("DB_PORT", "3306")),
        "user":     os.getenv("DB_USER",     "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": db or os.getenv("DB_NAME", "vlad"),
    }


def _load_graph_from_db(pair_id: int) -> ExtremGraph:
    """
    Загружает граф из vlad_extremum_graph_svc55 для данной пары.
    Строит ExtremGraph с реальными transition_count.
    """
    graph = ExtremGraph()
    import mysql.connector
    try:
        conn = mysql.connector.connect(**_db_cfg())
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT from_level, to_level, transition_count "
            f"FROM `{GRAPH_TABLE}` WHERE pair = %s",
            (pair_id,)
        )
        for r in cur.fetchall():
            from_id = graph.add_node(int(r["from_level"]))
            to_id   = graph.add_node(int(r["to_level"]))
            graph.get_node(from_id).add_relation(to_id, int(r["transition_count"] or 1))
        cur.close(); conn.close()
        log.debug(f"[graph] pair={pair_id} loaded {len(graph)} nodes")
    except Exception as e:
        log.warning(f"[graph] _load_graph_from_db pair={pair_id}: {e}")
    return graph


def _load_stats_from_db(pair_id: int) -> dict:
    if pair_id in _STATS_CACHE:
        return _STATS_CACHE[pair_id]
    stats = {"avg_candles": 50}
    import mysql.connector
    try:
        conn = mysql.connector.connect(**_db_cfg())
        cur  = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM `{STATS_TABLE}` WHERE pair = %s", (pair_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            stats = dict(row)
    except Exception as e:
        log.warning(f"[graph] stats pair={pair_id}: {e}")
    _STATS_CACHE[pair_id] = stats
    return stats


def _get_graph(pair_id: int) -> ExtremGraph:
    """Возвращает граф из кеша или перезагружает из БД."""
    now = time.time()
    if pair_id not in _GRAPH_CACHE or now - _GRAPH_TTL.get(pair_id, 0) > GRAPH_CACHE_TTL:
        _GRAPH_CACHE[pair_id] = _load_graph_from_db(pair_id)
        _GRAPH_TTL[pair_id]   = now
    return _GRAPH_CACHE[pair_id]


# ══════════════════════════════════════════════════════════════════════════════
# Определение текущего экстремума
# ══════════════════════════════════════════════════════════════════════════════

def _detect_new_extremum(
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
    avg_candles: int,
) -> Optional[dict]:
    """
    Проверяет: сформировался ли новый экстремум на последнем баре?
    Возвращает {'level': int, 'direction': +1/-1, 'price': float} или None.
    direction: +1 = MAX (локальный пик), -1 = MIN (локальное дно).
    """
    n   = len(closes)
    win = max(5, min(avg_candles // 2, 30))
    if n < win + 2:
        return None

    is_max = float(highs[-1]) >= float(np.max(highs[-win - 1:-1]))
    is_min = float(lows[-1])  <= float(np.min(lows[-win - 1:-1]))

    if not is_max and not is_min:
        return None

    price = float(highs[-1]) if is_max else float(lows[-1])
    return {
        "level":     round_to_level(price),
        "direction": +1 if is_max else -1,
        "price":     price,
    }


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа фреймворка
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],
    date:          datetime,
    *,
    type:          int  = 0,
    var:           int  = 2,
    param:         str  = "",
    dataset_index: dict | None = None,
    **kw,
) -> dict[str, float]:
    """
    Применяет граф вероятностей для генерации торгового сигнала.

    type=0  Жадный путь (1 нитка, следуем наибольшей вероятности)
    type=1  Квантовый обход (дерево, взвешенная сумма всех ветвей)

    var → ограничение глубины дерева для type=1 (10/20/30/40/50)

    Возвращает {"output": bull_ratio} или {} (нет сигнала).
    """
    if not rates:
        return {}

    pair_id = _detect_pair_id(rates)

    # ── Статистика ─────────────────────────────────────────────────────────
    stats       = _load_stats_from_db(pair_id)
    avg_candles = int(stats.get("avg_candles") or 50)

    # ── Мин. интервал (0 ≤ elapsed < avg_candles → пропускаем) ────────────
    try:
        current_ts = int(rates[-1]["date"].timestamp()) \
                     if hasattr(rates[-1]["date"], "timestamp") \
                     else int(rates[-1]["date"])
    except Exception:
        return {}

    if pair_id in _LAST_SIGNAL_TS:
        elapsed = (current_ts - _LAST_SIGNAL_TS[pair_id]) // 3600
        if 0 <= elapsed < avg_candles:
            return {}

    # ── Котировки (последние 2×avg_candles или 100 баров) ─────────────────
    tail   = rates[-max(avg_candles * 2, 100):]
    closes = np.array([float(x.get("close") or 0) for x in tail], dtype=np.float64)
    highs  = np.array([float(x.get("max")   or 0) for x in tail], dtype=np.float64)
    lows   = np.array([float(x.get("min")   or 0) for x in tail], dtype=np.float64)

    # ── Детектируем новый экстремум ────────────────────────────────────────
    ext = _detect_new_extremum(closes, highs, lows, avg_candles)
    if ext is None:
        return {}

    # ── Загружаем граф ─────────────────────────────────────────────────────
    graph = _get_graph(pair_id)
    if len(graph) == 0:
        return {}

    # ── Находим стартовый узел (или ближайший) ─────────────────────────────
    start_node = graph.get_node_by_value(ext["level"])
    if start_node is None:
        start_node = graph.find_nearest(ext["level"])
    if start_node is None:
        return {}

    # ── Обходим граф ──────────────────────────────────────────────────────
    if type == 0:
        predicted = walk_type0(graph, start_node.id)
    else:
        max_depth = VAR_DEPTH.get(var, 20)
        predicted = walk_type1(graph, start_node.id, max_depth=max_depth)

    # ── Вычисляем bull_ratio ───────────────────────────────────────────────
    current_close = float(closes[-1])
    bull_ratio    = compute_bull_ratio(predicted, current_close, ext["direction"])

    if bull_ratio is None:
        return {}   # противоречивый сигнал

    _LAST_SIGNAL_TS[pair_id] = current_ts

    direction_str = "↑ LONG" if bull_ratio > 0.5 else "↓ SHORT"
    log.info(
        f"[graph] pair={pair_id} type={type} var={var} "
        f"level={ext['level']} predicted={predicted:.1f} "
        f"close={current_close:.4f} → {direction_str} br={bull_ratio:.4f}"
    )
    return {OUTPUT_KEY: round(bull_ratio, 6)}
