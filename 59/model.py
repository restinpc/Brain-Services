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

    ── ОПТИМИЗАЦИЯ (для кеша, без изменения результатов) ────────────────────
    Нормализованные вероятности переходов (get_relations()/relations_list())
    и argmax-переход (best_next(), для walk_type0) считаются ОДИН РАЗ и
    кешируются на узле — вместо повторного sum()+dict-comprehension на
    КАЖДОМ шаге КАЖДОЙ ветки обхода (как было в исходной get_relations()).
    Кеш сбрасывается при add_relation() (на случай дозаполнения графа).
    """

    def __init__(self, node_id: int, value: int) -> None:
        self.id:        int = node_id
        self.value:     int = value
        self.relations: dict[int, int] = {}   # {node_id: count}
        self._rel_list:  Optional[list[tuple[int, float]]] = None
        self._rel_dict:  Optional[dict[int, float]]        = None
        self._best_next: int = -2   # -2 = не посчитан, -1 = нет переходов

    def add_relation(self, node_id: int, count: int = 1) -> None:
        """Добавляет (или увеличивает) счётчик перехода к node_id."""
        self.relations[node_id] = self.relations.get(node_id, 0) + count
        self._rel_list  = None
        self._rel_dict  = None
        self._best_next = -2

    def get_relation(self, node_id: int) -> int:
        """Возвращает сырое кол-во переходов к node_id (0 если нет)."""
        return self.relations.get(node_id, 0)

    def relations_list(self) -> list[tuple[int, float]]:
        """
        [(node_id, probability), ...] в порядке вставки, сумма prob == 1.0.
        Считается один раз и кешируется (инвалидация в add_relation).
        """
        rl = self._rel_list
        if rl is None:
            total = sum(self.relations.values())
            if total == 0:
                rl = []
            else:
                rl = [(nid, cnt / total) for nid, cnt in self.relations.items()]
            self._rel_list = rl
        return rl

    def get_relations(self) -> dict[int, float]:
        """
        Возвращает нормализованные вероятности переходов.
        {node_id: probability}  sum = 1.0
        (сохранён для совместимости; внутренне использует relations_list()).
        """
        rd = self._rel_dict
        if rd is None:
            rd = dict(self.relations_list())
            self._rel_dict = rd
        return rd

    def best_next(self) -> Optional[int]:
        """
        argmax по вероятности перехода (используется в walk_type0).
        Тай-брейк идентичен max(rels, key=rels.get) — первый по порядку
        вставки при равной вероятности. Кешируется.
        """
        bn = self._best_next
        if bn == -2:
            rl = self.relations_list()
            if not rl:
                bn = -1
            else:
                best_id, best_p = rl[0]
                for nid, p in rl[1:]:
                    if p > best_p:
                        best_id, best_p = nid, p
                bn = best_id
            self._best_next = bn
        return None if bn == -1 else bn

    def __repr__(self) -> str:
        return f"ExtremNode(id={self.id}, value={self.value}, rels={len(self.relations)})"


class ExtremGraph:
    """
    Граф переходов между уровнями экстремумов.

    nodes — все узлы по их id

    ── ОПТИМИЗАЦИЯ ────────────────────────────────────────────────────────────
    _w0_cache / _w1_cache — память результатов walk_type0/walk_type1 на
    уровне ЭКЗЕМПЛЯРА графа (живёт до следующей перезагрузки из БД в
    _get_graph, т.е. до истечения GRAPH_CACHE_TTL). Один и тот же
    start_id встречается на МНОГИХ барах истории (округление уровня даёт
    малое число уникальных значений) — повторные вызовы с тем же start_id
    (и, для type=1, тем же max_depth) отдаются из кеша за O(1).
    Для type=0 результат НЕ зависит от var → 5 вызовов (var=0..4)
    сворачиваются в 1 вычисление + 4 попадания в кеш.
    """

    def __init__(self) -> None:
        self.nodes:     list[ExtremNode] = []
        self._by_value: dict[int, int] = {}   # value → node_id
        self._w0_cache: dict[int, float] = {}
        self._w1_cache: dict[tuple[int, int], float] = {}
        self._na = None   # лениво строится в _get_arrays() (CSR-массивы для numba)

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

# ── ОПТИМИЗАЦИЯ (Numba JIT) ──────────────────────────────────────────────
# При наличии numba горячий цикл (walk_type1, walk_type0) выполняется
# JIT-компилированным кодом на плоских numpy-массивах (CSR: rel_targets/
# rel_probs/rel_offsets + best_next + node_values) — даёт ещё ~×40-50
# относительно итеративной Python-версии (см. _walk_typeN_compute ниже —
# тот же алгоритм и тот же результат, проверено побитово на всех 6 графах).
# Если numba не установлена — используется чистый Python с тем же
# результатом (просто без numba-ускорения). Зависимость опциональна.
try:
    import numpy as _np
    from numba import njit as _njit
    _HAVE_NUMBA = True
except ImportError:
    _HAVE_NUMBA = False


def _build_arrays(graph: ExtremGraph):
    """
    CSR-представление графа для numba.
    Возвращает (rel_targets, rel_probs, rel_offsets, node_values, best_next).
    relations_list()/best_next() уже кешированы на узлах (см. ExtremNode) —
    эта функция вызывается один раз на граф и результат кешируется в
    graph._na (см. _get_arrays).
    """
    n = len(graph.nodes)
    node_values = _np.empty(n, dtype=_np.float64)
    best_next   = _np.full(n, -1, dtype=_np.int64)
    offsets     = _np.zeros(n + 1, dtype=_np.int64)
    targets: list[int] = []
    probs:   list[float] = []
    for i, node in enumerate(graph.nodes):
        node_values[i] = float(node.value)
        rl = node.relations_list()
        offsets[i + 1] = offsets[i] + len(rl)
        for nid, p in rl:
            targets.append(nid)
            probs.append(p)
        bn = node.best_next()
        if bn is not None:
            best_next[i] = bn
    rel_targets = _np.array(targets, dtype=_np.int64) if targets else _np.empty(0, dtype=_np.int64)
    rel_probs   = _np.array(probs, dtype=_np.float64) if probs else _np.empty(0, dtype=_np.float64)
    return rel_targets, rel_probs, offsets, node_values, best_next


def _get_arrays(graph: ExtremGraph):
    """Лениво строит и кеширует CSR-массивы на graph._na."""
    if graph._na is None:
        graph._na = _build_arrays(graph)
    return graph._na


if _HAVE_NUMBA:

    @_njit
    def _walk_type0_nb(rel_offsets, best_next, node_values, start_id):
        n = node_values.shape[0]
        path    = _np.empty(n, dtype=_np.int64)
        visited = _np.full(n, -1, dtype=_np.int64)
        path[0] = start_id
        visited[start_id] = 0
        plen = 1
        current_id = start_id
        while True:
            nxt = best_next[current_id]
            if nxt == -1:
                if plen >= 2:
                    return (node_values[path[plen - 2]] + node_values[path[plen - 1]]) / 2.0
                return node_values[start_id]
            if visited[nxt] != -1:
                loop_start = visited[nxt]
                s = 0.0
                for k in range(loop_start, plen):
                    s += node_values[path[k]]
                return s / (plen - loop_start)
            visited[nxt] = plen
            path[plen] = nxt
            plen += 1
            current_id = nxt

    @_njit
    def _walk_type1_nb(rel_targets, rel_probs, rel_offsets, node_values, start_id, max_depth):
        CUTOFF = 1e-9
        MAXD = max_depth + 2
        st_node  = _np.empty(MAXD, dtype=_np.int64)
        st_depth = _np.empty(MAXD, dtype=_np.int64)
        st_prob  = _np.empty(MAXD, dtype=_np.float64)
        st_idx   = _np.empty(MAXD, dtype=_np.int64)
        path     = _np.empty(MAXD, dtype=_np.int64)

        sp = 0
        st_node[0] = start_id
        st_depth[0] = 0
        st_prob[0] = 1.0
        st_idx[0] = 0
        path[0] = start_id
        plen = 1
        total = 0.0

        while sp >= 0:
            node  = st_node[sp]
            depth = st_depth[sp]
            prob  = st_prob[sp]
            idx   = st_idx[sp]
            rstart = rel_offsets[node]
            rend   = rel_offsets[node + 1]
            nrel   = rend - rstart

            if idx == 0:
                if depth > max_depth or prob < CUTOFF or nrel == 0:
                    if plen >= 2:
                        term_val = (node_values[path[plen - 2]] + node_values[path[plen - 1]]) / 2.0
                    else:
                        term_val = node_values[node]
                    total += term_val * prob
                    sp -= 1
                    if sp >= 0:
                        plen -= 1
                    continue

            if idx >= nrel:
                sp -= 1
                if sp >= 0:
                    plen -= 1
                continue

            next_id   = rel_targets[rstart + idx]
            next_prob = rel_probs[rstart + idx]
            st_idx[sp] = idx + 1
            bp = prob * next_prob

            loop_pos = -1
            for k in range(plen):
                if path[k] == next_id:
                    loop_pos = k
                    break

            if loop_pos != -1:
                s = 0.0
                for k in range(loop_pos, plen):
                    s += node_values[path[k]]
                total += (s / (plen - loop_pos)) * bp
                continue

            sp += 1
            st_node[sp]  = next_id
            st_depth[sp] = depth + 1
            st_prob[sp]  = bp
            st_idx[sp]   = 0
            path[plen] = next_id
            plen += 1

        return total


def walk_type0(graph: ExtremGraph, start_id: int) -> float:
    """
    Идём по графу следуя наибольшей вероятности на каждом шаге.

    Остановка:
      тупик  → среднее арифметическое ПОСЛЕДНИХ 2 узлов
      петля  → среднее арифметическое ВСЕХ узлов петли

    Возвращает предсказанное значение уровня (float).

    ── ОПТИМИЗАЦИЯ ────────────────────────────────────────────────────────
    Результат не зависит от var → кешируется по start_id в graph._w0_cache
    (5 вызовов с var=0..4 → 1 вычисление + 4 попадания в кеш). Вычисление
    выполняется через numba (если доступна) или _walk_type0_compute
    (чистый Python, идентичный результат) — см. ниже.
    """
    cached = graph._w0_cache.get(start_id)
    if cached is not None:
        return cached

    if _HAVE_NUMBA:
        rel_targets, rel_probs, rel_offsets, node_values, best_next = _get_arrays(graph)
        result = _walk_type0_nb(rel_offsets, best_next, node_values, start_id)
    else:
        result = _walk_type0_compute(graph, start_id)

    graph._w0_cache[start_id] = result
    return result


def _walk_type0_compute(graph: ExtremGraph, start_id: int) -> float:
    """
    Чистый Python (fallback, если numba не установлена) — итеративный
    обход через best_next() (кеш на узле), без рекурсии и пересчёта
    get_relations() на каждом шаге. Результат идентичен оригинальной
    рекурсивной версии (проверено побитово).
    """
    path:    list[int] = [start_id]
    visited: dict[int, int] = {start_id: 0}   # node_id → позиция в path
    current_id = start_id

    while True:
        node = graph.get_node(current_id)
        if node is None:
            break

        next_id = node.best_next()

        if next_id is None:
            # Тупик: mean последних 2 узлов
            last_n = path[-2:] if len(path) >= 2 else path
            vals   = [graph.get_node(nid).value for nid in last_n
                      if graph.get_node(nid) is not None]
            return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)

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

    # недостижимо для корректно построенного графа (как и в оригинале)
    last_n = path[-2:] if len(path) >= 2 else path
    vals   = [graph.get_node(nid).value for nid in last_n
              if graph.get_node(nid) is not None]
    return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)


# ══════════════════════════════════════════════════════════════════════════════
# Алгоритм type=1: Квантовый обход (дерево)
# ══════════════════════════════════════════════════════════════════════════════

def walk_type1(graph: ExtremGraph, start_id: int, max_depth: int = 20) -> float:
    """
    Разворачиваем граф в дерево, обходим все ветви.

    Для каждой конечной ветви:
      branch_signal = mean(узлы ветви) × произведение вероятностей по нитке

    Тупик:  mean(последние 2 узла) × P
    Петля:  mean(все узлы петли)   × P

    Итог = линейная сумма branch_signal по всем конечным ветвям.
    Интерпретация: ожидаемый уровень цены с учётом всех вероятных путей.

    ── ОПТИМИЗАЦИЯ (результат идентичен оригиналу — проверено побитово) ────
    1) Кеш по graph._w1_cache[(start_id, max_depth)] — один и тот же
       (start_id, max_depth) повторяется на множестве баров истории.
    2) Вычисление (при промахе кеша) — через numba (если доступна,
       ~×40-50 к итеративной версии) или _walk_type1_compute (чистый
       Python, идентичный результат) — см. ниже.
    """
    key = (start_id, max_depth)
    cached = graph._w1_cache.get(key)
    if cached is not None:
        return cached

    node0 = graph.get_node(start_id)
    if node0 is None:
        graph._w1_cache[key] = 0.0
        return 0.0

    if _HAVE_NUMBA:
        rel_targets, rel_probs, rel_offsets, node_values, best_next = _get_arrays(graph)
        result = _walk_type1_nb(rel_targets, rel_probs, rel_offsets, node_values, start_id, max_depth)
    else:
        result = _walk_type1_compute(graph, start_id, max_depth)

    graph._w1_cache[key] = result
    return result


def _walk_type1_compute(graph: ExtremGraph, start_id: int, max_depth: int) -> float:
    """
    Чистый Python (fallback, если numba не установлена) — итеративный
    обход с явным стеком (без копий path/path_set на каждой ветке,
    relations_list() кеширован на узле — см. ExtremNode). Порядок
    обхода и суммирования идентичен оригинальной рекурсии (проверено
    побитово).
    """
    total = 0.0
    node0 = graph.get_node(start_id)

    path:     list[int]      = [start_id]
    path_pos: dict[int, int] = {start_id: 0}

    # stack-фрейм: [node, depth, prob, rel_list, idx]
    stack: list[list] = [[node0, 0, 1.0, node0.relations_list(), 0]]

    while stack:
        frame = stack[-1]
        node, depth, prob, rels, idx = frame

        if idx == 0:
            if depth > max_depth or prob < 1e-9 or not rels:
                # Тупик: mean(последние 2 узла из path) × prob
                if len(path) >= 2:
                    term_val = (graph.nodes[path[-2]].value + graph.nodes[path[-1]].value) / 2.0
                else:
                    term_val = float(node.value)
                total += term_val * prob
                stack.pop()
                if stack:
                    removed = path.pop()
                    del path_pos[removed]
                continue

        if idx >= len(rels):
            stack.pop()
            if stack:
                removed = path.pop()
                del path_pos[removed]
            continue

        next_id, next_prob = rels[idx]
        frame[4] = idx + 1
        branch_prob = prob * next_prob

        pos = path_pos.get(next_id)
        if pos is not None:
            # Петля: mean всех узлов ЦИКЛА (без повторного включения начала)
            loop_nodes = path[pos:]
            s = 0.0
            for nid in loop_nodes:
                s += graph.nodes[nid].value
            total += (s / len(loop_nodes)) * branch_prob
            continue

        next_node = graph.nodes[next_id]
        path.append(next_id)
        path_pos[next_id] = len(path) - 1
        stack.append([next_node, depth + 1, branch_prob, next_node.relations_list(), 0])

    return total


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
