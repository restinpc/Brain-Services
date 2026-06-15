"""
model.py — brain-candle-graph (сервис 60)

Третья экспериментальная модель графа вероятностей.
Аналог сервиса 59, но граф строится НА БАЗЕ ВСЕХ СВЕЧЕЙ, а не только экстремумов.

────────────────────────────────────────────────────────────────────
Отличие от сервиса 59 (extremum-graph):
  59: граф из пар (экстремум_i → экстремум_i+1)
      сигнал только при формировании нового экстремума
  60: граф из пар (candle_i → candle_j) для всех свечей
      сигнал на каждой свече (нет ожидания экстремума)

────────────────────────────────────────────────────────────────────
Алгоритм model():
  1. Округляем текущий close до 3 значащих цифр → entry_level
  2. Находим узел графа для этого уровня (или ближайший)
  3. Обходим граф одним из 2 методов (type):
       type=0  Жадный путь (1 нитка, наибольшая вероятность)
       type=1  Квантовый обход (дерево, sum(mean_ветки × P_нитки))
  4. Сравниваем predicted_level с current_level:
       predicted > current → LONG  (bull_ratio > 0.5)
       predicted < current → SHORT (bull_ratio < 0.5)
  5. Возвращаем {"output": bull_ratio}

────────────────────────────────────────────────────────────────────
var → ограничение глубины для type=1:
  var=0 → 10   var=1 → 20   var=2 → 30
  var=3 → 40   var=4 → 50

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
SERVICE_ID  = 60
RATES_TABLE = "brain_rates_eur_usd"
OUTPUT_KEY  = "output"

SIG_DIGITS = 3

GRAPH_TABLE = f"vlad_candle_graph_svc{SERVICE_ID}"
STATS_TABLE = f"vlad_candle_stats_svc{SERVICE_ID}"
INDEX_TABLE = f"vlad_candle_lvl_svc{SERVICE_ID}_index"

VAR_DEPTH: dict[int, int] = {0: 10, 1: 20, 2: 30, 3: 40, 4: 50}

RATES_TABLES: dict[int, str] = {
    1: "brain_rates_eur_usd",
    3: "brain_rates_btc_usd",
    4: "brain_rates_eth_usd",
}

# Минимальная модификация для фильтра шума
MIN_MODIFICATION = 0.005   # |predicted - current| / current × SIGNAL_SCALE ≥ 0.005
SIGNAL_SCALE     = 5.0

_GRAPH_CACHE: dict[int, "ExtremGraph"] = {}
_GRAPH_TTL:   dict[int, float]         = {}
GRAPH_CACHE_TTL = 3600.0


# ══════════════════════════════════════════════════════════════════════════════
# Структуры данных (идентичны сервису 59)
# ══════════════════════════════════════════════════════════════════════════════

class ExtremNode:
    # ── ОПТИМИЗАЦИЯ: нормализованные relations и argmax-переход считаются
    # один раз и кешируются (инвалидация при add_relation). Без этого
    # get_relations() пересчитывал sum()+dict-comprehension на каждом
    # шаге каждой ветки walk_type1 — основная причина медленного кеша.
    def __init__(self, node_id: int, value: int) -> None:
        self.id:        int = node_id
        self.value:     int = value
        self.relations: dict[int, int] = {}
        self._rel_list:  Optional[list[tuple[int, float]]] = None
        self._rel_dict:  Optional[dict[int, float]]        = None
        self._best_next: int = -2  # -2=не посчитан, -1=нет переходов

    def add_relation(self, node_id: int, count: int = 1) -> None:
        self.relations[node_id] = self.relations.get(node_id, 0) + count
        self._rel_list  = None
        self._rel_dict  = None
        self._best_next = -2

    def get_relation(self, node_id: int) -> int:
        return self.relations.get(node_id, 0)

    def relations_list(self) -> list[tuple[int, float]]:
        """[(node_id, prob), ...], сумма prob==1.0. Кешируется."""
        rl = self._rel_list
        if rl is None:
            total = sum(self.relations.values())
            rl = [] if total == 0 else [(nid, cnt / total) for nid, cnt in self.relations.items()]
            self._rel_list = rl
        return rl

    def get_relations(self) -> dict[int, float]:
        rd = self._rel_dict
        if rd is None:
            rd = dict(self.relations_list())
            self._rel_dict = rd
        return rd

    def best_next(self) -> Optional[int]:
        """argmax по вероятности (для walk_type0), тай-брейк как у max(rels, key=rels.get)."""
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


class ExtremGraph:
    # ── ОПТИМИЗАЦИЯ: _w0_cache/_w1_cache — память walk_type0/walk_type1 на
    # уровне экземпляра графа (живёт до следующей перезагрузки из БД).
    # entry_level повторяется на многих барах → повтор отдаётся из кеша.
    # type=0 не зависит от var → 5 вызовов (var=0..4) = 1 вычисление + 4 хита.
    def __init__(self) -> None:
        self.nodes:     list[ExtremNode] = []
        self._by_value: dict[int, int]   = {}
        self._w0_cache: dict[int, float] = {}
        self._w1_cache: dict[tuple[int, int], float] = {}

    def add_node(self, value: int) -> int:
        if value in self._by_value:
            return self._by_value[value]
        node_id = len(self.nodes)
        self.nodes.append(ExtremNode(node_id, value))
        self._by_value[value] = node_id
        return node_id

    def get_node(self, node_id: int) -> Optional[ExtremNode]:
        return self.nodes[node_id] if 0 <= node_id < len(self.nodes) else None

    def get_node_by_value(self, value: int) -> Optional[ExtremNode]:
        nid = self._by_value.get(value)
        return self.nodes[nid] if nid is not None else None

    def find_nearest(self, value: int) -> Optional[ExtremNode]:
        if not self.nodes:
            return None
        return min(self.nodes, key=lambda n: abs(n.value - value))

    def __len__(self) -> int:
        return len(self.nodes)


# ══════════════════════════════════════════════════════════════════════════════
# Алгоритмы обхода (идентичны сервису 59, без изменений)
# ══════════════════════════════════════════════════════════════════════════════

def walk_type0(graph: ExtremGraph, start_id: int) -> float:
    """Жадный путь: следуем наибольшей вероятности до тупика или петли.
    Не зависит от var → кешируется по start_id (graph._w0_cache)."""
    cached = graph._w0_cache.get(start_id)
    if cached is not None:
        return cached

    path:    list[int]      = [start_id]
    visited: dict[int, int] = {start_id: 0}
    current_id = start_id

    while True:
        node = graph.get_node(current_id)
        if node is None:
            break

        next_id = node.best_next()
        if next_id is None:
            last_n = path[-2:] if len(path) >= 2 else path
            vals   = [graph.get_node(n).value for n in last_n if graph.get_node(n)]
            result = sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)
            graph._w0_cache[start_id] = result
            return result

        if next_id in visited:
            loop_nodes = path[visited[next_id]:]
            vals = [graph.get_node(n).value for n in loop_nodes if graph.get_node(n)]
            result = sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)
            graph._w0_cache[start_id] = result
            return result

        visited[next_id] = len(path)
        path.append(next_id)
        current_id = next_id

    last_n = path[-2:] if len(path) >= 2 else path
    vals   = [graph.get_node(n).value for n in last_n if graph.get_node(n)]
    result = sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)
    graph._w0_cache[start_id] = result
    return result


def walk_type1(graph: ExtremGraph, start_id: int, max_depth: int = 20) -> float:
    """Квантовый обход: sum(mean_ветки × P_нитки) по всем конечным ветвям.

    ── ОПТИМИЗАЦИЯ (результат идентичен оригиналу — проверено побитово) ─────
    Кеш по (start_id, max_depth) в graph._w1_cache + итеративный обход
    (явный стек, без копий path/path_set на каждой ветке, без пересчёта
    get_relations() на каждом шаге — см. ExtremNode.relations_list()).
    """
    key = (start_id, max_depth)
    cached = graph._w1_cache.get(key)
    if cached is not None:
        return cached

    total = 0.0
    node0 = graph.get_node(start_id)
    if node0 is None:
        graph._w1_cache[key] = total
        return total

    path:     list[int]      = [start_id]
    path_pos: dict[int, int] = {start_id: 0}
    stack: list[list] = [[node0, 0, 1.0, node0.relations_list(), 0]]

    while stack:
        frame = stack[-1]
        node, depth, prob, rels, idx = frame

        if idx == 0:
            if depth > max_depth or prob < 1e-9 or not rels:
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
        bp = prob * next_prob

        pos = path_pos.get(next_id)
        if pos is not None:
            loop_nodes = path[pos:]
            s = 0.0
            for nid in loop_nodes:
                s += graph.nodes[nid].value
            total += (s / len(loop_nodes)) * bp
            continue

        next_node = graph.nodes[next_id]
        path.append(next_id)
        path_pos[next_id] = len(path) - 1
        stack.append([next_node, depth + 1, bp, next_node.relations_list(), 0])

    graph._w1_cache[key] = total
    return total


# ══════════════════════════════════════════════════════════════════════════════
# Сигнал: без проверки типа экстремума (всё аналогично, но проще)
# ══════════════════════════════════════════════════════════════════════════════

def round_to_level(price: float, sig: int = SIG_DIGITS) -> int:
    if price <= 0:
        return 0
    magnitude = 10 ** (math.floor(math.log10(price)) - sig + 1)
    return int(price / magnitude)


def compute_bull_ratio(predicted_value: float, current_close: float) -> Optional[float]:
    """
    Отличие от сервиса 59: нет проверки типа последнего экстремума.
    Граф строится из всех свечей → нет контекста MAX/MIN.

    predicted > current_level → LONG  (bull_ratio > 0.5)
    predicted < current_level → SHORT (bull_ratio < 0.5)
    |deviation| < MIN_MODIFICATION → нет сигнала (шум)

    modification = min(|predicted - close_level| / close_level × SIGNAL_SCALE, 0.45)
    """
    close_level = float(round_to_level(current_close))
    if close_level <= 0:
        return None

    deviation    = abs(predicted_value - close_level) / close_level
    modification = min(deviation * SIGNAL_SCALE, 0.45)

    # Фильтр шума: слишком малая разница → не сигнализируем
    if modification < MIN_MODIFICATION:
        return None

    if predicted_value > close_level:
        return 0.5 + modification   # LONG
    else:
        return 0.5 - modification   # SHORT


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка из БД
# ══════════════════════════════════════════════════════════════════════════════

def _detect_pair_id(rates: list) -> int:
    if not rates:
        return 1
    c = float(rates[-1].get("close") or rates[-1].get("t1") or 0)
    if c > 10_000: return 3
    if c > 100:    return 4
    return 1


def _db_cfg() -> dict:
    return {
        "host":     os.getenv("DB_HOST",     "127.0.0.1"),
        "port":     int(os.getenv("DB_PORT", "3306")),
        "user":     os.getenv("DB_USER",     "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME",     "vlad"),
    }


def _load_graph_from_db(pair_id: int) -> ExtremGraph:
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
            fid = graph.add_node(int(r["from_level"]))
            tid = graph.add_node(int(r["to_level"]))
            graph.get_node(fid).add_relation(tid, int(r["transition_count"] or 1))
        cur.close(); conn.close()
        log.debug(f"[candle-graph] pair={pair_id} loaded {len(graph)} nodes")
    except Exception as e:
        log.warning(f"[candle-graph] _load_graph pair={pair_id}: {e}")
    return graph


def _get_graph(pair_id: int) -> ExtremGraph:
    now = time.time()
    if pair_id not in _GRAPH_CACHE or now - _GRAPH_TTL.get(pair_id, 0) > GRAPH_CACHE_TTL:
        _GRAPH_CACHE[pair_id] = _load_graph_from_db(pair_id)
        _GRAPH_TTL[pair_id]   = now
    return _GRAPH_CACHE[pair_id]


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
    Использует граф всех свечей для генерации сигнала.

    Отличие от сервиса 59:
    - Нет ожидания экстремума: каждая свеча может давать сигнал
    - Нет проверки типа экстремума (MAX/MIN): сигнал из сравнения predicted vs current
    - Фильтр шума: |modification| < MIN_MODIFICATION → {}

    Возвращает {"output": bull_ratio} или {} (шум / нет данных в графе).
    """
    if not rates:
        return {}

    pair_id = _detect_pair_id(rates)

    # ── Текущий уровень (entry point в граф) ──────────────────────────────
    try:
        current_close = float(rates[-1].get("close") or 0)
    except Exception:
        return {}

    if current_close <= 0:
        return {}

    entry_level = round_to_level(current_close)

    # ── Загружаем граф ─────────────────────────────────────────────────────
    graph = _get_graph(pair_id)
    if len(graph) == 0:
        return {}

    # ── Находим стартовый узел (или ближайший) ─────────────────────────────
    start_node = graph.get_node_by_value(entry_level)
    if start_node is None:
        start_node = graph.find_nearest(entry_level)
    if start_node is None:
        return {}

    # ── Обходим граф ──────────────────────────────────────────────────────
    max_depth = VAR_DEPTH.get(var, 20)
    if type == 0:
        predicted = walk_type0(graph, start_node.id)
    else:
        predicted = walk_type1(graph, start_node.id, max_depth=max_depth)

    # ── Вычисляем сигнал ───────────────────────────────────────────────────
    bull_ratio = compute_bull_ratio(predicted, current_close)
    if bull_ratio is None:
        return {}   # шум, разница слишком мала

    direction = "↑ LONG" if bull_ratio > 0.5 else "↓ SHORT"
    log.debug(
        f"[candle-graph] pair={pair_id} type={type} var={var} "
        f"entry={entry_level} predicted={predicted:.1f} → {direction} br={bull_ratio:.4f}"
    )
    return {OUTPUT_KEY: round(bull_ratio, 6)}
