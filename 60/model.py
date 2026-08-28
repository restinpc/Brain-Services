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
  5. Возвращаем {"output": score}, где знак задаёт направление

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
from bisect import bisect_left
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

# Level-change filter: сигнал только при смене rounded level
# EUR: avg 13.6h на одном уровне → без фильтра 93% сигналов — дубли
# BTC: avg 1.7h → без фильтра 49% дублей. Фикс acc: EUR 48.7%→55%, BTC 52.1%→53.3%


# ══════════════════════════════════════════════════════════════════════════════
# Структуры данных (идентичны сервису 59)
# ══════════════════════════════════════════════════════════════════════════════

class ExtremNode:
    """
    Узел графа экстремумов.

    Оптимизация:
      * вероятности переходов кешируются после первого расчёта;
      * лучший переход для type=0 кешируется отдельно;
      * __slots__ уменьшает расход памяти при большом числе узлов.
    """

    __slots__ = ("id", "value", "relations", "_relations_prob", "_best_next")

    def __init__(self, node_id: int, value: int) -> None:
        self.id: int = node_id
        self.value: int = value
        self.relations: dict[int, int] = {}   # {node_id: count}
        self._relations_prob: Optional[dict[int, float]] = None
        self._best_next: Optional[int] = None

    def add_relation(self, node_id: int, count: int = 1) -> None:
        """Добавляет (или увеличивает) счётчик перехода к node_id."""
        self.relations[node_id] = self.relations.get(node_id, 0) + count
        # После изменения сырых transition_count кеши должны быть сброшены.
        self._relations_prob = None
        self._best_next = None

    def get_relation(self, node_id: int) -> int:
        """Возвращает сырое кол-во переходов к node_id (0 если нет)."""
        return self.relations.get(node_id, 0)

    def get_relations(self) -> dict[int, float]:
        """
        Возвращает нормализованные вероятности переходов.
        {node_id: probability}  sum = 1.0

        В старой версии этот словарь пересоздавался при каждом шаге обхода.
        При массовом fill_cache это лишняя работа, поэтому теперь результат
        кешируется внутри узла до следующего add_relation().
        """
        cached = self._relations_prob
        if cached is not None:
            return cached

        relations = self.relations
        total = sum(relations.values())
        if total == 0:
            self._relations_prob = {}
        else:
            inv_total = 1.0 / total
            self._relations_prob = {nid: cnt * inv_total for nid, cnt in relations.items()}
        return self._relations_prob

    def get_best_next(self) -> Optional[int]:
        """
        Лучший следующий узел для type=0.

        Для выбора максимальной вероятности не нужно нормализовать relations:
        max(count / total) даёт тот же node_id, что и max(count).
        """
        if self._best_next is not None:
            return self._best_next
        if not self.relations:
            return None
        self._best_next = max(self.relations.items(), key=lambda item: item[1])[0]
        return self._best_next

    def __repr__(self) -> str:
        return f"ExtremNode(id={self.id}, value={self.value}, rels={len(self.relations)})"


class ExtremGraph:
    """
    Граф переходов между уровнями экстремумов.

    Оптимизация:
      * find_nearest теперь O(log N), а не O(N), за счёт отсортированного индекса;
      * ближайшие уровни кешируются, что помогает при повторяющихся live/cache вызовах.
    """

    __slots__ = ("nodes", "_by_value", "_sorted_values", "_nearest_cache")

    def __init__(self) -> None:
        self.nodes: list[ExtremNode] = []
        self._by_value: dict[int, int] = {}   # value → node_id
        self._sorted_values: Optional[list[int]] = None
        self._nearest_cache: dict[int, int] = {}

    def add_node(self, value: int) -> int:
        """
        Добавляет узел с данным value. Если уже существует — возвращает id.
        Возвращает node_id.
        """
        existing = self._by_value.get(value)
        if existing is not None:
            return existing

        node_id = len(self.nodes)
        node = ExtremNode(node_id, value)
        self.nodes.append(node)
        self._by_value[value] = node_id
        self._sorted_values = None
        self._nearest_cache.clear()
        return node_id

    def finalize(self) -> None:
        """
        Подготавливает быстрые индексы после массовой загрузки из БД.
        Вызывать не обязательно: find_nearest построит индекс лениво.
        """
        self._sorted_values = sorted(self._by_value)
        self._nearest_cache.clear()

    def get_node(self, node_id: int) -> Optional[ExtremNode]:
        """Возвращает узел по его id или None."""
        nodes = self.nodes
        if 0 <= node_id < len(nodes):
            return nodes[node_id]
        return None

    def get_node_by_value(self, value: int) -> Optional[ExtremNode]:
        """Возвращает узел по значению уровня или None."""
        nid = self._by_value.get(value)
        return self.nodes[nid] if nid is not None else None

    def find_nearest(self, value: int) -> Optional[ExtremNode]:
        """Возвращает узел с ближайшим value (для уровней не в графе)."""
        nodes = self.nodes
        if not nodes:
            return None

        exact = self._by_value.get(value)
        if exact is not None:
            return nodes[exact]

        cached_id = self._nearest_cache.get(value)
        if cached_id is not None:
            return nodes[cached_id]

        values = self._sorted_values
        if values is None:
            values = sorted(self._by_value)
            self._sorted_values = values

        pos = bisect_left(values, value)
        if pos <= 0:
            nearest_value = values[0]
        elif pos >= len(values):
            nearest_value = values[-1]
        else:
            left_value = values[pos - 1]
            right_value = values[pos]
            left_dist = abs(value - left_value)
            right_dist = abs(right_value - value)
            if left_dist < right_dist:
                nearest_value = left_value
            elif right_dist < left_dist:
                nearest_value = right_value
            else:
                # При равной дистанции сохраняем поведение min(self.nodes, ...):
                # выбираем тот узел, который был добавлен раньше.
                left_id = self._by_value[left_value]
                right_id = self._by_value[right_value]
                nearest_value = left_value if left_id <= right_id else right_value

        nearest_id = self._by_value[nearest_value]
        self._nearest_cache[value] = nearest_id
        return nodes[nearest_id]

    def __len__(self) -> int:
        return len(self.nodes)

    def __repr__(self) -> str:
        return f"ExtremGraph({len(self.nodes)} nodes)"


# ══════════════════════════════════════════════════════════════════════════════
# Алгоритм type=0: Жадный путь (1 нитка)
# ══════════════════════════════════════════════════════════════════════════════

def walk_type0(graph: ExtremGraph, start_id: int) -> float:
    """
    Жадный путь: идём по самому частому переходу.

    Оптимизация относительно старой версии:
      * не строим normalized probabilities на каждом шаге;
      * не вызываем graph.get_node() по 2 раза на один и тот же id;
      * значения узлов берём напрямую из graph.nodes.
    """
    nodes = graph.nodes
    nodes_len = len(nodes)
    if not (0 <= start_id < nodes_len):
        return 0.0

    path: list[int] = [start_id]
    visited: dict[int, int] = {start_id: 0}   # node_id → позиция в path
    current_id = start_id

    while True:
        if not (0 <= current_id < nodes_len):
            return float(nodes[start_id].value)

        node = nodes[current_id]
        next_id = node.get_best_next()

        if next_id is None:
            # Тупик: mean последних 2 узлов.
            if len(path) >= 2:
                return (nodes[path[-1]].value + nodes[path[-2]].value) * 0.5
            return float(nodes[start_id].value)

        loop_start = visited.get(next_id)
        if loop_start is not None:
            # Петля: mean всех узлов в цикле.
            loop_nodes = path[loop_start:]
            return sum(nodes[nid].value for nid in loop_nodes) / len(loop_nodes)

        visited[next_id] = len(path)
        path.append(next_id)
        current_id = next_id


# ══════════════════════════════════════════════════════════════════════════════
# Алгоритм type=1: Квантовый обход (дерево)
# ══════════════════════════════════════════════════════════════════════════════

def walk_type1(graph: ExtremGraph, start_id: int, max_depth: int = 20) -> float:
    """
    Квантовый обход дерева: взвешенная сумма всех конечных ветвей.

    Оптимизация относительно старой версии:
      * path и visited/positions ведутся in-place через append/pop;
      * нет копирования path + [next_id] и frozenset | {next_id} на каждой ветке;
      * loop_start берётся из dict за O(1), без path.index();
      * вероятности relations кешируются внутри ExtremNode.
    """
    nodes = graph.nodes
    nodes_len = len(nodes)
    if not (0 <= start_id < nodes_len):
        return 0.0

    total_signal = 0.0
    path: list[int] = [start_id]
    pos_by_node: dict[int, int] = {start_id: 0}

    def terminal(prob: float) -> None:
        """Добавляет сигнал конечной ветви к общей сумме."""
        nonlocal total_signal
        if not path:
            return
        if len(path) >= 2:
            mean_val = (nodes[path[-1]].value + nodes[path[-2]].value) * 0.5
        else:
            mean_val = float(nodes[path[-1]].value)
        total_signal += mean_val * prob

    def recurse(node_id: int, prob: float, depth: int) -> None:
        nonlocal total_signal

        if depth > max_depth or prob < 1e-9:
            terminal(prob)
            return

        if not (0 <= node_id < nodes_len):
            terminal(prob)
            return

        rels = nodes[node_id].get_relations()
        if not rels:
            terminal(prob)
            return

        for next_id, next_prob in rels.items():
            branch_prob = prob * next_prob
            loop_start = pos_by_node.get(next_id)

            if loop_start is not None:
                # Петля: mean всех узлов цикла, без повторного включения next_id.
                loop_nodes = path[loop_start:]
                if loop_nodes:
                    total_signal += (
                        sum(nodes[nid].value for nid in loop_nodes) / len(loop_nodes)
                    ) * branch_prob
                continue

            # В нормальном графе next_id всегда валиден: он создан через add_node().
            # Проверку всё равно оставляем, чтобы битая relation не роняла сервис.
            if not (0 <= next_id < nodes_len):
                # Поведение максимально близко к старой версии: она добавляла
                # invalid id в path, затем terminal() отбрасывал invalid-узел и
                # фактически усреднял только текущий валидный узел.
                total_signal += float(nodes[path[-1]].value) * branch_prob
                continue

            pos_by_node[next_id] = len(path)
            path.append(next_id)
            recurse(next_id, branch_prob, depth + 1)
            path.pop()
            del pos_by_node[next_id]

    recurse(start_id, 1.0, 0)
    return total_signal


# ══════════════════════════════════════════════════════════════════════════════
# Преобразование предсказанного значения → bull_ratio
# ══════════════════════════════════════════════════════════════════════════════

def round_to_level(price: float, sig: int = SIG_DIGITS) -> int:
    """
    Округляет цену до целочисленного уровня графа, МОНОТОННОГО ГЛОБАЛЬНО —
    без разрыва на границах степеней десяти.

    Кодирование: level = exp * bucket_size + (mantissa - lo)
      exp      = floor(log10(price))              — порядок величины
      mantissa = int(price / 10^(exp-sig+1))       — первые `sig` значащих
                 цифр, диапазон [lo, hi) = [10^(sig-1), 10^sig)
      bucket_size = hi - lo                        — сдвигаем mantissa на lo,
                 чтобы соседние декады стыковались БЕЗ ЗАЗОРА

    БАГ СТАРОЙ ВЕРСИИ (level = mantissa напрямую, без exp): на границе
    декады уровень проваливался, а не рос:
      BTC $99,999  -> level 99   (sig=2)
      BTC $100,000 -> level 10   (sig=2)   ← падение на -89!
    Это разрывало граф свечей на несвязные кластеры (продакшн-инцидент
    2026-07-01: from_level для BTC содержал изолированные {10,11,12} и
    {58..97} без единого ребра между ними).

    Новая кодировка внутри ОДНОЙ декады даёт ТЕ ЖЕ относительные расстояния
    что и раньше — вся ранее проведённая калибровка sig_digits через
    backtest остаётся методологически применимой.
    """
    if not math.isfinite(price) or price <= 0:
        return 0
    exp = math.floor(math.log10(price))
    lo, hi = 10 ** (sig - 1), 10 ** sig
    magnitude = 10 ** (exp - sig + 1)
    mantissa  = int(price / magnitude)
    # Клэмп: log10 вблизи ТОЧНОЙ степени десяти может дать exp на 1 не в ту
    # сторону из-за погрешности float.
    if mantissa >= hi:
        exp += 1
        magnitude = 10 ** (exp - sig + 1)
        mantissa  = int(price / magnitude)
    elif mantissa < lo:
        exp -= 1
        magnitude = 10 ** (exp - sig + 1)
        mantissa  = int(price / magnitude)
    bucket_size = hi - lo
    return exp * bucket_size + (mantissa - lo)


def level_to_price(level: float, sig: int = SIG_DIGITS) -> float:
    """
    Обратное преобразование level -> price (приближённое).
    Нужно, чтобы compute_bull_ratio считал величину отклонения через
    РЕАЛЬНУЮ цену, а не через разность целочисленных уровней (которая
    перестала быть напрямую пропорциональна цене после введения
    exp-смещения — это и есть цена за устранение разрыва на границе декады).
    Поддерживает дробный level (после mean() в walk_type0 при обработке циклов).
    """
    lo, hi = 10 ** (sig - 1), 10 ** sig
    bucket_size = hi - lo
    exp = math.floor(level / bucket_size)
    mantissa = (level - exp * bucket_size) + lo
    magnitude = 10 ** (exp - sig + 1)
    return mantissa * magnitude


def compute_bull_ratio(
    predicted_value: float,
    current_close:   float,
    sig_digits:       int = SIG_DIGITS,
) -> Optional[float]:
    """
    Отличие от сервиса 59: нет проверки типа последнего экстремума.
    Граф строится из всех свечей → нет контекста MAX/MIN.

    Сравниваем предсказанный уровень с текущей ценой через РЕАЛЬНУЮ цену
    (не через разность целочисленных уровней) — decode predicted_value
    обратно в цену через level_to_price и сравниваем НАПРЯМУЮ с
    current_close. Это устраняет зависимость знака и величины сигнала
    от особенностей кодирования уровней.

    predicted_price > current_close → LONG  (bull_ratio > 0.5)
    predicted_price < current_close → SHORT (bull_ratio < 0.5)
    |deviation| < MIN_MODIFICATION → нет сигнала (шум)

    sig_digits: то же значение, с которым context_idx.py строил граф для
    ЭТОЙ пары (не модульная константа SIG_DIGITS — она лишь дефолт-фолбэк).

    modification = min(|predicted_price - current_close| / current_close × SIGNAL_SCALE, 0.45)
    """
    if not math.isfinite(current_close) or current_close <= 0:
        return None
    if not math.isfinite(predicted_value):
        return None

    predicted_price = level_to_price(predicted_value, sig_digits)
    if not math.isfinite(predicted_price) or predicted_price <= 0:
        return None

    deviation    = abs(predicted_price - current_close) / current_close
    modification = min(deviation * SIGNAL_SCALE, 0.45)

    # Фильтр шума: слишком малая разница → не сигнализируем
    if modification < MIN_MODIFICATION:
        return None

    if predicted_price > current_close:
        return 0.5 + modification   # LONG
    else:
        return 0.5 - modification   # SHORT


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка из БД
# ══════════════════════════════════════════════════════════════════════════════

def _detect_pair_id(rates: list, dataset_index: dict | None = None) -> int:
    """
    Определяет пару по rates_table из dataset_index (первичный источник),
    с фолбэком на цену. Ценовой фолбэк ненадёжен: ETH < $100 → pair=1 (EUR).
    """
    table = str((dataset_index or {}).get("rates_table") or "").lower()
    if table:
        if "btc" in table: return 3
        if "eth" in table: return 4
        if "eur" in table: return 1
    if not rates:
        return 1
    try:
        last = rates[-1] if isinstance(rates[-1], dict) else {}
        c = float(last.get("close") or last.get("t1") or 0)
    except (TypeError, ValueError, AttributeError):
        return 1
    if not math.isfinite(c) or c <= 0:
        return 1
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
    try:
        import mysql.connector
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


# ── sig_digits per pair (round_to_level granularity) ────────────────────────────
# SIG_DIGITS=3 — общий дефолт. context_idx.py подбирает per-pair sig_digits
# через walk-forward валидацию на исторических данных (см. _choose_sig_digits
# там) и сохраняет в STATS_TABLE. Раньше model.py никогда не читал это значение —
# round_to_level всегда использовал хардкод SIG_DIGITS=3 независимо от того,
# с каким sig был построен граф. Теперь модель читает то же значение.
def invalidate_pair_cache(pair_id: int | None = None) -> None:
    """Сбрасывает кеш графа и точности после публикации нового индекса."""
    if pair_id is None:
        _GRAPH_CACHE.clear()
        _GRAPH_TTL.clear()
        _SIG_CACHE.clear()
        _SIG_TTL.clear()
        return
    _GRAPH_CACHE.pop(pair_id, None)
    _GRAPH_TTL.pop(pair_id, None)
    _SIG_CACHE.pop(pair_id, None)
    _SIG_TTL.pop(pair_id, None)


_SIG_CACHE: dict[int, int]   = {}
_SIG_TTL:   dict[int, float] = {}
SIG_CACHE_TTL = 3600.0   # тот же TTL что у графа — синхронно обновляются


def _load_sig_digits_from_db(pair_id: int) -> int:
    """Читает sig_digits для пары из STATS_TABLE. Фолбэк — модульная константа."""
    try:
        import mysql.connector
        conn = mysql.connector.connect(**_db_cfg())
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT sig_digits FROM `{STATS_TABLE}` WHERE pair = %s",
            (pair_id,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row.get("sig_digits"):
            return int(row["sig_digits"])
    except Exception as e:
        log.warning(f"[candle-graph] _load_sig_digits_from_db pair={pair_id}: {e}")
    return SIG_DIGITS


def _get_sig_digits(pair_id: int) -> int:
    """Возвращает sig_digits для пары из кеша или перезагружает из БД."""
    now = time.time()
    if pair_id not in _SIG_CACHE or now - _SIG_TTL.get(pair_id, 0) > SIG_CACHE_TTL:
        _SIG_CACHE[pair_id] = _load_sig_digits_from_db(pair_id)
        _SIG_TTL[pair_id]    = now
    return _SIG_CACHE[pair_id]


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

    Возвращает {"output": score} или {}, где score > 0 — LONG, score < 0 — SHORT.
    """
    if not rates:
        return {}
    if type not in (0, 1):
        log.warning(f"[candle-graph] unsupported type={type}; expected 0 or 1")
        return {}
    if type == 1 and var not in VAR_DEPTH:
        log.warning(f"[candle-graph] unsupported var={var}; expected one of {sorted(VAR_DEPTH)}")
        return {}
    # При единственном баре level-change filter (rates[-2]) недоступен,
    # а сигнал без сравнения уровней не имеет смысла.
    if len(rates) < 2:
        return {}

    pair_id = _detect_pair_id(rates, dataset_index)
    sig_digits = _get_sig_digits(pair_id)

    # ── Текущий уровень (entry point в граф) ──────────────────────────────
    try:
        current_close = float(rates[-1].get("close") or 0)
    except Exception:
        return {}

    if not math.isfinite(current_close) or current_close <= 0:
        return {}

    entry_level = round_to_level(current_close, sig_digits)
    if entry_level <= 0:
        return {}

    # ── Level-change filter (stateless через rates[-2]) ──────────────────────
    # Сигнал только когда rounded level ИЗМЕНИЛСЯ с предыдущего бара.
    # Stateless: сравниваем с предыдущим баром из rates, не из глобального dict.
    # Это безопасно при параллельном fill_cache и рестартах сервиса.
    # round_to_level использует sig_digits ЭТОЙ пары (тот же, которым context_idx.py
    # построил граф) — иначе "уровень не изменился" мог бы определяться иначе,
    # чем в графе, и фильтр давал бы несогласованный с графом результат.
    if len(rates) >= 2:
        try:
            prev_close = float(rates[-2].get("close") or 0)
        except (TypeError, ValueError, AttributeError):
            return {}
        if not math.isfinite(prev_close) or prev_close <= 0:
            return {}
        if round_to_level(prev_close, sig_digits) == entry_level:
            return {}   # уровень не изменился — пропускаем


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
    if type == 0:
        predicted = walk_type0(graph, start_node.id)
    else:  # type == 1 validated above
        predicted = walk_type1(graph, start_node.id, max_depth=VAR_DEPTH[var])

    # ── Вычисляем сигнал ───────────────────────────────────────────────────
    bull_ratio = compute_bull_ratio(predicted, current_close, sig_digits)
    if bull_ratio is None:
        return {}   # шум, разница слишком мала

    # Brain Framework/PHP ожидает знаковое значение:
    #   output > 0 → LONG, output < 0 → SHORT, output = 0/{} → нет сигнала.
    # Поэтому внутренний bull_ratio переводим в signed score относительно 0.5.
    score = bull_ratio - 0.5
    if not math.isfinite(score) or abs(score) < 1e-9:
        return {}

    direction = "↑ LONG" if score > 0 else "↓ SHORT"
    log.debug(
        f"[candle-graph] pair={pair_id} type={type} var={var} sig={sig_digits} "
        f"entry={entry_level} predicted={predicted:.1f} → {direction} "
        f"br={bull_ratio:.4f} score={score:.6f}"
    )
    return {OUTPUT_KEY: round(score, 6)}
