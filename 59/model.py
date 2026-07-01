"""
model.py — brain-extremum-graph (сервис 59)  v2

Граф вероятностей переходов между экстремальными уровнями.
Структуры данных ExtremNode / ExtremGraph + 2 алгоритма обхода.

────────────────────────────────────────────────────────────────────
Общий алгоритм:
  1. Парсим экстремумы, округляем до 3 значащих цифр
  2. Загружаем граф переходов (vlad_extremum_graph_svc59)
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
GRAPH_CACHE_TTL = 3600.0   # обновляем граф раз в час

# Порядок подтверждения экстремума (должен совпадать с GRAPH_ORDER в context_idx.py)
EXTREMUM_ORDER = 5
# _LAST_SIGNAL_TS удалён: новый _detect_new_extremum детектирует подтверждённые
# экстремумы (order баров назад) — они уже естественно разделены avg_candles барами,
# поэтому глобальный state-фильтр избыточен и опасен при параллельном fill_cache.


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
    sig_digits:          int = SIG_DIGITS,
) -> Optional[float]:
    """
    Сравниваем предсказанный уровень с текущей ценой и типом экстремума.

    ВАЖНО: predicted_value — целочисленный уровень (e.g. 932 для BTC 93211).
    current_close — сырая цена из котировок (e.g. 93211).
    Перед сравнением current_close конвертируется в тот же целочисленный масштаб,
    используя sig_digits ЭТОЙ пары (тот же, которым был построен граф) —
    не модульную константу, иначе close_level не совпадёт с уровнями графа.

    modification = min(|predicted - close_level| / close_level × SIGNAL_SCALE, 0.45)
    """
    # Конвертируем close в тот же целочисленный масштаб что у узлов графа
    close_level = float(round_to_level(current_close, sig_digits))
    if close_level <= 0:
        return None

    diff = predicted_value - close_level
    if abs(diff) < 1e-9:
        return None   # predicted == current → нет информации (было бы 0.5 = шум)

    deviation    = abs(diff) / close_level
    modification = min(deviation * SIGNAL_SCALE, 0.45)
    if modification <= 0:
        return None

    # Сигнал = направление walk (граф обучен на реальных переходах)
    return 0.5 + modification if diff > 0 else 0.5 - modification


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка из БД
# ══════════════════════════════════════════════════════════════════════════════

def round_to_level(price: float, sig: int = SIG_DIGITS) -> int:
    if price <= 0:
        return 0
    magnitude = 10 ** (math.floor(math.log10(price)) - sig + 1)
    return int(price / magnitude)


def _detect_pair_id(rates: list, dataset_index: dict | None = None) -> int:
    """
    Определяет пару по rates_table из dataset_index (первичный источник),
    с фолбэком на цену (для backward-совместимости).
    Ценовой фолбэк ненадёжен на граничных значениях (ETH < $100 → EUR).
    """
    table = str((dataset_index or {}).get("rates_table") or "").lower()
    if table:
        if "btc" in table: return 3
        if "eth" in table: return 4
        if "eur" in table: return 1
    # Фолбэк по цене
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
    Загружает граф из vlad_extremum_graph_svc59 для данной пары.
    Строит ExtremGraph с реальными transition_count.
    """
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
            from_id = graph.add_node(int(r["from_level"]))
            to_id   = graph.add_node(int(r["to_level"]))
            graph.get_node(from_id).add_relation(to_id, int(r["transition_count"] or 1))
        cur.close(); conn.close()
        log.debug(f"[graph] pair={pair_id} loaded {len(graph)} nodes")
    except Exception as e:
        log.warning(f"[graph] _load_graph_from_db pair={pair_id}: {e}")
    return graph


def _get_graph(pair_id: int) -> ExtremGraph:
    """Возвращает граф из кеша или перезагружает из БД."""
    now = time.time()
    if pair_id not in _GRAPH_CACHE or now - _GRAPH_TTL.get(pair_id, 0) > GRAPH_CACHE_TTL:
        _GRAPH_CACHE[pair_id] = _load_graph_from_db(pair_id)
        _GRAPH_TTL[pair_id]   = now
    return _GRAPH_CACHE[pair_id]


# ── sig_digits per pair (round_to_level granularity) ────────────────────────────
# КРИТИЧНО: SIG_DIGITS=3 — общий дефолт для всех пар, но он не обязательно
# оптимален для каждой. context_idx.py подбирает per-pair sig_digits через
# walk-forward валидацию на исторических данных (см. _choose_sig_digits там)
# и сохраняет в STATS_TABLE. Раньше model.py НИКОГДА не читал это значение —
# граф мог строиться с одним sig, а live-детекция всегда использовала
# хардкод SIG_DIGITS=3, независимо от того что выбрал context_idx. Теперь
# модель читает то же значение, что и было использовано при построении графа.
_SIG_CACHE: dict[int, int] = {}
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
        log.warning(f"[graph] _load_sig_digits_from_db pair={pair_id}: {e}")
    return SIG_DIGITS


def _get_sig_digits(pair_id: int) -> int:
    """Возвращает sig_digits для пары из кеша или перезагружает из БД."""
    now = time.time()
    if pair_id not in _SIG_CACHE or now - _SIG_TTL.get(pair_id, 0) > SIG_CACHE_TTL:
        _SIG_CACHE[pair_id] = _load_sig_digits_from_db(pair_id)
        _SIG_TTL[pair_id]    = now
    return _SIG_CACHE[pair_id]


# ══════════════════════════════════════════════════════════════════════════════
# Определение текущего экстремума
# ══════════════════════════════════════════════════════════════════════════════

def _detect_new_extremum(
    closes: np.ndarray,
    highs:  np.ndarray,
    lows:   np.ndarray,
    sig_digits: int = SIG_DIGITS,
) -> Optional[dict]:
    """
    Ищет подтверждённый экстремум на баре EXTREMUM_ORDER позиций назад.

    Вариант A: полностью согласован с context_idx.py.
    context_idx.py теперь использует ВСЕ confirmed argrelextrema(order=5)
    без дополнительной фильтрации подряд идущих одного направления.
    model.py детектирует тем же критерием: 0% несогласованных сигналов.

    Ранее context_idx.py удалял "менее выраженный" из пары подряд идущих
    MAX/MAX или MIN/MIN, но live-детектор не мог реплицировать это без
    look-ahead => 12-13% сигналов не попадали в граф. Фильтр убран везде.

    sig_digits: то же значение, с которым context_idx.py строил граф для
    ЭТОЙ пары (не модульная константа SIG_DIGITS — она лишь дефолт-фолбэк).
    Подбирается адаптивно per-pair, см. _choose_sig_digits в context_idx.py.

    Возвращает {'level': int, 'direction': +1/-1, 'price': float} или None.
    """
    order = EXTREMUM_ORDER
    n     = len(highs)
    if n < 2 * order + 2:
        return None

    abs_cand = n - order - 1

    cand_h = float(highs[abs_cand])
    cand_l = float(lows[abs_cand])

    # Валидация OHLC: нулевые или отрицательные значения → битые данные
    if cand_h <= 0 or cand_l <= 0:
        return None

    left_h  = highs[abs_cand - order : abs_cand]
    left_l  = lows[abs_cand - order : abs_cand]
    right_h = highs[abs_cand + 1 : abs_cand + order + 1]
    right_l = lows[abs_cand + 1 : abs_cand + order + 1]

    if len(left_h) < order or len(right_h) < order:
        return None

    # Защита от нулей в окне (битые бары в середине ряда)
    if float(np.min(left_h)) <= 0 or float(np.min(right_h)) <= 0:
        return None
    if float(np.min(left_l)) <= 0 or float(np.min(right_l)) <= 0:
        return None

    # Строгое неравенство — идентично argrelextrema(np.greater/np.less, order=5)
    is_max = (cand_h > float(np.max(left_h)) and cand_h > float(np.max(right_h)))
    is_min = (cand_l < float(np.min(left_l)) and cand_l < float(np.min(right_l)))

    if not is_max and not is_min:
        return None

    # Outside-bar guard: бар одновременно MAX и MIN — убираем неоднозначность
    if is_max and is_min:
        return None

    price = cand_h if is_max else cand_l
    level = round_to_level(price, sig_digits)
    if level <= 0:
        return None   # round_to_level вернул 0 из-за некорректной цены
    return {
        "level":     level,
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

    pair_id = _detect_pair_id(rates, dataset_index)
    sig_digits = _get_sig_digits(pair_id)

    # ── Котировки (последние N баров, достаточно для confirmed extremum detect) ──
    # Нужно минимум 2*EXTREMUM_ORDER + 2 = 12 баров; берём 100 для надёжности.
    # _LAST_SIGNAL_TS удалён: подтверждённые экстремумы (order=5) уже
    # разделены ~avg_candles барами естественно и не требуют дополнительного state.
    tail   = rates[-100:]
    closes = np.array([float(x.get("close") or 0) for x in tail], dtype=np.float64)
    highs  = np.array([float(x.get("max")   or 0) for x in tail], dtype=np.float64)
    lows   = np.array([float(x.get("min")   or 0) for x in tail], dtype=np.float64)

    # ── Детектируем новый экстремум ────────────────────────────────────────
    ext = _detect_new_extremum(closes, highs, lows, sig_digits)
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
    bull_ratio    = compute_bull_ratio(predicted, current_close, ext["direction"], sig_digits)

    if bull_ratio is None:
        return {}   # противоречивый сигнал

    direction_str = "↑ LONG" if bull_ratio > 0.5 else "↓ SHORT"
    log.info(
        f"[graph] pair={pair_id} type={type} var={var} sig={sig_digits} "
        f"level={ext['level']} predicted={predicted:.1f} "
        f"close={current_close:.4f} → {direction_str} br={bull_ratio:.4f}"
    )
    return {OUTPUT_KEY: round(bull_ratio, 6)}
