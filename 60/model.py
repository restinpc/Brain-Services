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

# Level-change filter: сигнал только при смене rounded level
# EUR: avg 13.6h на одном уровне → без фильтра 93% сигналов — дубли
# BTC: avg 1.7h → без фильтра 49% дублей. Фикс acc: EUR 48.7%→55%, BTC 52.1%→53.3%


# ══════════════════════════════════════════════════════════════════════════════
# Структуры данных (идентичны сервису 59)
# ══════════════════════════════════════════════════════════════════════════════

class ExtremNode:
    def __init__(self, node_id: int, value: int) -> None:
        self.id:        int = node_id
        self.value:     int = value
        self.relations: dict[int, int] = {}

    def add_relation(self, node_id: int, count: int = 1) -> None:
        self.relations[node_id] = self.relations.get(node_id, 0) + count

    def get_relation(self, node_id: int) -> int:
        return self.relations.get(node_id, 0)

    def get_relations(self) -> dict[int, float]:
        total = sum(self.relations.values())
        if total == 0:
            return {}
        return {nid: cnt / total for nid, cnt in self.relations.items()}


class ExtremGraph:
    def __init__(self) -> None:
        self.nodes:     list[ExtremNode] = []
        self._by_value: dict[int, int]   = {}

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
    """Жадный путь: следуем наибольшей вероятности до тупика или петли."""
    path:    list[int]     = [start_id]
    visited: dict[int, int] = {start_id: 0}
    current_id = start_id

    while True:
        node = graph.get_node(current_id)
        if node is None:
            break

        rels = node.get_relations()
        if not rels:
            last_n = path[-2:] if len(path) >= 2 else path
            vals   = [graph.get_node(n).value for n in last_n if graph.get_node(n)]
            return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)

        next_id = max(rels, key=rels.get)

        if next_id in visited:
            loop_nodes = path[visited[next_id]:]
            vals = [graph.get_node(n).value for n in loop_nodes if graph.get_node(n)]
            return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)

        visited[next_id] = len(path)
        path.append(next_id)
        current_id = next_id

    last_n = path[-2:] if len(path) >= 2 else path
    vals   = [graph.get_node(n).value for n in last_n if graph.get_node(n)]
    return sum(vals) / len(vals) if vals else float(graph.get_node(start_id).value)


def walk_type1(graph: ExtremGraph, start_id: int, max_depth: int = 20) -> float:
    """Квантовый обход: sum(mean_ветки × P_нитки) по всем конечным ветвям."""
    total = [0.0]

    def _terminal(path: list[int], prob: float) -> None:
        last_n = path[-2:] if len(path) >= 2 else path
        vals   = [graph.get_node(n).value for n in last_n if graph.get_node(n)]
        if vals:
            total[0] += (sum(vals) / len(vals)) * prob

    def _recurse(node_id: int, path: list[int],
                 path_set: frozenset, prob: float, depth: int) -> None:
        if depth > max_depth or prob < 1e-9:
            _terminal(path, prob)
            return
        node = graph.get_node(node_id)
        if node is None:
            _terminal(path, prob)
            return
        rels = node.get_relations()
        if not rels:
            _terminal(path, prob)
            return

        for next_id, next_prob in rels.items():
            bp = prob * next_prob
            if next_id in path_set:
                loop_nodes = path[path.index(next_id):]  # без дубля начала цикла
                vals = [graph.get_node(n).value for n in loop_nodes if graph.get_node(n)]
                if vals:
                    total[0] += (sum(vals) / len(vals)) * bp
            else:
                _recurse(next_id, path + [next_id], path_set | {next_id}, bp, depth + 1)

    _recurse(start_id, [start_id], frozenset([start_id]), 1.0, 0)
    return total[0]


# ══════════════════════════════════════════════════════════════════════════════
# Сигнал: без проверки типа экстремума (всё аналогично, но проще)
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
    if price <= 0:
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
    if current_close <= 0:
        return None

    predicted_price = level_to_price(predicted_value, sig_digits)
    if predicted_price <= 0:
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

    Возвращает {"output": bull_ratio} или {} (шум / нет данных в графе).
    """
    if not rates:
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

    if current_close <= 0:
        return {}

    entry_level = round_to_level(current_close, sig_digits)

    # ── Level-change filter (stateless через rates[-2]) ──────────────────────
    # Сигнал только когда rounded level ИЗМЕНИЛСЯ с предыдущего бара.
    # Stateless: сравниваем с предыдущим баром из rates, не из глобального dict.
    # Это безопасно при параллельном fill_cache и рестартах сервиса.
    # round_to_level использует sig_digits ЭТОЙ пары (тот же, которым context_idx.py
    # построил граф) — иначе "уровень не изменился" мог бы определяться иначе,
    # чем в графе, и фильтр давал бы несогласованный с графом результат.
    if len(rates) >= 2:
        prev_close = float(rates[-2].get("close") or 0)
        if prev_close > 0 and round_to_level(prev_close, sig_digits) == entry_level:
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
    max_depth = VAR_DEPTH.get(var, 20)
    if type == 0:
        predicted = walk_type0(graph, start_node.id)
    else:
        predicted = walk_type1(graph, start_node.id, max_depth=max_depth)

    # ── Вычисляем сигнал ───────────────────────────────────────────────────
    bull_ratio = compute_bull_ratio(predicted, current_close, sig_digits)
    if bull_ratio is None:
        return {}   # шум, разница слишком мала

    # Brain Framework/PHP ожидает знаковое значение:
    #   output > 0 → LONG, output < 0 → SHORT, output = 0/{} → нет сигнала.
    # Поэтому внутренний bull_ratio переводим в signed score относительно 0.5.
    score = bull_ratio - 0.5
    if abs(score) < 1e-9:
        return {}

    direction = "↑ LONG" if score > 0 else "↓ SHORT"
    log.debug(
        f"[candle-graph] pair={pair_id} type={type} var={var} sig={sig_digits} "
        f"entry={entry_level} predicted={predicted:.1f} → {direction} "
        f"br={bull_ratio:.4f} score={score:.6f}"
    )
    return {OUTPUT_KEY: round(score, 6)}
