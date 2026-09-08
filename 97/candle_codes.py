"""
candle_codes.py — индекс бинарных кодов направления свечек.

Код паттерна читается от текущей свечки в прошлое: первый разряд — последняя
закрытая свечка, второй — предыдущая, и так далее. Рост тела даёт 0, падение —
1, поэтому у каждой даты получается ровно 24 вложенных кода (0, 01, 011, 0110,
...), где код длины L — префикс кода длины L+1.

Значение кода — сумма T1 тех свечек, которые в истории следовали за таким же
паттерном. Прямой поиск стоил бы O(N) на каждый из 24 кодов, то есть O(N²) на
/fill_cache по всей истории. Поэтому история индексируется один раз на таблицу
котировок: для каждой длины все окна сортируются по коду (внутри кода — по
позиции), а суммы T1 следующих свечек сворачиваются в префиксные суммы. После
этого значение кода — два бинарных поиска и одно вычитание.

Индекс строится на полном массиве np_rates, а будущее отсекается уже на
запросе: учитываются только паттерны, чья следующая свечка закрылась не позже
запрошенной даты.
"""

from __future__ import annotations

import threading
from datetime import datetime

import numpy as np

# Ограничение из постановки: длиннее 24 свечек паттерны не строим.
MAX_PATTERN_LENGTH = 24

_HOUR_SECONDS = 3600
_DAY_SECONDS = 86400

# Коды (до 2^24) и позиции хранятся в int32 ради памяти индекса. Ключи поиска
# обязаны быть скалярами того же типа: от python int numpy приводит к общему
# типу весь массив, и бинарный поиск начинает стоить как полная копия.
_CODE_DTYPE = np.int32
_POSITION_DTYPE = np.int32

# По одному индексу на таблицу котировок: 3 часовых + 3 дневных.
_CACHE_LIMIT = 6
_CACHE: dict[tuple, "CodeHistory"] = {}
_CACHE_LOCK = threading.Lock()


class _Level:
    """Окна одной длины, отсортированные по коду и позиции.

    ``codes`` и ``anchors`` — параллельные массивы: anchors[k] — индекс
    последней свечки окна, codes[k] — его код. ``cumsum`` — префиксные суммы
    T1 свечек, следующих за этими окнами (cumsum[0] = 0).
    """

    __slots__ = ("codes", "anchors", "cumsum")

    def __init__(self, codes: np.ndarray, anchors: np.ndarray, cumsum: np.ndarray):
        self.codes = codes
        self.anchors = anchors
        self.cumsum = cumsum

    def sum_after(self, code: int, last_anchor: int) -> float:
        """Сумма T1 следующих свечек по всем окнам с кодом code до last_anchor."""
        target = _CODE_DTYPE(code)
        lo = int(self.codes.searchsorted(target, "left"))
        hi = int(self.codes.searchsorted(target, "right"))
        if lo >= hi:
            return 0.0
        # Внутри группы позиции возрастают, поэтому отсечь будущее можно
        # бинарным поиском по срезу-представлению, без копирования.
        found = int(self.anchors[lo:hi].searchsorted(
            _POSITION_DTYPE(last_anchor), "right"
        ))
        if found == 0:
            return 0.0
        return float(self.cumsum[lo + found] - self.cumsum[lo])


class CodeHistory:
    """Индекс одной таблицы котировок."""

    __slots__ = ("dates_ns", "bits", "levels")

    def __init__(self, dates_ns: np.ndarray, bits: np.ndarray, levels: list[_Level]):
        self.dates_ns = dates_ns
        self.bits = bits
        self.levels = levels

    def last_closed_index(self, date: datetime, is_daily: bool) -> int:
        """Индекс последней свечки, полностью закрытой к моменту date.

        Свечка со стартом t закрывается в t + длительность таймфрейма, поэтому
        годятся только свечки со стартом не позже date минус эта длительность.
        При date, попадающем ровно на границу свечки, это даёт предыдущую
        свечку — тот же выбор, что делает фреймворк в
        _previous_completed_candle_direction.
        """
        unit = _DAY_SECONDS if is_daily else _HOUR_SECONDS
        cutoff = int(date.timestamp()) - unit
        return int(self.dates_ns.searchsorted(cutoff, "right")) - 1

    def codes_at(
        self,
        date: datetime,
        *,
        is_daily: bool,
        max_length: int = MAX_PATTERN_LENGTH,
    ) -> list[tuple[str, float]]:
        """Пары код-значение для всех длин, которые влезают в историю."""
        anchor = self.last_closed_index(date, is_daily)
        if anchor < 0:
            return []

        limit = min(max_length, len(self.levels), anchor + 1)
        pairs: list[tuple[str, float]] = []
        code = 0
        text = ""
        for length in range(1, limit + 1):
            bit = int(self.bits[anchor - length + 1])
            code |= bit << (length - 1)
            text += "1" if bit else "0"
            # Аналог обязан иметь закрытую следующую свечку, поэтому его якорь
            # не позже anchor - 1. Текущее окно так отсекается заодно: его
            # следующая свечка ещё не сформирована.
            pairs.append((text, self.levels[length - 1].sum_after(code, anchor - 1)))
        return pairs


def _build(np_rates: dict, max_length: int) -> CodeHistory:
    dates_ns = np.ascontiguousarray(np_rates["dates_ns"], dtype=np.int64)
    opens = np.asarray(np_rates["open"], dtype=np.float64)
    closes = np.asarray(np_rates["close"], dtype=np.float64)
    stored_t1 = np.nan_to_num(
        np.asarray(np_rates["t1"], dtype=np.float64),
        nan=0.0, posinf=0.0, neginf=0.0,
    )
    size = dates_ns.size

    # Рост тела → 0, иначе → 1. Свечка без тела (close == open) идёт в 1 так же,
    # как её считает небычьей _previous_completed_candle_direction.
    # int64 обязателен: разряды кода сдвигаются до 2^23.
    bits = (closes <= opens).astype(np.int64)

    # following[j] — показатель свечки, следующей за окном с якорем j.
    following = np.zeros(size, dtype=np.float64)
    following[:-1] = stored_t1[1:]

    levels: list[_Level] = []
    # Код окна длины L, заканчивающегося на i, наращивается из кода длины L-1
    # добавлением старшего разряда — направления свечки i-L+1.
    codes = np.zeros(size, dtype=np.int64)
    for length in range(1, max_length + 1):
        # Дальше окно уже не влезает в историю. Уровень, в который влезло окно,
        # но не влез ни один аналог со следующей свечкой, остаётся пустым:
        # код у даты есть, а сумма по нему нулевая.
        if length > size:
            break
        codes[length - 1:] += bits[: size - length + 1] * (1 << (length - 1))

        anchors = np.arange(length - 1, size - 1, dtype=np.int64)
        window_codes = codes[length - 1: size - 1]
        # Сортировка устойчивая, а окна перечислены по возрастанию якоря,
        # поэтому внутри каждого кода позиции остаются упорядоченными.
        order = np.argsort(window_codes, kind="stable")
        cumsum = np.empty(order.size + 1, dtype=np.float64)
        cumsum[0] = 0.0
        np.cumsum(following[anchors[order]], out=cumsum[1:])
        levels.append(_Level(
            window_codes[order].astype(_CODE_DTYPE),
            anchors[order].astype(_POSITION_DTYPE),
            cumsum,
        ))

    return CodeHistory(dates_ns, bits, levels)


def history_for(
    np_rates: dict | None,
    *,
    max_length: int = MAX_PATTERN_LENGTH,
    table: str = "",
) -> CodeHistory | None:
    """Индекс таблицы котировок с кешированием между вызовами model().

    Ключ кеша включает длину и границы серии, поэтому дозагрузка свежей свечки
    (_append_np_rates_row) приводит к перестроению, а не к устаревшим суммам.
    """
    if not np_rates:
        return None
    dates_ns = np_rates.get("dates_ns")
    if dates_ns is None or len(dates_ns) < 2:
        return None

    key = (
        str(table),
        int(max_length),
        int(len(dates_ns)),
        int(dates_ns[0]),
        int(dates_ns[-1]),
    )
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached is None:
            cached = _build(np_rates, max_length)
            # Индексов держим по одному на таблицу: после дозагрузки свечки
            # прежняя версия серии уже не понадобится.
            for stale in [k for k in _CACHE if k[0] and k[0] == key[0]]:
                del _CACHE[stale]
            if len(_CACHE) >= _CACHE_LIMIT:
                _CACHE.clear()
            _CACHE[key] = cached
    return cached
