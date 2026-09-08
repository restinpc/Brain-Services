"""
model.py — CV-паттерны brain_index{0-6}_* и суммы следующей за паттерном свечи.

Идея. Лайт-версия (модель 97) кодировала направление тела как 0/1. Здесь паттерн
уже лежит в мастер-базе: на каждую свечку 124 ячейки 1x1…12x12 с 4-разрядным
кодом машинного зрения пары свечей. type — id алгоритма computer vision (0-6),
то есть выбор таблицы brain_index{type}_{инструмент}.

Вложенные подмножества — квадраты растущей глубины: только 1x1, блок 2x2, …,
блок 12x12. На каждом уровне в истории ищутся такие же блоки, суммируется
хранимый T1 свечек, которые за ними следовали. Ответ — пары код-значение,
где код — конкатенация 4-разрядных ячеек блока.

var=0 — суммы как есть, var=1 — значение × число ячеек в блоке (аналог type=1
модели 97, где множителем было число разрядов).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import vision_codes as vc
from brain_framework import get_service_config

_MAX_DEPTH: int | None = None


def _max_depth() -> int:
    global _MAX_DEPTH
    if _MAX_DEPTH is None:
        model_cfg = (get_service_config() or {}).get("model") or {}
        requested = int(model_cfg.get("max_pattern_depth", vc.MAX_PATTERN_DEPTH))
        _MAX_DEPTH = max(1, min(vc.MAX_PATTERN_DEPTH, requested))
    return _MAX_DEPTH


def _is_daily(index: dict) -> bool:
    if "is_daily" in index:
        return bool(index["is_daily"])
    return str(index.get("rates_table") or "").endswith("_day")


def _algorithm(type_value) -> int | None:
    try:
        algorithm = int(type_value)
    except (TypeError, ValueError):
        return None
    if 0 <= algorithm <= vc.MAX_ALGORITHM:
        return algorithm
    return None


def _to_result(pairs: list[tuple[str, float]], var: int) -> dict[str, float]:
    scale_by_cells = int(var) == 1
    out: dict[str, float] = {}
    for code, value in pairs:
        cells = max(1, len(code) // 4) if scale_by_cells else 1
        out[code] = round(value * cells, 6)
    return out


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    """Индексы считает монолит; здесь только диагностика и сброс локального кеша."""
    del engine_vlad, engine_brain
    vc.invalidate()
    tables = vc.discover_tables()
    return {
        "mode": "discover",
        "index_tables": tables,
        "index_table_count": len(tables),
    }


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    del rates, dataset, param
    if not isinstance(date, datetime):
        return {}
    algorithm = _algorithm(type)
    if algorithm is None:
        return {}

    index = dataset_index or {}
    history = vc.history_for(
        index.get("np_rates"),
        algorithm=algorithm,
        rates_table=str(index.get("rates_table") or ""),
        max_depth=_max_depth(),
    )
    if history is None:
        return {}

    pairs = history.codes_at(
        date,
        is_daily=_is_daily(index),
        max_depth=_max_depth(),
    )
    return _to_result(pairs, var)


def batch_model(rates, dataset, dates, *, type=0, var=0, param="", dataset_index=None):
    del rates, dataset, param
    algorithm = _algorithm(type)
    if algorithm is None or not dates:
        return {date: {} for date in dates}

    index = dataset_index or {}
    history = vc.history_for(
        index.get("np_rates"),
        algorithm=algorithm,
        rates_table=str(index.get("rates_table") or ""),
        max_depth=_max_depth(),
    )
    if history is None:
        return {date: {} for date in dates}

    is_daily = _is_daily(index)
    depth = _max_depth()
    return {
        date: _to_result(
            history.codes_at(date, is_daily=is_daily, max_depth=depth),
            var,
        )
        if isinstance(date, datetime) else {}
        for date in dates
    }
