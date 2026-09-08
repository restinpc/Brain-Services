"""
model.py — весовые коды направления свечек и суммы следующей за паттерном свечи.

Идея. Тело каждой свечки даёт один разряд: рост → 0, падение → 1. Идём по
таблице котировок от последней закрытой свечки назад в историю и получаем
вложенное подмножество кодов — 0, 01, 011, 0110, ... — до 24 разрядов, то есть
24 весовых кода на каждую дату. Значение кода: проходим историю котировки,
находим все места с таким же паттерном и суммируем показатели (T1) свечек,
которые за этими паттернами следовали. Результат модели — 24 пары код-значение.

Короткие коды почти всегда набирают много аналогов, длинные — единицы или ни
одного (значение 0), поэтому длина кода сама по себе несёт информацию о том,
насколько редок текущий рисунок рынка.

Модель не использует стандартный enriched-конвейер: событием здесь является
каждая свечка каждого инструмента, и таблица «24 кода × вся история» была бы
копией котировок в разы большего объёма. Направления и суммы считаются прямо по
dataset_index["np_rates"] и переиспользуются между вызовами (см. candle_codes.py),
поэтому исторический срез rates[:idx] модели тоже не нужен.

Показатель свечки берётся из хранимого T1 (np_rates["t1"]) — той же величины,
которой фреймворк оценивает исходы во всех остальных сервисах. Тело close-open
используется только для направления: подменять им T1 фреймворк прямо запрещает.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import candle_codes as cc
from brain_framework import get_service_config

_MAX_LENGTH: int | None = None


def _max_length() -> int:
    """Максимальная длина паттерна из config.toml, но не выше предела кода."""
    global _MAX_LENGTH
    if _MAX_LENGTH is None:
        model_cfg = (get_service_config() or {}).get("model") or {}
        requested = int(model_cfg.get("max_pattern_length", cc.MAX_PATTERN_LENGTH))
        _MAX_LENGTH = max(1, min(cc.MAX_PATTERN_LENGTH, requested))
    return _MAX_LENGTH


def _is_daily(index: dict) -> bool:
    if "is_daily" in index:
        return bool(index["is_daily"])
    return str(index.get("rates_table") or "").endswith("_day")


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    del rates, dataset, var, param
    if not isinstance(date, datetime):
        return {}

    index = dataset_index or {}
    max_length = _max_length()
    history = cc.history_for(
        index.get("np_rates"),
        max_length=max_length,
        table=str(index.get("rates_table") or ""),
    )
    if history is None:
        return {}

    pairs = history.codes_at(
        date,
        is_daily=_is_daily(index),
        max_length=max_length,
    )

    # type=1 усиливает вклад длинных кодов: значение × количество разрядов.
    scale_by_digits = int(type) == 1
    return {
        code: round(value * len(code) if scale_by_digits else value, 6)
        for code, value in pairs
    }
