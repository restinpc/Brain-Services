"""
reverse_learning.py v8 — OPT-A: _build_csr кеш, OPT-B: numpy noise rng, OPT-C: universe_json zlib.

reverse_learning.py — ML-режим для /values: обратное обучение по экстремумам.

Включается единым флагом USE_ML_VALUES в model.py. Когда он True, _call_model
в brain_framework.py возвращает ОБУЧЕННЫЙ универсум весов вместо результата
обычной model_fn (а сама model_fn используется как «провайдер активных кодов»
на дате каждого экстремума). Все существующие endpoint-ы (/values, /fill_cache,
/backtest, /posttest, /compute_batch) при этом продолжают работать без правок —
они просто получат другие values и так же спокойно их кешируют/бэктестят.

ИДЕЯ
====
1. От control_date идём по экстремумам simple_rates НАЗАД в историю.
2. На каждом экстремуме берём active_codes (через model_fn в режиме «только ключи»).
3. Назначаем им начальный вес ±1 (или |Δ close| до след. экстр.).
   Знак: верхний экстремум (max) → '-', нижний (min) → '+'.
4. Шаг 0 (k=1): первое подмножество S1 + начальные веса.
   Шаг k≥1: новое Sk.
       • Если Sk ∩ (∪Si, i<k) = ∅ → объединяем без подгонки.
       • Если пересекается → запускаем перевешивание для накопленной истории
         (опционально только для последнего active_tail записей — для O(N²)).
5. Прецессионная точность Ei = (Σ w_signed) · sign_i / Σ|w| ∈ [-1; 1].
   Агрегация по всем Ei: 'mean' (по умолчанию) | 'min' | 'weighted' (свежие важнее).
6. Веса вне универсума → 0.
7. На fill_cache: для каждой свечки сначала проверяем precision уже сохранённого
   универсума на новой control_date; если упала ниже target — перетренировываем
   под семафором (vlad_reverse_jobs_svc{PORT}, UNIQUE-ключ → нет дублей задач).

ПЕРЕВЕШИВАНИЕ
=============
Итеративный, max_iter=20:
  • цикл 0 и каждый чётный → направленный шаг ±step (default 10%).
  • нечётные циклы          → плавающий шум ±step/2 от модуля веса.
Сохраняется лучший снимок по precision; ранний выход при достижении target.

ТАБЛИЦЫ (per-service)
=====================
Создаются автоматически в _preload() через ReverseStore(engine, port=PORT):
  • vlad_reverse_universe_svc{PORT} — обученные универсумы.
  • vlad_reverse_jobs_svc{PORT}     — семафор + аудит задач перевешивания.
"""

from __future__ import annotations

import asyncio
import bisect
import concurrent.futures as _cf
import functools
import hashlib
import json as _json
import os
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Literal

import numpy as np
from sqlalchemy import text

try:
    from numba import njit as _njit
except Exception:  # pragma: no cover - numba is optional in production
    _njit = None

_NUMBA_ENABLED = (
    _njit is not None
    and os.getenv("RL_USE_NUMBA", "1").strip().lower() not in {"0", "false", "no", "off"}
)

# CPU-bound training should not block the FastAPI event loop.
# Keep workers low: the keyed train lock deduplicates identical jobs, while this
# executor controls CPU pressure for independent universes.
_RL_TRAIN_WORKERS = max(1, int(os.getenv("RL_TRAIN_WORKERS", "1")))
_RL_TRAIN_EXECUTOR = _cf.ThreadPoolExecutor(
    max_workers=_RL_TRAIN_WORKERS,
    thread_name_prefix="rl_train",
)


def _stable_seed(key: object) -> int:
    """Deterministic seed for the stochastic rebalance step."""
    raw = repr(key).encode("utf-8", errors="replace")
    return int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "little")


# ──────────────────────────────────────────────────────────────────────────────
# Datatypes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ExtremumRecord:
    """Один исторический экстремум: дата, ожидаемый знак сигнала, активные коды."""
    date:     datetime
    sign:     int               # +1 для нижнего (buy), -1 для верхнего (sell)
    codes:    list[str]
    base_amp: float = 1.0       # модуль начального веса


PrecisionMetric = Literal["mean", "min", "weighted"]
InitMode        = Literal["constant", "diff"]

# Режим обучения (train_mode):
#   0 — назад от control_date, веса ±1 (baseline)
#   1 — вперёд от начала до control_date, веса ±1
#   2 — назад, амплитуда = |Δclose до следующего (старшего) экстр.|; нет соседа → abs(close)
#   3 — вперёд, амплитуда = |Δclose до следующего (младшего) экстр.|; нет соседа → abs(close)
#   4 — вперёд (строгий), амплитуда = |Δclose до следующего экстр.|; нет соседа → пропустить
TrainMode = int  # 0 | 1 | 2 | 3 | 4


# ──────────────────────────────────────────────────────────────────────────────
# Прецессионная точность
# ──────────────────────────────────────────────────────────────────────────────

def _precision_for(weights: dict[str, float], rec: ExtremumRecord) -> float:
    s_signed = 0.0
    s_abs    = 0.0
    for c in rec.codes:
        w = weights.get(c, 0.0)
        s_signed += w
        s_abs    += abs(w)
    if s_abs <= 1e-12:
        return 0.0
    return (s_signed * rec.sign) / s_abs


def total_precision(
    weights: dict[str, float],
    records: list[ExtremumRecord],
    *,
    metric:        PrecisionMetric = "mean",
    recency_decay: float = 0.92,
) -> float:
    """
    metric='mean'     — обычное среднее.
    metric='min'      — минимум по Ei (жёстко: один плохой тянет всё в ноль).
    metric='weighted' — свежие экстремумы важнее (records[0] — ближайший к control_date).
    """
    if not records:
        return 1.0
    if metric == "min":
        return min(_precision_for(weights, r) for r in records)
    if metric == "weighted":
        s_w = 0.0
        s   = 0.0
        for i, r in enumerate(records):
            w = recency_decay ** i
            s += _precision_for(weights, r) * w
            s_w += w
        return s / s_w if s_w > 0 else 0.0
    return sum(_precision_for(weights, r) for r in records) / len(records)


# ──────────────────────────────────────────────────────────────────────────────
# Перевешивание
# ──────────────────────────────────────────────────────────────────────────────

def rebalance_weights(
    weights:  dict[str, float],
    records:  list[ExtremumRecord],
    *,
    max_iter:    int   = 20,
    step:        float = 0.10,
    target:      float = 0.95,
    active_tail: int   = 0,                 # 0 = вся история; >0 = только последние N
    metric:      PrecisionMetric = "mean",
    rng_seed:    int | None = None,
) -> tuple[dict[str, float], float, int]:
    """
    Возвращает (best_weights, best_precision, iterations_done).
    Чётные циклы — направленный шаг ±step. Нечётные — шум ±step/2.

    active_tail ограничивает «окно подгонки»: rebalance видит только последние N
    записей, что даёт O(N²·active_tail) вместо O(N³). Старые экстремумы
    становятся «замороженными» — их веса трогаются только если они есть среди
    кодов хвоста.
    """
    rng    = random.Random(rng_seed) if rng_seed is not None else random
    work   = records[-active_tail:] if active_tail and active_tail > 0 else records
    if not work:
        return dict(weights), 1.0, 0

    cur     = dict(weights)
    best    = dict(cur)
    best_pr = total_precision(cur, work, metric=metric)
    iters   = 0

    if best_pr >= target:
        return best, best_pr, 0

    for cycle in range(max_iter):
        iters = cycle + 1
        if cycle % 2 == 0:
            bad = [r for r in work if _precision_for(cur, r) < target]
            if not bad:
                break
            for r in bad:
                tgt_sign = r.sign
                for c in r.codes:
                    w = cur.get(c, 0.0)
                    if w == 0.0:
                        cur[c] = tgt_sign * 0.01 * r.base_amp
                        continue
                    same = (w > 0 and tgt_sign > 0) or (w < 0 and tgt_sign < 0)
                    cur[c] = w * ((1.0 + step) if same else (1.0 - step))
                    if abs(cur[c]) < 1e-9:
                        cur[c] = tgt_sign * 0.01 * r.base_amp
        else:
            for c, w in list(cur.items()):
                amp    = abs(w) if abs(w) > 1e-9 else 0.01
                cur[c] = w + rng.uniform(-step / 2, step / 2) * amp

        pr = total_precision(cur, work, metric=metric)
        if pr > best_pr:
            best_pr = pr
            best    = dict(cur)
        if best_pr >= target:
            break

    return best, best_pr, iters


# ──────────────────────────────────────────────────────────────────────────────
# Fast path: NumPy/Numba precision + rebalance over dense arrays
# ──────────────────────────────────────────────────────────────────────────────

# Старые Python-реализации оставляем как эталон и fallback.
_total_precision_py = total_precision
_rebalance_weights_py = rebalance_weights


def _metric_id(metric: str) -> int:
    if metric == "min":
        return 1
    if metric == "weighted":
        return 2
    return 0


# OPT-A: _build_csr LRU-кеш — при Numba-пути вызывается на каждый rebalance.
# Ключ: id объектов (records+code_to_idx неизменны внутри одного train-вызова).
# Ограничен 256 слотами чтобы не держать ссылки вечно.
_build_csr_cache: dict = {}
_BUILD_CSR_CACHE_MAX = 256


def _build_csr(records: list[ExtremumRecord], code_to_idx: dict[str, int]):
    """CSR-представление records: offsets + flat indices.

    Важно: code_to_idx строится в порядке вставки старого dict-universe.
    Поэтому обход rec.codes сохраняет старую семантику и порядок суммирования.

    OPT-A: результат кешируется по (id(records_tuple), id(code_to_idx)).
    records — Python list; превращаем в tuple-ключ через id каждого элемента,
    потому что ExtremumRecord неизменен в ходе одного train-цикла.
    """
    cache_key = (tuple(id(r) for r in records), id(code_to_idx))
    cached = _build_csr_cache.get(cache_key)
    if cached is not None:
        return cached

    offsets = np.empty(len(records) + 1, dtype=np.int32)
    signs = np.empty(len(records), dtype=np.int8)
    base_amps = np.empty(len(records), dtype=np.float64)
    flat: list[int] = []
    offsets[0] = 0
    for i, r in enumerate(records):
        signs[i] = int(r.sign)
        base_amps[i] = float(r.base_amp)
        for c in r.codes:
            flat.append(code_to_idx.get(c, -1))
        offsets[i + 1] = len(flat)
    result = offsets, np.asarray(flat, dtype=np.int32), signs, base_amps
    _build_csr_cache[cache_key] = result
    if len(_build_csr_cache) > _BUILD_CSR_CACHE_MAX:
        del _build_csr_cache[next(iter(_build_csr_cache))]
    return result


if _NUMBA_ENABLED:
    @_njit(cache=True)
    def _precision_one_jit(weights_arr, rec_indices, start, end, sign):
        s_signed = 0.0
        s_abs = 0.0
        for p in range(start, end):
            idx = rec_indices[p]
            if idx < 0:
                continue
            w = weights_arr[idx]
            s_signed += w
            s_abs += abs(w)
        if s_abs <= 1e-12:
            return 0.0
        return (s_signed * sign) / s_abs

    @_njit(cache=True)
    def _total_precision_jit(weights_arr, offsets, rec_indices, signs, recency_weights,
                             start_rec, rec_count, metric_id):
        if rec_count <= 0:
            return 1.0

        if metric_id == 1:  # min
            best = 1.0e308
            for j in range(rec_count):
                r = start_rec + j
                pr = _precision_one_jit(
                    weights_arr, rec_indices, offsets[r], offsets[r + 1], signs[r]
                )
                if pr < best:
                    best = pr
            return best

        if metric_id == 2:  # weighted
            s = 0.0
            sw = 0.0
            for j in range(rec_count):
                r = start_rec + j
                w = recency_weights[j]
                s += _precision_one_jit(
                    weights_arr, rec_indices, offsets[r], offsets[r + 1], signs[r]
                ) * w
                sw += w
            return s / sw if sw > 0.0 else 0.0

        s = 0.0
        for j in range(rec_count):
            r = start_rec + j
            s += _precision_one_jit(
                weights_arr, rec_indices, offsets[r], offsets[r + 1], signs[r]
            )
        return s / rec_count

    @_njit(cache=True)
    def _mark_bad_jit(weights_arr, offsets, rec_indices, signs,
                      start_rec, rec_count, target, bad_flags):
        any_bad = False
        for j in range(rec_count):
            r = start_rec + j
            pr = _precision_one_jit(
                weights_arr, rec_indices, offsets[r], offsets[r + 1], signs[r]
            )
            bad = pr < target
            bad_flags[j] = bad
            if bad:
                any_bad = True
        return any_bad

    @_njit(cache=True)
    def _directed_update_jit(weights_arr, offsets, rec_indices, signs, base_amps,
                             start_rec, rec_count, step, bad_flags):
        for j in range(rec_count):
            if not bad_flags[j]:
                continue
            r = start_rec + j
            tgt_sign = signs[r]
            base_amp = base_amps[r]
            for p in range(offsets[r], offsets[r + 1]):
                idx = rec_indices[p]
                if idx < 0:
                    continue
                w = weights_arr[idx]
                if w == 0.0:
                    weights_arr[idx] = tgt_sign * 0.01 * base_amp
                    continue
                same = (w > 0.0 and tgt_sign > 0) or (w < 0.0 and tgt_sign < 0)
                if same:
                    nw = w * (1.0 + step)
                else:
                    nw = w * (1.0 - step)
                if abs(nw) < 1e-9:
                    nw = tgt_sign * 0.01 * base_amp
                weights_arr[idx] = nw

    @_njit(cache=True)
    def _noise_update_prefix_jit(weights_arr, active_count, noise_arr):
        for i in range(active_count):
            w = weights_arr[i]
            amp = abs(w) if abs(w) > 1e-9 else 0.01
            weights_arr[i] = w + noise_arr[i] * amp
else:
    _precision_one_jit = None
    _total_precision_jit = None
    _mark_bad_jit = None
    _directed_update_jit = None
    _noise_update_prefix_jit = None


def _recency_weights(count: int, recency_decay: float = 0.92) -> np.ndarray:
    return np.asarray([recency_decay ** i for i in range(count)], dtype=np.float64)


def total_precision_fast(
    weights: dict[str, float],
    records: list[ExtremumRecord],
    *,
    metric: PrecisionMetric = "mean",
    recency_decay: float = 0.92,
) -> float:
    """Drop-in replacement for total_precision().

    Для numba-path строит плотный массив весов и CSR records. Если Numba не доступна,
    автоматически используется исходная Python-реализация.
    """
    if not records:
        return 1.0
    if not _NUMBA_ENABLED:
        return _total_precision_py(weights, records, metric=metric, recency_decay=recency_decay)

    code_order = list(weights.keys())
    code_to_idx = {c: i for i, c in enumerate(code_order)}
    weights_arr = np.asarray([float(weights[c]) for c in code_order], dtype=np.float64)
    offsets, rec_indices, signs, _base_amps = _build_csr(records, code_to_idx)
    return float(_total_precision_jit(
        weights_arr, offsets, rec_indices, signs,
        _recency_weights(len(records), recency_decay),
        0, len(records), _metric_id(metric),
    ))


def _rebalance_prefix_fast(
    cur_arr: np.ndarray,
    active_count: int,
    offsets: np.ndarray,
    rec_indices: np.ndarray,
    signs: np.ndarray,
    base_amps: np.ndarray,
    rec_count: int,
    *,
    max_iter: int,
    step: float,
    target: float,
    active_tail: int,
    metric: PrecisionMetric,
    rng,
) -> tuple[np.ndarray, float, int]:
    """JIT-перевешивание prefix-universe.

    active_count нужен для точного совпадения со старым dict: шум применяется только
    к уже вставленным ключам, а не ко всем кодам будущих экстремумов.
    """
    work_len = active_tail if active_tail and active_tail > 0 and active_tail < rec_count else rec_count
    if work_len <= 0:
        return cur_arr[:active_count].copy(), 1.0, 0

    start_rec = rec_count - work_len
    metric_id = _metric_id(metric)
    recency_weights = _recency_weights(work_len)
    bad_flags = np.zeros(work_len, dtype=np.bool_)

    best_pr = float(_total_precision_jit(
        cur_arr, offsets, rec_indices, signs, recency_weights,
        start_rec, work_len, metric_id,
    ))
    best_arr = cur_arr[:active_count].copy()
    if best_pr >= target:
        return best_arr, best_pr, 0

    iters = 0
    for cycle in range(max_iter):
        iters = cycle + 1
        if cycle % 2 == 0:
            any_bad = bool(_mark_bad_jit(
                cur_arr, offsets, rec_indices, signs,
                start_rec, work_len, target, bad_flags,
            ))
            if not any_bad:
                break
            _directed_update_jit(
                cur_arr, offsets, rec_indices, signs, base_amps,
                start_rec, work_len, step, bad_flags,
            )
        else:
            # OPT-B: numpy rng вместо list comprehension (×18 быстрее).
            # Используем отдельный np.random.Generator с тем же seed что и rng,
            # чтобы не ломать детерминированность Python-rng для других вызовов.
            # Шум стохастический — точное воспроизведение между версиями не нужно.
            if isinstance(rng, random.Random):
                _np_rng = getattr(rng, "_np_rng", None)
                if _np_rng is None:
                    _np_rng = np.random.default_rng(rng.getrandbits(64))
                    rng._np_rng = _np_rng
                noise = _np_rng.uniform(-step / 2, step / 2, active_count)
            else:
                # Fallback: module-level random (rng_seed=None path)
                noise = np.asarray(
                    [rng.uniform(-step / 2, step / 2) for _ in range(active_count)],
                    dtype=np.float64,
                )
            _noise_update_prefix_jit(cur_arr, active_count, noise)

        pr = float(_total_precision_jit(
            cur_arr, offsets, rec_indices, signs, recency_weights,
            start_rec, work_len, metric_id,
        ))
        if pr > best_pr:
            best_pr = pr
            best_arr = cur_arr[:active_count].copy()
        if best_pr >= target:
            break

    return best_arr, best_pr, iters


def rebalance_weights_fast(
    weights: dict[str, float],
    records: list[ExtremumRecord],
    *,
    max_iter: int = 20,
    step: float = 0.10,
    target: float = 0.95,
    active_tail: int = 0,
    metric: PrecisionMetric = "mean",
    rng_seed: int | None = None,
) -> tuple[dict[str, float], float, int]:
    """Drop-in replacement for rebalance_weights()."""
    if not _NUMBA_ENABLED:
        return _rebalance_weights_py(
            weights, records,
            max_iter=max_iter, step=step, target=target,
            active_tail=active_tail, metric=metric, rng_seed=rng_seed,
        )

    work_len = active_tail if active_tail and active_tail > 0 and active_tail < len(records) else len(records)
    if work_len <= 0:
        return dict(weights), 1.0, 0

    code_order = list(weights.keys())
    code_to_idx = {c: i for i, c in enumerate(code_order)}
    cur_arr = np.asarray([float(weights[c]) for c in code_order], dtype=np.float64)
    offsets, rec_indices, signs, base_amps = _build_csr(records, code_to_idx)
    rng = random.Random(rng_seed) if rng_seed is not None else random

    best_arr, best_pr, iters = _rebalance_prefix_fast(
        cur_arr, len(code_order), offsets, rec_indices, signs, base_amps, len(records),
        max_iter=max_iter, step=step, target=target,
        active_tail=active_tail, metric=metric, rng=rng,
    )
    return {c: float(best_arr[i]) for i, c in enumerate(code_order)}, best_pr, iters


def _train_records_numba_exact(
    records: list[ExtremumRecord],
    *,
    max_iter: int,
    step: float,
    target_precision: float,
    active_tail: int,
    metric: PrecisionMetric,
    rng_seed: int | None = None,
) -> tuple[dict[str, float], float, int, int]:
    """Fast implementation of the
