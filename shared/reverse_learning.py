"""
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


def _build_csr(records: list[ExtremumRecord], code_to_idx: dict[str, int]):
    """CSR-представление records: offsets + flat indices.

    Важно: code_to_idx строится в порядке вставки старого dict-universe.
    Поэтому обход rec.codes сохраняет старую семантику и порядок суммирования.
    """
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
    return offsets, np.asarray(flat, dtype=np.int32), signs, base_amps


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
            # ВАЖНО: сохраняем точный порядок и число random.uniform(), как в старом
            # for c, w in list(cur.items()). Нельзя заменять на np.random/numba RNG.
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
    """Fast implementation of the train loop over already-built records.

    Результат должен совпадать со старым dict-алгоритмом при одинаковом состоянии
    random: порядок вставки ключей и порядок шумовых random.uniform() сохранён.
    """
    if not _NUMBA_ENABLED:
        # Python fallback: тот же код, что был в train_at_date.
        # If rng_seed is supplied, use a deterministic sequence of per-rebalance
        # seeds instead of the process-global RNG.
        rng = random.Random(rng_seed) if rng_seed is not None else None
        universe: dict[str, float] = {}
        all_seen: set[str] = set()
        iterations_total = 0
        for rec_idx, rec in enumerate(records):
            rec_set = set(rec.codes)
            overlap = bool(rec_set & all_seen)
            if not overlap:
                for c in rec.codes:
                    if c not in universe:
                        universe[c] = rec.sign * rec.base_amp
                all_seen |= rec_set
                continue
            for c in rec.codes:
                if c not in universe:
                    universe[c] = rec.sign * rec.base_amp
            all_seen |= rec_set
            universe, _pr, iters = _rebalance_weights_py(
                universe, records[:rec_idx + 1],
                max_iter=max_iter, step=step, target=target_precision,
                active_tail=active_tail, metric=metric,
                rng_seed=(rng.randrange(0, 2**63) if rng is not None else None),
            )
            iterations_total += iters
        final_precision = _total_precision_py(universe, records, metric=metric)
        if final_precision < target_precision:
            universe, final_precision, iters = _rebalance_weights_py(
                universe, records,
                max_iter=max_iter, step=step, target=target_precision,
                active_tail=active_tail, metric=metric,
                rng_seed=(rng.randrange(0, 2**63) if rng is not None else None),
            )
            iterations_total += iters
        universe = {k: float(v) for k, v in universe.items() if abs(float(v)) > 1e-12}
        return universe, float(final_precision), int(iterations_total), len(records)

    # 1) Строим глобальный code_order ровно в порядке появления в старом dict.
    code_to_idx: dict[str, int] = {}
    code_order: list[str] = []
    new_indices_by_rec: list[list[int]] = []
    overlap_by_rec: list[bool] = []
    seen: set[str] = set()

    for rec in records:
        rec_set = set(rec.codes)
        overlap_by_rec.append(bool(rec_set & seen))
        new_idxs: list[int] = []
        for c in rec.codes:
            if c not in code_to_idx:
                code_to_idx[c] = len(code_order)
                code_order.append(c)
                new_idxs.append(code_to_idx[c])
        new_indices_by_rec.append(new_idxs)
        seen |= rec_set

    offsets, rec_indices, signs, base_amps = _build_csr(records, code_to_idx)
    rng = random.Random(rng_seed) if rng_seed is not None else random
    cur_arr = np.zeros(len(code_order), dtype=np.float64)
    active_count = 0
    iterations_total = 0

    for rec_idx, rec in enumerate(records):
        for idx in new_indices_by_rec[rec_idx]:
            cur_arr[idx] = rec.sign * rec.base_amp
        active_count += len(new_indices_by_rec[rec_idx])

        if not overlap_by_rec[rec_idx]:
            continue

        best_arr, _pr, iters = _rebalance_prefix_fast(
            cur_arr, active_count, offsets, rec_indices, signs, base_amps, rec_idx + 1,
            max_iter=max_iter, step=step, target=target_precision,
            active_tail=active_tail, metric=metric, rng=rng,
        )
        cur_arr[:active_count] = best_arr
        iterations_total += iters

    final_precision = float(_total_precision_jit(
        cur_arr, offsets, rec_indices, signs, _recency_weights(len(records)),
        0, len(records), _metric_id(metric),
    ))

    if final_precision < target_precision:
        best_arr, final_precision, iters = _rebalance_prefix_fast(
            cur_arr, active_count, offsets, rec_indices, signs, base_amps, len(records),
            max_iter=max_iter, step=step, target=target_precision,
            active_tail=active_tail, metric=metric, rng=rng,
        )
        cur_arr[:active_count] = best_arr
        iterations_total += iters

    universe = {
        code_order[i]: float(cur_arr[i])
        for i in range(active_count)
        if abs(float(cur_arr[i])) > 1e-12
    }
    return universe, float(final_precision), int(iterations_total), len(records)


# Публичные имена теперь указывают на быстрые drop-in функции.
total_precision = total_precision_fast
rebalance_weights = rebalance_weights_fast


# ──────────────────────────────────────────────────────────────────────────────
# Сборка экстремумов от control_date НАЗАД / ВПЕРЁД
# ──────────────────────────────────────────────────────────────────────────────

# Cache for extremum flags: id(np_simple_rates) + interval -> (ext_max, ext_min)
# np_simple_rates dict doesn't change during fill_cache, so this is safe.
_extremum_flags_cache: dict = {}

# Cache for precomputed sorted extremum list:
# (id(np_simple_rates), n, radius) -> ([(ts_int, sign_int), ...], [ts_int, ...])
# Computed once per dataset, then collect_extremums_back/forward use bisect O(1)
# instead of scanning the full candle array on each call (up to 119k iterations).
_all_extremums_cache: dict = {}


def _extremum_flags(
    np_simple_rates: dict | None,
    *,
    interval: int = 3,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Возвращает (ext_max, ext_min).

    interval — полный минимальный интервал регистрации экстремума.
    interval=3 эквивалентен старой логике: одна свеча слева и одна справа.
    interval=5/7/9 расширяет симметричное окно до ±2/±3/±4 свечей.

    OPTIMIZED: векторизовано через sliding_window_view вместо Python-цикла.
    Ускорение ~1400x на 119k свечей (1000ms → 0.7ms).
    Результат кешируется по id объекта + interval.
    """
    if not np_simple_rates:
        return np.zeros(0, dtype=bool), np.zeros(0, dtype=bool)

    dates_ns = np_simple_rates.get("dates_ns")
    n = len(dates_ns) if dates_ns is not None else 0
    if n == 0:
        return np.zeros(0, dtype=bool), np.zeros(0, dtype=bool)

    try:
        interval_i = int(interval or 3)
    except Exception:
        interval_i = 3
    if interval_i < 3:
        interval_i = 3

    radius = max(1, interval_i // 2)

    # Check cache first
    cache_key = (id(np_simple_rates), n, radius)
    cached = _extremum_flags_cache.get(cache_key)
    if cached is not None:
        return cached

    high = np.asarray(
        np_simple_rates.get("max", np_simple_rates.get("high", np_simple_rates.get("close"))),
        dtype=np.float64,
    )
    low = np.asarray(
        np_simple_rates.get("min", np_simple_rates.get("low", np_simple_rates.get("close"))),
        dtype=np.float64,
    )

    ext_max = np.zeros(n, dtype=bool)
    ext_min = np.zeros(n, dtype=bool)
    if n < 2 * radius + 1:
        _extremum_flags_cache[cache_key] = (ext_max, ext_min)
        return ext_max, ext_min

    # Vectorized via sliding_window_view — replaces Python loop O(n*radius*4) → O(n)
    from numpy.lib.stride_tricks import sliding_window_view
    w  = 2 * radius + 1
    wh = sliding_window_view(high, w)   # shape: (n - w + 1, w)
    wl = sliding_window_view(low,  w)

    center_h = wh[:, radius]
    center_l = wl[:, radius]

    inner_max = (center_h > wh[:, :radius].max(axis=1)) &                 (center_h > wh[:, radius+1:].max(axis=1))
    inner_min = (center_l < wl[:, :radius].min(axis=1)) &                 (center_l < wl[:, radius+1:].min(axis=1))

    ext_max[radius:n - radius] = inner_max
    ext_min[radius:n - radius] = inner_min

    result = (ext_max, ext_min)
    _extremum_flags_cache[cache_key] = result
    return result


def _get_all_extremums(
    np_simple_rates: dict,
    *,
    interval: int = 3,
) -> tuple[list[tuple[int, int]], list[int]]:
    """
    Возвращает (all_ext, all_ext_ts) — все экстремумы датасета в хронологическом
    порядке. Вычисляется один раз per (id, n, radius), затем кешируется.

    all_ext    : [(unix_ts_int, sign_int), ...]   знак: -1 max, +1 min
    all_ext_ts : [unix_ts_int, ...]               только timestamps (для bisect)

    collect_extremums_back/forward используют bisect → O(log n + limit)
    вместо Python-цикла по всему массиву O(n).
    """
    dates_ns = np_simple_rates.get("dates_ns")
    n = len(dates_ns) if dates_ns is not None else 0
    if n == 0:
        return [], []

    try:
        interval_i = int(interval or 3)
    except Exception:
        interval_i = 3
    if interval_i < 3:
        interval_i = 3
    radius = max(1, interval_i // 2)

    cache_key = (id(np_simple_rates), n, radius)
    cached = _all_extremums_cache.get(cache_key)
    if cached is not None:
        return cached

    ext_max, ext_min = _extremum_flags(np_simple_rates, interval=interval_i)

    # Vectorized: build combined int8 array, flatnonzero at C speed
    combined = np.zeros(n, dtype=np.int8)
    combined[ext_min] = 1
    combined[ext_max] = -1   # max wins (matches original elif logic)

    indices = np.flatnonzero(combined)  # ascending, C speed

    # Plain Python lists — no numpy overhead per bisect/slice lookup
    all_ext    = [(int(dates_ns[i]), int(combined[i])) for i in indices]
    all_ext_ts = [x[0] for x in all_ext]

    result = (all_ext, all_ext_ts)
    _all_extremums_cache[cache_key] = result
    return result


def collect_extremums_back(
    np_simple_rates: dict | None,
    control_date:    datetime,
    *,
    limit: int = 50,
    extremum_interval: int = 3,
) -> list[tuple[datetime, int]]:
    """
    [(date, sign)] от ближайшего к control_date к более старым.

    OPTIMIZED: precomputed list + bisect → O(log n + limit)
    вместо Python-цикла O(n) по всему массиву свечей.
    """
    if not np_simple_rates:
        return []

    all_ext, all_ext_ts = _get_all_extremums(np_simple_rates, interval=extremum_interval)
    if not all_ext:
        return []

    ts         = int(control_date.timestamp())
    cutoff_idx = bisect.bisect_right(all_ext_ts, ts)   # первый индекс > ts
    start      = max(0, cutoff_idx - limit)
    chunk      = all_ext[start:cutoff_idx]

    # chunk в хронологическом порядке → разворачиваем (ближайший к control_date первый)
    return [(datetime.fromtimestamp(t), s) for t, s in reversed(chunk)]


def collect_extremums_forward(
    np_simple_rates: dict | None,
    control_date:    datetime,
    *,
    limit: int = 50,
    extremum_interval: int = 3,
) -> list[tuple[datetime, int]]:
    """
    [(date, sign)] в хронологическом порядке, до control_date включительно.
    Возвращает последние `limit` записей.

    OPTIMIZED: precomputed list + bisect → O(log n + limit).
    """
    if not np_simple_rates:
        return []

    all_ext, all_ext_ts = _get_all_extremums(np_simple_rates, interval=extremum_interval)
    if not all_ext:
        return []

    ts         = int(control_date.timestamp())
    cutoff_idx = bisect.bisect_right(all_ext_ts, ts)
    start      = max(0, cutoff_idx - limit)
    chunk      = all_ext[start:cutoff_idx]

    # Хронологический порядок — разворот не нужен
    return [(datetime.fromtimestamp(t), s) for t, s in chunk]


def group_control_dates_by_extremum_state(
    np_simple_rates: dict | None,
    control_dates: list[datetime],
    *,
    train_mode: TrainMode = 0,
    extremum_limit: int = 50,
    extremum_interval: int = 3,
) -> tuple[dict[tuple, list[int]], dict[tuple, list[tuple[datetime, int]]]]:
    """Group control dates by the exact extremum sequence used for training.

    This is the exact safe form of incremental fill-cache training: every candle
    between two consecutive extrema has the same `seq_tuple`, therefore the ML
    universe is byte-for-byte the same for those candles.  We can train once for
    the group and copy the result to all dates in the group without changing
    answers.

    Returns:
        groups: {seq_tuple: [indexes into control_dates]}
        seq_by_key: {seq_tuple: original [(date, sign), ...] sequence}
    """
    groups: dict[tuple, list[int]] = {}
    seq_by_key: dict[tuple, list[tuple[datetime, int]]] = {}
    if not control_dates:
        return groups, seq_by_key

    forward = train_mode in (1, 3, 4)
    collect_fn = collect_extremums_forward if forward else collect_extremums_back

    for idx, dt in enumerate(control_dates):
        seq = collect_fn(
            np_simple_rates,
            dt,
            limit=extremum_limit,
            extremum_interval=extremum_interval,
        )
        key = tuple((int(d.timestamp()), int(sign)) for d, sign in seq)
        groups.setdefault(key, []).append(idx)
        if key not in seq_by_key:
            seq_by_key[key] = seq

    return groups, seq_by_key


def _diff_amp(
    np_simple_rates: dict | None,
    date_a: datetime,
    date_b: datetime | None,
) -> float:
    if not np_simple_rates:
        return 1.0

    dates_ns = np_simple_rates["dates_ns"]
    close    = np_simple_rates["close"]
    n        = len(close)

    if n == 0:
        return 1.0

    ia = min(
        int(np.searchsorted(dates_ns, np.int64(int(date_a.timestamp())))),
        n - 1,
    )

    if date_b is None:
        return abs(float(close[ia])) or 1.0

    ib = min(
        int(np.searchsorted(dates_ns, np.int64(int(date_b.timestamp())))),
        n - 1,
    )

    return abs(float(close[ia] - close[ib])) or 1.0


# ──────────────────────────────────────────────────────────────────────────────
# Основной пайплайн обучения, чистая алгоритмика без БД
# ──────────────────────────────────────────────────────────────────────────────

ActiveCodesProvider = Callable[[datetime], Awaitable[list[str]]]


# Cache: (seq_hash, codes_hash, train_mode, max_iter, step, target, active_tail, metric)
# -> (universe, precision, iters, ext_cnt)
# Adjacent hourly candles almost always share identical extremum sets -> cache hit -> skip retrain
_train_at_date_cache: dict = {}
_TRAIN_CACHE_MAX = 4096  # увеличено с 2000: больше типов/vars/пар помещается без вытеснения

# Cache: (model_tag, seq_tuple) -> (pre_codes, codes_key)
# Avoids 50 `await active_codes_at()` calls when seq_tuple is unchanged between
# consecutive candles (the common case: adjacent hourly/daily candles share the
# same 50 extremums, so codes never need to be re-fetched from _ml_active_cache).
_seq_codes_cache: dict = {}
_SEQ_CODES_CACHE_MAX = 1024


def _build_records_from_seq_codes(
    *,
    seq: list[tuple[datetime, int]],
    pre_codes: list[list[str]],
    np_simple_rates: dict | None,
    train_mode: TrainMode,
) -> list[ExtremumRecord]:
    """Build ExtremumRecord objects from already collected extremums/codes.

    This prevents maybe_retrain() from collecting the same extremums and active
    codes, then asking train_at_date() to do it all again.
    """
    use_diff = train_mode in (2, 3, 4)
    strict_amp = train_mode == 4
    records: list[ExtremumRecord] = []

    for idx, ((ext_date, sign), codes) in enumerate(zip(seq, pre_codes)):
        if not codes:
            continue

        if use_diff:
            has_next = idx + 1 < len(seq)
            next_date = seq[idx + 1][0] if has_next else None
            if strict_amp and not has_next:
                continue
            amp = _diff_amp(np_simple_rates, ext_date, next_date)
        else:
            amp = 1.0

        records.append(ExtremumRecord(
            date=ext_date,
            sign=sign,
            codes=codes,
            base_amp=float(amp),
        ))

    return records


async def _run_train_records(
    *,
    records: list[ExtremumRecord],
    max_iter: int,
    step: float,
    target_precision: float,
    active_tail: int,
    metric: PrecisionMetric,
    rng_seed: int | None,
) -> tuple[dict[str, float], float, int, int]:
    """Run CPU-bound training off the event loop."""
    loop = asyncio.get_running_loop()
    fn = functools.partial(
        _train_records_numba_exact,
        records,
        max_iter=max_iter,
        step=step,
        target_precision=target_precision,
        active_tail=active_tail,
        metric=metric,
        rng_seed=rng_seed,
    )
    return await loop.run_in_executor(_RL_TRAIN_EXECUTOR, fn)


async def train_prebuilt_records(
    *,
    records: list[ExtremumRecord],
    cache_key: tuple,
    max_iter: int,
    step: float,
    target_precision: float,
    active_tail: int,
    metric: PrecisionMetric,
) -> tuple[dict[str, float], float, int, int, bool]:
    """Train using records that were already built by maybe_retrain().

    Returns (universe, precision, iterations, ext_count, from_cache).
    """
    cached = _train_at_date_cache.get(cache_key)
    if cached is not None:
        return (*cached, True)

    if not records:
        return {}, 1.0, 0, 0, False

    universe, final_precision, iterations_total, ext_count = await _run_train_records(
        records=records,
        max_iter=max_iter,
        step=step,
        target_precision=target_precision,
        active_tail=active_tail,
        metric=metric,
        rng_seed=_stable_seed(cache_key),
    )

    result = (universe, float(final_precision), int(iterations_total), int(ext_count))
    _train_at_date_cache[cache_key] = result
    if len(_train_at_date_cache) > _TRAIN_CACHE_MAX:
        del _train_at_date_cache[next(iter(_train_at_date_cache))]

    return (*result, False)


async def train_at_date(
    *,
    np_simple_rates:  dict | None,
    control_date:     datetime,
    active_codes_at:  ActiveCodesProvider,
    train_mode:       TrainMode = 0,
    max_iter:         int   = 20,
    step:             float = 0.10,
    target_precision: float = 0.95,
    extremum_limit:   int   = 50,
    extremum_interval: int = 3,
    active_tail:      int   = 0,
    metric:           PrecisionMetric = "mean",
) -> tuple[dict[str, float], float, int, int]:
    """
    Returns:
        (universe, final_precision, iterations_total, extremum_count)

    train_mode:
      0 — назад, веса ±1
      1 — вперёд, веса ±1
      2 — назад, амплитуда |Δclose до соседнего экстремума|;
          нет соседа → abs(close)
      3 — вперёд, амплитуда |Δclose до следующего экстремума|;
          нет соседа → abs(close)
      4 — вперёд строго, амплитуда |Δclose|;
          нет следующего экстремума → запись пропускается

    extremum_interval:
      3/5/7/9 — минимальный интервал регистрации экстремума.
    """

    forward = train_mode in (1, 3, 4)

    if forward:
        seq = collect_extremums_forward(
            np_simple_rates,
            control_date,
            limit=extremum_limit,
            extremum_interval=extremum_interval,
        )
    else:
        seq = collect_extremums_back(
            np_simple_rates,
            control_date,
            limit=extremum_limit,
            extremum_interval=extremum_interval,
        )

    if not seq:
        return {}, 1.0, 0, 0

    # Fast path: collect codes for all extremums first, then check cache.
    # Adjacent candles nearly always share the same 50 extremums -> cache hit.
    seq_tuple = tuple((int(d.timestamp()), s) for d, s in seq)

    seq_codes_key = (id(active_codes_at), seq_tuple)
    cached_seq_codes = _seq_codes_cache.get(seq_codes_key)
    if cached_seq_codes is not None:
        pre_codes, codes_key = cached_seq_codes
    else:
        pre_codes: list[list[str]] = []
        for ext_date, _ in seq:
            try:
                codes = await active_codes_at(ext_date)
            except Exception:
                codes = []
            pre_codes.append(sorted(set(codes)))

        codes_key = tuple(tuple(c) for c in pre_codes)
        _seq_codes_cache[seq_codes_key] = (pre_codes, codes_key)
        if len(_seq_codes_cache) > _SEQ_CODES_CACHE_MAX:
            del _seq_codes_cache[next(iter(_seq_codes_cache))]
    cache_key  = (seq_tuple, codes_key, train_mode, max_iter,
                  round(step, 6), round(target_precision, 6),
                  active_tail, metric)

    cached = _train_at_date_cache.get(cache_key)
    if cached is not None:
        return (*cached, True)   # True = from cache, skip DB writes

    use_diff   = train_mode in (2, 3, 4)
    strict_amp = train_mode == 4

    records: list[ExtremumRecord] = []

    for idx, ((ext_date, sign), codes) in enumerate(zip(seq, pre_codes)):
        if not codes:
            continue

        if use_diff:
            has_next = idx + 1 < len(seq)
            next_date = seq[idx + 1][0] if has_next else None

            if strict_amp and not has_next:
                continue

            amp = _diff_amp(np_simple_rates, ext_date, next_date)
        else:
            amp = 1.0

        # Дедупликация кодов на одном экстремуме, чтобы один код не усиливался
        # дважды из-за повторного события в одной свече.
        records.append(
            ExtremumRecord(
                date=ext_date,
                sign=sign,
                codes=sorted(set(codes)),
                base_amp=float(amp),
            )
        )

    if not records:
        return {}, 1.0, 0, 0

    # Fast path: NumPy/Numba-перевешивание по CSR-представлению records.
    # При RL_USE_NUMBA=0 или отсутствии numba автоматически сработает Python fallback.
    universe, final_precision, iterations_total, ext_count = await _run_train_records(
        records=records,
        max_iter=max_iter,
        step=step,
        target_precision=target_precision,
        active_tail=active_tail,
        metric=metric,
        rng_seed=_stable_seed(cache_key),
    )

    result = (universe, float(final_precision), int(iterations_total), int(ext_count))
    _train_at_date_cache[cache_key] = result
    # Keep cache bounded
    if len(_train_at_date_cache) > _TRAIN_CACHE_MAX:
        oldest = next(iter(_train_at_date_cache))
        del _train_at_date_cache[oldest]
    return (*result, False)   # False = computed fresh, not from cache


# ──────────────────────────────────────────────────────────────────────────────
# ReverseStore: БД-хранилище обученных universe + jobs-семафор
# ──────────────────────────────────────────────────────────────────────────────

class ReverseStore:
    # Connection errors that warrant dispose + retry.
    # "Packet sequence number wrong" happens when a fresh or stale connection
    # gets a corrupt MySQL packet (server overload, proxy quirk, idle timeout).
    _RETRYABLE = (
        "Packet sequence",
        "Lost connection",
        "server has gone away",
        "Can't connect",
        "Connection refused",
    )

    def __init__(self, engine, *, port: int):
        self.engine = engine
        self.port = int(port)
        self.universe_table = f"vlad_reverse_universe_svc{self.port}"
        self.jobs_table = f"vlad_reverse_jobs_svc{self.port}"
        # In-memory cache: (pair, day_flag, control_date, params_hash) -> (universe, precision)
        # Eliminates repeated DB round-trips for the same key during fill_cache
        self._universe_cache: dict = {}
        self._train_locks: dict[tuple, asyncio.Lock] = {}
        self._train_locks_guard = asyncio.Lock()

        # Separate read-only engine with NullPool for load_universe.
        #
        # Problem: load_universe uses the shared engine pool (size=20, overflow=20=40 total).
        # During fill_cache + concurrent /values requests, the pool gets exhausted and
        # each load_universe call waits 30 seconds (pool_timeout) before failing.
        # With 660k candles that's days of fill_cache time.
        #
        # NullPool: no pooling — each load_universe opens its own connection and closes it
        # immediately. Never competes with fill_cache inserts, bg_reload, or other readers.
        try:
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy.pool import NullPool
            self._read_engine = create_async_engine(
                engine.sync_engine.url,
                poolclass=NullPool,
                connect_args=getattr(engine.sync_engine, "dialect", None) and
                             getattr(engine.sync_engine.pool, "_creator_arg", {}) or {},
            )
        except Exception:
            self._read_engine = engine  # fallback to shared engine

    async def _get_train_lock(self, key: tuple) -> asyncio.Lock:
        async with self._train_locks_guard:
            lock = self._train_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._train_locks[key] = lock
            if len(self._train_locks) > 4096:
                for old_key in list(self._train_locks.keys())[:512]:
                    if old_key != key:
                        self._train_locks.pop(old_key, None)
            return lock

    def clear_universe_cache(self) -> None:
        """Call at the start of fill_cache to start fresh."""
        self._universe_cache.clear()

    def _is_retryable(self, exc: Exception) -> bool:
        msg = str(exc)
        return any(kw in msg for kw in self._RETRYABLE)

    async def _run_with_retry(self, coro_fn, max_attempts: int = 3):
        """
        Запускает DB-корутину с retry + dispose на ошибках соединения.

        'Packet sequence number wrong' возникает когда aiomysql получает
        неожиданный пакет от MySQL при установке нового соединения (перегрузка
        сервера, proxy, истекший idle-timeout). Dispose сбрасывает пул,
        следующая попытка создаёт чистый сокет.
        """
        for attempt in range(max_attempts):
            try:
                return await coro_fn()
            except Exception as exc:
                if attempt < max_attempts - 1 and self._is_retryable(exc):
                    try:
                        await self.engine.dispose()
                    except Exception:
                        pass
                    await asyncio.sleep(1.5 * (attempt + 1))
                else:
                    raise

    async def ensure_tables(self) -> None:
        async def _do():
            async with self.engine.begin() as conn:
                await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{self.universe_table}` (
                    `id`                BIGINT       NOT NULL AUTO_INCREMENT,
                    `pair`              INT          NOT NULL,
                    `day_flag`          TINYINT      NOT NULL DEFAULT 0,
                    `control_date`      DATETIME     NOT NULL,
                    `params_hash`       CHAR(32)     NOT NULL,
                    `universe_json`     LONGTEXT     NOT NULL,
                    `precision_val`     DOUBLE       NOT NULL DEFAULT 0,
                    `iterations`        INT          NOT NULL DEFAULT 0,
                    `extremum_count`    INT          NOT NULL DEFAULT 0,
                    `history_window`    INT          NOT NULL DEFAULT 0,
                    `active_tail`       INT          NOT NULL DEFAULT 0,
                    `precision_metric`  VARCHAR(16)  NOT NULL DEFAULT 'mean',
                    `init_mode`         VARCHAR(32)  NOT NULL DEFAULT 'mode0',
                    `step_size`         DOUBLE       NOT NULL DEFAULT 0.10,
                    `target_precision`  DOUBLE       NOT NULL DEFAULT 0.95,
                    `created_at`        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                    `updated_at`        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                                   ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    UNIQUE KEY `uk_reverse_universe`
                        (`pair`, `day_flag`, `control_date`, `params_hash`),
                    KEY `idx_pair_day_date`
                        (`pair`, `day_flag`, `control_date`),
                    KEY `idx_params_hash` (`params_hash`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

                await conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS `{self.jobs_table}` (
                        `id`                  BIGINT       NOT NULL AUTO_INCREMENT,
                        `pair`                INT          NOT NULL,
                        `day_flag`            TINYINT      NOT NULL DEFAULT 0,
                        `control_date`        DATETIME     NOT NULL,
                        `params_hash`         CHAR(32)     NOT NULL,
                        `state`               VARCHAR(16)  NOT NULL DEFAULT 'queued',
                        `triggered_by`        VARCHAR(32)  NOT NULL DEFAULT 'unknown',
                        `error_msg`           TEXT         NULL,
                        `precision_before`    DOUBLE       NULL,
                        `precision_after`     DOUBLE       NULL,
                        `iterations`          INT          NOT NULL DEFAULT 0,
                        `universe_size`       INT          NOT NULL DEFAULT 0,
                        `prev_universe_size`  INT          NOT NULL DEFAULT 0,
                        `extremum_count`      INT          NOT NULL DEFAULT 0,
                        `history_window`      INT          NOT NULL DEFAULT 0,
                        `active_tail`         INT          NOT NULL DEFAULT 0,
                        `precision_metric`    VARCHAR(16)  NOT NULL DEFAULT 'mean',
                        `init_mode`           VARCHAR(32)  NOT NULL DEFAULT 'mode0',
                        `step_size`           DOUBLE       NOT NULL DEFAULT 0.10,
                        `target_precision`    DOUBLE       NOT NULL DEFAULT 0.95,
                        `created_at`          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                        `updated_at`          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                                         ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `uk_reverse_job`
                            (`pair`, `day_flag`, `control_date`, `params_hash`),
                        KEY `idx_state` (`state`),
                        KEY `idx_pair_day_date`
                            (`pair`, `day_flag`, `control_date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))

        await self._run_with_retry(_do)

    async def load_universe(
        self,
        *,
        pair: int,
        day_flag: int,
        control_date: datetime,
        params_hash: str,
    ) -> tuple[dict[str, float], float] | None:
        # Check in-memory cache first — avoids DB round-trip on repeated lookups
        cache_key = (pair, day_flag, control_date, params_hash)
        if cache_key in self._universe_cache:
            return self._universe_cache[cache_key]

        params = {"pair": pair, "day": day_flag, "dt": control_date, "ph": params_hash}

        async def _fetch():
            # Use _read_engine (NullPool) — does NOT compete with shared pool.
            # asyncio.wait_for gives a hard 5-second ceiling so fill_cache never
            # stalls 30 seconds per candle when the DB is under load.
            async def _query():
                async with self._read_engine.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT universe_json, precision_val
                        FROM `{self.universe_table}`
                        WHERE pair = :pair
                          AND day_flag = :day
                          AND control_date = :dt
                          AND params_hash = :ph
                        LIMIT 1
                    """), params)
                    return res.mappings().first()

            return await asyncio.wait_for(_query(), timeout=5.0)

        try:
            row = await self._run_with_retry(_fetch)
        except Exception:
            return None

        if not row:
            self._universe_cache[cache_key] = None
            return None

        try:
            raw = _json.loads(row["universe_json"] or "{}")
            universe = {
                str(k): float(v)
                for k, v in raw.items()
                if v is not None
            }
            result = universe, float(row["precision_val"] or 0.0)
            self._universe_cache[cache_key] = result
            return result
        except Exception:
            return None

    async def save_universe(
        self,
        *,
        pair: int,
        day_flag: int,
        control_date: datetime,
        params_hash: str,
        universe: dict[str, float],
        precision_val: float,
        iterations: int,
        extremum_cnt: int,
        history_window: int,
        active_tail: int,
        precision_metric: str,
        init_mode: str,
        step_size: float,
        target_precision: float,
    ) -> None:
        payload = _json.dumps(
            universe,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO `{self.universe_table}` (
                    pair, day_flag, control_date, params_hash,
                    universe_json, precision_val, iterations, extremum_count,
                    history_window, active_tail, precision_metric, init_mode,
                    step_size, target_precision
                )
                VALUES (
                    :pair, :day, :dt, :ph,
                    :payload, :pr, :iters, :ext_cnt,
                    :hist, :tail, :metric, :init_mode,
                    :step, :target
                )
                ON DUPLICATE KEY UPDATE
                    universe_json     = VALUES(universe_json),
                    precision_val     = VALUES(precision_val),
                    iterations        = VALUES(iterations),
                    extremum_count    = VALUES(extremum_count),
                    history_window    = VALUES(history_window),
                    active_tail       = VALUES(active_tail),
                    precision_metric  = VALUES(precision_metric),
                    init_mode         = VALUES(init_mode),
                    step_size         = VALUES(step_size),
                    target_precision  = VALUES(target_precision),
                    updated_at        = CURRENT_TIMESTAMP
            """), {
                "pair": pair,
                "day": day_flag,
                "dt": control_date,
                "ph": params_hash,
                "payload": payload,
                "pr": float(precision_val),
                "iters": int(iterations),
                "ext_cnt": int(extremum_cnt),
                "hist": int(history_window),
                "tail": int(active_tail),
                "metric": str(precision_metric),
                "init_mode": str(init_mode),
                "step": float(step_size),
                "target": float(target_precision),
            })

        # Update in-memory cache so subsequent load_universe calls skip DB
        cache_key = (pair, day_flag, control_date, params_hash)
        self._universe_cache[cache_key] = (dict(universe), float(precision_val))

    async def reserve_slot(
        self,
        *,
        pair: int,
        day_flag: int,
        control_date: datetime,
        params_hash: str,
        triggered_by: str,
        history_window: int,
        active_tail: int,
        precision_metric: str,
        init_mode: str,
        step_size: float,
        target_precision: float,
    ) -> bool:
        """
        Семафор: True если мы создали/заняли job.
        False если такой job уже существует.
        """
        await self.ensure_tables()
        async with self.engine.begin() as conn:
            res = await conn.execute(text(f"""
                INSERT IGNORE INTO `{self.jobs_table}` (
                    pair, day_flag, control_date, params_hash,
                    state, triggered_by,
                    history_window, active_tail, precision_metric, init_mode,
                    step_size, target_precision
                )
                VALUES (
                    :pair, :day, :dt, :ph,
                    'running', :triggered_by,
                    :hist, :tail, :metric, :init_mode,
                    :step, :target
                )
            """), {
                "pair": pair,
                "day": day_flag,
                "dt": control_date,
                "ph": params_hash,
                "triggered_by": triggered_by,
                "hist": int(history_window),
                "tail": int(active_tail),
                "metric": str(precision_metric),
                "init_mode": str(init_mode),
                "step": float(step_size),
                "target": float(target_precision),
            })

            if res.rowcount == 1:
                return True

            # Если job уже есть, но он не running, можно перезаписать как running.
            upd = await conn.execute(text(f"""
                UPDATE `{self.jobs_table}`
                SET state = 'running',
                    triggered_by = :triggered_by,
                    error_msg = NULL,
                    history_window = :hist,
                    active_tail = :tail,
                    precision_metric = :metric,
                    init_mode = :init_mode,
                    step_size = :step,
                    target_precision = :target,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pair = :pair
                  AND day_flag = :day
                  AND control_date = :dt
                  AND params_hash = :ph
                  AND state <> 'running'
            """), {
                "pair": pair,
                "day": day_flag,
                "dt": control_date,
                "ph": params_hash,
                "triggered_by": triggered_by,
                "hist": int(history_window),
                "tail": int(active_tail),
                "metric": str(precision_metric),
                "init_mode": str(init_mode),
                "step": float(step_size),
                "target": float(target_precision),
            })

            return upd.rowcount == 1

    async def finish_slot(
        self,
        *,
        pair: int,
        day_flag: int,
        control_date: datetime,
        params_hash: str,
        state: str,
        error_msg: str | None = None,
        precision_before: float | None = None,
        precision_after: float | None = None,
        iterations: int = 0,
        universe_size: int = 0,
        prev_universe_size: int = 0,
        extremum_cnt: int = 0,
        history_window: int = 0,
        active_tail: int = 0,
        precision_metric: str = "mean",
        init_mode: str = "mode0",
        step_size: float = 0.10,
        target_precision: float = 0.95,
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                UPDATE `{self.jobs_table}`
                SET state = :state,
                    error_msg = :error_msg,
                    precision_before = :pr_before,
                    precision_after = :pr_after,
                    iterations = :iters,
                    universe_size = :universe_size,
                    prev_universe_size = :prev_universe_size,
                    extremum_count = :ext_cnt,
                    history_window = :hist,
                    active_tail = :tail,
                    precision_metric = :metric,
                    init_mode = :init_mode,
                    step_size = :step,
                    target_precision = :target,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pair = :pair
                  AND day_flag = :day
                  AND control_date = :dt
                  AND params_hash = :ph
            """), {
                "pair": pair,
                "day": day_flag,
                "dt": control_date,
                "ph": params_hash,
                "state": state,
                "error_msg": error_msg,
                "pr_before": precision_before,
                "pr_after": precision_after,
                "iters": int(iterations),
                "universe_size": int(universe_size),
                "prev_universe_size": int(prev_universe_size),
                "ext_cnt": int(extremum_cnt),
                "hist": int(history_window),
                "tail": int(active_tail),
                "metric": str(precision_metric),
                "init_mode": str(init_mode),
                "step": float(step_size),
                "target": float(target_precision),
            })

    async def train_and_save(
        self,
        *,
        pair: int,
        day_flag: int,
        control_date: datetime,
        params_hash: str,
        np_simple_rates: dict | None,
        active_codes_at: ActiveCodesProvider,
        train_mode: TrainMode = 0,
        max_iter: int = 20,
        step: float = 0.10,
        target_precision: float = 0.95,
        extremum_limit: int = 50,
        extremum_interval: int = 3,
        active_tail: int = 0,
        metric: PrecisionMetric = "mean",
        triggered_by: str = "train",
        log_fn: Callable[[str], None] | None = None,
        skip_db_writes: bool = False,
    ) -> tuple[dict[str, float], float]:
        init_mode = f"mode{train_mode}/iv{extremum_interval}"

        # skip_db_writes=True: fill_cache mode — skip reserve/save/finish DB ops.
        # load_universe still runs to reuse existing training from previous sessions.
        if skip_db_writes:
            prev_loaded = None
            prev_size   = 0
            pr_before   = None
        else:
            prev_loaded = await self.load_universe(
                pair=pair,
                day_flag=day_flag,
                control_date=control_date,
                params_hash=params_hash,
            )
            prev_size = len(prev_loaded[0]) if prev_loaded else 0
            pr_before = float(prev_loaded[1]) if prev_loaded else None

        if skip_db_writes:
            reserved = True   # skip semaphore DB op in fill_cache mode
        else:
            reserved = await self.reserve_slot(
                pair=pair,
                day_flag=day_flag,
                control_date=control_date,
                params_hash=params_hash,
                triggered_by=triggered_by,
                history_window=extremum_limit,
                active_tail=active_tail,
                precision_metric=metric,
                init_mode=init_mode,
                step_size=step,
                target_precision=target_precision,
            )

            if not reserved:
                loaded = await self.load_universe(
                    pair=pair,
                    day_flag=day_flag,
                    control_date=control_date,
                    params_hash=params_hash,
                )
                if loaded:
                    return loaded
                return {}, 0.0

        try:
            universe, pr, iters, ext_cnt, _from_train_cache = await train_at_date(
                np_simple_rates=np_simple_rates,
                control_date=control_date,
                active_codes_at=active_codes_at,
                train_mode=train_mode,
                max_iter=max_iter,
                step=step,
                target_precision=target_precision,
                extremum_limit=extremum_limit,
                extremum_interval=extremum_interval,
                active_tail=active_tail,
                metric=metric,
            )

            # Update in-memory cache immediately
            if universe:
                self._universe_cache[(pair, day_flag, control_date, params_hash)] = (universe, pr)

            # Skip DB writes on cache hit OR in fill_cache mode
            if _from_train_cache or skip_db_writes:
                return universe, pr

            if universe:
                await self.save_universe(
                    pair=pair,
                    day_flag=day_flag,
                    control_date=control_date,
                    params_hash=params_hash,
                    universe=universe,
                    precision_val=pr,
                    iterations=iters,
                    extremum_cnt=ext_cnt,
                    history_window=extremum_limit,
                    active_tail=active_tail,
                    precision_metric=metric,
                    init_mode=init_mode,
                    step_size=step,
                    target_precision=target_precision,
                )

            await self.finish_slot(
                pair=pair,
                day_flag=day_flag,
                control_date=control_date,
                params_hash=params_hash,
                state="done",
                precision_before=pr_before,
                precision_after=pr,
                iterations=iters,
                universe_size=len(universe),
                prev_universe_size=prev_size,
                extremum_cnt=ext_cnt,
                history_window=extremum_limit,
                active_tail=active_tail,
                precision_metric=metric,
                init_mode=init_mode,
                step_size=step,
                target_precision=target_precision,
            )

            if log_fn:
                log_fn(
                    f"  ✅ ml-train {control_date}: "
                    f"mode={train_mode}, interval={extremum_interval}, "
                    f"prec={pr:.4f}, size={len(universe)}, "
                    f"ext={ext_cnt}, iters={iters}"
                )

            return universe, pr

        except Exception as e:
            await self.finish_slot(
                pair=pair,
                day_flag=day_flag,
                control_date=control_date,
                params_hash=params_hash,
                state="failed",
                error_msg=str(e)[:1000],
                precision_before=pr_before,
                iterations=0,
                universe_size=0,
                prev_universe_size=prev_size,
                extremum_cnt=0,
                history_window=extremum_limit,
                active_tail=active_tail,
                precision_metric=metric,
                init_mode=init_mode,
                step_size=step,
                target_precision=target_precision,
            )

            if log_fn:
                log_fn(f"  ❌ ml-train {control_date}: {e}")

            return {}, 0.0

    async def maybe_retrain(
        self,
        *,
        pair: int,
        day_flag: int,
        control_date: datetime,
        params_hash: str,
        np_simple_rates: dict | None,
        active_codes_at: ActiveCodesProvider,
        train_mode: TrainMode = 0,
        max_iter: int = 20,
        step: float = 0.10,
        target_precision: float = 0.95,
        extremum_limit: int = 50,
        extremum_interval: int = 3,
        active_tail: int = 0,
        metric: PrecisionMetric = "mean",
        log_fn: Callable[[str], None] | None = None,
        skip_db_writes: bool = False,
        model_tag: str = "",
    ) -> tuple[dict[str, float], float]:
        """Return a valid ML universe, retraining only when necessary.

        Optimized path:
          • collect extremums + active codes once;
          • build records once;
          • use a per-train-cache-key async lock instead of one global semaphore;
          • train prebuilt records directly, without re-entering train_at_date().
        """
        init_mode = f"mode{train_mode}/iv{extremum_interval}"

        forward = train_mode in (1, 3, 4)
        seq = (
            collect_extremums_forward(
                np_simple_rates, control_date,
                limit=extremum_limit, extremum_interval=extremum_interval,
            )
            if forward else
            collect_extremums_back(
                np_simple_rates, control_date,
                limit=extremum_limit, extremum_interval=extremum_interval,
            )
        )
        seq_tuple = tuple((int(d.timestamp()), s) for d, s in seq)

        seq_cache_key = (model_tag, seq_tuple)
        cached_seq = _seq_codes_cache.get(seq_cache_key)
        if cached_seq is not None:
            pre_codes, codes_key = cached_seq
        else:
            pre_codes: list[list[str]] = []
            for ext_date, _ in seq:
                try:
                    codes = await active_codes_at(ext_date)
                except Exception:
                    codes = []
                pre_codes.append(sorted(set(codes)))
            codes_key = tuple(tuple(c) for c in pre_codes)
            _seq_codes_cache[seq_cache_key] = (pre_codes, codes_key)
            if len(_seq_codes_cache) > _SEQ_CODES_CACHE_MAX:
                del _seq_codes_cache[next(iter(_seq_codes_cache))]

        train_cache_key = (
            seq_tuple, codes_key, train_mode, max_iter,
            round(step, 6), round(target_precision, 6), active_tail, metric,
        )

        cached_train = _train_at_date_cache.get(train_cache_key)
        if cached_train is not None:
            universe, pr, _, _ = cached_train
            self._universe_cache[(pair, day_flag, control_date, params_hash)] = (universe, pr)
            return universe, pr

        records = _build_records_from_seq_codes(
            seq=seq,
            pre_codes=pre_codes,
            np_simple_rates=np_simple_rates,
            train_mode=train_mode,
        )
        if not records:
            return {}, 1.0

        lock = await self._get_train_lock(train_cache_key)
        async with lock:
            # Double-check after waiting: another request may have trained this exact universe.
            cached_train = _train_at_date_cache.get(train_cache_key)
            if cached_train is not None:
                universe, pr, _, _ = cached_train
                self._universe_cache[(pair, day_flag, control_date, params_hash)] = (universe, pr)
                return universe, pr

            loaded = None if skip_db_writes else await self.load_universe(
                pair=pair,
                day_flag=day_flag,
                control_date=control_date,
                params_hash=params_hash,
            )
            prev_size = len(loaded[0]) if loaded else 0
            pr_before = float(loaded[1]) if loaded else None

            if loaded:
                universe_loaded, stored_pr = loaded
                current_pr = total_precision(universe_loaded, records, metric=metric)
                if current_pr >= target_precision:
                    return universe_loaded, current_pr

                if log_fn:
                    log_fn(
                        f"  ⚠️ ml precision drop {control_date}: "
                        f"stored={stored_pr:.4f}, current={current_pr:.4f}, "
                        f"target={target_precision:.4f}; retrain"
                    )

            reserved = True
            if not skip_db_writes:
                reserved = await self.reserve_slot(
                    pair=pair,
                    day_flag=day_flag,
                    control_date=control_date,
                    params_hash=params_hash,
                    triggered_by="retrain",
                    history_window=extremum_limit,
                    active_tail=active_tail,
                    precision_metric=metric,
                    init_mode=init_mode,
                    step_size=step,
                    target_precision=target_precision,
                )
                if not reserved:
                    loaded_after_wait = await self.load_universe(
                        pair=pair,
                        day_flag=day_flag,
                        control_date=control_date,
                        params_hash=params_hash,
                    )
                    if loaded_after_wait:
                        return loaded_after_wait
                    return {}, 0.0

            try:
                universe, pr, iters, ext_cnt, from_train_cache = await train_prebuilt_records(
                    records=records,
                    cache_key=train_cache_key,
                    max_iter=max_iter,
                    step=step,
                    target_precision=target_precision,
                    active_tail=active_tail,
                    metric=metric,
                )

                if universe:
                    self._universe_cache[(pair, day_flag, control_date, params_hash)] = (universe, pr)

                if skip_db_writes:
                    return universe, pr

                if universe and not from_train_cache:
                    await self.save_universe(
                        pair=pair,
                        day_flag=day_flag,
                        control_date=control_date,
                        params_hash=params_hash,
                        universe=universe,
                        precision_val=pr,
                        iterations=iters,
                        extremum_cnt=ext_cnt,
                        history_window=extremum_limit,
                        active_tail=active_tail,
                        precision_metric=metric,
                        init_mode=init_mode,
                        step_size=step,
                        target_precision=target_precision,
                    )

                await self.finish_slot(
                    pair=pair,
                    day_flag=day_flag,
                    control_date=control_date,
                    params_hash=params_hash,
                    state="done",
                    precision_before=pr_before,
                    precision_after=pr,
                    iterations=iters,
                    universe_size=len(universe),
                    prev_universe_size=prev_size,
                    extremum_cnt=ext_cnt,
                    history_window=extremum_limit,
                    active_tail=active_tail,
                    precision_metric=metric,
                    init_mode=init_mode,
                    step_size=step,
                    target_precision=target_precision,
                )

                if log_fn:
                    log_fn(
                        f"  ✅ ml-train {control_date}: "
                        f"mode={train_mode}, interval={extremum_interval}, "
                        f"prec={pr:.4f}, size={len(universe)}, "
                        f"ext={ext_cnt}, iters={iters}"
                    )

                return universe, pr

            except Exception as e:
                if not skip_db_writes:
                    await self.finish_slot(
                        pair=pair,
                        day_flag=day_flag,
                        control_date=control_date,
                        params_hash=params_hash,
                        state="failed",
                        error_msg=str(e)[:1000],
                        precision_before=pr_before,
                        iterations=0,
                        universe_size=0,
                        prev_universe_size=prev_size,
                        extremum_cnt=0,
                        history_window=extremum_limit,
                        active_tail=active_tail,
                        precision_metric=metric,
                        init_mode=init_mode,
                        step_size=step,
                        target_precision=target_precision,
                    )

                if log_fn:
                    log_fn(f"  ❌ ml-train {control_date}: {e}")

                return {}, 0.0
