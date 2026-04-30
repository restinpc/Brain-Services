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
import json as _json
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Literal

import numpy as np
from sqlalchemy import text


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
# Сборка экстремумов от control_date НАЗАД / ВПЕРЁД
# ──────────────────────────────────────────────────────────────────────────────

# Cache for extremum flags: id(np_simple_rates) + interval -> (ext_max, ext_min)
# np_simple_rates dict doesn't change during fill_cache, so this is safe.
_extremum_flags_cache: dict = {}


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


def collect_extremums_back(
    np_simple_rates: dict | None,
    control_date:    datetime,
    *,
    limit: int = 50,
    extremum_interval: int = 3,
) -> list[tuple[datetime, int]]:
    """[(date, sign)] от ближайшего к control_date к более старым."""
    if not np_simple_rates:
        return []

    dates_ns = np_simple_rates["dates_ns"]
    ext_max, ext_min = _extremum_flags(
        np_simple_rates,
        interval=extremum_interval,
    )

    ts       = np.int64(int(control_date.timestamp()))
    cutoff   = int(np.searchsorted(dates_ns, ts, side="right"))

    out: list[tuple[datetime, int]] = []
    for i in range(cutoff - 1, -1, -1):
        if ext_max[i]:
            out.append((datetime.fromtimestamp(int(dates_ns[i])), -1))
        elif ext_min[i]:
            out.append((datetime.fromtimestamp(int(dates_ns[i])), +1))
        if len(out) >= limit:
            break
    return out


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
    Знак: ext_max → -1, ext_min → +1.
    """
    if not np_simple_rates:
        return []

    dates_ns = np_simple_rates["dates_ns"]
    ext_max, ext_min = _extremum_flags(
        np_simple_rates,
        interval=extremum_interval,
    )

    ts       = np.int64(int(control_date.timestamp()))
    cutoff   = int(np.searchsorted(dates_ns, ts, side="right"))

    out: list[tuple[datetime, int]] = []
    for i in range(cutoff):
        if ext_max[i]:
            out.append((datetime.fromtimestamp(int(dates_ns[i])), -1))
        elif ext_min[i]:
            out.append((datetime.fromtimestamp(int(dates_ns[i])), +1))

    return out[-limit:] if len(out) > limit else out


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

    pre_codes: list[list[str]] = []
    for ext_date, _ in seq:
        try:
            codes = await active_codes_at(ext_date)
        except Exception:
            codes = []
        pre_codes.append(sorted(set(codes)))

    codes_key = tuple(tuple(c) for c in pre_codes)
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

    universe: dict[str, float] = {}
    all_seen: set[str] = set()
    iterations_total = 0

    for rec_idx, rec in enumerate(records):
        rec_set = set(rec.codes)
        overlap = bool(rec_set & all_seen)

        if not overlap:
            # Новое непересекающееся подмножество.
            for c in rec.codes:
                if c not in universe:
                    universe[c] = rec.sign * rec.base_amp
            all_seen |= rec_set
            continue

        # Пересечение с уже обученным множеством.
        # Сначала добавляем отсутствующие веса с базовым знаком.
        for c in rec.codes:
            if c not in universe:
                universe[c] = rec.sign * rec.base_amp

        all_seen |= rec_set

        universe, _pr, iters = rebalance_weights(
            universe,
            records[:rec_idx + 1],
            max_iter=max_iter,
            step=step,
            target=target_precision,
            active_tail=active_tail,
            metric=metric,
        )
        iterations_total += iters

    final_precision = total_precision(universe, records, metric=metric)

    if final_precision < target_precision:
        universe, final_precision, iters = rebalance_weights(
            universe,
            records,
            max_iter=max_iter,
            step=step,
            target=target_precision,
            active_tail=active_tail,
            metric=metric,
        )
        iterations_total += iters

    # Отбрасываем численный мусор.
    universe = {
        k: float(v)
        for k, v in universe.items()
        if abs(float(v)) > 1e-12
    }

    result = (universe, float(final_precision), int(iterations_total), len(records))
    _train_at_date_cache[cache_key] = result
    # Keep cache bounded: drop oldest entries beyond 2000
    if len(_train_at_date_cache) > 2000:
        oldest = next(iter(_train_at_date_cache))
        del _train_at_date_cache[oldest]
    return (*result, False)   # False = computed fresh, not from cache


# ──────────────────────────────────────────────────────────────────────────────
# ReverseStore: БД-хранилище обученных universe + jobs-семафор
# ──────────────────────────────────────────────────────────────────────────────

class ReverseStore:
    def __init__(self, engine, *, port: int):
        self.engine = engine
        self.port = int(port)
        self.universe_table = f"vlad_reverse_universe_svc{self.port}"
        self.jobs_table = f"vlad_reverse_jobs_svc{self.port}"
        # In-memory cache: (pair, day_flag, control_date, params_hash) -> (universe, precision)
        # Eliminates repeated DB round-trips for the same key during fill_cache
        self._universe_cache: dict = {}

    def clear_universe_cache(self) -> None:
        """Call at the start of fill_cache to start fresh."""
        self._universe_cache.clear()

    async def ensure_tables(self) -> None:
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

        async with self.engine.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT universe_json, precision_val
                FROM `{self.universe_table}`
                WHERE pair = :pair
                  AND day_flag = :day
                  AND control_date = :dt
                  AND params_hash = :ph
                LIMIT 1
            """), {
                "pair": pair,
                "day": day_flag,
                "dt": control_date,
                "ph": params_hash,
            })

            row = res.mappings().first()

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
    ) -> tuple[dict[str, float], float]:
        # ── Fast pre-check: compute extremum seq+codes, check train cache ──────
        # If hit: skip ALL DB operations (load, reserve, save, finish).
        # Adjacent candles share identical extremum sets ~50% of the time.
        _forward_pre = train_mode in (1, 3, 4)
        _seq_pre = (
            collect_extremums_forward(
                np_simple_rates, control_date,
                limit=extremum_limit, extremum_interval=extremum_interval,
            )
            if _forward_pre else
            collect_extremums_back(
                np_simple_rates, control_date,
                limit=extremum_limit, extremum_interval=extremum_interval,
            )
        )
        _pre_codes: list[list[str]] = []
        for _ext_date, _ in _seq_pre:
            try:
                _c = await active_codes_at(_ext_date)
            except Exception:
                _c = []
            _pre_codes.append(sorted(set(_c)))

        _seq_tuple = tuple((int(d.timestamp()), s) for d, s in _seq_pre)
        _codes_key  = tuple(tuple(c) for c in _pre_codes)
        _train_cache_key = (
            _seq_tuple, _codes_key, train_mode, max_iter,
            round(step, 6), round(target_precision, 6), active_tail, metric,
        )
        _cached_train = _train_at_date_cache.get(_train_cache_key)
        if _cached_train is not None:
            _univ_cached, _pr_cached, _, _ = _cached_train
            # Populate universe cache so load_universe skips DB next time too
            self._universe_cache[(pair, day_flag, control_date, params_hash)] = (
                _univ_cached, _pr_cached
            )
            return _univ_cached, _pr_cached
        # ────────────────────────────────────────────────────────────────────────

        loaded = await self.load_universe(
            pair=pair,
            day_flag=day_flag,
            control_date=control_date,
            params_hash=params_hash,
        )

        if loaded:
            universe, stored_pr = loaded

            # Быстрая проверка актуальной precision на текущей control_date.
            # Если упала ниже target, запускаем retrain.
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

            records: list[ExtremumRecord] = []

            for idx, (ext_date, sign) in enumerate(seq):
                try:
                    codes = await active_codes_at(ext_date)
                except Exception:
                    codes = []

                if not codes:
                    continue

                # Для проверки уже сохранённого universe амплитуда не влияет
                # на знак precision, но для一致ности с train_at_date считаем её.
                if train_mode in (2, 3, 4):
                    has_next = idx + 1 < len(seq)
                    next_date = seq[idx + 1][0] if has_next else None
                    if train_mode == 4 and not has_next:
                        continue
                    amp = _diff_amp(np_simple_rates, ext_date, next_date)
                else:
                    amp = 1.0

                records.append(
                    ExtremumRecord(
                        date=ext_date,
                        sign=sign,
                        codes=sorted(set(codes)),
                        base_amp=float(amp),
                    )
                )

            current_pr = total_precision(universe, records, metric=metric)

            if current_pr >= target_precision:
                return universe, current_pr

            if log_fn:
                log_fn(
                    f"  ⚠️ ml precision drop {control_date}: "
                    f"stored={stored_pr:.4f}, current={current_pr:.4f}, "
                    f"target={target_precision:.4f}; retrain"
                )

        return await self.train_and_save(
            pair=pair,
            day_flag=day_flag,
            control_date=control_date,
            params_hash=params_hash,
            np_simple_rates=np_simple_rates,
            active_codes_at=active_codes_at,
            train_mode=train_mode,
            max_iter=max_iter,
            step=step,
            target_precision=target_precision,
            extremum_limit=extremum_limit,
            extremum_interval=extremum_interval,
            active_tail=active_tail,
            metric=metric,
            triggered_by="retrain",
            log_fn=log_fn,
            skip_db_writes=skip_db_writes,
        )
