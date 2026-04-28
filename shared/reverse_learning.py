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
# Сборка экстремумов от control_date НАЗАД
# ──────────────────────────────────────────────────────────────────────────────

def collect_extremums_back(
    np_simple_rates: dict | None,
    control_date:    datetime,
    *,
    limit: int = 50,
) -> list[tuple[datetime, int]]:
    """[(date, sign)] от ближайшего к control_date к более старым. limit ограничивает длину."""
    if not np_simple_rates:
        return []
    dates_ns = np_simple_rates["dates_ns"]
    ext_max  = np_simple_rates["ext_max"]
    ext_min  = np_simple_rates["ext_min"]
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


def _diff_amp(np_simple_rates: dict | None,
              date_a: datetime, date_b: datetime | None) -> float:
    if not np_simple_rates:
        return 1.0
    dates_ns = np_simple_rates["dates_ns"]
    close    = np_simple_rates["close"]
    n        = len(close)
    if n == 0:
        return 1.0
    ia = min(int(np.searchsorted(dates_ns, np.int64(int(date_a.timestamp())))), n - 1)
    if date_b is None:
        return abs(float(close[ia])) or 1.0
    ib = min(int(np.searchsorted(dates_ns, np.int64(int(date_b.timestamp())))), n - 1)
    return abs(float(close[ia] - close[ib])) or 1.0


# ──────────────────────────────────────────────────────────────────────────────
# Основной пайплайн обучения (чистая алгоритмика, без БД)
# ──────────────────────────────────────────────────────────────────────────────

ActiveCodesProvider = Callable[[datetime], Awaitable[list[str]]]


async def train_at_date(
    *,
    np_simple_rates:  dict | None,
    control_date:     datetime,
    active_codes_at:  ActiveCodesProvider,
    init_mode:        InitMode = "constant",
    max_iter:         int   = 20,
    step:             float = 0.10,
    target_precision: float = 0.95,
    extremum_limit:   int   = 50,
    active_tail:      int   = 0,
    metric:           PrecisionMetric = "mean",
) -> tuple[dict[str, float], float, int, int]:
    """
    Returns: (universe, final_precision, iterations_total, extremum_count).
    """
    seq = collect_extremums_back(np_simple_rates, control_date, limit=extremum_limit)
    if not seq:
        return {}, 1.0, 0, 0

    records: list[ExtremumRecord] = []
    for idx, (ext_date, sign) in enumerate(seq):
        try:
            codes = await active_codes_at(ext_date)
        except Exception:
            codes = []
        if not codes:
            continue
        amp = (_diff_amp(np_simple_rates, ext_date,
                         seq[idx + 1][0] if idx + 1 < len(seq) else None)
               if init_mode == "diff" else 1.0)
        records.append(ExtremumRecord(
            date=ext_date, sign=sign, codes=list(codes), base_amp=amp))

    if not records:
        return {}, 1.0, 0, 0

    universe: dict[str, float]    = {}
    history:  list[ExtremumRecord] = []
    iters_total = 0

    for rec in records:
        had_inter = any(c in universe for c in rec.codes)
        for c in rec.codes:
            if c not in universe:
                universe[c] = float(rec.sign) * rec.base_amp
        history.append(rec)

        if had_inter:
            universe, _pr, iters = rebalance_weights(
                universe, history,
                max_iter=max_iter, step=step, target=target_precision,
                active_tail=active_tail, metric=metric)
            iters_total += iters

    final_pr = total_precision(universe, records, metric=metric)
    return universe, final_pr, iters_total, len(records)


# ──────────────────────────────────────────────────────────────────────────────
# ReverseStore: per-service таблицы и весь БД-API
# ──────────────────────────────────────────────────────────────────────────────

class ReverseStore:
    """
    Per-service хранилище. Имена таблиц параметризованы PORT-ом сервиса
    (по аналогии с vlad_values_cache_svc{PORT}).

        store = ReverseStore(engine, port=8888)
        await store.ensure_tables()
        ...
    """

    def __init__(self, engine, *, port: int):
        self.engine     = engine
        self.t_universe = f"vlad_reverse_universe_svc{port}"
        self.t_jobs     = f"vlad_reverse_jobs_svc{port}"

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def ensure_tables(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{self.t_universe}` (
                    id               BIGINT       NOT NULL AUTO_INCREMENT,
                    pair             TINYINT      NOT NULL,
                    day_flag         TINYINT      NOT NULL,
                    control_date     DATETIME     NOT NULL,
                    params_hash      CHAR(32)     NOT NULL,
                    universe_json    LONGTEXT     NOT NULL,
                    precision_val    DECIMAL(8,5) NOT NULL DEFAULT 0,
                    iterations       SMALLINT     NOT NULL DEFAULT 0,
                    extremum_cnt     INT          NOT NULL DEFAULT 0,
                    -- Поля конфига обучения (полезны для последующей оптимизации):
                    history_window   INT          NOT NULL DEFAULT 0,
                    active_tail      INT          NOT NULL DEFAULT 0,
                    precision_metric VARCHAR(16)  NOT NULL DEFAULT 'mean',
                    init_mode        VARCHAR(16)  NOT NULL DEFAULT 'constant',
                    step_size        DECIMAL(5,3) NOT NULL DEFAULT 0.100,
                    target_precision DECIMAL(5,3) NOT NULL DEFAULT 0.950,
                    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                                  ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE KEY uq_universe (pair, day_flag, control_date, params_hash),
                    INDEX idx_lookup (pair, day_flag, params_hash, control_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{self.t_jobs}` (
                    id                 BIGINT       NOT NULL AUTO_INCREMENT,
                    pair               TINYINT      NOT NULL,
                    day_flag           TINYINT      NOT NULL,
                    control_date       DATETIME     NOT NULL,
                    params_hash        CHAR(32)     NOT NULL,
                    state              ENUM('pending','running','done','failed')
                                       NOT NULL DEFAULT 'pending',
                    triggered_by       ENUM('train','retrain','manual')
                                       NOT NULL DEFAULT 'train',
                    precision_before   DECIMAL(8,5) DEFAULT NULL,
                    precision_after    DECIMAL(8,5) DEFAULT NULL,
                    iterations         SMALLINT     NOT NULL DEFAULT 0,
                    universe_size      INT          NOT NULL DEFAULT 0,
                    prev_universe_size INT          NOT NULL DEFAULT 0,
                    extremum_cnt       INT          NOT NULL DEFAULT 0,
                    -- Конфиг — повторно для аудита:
                    history_window     INT          NOT NULL DEFAULT 0,
                    active_tail        INT          NOT NULL DEFAULT 0,
                    precision_metric   VARCHAR(16)  NOT NULL DEFAULT 'mean',
                    init_mode          VARCHAR(16)  NOT NULL DEFAULT 'constant',
                    step_size          DECIMAL(5,3) NOT NULL DEFAULT 0.100,
                    target_precision   DECIMAL(5,3) NOT NULL DEFAULT 0.950,
                    started_at         DATETIME     NULL,
                    finished_at        DATETIME     NULL,
                    error_msg          TEXT         NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY uq_job (pair, day_flag, control_date, params_hash),
                    INDEX idx_state (state, started_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

    # ── Universe persistence ──────────────────────────────────────────────────

    async def save_universe(
        self, *, pair: int, day_flag: int, control_date: datetime,
        params_hash: str, universe: dict[str, float], precision_val: float,
        iterations: int, extremum_cnt: int,
        history_window: int = 0, active_tail: int = 0,
        precision_metric: str = "mean", init_mode: str = "constant",
        step_size: float = 0.10, target_precision: float = 0.95,
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO `{self.t_universe}`
                    (pair, day_flag, control_date, params_hash,
                     universe_json, precision_val, iterations, extremum_cnt,
                     history_window, active_tail, precision_metric,
                     init_mode, step_size, target_precision)
                VALUES (:pair, :day, :cd, :ph,
                        :uj, :pv, :it, :ec,
                        :hw, :at, :pm,
                        :im, :ss, :tp)
                ON DUPLICATE KEY UPDATE
                    universe_json    = VALUES(universe_json),
                    precision_val    = VALUES(precision_val),
                    iterations       = VALUES(iterations),
                    extremum_cnt     = VALUES(extremum_cnt),
                    history_window   = VALUES(history_window),
                    active_tail      = VALUES(active_tail),
                    precision_metric = VALUES(precision_metric),
                    init_mode        = VALUES(init_mode),
                    step_size        = VALUES(step_size),
                    target_precision = VALUES(target_precision)
            """), {"pair": pair, "day": day_flag, "cd": control_date,
                   "ph": params_hash,
                   "uj": _json.dumps(universe, ensure_ascii=False),
                   "pv": float(precision_val), "it": int(iterations),
                   "ec": int(extremum_cnt),
                   "hw": int(history_window), "at": int(active_tail),
                   "pm": precision_metric, "im": init_mode,
                   "ss": float(step_size), "tp": float(target_precision)})

    async def load_universe(
        self, *, pair: int, day_flag: int, control_date: datetime,
        params_hash: str,
    ) -> tuple[dict[str, float] | None, float, int]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(text(f"""
                SELECT universe_json, precision_val, extremum_cnt
                FROM `{self.t_universe}`
                WHERE pair=:pair AND day_flag=:day
                  AND control_date=:cd AND params_hash=:ph
            """), {"pair": pair, "day": day_flag,
                   "cd": control_date, "ph": params_hash})).fetchone()
        if not row:
            return None, 0.0, 0
        try:
            u = _json.loads(row[0])
        except Exception:
            return None, 0.0, 0
        return u, float(row[1] or 0), int(row[2] or 0)

    async def load_latest_universe(
        self, *, pair: int, day_flag: int, params_hash: str,
        on_or_before: datetime,
    ) -> tuple[dict[str, float] | None, float, datetime | None]:
        async with self.engine.connect() as conn:
            row = (await conn.execute(text(f"""
                SELECT universe_json, precision_val, control_date
                FROM `{self.t_universe}`
                WHERE pair=:pair AND day_flag=:day AND params_hash=:ph
                  AND control_date <= :cd
                ORDER BY control_date DESC
                LIMIT 1
            """), {"pair": pair, "day": day_flag, "ph": params_hash,
                   "cd": on_or_before})).fetchone()
        if not row:
            return None, 0.0, None
        try:
            u = _json.loads(row[0])
        except Exception:
            return None, 0.0, None
        return u, float(row[1] or 0), row[2]

    # ── Семафор задач перевешивания ──────────────────────────────────────────

    async def acquire_slot(
        self, *, pair: int, day_flag: int, control_date: datetime,
        params_hash: str, triggered_by: str = "train",
        stale_after_secs: int = 300,
    ) -> bool:
        """
        True  — задача наша (вставили или перехватили зависшую/готовую).
        False — выполняется кем-то другим прямо сейчас.
        """
        p = {"pair": pair, "day": day_flag, "cd": control_date,
             "ph": params_hash, "tb": triggered_by}
        async with self.engine.begin() as conn:
            r = await conn.execute(text(f"""
                INSERT IGNORE INTO `{self.t_jobs}`
                    (pair, day_flag, control_date, params_hash,
                     state, triggered_by, started_at)
                VALUES (:pair, :day, :cd, :ph, 'running', :tb, NOW())
            """), p)
            if r.rowcount > 0:
                return True

            row = (await conn.execute(text(f"""
                SELECT state, started_at FROM `{self.t_jobs}`
                WHERE pair=:pair AND day_flag=:day
                  AND control_date=:cd AND params_hash=:ph
            """), p)).fetchone()
            if not row:
                return False

            state, started = row[0], row[1]
            if state in ('done', 'failed'):
                await conn.execute(text(f"""
                    UPDATE `{self.t_jobs}`
                    SET state='running', triggered_by=:tb, started_at=NOW(),
                        finished_at=NULL, error_msg=NULL
                    WHERE pair=:pair AND day_flag=:day
                      AND control_date=:cd AND params_hash=:ph
                """), p)
                return True
            if state == 'running' and started:
                if (datetime.now() - started).total_seconds() > stale_after_secs:
                    await conn.execute(text(f"""
                        UPDATE `{self.t_jobs}`
                        SET started_at=NOW(), error_msg='reclaimed-stale',
                            triggered_by=:tb
                        WHERE pair=:pair AND day_flag=:day
                          AND control_date=:cd AND params_hash=:ph
                    """), p)
                    return True
            return False

    async def finish_slot(
        self, *, pair: int, day_flag: int, control_date: datetime,
        params_hash: str, state: str = 'done',
        precision_before: float | None = None,
        precision_after:  float | None = None,
        iterations: int = 0, universe_size: int = 0,
        prev_universe_size: int = 0, extremum_cnt: int = 0,
        history_window: int = 0, active_tail: int = 0,
        precision_metric: str = "mean", init_mode: str = "constant",
        step_size: float = 0.10, target_precision: float = 0.95,
        error_msg: str | None = None,
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                UPDATE `{self.t_jobs}`
                SET state=:st, finished_at=NOW(),
                    precision_before=:pb, precision_after=:pa,
                    iterations=:it, universe_size=:us,
                    prev_universe_size=:pus, extremum_cnt=:ec,
                    history_window=:hw, active_tail=:at,
                    precision_metric=:pm, init_mode=:im,
                    step_size=:ss, target_precision=:tp,
                    error_msg=:em
                WHERE pair=:pair AND day_flag=:day
                  AND control_date=:cd AND params_hash=:ph
            """), {"st": state, "pb": precision_before, "pa": precision_after,
                   "it": iterations, "us": universe_size, "pus": prev_universe_size,
                   "ec": extremum_cnt, "hw": history_window, "at": active_tail,
                   "pm": precision_metric, "im": init_mode,
                   "ss": float(step_size), "tp": float(target_precision),
                   "em": error_msg, "pair": pair, "day": day_flag,
                   "cd": control_date, "ph": params_hash})

    # ── Высокоуровневое API под семафором ─────────────────────────────────────

    async def train_and_persist(
        self, *,
        pair: int, day_flag: int, control_date: datetime, params_hash: str,
        np_simple_rates: dict | None, active_codes_at: ActiveCodesProvider,
        init_mode:        InitMode        = "constant",
        max_iter:         int             = 20,
        step:             float           = 0.10,
        target_precision: float           = 0.95,
        extremum_limit:   int             = 50,
        active_tail:      int             = 0,
        metric:           PrecisionMetric = "mean",
        triggered_by:     str             = "train",
        log_fn:           Callable[[str], None] | None = None,
    ) -> tuple[dict[str, float], float, int, int, str]:
        """
        Returns: (universe, precision, iterations, extremum_cnt, status).
            status ∈ {'trained', 'reused', 'busy-empty'}.
        """
        got = await self.acquire_slot(
            pair=pair, day_flag=day_flag, control_date=control_date,
            params_hash=params_hash, triggered_by=triggered_by)

        if not got:
            for _ in range(20):                     # ждём ~10 сек чужого воркера
                await asyncio.sleep(0.5)
                u, pr, ec = await self.load_universe(
                    pair=pair, day_flag=day_flag,
                    control_date=control_date, params_hash=params_hash)
                if u is not None:
                    return u, pr, 0, ec, 'reused'
            return {}, 0.0, 0, 0, 'busy-empty'

        u_prev, pr_prev, _ = await self.load_universe(
            pair=pair, day_flag=day_flag,
            control_date=control_date, params_hash=params_hash)
        pr_before = pr_prev if u_prev is not None else None
        prev_size = len(u_prev) if u_prev is not None else 0

        try:
            universe, pr, iters, ext_cnt = await train_at_date(
                np_simple_rates=np_simple_rates,
                control_date=control_date,
                active_codes_at=active_codes_at,
                init_mode=init_mode, max_iter=max_iter, step=step,
                target_precision=target_precision,
                extremum_limit=extremum_limit,
                active_tail=active_tail, metric=metric)

            if universe:
                await self.save_universe(
                    pair=pair, day_flag=day_flag, control_date=control_date,
                    params_hash=params_hash, universe=universe,
                    precision_val=pr, iterations=iters, extremum_cnt=ext_cnt,
                    history_window=extremum_limit, active_tail=active_tail,
                    precision_metric=metric, init_mode=init_mode,
                    step_size=step, target_precision=target_precision)
            await self.finish_slot(
                pair=pair, day_flag=day_flag, control_date=control_date,
                params_hash=params_hash, state='done',
                precision_before=pr_before, precision_after=pr,
                iterations=iters, universe_size=len(universe),
                prev_universe_size=prev_size, extremum_cnt=ext_cnt,
                history_window=extremum_limit, active_tail=active_tail,
                precision_metric=metric, init_mode=init_mode,
                step_size=step, target_precision=target_precision)
            if log_fn:
                log_fn(f"  ✅ ml-train {control_date}: prec={pr:.4f} "
                       f"iters={iters} ext={ext_cnt} |U|={len(universe)}")
            return universe, pr, iters, ext_cnt, 'trained'

        except Exception as e:
            await self.finish_slot(
                pair=pair, day_flag=day_flag, control_date=control_date,
                params_hash=params_hash, state='failed',
                error_msg=str(e)[:1000],
                history_window=extremum_limit, active_tail=active_tail,
                precision_metric=metric, init_mode=init_mode,
                step_size=step, target_precision=target_precision)
            if log_fn:
                log_fn(f"  ❌ ml-train {control_date}: {e}")
            raise

    async def maybe_retrain(
        self, *,
        pair: int, day_flag: int, control_date: datetime, params_hash: str,
        np_simple_rates: dict | None, active_codes_at: ActiveCodesProvider,
        init_mode:        InitMode        = "constant",
        max_iter:         int             = 20,
        step:             float           = 0.10,
        target_precision: float           = 0.95,
        extremum_limit:   int             = 50,
        active_tail:      int             = 0,
        metric:           PrecisionMetric = "mean",
        log_fn:           Callable[[str], None] | None = None,
    ) -> tuple[dict[str, float], float]:
        """
        Smart: переиспользует свежий универсум, если на новой control_date
        precision держится ≥ target. Иначе — перетренировка под семафором.
        """
        u_prev, _, prev_dt = await self.load_latest_universe(
            pair=pair, day_flag=day_flag, params_hash=params_hash,
            on_or_before=control_date)

        if u_prev is not None:
            seq = collect_extremums_back(np_simple_rates, control_date,
                                         limit=extremum_limit)
            records: list[ExtremumRecord] = []
            for ext_date, sign in seq:
                try:
                    codes = await active_codes_at(ext_date)
                except Exception:
                    codes = []
                if codes:
                    records.append(ExtremumRecord(
                        date=ext_date, sign=sign, codes=list(codes)))
            if records:
                pr_now = total_precision(u_prev, records, metric=metric)
                if pr_now >= target_precision:
                    if log_fn:
                        log_fn(f"  ↻ ml-reuse {control_date} "
                               f"(prev={prev_dt}, prec={pr_now:.4f})")
                    return u_prev, pr_now

        universe, pr, _it, _ec, _st = await self.train_and_persist(
            pair=pair, day_flag=day_flag, control_date=control_date,
            params_hash=params_hash, np_simple_rates=np_simple_rates,
            active_codes_at=active_codes_at, init_mode=init_mode,
            max_iter=max_iter, step=step,
            target_precision=target_precision, extremum_limit=extremum_limit,
            active_tail=active_tail, metric=metric,
            triggered_by="retrain", log_fn=log_fn)
        return universe, pr
