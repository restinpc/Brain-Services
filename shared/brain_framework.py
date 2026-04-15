"""
brain_framework.py v11 — полная поддержка backtest в IS_SIMPLE режиме.
С патчем: per-service таблицы кеша (vlad_values_cache_svc{PORT}).

server.py для всех сервисов одинаковый:
    from brain_framework import build_app
"""

from __future__ import annotations

import asyncio
import bisect
import inspect
import json as _json
import math
import os
import random
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import numpy as np

from fastapi import FastAPI, Query
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)

from common import (
    MODE, IS_DEV,
    log, send_error_trace,
    ok_response, err_response,
    resolve_workers, build_engines,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values


# 
#  NUMPY-УТИЛИТЫ 
# 

def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _build_np_rates_for_table(rates, candle_ranges, extremums):
    if not rates:
        return None
    sorted_dates = sorted(rates.keys())
    n = len(sorted_dates)
    dates_ns   = np.array([_dt_to_ts(d) for d in sorted_dates], dtype=np.int64)
    t1_arr     = np.array([rates.get(d, 0.0) for d in sorted_dates], dtype=np.float64)
    ranges_arr = np.array([candle_ranges.get(d, 0.0) for d in sorted_dates], dtype=np.float64)
    ext_min_set = extremums.get("min", set())
    ext_max_set = extremums.get("max", set())
    ext_min_arr = np.fromiter((d in ext_min_set for d in sorted_dates), dtype=bool, count=n)
    ext_max_arr = np.fromiter((d in ext_max_set for d in sorted_dates), dtype=bool, count=n)
    return {
        "dates_ns": dates_ns, "t1": t1_arr, "ranges": ranges_arr,
        "ext_min": ext_min_arr, "ext_max": ext_max_arr,
    }


# 
#  ВЫЧИСЛИТЕЛЬНЫЕ УТИЛИТЫ (Python-fallback) 
# 

def _compute_t1(t_dates, ram_rates, candle_ranges, avg_range):
    return sum(ram_rates.get(d, 0.0) for d in t_dates)


def _compute_extremum(t_dates, ext_set, candle_ranges, avg_range, modification, total_hist):
    if not t_dates or total_hist == 0:
        return None
    val = ((sum(1 for d in t_dates if d in ext_set) / total_hist) * 2 - 1) * modification
    return val if val != 0 else None


# 
#  ПУБЛИЧНЫЕ ХЕЛПЕРЫ ДЛЯ rebuild_index() 
# 

async def ensure_ctx_table(engine, table: str, extra_columns: str = "") -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                `id`               INT         NOT NULL AUTO_INCREMENT,
                {extra_columns}
                `fingerprint_hash` CHAR(32)    NOT NULL DEFAULT '',
                `occurrence_count` INT         NOT NULL DEFAULT 0,
                `first_dt`         DATETIME    NULL,
                `last_dt`          DATETIME    NULL,
                `updated_at`       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
                                               ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_fingerprint` (`fingerprint_hash`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def upsert_ctx_rows(engine, table: str, rows: list[dict]) -> dict[str, int]:
    if not rows:
        return {}
    _reserved = {"fingerprint_hash", "occurrence_count", "first_dt", "last_dt"}
    extra_cols   = [k for k in rows[0] if k not in _reserved]
    extra_insert = (", ".join(f"`{c}`" for c in extra_cols) + ", ") if extra_cols else ""
    extra_vals   = (", ".join(f":{c}" for c in extra_cols) + ", ") if extra_cols else ""
    async with engine.begin() as conn:
        for row in rows:
            params = {"fp": row["fingerprint_hash"], "cnt": row["occurrence_count"],
                      "fd": row.get("first_dt"), "ld": row.get("last_dt")}
            for c in extra_cols:
                params[c] = row.get(c)
            await conn.execute(text(f"""
                INSERT INTO `{table}`
                    ({extra_insert}`fingerprint_hash`, `occurrence_count`, `first_dt`, `last_dt`)
                VALUES ({extra_vals}:fp, :cnt, :fd, :ld)
                ON DUPLICATE KEY UPDATE
                    occurrence_count = occurrence_count + :cnt,
                    last_dt  = IF(:ld > last_dt  OR last_dt  IS NULL, :ld,  last_dt),
                    first_dt = IF(:fd < first_dt OR first_dt IS NULL, :fd, first_dt)
            """), params)
    fp_list  = [r["fingerprint_hash"] for r in rows]
    fp_to_id: dict[str, int] = {}
    async with engine.connect() as conn:
        for i in range(0, len(fp_list), 500):
            batch = fp_list[i:i + 500]
            placeholders = ", ".join(f":fp{j}" for j in range(len(batch)))
            params = {f"fp{j}": fp for j, fp in enumerate(batch)}
            res = await conn.execute(text(
                f"SELECT id, fingerprint_hash FROM `{table}` "
                f"WHERE fingerprint_hash IN ({placeholders})"), params)
            for r in res.fetchall():
                fp_to_id[r[1]] = r[0]
    return fp_to_id


async def ensure_weights_table(engine, table: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                `id`          INT         NOT NULL AUTO_INCREMENT,
                `weight_code` VARCHAR(64) NOT NULL,
                `ctx_id`      INT         NOT NULL,
                `mode`        TINYINT     NOT NULL DEFAULT 0,
                `shift`       SMALLINT    NOT NULL DEFAULT 0,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_weight_code` (`weight_code`),
                INDEX idx_ctx_id (`ctx_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def insert_weight_codes(engine, table, ctx_id, make_code_fn,
                               occurrence_count, recurring_min_count=2,
                               shift_max=24, modes=(0, 1)) -> int:
    max_shift = shift_max if occurrence_count >= recurring_min_count else 0
    added = 0
    async with engine.begin() as conn:
        for mode in modes:
            for shift in range(0, max_shift + 1):
                wc = make_code_fn(ctx_id, mode, shift)
                r  = await conn.execute(text(f"""
                    INSERT IGNORE INTO `{table}` (weight_code, ctx_id, mode, shift)
                    VALUES (:wc, :cid, :mode, :shift)
                """), {"wc": wc, "cid": ctx_id, "mode": mode, "shift": shift})
                added += r.rowcount
    return added


async def ensure_events_table(engine, table: str, extra_columns: str = "") -> None:
    pk = extra_columns if extra_columns else "PRIMARY KEY (`ctx_id`, `event_date`),"
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                {pk}
                `ctx_id`     INT      NOT NULL,
                `event_date` DATETIME NULL,
                INDEX idx_ctx_id    (`ctx_id`),
                INDEX idx_event_date(`event_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def insert_events(engine, table, events: list[dict]) -> int:
    if not events:
        return 0
    _reserved  = {"ctx_id", "event_date"}
    extra_cols = [k for k in events[0] if k not in _reserved]
    extra_insert = (", ".join(f"`{c}`" for c in extra_cols) + ", ") if extra_cols else ""
    extra_vals   = (", ".join(f":{c}" for c in extra_cols) + ", ") if extra_cols else ""
    added = 0
    async with engine.begin() as conn:
        for ev in events:
            params = {"cid": ev["ctx_id"], "ed": ev.get("event_date")}
            for c in extra_cols:
                params[c] = ev.get(c)
            r = await conn.execute(text(f"""
                INSERT IGNORE INTO `{table}`
                    ({extra_insert}`ctx_id`, `event_date`)
                VALUES ({extra_vals}:cid, :ed)
            """), params)
            added += r.rowcount
    return added


# 
#  ModelContext 
# 

class _ModelContext:
    def __init__(self, table, pair_id, day_flag, target_date, state):
        self._table       = table
        self._target_date = target_date
        self._state       = state
        self.delta_unit   = timedelta(days=1) if day_flag else timedelta(hours=1)
        self.modification = {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)

    @property
    def ctx_index(self):     return self._state.ctx_index
    @property
    def rates_t1(self):      return self._state.rates.get(self._table, {})
    @property
    def candle_ranges(self): return self._state.candle_ranges.get(self._table, {})
    @property
    def avg_range(self):     return self._state.avg_range.get(self._table, 0.0)
    @property
    def extremums(self):     return self._state.extremums.get(self._table, {"min": set(), "max": set()})
    @property
    def prev_candle(self):
        candles = self._state.last_candles.get(self._table, [])
        if not candles:
            return None
        idx = bisect.bisect_left(candles, (self._target_date, False))
        return candles[idx - 1] if idx > 0 else None

    def find_events(self, shift_window=None):
        s  = self._state
        sw = shift_window if shift_window is not None else s.SHIFT_WINDOW
        td = self._target_date
        du = self.delta_unit

        if s.np_built and s.np_sorted_dates_ns is not None and len(s.np_sorted_dates_ns):
            target_ts  = _dt_to_ts(td)
            delta_sec  = int(du.total_seconds())
            snd        = s.np_sorted_dates_ns
            shifts     = np.arange(-sw, sw + 1, dtype=np.int64)
            dt_ts      = target_ts + shifts * delta_sec
            dt_end_ts  = dt_ts + delta_sec
            future_mask = dt_ts > target_ts
            dt_ts      = dt_ts[~future_mask]
            dt_end_ts  = dt_end_ts[~future_mask]
            shifts_f   = shifts[~future_mask]
            left_arr   = np.searchsorted(snd, dt_ts,     side="left")
            right_arr  = np.searchsorted(snd, dt_end_ts, side="left")
            observations = []
            for i, (l, r, sh_arr) in enumerate(zip(left_arr, right_arr, shifts_f)):
                if l >= r:
                    continue
                obs_dt    = s.sorted_dates[l:r]
                shift_int = int(sh_arr)
                for j, odt in enumerate(obs_dt):
                    for event_key in s.sorted_data[l + j]:
                        ctx_info = s.ctx_index.get(event_key)
                        if ctx_info is None:
                            continue
                        occ = ctx_info.get("occurrence_count", 0)
                        is_recurring = occ >= s.RECURRING_MIN_COUNT
                        shift_val = shift_int
                        if not is_recurring and shift_val != 0:
                            continue
                        if is_recurring and abs(shift_val) > sw:
                            continue
                        observations.append((event_key, odt, shift_val))
            return observations

        observations = []
        for shift in range(-sw, sw + 1):
            dt = td + du * shift
            if s.FILTER_FUTURE_EVENTS and dt > td:
                continue
            dt_end = dt + du
            _l = bisect.bisect_left(s.sorted_dates, dt)
            _r = bisect.bisect_left(s.sorted_dates, dt_end)
            for _i in range(_l, _r):
                obs_dt = s.sorted_dates[_i]
                for event_key in s.sorted_data[_i]:
                    ctx_info = s.ctx_index.get(event_key)
                    if ctx_info is None:
                        continue
                    occ = ctx_info.get("occurrence_count", 0)
                    is_recurring = occ >= s.RECURRING_MIN_COUNT
                    shift_val = shift
                    if not is_recurring and shift_val != 0:
                        continue
                    if is_recurring and abs(shift_val) > sw:
                        continue
                    observations.append((event_key, obs_dt, shift_val))
        return observations

    def event_history(self, event_key):
        all_hist = self._state.event_history.get(event_key, [])
        idx = bisect.bisect_left(all_hist, self._target_date)
        return all_hist[:idx]

    def compute_t1(self, t_dates):
        s = self._state
        np_rates = s.np_rates.get(self._table)
        if s.np_built and np_rates is not None and t_dates:
            t_ts = np.array([_dt_to_ts(d) for d in t_dates], dtype=np.int64)
            ri   = np.searchsorted(np_rates["dates_ns"], t_ts, side="left")
            n    = len(np_rates["dates_ns"])
            in_b = ri < n
            exact = np.zeros(len(t_ts), dtype=bool)
            if np.any(in_b):
                exact[in_b] = np_rates["dates_ns"][ri[in_b]] == t_ts[in_b]
            if not np.any(exact):
                return 0.0
            return float(np.sum(np_rates["t1"][ri[exact]]))
        return _compute_t1(t_dates, self.rates_t1, self.candle_ranges, self.avg_range)

    def compute_extremum(self, t_dates, is_bull=True, total_hist=0):
        if not t_dates or total_hist == 0:
            return None
        s = self._state
        np_rates = s.np_rates.get(self._table)
        if s.np_built and np_rates is not None:
            t_ts = np.array([_dt_to_ts(d) for d in t_dates], dtype=np.int64)
            ri   = np.searchsorted(np_rates["dates_ns"], t_ts, side="left")
            n    = len(np_rates["dates_ns"])
            in_b = ri < n
            exact = np.zeros(len(t_ts), dtype=bool)
            if np.any(in_b):
                exact[in_b] = np_rates["dates_ns"][ri[in_b]] == t_ts[in_b]
            if not np.any(exact):
                return None
            ext_arr  = np_rates["ext_max" if is_bull else "ext_min"]
            ext_hits = ext_arr[ri[exact]]
            val = (float(np.count_nonzero(ext_hits)) / total_hist) * 2 - 1
            val *= self.modification
            return val if val != 0 else None
        ext_set = self.extremums["max" if is_bull else "min"]
        return _compute_extremum(t_dates, ext_set, self.candle_ranges,
                                 self.avg_range, self.modification, total_hist)


# 
#  STATE 
# 

class _State:
    # Критичные переменные — читаются из ENV, переопределяются model.py
    SERVICE_ID:   int = 0
    PORT:         int = 9000
    NODE_NAME:    str = "brain-svc"
    SERVICE_TEXT: str = "Brain microservice"

    # Опциональные таблицы (нарастаются в model.py по необходимости)
    WEIGHTS_TABLE:       str | None = None
    WEIGHTS_CODE_COLUMN: str        = "weight_code"
    CTX_TABLE:           str | None = None
    CTX_QUERY:           str | None = None
    CTX_KEY_COLUMNS:     list       = None
    EVENTS_TABLE:        str | None = None
    EVENTS_QUERY:        str | None = None
    EVENTS_ENGINE:       str        = "vlad"
    EVENT_KEY_COLUMNS:   list       = None
    EVENT_DATE_COLUMN:   str        = "event_date"
    DATASET_TABLE:       str | None = None
    DATASET_QUERY:       str | None = None
    DATASET_ENGINE:      str        = "vlad"
    FILTER_DATASET_BY_DATE: bool    = False
    SHIFT_WINDOW:        int        = 12
    RECURRING_MIN_COUNT: int        = 2
    FILTER_FUTURE_EVENTS: bool      = True
    RELOAD_INTERVAL:     int        = 3600
    REBUILD_INTERVAL:    int        = 0
    VAR_RANGE:           list       = None
    REBUILD_SOURCE_QUERY:  str | None = None
    REBUILD_SOURCE_ENGINE: str        = "vlad"
    REBUILD_ID_COLUMN:     str        = "id"
    REBUILD_DATE_COLUMN:   str | None = None
    REBUILD_SHIFT_MAX:     int        = 0
    # Дефолтный date_from для fill_cache (переопределяется через CACHE_DATE_FROM в model.py)
    CACHE_DATE_FROM:       str        = "2025-01-15"
    # Опциональная функция (event_key) → str для человекочитаемых имён в нарративе.
    # Задаётся в model.py: LABEL_FN = lambda key: MY_DICT.get(key[0])
    # Если None — используется str(event_key).
    LABEL_FN: object = None

    def __init__(self):
        self.CTX_KEY_COLUMNS   = ["id"]
        self.EVENT_KEY_COLUMNS = ["event_id"]
        self.VAR_RANGE         = []
        self.model_fn         = None
        self.load_events_fn   = None
        self.rebuild_index_fn = None
        self.make_ctx_key_fn  = None
        self.index_builder_fn  = None   # async def build_index(ev, eb) → dict
        self.weight_builder_fn = None   # async def build_weights(ev) → dict
        self.model_needs_ctx   = False
        self.model_needs_index = False  # True если model() принимает dataset_index
        self.engine_vlad  = None
        self.engine_brain = None
        self.engine_super = None
        self.weight_codes:  list       = []
        self.ctx_index:     dict       = {}
        self.events_by_dt:  dict       = {}
        self.sorted_dates:  list       = []
        self.sorted_data:   list       = []
        self.event_history: dict       = {}
        self.events_seen:   set        = set()
        self.events_max_dt: datetime | None = None
        self.dataset:       list[dict] = []
        # Индекс датасета — строится в _load_dataset, используется в model_fn
        self.dataset_dates:     list = []   # все даты в порядке dataset
        self.dataset_by_key:    dict = {}   # ключ -> [events...]
        self.dataset_key_dates: dict = {}   # ключ -> [dates...] для bisect
        self.dataset_key_field: str  = 'ctx_id'  # переопред. через DATASET_KEY в model.py
        self.rates:         dict = {}
        self.extremums:     dict = {}
        self.candle_ranges: dict = {}
        self.avg_range:     dict = {}
        self.last_candles:  dict = {}
        self.global_rates:  dict = {}
        self.last_rates_refresh: dict = {}
        self.ctx_row_count:     int = 0
        self.weights_row_count: int = 0
        self.service_url:   str  = ""
        self.last_reload:   datetime | None = None
        self.last_rebuild:  datetime | None = None
        self.fill_task:   asyncio.Task | None = None
        self.fill_cancel: asyncio.Event       = asyncio.Event()
        self.fill_status: dict                = {"state": "idle"}
        self.np_rates: dict                   = {}
        self.np_sorted_dates_ns: np.ndarray | None = None
        self.np_built: bool = False

        # IS_SIMPLE: автоматически True если model.py содержит RATES_TABLE
        self.IS_SIMPLE:           bool  = False
        self.RATES_TABLE_SIMPLE:  str   = "brain_rates_eur_usd"
        self.simple_rates:        list  = []
        self.simple_rates_dates:  list  = []
        self.last_simple_rate_dt: datetime | None = None
        # Numpy-массивы для simple_rates (строятся в _load_simple_rates)
        self.np_simple_rates: dict | None = None

        # Имя таблицы кеша для этого сервиса.
        # Формат: vlad_values_cache_svc{PORT} (напр. vlad_values_cache_svc9036)
        # Устанавливается в _preload() после определения service_url.
        self._cache_table: str = "vlad_values_cache"

    @property
    def cache_table(self) -> str:
        return self._cache_table


# 
#  КОНСТАНТЫ 
# 

_RATES_TABLES = [
    "brain_rates_eur_usd", "brain_rates_eur_usd_day",
    "brain_rates_btc_usd", "brain_rates_btc_usd_day",
    "brain_rates_eth_usd", "brain_rates_eth_usd_day",
]
_INSTRUMENTS = {
    1: {"hour": "brain_rates_eur_usd", "day": "brain_rates_eur_usd_day"},
    3: {"hour": "brain_rates_btc_usd", "day": "brain_rates_btc_usd_day"},
    4: {"hour": "brain_rates_eth_usd", "day": "brain_rates_eth_usd_day"},
}
_PAIR_CFG = {
    1: (0.0002, 100_000.0, 50_000.0),
    3: (60.0,        1.0, 100_000.0),
    4: (10.0,        1.0,   5_000.0),
}


# 
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ 
# 

def _parse_date(s: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%d-%m %H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def _rates_table(pair_id: int, day_flag: int) -> str:
    base = {1: "brain_rates_eur_usd", 3: "brain_rates_btc_usd",
            4: "brain_rates_eth_usd"}.get(pair_id, "brain_rates_eur_usd")
    return base + ("_day" if day_flag == 1 else "")


def _engine_for(name: str, s: _State):
    return {"vlad": s.engine_vlad, "brain": s.engine_brain,
            "super": s.engine_super}.get(name, s.engine_vlad)


def _params_hash(params: dict) -> str:
    import hashlib
    return hashlib.md5(
        _json.dumps(params, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def _filter_rates_lte(table: str, date: datetime, s: _State) -> list[dict]:
    rows = s.global_rates.get(table, [])
    if not rows:
        return []
    keys = [r["date"] for r in rows]
    idx  = bisect.bisect_right(keys, date)
    return rows[:idx]


def _filter_dataset_lte(date: datetime, s: _State) -> list[dict]:
    if not s.FILTER_DATASET_BY_DATE:
        return s.dataset
    return [e for e in s.dataset
            if e.get("date") is not None and e["date"] <= date]


# 
#  NUMPY: BUILD / REBUILD / APPEND 
# 

def _rebuild_np_rates(s: _State) -> None:
    s.np_built = False
    s.np_rates.clear()
    for table in _RATES_TABLES:
        np_r = _build_np_rates_for_table(
            s.rates.get(table, {}),
            s.candle_ranges.get(table, {}),
            s.extremums.get(table, {}),
        )
        if np_r is not None:
            np_r["avg_range"] = s.avg_range.get(table, 0.0)
        s.np_rates[table] = np_r
    s.np_built = True


def _rebuild_np_sorted_dates(s: _State) -> None:
    if s.sorted_dates:
        s.np_sorted_dates_ns = np.array(
            [_dt_to_ts(d) for d in s.sorted_dates], dtype=np.int64)
    else:
        s.np_sorted_dates_ns = np.empty(0, dtype=np.int64)


def _append_np_rates_row(table, dt, t1, rng, s: _State) -> None:
    np_r = s.np_rates.get(table)
    if np_r is None:
        return
    ts = np.int64(_dt_to_ts(dt))
    np_r["dates_ns"] = np.append(np_r["dates_ns"], ts)
    np_r["t1"]       = np.append(np_r["t1"],       np.float64(t1 if t1 is not None else 0.0))
    np_r["ranges"]   = np.append(np_r["ranges"],   np.float64(rng))
    np_r["ext_min"]  = np.append(np_r["ext_min"],  False)
    np_r["ext_max"]  = np.append(np_r["ext_max"],  False)


# 
#  ЗАГРУЗКА ДАННЫХ 
# 

async def _load_rates(s: _State):
    for table in _RATES_TABLES:
        s.rates[table] = {}
        s.last_candles[table] = []
        s.candle_ranges[table] = {}
        s.avg_range[table] = 0.0
        s.extremums[table] = {"min": set(), "max": set()}
        s.global_rates[table] = []
        try:
            async with s.engine_brain.connect() as conn:
                res = await conn.execute(text(
                    f"SELECT date, open, close, `max`, `min`, t1 "
                    f"FROM `{table}` ORDER BY date"))
                ranges = []
                for r in res.mappings().all():
                    dt = r["date"]
                    s.global_rates[table].append({
                        "date":  dt,
                        "open":  float(r["open"]  or 0),
                        "close": float(r["close"] or 0),
                        "min":   float(r["min"]   or 0),
                        "max":   float(r["max"]   or 0),
                    })
                    if r["t1"] is not None:
                        s.rates[table][dt] = float(r["t1"])
                    s.last_candles[table].append((dt, r["close"] > r["open"]))
                    rng = float(r["max"] or 0) - float(r["min"] or 0)
                    s.candle_ranges[table][dt] = rng
                    ranges.append(rng)
                s.avg_range[table] = sum(ranges) / len(ranges) if ranges else 0.0
                interval = "1 DAY" if table.endswith("_day") else "1 HOUR"
                for typ in ("min", "max"):
                    op = ">" if typ == "max" else "<"
                    q = f"""SELECT t1.date FROM `{table}` t1
                        JOIN `{table}` tp ON tp.date = t1.date - INTERVAL {interval}
                        JOIN `{table}` tn ON tn.date = t1.date + INTERVAL {interval}
                        WHERE t1.`{typ}` {op} tp.`{typ}`
                          AND t1.`{typ}` {op} tn.`{typ}`"""
                    res_ext = await conn.execute(text(q))
                    s.extremums[table][typ] = {r["date"] for r in res_ext.mappings().all()}
            log(f"  {table}: {len(s.rates[table])} candles", s.NODE_NAME)
        except Exception as e:
            log(f"   {table}: {e}", s.NODE_NAME, level="error")
    try:
        _rebuild_np_rates(s)
        log(f"   NP_RATES built: {sum(1 for v in s.np_rates.values() if v)} tables",
            s.NODE_NAME)
    except Exception as e:
        log(f"   NP_RATES build failed: {e}", s.NODE_NAME, level="error")


async def _refresh_rates(table: str, s: _State):
    now  = datetime.now()
    last = s.last_rates_refresh.get(table)
    if last and (now - last).total_seconds() < 30:
        return
    s.last_rates_refresh[table] = now
    ram = s.rates.get(table)
    if not ram:
        return
    max_dt = max(ram.keys())
    try:
        async with s.engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `max`, `min`, t1 "
                f"FROM `{table}` WHERE date > :dt ORDER BY date"), {"dt": max_dt})
            n = 0
            for r in res.mappings().all():
                dt  = r["date"]
                t1  = float(r["t1"]) if r["t1"] is not None else None
                rng = float(r["max"] or 0) - float(r["min"] or 0)
                if t1 is not None:
                    ram[dt] = t1
                s.last_candles.setdefault(table, []).append(
                    (dt, (r["close"] or 0) > (r["open"] or 0)))
                s.candle_ranges.setdefault(table, {})[dt] = rng
                s.global_rates.setdefault(table, []).append({
                    "date":  dt, "open":  float(r["open"]  or 0),
                    "close": float(r["close"] or 0),
                    "min":   float(r["min"]   or 0), "max": float(r["max"] or 0),
                })
                if s.np_built:
                    _append_np_rates_row(table, dt, t1, rng, s)
                n += 1
            if n > 0:
                log(f"   +{n} candle(s) {table}", s.NODE_NAME)
    except Exception as e:
        log(f"   refresh {table}: {e}", s.NODE_NAME, level="warning")


#  IS_SIMPLE: загрузка одной таблицы котировок 
def _build_np_simple_rates(s: _State) -> None:
    """Строит numpy-массивы для simple_rates — аналог np_rates для IS_SIMPLE."""
    if not s.simple_rates:
        s.np_simple_rates = None
        return
    n = len(s.simple_rates)
    dates_ns = np.array([_dt_to_ts(r['date']) for r in s.simple_rates], dtype=np.int64)
    t1_arr   = np.array([float((r.get('close') or 0) - (r.get('open') or 0))
                         for r in s.simple_rates], dtype=np.float64)
    rng_arr  = np.array([float((r.get('max') or 0) - (r.get('min') or 0))
                         for r in s.simple_rates], dtype=np.float64)
    avg_rng  = float(np.mean(rng_arr)) if n > 0 else 0.0
    # Локальные экстремумы
    max_arr  = np.array([float(r.get('max') or 0) for r in s.simple_rates], dtype=np.float64)
    min_arr  = np.array([float(r.get('min') or 0) for r in s.simple_rates], dtype=np.float64)
    ext_max  = np.zeros(n, dtype=bool)
    ext_min  = np.zeros(n, dtype=bool)
    if n > 2:
        ext_max[1:-1] = (max_arr[1:-1] > max_arr[:-2]) & (max_arr[1:-1] > max_arr[2:])
        ext_min[1:-1] = (min_arr[1:-1] < min_arr[:-2]) & (min_arr[1:-1] < min_arr[2:])
    close_arr = np.array([float(r.get('close') or 0) for r in s.simple_rates], dtype=np.float64)
    open_arr  = np.array([float(r.get('open')  or 0) for r in s.simple_rates], dtype=np.float64)
    s.np_simple_rates = {
        'dates_ns': dates_ns,
        't1':       t1_arr,
        'ranges':   rng_arr,
        'avg_range': avg_rng,
        'ext_max':  ext_max,
        'ext_min':  ext_min,
        'close':    close_arr,
        'open':     open_arr,
    }


async def _load_simple_rates(s: _State) -> None:
    table = s.RATES_TABLE_SIMPLE
    try:
        async with s.engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `min`, `max` "
                f"FROM `{table}` ORDER BY date"
            ))
            s.simple_rates = [
                {"date":  r["date"],
                 "open":  float(r["open"]  or 0),
                 "close": float(r["close"] or 0),
                 "min":   float(r["min"]   or 0),
                 "max":   float(r["max"]   or 0)}
                for r in res.mappings().all()
            ]
            s.simple_rates_dates  = [r["date"] for r in s.simple_rates]
            s.last_simple_rate_dt = s.simple_rates_dates[-1] if s.simple_rates_dates else None
        _build_np_simple_rates(s)
        log(f"  simple_rates: {len(s.simple_rates)} candles from {table}", s.NODE_NAME)
    except Exception as e:
        log(f"   simple_rates: {e}", s.NODE_NAME, level="error")
        s.simple_rates, s.simple_rates_dates = [], []
        s.np_simple_rates = None


async def _refresh_simple_rates(s: _State) -> None:
    if not s.last_simple_rate_dt:
        return
    table = s.RATES_TABLE_SIMPLE
    try:
        async with s.engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `min`, `max` "
                f"FROM `{table}` WHERE date > :dt ORDER BY date"
            ), {"dt": s.last_simple_rate_dt})
            new_rows = res.mappings().all()
        for r in new_rows:
            row = {"date":  r["date"],
                   "open":  float(r["open"]  or 0),
                   "close": float(r["close"] or 0),
                   "min":   float(r["min"]   or 0),
                   "max":   float(r["max"]   or 0)}
            s.simple_rates.append(row)
            s.simple_rates_dates.append(row["date"])
        if new_rows:
            s.last_simple_rate_dt = s.simple_rates_dates[-1]
            _build_np_simple_rates(s)
            log(f"   +{len(new_rows)} candle(s) {table}", s.NODE_NAME)
    except Exception as e:
        log(f"   refresh simple_rates: {e}", s.NODE_NAME, level="warning")


def _simple_rates_lte(date: datetime, s: _State) -> list:
    idx = bisect.bisect_right(s.simple_rates_dates, date)
    return s.simple_rates[:idx]


async def _load_weight_codes(s: _State):
    if not s.WEIGHTS_TABLE:
        s.weight_codes = []
        return
    try:
        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(
                f"SELECT `{s.WEIGHTS_CODE_COLUMN}` FROM `{s.WEIGHTS_TABLE}`"))
            s.weight_codes = [r[0] for r in res.fetchall()]
            s.weights_row_count = len(s.weight_codes)
        log(f"  weight_codes: {len(s.weight_codes)}", s.NODE_NAME)
    except Exception as e:
        s.weight_codes = []
        log(f"   weight_codes: {e}", s.NODE_NAME, level="error")


async def _load_ctx_index(s: _State):
    if not s.CTX_TABLE and not s.CTX_QUERY:
        s.ctx_index = {}
        return
    query = s.CTX_QUERY or f"SELECT * FROM `{s.CTX_TABLE}`"
    try:
        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(query))
            s.ctx_index = {}
            for r in res.mappings().all():
                key = tuple(r[col] for col in s.CTX_KEY_COLUMNS)
                s.ctx_index[key] = dict(r)
            s.ctx_row_count = len(s.ctx_index)
        log(f"  ctx_index: {s.ctx_row_count}", s.NODE_NAME)
    except Exception as e:
        s.ctx_index = {}
        log(f"   ctx_index: {e}", s.NODE_NAME, level="error")


async def _load_ctx_index_incremental(s: _State) -> list:
    if not s.CTX_TABLE and not s.CTX_QUERY:
        return []
    query = s.CTX_QUERY or f"SELECT * FROM `{s.CTX_TABLE}`"
    new_keys = []
    try:
        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(query))
            for r in res.mappings().all():
                key = tuple(r[col] for col in s.CTX_KEY_COLUMNS)
                if key not in s.ctx_index:
                    new_keys.append(key)
                s.ctx_index[key] = dict(r)
            s.ctx_row_count = len(s.ctx_index)
        if new_keys:
            log(f"   ctx_index: +{len(new_keys)} ключей", s.NODE_NAME, force=True)
    except Exception as e:
        log(f"   ctx_index incremental: {e}", s.NODE_NAME, level="error")
    return new_keys


async def _load_dataset(s: _State):
    if not s.DATASET_QUERY and not s.DATASET_TABLE:
        s.dataset = []
        return
    query = s.DATASET_QUERY or f"SELECT * FROM `{s.DATASET_TABLE}`"
    try:
        from datetime import date as _date
        async with _engine_for(s.DATASET_ENGINE, s).connect() as conn:
            res = await conn.execute(text(query))
            rows = []
            for r in res.mappings().all():
                row = dict(r)
                # MySQL DATE → Python datetime.date, но весь фреймворк работает
                # с datetime.datetime. Нормализуем все date-поля при загрузке.
                for k, v in row.items():
                    if type(v) is _date:
                        row[k] = datetime(v.year, v.month, v.day)
                rows.append(row)
            s.dataset = rows
        _build_dataset_index(s)
        log(f"  dataset: {len(s.dataset)} rows", s.NODE_NAME)
    except Exception as e:
        s.dataset = []
        log(f"   dataset: {e}", s.NODE_NAME, level="error")


def _build_dataset_index(s: _State) -> None:
    from collections import defaultdict as _dd
    key_field = s.dataset_key_field
    dates: list = []
    by_key: dict = _dd(list)
    for e in s.dataset:
        dates.append(e.get('date'))
        k = e.get(key_field)
        if k is not None:
            by_key[k].append(e)
    by_key = dict(by_key)
    s.dataset_dates     = dates
    s.dataset_by_key    = by_key
    s.dataset_key_dates = {k: [e['date'] for e in evts] for k, evts in by_key.items()}
    log(f'  dataset_index: {len(by_key)} unique {key_field}s', s.NODE_NAME)


def _ceil_to_hour(dt: datetime) -> datetime:
    if dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt
    return dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


def _register_event(dt: datetime, event_key: tuple, s: _State) -> bool:
    dt = _ceil_to_hour(dt)
    sig = (event_key, dt)
    if sig in s.events_seen:
        return False
    s.events_seen.add(sig)
    s.events_by_dt.setdefault(dt, []).append(event_key)
    s.event_history.setdefault(event_key, []).append(dt)
    if s.events_max_dt is None or dt > s.events_max_dt:
        s.events_max_dt = dt
    return True


def _build_sorted_arrays(s: _State):
    if s.events_by_dt:
        items = sorted(s.events_by_dt.items())
        s.sorted_dates = [x[0] for x in items]
        s.sorted_data  = [x[1] for x in items]
    else:
        s.sorted_dates, s.sorted_data = [], []
    for key in s.event_history:
        s.event_history[key].sort()
    _rebuild_np_sorted_dates(s)


async def _load_events_append(since_dt: datetime | None, s: _State):
    if s.load_events_fn is not None:
        try:
            async with s.engine_vlad.connect() as cv:
                async with s.engine_brain.connect() as cb:
                    pairs = await s.load_events_fn(cv, cb)
            loaded = skipped = 0
            for event_key, dt in pairs:
                if not isinstance(event_key, tuple):
                    event_key = (event_key,)
                if event_key in s.ctx_index:
                    _register_event(dt, event_key, s)
                    loaded += 1
                else:
                    skipped += 1
            log(f"  events(custom): +{loaded} skip={skipped}", s.NODE_NAME)
        except Exception as e:
            log(f"   load_events: {e}", s.NODE_NAME, level="error")
        return

    if s.EVENTS_TABLE and since_dt is not None:
        query  = (f"SELECT * FROM `{s.EVENTS_TABLE}` "
                  f"WHERE `{s.EVENT_DATE_COLUMN}` > :since "
                  f"ORDER BY `{s.EVENT_DATE_COLUMN}`")
        params = {"since": since_dt}
    elif s.EVENTS_TABLE:
        query  = f"SELECT * FROM `{s.EVENTS_TABLE}` ORDER BY `{s.EVENT_DATE_COLUMN}`"
        params = {}
    elif s.EVENTS_QUERY:
        query, params = s.EVENTS_QUERY, {}
    else:
        return

    try:
        async with _engine_for(s.EVENTS_ENGINE, s).connect() as conn:
            res = await conn.execute(text(query), params)
            loaded = dup = skipped = 0
            for r in res.mappings().all():
                key = tuple(r[col] for col in s.EVENT_KEY_COLUMNS)
                dt  = r[s.EVENT_DATE_COLUMN]
                if dt is None:
                    skipped += 1
                    continue
                if key in s.ctx_index:
                    if _register_event(dt, key, s):
                        loaded += 1
                    else:
                        dup += 1
                else:
                    skipped += 1
        sfx = f" since={since_dt}" if since_dt else ""
        log(f"  events{sfx}: +{loaded} dup={dup} skip={skipped}", s.NODE_NAME)
    except Exception as e:
        log(f"   events: {e}", s.NODE_NAME, level="error")


async def _load_events_full(s: _State):
    s.events_by_dt.clear()
    s.event_history.clear()
    s.events_seen.clear()
    s.events_max_dt = None
    await _load_events_append(None, s)


async def _check_data_changed(s: _State) -> tuple[bool, bool]:
    ctx_ch = wgt_ch = False
    try:
        if s.CTX_TABLE:
            async with s.engine_vlad.connect() as conn:
                cnt = (await conn.execute(
                    text(f"SELECT COUNT(*) FROM `{s.CTX_TABLE}`"))).scalar()
                if cnt != s.ctx_row_count:
                    ctx_ch = True
        if s.WEIGHTS_TABLE:
            async with s.engine_vlad.connect() as conn:
                cnt = (await conn.execute(
                    text(f"SELECT COUNT(*) FROM `{s.WEIGHTS_TABLE}`"))).scalar()
                if cnt != s.weights_row_count:
                    wgt_ch = True
    except Exception as e:
        log(f"   check_changed: {e}", s.NODE_NAME, level="warning")
    return ctx_ch, wgt_ch


# 
#  DDL BACKTEST 
# 

_DDL_BT_RESULTS = """
CREATE TABLE IF NOT EXISTS vlad_backtest_results (
    id BIGINT NOT NULL AUTO_INCREMENT,
    service_url VARCHAR(255) NOT NULL,
    model_id INT NOT NULL DEFAULT 0,
    pair TINYINT NOT NULL, day_flag TINYINT NOT NULL, tier TINYINT NOT NULL,
    params_hash CHAR(32) NOT NULL, params_json TEXT NOT NULL,
    date_from DATETIME NOT NULL, date_to DATETIME NOT NULL,
    balance_final DECIMAL(18,4) NOT NULL DEFAULT 0,
    total_result DECIMAL(18,4) NOT NULL DEFAULT 0,
    summary_lost DECIMAL(18,6) NOT NULL DEFAULT 0,
    value_score DECIMAL(18,4) NOT NULL DEFAULT 0,
    trade_count INT NOT NULL DEFAULT 0, win_count INT NOT NULL DEFAULT 0,
    accuracy DECIMAL(7,4) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_bt (service_url(100), pair, day_flag, tier, params_hash, date_from, date_to),
    INDEX idx_score (service_url(100), pair, day_flag, tier, value_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_DDL_BT_SUMMARY = """
CREATE TABLE IF NOT EXISTS vlad_backtest_summary (
    id INT NOT NULL AUTO_INCREMENT,
    model_id INT NOT NULL, service_url VARCHAR(255) NOT NULL,
    pair TINYINT NOT NULL, day_flag TINYINT NOT NULL, tier TINYINT NOT NULL,
    date_from DATETIME NOT NULL, date_to DATETIME NOT NULL,
    total_combinations INT NOT NULL DEFAULT 0,
    best_score DECIMAL(18,4) NOT NULL DEFAULT 0,
    avg_score DECIMAL(18,4) NOT NULL DEFAULT 0,
    best_accuracy DECIMAL(7,4) NOT NULL DEFAULT 0,
    avg_accuracy DECIMAL(7,4) NOT NULL DEFAULT 0,
    best_params_json TEXT,
    computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_sum (model_id, pair, day_flag, tier, date_from, date_to)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


# 
#  _universal_rebuild 
# 

async def _universal_rebuild(s: _State) -> dict:
    if not s.CTX_TABLE or not s.WEIGHTS_TABLE:
        raise RuntimeError("Универсальный rebuild требует CTX_TABLE и WEIGHTS_TABLE")
    if not s.EVENTS_TABLE:
        raise RuntimeError("Универсальный rebuild требует EVENTS_TABLE")

    events_table = s.EVENTS_TABLE
    id_col       = s.REBUILD_ID_COLUMN
    date_col     = s.REBUILD_DATE_COLUMN
    shift_max    = s.REBUILD_SHIFT_MAX

    last_id = 0
    try:
        async with s.engine_vlad.connect() as conn:
            row = (await conn.execute(
                text(f"SELECT MAX(`{id_col}`) FROM `{events_table}`")
            )).fetchone()
            last_id = int(row[0]) if row and row[0] is not None else 0
    except Exception:
        last_id = 0

    engine = _engine_for(s.REBUILD_SOURCE_ENGINE, s)
    async with engine.connect() as conn:
        res = await conn.execute(text(s.REBUILD_SOURCE_QUERY), {"last_id": last_id})
        rows = [dict(r) for r in res.mappings().all()]

    if not rows:
        return {"processed": 0, "new_contexts": 0, "new_weights": 0,
                "new_events": 0, "last_id": last_id}

    ctx_map: dict[str, dict] = {}
    make_key = s.make_ctx_key_fn or (
        lambda row: "|".join(str(row.get(col, "")) for col in s.CTX_KEY_COLUMNS)
    )

    for row in rows:
        try:
            ctx_key = make_key(row)
        except Exception:
            continue
        row_id   = row.get(id_col)
        row_date = row.get(date_col) if date_col else None
        if ctx_key not in ctx_map:
            ctx_map[ctx_key] = {"count": 0, "first_dt": row_date,
                                "last_dt": row_date, "items": []}
        d = ctx_map[ctx_key]
        d["count"] += 1
        if row_date:
            if d["first_dt"] is None or row_date < d["first_dt"]:
                d["first_dt"] = row_date
            if d["last_dt"]  is None or row_date > d["last_dt"]:
                d["last_dt"]  = row_date
        if row_id is not None:
            d["items"].append((row_id, row_date))

    async with s.engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{s.CTX_TABLE}` (
                `id`               INT          NOT NULL AUTO_INCREMENT,
                `ctx_key`          VARCHAR(255) NOT NULL DEFAULT '',
                `occurrence_count` INT          NOT NULL DEFAULT 0,
                `first_dt`         DATETIME     NULL,
                `last_dt`          DATETIME     NULL,
                `updated_at`       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                                ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_ctx_key` (`ctx_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        for ctx_key, d in ctx_map.items():
            await conn.execute(text(f"""
                INSERT INTO `{s.CTX_TABLE}` (ctx_key, occurrence_count, first_dt, last_dt)
                VALUES (:ck, :cnt, :fd, :ld)
                ON DUPLICATE KEY UPDATE
                    occurrence_count = occurrence_count + :cnt,
                    last_dt  = IF(:ld > last_dt  OR last_dt  IS NULL, :ld,  last_dt),
                    first_dt = IF(:fd < first_dt OR first_dt IS NULL, :fd, first_dt)
            """), {"ck": ctx_key, "cnt": d["count"],
                   "fd": d["first_dt"], "ld": d["last_dt"]})

    ctx_key_to_id: dict[str, int] = {}
    key_list = list(ctx_map.keys())
    async with s.engine_vlad.connect() as conn:
        for i in range(0, len(key_list), 500):
            batch = key_list[i:i + 500]
            placeholders = ", ".join(f":k{j}" for j in range(len(batch)))
            params = {f"k{j}": k for j, k in enumerate(batch)}
            res = await conn.execute(text(
                f"SELECT id, ctx_key FROM `{s.CTX_TABLE}` "
                f"WHERE ctx_key IN ({placeholders})"), params)
            for r in res.fetchall():
                ctx_key_to_id[r[1]] = r[0]

    new_weights = 0
    wc_prefix = s.NODE_NAME.replace("brain-", "").upper()[:4]
    async with s.engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{s.WEIGHTS_TABLE}` (
                `id`          INT         NOT NULL AUTO_INCREMENT,
                `weight_code` VARCHAR(64) NOT NULL,
                `ctx_id`      INT         NOT NULL,
                `mode`        TINYINT     NOT NULL DEFAULT 0,
                `shift`       SMALLINT    NOT NULL DEFAULT 0,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_weight_code` (`weight_code`),
                INDEX idx_ctx_id (`ctx_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        for ctx_key, ctx_id in ctx_key_to_id.items():
            total_count = ctx_map.get(ctx_key, {}).get("count", 0)
            max_shift   = shift_max if total_count >= s.RECURRING_MIN_COUNT else 0
            for mode in (0, 1):
                for shift in range(0, max_shift + 1):
                    wc = f"{wc_prefix}{ctx_id}_{mode}_{shift}"
                    r  = await conn.execute(text(f"""
                        INSERT IGNORE INTO `{s.WEIGHTS_TABLE}`
                            (weight_code, ctx_id, mode, shift)
                        VALUES (:wc, :cid, :mode, :shift)
                    """), {"wc": wc, "cid": ctx_id, "mode": mode, "shift": shift})
                    new_weights += r.rowcount

    new_events = 0
    if events_table and date_col:
        async with s.engine_vlad.begin() as conn:
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{events_table}` (
                    `{id_col}`   BIGINT   NOT NULL,
                    `ctx_id`     INT      NOT NULL,
                    `{date_col}` DATETIME NULL,
                    PRIMARY KEY (`{id_col}`),
                    INDEX idx_ctx_id (`ctx_id`),
                    INDEX idx_date   (`{date_col}`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            for ctx_key, d in ctx_map.items():
                ctx_id = ctx_key_to_id.get(ctx_key)
                if ctx_id is None:
                    continue
                for row_id, row_date in d["items"]:
                    r = await conn.execute(text(f"""
                        INSERT IGNORE INTO `{events_table}`
                            (`{id_col}`, ctx_id, `{date_col}`)
                        VALUES (:rid, :cid, :dt)
                    """), {"rid": row_id, "cid": ctx_id, "dt": row_date})
                    new_events += r.rowcount

    new_max_id = max((row.get(id_col) or 0) for row in rows) if rows else last_id
    log(f"  universal_rebuild: processed={len(rows)} ctx={len(ctx_map)} "
        f"weights={new_weights} events={new_events}", s.NODE_NAME, force=True)
    return {
        "processed":    len(rows),
        "new_contexts": len(ctx_key_to_id),
        "new_weights":  new_weights,
        "new_events":   new_events,
        "prev_last_id": last_id,
        "new_last_id":  new_max_id,
    }


# 
#  __detail__ УТИЛИТЫ 
# 

_DETAIL_KEY = "__detail__"


def _extract_detail(result: dict | None) -> tuple[dict | None, any]:
    if result is None:
        return result, None
    detail = result.pop(_DETAIL_KEY, None)
    return result, detail


def _make_detail_serializable(detail) -> any:
    if detail is None:
        return None
    if isinstance(detail, dict):
        return {str(k): _make_detail_serializable(v) for k, v in detail.items()}
    if isinstance(detail, (list, tuple)):
        return [_make_detail_serializable(v) for v in detail]
    if isinstance(detail, datetime):
        return detail.isoformat()
    if isinstance(detail, float):
        if math.isnan(detail) or math.isinf(detail):
            return None
        return detail
    if isinstance(detail, (int, str, bool)):
        return detail
    return str(detail)


# 
#  УНИВЕРСАЛЬНЫЙ ПОСТРОИТЕЛЬ НАРРАТИВА 
# 

def _pluralize_n(n: int, forms: tuple[str, str, str]) -> str:
    abs_n = abs(n)
    if abs_n % 100 in range(11, 20):
        return forms[2]
    r = abs_n % 10
    if r == 1:          return forms[0]
    if r in (2, 3, 4):  return forms[1]
    return forms[2]


def _shift_label(shift: int | None, day_flag: int) -> str:
    if shift is None or shift == 0:
        return "в момент целевой свечи"
    if day_flag:
        forms = ("день", "дня", "дней")
    else:
        forms = ("час", "часа", "часов")
    unit = _pluralize_n(shift, forms)
    if shift > 0:
        return f"{abs(shift)} {unit} назад"
    return f"через {abs(shift)} {unit}"


def build_detail_narrative(
    observations: list[tuple],
    result: dict,
    date: "datetime",
    delta_unit: "timedelta",
    shift_window: int,
    ctx,
    *,
    label_fn=None,
    day_flag: int = 0,
    calc_type: int = 0,
) -> list[str]:
    if not observations:
        return ["Нет контекстных событий в заданном промежутке."]

    sw        = shift_window
    start_dt  = date - delta_unit * sw
    end_dt    = date + delta_unit * sw

    groups: dict[tuple, dict] = {}
    for event_key, obs_dt, shift_val in observations:
        gk = (event_key, shift_val)
        if gk not in groups:
            ctx_info    = ctx.ctx_index.get(event_key, {})
            occ         = ctx_info.get("occurrence_count", 0)
            valid_dts   = ctx.event_history(event_key)
            t_dates     = [d + delta_unit * shift_val for d in valid_dts
                           if (d + delta_unit * shift_val) < date]
            label       = (label_fn(event_key) if label_fn else None) or str(event_key)
            groups[gk]  = {
                "event_key":        event_key,
                "shift_val":        shift_val,
                "label":            label,
                "occurrence_count": occ,
                "t_dates_count":    len(t_dates),
                "wc_t1":   {},
                "wc_ext":  {},
            }
        g = groups[gk]
        for wc, val in result.items():
            if wc not in g["wc_t1"] and wc not in g["wc_ext"]:
                parts = wc.rsplit("_", 2 if g["shift_val"] is not None else 1)
                try:
                    mode_candidate = int(parts[-2]) if len(parts) >= 2 else -1
                except ValueError:
                    mode_candidate = -1
                shift_suffix = f"_{shift_val}" if (g["shift_val"] is not None and g["shift_val"] != 0) else ""
                if wc.endswith(shift_suffix) or shift_val == 0:
                    if mode_candidate == 0:
                        g["wc_t1"][wc] = val
                    elif mode_candidate == 1:
                        g["wc_ext"][wc] = val

    if not groups:
        return ["Событий не найдено после группировки."]

    sorted_groups = sorted(groups.values(), key=lambda g: (-(g["shift_val"] or 0), str(g["event_key"])))
    n = len(sorted_groups)

    type_desc = {
        0: "Type = 0: учитывается и сумма свечей (T1), и вероятность экстремума.",
        1: "Type = 1: учитывается только сумма свечей (T1).",
        2: "Type = 2: учитывается только вероятность экстремума.",
    }.get(calc_type, f"Type = {calc_type}.")

    evt_word = _pluralize_n(n, ("событие", "события", "событий"))

    lines: list[str] = []

    lines.append(
        f"Окно поиска: {start_dt.strftime('%Y-%m-%d %H:%M')} — "
        f"{end_dt.strftime('%Y-%m-%d %H:%M')}. "
        f"Найдено {n} {evt_word}."
    )
    lines.append("")

    for i, g in enumerate(sorted_groups, 1):
        tl = _shift_label(g["shift_val"], day_flag)
        lines.append(f'{i}. "{g["label"]}" — {tl}')

    lines.append("")
    lines.append(type_desc)
    lines.append("")

    for i, g in enumerate(sorted_groups, 1):
        occ = g["occurrence_count"]
        tl  = _shift_label(g["shift_val"], day_flag)
        t1_sum  = round(sum(g["wc_t1"].values()),  6) if g["wc_t1"]  else None
        ext_sum = round(sum(g["wc_ext"].values()), 6) if g["wc_ext"] else None

        lines.append(f'Событие {i}. "{g["label"]}", {tl}.')

        hist_parts = [f"В истории это событие повторялось {occ} раз(а)"]
        if t1_sum is not None and calc_type in (0, 1):
            hist_parts.append(f"сумма свечей (T1) = {t1_sum}")
        if ext_sum is not None and calc_type in (0, 2):
            hist_parts.append(f"вероятность экстремума = {ext_sum}")
        lines.append(", ".join(hist_parts) + ".")

        codes: list[str] = []
        for wc, v in {**g["wc_t1"], **g["wc_ext"]}.items():
            codes.append(f"{wc}: {v}")
        if codes:
            lines.append("Вес(а): " + ", ".join(codes) + ".")

        lines.append("")

    return lines


# 
#  build_app 
# 

def _discover_builders(model_module):
    import importlib.util as _ilu
    import os as _os

    model_dir = _os.path.dirname(_os.path.abspath(
        sys.modules[model_module.__name__].__file__
    ))

    def _load(filename, fn_name):
        path = _os.path.join(model_dir, filename)
        if not _os.path.exists(path):
            return None
        try:
            spec   = _ilu.spec_from_file_location(filename[:-3], path)
            module = _ilu.module_from_spec(spec)
            spec.loader.exec_module(module)
            fn = getattr(module, fn_name, None)
            if fn is None:
                log(f"   {filename} найден, но {fn_name}() отсутствует",
                    "brain-framework", level="warning")
            return fn
        except Exception as e:
            log(f"   ошибка импорта {filename}: {e}",
                "brain-framework", level="error")
            return None

    build_index_fn   = _load("context_idx.py", "build_index")
    build_weights_fn = _load("weights.py",     "build_weights")

    if build_index_fn:
        log("   context_idx.py (build_index)", "brain-framework", force=True)
    if build_weights_fn:
        log("   weights.py (build_weights)", "brain-framework", force=True)

    return build_index_fn, build_weights_fn


def build_app(model_module) -> FastAPI:
    s = _State()

    def _g(attr, default):
        return getattr(model_module, attr, default)

    #  Критичные переменные: ENV → переопределение из model.py 
    s.SERVICE_ID   = _g("SERVICE_ID",   int(os.getenv("SERVICE_ID",   "0")))
    s.PORT         = _g("PORT",         int(os.getenv("PORT",         "9000")))
    s.NODE_NAME    = _g("NODE_NAME",    os.getenv("NODE_NAME",    "brain-svc"))
    s.SERVICE_TEXT = _g("SERVICE_TEXT", os.getenv("SERVICE_TEXT", "Brain microservice"))

    #  Опциональные таблицы и настройки 
    s.WEIGHTS_TABLE          = _g("WEIGHTS_TABLE",          None)
    s.WEIGHTS_CODE_COLUMN    = _g("WEIGHTS_CODE_COLUMN",    "weight_code")
    s.CTX_TABLE              = _g("CTX_TABLE",              None)
    s.CTX_QUERY              = _g("CTX_QUERY",              None)
    s.CTX_KEY_COLUMNS        = _g("CTX_KEY_COLUMNS",        ["id"])
    s.EVENTS_TABLE           = _g("EVENTS_TABLE",           None)
    s.EVENTS_QUERY           = _g("EVENTS_QUERY",           None)
    s.EVENTS_ENGINE          = _g("EVENTS_ENGINE",          "vlad")
    s.EVENT_KEY_COLUMNS      = _g("EVENT_KEY_COLUMNS",      ["event_id"])
    s.EVENT_DATE_COLUMN      = _g("EVENT_DATE_COLUMN",      "event_date")
    s.DATASET_TABLE          = _g("DATASET_TABLE",          None)
    s.DATASET_QUERY          = _g("DATASET_QUERY",          None)
    s.DATASET_ENGINE         = _g("DATASET_ENGINE",         "vlad")
    s.FILTER_DATASET_BY_DATE = _g("FILTER_DATASET_BY_DATE", False)
    s.SHIFT_WINDOW           = _g("SHIFT_WINDOW",           12)
    s.RECURRING_MIN_COUNT    = _g("RECURRING_MIN_COUNT",    2)
    s.FILTER_FUTURE_EVENTS   = _g("FILTER_FUTURE_EVENTS",   True)
    s.RELOAD_INTERVAL        = _g("RELOAD_INTERVAL",        3600)
    s.REBUILD_INTERVAL       = int(_g("REBUILD_INTERVAL",   0))
    s.VAR_RANGE              = _g("VAR_RANGE",              [0])
    s.REBUILD_SOURCE_QUERY   = _g("REBUILD_SOURCE_QUERY",   None)
    s.REBUILD_SOURCE_ENGINE  = _g("REBUILD_SOURCE_ENGINE",  "vlad")
    s.REBUILD_ID_COLUMN      = _g("REBUILD_ID_COLUMN",      "id")
    s.REBUILD_DATE_COLUMN    = _g("REBUILD_DATE_COLUMN",    None)
    s.REBUILD_SHIFT_MAX      = _g("REBUILD_SHIFT_MAX",      0)
    s.CACHE_DATE_FROM        = _g("CACHE_DATE_FROM",        "2025-01-15")
    s.LABEL_FN               = _g("LABEL_FN",               None)

    # Автодетекция режима: RATES_TABLE в model.py → IS_SIMPLE (нет pair/day/ctx)
    s.IS_SIMPLE          = hasattr(model_module, "RATES_TABLE")
    s.RATES_TABLE_SIMPLE  = _g("RATES_TABLE", "brain_rates_eur_usd")
    s.dataset_key_field   = _g("DATASET_KEY", "ctx_id")

    s.model_fn         = getattr(model_module, "model",         None)
    s.load_events_fn   = getattr(model_module, "load_events",   None)
    s.rebuild_index_fn = getattr(model_module, "rebuild_index", None)
    s.make_ctx_key_fn  = getattr(model_module, "make_ctx_key",  None)

    s.index_builder_fn, s.weight_builder_fn = _discover_builders(model_module)

    if s.model_fn is None:
        raise RuntimeError("model_module должен определять функцию model()")

    sig = inspect.signature(s.model_fn)
    s.model_needs_ctx   = "ctx"           in sig.parameters
    s.model_needs_index = "dataset_index" in sig.parameters

    log(f"  mode={'simple' if s.IS_SIMPLE else 'full'} | "
        f"model(): ctx={'yes' if s.model_needs_ctx else 'no'} | "
        f"VAR_RANGE={s.VAR_RANGE} | "
        f"rebuild={'custom' if s.rebuild_index_fn else ('universal' if s.REBUILD_SOURCE_QUERY else 'no')}",
        s.NODE_NAME, force=True)

    s.engine_vlad, s.engine_brain, s.engine_super = build_engines()

    #  _call_model (IS_SIMPLE + полный режим) 

    async def _call_model(pair, day, date_str, calc_type=0, calc_var=0, param=""):
        target_date = _parse_date(date_str)
        if not target_date:
            return None

        if s.IS_SIMPLE:
            await _refresh_simple_rates(s)
            rates_f   = _simple_rates_lte(target_date, s)
            dataset_f = _filter_dataset_lte(target_date, s)
            result = s.model_fn(rates=rates_f, dataset=dataset_f,
                                date=target_date, type=calc_type,
                                var=calc_var, param=param, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
            result, _ = _extract_detail(result)
            return result

        table = _rates_table(pair, day)
        await _refresh_rates(table, s)
        rates_filtered   = _filter_rates_lte(table, target_date, s)
        dataset_filtered = _filter_dataset_lte(target_date, s)
        if s.model_needs_ctx:
            ctx = _ModelContext(table, pair, day, target_date, s)
            result = s.model_fn(rates=rates_filtered, dataset=dataset_filtered,
                                date=target_date, type=calc_type, var=calc_var,
                                param=param, ctx=ctx, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
        else:
            result = s.model_fn(rates=rates_filtered, dataset=dataset_filtered,
                                date=target_date, type=calc_type, var=calc_var,
                                param=param, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
        result, _ = _extract_detail(result)
        return result

    async def _call_model_with_detail(pair, day, date_str, calc_type=0, calc_var=0, param=""):
        target_date = _parse_date(date_str)
        if not target_date:
            return None, None

        if s.IS_SIMPLE:
            await _refresh_simple_rates(s)
            rates_f   = _simple_rates_lte(target_date, s)
            dataset_f = _filter_dataset_lte(target_date, s)
            result = s.model_fn(rates=rates_f, dataset=dataset_f,
                                date=target_date, type=calc_type,
                                var=calc_var, param=param, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
            result, detail = _extract_detail(result)
            return result, _make_detail_serializable(detail)

        table = _rates_table(pair, day)
        await _refresh_rates(table, s)
        rates_filtered = _filter_rates_lte(table, target_date, s)
        dataset_f      = _filter_dataset_lte(target_date, s)
        if s.model_needs_ctx:
            ctx = _ModelContext(table, pair, day, target_date, s)
            result = s.model_fn(rates=rates_filtered, dataset=dataset_f,
                                date=target_date, type=calc_type,
                                var=calc_var, param=param, ctx=ctx, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
        else:
            result = s.model_fn(rates=rates_filtered, dataset=dataset_f,
                                date=target_date, type=calc_type,
                                var=calc_var, param=param, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
        result, detail = _extract_detail(result)
        return result, _make_detail_serializable(detail)

    #  _preload (IS_SIMPLE + полный режим) 

    async def _preload():
        log(" FULL DATA RELOAD", s.NODE_NAME, force=True)
        s.np_built = False
        s.weight_codes.clear()
        s.ctx_index.clear()
        s.dataset.clear()

        if s.IS_SIMPLE:
            await _load_simple_rates(s)
            await _load_rates(s)
            await _load_dataset(s)
            await _load_weight_codes(s)
            s.service_url = f"http://localhost:{s.PORT}"
            s._cache_table = f"vlad_values_cache_svc{s.PORT}"
            try:
                await ensure_cache_table(s.engine_vlad, s.cache_table)
                async with s.engine_vlad.begin() as conn:
                    await conn.execute(text(_DDL_BT_RESULTS))
                    await conn.execute(text(_DDL_BT_SUMMARY))
            except Exception as e:
                log(f"   tables: {e}", s.NODE_NAME, level="error")
            s.last_reload = datetime.now()
            global_rates_total = sum(len(v) for v in s.global_rates.values())
            log(f" RELOAD DONE (simple): rates={len(s.simple_rates)} "
                f"global_rates={global_rates_total} "
                f"dataset={len(s.dataset)} weights={len(s.weight_codes)}",
                s.NODE_NAME, force=True)
            return

        # Полный режим (сервисы 33-35)
        await _load_weight_codes(s)
        await _load_ctx_index(s)
        await _load_events_full(s)
        _build_sorted_arrays(s)
        await _load_rates(s)
        await _load_dataset(s)
        s.service_url = f"http://localhost:{s.PORT}"
        s._cache_table = f"vlad_values_cache_svc{s.PORT}"
        log(f"  service_url:  {s.service_url}", s.NODE_NAME)
        log(f"  cache_table:  {s.cache_table}", s.NODE_NAME)
        try:
            await ensure_cache_table(s.engine_vlad, s.cache_table)
            async with s.engine_vlad.begin() as conn:
                await conn.execute(text(_DDL_BT_RESULTS))
                await conn.execute(text(_DDL_BT_SUMMARY))
        except Exception as e:
            log(f"   tables: {e}", s.NODE_NAME, level="error")
        s.last_reload = datetime.now()
        log(f" RELOAD DONE: weights={len(s.weight_codes)} ctx={len(s.ctx_index)} "
            f"events={len(s.events_by_dt)} dataset={len(s.dataset)} "
            f"np_built={s.np_built}", s.NODE_NAME, force=True)

    #  _do_rebuild (с поддержкой IS_SIMPLE) 

    async def _do_rebuild() -> dict:
        stats = {}

        if s.index_builder_fn is not None:
            try:
                index_stats = await s.index_builder_fn(s.engine_vlad, s.engine_brain)
                stats["index"] = index_stats or {}
                log(f"   build_index: {index_stats}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   build_index: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "build_index")
                stats["index"] = {"error": str(e)}

        if s.weight_builder_fn is not None:
            try:
                weight_stats = await s.weight_builder_fn(s.engine_vlad)
                stats["weights"] = weight_stats or {}
                log(f"   build_weights: {weight_stats}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   build_weights: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "build_weights")
                stats["weights"] = {"error": str(e)}

        elif s.rebuild_index_fn is not None and s.index_builder_fn is None:
            try:
                legacy = await s.rebuild_index_fn(
                    s.engine_vlad, s.engine_brain, s.engine_super)
                stats["legacy_rebuild"] = legacy or {}
            except Exception as e:
                log(f"   rebuild_index (legacy): {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "rebuild_index")
                return {"error": str(e)}

        elif (s.REBUILD_SOURCE_QUERY is not None
              and s.index_builder_fn is None
              and s.rebuild_index_fn is None):
            try:
                stats["universal_rebuild"] = await _universal_rebuild(s)
            except Exception as e:
                log(f"   universal_rebuild: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "universal_rebuild")
                return {"error": str(e)}

        # Шаг 5: обновление данных в памяти
        await _load_weight_codes(s)

        if not s.IS_SIMPLE:
            new_keys = await _load_ctx_index_incremental(s)
            if new_keys:
                await _load_events_full(s)
            else:
                since = s.events_max_dt
                await _load_events_append(since, s)
            _build_sorted_arrays(s)
            return_new_keys = len(new_keys)
        else:
            # IS_SIMPLE: перезагружаем датасет — build_index мог записать новые строки
            await _load_dataset(s)
            return_new_keys = 0

        s.last_rebuild = datetime.now()
        log(f" rebuild done: weights={len(s.weight_codes)}"
            + (f" dataset={len(s.dataset)}" if s.IS_SIMPLE else ""),
            s.NODE_NAME, force=True)

        return {
            **stats,
            "new_ctx_keys":  return_new_keys,
            "ctx_total":     len(s.ctx_index),
            "weights_total": len(s.weight_codes),
            "events_total":  len(s.events_by_dt),
            "rebuilt_at":    s.last_rebuild.isoformat(),
        }

    #  _bg_reload (IS_SIMPLE + полный) 

    async def _bg_reload():
        while True:
            await asyncio.sleep(s.RELOAD_INTERVAL)
            try:
                if s.IS_SIMPLE:
                    # Каждый тик (RELOAD_INTERVAL): обновляем котировки
                    await _refresh_simple_rates(s)
                    for _tbl_bg in _RATES_TABLES:
                        await _refresh_rates(_tbl_bg, s)

                    # Rebuild по отдельному интервалу (REBUILD_INTERVAL).
                    # _do_rebuild() сам перезагружает датасет и веса после записи.
                    if (s.index_builder_fn is not None
                            and s.REBUILD_INTERVAL > 0
                            and (s.last_rebuild is None or
                                 (datetime.now() - s.last_rebuild).total_seconds()
                                 >= s.REBUILD_INTERVAL)):
                        log(" IS_SIMPLE auto-rebuild...", s.NODE_NAME, force=True)
                        await _do_rebuild()

                    s.last_reload = datetime.now()
                    continue

                # Полный режим
                if ((s.rebuild_index_fn or s.REBUILD_SOURCE_QUERY)
                        and s.REBUILD_INTERVAL > 0
                        and (s.last_rebuild is None or
                             (datetime.now() - s.last_rebuild).total_seconds()
                             >= s.REBUILD_INTERVAL)):
                    await _do_rebuild()
                    s.last_reload = datetime.now()
                    continue
                ctx_ch, wgt_ch = await _check_data_changed(s)
                events_updated = False
                if ctx_ch:
                    new_keys = await _load_ctx_index_incremental(s)
                    if new_keys:
                        await _load_events_full(s)
                        events_updated = True
                if wgt_ch:
                    await _load_weight_codes(s)
                if not events_updated:
                    since = s.events_max_dt
                    await _load_events_append(since, s)
                    if s.events_max_dt != since:
                        events_updated = True
                if events_updated:
                    _build_sorted_arrays(s)
                s.last_reload = datetime.now()
            except Exception as e:
                log(f" bg reload: {e}", s.NODE_NAME, level="error", force=True)
                send_error_trace(e, s.NODE_NAME, "bg_reload")

    #  fill_cache helpers 

    async def _bulk_insert(rows: list[dict]) -> int:
        if not rows:
            return 0
        _tbl = s.cache_table
        try:
            async with s.engine_vlad.begin() as conn:
                r = await conn.execute(text(f"""
                    INSERT IGNORE INTO `{_tbl}`
                        (service_url, pair, day_flag, date_val,
                         params_hash, params_json, result_json)
                    VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
                """), rows)
                return r.rowcount
        except Exception as e:
            log(f"   bulk insert: {e}", s.NODE_NAME, level="warning")
            return 0

    async def _cached_dates(pair, day, p_hash) -> set:
        _tbl = s.cache_table
        try:
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT date_val FROM `{_tbl}`
                    WHERE service_url=:url AND pair=:pair
                      AND day_flag=:day AND params_hash=:ph
                """), {"url": s.service_url, "pair": pair, "day": day, "ph": p_hash})
                return {row[0] for row in res.fetchall()}
        except Exception:
            return set()

    async def _fill_worker(pairs, days, date_from_str, date_to_str, types, batch_size):
        s.fill_cancel.clear()
        dt_from = _parse_date(date_from_str) if date_from_str else None
        dt_to   = _parse_date(date_to_str)   if date_to_str   else None

        #  IS_SIMPLE: игнорируем pairs/days 
        if s.IS_SIMPLE:
            slots = [(tp, var) for tp in types for var in s.VAR_RANGE]
            candles = [r for r in s.simple_rates
                       if (dt_from is None or r["date"] >= dt_from)
                       and (dt_to   is None or r["date"] <= dt_to)]
            total = len(candles) * len(slots)
            done = skipped = errors = 0
            s.fill_status = {"state": "running", "total": total, "done": 0,
                             "skipped": 0, "errors": 0,
                             "started_at": datetime.now().isoformat()}
            log(f" fill_cache (simple): {len(candles)} candles × {len(slots)} slots",
                s.NODE_NAME, force=True)

            for calc_type, var in slots:
                if s.fill_cancel.is_set():
                    break
                extra       = {"type": calc_type, "var": var, "param": ""}
                p_hash      = _params_hash(extra)
                params_json = _json.dumps(extra, ensure_ascii=False)
                _tbl = s.cache_table
                try:
                    async with s.engine_vlad.connect() as conn:
                        res = await conn.execute(text(f"""
                            SELECT date_val FROM `{_tbl}`
                            WHERE service_url=:url AND params_hash=:ph
                        """), {"url": s.service_url, "ph": p_hash})
                        cached = {r[0] for r in res.fetchall()}
                except Exception:
                    cached = set()

                to_fetch  = [c for c in candles if c["date"] not in cached]
                skipped  += len(candles) - len(to_fetch)

                for i in range(0, len(to_fetch), batch_size):
                    if s.fill_cancel.is_set():
                        break
                    batch = to_fetch[i:i + batch_size]

                    def _sync_simple(candle, _ct=calc_type, _v=var):
                        td = candle["date"]
                        try:
                            res = s.model_fn(rates=_simple_rates_lte(td, s),
                                             dataset=_filter_dataset_lte(td, s),
                                             date=td, type=_ct, var=_v, param="", dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
                            res, _ = _extract_detail(res)
                            return res or {}
                        except Exception:
                            return None

                    # Вычисляем весь батч в одном потоке — model() CPU-bound,
                    # GIL не даёт выигрыша от to_thread для чистого Python.
                    # asyncio.to_thread нужен только чтобы не блокировать event loop.
                    results = await asyncio.to_thread(
                        lambda: [_sync_simple(c) for c in batch]
                    )
                    insert_rows = []
                    for candle, result in zip(batch, results):
                        if result is None:
                            errors += 1
                            continue
                        insert_rows.append({
                            "url": s.service_url, "dv": candle["date"],
                            "ph": p_hash, "pj": params_json,
                            "rj": _json.dumps(result, ensure_ascii=False),
                        })
                    if insert_rows:
                        _tbl = s.cache_table
                        try:
                            async with s.engine_vlad.begin() as conn:
                                await conn.execute(text(f"""
                                    INSERT IGNORE INTO `{_tbl}`
                                        (service_url, pair, day_flag, date_val,
                                         params_hash, params_json, result_json)
                                    VALUES (:url, 0, 0, :dv, :ph, :pj, :rj)
                                """), insert_rows)
                        except Exception as e:
                            log(f"   bulk insert: {e}", s.NODE_NAME, level="warning")
                    done += len(batch)
                    s.fill_status.update({"done": done, "skipped": skipped, "errors": errors})
                    log(f"  [simple {calc_type}/{var}] {done}/{total} err={errors}",
                        s.NODE_NAME, force=True)

            state = "stopped" if s.fill_cancel.is_set() else "done"
            s.fill_status.update({"state": state, "finished_at": datetime.now().isoformat()})
            log(f" fill_cache (simple) {state}: done={done} skip={skipped} err={errors}",
                s.NODE_NAME, force=True)

            #  Авто-бэктест после заполнения кэша (IS_SIMPLE) 
            if not s.fill_cancel.is_set():
                log(" auto-backtest после fill_cache (simple)...", s.NODE_NAME, force=True)
                bt_date_from = _parse_date(date_from_str) if date_from_str else datetime(2025, 1, 1)
                bt_date_to   = _parse_date(date_to_str)   if date_to_str   else datetime.now()
                for bt_type, bt_var in slots:
                    try:
                        bt_result = await _backtest(
                            0, 0, tier=0,
                            extra_params={"type": bt_type, "var": bt_var},
                            df=bt_date_from, dt=bt_date_to,
                        )
                        label = f"type={bt_type} var={bt_var}"
                        if "error" in bt_result:
                            log(f"   backtest [{label}]: {bt_result['error']}",
                                s.NODE_NAME, force=True)
                        else:
                            log(f"   backtest [{label}]: "
                                f"score={bt_result.get('value_score')} "
                                f"acc={bt_result.get('accuracy')} "
                                f"trades={bt_result.get('trade_count')}",
                                s.NODE_NAME, force=True)
                    except Exception as e:
                        log(f"   backtest error type={bt_type} var={bt_var}: {e}",
                            s.NODE_NAME, level="error")
                try:
                    await _upsert_summary(0, 0, tier=0, df=bt_date_from, dt=bt_date_to)
                except Exception as e:
                    log(f"   summary (simple): {e}", s.NODE_NAME, level="warning")
                s.fill_status["auto_backtest"] = "done"
                log(" auto-backtest (simple) завершён", s.NODE_NAME, force=True)
            return

        #  Полный режим (старый код) 
        slots   = [(pair, day, tp, var)
                   for pair in pairs for day in days
                   for tp in types for var in s.VAR_RANGE]
        unique_pd = {(p, d) for p, d, *_ in slots}
        total_dates = sum(
            sum(1 for r in s.global_rates.get(_rates_table(p, d), [])
                if (dt_from is None or r["date"] >= dt_from)
                and (dt_to   is None or r["date"] <= dt_to))
            for p, d in unique_pd
        ) * len(types) * len(s.VAR_RANGE)
        s.fill_status = {
            "state": "running", "pairs": pairs, "days": days,
            "types": types, "var_range": s.VAR_RANGE, "batch_size": batch_size,
            "slots_total": len(slots), "slots_done": 0,
            "overall_total": total_dates, "overall_done": 0,
            "overall_skipped": 0, "errors": 0,
            "current_slot": None, "started_at": datetime.now().isoformat(),
        }
        log(f" fill_cache: {len(slots)} слотов, ~{total_dates} дат, batch={batch_size}",
            s.NODE_NAME, force=True)
        overall_done = overall_skipped = errors = 0

        for slot_idx, (pair, day, calc_type, var) in enumerate(slots):
            if s.fill_cancel.is_set():
                s.fill_status["state"] = "stopped"
                return
            table   = _rates_table(pair, day)
            candles = [r for r in s.global_rates.get(table, [])
                       if (dt_from is None or r["date"] >= dt_from)
                       and (dt_to   is None or r["date"] <= dt_to)]
            if not candles:
                s.fill_status["slots_done"] = slot_idx + 1
                continue
            extra       = {"type": calc_type, "var": var, "param": ""}
            p_hash      = _params_hash(extra)
            params_json = _json.dumps(extra, ensure_ascii=False)
            slot_label  = f"pair={pair} day={'d' if day else 'h'} type={calc_type} var={var}"
            s.fill_status["current_slot"] = slot_label
            cached   = await _cached_dates(pair, day, p_hash)
            to_fetch = [c for c in candles if c["date"] not in cached]
            skipped  = len(candles) - len(to_fetch)
            overall_skipped += skipped
            log(f"  [{slot_label}] {len(candles)} дат: кеш={skipped} нужно={len(to_fetch)}",
                s.NODE_NAME, force=True)
            slot_done = 0
            for batch_start in range(0, len(to_fetch), batch_size):
                if s.fill_cancel.is_set():
                    s.fill_status["state"] = "stopped"
                    return
                batch = to_fetch[batch_start: batch_start + batch_size]

                def _sync_compute(candle):
                    td = candle["date"]
                    r  = _filter_rates_lte(table, td, s)
                    ds = _filter_dataset_lte(td, s)
                    try:
                        if s.model_needs_ctx:
                            ctx = _ModelContext(table, pair, day, td, s)
                            res = s.model_fn(rates=r, dataset=ds, date=td,
                                             type=calc_type, var=var, param="", ctx=ctx, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
                        else:
                            res = s.model_fn(rates=r, dataset=ds, date=td,
                                             type=calc_type, var=var, param="", dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
                        res, _ = _extract_detail(res)
                        return res or {}
                    except Exception:
                        return None

                tasks   = [asyncio.to_thread(_sync_compute, c) for c in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                insert_rows = []
                for candle, result in zip(batch, results):
                    if isinstance(result, Exception) or result is None:
                        errors += 1
                        continue
                    insert_rows.append({
                        "url": s.service_url, "pair": pair, "day": day,
                        "dv": candle["date"], "ph": p_hash, "pj": params_json,
                        "rj": _json.dumps(result, ensure_ascii=False),
                    })
                n_inserted = await _bulk_insert(insert_rows)
                n_empty_batch = sum(1 for r in insert_rows if not _json.loads(r["rj"]))
                slot_done    += len(batch)
                overall_done += len(batch)
                pct = overall_done * 100 // max(total_dates, 1)
                s.fill_status.update({
                    "overall_done": overall_done, "overall_skipped": overall_skipped,
                    "errors": errors, "slot_done": slot_done,
                    "slot_total": len(to_fetch),
                })
                empty_note = f" empty={n_empty_batch}" if n_empty_batch else ""
                log(f"  [{slot_label}] {slot_done}/{len(to_fetch)} | "
                    f"всего {overall_done}/{total_dates} ({pct}%)"
                    f" written={n_inserted}{empty_note} err={errors}",
                    s.NODE_NAME, force=True)
            s.fill_status["slots_done"] = slot_idx + 1
        s.fill_status.update({"state": "done", "finished_at": datetime.now().isoformat()})
        log(f" fill done: done={overall_done} skip={overall_skipped} err={errors}",
            s.NODE_NAME, force=True)

        #  Авто-бэктест после заполнения кэша (только для полного режима) 
        if not s.IS_SIMPLE:
            log(" auto-backtest после fill_cache...", s.NODE_NAME, force=True)
            bt_date_from = _parse_date(date_from_str) if date_from_str else datetime(2025, 1, 1)
            bt_date_to   = _parse_date(date_to_str)   if date_to_str   else datetime.now()
            for bt_pair in pairs:
                for bt_day in days:
                    for bt_type in types:
                        for bt_var in s.VAR_RANGE:
                            try:
                                bt_result = await _backtest(
                                    bt_pair, bt_day, tier=0,
                                    extra_params={"type": bt_type, "var": bt_var},
                                    df=bt_date_from, dt=bt_date_to,
                                )
                                label = f"pair={bt_pair} day={'d' if bt_day else 'h'} type={bt_type} var={bt_var}"
                                if "error" in bt_result:
                                    log(f"   backtest [{label}]: {bt_result['error']}",
                                        s.NODE_NAME, force=True)
                                else:
                                    log(f"   backtest [{label}]: "
                                        f"score={bt_result.get('value_score')} "
                                        f"acc={bt_result.get('accuracy')} "
                                        f"trades={bt_result.get('trade_count')}",
                                        s.NODE_NAME, force=True)
                            except Exception as e:
                                log(f"   backtest error pair={bt_pair} day={bt_day} type={bt_type} var={bt_var}: {e}",
                                    s.NODE_NAME, level="error")
                    try:
                        await _upsert_summary(bt_pair, bt_day, tier=0,
                                              df=bt_date_from, dt=bt_date_to)
                    except Exception as e:
                        log(f"   summary pair={bt_pair} day={bt_day}: {e}",
                            s.NODE_NAME, level="warning")
            s.fill_status["auto_backtest"] = "done"
            log(" auto-backtest завершён", s.NODE_NAME, force=True)

    #  _backtest (адаптирован для IS_SIMPLE) 

    async def _backtest(pair, day, tier, extra_params, df, dt) -> dict:
        p_hash = _params_hash(extra_params)
        async with s.engine_vlad.connect() as conn:
            ex = (await conn.execute(text("""
                SELECT value_score, accuracy, trade_count FROM vlad_backtest_results
                WHERE service_url=:url AND pair=:pair AND day_flag=:day
                  AND tier=:tier AND params_hash=:ph AND date_from=:df AND date_to=:dt
                LIMIT 1
            """), {"url": s.service_url, "pair": pair, "day": day, "tier": tier,
                   "ph": p_hash, "df": df, "dt": dt})).fetchone()
        if ex:
            return {"value_score": float(ex[0]), "accuracy": float(ex[1]),
                    "trade_count": int(ex[2]), "params": extra_params, "skipped": True}

        _cache_pair = 0 if s.IS_SIMPLE else pair
        _cache_day  = 0 if s.IS_SIMPLE else day
        _tbl = s.cache_table
        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT date_val, result_json FROM `{_tbl}`
                WHERE service_url=:url AND pair=:pair AND day_flag=:day
                  AND params_hash=:ph AND date_val>=:df AND date_val<=:dt
                ORDER BY date_val
            """), {"url": s.service_url, "pair": _cache_pair, "day": _cache_day,
                   "ph": p_hash, "df": df, "dt": dt})
            cache_map = {row[0]: _json.loads(row[1]) for row in res.fetchall()}
        if not cache_map:
            return {"error": "no cached data"}

        if s.IS_SIMPLE:
            rows = [r for r in s.simple_rates if df <= r["date"] <= dt]
            spread, mod, lot_div = (0.0002, 100_000.0, 50_000.0)  # дефолт EUR
        else:
            table  = _rates_table(pair, day)
            rows   = [r for r in s.global_rates.get(table, []) if df <= r["date"] <= dt]
            spread, mod, lot_div = _PAIR_CFG.get(pair, (0.0002, 100_000.0, 50_000.0))

        balance = highest = 10_000.0
        summary_lost = 0.0
        trade_count = win_count = 0
        position = None
        for candle in rows:
            vals = cache_map.get(candle["date"])
            signal = sum(vals.values()) if vals else 0.0
            if position is not None:
                direction, entry_price = position
                should_close = (signal == 0.0
                                or (signal > 0 and direction < 0)
                                or (signal < 0 and direction > 0))
                if should_close:
                    op = candle["open"]
                    if op:
                        lot = max(round(balance / lot_div, 2), 0.01)
                        pct = (op - entry_price) / entry_price * direction
                        pnl = lot * mod * pct
                        balance += pnl
                        trade_count += 1
                        if pnl >= 0:
                            win_count += 1
                        else:
                            summary_lost += abs(pnl)
                        if balance > highest:
                            highest = balance
                    position = None
            if signal != 0.0 and position is None:
                position = (1.0 if signal > 0 else -1.0, candle["open"])
        if trade_count < 5:
            return {"error": f"not enough trades: {trade_count}"}
        total_result = balance - 10_000.0
        value_score  = total_result - summary_lost
        accuracy     = win_count / trade_count
        result = {
            "balance_final": round(balance, 4), "total_result": round(total_result, 4),
            "summary_lost":  round(summary_lost, 6), "value_score": round(value_score, 4),
            "trade_count":   trade_count, "win_count": win_count,
            "accuracy":      round(accuracy, 4), "params": extra_params,
        }
        try:
            async with s.engine_vlad.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO vlad_backtest_results
                        (service_url, model_id, pair, day_flag, tier,
                         params_hash, params_json, date_from, date_to,
                         balance_final, total_result, summary_lost,
                         value_score, trade_count, win_count, accuracy)
                    VALUES (:url,:mid,:pair,:day,:tier,:ph,:pj,:df,:dt,
                            :bf,:tr,:sl,:vs,:tc,:wc,:acc)
                    ON DUPLICATE KEY UPDATE
                        balance_final=VALUES(balance_final),total_result=VALUES(total_result),
                        summary_lost=VALUES(summary_lost),value_score=VALUES(value_score),
                        trade_count=VALUES(trade_count),win_count=VALUES(win_count),
                        accuracy=VALUES(accuracy),created_at=CURRENT_TIMESTAMP
                """), {
                    "url": s.service_url, "mid": s.SERVICE_ID,
                    "pair": pair, "day": day, "tier": tier, "ph": p_hash,
                    "pj": _json.dumps(extra_params, ensure_ascii=False),
                    "df": df, "dt": dt,
                    "bf": result["balance_final"], "tr": result["total_result"],
                    "sl": result["summary_lost"],  "vs": result["value_score"],
                    "tc": trade_count, "wc": win_count, "acc": result["accuracy"],
                })
        except Exception as e:
            log(f"   backtest save: {e}", s.NODE_NAME, level="warning")
        return result

    async def _upsert_summary(pair, day, tier, df, dt):
        async with s.engine_vlad.connect() as conn:
            row = (await conn.execute(text("""
                SELECT COUNT(*), MAX(value_score), AVG(value_score),
                       MAX(accuracy), AVG(accuracy),
                       MAX(CASE WHEN value_score=(
                           SELECT MAX(value_score) FROM vlad_backtest_results r2
                           WHERE r2.service_url=:url AND r2.pair=:pair
                             AND r2.day_flag=:day AND r2.tier=:tier
                             AND r2.date_from=:df AND r2.date_to=:dt)
                       THEN params_json END)
                FROM vlad_backtest_results
                WHERE service_url=:url AND pair=:pair AND day_flag=:day
                  AND tier=:tier AND date_from=:df AND date_to=:dt
            """), {"url": s.service_url, "pair": pair, "day": day,
                   "tier": tier, "df": df, "dt": dt})).fetchone()
        if not row or not row[0]:
            return
        async with s.engine_vlad.begin() as conn:
            await conn.execute(text("""
                INSERT INTO vlad_backtest_summary
                    (model_id,service_url,pair,day_flag,tier,date_from,date_to,
                     total_combinations,best_score,avg_score,
                     best_accuracy,avg_accuracy,best_params_json)
                VALUES (:mid,:url,:pair,:day,:tier,:df,:dt,
                        :cnt,:bs,:as_,:ba,:aa,:bpj)
                ON DUPLICATE KEY UPDATE
                    total_combinations=VALUES(total_combinations),
                    best_score=VALUES(best_score),avg_score=VALUES(avg_score),
                    best_accuracy=VALUES(best_accuracy),avg_accuracy=VALUES(avg_accuracy),
                    best_params_json=VALUES(best_params_json),computed_at=CURRENT_TIMESTAMP
            """), {
                "mid": s.SERVICE_ID, "url": s.service_url,
                "pair": pair, "day": day, "tier": tier, "df": df, "dt": dt,
                "cnt": row[0], "bs": float(row[1] or 0), "as_": float(row[2] or 0),
                "ba": float(row[3] or 0), "aa": float(row[4] or 0), "bpj": row[5],
            })

    #  FastAPI (эндпоинты) 

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            await _preload()
        except Exception as e:
            log(f" initial load: {e}", s.NODE_NAME, level="error", force=True)
        task = asyncio.create_task(_bg_reload())
        yield
        task.cancel()
        s.fill_cancel.set()
        for eng in (s.engine_vlad, s.engine_brain, s.engine_super):
            try:
                await eng.dispose()
            except Exception:
                pass

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    async def ep_root():
        version = 0
        try:
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text(
                    "SELECT version FROM version_microservice WHERE microservice_id=:id"),
                    {"id": s.SERVICE_ID})
                row = res.fetchone()
                if row:
                    version = row[0]
        except Exception:
            pass
        return {
            "status": "ok", "version": f"1.{version}.0",
            "mode": MODE, "name": s.NODE_NAME, "text": s.SERVICE_TEXT,
            "metadata": {
                "weight_codes":   len(s.weight_codes),
                "ctx_index":      len(s.ctx_index),
                "events":         len(s.events_by_dt),
                "events_max_dt":  s.events_max_dt.isoformat() if s.events_max_dt else None,
                "dataset":        len(s.dataset),
                "var_range":      s.VAR_RANGE,
                "model_has_ctx":  s.model_needs_ctx,
                "np_built":       s.np_built,
                "last_reload":    s.last_reload.isoformat() if s.last_reload else None,
                "last_rebuild":   s.last_rebuild.isoformat() if s.last_rebuild else None,
                "rebuild_auto":   s.REBUILD_INTERVAL > 0 and (
                    s.rebuild_index_fn is not None or s.REBUILD_SOURCE_QUERY is not None),
                "rebuild_interval_sec": s.REBUILD_INTERVAL,
            },
        }

    @app.get("/weights")
    async def ep_weights():
        return ok_response({"codes": s.weight_codes, "var_range": s.VAR_RANGE})

    def _build_narrative(payload: dict, date_str: str, day_flag: int, calc_type: int) -> list[str]:
        if not payload:
            return ["Нет данных для текущей даты."]

        target_date = _parse_date(date_str)
        if not target_date:
            return [f"Не удалось разобрать дату: {date_str!r}"]

        du       = timedelta(days=1) if day_flag else timedelta(hours=1)
        sw       = s.SHIFT_WINDOW
        start_dt = target_date - du * sw
        end_dt   = target_date

        type_desc = {
            0: "Type = 0: учитывается и сумма свечей (T1), и вероятность экстремума.",
            1: "Type = 1: учитывается только сумма свечей (T1).",
            2: "Type = 2: учитывается только вероятность экстремума.",
        }.get(calc_type, f"Type = {calc_type}.")

        import re as _re
        _wc_pat = _re.compile(r'^[A-Za-z]*(\d+)_(\d+)_(-?\d+)$')

        groups: dict[tuple, dict] = {}
        unmatched: dict[str, float] = {}

        for wc, val in payload.items():
            m = _wc_pat.match(wc)
            if not m:
                unmatched[wc] = val
                continue
            ctx_id = int(m.group(1))
            mode   = int(m.group(2))
            shift  = int(m.group(3))
            gk = (ctx_id, shift)
            if gk not in groups:
                label = None
                if s.LABEL_FN:
                    try:
                        label = s.LABEL_FN((ctx_id,))
                    except Exception:
                        pass
                if not label:
                    for key, info in s.ctx_index.items():
                        if key[0] == ctx_id:
                            label = (info.get("person_token") or
                                     info.get("ctx_key") or
                                     info.get("event_name") or
                                     info.get("name"))
                            if label:
                                break
                if not label:
                    label = f"ctx_id={ctx_id}"

                occ = 0
                for key, info in s.ctx_index.items():
                    if key[0] == ctx_id:
                        occ = info.get("occurrence_count", 0)
                        break

                groups[gk] = {
                    "ctx_id": ctx_id,
                    "shift":  shift,
                    "label":  label,
                    "occ":    occ,
                    "t1":  {},
                    "ext": {},
                }
            g = groups[gk]
            if mode == 0:
                g["t1"][wc]  = val
            elif mode == 1:
                g["ext"][wc] = val

        if not groups and not unmatched:
            return ["Нет событий для декодирования."]

        sorted_groups = sorted(groups.values(), key=lambda g: (-g["shift"], g["ctx_id"]))
        n = len(sorted_groups)
        evt_word = _pluralize_n(n, ("событие", "события", "событий"))

        lines: list[str] = []
        lines.append(
            f"Окно поиска: {start_dt.strftime('%Y-%m-%d %H:%M')} — "
            f"{end_dt.strftime('%Y-%m-%d %H:%M')}. "
            f"Найдено {n} {evt_word}."
        )
        lines.append("")

        for i, g in enumerate(sorted_groups, 1):
            tl = _shift_label(g["shift"], day_flag)
            lines.append(f'{i}. "{g["label"]}" — {tl}')

        lines.append("")
        lines.append(type_desc)
        lines.append("")

        for i, g in enumerate(sorted_groups, 1):
            tl      = _shift_label(g["shift"], day_flag)
            t1_sum  = round(sum(g["t1"].values()),  6) if g["t1"]  else None
            ext_sum = round(sum(g["ext"].values()), 6) if g["ext"] else None

            lines.append(f'Событие {i}. "{g["label"]}", {tl}.')

            hist_parts = [f"В истории это событие повторялось {g['occ']} раз(а)"]
            if t1_sum is not None and calc_type in (0, 1):
                hist_parts.append(f"сумма свечей (T1) = {t1_sum}")
            if ext_sum is not None and calc_type in (0, 2):
                hist_parts.append(f"вероятность экстремума = {ext_sum}")
            lines.append(", ".join(hist_parts) + ".")

            codes = [f"{wc}: {v}" for wc, v in {**g["t1"], **g["ext"]}.items()]
            if codes:
                lines.append("Веса: " + ", ".join(codes) + ".")
            lines.append("")

        if unmatched:
            lines.append(f"Прочие веса ({len(unmatched)}): " +
                         ", ".join(f"{k}: {v}" for k, v in list(unmatched.items())[:10]) +
                         ("..." if len(unmatched) > 10 else "") + ".")

        return lines

    @app.get("/values")
    async def ep_values(
        pair:  int = Query(1), day: int = Query(1),
        date:  str = Query(...),
        type:  int = Query(0, ge=0, le=2),
        var:   int = Query(0),
        param: str = Query(""),
    ):
        if s.VAR_RANGE and var not in s.VAR_RANGE:
            return err_response(f"var={var} не входит в VAR_RANGE={s.VAR_RANGE}")
        try:
            resp = await cached_values(
                engine_vlad=s.engine_vlad, service_url=s.service_url,
                pair=pair, day=day, date=date,
                extra_params={"type": type, "var": var, "param": param},
                compute_fn=lambda: _call_model(pair, day, date,
                                               calc_type=type, calc_var=var, param=param),
                node=s.NODE_NAME,
                table_name=s.cache_table,
            )
            payload = resp.get("payLoad") or {}
            day_flag = 1 if day == 1 else 0
            resp["details"] = _build_narrative(payload, date, day_flag, type)
            return resp
        except Exception as e:
            send_error_trace(e, node=s.NODE_NAME, script="get_values")
            return err_response(str(e))

    @app.post("/compute_batch")
    async def ep_compute_batch(
        dates: list[str],
        pair: int = Query(1), day: int = Query(1),
        type: int = Query(0), var: int = Query(0), param: str = Query(""),
    ):
        result = {}
        for date_str in dates:
            try:
                r = await _call_model(pair, day, date_str,
                                      calc_type=type, calc_var=var, param=param)
                result[date_str] = r or {}
            except Exception:
                result[date_str] = {}
        return result

    @app.get("/fill_cache")
    async def ep_fill_cache(
        pairs:      str = Query("1,3,4"),
        days:       str = Query("0,1"),
        date_from:  str = Query(""),
        date_to:    str = Query(""),
        batch_size: int = Query(300),
    ):
        if s.fill_task and not s.fill_task.done():
            return err_response("Fill already running.")
        try:
            pl = [int(p.strip()) for p in pairs.split(",") if p.strip()]
            dl = [int(d.strip()) for d in days.split(",")  if d.strip()]
        except ValueError:
            return err_response("pairs и days — числа через запятую")
        if not s.IS_SIMPLE:
            if not all(p in {1, 3, 4} for p in pl):
                return err_response("Допустимые pair: 1 (EUR), 3 (BTC), 4 (ETH)")
            if not all(d in {0, 1} for d in dl):
                return err_response("Допустимые day: 0 (hourly), 1 (daily)")
        all_types = [0, 1, 2]
        effective_date_from = date_from if date_from.strip() else s.CACHE_DATE_FROM
        s.fill_task = asyncio.create_task(
            _fill_worker(pl, dl, effective_date_from, date_to, all_types, batch_size))
        return ok_response({
            "started":    True,
            "pairs":      pl,
            "days":       dl,
            "types":      all_types,
            "var_range":  s.VAR_RANGE,
            "batch_size": batch_size,
            "date_from":  effective_date_from,
            "date_to":    date_to or "now",
            "slots_total": len(pl) * len(dl) * len(all_types) * len(s.VAR_RANGE),
        })

    @app.get("/fill_status")
    async def ep_fill_status():
        return ok_response(s.fill_status)

    @app.get("/fill_stop")
    async def ep_fill_stop():
        s.fill_cancel.set()
        return ok_response({"stopped": True})

    @app.get("/reload")
    async def ep_reload():
        try:
            await _preload()
            return ok_response({"reloaded_at": s.last_reload.isoformat()})
        except Exception as e:
            send_error_trace(e, s.NODE_NAME, "reload")
            return err_response(str(e))

    @app.get("/rebuild_index")
    async def ep_rebuild_index():
        if s.rebuild_index_fn is None and s.REBUILD_SOURCE_QUERY is None:
            return err_response(
                "Rebuild не настроен. Добавь rebuild_index() в model.py "
                "или задай REBUILD_SOURCE_QUERY.")
        result = await _do_rebuild()
        if "error" in result:
            return err_response(result["error"])
        return ok_response(result)

    @app.get("/patch")
    async def ep_patch():
        try:
            async with s.engine_vlad.begin() as conn:
                res = await conn.execute(text(
                    "SELECT version FROM version_microservice WHERE microservice_id=:id"),
                    {"id": s.SERVICE_ID})
                row = res.fetchone()
                if not row:
                    return err_response(f"SID {s.SERVICE_ID} not found")
                old = row[0]
                new = old + 1
                await conn.execute(text(
                    "UPDATE version_microservice SET version=:v WHERE microservice_id=:id"),
                    {"v": new, "id": s.SERVICE_ID})
            return {"status": "ok", "from": old, "to": new}
        except Exception as e:
            return err_response(str(e))

    @app.get("/backtest")
    async def ep_backtest(
        pairs:     str = Query("1,3,4"),
        days:      str = Query("0,1"),
        tier:      int = Query(0, ge=0, le=1),
        date_from: str = Query(""),
        date_to:   str = Query(""),
        type:      int = Query(0),
        var:       int = Query(-1),
    ):
        # IS_SIMPLE: pairs/days игнорируются, всегда pair=0 day=0
        if s.IS_SIMPLE:
            pl = [0]
            dl = [0]
        else:
            try:
                pl = [int(p.strip()) for p in pairs.split(",") if p.strip()]
                dl = [int(d.strip()) for d in days.split(",")  if d.strip()]
            except ValueError:
                return err_response("pairs и days — числа через запятую")
            if not pl or not dl:
                return err_response("pairs и days не могут быть пустыми")

        df = _parse_date(date_from) if date_from.strip() else _parse_date(s.CACHE_DATE_FROM)
        dt = _parse_date(date_to)   if date_to.strip()   else datetime.now()
        if df is None or dt is None:
            return err_response("Invalid date format")

        vars_to_run = s.VAR_RANGE if var == -1 else [var]
        all_results = {}

        for bt_pair in pl:
            for bt_day in dl:
                key = 'simple' if s.IS_SIMPLE else f"pair={bt_pair} day={'d' if bt_day else 'h'}"
                all_results[key] = {}
                for v in vars_to_run:
                    try:
                        all_results[key][f"var={v}"] = await _backtest(
                            bt_pair, bt_day, tier, {"type": type, "var": v}, df, dt)
                    except Exception as e:
                        all_results[key][f"var={v}"] = {"error": str(e)}
                try:
                    await _upsert_summary(bt_pair, bt_day, tier, df, dt)
                except Exception as e:
                    log(f"   summary pair={bt_pair} day={bt_day}: {e}",
                        s.NODE_NAME, level="warning")

        return ok_response({
            "pairs":     pl,
            "days":      dl,
            "tier":      tier,
            "date_from": df.isoformat(),
            "date_to":   dt.isoformat(),
            "vars":      vars_to_run,
            "results":   all_results,
        })

    @app.get("/summary")
    async def ep_summary(pair: int = Query(-1), day: int = Query(-1),
                         tier: int = Query(-1)):
        conds  = ["service_url=:url"]
        params = {"url": s.service_url}
        if pair >= 0: conds.append("pair=:pair");    params["pair"] = pair
        if day  >= 0: conds.append("day_flag=:day"); params["day"]  = day
        if tier >= 0: conds.append("tier=:tier");    params["tier"] = tier
        try:
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text(
                    f"SELECT model_id,pair,day_flag,tier,date_from,date_to,"
                    f"total_combinations,best_score,avg_score,"
                    f"best_accuracy,avg_accuracy,best_params_json,computed_at "
                    f"FROM vlad_backtest_summary WHERE {' AND '.join(conds)} "
                    f"ORDER BY computed_at DESC"), params)
                rows = []
                for r in res.mappings().all():
                    rows.append({
                        "model_id":   r["model_id"], "pair": r["pair"],
                        "day_flag":   r["day_flag"], "tier": r["tier"],
                        "date_from":  r["date_from"].isoformat() if r["date_from"] else None,
                        "date_to":    r["date_to"].isoformat()   if r["date_to"]   else None,
                        "total_combinations": r["total_combinations"],
                        "best_score":    float(r["best_score"]),
                        "avg_score":     float(r["avg_score"]),
                        "best_accuracy": float(r["best_accuracy"]),
                        "avg_accuracy":  float(r["avg_accuracy"]),
                        "best_params":   _json.loads(r["best_params_json"])
                                         if r["best_params_json"] else None,
                        "computed_at":   r["computed_at"].isoformat()
                                         if r["computed_at"] else None,
                    })
            return ok_response(rows)
        except Exception as e:
            return err_response(str(e))

    @app.get("/pretest")
    async def ep_pretest():
        log(" PRETEST START", s.NODE_NAME, force=True)
        model_path = os.path.join(os.path.dirname(os.path.abspath(
            sys.modules[model_module.__name__].__file__)), "model.py")

        #  Тест 1: синтаксис model.py 
        try:
            import ast as _ast
            with open(model_path, "r", encoding="utf-8") as f:
                _ast.parse(f.read())
        except SyntaxError as e:
            return {"status": "error",
                    "error": f"[Тест 1 — Синтаксис] строка {e.lineno}: {e.msg}"}
        except OSError:
            pass
        log("   Тест 1: синтаксис model.py OK", s.NODE_NAME, force=True)

        # 
        # Тест 2: структура dict[str, float] на одной дате
        # Тест 3: по КАЖДОМУ из 6 инструментов (EUR/BTC/ETH × hour/day):
        #   - date_start берётся из brain.brain_models WHERE id=SERVICE_ID (поле date)
        #     фолбэк: 2025-01-15 если таблица недоступна или запись не найдена
        #   - 10 случайных дат >= date_start из котировок инструмента
        #   - 90% вызовов model() должны давать непустой dict
        # IS_SIMPLE и full: одинаковая логика — global_rates загружен для обоих.
        # 

        # Загружаем даты всех 6 инструментов из global_rates (одинаково для IS_SIMPLE и full).
        _instr_dates: dict[str, list] = {}
        for _tbl_pt in _RATES_TABLES:
            _rr_pt = s.global_rates.get(_tbl_pt, [])
            _instr_dates[_tbl_pt] = [r["date"] for r in _rr_pt]

        #  Тест 2: проверяем структуру model() 
        if s.IS_SIMPLE:
            if not s.simple_rates:
                return {"status": "error",
                        "error": "[Тест 2 — Структура] simple_rates пустой"}
            if not s.dataset:
                return {"status": "error",
                        "error": "[Тест 2 — Структура] dataset пустой"}
            _ds_dates_pt = sorted(
                e["date"] for e in s.dataset if e.get("date") is not None)
            if not _ds_dates_pt:
                return {"status": "error",
                        "error": "[Тест 2 — Структура] dataset не содержит дат"}
            _mid_pt  = _ds_dates_pt[len(_ds_dates_pt) // 2]
            _eur_dts = _instr_dates.get(s.RATES_TABLE_SIMPLE, s.simple_rates_dates)
            _td2     = next((d for d in _eur_dts if d >= _mid_pt),
                            s.simple_rates[-1]["date"])
            _rf2 = _simple_rates_lte(_td2, s)
            _ds2 = _filter_dataset_lte(_td2, s)
            try:
                _res2 = s.model_fn(rates=_rf2, dataset=_ds2, date=_td2,
                                   type=0, var=s.VAR_RANGE[0], param="", dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
                _res2, _ = _extract_detail(_res2)
            except Exception as _e2:
                return {"status": "error",
                        "error": f"[Тест 2 — Структура] model() exception: {_e2}"}
        else:
            _mid2_found = False
            for _pid2, _tfs2 in _INSTRUMENTS.items():
                _rows2 = s.global_rates.get(_tfs2["hour"], [])
                if not _rows2:
                    continue
                _td2  = _rows2[len(_rows2) // 2]["date"]
                _dts2 = _instr_dates.get(_tfs2["hour"], [])
                _rf2  = _rows2[:bisect.bisect_right(_dts2, _td2)] if _dts2 else []
                _ds2  = _filter_dataset_lte(_td2, s)
                try:
                    if s.model_needs_ctx:
                        _ctx2 = _ModelContext(_tfs2["hour"], _pid2, 0, _td2, s)
                        _res2 = s.model_fn(rates=_rf2, dataset=_ds2, date=_td2,
                                           type=0, var=s.VAR_RANGE[0], param="", ctx=_ctx2, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
                    else:
                        _res2 = s.model_fn(rates=_rf2, dataset=_ds2, date=_td2,
                                           type=0, var=s.VAR_RANGE[0], param="", dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None))
                except Exception as _e2:
                    return {"status": "error",
                            "error": f"[Тест 2 — Структура] model() exception: {_e2}"}
                _res2, _ = _extract_detail(_res2)
                _mid2_found = True
                break
            if not _mid2_found:
                return {"status": "error",
                        "error": "[Тест 2 — Структура] нет данных котировок"}

        if _res2 is None:
            return {"status": "error",
                    "error": "[Тест 2 — Структура] model() вернул None"}
        if not isinstance(_res2, dict):
            return {"status": "error",
                    "error": f"[Тест 2 — Структура] ожидается dict, получен {type(_res2).__name__}"}
        for _k2, _v2 in _res2.items():
            if not isinstance(_k2, str):
                return {"status": "error",
                        "error": f"[Тест 2 — Структура] ключ {_k2!r} не str"}
            if not isinstance(_v2, (int, float)) or _v2 != _v2 or abs(_v2) == float("inf"):
                return {"status": "error",
                        "error": f"[Тест 2 — Структура] значение '{_k2}' не конечный float"}
        log(f"   Тест 2: структура model() OK — {len(_res2)} ключей",
            s.NODE_NAME, force=True)

        #  Тест 3: по каждому из 6 инструментов 
        # Дата старта: из brain.brain_models WHERE id = SERVICE_ID, поле date.
        # Фолбэк: 2025-01-15.
        # Берём 10 случайных дат >= date_start из котировок каждого инструмента.
        # 90% вызовов model() должны давать непустой dict.
        _pt3_date_start: datetime = datetime(2025, 1, 15)
        try:
            async with s.engine_brain.connect() as _conn_pt3:
                _row_pt3 = (await _conn_pt3.execute(
                    text("SELECT `date` FROM `brain_models` WHERE `id` = :sid LIMIT 1"),
                    {"sid": s.SERVICE_ID}
                )).fetchone()
                if _row_pt3 and _row_pt3[0] is not None:
                    _val_pt3 = _row_pt3[0]
                    if isinstance(_val_pt3, datetime):
                        _pt3_date_start = _val_pt3
                    else:
                        # date object → datetime
                        _pt3_date_start = datetime(_val_pt3.year, _val_pt3.month, _val_pt3.day)
                    log(f"  [Тест 3] date_start из brain_models: {_pt3_date_start.date()}",
                        s.NODE_NAME, force=True)
                else:
                    log(f"  [Тест 3] brain_models нет записи для SERVICE_ID={s.SERVICE_ID}, "
                        f"фолбэк 2025-01-15", s.NODE_NAME, force=True)
        except Exception as _e_pt3:
            log(f"  [Тест 3] brain_models недоступна ({_e_pt3}), фолбэк 2025-01-15",
                s.NODE_NAME, level="warning", force=True)

        _failures3: list = []

        # Собираем задачи для всех инструментов
        _tasks3: list = []   # (pid, tf, tbl, day, td) — метаданные
        _coros3: list = []   # coroutine для каждого вызова model_fn

        for _pid3, _tfs3 in _INSTRUMENTS.items():
            for _tf3, _tbl3 in _tfs3.items():
                _dts3 = _instr_dates.get(_tbl3, [])
                if not _dts3:
                    _failures3.append(f"pair{_pid3}/{_tf3}: нет котировок")
                    continue

                _day3  = 1 if _tf3 == "day" else 0
                _rows3 = s.global_rates.get(_tbl3, [])

                # 10 случайных дат >= _pt3_date_start из котировок инструмента
                _pool3 = [d for d in _dts3 if d >= _pt3_date_start]
                if not _pool3:
                    # Фолбэк: берём из всего диапазона котировок
                    _pool3 = _dts3
                    log(f"  [Тест 3] pair{_pid3}/{_tf3}: нет дат >= {_pt3_date_start.date()}, "
                        f"используем весь диапазон", s.NODE_NAME, force=True)
                _sample3 = random.sample(_pool3, min(10, len(_pool3)))
                _sample3.sort()
                log(f"  [Тест 3] pair{_pid3}/{_tf3}: {len(_sample3)} дат "
                    f"(от {_sample3[0].date() if _sample3 else '—'} "
                    f"до {_sample3[-1].date() if _sample3 else '—'})...",
                    s.NODE_NAME, force=True)

                for _td3 in _sample3:
                    _rf3 = _rows3[:bisect.bisect_right(_dts3, _td3)]
                    _ds3 = _filter_dataset_lte(_td3, s)

                    if s.model_needs_ctx:
                        _ctx3 = _ModelContext(_tbl3, _pid3, _day3, _td3, s)
                        def _mk3(_r=_rf3, _d=_ds3, _t=_td3, _c=_ctx3):
                            res, _ = _extract_detail(
                                s.model_fn(rates=_r, dataset=_d, date=_t,
                                           type=0, var=s.VAR_RANGE[0], param="", ctx=_c, dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None)))
                            return bool(res)
                    else:
                        def _mk3(_r=_rf3, _d=_ds3, _t=_td3):
                            res, _ = _extract_detail(
                                s.model_fn(rates=_r, dataset=_d, date=_t,
                                           type=0, var=s.VAR_RANGE[0], param="", dataset_index=({"dates": s.dataset_dates, "by_key": s.dataset_by_key, "key_dates": s.dataset_key_dates, "key_field": s.dataset_key_field, "np_rates": s.np_simple_rates} if s.model_needs_index else None)))
                            return bool(res)

                    _tasks3.append((_pid3, _tf3, _tbl3, _day3, _td3))
                    _coros3.append(asyncio.to_thread(_mk3))


        # Запускаем все вызовы параллельно
        _results3 = await asyncio.gather(*_coros3, return_exceptions=True)

        # Агрегируем результаты по инструментам
        _instr_counts: dict = {}   # (pid, tf) -> [ne, total]
        for (_pid3, _tf3, _tbl3, _day3, _td3), _r3 in zip(_tasks3, _results3):
            key = (_pid3, _tf3)
            if key not in _instr_counts:
                _instr_counts[key] = [0, 0]
            _instr_counts[key][1] += 1
            if isinstance(_r3, Exception):
                log(f"     pair{_pid3}/{_tf3} {_td3}: {_r3}", s.NODE_NAME, level="warning")
            elif _r3:
                _instr_counts[key][0] += 1


        for ((_pid3, _tf3), (_ne3, _tot3)) in _instr_counts.items():
            _cov3 = _ne3 / _tot3 if _tot3 else 0
            _ok3  = _cov3 >= 0.90
            log(f"  {chr(9989) if _ok3 else chr(10060)} pair{_pid3}/{_tf3}: "
                f"{_ne3}/{_tot3} ({_cov3:.0%}), порог 90%",
                s.NODE_NAME, force=True)
            if not _ok3:
                _failures3.append(
                    f"pair{_pid3}/{_tf3}: {_ne3}/{_tot3} ({_cov3:.0%}) < 90%")

        if _failures3:
            log(f" PRETEST FAILED: {_failures3}", s.NODE_NAME, force=True)
            return {"status": "error",
                    "error": f"[Тест 3 — Покрытие] {' | '.join(_failures3)}"}

        log(" PRETEST PASSED", s.NODE_NAME, force=True)
        return {"status": "ok", "var_range": s.VAR_RANGE,
                "model_has_ctx": s.model_needs_ctx,
                "is_simple": s.IS_SIMPLE,
                "np_built": s.np_built}

    @app.get("/universe_sync")
    async def ep_universe_sync():
        if s.IS_SIMPLE:
            return err_response("universe_sync не поддерживается в IS_SIMPLE режиме.")
        try:
            async with s.engine_vlad.begin() as conn:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `vlad_weights_universe` (
                        `id`          INT         NOT NULL AUTO_INCREMENT,
                        `service_id`  INT         NOT NULL,
                        `weight_code` VARCHAR(64) NOT NULL,
                        `added_at`    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `uk_svc_code` (`service_id`, `weight_code`),
                        INDEX idx_service_id (`service_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text("""
                    SELECT DISTINCT weight_code FROM vlad_values_cache
                    WHERE service_url=:url
                      AND weight_code IS NOT NULL AND weight_code != ''
                """), {"url": s.service_url})
                cache_codes: set[str] = {row[0] for row in res.fetchall()}
            if not cache_codes:
                return ok_response({
                    "total_in_cache": 0, "already_in_universe": 0, "added": 0,
                    "note": "Кеш пуст. Сначала /fill_cache, потом /universe_sync.",
                })
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text("""
                    SELECT weight_code FROM vlad_weights_universe
                    WHERE service_id=:sid
                """), {"sid": s.SERVICE_ID})
                existing: set[str] = {row[0] for row in res.fetchall()}
            new_codes = cache_codes - existing
            if new_codes:
                async with s.engine_vlad.begin() as conn:
                    for code in new_codes:
                        await conn.execute(text("""
                            INSERT IGNORE INTO vlad_weights_universe
                                (service_id, weight_code)
                            VALUES (:sid, :code)
                        """), {"sid": s.SERVICE_ID, "code": code})
            return ok_response({
                "total_in_cache":      len(cache_codes),
                "already_in_universe": len(existing & cache_codes),
                "added":               len(new_codes),
            })
        except Exception as e:
            send_error_trace(e, s.NODE_NAME, "universe_sync")
            return err_response(str(e))

    @app.get("/posttest")
    async def ep_posttest(
        pairs:     str = Query("1,3,4"),
        days:      str = Query("0,1"),
        date_from: str = Query(""),
        date_to:   str = Query(""),
    ):
        # IS_SIMPLE и full: одинаковая структура вывода {"1":{"hour":{...},"day":{...}}, "3":{...}, "4":{...}}
        # IS_SIMPLE: кеш хранится с pair=0/day=0, но статистика считается по котировкам
        #   каждого из 6 инструментов отдельно (global_rates загружен для всех).
        # full: котировки и кеш по каждой паре/таймфрейму.
        try:
            pl = [int(p.strip()) for p in pairs.split(",") if p.strip()]
            dl = [int(d.strip()) for d in days.split(",")  if d.strip()]
        except ValueError:
            return err_response("pairs и days — числа через запятую")

        from datetime import datetime as _dt

        dt_from = _parse_date(date_from) if date_from.strip() else _parse_date(s.CACHE_DATE_FROM)
        dt_to   = _parse_date(date_to)   if date_to.strip()   else _dt.now()

        async def _process_slot(pair_id: int, day_flag: int) -> dict:
            tf_name = "day" if day_flag else "hour"
            table   = _rates_table(pair_id, day_flag)
            all_rows = s.global_rates.get(table, [])
            rows = [r for r in all_rows
                    if (dt_from is None or r["date"] >= dt_from)
                    and (dt_to   is None or r["date"] <= dt_to)]

            # IS_SIMPLE: кеш записывался с pair=0/day=0 — читаем оттуда.
            # full:      кеш по реальной паре/таймфрейму.
            _cache_pair = 0 if s.IS_SIMPLE else pair_id
            _cache_day  = 0 if s.IS_SIMPLE else day_flag
            url_p = {"url": s.service_url, "pair": _cache_pair, "day": _cache_day,
                     "df": dt_from, "dt": dt_to}

            _tbl = s.cache_table

            # Тест 1: SQL-агрегат по количеству ключей
            data_stats = {"min": 0.0, "max": 0.0, "avg": 0.0}
            try:
                async with s.engine_vlad.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT MIN(JSON_LENGTH(result_json)),
                               MAX(JSON_LENGTH(result_json)),
                               AVG(JSON_LENGTH(result_json))
                        FROM `{_tbl}`
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND result_json != '{{}}'
                          AND (:df IS NULL OR date_val >= :df)
                          AND (:dt IS NULL OR date_val <= :dt)
                    """), url_p)
                    row = res.fetchone()
                    if row and row[0] is not None:
                        data_stats = {"min": float(row[0]), "max": float(row[1]),
                                      "avg": round(float(row[2]), 4)}
            except Exception as e:
                log(f"   posttest t1 {pair_id}/{tf_name}: {e}", s.NODE_NAME, level="warning")

            # Тест 2: подсчёт знаков
            values_stats = {"plus": 0, "minus": 0}
            try:
                async with s.engine_vlad.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT result_json FROM `{_tbl}`
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND result_json != '{{}}'
                          AND (:df IS NULL OR date_val >= :df)
                          AND (:dt IS NULL OR date_val <= :dt)
                    """), url_p)
                    plus_cnt = minus_cnt = 0
                    for (rj_str,) in res:
                        try:
                            for v in _json.loads(rj_str).values():
                                if v > 0:   plus_cnt  += 1
                                elif v < 0: minus_cnt += 1
                        except Exception:
                            pass
                    values_stats = {"plus": plus_cnt, "minus": minus_cnt}
            except Exception as e:
                log(f"   posttest t2 {pair_id}/{tf_name}: {e}", s.NODE_NAME, level="warning")

            # Тест 3: живой вызов с актуальной датой
            # IS_SIMPLE: _call_model использует simple_rates (корректно для model())
            # full:      _call_model использует котировки конкретной пары
            now_str = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                sync_result = await _call_model(pair_id, day_flag, now_str)
                sync_out = sync_result or {}
            except Exception as e:
                sync_out = {"error": str(e)}

            # Читаем кэш один раз для тестов 4 и 5
            # IS_SIMPLE: для анализа проплешин и симуляции используем котировки
            #   конкретного инструмента из global_rates, но сигналы из общего кеша.
            cache_signals: dict = {}
            try:
                async with s.engine_vlad.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT date_val, result_json FROM `{_tbl}`
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND (:df IS NULL OR date_val >= :df)
                          AND (:dt IS NULL OR date_val <= :dt)
                        ORDER BY date_val
                    """), url_p)
                    for (dt_val, rj_str) in res:
                        try:
                            rj = _json.loads(rj_str)
                            cache_signals[dt_val] = (bool(rj), sum(rj.values()) if rj else 0.0)
                        except Exception:
                            cache_signals[dt_val] = (False, 0.0)
            except Exception as e:
                log(f"   posttest cache {pair_id}/{tf_name}: {e}", s.NODE_NAME, level="warning")

            def _compute_hole_and_sim():
                non_empty = np.array(
                    [cache_signals.get(r["date"], (False, 0.0))[0] for r in rows],
                    dtype=bool,
                )
                if len(non_empty) == 0 or np.all(non_empty):
                    hole = 0
                elif not np.any(non_empty):
                    hole = len(rows)
                else:
                    padded = np.concatenate(([False], ~non_empty, [False]))
                    diffs  = np.diff(padded.astype(np.int8))
                    runs   = np.where(diffs == -1)[0] - np.where(diffs == 1)[0]
                    hole   = int(np.max(runs)) if len(runs) > 0 else 0

                _, mod, lot_div = _PAIR_CFG.get(pair_id, (0.0002, 100_000.0, 50_000.0))
                equity = 10_000.0
                total_profit = total_dropdown = 0.0
                wins = trades = 0
                position = None
                entry_price = 0.0
                direction = 0.0

                for i, r in enumerate(rows):
                    _, signal = cache_signals.get(r["date"], (False, 0.0))
                    if i + 1 >= len(rows):
                        continue
                    op = rows[i + 1]["open"]
                    if not op:
                        continue
                    if position is not None:
                        if (signal == 0.0
                                or (signal > 0 and direction < 0)
                                or (signal < 0 and direction > 0)):
                            pnl    = equity * 0.10 * (op - entry_price) / entry_price * direction
                            equity += pnl
                            trades += 1
                            if pnl >= 0: total_profit   += pnl;  wins += 1
                            else:        total_dropdown += abs(pnl)
                            position = None
                    if signal != 0.0 and position is None:
                        direction = 1.0 if signal > 0 else -1.0
                        entry_price = op
                        position = (direction, entry_price)

                if position is not None and rows:
                    lp = rows[-1]["close"]
                    if lp and entry_price:
                        pnl    = equity * 0.10 * (lp - entry_price) / entry_price * direction
                        equity += pnl
                        trades += 1
                        if pnl >= 0: total_profit   += pnl;  wins += 1
                        else:        total_dropdown += abs(pnl)

                cw = round(wins / trades, 4) if trades > 0 else 0.0
                return hole, {
                    "profit":   round(total_profit,   2),
                    "dropdown": round(total_dropdown, 2),
                    "cw":       cw,
                    "result":   round(total_profit - total_dropdown, 2),
                }

            hole, history_stats = await asyncio.to_thread(_compute_hole_and_sim)

            return {
                "data":    data_stats,
                "values":  values_stats,
                "sync":    sync_out,
                "hole":    hole,
                "history": history_stats,
            }

        slots   = [(p, d) for p in pl for d in dl]
        tasks   = [asyncio.create_task(_process_slot(p, d)) for p, d in slots]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: dict = {}
        for (pair_id, day_flag), result in zip(slots, results):
            pair_str = str(pair_id)
            tf_name  = "day" if day_flag else "hour"
            output.setdefault(pair_str, {})
            output[pair_str][tf_name] = (
                {"error": str(result)} if isinstance(result, Exception) else result
            )

        return ok_response(output)

    return app