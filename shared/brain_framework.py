"""
brain_framework.py v14 — IS_SIMPLE режим + опциональный ML (USE_ML_VALUES).
Все оптимизации применены: bulk upsert, searchsorted, threadpool executor.
"""

from __future__ import annotations

import asyncio
import bisect
import concurrent.futures as _cf
import inspect
import json as _json
import math
import os
import random
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

try:
    import requests as _requests
except ImportError:
    _requests = None  # type: ignore

import numpy as np

from fastapi import FastAPI, Query
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Трассировка
# ──────────────────────────────────────────────────────────────────────────────
_TRACE_HANDLER = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
_TRACE_URL     = f"{_TRACE_HANDLER}/trace.php"
_ALERT_EMAIL   = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def _send_trace(subject: str, body: str, node: str, is_error: bool = False) -> None:
    if _requests is None:
        return
    level = "ERROR" if is_error else "INFO"
    try:
        _requests.post(
            _TRACE_URL,
            data={"url": "fill_cache", "node": node, "email": _ALERT_EMAIL,
                  "logs": f"[{level}] {subject}\n\nNode: {node}\n\n{body}"},
            timeout=10,
        )
    except Exception:
        pass


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
import reverse_learning as rl

# ──────────────────────────────────────────────────────────────────────────────
# THREADPOOL ДЛЯ ПАРАЛЛЕЛЬНЫХ ВЫЧИСЛЕНИЙ
# ──────────────────────────────────────────────────────────────────────────────
_FILL_EXECUTOR = _cf.ThreadPoolExecutor(
    max_workers=min(8, (os.cpu_count() or 4) * 2),
    thread_name_prefix="fill_worker",
)

# ══════════════════════════════════════════════════════════════════════════════
# NUMPY-УТИЛИТЫ
# ══════════════════════════════════════════════════════════════════════════════

def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def _build_np_rates_for_table(rates, candle_ranges, extremums, global_rates_list=None):
    if not rates:
        return None
    sorted_dates = sorted(rates.keys())
    n            = len(sorted_dates)
    dates_ns     = np.array([_dt_to_ts(d) for d in sorted_dates], dtype=np.int64)
    t1_arr       = np.array([rates.get(d, 0.0) for d in sorted_dates], dtype=np.float64)
    ranges_arr   = np.array([candle_ranges.get(d, 0.0) for d in sorted_dates], dtype=np.float64)
    ext_min_set  = extremums.get("min", set())
    ext_max_set  = extremums.get("max", set())
    ext_min_arr  = np.fromiter((d in ext_min_set for d in sorted_dates), dtype=bool, count=n)
    ext_max_arr  = np.fromiter((d in ext_max_set for d in sorted_dates), dtype=bool, count=n)
    if global_rates_list:
        _gr_map   = {r["date"]: r for r in global_rates_list}
        close_arr = np.array([float(_gr_map[d]["close"]) if d in _gr_map else 0.0
                              for d in sorted_dates], dtype=np.float64)
        open_arr  = np.array([float(_gr_map[d]["open"])  if d in _gr_map else 0.0
                              for d in sorted_dates], dtype=np.float64)
        max_arr   = np.array([float(_gr_map[d]["max"])   if d in _gr_map else 0.0
                              for d in sorted_dates], dtype=np.float64)
        min_arr   = np.array([float(_gr_map[d]["min"])   if d in _gr_map else 0.0
                              for d in sorted_dates], dtype=np.float64)
    else:
        close_arr = np.zeros(n, dtype=np.float64)
        open_arr  = np.zeros(n, dtype=np.float64)
        max_arr   = np.zeros(n, dtype=np.float64)
        min_arr   = np.zeros(n, dtype=np.float64)
    return {
        "dates_ns": dates_ns, "t1": t1_arr, "ranges": ranges_arr,
        "ext_min": ext_min_arr, "ext_max": ext_max_arr,
        "close": close_arr, "open": open_arr,
        "max": max_arr, "min": min_arr,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ПУБЛИЧНЫЕ ХЕЛПЕРЫ ДЛЯ rebuild_index() В model.py
# ══════════════════════════════════════════════════════════════════════════════

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


# === ПАТЧ 3: upsert_ctx_rows — BULK executemany ===
async def upsert_ctx_rows(engine, table: str, rows: list[dict]) -> dict[str, int]:
    if not rows:
        return {}
    
    _reserved  = {"fingerprint_hash", "occurrence_count", "first_dt", "last_dt"}
    extra_cols = [k for k in rows[0] if k not in _reserved]
    extra_insert = (", ".join(f"`{c}`" for c in extra_cols) + ", ") if extra_cols else ""
    extra_vals   = (", ".join(f":{c}" for c in extra_cols) + ", ") if extra_cols else ""

    params_list = []
    for row in rows:
        p = {"fp": row["fingerprint_hash"], "cnt": row["occurrence_count"],
             "fd": row.get("first_dt"), "ld": row.get("last_dt")}
        for c in extra_cols:
            p[c] = row.get(c)
        params_list.append(p)

    async with engine.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{table}`
                ({extra_insert}`fingerprint_hash`, `occurrence_count`, `first_dt`, `last_dt`)
            VALUES ({extra_vals}:fp, :cnt, :fd, :ld)
            ON DUPLICATE KEY UPDATE
                occurrence_count = occurrence_count + :cnt,
                last_dt  = IF(:ld > last_dt  OR last_dt  IS NULL, :ld,  last_dt),
                first_dt = IF(:fd < first_dt OR first_dt IS NULL, :fd, first_dt)
        """), params_list)

    fp_list  = [r["fingerprint_hash"] for r in rows]
    fp_to_id: dict[str, int] = {}
    async with engine.connect() as conn:
        for i in range(0, len(fp_list), 500):
            batch        = fp_list[i:i + 500]
            placeholders = ", ".join(f":fp{j}" for j in range(len(batch)))
            params       = {f"fp{j}": fp for j, fp in enumerate(batch)}
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


# === ПАТЧ 4: insert_weight_codes — BULK INSERT ===
async def insert_weight_codes(engine, table, ctx_id, make_code_fn,
                               occurrence_count, recurring_min_count=2,
                               shift_max=24, modes=(0, 1)) -> int:
    max_shift = shift_max if occurrence_count >= recurring_min_count else 0
    rows = [
        {"wc": make_code_fn(ctx_id, mode, shift), "cid": ctx_id,
         "mode": mode, "shift": shift}
        for mode in modes
        for shift in range(0, max_shift + 1)
    ]
    if not rows:
        return 0
    async with engine.begin() as conn:
        r = await conn.execute(text(f"""
            INSERT IGNORE INTO `{table}` (weight_code, ctx_id, mode, shift)
            VALUES (:wc, :cid, :mode, :shift)
        """), rows)
    return r.rowcount


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


# ══════════════════════════════════════════════════════════════════════════════
# STATE
# ══════════════════════════════════════════════════════════════════════════════

class _State:
    SERVICE_ID:   int = 0
    PORT:         int = 9000
    NODE_NAME:    str = "brain-svc"
    SERVICE_TEXT: str = "Brain microservice"

    WEIGHTS_TABLE:       str | None = None
    WEIGHTS_CODE_COLUMN: str        = "weight_code"
    CTX_TABLE:           str | None = None
    CTX_QUERY:           str | None = None
    CTX_KEY_COLUMNS:     list       = None
    DATASET_TABLE:       str | None = None
    DATASET_QUERY:       str | None = None
    DATASET_ENGINE:      str        = "vlad"
    FILTER_DATASET_BY_DATE: bool    = False
    SHIFT_WINDOW:        int        = 12
    RELOAD_INTERVAL:     int        = 3600
    REBUILD_INTERVAL:    int        = 0
    VAR_RANGE:           list       = None
    TYPES_RANGE:         list       = None
    CACHE_DATE_FROM:     str        = "2025-01-15"
    LABEL_FN:            object     = None

    URL_MAP_QUERY:  str | None = None
    URL_MAP_ENGINE: str        = "vlad"

    USE_ML_VALUES:        bool  = False
    ML_INIT_MODE:         str   = "constant"
    ML_TARGET_PRECISION:  float = 0.95
    ML_MAX_ITER:          int   = 20
    ML_STEP:              float = 0.10
    ML_EXTREMUM_LIMIT:    int   = 50
    ML_ACTIVE_TAIL:       int   = 0
    ML_PRECISION_METRIC:  str   = "mean"

    def __init__(self):
        self.CTX_KEY_COLUMNS   = ["id"]
        self.VAR_RANGE         = []
        self.TYPES_RANGE       = [0, 1, 2, 3, 4]

        self.model_fn          = None
        self.index_builder_fn  = None
        self.weight_builder_fn = None
        self.model_needs_index = False

        self.engine_vlad  = None
        self.engine_brain = None
        self.engine_super = None

        self.weight_codes:  list       = []
        self.ctx_index:     dict       = {}
        self.url_map:       dict       = {}
        self.dataset:       list[dict] = []
        self.dataset_dates:     list = []
        self.dataset_by_key:    dict = {}
        self.dataset_key_dates: dict = {}
        self.dataset_key_field: str  = "ctx_id"
        self._dataset_ts_arr:   np.ndarray | None = None  # для быстрого searchsorted

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

        self.np_rates: dict      = {}
        self.np_built: bool      = False

        self.RATES_TABLE:         str   = "brain_rates_eur_usd"
        self.simple_rates:        list  = []
        self.simple_rates_dates:  list  = []
        self.last_simple_rate_dt: datetime | None = None
        self.np_simple_rates:     dict | None     = None

        self._cache_table: str  = "vlad_values_cache"
        self._label_cache:  dict = {}

        self.reverse_store: rl.ReverseStore | None = None
        self._ml_active_cache: dict = {}
        # Semaphore: ensures only 1 ML train runs at a time, preventing DB pool exhaustion
        self._ml_semaphore: asyncio.Semaphore = asyncio.Semaphore(1)
        # Flag: True during fill_cache — skips vlad_reverse_universe DB writes for speed
        self._fill_cache_active: bool = False

    @property
    def cache_table(self) -> str:
        return self._cache_table


# ══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

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
    idx = bisect.bisect_right([r["date"] for r in rows], date)
    return rows[:idx]


# === ПАТЧ 1: _filter_dataset_lte — binary search ===
def _filter_dataset_lte(date: datetime, s: _State) -> list[dict]:
    if not s.FILTER_DATASET_BY_DATE:
        return s.dataset
    idx = bisect.bisect_right(s.dataset_dates, date)
    return s.dataset[:idx]


# ══════════════════════════════════════════════════════════════════════════════
# NUMPY: BUILD / REBUILD / APPEND
# ══════════════════════════════════════════════════════════════════════════════

def _rebuild_np_rates(s: _State) -> None:
    s.np_built = False
    s.np_rates.clear()
    for table in _RATES_TABLES:
        np_r = _build_np_rates_for_table(
            s.rates.get(table, {}),
            s.candle_ranges.get(table, {}),
            s.extremums.get(table, {}),
            s.global_rates.get(table, []),
        )
        if np_r is not None:
            np_r["avg_range"] = s.avg_range.get(table, 0.0)
        s.np_rates[table] = np_r
    s.np_built = True


def _append_np_rates_row(table, dt, t1, rng, s: _State, close=0.0, open_=0.0,
                         max_=0.0, min_=0.0) -> None:
    np_r = s.np_rates.get(table)
    if np_r is None:
        return
    ts = np.int64(_dt_to_ts(dt))
    np_r["dates_ns"] = np.append(np_r["dates_ns"], ts)
    np_r["t1"]       = np.append(np_r["t1"],       np.float64(t1 if t1 is not None else 0.0))
    np_r["ranges"]   = np.append(np_r["ranges"],   np.float64(rng))
    np_r["ext_min"]  = np.append(np_r["ext_min"],  False)
    np_r["ext_max"]  = np.append(np_r["ext_max"],  False)
    np_r["close"]    = np.append(np_r["close"],    np.float64(close))
    np_r["open"]     = np.append(np_r["open"],     np.float64(open_))
    if "max" in np_r:
        np_r["max"] = np.append(np_r["max"], np.float64(max_))
    if "min" in np_r:
        np_r["min"] = np.append(np_r["min"], np.float64(min_))


# ══════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════════

async def _load_rates(s: _State):
    for table in _RATES_TABLES:
        s.rates[table]        = {}
        s.last_candles[table] = []
        s.candle_ranges[table] = {}
        s.extremums[table]    = {"min": set(), "max": set()}
        s.global_rates[table] = []
        try:
            async with s.engine_brain.connect() as conn:
                res = await conn.execute(text(
                    f"SELECT date, open, close, `max`, `min`, t1 "
                    f"FROM `{table}` ORDER BY date"))
                ranges = []
                for r in res.mappings().all():
                    dt  = r["date"]
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
                    q  = (f"SELECT t1.date FROM `{table}` t1 "
                          f"JOIN `{table}` tp ON tp.date = t1.date - INTERVAL {interval} "
                          f"JOIN `{table}` tn ON tn.date = t1.date + INTERVAL {interval} "
                          f"WHERE t1.`{typ}` {op} tp.`{typ}` AND t1.`{typ}` {op} tn.`{typ}`")
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
                    _append_np_rates_row(table, dt, t1, rng, s,
                                         close=float(r["close"] or 0),
                                         open_=float(r["open"]  or 0),
                                         max_=float(r["max"] or 0),
                                         min_=float(r["min"] or 0))
                n += 1
            if n > 0:
                log(f"   +{n} candle(s) {table}", s.NODE_NAME)
    except Exception as e:
        log(f"   refresh {table}: {e}", s.NODE_NAME, level="warning")


# ── Котировки основного инструмента ─────────────────────────────────────────────

def _build_np_simple_rates(s: _State) -> None:
    if not s.simple_rates:
        s.np_simple_rates = None
        return
    n         = len(s.simple_rates)
    dates_ns  = np.array([_dt_to_ts(r["date"]) for r in s.simple_rates], dtype=np.int64)
    t1_arr    = np.array([float((r.get("close") or 0) - (r.get("open") or 0))
                          for r in s.simple_rates], dtype=np.float64)
    rng_arr   = np.array([float((r.get("max") or 0) - (r.get("min") or 0))
                          for r in s.simple_rates], dtype=np.float64)
    avg_rng   = float(np.mean(rng_arr)) if n > 0 else 0.0
    max_arr   = np.array([float(r.get("max") or 0) for r in s.simple_rates], dtype=np.float64)
    min_arr   = np.array([float(r.get("min") or 0) for r in s.simple_rates], dtype=np.float64)
    ext_max   = np.zeros(n, dtype=bool)
    ext_min   = np.zeros(n, dtype=bool)
    if n > 2:
        ext_max[1:-1] = (max_arr[1:-1] > max_arr[:-2]) & (max_arr[1:-1] > max_arr[2:])
        ext_min[1:-1] = (min_arr[1:-1] < min_arr[:-2]) & (min_arr[1:-1] < min_arr[2:])
    s.np_simple_rates = {
        "dates_ns":  dates_ns,
        "t1":        t1_arr,
        "ranges":    rng_arr,
        "avg_range": avg_rng,
        "ext_max":   ext_max,
        "ext_min":   ext_min,
        "close":     np.array([float(r.get("close") or 0) for r in s.simple_rates], dtype=np.float64),
        "open":      np.array([float(r.get("open")  or 0) for r in s.simple_rates], dtype=np.float64),
        "max":       max_arr,
        "min":       min_arr,
    }


async def _load_simple_rates(s: _State) -> None:
    table = s.RATES_TABLE
    try:
        async with s.engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `min`, `max` FROM `{table}` ORDER BY date"))
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
        log(f"  simple_rates ({table}): {len(s.simple_rates)} candles", s.NODE_NAME)
    except Exception as e:
        log(f"   simple_rates: {e}", s.NODE_NAME, level="error")
        s.simple_rates, s.simple_rates_dates = [], []
        s.np_simple_rates = None


async def _refresh_simple_rates(s: _State) -> None:
    if not s.last_simple_rate_dt:
        return
    table = s.RATES_TABLE
    try:
        async with s.engine_brain.connect() as conn:
            res = await conn.execute(text(
                f"SELECT date, open, close, `min`, `max` "
                f"FROM `{table}` WHERE date > :dt ORDER BY date"),
                {"dt": s.last_simple_rate_dt})
            new_rows = res.mappings().all()
        for r in new_rows:
            row = {"date":  r["date"], "open":  float(r["open"]  or 0),
                   "close": float(r["close"] or 0), "min": float(r["min"] or 0),
                   "max":   float(r["max"]   or 0)}
            s.simple_rates.append(row)
            s.simple_rates_dates.append(row["date"])
        if new_rows:
            s.last_simple_rate_dt = s.simple_rates_dates[-1]
            _build_np_simple_rates(s)
            log(f"   +{len(new_rows)} candle(s) {table}", s.NODE_NAME)
    except Exception as e:
        log(f"   refresh simple_rates: {e}", s.NODE_NAME, level="warning")


# ── Веса, контекст, датасет ───────────────────────────────────────────────────

async def _load_weight_codes(s: _State):
    if not s.WEIGHTS_TABLE:
        s.weight_codes = []
        return
    try:
        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(
                f"SELECT `{s.WEIGHTS_CODE_COLUMN}` FROM `{s.WEIGHTS_TABLE}`"))
            s.weight_codes        = [r[0] for r in res.fetchall()]
            s.weights_row_count   = len(s.weight_codes)
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


async def _load_url_map(s: _State):
    if not s.URL_MAP_QUERY:
        s.url_map = {}
        return
    try:
        async with _engine_for(s.URL_MAP_ENGINE, s).connect() as conn:
            res = await conn.execute(text(s.URL_MAP_QUERY))
            s.url_map = {r["url"]: r["event_id"] for r in res.mappings().all()
                         if r["url"] and r["event_id"] is not None}
        log(f"  url_map: {len(s.url_map)} entries", s.NODE_NAME)
    except Exception as e:
        s.url_map = {}
        log(f"   url_map: {e}", s.NODE_NAME, level="error")


def _build_label_from_row(info: dict, ctx_id: int) -> str:
    label = (info.get("person_token") or info.get("ctx_key") or
             info.get("event_name") or info.get("name"))
    if not label:
        dr  = info.get("debt_regime")
        tga = info.get("tga_level_class")
        mb  = info.get("maturity_bucket")
        acc = info.get("accepted")
        if dr and tga:
            label = f"{dr}_{tga}"
        elif mb is not None and acc is not None:
            label = f"{mb}_{'accepted' if int(acc) else 'rejected'}"
    return label or f"ctx_id={ctx_id}"


async def _load_label_cache(s: _State):
    if not s.CTX_TABLE:
        return
    try:
        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(f"SELECT * FROM `{s.CTX_TABLE}`"))
            s._label_cache = {}
            for r in res.mappings().all():
                info   = dict(r)
                ctx_id = info.get("id")
                if ctx_id is None:
                    continue
                s._label_cache[int(ctx_id)] = _build_label_from_row(info, int(ctx_id))
        log(f"  label_cache: {len(s._label_cache)}", s.NODE_NAME)
    except Exception as e:
        log(f"   label_cache: {e}", s.NODE_NAME, level="warning")


async def _load_dataset(s: _State):
    if not s.DATASET_QUERY and not s.DATASET_TABLE:
        s.dataset = []
        return
    query = s.DATASET_QUERY or f"SELECT * FROM `{s.DATASET_TABLE}`"
    try:
        from datetime import date as _date
        async with _engine_for(s.DATASET_ENGINE, s).connect() as conn:
            res  = await conn.execute(text(query))
            rows = []
            for r in res.mappings().all():
                row = dict(r)
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


# === ПАТЧ 2: _build_dataset_index — сортировка + numpy timestamps ===
def _build_dataset_index(s: _State) -> None:
    from collections import defaultdict as _dd
    from datetime import datetime as _dt
    import numpy as _np

    # Гарантируем сортировку
    s.dataset.sort(key=lambda e: e.get("date") or _dt.min)

    key_field = s.dataset_key_field
    dates = []
    by_key = _dd(list)
    for e in s.dataset:
        dates.append(e.get("date"))
        k = e.get(key_field)
        if k is not None:
            by_key[k].append(e)
    by_key = dict(by_key)
    s.dataset_dates     = dates
    s.dataset_by_key    = by_key
    s.dataset_key_dates = {k: [e["date"] for e in evts] for k, evts in by_key.items()}

    # numpy-массив unix-timestamps для быстрого searchsorted в model()
    s._dataset_ts_arr = _np.array(
        [int(d.timestamp()) if isinstance(d, _dt) and d is not None else 0
         for d in dates],
        dtype=_np.int64,
    )

    log(f"  dataset_index: {len(by_key)} unique {key_field}s", s.NODE_NAME)


# ══════════════════════════════════════════════════════════════════════════════
# DDL
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# __detail__ УТИЛИТЫ
# ══════════════════════════════════════════════════════════════════════════════

_DETAIL_KEY = "__detail__"


def _extract_detail(result: dict | None) -> tuple[dict | None, object]:
    if result is None:
        return result, None
    detail = result.pop(_DETAIL_KEY, None)
    return result, detail


def _make_detail_serializable(detail) -> object:
    if detail is None:
        return None
    if isinstance(detail, dict):
        return {str(k): _make_detail_serializable(v) for k, v in detail.items()}
    if isinstance(detail, (list, tuple)):
        return [_make_detail_serializable(v) for v in detail]
    if isinstance(detail, datetime):
        return detail.isoformat()
    if isinstance(detail, float):
        return None if (math.isnan(detail) or math.isinf(detail)) else detail
    if isinstance(detail, (int, str, bool)):
        return detail
    return str(detail)


# ══════════════════════════════════════════════════════════════════════════════
# НАРРАТИВ
# ══════════════════════════════════════════════════════════════════════════════

def _pluralize_n(n: int, forms: tuple[str, str, str]) -> str:
    abs_n = abs(n)
    if abs_n % 100 in range(11, 20):
        return forms[2]
    r = abs_n % 10
    if r == 1:         return forms[0]
    if r in (2, 3, 4): return forms[1]
    return forms[2]


def _shift_label(shift: int | None, day_flag: int) -> str:
    if shift is None or shift == 0:
        return "в момент целевой свечи"
    forms = ("день", "дня", "дней") if day_flag else ("час", "часа", "часов")
    unit  = _pluralize_n(shift, forms)
    return f"{abs(shift)} {unit} назад" if shift > 0 else f"через {abs(shift)} {unit}"


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-DISCOVERY
# ══════════════════════════════════════════════════════════════════════════════

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
        log("   context_idx.py (build_index)",   "brain-framework", force=True)
    if build_weights_fn:
        log("   weights.py (build_weights)",      "brain-framework", force=True)

    return build_index_fn, build_weights_fn


# ══════════════════════════════════════════════════════════════════════════════
# build_app
# ══════════════════════════════════════════════════════════════════════════════

def build_app(model_module) -> FastAPI:
    s = _State()

    def _g(attr, default):
        return getattr(model_module, attr, default)

    if not hasattr(model_module, "RATES_TABLE"):
        raise RuntimeError(
            "model.py должен содержать RATES_TABLE"
        )

    # ── Критичные переменные ──────────────────────────────────────────────────
    s.SERVICE_ID   = _g("SERVICE_ID",   int(os.getenv("SERVICE_ID",   "0")))
    s.PORT         = _g("PORT",         int(os.getenv("PORT",         "9000")))
    s.NODE_NAME    = _g("NODE_NAME",    os.getenv("NODE_NAME",    "brain-svc"))
    s.SERVICE_TEXT = _g("SERVICE_TEXT", os.getenv("SERVICE_TEXT", "Brain microservice"))

    # ── Опциональные настройки ────────────────────────────────────────────────
    s.WEIGHTS_TABLE          = _g("WEIGHTS_TABLE",          None)
    s.WEIGHTS_CODE_COLUMN    = _g("WEIGHTS_CODE_COLUMN",    "weight_code")
    s.CTX_TABLE              = _g("CTX_TABLE",              None)
    s.CTX_QUERY              = _g("CTX_QUERY",              None)
    s.CTX_KEY_COLUMNS        = _g("CTX_KEY_COLUMNS",        ["id"])
    s.DATASET_TABLE          = _g("DATASET_TABLE",          None)
    s.DATASET_QUERY          = _g("DATASET_QUERY",          None)
    s.DATASET_ENGINE         = _g("DATASET_ENGINE",         "vlad")
    s.FILTER_DATASET_BY_DATE = _g("FILTER_DATASET_BY_DATE", False)
    s.SHIFT_WINDOW           = _g("SHIFT_WINDOW",           12)
    s.RELOAD_INTERVAL        = _g("RELOAD_INTERVAL",        3600)
    s.REBUILD_INTERVAL       = int(_g("REBUILD_INTERVAL",   0))
    s.VAR_RANGE              = _g("VAR_RANGE",              [0])
    s.TYPES_RANGE            = _g("TYPES_RANGE",            [0, 1, 2, 3, 4])
    s.CACHE_DATE_FROM        = _g("CACHE_DATE_FROM",        "2025-01-15")
    s.LABEL_FN               = _g("LABEL_FN",               None)

    s.RATES_TABLE        = _g("RATES_TABLE",  "brain_rates_eur_usd")
    s.dataset_key_field  = _g("DATASET_KEY",  "ctx_id")

    s.URL_MAP_QUERY  = _g("URL_MAP_QUERY",  None)
    s.URL_MAP_ENGINE = _g("URL_MAP_ENGINE", "vlad")

    s.USE_ML_VALUES        = bool(_g("USE_ML_VALUES",        False))
    s.ML_INIT_MODE         = _g("ML_INIT_MODE",        "constant")
    s.ML_TARGET_PRECISION  = float(_g("ML_TARGET_PRECISION", 0.95))
    s.ML_MAX_ITER          = int(_g("ML_MAX_ITER",     20))
    s.ML_STEP              = float(_g("ML_STEP",       0.10))
    s.ML_EXTREMUM_LIMIT    = int(_g("ML_EXTREMUM_LIMIT", 50))
    s.ML_ACTIVE_TAIL       = int(_g("ML_ACTIVE_TAIL",  0))
    s.ML_PRECISION_METRIC  = _g("ML_PRECISION_METRIC", "mean")

    s.model_fn = getattr(model_module, "model", None)
    if s.model_fn is None:
        raise RuntimeError("model_module должен определять функцию model()")

    sig = inspect.signature(s.model_fn)
    s.model_needs_index = "dataset_index" in sig.parameters

    s.index_builder_fn, s.weight_builder_fn = _discover_builders(model_module)

    _has_rebuild = s.index_builder_fn or s.weight_builder_fn
    log(f"  VAR_RANGE={s.VAR_RANGE} | "
        f"dataset_index={'yes' if s.model_needs_index else 'no'} | "
        f"rebuild={'builders' if _has_rebuild else 'no'} | "
        f"ml={'ON' if s.USE_ML_VALUES else 'off'}",
        s.NODE_NAME, force=True)

    s.engine_vlad, s.engine_brain, s.engine_super = build_engines()
    s.reverse_store = rl.ReverseStore(s.engine_vlad, port=s.PORT)

    # ── _call_model ───────────────────────────────────────────────────────────

    async def _call_model(pair, day, date_str, calc_type=0, calc_var=0, param="",
                          _skip_refresh: bool = False):
        target_date = _parse_date(date_str)
        if not target_date:
            return None
        table   = _rates_table(pair, day)
        if not _skip_refresh:
            await _refresh_rates(table, s)
        rates_f   = _filter_rates_lte(table, target_date, s)
        dataset_f = _filter_dataset_lte(target_date, s)
        np_r      = s.np_rates.get(table)

        # dataset_index с dataset_timestamps для model()
        dataset_index_dict = None
        if s.model_needs_index:
            dataset_index_dict = {
                "dates": s.dataset_dates,
                "by_key": s.dataset_by_key,
                "key_dates": s.dataset_key_dates,
                "key_field": s.dataset_key_field,
                "np_rates": np_r,
                "ctx_index": s.ctx_index,
                "url_map": s.url_map,
                "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
            }

        def _model_at(date_x: datetime, rates_x: list, dataset_x: list) -> dict:
            r = s.model_fn(
                rates=rates_x, dataset=dataset_x, date=date_x,
                type=calc_type, var=calc_var, param=param,
                dataset_index=dataset_index_dict,
            )
            r, _ = _extract_detail(r)
            return r or {}

        if not s.USE_ML_VALUES:
            return _model_at(target_date, rates_f, dataset_f)

        # ── ML-РЕЖИМ ─────────────────────────────────────────────────────────
        async def _active_codes_at(ext_dt: datetime) -> list[str]:
            ts  = int(ext_dt.timestamp())
            key = (pair, day, ts, calc_type, calc_var, param)
            cached = s._ml_active_cache.get(key)
            if cached is not None:
                return cached
            try:
                rates_at   = _filter_rates_lte(table, ext_dt, s)
                dataset_at = _filter_dataset_lte(ext_dt, s)
                codes      = list(_model_at(ext_dt, rates_at, dataset_at).keys())
            except Exception:
                codes = []
            s._ml_active_cache[key] = codes
            return codes

        params_hash = _params_hash({
            "type": calc_type, "var": calc_var, "param": param,
        })

        async with s._ml_semaphore:
            try:
                universe, _pr = await s.reverse_store.maybe_retrain(
                    pair=pair, day_flag=day, control_date=target_date,
                    params_hash=params_hash,
                    np_simple_rates=(np_r or s.np_simple_rates),
                    active_codes_at=_active_codes_at,
                    train_mode=calc_type,
                    max_iter=s.ML_MAX_ITER,
                    step=s.ML_STEP,
                    target_precision=s.ML_TARGET_PRECISION,
                    extremum_limit=s.ML_EXTREMUM_LIMIT,
                    extremum_interval=calc_var,
                    active_tail=s.ML_ACTIVE_TAIL,
                    metric=s.ML_PRECISION_METRIC,
                    log_fn=lambda m: log(m, s.NODE_NAME),
                    skip_db_writes=s._fill_cache_active,
                )
                return universe
            except Exception as e:
                log(f"   ML _call_model {target_date}: {e}",
                    s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "ml_call_model")
                return {}

    # ── _preload ──────────────────────────────────────────────────────────────

    async def _preload():
        log(" FULL DATA RELOAD", s.NODE_NAME, force=True)
        s.np_built = False
        s.weight_codes.clear()
        s.ctx_index.clear()
        s.dataset.clear()
        s.url_map.clear()
        s._dataset_ts_arr = None
        if s.reverse_store:
            s.reverse_store.clear_universe_cache()

        await _load_simple_rates(s)
        await _load_rates(s)
        await _load_dataset(s)
        await _load_weight_codes(s)
        await _load_ctx_index(s)
        await _load_url_map(s)
        await _load_label_cache(s)

        s.service_url  = f"http://localhost:{s.PORT}"
        s._cache_table = f"vlad_values_cache_svc{s.PORT}"

        try:
            await ensure_cache_table(s.engine_vlad, s.cache_table)
            if s.USE_ML_VALUES:
                await s.reverse_store.ensure_tables()
                s._ml_active_cache.clear()
            async with s.engine_vlad.begin() as conn:
                await conn.execute(text(_DDL_BT_RESULTS))
                await conn.execute(text(_DDL_BT_SUMMARY))
        except Exception as e:
            log(f"   tables: {e}", s.NODE_NAME, level="error")

        s.last_reload = datetime.now()
        log(f" RELOAD DONE: rates={len(s.simple_rates)} "
            f"global_rates={sum(len(v) for v in s.global_rates.values())} "
            f"dataset={len(s.dataset)} weights={len(s.weight_codes)} "
            f"url_map={len(s.url_map)}",
            s.NODE_NAME, force=True)

    # ── _do_rebuild ───────────────────────────────────────────────────────────

    async def _do_rebuild() -> dict:
        stats = {}

        if s.index_builder_fn is not None:
            try:
                result = await s.index_builder_fn(s.engine_vlad, s.engine_brain)
                stats["index"] = result or {}
                log(f"   build_index: {result}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   build_index: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "build_index")
                return {"error": str(e)}

        if s.weight_builder_fn is not None:
            try:
                result = await s.weight_builder_fn(s.engine_vlad)
                stats["weights"] = result or {}
                log(f"   build_weights: {result}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   build_weights: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "build_weights")
                return {"error": str(e)}

        await _load_weight_codes(s)
        await _load_ctx_index(s)
        await _load_label_cache(s)
        await _load_url_map(s)

        s.last_rebuild = datetime.now()
        log(f" rebuild done: weights={len(s.weight_codes)} ctx={len(s.ctx_index)} url_map={len(s.url_map)}",
            s.NODE_NAME, force=True)

        return {
            **stats,
            "ctx_total":     len(s.ctx_index),
            "weights_total": len(s.weight_codes),
            "url_map_total": len(s.url_map),
            "rebuilt_at":    s.last_rebuild.isoformat(),
        }

    # ── _bg_reload ────────────────────────────────────────────────────────────

    async def _bg_reload():
        while True:
            await asyncio.sleep(s.RELOAD_INTERVAL)
            try:
                await _refresh_simple_rates(s)
                for table in _RATES_TABLES:
                    await _refresh_rates(table, s)
                if (s.REBUILD_INTERVAL > 0
                        and (s.index_builder_fn or s.weight_builder_fn)
                        and (s.last_rebuild is None or
                             (datetime.now() - s.last_rebuild).total_seconds()
                             >= s.REBUILD_INTERVAL)):
                    await _do_rebuild()
                s.last_reload = datetime.now()
            except Exception as e:
                log(f" bg_reload: {e}", s.NODE_NAME, level="error", force=True)
                send_error_trace(e, s.NODE_NAME, "bg_reload")

    # ── fill_cache helpers ────────────────────────────────────────────────────

    async def _cached_dates(pair, day, p_hash) -> set:
        try:
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT date_val FROM `{s.cache_table}`
                    WHERE service_url=:url AND pair=:pair
                      AND day_flag=:day AND params_hash=:ph
                """), {"url": s.service_url, "pair": pair, "day": day, "ph": p_hash})
                return {row[0] for row in res.fetchall()}
        except Exception:
            return set()

    # ── _sync_compute для поточной обработки ─────────────────────────────────
    def _sync_compute(candle, calc_type, calc_var, all_rows, all_dates, np_rates_pd, s_state):
        td = candle["date"]
        idx = bisect.bisect_right(all_dates, td)
        rates_f = all_rows[:idx]
        ds_f = _filter_dataset_lte(td, s_state)

        dataset_index_dict = None
        if s_state.model_needs_index:
            dataset_index_dict = {
                "dates": s_state.dataset_dates,
                "by_key": s_state.dataset_by_key,
                "key_dates": s_state.dataset_key_dates,
                "key_field": s_state.dataset_key_field,
                "np_rates": np_rates_pd,
                "ctx_index": s_state.ctx_index,
                "url_map": s_state.url_map,
                "dataset_timestamps": getattr(s_state, "_dataset_ts_arr", None),
            }

        try:
            res = s_state.model_fn(
                rates=rates_f, dataset=ds_f, date=td,
                type=calc_type, var=calc_var, param="",
                dataset_index=dataset_index_dict,
            )
            res, _ = _extract_detail(res)
            return res or {}
        except Exception as _e:
            import traceback as _tb
            log(f"   fill {td} t={calc_type} v={calc_var}: {_e}\n{_tb.format_exc()}",
                s_state.NODE_NAME, level="error", force=True)
            return None

    # ── _fill_worker ──────────────────────────────────────────────────────────

    async def _fill_worker(pairs, days, date_from_str, date_to_str, types, batch_size):
        s.fill_cancel.clear()
        s._fill_cache_active = True   # skip vlad_reverse_universe writes for speed
        # Clear ML universe cache so fill_cache starts fresh
        if s.reverse_store:
            s.reverse_store.clear_universe_cache()
        dt_from = _parse_date(date_from_str) if date_from_str else None
        dt_to   = _parse_date(date_to_str)   if date_to_str   else None

        pd_slots       = [(p, d) for p in pairs for d in days]
        type_var_slots = [(tp, var) for tp in types for var in s.VAR_RANGE]
        total_slots    = len(pd_slots) * len(type_var_slots)

        total_candles = sum(
            sum(1 for r in s.global_rates.get(_rates_table(p, d), [])
                if (dt_from is None or r["date"] >= dt_from)
                and (dt_to   is None or r["date"] <= dt_to))
            for p, d in pd_slots
        )
        total  = total_candles * len(type_var_slots)
        done   = skipped = errors = 0
        s.fill_status = {
            "state": "running", "total": total, "done": 0,
            "skipped": 0, "errors": 0,
            "pairs": pairs, "days": days,
            "slots_total": total_slots, "slots_done": 0,
            "started_at": datetime.now().isoformat(),
        }
        log(f" fill_cache: {len(pd_slots)} инструментов × "
            f"{len(type_var_slots)} type/var слотов", s.NODE_NAME, force=True)

        for slot_idx, (pair_id, day_flag) in enumerate(pd_slots):
            if s.fill_cancel.is_set():
                break

            tbl      = _rates_table(pair_id, day_flag)
            all_rows = s.global_rates.get(tbl, [])
            candles  = [r for r in all_rows
                        if (dt_from is None or r["date"] >= dt_from)
                        and (dt_to   is None or r["date"] <= dt_to)]
            if not candles:
                s.fill_status["slots_done"] = slot_idx + 1
                log(f"  [pair{pair_id}/{'d' if day_flag else 'h'}] нет свечей, пропуск",
                    s.NODE_NAME, force=True)
                continue

            np_rates_pd  = s.np_rates.get(tbl)
            all_dates_pd = [r["date"] for r in all_rows]
            instr_label  = f"pair{pair_id}/{'d' if day_flag else 'h'}"
            log(f"  [{instr_label}] {len(candles)} свечей × "
                f"{len(type_var_slots)} type/var слотов", s.NODE_NAME, force=True)

            for calc_type, var in type_var_slots:
                if s.fill_cancel.is_set():
                    break

                extra       = {"type": calc_type, "var": var, "param": ""}
                p_hash      = _params_hash(extra)
                params_json = _json.dumps(extra, ensure_ascii=False)
                _tbl_c      = s.cache_table

                try:
                    async with s.engine_vlad.connect() as conn:
                        res = await conn.execute(text(f"""
                            SELECT date_val FROM `{_tbl_c}`
                            WHERE service_url=:url AND pair=:pair
                              AND day_flag=:day AND params_hash=:ph
                        """), {"url": s.service_url, "pair": pair_id,
                               "day": day_flag, "ph": p_hash})
                        cached = {r[0] for r in res.fetchall()}
                except Exception:
                    cached = set()

                to_fetch  = [c for c in candles if c["date"] not in cached]
                skipped  += len(candles) - len(to_fetch)

                for i in range(0, len(to_fetch), batch_size):
                    if s.fill_cancel.is_set():
                        break
                    batch = to_fetch[i:i + batch_size]

                    if s.USE_ML_VALUES:
                        # Refresh rates once per batch, not per candle
                        await _refresh_rates(_rates_table(pair_id, day_flag), s)

                        results = []
                        for candle in batch:
                            try:
                                result = await _call_model(
                                    pair_id, day_flag,
                                    candle["date"].strftime("%Y-%m-%d %H:%M:%S"),
                                    calc_type=calc_type, calc_var=var, param="",
                                    _skip_refresh=True)
                            except Exception as _e:
                                import traceback as _tb
                                log(f"   ml-fill {candle['date']} t={calc_type} v={var}: {_e}\n{_tb.format_exc()}",
                                    s.NODE_NAME, level="error", force=True)
                                result = None
                            results.append(result)
                    else:
                        # ПАТЧ 5: параллельные потоки внутри батча
                        loop = asyncio.get_event_loop()
                        futs = [
                            loop.run_in_executor(
                                _FILL_EXECUTOR,
                                _sync_compute,
                                candle, calc_type, var,
                                all_rows, all_dates_pd, np_rates_pd, s
                            )
                            for candle in batch
                        ]
                        results = list(await asyncio.gather(*futs))

                    insert_rows = []
                    for candle, result in zip(batch, results):
                        if result is None:
                            errors += 1
                            continue
                        insert_rows.append({
                            "url":  s.service_url, "pair": pair_id,
                            "day":  day_flag,      "dv":   candle["date"],
                            "ph":   p_hash,        "pj":   params_json,
                            "rj":   _json.dumps(result, ensure_ascii=False),
                        })
                    if insert_rows:
                        try:
                            async with s.engine_vlad.begin() as conn:
                                await conn.execute(text(f"""
                                    INSERT IGNORE INTO `{_tbl_c}`
                                        (service_url, pair, day_flag, date_val,
                                         params_hash, params_json, result_json)
                                    VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
                                """), insert_rows)
                        except Exception as e:
                            log(f"   bulk insert: {e}", s.NODE_NAME, level="warning")
                    done += len(batch)
                    s.fill_status.update({"done": done, "skipped": skipped, "errors": errors})
                    log(f"  [{instr_label} t={calc_type}/v={var}] "
                        f"{done}/{total} err={errors}", s.NODE_NAME, force=True)

            s.fill_status["slots_done"] = slot_idx + 1

        s._fill_cache_active = False  # restore normal mode
        state = "stopped" if s.fill_cancel.is_set() else "done"
        s.fill_status.update({"state": state, "finished_at": datetime.now().isoformat()})
        log(f" fill_cache {state}: done={done} skip={skipped} err={errors}",
            s.NODE_NAME, force=True)

        # ── Авто-бэктест ──────────────────────────────────────────────────────
        if not s.fill_cancel.is_set():
            log(" auto-backtest после fill_cache...", s.NODE_NAME, force=True)
            bt_df = _parse_date(date_from_str) if date_from_str else datetime(2025, 1, 1)
            bt_dt = _parse_date(date_to_str)   if date_to_str   else datetime.now()
            for bt_pair, bt_day in pd_slots:
                for bt_type, bt_var in type_var_slots:
                    try:
                        bt = await _backtest(
                            bt_pair, bt_day, tier=0,
                            extra_params={"type": bt_type, "var": bt_var},
                            df=bt_df, dt=bt_dt,
                        )
                        label = f"pair{bt_pair}/{'d' if bt_day else 'h'} t={bt_type} v={bt_var}"
                        if "error" in bt:
                            log(f"   backtest [{label}]: {bt['error']}",
                                s.NODE_NAME, force=True)
                        else:
                            log(f"   backtest [{label}]: "
                                f"score={bt.get('value_score')} "
                                f"acc={bt.get('accuracy')} "
                                f"trades={bt.get('trade_count')}",
                                s.NODE_NAME, force=True)
                    except Exception as e:
                        log(f"   backtest pair={bt_pair} day={bt_day}: {e}",
                            s.NODE_NAME, level="error")
                try:
                    await _upsert_summary(bt_pair, bt_day, tier=0, df=bt_df, dt=bt_dt)
                except Exception as e:
                    log(f"   summary pair={bt_pair} day={bt_day}: {e}",
                        s.NODE_NAME, level="warning")
            s.fill_status["auto_backtest"] = "done"
            log(" auto-backtest завершён", s.NODE_NAME, force=True)

        # ── Трассировка ───────────────────────────────────────────────────────
        _fc_el  = datetime.now() - datetime.fromisoformat(
            s.fill_status.get("started_at", datetime.now().isoformat()))
        _h, _r  = divmod(int(_fc_el.total_seconds()), 3600)
        _m, _sc = divmod(_r, 60)
        _send_trace(
            subject  = f"{'' if state == 'done' else ''} fill_cache {state} — {s.service_url}",
            body     = (f"Сервис : {s.service_url}\n"
                        f"Пары   : {pairs}  Дни: {days}\n"
                        f"Период : {date_from_str or s.CACHE_DATE_FROM} → {date_to_str or 'now'}\n"
                        f"Кеш    : done={done}  skip={skipped}  err={errors}\n"
                        f"Бэктест: {'done' if not s.fill_cancel.is_set() else 'skipped'}\n"
                        f"Прошло : {_h}h {_m}m {_sc}s"),
            node     = s.NODE_NAME,
            is_error = (state != "done"),
        )

    # ── _backtest ─────────────────────────────────────────────────────────────

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

        async with s.engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT date_val, result_json FROM `{s.cache_table}`
                WHERE service_url=:url AND pair=:pair AND day_flag=:day
                  AND params_hash=:ph AND date_val>=:df AND date_val<=:dt
                ORDER BY date_val
            """), {"url": s.service_url, "pair": pair, "day": day,
                   "ph": p_hash, "df": df, "dt": dt})
            cache_map = {row[0]: _json.loads(row[1]) for row in res.fetchall()}
        if not cache_map:
            return {"error": "no cached data"}

        table  = _rates_table(pair, day)
        rows   = [r for r in s.global_rates.get(table, []) if df <= r["date"] <= dt]
        _, mod, lot_div = _PAIR_CFG.get(pair, (0.0002, 100_000.0, 50_000.0))

        balance = highest = 10_000.0
        summary_lost = 0.0
        trade_count  = win_count = 0
        position     = None
        for candle in rows:
            vals   = cache_map.get(candle["date"])
            signal = sum(vals.values()) if vals else 0.0
            if position is not None:
                direction, entry_price = position
                if (signal == 0.0 or (signal > 0 and direction < 0)
                        or (signal < 0 and direction > 0)):
                    op = candle["open"]
                    if op:
                        lot = max(round(balance / lot_div, 2), 0.01)
                        pnl = lot * mod * (op - entry_price) / entry_price * direction
                        balance     += pnl
                        trade_count += 1
                        if pnl >= 0:
                            win_count   += 1
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
        result = {
            "balance_final": round(balance, 4), "total_result": round(total_result, 4),
            "summary_lost":  round(summary_lost, 6), "value_score": round(value_score, 4),
            "trade_count":   trade_count, "win_count": win_count,
            "accuracy":      round(win_count / trade_count, 4), "params": extra_params,
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
                        balance_final=VALUES(balance_final),
                        total_result=VALUES(total_result),
                        summary_lost=VALUES(summary_lost),
                        value_score=VALUES(value_score),
                        trade_count=VALUES(trade_count),
                        win_count=VALUES(win_count),
                        accuracy=VALUES(accuracy),
                        created_at=CURRENT_TIMESTAMP
                """), {
                    "url": s.service_url, "mid": s.SERVICE_ID,
                    "pair": pair, "day": day, "tier": tier,
                    "ph": p_hash, "pj": _json.dumps(extra_params, ensure_ascii=False),
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
                    best_score=VALUES(best_score), avg_score=VALUES(avg_score),
                    best_accuracy=VALUES(best_accuracy), avg_accuracy=VALUES(avg_accuracy),
                    best_params_json=VALUES(best_params_json),
                    computed_at=CURRENT_TIMESTAMP
            """), {
                "mid": s.SERVICE_ID, "url": s.service_url,
                "pair": pair, "day": day, "tier": tier, "df": df, "dt": dt,
                "cnt": row[0], "bs": float(row[1] or 0), "as_": float(row[2] or 0),
                "ba": float(row[3] or 0), "aa": float(row[4] or 0), "bpj": row[5],
            })

    # ══════════════════════════════════════════════════════════════════════════
    # FastAPI endpoints
    # ══════════════════════════════════════════════════════════════════════════

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
                row = (await conn.execute(text(
                    "SELECT version FROM version_microservice WHERE microservice_id=:id"),
                    {"id": s.SERVICE_ID})).fetchone()
                if row:
                    version = row[0]
        except Exception:
            pass
        return {
            "status": "ok", "version": f"1.{version}.0",
            "mode": MODE, "name": s.NODE_NAME, "text": s.SERVICE_TEXT,
            "metadata": {
                "weight_codes":       len(s.weight_codes),
                "ctx_index":          len(s.ctx_index),
                "url_map":            len(s.url_map),
                "dataset":            len(s.dataset),
                "var_range":          s.VAR_RANGE,
                "types_range":        s.TYPES_RANGE,
                "np_built":           s.np_built,
                "simple_rates":       len(s.simple_rates),
                "last_reload":        s.last_reload.isoformat() if s.last_reload else None,
                "last_rebuild":       s.last_rebuild.isoformat() if s.last_rebuild else None,
                "rebuild_auto":       s.REBUILD_INTERVAL > 0 and bool(
                    s.index_builder_fn or s.weight_builder_fn),
                "rebuild_interval_s": s.REBUILD_INTERVAL,
                "ml_mode": ({
                    "enabled":          True,
                    "init_mode":        s.ML_INIT_MODE,
                    "target_precision": s.ML_TARGET_PRECISION,
                    "max_iter":         s.ML_MAX_ITER,
                    "step":             s.ML_STEP,
                    "extremum_limit":   s.ML_EXTREMUM_LIMIT,
                    "active_tail":      s.ML_ACTIVE_TAIL,
                    "precision_metric": s.ML_PRECISION_METRIC,
                } if s.USE_ML_VALUES else {"enabled": False}),
            },
        }

    @app.get("/weights")
    async def ep_weights():
        return ok_response({"codes": s.weight_codes, "var_range": s.VAR_RANGE})

    # ── _build_narrative ──────────────────────────────────────────────────────────

    def _build_narrative(payload: dict, date_str: str, day_flag: int, calc_type: int) -> list[str]:
        if not payload:
            return ["Нет данных для текущей даты."]
        target_date = _parse_date(date_str)
        if not target_date:
            return [f"Не удалось разобрать дату: {date_str!r}"]

        du       = timedelta(days=1) if day_flag else timedelta(hours=1)
        start_dt = target_date - du * s.SHIFT_WINDOW
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
            gk     = (ctx_id, shift)
            if gk not in groups:
                label = None
                if s.LABEL_FN:
                    try:
                        label = s.LABEL_FN((ctx_id,))
                    except Exception:
                        pass
                if not label:
                    label = s._label_cache.get(ctx_id)
                if not label:
                    for key, info in s.ctx_index.items():
                        if key[0] == ctx_id:
                            label = _build_label_from_row(info, ctx_id)
                            break
                if not label:
                    label = f"ctx_id={ctx_id}"
                occ = 0
                for key, info in s.ctx_index.items():
                    if key[0] == ctx_id:
                        occ = info.get("occurrence_count", 0)
                        break
                groups[gk] = {"ctx_id": ctx_id, "shift": shift, "label": label,
                               "occ": occ, "t1": {}, "ext": {}}
            g = groups[gk]
            if mode == 0:   g["t1"][wc]  = val
            elif mode == 1: g["ext"][wc] = val

        if not groups and not unmatched:
            return ["Нет событий для декодирования."]

        sorted_groups = sorted(groups.values(), key=lambda g: (-g["shift"], g["ctx_id"]))
        n        = len(sorted_groups)
        evt_word = _pluralize_n(n, ("событие", "события", "событий"))
        lines: list[str] = []
        lines.append(
            f"Окно поиска: {start_dt.strftime('%Y-%m-%d %H:%M')} — "
            f"{target_date.strftime('%Y-%m-%d %H:%M')}. Найдено {n} {evt_word}.")
        lines.append("")

        for i, g in enumerate(sorted_groups, 1):
            lines.append(f'{i}. "{g["label"]}" — {_shift_label(g["shift"], day_flag)}')

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

    # ── /values ───────────────────────────────────────────────────────────────

    @app.get("/values")
    async def ep_values(
        pair: int = Query(1), day: int = Query(1),
        date: str = Query(...),
        type: int = Query(0, ge=0, le=4),
        var:  int = Query(0),
        param: str = Query(""),
    ):
        if s.TYPES_RANGE and type not in s.TYPES_RANGE:
            return err_response(f"type={type} не входит в TYPES_RANGE={s.TYPES_RANGE}")
        if s.VAR_RANGE and var not in s.VAR_RANGE:
            return err_response(f"var={var} не входит в VAR_RANGE={s.VAR_RANGE}")
        try:
            resp = await cached_values(
                engine_vlad=s.engine_vlad, service_url=s.service_url,
                pair=pair, day=day, date=date,
                extra_params={"type": type, "var": var, "param": param},
                compute_fn=lambda: _call_model(pair, day, date,
                                               calc_type=type, calc_var=var, param=param),
                node=s.NODE_NAME, table_name=s.cache_table,
            )
            payload = resp.get("payLoad") or {}
            resp["details"] = _build_narrative(payload, date, 1 if day == 1 else 0, type)
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

    # ── fill_cache ────────────────────────────────────────────────────────────

    async def _start_fill(pairs_str: str, days_str: str,
                          date_from: str, date_to: str, batch_size: int):
        if s.fill_task and not s.fill_task.done():
            return err_response("Fill already running.")
        try:
            pl = [int(p.strip()) for p in pairs_str.split(",") if p.strip()]
            dl = [int(d.strip()) for d in days_str.split(",")  if d.strip()]
        except ValueError:
            return err_response("pairs и days — числа через запятую")
        if not all(p in {1, 3, 4} for p in pl):
            return err_response("Допустимые pair: 1 (EUR), 3 (BTC), 4 (ETH)")
        if not all(d in {0, 1} for d in dl):
            return err_response("Допустимые day: 0 (hourly), 1 (daily)")
        all_types  = list(s.TYPES_RANGE or [0, 1, 2, 3, 4])
        eff_from   = date_from if date_from.strip() else s.CACHE_DATE_FROM
        s.fill_cancel.clear()
        s.fill_task = asyncio.create_task(
            _fill_worker(pl, dl, eff_from, date_to, all_types, batch_size))
        return ok_response({
            "started":     True, "pairs": pl, "days": dl,
            "types":       all_types, "var_range": s.VAR_RANGE,
            "batch_size":  batch_size, "date_from": eff_from,
            "date_to":     date_to or "now",
            "slots_total": len(pl) * len(dl) * len(all_types) * len(s.VAR_RANGE),
        })

    @app.get("/fill_cache")
    async def ep_fill_cache(
        pairs: str = Query("1,3,4"), days: str = Query("0,1"),
        date_from: str = Query(""), date_to: str = Query(""),
        batch_size: int = Query(300),
    ):
        return await _start_fill(pairs, days, date_from, date_to, batch_size)

    @app.get("/fill_cache_day")
    async def ep_fill_cache_day(
        pairs: str = Query("1,3,4"),
        date_from: str = Query(""), date_to: str = Query(""),
        batch_size: int = Query(300),
    ):
        return await _start_fill(pairs, "1", date_from, date_to, batch_size)

    @app.get("/fill_cache_hour")
    async def ep_fill_cache_hour(
        pairs: str = Query("1,3,4"),
        date_from: str = Query(""), date_to: str = Query(""),
        batch_size: int = Query(300),
    ):
        return await _start_fill(pairs, "0", date_from, date_to, batch_size)

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
        if not s.index_builder_fn and not s.weight_builder_fn:
            return err_response(
                "Rebuild не настроен. "
                "Добавь context_idx.py (build_index) и/или weights.py (build_weights) "
                "рядом с model.py.")
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
        pairs: str = Query("1,3,4"), days: str = Query("0,1"),
        tier:  int = Query(0, ge=0, le=1),
        date_from: str = Query(""), date_to: str = Query(""),
        type:  int = Query(0), var: int = Query(-1),
    ):
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
                key = f"pair={bt_pair} day={'d' if bt_day else 'h'}"
                all_results[key] = {}
                for v in vars_to_run:
                    try:
                        all_results[key][f"var={v}"] = await _backtest(
                            bt_pair, bt_day, tier, {"type": type, "var": v, "param": ""}, df, dt)
                    except Exception as e:
                        all_results[key][f"var={v}"] = {"error": str(e)}
                try:
                    await _upsert_summary(bt_pair, bt_day, tier, df, dt)
                except Exception as e:
                    log(f"   summary pair={bt_pair} day={bt_day}: {e}",
                        s.NODE_NAME, level="warning")

        return ok_response({
            "pairs": pl, "days": dl, "tier": tier,
            "date_from": df.isoformat(), "date_to": dt.isoformat(),
            "vars": vars_to_run, "results": all_results,
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
                        "model_id": r["model_id"], "pair": r["pair"],
                        "day_flag": r["day_flag"], "tier": r["tier"],
                        "date_from": r["date_from"].isoformat() if r["date_from"] else None,
                        "date_to":   r["date_to"].isoformat()   if r["date_to"]   else None,
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

        # ── Тест 1: синтаксис model.py ────────────────────────────────────────
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

        # ── Тест 2: структура model() на одной дате ───────────────────────────
        _eur_rows = s.global_rates.get(s.RATES_TABLE, [])
        if not _eur_rows:
            return {"status": "error",
                    "error": "[Тест 2 — Структура] global_rates пустой"}
        if not s.dataset:
            return {"status": "error",
                    "error": "[Тест 2 — Структура] dataset пустой"}

        _ds_dates2 = sorted(e["date"] for e in s.dataset if e.get("date") is not None)
        _mid2      = _ds_dates2[len(_ds_dates2) // 2]
        _eur_dts2  = [r["date"] for r in _eur_rows]
        _td2       = next((d for d in _eur_dts2 if d >= _mid2),
                          _eur_rows[-1]["date"] if _eur_rows else _mid2)
        _rf2  = _eur_rows[:bisect.bisect_right(_eur_dts2, _td2)]
        _ds2  = _filter_dataset_lte(_td2, s)

        dataset_index_dict2 = None
        if s.model_needs_index:
            dataset_index_dict2 = {
                "dates": s.dataset_dates,
                "by_key": s.dataset_by_key,
                "key_dates": s.dataset_key_dates,
                "key_field": s.dataset_key_field,
                "np_rates": s.np_simple_rates,
                "ctx_index": s.ctx_index,
                "url_map": s.url_map,
                "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
            }

        try:
            _res2 = s.model_fn(
                rates=_rf2, dataset=_ds2, date=_td2,
                type=0, var=s.VAR_RANGE[0], param="",
                dataset_index=dataset_index_dict2,
            )
            _res2, _ = _extract_detail(_res2)
        except Exception as _e2:
            return {"status": "error",
                    "error": f"[Тест 2 — Структура] model() exception: {_e2}"}

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

        # ── Тест 3: 10 случайных дат по каждому из 6 инструментов (≥90%) ─────
        _instr_dates: dict[str, list] = {
            tbl: [r["date"] for r in s.global_rates.get(tbl, [])]
            for tbl in sum([[v["hour"], v["day"]] for v in _INSTRUMENTS.values()], [])
        }

        _pt3_date_start: datetime = datetime(2025, 1, 15)
        try:
            async with s.engine_brain.connect() as _conn_pt3:
                _row_pt3 = (await _conn_pt3.execute(
                    text("SELECT `date` FROM `brain_models` WHERE `id` = :sid LIMIT 1"),
                    {"sid": s.SERVICE_ID})).fetchone()
                if _row_pt3 and _row_pt3[0] is not None:
                    _val = _row_pt3[0]
                    _pt3_date_start = (_val if isinstance(_val, datetime)
                                       else datetime(_val.year, _val.month, _val.day))
                    log(f"  [Тест 3] date_start из brain_models: {_pt3_date_start.date()}",
                        s.NODE_NAME, force=True)
        except Exception as _e_pt3:
            log(f"  [Тест 3] brain_models недоступна ({_e_pt3}), фолбэк 2025-01-15",
                s.NODE_NAME, level="warning", force=True)

        _tasks3: list = []
        _coros3: list = []

        for _pid3, _tfs3 in _INSTRUMENTS.items():
            for _tf3, _tbl3 in _tfs3.items():
                _dts3  = _instr_dates.get(_tbl3, [])
                _day3  = 1 if _tf3 == "day" else 0
                _rows3 = s.global_rates.get(_tbl3, [])
                if not _dts3:
                    continue
                _pool3   = [d for d in _dts3 if d >= _pt3_date_start] or _dts3
                _sample3 = sorted(random.sample(_pool3, min(10, len(_pool3))))
                log(f"  [Тест 3] pair{_pid3}/{_tf3}: {len(_sample3)} дат "
                    f"(от {_sample3[0].date() if _sample3 else '—'} "
                    f"до {_sample3[-1].date() if _sample3 else '—'})...",
                    s.NODE_NAME, force=True)

                for _td3 in _sample3:
                    _rf3 = _rows3[:bisect.bisect_right(_dts3, _td3)]
                    _ds3 = _filter_dataset_lte(_td3, s)

                    dataset_index_dict3 = None
                    if s.model_needs_index:
                        dataset_index_dict3 = {
                            "dates": s.dataset_dates,
                            "by_key": s.dataset_by_key,
                            "key_dates": s.dataset_key_dates,
                            "key_field": s.dataset_key_field,
                            "np_rates": s.np_simple_rates,
                            "ctx_index": s.ctx_index,
                            "url_map": s.url_map,
                            "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
                        }

                    def _mk3(_r=_rf3, _d=_ds3, _t=_td3):
                        res, _ = _extract_detail(
                            s.model_fn(
                                rates=_r, dataset=_d, date=_t,
                                type=0, var=s.VAR_RANGE[0], param="",
                                dataset_index=dataset_index_dict3,
                            )
                        )
                        return bool(res)

                    _tasks3.append((_pid3, _tf3))
                    _coros3.append(asyncio.to_thread(_mk3))

        _results3    = await asyncio.gather(*_coros3, return_exceptions=True)
        _instr_counts: dict = {}
        for (_pid3, _tf3), _r3 in zip(_tasks3, _results3):
            key = (_pid3, _tf3)
            if key not in _instr_counts:
                _instr_counts[key] = [0, 0]
            _instr_counts[key][1] += 1
            if isinstance(_r3, Exception):
                log(f"     pair{_pid3}/{_tf3}: {_r3}", s.NODE_NAME, level="warning")
            elif _r3:
                _instr_counts[key][0] += 1

        _failures3: list = []
        for (_pid3, _tf3), (_ne3, _tot3) in _instr_counts.items():
            _cov3 = _ne3 / _tot3 if _tot3 else 0
            _ok3  = _cov3 >= 0.90
            log(f"  {chr(9989) if _ok3 else chr(10060)} pair{_pid3}/{_tf3}: "
                f"{_ne3}/{_tot3} ({_cov3:.0%}), порог 90%",
                s.NODE_NAME, force=True)
            if not _ok3:
                _failures3.append(f"pair{_pid3}/{_tf3}: {_ne3}/{_tot3} ({_cov3:.0%}) < 90%")

        if _failures3:
            log(f" PRETEST FAILED: {_failures3}", s.NODE_NAME, force=True)
            return {"status": "error",
                    "error": f"[Тест 3 — Покрытие] {' | '.join(_failures3)}"}

        # ── Тест 4: rebuild_index ─────────────────────────────────────────────
        if s.index_builder_fn is not None or s.weight_builder_fn is not None:
            log("  [Тест 4] проверяем rebuild_index / обновление весов...",
                s.NODE_NAME, force=True)
            try:
                _rb4 = await _do_rebuild()
                if "error" in _rb4:
                    log(f" Тест 4: rebuild вернул ошибку: {_rb4['error']}",
                        s.NODE_NAME, force=True)
                    return {"status": "error",
                            "error": f"[Тест 4 — Rebuild] {_rb4['error']}"}
                log(f"   Тест 4: rebuild OK — "
                    f"ctx={_rb4.get('ctx_total')} "
                    f"weights={_rb4.get('weights_total')} "
                    f"url_map={_rb4.get('url_map_total')}",
                    s.NODE_NAME, force=True)
            except Exception as _e4:
                log(f" Тест 4: exception: {_e4}", s.NODE_NAME, force=True)
                return {"status": "error", "error": f"[Тест 4 — Rebuild] {_e4}"}
        else:
            log("   Тест 4: rebuild не настроен, пропуск", s.NODE_NAME, force=True)

        log(" PRETEST PASSED", s.NODE_NAME, force=True)
        return {"status": "ok", "var_range": s.VAR_RANGE, "np_built": s.np_built}

    @app.get("/posttest")
    async def ep_posttest(
        pairs: str = Query("1,3,4"), days: str = Query("0,1"),
        date_from: str = Query(""), date_to: str = Query(""),
    ):
        try:
            pl = [int(p.strip()) for p in pairs.split(",") if p.strip()]
            dl = [int(d.strip()) for d in days.split(",")  if d.strip()]
        except ValueError:
            return err_response("pairs и days — числа через запятую")

        dt_from = _parse_date(date_from) if date_from.strip() else _parse_date(s.CACHE_DATE_FROM)
        dt_to   = _parse_date(date_to)   if date_to.strip()   else datetime.now()

        async def _process_slot(pair_id: int, day_flag: int) -> dict:
            tf_name  = "day" if day_flag else "hour"
            table    = _rates_table(pair_id, day_flag)
            all_rows = s.global_rates.get(table, [])
            rows     = [r for r in all_rows
                        if (dt_from is None or r["date"] >= dt_from)
                        and (dt_to   is None or r["date"] <= dt_to)]

            url_p = {"url": s.service_url, "pair": pair_id, "day": day_flag,
                     "df": dt_from, "dt": dt_to}
            _tbl  = s.cache_table

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
                log(f"   posttest t1 {pair_id}/{tf_name}: {e}",
                    s.NODE_NAME, level="warning")

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
                log(f"   posttest t2 {pair_id}/{tf_name}: {e}",
                    s.NODE_NAME, level="warning")

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                sync_out = await _call_model(pair_id, day_flag, now_str) or {}
            except Exception as e:
                sync_out = {"error": str(e)}

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
                log(f"   posttest cache {pair_id}/{tf_name}: {e}",
                    s.NODE_NAME, level="warning")

            def _compute_hole_and_sim():
                non_empty = np.array(
                    [cache_signals.get(r["date"], (False, 0.0))[0] for r in rows], dtype=bool)
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
                entry_price = direction = 0.0

                for i, r in enumerate(rows):
                    _, signal = cache_signals.get(r["date"], (False, 0.0))
                    if i + 1 >= len(rows):
                        continue
                    op = rows[i + 1]["open"]
                    if not op:
                        continue
                    if position is not None:
                        if (signal == 0.0 or (signal > 0 and direction < 0)
                                or (signal < 0 and direction > 0)):
                            pnl    = equity * 0.10 * (op - entry_price) / entry_price * direction
                            equity += pnl
                            trades += 1
                            if pnl >= 0: total_profit   += pnl; wins += 1
                            else:        total_dropdown += abs(pnl)
                            position = None
                    if signal != 0.0 and position is None:
                        direction   = 1.0 if signal > 0 else -1.0
                        entry_price = op
                        position    = (direction, entry_price)

                if position is not None and rows:
                    lp = rows[-1]["close"]
                    if lp and entry_price:
                        pnl    = equity * 0.10 * (lp - entry_price) / entry_price * direction
                        equity += pnl
                        trades += 1
                        if pnl >= 0: total_profit   += pnl; wins += 1
                        else:        total_dropdown += abs(pnl)

                cw = round(wins / trades, 4) if trades > 0 else 0.0
                return hole, {
                    "profit":   round(total_profit,   2),
                    "dropdown": round(total_dropdown, 2),
                    "cw":       cw,
                    "result":   round(total_profit - total_dropdown, 2),
                }

            hole, history_stats = await asyncio.to_thread(_compute_hole_and_sim)
            return {"data": data_stats, "values": values_stats,
                    "sync": sync_out, "hole": hole, "history": history_stats}

        slots   = [(p, d) for p in pl for d in dl]
        tasks   = [asyncio.create_task(_process_slot(p, d)) for p, d in slots]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: dict = {}
        for (pair_id, day_flag), result in zip(slots, results):
            output.setdefault(str(pair_id), {})[
                "day" if day_flag else "hour"
            ] = ({"error": str(result)} if isinstance(result, Exception) else result)

        return ok_response(output)

    return app
