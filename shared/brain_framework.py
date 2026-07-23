"""
brain_framework.py v20.1 — безопасное развёртывание fused fill_cache.

Оптимизации v20 над v19:
  AUTO-3: один проход по событиям сразу для всех type/var.
  AUTO-4: на H1 повторное использование точного результата для одинакового
          состояния дня (midnight / bull / bear), с автоматической проверкой.
  AUTO-5: объединённые bulk INSERT и однократная сериализация одинаковых результатов.

Критическое исправление v20.1:
  SAFE-STATE: модели с глобальным хронологическим состоянием автоматически
              считаются строго последовательно; fill/pretest/live разделены по scope.

Сохранены оптимизации v19:
  AUTO-1: zero-copy list views вместо rates[:idx] / dataset[:idx] на каждой свече.
  AUTO-2: run_standard_model автоматически выбирает события только из точного
          окна [date-shift_window, date] через NumPy searchsorted.
          MODEL_USES_RATE_HISTORY и MODEL_CAN_FILTER_DATASET_BY_DATE не нужны.

Сохранены оптимизации v18:
  OPT-1..5: fill_cache (см. v15).
  OPT-6: result_json сжимается zlib+base64 с префиксом 'z:' при записи.
          Старые строки (без префикса) читаются как обычный JSON — нулевая миграция.
          Экономия ~60% места. Точность float снижена 6→4 знака (достаточно для весов).
"""

from __future__ import annotations

import asyncio
import bisect
import concurrent.futures as _cf
import inspect
import json as _json
import zlib   as _zlib
import base64 as _b64
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
# ZERO-COPY READ-ONLY VIEWS
# ──────────────────────────────────────────────────────────────────────────────

class _ListView:
    """Read-only list-compatible window without copying the underlying rows.

    fill_cache historically built ``rates[:idx]`` for every candle.  On a long
    H1 history this repeatedly copied millions of Python references.  This view
    preserves the observable prefix/window semantics used by model.py while
    making construction O(1).  An explicit slice requested *inside* a model is
    still returned as a normal list for backward compatibility.
    """
    __slots__ = ("_rows", "_start", "_end")

    def __init__(self, rows, start: int = 0, end: int | None = None):
        n = len(rows)
        start = max(0, min(int(start), n))
        end = n if end is None else max(start, min(int(end), n))
        self._rows = rows
        self._start = start
        self._end = end

    def __len__(self):
        return self._end - self._start

    def __iter__(self):
        rows = self._rows
        for i in range(self._start, self._end):
            yield rows[i]

    def __getitem__(self, item):
        n = len(self)
        if isinstance(item, slice):
            start, stop, step = item.indices(n)
            if step == 1:
                return list(_ListView(self._rows, self._start + start, self._start + stop))
            return [self[i] for i in range(start, stop, step)]
        idx = int(item)
        if idx < 0:
            idx += n
        if idx < 0 or idx >= n:
            raise IndexError("list index out of range")
        return self._rows[self._start + idx]

    def copy(self):
        return list(self)

    def __repr__(self):
        return repr(list(self))


def _list_view(rows, start: int = 0, end: int | None = None):
    """Return an O(1) list window; avoid wrapping an already exact full list."""
    n = len(rows)
    end = n if end is None else end
    if start <= 0 and end >= n:
        return rows
    return _ListView(rows, start, end)


# ──────────────────────────────────────────────────────────────────────────────
# Трассировка
# ──────────────────────────────────────────────────────────────────────────────
_TRACE_HANDLER = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
_TRACE_URL     = f"{_TRACE_HANDLER}/trace.php"
_DEFAULT_DEVELOPER_EMAIL = "vladyurjevitch@yandex.ru"
_ALERT_EMAIL = os.getenv("ALERT_EMAIL", _DEFAULT_DEVELOPER_EMAIL).strip() or _DEFAULT_DEVELOPER_EMAIL


def _send_trace(
    subject: str,
    body: str,
    node: str,
    is_error: bool = False,
    email: str | None = None,
) -> None:
    if _requests is None:
        return
    level = "ERROR" if is_error else "INFO"
    try:
        _requests.post(
            _TRACE_URL,
            data={"url": "fill_cache", "node": node,
                  "email": (str(email or "").strip() or _ALERT_EMAIL),
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
    log, send_error_trace, set_alert_email,
    ok_response, err_response,
    resolve_workers, build_engines, build_cache_engine,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values
import reverse_learning as rl

# Лимит _ml_active_cache: без него _prewarm_ml_active_cache кешировал всю
# историю (сотни тысяч экстремумов → 1-2 GB RAM → glibc heap corruption).
_ML_ACTIVE_CACHE_MAX = 100_000

# ──────────────────────────────────────────────────────────────────────────────
# OPT-6: КОДЕК result_json  (zlib + base64, обратная совместимость)
# ──────────────────────────────────────────────────────────────────────────────
_RJ_PREFIX       = "z:"                       # маркер нового формата
_RJ_EMPTY_PLAIN  = "{}"                        # старый sentinel
_RJ_EMPTY_NEW    = "z:eJyrrgUAAXUA+Q=="        # zlib({}) level=6
_RJ_FLOAT_DIGITS = 4                           # 6→4: меньше JSON-мусора до сжатия


def _rj_encode(result: dict) -> str:
    """Сериализует result dict → сжатая строка с префиксом 'z:'.
    Пустой dict → специальный литерал (не тратим zlib на 2 байта)."""
    if not result:
        return _RJ_EMPTY_NEW
    raw = _json.dumps(
        {k: round(v, _RJ_FLOAT_DIGITS) for k, v in result.items()},
        ensure_ascii=False,
        separators=(",", ":"),      # убираем пробелы → ещё -5% до сжатия
    ).encode()
    return _RJ_PREFIX + _b64.b64encode(_zlib.compress(raw, 6)).decode()


def _rj_decode(s: str) -> dict:
    """Десериализует result_json строку → dict.
    Прозрачно обрабатывает старый (plain JSON) и новый ('z:...') форматы."""
    if not s or s == _RJ_EMPTY_PLAIN:
        return {}
    if s.startswith(_RJ_PREFIX):
        try:
            return _json.loads(_zlib.decompress(_b64.b64decode(s[2:])))
        except Exception:
            return {}
    # Старый формат — обычный JSON (обратная совместимость)
    try:
        return _json.loads(s)
    except Exception:
        return {}


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
# [A] КОНФИГ-ЛОАДЕР И run_standard_model
# ══════════════════════════════════════════════════════════════════════════════

import json as _json_cfg
import sys as _sys_cfg
from typing import Callable, Optional

# Глобальный кеш конфига текущего сервиса (один процесс = один сервис)
_SERVICE_CONFIG: dict = {}


def get_service_config() -> dict:
    """
    Публичный геттер — вызывается из model.py для чтения конфига.
    Безопасно вызывать из enrich_dataset() и model().
    """
    return _SERVICE_CONFIG


def _valid_developer_email(value) -> str | None:
    """Return a normalized email or None for an empty/invalid value."""
    email = str(value or "").strip()
    if not email or "@" not in email:
        return None
    local, domain = email.rsplit("@", 1)
    if not local or not domain or "." not in domain:
        return None
    return email


def _resolve_developer_email(config: dict, model_module) -> str:
    """Resolve per-model trace email with backward-compatible fallbacks.

    Priority:
      1. config.toml/config.json: [developer].email
      2. config alias: [service].developer_email
      3. model.py: DEVELOPER_EMAIL
      4. .env: ALERT_EMAIL
      5. historical framework default
    Invalid/empty candidates are skipped rather than breaking trace delivery.
    """
    developer_cfg = config.get("developer", {})
    service_cfg = config.get("service", {})
    if not isinstance(developer_cfg, dict):
        developer_cfg = {}
    if not isinstance(service_cfg, dict):
        service_cfg = {}

    candidates = (
        developer_cfg.get("email"),
        service_cfg.get("developer_email"),
        getattr(model_module, "DEVELOPER_EMAIL", None),
        os.getenv("ALERT_EMAIL"),
        _DEFAULT_DEVELOPER_EMAIL,
    )
    for candidate in candidates:
        email = _valid_developer_email(candidate)
        if email:
            return email
    return _DEFAULT_DEVELOPER_EMAIL


def _load_service_config(model_dir: str) -> dict:
    """
    Ищет конфиг в директории сервиса. Порядок приоритета:
      1. config.toml  — предпочтительный (поддерживает комментарии)
      2. config.json  — для PHP-совместимости
      3. {}           — дефолты подставит build_app из model.py атрибутов

    .env остаётся только для секретов (DB_HOST, DB_PASSWORD и т.д.)
    """
    import os

    toml_path = os.path.join(model_dir, "config.toml")
    if os.path.exists(toml_path):
        try:
            if _sys_cfg.version_info >= (3, 11):
                import tomllib
                with open(toml_path, "rb") as f:
                    data = tomllib.load(f)
            else:
                import tomli  # pip install tomli
                with open(toml_path, "rb") as f:
                    data = tomli.load(f)
            log(f"   config: {toml_path}", "brain-framework", force=True)
            return data
        except ImportError:
            log("   config: tomli not installed, trying config.json",
                "brain-framework", level="warning")
        except Exception as e:
            log(f"   config: toml error — {e}", "brain-framework", level="error")

    json_path = os.path.join(model_dir, "config.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = _json_cfg.load(f)
            log(f"   config: {json_path}", "brain-framework", force=True)
            return data
        except Exception as e:
            log(f"   config: json error — {e}", "brain-framework", level="error")

    log("   config: no config file, using model.py attributes",
        "brain-framework", force=True)
    return {}


# ── run_standard_model ────────────────────────────────────────────────────────


def _run_standard_model_multi_slots(
    rates,
    dataset,
    date: datetime,
    *,
    slots: list[tuple[int, int]],
    dataset_index: dict | None,
    shift_window: int,
    apply_var_fn: Callable[[float, float, int, dict], float],
    min_occurrence: int = 2,
) -> dict[tuple[int, int], dict[str, float]]:
    """Compute many ``type/var`` combinations in one exact event pass."""
    slot_list = list(dict.fromkeys((int(t), int(v)) for t, v in slots))
    outputs: dict[tuple[int, int], dict[str, float]] = {
        slot: {} for slot in slot_list
    }
    if not rates or not dataset or not slot_list:
        return outputs

    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if not ctx_index:
        return outputs
    reverse: dict[str, tuple[int, dict]] = {
        str(info.get("event_type") or "").strip().lower(): (int(info["id"]), info)
        for _, info in ctx_index.items()
        if info.get("id") and info.get("event_type")
    }
    if not reverse:
        return outputs

    np_rates = (dataset_index or {}).get("np_rates")
    np_view, is_bull = _std_slice_np(np_rates, date, rates)
    is_daily = rates[-1]["date"].hour == 0 and rates[-1]["date"].minute == 0
    r_t1, r_t1d, ext_set, ext_day = _std_rate_dicts(
        rates, is_bull, is_daily, np_view
    )

    window_sec = int(shift_window) * 86400
    effective_dataset = dataset
    if dataset_index:
        full_dataset = dataset_index.get("full_dataset")
        dataset_ts = dataset_index.get("dataset_timestamps")
        if (
            full_dataset is not None
            and dataset_ts is not None
            and len(dataset_ts) == len(full_dataset)
        ):
            date_ts = int(date.timestamp())
            left = int(np.searchsorted(dataset_ts, date_ts - window_sec, side="left"))
            right = int(np.searchsorted(dataset_ts, date_ts, side="right"))
            effective_dataset = _list_view(full_dataset, left, right)

    vars_needed = sorted({v for _, v in slot_list})
    types_by_var: dict[int, list[int]] = {}
    for tp, vr in slot_list:
        types_by_var.setdefault(vr, []).append(tp)

    for row in effective_dataset:
        parsed = _std_get_event(row)
        if parsed is None:
            continue
        event_time, pct, event_type = parsed
        lookup = reverse.get(event_type)
        if lookup is None:
            continue
        ctx_id, ctx_info = lookup
        occ = int(ctx_info.get("occurrence_count") or 0)

        diff_sec = (date - event_time).total_seconds()
        if diff_sec < 0 or diff_sec > window_sec:
            continue
        shift = int(diff_sec // 86400)
        if occ < min_occurrence and shift != 0:
            continue
        t_date = (event_time + timedelta(days=shift)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if t_date >= date:
            continue

        t1, ext_hit = _std_candle(
            t_date, np_view, is_daily, r_t1, r_t1d, ext_set, ext_day
        )
        direction = 1.0 if pct > 0 else -1.0
        signed_t1 = t1 * direction
        for vr in vars_needed:
            weighted_t1 = apply_var_fn(signed_t1, pct, vr, ctx_info)
            for tp in types_by_var.get(vr, ()):
                contribution = _std_signal(
                    tp, ctx_id, shift, weighted_t1,
                    ext_hit, pct, occ, direction,
                )
                out = outputs[(tp, vr)]
                for wc, val in contribution.items():
                    if val != 0.0:
                        out[wc] = out.get(wc, 0.0) + val

    return {
        slot: {k: v for k, v in out.items() if v != 0.0}
        for slot, out in outputs.items()
    }


def _standard_dataset_is_midnight(dataset) -> bool:
    """True when every parseable standard enriched event is at midnight."""
    if not dataset:
        return False
    seen = False
    for row in dataset:
        parsed = _std_get_event(row)
        if parsed is None:
            continue
        seen = True
        event_time = parsed[0]
        if (
            event_time.hour != 0
            or event_time.minute != 0
            or event_time.second != 0
            or event_time.microsecond != 0
        ):
            return False
    return seen


def run_standard_model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    *,
    type: int,
    var: int,
    dataset_index: dict | None,
    shift_window: int,
    apply_var_fn: Callable[[float, float, int, dict], float],
    min_occurrence: int = 2,
    get_event_fn: Optional[Callable[[dict], Optional[tuple]]] = None,
    signal_fn: Optional[Callable] = None,
) -> dict[str, float]:
    """
    Универсальный движок модели. Вызывается из model() сервиса.

    Параметры
    ─────────
    apply_var_fn(signed_t1, pct, var, ctx_info) → float
        Специфична для сервиса. Как взвешивать T1.

    signal_fn(type, ctx_id, shift, weighted_t1, ext_hit, pct, occ, direction, ctx_info)
        → dict[str, float]  — свои weight_code для этого event
        → {}                — пропустить событие
        → None              — использовать встроенную логику (type 0/1/2)
        Передавать только если нужны кастомные type (3, 4, 5...).
        Для type 0/1/2 возвращать None — дублировать логику не нужно.

    get_event_fn(row) → (event_time, pct, event_type) | None
        Как парсить строку датасета.
        Дефолт: читает готовые поля из enriched-таблицы (date_dt / event_time,
        pct_change, event_type). Передавать только для нестандартных датасетов.
    """
    if not rates or not dataset:
        return {}

    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if not ctx_index:
        return {}

    reverse: dict[str, tuple[int, dict]] = {
        str(info.get("event_type") or "").strip().lower(): (int(info["id"]), info)
        for _, info in ctx_index.items()
        if info.get("id") and info.get("event_type")
    }
    if not reverse:
        return {}

    np_rates = (dataset_index or {}).get("np_rates")
    np_view, is_bull = _std_slice_np(np_rates, date, rates)
    is_daily = rates[-1]["date"].hour == 0 and rates[-1]["date"].minute == 0
    r_t1, r_t1d, ext_set, ext_day = _std_rate_dicts(rates, is_bull, is_daily, np_view)

    _get_event = get_event_fn or _std_get_event
    result: dict[str, float] = {}
    window_sec = shift_window * 86400

    # All current services 62-70 use the default enriched-event format.  Their
    # original loop discarded events outside [date-shift_window, date] anyway.
    # When the framework index is available, select that exact equivalent window
    # with two searchsorted calls instead of scanning the whole historical set.
    effective_dataset = dataset
    if get_event_fn is None and dataset_index:
        full_dataset = dataset_index.get("full_dataset")
        dataset_ts = dataset_index.get("dataset_timestamps")
        if full_dataset is not None and dataset_ts is not None and len(dataset_ts) == len(full_dataset):
            date_ts = int(date.timestamp())
            left = int(np.searchsorted(dataset_ts, date_ts - window_sec, side="left"))
            right = int(np.searchsorted(dataset_ts, date_ts, side="right"))
            effective_dataset = _list_view(full_dataset, left, right)

    for row in effective_dataset:
        parsed = _get_event(row)
        if parsed is None:
            continue
        event_time, pct, event_type = parsed

        lookup = reverse.get(event_type)
        if lookup is None:
            continue
        ctx_id, ctx_info = lookup
        occ = int(ctx_info.get("occurrence_count") or 0)

        diff_sec = (date - event_time).total_seconds()
        if diff_sec < 0 or diff_sec > window_sec:
            continue

        shift = int(diff_sec // 86400)
        if occ < min_occurrence and shift != 0:
            continue

        t_date = (event_time + timedelta(days=shift)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if t_date >= date:
            continue

        t1, ext_hit = _std_candle(t_date, np_view, is_daily, r_t1, r_t1d, ext_set, ext_day)
        direction   = 1.0 if pct > 0 else -1.0
        weighted_t1 = apply_var_fn(t1 * direction, pct, var, ctx_info)

        if signal_fn is not None:
            contribution = signal_fn(
                type, ctx_id, shift, weighted_t1,
                ext_hit, pct, occ, direction, ctx_info,
            )
            if contribution is None:
                contribution = _std_signal(
                    type, ctx_id, shift, weighted_t1,
                    ext_hit, pct, occ, direction,
                )
        else:
            contribution = _std_signal(
                type, ctx_id, shift, weighted_t1,
                ext_hit, pct, occ, direction,
            )

        for wc, val in contribution.items():
            if val != 0.0:
                result[wc] = result.get(wc, 0.0) + val

    return {k: v for k, v in result.items() if v != 0.0}


def _std_signal(
    type: int, ctx_id: int, shift: int,
    weighted_t1: float, ext_hit: bool,
    pct: float, occ: int, direction: float,
) -> dict[str, float]:
    """Встроенная логика типов 0 / 1 / 2."""
    result: dict[str, float] = {}
    if weighted_t1 != 0.0 and type in (0, 1):
        result[f"{ctx_id}_0_{shift}"] = round(weighted_t1, 6)
    if type in (0, 2) and occ > 0 and ext_hit:
        ext = ((1.0 / occ) * 2 - 1) * direction
        if ext != 0.0:
            result[f"{ctx_id}_1_{shift}"] = round(ext, 6)
    return result


def _std_get_event(row: dict) -> Optional[tuple]:
    """Парсит строку enriched-датасета."""
    v = row.get("event_time") or row.get("date_dt")
    if v is None:
        return None
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                v = datetime.strptime(v[:19], fmt)
                break
            except ValueError:
                pass
        else:
            return None
    try:
        return v, float(row["pct_change"]), str(row["event_type"])
    except (KeyError, TypeError, ValueError):
        return None


def _std_slice_np(np_rates, date, rates):
    is_bull = float(rates[-1].get("close") or 0) > float(rates[-1].get("open") or 0)
    if np_rates is None:
        return None, is_bull
    dn = np_rates.get("dates_ns")
    if dn is None:
        return None, is_bull
    cut = int(np.searchsorted(dn, int(date.timestamp()), side="right"))
    if cut > 0:
        is_bull = float(np_rates["close"][cut - 1]) > float(np_rates["open"][cut - 1])
    return {
        "dates_ns": dn[:cut],
        "t1":       np_rates["t1"][:cut],
        "ext":      (np_rates["ext_max"] if is_bull else np_rates["ext_min"])[:cut],
        "cut":      cut,
    }, is_bull


def _std_rate_dicts(rates, is_bull, is_daily, np_view):
    if np_view is not None:
        return {}, {}, set(), {}
    t1   = {r["date"]: float((r.get("close") or 0) - (r.get("open") or 0)) for r in rates}
    emax, emin = set(), set()
    for i in range(1, len(rates) - 1):
        h  = float(rates[i].get("max") or 0)
        lo = float(rates[i].get("min") or 0)
        if h  > float(rates[i-1].get("max") or 0) and h  > float(rates[i+1].get("max") or 0): emax.add(rates[i]["date"])
        if lo < float(rates[i-1].get("min") or 0) and lo < float(rates[i+1].get("min") or 0): emin.add(rates[i]["date"])
    ext  = emax if is_bull else emin
    t1d  = {r["date"].date(): float((r.get("close") or 0) - (r.get("open") or 0)) for r in rates} if is_daily else {}
    extd = {d.date(): True for d in ext} if is_daily else {}
    return t1, t1d, ext, extd


def _std_candle(t_date, np_view, is_daily, r_t1, r_t1d, ext_set, ext_day):
    if np_view is not None:
        ts  = int(t_date.timestamp())
        idx = int(np.searchsorted(np_view["dates_ns"], ts, side="left"))
        if idx >= np_view["cut"] or int(np_view["dates_ns"][idx]) != ts:
            if not is_daily:
                return 0.0, False
            l = int(np.searchsorted(np_view["dates_ns"], ts, side="left"))
            r = int(np.searchsorted(np_view["dates_ns"], ts + 86400, side="left"))
            if r <= l:
                return 0.0, False
            idx = r - 1
        return float(np_view["t1"][idx]), bool(np_view["ext"][idx])
    if is_daily:
        dk = t_date.date()
        return r_t1d.get(dk, 0.0), bool(ext_day.get(dk, False))
    return r_t1.get(t_date, 0.0), t_date in ext_set


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
                occurrence_count = occurrence_count + VALUES(`occurrence_count`),
                last_dt  = IF(VALUES(`last_dt`)  > last_dt  OR last_dt  IS NULL, VALUES(`last_dt`),  last_dt),
                first_dt = IF(VALUES(`first_dt`) < first_dt OR first_dt IS NULL, VALUES(`first_dt`), first_dt)
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
    """Bulk INSERT IGNORE — одна транзакция, чанки по 500 строк вместо N запросов."""
    if not events:
        return 0
    _reserved  = {"ctx_id", "event_date"}
    extra_cols = [k for k in events[0] if k not in _reserved]
    extra_insert = (", ".join(f"`{c}`" for c in extra_cols) + ", ") if extra_cols else ""
    extra_vals   = (", ".join(f":{c}" for c in extra_cols) + ", ") if extra_cols else ""

    all_params = []
    for ev in events:
        p = {"cid": ev["ctx_id"], "ed": ev.get("event_date")}
        for c in extra_cols:
            p[c] = ev.get(c)
        all_params.append(p)

    added = 0
    sql = text(f"""
        INSERT IGNORE INTO `{table}`
            ({extra_insert}`ctx_id`, `event_date`)
        VALUES ({extra_vals}:cid, :ed)
    """)
    async with engine.begin() as conn:
        for i in range(0, len(all_params), 500):
            r = await conn.execute(sql, all_params[i: i + 500])
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
    DEVELOPER_EMAIL: str = _DEFAULT_DEVELOPER_EMAIL

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
    PARAM_RANGE:         list       = None
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
    FILL_ML_WORKERS:      int   = 1
    FILL_ML_BATCH_SIZE:   int   = 2000

    # Новые поля для enriched-паттерна
    PARSER_TABLE:         str | None = None
    ENRICHED_TABLE:       str | None = None
    DATASET_INDEX_FIELDS: list | None = None
    DATASET_DATE_FIELD:   str = "date_dt"
    enrich_fn:            object = None

    def __init__(self):
        self.CTX_KEY_COLUMNS   = ["id"]
        self.VAR_RANGE         = []
        self.TYPES_RANGE       = [0, 1, 2, 3, 4]
        self.PARAM_RANGE       = [""]

        self.model_fn          = None
        self.batch_model_fn    = None
        self.index_builder_fn  = None
        self.weight_builder_fn = None
        self.model_needs_index = False
        self.model_can_filter_dataset_by_date = False
        self.model_uses_rate_history = True

        self.engine_vlad  = None
        self.engine_brain = None
        self.engine_super = None
        # Отдельный пул, но те же SUPER_* реквизиты: общий кеш находится на Brain 1.
        self.engine_cache = None
        self.cache_writer: bool | None = None
        self.cache_role: str = "unknown"
        self.cache_upstream_url: str = ""

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
        self.global_rates_dates: dict = {}
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
        # Backward-compatible field; ML training is deduplicated inside ReverseStore
        # by per-train-key locks instead of one global /values semaphore.
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
            "super": s.engine_super, "cache": s.engine_cache}.get(name, s.engine_vlad)


def _env_bool_override(name: str) -> bool | None:
    raw = os.getenv(name, "auto").strip().lower()
    if raw in {"1", "true", "yes", "on", "writer"}:
        return True
    if raw in {"0", "false", "no", "off", "reader"}:
        return False
    return None


async def _mysql_server_identity(engine) -> tuple | None:
    """Идентификатор физического MySQL-сервера, независимо от имени схемы."""
    if engine is None:
        return None
    for query, prefix in (
        ("SELECT @@server_uuid", "uuid"),
        ("SELECT @@server_id, @@hostname, @@port", "server"),
        ("SELECT @@hostname, @@port", "host"),
    ):
        try:
            async with engine.connect() as conn:
                row = (await conn.execute(text(query))).fetchone()
            if row and row[0] is not None:
                return (prefix, *tuple(str(v) for v in row))
        except Exception:
            continue
    return None


def _build_cache_upstream_url(port: int) -> str:
    """URL Python-сервиса на Brain 1 для fallback при cache MISS.

    По умолчанию HTTP-хост берётся из SUPER_HOST, поскольку SUPER_* уже ведёт
    на первую ноду. При необходимости можно задать CACHE_UPSTREAM_URL:
      http://10.0.0.1          -> порт сервиса добавится автоматически
      http://cache/{port}      -> подстановка {port}
      http://10.0.0.1:8916     -> используется как есть
    """
    raw = os.getenv("CACHE_UPSTREAM_URL", "").strip().rstrip("/")
    if raw:
        if "{port}" in raw:
            return raw.format(port=port).rstrip("/")
        try:
            from urllib.parse import urlsplit
            parsed = urlsplit(raw if "://" in raw else f"http://{raw}")
            normalized = raw if "://" in raw else f"http://{raw}"
            if parsed.port is not None:
                return normalized.rstrip("/")
            return f"{normalized.rstrip('/')}:{port}"
        except Exception:
            return f"{raw}:{port}"

    host = os.getenv("SUPER_HOST", "").strip()
    if not host:
        return ""
    scheme = os.getenv("CACHE_UPSTREAM_SCHEME", "http").strip() or "http"
    return f"{scheme}://{host}:{port}"


async def _detect_cache_role(s: _State) -> bool:
    """Определяет, является ли текущая нода единственным cache-writer.

    CACHE_WRITER=1/0 имеет приоритет. В режиме auto Brain 1 определяется по
    совпадению физического MySQL-сервера DB_* и SUPER_*. При ошибке определения
    выбирается безопасная роль reader, чтобы дочерняя нода не начала считать.
    """
    override = _env_bool_override("CACHE_WRITER")
    local_id = super_id = None
    if override is None:
        local_id = await _mysql_server_identity(s.engine_vlad)
        super_id = await _mysql_server_identity(s.engine_cache)
        is_writer = bool(local_id and super_id and local_id == super_id)
        source = "mysql-auto" if (local_id and super_id) else "safe-reader-fallback"
    else:
        is_writer = override
        source = "CACHE_WRITER env"

    s.cache_writer = is_writer
    s.cache_role = "writer-brain1" if is_writer else "reader-child"
    s.cache_upstream_url = _build_cache_upstream_url(s.PORT)
    log(
        f" central cache role={s.cache_role} source={source} "
        f"storage=SUPER_* upstream={s.cache_upstream_url or 'not-configured'} "
        f"local_mysql={local_id or 'n/a'} super_mysql={super_id or 'n/a'}",
        s.NODE_NAME, force=True,
    )
    return is_writer


async def _proxy_values_to_brain1(
    s: _State, *, pair: int, day: int, date: str,
    calc_type: int, calc_var: int, param: str,
) -> dict:
    """Получает cache MISS с Brain 1, не исполняя локальную model()."""
    if _requests is None:
        raise RuntimeError("requests package is not installed")
    if not s.cache_upstream_url:
        raise RuntimeError(
            "CACHE_UPSTREAM_URL is empty and SUPER_HOST is not configured"
        )

    timeout = max(1.0, float(os.getenv("CACHE_UPSTREAM_TIMEOUT", "120")))
    url = f"{s.cache_upstream_url.rstrip('/')}/values"
    params = {
        "pair": pair, "day": day, "date": date,
        "type": calc_type, "var": calc_var, "param": param,
        "_cache_hop": 1,
    }

    def _request():
        response = _requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or data.get("status") != "ok":
            raise RuntimeError(
                data.get("error", "invalid response from Brain 1")
                if isinstance(data, dict) else "invalid JSON response from Brain 1"
            )
        payload = data.get("payLoad")
        if payload is None:
            raise RuntimeError("Brain 1 response has no payLoad")
        return payload

    return await asyncio.to_thread(_request)


def _params_hash(params: dict) -> str:
    import hashlib
    return hashlib.md5(
        _json.dumps(params, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def _filter_rates_lte(table: str, date: datetime, s: _State) -> list[dict]:
    rows = s.global_rates.get(table, [])
    if not rows:
        return []
    dates = s.global_rates_dates.get(table)
    if dates is None or len(dates) != len(rows):
        dates = [r["date"] for r in rows]
        s.global_rates_dates[table] = dates
    idx = bisect.bisect_right(dates, date)
    return _list_view(rows, 0, idx)


# === ПАТЧ 1: _filter_dataset_lte — binary search ===
def _filter_dataset_lte(date: datetime, s: _State) -> list[dict]:
    if not s.FILTER_DATASET_BY_DATE:
        return s.dataset
    idx = bisect.bisect_right(s.dataset_dates, date)
    return _list_view(s.dataset, 0, idx)


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
        s.global_rates_dates[table] = []
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
                s.global_rates_dates[table] = [r["date"] for r in s.global_rates[table]]
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
                s.global_rates_dates.setdefault(table, []).append(dt)
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

    # ── Загрузка конфига ──────────────────────────────────────────────────────
    global _SERVICE_CONFIG
    _model_dir = os.path.dirname(os.path.abspath(
        sys.modules[model_module.__name__].__file__
    ))
    _SERVICE_CONFIG = _load_service_config(_model_dir)
    _c = _SERVICE_CONFIG  # алиас

    # Хелпер: config.toml[section][key] → model.py attr → .env → default
    def _get(section: str, key: str, attr: str, env_key: str = "", default=None):
        v = _c.get(section, {}).get(key)
        if v is not None:
            return v
        v = getattr(model_module, attr, None)
        if v is not None:
            return v
        if env_key:
            v = os.getenv(env_key)
            if v is not None:
                try:
                    return type(default)(v) if default is not None else v
                except Exception:
                    return v
        return default

    # ── Идентификация ─────────────────────────────────────────────────────────
    s.SERVICE_ID   = int(_get("service", "id",   "SERVICE_ID",   "SERVICE_ID",   0))
    s.PORT         = int(_get("service", "port", "PORT",         "PORT",         9000))
    s.NODE_NAME    =     _get("service", "name", "NODE_NAME",    "NODE_NAME",    "brain-svc")
    s.SERVICE_TEXT =     _get("service", "text", "SERVICE_TEXT", "SERVICE_TEXT", "Brain microservice")

    # Почта разработчика конкретной модели. Стандартный ключ:
    #   [developer]
    #   email = "developer@example.com"
    # Старые модели без параметра продолжают использовать ALERT_EMAIL/.env
    # либо исторический адрес vladyurjevitch@yandex.ru.
    s.DEVELOPER_EMAIL = set_alert_email(
        _resolve_developer_email(_c, model_module)
    )

    # ── Котировки ─────────────────────────────────────────────────────────────
    s.RATES_TABLE = _get("rates", "table", "RATES_TABLE", "RATES_TABLE", "brain_rates_eur_usd")

    # ── Кеш ───────────────────────────────────────────────────────────────────
    s.CACHE_DATE_FROM  =      _get("cache", "date_from",        "CACHE_DATE_FROM",  "", "2025-01-15")
    s.VAR_RANGE        =      _get("cache", "var_range",        "VAR_RANGE",        "", [0])
    s.TYPES_RANGE      =      _get("cache", "types_range",      "TYPES_RANGE",      "", [0, 1, 2, 3, 4])
    s.PARAM_RANGE      =      _get("cache", "param_range",      "PARAM_RANGE",      "", [""])
    if not isinstance(s.PARAM_RANGE, list):
        s.PARAM_RANGE = [str(s.PARAM_RANGE)]
    s.PARAM_RANGE = [str(v) for v in s.PARAM_RANGE] or [""]
    s.SHIFT_WINDOW     = int( _get("cache", "shift_window",     "SHIFT_WINDOW",     "", 12))
    s.REBUILD_INTERVAL = int(_get("cache", "rebuild_interval", "REBUILD_INTERVAL", "", 0))
    s.RELOAD_INTERVAL = int(_get("cache", "reload_interval", "RELOAD_INTERVAL", "", 3600))

    # ── Датасет (новый способ — через enriched_table) ─────────────────────────
    s.PARSER_TABLE         = _get("dataset", "parser_table",   "PARSER_TABLE",         "", None)
    s.ENRICHED_TABLE       = _get("dataset", "enriched_table", "ENRICHED_TABLE",       "", None)
    s.DATASET_INDEX_FIELDS = _get("dataset", "index_fields",   "DATASET_INDEX_FIELDS", "", None)
    s.DATASET_DATE_FIELD   = _get("dataset", "date_field",     "DATASET_DATE_FIELD",   "", "date_dt")

    # ── Датасет (старый способ — прямой запрос, обратная совместимость) ───────
    s.DATASET_TABLE          = _get("dataset", "table",          "DATASET_TABLE",          "", None)
    s.DATASET_QUERY          = _get("dataset", "query",          "DATASET_QUERY",           "", None)
    s.DATASET_ENGINE         = _get("dataset", "engine",         "DATASET_ENGINE",          "", "vlad")
    s.FILTER_DATASET_BY_DATE = bool(_get("dataset", "filter_by_date", "FILTER_DATASET_BY_DATE", "", False))
    s.dataset_key_field = _get("dataset", "key_field", "DATASET_KEY", "", "ctx_id")

    # ── Контекст и веса ───────────────────────────────────────────────────────
    s.CTX_KEY_COLUMNS    = _get("ctx", "key_columns",        "CTX_KEY_COLUMNS",    "", ["id"])
    s.CTX_TABLE          = _get("ctx", "table",              "CTX_TABLE",          "", None)
    s.CTX_QUERY          = _get("ctx", "query",              "CTX_QUERY",          "", None)
    s.WEIGHTS_TABLE      = _get("ctx", "weights_table",      "WEIGHTS_TABLE",      "", None)
    s.WEIGHTS_CODE_COLUMN= _get("ctx", "weights_code_column","WEIGHTS_CODE_COLUMN","", "weight_code")
    s.URL_MAP_QUERY      = _get("ctx", "url_map_query",      "URL_MAP_QUERY",      "", None)
    s.URL_MAP_ENGINE     = _get("ctx", "url_map_engine",     "URL_MAP_ENGINE",     "", "vlad")

    # ── ML ────────────────────────────────────────────────────────────────────
    s.USE_ML_VALUES       = bool( _get("ml", "enabled",          "USE_ML_VALUES",       "", False))
    s.ML_INIT_MODE        =       _get("ml", "init_mode",        "ML_INIT_MODE",        "", "constant")
    s.ML_TARGET_PRECISION = float(_get("ml", "target_precision", "ML_TARGET_PRECISION", "", 0.95))
    s.ML_MAX_ITER         = int(  _get("ml", "max_iter",         "ML_MAX_ITER",         "", 20))
    s.ML_STEP             = float(_get("ml", "step",             "ML_STEP",             "", 0.10))
    s.ML_EXTREMUM_LIMIT   = int(  _get("ml", "extremum_limit",   "ML_EXTREMUM_LIMIT",   "", 50))
    s.ML_ACTIVE_TAIL      = int(  _get("ml", "active_tail",      "ML_ACTIVE_TAIL",      "", 0))
    s.ML_PRECISION_METRIC =       _get("ml", "precision_metric", "ML_PRECISION_METRIC", "", "mean")
    s.FILL_ML_WORKERS     = max(1, int(_get("cache", "ml_workers", "FILL_ML_WORKERS", "FILL_ML_WORKERS", getattr(rl, "_RL_TRAIN_WORKERS", 1))))
    s.FILL_ML_BATCH_SIZE  = max(100, int(_get("cache", "ml_batch_size", "FILL_ML_BATCH_SIZE", "FILL_ML_BATCH_SIZE", 2000)))

    # ── Прочее ────────────────────────────────────────────────────────────────
    s.LABEL_FN  = getattr(model_module, "LABEL_FN",  None)
    s.enrich_fn = getattr(model_module, "enrich_dataset", None)

    # ── Авто-деривация для новых сервисов с ENRICHED_TABLE ───────────────────
    if s.ENRICHED_TABLE:
        et = s.ENRICHED_TABLE
        df = s.DATASET_DATE_FIELD

        if not s.WEIGHTS_TABLE:
            s.WEIGHTS_TABLE = f"{et}_weights"

        if not s.DATASET_QUERY and not s.DATASET_TABLE:
            s.DATASET_QUERY  = f"SELECT *, `{df}` AS date FROM `{et}` ORDER BY `{df}`"
            s.DATASET_ENGINE = _get("dataset", "engine", "DATASET_ENGINE", "", "vlad")

        if not s.CTX_TABLE and not s.CTX_QUERY:
            s.CTX_QUERY = f"""
                SELECT
                    idx.id,
                    idx.event_type,
                    idx.date_added,
                    COALESCE(agg.occurrence_count,   0) AS occurrence_count,
                    COALESCE(agg.avg_abs_pct_change, 0) AS avg_abs_pct_change,
                    COALESCE(agg.avg_pct_change,     0) AS avg_pct_change
                FROM `{et}_indexes` idx
                LEFT JOIN (
                    SELECT
                        event_type,
                        COUNT(*)             AS occurrence_count,
                        AVG(ABS(pct_change)) AS avg_abs_pct_change,
                        AVG(pct_change)      AS avg_pct_change
                    FROM `{et}`
                    GROUP BY event_type
                ) agg ON agg.event_type = idx.event_type
                WHERE idx.mask_id = 1
            """

    # ── model() и batch_model() ──────────────────────────────────────────────
    s.model_fn = getattr(model_module, "model", None)
    if s.model_fn is None:
        raise RuntimeError("model_module должен определять функцию model()")

    # batch_model() — опциональная пакетная точка входа (O(N) fill_cache).
    s.batch_model_fn = getattr(model_module, "batch_model", None)

    sig = inspect.signature(s.model_fn)
    s.model_needs_index = "dataset_index" in sig.parameters
    s.model_can_filter_dataset_by_date = bool(_get("dataset", "model_can_filter", "MODEL_CAN_FILTER_DATASET_BY_DATE", "", False))
    s.model_uses_rate_history = bool(_get("dataset", "model_uses_rates", "MODEL_USES_RATE_HISTORY", "", True))

    s.index_builder_fn, s.weight_builder_fn = _discover_builders(model_module)

    _has_rebuild = s.index_builder_fn or s.weight_builder_fn
    log(f"  VAR_RANGE={s.VAR_RANGE} | "
        f"dataset_index={'yes' if s.model_needs_index else 'no'} | "
        f"rebuild={'builders' if _has_rebuild else 'no'} | "
        f"ml={'ON' if s.USE_ML_VALUES else 'off'} | "
        f"batch_model={'yes' if s.batch_model_fn else 'no'} | "
        f"enriched_table={s.ENRICHED_TABLE or 'no'} | "
        f"normal_cache=zero-copy | standard_window=auto",
        s.NODE_NAME, force=True)

    s.engine_vlad, s.engine_brain, s.engine_super = build_engines()
    s.engine_cache = build_cache_engine()

    # ── Patch connection pool settings ────────────────────────────────────────
    # build_engines() в common.py создаёт движки без pool_pre_ping.
    # Это приводит к "Packet sequence number wrong" при использовании
    # протухших соединений из пула. Патчим pool._pre_ping напрямую
    # (публичный API не позволяет менять это после создания движка).
    for _eng in (s.engine_vlad, s.engine_brain, s.engine_super, s.engine_cache):
        if _eng is None:
            continue
        try:
            _pool = _eng.sync_engine.pool
            _pool._pre_ping    = True   # проверять соединение перед выдачей из пула
            _pool._recycle     = 1800   # принудительно пересоздавать каждые 30 мин
        except Exception as _pe:
            log(f"   pool patch: {_pe}", s.NODE_NAME, level="warning")

    s.reverse_store = rl.ReverseStore(s.engine_vlad, port=s.PORT)

    # ── _call_model ───────────────────────────────────────────────────────────

    async def _call_model(pair, day, date_str, calc_type=0, calc_var=0, param="",
                          _skip_refresh: bool = False):
        if s.cache_writer is False:
            raise RuntimeError(
                "Local model() is disabled on child node; cache MISS must be "
                "computed by Brain 1"
            )
        target_date = _parse_date(date_str)
        if not target_date:
            return None
        table = _rates_table(pair, day)
        if not _skip_refresh:
            await _refresh_rates(table, s)
        np_r = s.np_rates.get(table)

        def _dataset_index_for(date_x: datetime):
            if not s.model_needs_index:
                return None
            di = {
                "dates": s.dataset_dates,
                "by_key": s.dataset_by_key,
                "key_dates": s.dataset_key_dates,
                "key_field": s.dataset_key_field,
                "np_rates": np_r,
                "ctx_index": s.ctx_index,
                "url_map": s.url_map,
                "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
                "filter_dataset_by_date": bool(s.FILTER_DATASET_BY_DATE),
                "dataset_cutoff_ts": float(date_x.timestamp()),
                "is_daily": bool(day),
                "rates_table": table,
                "execution_scope": "live",
            }
            # Safe internal accelerator for run_standard_model(); no model flag required.
            di["full_dataset"] = s.dataset
            return di

        def _model_inputs_for(date_x: datetime):
            if s.model_uses_rate_history:
                rates_x = _filter_rates_lte(table, date_x, s)
            else:
                # Generic capability path: model declared that it does not need
                # the historical rate slice. Keep a non-empty marker for backward
                # compatibility with models that only check truthiness/timeframe.
                marker_dt = date_x.replace(hour=0, minute=0, second=0, microsecond=0) if day else date_x
                rates_x = [{"date": marker_dt}]

            dataset_x = s.dataset if s.model_can_filter_dataset_by_date else _filter_dataset_lte(date_x, s)
            return rates_x, dataset_x, _dataset_index_for(date_x)

        def _model_at(date_x: datetime) -> dict:
            rates_x, dataset_x, dataset_index_dict = _model_inputs_for(date_x)
            r = s.model_fn(
                rates=rates_x, dataset=dataset_x, date=date_x,
                type=calc_type, var=calc_var, param=param,
                dataset_index=dataset_index_dict,
            )
            r, _ = _extract_detail(r)
            return r or {}

        if not s.USE_ML_VALUES:
            return _model_at(target_date)

        # ── ML-РЕЖИМ ─────────────────────────────────────────────────────────
        async def _active_codes_at(ext_dt: datetime) -> list[str]:
            ts  = int(ext_dt.timestamp())
            key = (pair, day, ts, calc_type, calc_var, param)
            cached = s._ml_active_cache.get(key)
            if cached is not None:
                return cached
            try:
                codes = list(_model_at(ext_dt).keys())
            except Exception:
                codes = []
            s._ml_active_cache[key] = codes
            return codes

        params_hash = _params_hash({
            "type": calc_type, "var": calc_var, "param": param,
        })

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
                model_tag=f"{pair}_{day}_{calc_type}_{calc_var}_{param}",
            )
            return universe
        except Exception as e:
            log(f"   ML _call_model {target_date}: {e}",
                s.NODE_NAME, level="error")
            send_error_trace(e, s.NODE_NAME, "ml_call_model")
            return {}

    # ── _preload ──────────────────────────────────────────────────────────────

    async def _preload():
        if s.cache_writer is None:
            await _detect_cache_role(s)
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

        if s.cache_writer:
            try:
                await ensure_cache_table(s.engine_cache, s.cache_table)
            except Exception as e:
                log(f"   central cache table: {e}", s.NODE_NAME, level="error")
        else:
            log(
                f" central cache reader: SUPER_* / {s.cache_table}",
                s.NODE_NAME, force=True,
            )

        if s.USE_ML_VALUES:
            try:
                await s.reverse_store.ensure_tables()
                s._ml_active_cache.clear()
                _warm_workers = await rl.warmup_train_executor()
                if _warm_workers:
                    log(f"   RL process workers ready: {_warm_workers}", s.NODE_NAME)
            except Exception as e:
                log(f"   reverse tables: {e}", s.NODE_NAME, level="error")

        try:
            async with s.engine_vlad.begin() as conn:
                await conn.execute(text(_DDL_BT_RESULTS))
                await conn.execute(text(_DDL_BT_SUMMARY))
        except Exception as e:
            log(f"   bt tables: {e}", s.NODE_NAME, level="error")

        s.last_reload = datetime.now()
        log(f" RELOAD DONE: rates={len(s.simple_rates)} "
            f"global_rates={sum(len(v) for v in s.global_rates.values())} "
            f"dataset={len(s.dataset)} weights={len(s.weight_codes)} "
            f"url_map={len(s.url_map)}",
            s.NODE_NAME, force=True)

    # ── _do_rebuild [C] ───────────────────────────────────────────────────────

    async def _do_rebuild() -> dict:
        stats = {}

        # Шаг 0: enrich_dataset() — трансформация raw → enriched
        # Вызывается если model.py содержит функцию enrich_dataset()
        if s.enrich_fn is not None:
            try:
                r = await s.enrich_fn(s.engine_vlad, s.engine_brain)
                stats["enrich"] = r or {}
                log(f"   enrich_dataset: {r}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   enrich_dataset: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "enrich_dataset")
                return {"error": str(e)}

        # Шаг 1: dataset_indexer — строит _mask и _indexes таблицы
        # Вызывается если заданы ENRICHED_TABLE + DATASET_INDEX_FIELDS
        if s.ENRICHED_TABLE and s.DATASET_INDEX_FIELDS:
            try:
                from dataset_indexer import build_indexes, parse_indexes
                await build_indexes(
                    s.engine_vlad, s.ENRICHED_TABLE, s.DATASET_INDEX_FIELDS
                )
                await parse_indexes(
                    s.engine_vlad, s.ENRICHED_TABLE, s.DATASET_DATE_FIELD
                )
                stats["dataset_indexer"] = {
                    "mask_table":    f"{s.ENRICHED_TABLE}_mask",
                    "indexes_table": f"{s.ENRICHED_TABLE}_indexes",
                }
                stats["auto_weights"] = await _auto_build_weights()
                log(f"   dataset_indexer: {s.ENRICHED_TABLE}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   dataset_indexer: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "dataset_indexer")
                return {"error": str(e)}

        # Шаг 2: кастомные context_idx.py + weights.py (старый способ)
        # Запускается если рядом с model.py лежат эти файлы
        _RETRIES = 3

        async def _attempt(fn, *args):
            for attempt in range(_RETRIES):
                try:
                    return await fn(*args)
                except Exception as exc:
                    if attempt < _RETRIES - 1 and (
                        "Packet sequence" in str(exc)
                        or "InternalError" in type(exc).__name__
                    ):
                        try:
                            await s.engine_vlad.dispose()
                        except Exception:
                            pass
                        await asyncio.sleep(1.5 * (attempt + 1))
                    else:
                        raise

        if s.index_builder_fn is not None:
            try:
                r = await _attempt(s.index_builder_fn, s.engine_vlad, s.engine_brain)
                stats["index"] = r or {}
                log(f"   build_index: {r}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   build_index: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "build_index")
                return {"error": str(e)}

        if s.weight_builder_fn is not None:
            try:
                r = await _attempt(s.weight_builder_fn, s.engine_vlad)
                stats["weights"] = r or {}
                log(f"   build_weights: {r}", s.NODE_NAME, force=True)
            except Exception as e:
                log(f"   build_weights: {e}", s.NODE_NAME, level="error")
                send_error_trace(e, s.NODE_NAME, "build_weights")
                return {"error": str(e)}

        await _load_weight_codes(s)
        await _load_ctx_index(s)
        await _load_label_cache(s)
        await _load_url_map(s)
        s.last_rebuild = datetime.now()
        log(
            f" rebuild done: weights={len(s.weight_codes)} ctx={len(s.ctx_index)}",
            s.NODE_NAME, force=True,
        )
        return {
            **stats,
            "ctx_total":     len(s.ctx_index),
            "weights_total": len(s.weight_codes),
            "rebuilt_at":    s.last_rebuild.isoformat(),
        }

    async def _auto_build_weights() -> dict:
        """
        Авто-генерация весов из _indexes-таблицы.
        Вызывается только для новых сервисов (ENRICHED_TABLE задан).
        Формат {idx_id}_{mode}_{shift} — совместим с run_standard_model.
        Использует INSERT IGNORE чтобы не менять id при повторных запусках
        и не инвалидировать кеш.
        """
        if not s.ENRICHED_TABLE or not s.WEIGHTS_TABLE:
            return {"skipped": True}

        et        = s.ENRICHED_TABLE
        idx_table = f"{et}_indexes"

        try:
            async with s.engine_vlad.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT idx.id, COALESCE(agg.cnt, 0) AS occ
                    FROM `{idx_table}` idx
                    LEFT JOIN (
                        SELECT event_type, COUNT(*) AS cnt
                        FROM   `{et}` GROUP BY event_type
                    ) agg ON agg.event_type = idx.event_type
                    WHERE idx.mask_id = 1
                    ORDER BY idx.id
                """))
                ctx_rows = res.fetchall()
        except Exception as e:
            return {"error": str(e)}

        rows = [
            {
                "wc":     f"{ctx_id}_{mode}_{shift}",
                "ctx_id": int(ctx_id),
                "mode":   int(mode),
                "shift":  int(shift),
            }
            for ctx_id, occ in ctx_rows
            for mode in (0, 1)
            for shift in range(0, (s.SHIFT_WINDOW if (occ or 0) >= 2 else 0) + 1)
        ]
        if not rows:
            return {"weights": 0}

        async with s.engine_vlad.begin() as conn:
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{s.WEIGHTS_TABLE}` (
                    id          INT         NOT NULL AUTO_INCREMENT,
                    weight_code VARCHAR(40) NOT NULL,
                    ctx_id      INT         NOT NULL,
                    mode        TINYINT     NOT NULL,
                    shift       SMALLINT    NOT NULL DEFAULT 0,
                    PRIMARY KEY (id),
                    UNIQUE KEY  uk_wc (weight_code),
                    INDEX       idx_ctx_id (ctx_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            # INSERT IGNORE — не меняет существующие id, не инвалидирует кеш
            for i in range(0, len(rows), 500):
                await conn.execute(text(f"""
                    INSERT IGNORE INTO `{s.WEIGHTS_TABLE}`
                        (weight_code, ctx_id, mode, shift)
                    VALUES (:wc, :ctx_id, :mode, :shift)
                """), rows[i: i + 500])

        return {"contexts": len(ctx_rows), "weights": len(rows)}

    # ── _bg_reload ────────────────────────────────────────────────────────────

    async def _bg_reload():
        while True:
            await asyncio.sleep(s.RELOAD_INTERVAL)
            try:
                await _refresh_simple_rates(s)
                for table in _RATES_TABLES:
                    await _refresh_rates(table, s)
                _has_any_rebuild = (
                    s.index_builder_fn or s.weight_builder_fn  # старый стиль
                    or s.enrich_fn or s.ENRICHED_TABLE          # новый стиль
                )
                if (s.REBUILD_INTERVAL > 0
                        and _has_any_rebuild
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
            async with s.engine_cache.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT date_val FROM `{s.cache_table}`
                    WHERE service_url=:url AND pair=:pair
                      AND day_flag=:day AND params_hash=:ph
                """), {"url": s.service_url, "pair": pair, "day": day, "ph": p_hash})
                return {row[0] for row in res.fetchall()}
        except Exception:
            return set()

    def _ordered_fill_control(s_state):
        """Return (ordered, reset_fn) for models with chronological global state.

        Such models must not be evaluated by several slot/chunk workers: the output
        depends on the exact ascending date order.  Detection is automatic and does
        not require an .env/config flag.  A model may expose
        ``reset_fill_cache_state(pair, day_flag, type, var)`` to clear only its
        fill-cache namespace before replaying the complete timeline.
        """
        fn_globals = getattr(s_state.model_fn, "__globals__", {}) or {}
        last_signal_state = fn_globals.get("_LAST_SIGNAL_TS")
        ordered = isinstance(last_signal_state, dict)
        reset_fn = fn_globals.get("reset_fill_cache_state")
        return ordered, reset_fn if callable(reset_fn) else None

    # ── _sync_compute для поточной обработки ─────────────────────────────────
    def _sync_compute(candle, calc_type, calc_var, calc_param, all_rows, all_dates, np_rates_pd, s_state, rates_tbl=""):
        td = candle["date"]

        # Обычный fill_cache обязан соблюдать те же capability-флаги, что /values.
        # Раньше здесь безусловно копировался весь исторический prefix rates[:idx]
        # и заново создавался срез dataset для КАЖДОЙ свечи, даже когда модель
        # прямо объявила, что умеет фильтровать данные сама или не использует
        # историю котировок. На длинной H1-истории это давало O(N^2) копирования.
        if s_state.model_uses_rate_history:
            idx = bisect.bisect_right(all_dates, td)
            rates_f = _list_view(all_rows, 0, idx)
        else:
            is_daily = bool(str(rates_tbl or s_state.RATES_TABLE or "").endswith("_day"))
            marker_dt = td.replace(hour=0, minute=0, second=0, microsecond=0) if is_daily else td
            rates_f = [{"date": marker_dt}]

        ds_f = (s_state.dataset if s_state.model_can_filter_dataset_by_date
                else _filter_dataset_lte(td, s_state))

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
                "filter_dataset_by_date": bool(s_state.FILTER_DATASET_BY_DATE),
                "dataset_cutoff_ts": float(td.timestamp()),
                "is_daily": bool(str(rates_tbl or s_state.RATES_TABLE or "").endswith("_day")),
                "rates_table": rates_tbl or s_state.RATES_TABLE,
                "execution_scope": "fill_cache",
            }
            # Safe internal accelerator for run_standard_model(); no model flag required.
            dataset_index_dict["full_dataset"] = s_state.dataset

        try:
            res = s_state.model_fn(
                rates=rates_f, dataset=ds_f, date=td,
                type=calc_type, var=calc_var, param=calc_param,
                dataset_index=dataset_index_dict,
            )
            res, _ = _extract_detail(res)
            return res or {}
        except Exception as _e:
            import traceback as _tb
            log(f"   fill {td} t={calc_type} v={calc_var}: {_e}\n{_tb.format_exc()}",
                s_state.NODE_NAME, level="error", force=True)
            return None

    def _sync_compute_chunk(candles_chunk, calc_type, calc_var, calc_param,
                            all_rows, all_dates, np_rates_pd, s_state, rates_tbl=""):
        """Один executor-job обрабатывает пачку свечей вместо job на каждую свечу."""
        return [
            _sync_compute(
                candle=c,
                calc_type=calc_type,
                calc_var=calc_var,
                calc_param=calc_param,
                all_rows=all_rows,
                all_dates=all_dates,
                np_rates_pd=np_rates_pd,
                s_state=s_state,
                rates_tbl=rates_tbl,
            )
            for c in candles_chunk
        ]

    def _standard_fast_apply_var(s_state):
        """Detect the exact simple run_standard_model wrapper automatically."""
        if s_state.USE_ML_VALUES or s_state.batch_model_fn is not None:
            return None
        fn = s_state.model_fn
        code = getattr(fn, "__code__", None)
        if code is None or "run_standard_model" not in set(code.co_names):
            return None
        apply_var = getattr(fn, "__globals__", {}).get("_apply_var")
        if not callable(apply_var):
            return None
        # Custom hooks change semantics; leave them on the generic path.
        try:
            import ast as _ast
            import textwrap as _textwrap
            tree = _ast.parse(_textwrap.dedent(inspect.getsource(fn)))
            calls = [
                node for node in _ast.walk(tree)
                if isinstance(node, _ast.Call)
                and ((isinstance(node.func, _ast.Name) and node.func.id == "run_standard_model")
                     or (isinstance(node.func, _ast.Attribute) and node.func.attr == "run_standard_model"))
            ]
            if len(calls) != 1:
                return None
            allowed = {
                "type", "var", "dataset_index", "shift_window", "apply_var_fn"
            }
            keyword_names = {kw.arg for kw in calls[0].keywords if kw.arg is not None}
            if not keyword_names.issubset(allowed):
                return None
        except Exception:
            # Runtime validation below remains the final safety gate.
            pass
        return apply_var

    def _standard_fast_compute_dates(
        candles_chunk,
        type_var_slots,
        all_rows,
        all_dates,
        np_rates_pd,
        rates_tbl,
        apply_var_fn,
        dataset_midnight,
        s_state,
    ):
        """Compute all requested slots, reusing exact equivalent H1 day states."""
        dataset_index_dict = {
            "dates": s_state.dataset_dates,
            "by_key": s_state.dataset_by_key,
            "key_dates": s_state.dataset_key_dates,
            "key_field": s_state.dataset_key_field,
            "np_rates": np_rates_pd,
            "ctx_index": s_state.ctx_index,
            "url_map": s_state.url_map,
            "dataset_timestamps": getattr(s_state, "_dataset_ts_arr", None),
            "filter_dataset_by_date": bool(s_state.FILTER_DATASET_BY_DATE),
            "is_daily": bool(str(rates_tbl or s_state.RATES_TABLE or "").endswith("_day")),
            "rates_table": rates_tbl or s_state.RATES_TABLE,
            "full_dataset": s_state.dataset,
            "execution_scope": "fill_cache",
        }

        is_hourly_table = not bool(str(rates_tbl or "").endswith("_day"))
        can_group_day = bool(dataset_midnight and is_hourly_table and np_rates_pd is not None)
        group_for_date: dict[datetime, tuple] = {}
        representatives: dict[tuple, dict] = {}

        dn = np_rates_pd.get("dates_ns") if np_rates_pd is not None else None
        opens = np_rates_pd.get("open") if np_rates_pd is not None else None
        closes = np_rates_pd.get("close") if np_rates_pd is not None else None

        for candle in candles_chunk:
            td = candle["date"]
            cut = bisect.bisect_right(all_dates, td)
            if cut <= 0:
                key = ("empty", td)
            elif can_group_day and dn is not None and opens is not None and closes is not None:
                # The standard model's output within an H1 day depends on:
                #   1) midnight-vs-non-midnight (legacy is_daily semantics),
                #   2) current candle bull/bear (selects max/min extrema),
                # while all enriched events are fixed at midnight.
                np_cut = int(np.searchsorted(dn, int(td.timestamp()), side="right"))
                if np_cut <= 0:
                    key = ("empty", td)
                else:
                    last_dt = all_rows[cut - 1]["date"]
                    legacy_daily = last_dt.hour == 0 and last_dt.minute == 0
                    is_bull = float(closes[np_cut - 1]) > float(opens[np_cut - 1])
                    key = (td.date(), legacy_daily, is_bull)
            else:
                key = ("exact", td)
            group_for_date[td] = key
            representatives.setdefault(key, candle)

        group_results: dict[tuple, dict | None] = {}
        for key, candle in representatives.items():
            td = candle["date"]
            cut = bisect.bisect_right(all_dates, td)
            rates_f = _list_view(all_rows, 0, cut)
            try:
                group_results[key] = _run_standard_model_multi_slots(
                    rates_f,
                    s_state.dataset,
                    td,
                    slots=type_var_slots,
                    dataset_index=dataset_index_dict,
                    shift_window=s_state.SHIFT_WINDOW,
                    apply_var_fn=apply_var_fn,
                )
            except Exception as exc:
                import traceback as _tb
                log(
                    f"   standard fast batch {td}: {exc}\n{_tb.format_exc()}",
                    s_state.NODE_NAME,
                    level="error",
                    force=True,
                )
                group_results[key] = None

        return {
            candle["date"]: group_results.get(group_for_date[candle["date"]])
            for candle in candles_chunk
        }, len(representatives)

    async def _try_fill_standard_fast(
        pair_id,
        day_flag,
        candles,
        type_var_slots,
        cached_by_hash,
        all_rows,
        all_dates,
        np_rates_pd,
        rates_tbl,
        batch_size,
    ):
        """Automatic fused fill for standard services; returns handled + counters."""
        if not type_var_slots:
            return False, 0, 0, 0
        _ordered_model, _ = _ordered_fill_control(s)
        if _ordered_model:
            return False, 0, 0, 0
        apply_var_fn = _standard_fast_apply_var(s)
        if apply_var_fn is None or not s.model_needs_index:
            return False, 0, 0, 0

        slot_meta = []
        for calc_type, var in type_var_slots:
            extra = {"type": calc_type, "var": var, "param": ""}
            p_hash = _params_hash(extra)
            slot_meta.append((
                (calc_type, var),
                p_hash,
                _json.dumps(extra, ensure_ascii=False),
                cached_by_hash.get(p_hash, set()),
            ))

        skipped_inc = 0
        pending = []
        for candle in candles:
            dv = candle["date"]
            missing_any = False
            for _slot, _ph, _pj, cached in slot_meta:
                if dv in cached:
                    skipped_inc += 1
                else:
                    missing_any = True
            if missing_any:
                pending.append(candle)

        if not pending:
            return True, 0, skipped_inc, 0

        dataset_midnight = _standard_dataset_is_midnight(s.dataset)

        # Safety gate: compare the fused implementation with the actual model
        # on representative first/middle/last dates before using it for a fill.
        sample_idx = sorted(set((0, len(pending) // 2, len(pending) - 1)))
        sample = [pending[i] for i in sample_idx]
        fast_sample, _ = _standard_fast_compute_dates(
            sample,
            type_var_slots,
            all_rows,
            all_dates,
            np_rates_pd,
            rates_tbl,
            apply_var_fn,
            dataset_midnight,
            s,
        )
        for candle in sample:
            td = candle["date"]
            slot_results = fast_sample.get(td)
            if slot_results is None:
                return False, 0, 0, 0
            for calc_type, var in type_var_slots:
                expected = _sync_compute(
                    candle=candle,
                    calc_type=calc_type,
                    calc_var=var,
                    calc_param="",
                    all_rows=all_rows,
                    all_dates=all_dates,
                    np_rates_pd=np_rates_pd,
                    s_state=s,
                    rates_tbl=rates_tbl,
                )
                actual = slot_results.get((calc_type, var))
                if expected != actual:
                    log(
                        f"  [pair{pair_id}/{'d' if day_flag else 'h'}] "
                        f"standard fast validation mismatch at {td} "
                        f"t={calc_type} v={var}; generic fallback",
                        s.NODE_NAME,
                        level="warning",
                        force=True,
                    )
                    return False, 0, 0, 0

        done_inc = 0
        errors_inc = 0
        groups_total = 0
        eff_batch = max(int(batch_size or 0), 5000)
        cache_table = s.cache_table

        for i in range(0, len(pending), eff_batch):
            if s.fill_cancel.is_set():
                break
            chunk = pending[i:i + eff_batch]
            by_date, groups_count = _standard_fast_compute_dates(
                chunk,
                type_var_slots,
                all_rows,
                all_dates,
                np_rates_pd,
                rates_tbl,
                apply_var_fn,
                dataset_midnight,
                s,
            )
            groups_total += groups_count

            insert_rows = []
            # The same group dictionary is shared by all equivalent H1 dates;
            # serialize each distinct result/slot only once.
            encoded_cache: dict[tuple[int, tuple[int, int]], str] = {}
            for candle in chunk:
                dv = candle["date"]
                slot_results = by_date.get(dv)
                for slot, p_hash, params_json, cached in slot_meta:
                    if dv in cached:
                        continue
                    done_inc += 1
                    result = None if slot_results is None else slot_results.get(slot)
                    if result is None:
                        errors_inc += 1
                        continue
                    enc_key = (id(result), slot)
                    result_json = encoded_cache.get(enc_key)
                    if result_json is None:
                        result_json = _rj_encode(result)
                        encoded_cache[enc_key] = result_json
                    insert_rows.append({
                        "url": s.service_url,
                        "pair": pair_id,
                        "day": day_flag,
                        "dv": dv,
                        "ph": p_hash,
                        "pj": params_json,
                        "rj": result_json,
                    })

            if insert_rows:
                try:
                    async with s.engine_cache.begin() as conn:
                        for j in range(0, len(insert_rows), 5000):
                            await conn.execute(text(f"""
                                INSERT IGNORE INTO `{cache_table}`
                                    (service_url, pair, day_flag, date_val,
                                     params_hash, params_json, result_json)
                                VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
                            """), insert_rows[j:j + 5000])
                except Exception as exc:
                    log(
                        f"   standard fast bulk insert: {exc}",
                        s.NODE_NAME,
                        level="warning",
                    )

            log(
                f"  [pair{pair_id}/{'d' if day_flag else 'h'} fast] "
                f"processed={min(i + len(chunk), len(pending))}/{len(pending)} "
                f"groups={groups_total} err={errors_inc}",
                s.NODE_NAME,
                force=True,
            )

        log(
            f"  [pair{pair_id}/{'d' if day_flag else 'h'}] "
            f"standard fused path: {len(pending)} dates -> {groups_total} states, "
            f"slots={len(type_var_slots)}",
            s.NODE_NAME,
            force=True,
        )
        return True, done_inc, skipped_inc, errors_inc

    # ── _prewarm_ml_active_cache ──────────────────────────────────────────────
    #
    # Суть оптимизации:
    #   maybe_retrain() при каждом вызове проходит по историческим экстремумам
    #   и для каждого вызывает _active_codes_at(ext_dt) → model(rates[:idx]).
    #   Это та же O(N²) проблема, что и в основном цикле.
    #
    #   Если batch_model доступен — вычисляем активные коды для ВСЕХ экстремумов
    #   одним вызовом (O(N)) и кладём в _ml_active_cache.
    #   После этого каждый _active_codes_at внутри maybe_retrain = O(1) lookup.

    async def _prewarm_ml_active_cache(
        pair_id: int,
        day_flag: int,
        type_var_slots: list,
        dt_from: datetime | None = None,
        dt_to: datetime | None = None,
    ) -> int:
        """
        Прогревает _ml_active_cache для всех экстремумов пары/таймфрейма.
        Возвращает количество прогретых записей.

        ИСПРАВЛЕНИЕ: ext_dates вычисляются ВНУТРИ цикла по calc_var, т.к. каждый
        интервал (3/5/7/9) даёт разный набор экстремумов. Предыдущая версия
        вычисляла ext_dates один раз с interval=3 и использовала их для всех
        calc_var — для var=5/7/9 кеш был бесполезен, каждый active_codes_at
        запускал model() (~5ms), 50 вызовов × 5ms = 250ms на каждую свечу.

        ИСПРАВЛЕНИЕ 2 (double free): _get_all_extremums возвращает ВСЕ экстремумы
        за полную историю np_r (до 119k свечей), порождая сотни тысяч записей в
        _ml_active_cache (574k для pair1/h) и вызывая OOM → glibc heap corruption.
        Теперь фильтруем ext_dates по окну fill_cache + 90-дневный lookback-буфер.
        """
        if s.batch_model_fn is None:
            return 0

        tbl    = _rates_table(pair_id, day_flag)
        np_r   = s.np_rates.get(tbl)
        rows   = s.global_rates.get(tbl, [])
        if not rows or np_r is None:
            return 0

        dataset_index_dict = None
        if s.model_needs_index:
            dataset_index_dict = {
                "dates":              s.dataset_dates,
                "by_key":             s.dataset_by_key,
                "key_dates":          s.dataset_key_dates,
                "key_field":          s.dataset_key_field,
                "np_rates":           np_r,
                "ctx_index":          s.ctx_index,
                "url_map":            s.url_map,
                "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
                "filter_dataset_by_date": bool(s.FILTER_DATASET_BY_DATE),
                "is_daily": bool(day_flag),
                "rates_table": tbl,
            }
            dataset_index_dict["full_dataset"] = s.dataset

        # Группируем слоты по calc_var — один batch_model call на интервал
        # (train_mode не влияет на набор дат экстремумов, только на обучение).
        from itertools import groupby as _groupby
        slots_by_var: dict[int, list[int]] = {}
        slots_by_var: dict[tuple[int, str], list[int]] = {}
        for calc_type, calc_var, calc_param in type_var_slots:
            slots_by_var.setdefault((calc_var, calc_param), []).append(calc_type)

        warmed = 0

        for (calc_var, calc_param), calc_types in slots_by_var.items():
            if s.fill_cancel.is_set():
                break

            # ── Получаем экстремумы ДЛЯ ДАННОГО ИНТЕРВАЛА через reverse_learning ──
            # np_r["ext_max"] вычислен с interval≈3 (одна свеча слева/справа).
            # Для calc_var=5/7/9 это неверный набор дат.
            # _get_all_extremums кеширует результат: O(1) для повторных вызовов.
            try:
                all_ext, _ = rl._get_all_extremums(np_r, interval=calc_var)
                # FIX: фильтруем по окну fill_cache + 90-дневный lookback-буфер.
                # collect_extremums_back с limit=50 и interval=9 смотрит назад
                # не более ~450 свечей ≈ 18 дней ч/б. 90 дней с запасом.
                if dt_from is not None:
                    _lb_ts  = int((dt_from - timedelta(days=90)).timestamp())
                    _end_ts = int((dt_to or datetime.now()).timestamp()) + 86400
                    ext_dates = [
                        datetime.fromtimestamp(t)
                        for t, _ in all_ext
                        if _lb_ts <= t <= _end_ts
                    ]
                else:
                    ext_dates = [datetime.fromtimestamp(t) for t, _ in all_ext]
            except Exception:
                # Fallback: если rl недоступен — используем np_r["ext_max"] (interval≈3)
                ext_max = np_r.get("ext_max")
                ext_min = np_r.get("ext_min")
                ext_dates_raw: list[datetime] = []
                if ext_max is not None:
                    ext_dates_raw += [rows[i]["date"] for i, v in enumerate(ext_max) if v]
                if ext_min is not None:
                    ext_dates_raw += [rows[i]["date"] for i, v in enumerate(ext_min) if v]
                # Fallback тоже фильтруем
                if dt_from is not None:
                    _lb  = dt_from - timedelta(days=90)
                    _end = dt_to or datetime.now()
                    ext_dates_raw = [d for d in ext_dates_raw if _lb <= d <= _end + timedelta(days=1)]
                ext_dates = sorted(set(ext_dates_raw))

            if not ext_dates:
                continue

            # ── Для каждого train_mode прогреваем кеш ────────────────────────
            for calc_type in calc_types:
                if s.fill_cancel.is_set():
                    break

                missing = [
                    d for d in ext_dates
                    if (pair_id, day_flag, int(d.timestamp()), calc_type, calc_var, calc_param)
                       not in s._ml_active_cache
                ]
                if not missing:
                    continue

                try:
                    # batch_model за один вызов — O(N) вместо O(N²).
                    rates_for_prewarm = rows if s.model_uses_rate_history else [{"date": missing[-1]}]
                    dataset_for_prewarm = s.dataset if s.model_can_filter_dataset_by_date else _filter_dataset_lte(missing[-1], s)
                    batch_results = s.batch_model_fn(
                        rates=rates_for_prewarm,
                        dataset=dataset_for_prewarm,
                        dates=missing,
                        type=calc_type, var=calc_var, param=calc_param,
                        dataset_index=dataset_index_dict,
                    )
                    for ext_dt, result in batch_results.items():
                        key = (pair_id, day_flag, int(ext_dt.timestamp()),
                               calc_type, calc_var, calc_param)
                        s._ml_active_cache[key] = list(result.keys())
                        warmed += 1
                    # FIX: страховочный лимит на _ml_active_cache (без него нет eviction)
                    if len(s._ml_active_cache) > _ML_ACTIVE_CACHE_MAX:
                        _evict = len(s._ml_active_cache) - _ML_ACTIVE_CACHE_MAX
                        for _k in list(s._ml_active_cache.keys())[:_evict]:
                            del s._ml_active_cache[_k]
                except Exception as _e:
                    log(f"   prewarm pair{pair_id}/{'d' if day_flag else 'h'} "
                        f"t={calc_type} v={calc_var}: {_e}",
                        s.NODE_NAME, level="warning")

        return warmed

    # ── _fill_worker ──────────────────────────────────────────────────────────

    async def _fill_worker(pairs, days, date_from_str, date_to_str, types, params, batch_size):
        if not s.cache_writer:
            s.fill_status = {
                "state": "error",
                "error": "fill_cache разрешён только на Brain 1",
                "cache_role": s.cache_role,
            }
            return
        s.fill_cancel.clear()
        s._fill_cache_active = True   # skip vlad_reverse_universe writes for speed
        # Preparation also needs cleanup protection.  Failures here happen
        # before the main processing loop (for example while refreshing rates),
        # so the loop's finally block cannot reset _fill_cache_active for us.
        try:
            # Clear ML universe cache so fill_cache starts fresh
            if s.reverse_store:
                s.reverse_store.clear_universe_cache()
            dt_from = _parse_date(date_from_str) if date_from_str else None
            dt_to   = _parse_date(date_to_str)   if date_to_str   else None

            # Build one consistent data snapshot for the whole fill.  The previous
            # ML path refreshed rates before every batch and repeatedly hit MySQL.
            await _refresh_simple_rates(s)

            pd_slots       = [(p, d) for p in pairs for d in days]
            type_var_slots = [(tp, var, param) for tp in types for var in s.VAR_RANGE for param in params]
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
                f"{len(type_var_slots)} type/var/param слотов", s.NODE_NAME, force=True)
        except Exception:
            s._fill_cache_active = False
            raise

        try:
            for slot_idx, (pair_id, day_flag) in enumerate(pd_slots):
                if s.fill_cancel.is_set():
                    break

                tbl = _rates_table(pair_id, day_flag)
                await _refresh_rates(tbl, s)
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
                    f"{len(type_var_slots)} type/var/param слотов", s.NODE_NAME, force=True)

                # OPT-1: один prefetch всех cached_dates для данного инструмента
                # вместо N отдельных SELECT на каждый (calc_type, var) слот.
                _tbl_c = s.cache_table
                cached_by_hash: dict[str, set] = {}
                try:
                    _wanted_hashes = [
                        _params_hash({"type": ct, "var": vv, "param": prm})
                        for ct, vv, prm in type_var_slots
                    ]
                    _where_ch = ["service_url=:url", "pair=:pair", "day_flag=:day"]
                    _params_ch = {"url": s.service_url, "pair": pair_id, "day": day_flag}
                    if dt_from is not None:
                        _where_ch.append("date_val>=:df")
                        _params_ch["df"] = dt_from
                    if dt_to is not None:
                        _where_ch.append("date_val<=:dt")
                        _params_ch["dt"] = dt_to
                    if _wanted_hashes:
                        _ph_marks = []
                        for _i, _ph in enumerate(_wanted_hashes):
                            _name = f"ph{_i}"
                            _ph_marks.append(f":{_name}")
                            _params_ch[_name] = _ph
                        _where_ch.append(f"params_hash IN ({','.join(_ph_marks)})")
                    async with s.engine_cache.connect() as conn:
                        _res_ch = await conn.execute(text(f"""
                            SELECT params_hash, date_val FROM `{_tbl_c}`
                            WHERE {' AND '.join(_where_ch)}
                        """), _params_ch)
                        for _ph, _dv in _res_ch.fetchall():
                            cached_by_hash.setdefault(_ph, set()).add(_dv)
                    log(f"  [{instr_label}] cached prefetch: "
                        f"{sum(len(v) for v in cached_by_hash.values())} строк "
                        f"по {len(cached_by_hash)} hash-слотам",
                        s.NODE_NAME, force=True)
                except Exception as _ce:
                    log(f"  [{instr_label}] cached prefetch failed: {_ce}",
                        s.NODE_NAME, level="warning")

                _ordered_model, _ordered_reset_fn = _ordered_fill_control(s)
                if _ordered_model:
                    log(
                        f"  [{instr_label}] stateful model detected: "
                        "strict date/slot order enabled",
                        s.NODE_NAME,
                        force=True,
                    )

                # AUTO-3/4/5: exact fused path for the standard wrappers used by
                # services 53, 56 and 62-70. Detection and result validation are automatic;
                # custom models continue through the generic implementation below.
                _fast_slots = [(t, v) for t, v, prm in type_var_slots] if all(prm == "" for _, _, prm in type_var_slots) else []
                _fast_handled, _fast_done, _fast_skipped, _fast_errors = (
                    await _try_fill_standard_fast(
                        pair_id, day_flag, candles, _fast_slots, cached_by_hash,
                        all_rows, all_dates_pd, np_rates_pd, tbl, batch_size,
                    )
                )
                if _fast_handled:
                    done += _fast_done
                    skipped += _fast_skipped
                    errors += _fast_errors
                    s.fill_status.update({
                        "done": done, "skipped": skipped, "errors": errors,
                        "slots_done": slot_idx + 1,
                    })
                    continue

                # ── Прогрев ML-кеша через batch_model ────────────────────────────
                # Если модель имеет batch_model и USE_ML_VALUES=True — вычисляем
                # активные коды для всех экстремумов одним вызовом.
                # maybe_retrain() будет получать _active_codes_at из кеша (O(1))
                # вместо пересчёта срезов rates на каждый экстремум.
                if s.USE_ML_VALUES and s.batch_model_fn is not None:
                    warmed = await _prewarm_ml_active_cache(
                        pair_id, day_flag, type_var_slots,
                        dt_from=dt_from, dt_to=dt_to,
                    )
                    if warmed:
                        log(f"  [{instr_label}] ML-кеш прогрет: {warmed} экстремумов",
                            s.NODE_NAME, force=True)

                # OPT-2: параллельный запуск type_var_slots через asyncio.gather
                # (только для не-ML пути — ML-обучение state-dependent, нельзя параллелить).
                # Счётчики done/skipped/errors защищены asyncio-lock (однопоточный event loop).
                _slot_lock = asyncio.Lock()
                _ml_compute_sem = asyncio.Semaphore(max(1, s.FILL_ML_WORKERS))

                async def _fill_one_slot(calc_type: int, var: int, calc_param: str) -> None:
                    nonlocal done, skipped, errors

                    extra       = {"type": calc_type, "var": var, "param": calc_param}
                    p_hash      = _params_hash(extra)
                    params_json = _json.dumps(extra, ensure_ascii=False)

                    # OPT-1: используем prefetch вместо отдельного SELECT
                    cached = cached_by_hash.get(p_hash, set())

                    if _ordered_model:
                        # Stateful models must replay the complete ascending timeline,
                        # including already cached dates, to reconstruct their state.
                        # Existing rows are not reinserted below.
                        if _ordered_reset_fn is not None:
                            try:
                                _ordered_reset_fn(pair_id, day_flag, calc_type, var)
                            except Exception as _reset_exc:
                                log(
                                    f"   state reset t={calc_type} v={var}: {_reset_exc}",
                                    s.NODE_NAME, level="warning", force=True,
                                )
                        to_fetch = sorted(candles, key=lambda c: c["date"])
                        skipped_local = sum(1 for c in candles if c["date"] in cached)
                    else:
                        to_fetch = [c for c in candles if c["date"] not in cached]
                        skipped_local = len(candles) - len(to_fetch)
                    async with _slot_lock:
                        skipped += skipped_local

                    # OPT-5: для batch_model один большой батч выгоднее
                    # (стоимость вызова константа, нет смысла дробить).
                    _eff_bs = (len(to_fetch)
                               if s.batch_model_fn is not None and not s.USE_ML_VALUES
                               else (max(batch_size, s.FILL_ML_BATCH_SIZE)
                                     if s.USE_ML_VALUES else batch_size)) or batch_size
                    # Exact extremum-state results can safely cross batch boundaries.
                    _ml_state_results: dict[tuple, dict | None] = {}
                    for i in range(0, len(to_fetch), _eff_bs):
                        if s.fill_cancel.is_set():
                            break
                        batch = to_fetch[i:i + _eff_bs]

                        if s.USE_ML_VALUES:
                            # OPT-ML-BATCH: соседние свечи обычно имеют один и тот же
                            # набор последних N экстремумов. Для таких свечей результат
                            # train_at_date() идентичен, поэтому считаем ML только один
                            # раз на уникальный seq_tuple и размножаем universe по датам.
                            results = [None] * len(batch)
                            try:
                                # Exact incremental-by-extremum-state path:
                                # train once per unique extremum sequence, then copy
                                # that universe to all candles that have the same
                                # training state. This preserves answers because
                                # train_at_date()/maybe_retrain() depend on control_date
                                # only through this exact sequence of extrema.
                                _batch_dates = [c["date"] for c in batch]
                                grouped, _seq_by_key = rl.group_control_dates_by_extremum_state(
                                    np_rates_pd or s.np_simple_rates,
                                    _batch_dates,
                                    train_mode=calc_type,
                                    extremum_limit=s.ML_EXTREMUM_LIMIT,
                                    extremum_interval=var,
                                )

                                if len(grouped) < len(batch):
                                    log(
                                        f"  [{instr_label} t={calc_type}/v={var}] "
                                        f"ML groups: {len(batch)} candles -> {len(grouped)} seq",
                                        s.NODE_NAME, force=True,
                                    )

                                async def _compute_ml_state(_state_key, _indices):
                                    if _state_key in _ml_state_results:
                                        return
                                    if s.fill_cancel.is_set():
                                        _ml_state_results[_state_key] = None
                                        return
                                    _rep = batch[_indices[0]]
                                    async with _ml_compute_sem:
                                        try:
                                            _result = await _call_model(
                                                pair_id, day_flag,
                                                _rep["date"].strftime("%Y-%m-%d %H:%M:%S"),
                                                calc_type=calc_type, calc_var=var, param=calc_param,
                                                _skip_refresh=True,
                                            )
                                        except Exception as _e:
                                            import traceback as _tb
                                            log(
                                                f"   ml-fill {_rep['date']} t={calc_type} v={var}: "
                                                f"{_e}\n{_tb.format_exc()}",
                                                s.NODE_NAME, level="error", force=True,
                                            )
                                            _result = None
                                    _ml_state_results[_state_key] = _result

                                await asyncio.gather(*(
                                    _compute_ml_state(_state_key, _indices)
                                    for _state_key, _indices in grouped.items()
                                ))
                                for _state_key, _indices in grouped.items():
                                    _result = _ml_state_results.get(_state_key)
                                    for _idx in _indices:
                                        results[_idx] = _result
                            except Exception as _e:
                                # Safety fallback: if grouping fails for any unexpected
                                # edge case, preserve old per-candle behavior.
                                import traceback as _tb
                                log(
                                    f"   ml-group-fill fallback t={calc_type} v={var}: "
                                    f"{_e}\n{_tb.format_exc()}",
                                    s.NODE_NAME, level="warning", force=True,
                                )
                                results = []
                                for candle in batch:
                                    try:
                                        result = await _call_model(
                                            pair_id, day_flag,
                                            candle["date"].strftime("%Y-%m-%d %H:%M:%S"),
                                            calc_type=calc_type, calc_var=var, param=calc_param,
                                            _skip_refresh=True,
                                        )
                                    except Exception as _e2:
                                        import traceback as _tb2
                                        log(
                                            f"   ml-fill {candle['date']} t={calc_type} v={var}: "
                                            f"{_e2}\n{_tb2.format_exc()}",
                                            s.NODE_NAME, level="error", force=True,
                                        )
                                        result = None
                                    results.append(result)
                        elif s.batch_model_fn is not None:
                            # ОПТИМИЗАЦИЯ: batch_model вычисляет траекторию ОДИН РАЗ
                            # для всего ряда (O(N)), а не O(N²) через per-candle вызовы.
                            dataset_index_dict_b = None
                            if s.model_needs_index:
                                dataset_index_dict_b = {
                                    "dates": s.dataset_dates,
                                    "by_key": s.dataset_by_key,
                                    "key_dates": s.dataset_key_dates,
                                    "key_field": s.dataset_key_field,
                                    "np_rates": np_rates_pd,
                                    "ctx_index": s.ctx_index,
                                    "url_map": s.url_map,
                                    "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
                                    "filter_dataset_by_date": bool(s.FILTER_DATASET_BY_DATE),
                                    "is_daily": bool(day_flag),
                                }
                                dataset_index_dict_b["full_dataset"] = s.dataset
                            try:
                                batch_dates = [c["date"] for c in batch]
                                # Полный срез all_rows до последней целевой даты.
                                max_batch_date = batch_dates[-1]
                                idx_end = bisect.bisect_right(all_dates_pd, max_batch_date)
                                rates_for_batch = (_list_view(all_rows, 0, idx_end)
                                                   if s.model_uses_rate_history
                                                   else [{"date": max_batch_date}])
                                dataset_f_b = s.dataset if s.model_can_filter_dataset_by_date else _filter_dataset_lte(max_batch_date, s)

                                batch_map = s.batch_model_fn(
                                    rates=rates_for_batch,
                                    dataset=dataset_f_b,
                                    dates=batch_dates,
                                    type=calc_type, var=var, param=calc_param,
                                    dataset_index=dataset_index_dict_b,
                                )
                                results = [batch_map.get(c["date"]) for c in batch]
                            except Exception as _e:
                                import traceback as _tb
                                log(f"   batch_model t={calc_type} v={var}: {_e}\n{_tb.format_exc()}",
                                    s.NODE_NAME, level="error", force=True)
                                results = [None] * len(batch)
                        elif _ordered_model:
                            # One executor job, one ascending sequence.  Splitting this
                            # batch into workers would make stateful output depend on
                            # thread scheduling rather than candle chronology.
                            loop = asyncio.get_running_loop()
                            results = await loop.run_in_executor(
                                _FILL_EXECUTOR,
                                _sync_compute_chunk,
                                batch, calc_type, var, calc_param,
                                all_rows, all_dates_pd, np_rates_pd, s, tbl,
                            )
                        else:
                            # Обычный model(): дробим батч максимум на число worker-ов.
                            # Раньше создавался Future на КАЖДУЮ свечу; на лёгких моделях
                            # диспетчеризация могла занимать больше времени, чем model().
                            loop = asyncio.get_running_loop()
                            workers = min(
                                max(1, getattr(_FILL_EXECUTOR, "_max_workers", 1)),
                                len(batch),
                            )
                            chunk_len = max(1, (len(batch) + workers - 1) // workers)
                            chunks = [batch[j:j + chunk_len]
                                      for j in range(0, len(batch), chunk_len)]
                            futs = [
                                loop.run_in_executor(
                                    _FILL_EXECUTOR,
                                    _sync_compute_chunk,
                                    chunk, calc_type, var, calc_param,
                                    all_rows, all_dates_pd, np_rates_pd, s, tbl,
                                )
                                for chunk in chunks
                            ]
                            chunk_results = await asyncio.gather(*futs)
                            results = [item for part in chunk_results for item in part]

                        insert_rows = []
                        batch_done = sum(
                            1 for candle in batch
                            if not (_ordered_model and candle["date"] in cached)
                        )
                        for candle, result in zip(batch, results):
                            already_cached = _ordered_model and candle["date"] in cached
                            if result is None:
                                # A failure while replaying a cached date is still
                                # relevant because subsequent state may be affected.
                                errors += 1
                                continue
                            if already_cached:
                                continue
                            insert_rows.append({
                                "url":  s.service_url, "pair": pair_id,
                                "day":  day_flag,      "dv":   candle["date"],
                                "ph":   p_hash,        "pj":   params_json,
                                "rj":   _rj_encode(result),           # OPT-6: zlib+b64
                            })
                        if insert_rows:
                            try:
                                async with s.engine_cache.begin() as conn:
                                    await conn.execute(text(f"""
                                        INSERT IGNORE INTO `{_tbl_c}`
                                            (service_url, pair, day_flag, date_val,
                                             params_hash, params_json, result_json)
                                        VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
                                    """), insert_rows)
                            except Exception as e:
                                log(f"   bulk insert: {e}", s.NODE_NAME, level="warning")
                        async with _slot_lock:
                            done += batch_done
                            s.fill_status.update({"done": done, "skipped": skipped, "errors": errors})
                        log(f"  [{instr_label} t={calc_type}/v={var}/p={calc_param!r}] "
                            f"{done}/{total} err={errors}", s.NODE_NAME, force=True)

                # ── Запуск слотов ─────────────────────────────────────────────────
                # ML-путь: последовательно (обучение зависит от предыдущего состояния).
                # Остальные: параллельно через gather.
                if s.USE_ML_VALUES or _ordered_model:
                    for calc_type, var, calc_param in type_var_slots:
                        if s.fill_cancel.is_set():
                            break
                        await _fill_one_slot(calc_type, var, calc_param)
                else:
                    # asyncio.gather запускает все слоты параллельно.
                    # DB-записи в разные params_hash — конфликтов нет.
                    await asyncio.gather(*[
                        _fill_one_slot(ct, v, prm)
                        for ct, v, prm in type_var_slots
                        if not s.fill_cancel.is_set()
                    ])

                s.fill_status["slots_done"] = slot_idx + 1

        finally:
            # Guaranteed cleanup — runs even if an exception aborts the loop.
            # Without this, _fill_cache_active stays True forever after a crash,
            # causing all subsequent /values calls to also use skip_db_writes=True.
            s._fill_cache_active = False

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
                for bt_type, bt_var, bt_param in type_var_slots:
                    try:
                        bt = await _backtest(
                            bt_pair, bt_day, tier=0,
                            extra_params={
                                "type": bt_type, "var": bt_var, "param": bt_param,
                            },
                            df=bt_df, dt=bt_dt,
                        )
                        label = (
                            f"pair{bt_pair}/{'d' if bt_day else 'h'} "
                            f"t={bt_type} v={bt_var} p={bt_param!r}"
                        )
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
            email    = s.DEVELOPER_EMAIL,
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

        async with s.engine_cache.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT date_val, result_json FROM `{s.cache_table}`
                WHERE service_url=:url AND pair=:pair AND day_flag=:day
                  AND params_hash=:ph AND date_val>=:df AND date_val<=:dt
                ORDER BY date_val
            """), {"url": s.service_url, "pair": pair, "day": day,
                   "ph": p_hash, "df": df, "dt": dt})
            cache_map = {row[0]: _rj_decode(row[1]) for row in res.fetchall()}  # OPT-6
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
        for eng in (s.engine_vlad, s.engine_brain, s.engine_super, s.engine_cache):
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
                "param_range":        s.PARAM_RANGE,
                "np_built":           s.np_built,
                "simple_rates":       len(s.simple_rates),
                "cache_role":         s.cache_role,
                "cache_storage":      "SUPER_*",
                "cache_table":        s.cache_table,
                "cache_upstream":     s.cache_upstream_url if not s.cache_writer else None,
                "last_reload":        s.last_reload.isoformat() if s.last_reload else None,
                "last_rebuild":       s.last_rebuild.isoformat() if s.last_rebuild else None,
                "rebuild_auto":       s.REBUILD_INTERVAL > 0 and bool(
                    s.index_builder_fn or s.weight_builder_fn
                    or s.enrich_fn or s.ENRICHED_TABLE),
                "rebuild_interval_s": s.REBUILD_INTERVAL,
                "enriched_table":     s.ENRICHED_TABLE,
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
        _cache_hop: int = Query(0, ge=0, le=1, include_in_schema=False),
    ):
        if s.TYPES_RANGE and type not in s.TYPES_RANGE:
            return err_response(f"type={type} не входит в TYPES_RANGE={s.TYPES_RANGE}")
        if s.VAR_RANGE and var not in s.VAR_RANGE:
            return err_response(f"var={var} не входит в VAR_RANGE={s.VAR_RANGE}")
        try:
            if not s.cache_writer and _cache_hop:
                return err_response(
                    "Central cache proxy loop detected: upstream service is not Brain 1"
                )
            resp = await cached_values(
                engine_vlad=s.engine_cache, service_url=s.service_url,
                pair=pair, day=day, date=date,
                extra_params={"type": type, "var": var, "param": param},
                compute_fn=lambda: _call_model(pair, day, date,
                                               calc_type=type, calc_var=var, param=param),
                node=s.NODE_NAME, table_name=s.cache_table,
                compute_on_miss=bool(s.cache_writer),
                miss_fn=(None if s.cache_writer else lambda: _proxy_values_to_brain1(
                    s, pair=pair, day=day, date=date, calc_type=type,
                    calc_var=var, param=param,
                )),
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
        if not s.cache_writer:
            return err_response(
                "compute_batch разрешён только на Brain 1; дочерняя нода работает cache-only"
            )
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
                          date_from: str, date_to: str, batch_size: int,
                          param: str | None = None):
        if not s.cache_writer:
            return err_response(
                "fill_cache запрещён на дочерней ноде. Запустите его на Brain 1; "
                "дочерние ноды читают общий кеш из SUPER_* и проксируют MISS."
            )
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
        all_params = [param] if param is not None else list(s.PARAM_RANGE or [""])
        eff_from   = date_from if date_from.strip() else s.CACHE_DATE_FROM
        s.fill_cancel.clear()

        async def _run_fill():
            try:
                await _fill_worker(
                    pl, dl, eff_from, date_to, all_types, all_params, batch_size
                )
            except Exception as exc:
                import traceback as _tb
                s.fill_status.update({
                    "state": "error",
                    "error": repr(exc),
                    "finished_at": datetime.now().isoformat(),
                })
                log(
                    f" fill_cache crashed: {exc}\n{_tb.format_exc()}",
                    s.NODE_NAME, level="error", force=True,
                )

        s.fill_task = asyncio.create_task(
            _run_fill())
        return ok_response({
            "started":     True, "pairs": pl, "days": dl,
            "types":       all_types, "var_range": s.VAR_RANGE,
            "param_range": all_params,
            "batch_size":  batch_size, "date_from": eff_from,
            "date_to":     date_to or "now",
            "slots_total": len(pl) * len(dl) * len(all_types) * len(s.VAR_RANGE) * len(all_params),
        })

    @app.get("/fill_cache")
    async def ep_fill_cache(
        pairs: str = Query("1,3,4"), days: str = Query("0,1"),
        date_from: str = Query(""), date_to: str = Query(""),
        batch_size: int = Query(300),
        param: str | None = Query(None, description="Один param; если не указан, используется PARAM_RANGE"),
    ):
        return await _start_fill(pairs, days, date_from, date_to, batch_size, param)

    @app.get("/fill_cache_day")
    async def ep_fill_cache_day(
        pairs: str = Query("1,3,4"),
        date_from: str = Query(""), date_to: str = Query(""),
        batch_size: int = Query(300),
        param: str | None = Query(None, description="Один param; если не указан, используется PARAM_RANGE"),
    ):
        return await _start_fill(pairs, "1", date_from, date_to, batch_size, param)

    @app.get("/fill_cache_hour")
    async def ep_fill_cache_hour(
        pairs: str = Query("1,3,4"),
        date_from: str = Query(""), date_to: str = Query(""),
        batch_size: int = Query(300),
        param: str | None = Query(None, description="Один param; если не указан, используется PARAM_RANGE"),
    ):
        return await _start_fill(pairs, "0", date_from, date_to, batch_size, param)

    @app.get("/fill_status")
    async def ep_fill_status():
        return ok_response(s.fill_status)

    @app.get("/fill_stop")
    async def ep_fill_stop():
        s.fill_cancel.set()
        return ok_response({"stopped": True})

    @app.get("/clear_cache")
    async def ep_clear_cache(
        pairs:        str  = Query("", description="Пары через запятую: 1,3,4 — пусто = все"),
        days:         str  = Query("", description="Таймфреймы: 0=hour,1=day — пусто = все"),
        date_from:    str  = Query("", description="От даты YYYY-MM-DD — пусто = без ограничения"),
        date_to:      str  = Query("", description="До даты YYYY-MM-DD — пусто = без ограничения"),
        types:        str  = Query("", description="type через запятую — пусто = все"),
        vars_:        str  = Query("", alias="vars", description="var через запятую — пусто = все"),
        also_backtest: bool = Query(False, description="Также очистить vlad_backtest_results"),
        stop_fill:    bool  = Query(True,  description="Остановить fill_cache если запущен"),
    ):
        """
        Очищает кеш текущего сервиса (таблица vlad_values_cache_svcPORT).

        Параметры позволяют точечно удалить только нужный срез:
            /clear_cache                        — весь кеш сервиса
            /clear_cache?pairs=1&days=0         — только EUR/USD hour
            /clear_cache?date_from=2025-01-01   — с определённой даты
            /clear_cache?types=0&vars=0,1       — конкретные type/var слоты
            /clear_cache?also_backtest=true     — + vlad_backtest_results
        """
        if not s.cache_writer:
            return err_response(
                "clear_cache разрешён только на Brain 1; общий кеш находится в SUPER_*"
            )
        # ── Остановить fill если запущен ──────────────────────────────────────
        was_running = False
        if stop_fill and s.fill_status.get("state") == "running":
            s.fill_cancel.set()
            was_running = True
            # Даём воркеру секунду завершить текущий батч
            await asyncio.sleep(1.0)

        # ── Парсим фильтры ────────────────────────────────────────────────────
        pair_list  = [int(p.strip()) for p in pairs.split(",")  if p.strip().isdigit()]
        day_list   = [int(d.strip()) for d in days.split(",")   if d.strip().isdigit()]
        type_list  = [int(t.strip()) for t in types.split(",")  if t.strip().isdigit()]
        var_list   = [int(v.strip()) for v in vars_.split(",")  if v.strip().isdigit()]

        dt_from = _parse_date(date_from) if date_from.strip() else None
        dt_to   = _parse_date(date_to)   if date_to.strip()   else None

        if dt_from and dt_to and dt_from > dt_to:
            return err_response("date_from не может быть позже date_to")

        # ── Строим WHERE для cache-таблицы ────────────────────────────────────
        def _build_where(extra_params: dict) -> tuple[str, dict]:
            clauses = ["service_url = :svc_url"]
            params  = {"svc_url": s.service_url, **extra_params}

            if pair_list:
                placeholders = ", ".join(f":pair{i}" for i in range(len(pair_list)))
                clauses.append(f"pair IN ({placeholders})")
                for i, p in enumerate(pair_list):
                    params[f"pair{i}"] = p

            if day_list:
                placeholders = ", ".join(f":day{i}" for i in range(len(day_list)))
                clauses.append(f"day_flag IN ({placeholders})")
                for i, d in enumerate(day_list):
                    params[f"day{i}"] = d

            if dt_from:
                clauses.append("date_val >= :dt_from")
                params["dt_from"] = dt_from
            if dt_to:
                clauses.append("date_val <= :dt_to")
                params["dt_to"] = dt_to

            # Фильтр по type/var через params_hash: пересчитываем хэши
            if type_list or var_list:
                t_range = type_list or s.TYPES_RANGE
                v_range = var_list  or s.VAR_RANGE
                hashes  = [
                    _params_hash({"type": t, "var": v, "param": ""})
                    for t in t_range for v in v_range
                ]
                placeholders = ", ".join(f":ph{i}" for i in range(len(hashes)))
                clauses.append(f"params_hash IN ({placeholders})")
                for i, h in enumerate(hashes):
                    params[f"ph{i}"] = h

            return " AND ".join(clauses), params

        deleted_cache     = 0
        deleted_backtest  = 0
        errors: list[str] = []

        # ── Удаляем из cache-таблицы ──────────────────────────────────────────
        try:
            where, params = _build_where({})
            async with s.engine_cache.begin() as conn:
                res = await conn.execute(
                    text(f"DELETE FROM `{s.cache_table}` WHERE {where}"), params
                )
                deleted_cache = res.rowcount
        except Exception as e:
            errors.append(f"cache: {e}")
            log(f"   clear_cache error: {e}", s.NODE_NAME, level="error")

        # ── Опционально — backtest results ────────────────────────────────────
        if also_backtest:
            try:
                # vlad_backtest_results фильтруется аналогично, без date_val
                clauses = ["service_url = :svc_url"]
                params2: dict = {"svc_url": s.service_url}
                if pair_list:
                    pls = ", ".join(f":pair{i}" for i in range(len(pair_list)))
                    clauses.append(f"pair IN ({pls})")
                    for i, p in enumerate(pair_list):
                        params2[f"pair{i}"] = p
                if day_list:
                    pls = ", ".join(f":day{i}" for i in range(len(day_list)))
                    clauses.append(f"day_flag IN ({pls})")
                    for i, d in enumerate(day_list):
                        params2[f"day{i}"] = d
                if type_list or var_list:
                    t_range = type_list or s.TYPES_RANGE
                    v_range = var_list  or s.VAR_RANGE
                    hashes  = [
                        _params_hash({"type": t, "var": v, "param": ""})
                        for t in t_range for v in v_range
                    ]
                    pls = ", ".join(f":ph{i}" for i in range(len(hashes)))
                    clauses.append(f"params_hash IN ({pls})")
                    for i, h in enumerate(hashes):
                        params2[f"ph{i}"] = h
                where2 = " AND ".join(clauses)
                async with s.engine_vlad.begin() as conn:
                    res = await conn.execute(
                        text(f"DELETE FROM vlad_backtest_results WHERE {where2}"), params2
                    )
                    deleted_backtest = res.rowcount
            except Exception as e:
                errors.append(f"backtest: {e}")
                log(f"   clear_cache backtest error: {e}", s.NODE_NAME, level="error")

        # ── Сбрасываем ML-кеш если чистим всё ────────────────────────────────
        if not pair_list and not day_list and not dt_from and not dt_to \
                and not type_list and not var_list:
            s._ml_active_cache.clear()
            if s.reverse_store:
                s.reverse_store.clear_universe_cache()

        log(
            f" clear_cache: удалено {deleted_cache} записей кеша"
            + (f", {deleted_backtest} backtest" if also_backtest else "")
            + (f" | фильтры: pairs={pair_list or 'all'} days={day_list or 'all'}"
               f" dates=[{dt_from or '*'} → {dt_to or '*'}]"
               f" types={type_list or 'all'} vars={var_list or 'all'}" if any(
                   [pair_list, day_list, dt_from, dt_to, type_list, var_list]) else " (полная очистка)"),
            s.NODE_NAME, force=True,
        )

        payload: dict = {
            "service_url":      s.service_url,
            "cache_table":      s.cache_table,
            "deleted_cache":    deleted_cache,
            "deleted_backtest": deleted_backtest,
            "fill_was_running": was_running,
            "filters": {
                "pairs":     pair_list  or "all",
                "days":      day_list   or "all",
                "date_from": dt_from.isoformat() if dt_from else None,
                "date_to":   dt_to.isoformat()   if dt_to   else None,
                "types":     type_list  or "all",
                "vars":      var_list   or "all",
            },
        }
        if errors:
            payload["errors"] = errors
        return ok_response(payload)

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
        if (not s.index_builder_fn and not s.weight_builder_fn
                and not s.enrich_fn and not s.ENRICHED_TABLE):
            return err_response(
                "Rebuild не настроен. "
                "Добавь context_idx.py + weights.py (старый стиль) "
                "или enrich_dataset() + ENRICHED_TABLE в model.py (новый стиль).")
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

    @app.get("/audit/future_leak")
    async def ep_audit_future_leak(
        pair: int = 1,
        day: int = 0,
        date: str = "",
        type: int = 0,
        var: int = 0,
        param: str = "",
    ):
        """Metamorphic no-future-data audit for one model invocation.

        The output must not change when data unavailable at the prediction time is
        replaced by sentinels. The audit checks separately:
          1) t1 at the target date and all later dates;
          2) all rate arrays strictly after the target date;
          3) dataset rows strictly after the target date.
        """
        import copy as _copy
        try:
            table = _rates_table(pair, day)
            await _refresh_rates(table, s)
            rows_all = s.global_rates.get(table, [])
            if len(rows_all) < 3:
                return err_response("Not enough rate rows for audit")

            target = _parse_date(date) if date else rows_all[-2]["date"]
            if target is None:
                return err_response("Invalid date")

            rates_base = _filter_rates_lte(table, target, s) if s.model_uses_rate_history else [{"date": target}]
            dataset_base = s.dataset if s.model_can_filter_dataset_by_date else _filter_dataset_lte(target, s)

            def _strict_dataset_index(np_r, dataset_rows):
                if not s.model_needs_index:
                    return None
                strict_rows = [e for e in dataset_rows if e.get("date") is not None and e["date"] <= target]
                strict_rows.sort(key=lambda e: e["date"])
                by_key = {}
                key_field = s.dataset_key_field
                if key_field:
                    for e in strict_rows:
                        k = e.get(key_field)
                        if k is not None:
                            by_key.setdefault(k, []).append(e)
                return {
                    "dates": [e["date"] for e in strict_rows],
                    "by_key": by_key,
                    "key_dates": {k: [e["date"] for e in v] for k, v in by_key.items()},
                    "key_field": key_field,
                    "np_rates": np_r,
                    "ctx_index": s.ctx_index,
                    "url_map": s.url_map,
                    "dataset_timestamps": None,
                    "filter_dataset_by_date": True,
                    "dataset_cutoff_ts": float(target.timestamp()),
                    "is_daily": bool(day),
                    "rates_table": table,
                    "execution_scope": "future_leak_audit",
                    "full_dataset": strict_rows,
                }

            def _call_direct(rates_x, dataset_x, np_r):
                result = s.model_fn(
                    rates=rates_x,
                    dataset=dataset_x,
                    date=target,
                    type=type,
                    var=var,
                    param=param,
                    dataset_index=_strict_dataset_index(np_r, dataset_x),
                )
                result, _ = _extract_detail(result)
                return result or {}

            np_base = s.np_rates.get(table)
            baseline = _call_direct(rates_base, dataset_base, np_base)

            # Test 1: t1 on target and future dates must be irrelevant.
            np_t1 = None
            if np_base is not None:
                np_t1 = {k: (v.copy() if hasattr(v, "copy") else _copy.deepcopy(v)) for k, v in np_base.items()}
                dn = np_t1.get("dates_ns")
                arr = np_t1.get("t1")
                if dn is not None and arr is not None:
                    mask = dn >= int(target.timestamp())
                    arr[mask] = 987654321.123
            rates_t1 = [_copy.deepcopy(r) for r in rates_base]
            for r in rates_t1:
                if r.get("date") is not None and r["date"] >= target:
                    r["t1"] = 987654321.123
            changed_t1 = _call_direct(rates_t1, dataset_base, np_t1)

            # Test 2: future OHLC/range arrays must be irrelevant.
            np_future = None
            if np_base is not None:
                np_future = {k: (v.copy() if hasattr(v, "copy") else _copy.deepcopy(v)) for k, v in np_base.items()}
                dn = np_future.get("dates_ns")
                if dn is not None:
                    mask = dn > int(target.timestamp())
                    for key in ("open", "close", "max", "min", "t1", "ranges", "ext_max", "ext_min"):
                        arr = np_future.get(key)
                        if arr is not None and hasattr(arr, "__setitem__"):
                            try:
                                arr[mask] = 123456789.0
                            except Exception:
                                pass
            changed_future_rates = _call_direct(rates_base, dataset_base, np_future)

            # Test 3: future dataset rows must be irrelevant. Strict snapshot removes them.
            strict_dataset = _filter_dataset_lte(target, s)
            strict_dataset_result = _call_direct(rates_base, strict_dataset, np_base)

            def _same(a, b, tol=1e-9):
                if set(a) != set(b):
                    return False
                for k in a:
                    try:
                        if abs(float(a[k]) - float(b[k])) > tol:
                            return False
                    except Exception:
                        if a[k] != b[k]:
                            return False
                return True

            tests = {
                "target_and_future_t1_invariant": _same(baseline, changed_t1),
                "future_rate_arrays_invariant": _same(baseline, changed_future_rates),
                "future_dataset_rows_invariant": _same(baseline, strict_dataset_result),
            }
            return ok_response({
                "service_id": s.SERVICE_ID,
                "pair": pair,
                "day": day,
                "date": target.strftime("%Y-%m-%d %H:%M:%S"),
                "type": type,
                "var": var,
                "tests": tests,
                "passed": all(tests.values()),
                "baseline_keys": len(baseline),
                "differences": {
                    "t1": None if tests["target_and_future_t1_invariant"] else {"baseline": baseline, "mutated": changed_t1},
                    "future_rates": None if tests["future_rate_arrays_invariant"] else {"baseline": baseline, "mutated": changed_future_rates},
                    "future_dataset": None if tests["future_dataset_rows_invariant"] else {"baseline": baseline, "strict_snapshot": strict_dataset_result},
                },
            })
        except Exception as e:
            return err_response(f"future leak audit failed: {e}")

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
            _np2 = s.np_rates.get(s.RATES_TABLE) or s.np_simple_rates
            dataset_index_dict2 = {
                "dates": s.dataset_dates,
                "by_key": s.dataset_by_key,
                "key_dates": s.dataset_key_dates,
                "key_field": s.dataset_key_field,
                "np_rates": _np2,
                "ctx_index": s.ctx_index,
                "url_map": s.url_map,
                "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
                "full_dataset": s.dataset,
                "rates_table": s.RATES_TABLE,
                "execution_scope": "pretest",
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

                # ── пул дат: только те, где модель уже «прогрелась» ──────
                # Требуем минимум _MIN_CTX3 свечей контекста до тестовой даты,
                # чтобы не попадать на "холодный старт" начала истории.
                _MIN_CTX3 = 50
                _pool3 = [
                    d for i, d in enumerate(_dts3)
                    if d >= _pt3_date_start and i >= _MIN_CTX3
                ]
                # Фолбэк: мало данных — берём последние 75% дат от date_start
                if len(_pool3) < 10:
                    _base3 = [d for d in _dts3 if d >= _pt3_date_start] or list(_dts3)
                    _pool3 = _base3[max(0, len(_base3) // 4):]

                # ── стратифицированный сэмплинг ───────────────────────────
                # Делим пул на 10 временны́х бакетов, берём 1 дату из каждого.
                # Гарантирует равномерное покрытие всей истории и убирает
                # нестабильность результатов между запусками.
                _n3 = min(10, len(_pool3))
                if _n3 == 0:
                    continue
                if len(_pool3) >= _n3 * 2:
                    _bsz3 = len(_pool3) // _n3
                    _sample3 = sorted([
                        random.choice(_pool3[_i3 * _bsz3: (_i3 + 1) * _bsz3])
                        for _i3 in range(_n3)
                    ])
                else:
                    _sample3 = sorted(
                        random.sample(_pool3, _n3) if len(_pool3) >= _n3 else _pool3
                    )
                # ─────────────────────────────────────────────────────────

                log(f"  [Тест 3] pair{_pid3}/{_tf3}: {len(_sample3)} дат "
                    f"(от {_sample3[0].date() if _sample3 else '—'} "
                    f"до {_sample3[-1].date() if _sample3 else '—'})...",
                    s.NODE_NAME, force=True)

                for _td3 in _sample3:
                    _rf3 = _rows3[:bisect.bisect_right(_dts3, _td3)]
                    _ds3 = _filter_dataset_lte(_td3, s)

                    dataset_index_dict3 = None
                    if s.model_needs_index:
                        _np3 = s.np_rates.get(_tbl3) or s.np_simple_rates
                        dataset_index_dict3 = {
                            "dates": s.dataset_dates,
                            "by_key": s.dataset_by_key,
                            "key_dates": s.dataset_key_dates,
                            "key_field": s.dataset_key_field,
                            "np_rates": _np3,
                            "ctx_index": s.ctx_index,
                            "url_map": s.url_map,
                            "dataset_timestamps": getattr(s, "_dataset_ts_arr", None),
                            "full_dataset": s.dataset,
                            "rates_table": _tbl3,
                        }

                    def _mk3(_r=_rf3, _d=_ds3, _t=_td3):
                        res, _ = _extract_detail(
                            s.model_fn(
                                rates=_r, dataset=_d, date=_t,
                                type=0, var=s.VAR_RANGE[0], param="",
                                dataset_index=dataset_index_dict3,
                            )
                        )
                        # PRETEST_ALLOW_EMPTY=True: модель объявила, что {} —
                        # валидный ответ (напр. стратегия с состоянием FLAT).
                        # Считаем успехом любой dict, включая пустой.
                        # PRETEST_ALLOW_EMPTY=False (умолчание): обычный
                        # микросервис обязан возвращать непустой dict — {}
                        # считается провалом.
                        _allow_empty = getattr(model_module, "PRETEST_ALLOW_EMPTY", False)
                        if _allow_empty:
                            return res is not None and isinstance(res, dict)
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
                f"{_ne3}/{_tot3} ({_cov3:.0%}) без ошибки, порог 90%",
                s.NODE_NAME, force=True)
            if not _ok3:
                _failures3.append(f"pair{_pid3}/{_tf3}: {_ne3}/{_tot3} ({_cov3:.0%}) < 90%")

        if _failures3:
            log(f" PRETEST FAILED: {_failures3}", s.NODE_NAME, force=True)
            return {"status": "error",
                    "error": f"[Тест 3 — Покрытие] {' | '.join(_failures3)}"}

        # ── Тест 4: rebuild_index ─────────────────────────────────────────────
        _has_any_rebuild4 = (
                s.index_builder_fn is not None or s.weight_builder_fn is not None
                or s.enrich_fn is not None or bool(s.ENRICHED_TABLE)
        )
        if _has_any_rebuild4:
            log("  [Тест 4] проверяем rebuild_index...", s.NODE_NAME, force=True)
            try:
                _rb4 = await _do_rebuild()
                if "error" in _rb4:
                    log(f" Тест 4: rebuild вернул ошибку: {_rb4['error']}",
                        s.NODE_NAME, force=True)
                    return {"status": "error",
                            "error": f"[Тест 4 — Rebuild] {_rb4['error']}"}

                if s.ENRICHED_TABLE:
                    _idx_table = f"{s.ENRICHED_TABLE}_indexes"
                    try:
                        async with s.engine_vlad.connect() as _conn4:
                            _cnt4 = (await _conn4.execute(
                                text(f"SELECT COUNT(*) FROM `{_idx_table}`")
                            )).scalar()
                        if not _cnt4:
                            return {"status": "error",
                                    "error": f"[Тест 4 — Rebuild] {_idx_table} пуста после rebuild"}
                        log(f"   Тест 4: {_idx_table} — {_cnt4} строк",
                            s.NODE_NAME, force=True)
                    except Exception as _e4chk:
                        return {"status": "error",
                                "error": f"[Тест 4 — Rebuild] проверка {_idx_table}: {_e4chk}"}

                log(f"   Тест 4: rebuild OK — "
                    f"ctx={_rb4.get('ctx_total')} "
                    f"weights={_rb4.get('weights_total')} "
                    f"enrich={_rb4.get('enrich')} "
                    f"indexer={_rb4.get('dataset_indexer')}",
                    s.NODE_NAME, force=True)
            except Exception as _e4:
                log(f" Тест 4: exception: {_e4}", s.NODE_NAME, force=True)
                return {"status": "error", "error": f"[Тест 4 — Rebuild] {_e4}"}
        else:
            log("   Тест 4: rebuild не настроен, пропуск", s.NODE_NAME, force=True)

        log(" PRETEST OK", s.NODE_NAME, force=True)
        return {"status": "ok"}

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
                async with s.engine_cache.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT MIN(JSON_LENGTH(result_json)),
                               MAX(JSON_LENGTH(result_json)),
                               AVG(JSON_LENGTH(result_json))
                        FROM `{_tbl}`
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND result_json NOT IN ('{{}}', 'z:eJyrrgUAAXUA+Q==')  -- OPT-6 sentinels
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
                async with s.engine_cache.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT result_json FROM `{_tbl}`
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND result_json NOT IN ('{{}}', 'z:eJyrrgUAAXUA+Q==')  -- OPT-6 sentinels
                          AND (:df IS NULL OR date_val >= :df)
                          AND (:dt IS NULL OR date_val <= :dt)
                    """), url_p)
                    plus_cnt = minus_cnt = 0
                    for (rj_str,) in res:
                        try:
                            for v in _rj_decode(rj_str).values():  # OPT-6
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
                async with s.engine_cache.connect() as conn:
                    res = await conn.execute(text(f"""
                        SELECT date_val, result_json FROM `{_tbl}`
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND (:df IS NULL OR date_val >= :df)
                          AND (:dt IS NULL OR date_val <= :dt)
                        ORDER BY date_val
                    """), url_p)
                    for (dt_val, rj_str) in res:
                        try:
                            rj = _rj_decode(rj_str)               # OPT-6
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


    # ══════════════════════════════════════════════════════════════════════════
    # CREATE SCORE — формула отбора моделей из backtest + posttest
    # ══════════════════════════════════════════════════════════════════════════

    def _rank_norm_desc(values: dict[int, float | None]) -> dict[int, float]:
        """
        Нормированный ранг: худший=0.0, лучший=1.0.
        Значения с None/NaN/Inf получают 0.0. При равенстве — средний ранг.
        """
        valid: list[tuple[int, float]] = []
        for k, v in values.items():
            if v is None:
                continue
            try:
                fv = float(v)
            except Exception:
                continue
            if math.isnan(fv) or math.isinf(fv):
                continue
            valid.append((k, fv))

        out = {k: 0.0 for k in values.keys()}
        n = len(valid)
        if n == 0:
            return out
        if n == 1:
            out[valid[0][0]] = 1.0
            return out

        # ascending: минимальное значение получает 0, максимальное — 1
        valid.sort(key=lambda x: x[1])
        i = 0
        while i < n:
            j = i
            while j + 1 < n and valid[j + 1][1] == valid[i][1]:
                j += 1
            avg_pos = (i + j) / 2.0
            rank = avg_pos / (n - 1)
            for pos in range(i, j + 1):
                out[valid[pos][0]] = round(float(rank), 4)
            i = j + 1
        return out

    async def _create_score_post_history(pair_id: int, day_flag: int,
                                         dt_from: datetime | None,
                                         dt_to: datetime | None) -> dict:
        """
        Лёгкая версия posttest.history для /create_score.
        Использует уже заполненный кеш, как и /posttest.
        """
        tf_name = "day" if day_flag else "hour"
        table = _rates_table(pair_id, day_flag)
        rows = [r for r in s.global_rates.get(table, [])
                if (dt_from is None or r["date"] >= dt_from)
                and (dt_to is None or r["date"] <= dt_to)]

        cache_signals: dict = {}
        try:
            async with s.engine_cache.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT date_val, result_json FROM `{s.cache_table}`
                    WHERE service_url=:url AND pair=:pair AND day_flag=:day
                      AND (:df IS NULL OR date_val >= :df)
                      AND (:dt IS NULL OR date_val <= :dt)
                    ORDER BY date_val
                """), {"url": s.service_url, "pair": pair_id, "day": day_flag,
                       "df": dt_from, "dt": dt_to})
                for dt_val, rj_str in res:
                    try:
                        rj = _rj_decode(rj_str)                    # OPT-6
                        cache_signals[dt_val] = sum(rj.values()) if rj else 0.0
                    except Exception:
                        cache_signals[dt_val] = 0.0
        except Exception as e:
            log(f"   create_score posttest {pair_id}/{tf_name}: {e}",
                s.NODE_NAME, level="warning")

        def _simulate():
            equity = 10_000.0
            total_profit = 0.0
            total_dropdown = 0.0
            wins = trades = 0
            position = None
            direction = entry_price = 0.0

            for i, r in enumerate(rows):
                signal = cache_signals.get(r["date"], 0.0)
                if i + 1 >= len(rows):
                    continue
                op = rows[i + 1]["open"]
                if not op:
                    continue

                if position is not None:
                    should_close = (
                        signal == 0.0
                        or (signal > 0 and direction < 0)
                        or (signal < 0 and direction > 0)
                    )
                    if should_close:
                        pnl = equity * 0.10 * (op - entry_price) / entry_price * direction
                        equity += pnl
                        trades += 1
                        if pnl >= 0:
                            total_profit += pnl
                            wins += 1
                        else:
                            total_dropdown += abs(pnl)
                        position = None

                if signal != 0.0 and position is None:
                    direction = 1.0 if signal > 0 else -1.0
                    entry_price = op
                    position = (direction, entry_price)

            if position is not None and rows:
                lp = rows[-1]["close"]
                if lp and entry_price:
                    pnl = equity * 0.10 * (lp - entry_price) / entry_price * direction
                    equity += pnl
                    trades += 1
                    if pnl >= 0:
                        total_profit += pnl
                        wins += 1
                    else:
                        total_dropdown += abs(pnl)

            cw = round(wins / trades, 4) if trades > 0 else 0.0
            return {
                "profit": round(total_profit, 2),
                "dropdown": round(total_dropdown, 2),
                "cw": cw,
                "result": round(total_profit - total_dropdown, 2),
                "trade_count": trades,
            }

        return await asyncio.to_thread(_simulate)

    async def _create_score_best_backtest(pair_id: int, day_flag: int, tier: int,
                                          calc_type: int,
                                          vars_to_run: list[int],
                                          df: datetime, dt: datetime,
                                          run_backtest: bool) -> dict:
        """Возвращает лучший backtest по value_score для выбранного timeframe."""
        results: list[dict] = []

        if run_backtest:
            for v in vars_to_run:
                params = {"type": calc_type, "var": v, "param": ""}
                try:
                    bt = await _backtest(pair_id, day_flag, tier, params, df, dt)
                except Exception as e:
                    bt = {"error": str(e), "params": params}
                if "error" not in bt:
                    results.append(bt)
        else:
            try:
                async with s.engine_vlad.connect() as conn:
                    res = await conn.execute(text("""
                        SELECT value_score, accuracy, trade_count, params_json
                        FROM vlad_backtest_results
                        WHERE service_url=:url AND pair=:pair AND day_flag=:day
                          AND tier=:tier AND date_from=:df AND date_to=:dt
                        ORDER BY value_score DESC
                    """), {"url": s.service_url, "pair": pair_id, "day": day_flag,
                           "tier": tier, "df": df, "dt": dt})
                    for r in res.mappings().all():
                        params = _json.loads(r["params_json"]) if r["params_json"] else {}
                        if vars_to_run and params.get("var") not in vars_to_run:
                            continue
                        if params.get("type", calc_type) != calc_type:
                            continue
                        results.append({
                            "value_score": float(r["value_score"]),
                            "accuracy": float(r["accuracy"]),
                            "trade_count": int(r["trade_count"]),
                            "params": params,
                            "skipped": True,
                        })
            except Exception as e:
                return {"error": str(e)}

        if not results:
            tf_name = "day" if day_flag else "hour"
            return {"error": f"no valid {tf_name} backtest"}
        return max(results, key=lambda x: float(x.get("value_score", -1e100)))

    @app.get("/create_score")
    async def ep_create_score(
        pairs: str = Query("1,3,4"),
        days: str = Query("0,1"),
        tier: int = Query(0, ge=0, le=1),
        date_from: str = Query(""), date_to: str = Query(""),
        type: int = Query(0), var: int = Query(-1),
        run_backtest: bool = Query(True),
        accept_score: float = Query(0.60),
        watch_score: float = Query(0.30),
        min_day_trades: int = Query(20, ge=0),
        min_hour_trades: int = Query(200, ge=0),
    ):
        """
        Считает оценку создания модели отдельно для day и hour в одном endpoint.

        DayScore/HourScore = 0.75 * rank(backtest value_score)
                           + 0.25 * rank(posttest result)

        Важно: day и hour НЕ смешиваются. У каждого timeframe свои ранги,
        свой score, своё решение create/watch/reject.
        """
        try:
            pl = [int(p.strip()) for p in pairs.split(",") if p.strip()]
            dl = [int(d.strip()) for d in days.split(",") if d.strip()]
        except ValueError:
            return err_response("pairs и days — числа через запятую")
        if not pl:
            return err_response("pairs не может быть пустым")
        if not dl:
            return err_response("days не может быть пустым")

        # Оставляем только допустимые таймфреймы и сохраняем порядок без дублей.
        clean_days: list[int] = []
        for d in dl:
            if d not in (0, 1):
                return err_response("days поддерживает только 0=hour и 1=day")
            if d not in clean_days:
                clean_days.append(d)
        dl = clean_days

        df = _parse_date(date_from) if date_from.strip() else _parse_date(s.CACHE_DATE_FROM)
        dt = _parse_date(date_to) if date_to.strip() else datetime.now()
        if df is None or dt is None:
            return err_response("Invalid date format")

        vars_to_run = (s.VAR_RANGE if var == -1 else [var]) or [0]
        pair_labels = {1: "EUR/USD", 3: "BTC/USD", 4: "ETH/USD"}
        tf_labels = {0: "hour", 1: "day"}

        raw: dict[int, dict[int, dict]] = {}
        for pair_id in pl:
            raw[pair_id] = {}
            for day_flag in dl:
                bt_best, post_tf = await asyncio.gather(
                    _create_score_best_backtest(
                        pair_id, day_flag, tier, type, vars_to_run, df, dt, run_backtest
                    ),
                    _create_score_post_history(pair_id, day_flag, df, dt),
                )
                raw[pair_id][day_flag] = {
                    "pair": pair_id,
                    "pair_label": pair_labels.get(pair_id, str(pair_id)),
                    "timeframe": tf_labels[day_flag],
                    "day_flag": day_flag,
                    "backtest": bt_best,
                    "posttest": post_tf,
                    "value_score": (
                        float(bt_best["value_score"]) if "value_score" in bt_best else None
                    ),
                    "posttest_result": float(post_tf.get("result", 0.0)),
                }

        results: dict[str, dict] = {str(pair_id): {
            "pair": pair_id,
            "pair_label": pair_labels.get(pair_id, str(pair_id)),
        } for pair_id in pl}
        ranking: dict[str, list[dict]] = {}
        selected: dict[str, list[int]] = {}

        for day_flag in dl:
            tf_name = tf_labels[day_flag]
            min_trades = min_day_trades if day_flag == 1 else min_hour_trades

            v_rank = _rank_norm_desc({
                pair_id: raw[pair_id][day_flag]["value_score"] for pair_id in pl
            })
            p_rank = _rank_norm_desc({
                pair_id: raw[pair_id][day_flag]["posttest_result"] for pair_id in pl
            })

            ranking[tf_name] = []
            selected[tf_name] = []

            for pair_id in pl:
                item_raw = raw[pair_id][day_flag]
                bt = item_raw["backtest"]
                post = item_raw["posttest"]

                score = round(
                    0.75 * v_rank.get(pair_id, 0.0)
                    + 0.25 * p_rank.get(pair_id, 0.0),
                    4,
                )

                reasons: list[str] = []
                value_score = item_raw["value_score"]
                bt_trade_count = bt.get("trade_count") if isinstance(bt, dict) else None
                post_trade_count = int(post.get("trade_count", 0) or 0)
                post_result = item_raw["posttest_result"]

                hard_reject = False
                if value_score is None:
                    reasons.append(f"нет валидного backtest {tf_name}")
                    hard_reject = True
                if bt_trade_count is not None and int(bt_trade_count) < min_trades:
                    reasons.append(
                        f"мало сделок backtest {tf_name}: {int(bt_trade_count)} < {min_trades}"
                    )
                    hard_reject = True
                if post_trade_count == 0:
                    reasons.append(f"posttest {tf_name} без сделок")
                if post_result < 0:
                    reasons.append(f"posttest {tf_name} отрицательный")

                if hard_reject:
                    decision = "reject"
                elif score >= accept_score:
                    decision = "create"
                    selected[tf_name].append(pair_id)
                elif score >= watch_score:
                    decision = "watch"
                else:
                    decision = "reject"

                tf_item = {
                    "timeframe": tf_name,
                    "day_flag": day_flag,
                    "score": score,
                    "decision": decision,
                    "reasons": reasons,
                    "metrics": {
                        "value_score": value_score,
                        "best_backtest_params": bt.get("params") if isinstance(bt, dict) else None,
                        "backtest_accuracy": bt.get("accuracy") if isinstance(bt, dict) else None,
                        "backtest_trade_count": bt_trade_count,
                        "posttest_result": post_result,
                        "posttest_cw": post.get("cw"),
                        "posttest_trades": post_trade_count,
                    },
                    "ranks": {
                        "V_rank": v_rank.get(pair_id, 0.0),
                        "P_rank": p_rank.get(pair_id, 0.0),
                    },
                }

                results[str(pair_id)][tf_name] = tf_item
                ranking[tf_name].append({
                    "pair": pair_id,
                    "pair_label": results[str(pair_id)]["pair_label"],
                    "score": score,
                    "decision": decision,
                })

            ranking[tf_name].sort(key=lambda x: x["score"], reverse=True)

        return ok_response({
            "formula": {
                "day": "DayScore = 0.75*V_day_rank + 0.25*P_day_rank",
                "hour": "HourScore = 0.75*V_hour_rank + 0.25*P_hour_rank",
            },
            "rank_rule": "ранги считаются отдельно внутри каждого timeframe: худший=0.0, лучший=1.0",
            "thresholds": {
                "create": accept_score,
                "watch": watch_score,
                "min_day_trades": min_day_trades,
                "min_hour_trades": min_hour_trades,
            },
            "pairs": pl,
            "days": dl,
            "tier": tier,
            "type": type,
            "vars": vars_to_run,
            "run_backtest": run_backtest,
            "date_from": df.isoformat(),
            "date_to": dt.isoformat(),
            "selected": selected,
            "ranking": ranking,
            "results": results,
        })

    return app
