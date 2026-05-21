"""
model.py — brain-calendar-multibar-weights  (brain_calendar edition)
╔══════════════════════════════════════════════════════════════════════════════╗
║  КЛЮЧЕВАЯ ИДЕЯ: мультибаровый взвешенный анализ                             ║
║                                                                              ║
║  Сервис 42 читал готовые forecast_dir/surprise_dir/revision_dir из          ║
║  brain_calendar_mb_events и использовал ctx_id как ключ датасета.           ║
║                                                                              ║
║  Этот сервис читает brain_calendar напрямую и вычисляет направления         ║
║  на лету по тем же правилам что builder context_idx.py:                     ║
║    forecast_dir  = BEAT/MISS/INLINE/UNKNOWN  (actual vs forecast)           ║
║    surprise_dir  = UP/DOWN/FLAT/UNKNOWN      (actual vs previous)           ║
║    revision_dir  = UP/DOWN/FLAT/NONE/UNKNOWN (oldPrevious vs previous)      ║
║                                                                              ║
║  DATASET_KEY = "url" — уникальный ключ события в brain_calendar.            ║
║  ctx_index строится по (event_id, currency, importance, fcd, scd, rcd)      ║
║  через brain_calendar_context_idx точно как в сервисе 42.                   ║
║                                                                              ║
║    score = w₀·t1[T] + w₁·t1[T−1h] + w₂·t1[T−2h] + ...                    ║
║    N = медианное расстояние между вершинами для данного инструмента          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Параметры:
  type=0 → T1-сумма + вероятность экстремума
  type=1 → только взвешенная T1-сумма
  type=2 → только взвешенная вероятность экстремума

  var=0 → все бары, t1, линейные веса
  var=1 → фильтр range > avg_range, потом t1
  var=2 → t1 * |t1| (квадрат со знаком)
  var=3 → фильтр + квадрат
  var=4 → (range − avg_range) для баров > avg_range

Все исправления багов 1–6 из сервиса 42 сохранены.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ОПТИМИЗАЦИИ (числовые результаты бит-идентичны оригиналу):

  1. URL-кэш вне shift-цикла — _compute_dirs, ctx_index, datetime→int64
     вычисляются 1 раз на URL вместо 13 (SHIFT_WINDOW+1). Экономия ~92%.

  2. int64-метки времени вместо datetime в горячем пути — нет создания объектов
     timedelta/datetime внутри циклов, только целочисленная арифметика.

  3. Векторизованные numpy-ядра — весовое окно (window до 12 баров) развёрнуто
     в матричные операции (M, W) вместо Python-цикла на каждый t_center.
     compute_multibar_t1_batch / _multibar_ext_soft_hit_batch /
     _multibar_ext_range_hit_batch обрабатывают все t_centers одним вызовом.

  4. build_ext_sets — один батч-вызов searchsorted вместо N вызовов поштучно.

  5. .searchsorted() метод вместо np.searchsorted() — устраняет _wrapfunc
     dispatch overhead во всех горячих функциях (~1.4ms на 5000 свечей).

  6. Двухуровневый URL-кэш — статический (_url_static_cache в dataset_index)
     переживает между вызовами model() в течение RELOAD_INTERVAL. Динамический
     кэш строится через arr.searchsorted() вместо bisect на datetime-списке.

  7. defaultdict(float) для result — устраняет result.get(wc, 0.0) на каждый hit.

  8. Safe division в векторных ядрах — np.where(w>0, x/safe_w, 0) предотвращает
     RuntimeWarning при делении на 0.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import numpy as np

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ СЕРВИСА
# ══════════════════════════════════════════════════════════════════════════════

NODE_NAME    = os.getenv("NODE_NAME", "brain-calendar-multibar-weights-v2")
SERVICE_TEXT = "Calendar weights · multi-bar adaptive window · brain_calendar"

RATES_TABLE = os.getenv("RATES_TABLE", "brain_rates_eur_usd")

WEIGHTS_TABLE       = os.getenv("WEIGHTS_TABLE", "brain_calendar_weights")
WEIGHTS_CODE_COLUMN = os.getenv("WEIGHTS_CODE_COLUMN", "weight_code")

CTX_TABLE       = os.getenv("CTX_TABLE", "brain_calendar_context_idx")
CTX_QUERY       = """
    SELECT event_id, currency_code, importance,
           forecast_dir, surprise_dir, revision_dir,
           occurrence_count
    FROM brain_calendar_context_idx
"""
CTX_KEY_COLUMNS = [
    "event_id", "currency_code", "importance",
    "forecast_dir", "surprise_dir", "revision_dir",
]

DATASET_ENGINE = os.getenv("DATASET_ENGINE", "brain")
DATASET_TABLE  = os.getenv("DATASET_TABLE", "brain_calendar")
DATASET_QUERY  = f"""
    SELECT
        Url              AS url,
        CurrencyCode     AS currency_code,
        Importance       AS importance,
        ForecastValue    AS forecast_value,
        PreviousValue    AS previous_value,
        OldPreviousValue AS old_previous_value,
        ActualValue      AS actual_value,
        FullDate         AS event_time,
        FullDate         AS date,
        EventType        AS event_type
    FROM {DATASET_TABLE}
    WHERE ActualValue IS NOT NULL
      AND Processed = 1
      AND FullDate IS NOT NULL
      AND Url IS NOT NULL
      AND Url != ''
      AND CurrencyCode IS NOT NULL
      AND (EventType IS NULL OR EventType NOT IN (2))
    ORDER BY FullDate
"""

DATASET_KEY = "url"

FILTER_DATASET_BY_DATE = False

URL_MAP_ENGINE = os.getenv("URL_MAP_ENGINE", "vlad")
URL_MAP_QUERY  = f"""
    SELECT Url AS url, EventId AS event_id
    FROM {DATASET_TABLE}
    WHERE Url IS NOT NULL
      AND Url != ''
      AND EventId IS NOT NULL
    GROUP BY Url, EventId
"""

VAR_RANGE       = [0, 1, 2, 3, 4]
TYPES_RANGE     = [0, 1, 2]
SHIFT_WINDOW    = 12
CACHE_DATE_FROM = os.getenv("CACHE_DATE_FROM", "2025-01-15")

REBUILD_INTERVAL = 86_400
RELOAD_INTERVAL  = 3_600

DIRECTION_THRESHOLD = float(os.getenv("DIRECTION_THRESHOLD", "0.01"))
SKIP_EVENT_TYPES    = {2}
RECURRING_MIN_COUNT = 2

FORECAST_MAP   = {"UNKNOWN": "X", "BEAT": "B", "MISS": "M", "INLINE": "I"}
SURPRISE_MAP   = {"UNKNOWN": "X", "UP": "U", "DOWN": "D", "FLAT": "F"}
REVISION_MAP   = {"NONE": "N", "FLAT": "T", "UP": "U", "DOWN": "D", "UNKNOWN": "X"}
IMPORTANCE_MAP = {"high": "H", "medium": "M", "low": "L", "none": "N"}

WINDOW_MIN_HOUR = 2
WINDOW_MAX_HOUR = 12
WINDOW_MIN_DAY  = 2
WINDOW_MAX_DAY  = 7


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ НАПРАВЛЕНИЙ ИЗ ЧИСЛОВЫХ КОЛОНОК brain_calendar
# ══════════════════════════════════════════════════════════════════════════════

def _safe_float(v: Any) -> float | None:
    """None-safe конвертация в float."""
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def _compute_dirs(row: dict) -> tuple[str, str, str]:
    """
    Вычисляет (forecast_dir, surprise_dir, revision_dir) из числовых колонок
    brain_calendar — точно как builder context_idx.py.
    """
    actual       = _safe_float(row.get("actual_value"))
    forecast     = _safe_float(row.get("forecast_value"))
    previous     = _safe_float(row.get("previous_value"))
    old_previous = _safe_float(row.get("old_previous_value"))
    thr = DIRECTION_THRESHOLD

    if actual is None or forecast is None:
        fcd = "UNKNOWN"
    elif actual > forecast + thr:
        fcd = "BEAT"
    elif actual < forecast - thr:
        fcd = "MISS"
    else:
        fcd = "INLINE"

    if actual is None or previous is None:
        scd = "UNKNOWN"
    elif actual > previous + thr:
        scd = "UP"
    elif actual < previous - thr:
        scd = "DOWN"
    else:
        scd = "FLAT"

    if old_previous is None or previous is None:
        rcd = "NONE"
    elif previous > old_previous + thr:
        rcd = "UP"
    elif previous < old_previous - thr:
        rcd = "DOWN"
    else:
        rcd = "FLAT"

    return fcd, scd, rcd


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def _make_code(event_id, currency, importance, fcd, scd, rcd, mode, shift=None):
    imp_c = IMPORTANCE_MAP.get((importance or "").lower(), "X")
    base  = (f"MB{event_id}_{currency}_{imp_c}"
             f"_{FORECAST_MAP.get(fcd, 'X')}"
             f"_{SURPRISE_MAP.get(scd, 'X')}"
             f"_{REVISION_MAP.get(rcd, 'X')}_{mode}")
    return base if shift is None else f"{base}_{shift}"


def get_linear_weights(n: int) -> list[float]:
    """[N, N-1, ..., 1]. Нормировка — внутри compute-функций."""
    return [float(n - i) for i in range(n)]


def get_adaptive_window(np_r: dict | None, day_flag: int) -> int:
    """
    Медиана расстояний между соседними вершинами (ext_max | ext_min) в барах.
    """
    if np_r is None:
        return 3
    ext_max = np_r.get("ext_max")
    ext_min = np_r.get("ext_min")
    if ext_max is None or ext_min is None or len(ext_max) == 0:
        return 3
    peak_idx = np.where(ext_max | ext_min)[0]
    if len(peak_idx) < 4:
        return 3
    median_gap = int(np.median(np.diff(peak_idx).astype(float)))
    lo, hi = (WINDOW_MIN_DAY, WINDOW_MAX_DAY) if day_flag else (WINDOW_MIN_HOUR, WINDOW_MAX_HOUR)
    return max(lo, min(hi, median_gap))


def get_modification(rates: list[dict]) -> float:
    """Масштабирующий коэффициент по последней цене закрытия."""
    if not rates:
        return 0.001
    last = float(rates[-1].get("close") or 1.0)
    if last > 10_000:
        return 1_000.0
    if last > 500:
        return 100.0
    return 0.001


def build_rates_lookup(rates: list[dict]) -> tuple[dict, dict]:
    """
    Оригинальная функция — сохранена для совместимости.
    t1_map[dt]  = close - open
    rng_map[dt] = max - min
    """
    t1_map : dict[datetime, float] = {}
    rng_map: dict[datetime, float] = {}
    for r in rates:
        dt = r["date"]
        t1_map[dt]  = float(r.get("close") or 0) - float(r.get("open") or 0)
        rng_map[dt] = float(r.get("max")   or 0) - float(r.get("min")  or 0)
    return t1_map, rng_map


def extract_avg_range(np_r: dict | None, rng_map: dict) -> float:
    """avg_range по ВСЕЙ истории (np_rates["ranges"]). Fallback: среднее по срезу."""
    if np_r is not None:
        arr = np_r.get("ranges")
        if arr is not None and len(arr) > 0:
            return float(np.mean(arr))
    return sum(rng_map.values()) / len(rng_map) if rng_map else 0.0


def build_ext_sets(np_r: dict | None, t1_map: dict) -> tuple[set, set]:
    """
    Аналог GLOBAL_EXTREMUMS[table]["max"/"min"].
    ОПТИМИЗИРОВАНО: один батч-вызов np.searchsorted вместо N поштучных.
    """
    ext_max_set: set[datetime] = set()
    ext_min_set: set[datetime] = set()
    if np_r is None:
        return ext_max_set, ext_min_set
    dates_ns     = np_r.get("dates_ns")
    ext_max_mask = np_r.get("ext_max")
    ext_min_mask = np_r.get("ext_min")
    if dates_ns is None or ext_max_mask is None:
        return ext_max_set, ext_min_set

    # ── Было: цикл с N вызовами np.searchsorted ───────────────────────────────
    # ── Стало: один батч-вызов на весь t1_map ─────────────────────────────────
    dts    = list(t1_map.keys())
    ts_arr = np.array([int(dt.timestamp()) for dt in dts], dtype=np.int64)
    n_dn   = len(dates_ns)

    idxs   = dates_ns.searchsorted(ts_arr)
    safe   = np.clip(idxs, 0, n_dn - 1)
    valid  = (idxs < n_dn) & (dates_ns[safe] == ts_arr)

    if valid.any():
        is_max = valid & ext_max_mask[safe]
        is_min = valid & ext_min_mask[safe]
        dts_arr = np.empty(len(dts), dtype=object)
        dts_arr[:] = dts
        if is_max.any():
            ext_max_set = set(dts_arr[is_max].tolist())
        if is_min.any():
            ext_min_set = set(dts_arr[is_min].tolist())

    return ext_max_set, ext_min_set


def prev_candle_is_bull(rates: list[dict], target_dt: datetime) -> bool | None:
    """Аналог find_prev_candle_trend() из оригинала."""
    for r in reversed(rates):
        if r["date"] < target_dt:
            return (r.get("close") or 0) > (r.get("open") or 0)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# NUMPY-ИНФРАСТРУКТУРА ДЛЯ ВЕКТОРИЗОВАННЫХ ВЫЧИСЛЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════


def _build_ext_arrays_np(
    np_r: dict | None,
    rates_sorted_ts: np.ndarray,   # (K,) int64 — уже готов из _build_rates_numpy
) -> tuple[np.ndarray, np.ndarray]:
    """
    Строит (ext_max_sorted_ts, ext_min_sorted_ts) как отсортированные int64-массивы,
    используя уже готовый rates_sorted_ts — БЕЗ дополнительных .timestamp() вызовов.

    Заменяет цепочку build_ext_sets() + _ext_set_to_sorted_ts() полностью:
      до:  N×searchsorted (build_ext_sets) + N×timestamp (ext_set→sorted_ts)
      сейчас: 1×searchsorted, 0 дополнительных timestamp вызовов.
    """
    empty = np.empty(0, dtype=np.int64)
    if np_r is None:
        return empty, empty
    dates_ns     = np_r.get("dates_ns")
    ext_max_mask = np_r.get("ext_max")
    ext_min_mask = np_r.get("ext_min")
    if dates_ns is None or ext_max_mask is None or ext_min_mask is None:
        return empty, empty

    n    = len(dates_ns)
    idxs = dates_ns.searchsorted(rates_sorted_ts)
    safe = np.clip(idxs, 0, n - 1)
    valid = (idxs < n) & (dates_ns[safe] == rates_sorted_ts)

    # rates_sorted_ts уже отсортирован → slicing сохраняет порядок
    ext_max_out = rates_sorted_ts[valid & ext_max_mask[safe]]
    ext_min_out = rates_sorted_ts[valid & ext_min_mask[safe]]
    return ext_max_out, ext_min_out

def _build_rates_numpy(
    rates: list[dict],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Конвертирует список свечей в три отсортированных numpy-массива:
      sorted_ts  — unix-секунды (int64), отсортированные
      t1_arr     — close - open (float64)
      rng_arr    — max - min   (float64)

    Используется как основной источник данных для всех батч-вычислений.
    Обычно rates уже отсортированы фреймворком, но argsort гарантирует порядок.
    """
    n   = len(rates)
    ts_ = np.empty(n, dtype=np.int64)
    t1_ = np.empty(n, dtype=np.float64)
    rn_ = np.empty(n, dtype=np.float64)
    for i, r in enumerate(rates):
        ts_[i] = int(r["date"].timestamp())
        t1_[i] = float(r.get("close") or 0) - float(r.get("open") or 0)
        rn_[i] = float(r.get("max")   or 0) - float(r.get("min")  or 0)
    idx = np.argsort(ts_, kind="stable")
    return ts_[idx], t1_[idx], rn_[idx]


def _ext_set_to_sorted_ts(ext_set: set[datetime]) -> np.ndarray:
    """
    Преобразует set datetime → отсортированный int64-массив unix-секунд.
    Используется для O(log N) батч-проверки принадлежности через searchsorted.
    """
    if not ext_set:
        return np.empty(0, dtype=np.int64)
    return np.array(sorted(int(dt.timestamp()) for dt in ext_set), dtype=np.int64)


def _batch_lookup(
    sorted_ts: np.ndarray,   # (K,) int64, отсортирован
    t1_arr:    np.ndarray,   # (K,) float64
    rng_arr:   np.ndarray,   # (K,) float64
    query_ts:  np.ndarray,   # произвольная форма, int64
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Батч-поиск t1 и rng для массива запросных меток времени.
    Возвращает (t1_out, rng_out, found_mask) той же формы что query_ts.

    Отсутствующие бары: t1=nan, rng=0.0, found=False —
    идентично поведению t1_map.get()/rng_map.get() из оригинала.
    """
    flat  = query_ts.ravel()
    n     = len(sorted_ts)
    idxs  = sorted_ts.searchsorted(flat)
    safe  = np.clip(idxs, 0, n - 1)
    found = (idxs < n) & (sorted_ts[safe] == flat)
    shape = query_ts.shape
    t1_out  = np.where(found, t1_arr[safe],  np.nan).reshape(shape)
    rng_out = np.where(found, rng_arr[safe], 0.0   ).reshape(shape)
    return t1_out, rng_out, found.reshape(shape)


def _batch_in_ext(
    ext_sorted_ts: np.ndarray,  # (E,) int64
    query_ts:      np.ndarray,  # произвольная форма, int64
) -> np.ndarray:                # bool, та же форма
    """Векторная проверка принадлежности меток ext_sorted_ts."""
    if len(ext_sorted_ts) == 0:
        return np.zeros(query_ts.shape, dtype=bool)
    flat  = query_ts.ravel()
    n     = len(ext_sorted_ts)
    idxs  = ext_sorted_ts.searchsorted(flat)
    safe  = np.clip(idxs, 0, n - 1)
    found = (idxs < n) & (ext_sorted_ts[safe] == flat)
    return found.reshape(query_ts.shape)


# ══════════════════════════════════════════════════════════════════════════════
# ВЕКТОРИЗОВАННЫЕ ЯДРА (заменяют скалярные compute_* функции внутри model)
# ══════════════════════════════════════════════════════════════════════════════

def _compute_t1_batch(
    t_centers_ts:  np.ndarray,   # (M,) int64 — все t_centers сразу
    sorted_ts:     np.ndarray,   # (K,) int64
    t1_arr:        np.ndarray,   # (K,)
    rng_arr:       np.ndarray,   # (K,)
    avg_range:     float,
    window:        int,
    weights_np:    np.ndarray,   # (window,) float64
    calc_var:      int,
    bar_delta_sec: int,
) -> np.ndarray:                  # (M,) результаты для каждого t_center
    """
    Векторизованный аналог compute_multibar_t1.

    Строит матрицу барных меток времени (M, W), выполняет батч-поиск
    и считает взвешенную сумму с нормировкой — без Python-цикла по window.
    Численно идентичен оригинальному compute_multibar_t1.
    """
    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4

    # bar_ts[m, i] = t_centers_ts[m] - i * bar_delta_sec
    offsets = np.arange(window, dtype=np.int64) * bar_delta_sec
    bar_ts  = t_centers_ts[:, None] - offsets[None, :]   # (M, W)

    t1_mat, rng_mat, found_mat = _batch_lookup(sorted_ts, t1_arr, rng_arr, bar_ts)

    # Маска активных баров
    if need_filter:
        active = rng_mat > avg_range
    else:
        active = np.ones(bar_ts.shape, dtype=bool)

    if use_range:
        # calc_var=4: need_filter всегда True, t1 не нужен
        value_mat = rng_mat - avg_range
    else:
        # bar должен присутствовать в rates (t1 not None)
        active   &= found_mat
        value_mat = np.where(
            found_mat,
            t1_mat * np.abs(t1_mat) if use_square else t1_mat,
            0.0,
        )

    w_mat   = np.where(active, weights_np[None, :], 0.0)
    w_total = w_mat.sum(axis=1)        # (M,)
    total   = (value_mat * w_mat).sum(axis=1)

    safe_w = np.where(w_total > 0.0, w_total, 1.0)
    return np.where(w_total > 0.0, total / safe_w, 0.0)


def _ext_soft_hit_batch(
    t_centers_ts:  np.ndarray,   # (M,) int64
    ext_sorted_ts: np.ndarray,   # (E,) int64
    sorted_ts:     np.ndarray,   # (K,) int64
    rng_arr:       np.ndarray,   # (K,)
    avg_range:     float,
    window:        int,
    weights_np:    np.ndarray,   # (window,)
    calc_var:      int,
    bar_delta_sec: int,
) -> tuple[np.ndarray, np.ndarray]:  # (soft_hits(M,), has_valid(M,))
    """
    Векторизованный аналог _multibar_ext_soft_hit.

    Возвращает (soft_hit[M], has_valid[M]) — без Python-цикла по window.
    Численно идентичен оригиналу.
    """
    need_filter = calc_var in (1, 3, 4)

    offsets = np.arange(window, dtype=np.int64) * bar_delta_sec
    bar_ts  = t_centers_ts[:, None] - offsets[None, :]   # (M, W)

    # Только rng нужен (t1 не используется в ext soft-hit)
    flat  = bar_ts.ravel()
    n     = len(sorted_ts)
    idxs  = sorted_ts.searchsorted(flat)
    safe  = np.clip(idxs, 0, n - 1)
    found = (idxs < n) & (sorted_ts[safe] == flat)
    rng_mat = np.where(found, rng_arr[safe], 0.0).reshape(bar_ts.shape)

    active = rng_mat > avg_range if need_filter else np.ones(bar_ts.shape, dtype=bool)

    in_ext_mat = _batch_in_ext(ext_sorted_ts, bar_ts)  # (M, W)

    w_mat         = np.where(active, weights_np[None, :], 0.0)
    w_total       = w_mat.sum(axis=1)                             # (M,)
    weighted_hits = (w_mat * in_ext_mat.astype(np.float64)).sum(axis=1)

    safe_w    = np.where(w_total > 0.0, w_total, 1.0)
    soft_hits = np.where(w_total > 0.0, weighted_hits / safe_w, 0.0)
    has_valid = w_total > 0.0
    return soft_hits, has_valid


def _ext_range_hit_batch(
    t_centers_ts:  np.ndarray,   # (M,) int64
    ext_sorted_ts: np.ndarray,   # (E,) int64
    sorted_ts:     np.ndarray,   # (K,) int64
    rng_arr:       np.ndarray,   # (K,)
    avg_range:     float,
    window:        int,
    weights_np:    np.ndarray,   # (window,)
    bar_delta_sec: int,
) -> np.ndarray:                  # (M,)
    """
    Векторизованный аналог _multibar_ext_range_hit (var=4).

    w_total = Σ w[i] для всех баров с rng > avg_range (независимо от ext).
    total   = Σ w[i] * (rng - avg_range) только для баров в ext_set.
    Численно идентичен оригиналу.
    """
    offsets = np.arange(window, dtype=np.int64) * bar_delta_sec
    bar_ts  = t_centers_ts[:, None] - offsets[None, :]

    flat  = bar_ts.ravel()
    n     = len(sorted_ts)
    idxs  = sorted_ts.searchsorted(flat)
    safe  = np.clip(idxs, 0, n - 1)
    found = (idxs < n) & (sorted_ts[safe] == flat)
    rng_mat = np.where(found, rng_arr[safe], 0.0).reshape(bar_ts.shape)

    active     = rng_mat > avg_range                     # (M, W)
    in_ext_mat = _batch_in_ext(ext_sorted_ts, bar_ts)    # (M, W)
    active_ext = active & in_ext_mat

    # w_total: все активные бары (не только ext)
    w_mat_all = np.where(active,     weights_np[None, :], 0.0)
    w_total   = w_mat_all.sum(axis=1)

    # total: только активные бары в ext
    w_mat_ext = np.where(active_ext, weights_np[None, :], 0.0)
    total     = ((rng_mat - avg_range) * w_mat_ext).sum(axis=1)

    safe_w = np.where(w_total > 0.0, w_total, 1.0)
    return np.where(w_total > 0.0, total / safe_w, 0.0)


# ══════════════════════════════════════════════════════════════════════════════
# СКАЛЯРНЫЕ ФУНКЦИИ (сохранены для тестов / совместимости)
# ══════════════════════════════════════════════════════════════════════════════

def compute_multibar_t1(
    t_center: datetime,
    t1_map: dict,
    rng_map: dict,
    avg_range: float,
    window: int,
    weights: list[float],
    calc_var: int,
    bar_delta: timedelta,
) -> float:
    """Скалярная версия (используется в тестах)."""
    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4

    total   = 0.0
    w_total = 0.0

    for i in range(window):
        bar_dt = t_center - i * bar_delta
        rng    = rng_map.get(bar_dt, 0.0)

        if need_filter and rng <= avg_range:
            continue

        w = weights[i] if i < len(weights) else 1.0

        if use_range:
            total   += w * (rng - avg_range)
            w_total += w
        else:
            t1 = t1_map.get(bar_dt)
            if t1 is None:
                continue
            total   += w * (t1 * abs(t1) if use_square else t1)
            w_total += w

    return (total / w_total) if w_total > 0 else 0.0


def _multibar_ext_soft_hit(
    t_center: datetime,
    ext_set: set,
    rng_map: dict,
    avg_range: float,
    window: int,
    weights: list[float],
    calc_var: int,
    bar_delta: timedelta,
) -> tuple[float, bool]:
    """Скалярная версия (используется в тестах)."""
    need_filter   = calc_var in (1, 3, 4)
    weighted_hits = 0.0
    w_total       = 0.0

    for i in range(window):
        bar_dt = t_center - i * bar_delta
        rng    = rng_map.get(bar_dt, 0.0)
        if need_filter and rng <= avg_range:
            continue
        w        = weights[i] if i < len(weights) else 1.0
        w_total += w
        if bar_dt in ext_set:
            weighted_hits += w

    if w_total > 0:
        return weighted_hits / w_total, True
    return 0.0, False


def _multibar_ext_range_hit(
    t_center: datetime,
    ext_set: set,
    rng_map: dict,
    avg_range: float,
    window: int,
    weights: list[float],
    bar_delta: timedelta,
) -> float:
    """Скалярная версия (используется в тестах)."""
    total   = 0.0
    w_total = 0.0

    for i in range(window):
        bar_dt = t_center - i * bar_delta
        rng    = rng_map.get(bar_dt, 0.0)
        if rng <= avg_range:
            continue
        w        = weights[i] if i < len(weights) else 1.0
        w_total += w
        if bar_dt in ext_set:
            total += w * (rng - avg_range)

    return (total / w_total) if w_total > 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИЯ model()
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict:
    """
    Мультибаровый анализ весов календарных событий (brain_calendar edition).

    Контракт dataset_index (фреймворк заполняет):
      by_key     — {url: [row, ...]}
      key_dates  — {url: [sorted datetime, ...]}
      ctx_index  — {(event_id, currency, importance, fcd, scd, rcd): {occurrence_count, ...}}
      np_rates   — numpy-массивы свечей (ext_max, ext_min, ranges, dates_ns)

    Все числовые результаты бит-идентичны оригинальному model().
    """
    if not rates or dataset_index is None:
        return {}

    by_key    = dataset_index.get("by_key",    {})
    key_dates = dataset_index.get("key_dates", {})
    ctx_index = dataset_index.get("ctx_index", {})
    np_r      = dataset_index.get("np_rates")

    if not by_key:
        return {}

    # ── Таймфрейм ─────────────────────────────────────────────────────────────
    if len(rates) >= 2:
        gap_sec  = (rates[-1]["date"] - rates[-2]["date"]).total_seconds()
        day_flag = 1 if gap_sec >= 86_400 * 0.9 else 0
    else:
        day_flag = 0

    bar_delta     = timedelta(days=1) if day_flag else timedelta(hours=1)
    bar_delta_sec = int(bar_delta.total_seconds())   # горячий путь — int, не timedelta
    modification  = get_modification(rates)

    # ── Адаптивное окно и линейные веса ──────────────────────────────────────
    window     = get_adaptive_window(np_r, day_flag)
    weights    = get_linear_weights(window)           # list — для скалярных функций
    weights_np = np.array(weights, dtype=np.float64)  # numpy — для батч-функций

    # ── Numpy-массивы ставок — единственный источник данных ─────────────────
    # Заменяет build_rates_lookup (Python-dict) + build_ext_sets:
    #   Было:  2×N timestamp-конвертаций + 2 dict-построения
    #   Стало: 1×N конвертация, всё остальное через numpy
    sorted_ts_np, t1_arr_np, rng_arr_np = _build_rates_numpy(rates)

    # ── avg_range ─────────────────────────────────────────────────────────────
    if np_r is not None:
        _rng_full = np_r.get("ranges")
        avg_range = float(np.mean(_rng_full)) if _rng_full is not None and len(_rng_full) > 0 \
                    else (float(np.mean(rng_arr_np)) if len(rng_arr_np) > 0 else 0.0)
    else:
        avg_range = float(np.mean(rng_arr_np)) if len(rng_arr_np) > 0 else 0.0

    # ── ext int64-массивы через _build_ext_arrays_np ──────────────────────────
    # Не создаёт datetime-set, не делает повторных .timestamp() вызовов
    ext_max_sorted_ts, ext_min_sorted_ts = _build_ext_arrays_np(np_r, sorted_ts_np)

    # ── Тренд предыдущей свечи ────────────────────────────────────────────────
    is_bull      = prev_candle_is_bull(rates, date)
    use_var4_ext = (var == 4)

    if type in (0, 2) and is_bull is not None:
        ext_sorted_ts = ext_max_sorted_ts if is_bull else ext_min_sorted_ts
    else:
        ext_sorted_ts = np.empty(0, dtype=np.int64)

    date_ts  = int(date.timestamp())
    url_map  = dataset_index.get("url_map", {})

    # ══════════════════════════════════════════════════════════════════════════
    # ОПТИМИЗАЦИЯ 1: двухуровневый кэш per-URL.
    #
    # СТАТИЧЕСКИЙ кэш (_url_static_cache) хранится в dataset_index и переживает
    # между вызовами model() в течение всего RELOAD_INTERVAL (3600 сек).
    # Содержит: event_id, dirs, is_rec, all_dates_ts (int64) — всё дорогое.
    # Строится ОДИН РАЗ при первом вызове, потом мгновенно переиспользуется.
    #
    # ДИНАМИЧЕСКИЙ кэш строится каждый вызов, но только из быстрых операций:
    # bisect + array slice (O(1), без datetime-арифметики).
    # ══════════════════════════════════════════════════════════════════════════
    static_cache: dict[str, dict] = dataset_index.get("_url_static_cache")  # type: ignore[assignment]
    if static_cache is None:
        static_cache = {}
        for url, dates_sorted in key_dates.items():
            rows = by_key.get(url)
            if not rows:
                continue
            row0      = rows[0]
            _um_entry = url_map.get(url)
            event_id  = (_um_entry.get("event_id") if isinstance(_um_entry, dict)
                         else _um_entry)
            currency   = row0.get("currency_code")
            importance = row0.get("importance") or "none"
            fcd, scd, rcd = _compute_dirs(row0)
            ctx_key = (event_id, currency, importance, fcd, scd, rcd)
            occ     = ctx_index.get(ctx_key, {}).get("occurrence_count", 0)
            is_rec  = occ >= RECURRING_MIN_COUNT
            # datetime → int64: дорого, но только ОДИН РАЗ за весь RELOAD_INTERVAL
            all_dates_ts = np.array(
                [int(d.timestamp()) for d in dates_sorted], dtype=np.int64
            )
            static_cache[url] = {
                "event_id":     event_id,
                "currency":     currency,
                "importance":   importance,
                "fcd":          fcd,
                "scd":          scd,
                "rcd":          rcd,
                "is_rec":       is_rec,
                "all_dates_ts": all_dates_ts,
            }
        dataset_index["_url_static_cache"] = static_cache

    # Динамический кэш: только searchsorted + array slice (микросекунды на URL)
    url_cache: dict[str, dict] = {}
    for url, sc in static_cache.items():
        idx_cut = int(sc["all_dates_ts"].searchsorted(date_ts))
        hist_ts = sc["all_dates_ts"][:idx_cut]   # view — без копии
        if len(hist_ts) == 0:
            continue
        url_cache[url] = {**sc, "hist_ts": hist_ts, "total_hist": len(hist_ts)}

    result: dict[str, float] = defaultdict(float)

    # ══════════════════════════════════════════════════════════════════════════
    # ОСНОВНОЙ ЦИКЛ: shift × url (без вложенного цикла по t_centers)
    # ══════════════════════════════════════════════════════════════════════════
    for shift in range(SHIFT_WINDOW + 1):
        shift_sec        = shift * bar_delta_sec
        # ОПТИМИЗАЦИЯ 3: shift-window bounds в int64 (не datetime объекты)
        check_dt_ts      = date_ts - shift_sec
        check_dt_next_ts = check_dt_ts + bar_delta_sec

        for url, info in url_cache.items():
            is_rec = info["is_rec"]
            if not is_rec and shift != 0:
                continue

            # ОПТИМИЗАЦИЯ 4: .searchsorted() метод вместо np.searchsorted (нет _wrapfunc dispatch)
            all_dates_ts = info["all_dates_ts"]
            lo = int(all_dates_ts.searchsorted(check_dt_ts))
            hi = int(all_dates_ts.searchsorted(check_dt_next_ts))
            if lo >= hi:
                continue

            hist_ts    = info["hist_ts"]
            total_hist = info["total_hist"]
            event_id   = info["event_id"]
            currency   = info["currency"]
            importance = info["importance"]
            fcd, scd, rcd = info["fcd"], info["scd"], info["rcd"]
            shift_arg  = shift if is_rec else None

            # ОПТИМИЗАЦИЯ 5: t_centers как int64-вектор (не список datetime)
            # t_centers[i] = hist_ts[i] + shift_sec
            t_centers_ts = hist_ts + shift_sec
            # фильтр: проекция строго < date (аналог оригинального if-условия)
            t_centers_ts = t_centers_ts[t_centers_ts < date_ts]
            if len(t_centers_ts) == 0:
                continue

            # ══════════════════════════════════════════════════════════════════
            # T1-СУММА (type 0 или 1)
            # ОПТИМИЗАЦИЯ 6: весь цикл по t_centers + window заменён одним
            #   батч-вызовом (матричные операции numpy вместо Python-цикла).
            # ══════════════════════════════════════════════════════════════════
            if type in (0, 1):
                t1_vals  = _compute_t1_batch(
                    t_centers_ts, sorted_ts_np, t1_arr_np, rng_arr_np,
                    avg_range, window, weights_np, var, bar_delta_sec,
                )
                t1_accum = float(t1_vals.sum())
                if t1_accum != 0:
                    wc = _make_code(event_id, currency, importance,
                                    fcd, scd, rcd, 0, shift_arg)
                    result[wc] += t1_accum

            # ══════════════════════════════════════════════════════════════════
            # EXTREMUM (type 0 или 2)
            # ══════════════════════════════════════════════════════════════════
            if type in (0, 2) and is_bull is not None:

                if use_var4_ext:
                    ext_vals  = _ext_range_hit_batch(
                        t_centers_ts, ext_sorted_ts, sorted_ts_np, rng_arr_np,
                        avg_range, window, weights_np, bar_delta_sec,
                    )
                    ext_accum = float(ext_vals.sum())
                    if ext_accum != 0:
                        wc = _make_code(event_id, currency, importance,
                                        fcd, scd, rcd, 1, shift_arg)
                        result[wc] += ext_accum

                else:
                    if total_hist == 0:
                        continue

                    soft_hits, has_valid_arr = _ext_soft_hit_batch(
                        t_centers_ts, ext_sorted_ts, sorted_ts_np, rng_arr_np,
                        avg_range, window, weights_np, var, bar_delta_sec,
                    )
                    # has_pool = any(has_valid) — аналог "if not pool" (БАГ 6)
                    if not has_valid_arr.any():
                        continue

                    soft_count = float(soft_hits.sum())
                    # Финальная формула — ОДИН РАЗ (БАГ 2)
                    val = (soft_count / total_hist * 2 - 1) * modification
                    if val != 0:
                        wc = _make_code(event_id, currency, importance,
                                        fcd, scd, rcd, 1, shift_arg)
                        result[wc] += val

    return {k: round(v, 6) for k, v in result.items() if v != 0}
