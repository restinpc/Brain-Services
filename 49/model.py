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
"""
from __future__ import annotations

import bisect
import os
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

# brain_calendar_context_idx — строится builder'ом context_idx.py.
# Фреймворк загружает его через CTX_QUERY и кладёт в dataset_index["ctx_index"].
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

# Основной датасет — brain_calendar.
# Направления (forecast_dir / surprise_dir / revision_dir) вычисляются
# в _compute_dirs() на лету из числовых колонок.
# DATASET_KEY = "url" — совпадает со стандартным конфигом brain_calendar.
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

# False: dataset_index["key_dates"] строится из полного датасета,
# поэтому shift=0 корректно видит события в [date, date+delta).
# hist_before внутри model() всё равно обрезает по < date — нет lookahead.
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

    forecast_dir  (actual vs forecast):
        actual > forecast + threshold → BEAT
        actual < forecast - threshold → MISS
        |actual - forecast| <= threshold → INLINE
        forecast is None → UNKNOWN

    surprise_dir  (actual vs previous):
        actual > previous + threshold → UP
        actual < previous - threshold → DOWN
        |actual - previous| <= threshold → FLAT
        previous is None → UNKNOWN

    revision_dir  (old_previous vs previous):
        old_previous is None или previous is None → NONE
        previous > old_previous + threshold → UP
        previous < old_previous - threshold → DOWN
        |previous - old_previous| <= threshold → FLAT
    """
    actual       = _safe_float(row.get("actual_value"))
    forecast     = _safe_float(row.get("forecast_value"))
    previous     = _safe_float(row.get("previous_value"))
    old_previous = _safe_float(row.get("old_previous_value"))
    thr = DIRECTION_THRESHOLD

    # forecast_dir
    if actual is None or forecast is None:
        fcd = "UNKNOWN"
    elif actual > forecast + thr:
        fcd = "BEAT"
    elif actual < forecast - thr:
        fcd = "MISS"
    else:
        fcd = "INLINE"

    # surprise_dir
    if actual is None or previous is None:
        scd = "UNKNOWN"
    elif actual > previous + thr:
        scd = "UP"
    elif actual < previous - thr:
        scd = "DOWN"
    else:
        scd = "FLAT"

    # revision_dir
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
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (идентичны сервису 42)
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
    Медиана устойчива к длинным трендовым участкам без разворотов.
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
    """
    Масштабирующий коэффициент по последней цене закрытия.
    {EURUSD: 0.001, ETHUSD: 100.0, BTCUSD: 1000.0}.
    """
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
    t1_map[dt]  = close - open  (прокси для DB-колонки t1)
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
    """
    avg_range по ВСЕЙ истории (np_rates["ranges"]).
    Fallback: среднее по переданному срезу.
    """
    if np_r is not None:
        arr = np_r.get("ranges")
        if arr is not None and len(arr) > 0:
            return float(np.mean(arr))
    return sum(rng_map.values()) / len(rng_map) if rng_map else 0.0


def build_ext_sets(np_r: dict | None, t1_map: dict) -> tuple[set, set]:
    """
    Аналог GLOBAL_EXTREMUMS[table]["max"/"min"] из оригинала.
    Ограничиваемся датами из t1_map (≤ target_date) — нет lookahead.
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
    for dt in t1_map:
        ts  = int(dt.timestamp())
        idx = int(np.searchsorted(dates_ns, ts))
        if idx < len(dates_ns) and dates_ns[idx] == ts:
            if ext_max_mask[idx]:
                ext_max_set.add(dt)
            if ext_min_mask[idx]:
                ext_min_set.add(dt)
    return ext_max_set, ext_min_set


def prev_candle_is_bull(rates: list[dict], target_dt: datetime) -> bool | None:
    """Аналог find_prev_candle_trend() из оригинала."""
    for r in reversed(rates):
        if r["date"] < target_dt:
            return (r.get("close") or 0) > (r.get("open") or 0)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# ЯДРО: мультибаровые вычисления (идентичны сервису 42)
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
    """
    Взвешенная T1-сумма по окну [t_center, t_center-Δ, ..., t_center-(N-1)Δ].

    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4

    Нормировка на w_total (реальные непропущенные бары).
    При window=1: результат = t1[t_center] ✓
    """
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
    """
    Возвращает (soft_hit, has_valid_bars).

    soft_hit ∈ [0, 1]: взвешенная доля баров окна в ext_set.
    has_valid_bars: True если хотя бы один бар прошёл фильтр.

    Репликация "if not pool: return None" из оригинала (БАГ 6).
    Финальная формула (*2-1)*mod применяется ОДИН РАЗ в model() (БАГ 2).
    """
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
    """
    var=4 extremum: взвешенная сумма (rng - avg_range) для баров-экстремумов.
    Без /total_hist и без (*2-1) — аналог оригинала (БАГ 3).
    Если pool пуст → 0.0, в model() не добавляется.
    """
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

    Отличие от сервиса 42:
      • DATASET_KEY = "url" вместо "ctx_id"
      • forecast_dir / surprise_dir / revision_dir вычисляются из числовых
        колонок brain_calendar через _compute_dirs() — не читаются из датасета.
      • ctx_index по-прежнему берётся из brain_calendar_context_idx через
        dataset_index["ctx_index"].

    Контракт dataset_index (фреймворк заполняет):
      by_key     — {url: [row, ...]}
      key_dates  — {url: [sorted datetime, ...]}
      ctx_index  — {(event_id, currency, importance, fcd, scd, rcd): {occurrence_count, ...}}
      np_rates   — numpy-массивы свечей (ext_max, ext_min, ranges, dates_ns)
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

    bar_delta    = timedelta(days=1) if day_flag else timedelta(hours=1)
    modification = get_modification(rates)

    # ── Адаптивное окно и линейные веса ──────────────────────────────────────
    window  = get_adaptive_window(np_r, day_flag)
    weights = get_linear_weights(window)    # [N, N-1, ..., 1]

    # ── Словари ставок / диапазонов / экстремумов ─────────────────────────────
    t1_map, rng_map = build_rates_lookup(rates)
    avg_range       = extract_avg_range(np_r, rng_map)
    ext_max_set, ext_min_set = build_ext_sets(np_r, t1_map)

    # ── Тренд предыдущей свечи ────────────────────────────────────────────────
    is_bull = prev_candle_is_bull(rates, date)
    ext_set: set[datetime] = set()
    if type in (0, 2) and is_bull is not None:
        ext_set = ext_max_set if is_bull else ext_min_set

    use_var4_ext = (var == 4)
    result: dict[str, float] = {}

    # ── Основной цикл: shift = 0 … SHIFT_WINDOW ──────────────────────────────
    for shift in range(0, SHIFT_WINDOW + 1):
        check_dt      = date - bar_delta * shift   # = dt из оригинала
        check_dt_next = check_dt + bar_delta        # = dt_end из оригинала

        for url, dates_sorted in key_dates.items():
            # bisect_LEFT для обеих границ — событие ровно на check_dt_next
            # не попадёт в этот слот и не дублируется в следующем (БАГ 4).
            lo = bisect.bisect_left(dates_sorted, check_dt)
            hi = bisect.bisect_left(dates_sorted, check_dt_next)
            if lo >= hi:
                continue

            rows = by_key.get(url)
            if not rows:
                continue

            # ── Вычисляем направления из первой строки датасета ──────────────
            # brain_calendar не хранит event_id и готовые _dir.
            # event_id берём из url_map (фреймворк строит из URL_MAP_QUERY).
            # Направления вычисляем на лету из числовых колонок.
            row0       = rows[0]
            url_map    = dataset_index.get("url_map", {})
            _um_entry  = url_map.get(url)
            event_id   = (_um_entry.get("event_id") if isinstance(_um_entry, dict)
                          else _um_entry)
            currency   = row0.get("currency_code")
            importance = row0.get("importance") or "none"
            fcd, scd, rcd = _compute_dirs(row0)

            ctx_key  = (event_id, currency, importance, fcd, scd, rcd)
            ctx_info = ctx_index.get(ctx_key, {})
            occ      = ctx_info.get("occurrence_count", 0)
            is_rec   = occ >= RECURRING_MIN_COUNT

            if not is_rec and shift != 0:
                continue

            shift_arg = shift if is_rec else None

            # ── Исторические даты этого url до target_date ───────────────────
            all_hist    = key_dates.get(url, [])
            idx_cut     = bisect.bisect_left(all_hist, date)   # строго < date
            hist_before = all_hist[:idx_cut]
            if not hist_before:
                continue

            # ── t_centers: проекции исторических событий со сдвигом shift ────
            t_centers = [
                d + bar_delta * shift
                for d in hist_before
                if (d + bar_delta * shift) < date
            ]
            if not t_centers:
                continue

            total_hist = len(hist_before)

            # ══════════════════════════════════════════════════════════════════
            # T1-СУММА (type 0 или 1)
            # ══════════════════════════════════════════════════════════════════
            if type in (0, 1):
                t1_accum = 0.0
                for t_ctr in t_centers:
                    t1_accum += compute_multibar_t1(
                        t_ctr, t1_map, rng_map, avg_range,
                        window, weights, var, bar_delta,
                    )
                if t1_accum != 0:
                    wc = _make_code(event_id, currency, importance,
                                    fcd, scd, rcd, 0, shift_arg)
                    result[wc] = result.get(wc, 0.0) + t1_accum

            # ══════════════════════════════════════════════════════════════════
            # EXTREMUM (type 0 или 2)
            # ══════════════════════════════════════════════════════════════════
            if type in (0, 2) and is_bull is not None:

                if use_var4_ext:
                    # var=4: range-based, без /total_hist и без *2-1 (БАГ 3)
                    ext_accum = 0.0
                    for t_ctr in t_centers:
                        ext_accum += _multibar_ext_range_hit(
                            t_ctr, ext_set, rng_map, avg_range,
                            window, weights, bar_delta,
                        )
                    if ext_accum != 0:
                        wc = _make_code(event_id, currency, importance,
                                        fcd, scd, rcd, 1, shift_arg)
                        result[wc] = result.get(wc, 0.0) + ext_accum

                else:
                    # var in (0,1,2,3): probability-based
                    # soft_count = Σ soft_hit(t_center) ∈ [0, len(t_centers)]
                    # has_pool   = any(has_valid_bars)
                    # Финальная формула — ОДИН РАЗ (БАГ 2)
                    # if not has_pool: continue — аналог "if not pool" (БАГ 6)
                    if total_hist == 0:
                        continue

                    soft_count = 0.0
                    has_pool   = False
                    for t_ctr in t_centers:
                        sh, valid = _multibar_ext_soft_hit(
                            t_ctr, ext_set, rng_map, avg_range,
                            window, weights, var, bar_delta,
                        )
                        soft_count += sh
                        if valid:
                            has_pool = True

                    if not has_pool:
                        continue

                    # Финальная формула — ОДИН РАЗ (БАГ 2)
                    val = (soft_count / total_hist * 2 - 1) * modification
                    if val != 0:
                        wc = _make_code(event_id, currency, importance,
                                        fcd, scd, rcd, 1, shift_arg)
                        result[wc] = result.get(wc, 0.0) + val

    return {k: round(v, 6) for k, v in result.items() if v != 0}