"""
model.py — Сервис 35: Веса новостей на основе NER-контекстов.

Источники: CNN, NYT, TWP, TGD, WSJ (5 NER-моделей)
Контекст = NER-отпечаток (feed_cat | person | location | misc)
Консенсус >= 2/3 моделей по каждой сущности.

Код веса: NW{ctx_id}_{mode}_{shift}
  mode=0 → сумма T1
  mode=1 → экстремумная вероятность
  shift  → сдвиг в часах (0..24 для повторяющихся)

Критичные переменные (PORT, NODE_NAME, SERVICE_ID, SERVICE_TEXT)
лежат в .env рядом с этим файлом и НЕ дублируются здесь.

Построение индекса и весов — см. index_builder.py и weight_builder.py.
Фреймворк вызывает их автоматически.
"""

from __future__ import annotations

import bisect
from collections import OrderedDict
from datetime import timedelta

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ДЛЯ ФРЕЙМВОРКА
# ══════════════════════════════════════════════════════════════════════════════

# Обязателен в v13 — определяет какую таблицу котировок грузить.
RATES_TABLE = "brain_rates_eur_usd"   # ← замените при необходимости

WEIGHTS_TABLE   = "vlad_news_weights_table"
CTX_TABLE       = "vlad_news_context_idx"
CTX_KEY_COLUMNS = ["id"]

# Фреймворк читает DATASET_QUERY / DATASET_ENGINE (не EVENTS_QUERY / EVENTS_ENGINE)
# Обязательно наличие колонки date (или псевдонима news_date AS date)
# чтобы _build_dataset_index мог построить key_dates по ctx_id.
DATASET_QUERY = """
    SELECT ctx_id, news_date, news_date AS date
    FROM vlad_news_ctx_map
    WHERE news_date IS NOT NULL
    ORDER BY news_date
"""
DATASET_ENGINE = "vlad"              # было EVENTS_ENGINE

# Фреймворк читает FILTER_DATASET_BY_DATE (не FILTER_FUTURE_EVENTS)
FILTER_DATASET_BY_DATE = True        # было FILTER_FUTURE_EVENTS

SHIFT_WINDOW        = 24             # часов
RECURRING_MIN_COUNT = 2              # минимум вхождений для ненулевого shift
CACHE_DATE_FROM     = "2025-01-15"

REBUILD_INTERVAL = 3600   # секунд между автоматическими вызовами rebuild

# Вариации
#   var=0  простая сумма T1 / базовый экстремум
#   var=1  только крупные свечи (range > avg_range)
#   var=2  T1 × |T1| — квадратичное усиление
#   var=3  крупные свечи + квадрат
#   var=4  амплитуда: (range − avg_range) вместо T1

VAR_RANGE = [0, 1, 2, 3, 4]

# Небольшие bounded-cache: fill_cache многократно вызывает model() для одной
# даты с разными type/var. Без кеша карты котировок и общий timeline событий
# пересобирались сотни тысяч раз.
_RATES_CACHE: "OrderedDict[tuple, tuple]" = OrderedDict()
_EVENTS_CACHE: "OrderedDict[tuple, tuple]" = OrderedDict()
_CACHE_LIMIT = 12

def _lru_get(cache, key):
    value = cache.get(key)
    if value is not None:
        cache.move_to_end(key)
    return value

def _lru_put(cache, key, value):
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > _CACHE_LIMIT:
        cache.popitem(last=False)

def _rates_state(rates):
    first = rates[0]
    last = rates[-1]
    return (
        len(rates), first.get("date"), last.get("date"),
        float(first.get("close") or 0.0), float(last.get("close") or 0.0),
    )

def _prepare_rates(rates):
    key = _rates_state(rates)
    cached = _lru_get(_RATES_CACHE, key)
    if cached is not None:
        return cached

    rates_t1 = {}
    candle_ranges = {}
    ext_max = set()
    ext_min = set()
    for r in rates:
        d = r["date"]
        stored = r.get("t1")
        t1 = float(stored) if stored is not None else float((r.get("close") or 0) - (r.get("open") or 0))
        rng = float((r.get("max") or 0) - (r.get("min") or 0))
        rates_t1[d] = t1
        candle_ranges[d] = rng

    avg_range = sum(candle_ranges.values()) / len(candle_ranges) if candle_ranges else 0.0
    for i in range(1, len(rates) - 1):
        h = float(rates[i].get("max") or 0)
        lo = float(rates[i].get("min") or 0)
        if h > float(rates[i - 1].get("max") or 0) and h > float(rates[i + 1].get("max") or 0):
            ext_max.add(rates[i]["date"])
        if lo < float(rates[i - 1].get("min") or 0) and lo < float(rates[i + 1].get("min") or 0):
            ext_min.add(rates[i]["date"])

    value = (rates_t1, candle_ranges, avg_range, ext_max, ext_min)
    _lru_put(_RATES_CACHE, key, value)
    return value

def _event_timeline(key_dates):
    # key_dates строится один раз при reload и затем переиспользуется.
    total = sum(len(v) for v in key_dates.values())
    key = (id(key_dates), len(key_dates), total)
    cached = _lru_get(_EVENTS_CACHE, key)
    if cached is not None:
        return cached
    events = sorted((dt, ctx_id) for ctx_id, dates in key_dates.items() for dt in dates)
    dates_only = [x[0] for x in events]
    value = (events, dates_only)
    _lru_put(_EVENTS_CACHE, key, value)
    return value


# ══════════════════════════════════════════════════════════════════════════════
# model() — ОСНОВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates,
    dataset,
    date,
    *,
    type          = 0,
    var           = 0,
    param         = "",
    dataset_index = None,
):
    """
    Вычисляет веса T1 + Extremum по NER-контекстам новостей.

    Параметры:
        rates         — список свечей [{"date", "open", "close", "min", "max"}, ...]
        dataset       — строки vlad_news_ctx_map (ctx_id + news_date)
        date          — текущая дата (datetime)
        type          — 0=оба режима, 1=только T1, 2=только Extremum
        var           — вариант расчёта (0..4)
        param         — доп. параметры (не используются)
        dataset_index — dict от фреймворка: {key_dates, np_rates, ctx_index, ...}

    Возвращает:
        словарь {weight_code: значение}
    """
    if not rates or not dataset:
        return {}

    # ── Предвычисление по котировкам (LRU между type/var-слотами) ───────────
    rates_t1, candle_ranges, avg_range, ext_max, ext_min = _prepare_rates(rates)

    prev = rates[-1] if rates else None
    is_bull = (
        float((prev.get("close") or 0)) > float((prev.get("open") or 0))
        if prev else True
    )
    ext_set = ext_max if is_bull else ext_min

    key_dates: dict = (dataset_index or {}).get("key_dates") or {}
    if not key_dates:
        return {}

    du = timedelta(hours=1)
    window_start = date - timedelta(hours=SHIFT_WINDOW)
    events, event_dates = _event_timeline(key_dates)
    lo = bisect.bisect_left(event_dates, window_start)
    hi = bisect.bisect_right(event_dates, date)

    result: dict[str, float] = {}

    # Раньше здесь полностью сканировался dataset для каждого model() вызова.
    # Теперь берётся только срез событий текущего 24-часового окна.
    for news_dt, ctx_id in events[lo:hi]:
        diff_sec = (date - news_dt).total_seconds()
        shift = int(diff_sec / 3600)

        hist = key_dates.get(ctx_id, [])
        idx_date = bisect.bisect_left(hist, date)
        if idx_date <= 0:
            continue

        if idx_date < RECURRING_MIN_COUNT and shift != 0:
            continue

        # d + shift < date  <=>  d < date - shift
        idx_shift = bisect.bisect_left(hist, date - du * shift)
        if idx_shift <= 0:
            continue
        t_dates = (d + du * shift for d in hist[:idx_shift])
        total_hist = idx_date

        if type in (0, 1):
            if var == 0:
                t1 = sum(rates_t1.get(d, 0.0) for d in t_dates)
            elif var == 1:
                t1 = sum(rates_t1.get(d, 0.0) for d in t_dates if candle_ranges.get(d, 0.0) > avg_range)
            elif var == 2:
                t1 = sum((v := rates_t1.get(d, 0.0)) * abs(v) for d in t_dates)
            elif var == 3:
                t1 = sum((v := rates_t1.get(d, 0.0)) * abs(v) for d in t_dates if candle_ranges.get(d, 0.0) > avg_range)
            elif var == 4:
                t1 = sum(candle_ranges.get(d, 0.0) - avg_range for d in t_dates if candle_ranges.get(d, 0.0) > avg_range)
            else:
                t1 = 0.0
            if t1 != 0.0:
                wc = f"NW{ctx_id}_0_{shift}"
                result[wc] = result.get(wc, 0.0) + t1

        if type in (0, 2) and prev is not None:
            # Генератор выше исчерпан T1-веткой, поэтому для extremum создаём его снова.
            t_dates2 = (d + du * shift for d in hist[:idx_shift])
            ext = None
            if var == 0:
                hits = sum(1 for d in t_dates2 if d in ext_set)
                val = (hits / total_hist) * 2 - 1 if total_hist > 0 else 0.0
                ext = val if val != 0 else None
            elif var == 1:
                pool = [d for d in t_dates2 if candle_ranges.get(d, 0.0) > avg_range]
                if pool and total_hist > 0:
                    val = (sum(1 for d in pool if d in ext_set) / total_hist) * 2 - 1
                    ext = val if val != 0 else None
            elif var == 2:
                pool = [d for d in t_dates2 if d in ext_set]
                if pool and total_hist > 0:
                    val = sum((v := rates_t1.get(d, 0.0)) * abs(v) for d in pool) / total_hist
                    ext = val if val != 0 else None
            elif var == 3:
                pool = [d for d in t_dates2 if d in ext_set and candle_ranges.get(d, 0.0) > avg_range]
                if pool and total_hist > 0:
                    val = (len(pool) / total_hist) * 2 - 1
                    ext = val if val != 0 else None
            elif var == 4:
                pool = [d for d in t_dates2 if d in ext_set]
                val = sum(candle_ranges.get(d, 0.0) - avg_range for d in pool if candle_ranges.get(d, 0.0) > avg_range)
                ext = val if val != 0 else None
            if ext is not None:
                wc = f"NW{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}
