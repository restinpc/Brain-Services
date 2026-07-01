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

    # ── Предвычисление по котировкам ──────────────────────────────────────────
    rates_t1      = {}
    candle_ranges = {}
    ext_max: set  = set()
    ext_min: set  = set()

    for r in rates:
        d   = r["date"]
        t1  = float((r.get("close") or 0) - (r.get("open") or 0))
        rng = float((r.get("max") or 0)   - (r.get("min") or 0))
        rates_t1[d]      = t1
        candle_ranges[d] = rng

    avg_range = (sum(candle_ranges.values()) / len(candle_ranges)
                 if candle_ranges else 0.0)

    for i in range(1, len(rates) - 1):
        h  = float(rates[i].get("max") or 0)
        lo = float(rates[i].get("min") or 0)
        if h  > float(rates[i - 1].get("max") or 0) and h  > float(rates[i + 1].get("max") or 0):
            ext_max.add(rates[i]["date"])
        if lo < float(rates[i - 1].get("min") or 0) and lo < float(rates[i + 1].get("min") or 0):
            ext_min.add(rates[i]["date"])

    # Направление предыдущей свечи
    prev    = rates[-1] if rates else None
    is_bull = (
        float((prev.get("close") or 0)) > float((prev.get("open") or 0))
        if prev else True
    )
    ext_set = ext_max if is_bull else ext_min

    # ── Индекс истории по ctx_id ──────────────────────────────────────────────
    # dataset_index["key_dates"] = {ctx_id: [date1, date2, ...]}
    # строится фреймворком из DATASET_QUERY (колонка "date" = news_date)
    key_dates: dict = (dataset_index or {}).get("key_dates") or {}

    du          = timedelta(hours=1)
    WINDOW_SEC  = SHIFT_WINDOW * 3600
    result: dict[str, float] = {}

    # ── Основной цикл по свежим событиям в окне [date-SHIFT_WINDOW, date] ─────
    for row in dataset:
        news_dt = row.get("news_date") or row.get("date")
        if news_dt is None:
            continue

        diff_sec = (date - news_dt).total_seconds()
        if diff_sec < 0 or diff_sec > WINDOW_SEC:
            continue

        shift  = int(diff_sec / 3600)
        ctx_id = row.get("ctx_id")
        if ctx_id is None:
            continue

        # История вхождений этого ctx_id до текущей даты
        valid_dts = [d for d in key_dates.get(ctx_id, []) if d < date]
        if not valid_dts:
            continue

        # Редкие контексты — только shift=0
        if len(valid_dts) < RECURRING_MIN_COUNT and shift != 0:
            continue

        t_dates = [d + du * shift for d in valid_dts
                   if (d + du * shift) < date]
        if not t_dates:
            continue

        total_hist = len(valid_dts)

        # ── T1 (type=0 или type=1) ────────────────────────────────────────────
        if type in (0, 1):
            if var == 0:
                t1 = sum(rates_t1.get(d, 0.0) for d in t_dates)
            elif var == 1:
                t1 = sum(
                    rates_t1.get(d, 0.0) for d in t_dates
                    if candle_ranges.get(d, 0.0) > avg_range
                )
            elif var == 2:
                t1 = sum(
                    (v := rates_t1.get(d, 0.0)) * abs(v)
                    for d in t_dates
                )
            elif var == 3:
                t1 = sum(
                    (v := rates_t1.get(d, 0.0)) * abs(v)
                    for d in t_dates
                    if candle_ranges.get(d, 0.0) > avg_range
                )
            elif var == 4:
                t1 = sum(
                    candle_ranges.get(d, 0.0) - avg_range
                    for d in t_dates
                    if candle_ranges.get(d, 0.0) > avg_range
                )
            else:
                t1 = 0.0

            if t1 != 0.0:
                wc = f"NW{ctx_id}_0_{shift}"
                result[wc] = result.get(wc, 0.0) + t1

        # ── Extremum (type=0 или type=2) ──────────────────────────────────────
        if type in (0, 2) and prev is not None:
            ext: float | None = None

            if var == 0:
                hits = sum(1 for d in t_dates if d in ext_set)
                if total_hist > 0:
                    val  = (hits / total_hist) * 2 - 1
                    ext  = val if val != 0 else None

            elif var == 1:
                pool = [d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range]
                if pool and total_hist > 0:
                    val = (sum(1 for d in pool if d in ext_set) / total_hist) * 2 - 1
                    ext = val if val != 0 else None

            elif var == 2:
                pool = [d for d in t_dates if d in ext_set]
                if pool and total_hist > 0:
                    val = sum(
                        (v := rates_t1.get(d, 0.0)) * abs(v)
                        for d in pool
                    ) / total_hist
                    ext = val if val != 0 else None

            elif var == 3:
                pool = [
                    d for d in t_dates
                    if d in ext_set and candle_ranges.get(d, 0.0) > avg_range
                ]
                if pool and total_hist > 0:
                    val = (len(pool) / total_hist) * 2 - 1
                    ext = val if val != 0 else None

            elif var == 4:
                pool = [d for d in t_dates if d in ext_set]
                val  = sum(
                    candle_ranges.get(d, 0.0) - avg_range
                    for d in pool
                    if candle_ranges.get(d, 0.0) > avg_range
                )
                ext = val if val != 0 else None

            if ext is not None:
                wc = f"NW{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}
