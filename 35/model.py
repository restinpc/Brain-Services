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

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ДЛЯ ФРЕЙМВОРКА
# ══════════════════════════════════════════════════════════════════════════════

WEIGHTS_TABLE   = "vlad_news_weights_table"
CTX_TABLE       = "vlad_news_context_idx"
CTX_KEY_COLUMNS = ["id"]

EVENTS_QUERY = """
    SELECT ctx_id, news_date
    FROM vlad_news_ctx_map
    WHERE news_date IS NOT NULL
    ORDER BY news_date
"""
EVENTS_ENGINE     = "vlad"
EVENT_KEY_COLUMNS = ["ctx_id"]
EVENT_DATE_COLUMN = "news_date"

SHIFT_WINDOW         = 24
RECURRING_MIN_COUNT  = 2
FILTER_FUTURE_EVENTS = True

REBUILD_INTERVAL = 3600   # секунд между автоматическими вызовами rebuild

# ── Вариации ─────────────────────────────────────────────────────────────────
#
#   var=0  простая сумма T1 / базовый экстремум
#   var=1  только крупные свечи (range > avg_range)
#   var=2  T1 × |T1| — квадратичное усиление
#   var=3  крупные свечи + квадрат
#   var=4  амплитуда: (range − avg_range) вместо T1

VAR_RANGE = [0, 1, 2, 3, 4]


# ══════════════════════════════════════════════════════════════════════════════
# model() — ОСНОВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════════════════════════════════════════

def model(rates, dataset, date, *, type=0, var=0, param="", ctx):
    """
    Вычисляет веса T1 + Extremum по NER-контекстам новостей.

    Параметры:
        rates   — входные ставки (не используются напрямую)
        dataset — набор данных
        date    — текущая дата
        type    — 0=оба режима, 1=только T1, 2=только Extremum
        var     — вариант расчёта (0..4)
        param   — доп. параметры (не используются)
        ctx     — контекст с методами compute_t1, compute_extremum и др.

    Возвращает:
        словарь {weight_code: значение}
    """
    if not rates:
        return {}

    observations = ctx.find_events()
    if not observations:
        return {}

    result = {}
    prev   = ctx.prev_candle
    du     = ctx.delta_unit

    rates_t1      = ctx.rates_t1
    candle_ranges = ctx.candle_ranges
    avg_range     = ctx.avg_range

    for event_key, obs_dt, shift in observations:
        valid_dts = ctx.event_history(event_key)
        if not valid_dts:
            continue

        t_dates = [d + du * shift for d in valid_dts
                   if (d + du * shift) < date]
        if not t_dates:
            continue

        ctx_id = event_key[0]

        # ── T1 (type=0 или type=1) ────────────────────────────────────────────
        if type in (0, 1):
            if var == 0:
                t1 = ctx.compute_t1(t_dates)
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
            _, is_bull = prev
            ext_set    = ctx.extremums["max" if is_bull else "min"]
            total_hist = len(valid_dts)

            if var == 0:
                ext = ctx.compute_extremum(t_dates, is_bull=is_bull, total_hist=total_hist)
            elif var == 1:
                pool = [d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range]
                if pool and total_hist > 0:
                    ext = (
                        (sum(1 for d in pool if d in ext_set) / total_hist) * 2 - 1
                    ) * ctx.modification
                    ext = ext if ext != 0 else None
                else:
                    ext = None
            elif var == 2:
                pool = [d for d in t_dates if d in ext_set]
                if pool and total_hist > 0:
                    ext = sum(
                        (v := rates_t1.get(d, 0.0)) * abs(v)
                        for d in pool
                    ) / total_hist * ctx.modification
                    ext = ext if ext != 0 else None
                else:
                    ext = None
            elif var == 3:
                pool = [
                    d for d in t_dates
                    if d in ext_set and candle_ranges.get(d, 0.0) > avg_range
                ]
                if pool and total_hist > 0:
                    ext = (len(pool) / total_hist * 2 - 1) * ctx.modification
                    ext = ext if ext != 0 else None
                else:
                    ext = None
            elif var == 4:
                pool = [d for d in t_dates if d in ext_set]
                ext  = sum(
                    candle_ranges.get(d, 0.0) - avg_range
                    for d in pool
                    if candle_ranges.get(d, 0.0) > avg_range
                )
                ext = ext if ext != 0 else None
            else:
                ext = None

            if ext is not None:
                wc = f"NW{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}
