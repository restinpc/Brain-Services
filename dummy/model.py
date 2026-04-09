"""
model.py — Болванка для нового микросервиса.

Критичные переменные (PORT, NODE_NAME, SERVICE_ID, SERVICE_TEXT)
лежат в .env рядом с этим файлом — НЕ дублируй их здесь.

Минимум для запуска:
  1. Реализуй тело model() под свой датасет.
  2. Раскомментируй и заполни нужные таблицы ниже.

Фреймворк автоматически:
  - строит нарратив details в /values из payload (по weight_code)
  - ищет имена через LABEL_FN → ctx_index → fallback "ctx_id=N"
  - заполняет кэш для type∈[0,1,2] × var∈VAR_RANGE через /fill_cache
  - пишет бэктест в vlad_backtest_results через /backtest
"""

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ (раскомментируй и заполни то, что нужно)
# ══════════════════════════════════════════════════════════════════════════════

# ── Таблица весовых кодов ─────────────────────────────────────────────────────
# WEIGHTS_TABLE = "vlad_my_weights_table"

# ── Контекстный индекс ────────────────────────────────────────────────────────
# CTX_TABLE        = "vlad_my_context_idx"
# CTX_KEY_COLUMNS  = ["id"]          # колонки составного ключа контекста

# ── События ───────────────────────────────────────────────────────────────────
# EVENTS_TABLE      = "vlad_my_events"   # или используй EVENTS_QUERY
# EVENTS_QUERY      = """
#     SELECT ctx_id, event_date
#     FROM vlad_my_events
#     WHERE event_date IS NOT NULL
#     ORDER BY event_date
# """
# EVENTS_ENGINE     = "vlad"             # "vlad" | "brain" | "super"
# EVENT_KEY_COLUMNS = ["ctx_id"]
# EVENT_DATE_COLUMN = "event_date"
# SHIFT_WINDOW      = 12                 # ± N часов/дней от target_date

# ── Dataset ────────────────────────────────────────────────────────────────────
# DATASET_TABLE          = "vlad_my_dataset"
# DATASET_QUERY          = "SELECT * FROM vlad_my_dataset ORDER BY date"
# FILTER_DATASET_BY_DATE = False         # True → dataset фильтруется <= date

# ── Автоматический rebuild через /rebuild_index ───────────────────────────────
# Вариант A — универсальный (без написания кода):
# REBUILD_SOURCE_QUERY  = "SELECT id, date_col FROM my_table WHERE id > :last_id ORDER BY id"
# REBUILD_SOURCE_ENGINE = "vlad"
# REBUILD_ID_COLUMN     = "id"
# REBUILD_DATE_COLUMN   = "date_col"
# REBUILD_SHIFT_MAX     = 0
#
# Вариант B — кастомный: напиши async def rebuild_index(...) в конце файла.
#
# REBUILD_INTERVAL = 3600   # секунд между автовызовами (0 = выключено)

# ── Дефолтный date_from для /fill_cache ───────────────────────────────────────
# CACHE_DATE_FROM = "2025-01-15"   # дефолт фреймворка, можно переопределить

# ── Вариации ──────────────────────────────────────────────────────────────────
VAR_RANGE = [0]   # ← заполни под свои нужды, например [0, 1, 2, 3, 4]

# ── Человекочитаемые имена для нарратива в /values ────────────────────────────
#
# Фреймворк декодирует weight_code как PREFIX{ctx_id}_{mode}_{shift}
# и ищет имя события в порядке приоритета:
#   1. LABEL_FN((ctx_id,))          ← задаёшь здесь
#   2. ctx_index[key]["person_token" | "ctx_key" | "event_name" | "name"]
#   3. fallback: "ctx_id=N"
#
# Пример — простой словарь:
# _MY_LABELS = {1: "Federal Reserve Minutes", 42: "CPI Release"}
# LABEL_FN = lambda key: _MY_LABELS.get(key[0])
#
# Пример — из ctx_index (если там есть поле "title"):
# LABEL_FN = lambda key: None   # фреймворк сам найдёт в ctx_index
#
# Если ctx_index содержит читаемое поле (person_token, ctx_key, event_name, name)
# — LABEL_FN можно не задавать совсем.


# ══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИЯ МОДЕЛИ
# ══════════════════════════════════════════════════════════════════════════════

def model(rates, dataset, date, *, type=0, var=0, param="", ctx):
    """
    Аргументы (сигнатуру не менять — фреймворк передаёт их автоматически):

        rates   — котировки строго <= date:
                  [{"date": datetime, "open": float, "close": float,
                    "min": float, "max": float}, ...]

        dataset — записи из DATASET_TABLE/DATASET_QUERY.
                  Если FILTER_DATASET_BY_DATE = True — уже отфильтрован <= date.

        date    — точка расчёта (datetime)

        type    — 0=T1+Extremum, 1=только T1, 2=только Extremum
        var     — вариация из VAR_RANGE
        param   — произвольная строка (зарезервировано)

        ctx     — контекст фреймворка:
                    ctx.find_events()                          → [(event_key, obs_dt, shift), ...]
                    ctx.event_history(event_key)               → [datetime, ...]  (до date)
                    ctx.compute_t1(t_dates)                    → float
                    ctx.compute_extremum(t_dates, is_bull,
                                         total_hist)           → float | None
                    ctx.prev_candle                            → (datetime, is_bull) | None
                    ctx.delta_unit                             → timedelta
                    ctx.modification                           → float (0.001/1000/100)
                    ctx.ctx_index                              → dict[tuple, dict]
                    ctx.rates_t1                               → dict[datetime, float]
                    ctx.candle_ranges                          → dict[datetime, float]
                    ctx.avg_range                              → float
                    ctx.extremums                              → {"min": set, "max": set}

    Возвращает dict[str, float]:
        {"PREFIX{ctx_id}_{mode}_{shift}": value, ...}

    Соглашение по weight_code (фреймворк декодирует для нарратива):
        PREFIX  — любые буквы (выбери свой: "MY", "NW", "CAL" и т.д.)
        ctx_id  — числовой id контекста (event_key[0])
        mode    — 0=T1, 1=Extremum
        shift   — сдвиг от -SHIFT_WINDOW до 0

    Если ctx не нужен — убери его из сигнатуры, фреймворк определит
    это автоматически через inspect.
    """
    if not rates:
        return {}

    # Получаем события в окне ±SHIFT_WINDOW вокруг date
    observations = ctx.find_events()
    if not observations:
        return {}

    result = {}
    prev   = ctx.prev_candle    # (datetime, is_bull) | None
    du     = ctx.delta_unit     # timedelta(hours=1) | timedelta(days=1)

    for event_key, obs_dt, shift in observations:
        # История этого контекста строго до target_date
        valid_dts = ctx.event_history(event_key)
        if not valid_dts:
            continue

        # Смотрим на свечи со сдвигом shift относительно каждого события
        t_dates = [d + du * shift for d in valid_dts
                   if (d + du * shift) < date]
        if not t_dates:
            continue

        ctx_id = event_key[0]   # числовой id контекста

        # ── T1 ────────────────────────────────────────────────────────────────
        if type in (0, 1):
            # var=0: стандартный compute_t1 (numpy-ускорен)
            t1 = ctx.compute_t1(t_dates)

            # ── Вариации (раскомментируй нужные и добавь в VAR_RANGE) ─────────
            #
            # var=1 — только крупные свечи (range > avg_range):
            # if var == 1:
            #     t1 = sum(
            #         ctx.rates_t1.get(d, 0.0) for d in t_dates
            #         if ctx.candle_ranges.get(d, 0.0) > ctx.avg_range
            #     )
            #
            # var=2 — T1 × |T1| (квадратичное усиление):
            # elif var == 2:
            #     t1 = sum(
            #         (v := ctx.rates_t1.get(d, 0.0)) * abs(v)
            #         for d in t_dates
            #     )
            #
            # var=3 — крупные свечи + квадрат:
            # elif var == 3:
            #     t1 = sum(
            #         (v := ctx.rates_t1.get(d, 0.0)) * abs(v)
            #         for d in t_dates
            #         if ctx.candle_ranges.get(d, 0.0) > ctx.avg_range
            #     )
            #
            # var=4 — амплитуда (range - avg_range):
            # elif var == 4:
            #     t1 = sum(
            #         ctx.candle_ranges.get(d, 0.0) - ctx.avg_range
            #         for d in t_dates
            #         if ctx.candle_ranges.get(d, 0.0) > ctx.avg_range
            #     )

            if t1 != 0.0:
                wc = f"MY{ctx_id}_0_{shift}"   # ← замени MY на свой префикс
                result[wc] = result.get(wc, 0.0) + t1

        # ── Extremum ──────────────────────────────────────────────────────────
        if type in (0, 2) and prev is not None:
            _, is_bull = prev
            ext = ctx.compute_extremum(
                t_dates,
                is_bull=is_bull,
                total_hist=len(valid_dts),
            )
            if ext is not None:
                wc = f"MY{ctx_id}_1_{shift}"   # ← тот же префикс, mode=1
                result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ══════════════════════════════════════════════════════════════════════════════
# REBUILD INDEX (опционально — только если нужна кастомная логика)
# ══════════════════════════════════════════════════════════════════════════════

# Раскомментируй и реализуй если универсального REBUILD_SOURCE_QUERY недостаточно.
# Фреймворк вызовет эту функцию через /rebuild_index и раз в REBUILD_INTERVAL сек.
# Если функции нет — используется универсальный rebuild по REBUILD_SOURCE_QUERY.
#
# async def rebuild_index(engine_vlad, engine_brain, engine_super=None) -> dict:
#     """Инкрементально обновляет контексты, веса и события. Идемпотентна."""
#     from sqlalchemy import text
#
#     # 1. Узнаём последний обработанный id
#     async with engine_vlad.connect() as conn:
#         row = (await conn.execute(
#             text("SELECT MAX(id) FROM vlad_my_ctx_map")
#         )).fetchone()
#     last_id = int(row[0]) if row and row[0] else 0
#
#     # 2. Загружаем новые записи
#     async with engine_vlad.connect() as conn:
#         res = await conn.execute(text("""
#             SELECT id, ctx_key, event_date FROM my_source_table
#             WHERE id > :last_id ORDER BY id
#         """), {"last_id": last_id})
#         rows = [dict(r) for r in res.mappings().all()]
#
#     if not rows:
#         return {"processed": 0, "new_contexts": 0, "new_weights": 0, "new_events": 0}
#
#     # 3. Используй хелперы фреймворка:
#     # from brain_framework import (
#     #     ensure_ctx_table, upsert_ctx_rows,
#     #     ensure_weights_table, insert_weight_codes,
#     #     ensure_events_table, insert_events,
#     # )
#
#     return {
#         "processed":   len(rows),
#         "new_contexts": 0,
#         "new_weights":  0,
#         "new_events":   0,
#     }