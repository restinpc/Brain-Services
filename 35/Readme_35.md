# README — Сервис 35: brain-news-weights

**SERVICE_ID:** 35 | **PORT:** 8898 | **NODE_NAME:** brain-news-weights  
**Файлы:** `server.py`, `news_index_updater.py`, `context_idx.py`, `weights.py`

---

## Что делает этот сервис

Отвечает на вопрос: **"Когда в прошлом публиковались новости с похожим NER-профилем,
как вёл себя рынок через N часов?"**

NER-профиль (fingerprint) строится из трёх тегов: `person | location | misc`.
Например, новость "Trump meets EU leaders in Brussels" → `trump | brussels | trade`.
Каждый уникальный набор тегов = один `ctx_id` в таблице `vlad_news_context_idx`.

---

## Таблицы БД

| Таблица | Где | Что хранит |
|---------|-----|-----------|
| `vlad_news_context_idx` | vlad | Уникальные NER-fingerprints: id, feed_cat, person_token, location_token, misc_token, fingerprint_hash, occurrence_count |
| `vlad_news_ctx_map` | vlad | Маппинг: news_id → ctx_id + news_date |
| `vlad_news_weights_table` | vlad | Все возможные weight_codes: NW{ctx_id}_{mode}_{shift} |
| `brain_cnn_ner` и др. | brain | Сырые NER-теги от 5 источников |
| `brain_rates_eur_usd[_day]` и др. | brain | Свечные данные для расчёта T1 и экстремумов |

---

## Формат weight_code

```
NW{ctx_id}_{mode}_{shift}
```

| Часть | Тип | Значение |
|-------|-----|----------|
| `NW` | prefix | News Weights — отличает от других сервисов |
| `ctx_id` | int | ID fingerprint из vlad_news_context_idx |
| `mode` | 0 или 1 | 0 = T1 sum, 1 = Extremum probability |
| `shift` | 0..24 | Сдвиг в часах между новостью и целевой датой |

**Примеры:**
```
NW42_0_0    ctx=42, T1,       shift=0  (в момент публикации)
NW42_0_3    ctx=42, T1,       shift=3ч после публикации
NW42_1_12   ctx=42, Extremum, shift=12ч после публикации
NW7_0_0     ctx=7,  T1,       нерегулярный (shift всегда 0)
```

---

## Алгоритм — Жизненный цикл сервера

### Старт и фоновый reload (каждые 3600 сек)

```
preload_all_data()                         ← BaseBrainService
  │
  ├─ load_my_data(conn_vlad, conn_brain)   ← NewsWeightsService (наш код)
  │    │
  │    ├─ [Шаг 0] run_incremental_update() via asyncio.to_thread
  │    │    ├─ find_new_news_ids()          найти необработанные news_id
  │    │    ├─ load_ner_for_new_ids()       загрузить NER только для них
  │    │    ├─ build_fp_records()           fingerprint = консенсус >= 2/5 моделей
  │    │    ├─ upsert_context_idx()         INSERT новые / UPDATE count существующих
  │    │    ├─ insert_ctx_map()             пометить как обработанные
  │    │    └─ update_weight_codes()        дописать коды для новых и upgraded
  │    │
  │    ├─ [Шаг 1] SELECT weight_code FROM vlad_news_weights_table
  │    │          → self.weight_codes[]
  │    │
  │    ├─ [Шаг 2] SELECT id, occurrence_count FROM vlad_news_context_idx
  │    │          → self.ctx_index[(ctx_id,)] = {occurrence_count: N}
  │    │
  │    └─ [Шаг 3] SELECT ctx_id, news_date FROM vlad_news_ctx_map
  │               → self.register_event(dt, (ctx_id,))
  │                   → self._events_by_dt[dt].append((ctx_id,))
  │                   → self._event_history[(ctx_id,)].append(dt)
  │
  ├─ _build_sorted_arrays()                bisect-индекс по датам событий
  └─ _load_rates()                         brain_rates_* таблицы в RAM
```

---

## Алгоритм — Расчёт весов (GET /values)

**Входные параметры:**

| Параметр | Тип | Описание |
|---------|-----|----------|
| `pair` | int | 1=EURUSD, 3=BTC, 4=ETH |
| `day` | int | 0=часовые свечи, 1=дневные |
| `date` | str | Целевая дата, например `2025-10-03 12:00:00` |
| `type` | int | 0=T1+Ext, 1=только T1, 2=только Ext |
| `var` | int | 0=raw, 1=vol_filter, 2=squared, 3=flt+sq, 4=range |

**Выход:** `{weight_code: float}` — словарь ненулевых значений

---

### Шаг 1: Валидация и подготовка

**1.1** Распарсить `date` → `target_date: datetime`  
→ если не распарсилось → вернуть `None`

**1.2** Определить `rates_table` по `pair` и `day`:  
```
pair=1, day=0 → brain_rates_eur_usd
pair=1, day=1 → brain_rates_eur_usd_day
pair=3, day=0 → brain_rates_btc_usd
```

**1.3** Подгрузить свежие свечи (`_refresh_rates_if_needed`):  
→ если прошло > 30 сек с последнего обновления для этой таблицы → SELECT новые свечи из БД  
→ нужно для актуальности T1 последних часов между reload'ами

**1.4** Определить `delta_unit`:  
```
day=0 → timedelta(hours=1)
day=1 → timedelta(days=1)
```

**1.5** Определить `modification_factor` (масштаб инструмента для mode=1):  
```
pair=1 → 0.001   (EURUSD двигается на ~0.0005)
pair=3 → 1000.0  (BTC двигается на ~500)
pair=4 → 100.0
```

---

### Шаг 2: Построить окно поиска событий

**2.1** Сформировать список дат для проверки:  
```
check_dts = [target_date + delta_unit * s
             for s in range(-shift_window, shift_window + 1)]
             = range(-24, 25) = 49 точек
```

**2.2** Отфильтровать будущие даты (`filter_future_events=True`):  
```
[dt for dt in check_dts if dt <= target_date]
```
→ остаётся 25 точек (от `target_date - 24h` до `target_date`)

---

### Шаг 3: Найти наблюдения через bisect

Для каждой `dt` из окна поиска:

**3.1** Найти диапазон `[_l, _r)` в `self._sorted_dates` через `bisect_left`:  
```
_l = bisect_left(_sorted_dates, dt)
_r = bisect_left(_sorted_dates, dt + delta_unit)
```
→ находит все события в интервале `[dt, dt + 1ч)`

**3.2** Для каждого совпавшего события:  
```
shift = round((target_date - obs_dt) / delta_unit)
```
→ shift=0 значит "новость вышла в тот же час"  
→ shift=3 значит "новость вышла 3 часа назад"

**3.3** Собрать список `observations = [(event_key, obs_dt, shift), ...]`

Если `observations` пустой → вернуть `{}`

---

### Шаг 4: Фильтрация по регулярности

Для каждого `(event_key, obs_dt, shift)`:

**4.1** Получить `ctx_info = self.ctx_index.get(event_key)`  
→ если нет → пропустить (нет в индексе)

**4.2** Проверить регулярность:  
```
is_recurring = occurrence_count >= recurring_min_count (= 2)
```

**4.3** Применить правила:  
```
Нерегулярный (is_recurring=False):
  → брать только если shift == 0
  → иначе: пропустить

Регулярный (is_recurring=True):
  → брать если |shift| <= shift_window (24)
  → иначе: пропустить
```

---

### Шаг 5: Взять историю ctx_id

**5.1** Получить полную историю: `all_hist = self._event_history[event_key]`  
→ список всех дат, когда этот ctx_id встречался (отсортирован)

**5.2** Отрезать будущее (защита от look-ahead bias):  
```
idx = bisect_left(all_hist, target_date)
valid_dts = all_hist[:idx]
```
→ берём только прошлые вхождения

Если `valid_dts` пустой → пропустить

---

### Шаг 6: Построить целевые даты T

**6.1** Для каждой исторической даты `d` в `valid_dts`:  
```
t_dates = [d + delta_unit * shift
           for d in valid_dts
           if (d + delta_unit * shift) < target_date]
```
→ "смотрим на свечу через `shift` часов после прошлого вхождения этого ctx_id"  
→ пример: ctx_id=42 встречался 10 раз → t_dates содержит до 10 дат

**6.2** Определить `shift_arg`:  
```
is_recurring → shift_arg = shift
нерегулярный → shift_arg = None
```
→ make_weight_code использует shift_arg для формирования кода:  
  `None` → 0 (нерегулярный, всегда shift=0 в коде)

---

### Шаг 7: Mode 0 — вычислить T1 sum

Выполняется если `calc_type in (0, 1)`:

**7.1** Вызвать `compute_t1_value(t_dates, calc_var, ram_rates, ram_ranges, avg_range)`:

| `calc_var` | Логика |
|-----------|--------|
| 0 | Простая сумма T1 по всем t_dates |
| 1 | Только свечи с `range > avg_range` (крупные) |
| 2 | `T1 × \|T1\|` (квадратичное усиление знака) |
| 3 | Фильтр крупных + квадрат |
| 4 | Сумма `(range - avg_range)` вместо T1 |

**7.2** Сформировать код: `wc = make_weight_code(event_key, mode=0, shift_arg)`  
**7.3** Добавить: `result[wc] += t1_sum`

---

### Шаг 8: Mode 1 — вычислить Extremum probability

Выполняется если `calc_type in (0, 2)` И есть `prev_candle`:

**8.1** Найти `prev_candle` — последнюю свечу строго перед `target_date` через bisect  
→ если нет свечи → пропустить mode=1

**8.2** Определить направление экстремума:  
```
prev_candle бычья (close > open) → ищем max-экстремумы (вершины)
prev_candle медвежья             → ищем min-экстремумы (впадины)
```

**8.3** Вызвать `compute_extremum_value(t_dates, calc_var, ext_set, ...)`:  
→ считает: какая доля из `t_dates` совпала с экстремумом  
→ нормализует: `(matches / total_hist) × 2 - 1` → диапазон `[-1, +1]`  
→ умножает на `modification_factor`

**8.4** Если результат ≠ 0:  
→ `wc = make_weight_code(event_key, mode=1, shift_arg)`  
→ `result[wc] += ext_val`

---

### Шаг 9: Вернуть результат

```python
return {k: round(v, 6) for k, v in result.items() if v != 0}
```

Нулевые значения исключаются: "нет сигнала = нет в ответе".  
Округление до 6 знаков.

---

## Алгоритм — Инкрементальное обновление индекса (`news_index_updater.py`)

### Когда вызывается

Автоматически при каждом `preload_all_data()` (старт + каждые 3600 сек).  
Также можно запустить вручную: `python news_index_updater.py`

### Алгоритм обновления (по шагам)

**1. Найти новые news_id**  
→ Читаем все `news_id` из brain_*_ner таблиц  
→ Вычитаем уже обработанные из `vlad_news_ctx_map`  
→ Результат: множество необработанных ids

**2. Загрузить NER только для новых**  
→ SELECT ... WHERE news_id IN (...) — не грузим всю историю

**3. Построить fingerprints**  
→ Консенсус >= 2 из 5 моделей по каждому токену  
→ `fingerprint_hash = MD5(feed_cat|person|location|misc)[:16]`

**4. Upsert в vlad_news_context_idx**  
→ Новый fingerprint → `INSERT`  
→ Существующий → `UPDATE occurrence_count += N, last_dt = max(last_dt, новая_дата)`

**5. Добавить в vlad_news_ctx_map**  
→ `INSERT IGNORE` — безопасно при повторном запуске

**6. Дополнить vlad_news_weights_table**  
→ Новый ctx_id (count=1) → только shift=0 × 2 mode = 2 кода  
→ ctx_id перешёл через порог 2 → дописать shift 1..24 × 2 mode = 48 кодов  
→ `INSERT IGNORE` — не трогает существующие

---

## Алгоритм — Загрузка котировок (`_load_rates`)

**1.** Для каждой из 6 таблиц brain_rates_*:

**2.** SELECT date, open, close, max, min, t1 — все строки

**3.** Построить в RAM:
```
rates[table]          = {datetime: t1_float}
last_candles[table]   = [(datetime, is_bull), ...]
candle_ranges[table]  = {datetime: high - low}
avg_range[table]      = mean(all ranges)
```

**4.** Экстремумы через SQL self-join:
```sql
SELECT t1.date FROM `table` t1
JOIN `table` t_prev ON t_prev.date = t1.date - INTERVAL 1 HOUR
JOIN `table` t_next ON t_next.date = t1.date + INTERVAL 1 HOUR
WHERE t1.max > t_prev.max AND t1.max > t_next.max   -- max-экстремум
```
→ `extremums[table]["max"] = set(datetime, ...)`  
→ аналогично для min-экстремумов

---

## Структура файлов сервиса

```
35/
├── server.py               ← запуск: python server.py
├── news_index_updater.py   ← инкрементальное обновление индекса
├── context_idx.py          ← первичная сборка (запускать один раз)
├── weights.py              ← первичная генерация (запускать один раз)

shared/
├── brain_framework.py      ← BaseBrainService + все утилиты
├── common.py               ← log, build_engines, ok/err response
└── cache_helper.py         ← cached_values, ensure_cache_table
```
## Известные ограничения

| Ограничение | Последствие |
|------------|-------------|
| NER без текста | Трамп+Европа+Торговля = Трамп+Европа+Война если misc-токен одинаковый |
| 3 тега максимум | Потеря нюансов для многоагентных новостей |
| MySQL FULLTEXT не используется | Нет мягкого текстового совпадения |
| MATCH AGAINST поможет | Для следующей версии модели |
| Синхронный mysql.connector в updater | Блокирует поток, нужен asyncmy в будущем |
