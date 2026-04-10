# Brain Framework

**Фреймворк для создания микросервисов торговых сигналов на основе событийных датасетов**

Brain Framework — это библиотека для быстрой разработки микросервисов, которые анализируют **события** (землетрясения, новости, календарные даты) и генерируют **числовые сигналы (веса)** для нейронной сети. 
Сигналы строятся на основе исторических движений цены (T1) и вероятности локальных экстремумов.

Фреймворк берёт на себя:
- загрузку котировок из БД,
- кэширование результатов,
- бэктестирование стратегий,
- REST API (FastAPI) со всеми нужными эндпоинтами,
- автоматический перезагруз данных и фоновое обновление.

Разработчику остаётся только:
1. Описать **типы событий** (контекстный индекс).
2. Сгенерировать **таблицу весов** (все возможные комбинации тип×сдвиг×режим).
3. Написать функцию **`model()`** — в ней реализовать логику поиска событий и вычисления сигналов.

## Архитектура микросервиса

Любой микросервис состоит из **трёх компонентов**:

| Компонент | Запуск | Что делает |
|-----------|--------|-------------|
| `create_context_idx.py` | один раз | Группирует датасет в типы, сохраняет в `ctx_index` |
| `create_weights.py` | один раз | Генерирует все возможные `weight_code` (тип×режим×сдвиг) |
| `model()` | каждый вызов | По текущей дате ищет события, вычисляет сигналы |

Фреймворк сам:
- загружает индексы и таблицу весов в память,
- строит кэш для всех комбинаций `type` (0,1,2) и `var` из `VAR_RANGE`,
- проводит бэктест и формирует `/summary`,
- перезагружает данные каждый час.

## Структура папки

```
Brain-Services/
  shared/
    brain_framework.py   # НЕ ТРОГАТЬ
    common.py, cache_helper.py ...
  37/                    # микросервис
    model.py             # ВСЯ ТВОЯ ЛОГИКА
    server.py            # прокладка (не трогать)
    .env                 # PORT, SERVICE_ID, NODE_NAME, SERVICE_TEXT
    create_context_idx.py
    create_weights.py
```

`server.py` — стандартный, не меняется:

## Что нужно определить в `model.py`

### 1. Конфигурация (обязательные поля)

```python
WEIGHTS_TABLE = "vlad_eq_weights"       # таблица весов
CTX_TABLE = "vlad_eq_context_idx"       # контекстный индекс
CTX_KEY_COLUMNS = ["id"]                # по каким колонкам индексировать ctx_index
DATASET_QUERY = "SELECT ... FROM events" # SQL запрос к датасету
DATASET_ENGINE = "brain"                # engine: "vlad" или "brain"
FILTER_DATASET_BY_DATE = True           # обрезать датасет по target_date
SHIFT_WINDOW = 24                       # максимальный сдвиг в часах/днях
RECURRING_MIN_COUNT = 2                 # минимальное число повторений для сдвигов >0
VAR_RANGE = [0, 1, 2, 3]               # твои гипотезы
```

### 2. Функция `model()`

**Сигнатура**:

```python
def model(rates, dataset, date, *, type=0, var=0, param="", ctx):
    """
    rates    : list[dict] — котировки <= date (open, close, min, max, date)
    dataset  : list[dict] — события из DATASET_QUERY (если FILTER_BY_DATE=True — уже <= date)
    date     : datetime    — текущая целевая свеча
    type     : 0 = T1+Extremum, 1 = только T1, 2 = только Extremum
    var      : int         — номер гипотезы из VAR_RANGE
    param    : str         — необязательный параметр
    ctx      : _ModelContext — контекст с методами:
                .ctx_index      — словарь {ключ: строка_из_CTX_TABLE}
                .rates_t1       — dict {дата: T1}
                .extremums      — dict {"min": set(), "max": set()}
                .find_events()  — возвращает список (event_key, obs_dt, shift)
                .compute_t1(t_dates) -> float
                .compute_extremum(t_dates, is_bull, total_hist) -> float
                .delta_unit     — timedelta(hours=1) или timedelta(days=1)
    """
    # Твоя логика: ищешь события в dataset, классифицируешь,
    # находишь ctx_id через ctx.ctx_index, вычисляешь сигнал.
    result = {}
    for event in dataset:
        # ... определяешь тип, shift, ctx_id
        wc = f"{ctx_id}_0_{shift}"
        result[wc] = result.get(wc, 0.0) + computed_value
    return result   # словарь вида {"123_0_3": 0.045, ...}
```

### 3. инкрементальное обновление индекса

- `rebuild_index(engine_vlad, engine_brain, engine_super) -> dict` — для инкрементального обновления контекстного индекса.
- `LABEL_FN = lambda key: ...` — для человекочитаемых имён событий в нарративе.

## Как работает контекстный индекс (ctx_index)

Фреймворк загружает всю таблицу `CTX_TABLE` в `ctx.ctx_index`. Индексация происходит по колонкам, указанным в `CTX_KEY_COLUMNS`. Пример:

```python
# CTX_KEY_COLUMNS = ["id"]
ctx.ctx_index = {
    (7,): {"id": 7, "mag_class": "large", "tsunami": 1, "occurrence_count": 5, ...},
    (8,): {...}
}
```

Ты строишь обратный маппинг для быстрого поиска по своим признакам (например, `(mag_class, tsunami, alert_level) → ctx_id`).

## Таблица весов (weights)

Фреймворк ожидает таблицу с колонкой `weight_code`. Скрипт `create_weights.py` генерирует все возможные коды:

```
{ctx_id}_{mode}_{shift}
```

- `mode = 0` — сумма движений цены за историю (T1)
- `mode = 1` — вероятность локального экстремума
- `shift` — от 0 до `SHIFT_WINDOW` (для редких типов только shift=0)

## Параметр `var` — твои гипотезы

Фреймворк автоматически бэктестит все значения из `VAR_RANGE`. Например:

| var | Гипотеза |
|-----|-----------|
| 0   | Базовый вес = T1 |
| 1   | Учитывать только события с avg_significance >= 400 |
| 2   | Взвешивать T1 по глубине |
| 3   | Игнорировать события, которые не ощущались людьми |

Внутри `model()` ты проверяешь `var` и модифицируешь сигнал.

## Эндпоинты

| Эндпоинт | Что делает |
|----------|-------------|
| `GET /` | Статус сервиса, количество весов, ctx_index, dataset |
| `GET /weights` | Список всех weight_code |
| `GET /values?pair=1&day=0&date=...&type=0&var=0` | Рассчитать сигнал для конкретной свечи. Возвращает `{wc: float}` и `details` (нарратив). |
| `GET /fill_cache?pairs=1,3,4&days=0,1` | Фоновое заполнение кэша для всех type×var. |
| `GET /fill_status` | Прогресс заполнения кэша. |
| `GET /backtest?pairs=...&days=...&type=0&var=0` | Бэктест по кэшу. |
| `GET /summary?pair=1&day=0` | Лучшие результаты (best_accuracy, best_score) для инструмента. |
| `GET /pretest` | Три теста: синтаксис, структура model(), покрытие (≥90% дат должны давать непустой результат). |
| `GET /posttest` | Расширенные тесты: проплешины, симуляция торговли, качество сигналов. |
| `GET /reload` | Принудительная перезагрузка ctx_index, dataset, весов. |
| `GET /rebuild_index` | Запуск `rebuild_index()` (если определён). |
| `GET /patch` | Инкремент версии микросервиса. |
| `GET /universe_sync` | Синхронизация weight_code из кэша в общую таблицу `vlad_weights_universe`. |

## Пайплайн разработки (от нуля до деплоя)

1. **Создать `create_context_idx.py`** — сгруппировать датасет в типы, сохранить в `CTX_TABLE`.
2. **Создать `create_weights.py`** — сгенерировать все weight_code.
3. **Запустить `server.py`** — проверить логи: `RELOAD DONE: weights=N ctx=N dataset=N`.
4. **Вызвать `GET /pretest`** — убедиться, что все тесты проходят (покрытие ≥90%).
5. **Вызвать `GET /fill_cache`** — дождаться завершения (следить через `/fill_status`).
6. **Вызвать `GET /posttest`** 
7. **Вызвать `GET /summary`** 

## Пример: USGS Earthquakes (землетрясения M4.5+)

- **Типы событий**: `mag_class (small/medium/large) × tsunami (0/1) × alert_level (none/green/...)`
- **Агрегаты в ctx_index**: `avg_significance`, `avg_depth_km`, `felt_ratio`
- **Гипотезы var**:
  - var=0: базовый T1
  - var=1: только значимые (avg_significance >= 400)
  - var=2: взвешивание по глубине
  - var=3: только ощутимые (felt_ratio >= 10%)
- **Функция model()**: ищет землетрясения в окне `±SHIFT_WINDOW`, определяет тип, получает `ctx_id` через обратный маппинг, вычисляет `weighted_t1` и `extremum`, возвращает словарь `{wc: value}`.
