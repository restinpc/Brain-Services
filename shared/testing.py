"""
testing.py — логика предварительного и пост-тестирования модели.

v4 — финальная версия:
  - NumPy-оптимизированный _build_history (vectorized stats)
  - Pretest: синтаксис → структура → покрытие 90%
  - Posttest: статистика → знаки → sync → проплешины → симуляция
  - Все тяжёлые операции через asyncio.to_thread()
  - filter_dataset_by_date пробрасывается из server.py

Импортируется из server.py, не требует изменений при смене model.py.
"""

import ast
import os
import random
import importlib
import inspect
import asyncio
from datetime import datetime
from typing import Callable

import numpy as np

# ══════════════════════════════════════════════════════════════════════════════
# Константы инструментов
# ══════════════════════════════════════════════════════════════════════════════

INSTRUMENTS: dict[int, dict[str, str]] = {
    1: {"hour": "brain_rates_eur_usd", "day": "brain_rates_eur_usd_day"},
    3: {"hour": "brain_rates_btc_usd", "day": "brain_rates_btc_usd_day"},
    4: {"hour": "brain_rates_eth_usd", "day": "brain_rates_eth_usd_day"},
}

PAIR_LABELS = {1: "EURUSD", 3: "BTCUSD", 4: "ETHUSD"}

_MODEL_PATH = os.path.join(os.path.dirname(__file__), "model.py")


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _reload_model():
    """Перезагружает модуль model.py и возвращает функцию model()."""
    import model as _m
    importlib.reload(_m)
    return _m.model


def _filter_lte(rows: list[dict], date: datetime) -> list[dict]:
    """
    Бинарный поиск: возвращает rows строго <= date.
    O(log n), rows должен быть отсортирован по date.
    """
    lo, hi = 0, len(rows)
    while lo < hi:
        mid = (lo + hi) // 2
        if rows[mid]["date"] <= date:
            lo = mid + 1
        else:
            hi = mid
    return rows[:lo]


def _filter_dataset_lte(dataset: list[dict], date: datetime) -> list[dict]:
    """
    Фильтрует dataset по дате строго <= date.
    Записи без ключа "date" не попадают в выборку.
    """
    return [e for e in dataset if e.get("date") is not None and e["date"] <= date]


# ══════════════════════════════════════════════════════════════════════════════
# PRETEST — 3 теста
# ══════════════════════════════════════════════════════════════════════════════

def _pt1_syntax() -> tuple[bool, str]:
    """
    Тест 1: Проверка синтаксиса model.py.
      - ast.parse() → синтаксических ошибок нет
      - importlib.reload() → нет ошибок импорта
      - hasattr(model) → функция model() существует
      - inspect.signature() → обязательные параметры rates, dataset, date
    """
    # 1a. Синтаксис через AST
    try:
        with open(_MODEL_PATH, "r", encoding="utf-8") as f:
            source = f.read()
        ast.parse(source)
    except SyntaxError as e:
        return False, f"SyntaxError в model.py (строка {e.lineno}): {e.msg}"
    except OSError as e:
        return False, f"Не удалось прочитать model.py: {e}"

    # 1b. Импорт
    try:
        import model as _m
        importlib.reload(_m)
    except Exception as e:
        return False, f"Ошибка импорта model.py: {e}"

    # 1c. Наличие функции model()
    if not hasattr(_m, "model"):
        return False, "model.py не определяет функцию 'model'"
    if not callable(_m.model):
        return False, "'model' в model.py не является callable"

    # 1d. Сигнатура
    try:
        sig = inspect.signature(_m.model)
        params = set(sig.parameters.keys())
    except Exception as e:
        return False, f"Не удалось получить сигнатуру model(): {e}"

    for required in ("rates", "dataset", "date"):
        if required not in params:
            return False, f"model() не имеет обязательного параметра '{required}'"

    return True, "ok"


def _pt2_output_structure(result) -> tuple[bool, str]:
    """
    Тест 2: Валидация структуры выхода model().
    Ожидается dict[str, float | int]. Пустой dict допустим.
    """
    if result is None:
        return False, "model() вернул None (возможно, некорректная дата)"
    if not isinstance(result, dict):
        return False, f"model() вернул {type(result).__name__}, ожидается dict"

    for k, v in result.items():
        if not isinstance(k, str):
            return False, f"Ключ {k!r} имеет тип {type(k).__name__}, ожидается str"
        if not isinstance(v, (int, float)):
            return False, (
                f"Значение по ключу '{k}' имеет тип {type(v).__name__}, "
                f"ожидается float"
            )
        if v != v:  # NaN check
            return False, f"Значение по ключу '{k}' равно NaN"
        if abs(v) == float("inf"):
            return False, f"Значение по ключу '{k}' равно Inf"

    return True, "ok"


def _pt3_coverage_sync(
    global_rates: dict[str, list[dict]],
    global_dataset: list[dict],
    filter_dataset_by_date: bool = False,
    n_samples: int = 10,
    threshold: float = 0.9,
) -> tuple[bool, str]:
    """
    Тест 3: 10 случайных дат по каждому инструменту.
    Тест пройден если ≥ 90% вызовов возвращают непустой dict.
    """
    model_fn = _reload_model()
    failures = []

    for pair_id, tfs in INSTRUMENTS.items():
        for tf_name, table in tfs.items():
            rows = global_rates.get(table, [])
            if not rows:
                failures.append(
                    f"{PAIR_LABELS[pair_id]}/{tf_name}: нет данных в GLOBAL_RATES"
                )
                continue

            # Берём пробы из второй половины истории (есть контекст)
            start = max(len(rows) // 2, 1)
            pool = rows[start:]
            if len(pool) < n_samples:
                pool = rows

            samples = random.sample(pool, min(n_samples, len(pool)))
            non_empty = 0
            call_errors = []

            for candle in samples:
                target_date = candle["date"]
                filtered = _filter_lte(rows, target_date)
                dataset = (
                    _filter_dataset_lte(global_dataset, target_date)
                    if filter_dataset_by_date
                    else global_dataset
                )
                try:
                    result = model_fn(
                        rates=filtered,
                        dataset=dataset,
                        date=target_date,
                    )
                    if result:
                        non_empty += 1
                except Exception as e:
                    call_errors.append(str(e))

            total = len(samples)
            rate = non_empty / total if total > 0 else 0.0

            if call_errors:
                failures.append(
                    f"{PAIR_LABELS[pair_id]}/{tf_name}: "
                    f"исключения при вызове — {call_errors[0]}"
                )
            elif rate < threshold:
                failures.append(
                    f"{PAIR_LABELS[pair_id]}/{tf_name}: "
                    f"только {non_empty}/{total} ({rate:.0%}) непустых "
                    f"(требуется ≥{threshold:.0%})"
                )

    if failures:
        return False, " | ".join(failures)
    return True, "ok"


async def run_pretest(
    global_rates: dict[str, list[dict]],
    global_dataset: list[dict],
    filter_dataset_by_date: bool = False,
) -> dict:
    """
    Точка входа для /pretest.
    Запускает 3 теста последовательно. При первой же ошибке → {status: error}.
    """

    # ── Тест 1: синтаксис ─────────────────────────────────────────────────────
    ok, msg = _pt1_syntax()
    if not ok:
        return {"status": "error", "error": f"[Тест 1 — Синтаксис] {msg}"}

    # ── Тест 2: структура выхода ──────────────────────────────────────────────
    sample_result = None
    for pair_id, tfs in INSTRUMENTS.items():
        table = tfs["hour"]
        rows = global_rates.get(table, [])
        if not rows:
            continue
        mid_candle = rows[len(rows) // 2]
        target_date = mid_candle["date"]
        filtered = _filter_lte(rows, target_date)
        dataset = (
            _filter_dataset_lte(global_dataset, target_date)
            if filter_dataset_by_date
            else global_dataset
        )
        model_fn = _reload_model()
        try:
            sample_result = await asyncio.to_thread(
                model_fn, filtered, dataset, target_date
            )
        except Exception as e:
            return {
                "status": "error",
                "error": f"[Тест 2 — Структура] model() выбросил: {e}",
            }
        break

    ok, msg = _pt2_output_structure(sample_result)
    if not ok:
        return {"status": "error", "error": f"[Тест 2 — Структура] {msg}"}

    # ── Тест 3: покрытие 90% ──────────────────────────────────────────────────
    ok, msg = await asyncio.to_thread(
        _pt3_coverage_sync,
        global_rates,
        global_dataset,
        filter_dataset_by_date,
    )
    if not ok:
        return {"status": "error", "error": f"[Тест 3 — Покрытие] {msg}"}

    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# POSTTEST — 5 тестов
# ══════════════════════════════════════════════════════════════════════════════

# ── Тест 1: статистика весов (NumPy-оптимизация) ─────────────────────────────

def _post1_stats(history: list[tuple[datetime, dict]]) -> dict:
    """
    min / max / avg количества ключей на тик (по непустым тикам).
    NumPy-векторизация вместо цикла.
    """
    counts = [len(r) for _, r in history if r]
    if not counts:
        return {"min": 0.0, "max": 0.0, "avg": 0.0}
    arr = np.array(counts, dtype=np.float64)
    return {
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "avg": round(float(np.mean(arr)), 4),
    }


# ── Тест 2: анализ знаков значений (NumPy-оптимизация) ───────────────────────

def _post2_signs(history: list[tuple[datetime, dict]]) -> dict:
    """
    Суммарное количество положительных и отрицательных значений.
    Собирает все значения в один np.array для векторизированного подсчёта.
    """
    all_values = []
    for _, result in history:
        if result:
            all_values.extend(result.values())

    if not all_values:
        return {"plus": 0, "minus": 0}

    arr = np.array(all_values, dtype=np.float64)
    return {
        "plus": int(np.count_nonzero(arr > 0)),
        "minus": int(np.count_nonzero(arr < 0)),
    }


# ── Тест 3: синхронный запрос с актуальной датой ─────────────────────────────

async def _post3_sync(
    pair_id: int, day_flag: int, calculate_fn: Callable
) -> dict:
    """
    Вызов calculate() с текущей датой.
    Проверяет, что модель работает в режиме реального времени.
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        result = await asyncio.to_thread(calculate_fn, pair_id, day_flag, now_str)
        return result if result else {}
    except Exception as e:
        return {"error": str(e)}


# ── Тест 4: поиск «проплешин» в кеше (NumPy-оптимизация) ─────────────────────

def _post4_holes(history: list[tuple[datetime, dict]]) -> int:
    """
    Максимальная длина непрерывной последовательности пустых тиков.
    Использует NumPy для нахождения max run.
    """
    if not history:
        return 0

    # True = есть данные, False = пусто
    non_empty = np.array([bool(r) for _, r in history], dtype=bool)

    if np.all(non_empty):
        return 0
    if not np.any(non_empty):
        return len(history)

    # Ищем максимальный run нулей
    empty = ~non_empty
    # Добавляем False-границы для корректного подсчёта на краях
    padded = np.concatenate(([False], empty, [False]))
    diffs = np.diff(padded.astype(int))
    starts = np.where(diffs == 1)[0]
    ends = np.where(diffs == -1)[0]
    runs = ends - starts

    return int(np.max(runs)) if len(runs) > 0 else 0


# ── Тест 5: симуляция торговли (NumPy-оптимизация) ────────────────────────────

def _post5_simulation(
    history: list[tuple[datetime, dict]],
    rates_rows: list[dict],
) -> dict:
    """
    Симуляция торговли по линейному суммированию весов.

    Алгоритм (правильный — с учётом проплешин):
      signal = sum(result.values())
      > 0  →  закрыть шорт (если открыт) + открыть лонг
      < 0  →  закрыть лонг (если открыт) + открыть шорт
      = 0  →  закрыть ВСЕ открытые позиции (не бездействие, а принудительное закрытие)

    Логика нуля важна при проплешинах: если модель долго молчит, позиция
    не висит месяц — она закрывается при первом нулевом сигнале.

    Позиция = 10% от текущего депозита (10 000$), без учёта спредов.
    """
    if not history or not rates_rows:
        return {"profit": 0.0, "dropdown": 0.0, "cw": 0.0, "result": 0.0}

    date_to_idx: dict[datetime, int] = {
        r["date"]: i for i, r in enumerate(rates_rows)
    }

    equity        = 10_000.0
    total_profit  = 0.0
    total_dropdown = 0.0
    wins          = 0
    trades        = 0

    # Текущая открытая позиция: (direction: +1/-1, open_price: float) или None
    position: tuple[float, float] | None = None

    for date, result in history:
        signal = sum(result.values()) if result else 0.0

        idx = date_to_idx.get(date)
        if idx is None or idx + 1 >= len(rates_rows):
            # Нет свечи — сигнал игнорируем, позиции не трогаем
            continue

        next_c = rates_rows[idx + 1]
        op = next_c["open"]
        cl = next_c["close"]
        if op == 0:
            continue

        # ── Закрытие текущей позиции ─────────────────────────────────────────
        # Закрываем если: сигнал = 0, или сигнал сменил направление
        if position is not None:
            direction, entry_price = position
            should_close = (
                signal == 0.0
                or (signal > 0 and direction < 0)
                or (signal < 0 and direction > 0)
            )
            if should_close:
                pos_size = equity * 0.10
                pct      = (op - entry_price) / entry_price * direction
                pnl      = pos_size * pct
                equity  += pnl
                trades  += 1
                if pnl >= 0:
                    total_profit   += pnl
                    wins           += 1
                else:
                    total_dropdown += abs(pnl)
                position = None

        # ── Открытие новой позиции ────────────────────────────────────────────
        # Открываем только при ненулевом сигнале (позиция уже точно закрыта выше)
        if signal != 0.0 and position is None:
            direction = 1.0 if signal > 0 else -1.0
            position  = (direction, op)

    # ── Принудительное закрытие в конце истории ───────────────────────────────
    if position is not None and rates_rows:
        direction, entry_price = position
        last_price = rates_rows[-1]["close"]
        if last_price and entry_price:
            pos_size       = equity * 0.10
            pct            = (last_price - entry_price) / entry_price * direction
            pnl            = pos_size * pct
            equity        += pnl
            trades        += 1
            if pnl >= 0:
                total_profit   += pnl
                wins           += 1
            else:
                total_dropdown += abs(pnl)

    cw = round(wins / trades, 4) if trades > 0 else 0.0

    return {
        "profit":   round(total_profit,   2),
        "dropdown": round(total_dropdown, 2),
        "cw":       cw,
        "result":   round(total_profit - total_dropdown, 2),
    }


# ── Вычисление полной истории model() по одной таблице ───────────────────────

def _build_history(
    table: str,
    global_rates: dict[str, list[dict]],
    global_dataset: list[dict],
    filter_dataset_by_date: bool = False,
) -> list[tuple[datetime, dict]]:
    """
    Синхронная функция: вызывает model() для каждого тика таблицы.
    Запускается в отдельном потоке через asyncio.to_thread().

    Оптимизации:
      - rows[:i+1] — O(1) срез (Python slice view)
      - Модель перезагружается один раз на весь прогон
      - dataset фильтруется линейно O(n_dataset) на тик
    """
    model_fn = _reload_model()
    rows = global_rates.get(table, [])
    history: list[tuple[datetime, dict]] = []

    for i, candle in enumerate(rows):
        target_date = candle["date"]
        filtered = rows[: i + 1]  # уже отсортированы; срез O(1) по памяти
        dataset = (
            _filter_dataset_lte(global_dataset, target_date)
            if filter_dataset_by_date
            else global_dataset
        )
        try:
            result = model_fn(
                rates=filtered,
                dataset=dataset,
                date=target_date,
            )
            history.append((target_date, result or {}))
        except Exception:
            history.append((target_date, {}))

    return history


# ── Точка входа posttest ──────────────────────────────────────────────────────

async def run_posttest(
    global_rates: dict[str, list[dict]],
    global_dataset: list[dict],
    calculate_fn: Callable,
    filter_dataset_by_date: bool = False,
) -> dict:
    """
    Точка входа для /posttest.

    Для каждого инструмента × таймфрейм:
      1. Строит полную историю model() (asyncio.to_thread)
      2. Параллельно запускает 5 тестов
      3. Собирает результат

    Инструменты × таймфреймы вычисляются параллельно.
    """

    async def _process_tf(pair_id: int, tf_name: str, table: str) -> dict:
        day_flag = 1 if tf_name == "day" else 0
        rows = global_rates.get(table, [])

        # Строим историю в потоке
        history: list[tuple[datetime, dict]] = await asyncio.to_thread(
            _build_history, table, global_rates, global_dataset, filter_dataset_by_date
        )

        # Тесты 1, 2, 4, 5 — синхронные, в потоках
        stats, signs, hole, sim = await asyncio.gather(
            asyncio.to_thread(_post1_stats, history),
            asyncio.to_thread(_post2_signs, history),
            asyncio.to_thread(_post4_holes, history),
            asyncio.to_thread(_post5_simulation, history, rows),
        )

        # Тест 3 — асинхронный
        sync_result = await _post3_sync(pair_id, day_flag, calculate_fn)

        return {
            "data": stats,       # тест 1
            "values": signs,     # тест 2
            "sync": sync_result, # тест 3
            "hole": hole,        # тест 4
            "history": sim,      # тест 5
        }

    # Запускаем все инструменты × таймфреймы параллельно
    tasks = {}
    for pair_id, tfs in INSTRUMENTS.items():
        for tf_name, table in tfs.items():
            key = (pair_id, tf_name)
            tasks[key] = asyncio.create_task(_process_tf(pair_id, tf_name, table))

    output: dict[str, dict] = {}
    for (pair_id, tf_name), task in tasks.items():
        pair_str = str(pair_id)
        if pair_str not in output:
            output[pair_str] = {}
        try:
            output[pair_str][tf_name] = await task
        except Exception as e:
            output[pair_str][tf_name] = {"error": str(e)}

    return output