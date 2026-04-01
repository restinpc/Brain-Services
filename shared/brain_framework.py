"""
brain_framework.py — Фреймворк для создания brain-* микросервисов.

Этот файл содержит ВСЮ повторяющуюся логику.

Всё остальное (котировки, экстремумы, bisect-индекс, lifecycle,
endpoints /, /weights, /values, /patch, кэш) делает этот файл.
"""

import asyncio
import bisect
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from common import (
    MODE, IS_DEV,
    log, send_error_trace,
    ok_response, err_response,
    resolve_workers, build_engines,
)
from cache_helper import ensure_cache_table, load_service_url, cached_values


# ══════════════════════════════════════════════════════════════════════════════
# УТИЛИТЫ 
# ══════════════════════════════════════════════════════════════════════════════

def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    """
    По ID инструмента и флагу таймфрейма возвращает имя таблицы с котировками.
    Неизвестный pair_id → brain_rates_eur_usd (fallback).
    Суффикс _day добавляется при day_flag=1.

        get_rates_table_name(1, 0) → "brain_rates_eur_usd"
        get_rates_table_name(3, 1) → "brain_rates_btc_usd_day"
    """
    return {
        1: "brain_rates_eur_usd",
        3: "brain_rates_btc_usd",
        4: "brain_rates_eth_usd",
    }.get(pair_id, "brain_rates_eur_usd") + ("_day" if day_flag == 1 else "")


def get_modification_factor(pair_id: int) -> float:
    """
    Возвращает коэффициент масштабирования для mode=1 (Extremum probability).
    """
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str: str) -> datetime | None:
    """
    Парсит строку с датой
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%Y-%d-%m %H:%M:%S"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def find_prev_candle_trend(candles: list, target_date: datetime):
    """
    Находит последнюю свечу строго ДО target_date и возвращает (datetime, is_bull).
    is_bull = True если close > open (бычья свеча).

    Нужно для calculate(): направление предыдущей свечи определяет, какой тип
    экстремума искать — max (после роста) или min (после падения).

    Поиск через bisect_left — O(log N). Свеча с date == target_date не берётся,
    она "текущая". Возвращает None еслb все свечи позже target_date.
    """
    if not candles:
        return None
    idx = bisect.bisect_left(candles, (target_date, False))
    return candles[idx - 1] if idx > 0 else None


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛИТЕЛЬНЫЕ ФУНКЦИИ — mode=0 (T1) и mode=1 (Extremum)
# ══════════════════════════════════════════════════════════════════════════════

def compute_t1_value(t_dates: list, calc_var: int,
                     ram_rates: dict, candle_ranges: dict,
                     avg_range: float) -> float:
    """
    Mode 0 — суммирует T1 по историческим аналогам из t_dates.

    T1 — направление свечи: положительное = цена пошла вверх, отрицательное = вниз.
    Смысл суммы: если на похожих новостях в прошлом рынок 8 раз рос и 2 падал,
    сумма будет положительной.

    calc_var меняет способ агрегации:
        0 — просто сложить все T1
        1 — считать только крупные свечи (range > avg_range), мелкий шум игнорируем
        2 — T1 × |T1|, квадратичное усиление: сильные движения весят больше слабых
        3 — комбинация: фильтр крупных + квадрат
        4 — вместо T1 суммируем (range − avg_range), то есть амплитуду сверх средней

    Если свеча для даты не нашлась в ram_rates / candle_ranges — пропускается (0.0).
    """
    need_filter = calc_var in (1, 3, 4)
    use_square  = calc_var in (2, 3)
    use_range   = calc_var == 4
    total = 0.0
    for d in t_dates:
        rng = candle_ranges.get(d, 0.0)
        if need_filter and rng <= avg_range:
            continue
        if use_range:
            total += rng - avg_range
        else:
            t1 = ram_rates.get(d, 0.0)
            total += t1 * abs(t1) if use_square else t1
    return total


def compute_extremum_value(t_dates: list, calc_var: int,
                           ext_set: set, candle_ranges: dict,
                           avg_range: float, modification: float,
                           total_hist: int):
    """
    Mode 1 — считает, как часто исторические аналоги совпали с экстремумом рынка.

    Логика:
        1. Отфильтровать t_dates — оставить только крупные свечи.
        2. Посчитать, сколько дат из pool входит в ext_set (множество экстремумов).
        3. Нормализовать: (попаданий / total_hist) × 2 − 1
           → получаем [-1, +1], где +1 = "все аналоги были экстремумами"
        4. Умножить на modification (масштаб инструмента).

    Какой ext_set передавать (min или max) — решает calculate() по направлению
    предыдущей свечи.

    Возвращает None если результат ноль — в ответ добавлять нечего.
    """
    need_filter = calc_var in (1, 3, 4)
    use_range   = calc_var == 4
    pool = ([d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range]
            if need_filter else t_dates)
    if not pool:
        return None
    if use_range:
        val = sum(candle_ranges.get(d, 0.0) - avg_range
                  for d in pool if d in ext_set)
        return val if val != 0 else None
    if total_hist == 0:
        return None
    val = ((sum(1 for d in pool if d in ext_set) / total_hist)
           * 2 - 1) * modification
    return val if val != 0 else None

# ══════════════════════════════════════════════════════════════════════════════
# БАЗОВЫЙ КЛАСС МИКРОСЕРВИСА
# ══════════════════════════════════════════════════════════════════════════════

class BaseBrainService:
    """
    Базовый класс для всех brain-* микросервисов.

    Наследуем этот класс и реализуем 2 метода:
      - load_my_data(conn_vlad, conn_brain) — загрузить weight_codes, ctx_index,
        и для каждого события вызвать register_event(dt, event_key).
      - make_weight_code(event_key, mode, shift_arg) — вернуть строку кода.

    Всё остальное (котировки, экстремумы, bisect, lifecycle,
    endpoints, кэш) берёт на себя BaseBrainService.
    """

    def __init__(self, service_id: int, port: int, node_name: str,
                 shift_window: int = 12,
                 recurring_min_count: int = 2,
                 filter_future_events: bool = True,
                 description: str = "Brain microservice"):
        """
        Инициализирует сервис, создаёт структуры данных и engines.
        Соединения с БД здесь не открываются — только в preload и при запросах.

        shift_window — сколько единиц delta_unit в каждую сторону смотреть
            при поиске событий. Для часовых свечей это часы, для дневных — дни.

        recurring_min_count — сколько раз должен встретиться ctx_id чтобы считаться
            "регулярным". Регулярный → ищем все сдвиги 0..shift_window.
            Нерегулярный → только точное совпадение (shift=0).

        filter_future_events — True для рыночных данных (новость не может быть
            из будущего), False для календарных событий (расписание известно заранее).
        """
        self.service_id             = service_id
        self.port                   = port
        self.node_name              = node_name
        self.shift_window           = shift_window
        self.recurring_min_count    = recurring_min_count
        self.filter_future_events   = filter_future_events
        self.description            = description

        self.engine_vlad, self.engine_brain, self.engine_super = build_engines()

        # Заполняем через load_my_data
        self.weight_codes = []   # строковые коды весов → отдаём на GET /weights
        self.ctx_index    = {}   # (ctx_id,) → {"occurrence_count": N}

        # Заполняем register_event, строит _build_sorted_arrays
        self._events_by_dt  = {}  # datetime → [event_key, ...]
        self._sorted_dates  = []  # плоский отсортированный список дат для bisect
        self._sorted_data   = []  # параллельный список event_keys
        self._event_history = {}  # event_key → [datetime, ...] — вся история

        # Заполняем _load_rates
        self.rates         = {}   # table → {datetime: t1}
        self.extremums     = {}   # table → {"min": set, "max": set}
        self.candle_ranges = {}   # table → {datetime: high-low}
        self.avg_range     = {}   # table → средний range
        self.last_candles  = {}   # table → [(datetime, is_bull)] — для find_prev_candle_trend
        self.service_url   = ""   # URL из brain_service — для записи кэша
        self.last_reload_time = None

        self._last_rates_refresh = {}  # throttle для _refresh_rates_if_needed

        log(f"MODE={MODE}", self.node_name, force=True)

    # ──────────────────────────────────────────────────────────────────
    # МЕТОДЫ, КОТОРЫЕ НАДО РЕАЛИЗОВАТЬ САМОМУ
    # ──────────────────────────────────────────────────────────────────

    async def load_my_data(self, conn_vlad, conn_brain) -> None:
        """
        Загрузить данные из БД в RAM.

        Вызывается из preload_all_data() при старте и каждые 3600 сек.
        conn_vlad и conn_brain — уже открытые async-соединения.

        Внутри нужно:
          1. Заполнить self.weight_codes (SELECT weight_code FROM ...)
          2. Заполнить self.ctx_index    (SELECT id, occurrence_count FROM ...)
          3. Для каждого события вызвать self.register_event(dt, event_key)

        Пример:
            res = await conn_vlad.execute(text("SELECT weight_code FROM my_weights"))
            for r in res.mappings().all():
                self.weight_codes.append(r["weight_code"])

            res = await conn_brain.execute(text("SELECT id, dt FROM my_events"))
            for r in res.mappings().all():
                self.register_event(r["dt"], (r["id"],))
        """
        raise NotImplementedError(
            "Реализуй load_my_data() — загрузка твоих данных из БД")

    def make_weight_code(self, event_key: tuple, mode: int,
                         shift_arg: int | None) -> str:
        """
        Собрать строку кода из event_key, mode и shift.

        Результат должен точно совпадать с weight_code в таблице весов.

        shift_arg=None означает нерегулярное событие → подставлять 0 в строку.

        Пример:
            def make_weight_code(self, event_key, mode, shift_arg):
                shift = shift_arg if shift_arg is not None else 0
                return f"MY{event_key[0]}_{mode}_{shift}"
        """
        raise NotImplementedError(
            "Реализуй make_weight_code() — формирование кода веса")

    # ──────────────────────────────────────────────────────────────────
    # МЕТОД РЕГИСТРАЦИИ СОБЫТИЙ (вызываем из load_my_data)
    # ──────────────────────────────────────────────────────────────────

    def register_event(self, dt: datetime, event_key: tuple) -> None:
        """
        Добавляет событие в два индекса. Вызывать из load_my_data().

        _events_by_dt[dt] → [event_key, ...]
            Нужен в calculate() для поиска событий в диапазоне дат (через bisect).

        _event_history[event_key] → [datetime, ...]
            Нужен чтобы взять всю историю конкретного ctx_id и построить t_dates.

        После load_my_data() базовый класс вызовет _build_sorted_arrays(),
        который отсортирует оба индекса.
        """
        self._events_by_dt.setdefault(dt, []).append(event_key)
        self._event_history.setdefault(event_key, []).append(dt)

    # ──────────────────────────────────────────────────────────────────
    # ВНУТРЕННЯЯ ЛОГИКА — джуниор НЕ трогает
    # ──────────────────────────────────────────────────────────────────

    def _build_sorted_arrays(self):
        """
        Строит плоские отсортированные массивы для bisect.

        Вызывается один раз после load_my_data(). Разделяет _events_by_dt
        на два параллельных списка: даты и соответствующие им event_keys.
        Такой формат нужен bisect_left.
        Также сортирует _event_history по каждому ключу.
        """
        if self._events_by_dt:
            sorted_items       = sorted(self._events_by_dt.items())
            self._sorted_dates = [x[0] for x in sorted_items]
            self._sorted_data  = [x[1] for x in sorted_items]
        else:
            self._sorted_dates = []
            self._sorted_data  = []
        for key in self._event_history:
            self._event_history[key].sort()

    async def _load_rates(self):
        """
        Загружает все 6 таблиц brain_rates_* в RAM.

        Для каждой таблицы строит:
          - rates[table]         — {datetime: t1} для compute_t1_value
          - last_candles[table]  — [(datetime, is_bull)] для find_prev_candle_trend
          - candle_ranges[table] — {datetime: high-low} для фильтра крупных свечей
          - avg_range[table]     — средний range как порог фильтрации
          - extremums[table]     — {"min": set, "max": set} для compute_extremum_value

        Экстремумы ищутся SQL: свеча является max-экстремумом если
        её max выше соседей слева и справа. Интервал соседства: 1 HOUR или 1 DAY.

        Ошибка на одной таблице не роняет загрузку остальных.
        """
        tables = [
            "brain_rates_eur_usd",     "brain_rates_eur_usd_day",
            "brain_rates_btc_usd",     "brain_rates_btc_usd_day",
            "brain_rates_eth_usd",     "brain_rates_eth_usd_day",
        ]
        for table in tables:
            self.rates[table]         = {}
            self.last_candles[table]  = []
            self.candle_ranges[table] = {}
            self.avg_range[table]     = 0.0
            self.extremums[table]     = {"min": set(), "max": set()}
            try:
                async with self.engine_brain.connect() as conn:
                    res = await conn.execute(text(
                        f"SELECT date, open, close, `max`, `min`, t1 "
                        f"FROM `{table}`"))
                    rows   = sorted(res.mappings().all(), key=lambda x: x["date"])
                    ranges = []
                    for r in rows:
                        dt = r["date"]
                        if r["t1"] is not None:
                            self.rates[table][dt] = float(r["t1"])
                        self.last_candles[table].append(
                            (dt, r["close"] > r["open"]))
                        rng = float(r["max"] or 0) - float(r["min"] or 0)
                        self.candle_ranges[table][dt] = rng
                        ranges.append(rng)
                    self.avg_range[table] = (
                        sum(ranges) / len(ranges) if ranges else 0.0)

                    interval = "1 DAY" if table.endswith("_day") else "1 HOUR"
                    for typ in ("min", "max"):
                        op = ">" if typ == "max" else "<"
                        q = f"""SELECT t1.date FROM `{table}` t1
                            JOIN `{table}` t_prev
                              ON t_prev.date = t1.date - INTERVAL {interval}
                            JOIN `{table}` t_next
                              ON t_next.date = t1.date + INTERVAL {interval}
                            WHERE t1.`{typ}` {op} t_prev.`{typ}`
                              AND t1.`{typ}` {op} t_next.`{typ}`"""
                        res_ext = await conn.execute(text(q))
                        self.extremums[table][typ] = {
                            r["date"] for r in res_ext.mappings().all()}
                log(f"  {table}: {len(self.rates[table])} candles",
                    self.node_name)
            except Exception as e:
                log(f"❌ {table}: {e}", self.node_name, level="error")

    async def _refresh_rates_if_needed(self, rates_table: str):
        """
        Дозагружает свежие свечи из БД — не чаще раза в 30 секунд.

        Вызывается в начале каждого calculate(). Нужно потому что полный reload
        раз в час — недостаточно для актуальности T1 текущего часа. Берёт только
        свечи с date > max известной даты, добавляет в RAM.

        Если запрос упал — calculate() продолжит со старыми данными, не падает.
        """
        now  = datetime.now()
        last = self._last_rates_refresh.get(rates_table)
        if last and (now - last).total_seconds() < 30:
            return
        self._last_rates_refresh[rates_table] = now
        ram = self.rates.get(rates_table)
        if not ram:
            return
        max_dt = max(ram.keys())
        try:
            async with self.engine_brain.connect() as conn:
                res = await conn.execute(text(
                    f"SELECT date, open, close, t1 "
                    f"FROM `{rates_table}` WHERE date > :dt "
                    f"ORDER BY date"), {"dt": max_dt})
                n = 0
                for r in res.mappings().all():
                    dt = r["date"]
                    if r["t1"] is not None:
                        ram[dt] = float(r["t1"])
                    cl = self.last_candles.get(rates_table)
                    if cl is not None:
                        cl.append((dt, (r["close"] or 0) > (r["open"] or 0)))
                    n += 1
                if n > 0:
                    log(f"  📥 Refreshed {n} candle(s) for {rates_table}",
                        self.node_name)
        except Exception as e:
            log(f"  ⚠️ Refresh error: {e}", self.node_name, level="warning")

    # ──────────────────────────────────────────────────────────────────
    # PRELOAD — полная загрузка данных при старте
    # ──────────────────────────────────────────────────────────────────

    async def preload_all_data(self):
        """
        Полная перезагрузка всего в RAM. Вызывается при старте и каждые 3600 сек.

        Порядок: очистка → load_my_data() → _build_sorted_arrays() →
                 _load_rates() → service_url → cache table.

        Каждый шаг в отдельном try/except — ошибка на одном шаге не роняет
        следующие.
        """
        log("🔄 FULL DATA RELOAD STARTED", self.node_name, force=True)

        self.weight_codes.clear()
        self.ctx_index.clear()
        self._events_by_dt.clear()
        self._sorted_dates.clear()
        self._sorted_data.clear()
        self._event_history.clear()
        self.rates.clear()
        self.extremums.clear()
        self.candle_ranges.clear()
        self.avg_range.clear()
        self.last_candles.clear()

        try:
            async with self.engine_vlad.connect() as cv:
                async with self.engine_brain.connect() as cb:
                    await self.load_my_data(cv, cb)
            log(f"  events: {len(self._events_by_dt)} dates, "
                f"weight_codes: {len(self.weight_codes)}, "
                f"ctx_index: {len(self.ctx_index)}",
                self.node_name)
        except Exception as e:
            log(f"❌ load_my_data: {e}", self.node_name, level="error")
            send_error_trace(e, self.node_name, "load_my_data")

        self._build_sorted_arrays()
        await self._load_rates()

        try:
            self.service_url = await load_service_url(
                self.engine_super, self.service_id)
        except Exception as e:
            log(f"❌ service_url: {e}", self.node_name, level="error")
            self.service_url = ""

        try:
            await ensure_cache_table(self.engine_vlad)
        except Exception as e:
            log(f"❌ cache table: {e}", self.node_name, level="error")

        self.last_reload_time = datetime.now()
        log("✅ FULL DATA RELOAD COMPLETED", self.node_name, force=True)

    # ──────────────────────────────────────────────────────────────────
    # CALCULATE — основной расчёт (вызывается из /values)
    # ──────────────────────────────────────────────────────────────────

    async def calculate(self, pair: int, day: int, date_str: str,
                        calc_type: int = 0, calc_var: int = 0) -> dict | None:
        """
        Считает веса для заданной целевой даты. Возвращает {weight_code: float}.

        Ищем в истории события с похожим контекстом (ctx_id), смотрим
        как вёл себя рынок после них, суммируем. Нет сигнала → нет в ответе.

        Алгоритм по шагам:
          1. Строим окно поиска [target_date - shift_window, target_date].
          2. Через bisect находим все события в этом окне.
          3. Для каждого события берём полную историю его ctx_id (только прошлое!).
          4. Строим t_dates — даты "через shift часов после каждого прошлого вхождения".
          5. Считаем T1-сумму и Extremum-вероятность по этим t_dates.
          6. Нулевые значения не включаем в ответ.

        Возвращает None если date_str не распарсился.
        Возвращает {} если событий в окне нет.

        calc_type: 0=T1+Extremum, 1=только T1, 2=только Extremum.
        calc_var: вариация агрегации 0..4, см. compute_t1_value().
        """
        target_date = parse_date_string(date_str)
        if not target_date:
            return None

        rates_table  = get_rates_table_name(pair, day)
        await self._refresh_rates_if_needed(rates_table)
        modification = get_modification_factor(pair)
        delta_unit   = timedelta(days=1) if day == 1 else timedelta(hours=1)

        """
        Окно поиска: 49 точек (shift_window=24 → от -24h до +24h),
        но filter_future_events=True обрежет правую половину → остаётся 25 точек
        Для новостей (filter_future_events=True)
        Нельзя использовать новости из будущего
        Ищем только события, которые случились до или в момент target_date
        Правильно: [-24, 0]
        
        Для календарных событий (filter_future_events=False)
        Расписание известно заранее (например, заседания ФРС)
        Можно использовать и будущие события
        Правильно: [-24, 24]
        """
        check_dts = [target_date + delta_unit * s
                     for s in range(-self.shift_window, self.shift_window + 1)]

        # Bisect-поиск: для каждой точки окна ищем события в интервале [dt, dt+1h)
        observations = []
        for dt in check_dts:
            if self.filter_future_events and dt > target_date:
                continue
            dt_end = dt + delta_unit
            _l = bisect.bisect_left(self._sorted_dates, dt)
            _r = bisect.bisect_left(self._sorted_dates, dt_end)
            for _i in range(_l, _r):
                obs_dt = self._sorted_dates[_i]
                for event_key in self._sorted_data[_i]:
                    shift = round((target_date - obs_dt) / delta_unit)
                    observations.append((event_key, obs_dt, shift))

        if not observations:
            return {}

        ram_rates   = self.rates.get(rates_table, {})
        ram_ranges  = self.candle_ranges.get(rates_table, {})
        avg_rng     = self.avg_range.get(rates_table, 0.0)
        ram_ext     = self.extremums.get(rates_table, {"min": set(), "max": set()})
        prev_candle = find_prev_candle_trend(
            self.last_candles.get(rates_table, []), target_date)

        result = {}
        for event_key, obs_dt, shift in observations:
            ctx_info = self.ctx_index.get(event_key)
            if ctx_info is None:
                continue

            occ          = ctx_info.get("occurrence_count", 0)
            is_recurring = occ >= self.recurring_min_count

            # Нерегулярный ctx_id: брать только если новость вышла именно сейчас (shift=0).
            # Регулярный: допускаем любой сдвиг в пределах окна.
            if not is_recurring and shift != 0:
                continue
            if is_recurring and abs(shift) > self.shift_window:
                continue

            # Берём историю ctx_id и обрезаем будущее — защита от look-ahead bias.
            # bisect_left находит первую дату >= target_date, берём всё левее.
            all_hist  = self._event_history.get(event_key, [])
            idx       = bisect.bisect_left(all_hist, target_date)
            valid_dts = all_hist[:idx]
            if not valid_dts:
                continue

            # t_dates: "через shift часов после каждого прошлого вхождения этого ctx_id"
            # Это и есть исторические аналоги — свечи, по которым считаем вес.
            t_dates = [d + delta_unit * shift for d in valid_dts
                       if (d + delta_unit * shift) < target_date]
            if not t_dates:
                continue

            # shift_arg=None → нерегулярное событие, make_weight_code подставит 0
            shift_arg = shift if is_recurring else None

            # Mode 0: сумма T1 по аналогам
            if calc_type in (0, 1):
                t1_sum = compute_t1_value(
                    t_dates, calc_var, ram_rates, ram_ranges, avg_rng)
                wc         = self.make_weight_code(event_key, 0, shift_arg)
                result[wc] = result.get(wc, 0.0) + t1_sum

            # Mode 1: вероятность экстремума.
            # Тип (max/min) определяем по направлению предыдущей свечи.
            if calc_type in (0, 2) and prev_candle:
                _, is_bull = prev_candle
                ext_set    = ram_ext["max" if is_bull else "min"]
                ext_val    = compute_extremum_value(
                    t_dates, calc_var, ext_set, ram_ranges,
                    avg_rng, modification, len(valid_dts))
                if ext_val is not None:
                    wc         = self.make_weight_code(event_key, 1, shift_arg)
                    result[wc] = result.get(wc, 0.0) + ext_val

        # Нули не отдаём — нет сигнала = нет в ответе.
        # Округление до 6 знаковн.
        return {k: round(v, 6) for k, v in result.items() if v != 0}

    # ──────────────────────────────────────────────────────────────────
    # FastAPI APP — создание приложения со всеми endpoints
    # ──────────────────────────────────────────────────────────────────

    def create_app(self) -> FastAPI:
        """
        Создаёт FastAPI-приложение со всеми стандартными endpoints и lifecycle.

        Endpoints:
            GET  /         — метаданные (version, размеры индексов, время reload)
            GET  /weights  — список всех weight_codes для PHP-стороны
            GET  /values   — основной расчёт (кэш → calculate)
            POST /patch    — инкремент версии в version_microservice (при деплое)

        Lifecycle:
            startup  → preload_all_data() + запуск background_reload()
            shutdown → cancel background_reload() + dispose engines

        background_reload(): каждые 3600 сек вызывает preload_all_data().
        Ошибки в reload логируются, но цикл не останавливается.
        """
        svc = self  # замыкание для вложенных async-функций

        async def background_reload():
            while True:
                await asyncio.sleep(3600)
                try:
                    await svc.preload_all_data()
                except Exception as e:
                    log(f"❌ Reload: {e}", svc.node_name,
                        level="error", force=True)
                    send_error_trace(e, svc.node_name, "reload")

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            try:
                await svc.preload_all_data()
            except Exception as e:
                log(f"❌ Preload failed: {e}", svc.node_name,
                    level="error", force=True)
            task = asyncio.create_task(background_reload())
            yield
            task.cancel()
            for name, eng in [("vlad",  svc.engine_vlad),
                               ("brain", svc.engine_brain),
                               ("super", svc.engine_super)]:
                try:
                    await eng.dispose()
                except Exception:
                    pass

        app = FastAPI(lifespan=lifespan)

        @app.get("/")
        async def metadata():
            # Версия из version_microservice нужна PHP-стороне для инвалидации кэша.
            # Формат "1.{version}.0" — исторически сложившийся, не менять.
            # Ошибка чтения → version=0, сервис не падает.
            version = 0
            try:
                async with svc.engine_vlad.connect() as _conn:
                    _res = await _conn.execute(
                        text("SELECT version FROM version_microservice "
                             "WHERE microservice_id = :id"),
                        {"id": svc.service_id},
                    )
                    _row = _res.fetchone()
                    if _row:
                        version = _row[0]
            except Exception as _e:
                log(f"⚠️ version_microservice read failed: {_e}",
                    svc.node_name, level="warning")

            return {
                "status":  "ok",
                "version": f"1.{version}.0",
                "mode":    MODE,
                "name":    svc.node_name,
                "text":    svc.description,
                "metadata": {
                    "weight_codes": len(svc.weight_codes),
                    "ctx_index":    len(svc.ctx_index),
                    "events":       len(svc._events_by_dt),
                    "last_reload":  (svc.last_reload_time.isoformat()
                                     if svc.last_reload_time else None),
                },
            }

        @app.get("/weights")
        async def weights():
            # PHP-сторона спрашивает этот список перед тем как запрашивать /values.
            return ok_response(svc.weight_codes)

        @app.get("/values")
        async def values(
            pair: int = Query(1),
            day:  int = Query(1),
            date: str = Query(...),
            type: int = Query(0, ge=0, le=2),
            var:  int = Query(0, ge=0, le=4),
        ):
            # cached_values проверяет кэш, при miss вызывает compute_fn и сохраняет.
            try:
                return await cached_values(
                    engine_vlad=svc.engine_vlad,
                    service_url=svc.service_url,
                    pair=pair, day=day, date=date,
                    extra_params={"type": type, "var": var},
                    compute_fn=lambda: svc.calculate(
                        pair, day, date,
                        calc_type=type, calc_var=var),
                    node=svc.node_name,
                )
            except Exception as e:
                send_error_trace(e, node=svc.node_name, script="get_values")
                return err_response(str(e))

        @app.post("/patch")
        async def patch():
            # Если version=0 → ставим 1. Если запись не найдена → 500.
            async with svc.engine_vlad.begin() as conn:
                res = await conn.execute(text(
                    "SELECT version FROM version_microservice "
                    "WHERE microservice_id = :id"),
                    {"id": svc.service_id})
                row = res.fetchone()
                if not row:
                    raise HTTPException(status_code=500,
                        detail=f"SID {svc.service_id} not found")
                old = row[0]
                new = max(old, 1)
                if new != old:
                    await conn.execute(text(
                        "UPDATE version_microservice "
                        "SET version = :v "
                        "WHERE microservice_id = :id"),
                        {"v": new, "id": svc.service_id})
            return {"status": "ok", "from": old, "to": new}

        return app

    # ──────────────────────────────────────────────────────────────────
    # RUN — запуск сервера
    # ──────────────────────────────────────────────────────────────────

    def run(self):
        """
        Запускает uvicorn. Число воркеров читается из БД через resolve_workers(),
        при ошибке — fallback на 1.
        """
        import uvicorn
        import asyncio as _asyncio
        try:
            _workers = _asyncio.run(
                resolve_workers(self.engine_super, self.service_id, default=1))
        except Exception:
            _workers = 1
        log(f"Starting with {_workers} worker(s) in {MODE} mode",
            self.node_name, force=True)
        uvicorn.run("server:app", host="0.0.0.0", port=self.port,
                    reload=False, workers=_workers)
