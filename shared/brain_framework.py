"""
brain_framework.py — Фреймворк для создания brain-* микросервисов.

Этот файл содержит ВСЮ повторяющуюся логику. Джуниор его НЕ ТРОГАЕТ.
Он кладётся в shared/ рядом с common.py и cache_helper.py.

Что даёт:
  - Загрузка котировок (brain_rates_*)
  - Вычисление экстремумов
  - compute_t1_value / compute_extremum_value
  - Подгрузка свежих свечей (_refresh_rates_if_needed)
  - Lifecycle (preload + background reload + lifespan)
  - Все стандартные endpoints (/, /weights, /values, /patch)
  - parse_date_string, find_prev_candle_trend, get_rates_table_name

Джуниор наследует BaseBrainService и реализует 3 метода:
  1. load_my_data(conn_vlad, conn_brain) — загрузить свои данные
  2. find_observations(target_date, delta_unit) — найти наблюдения в окне
  3. process_observation(obs, target_date, ...) — обработать одно наблюдение
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
# УТИЛИТЫ — общие для всех сервисов, никогда не меняются
# ══════════════════════════════════════════════════════════════════════════════

def get_rates_table_name(pair_id: int, day_flag: int) -> str:
    """Имя таблицы котировок по pair и day."""
    return {
        1: "brain_rates_eur_usd",
        3: "brain_rates_btc_usd",
        4: "brain_rates_eth_usd",
    }.get(pair_id, "brain_rates_eur_usd") + ("_day" if day_flag == 1 else "")


def get_modification_factor(pair_id: int) -> float:
    """
    Коэффициент масштабирования для mode=1 (extremum).
    EUR/USD двигается на ~0.0005, BTC — на ~500.
    Без этого коэффициента extremum-вероятность (−1..+1)
    несопоставима по масштабу с T1.
    """
    return {1: 0.001, 3: 1000.0, 4: 100.0}.get(pair_id, 1.0)


def parse_date_string(date_str: str) -> datetime | None:
    """Парсинг даты в нескольких форматах. Возвращает None при ошибке."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%Y-%d-%m %H:%M:%S"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def find_prev_candle_trend(candles: list, target_date: datetime):
    """
    Находит последнюю свечу ПЕРЕД target_date.
    candles — отсортированный список [(datetime, is_bull), ...]
    Возвращает (datetime, is_bull) или None.
    Используется для определения: ищем min или max экстремум.
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
    Mode 0 — сумма T1 по историческим аналогам.

    calc_var определяет вариацию:
      0 = простая сумма T1
      1 = только крупные свечи (range > avg_range)
      2 = T1 × |T1| (квадратичное усиление)
      3 = фильтрация + квадрат
      4 = сумма (range − avg_range) вместо T1
    """
    need_filter = calc_var in (1, 3, 4)
    use_square = calc_var in (2, 3)
    use_range = calc_var == 4
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
    Mode 1 — вероятность экстремума.

    Логика:
      1. Отфильтровать t_dates по размеру свечи (если calc_var требует)
      2. Посчитать: сколько из pool совпало с ext_set (экстремумами)
      3. Нормализовать: (matches / total) * 2 − 1  →  диапазон [−1, +1]
      4. Умножить на modification factor
      5. Вернуть None если значение = 0

    Поправка на направление (min/max) делается СНАРУЖИ:
      - Если prev_candle бычья → ext_set = max → инвертировать
      - Если медвежья → ext_set = min → оставить как есть
      Это закодировано в самом modification factor (знак).
    """
    need_filter = calc_var in (1, 3, 4)
    use_range = calc_var == 4
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
# CONFIDENCE FUNCTIONS — байесовское сглаживание
# ══════════════════════════════════════════════════════════════════════════════

def confidence_bayes(count: int, prior: int = 10) -> float:
    """
    Чем больше наблюдений — тем выше достоверность (ближе к 1.0).
    count=0 → 0.0, count=10 → 0.5, count=100 → 0.91
    """
    return count / (count + prior) if count > 0 else 0.0


def confidence_none(count: int, prior: int = 0) -> float:
    """Без сглаживания — всегда 1.0."""
    return 1.0


CONFIDENCE_FUNCS = {"bayes": confidence_bayes, "none": confidence_none}


# ══════════════════════════════════════════════════════════════════════════════
# БАЗОВЫЙ КЛАСС МИКРОСЕРВИСА
# ══════════════════════════════════════════════════════════════════════════════

class BaseBrainService:
    """
    Базовый класс для всех brain-* микросервисов.

    Джуниор наследует этот класс и реализует 3 метода:

    1. async load_my_data(self, conn_vlad, conn_brain):
       Загрузить свои данные (индексы, контексты, события).
       Заполнить self.weight_codes, self.ctx_index, и вызвать
       self.register_event(dt, event_key) для каждого события.

    2. find_observations(self, target_date, check_dts, delta_unit):
       Найти наблюдения в окне вокруг target_date.
       Вернуть список [(event_key, obs_dt, shift), ...]

    3. make_weight_code(self, event_key, mode, shift_arg):
       Создать строковый код веса из ключа события.

    Всё остальное (котировки, экстремумы, lifecycle, endpoints,
    кэширование, bisect) — делает базовый класс.
    """

    def __init__(self, service_id: int, port: int, node_name: str,
                 shift_window: int = 12,
                 recurring_min_count: int = 2,
                 filter_future_events: bool = True,
                 description: str = "Brain microservice"):
        """
        service_id: уникальный ID сервиса (из brain_service)
        port: HTTP порт
        node_name: имя для логов
        shift_window: окно поиска событий (±N часов/дней)
        recurring_min_count: минимум повторений для "редкого" события
        filter_future_events: True = рыночные данные (фильтр dt>target),
                              False = календарь (расписание известно заранее)
        """
        self.service_id = service_id
        self.port = port
        self.node_name = node_name
        self.shift_window = shift_window
        self.recurring_min_count = recurring_min_count
        self.filter_future_events = filter_future_events
        self.description = description

        # Создаём engines
        self.engine_vlad, self.engine_brain, self.engine_super = build_engines()

        # ── Данные, которые заполняет ДЖУНИОР через load_my_data ──
        self.weight_codes = []       # список строковых кодов весов
        self.ctx_index = {}          # (key_tuple) → {"occurrence_count": N}

        # ── Данные, которые заполняет register_event ──
        self._events_by_dt = {}       # datetime → [event_key, ...]
        self._sorted_dates = []       # для bisect
        self._sorted_data = []        # параллельный массив
        self._event_history = {}      # event_key → [datetime, ...]  (sorted)

        # ── Данные, которые загружает БАЗОВЫЙ КЛАСС ──
        self.rates = {}              # table → {datetime: t1}
        self.extremums = {}          # table → {"min": set, "max": set}
        self.candle_ranges = {}      # table → {datetime: range}
        self.avg_range = {}          # table → float
        self.last_candles = {}       # table → [(datetime, is_bull)]
        self.service_url = ""
        self.last_reload_time = None

        self._last_rates_refresh = {}

        log(f"MODE={MODE}", self.node_name, force=True)

    # ──────────────────────────────────────────────────────────────────
    # МЕТОДЫ, КОТОРЫЕ ДЖУНИОР ДОЛЖЕН РЕАЛИЗОВАТЬ
    # ──────────────────────────────────────────────────────────────────

    async def load_my_data(self, conn_vlad, conn_brain) -> None:
        """
        Загрузить свои данные. Джуниор ОБЯЗАН реализовать.

        Что нужно сделать:
          1. Загрузить weight_codes → self.weight_codes
          2. Загрузить ctx_index → self.ctx_index
          3. Загрузить события и для каждого вызвать:
             self.register_event(dt, event_key)

        conn_vlad и conn_brain — открытые async connections.

        Пример:
            res = await conn_vlad.execute(text("SELECT ..."))
            for r in res.mappings().all():
                self.weight_codes.append(r["weight_code"])

            res = await conn_brain.execute(text("SELECT ..."))
            for r in res.mappings().all():
                dt = r["datetime"]
                key = (r["event_id"], r["direction"])
                self.register_event(dt, key)
        """
        raise NotImplementedError(
            "Реализуй load_my_data() — загрузка твоих данных из БД")

    def make_weight_code(self, event_key: tuple, mode: int,
                         shift_arg: int | None) -> str:
        """
        Создать строковый код веса. Джуниор ОБЯЗАН реализовать.

        event_key: кортеж, который ты передавал в register_event
        mode: 0 = T1, 1 = Extremum
        shift_arg: число (сдвиг в часах/днях) или None (нулевой час)

        Пример:
            base = f"{event_key[0]}_{event_key[1]}_{mode}"
            return base if shift_arg is None else f"{base}_{shift_arg}"
        """
        raise NotImplementedError(
            "Реализуй make_weight_code() — формирование кода веса")

    # ──────────────────────────────────────────────────────────────────
    # МЕТОД РЕГИСТРАЦИИ СОБЫТИЙ (джуниор вызывает в load_my_data)
    # ──────────────────────────────────────────────────────────────────

    def register_event(self, dt: datetime, event_key: tuple) -> None:
        """
        Зарегистрировать одно событие. Вызывай из load_my_data().

        dt: дата/время события
        event_key: кортеж, идентифицирующий контекст события.
                   Должен совпадать с ключами в self.ctx_index.
        """
        self._events_by_dt.setdefault(dt, []).append(event_key)
        self._event_history.setdefault(event_key, []).append(dt)

    # ──────────────────────────────────────────────────────────────────
    # ВНУТРЕННЯЯ ЛОГИКА — джуниор НЕ трогает
    # ──────────────────────────────────────────────────────────────────

    def _build_sorted_arrays(self):
        """Построить отсортированные массивы для bisect после загрузки."""
        if self._events_by_dt:
            sorted_items = sorted(self._events_by_dt.items())
            self._sorted_dates = [x[0] for x in sorted_items]
            self._sorted_data = [x[1] for x in sorted_items]
        else:
            self._sorted_dates = []
            self._sorted_data = []
        # Отсортировать историю каждого события
        for key in self._event_history:
            self._event_history[key].sort()

    async def _load_rates(self):
        """Загрузить все таблицы brain_rates_* в память."""
        tables = [
            "brain_rates_eur_usd", "brain_rates_eur_usd_day",
            "brain_rates_btc_usd", "brain_rates_btc_usd_day",
            "brain_rates_eth_usd", "brain_rates_eth_usd_day",
        ]
        for table in tables:
            self.rates[table] = {}
            self.last_candles[table] = []
            self.candle_ranges[table] = {}
            self.avg_range[table] = 0.0
            self.extremums[table] = {"min": set(), "max": set()}
            try:
                async with self.engine_brain.connect() as conn:
                    res = await conn.execute(text(
                        f"SELECT date, open, close, `max`, `min`, t1 "
                        f"FROM `{table}`"))
                    rows = sorted(res.mappings().all(),
                                  key=lambda x: x["date"])
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

                    # Экстремумы — динамический INTERVAL
                    interval = ("1 DAY" if table.endswith("_day")
                                else "1 HOUR")
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
        """Подгрузить свежие свечи из БД. Не чаще раз в 30 сек."""
        now = datetime.now()
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
                        cl.append(
                            (dt, (r["close"] or 0) > (r["open"] or 0)))
                    n += 1
                if n > 0:
                    log(f"  📥 Refreshed {n} candle(s) for {rates_table}",
                        self.node_name)
        except Exception as e:
            log(f"  ⚠️ Refresh error: {e}", self.node_name,
                level="warning")

    # ──────────────────────────────────────────────────────────────────
    # PRELOAD — полная загрузка данных при старте
    # ──────────────────────────────────────────────────────────────────

    async def preload_all_data(self):
        """Загрузить ВСЕ данные в RAM. Вызывается при старте и каждый час."""
        log("🔄 FULL DATA RELOAD STARTED", self.node_name, force=True)

        # Очистка
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

        # 1. Данные джуниора
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

        # 2. Построить sorted arrays для bisect
        self._build_sorted_arrays()

        # 3. Котировки + экстремумы
        await self._load_rates()

        # 4. SERVICE_URL
        try:
            self.service_url = await load_service_url(
                self.engine_super, self.service_id)
        except Exception as e:
            log(f"❌ service_url: {e}", self.node_name, level="error")
            self.service_url = ""

        # 5. Cache table
        try:
            await ensure_cache_table(self.engine_vlad)
        except Exception as e:
            log(f"❌ cache table: {e}", self.node_name, level="error")

        self.last_reload_time = datetime.now()
        log("✅ FULL DATA RELOAD COMPLETED", self.node_name, force=True)

    # ──────────────────────────────────────────────────────────────────
    # CALCULATE — основной расчёт (полностью стандартный)
    # ──────────────────────────────────────────────────────────────────

    async def calculate(self, pair: int, day: int, date_str: str,
                        calc_type: int = 0, calc_var: int = 0) -> dict | None:
        """
        Вычислить веса для заданной даты.
        Возвращает {weight_code: value} или {} если ничего не найдено.
        """
        target_date = parse_date_string(date_str)
        if not target_date:
            return None

        rates_table = get_rates_table_name(pair, day)
        await self._refresh_rates_if_needed(rates_table)
        modification = get_modification_factor(pair)
        delta_unit = timedelta(days=1) if day == 1 else timedelta(hours=1)

        # Построить окно поиска
        check_dts = [target_date + delta_unit * s
                     for s in range(-self.shift_window,
                                    self.shift_window + 1)]

        # Найти наблюдения через bisect
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

        # Подготовить данные котировок
        ram_rates = self.rates.get(rates_table, {})
        ram_ranges = self.candle_ranges.get(rates_table, {})
        avg_rng = self.avg_range.get(rates_table, 0.0)
        ram_ext = self.extremums.get(rates_table,
                                     {"min": set(), "max": set()})
        prev_candle = find_prev_candle_trend(
            self.last_candles.get(rates_table, []), target_date)

        # Вычислить веса
        result = {}
        for event_key, obs_dt, shift in observations:
            ctx_info = self.ctx_index.get(event_key)
            if ctx_info is None:
                continue
            occ = ctx_info.get("occurrence_count", 0)
            is_recurring = occ >= self.recurring_min_count
            if not is_recurring and shift != 0:
                continue
            if is_recurring and abs(shift) > self.shift_window:
                continue

            # Взять историю: только прошлые вхождения
            all_hist = self._event_history.get(event_key, [])
            idx = bisect.bisect_left(all_hist, target_date)
            valid_dts = all_hist[:idx]
            if not valid_dts:
                continue

            # t_dates: исторические свечи, строго до target
            t_dates = [d + delta_unit * shift for d in valid_dts
                       if (d + delta_unit * shift) < target_date]
            if not t_dates:
                continue
            shift_arg = shift if is_recurring else None

            # Mode 0: T1 sum
            if calc_type in (0, 1):
                t1_sum = compute_t1_value(
                    t_dates, calc_var, ram_rates, ram_ranges, avg_rng)
                wc = self.make_weight_code(event_key, 0, shift_arg)
                result[wc] = result.get(wc, 0.0) + t1_sum

            # Mode 1: Extremum probability
            if calc_type in (0, 2) and prev_candle:
                _, is_bull = prev_candle
                ext_set = ram_ext["max" if is_bull else "min"]
                ext_val = compute_extremum_value(
                    t_dates, calc_var, ext_set, ram_ranges,
                    avg_rng, modification, len(valid_dts))
                if ext_val is not None:
                    wc = self.make_weight_code(event_key, 1, shift_arg)
                    result[wc] = result.get(wc, 0.0) + ext_val

        return {k: round(v, 6) for k, v in result.items() if v != 0}

    # ──────────────────────────────────────────────────────────────────
    # FastAPI APP — создание приложения со всеми endpoints
    # ──────────────────────────────────────────────────────────────────

    def create_app(self) -> FastAPI:
        """Создать FastAPI приложение с lifecycle и endpoints."""
        svc = self  # замыкание

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
            for name, eng in [("vlad", svc.engine_vlad),
                              ("brain", svc.engine_brain),
                              ("super", svc.engine_super)]:
                try:
                    await eng.dispose()
                except Exception:
                    pass

        app = FastAPI(lifespan=lifespan)

        @app.get("/")
        async def metadata():
            # Читаем версию из version_microservice — та же логика что в старых
            # сервисах (SID 31 и др.): "1.{version}.0"
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
            return ok_response(svc.weight_codes)

        @app.get("/values")
        async def values(
            pair: int = Query(1), day: int = Query(1),
            date: str = Query(...),
            type: int = Query(0, ge=0, le=2),
            var: int = Query(0, ge=0, le=4),
        ):
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
                send_error_trace(e, node=svc.node_name,
                                 script="get_values")
                return err_response(str(e))

        @app.post("/patch")
        async def patch():
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
        """Запустить uvicorn сервер."""
        import uvicorn
        import asyncio as _asyncio
        try:
            _workers = _asyncio.run(
                resolve_workers(self.engine_super,
                                self.service_id, default=1))
        except Exception:
            _workers = 1
        log(f"Starting with {_workers} worker(s) in {MODE} mode",
            self.node_name, force=True)
        uvicorn.run("server:app", host="0.0.0.0", port=self.port,
                    reload=False, workers=_workers)