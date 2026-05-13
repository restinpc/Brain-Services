"""
model.py — brain-extremum-interval
=====================================

Сигнал: текущий интервал между экстремумами попадает в зону
entry_x ± BIN_HALF или exit_x ± BIN_HALF по гистограмме паттернов.

Совместимость с brain_framework.py v14:
  • dates_ns = секунды (int(dt.timestamp())), НЕ наносекунды
  • by_key ключи = строки (event_key — VARCHAR из MySQL)
  • key_dates = {event_key: [sorted datetime list]}
  • ctx_index = {(ctx_uid_md5,): row_dict}  (CTX_KEY_COLUMNS=["ctx_uid"])
  • np_rates = np_simple_rates = EUR/USD массивы даже для BTC/ETH пар
    → модель использует `rates` (реальный инструмент) для T1, не np_rates

Пороги по инструменту:
  EUR/USD: [0.3, 0.5, 0.8]  (var=0→0.3, var=1→0.5, var=2→0.8)
  BTC/USD: [3.0, 5.0, 8.0]  (var=0→3.0, var=1→5.0, var=2→8.0)
  ETH/USD: [3.0, 5.0, 8.0]

Коды весов совпадают с extremum_weights.py:
  EXT_{table_code}_v{var}_{ext_code}_{bint}_{mode}[_{shift}]
  mode=0 → T1 (sum close-open)
  mode=1 → сила паттерна (max_diff_abs / occ) × sign × mod

type=0 → ext_type="all",  type=1 → "max",  type=2 → "min"
var=0..2 → порог по инструменту (см. выше)
"""

from __future__ import annotations

import bisect
import os
from datetime import datetime, timedelta
from typing import Any

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════

NODE_NAME    = os.getenv("NODE_NAME",    "brain-extremum-interval")
SERVICE_TEXT = "Extremum interval · ZigZag dip→rise · multi-threshold"

# Главная таблица котировок для IS_SIMPLE режима.
# Реальный инструмент в model() определяется по `rates`, не по RATES_TABLE.
RATES_TABLE = os.getenv("RATES_TABLE", "brain_rates_eur_usd")

DATASET_ENGINE         = "vlad"
DATASET_TABLE          = os.getenv("DATASET_TABLE",   "brain_extremum_events")
DATASET_KEY            = "event_key"
FILTER_DATASET_BY_DATE = False

CTX_SOURCE_TABLE  = os.getenv("EXTREMUM_CTX_TABLE",     "brain_extremum_ctx")
CTX_TABLE         = CTX_SOURCE_TABLE
WEIGHTS_TABLE     = os.getenv("EXTREMUM_WEIGHTS_TABLE", "brain_extremum_weights")

BIN_HALF      = float(os.getenv("EXTREMUM_BIN_HALF",     "0.25"))
SHIFT_WINDOW  = int(os.getenv("EXTREMUM_SHIFT_WINDOW",   "168"))
RECURRING_MIN = int(os.getenv("EXTREMUM_RECURRING_MIN",  "2"))

CACHE_DATE_FROM  = os.getenv("CACHE_DATE_FROM", "2025-01-01")
REBUILD_INTERVAL = 86_400
RELOAD_INTERVAL  = 3_600

VAR_RANGE    = [0, 1, 2]
TYPES_RANGE  = [0, 1, 2]

# ── Поддерживаемые таблицы ──────────────────────────────────────────────────

SUPPORTED_TABLES = (
    "brain_rates_eur_usd",
    "brain_rates_btc_usd",
    "brain_rates_eth_usd",
    "brain_rates_eur_usd_day",
    "brain_rates_btc_usd_day",
    "brain_rates_eth_usd_day",
)

# Пороги по инструменту — индексируются через var (0, 1, 2).
# ПОРЯДОК: от наибольшего порога к наименьшему — так var=0 даёт
# наибольшее число паттернов (редкие, но чёткие сигналы).
# EUR/USD 0.8%: ~15 паттернов | 0.5%: ~6 | 0.3%: 0 (гистограмма гладкая)
# BTC/USD 8.0%: ~42 паттерна  | 5.0%: ~22 | 3.0%: ~6
_TABLE_THRESHOLDS: dict[str, list[float]] = {
    "brain_rates_eur_usd":      [0.8, 0.5, 0.3],
    "brain_rates_eur_usd_day":  [0.8, 0.5, 0.3],
    "brain_rates_btc_usd":      [8.0, 5.0, 3.0],
    "brain_rates_btc_usd_day":  [8.0, 5.0, 3.0],
    "brain_rates_eth_usd":      [8.0, 5.0, 3.0],
    "brain_rates_eth_usd_day":  [8.0, 5.0, 3.0],
}

TABLE_CODE_MAP: dict[str, str] = {
    "brain_rates_btc_usd":      "btcusd",
    "brain_rates_eth_usd":      "ethusd",
    "brain_rates_eur_usd":      "eurusd",
    "brain_rates_btc_usd_day":  "btcusd_d",
    "brain_rates_eth_usd_day":  "ethusd_d",
    "brain_rates_eur_usd_day":  "eurusd_d",
}

EXT_TYPE_CODE_MAP: dict[str, str] = {
    "all": "A",
    "max": "X",
    "min": "N",
}

_EXT_TYPE_BY_TYPE: dict[int, str] = {
    0: "all",
    1: "max",
    2: "min",
}


# ══════════════════════════════════════════════════════════════════════════════
# SQL
# ══════════════════════════════════════════════════════════════════════════════

_TABLE_LIST_SQL = ", ".join(f"'{t}'" for t in SUPPORTED_TABLES)

DATASET_QUERY = f"""
    SELECT
        event_key   AS event_key,
        table_name  AS table_name,
        `date`      AS `date`,
        close_price AS close_price
    FROM {DATASET_TABLE}
    WHERE event_key IS NOT NULL
      AND table_name IN ({_TABLE_LIST_SQL})
      AND `date` IS NOT NULL
    ORDER BY `date`
"""

CTX_QUERY = f"""
    SELECT
        MD5(CONCAT_WS('|',
            COALESCE(event_key,       ''),
            COALESCE(table_name,      ''),
            COALESCE(CAST(threshold_pct AS CHAR), ''),
            COALESCE(ext_type,        ''),
            COALESCE(CAST(frame     AS CHAR), ''),
            COALESCE(CAST(divisor   AS CHAR), ''),
            COALESCE(CAST(entry_x   AS CHAR), ''),
            COALESCE(CAST(exit_x    AS CHAR), ''),
            COALESCE(CAST(max_dip_x AS CHAR), ''),
            COALESCE(CAST(max_diff_abs      AS CHAR), ''),
            COALESCE(CAST(occurrence_count  AS CHAR), '')
        )) AS ctx_uid,

        event_key,
        table_name,
        threshold_pct,
        ext_type,
        frame,
        divisor,
        entry_x,
        exit_x,
        max_dip_x,
        max_diff_abs,
        occurrence_count

    FROM {CTX_SOURCE_TABLE}
    WHERE table_name IN ({_TABLE_LIST_SQL})
      AND event_key IS NOT NULL
      AND threshold_pct IS NOT NULL
      AND ext_type IS NOT NULL
      AND entry_x IS NOT NULL
      AND exit_x IS NOT NULL
"""

CTX_KEY_COLUMNS = ["ctx_uid"]


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _make_weight_code(
    table_name: str,
    var_idx: int,
    ext_type: str,
    entry_x: float,
    mode: int,
    shift: int | None = None,
) -> str:
    """Формат совпадает с extremum_weights.py."""
    tc   = TABLE_CODE_MAP.get(table_name, table_name.replace("brain_rates_", ""))
    etc  = EXT_TYPE_CODE_MAP.get(ext_type, ext_type[0].upper())
    bint = int(round(entry_x * 100))
    base = f"EXT_{tc}_v{var_idx}_{etc}_{bint}_{mode}"
    return base if shift is None else f"{base}_{shift}"


def _get_modification(rates: list[dict]) -> float:
    """Масштабирование под цену инструмента."""
    if not rates:
        return 0.001
    last = float(rates[-1].get("close") or 1.0)
    if last > 10_000:
        return 1_000.0
    if last > 500:
        return 100.0
    return 0.001


def _detect_day_flag(rates: list[dict]) -> int:
    """0 = hourly, 1 = daily."""
    if len(rates) < 2:
        return 0
    gap = (rates[-1]["date"] - rates[-2]["date"]).total_seconds()
    return 1 if gap >= 86_400 * 0.9 else 0


def _detect_table_name(rates: list[dict]) -> str:
    """Определяем инструмент по цене последней свечи."""
    day_flag = _detect_day_flag(rates)
    last     = float(rates[-1].get("close") or 1.0) if rates else 1.0

    if last > 10_000:
        base = "brain_rates_btc_usd"
    elif last > 500:
        base = "brain_rates_eth_usd"
    else:
        base = "brain_rates_eur_usd"

    return base + ("_day" if day_flag else "")


def _current_interval(
    date_a: datetime,
    date_b: datetime,
    divisor: int,
    bar_delta: timedelta,
) -> float:
    """
    Интервал между двумя датами в единицах divisor.
    hours → bars → bars / divisor
    """
    hours = (date_a - date_b).total_seconds() / 3600.0
    bars  = hours / (bar_delta.total_seconds() / 3600.0)
    return bars / divisor


def _in_bin(value: float, center: float, half: float = BIN_HALF) -> bool:
    return abs(value - center) <= half


def _linear_weights(n: int) -> list[float]:
    """Линейные веса [n, n-1, …, 1] — сильнее весят ближайшие к концу."""
    return [float(n - i) for i in range(n)]


# ══════════════════════════════════════════════════════════════════════════════
# model()
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],
    date:          datetime,
    *,
    type:          int         = 0,
    var:           int         = 0,
    param:         str         = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:
    """
    Для каждой свечи date:
      1. Определяем инструмент и порог по `rates` + `var`
      2. Ищем ctx-паттерны для (table_name, threshold, ext_type) в ctx_index
      3. Находим последний экстремум до `date` в key_dates[event_key]
      4. Вычисляем текущий интервал
      5. Если интервал попадает в entry_x или exit_x → генерируем сигнал
         mode=0: T1-сумма исторических entry-баров (с линейными весами)
         mode=1: сила паттерна × знак × mod
      6. Shift-loop: повторяем для shift=0..SHIFT_WINDOW (смещаем точку наблюдения)
    """
    if not rates or dataset_index is None:
        return {}

    by_key:    dict = dataset_index.get("by_key",    {})
    key_dates: dict = dataset_index.get("key_dates", {})
    ctx_index: dict = dataset_index.get("ctx_index", {})

    if not ctx_index:
        return {}

    # ── Инструмент ───────────────────────────────────────────────────────────
    table_name = _detect_table_name(rates)
    day_flag   = _detect_day_flag(rates)
    bar_delta  = timedelta(days=1) if day_flag else timedelta(hours=1)
    mod        = _get_modification(rates)

    # ── Порог по инструменту (не глобальный!) ────────────────────────────────
    thresholds = _TABLE_THRESHOLDS.get(table_name, [0.3, 0.5, 0.8])
    if var >= len(thresholds):
        return {}
    target_thr = thresholds[var]

    # ── Тип экстремума ───────────────────────────────────────────────────────
    target_ext_type = _EXT_TYPE_BY_TYPE.get(type, "all")

    # ── T1 lookup из реальных котировок инструмента ──────────────────────────
    # ВАЖНО: используем `rates` (реальный инструмент), НЕ np_rates (всегда EUR)
    t1_map: dict[datetime, float] = {
        r["date"]: float(r.get("close") or 0) - float(r.get("open") or 0)
        for r in rates
    }

    # ── Ctx-паттерны для (table_name, threshold, ext_type) ───────────────────
    # Допуск 1e-6: MySQL FLOAT/DOUBLE хранит 0.3 как 0.2999999...
    matching_patterns: list[dict] = [
        row for _, row in ctx_index.items()
        if row.get("table_name") == table_name
        and abs(_to_float(row.get("threshold_pct")) - target_thr) < 1e-6
        and row.get("ext_type") == target_ext_type
    ]

    if not matching_patterns:
        return {}

    # ── event_key и исторические даты экстремумов ────────────────────────────
    # Ключ строится так же как в extremum_ctx.py:
    #   f"{table_name}_{thr}_{ext_type}"
    # Python f"{3.0}" = "3.0", f"{0.3}" = "0.3" — совпадает с ключами в БД
    event_key = f"{table_name}_{target_thr}_{target_ext_type}"

    # Пробуем стандартный ключ; если нет — берём event_key из первого паттерна
    dates_sorted: list[datetime] = key_dates.get(event_key, [])
    if not dates_sorted:
        for pat in matching_patterns:
            ek = str(pat.get("event_key") or "")
            if ek and ek in key_dates:
                dates_sorted = key_dates[ek]
                event_key    = ek
                break

    if not dates_sorted:
        return {}

    # divisor из первого паттерна (hourly=24, daily=7)
    divisor = int(matching_patterns[0].get("divisor") or (7 if day_flag else 24))

    # ── Все исторические экстремумы строго до `date` ─────────────────────────
    idx_all        = bisect.bisect_left(dates_sorted, date)
    all_hist_dates = dates_sorted[:idx_all]

    if not all_hist_dates:
        return {}

    # ── Shift-loop ────────────────────────────────────────────────────────────
    result: dict[str, float] = {}

    for shift in range(0, SHIFT_WINDOW + 1):
        # Смещаем точку наблюдения назад на shift баров
        shifted_date = date - bar_delta * shift

        # Последний экстремум строго до shifted_date
        idx_sh = bisect.bisect_left(all_hist_dates, shifted_date)
        if idx_sh == 0:
            continue

        last_ext_dt      = all_hist_dates[idx_sh - 1]
        current_interval = _current_interval(shifted_date, last_ext_dt, divisor, bar_delta)

        for pat in matching_patterns:
            entry_x      = _to_float(pat.get("entry_x"))
            exit_x       = _to_float(pat.get("exit_x"))
            max_diff_abs = _to_float(pat.get("max_diff_abs"), 1.0) or 1.0
            occ          = int(pat.get("occurrence_count") or 1)
            is_recurring = occ >= RECURRING_MIN

            # Одиночные паттерны — только shift=0
            if not is_recurring and shift != 0:
                continue

            shift_arg = shift if is_recurring else None

            in_entry = _in_bin(current_interval, entry_x)
            in_exit  = _in_bin(current_interval, exit_x)

            if not in_entry and not in_exit:
                continue

            # ── Знак сигнала ──────────────────────────────────────────────
            if target_ext_type == "max":
                # После локального максимума: entry → разворот вниз
                sign = -1.0 if in_entry else 0.5

            elif target_ext_type == "min":
                # После локального минимума: entry → разворот вверх
                sign = 1.0 if in_entry else -0.5

            else:  # "all" — по направлению последнего бара
                last_t1 = t1_map.get(last_ext_dt, 0.0)
                if last_t1 == 0:
                    continue
                sign = (
                    ( 1.0 if last_t1 > 0 else -1.0) if in_entry
                    else (-0.5 if last_t1 > 0 else  0.5)
                )

            # ── mode=0: T1-взвешенная сумма исторических entry-баров ──────
            wc0 = _make_weight_code(table_name, var, target_ext_type, entry_x, 0, shift_arg)

            t1_accum = 0.0
            w_total  = 0.0
            hist_sub = all_hist_dates[:idx_sh]   # только до shifted_date
            weights  = _linear_weights(len(hist_sub))

            for j in range(len(hist_sub) - 1, -1, -1):
                if j == 0:
                    break
                prev_dt = hist_sub[j - 1]
                h_intv  = _current_interval(hist_sub[j], prev_dt, divisor, bar_delta)

                if not _in_bin(h_intv, entry_x):
                    continue

                t1_val = t1_map.get(hist_sub[j])
                if t1_val is None:
                    # пробуем следующий бар
                    t1_val = t1_map.get(hist_sub[j] + bar_delta, 0.0)

                w         = weights[j] if j < len(weights) else 1.0
                t1_accum += w * t1_val
                w_total  += w

            if w_total > 0 and t1_accum != 0:
                result[wc0] = result.get(wc0, 0.0) + (t1_accum / w_total)

            # ── mode=1: сила паттерна × знак × mod ───────────────────────
            wc1      = _make_weight_code(table_name, var, target_ext_type, entry_x, 1, shift_arg)
            strength = (max_diff_abs / max(occ, 1)) * sign * mod

            if strength != 0:
                result[wc1] = result.get(wc1, 0.0) + strength

    return {k: round(v, 6) for k, v in result.items() if v != 0}