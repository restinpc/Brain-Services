"""
model.py — Earthquakes (USGS)
==============================

Фреймворк: brain_framework v12.
Rebuild:   context_idx.py (build_index) + weights.py (build_weights).

Датасет: vlad_usgs_earthquakes (brain DB).
Одна строка = одно событие × ctx_id из sasha_eq_context_idx.
Группировка контекста: mag_class × tsunami × alert_level → до 12 типов.

Код веса: {ctx_id}_{mode}_{shift}
  mode=0 → T1 (направление свечи),  mode=1 → Extremum
  shift  → 0..SHIFT_WINDOW часов

VAR-СТРАТЕГИИ:
  var=0  BASELINE                  — сырой T1, все типы равнозначны
  var=1  HIGH-SIGNIFICANCE FILTER  — только avg_significance >= 400
  var=2  SHALLOW DEPTH AMPLIFIER   — T1 × (1 / (depth_km/10 + 1))
  var=3  FELT REPORTS FILTER       — только felt_ratio >= 0.1
"""

from __future__ import annotations
from datetime import datetime, timedelta


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ФРЕЙМВОРКА
# ══════════════════════════════════════════════════════════════════════════════

# Обязателен в v12 — определяет какую таблицу котировок грузить.
RATES_TABLE = "brain_rates_eur_usd"

WEIGHTS_TABLE   = "sasha_eq_weights"
CTX_TABLE       = "sasha_eq_context_idx"
CTX_KEY_COLUMNS = ["id"]

DATASET_QUERY = """
    SELECT id, magnitude, tsunami, alert_level,
           depth_km, significance, felt_reports, event_time,
           event_time AS date
    FROM vlad_usgs_earthquakes
    WHERE event_time IS NOT NULL
    ORDER BY event_time
"""
DATASET_ENGINE         = "brain"
FILTER_DATASET_BY_DATE = True
SHIFT_WINDOW           = 24   # часов: окно поиска + нарратив /values
CACHE_DATE_FROM        = "2025-01-15"
VAR_RANGE              = [0, 1, 2, 3]
REBUILD_INTERVAL       = 7200   # раз в сутки

# Внутренняя константа — не читается фреймворком
_MIN_OCCURRENCE = 2   # минимум повторений типа для ненулевого shift


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════════════════════

def _mag_class(mag: float) -> str:
    if mag < 5.5:
        return "small"
    if mag < 6.5:
        return "medium"
    return "large"


def _build_reverse(ctx_index: dict) -> dict:
    """(mag_class, tsunami, alert_level) → (ctx_id, info)."""
    result = {}
    for key, info in ctx_index.items():
        ctx_id = info.get("id")
        mc     = info.get("mag_class")
        ts     = info.get("tsunami")
        al     = info.get("alert_level")
        if ctx_id and mc:
            lookup_key = (mc, int(ts or 0), str(al or "none").lower())
            result[lookup_key] = (ctx_id, info)
    return result


def _apply_var(t1: float, var: int, info: dict) -> float:
    if var == 0:
        return t1
    if var == 1:
        return t1 if (info.get("avg_significance") or 0) >= 400 else 0.0
    if var == 2:
        depth = info.get("avg_depth_km") or 50
        return t1 * (1.0 / (depth / 10.0 + 1.0))
    if var == 3:
        return t1 if (info.get("felt_ratio") or 0) >= 0.1 else 0.0
    return 0.0


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
    Для каждой свечи (date):
      1. Строим обратный индекс (mag_class, tsunami, alert_level) → ctx_id
      2. Ищем события в [date - SHIFT_WINDOW, date]
      3. По ctx_id и shift считаем T1 / Extremum
      4. Применяем var-гипотезу
      5. Возвращаем {weight_code: float}
    """
    if not rates or not dataset:
        return {}

    # ── Обратный индекс ctx ───────────────────────────────────────────────────
    ctx_index = (dataset_index or {}).get("ctx_index") or {}
    if not ctx_index:
        return {}
    reverse = _build_reverse(ctx_index)
    if not reverse:
        return {}

    # ── Детектируем таймфрейм и is_bull из котировок ─────────────────────────
    last_candle = rates[-1] if rates else None
    is_daily    = bool(last_candle) and (
        last_candle["date"].hour == 0 and last_candle["date"].minute == 0
    )
    is_bull = (
        float(last_candle.get("close") or 0) > float(last_candle.get("open") or 0)
    ) if last_candle else True

    # ── Numpy-путь ────────────────────────────────────────────────────────────
    np_rates = (dataset_index or {}).get("np_rates")
    np_view  = None
    if np_rates is not None:
        dates_ns = np_rates.get("dates_ns")
        if dates_ns is not None:
            import numpy as _np
            cut = int(_np.searchsorted(dates_ns, _dt_to_ts(date), side="right"))
            if cut > 0:
                is_bull = (
                    float(np_rates["close"][cut - 1]) > float(np_rates["open"][cut - 1])
                )
            ext_arr = np_rates["ext_max"][:cut] if is_bull else np_rates["ext_min"][:cut]
            np_view = {
                "dates_ns": dates_ns[:cut],
                "t1":       np_rates["t1"][:cut],
                "ext":      ext_arr,
                "cut":      cut,
            }

    # ── Python fallback ───────────────────────────────────────────────────────
    rates_t1 = {}
    ext_set  = set()
    if np_view is None:
        rates_t1 = {
            r["date"]: float((r.get("close") or 0) - (r.get("open") or 0))
            for r in rates
        }
        ext_max: set = set()
        ext_min: set = set()
        for i in range(1, len(rates) - 1):
            h  = float(rates[i].get("max") or 0)
            lo = float(rates[i].get("min") or 0)
            if h  > float(rates[i - 1].get("max") or 0) and h  > float(rates[i + 1].get("max") or 0):
                ext_max.add(rates[i]["date"])
            if lo < float(rates[i - 1].get("min") or 0) and lo < float(rates[i + 1].get("min") or 0):
                ext_min.add(rates[i]["date"])
        ext_set = ext_max if is_bull else ext_min

    # ── Основной цикл ─────────────────────────────────────────────────────────
    result:    dict[str, float] = {}
    WINDOW_SEC = SHIFT_WINDOW * 3600

    for eq in dataset:
        eq_time = eq.get("event_time")
        if eq_time is None:
            continue

        diff_sec = (date - eq_time).total_seconds()
        if diff_sec < 0 or diff_sec > WINDOW_SEC:
            continue

        shift = int(diff_sec / 3600)

        mc     = _mag_class(float(eq.get("magnitude") or 0))
        ts     = int(eq.get("tsunami") or 0)
        al     = str(eq.get("alert_level") or "none").lower()
        lookup = reverse.get((mc, ts, al))
        if lookup is None:
            continue

        ctx_id, info = lookup
        occ = info.get("occurrence_count", 0)

        # Редкие типы — только shift=0
        if occ < _MIN_OCCURRENCE and shift != 0:
            continue

        # Целевая дата котировки
        eq_time_h = eq_time.replace(minute=0, second=0, microsecond=0)
        t_date    = eq_time_h + timedelta(hours=shift)
        if is_daily:
            t_date = t_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if t_date >= date:
            continue

        # ── T1 и Extremum ─────────────────────────────────────────────────────
        if np_view is not None:
            import numpy as _np_i
            t_ts  = _dt_to_ts(t_date)
            idx   = int(_np_i.searchsorted(np_view["dates_ns"], t_ts, side="left"))
            if idx >= np_view["cut"] or int(np_view["dates_ns"][idx]) != t_ts:
                continue
            t1      = float(np_view["t1"][idx])
            ext_hit = bool(np_view["ext"][idx])
        else:
            t1      = rates_t1.get(t_date, 0.0)
            ext_hit = t_date in ext_set

        weighted_t1 = _apply_var(t1, var, info)
        if weighted_t1 != 0.0 and type in (0, 1):
            wc = f"{ctx_id}_0_{shift}"
            result[wc] = result.get(wc, 0.0) + round(weighted_t1, 6)

        if type in (0, 2) and occ > 0 and ext_hit:
            ext = (1.0 / occ) * 2 - 1
            if ext != 0:
                wc = f"{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + round(ext, 6)

    return {k: v for k, v in result.items() if v != 0}
