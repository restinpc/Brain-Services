"""
model.py — Сервис 36: UCDP GED conflict events.

Код веса: {ctx_id}_{mode}_{shift}
  mode=0 → T1, mode=1 → Extremum, shift → 0..72 ч

Датасет: DATASET_QUERY — кросс-БД JOIN brain.vlad_ucdp + vlad_ucdp_context_idx.
Обе БД должны быть на одном MySQL-сервере (запрос идёт через engine_vlad).

VAR-СТРАТЕГИИ:
  var=0  Базовый T1 sum — baseline
  var=1  T1 × severity (avg_deaths) — экономический шок от жертв
  var=2  Только крупные свечи (range > avg_range) — фильтр шума
  var=3  T1 × fear_factor = (1+civilian_ratio) × min(high_estimate_ratio,3)
         ТЕОРЕТИЧЕСКИ ЛУЧШИЙ: медийность × неопределённость = рыночная паника
  var=4  T1 × |T1| квадратичное — усиливает однозначные сигналы
  var=5  Амплитуда крупных свечей — волатильность важнее направления
"""

from __future__ import annotations
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ФРЕЙМВОРКА
# ══════════════════════════════════════════════════════════════════════════════

RATES_TABLE = "brain_rates_eur_usd"

DATASET_QUERY = """
    SELECT
        u.date_start                              AS date,
        COALESCE(u.best_estimate,    0)           AS best_estimate,
        COALESCE(u.deaths_civilians, 0)           AS deaths_civilians,
        COALESCE(u.high_estimate,    0)           AS high_estimate,
        c.id                                      AS ctx_id,
        c.occurrence_count,
        c.avg_deaths,
        COALESCE(c.civilian_ratio,      0)        AS civilian_ratio,
        COALESCE(c.high_estimate_ratio, 1)        AS high_estimate_ratio
    FROM brain.vlad_ucdp u
    JOIN vlad_ucdp_context_idx c
        ON  c.violence_class =
                CASE u.type_of_violence
                    WHEN 1 THEN 'state_based'
                    WHEN 2 THEN 'non_state'
                    WHEN 3 THEN 'one_sided'
                    ELSE 'unknown' END
        AND c.death_class =
                CASE
                    WHEN COALESCE(u.best_estimate, 0) <= 2  THEN 'low'
                    WHEN COALESCE(u.best_estimate, 0) <= 15 THEN 'medium'
                    ELSE 'high' END
        AND c.region =
                CASE
                    WHEN LOWER(COALESCE(u.region,'')) LIKE '%africa%'      THEN 'africa'
                    WHEN LOWER(COALESCE(u.region,'')) LIKE '%americas%'    THEN 'americas'
                    WHEN LOWER(COALESCE(u.region,'')) LIKE '%asia%'        THEN 'asia'
                    WHEN LOWER(COALESCE(u.region,'')) LIKE '%europe%'      THEN 'europe'
                    WHEN LOWER(COALESCE(u.region,'')) LIKE '%middle east%' THEN 'middle east'
                    WHEN LOWER(COALESCE(u.region,'')) LIKE '%oceania%'     THEN 'oceania'
                    ELSE 'other' END
    WHERE u.date_start IS NOT NULL
      AND u.type_of_violence IN (1, 2, 3)
    ORDER BY u.date_start
"""

WEIGHTS_TABLE    = "vlad_ucdp_weights_table"
VAR_RANGE        = [0, 1, 2, 3, 4, 5]
CACHE_DATE_FROM  = "2025-01-15"
REBUILD_INTERVAL = 86400  # раз в сутки (датасет меняется редко)

_SHIFT_WINDOW = 72   # часов — окно поиска недавних событий
_MIN_HISTORY  = 2    # минимум исторических аналогов


# ══════════════════════════════════════════════════════════════════════════════
# model()
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:   list[dict],
    dataset: list[dict],
    date:    datetime,
    *,
    type:  int = 0,
    var:   int = 0,
    param: str = "",
) -> dict[str, float]:
    if not rates or not dataset:
        return {}

    # Lookup котировок
    rates_t1    = {r["date"]: float((r.get("close") or 0) - (r.get("open") or 0))
                   for r in rates}
    rates_range = {r["date"]: float((r.get("max") or 0) - (r.get("min") or 0))
                   for r in rates}
    avg_range   = sum(rates_range.values()) / len(rates_range) if rates_range else 0.0

    # Локальные экстремумы
    sr = sorted(rates, key=lambda r: r["date"])
    ext_max: set = set()
    ext_min: set = set()
    for i in range(1, len(sr) - 1):
        h = float(sr[i].get("max") or 0)
        l = float(sr[i].get("min") or 0)
        if h > float(sr[i-1].get("max") or 0) and h > float(sr[i+1].get("max") or 0):
            ext_max.add(sr[i]["date"])
        if l < float(sr[i-1].get("min") or 0) and l < float(sr[i+1].get("min") or 0):
            ext_min.add(sr[i]["date"])

    last = sr[-1] if sr else None
    is_bull = (float(last.get("close") or 0) > float(last.get("open") or 0)) if last else True
    ext_set = ext_max if is_bull else ext_min

    # Недавние события в окне [date - SHIFT_WINDOW, date]
    window_start = date - timedelta(hours=_SHIFT_WINDOW)
    recent = [e for e in dataset
              if e.get("date") is not None and window_start <= e["date"] <= date]
    if not recent:
        return {}

    result: dict[str, float] = {}

    for event in recent:
        ctx_id = event.get("ctx_id")
        if ctx_id is None:
            continue

        shift = max(0, min(int((date - event["date"]).total_seconds() / 3600), _SHIFT_WINDOW))

        # История — прошлые события того же типа ДО текущего
        historical = [e for e in dataset
                      if e.get("ctx_id") == ctx_id
                      and e.get("date") is not None
                      and e["date"] < event["date"]]
        if len(historical) < _MIN_HISTORY:
            continue

        total_hist = len(historical)

        # Проекция: исторические даты + shift часов
        t_dates = [td for h in historical
                   if (td := h["date"] + timedelta(hours=shift)) < date
                   and td in rates_t1]
        if not t_dates:
            continue

        # Модификаторы вариаций
        severity = 1.0 + min(float(event.get("avg_deaths") or 0) / 10.0, 2.0)
        fear     = (1.0 + float(event.get("civilian_ratio") or 0)) * \
                   min(float(event.get("high_estimate_ratio") or 1), 3.0)

        # ── T1 (mode=0) ───────────────────────────────────────────────────────
        if type in (0, 1):
            if var == 0:
                t1 = sum(rates_t1[d] for d in t_dates)
            elif var == 1:
                t1 = sum(rates_t1[d] for d in t_dates) * severity
            elif var == 2:
                t1 = sum(rates_t1[d] for d in t_dates if rates_range.get(d, 0) > avg_range)
            elif var == 3:
                t1 = sum(rates_t1[d] for d in t_dates) * fear
            elif var == 4:
                t1 = sum((v := rates_t1[d]) * abs(v) for d in t_dates)
            elif var == 5:
                t1 = sum(rates_range[d] - avg_range for d in t_dates
                         if rates_range.get(d, 0) > avg_range)
            else:
                t1 = 0.0

            if t1 != 0.0:
                wc = f"{ctx_id}_0_{shift}"
                result[wc] = result.get(wc, 0.0) + t1

        # ── Extremum (mode=1) ─────────────────────────────────────────────────
        if type in (0, 2):
            ext = _ext_base(t_dates, ext_set, total_hist)
            if ext is not None:
                if var == 1:
                    ext *= severity
                elif var == 2:
                    large = [d for d in t_dates if rates_range.get(d, 0) > avg_range]
                    ext   = _ext_base(large, ext_set, total_hist) if large else None
                elif var == 3:
                    ext *= fear
                elif var == 4:
                    ext = ext * abs(ext)
                elif var == 5:
                    hits = [d for d in t_dates if d in ext_set and rates_range.get(d, 0) > avg_range]
                    ext  = sum(rates_range[d] - avg_range for d in hits) / total_hist if hits else None

            if ext is not None:
                wc = f"{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}


def _ext_base(t_dates: list, ext_set: set, total_hist: int) -> float | None:
    if not t_dates or total_hist == 0:
        return None
    val = (sum(1 for d in t_dates if d in ext_set) / total_hist) * 2 - 1
    return val if val != 0 else None
