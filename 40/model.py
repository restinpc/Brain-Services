"""
model.py — Buybacks Security Details.
======================================

Датасет: brain.vlad_tr_buybacks_security_details (кросс-БД через engine_vlad).
  vlad видит brain.* на том же MySQL-сервере — JOIN без отдельной промежуточной таблицы.

IS_SIMPLE режим (задан RATES_TABLE).
Код веса: {ctx_id}_{mode}_{shift},  mode=0 → T1,  mode=1 → Extremum,  shift 0..72 ч

DATASET_QUERY агрегирует raw-таблицу до уровня (operation_date × maturity_bucket × accepted)
и через JOIN добавляет ctx_id из vlad_tr_buybacks_context_idx.
Одна строка = одна операция × один участок кривой × статус принятия.

SHIFT_WINDOW = 72 ч: операции ~раз в неделю, рыночная реакция 1–3 дня.

VAR-СТРАТЕГИИ:

  var=0  BASELINE
         Сырой T1 sum. Все строки датасета (accepted=0 и accepted=1) равнозначны.

  var=1  ACCEPTED ONLY
         Пропускаем accepted=0.
         Гипотеза: рынок реагирует только на фактические покупки Treasury.
         Источник: IMF WP 2025 — "further boost prices for those purchased".

  var=2  VOLUME AMPLIFIER
         T1 × clamp(total_par / avg_par_for_type, 0.2, 5.0)
         Гипотеза: объём операции определяет силу ценового давления.

  var=3  MATURITY CURVE WEIGHTING
         long → ×1.5,  medium → ×1.0,  short → ×0.6
         Гипотеза: длинные бумаги сильнее сжимают term premium → risk-on/off.

  var=4  ACCEPTED × VOLUME COMBINED
         Только accepted=1 + коэффициент объёма. Самый точный фильтр.

  var=5  LARGE CANDLE FILTER
         Только исторические свечи с range > avg_range.
         Гипотеза: сигнал значим только в дни реального движения рынка.
"""

from __future__ import annotations
from datetime import datetime, timedelta
import bisect


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


# ──────────────────────────────────────────────────────────────
# КОНФИГ ФРЕЙМВОРКА
# ──────────────────────────────────────────────────────────────

RATES_TABLE = "brain_rates_eur_usd"   # IS_SIMPLE = True

# DATASET_ENGINE="vlad": фреймворк выполняет запрос через engine_vlad.
# vlad видит brain.* → кросс-БД JOIN без промежуточной таблицы.
#
# Запрос агрегирует securities → (operation_date × maturity_bucket × accepted),
# JOIN-ит с vlad_tr_buybacks_context_idx чтобы получить ctx_id и агрегаты типа.
#
# Поле `date` = operation_date + 18:40 UTC (1:40 PM EST).
# Все числовые поля в source хранятся как TEXT → CAST с NULLIF.
#
# КРИТИЧНО: CASE-выражения maturity_bucket и accepted здесь
# должны совпадать с логикой в context_idx.py (пороги 5 и 15 лет).
DATASET_ENGINE         = "vlad"
FILTER_DATASET_BY_DATE = True
DATASET_KEY            = "ctx_id"

DATASET_QUERY = """
SELECT
    /*
     * date: operation_date + 18:40 UTC (=13:40 EST стандарт).
     * Используем фиксированный offset — допустимое упрощение
     * для hourly-фрейма котировок.
     */
    STR_TO_DATE(CONCAT(s.operation_date, ' 18:00:00'), '%Y-%m-%d %H:%i:%s') AS `date`,

    ctx.id AS ctx_id,

    CASE
        WHEN DATEDIFF(
            STR_TO_DATE(s.maturity_date,  '%Y-%m-%d'),
            STR_TO_DATE(s.operation_date, '%Y-%m-%d')
        ) / 365.25 < 5  THEN 'short'
        WHEN DATEDIFF(
            STR_TO_DATE(s.maturity_date,  '%Y-%m-%d'),
            STR_TO_DATE(s.operation_date, '%Y-%m-%d')
        ) / 365.25 < 15 THEN 'medium'
        ELSE 'long'
    END AS maturity_bucket,

    CASE
        WHEN COALESCE(
            CAST(NULLIF(NULLIF(s.par_amt_accepted, ''), 'NULL') AS DECIMAL(20,2)),
            0
        ) > 0 THEN 1 ELSE 0
    END AS accepted,

    SUM(COALESCE(
        CAST(NULLIF(NULLIF(s.par_amt_accepted, ''), 'NULL') AS DECIMAL(20,2)),
        0
    ))                                                            AS total_par,

    COUNT(*)                                                      AS n_securities,

    AVG(CAST(NULLIF(NULLIF(s.coupon_rate_pct, ''), 'NULL')
        AS DECIMAL(10,3)))                                        AS avg_coupon,

    AVG(DATEDIFF(
        STR_TO_DATE(s.maturity_date,  '%Y-%m-%d'),
        STR_TO_DATE(s.operation_date, '%Y-%m-%d')
    ) / 365.25)                                                   AS avg_residual_yr,

    ctx.occurrence_count,
    ctx.avg_par_accepted

FROM brain.vlad_tr_buybacks_security_details s

JOIN vlad_tr_buybacks_context_idx ctx
    ON ctx.maturity_bucket = CASE
        WHEN DATEDIFF(
            STR_TO_DATE(s.maturity_date,  '%Y-%m-%d'),
            STR_TO_DATE(s.operation_date, '%Y-%m-%d')
        ) / 365.25 < 5  THEN 'short'
        WHEN DATEDIFF(
            STR_TO_DATE(s.maturity_date,  '%Y-%m-%d'),
            STR_TO_DATE(s.operation_date, '%Y-%m-%d')
        ) / 365.25 < 15 THEN 'medium'
        ELSE 'long'
    END
    AND ctx.accepted = CASE
        WHEN COALESCE(
            CAST(NULLIF(NULLIF(s.par_amt_accepted, ''), 'NULL') AS DECIMAL(20,2)),
            0
        ) > 0 THEN 1 ELSE 0
    END

WHERE s.operation_date IS NOT NULL
  AND s.maturity_date  IS NOT NULL
  AND STR_TO_DATE(s.maturity_date,  '%Y-%m-%d') >
      STR_TO_DATE(s.operation_date, '%Y-%m-%d')

GROUP BY
    s.operation_date,
    maturity_bucket,
    accepted,
    ctx.id,
    ctx.occurrence_count,
    ctx.avg_par_accepted

ORDER BY `date`
"""

WEIGHTS_TABLE   = "vlad_tr_buybacks_weights_table"
CTX_TABLE       = "vlad_tr_buybacks_context_idx"
CTX_KEY_COLUMNS = ["id"]

# CTX_QUERY добавляет вычисляемое поле `name`, которое подхватывается
# фреймворком в _build_narrative через info.get("name").
# Формат: "short_accepted", "long_rejected" и т.д.
CTX_QUERY = """
    SELECT
        id,
        CONCAT(
            maturity_bucket,
            '_',
            IF(accepted = 1, 'accepted', 'rejected')
        )                  AS name,
        occurrence_count,
        avg_par_accepted,
        maturity_bucket,
        accepted
    FROM vlad_tr_buybacks_context_idx
"""

VAR_RANGE         = [0, 1, 2, 3, 4, 5]
CACHE_DATE_FROM   = "2025-01-15"
RELOAD_INTERVAL   = 3600
REBUILD_INTERVAL  = 7200

_SHIFT_WINDOW = 168   # часов — 3 торговых дня
_MIN_HISTORY  = 2    # минимум исторических аналогов для сигнала

_MATURITY_WEIGHTS = {
    "long":   1.5,
    "medium": 1.0,
    "short":  0.6,
}


# ──────────────────────────────────────────────────────────────
# model()
# ──────────────────────────────────────────────────────────────

def model(
    rates:          list[dict],
    dataset:        list[dict],
    date:           datetime,
    *,
    type:           int         = 0,
    var:            int         = 0,
    param:          str         = "",
    dataset_index:  dict | None = None,
) -> dict[str, float]:
    """
    IS_SIMPLE mode. Для каждой свечи (date):
      1. Ищем buyback-события в [date - 72ч, date]
      2. ctx_id, shift, accepted, total_par из dataset row
      3. По историческим аналогам смотрим T1 / Extremum
      4. Применяем var-гипотезу
      5. Возвращаем {weight_code: float}
    """
    if not rates or not dataset:
        return {}

    # ── numpy-путь ────────────────────────────────────────────
    _np = dataset_index.get("np_rates") if dataset_index else None

    if _np is not None:
        import numpy as _np_mod
        _dates_ns  = _np["dates_ns"]
        _t1_arr    = _np["t1"]
        _rng_arr   = _np["ranges"]
        avg_range  = float(_np["avg_range"])
        _cut       = int(_np_mod.searchsorted(_dates_ns, _dt_to_ts(date), side="right"))
        _dates_ns_cut = _dates_ns[:_cut]
        _t1_cut    = _t1_arr[:_cut]
        _rng_cut   = _rng_arr[:_cut]
        _ext_max_c = _np["ext_max"][:_cut]
        _ext_min_c = _np["ext_min"][:_cut]
        is_bull = (
            float(_np["close"][_cut-1]) > float(_np["open"][_cut-1])
        ) if _cut > 0 else True
        _ext_cut  = _ext_max_c if is_bull else _ext_min_c
        rates_t1  = None
        rates_rng = None
        ext_set   = None
    else:
        _np_mod   = None
        rates_t1  = {
            r["date"]: float((r.get("close") or 0) - (r.get("open") or 0))
            for r in rates
        }
        rates_rng = {
            r["date"]: float((r.get("max") or 0) - (r.get("min") or 0))
            for r in rates
        }
        avg_range = sum(rates_rng.values()) / len(rates_rng) if rates_rng else 0.0
        ext_max2: set = set()
        ext_min2: set = set()
        for i in range(1, len(rates) - 1):
            h  = float(rates[i].get("max") or 0)
            lo = float(rates[i].get("min") or 0)
            if h  > float(rates[i-1].get("max") or 0) and h  > float(rates[i+1].get("max") or 0):
                ext_max2.add(rates[i]["date"])
            if lo < float(rates[i-1].get("min") or 0) and lo < float(rates[i+1].get("min") or 0):
                ext_min2.add(rates[i]["date"])
        last    = rates[-1] if rates else None
        is_bull = (
            float(last.get("close") or 0) > float(last.get("open") or 0)
        ) if last else True
        ext_set = ext_max2 if is_bull else ext_min2

    # ── Индекс датасета ───────────────────────────────────────
    _ds_dates    = dataset_index["dates"]     if dataset_index else [e["date"] for e in dataset]
    _by_ctx      = dataset_index["by_key"]    if dataset_index else {}
    _ctx_dates_c = dataset_index["key_dates"] if dataset_index else {}

    # ── События в окне [date - SHIFT_WINDOW, date] ────────────
    window_start = date - timedelta(hours=_SHIFT_WINDOW)
    i_l    = bisect.bisect_left(_ds_dates,  window_start)
    i_r    = bisect.bisect_right(_ds_dates, date)
    recent = [e for e in dataset[i_l:i_r] if e.get("ctx_id") is not None]
    if not recent:
        return {}

    # Дедупликация (ctx_id, shift)
    _seen: set = set()
    deduped = []
    for e in recent:
        _k = (e.get("ctx_id"), int((date - e["date"]).total_seconds() / 3600))
        if _k not in _seen:
            _seen.add(_k)
            deduped.append(e)

    result: dict[str, float] = {}

    for event in deduped:
        ctx_id = event.get("ctx_id")
        if ctx_id is None:
            continue

        shift = max(0, min(
            int((date - event["date"]).total_seconds() / 3600),
            _SHIFT_WINDOW,
        ))

        accepted  = int(event.get("accepted")        or 0)
        total_par = float(event.get("total_par")     or 0)
        avg_par   = float(event.get("avg_par_accepted") or 1) or 1.0
        mb        = str(event.get("maturity_bucket") or "medium")

        # ── var-фильтры ───────────────────────────────────────
        if var in (1, 4) and accepted == 0:
            continue

        vol_factor = max(0.2, min(total_par / avg_par, 5.0)) if avg_par > 0 else 1.0
        dur_factor = _MATURITY_WEIGHTS.get(mb, 1.0)

        # ── Исторические аналоги ──────────────────────────────
        ctx_events = _by_ctx.get(ctx_id, [])
        _ctx_dates = _ctx_dates_c.get(ctx_id, [])
        _hi        = bisect.bisect_left(_ctx_dates, event["date"])
        historical = ctx_events[:_hi]
        if len(historical) < _MIN_HISTORY:
            continue

        total_hist = len(historical)
        _shift_td  = timedelta(hours=shift)

        # ── numpy-путь ────────────────────────────────────────
        if _np is not None:
            import numpy as _np_i

            _date_ts = _dt_to_ts(date)
            proj_ts  = _np_i.array(
                [_dt_to_ts(h["date"]) + shift * 3600 for h in historical],
                dtype=_np_i.int64,
            )
            proj_ts = proj_ts[proj_ts < _date_ts]
            if len(proj_ts) == 0:
                continue

            idx   = _np_i.searchsorted(_dates_ns_cut, proj_ts, side="left")
            in_b  = idx < _cut
            exact = _np_i.zeros(len(proj_ts), dtype=bool)
            if _np_i.any(in_b):
                exact[in_b] = _dates_ns_cut[idx[in_b]] == proj_ts[in_b]
            if not _np_i.any(exact):
                continue

            t1_vals    = _t1_cut[idx[exact]]
            rng_vals   = _rng_cut[idx[exact]]
            ext_hits   = _ext_cut[idx[exact]]
            large_mask = rng_vals > avg_range
            t1_sum     = float(_np_i.sum(t1_vals))

            if type in (0, 1):
                if   var == 0: t1 = t1_sum
                elif var == 1: t1 = t1_sum                              # accepted filtered above
                elif var == 2: t1 = t1_sum * vol_factor
                elif var == 3: t1 = t1_sum * dur_factor
                elif var == 4: t1 = t1_sum * vol_factor                 # accepted filtered above
                elif var == 5: t1 = float(_np_i.sum(t1_vals[large_mask]))
                else:          t1 = 0.0
                if t1 != 0.0:
                    wc = f"{ctx_id}_0_{shift}"
                    result[wc] = result.get(wc, 0.0) + t1

            if type in (0, 2):
                n_ext = int(_np_i.count_nonzero(ext_hits))
                val   = (n_ext / total_hist) * 2 - 1
                ext: float | None = val if val != 0 else None
                if ext is not None:
                    if   var == 2: ext *= vol_factor
                    elif var == 3: ext *= dur_factor
                    elif var == 4: ext *= vol_factor
                    elif var == 5:
                        n2  = int(_np_i.count_nonzero(ext_hits[large_mask]))
                        v2  = (n2 / total_hist) * 2 - 1
                        ext = v2 if v2 != 0 else None
                if ext is not None:
                    wc = f"{ctx_id}_1_{shift}"
                    result[wc] = result.get(wc, 0.0) + ext

        else:
            # ── Python fallback ───────────────────────────────
            t_dates = [
                td for h in historical
                if (td := h["date"] + _shift_td) < date and td in rates_t1
            ]
            if not t_dates:
                continue

            t1_sum = sum(rates_t1[d] for d in t_dates)

            if type in (0, 1):
                if   var == 0: t1 = t1_sum
                elif var == 1: t1 = t1_sum
                elif var == 2: t1 = t1_sum * vol_factor
                elif var == 3: t1 = t1_sum * dur_factor
                elif var == 4: t1 = t1_sum * vol_factor
                elif var == 5: t1 = sum(
                    rates_t1[d] for d in t_dates
                    if rates_rng.get(d, 0) > avg_range
                )
                else:           t1 = 0.0
                if t1 != 0.0:
                    wc = f"{ctx_id}_0_{shift}"
                    result[wc] = result.get(wc, 0.0) + t1

            if type in (0, 2):
                ext = _ext_base(t_dates, ext_set, total_hist)
                if ext is not None:
                    if   var == 2: ext *= vol_factor
                    elif var == 3: ext *= dur_factor
                    elif var == 4: ext *= vol_factor
                    elif var == 5:
                        large = [d for d in t_dates if rates_rng.get(d, 0) > avg_range]
                        ext   = _ext_base(large, ext_set, total_hist) if large else None
                if ext is not None:
                    wc = f"{ctx_id}_1_{shift}"
                    result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}


def _ext_base(t_dates: list, ext_set: set, total_hist: int) -> float | None:
    if not t_dates or total_hist == 0:
        return None
    val = (sum(1 for d in t_dates if d in ext_set) / total_hist) * 2 - 1
    return val if val != 0 else None
