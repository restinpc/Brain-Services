"""
model.py — Сервис: Daily Treasury Statement (DTS)

IS_SIMPLE режим: фреймворк определяет по наличию RATES_TABLE.
В этом режиме:
  - нет ctx объекта (нет pair/day/full-mode)
  - dataset_index передаётся со структурой:
      {"dates", "by_key", "key_dates", "key_field", "np_rates"}
    где np_rates = s.np_simple_rates из brain_rates_eur_usd

Код веса: {ctx_id}_{mode}_{shift}
  mode=0 → T1, mode=1 → Extremum, shift 0..72 ч

DATASET_QUERY:
  Выполняется через engine_vlad (IS_SIMPLE, DATASET_ENGINE="vlad").
  Кросс-БД JOIN: brain.vlad_tr_daily_treasury_statement_all + vlad_tr_context_idx.
  Одна строка на день, ctx_id из JOIN.

  JOIN использует ТОЧНО ту же логику классификации что context_idx.py:
    debt_regime   = f(headroom)        → suspended / stress / normal
    tga_level_class = f(tga_closing)   → critical / low / adequate / elevated
  Это гарантирует совпадение JOIN → непустые ctx_id.

VAR-СТРАТЕГИИ (каждая — рыночная гипотеза, источник: BlackRock, Morgan Stanley,
               TaxTracking.com, DailyJobsUpdate.com):

  var=0  BASELINE
         Сырой T1 sum, все типы равнозначны.

  var=1  DEBT CEILING PROXIMITY
         stress режим: T1 × (200 000 / max(headroom, 1 000)), clamp [1, 10]
         suspended:    T1 × 0.1 (нет давления потолка, сигнал ослабляем)
         normal:       T1 × 1.0
         Источник: BlackRock "TGA rebuild helped inform reduction of directional equity
         exposure". Morgan Stanley: $600B TGA rebuild = эквивалент +0.25% rate hike.

  var=2  FTD ECONOMIC SURPRISE
         T1 × (event_ftd / max(avg_ftd_withheld_для_типа, 1))
         Гипотеза: withheld taxes — ежедневный Nonfarm Payrolls (2-дневный лаг).
         Если FTD выше нормы для этого типа → экономика крепче ожиданий →
         Fed hawk → USD ↑, bonds ↓.
         Источник: TaxTracking.com, DailyJobsUpdate.com.

  var=3  TGA REBUILD AMPLIFIER (elevated → drain резервов)
         Только tga_level_class = 'elevated' (TGA > $600B): T1 × rebuild_factor
         rebuild_factor = max(avg_daily_change / 10 000, 0.5), clamp [0.5, 8]
         Остальные уровни: T1 × 1.0.
         Гипотеза: в фазе rebuild Treasury агрессивно занимает → drain резервов →
         предсказуемый USD ↑, risk ↓.

  var=4  TAX SEASON CALENDAR PRECISION
         Только месяцы 1,3,4,6,9,12 И tax_month_ratio >= 0.4 для типа.
         Гипотеза: April (individual filing), Jun/Sep/Dec/Mar (quarterly estimates)
         FTD-потоки однонаправлены и предсказуемы. Вне сезона — шум.

  var=5  LARGE CANDLE FILTER (market confirmation)
         Только свечи range > avg_range.
         Гипотеза: DTS-сигнал имеет значение только когда рынок сам уже движется.
         В тихие дни Treasury-данные игнорируются участниками.
"""

from __future__ import annotations
from datetime import datetime, timedelta
import bisect


def _dt_to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


# 
# КОНФИГ ФРЕЙМВОРКА
# 

# Наличие RATES_TABLE → IS_SIMPLE = True (нет ctx, нет pair/day)
RATES_TABLE = "brain_rates_eur_usd"

# Выполняется через engine_vlad (DATASET_ENGINE = "vlad").
# Подзапрос агрегирует DTS per-day, затем JOIN с vlad_tr_context_idx.
#
# КРИТИЧНО: логика JOIN (debt_regime + tga_level_class) должна совпадать
# с классификацией в context_idx.py. Любое расхождение → JOIN не матчит
# → ctx_id = NULL у всех строк → пустые сигналы.
#
# tga_closing: COALESCE handles NULL post-Apr 2022:
#   pre-Apr 2022:  closing_bal_mil_amt ← закрытие TGA
#   post-Apr 2022: closing_bal_mil_amt = NULL; Treasury переложил в today_mil_amt

# Запрос через vlad — он видит brain.* на том же MySQL-сервере
DATASET_TABLE  = "vlad_tr_dts_dataset"
DATASET_ENGINE = "vlad"
FILTER_DATASET_BY_DATE = True

WEIGHTS_TABLE     = "vlad_tr_weights_table"
CTX_TABLE         = "vlad_tr_context_idx"
CTX_KEY_COLUMNS   = ["id"]
DATASET_KEY       = "ctx_id"    # фреймворк строит by_key индекс по этому полю

VAR_RANGE         = [4, 5]
CACHE_DATE_FROM   = "2025-01-15"
RELOAD_INTERVAL  = 3600
REBUILD_INTERVAL  = 4000

_SHIFT_WINDOW  = 72   # часов — DTS выходит раз в день, реакция до 3 суток
_MIN_HISTORY   = 2    # минимум исторических аналогов для сигнала

_TAX_MONTHS = {1, 3, 4, 6, 9, 12}  # April, June, Sep, Dec, Mar, Jan


# 
# model()
# 

def model(
    rates:         list[dict],
    dataset:       list[dict],
    date:          datetime,
    *,
    type:          int        = 0,
    var:           int        = 0,
    param:         str        = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:
    """
    IS_SIMPLE mode. Фреймворк передаёт dataset_index со структурой:
      {
        "dates":     [...],            # все даты датасета
        "by_key":    {ctx_id: [...]},  # rows сгруппированные по ctx_id
        "key_dates": {ctx_id: [...]},  # даты по ctx_id (для bisect)
        "key_field": "ctx_id",
        "np_rates":  {                 # из brain_rates_eur_usd
          "dates_ns", "t1", "ranges", "avg_range",
          "ext_max", "ext_min", "close", "open"
        }
      }
    """
    if not rates or not dataset:
        return {}

    #  numpy-путь 
    _np = dataset_index.get("np_rates") if dataset_index else None

    if _np is not None:
        import numpy as _np_mod
        _dates_ns     = _np["dates_ns"]
        _t1_arr       = _np["t1"]
        _rng_arr      = _np["ranges"]
        avg_range     = float(_np["avg_range"])
        _cut          = int(_np_mod.searchsorted(_dates_ns, _dt_to_ts(date), side="right"))
        _dates_ns_cut = _dates_ns[:_cut]
        _t1_cut       = _t1_arr[:_cut]
        _rng_cut      = _rng_arr[:_cut]
        _ext_max_cut  = _np["ext_max"][:_cut]
        _ext_min_cut  = _np["ext_min"][:_cut]
        is_bull = (float(_np["close"][_cut-1]) > float(_np["open"][_cut-1])) if _cut > 0 else True
        _ext_cut  = _ext_max_cut if is_bull else _ext_min_cut
        rates_t1  = None
        rates_rng = None
        ext_set   = None
    else:
        # Python fallback
        _np_mod   = None
        rates_t1  = {r["date"]: float((r.get("close") or 0) - (r.get("open") or 0))
                     for r in rates}
        rates_rng = {r["date"]: float((r.get("max") or 0) - (r.get("min") or 0))
                     for r in rates}
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
        is_bull = (float(last.get("close") or 0) > float(last.get("open") or 0)) if last else True
        ext_set = ext_max2 if is_bull else ext_min2

    #  индекс датасета 
    _ds_dates        = dataset_index["dates"]     if dataset_index else [e["date"] for e in dataset]
    _by_ctx          = dataset_index["by_key"]    if dataset_index else {}
    _ctx_dates_cache = dataset_index["key_dates"] if dataset_index else {}

    #  события в окне [date - 72ч, date] 
    window_start = date - timedelta(hours=_SHIFT_WINDOW)
    _i_left  = bisect.bisect_left(_ds_dates,  window_start)
    _i_right = bisect.bisect_right(_ds_dates, date)
    recent   = [e for e in dataset[_i_left:_i_right] if e.get("ctx_id") is not None]
    if not recent:
        return {}

    # Дедупликация (ctx_id, shift) — DTS выходит раз в день
    _seen: set = set()
    recent_dedup: list = []
    for e in recent:
        _k = (e.get("ctx_id"), int((date - e["date"]).total_seconds() / 3600))
        if _k not in _seen:
            _seen.add(_k)
            recent_dedup.append(e)
    recent = recent_dedup

    result: dict[str, float] = {}

    for event in recent:
        ctx_id = event.get("ctx_id")
        if ctx_id is None:
            continue

        shift = max(0, min(int((date - event["date"]).total_seconds() / 3600), _SHIFT_WINDOW))

        # Исторические аналоги — события того же типа до текущего
        ctx_events  = _by_ctx.get(ctx_id, [])
        _ctx_dates  = _ctx_dates_cache.get(ctx_id, [])
        _hi         = bisect.bisect_left(_ctx_dates, event["date"])
        historical  = ctx_events[:_hi]
        if len(historical) < _MIN_HISTORY:
            continue

        total_hist = len(historical)
        _shift_td  = timedelta(hours=shift)

        #  VAR-модификаторы 

        debt_regime   = str(event.get("debt_regime")      or "normal")
        tga_level     = str(event.get("tga_level_class")  or "adequate")
        avg_chg       = float(event.get("avg_daily_change")  or 0)
        avg_ftd_ctx   = float(event.get("avg_ftd_withheld")  or 1)
        event_ftd     = float(event.get("ftd_withheld")      or 0)
        tax_ratio     = float(event.get("tax_month_ratio")   or 0)
        cal_month     = int(event.get("calendar_month")      or 0)
        headroom_val  = event.get("headroom")   # может быть None

        # var=1: Debt Ceiling Proximity
        # stress: обратно пропорционально headroom (чем ближе к потолку → сильнее сигнал)
        # suspended: ослабляем (нет потолкового давления)
        if debt_regime == "stress" and headroom_val is not None:
            debt_w = min(200_000 / max(float(headroom_val), 1_000), 10.0)
        elif debt_regime == "suspended":
            debt_w = 0.1
        else:
            debt_w = 1.0

        # var=2: FTD Economic Surprise
        # actual / avg_for_type: выше нормы → экономика крепче ожиданий
        ftd_surprise = float(event_ftd) / max(float(avg_ftd_ctx), 1.0)
        ftd_surprise = max(0.1, min(ftd_surprise, 5.0))

        # var=3: TGA Rebuild Amplifier
        # elevated = TGA > $600B = пост-потолочный rebuild, drain резервов
        if tga_level == "elevated" and avg_chg > 0:
            rebuild_w = max(0.5, min(avg_chg / 10_000, 8.0))
        else:
            rebuild_w = 1.0

        # var=4: Tax Season Precision
        is_tax_season = (cal_month in _TAX_MONTHS and tax_ratio >= 0.4)

        #  numpy-путь 
        if _np is not None:
            import numpy as _np_i
            _date_ts = _dt_to_ts(date)
            proj_ts  = _np_i.array(
                [_dt_to_ts(h["date"]) + shift * 3600 for h in historical],
                dtype=_np_i.int64)
            proj_ts  = proj_ts[proj_ts < _date_ts]
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
                elif var == 1: t1 = t1_sum * debt_w
                elif var == 2: t1 = t1_sum * ftd_surprise
                elif var == 3: t1 = t1_sum * rebuild_w
                elif var == 4: t1 = t1_sum if is_tax_season else 0.0
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
                    if   var == 1: ext *= debt_w
                    elif var == 2: ext *= ftd_surprise
                    elif var == 3: ext *= rebuild_w
                    elif var == 4: ext = ext if is_tax_season else None
                    elif var == 5:
                        n2 = int(_np_i.count_nonzero(ext_hits[large_mask]))
                        v2 = (n2 / total_hist) * 2 - 1
                        ext = v2 if v2 != 0 else None
                if ext is not None:
                    wc = f"{ctx_id}_1_{shift}"
                    result[wc] = result.get(wc, 0.0) + ext

        else:
            #  Python-fallback 
            t_dates = [td for h in historical
                       if (td := h["date"] + _shift_td) < date
                       and td in rates_t1]
            if not t_dates:
                continue
            t1_sum = sum(rates_t1[d] for d in t_dates)

            if type in (0, 1):
                if   var == 0: t1 = t1_sum
                elif var == 1: t1 = t1_sum * debt_w
                elif var == 2: t1 = t1_sum * ftd_surprise
                elif var == 3: t1 = t1_sum * rebuild_w
                elif var == 4: t1 = t1_sum if is_tax_season else 0.0
                elif var == 5: t1 = sum(rates_t1[d] for d in t_dates
                                        if rates_rng.get(d, 0) > avg_range)
                else:          t1 = 0.0
                if t1 != 0.0:
                    wc = f"{ctx_id}_0_{shift}"
                    result[wc] = result.get(wc, 0.0) + t1

            if type in (0, 2):
                ext = _ext_base(t_dates, ext_set, total_hist)
                if ext is not None:
                    if   var == 1: ext *= debt_w
                    elif var == 2: ext *= ftd_surprise
                    elif var == 3: ext *= rebuild_w
                    elif var == 4: ext = ext if is_tax_season else None
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
