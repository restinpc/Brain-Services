"""
model.py — Wave Resonance Service (Brain Framework)
====================================================
model() ищет текущую волновую конфигурацию в ctx_index,
и если находит совпадение — возвращает weight codes с резонансными значениями.

Коды вида: WR_{table_code}_{ctx_id}_{mode}_{shift}
  mode=0  значение = текущий нормированный резонансный сигнал
  mode=1  значение = прогноз на shift баров вперёд

Конфигурация сервиса:
  CTX_TABLE        = "brain_wave_resonance_ctx"
  CTX_KEY_COLUMNS  = ["fingerprint_hash"]   ← O(1) lookup по fingerprint
  WEIGHTS_TABLE    = "brain_wave_resonance_weights"
  model_needs_index = True
"""
from __future__ import annotations

import hashlib
import json
import logging
import numpy as np

from wave_resonance import (
    detect_waves_fft, detect_waves_cwt,
    filter_harmonics,
    eval_superposition,
    compute_resonance_factor,
    _find_resonant_pairs,
)
from weights import make_weight_code, CTX_TABLE, WEIGHTS_TABLE, SHIFT_MAX, RECURRING_MIN
from context_idx import FREQ_THRESH, BIN_SIZE, N_COMP, WINDOW

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ СЕРВИСА
# ══════════════════════════════════════════════════════════════════════════════

CTX_TABLE        = CTX_TABLE
CTX_KEY_COLUMNS  = ["fingerprint_hash"]   # ключ ctx_index = (fingerprint,)
WEIGHTS_TABLE    = WEIGHTS_TABLE

TYPES_RANGE  = [0, 1, 2, 3]
VAR_RANGE    = [0, 3, 5, 7, 10]
SHIFT_WINDOW = SHIFT_MAX

# Говорим фреймворку что нам нужен ctx_index в dataset_index
model_needs_index       = True
model_uses_rate_history = True

# ══════════════════════════════════════════════════════════════════════════════
# ПАРАМЕТРЫ ПО УМОЛЧАНИЮ
# ══════════════════════════════════════════════════════════════════════════════

_D_FREQ_THRESH  = FREQ_THRESH
_D_PHASE_THRESH = 0.8
_D_K_RES        = 2.0
_D_K_DAMP       = 0.3
_D_METHOD       = 'fft'
_D_MIN_PERIOD   = 4

# ══════════════════════════════════════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════════════════════

def _extract_prices(rates, dataset_index) -> np.ndarray:
    """
    Цены ВСЕГДА из rates (исторический срез до текущей даты).
    np_rates НЕ используется для цен — он содержит весь текущий массив,
    что приводит к одинаковому fingerprint для всех исторических баров.
    """
    prices = []
    for r in rates:
        v = r.get('close') or r.get('open') or r.get('value')
        if v is not None:
            try:
                prices.append(float(v))
            except (TypeError, ValueError):
                pass
    return np.array(prices, dtype=np.float64)


def _extract_t1(rates, dataset_index) -> float:
    """
    T1 — последнее значение из rates (текущий бар).
    Фолбэк: np_rates['t1'][-1] если в rates нет поля t1.
    """
    # Попробовать из последней записи rates
    if rates:
        last = rates[-1]
        v = last.get('t1')
        if v is not None:
            try:
                return round(float(v), 6)
            except (TypeError, ValueError):
                pass
    # Фолбэк: np_rates
    if dataset_index:
        np_r = dataset_index.get('np_rates')
        if np_r is not None:
            t1_arr = np_r.get('t1')
            if t1_arr is not None and len(t1_arr) > 0:
                try:
                    return round(float(t1_arr[-1]), 6)
                except (TypeError, ValueError):
                    pass
    return 0.0


def _parse_params(param: str) -> dict:
    if not param:
        return {}
    try:
        p = json.loads(param)
        return p if isinstance(p, dict) else {}
    except Exception:
        return {}


def _n_comp(var: int) -> int:
    return max(3, var) if var > 0 else N_COMP


def _make_fingerprint(table_name: str, period_bin: int, n_pairs: int, res_dir: int) -> str:
    s = f"{table_name}|{period_bin}|{n_pairs}|{res_dir}"
    return hashlib.md5(s.encode()).hexdigest()


def _get_table_name(dataset_index) -> str:
    """Извлечь имя таблицы котировок из dataset_index."""
    if dataset_index:
        return dataset_index.get('rates_table', '')
    return ''


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ ВОЛНОВОЙ КОНФИГУРАЦИИ И СИГНАЛОВ
# ══════════════════════════════════════════════════════════════════════════════

def _compute_config_and_signals(
    prices:      np.ndarray,
    n_comp:      int,
    freq_thresh: float,
    phase_thresh: float,
    k_res:       float,
    k_damp:      float,
    method:      str,
    min_period:  int,
    n_forward:   int,
) -> tuple[dict | None, float, list[float]]:
    """
    Возвращает (config, signal_val, fwd_vals).
    config = {period_bin, n_pairs, res_dir} или None если волны не найдены.
    signal_val = нормированный резонансный сигнал на текущем баре.
    fwd_vals   = прогнозные значения на 0..n_forward баров вперёд.
    """
    n = len(prices)

    if method == 'cwt' and n >= 32:
        waves = detect_waves_cwt(prices, n_comp, min_period)
    else:
        waves = detect_waves_fft(prices, n_comp, min_period)

    if not waves:
        return None, 0.0, [0.0] * n_forward

    waves = filter_harmonics(waves)
    dom   = max(waves, key=lambda w: w['amplitude'])

    period_bin = int(round(dom['period'] / BIN_SIZE) * BIN_SIZE)
    period_bin = max(BIN_SIZE, period_bin)

    indices = np.arange(n, dtype=np.float64)
    superpos = eval_superposition(waves, indices)
    factor, res_type = compute_resonance_factor(
        waves, indices,
        freq_thresh=freq_thresh, phase_thresh=phase_thresh,
        k_res=k_res, k_damp=k_damp,
    )
    modulated = superpos * factor
    mod_max   = np.abs(modulated).max()
    scale     = mod_max if mod_max > 1e-12 else 1.0
    mod_norm  = modulated / scale

    n_pairs = min(len(_find_resonant_pairs(waves, freq_thresh)), 127)
    res_dir = int(res_type[-1])

    config = {
        'period_bin': period_bin,
        'n_pairs':    n_pairs,
        'res_dir':    res_dir,
    }

    signal_val = float(mod_norm[-1])

    # Прогноз
    fwd_n   = n + n_forward
    fwd_idx = np.arange(fwd_n, dtype=np.float64)
    fwd_sp  = eval_superposition(waves, fwd_idx)
    fwd_f, _ = compute_resonance_factor(
        waves, fwd_idx,
        freq_thresh=freq_thresh, phase_thresh=phase_thresh,
        k_res=k_res, k_damp=k_damp,
    )
    fwd_norm = (fwd_sp * fwd_f) / scale
    fwd_vals = [round(float(fwd_norm[n + i]), 6) for i in range(n_forward)]

    return config, round(signal_val, 6), fwd_vals


# ══════════════════════════════════════════════════════════════════════════════
# MODEL — Brain Framework контракт
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates,
    dataset,
    date,
    type:          int = 0,
    var:           int = 0,
    param:         str = "",
    dataset_index      = None,
) -> dict[str, float]:
    """
    Точка входа Brain Framework.

    1. Извлечь цены из rates / np_rates
    2. Вычислить волновую конфигурацию (period_bin, n_pairs, res_dir)
    3. Сформировать fingerprint и найти в ctx_index
    4. Если контекст найден — вернуть weight codes с резонансными значениями
    5. Если нет — вернуть {}
    """
    prices = _extract_prices(rates, dataset_index)
    if len(prices) < WINDOW:
        return {}

    ctx_index  = (dataset_index or {}).get('ctx_index', {})
    table_name = _get_table_name(dataset_index)

    extra = _parse_params(param)
    freq_thresh  = float(extra.get('freq_thresh',  _D_FREQ_THRESH))
    phase_thresh = float(extra.get('phase_thresh', _D_PHASE_THRESH))
    k_res        = float(extra.get('k_res',        _D_K_RES))
    k_damp       = float(extra.get('k_damp',       _D_K_DAMP))
    method       = str  (extra.get('method',       _D_METHOD))
    min_period   = int  (extra.get('min_period',   _D_MIN_PERIOD))

    try:
        config, signal_val, fwd_vals = _compute_config_and_signals(
            prices      = prices[-WINDOW:],   # последнее окно = текущая конфигурация
            n_comp      = _n_comp(var),
            freq_thresh = freq_thresh,
            phase_thresh= phase_thresh,
            k_res       = k_res,
            k_damp      = k_damp,
            method      = method,
            min_period  = min_period,
            n_forward   = SHIFT_MAX + 1,      # +1: fwd_vals[0..SHIFT_MAX] все валидны
        )
    except Exception as exc:
        log.error("wave_resonance _compute error: %s", exc, exc_info=True)
        return {}

    if config is None:
        return {}

    # Поиск контекста в ctx_index
    fp  = _make_fingerprint(
        table_name,
        config['period_bin'],
        config['n_pairs'],
        config['res_dir'],
    )
    ctx_row = ctx_index.get((fp,))

    if ctx_row is None:
        # Контекст не зарегистрирован в истории — отдаём пустой результат
        # (новая конфигурация, обнаруженная после последнего rebuild)
        return {}

    ctx_id     = ctx_row['id']
    table_code = ctx_row.get('table_code', 'unk')
    occ        = ctx_row.get('occurrence_count', 0) or 0
    shift_max  = SHIFT_MAX if occ >= RECURRING_MIN else 0

    # ── T1 текущего бара (mode=0) ──────────────────────────────────────────────
    t1_val = _extract_t1(rates, dataset_index)

    result: dict[str, float] = {}

    # mode=0: T1 — базовый код без shift-суффикса (shift=None)
    # Аналог EXT_btcusd_v1_N_475_0 в extremum-сервисе
    result[make_weight_code(table_code, ctx_id, mode=0, shift=None)] = t1_val

    # mode=1: базовый код (shift=None) = прогноз на 0 баров = текущий сигнал
    result[make_weight_code(table_code, ctx_id, mode=1, shift=None)] = (
        fwd_vals[0] if fwd_vals else 0.0
    )

    # mode=1: shifted коды — только для recurring (shift=1..shift_max)
    if shift_max > 0:
        for shift in range(1, shift_max + 1):
            result[make_weight_code(table_code, ctx_id, mode=1, shift=shift)] = (
                fwd_vals[shift] if shift < len(fwd_vals) else 0.0
            )

    return result
