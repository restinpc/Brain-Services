"""
wave_resonance.py — Wave Resonance Algorithm
==============================================
Физически корректная суперпозиция синусоид с резонансом.

Принципы:
  - FFT / CWT для автодетекции доминирующих компонент
  - Попарная проверка резонанса по двум критериям:
      1. Близость частот (|Δf| / f_mean < freq_thresh)
      2. Близость мгновенных фаз (непрерывное, через cos(Δφ))
  - Итоговый коэффициент модуляции — среднее по парам, непрерывное в [-1, 1]
  - Нормировка выхода: сигнал всегда в [-1, 1]

Режимы автодетекции:
  'fft'  — быстрый, хорошо для периодических сигналов
  'cwt'  — медленнее, но точнее для нестационарных рядов (Morlet)
"""

from __future__ import annotations

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
from typing import Optional

# ══════════════════════════════════════════════════════════════════════════════
# ДЕTREND + НОРМИРОВКА
# ══════════════════════════════════════════════════════════════════════════════

def _detrend_normalize(prices: np.ndarray) -> tuple[np.ndarray, float, float]:
    """
    Линейный детренд + нормировка по СКО.
    Возвращает (normalized, std, mean_price).
    std нужен чтобы потом перевести сигнал обратно в цены.
    """
    n = len(prices)
    x = np.arange(n, dtype=np.float64)
    # polyfit быстрее lstsq для степени 1
    coeffs = np.polyfit(x, prices, 1)
    detrended = prices - np.polyval(coeffs, x)
    std = detrended.std()
    if std < 1e-12:
        return np.zeros(n), std, float(prices.mean())
    return detrended / std, std, float(prices.mean())


# ══════════════════════════════════════════════════════════════════════════════
# АВТОДЕТЕКЦИЯ ВОЛН: FFT
# ══════════════════════════════════════════════════════════════════════════════

def detect_waves_fft(
    prices: np.ndarray,
    n_components: int = 5,
    min_period: int = 4,
    max_period_ratio: float = 0.5,   # max_period = n * ratio
) -> list[dict]:
    """
    Обнаружение N доминирующих частотных компонент через FFT.

    Возвращает список словарей:
      { frequency, period, amplitude, phase }

    sorted по убыванию amplitude.
    """
    n = len(prices)
    if n < 16:
        return []

    normalized, std, _ = _detrend_normalize(prices)
    if std < 1e-12:
        return []

    max_period = int(n * max_period_ratio)

    # Hann-окно уменьшает spectral leakage
    window = np.hanning(n)
    windowed = normalized * window
    # Коррекция амплитуды за окно: sum(hann)/n ≈ 0.5
    window_correction = 1.0 / (window.mean() or 0.5)

    spectrum  = np.fft.rfft(windowed)
    freqs     = np.fft.rfftfreq(n)              # cycles per bar
    amplitudes = np.abs(spectrum) * (2.0 / n) * window_correction
    phases    = np.angle(spectrum)

    # Рабочий диапазон частот
    with np.errstate(divide='ignore', invalid='ignore'):
        periods = np.where(freqs > 0, 1.0 / freqs, np.inf)

    valid = np.where(
        (freqs > 0) &
        (periods >= min_period) &
        (periods <= max_period)
    )[0]

    if len(valid) == 0:
        return []

    # Топ-N по амплитуде
    top_n   = min(n_components, len(valid))
    top_idx = valid[np.argsort(amplitudes[valid])[::-1]][:top_n]

    waves = []
    for idx in top_idx:
        f  = float(freqs[idx])
        waves.append({
            'frequency': f,
            'period':    float(1.0 / f),
            'amplitude': float(amplitudes[idx]),
            'phase':     float(phases[idx]),
            'source':    'fft',
        })

    return waves


# ══════════════════════════════════════════════════════════════════════════════
# АВТОДЕТЕКЦИЯ ВОЛН: CWT (Morlet)
# ══════════════════════════════════════════════════════════════════════════════

def detect_waves_cwt(
    prices: np.ndarray,
    n_components: int = 5,
    min_period: int = 4,
    max_period_ratio: float = 0.5,
    wavelet_w: float = 6.0,       # Morlet angular frequency
) -> list[dict]:
    """
    Обнаружение N доминирующих компонент через CWT (Morlet).
    Точнее для нестационарных рядов — выдаёт мгновенную частоту и фазу.

    Возвращает тот же формат что detect_waves_fft.
    """
    n = len(prices)
    if n < 32:
        return detect_waves_fft(prices, n_components, min_period, max_period_ratio)

    try:
        from scipy.signal import cwt as scipy_cwt, morlet2, find_peaks
    except ImportError:
        return detect_waves_fft(prices, n_components, min_period, max_period_ratio)

    normalized, std, _ = _detrend_normalize(prices)
    if std < 1e-12:
        return []

    max_period = int(n * max_period_ratio)

    # Шкалы = периоды в барах
    scales_all = np.arange(min_period, max_period + 1, dtype=float)
    # morlet2 scale → period: T = 2π·scale / w
    cwt_scales = scales_all * wavelet_w / (2.0 * np.pi)

    # CWT: (n_scales, n_time)
    coefs = scipy_cwt(normalized, morlet2, cwt_scales, w=wavelet_w)

    # Средняя мощность по времени для каждого масштаба → scalogram
    power_mean = np.abs(coefs).mean(axis=1)  # shape: (n_scales,)

    # Найти пики по scalogram
    peaks, _ = find_peaks(power_mean, distance=2)
    if len(peaks) == 0:
        peaks = np.argsort(power_mean)[::-1]

    top_n    = min(n_components, len(peaks))
    top_idx  = peaks[np.argsort(power_mean[peaks])[::-1]][:top_n]

    waves = []
    for idx in top_idx:
        period = float(scales_all[idx])
        f      = 1.0 / period

        # Мгновенная фаза на последнем баре (angle CWT)
        phase = float(np.angle(coefs[idx, -1]))

        # Амплитуда: нормируем к std ценового ряда
        amp = float(power_mean[idx]) * 2.0 / n

        waves.append({
            'frequency': f,
            'period':    period,
            'amplitude': amp,
            'phase':     phase,
            'source':    'cwt',
        })

    return waves


# ══════════════════════════════════════════════════════════════════════════════
# ФИЛЬТР ГАРМОНИК
# ══════════════════════════════════════════════════════════════════════════════

def filter_harmonics(waves: list[dict], tol: float = 0.06) -> list[dict]:
    """
    Удалить волны, которые являются точными целочисленными гармониками
    доминирующей компоненты (fundamental).

    Гармоники T/2, T/3, T/4... возникают в FFT как артефакты и не несут
    самостоятельной частотной информации. Оставляем fundamental + субгармоники
    (T*2, T*3) и НЕгармонические компоненты.

    tol — допуск кратности (0.06 = ±6%)
    """
    if len(waves) < 2:
        return waves

    # Доминирующая по амплитуде
    dominant = max(waves, key=lambda w: w['amplitude'])
    f_dom    = dominant['frequency']

    kept = []
    for w in waves:
        ratio = w['frequency'] / f_dom
        # Кратные: ratio ≈ 2, 3, 4, ... → это гармоники, убрать
        # ratio ≈ 0.5, 0.33, ... → субгармоники, оставить (несут реальный период)
        is_harmonic = (ratio > 1.1) and (abs(ratio - round(ratio)) < tol)
        if not is_harmonic:
            kept.append(w)

    return kept if kept else waves   # никогда не возвращаем пустой список


# ══════════════════════════════════════════════════════════════════════════════
# СУПЕРПОЗИЦИЯ (ВЕКТОРИЗОВАНА)
# ══════════════════════════════════════════════════════════════════════════════

def eval_superposition(waves: list[dict], indices: np.ndarray) -> np.ndarray:
    """
    Вычислить сумму синусоид на заданных индексах (bars).
    Формула: sum_k A_k * sin(2π * i / T_k + φ_k)
    """
    if not waves:
        return np.zeros(len(indices))

    result = np.zeros(len(indices), dtype=np.float64)
    for w in waves:
        result += w['amplitude'] * np.sin(
            2.0 * np.pi * indices / w['period'] + w['phase']
        )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# РЕЗОНАНСНЫЙ ДВИЖОК
# ══════════════════════════════════════════════════════════════════════════════

def _find_resonant_pairs(
    waves: list[dict],
    freq_thresh: float,
) -> list[tuple[int, int]]:
    """
    Найти пары волн с близкими частотами.
    |f_a - f_b| / ((f_a + f_b)/2) < freq_thresh
    """
    pairs = []
    for a in range(len(waves)):
        for b in range(a + 1, len(waves)):
            fa, fb = waves[a]['frequency'], waves[b]['frequency']
            fmid   = (fa + fb) * 0.5
            if fmid > 0 and abs(fa - fb) / fmid < freq_thresh:
                pairs.append((a, b))
    return pairs


def compute_resonance_factor(
    waves: list[dict],
    indices: np.ndarray,
    freq_thresh: float   = 0.15,
    phase_thresh: float  = 0.8,   # радиан, до π
    k_res:  float        = 2.0,
    k_damp: float        = 0.3,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Вычислить коэффициент модуляции резонанса для каждого бара.

    Физика:
      - cos(Δφ) = +1 → полный конструктивный резонанс → factor = k_res
      - cos(Δφ) = -1 → полный деструктивный резонанс → factor = k_damp
      - cos(Δφ) = 0  → нейтральная зона              → factor = 1.0
      - Линейная интерполяция между этими точками

    Если пар несколько → среднее значение factor по всем парам.

    Возвращает:
      factor_arr [n] — float, коэффициент модуляции
      res_type_arr [n] — +1 конструктив, -1 деструктив, 0 нейтраль
    """
    n = len(indices)
    pairs = _find_resonant_pairs(waves, freq_thresh)

    if not pairs:
        return np.ones(n), np.zeros(n)

    # Накопление факторов по всем парам
    factor_sum = np.zeros(n, dtype=np.float64)
    pair_count = len(pairs)

    for a, b in pairs:
        wa, wb = waves[a], waves[b]
        phase_a = 2.0 * np.pi * indices / wa['period'] + wa['phase']
        phase_b = 2.0 * np.pi * indices / wb['period'] + wb['phase']

        # Мгновенная разность фаз → [-π, π]
        d_phi = (phase_a - phase_b + np.pi) % (2.0 * np.pi) - np.pi

        # cos(Δφ) ∈ [-1, 1]:
        #   +1 → синфазно (конструктив)
        #   -1 → противофазно (деструктив)
        cos_dp = np.cos(d_phi)

        # Порог: применяем резонанс только когда |cos| выше порога
        # phase_thresh ≈ 0.8 рад ↔ cos_thresh ≈ cos(0.8) ≈ 0.697
        cos_thresh = float(np.cos(phase_thresh))

        # Непрерывное отображение:
        # factor = 1 + (k_res - 1) * (cos_dp - cos_thresh) / (1 - cos_thresh)  при cos_dp > cos_thresh
        # factor = 1 + (1 - k_damp) * (cos_dp + cos_thresh) / (1 - cos_thresh) при cos_dp < -cos_thresh
        # factor = 1 в зоне нейтраль
        factor = np.ones(n, dtype=np.float64)

        in_res  = cos_dp >  cos_thresh   # конструктивный
        in_damp = cos_dp < -cos_thresh   # деструктивный

        if np.any(in_res):
            t = (cos_dp[in_res] - cos_thresh) / (1.0 - cos_thresh)
            factor[in_res] = 1.0 + (k_res - 1.0) * t

        if np.any(in_damp):
            t = (-cos_dp[in_damp] - cos_thresh) / (1.0 - cos_thresh)
            factor[in_damp] = 1.0 - (1.0 - k_damp) * t

        factor_sum += factor

    factor_mean = factor_sum / pair_count

    # Тип резонанса по среднему фактору
    res_type = np.zeros(n, dtype=np.float64)
    res_type[factor_mean > 1.05]  =  1.0   # +5% → считаем конструктивом
    res_type[factor_mean < 0.95]  = -1.0   # -5% → деструктив

    return factor_mean, res_type


# ══════════════════════════════════════════════════════════════════════════════
# ОГИБАЮЩАЯ (rolling max/min)
# ══════════════════════════════════════════════════════════════════════════════

def compute_envelope(signal: np.ndarray, window: int = 10) -> tuple[np.ndarray, np.ndarray]:
    """Rolling max/min огибающая сигнала."""
    n = len(signal)
    w = min(window, n)
    if n < w or w < 2:
        m = float(np.mean(signal))
        return np.full(n, m), np.full(n, m)

    windows  = sliding_window_view(signal, w)
    env_up   = np.empty(n)
    env_dn   = np.empty(n)
    env_up[:w - 1] = windows[0].max()
    env_dn[:w - 1] = windows[0].min()
    env_up[w - 1:] = windows.max(axis=1)
    env_dn[w - 1:] = windows.min(axis=1)

    return env_up, env_dn


# ══════════════════════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ АЛГОРИТМА
# ══════════════════════════════════════════════════════════════════════════════

def compute_wave_resonance(
    prices: np.ndarray,
    n_fft_components: int   = 5,
    min_period:       int   = 4,
    freq_thresh:      float = 0.15,
    phase_thresh:     float = 0.8,
    k_res:            float = 2.0,
    k_damp:           float = 0.3,
    n_forward:        int   = 12,
    calc_type:        int   = 0,
    method:           str   = 'fft',   # 'fft' или 'cwt'
    do_filter_harmonics: bool = True,   # убирать кратные гармоники FFT
    extra_params:     dict  = None,
) -> dict[str, float]:
    """
    Основная точка входа. Возвращает dict[str, float] совместимый с Brain Framework.

    Параметры:
      prices          — numpy массив цен (oldest → newest)
      n_fft_components— сколько компонент извлечь
      min_period      — минимальный период в барах
      freq_thresh     — порог близости частот (Δf/f_mean)
      phase_thresh    — порог близости фаз в радианах [0..π]
      k_res           — усиление при конструктивном резонансе
      k_damp          — ослабление при деструктивном резонансе
      n_forward       — количество баров вперёд для проекций
      calc_type       — 0 полный, 1 сигнал+прогноз, 2 метаданные, 3 компоненты
      method          — 'fft' или 'cwt'
      extra_params    — override параметров из JSON строки (param поля)

    Возвращаемые ключи (при calc_type=0):
      res_signal  — текущий нормированный сигнал [-1, 1]
      res_dir     — направление резонанса: +1, 0, -1
      res_factor  — мгновенный коэффициент модуляции
      dom_period  — доминирующий период (баров)
      dom_amp     — амплитуда доминирующей волны
      n_res_pairs — кол-во резонирующих пар
      fwd_0..N    — прогноз N баров вперёд
      wave_N_val  — значение N-й компонентной волны (type=3)
      env_up/dn   — огибающие (type=0)
    """
    if extra_params:
        freq_thresh  = extra_params.get('freq_thresh',  freq_thresh)
        phase_thresh = extra_params.get('phase_thresh', phase_thresh)
        k_res        = extra_params.get('k_res',        k_res)
        k_damp       = extra_params.get('k_damp',       k_damp)
        method       = extra_params.get('method',       method)

    prices = np.asarray(prices, dtype=np.float64)
    n = len(prices)

    if n < 16:
        return {}

    # ── 1. Автодетекция волн ──────────────────────────────────────────────────
    if method == 'cwt' and n >= 32:
        waves = detect_waves_cwt(prices, n_fft_components, min_period)
    else:
        waves = detect_waves_fft(prices, n_fft_components, min_period)

    if not waves:
        return {}

    # Убрать чистые гармоники (T/2, T/3, ...) — они артефакты FFT, не несут
    # самостоятельной информации для резонансного анализа
    if do_filter_harmonics and len(waves) > 1:
        waves = filter_harmonics(waves)

    # ── 2. Суперпозиция на исторических барах ────────────────────────────────
    indices  = np.arange(n, dtype=np.float64)
    superpos = eval_superposition(waves, indices)

    factor, res_type = compute_resonance_factor(
        waves, indices,
        freq_thresh=freq_thresh, phase_thresh=phase_thresh,
        k_res=k_res, k_damp=k_damp,
    )
    modulated = superpos * factor

    # ── 3. Нормировка модулированного сигнала ────────────────────────────────
    mod_max = np.abs(modulated).max()
    scale   = mod_max if mod_max > 1e-12 else 1.0
    mod_norm = modulated / scale

    # ── 4. Текущие значения (последний бар) ──────────────────────────────────
    cur = n - 1
    cur_signal = float(mod_norm[cur])
    cur_type   = float(res_type[cur])
    cur_factor = float(factor[cur])

    result: dict[str, float] = {}

    # ── 5. Тип 0 / 1: сигнал + прогноз ──────────────────────────────────────
    if calc_type in (0, 1):
        result['res_signal'] = round(cur_signal,  6)
        result['res_dir']    = round(cur_type,    0)
        result['res_factor'] = round(cur_factor,  6)

        # Прогноз: экстраполируем синусоиды вперёд
        fwd_n       = n + n_forward
        fwd_indices = np.arange(fwd_n, dtype=np.float64)
        fwd_super   = eval_superposition(waves, fwd_indices)
        fwd_factor, fwd_rtype = compute_resonance_factor(
            waves, fwd_indices,
            freq_thresh=freq_thresh, phase_thresh=phase_thresh,
            k_res=k_res, k_damp=k_damp,
        )
        fwd_mod  = fwd_super * fwd_factor
        fwd_norm = fwd_mod / scale  # та же нормировка что и историческая

        for i in range(n_forward):
            result[f'fwd_{i}'] = round(float(fwd_norm[n + i]), 6)

    # ── 6. Тип 0 / 2: метаданные ─────────────────────────────────────────────
    if calc_type in (0, 2):
        dom = max(waves, key=lambda w: w['amplitude'])
        res_pairs = _find_resonant_pairs(waves, freq_thresh)

        result['dom_period']  = round(dom['period'],    2)
        result['dom_amp']     = round(dom['amplitude'], 6)
        result['n_res_pairs'] = float(len(res_pairs))

        # Доля баров в конструктивном / деструктивном резонансе
        n_total = max(n, 1)
        result['pct_res']  = round(float((res_type ==  1).sum()) / n_total, 4)
        result['pct_damp'] = round(float((res_type == -1).sum()) / n_total, 4)

    # ── 7. Тип 0 / 3: компоненты ─────────────────────────────────────────────
    if calc_type in (0, 3):
        for i, w in enumerate(waves):
            val = w['amplitude'] * np.sin(
                2.0 * np.pi * cur / w['period'] + w['phase']
            )
            result[f'wave_{i}_val']    = round(float(val),          6)
            result[f'wave_{i}_period'] = round(float(w['period']),  2)
            result[f'wave_{i}_amp']    = round(float(w['amplitude']), 6)

    # ── 8. Тип 0: огибающая ──────────────────────────────────────────────────
    if calc_type == 0:
        env_up, env_dn = compute_envelope(mod_norm, window=min(20, n // 5 + 1))
        result['env_up'] = round(float(env_up[cur]), 6)
        result['env_dn'] = round(float(env_dn[cur]), 6)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНАЯ: получить волны + полный сигнал для визуализации
# ══════════════════════════════════════════════════════════════════════════════

def get_full_signal(
    prices: np.ndarray,
    n_fft_components: int   = 5,
    method:           str   = 'fft',
    freq_thresh:      float = 0.15,
    phase_thresh:     float = 0.8,
    k_res:            float = 2.0,
    k_damp:           float = 0.3,
    n_forward:        int   = 12,
    do_filter_harmonics: bool = True,
) -> dict:
    """
    Для отладки и визуализации.
    Возвращает полные массивы: waves, superpos, modulated, res_type, factor, fwd_*.
    """
    prices = np.asarray(prices, dtype=np.float64)
    n = len(prices)

    if method == 'cwt' and n >= 32:
        waves = detect_waves_cwt(prices, n_fft_components)
    else:
        waves = detect_waves_fft(prices, n_fft_components)

    if not waves:
        return {'waves': [], 'error': 'no waves detected'}

    if do_filter_harmonics and len(waves) > 1:
        waves = filter_harmonics(waves)

    indices  = np.arange(n, dtype=np.float64)
    superpos = eval_superposition(waves, indices)

    factor, res_type = compute_resonance_factor(
        waves, indices,
        freq_thresh=freq_thresh, phase_thresh=phase_thresh,
        k_res=k_res, k_damp=k_damp,
    )
    modulated = superpos * factor
    mod_max   = np.abs(modulated).max() or 1.0
    mod_norm  = modulated / mod_max

    # Прогноз
    fwd_indices = np.arange(n, n + n_forward, dtype=np.float64)
    fwd_super   = eval_superposition(waves, fwd_indices)
    fwd_factor, fwd_rtype = compute_resonance_factor(
        waves, fwd_indices,
        freq_thresh=freq_thresh, phase_thresh=phase_thresh,
        k_res=k_res, k_damp=k_damp,
    )
    fwd_mod  = fwd_super * fwd_factor / mod_max

    # Волны по отдельности, нормированные
    wave_signals = []
    for w in waves:
        sig = w['amplitude'] * np.sin(
            2.0 * np.pi * indices / w['period'] + w['phase']
        )
        wave_signals.append((sig / mod_max).tolist())

    return {
        'waves':        waves,
        'superpos':     (superpos / mod_max).tolist(),
        'modulated':    mod_norm.tolist(),
        'res_type':     res_type.tolist(),
        'factor':       factor.tolist(),
        'wave_signals': wave_signals,
        'fwd_mod':      fwd_mod.tolist(),
        'fwd_rtype':    fwd_rtype.tolist(),
    }
