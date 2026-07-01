"""
context_idx.py — Brain Framework build_index() для Wave Resonance сервиса.

Логика:
  Скользящим окном проходим по каждой brain_rates таблице.
  На каждом баре вычисляем волновую конфигурацию:
    period_bin — доминирующий период, округлённый до BIN_SIZE баров
    n_pairs    — количество резонирующих пар волн
    res_dir    — направление резонанса на текущем баре (-1/0/+1)
  Уникальная тройка (table, period_bin, n_pairs, res_dir) = контекст.
  Считаем сколько раз каждый контекст встречается в истории.
  Результат → brain_wave_resonance_ctx.

Пример fingerprint: md5("brain_rates_btc_usd|25|1|1")
"""

from __future__ import annotations

import hashlib
import os
import numpy as np
from datetime import datetime
from sqlalchemy import text

from brain_framework import ensure_ctx_table, upsert_ctx_rows

from wave_resonance import (
    detect_waves_fft,
    filter_harmonics,
    compute_resonance_factor,
    _find_resonant_pairs,
)

# ══════════════════════════════════════════════════════════════════════════════
# КОНСТАНТЫ
# ══════════════════════════════════════════════════════════════════════════════

CTX_TABLE    = "brain_wave_resonance_ctx"
TRUNCATE_CTX = os.getenv("WR_TRUNCATE_CTX", "1") == "1"

# Таблица → короткий код для weight_code строк
TABLES: dict[str, str] = {
    "brain_rates_btc_usd":      "buh",   # BTC/USD hourly
    "brain_rates_btc_usd_day":  "bud",   # BTC/USD daily
    "brain_rates_eth_usd":      "euh",   # ETH/USD hourly
    "brain_rates_eth_usd_day":  "eud",   # ETH/USD daily
    "brain_rates_eur_usd":      "urh",   # EUR/USD hourly
    "brain_rates_eur_usd_day":  "urd",   # EUR/USD daily
}

WINDOW      = 128    # баров в окне FFT (мин. для разрешения близких частот)
STEP        = 8      # шаг скользящего окна (производительность, WINDOW=128)
BIN_SIZE    = 5      # округление периода в барах (T=23..27 → 25)
N_COMP      = 5      # FFT компонент
FREQ_THRESH = 0.45   # порог близости частот (гармоники 3:2 → Δf/f≈0.4)


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ КОНФИГУРАЦИИ ВОЛНЫ
# ══════════════════════════════════════════════════════════════════════════════

def _wave_config(prices: np.ndarray) -> dict | None:
    """
    Вычислить волновую конфигурацию для окна prices.
    Возвращает {period_bin, n_pairs, res_dir} или None если волны не найдены.
    """
    waves = detect_waves_fft(prices, n_components=N_COMP, min_period=4)
    if not waves:
        return None

    waves = filter_harmonics(waves)
    dom   = max(waves, key=lambda w: w['amplitude'])

    # Доминирующий период → ближайший кратный BIN_SIZE
    period_bin = int(round(dom['period'] / BIN_SIZE) * BIN_SIZE)
    period_bin = max(BIN_SIZE, period_bin)   # минимум = BIN_SIZE

    # Резонирующие пары
    pairs  = _find_resonant_pairs(waves, FREQ_THRESH)
    n_pairs = min(len(pairs), 127)   # TINYINT

    # Направление резонанса на последнем баре окна
    if n_pairs > 0:
        indices = np.arange(len(prices), dtype=np.float64)
        _, res_type = compute_resonance_factor(
            waves, indices, freq_thresh=FREQ_THRESH,
        )
        res_dir = int(res_type[-1])   # -1 / 0 / +1
    else:
        res_dir = 0

    return {
        'period_bin': period_bin,
        'n_pairs':    n_pairs,
        'res_dir':    res_dir,
    }


def _fingerprint(table: str, cfg: dict) -> str:
    """MD5 хеш уникального ключа контекста."""
    s = f"{table}|{cfg['period_bin']}|{cfg['n_pairs']}|{cfg['res_dir']}"
    return hashlib.md5(s.encode()).hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# СКАНИРОВАНИЕ ИСТОРИИ
# ══════════════════════════════════════════════════════════════════════════════

def _scan_table(
    prices: np.ndarray,
    dates:  list[datetime],
    table:  str,
    code:   str,
) -> list[dict]:
    """
    Скользящим окном сканировать таблицу.
    Возвращает список строк готовых к upsert_ctx_rows.
    """
    n = len(prices)
    if n < WINDOW:
        return []

    acc: dict[str, dict] = {}   # fingerprint → накопленная строка

    for end in range(WINDOW, n + 1, STEP):
        window = prices[end - WINDOW: end]
        dt     = dates[end - 1]

        try:
            cfg = _wave_config(window)
        except Exception:
            continue

        if cfg is None:
            continue

        fp = _fingerprint(table, cfg)

        if fp not in acc:
            acc[fp] = {
                'fingerprint_hash': fp,
                'occurrence_count': 0,
                'first_dt':         dt,
                'last_dt':          dt,
                # extra columns
                'table_name': table,
                'table_code': code,
                'period_bin': cfg['period_bin'],
                'n_pairs':    cfg['n_pairs'],
                'res_dir':    cfg['res_dir'],
            }

        row = acc[fp]
        row['occurrence_count'] += 1
        if dt < row['first_dt']:
            row['first_dt'] = dt
        if dt > row['last_dt']:
            row['last_dt'] = dt

    return list(acc.values())


# ══════════════════════════════════════════════════════════════════════════════
# BUILD_INDEX — Brain Framework точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    Создать/обновить таблицу контекстов brain_wave_resonance_ctx.
    Вызывается фреймворком при /rebuild.

    Возвращает статистику {table_name: upserted_count, ...}.
    """
    # Создать таблицу (идемпотентно)
    extra = (
        "`table_name`  VARCHAR(64)  NOT NULL DEFAULT '', "
        "`table_code`  VARCHAR(8)   NOT NULL DEFAULT '', "
        "`period_bin`  SMALLINT     NOT NULL DEFAULT 0,  "
        "`n_pairs`     TINYINT      NOT NULL DEFAULT 0,  "
        "`res_dir`     TINYINT      NOT NULL DEFAULT 0,  "
    )
    await ensure_ctx_table(engine_vlad, CTX_TABLE, extra_columns=extra)

    stats: dict[str, int] = {}

    # При TRUNCATE_CTX=1 очищаем старые контексты перед пересозданием.
    # Нужно когда меняются WINDOW / FREQ_THRESH / BIN_SIZE —
    # старые fingerprint больше не будут совпадать с моделью.
    if TRUNCATE_CTX:
        async with engine_vlad.begin() as c2:
            await c2.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))

    for table, code in TABLES.items():
        try:
            # Загрузить цены из brain DB
            async with engine_brain.connect() as conn:
                res = await conn.execute(
                    text(f"SELECT date, close FROM `{table}` ORDER BY date")
                )
                rows_db = res.fetchall()

            if not rows_db:
                stats[table] = 0
                continue

            prices = np.array([float(r[1]) for r in rows_db], dtype=np.float64)
            dates  = [r[0] for r in rows_db]

            # Сканировать паттерны
            ctx_rows = _scan_table(prices, dates, table, code)

            if not ctx_rows:
                stats[table] = 0
                continue

            # Upsert в контекстную таблицу
            fp_to_id = await upsert_ctx_rows(engine_vlad, CTX_TABLE, ctx_rows)
            stats[table] = len(fp_to_id)

        except Exception as exc:
            stats[table] = -1
            # не прерываем — обрабатываем остальные таблицы
            import traceback as _tb
            print(f"[build_index] {table}: {exc}\n{_tb.format_exc()}")

    return stats
