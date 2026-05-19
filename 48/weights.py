"""
weights.py — Brain Framework build_weights() для Wave Resonance сервиса.

Паттерн weight_code:
    WR_{table_code}_{ctx_id}_{mode}        ← базовый (без shift, для однократных)
    WR_{table_code}_{ctx_id}_{mode}_{shift} ← со сдвигом (для recurring)

  mode=0  — T1 текущего бара (из np_rates['t1'])
  mode=1  — резонансный прогноз на shift баров вперёд
  shift   — 0..SHIFT_MAX для recurring (occurrence_count >= RECURRING_MIN)
             только базовый код для однократных

Соответствие с extremum-сервисом:
  - Базовый код без shift  ← EXT_btcusd_v1_N_475_0
  - Shifted код            ← EXT_btcusd_v1_N_475_0_3
  - TRUNCATE + INSERT      ← не накапливаем устаревшие коды
  - Богатые колонки        ← таблица самодостаточна для отладки
"""

from __future__ import annotations

import os
from sqlalchemy import text
from brain_framework import ensure_weights_table

CTX_TABLE     = "brain_wave_resonance_ctx"
WEIGHTS_TABLE = "brain_wave_resonance_weights"

SHIFT_MAX      = int(os.getenv("WR_SHIFT_MAX",      "12"))
RECURRING_MIN  = int(os.getenv("WR_RECURRING_MIN",  "2"))
TRUNCATE_OUT   = os.getenv("WR_TRUNCATE_OUT", "1") == "1"

_MODES_RAW = os.getenv("WR_MODES", "0,1")
MODES: tuple[int, ...] = tuple(int(x.strip()) for x in _MODES_RAW.split(",") if x.strip())


# ══════════════════════════════════════════════════════════════════════════════
# ФОРМАТ КОДА
# ══════════════════════════════════════════════════════════════════════════════

def make_weight_code(
    table_code: str,
    ctx_id:     int,
    mode:       int,
    shift:      int | None = None,   # None → базовый код без суффикса
) -> str:
    """
    WR_{table_code}_{ctx_id}_{mode}          — базовый
    WR_{table_code}_{ctx_id}_{mode}_{shift}  — со сдвигом
    """
    base = f"WR_{table_code}_{ctx_id}_{mode}"
    return base if shift is None else f"{base}_{shift}"


def decode_weight_code(code: str) -> dict:
    """
    Обратное декодирование: WR_buh_42_1_3 → {table_code, ctx_id, mode, shift}
    """
    parts = code.split("_")
    # WR + table_code + ctx_id + mode [+ shift]
    if len(parts) < 4 or parts[0] != "WR":
        return {}
    try:
        # table_code может содержать _ (например bud), ctx_id — число перед mode
        # mode — предпоследнее или последнее
        # Ищем ctx_id справа: последний числовой элемент перед mode
        mode_i  = -2 if len(parts) >= 5 else -1
        shift   = int(parts[-1]) if len(parts) >= 5 else None
        mode    = int(parts[mode_i])
        ctx_id  = int(parts[mode_i - 1])
        tc      = "_".join(parts[1:mode_i - 1])
        return {"table_code": tc, "ctx_id": ctx_id, "mode": mode, "shift": shift}
    except (ValueError, IndexError):
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# BUILD_WEIGHTS — Brain Framework точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def build_weights(engine_vlad) -> dict:
    """
    Вызывается фреймворком при /rebuild (после build_index).

    1. Создать таблицу через ensure_weights_table (framework helper)
    2. Опционально TRUNCATE (WR_TRUNCATE_OUT=1 по умолчанию)
    3. Загрузить все ctx-строки
    4. Для каждого ctx сгенерировать базовый + shifted коды
    5. Bulk INSERT
    """
    # 1. Создать таблицу (фреймворк знает DDL)
    await ensure_weights_table(engine_vlad, WEIGHTS_TABLE)

    # 2. Опционально очистить перед пересозданием
    if TRUNCATE_OUT:
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{WEIGHTS_TABLE}`"))

    # 3. Загрузить контексты
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT id, table_code, occurrence_count
            FROM `{CTX_TABLE}`
            ORDER BY id
        """))
        contexts = res.fetchall()

    if not contexts:
        return {"table": WEIGHTS_TABLE, "inserted": 0, "patterns": 0}

    # 4. Генерировать коды
    all_rows: list[dict] = []
    recurring = 0
    non_recurring = 0

    for ctx_id, table_code, occ in contexts:
        is_recurring = (occ or 0) >= RECURRING_MIN
        if is_recurring:
            recurring += 1
        else:
            non_recurring += 1

        for mode in MODES:
            # Базовый код — всегда (без shift-суффикса)
            all_rows.append({
                "wc":    make_weight_code(table_code, ctx_id, mode, shift=None),
                "cid":   ctx_id,
                "mode":  mode,
                "shift": 0,       # в таблице shift=0 для базовых кодов
            })

            # Shifted коды — только для recurring
            if is_recurring:
                for shift in range(1, SHIFT_MAX + 1):
                    all_rows.append({
                        "wc":    make_weight_code(table_code, ctx_id, mode, shift=shift),
                        "cid":   ctx_id,
                        "mode":  mode,
                        "shift": shift,
                    })

    # 5. Bulk INSERT (после TRUNCATE дубликатов нет, но IGNORE на всякий случай)
    inserted = 0
    BATCH = 500
    async with engine_vlad.begin() as conn:
        for i in range(0, len(all_rows), BATCH):
            batch = all_rows[i:i + BATCH]
            r = await conn.execute(text(f"""
                INSERT IGNORE INTO `{WEIGHTS_TABLE}`
                    (weight_code, ctx_id, mode, shift)
                VALUES (:wc, :cid, :mode, :shift)
            """), batch)
            inserted += r.rowcount

    return {
        "table":         WEIGHTS_TABLE,
        "ctx_table":     CTX_TABLE,
        "patterns":      len(contexts),
        "recurring":     recurring,
        "non_recurring": non_recurring,
        "generated":     len(all_rows),
        "inserted":      inserted,
        "modes":         list(MODES),
        "shift_max":     SHIFT_MAX,
        "truncated":     TRUNCATE_OUT,
    }
