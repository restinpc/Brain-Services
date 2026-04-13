"""
weights.py — Сервис 36: генерация vlad_ucdp_weights_table.

Два режима:
  1. python weights.py          — полная пересборка (TRUNCATE + INSERT)
  2. from weights import build_weights
     await build_weights(engine_vlad)  — инкрементальное обновление

Формат кода: {ctx_id}_{mode}_{shift}
  mode=0  → T1
  mode=1  → Extremum
  shift   → 0..SHIFT_MAX часов

ctx_id в отдельной колонке не нужен — он уже закодирован в weight_code.
Таблица содержит только список допустимых кодов для фреймворка (/weights endpoint).
"""

from __future__ import annotations

import os

import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

CTX_TABLE     = "vlad_ucdp_context_idx"
WEIGHTS_TABLE = "vlad_ucdp_weights_table"

SHIFT_MAX      = 72
MIN_OCCURRENCE = 3
MODES          = (0, 1)
BATCH_SIZE     = 2000

_DDL_WEIGHTS = f"""
CREATE TABLE IF NOT EXISTS `{WEIGHTS_TABLE}` (
    id           INT         NOT NULL AUTO_INCREMENT,
    weight_code  VARCHAR(32) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_weight_code (weight_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='UCDP GED weight codes s36'
"""


# ══════════════════════════════════════════════════════════════════════════════
# ОБЩАЯ ЛОГИКА
# ══════════════════════════════════════════════════════════════════════════════

def _make_code(ctx_id: int, mode: int, shift: int) -> str:
    return f"{ctx_id}_{mode}_{shift}"


def _generate_codes(ctx_id: int, occ: int) -> list[str]:
    max_shift = SHIFT_MAX if occ >= MIN_OCCURRENCE else 0
    return [
        _make_code(ctx_id, mode, shift)
        for mode in MODES
        for shift in range(0, max_shift + 1)
    ]


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 1: вызов фреймворком (async, инкрементальный)
# ══════════════════════════════════════════════════════════════════════════════

async def build_weights(engine_vlad) -> dict:
    """
    Инкрементально добавляет weight_codes для новых и доросших ctx_id.
    Идемпотентна: INSERT IGNORE.
    """
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL_WEIGHTS))

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id"
        ))
        all_contexts = [(int(r[0]), int(r[1])) for r in res.fetchall()]

    if not all_contexts:
        return {"new_weights": 0, "total_weights": 0, "ctx_total": 0}

    # Читаем существующие коды чтобы понять полноту набора по каждому ctx_id
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT weight_code FROM `{WEIGHTS_TABLE}`"
        ))
        existing: set[str] = {r[0] for r in res.fetchall()}

    new_codes = []
    upgraded  = 0

    for ctx_id, occ in all_contexts:
        shift0 = _make_code(ctx_id, 0, 0)   # минимальный код для этого ctx_id
        has_any = shift0 in existing

        if not has_any:
            # Новый ctx_id
            new_codes.extend(_generate_codes(ctx_id, occ))

        elif occ >= MIN_OCCURRENCE:
            # Проверяем: есть ли уже shift=SHIFT_MAX (полный набор)?
            shift_max_code = _make_code(ctx_id, 0, SHIFT_MAX)
            if shift_max_code not in existing:
                # Был одиночным, дорос до recurring → догенерируем
                new_codes.extend(_generate_codes(ctx_id, occ))
                upgraded += 1

    added = 0
    if new_codes:
        async with engine_vlad.begin() as conn:
            for i in range(0, len(new_codes), BATCH_SIZE):
                for wc in new_codes[i:i + BATCH_SIZE]:
                    r = await conn.execute(text(
                        f"INSERT IGNORE INTO `{WEIGHTS_TABLE}` (weight_code) VALUES (:wc)"
                    ), {"wc": wc})
                    added += r.rowcount

    async with engine_vlad.connect() as conn:
        total = (await conn.execute(
            text(f"SELECT COUNT(*) FROM `{WEIGHTS_TABLE}`")
        )).scalar()

    return {
        "new_weights":   added,
        "total_weights": int(total or 0),
        "ctx_total":     len(all_contexts),
        "ctx_new":       sum(1 for cid, _ in all_contexts
                             if _make_code(cid, 0, 0) not in existing),
        "ctx_upgraded":  upgraded,
    }


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# ══════════════════════════════════════════════════════════════════════════════

def main():
    vlad = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database="vlad",
        autocommit=False,
    )
    try:
        cur = vlad.cursor()

        print(f"Создание таблицы `{WEIGHTS_TABLE}`...")
        cur.execute(_DDL_WEIGHTS)
        vlad.commit()

        print(f"Загрузка контекстов из `{CTX_TABLE}`...")
        cur.execute(f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id")
        contexts = [(int(r[0]), int(r[1])) for r in cur.fetchall()]
        print(f"  Контекстов: {len(contexts)}")

        all_codes = []
        recurring = 0
        for ctx_id, occ in contexts:
            all_codes.extend(_generate_codes(ctx_id, occ))
            if occ >= MIN_OCCURRENCE:
                recurring += 1

        print(f"  Recurring (>= {MIN_OCCURRENCE}): {recurring}")
        print(f"  Одиночных: {len(contexts) - recurring}")
        print(f"  Кодов всего: {len(all_codes)}")

        print("Очистка таблицы...")
        cur.execute(f"TRUNCATE TABLE `{WEIGHTS_TABLE}`")
        vlad.commit()

        print("Запись...")
        sql = f"INSERT IGNORE INTO `{WEIGHTS_TABLE}` (weight_code) VALUES (%s)"
        added = 0
        for i in range(0, len(all_codes), BATCH_SIZE):
            cur.executemany(sql, [(wc,) for wc in all_codes[i:i + BATCH_SIZE]])
            vlad.commit()
            added += cur.rowcount

        print(f"\nOK: {added} строк")

        print("\n-- Примеры --")
        cur.execute(f"SELECT weight_code FROM `{WEIGHTS_TABLE}` LIMIT 10")
        for (wc,) in cur.fetchall():
            print(f"  {wc}")
        print("\nГотово.")
    finally:
        vlad.close()


if __name__ == "__main__":
    main()
