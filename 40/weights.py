"""
weights.py — Buybacks Security Details.
========================================
Генерирует таблицу vlad_tr_buybacks_weights_table.

Формат кода: {ctx_id}_{mode}_{shift}
  mode=0  → T1 (направление свечи)
  mode=1  → Extremum (локальный экстремум)
  shift   → 0..SHIFT_MAX часов

Два режима:
  1. python weights.py            — полная пересборка (TRUNCATE + INSERT)
  2. from weights import build_weights
     await build_weights(engine_vlad)  — инкрементальное обновление

SHIFT_MAX = 72 ч: buyback-операции происходят раз в неделю ~в 13:40 EST.
Рынок реагирует 1–3 торговых дня (research: "sizeable cumulative effects on
bond returns" — Federal Reserve Bank 2024). Институциональные desk'и
(BlackRock, hedge funds) реагируют в тот же день или следующий.

Preferred habitats model: эффект buyback распространяется на near-substitutes
в том же maturity bucket → shift до 72ч покрывает полный цикл реакции.
"""

from __future__ import annotations

import os
import sys

import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

CTX_TABLE     = "vlad_tr_buybacks_context_idx"
WEIGHTS_TABLE = "vlad_tr_buybacks_weights_table"

SHIFT_MAX      = 168
MIN_OCCURRENCE = 2    # occurrence >= 2 → recurring → все shifts; иначе только shift=0
MODES          = (0, 1)
BATCH_SIZE     = 2000

_DDL_WEIGHTS = f"""
CREATE TABLE IF NOT EXISTS `{WEIGHTS_TABLE}` (
    id          INT         NOT NULL AUTO_INCREMENT,
    weight_code VARCHAR(32) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_weight_code (weight_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Buybacks Treasury weight codes'
"""


# ──────────────────────────────────────────────────────────────
# Общая логика
# ──────────────────────────────────────────────────────────────

def _make_code(ctx_id: int, mode: int, shift: int) -> str:
    return f"{ctx_id}_{mode}_{shift}"


def _generate_codes(ctx_id: int, occ: int) -> list[str]:
    """
    Рутинное событие (occ >= MIN_OCCURRENCE):
        Генерируем коды для всех shift от 0 до SHIFT_MAX.
        У нас достаточно исторических примеров → надёжная статистика.

    Редкое событие (occ < MIN_OCCURRENCE):
        Только shift=0 — смотрим только в момент самого события.
        Статистики мало, расширять окно бессмысленно — будет шум.
    """
    max_shift = SHIFT_MAX if occ >= MIN_OCCURRENCE else 0
    return [
        _make_code(ctx_id, mode, shift)
        for mode  in MODES
        for shift in range(0, max_shift + 1)
    ]


# ──────────────────────────────────────────────────────────────
# РЕЖИМ 1: вызов фреймворком (async, инкрементальный)
# ──────────────────────────────────────────────────────────────

async def build_weights(engine_vlad) -> dict:
    """
    Инкрементально добавляет weight_codes для новых и доросших ctx_id.
    INSERT IGNORE — идемпотентна.
    Вызывается фреймворком сразу после build_index в /rebuild_index.
    """
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL_WEIGHTS))

    # Читаем все контексты
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id"
        ))
        all_contexts = [(int(r[0]), int(r[1])) for r in res.fetchall()]

    if not all_contexts:
        return {
            "new_weights": 0, "total_weights": 0, "ctx_total": 0,
            "warning": "ctx_index пустой — сначала запусти /rebuild_index",
        }

    # Существующие коды
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT weight_code FROM `{WEIGHTS_TABLE}`"
        ))
        existing: set[str] = {r[0] for r in res.fetchall()}

    new_codes: list[str] = []
    upgraded = 0

    for ctx_id, occ in all_contexts:
        base_code = _make_code(ctx_id, 0, 0)

        if base_code not in existing:
            # Новый ctx_id — генерируем весь набор
            new_codes.extend(_generate_codes(ctx_id, occ))

        elif occ >= MIN_OCCURRENCE:
            # Дорос до recurring — если ещё нет полного набора, догенерируем
            top_code = _make_code(ctx_id, 0, SHIFT_MAX)
            if top_code not in existing:
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


# ──────────────────────────────────────────────────────────────
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# ──────────────────────────────────────────────────────────────

def main():
    required = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD']
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"\n Ошибка: не найдены переменные окружения: {', '.join(missing)}")
        sys.exit(1)

    print(f"Подключение к vlad ({os.getenv('DB_HOST')}:{os.getenv('DB_PORT')})...")
    try:
        vlad = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database="vlad",
            use_pure=True, allow_local_infile=False,
            autocommit=False, connection_timeout=10,
        )
        print(" Подключено\n")
    except Exception as e:
        print(f" Ошибка: {e}")
        sys.exit(1)

    try:
        cur = vlad.cursor()

        print(f"Создание `{WEIGHTS_TABLE}`...")
        cur.execute(_DDL_WEIGHTS)
        vlad.commit()

        print(f"Загрузка контекстов из `{CTX_TABLE}`...")
        cur.execute(f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id")
        contexts = [(int(r[0]), int(r[1])) for r in cur.fetchall()]
        if not contexts:
            print(f"  `{CTX_TABLE}` пуста — сначала запусти context_idx.py")
            return
        print(f"  Контекстов: {len(contexts)}")

        # Показываем статистику
        print(f"\n  {'ctx_id':>8} {'occ':>6} {'тип':>12} {'кодов':>8}")
        print("  " + "─" * 40)
        all_codes: list[str] = []
        recurring = one_shot = 0
        for ctx_id, occ in contexts:
            codes = _generate_codes(ctx_id, occ)
            all_codes.extend(codes)
            kind = "recurring" if occ >= MIN_OCCURRENCE else "one-shot"
            print(f"  {ctx_id:>8} {occ:>6} {kind:>12} {len(codes):>8}")
            if occ >= MIN_OCCURRENCE:
                recurring += 1
            else:
                one_shot  += 1

        print(f"\n  Recurring (occ >= {MIN_OCCURRENCE}): {recurring} "
              f"→ {(SHIFT_MAX + 1) * len(MODES)} кодов каждый")
        print(f"  Одиночных (occ < {MIN_OCCURRENCE}):  {one_shot} "
              f"→ {len(MODES)} кода каждый (только shift=0)")
        print(f"  Кодов всего: {len(all_codes)}")

        print(f"\nОчистка `{WEIGHTS_TABLE}` (TRUNCATE)...")
        cur.execute(f"TRUNCATE TABLE `{WEIGHTS_TABLE}`")
        vlad.commit()

        print("Запись батчами...")
        sql   = f"INSERT IGNORE INTO `{WEIGHTS_TABLE}` (weight_code) VALUES (%s)"
        added = 0
        total = len(all_codes)

        for i in range(0, total, BATCH_SIZE):
            batch = all_codes[i:i + BATCH_SIZE]
            cur.executemany(sql, [(wc,) for wc in batch])
            vlad.commit()
            added += cur.rowcount
            pct    = min((i + BATCH_SIZE) * 100 // total, 100)
            print(f"  {pct:>3}%  ({min(i + BATCH_SIZE, total)}/{total})", end="\r")

        print(f"\n  Записано строк: {added}")

        # Примеры
        print("\n-- Примеры кодов --")
        cur.execute(f"SELECT weight_code FROM `{WEIGHTS_TABLE}` LIMIT 12")
        for (wc,) in cur.fetchall():
            print(f"  {wc}")

        print(f"\n Готово. Всего кодов: {added}")

    except Exception as e:
        print(f"\n Ошибка: {e}")
        vlad.rollback()
        raise
    finally:
        vlad.close()


if __name__ == "__main__":
    main()
