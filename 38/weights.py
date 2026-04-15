"""
weights.py — Сервис DTS: Daily Treasury Statement.
===================================================
Генерирует таблицу vlad_tr_weights_table.

Формат кода: {ctx_id}_{mode}_{shift}
  mode=0  → T1 (направление свечи)
  mode=1  → Extremum (локальный экстремум)
  shift   → 0..SHIFT_MAX часов

Два режима:
  1. python weights.py            — полная пересборка (TRUNCATE + INSERT)
  2. from weights import build_weights
     await build_weights(engine_vlad)  — инкрементальное обновление

SHIFT_MAX = 72 ч: DTS выходит ~16:00 ET раз в день.
Рынок реагирует 1–3 торговых дня. Institutional desks (BlackRock, MS)
реагируют в тот же день или следующий.
"""

from __future__ import annotations

import os
import sys

import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import text

# Загружаем .env из корня проекта
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

CTX_TABLE     = "vlad_tr_context_idx"
WEIGHTS_TABLE = "vlad_tr_weights_table"

SHIFT_MAX      = 72
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
COMMENT='DTS Treasury weight codes'
"""


# 
# ОБЩАЯ ЛОГИКА
# 

def _make_code(ctx_id: int, mode: int, shift: int) -> str:
    return f"{ctx_id}_{mode}_{shift}"


def _generate_codes(ctx_id: int, occ: int) -> list[str]:
    max_shift = SHIFT_MAX if occ >= MIN_OCCURRENCE else 0
    return [
        _make_code(ctx_id, mode, shift)
        for mode  in MODES
        for shift in range(0, max_shift + 1)
    ]


# 
# РЕЖИМ 1: вызов фреймворком (async, инкрементальный)
# 

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
        return {"new_weights": 0, "total_weights": 0, "ctx_total": 0,
                "warning": "ctx_index пустой — сначала запусти /rebuild_index"}

    # Существующие коды — проверяем полноту набора для каждого ctx_id
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT weight_code FROM `{WEIGHTS_TABLE}`"
        ))
        existing: set[str] = {r[0] for r in res.fetchall()}

    new_codes: list[str] = []
    upgraded  = 0

    for ctx_id, occ in all_contexts:
        base_code = _make_code(ctx_id, 0, 0)   # минимальный код для ctx_id

        if base_code not in existing:
            # Новый ctx_id — генерируем весь набор
            new_codes.extend(_generate_codes(ctx_id, occ))

        elif occ >= MIN_OCCURRENCE:
            # Проверяем: есть ли уже полный набор до SHIFT_MAX?
            top_code = _make_code(ctx_id, 0, SHIFT_MAX)
            if top_code not in existing:
                # Был одиночным (occ=1), дорос до recurring → догенерируем
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


# 
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# 

def main():
    # Проверяем переменные окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD']
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        print(f"\n Ошибка: Не найдены переменные окружения: {', '.join(missing)}")
        print(f"Текущая директория: {os.getcwd()}")
        print(f"Путь к .env: {os.path.join(os.path.dirname(__file__), '..', '.env')}")
        print("\nПроверьте наличие файла .env в корне проекта с переменными:")
        print("DB_HOST=localhost")
        print("DB_PORT=3306")
        print("DB_USER=your_user")
        print("DB_PASSWORD=your_password")
        sys.exit(1)

    print(f"Подключение к базе данных vlad ({os.getenv('DB_HOST')}:{os.getenv('DB_PORT')})...")

    try:
        vlad = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database="vlad",
            use_pure=True,
            allow_local_infile=False,
            autocommit=False,
            connection_timeout=10,
        )
        print(" Подключено к vlad\n")
    except Exception as e:
        print(f" Ошибка подключения к vlad: {e}")
        sys.exit(1)

    try:
        cur = vlad.cursor()

        print(f"Создание таблицы `{WEIGHTS_TABLE}` в vlad...")
        cur.execute(_DDL_WEIGHTS)
        vlad.commit()
        print("   Таблица создана")

        print(f"Загрузка контекстов из `{CTX_TABLE}`...")
        cur.execute(f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id")
        contexts = [(int(r[0]), int(r[1])) for r in cur.fetchall()]

        if not contexts:
            print(f"   Таблица `{CTX_TABLE}` пуста — сначала запусти context_idx.py")
            return

        print(f"   Контекстов: {len(contexts)}")

        all_codes: list[str] = []
        recurring = one_shot = 0
        for ctx_id, occ in contexts:
            codes = _generate_codes(ctx_id, occ)
            all_codes.extend(codes)
            if occ >= MIN_OCCURRENCE:
                recurring += 1
            else:
                one_shot  += 1

        print(f"  Recurring (occ >= {MIN_OCCURRENCE}): {recurring}  "
              f"→ {(SHIFT_MAX + 1) * len(MODES)} кодов каждый")
        print(f"  Одиночных (occ < {MIN_OCCURRENCE}):  {one_shot}  "
              f"→ {len(MODES)} кода каждый (только shift=0)")
        print(f"  Кодов всего: {len(all_codes)}")

        print("Очистка таблицы (TRUNCATE)...")
        cur.execute(f"TRUNCATE TABLE `{WEIGHTS_TABLE}`")
        vlad.commit()
        print("   Таблица очищена")

        print("Запись батчами...")
        sql   = f"INSERT IGNORE INTO `{WEIGHTS_TABLE}` (weight_code) VALUES (%s)"
        added = 0
        total_codes = len(all_codes)

        for i in range(0, total_codes, BATCH_SIZE):
            batch = all_codes[i:i + BATCH_SIZE]
            cur.executemany(sql, [(wc,) for wc in batch])
            vlad.commit()
            added += cur.rowcount
            pct = min((i + BATCH_SIZE) * 100 // total_codes, 100)
            print(f"  {pct:>3}%  ({min(i + BATCH_SIZE, total_codes)}/{total_codes})",
                  end="\r")

        print(f"\n   Записано строк: {added}")

        # Примеры
        print("\n-- Примеры кодов --")
        cur.execute(f"SELECT weight_code FROM `{WEIGHTS_TABLE}` LIMIT 12")
        for (wc,) in cur.fetchall():
            print(f"  {wc}")

        print("\n-- Итог по ctx_id --")
        cur.execute(f"""
            SELECT
                CAST(SUBSTRING_INDEX(weight_code,'_',1) AS UNSIGNED) AS ctx_id,
                COUNT(*) AS codes
            FROM `{WEIGHTS_TABLE}`
            GROUP BY ctx_id
            ORDER BY ctx_id
        """)
        print(f"  {'ctx_id':>8} {'codes':>8} {'status':>10}")
        print("  " + "" * 28)
        for r in cur.fetchall():
            ctx_id, codes = r[0], r[1]
            expected_codes = (SHIFT_MAX + 1) * len(MODES)
            status = " full" if codes == expected_codes else " partial"
            print(f"  {ctx_id:>8} {codes:>8} {status:>10}")

        print(f"\n Готово. Всего кодов: {added}")

    except Exception as e:
        print(f"\n Ошибка: {e}")
        vlad.rollback()
        raise
    finally:
        vlad.close()


if __name__ == "__main__":
    main()