"""
weights.py — Сервис 35: генерация vlad_news_weights_table.

Два режима запуска:
  1. Самостоятельный скрипт (полная пересборка):
         python weights.py
     Читает .env, пересобирает таблицу весов с нуля (TRUNCATE + INSERT).

  2. Вызов фреймворком (инкрементальное обновление):
         from weights import build_weights
         stats = await build_weights(engine_vlad)
     Добавляет weight_codes только для ctx_id без весов (INSERT IGNORE).

Формат кода: NW{ctx_id}_{mode}_{shift}
  mode=0 → T1 (сумма свечей)
  mode=1 → Extremum (вероятность экстремума)
  shift  → 0..SHIFT_MAX для recurring (occurrence_count >= MIN_OCCURRENCE)
            только 0 для одиночных контекстов
"""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()

# 
# КОНФИГ
# 

CTX_TABLE     = "vlad_news_context_idx"
WEIGHTS_TABLE = "vlad_news_weights_table"

SHIFT_MAX      = 24
MIN_OCCURRENCE = 2
MODES          = (0, 1)
WC_PREFIX      = "NW"
BATCH_SIZE     = 5000


# 
# DDL
# 

_DDL_WEIGHTS = f"""
CREATE TABLE IF NOT EXISTS `{WEIGHTS_TABLE}` (
    `id`          INT         NOT NULL AUTO_INCREMENT,
    `weight_code` VARCHAR(40) NOT NULL,
    `ctx_id`      INT         NOT NULL,
    `mode`        TINYINT     NOT NULL DEFAULT 0,
    `shift`       SMALLINT    NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_weight_code` (`weight_code`),
    INDEX idx_ctx_id (`ctx_id`),
    INDEX idx_mode   (`mode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


# 
# ОБЩАЯ ЛОГИКА
# 

def _make_weight_code(ctx_id: int, mode: int, shift: int) -> str:
    return f"{WC_PREFIX}{ctx_id}_{mode}_{shift}"


def _generate_codes(ctx_id: int, occ: int) -> list[tuple]:
    """Генерирует список (wc, ctx_id, mode, shift) для одного контекста."""
    max_shift = SHIFT_MAX if occ >= MIN_OCCURRENCE else 0
    return [
        (_make_weight_code(ctx_id, mode, shift), ctx_id, mode, shift)
        for mode in MODES
        for shift in range(0, max_shift + 1)
    ]


async def _insert_codes(engine_vlad, codes: list[tuple]) -> int:
    """Batch INSERT IGNORE. Возвращает количество добавленных строк."""
    added = 0
    async with engine_vlad.begin() as conn:
        for i in range(0, len(codes), BATCH_SIZE):
            batch = codes[i:i + BATCH_SIZE]
            for wc, ctx_id, mode, shift in batch:
                r = await conn.execute(text(f"""
                    INSERT IGNORE INTO `{WEIGHTS_TABLE}`
                        (weight_code, ctx_id, mode, shift)
                    VALUES (:wc, :cid, :mode, :shift)
                """), {"wc": wc, "cid": ctx_id, "mode": mode, "shift": shift})
                added += r.rowcount
    return added


# 
# РЕЖИМ 1: вызов фреймворком (инкрементальный)
# 

async def build_weights(engine_vlad) -> dict:
    """
    Инкрементально добавляет weight_codes.
    Обрабатывает два случая:
      - Новый ctx_id (ещё нет в весах) → генерируем полный набор
      - Существующий ctx_id, который дорос до recurring
        (occurrence_count >= MIN_OCCURRENCE, но MAX(shift) == 0) → догенерируем shift 1..SHIFT_MAX
    Идемпотентна: INSERT IGNORE — безопасно запускать часто.
    """
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL_WEIGHTS))

    # Все контексты: id + occurrence_count
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id"
        ))
        all_contexts = [(int(r[0]), int(r[1])) for r in res.fetchall()]

    if not all_contexts:
        return {"new_weights": 0, "total_weights": 0, "ctx_total": 0}

    # Для существующих ctx_id читаем MAX(shift) — это покажет
    # был ли контекст одиночным (max_shift=0) и теперь стал recurring
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(
            f"SELECT ctx_id, MAX(shift) FROM `{WEIGHTS_TABLE}` GROUP BY ctx_id"
        ))
        existing_max_shift: dict[int, int] = {int(r[0]): int(r[1]) for r in res.fetchall()}

    new_codes = []
    upgraded  = 0  # одиночные → recurring

    for ctx_id, occ in all_contexts:
        current_max_shift = existing_max_shift.get(ctx_id)

        if current_max_shift is None:
            # Новый ctx_id — генерируем полный набор
            new_codes.extend(_generate_codes(ctx_id, occ))

        elif current_max_shift == 0 and occ >= MIN_OCCURRENCE:
            # Был одиночным (shift=0 only), теперь recurring —
            # догенерируем shift 1..SHIFT_MAX для всех mode
            # (shift=0 уже есть, INSERT IGNORE пропустит дубли)
            new_codes.extend(_generate_codes(ctx_id, occ))
            upgraded += 1

        # else: recurring и уже имеет полный набор — пропускаем

    new_weights = await _insert_codes(engine_vlad, new_codes) if new_codes else 0

    async with engine_vlad.connect() as conn:
        total = (await conn.execute(
            text(f"SELECT COUNT(*) FROM `{WEIGHTS_TABLE}`")
        )).scalar()

    return {
        "new_weights":     new_weights,
        "total_weights":   int(total or 0),
        "ctx_total":       len(all_contexts),
        "ctx_new":         sum(1 for ctx_id, _ in all_contexts if ctx_id not in existing_max_shift),
        "ctx_upgraded":    upgraded,
    }


# 
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# 

async def _run_standalone():
    """Полная пересборка: TRUNCATE + INSERT для всех контекстов из CTX_TABLE."""

    def _make_engine(url: str):
        return create_async_engine(url, pool_pre_ping=True, echo=False)

    vlad_url = (
        f"mysql+aiomysql://{os.getenv('DB_USER','root')}:"
        f"{os.getenv('DB_PASSWORD','')}@"
        f"{os.getenv('DB_HOST','localhost')}:"
        f"{os.getenv('DB_PORT','3306')}/"
        f"{os.getenv('DB_NAME','vlad')}?charset=utf8mb4"
    )
    engine_vlad = _make_engine(vlad_url)

    try:
        print(f"Создание таблицы `{WEIGHTS_TABLE}`...")
        async with engine_vlad.begin() as conn:
            await conn.execute(text(_DDL_WEIGHTS))

        print(f"Загрузка контекстов из `{CTX_TABLE}`...")
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text(
                f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id"
            ))
            contexts = [(int(r[0]), int(r[1])) for r in res.fetchall()]
        print(f"  Загружено контекстов: {len(contexts)}")

        # Генерируем ВСЕ коды
        all_codes = []
        recurring = 0
        for ctx_id, occ in contexts:
            all_codes.extend(_generate_codes(ctx_id, occ))
            if occ >= MIN_OCCURRENCE:
                recurring += 1

        print(f"  Recurring (>= {MIN_OCCURRENCE}): {recurring}")
        print(f"  Одиночных: {len(contexts) - recurring}")
        print(f"  Кодов для вставки: {len(all_codes)}")

        # TRUNCATE перед полной пересборкой
        print("Очистка таблицы...")
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{WEIGHTS_TABLE}`"))

        print("Запись weight_codes...")
        added = await _insert_codes(engine_vlad, all_codes)

        print(f"\nOK: записано {added} строк")
        print(f"    shift_max={SHIFT_MAX} → {(SHIFT_MAX+1)*len(MODES)} кодов на recurring")

        # Примеры
        print("\n-- Примеры weight_code --")
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT weight_code, ctx_id, mode, shift
                FROM `{WEIGHTS_TABLE}`
                ORDER BY ctx_id, mode, shift
                LIMIT 20
            """))
            for wc, cid, mode, shift in res.fetchall():
                print(f"  {wc:<22}  ctx={cid:<5}  mode={mode}  shift={shift}")

        print("\nГотово.")

    finally:
        await engine_vlad.dispose()


if __name__ == "__main__":
    asyncio.run(_run_standalone())
