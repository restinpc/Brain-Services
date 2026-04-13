"""
context_idx.py — Сервис 36: UCDP GED conflict events.
======================================================
Строит таблицу типов конфликтных событий vlad_ucdp_context_idx.

Группировка: violence_class × death_class × region
  violence_class : state_based | non_state | one_sided  (из type_of_violence)
  death_class    : low | medium | high                  (из best_estimate)
  region         : africa | americas | asia | europe | middle east | oceania | other

Максимум типов: 3 × 3 × 7 = 63 (реально заполненных меньше).

Два режима:
  1. python context_idx.py     — полная пересборка с нуля
  2. from context_idx import build_index
     await build_index(engine_vlad, engine_brain)  — инкрементальное обновление
"""

from __future__ import annotations

import asyncio
import os

import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

CTX_TABLE = "vlad_ucdp_context_idx"

_DDL = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
    id                  INT          NOT NULL AUTO_INCREMENT,
    violence_class      VARCHAR(20)  NOT NULL,
    death_class         VARCHAR(10)  NOT NULL,
    region              VARCHAR(40)  NOT NULL DEFAULT 'other',

    occurrence_count    INT          NOT NULL DEFAULT 0,

    -- Агрегаты для model() — не нужно лезть в датасет при каждом расчёте
    avg_deaths          DOUBLE       NULL COMMENT 'Среднее best_estimate',
    civilian_ratio      DOUBLE       NULL COMMENT 'avg(deaths_civilians / best_estimate)',
    avg_duration_days   DOUBLE       NULL COMMENT 'Средняя длительность события в днях',
    high_estimate_ratio DOUBLE       NULL COMMENT 'avg(high_estimate / best_estimate) — коэф. неопределённости',

    first_dt            DATE         NULL,
    last_dt             DATE         NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uk_ctx (violence_class, death_class, region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='UCDP GED: ctx_index для микросервиса Brain s36'
"""

# SQL для группировки — используется и в standalone, и в framework-режиме
_GROUP_SQL = """
    SELECT
        type_of_violence,
        CASE
            WHEN COALESCE(best_estimate, 0) <= 2  THEN 'low'
            WHEN COALESCE(best_estimate, 0) <= 15 THEN 'medium'
            ELSE 'high'
        END                                                          AS dc,
        CASE
            WHEN LOWER(COALESCE(region,'')) LIKE '%africa%'      THEN 'africa'
            WHEN LOWER(COALESCE(region,'')) LIKE '%americas%'    THEN 'americas'
            WHEN LOWER(COALESCE(region,'')) LIKE '%asia%'        THEN 'asia'
            WHEN LOWER(COALESCE(region,'')) LIKE '%europe%'      THEN 'europe'
            WHEN LOWER(COALESCE(region,'')) LIKE '%middle east%' THEN 'middle east'
            WHEN LOWER(COALESCE(region,'')) LIKE '%oceania%'     THEN 'oceania'
            ELSE 'other'
        END                                                          AS reg,
        COUNT(*)                                                     AS cnt,
        AVG(COALESCE(best_estimate, 0))                              AS avg_deaths,
        AVG(
            CASE WHEN COALESCE(best_estimate, 0) > 0
                 THEN COALESCE(deaths_civilians, 0) / COALESCE(best_estimate, 0)
                 ELSE 0 END
        )                                                            AS civilian_ratio,
        AVG(COALESCE(DATEDIFF(date_end, date_start), 0))            AS avg_duration_days,
        AVG(
            CASE WHEN COALESCE(best_estimate, 0) > 0
                 THEN COALESCE(high_estimate, 0) / COALESCE(best_estimate, 0)
                 ELSE 1 END
        )                                                            AS high_ratio,
        MIN(date_start)                                              AS first_dt,
        MAX(date_start)                                              AS last_dt
    FROM vlad_ucdp
    WHERE date_start IS NOT NULL
      AND type_of_violence IN (1, 2, 3)
    GROUP BY
        type_of_violence,
        CASE
            WHEN COALESCE(best_estimate, 0) <= 2  THEN 'low'
            WHEN COALESCE(best_estimate, 0) <= 15 THEN 'medium'
            ELSE 'high'
        END,
        CASE
            WHEN LOWER(COALESCE(region,'')) LIKE '%africa%'      THEN 'africa'
            WHEN LOWER(COALESCE(region,'')) LIKE '%americas%'    THEN 'americas'
            WHEN LOWER(COALESCE(region,'')) LIKE '%asia%'        THEN 'asia'
            WHEN LOWER(COALESCE(region,'')) LIKE '%europe%'      THEN 'europe'
            WHEN LOWER(COALESCE(region,'')) LIKE '%middle east%' THEN 'middle east'
            WHEN LOWER(COALESCE(region,'')) LIKE '%oceania%'     THEN 'oceania'
            ELSE 'other'
        END
"""

_UPSERT_SQL = f"""
    INSERT INTO `{CTX_TABLE}`
        (violence_class, death_class, region,
         occurrence_count,
         avg_deaths, civilian_ratio, avg_duration_days, high_estimate_ratio,
         first_dt, last_dt)
    VALUES (:vc, :dc, :reg, :cnt, :avg_d, :civ_r, :dur, :hi_r, :fd, :ld)
    ON DUPLICATE KEY UPDATE
        occurrence_count    = VALUES(occurrence_count),
        avg_deaths          = VALUES(avg_deaths),
        civilian_ratio      = VALUES(civilian_ratio),
        avg_duration_days   = VALUES(avg_duration_days),
        high_estimate_ratio = VALUES(high_estimate_ratio),
        last_dt             = GREATEST(last_dt, VALUES(last_dt))
"""


# ══════════════════════════════════════════════════════════════════════════════
# КЛАССИФИКАТОРЫ (используются и в model.py)
# ══════════════════════════════════════════════════════════════════════════════

def violence_class(tov) -> str:
    return {1: "state_based", 2: "non_state", 3: "one_sided"}.get(int(tov or 0), "unknown")


def death_class(best) -> str:
    b = int(best or 0)
    if b <= 2:   return "low"
    if b <= 15:  return "medium"
    return "high"


def normalize_region(raw: str) -> str:
    r = (raw or "").strip().lower()
    for canonical in ("africa", "americas", "asia", "europe", "middle east", "oceania"):
        if canonical in r:
            return canonical
    return "other"


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 1: вызов фреймворком (async, инкрементальный)
# ══════════════════════════════════════════════════════════════════════════════

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    Пересчитывает агрегаты ctx_index из актуального датасета vlad_ucdp.
    Использует UPSERT — ctx_id стабильны, не меняются при обновлении.
    Безопасно запускать часто.
    """
    await engine_vlad.dispose()
    await engine_brain.dispose()

    # Создаём таблицу если не существует
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL))

    # Читаем агрегаты из brain
    async with engine_brain.connect() as conn:
        res = await conn.execute(text(_GROUP_SQL))
        rows = res.fetchall()

    if not rows:
        return {"new_contexts": 0, "total_contexts": 0}

    # UPSERT в vlad
    upserted = 0
    async with engine_vlad.begin() as conn:
        for row in rows:
            tov, dc, reg, cnt, avg_d, civ_r, dur, hi_r, fd, ld = row
            await conn.execute(text(_UPSERT_SQL), {
                "vc":    violence_class(tov),
                "dc":    dc,
                "reg":   reg,
                "cnt":   int(cnt),
                "avg_d": float(avg_d or 0),
                "civ_r": float(civ_r or 0),
                "dur":   float(dur or 0),
                "hi_r":  float(hi_r or 1),
                "fd":    fd,
                "ld":    ld,
            })
            upserted += 1

    async with engine_vlad.connect() as conn:
        total = (await conn.execute(
            text(f"SELECT COUNT(*) FROM `{CTX_TABLE}`")
        )).scalar()

    return {
        "processed_rows": len(rows),
        "upserted":       upserted,
        "total_contexts": int(total or 0),
    }


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# ══════════════════════════════════════════════════════════════════════════════

def main():
    brain = mysql.connector.connect(
        host=os.getenv("MASTER_HOST"),
        port=int(os.getenv("MASTER_PORT", 3306)),
        user=os.getenv("MASTER_USER"),
        password=os.getenv("MASTER_PASSWORD"),
        database="brain",
        autocommit=False,
    )
    vlad = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database="vlad",
        autocommit=False,
    )

    try:
        bc, vc = brain.cursor(), vlad.cursor()

        print("Создание таблицы...")
        vc.execute(_DDL)
        vlad.commit()

        print("Группировка датасета...")
        bc.execute(_GROUP_SQL)
        rows = bc.fetchall()
        print(f"  Типов найдено: {len(rows)}")

        upserted = 0
        for row in rows:
            tov, dc, reg, cnt, avg_d, civ_r, dur, hi_r, fd, ld = row
            vc.execute(f"""
                INSERT INTO `{CTX_TABLE}`
                    (violence_class, death_class, region,
                     occurrence_count,
                     avg_deaths, civilian_ratio, avg_duration_days, high_estimate_ratio,
                     first_dt, last_dt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    occurrence_count    = VALUES(occurrence_count),
                    avg_deaths          = VALUES(avg_deaths),
                    civilian_ratio      = VALUES(civilian_ratio),
                    avg_duration_days   = VALUES(avg_duration_days),
                    high_estimate_ratio = VALUES(high_estimate_ratio),
                    last_dt             = GREATEST(last_dt, VALUES(last_dt))
            """, (
                violence_class(tov), dc, reg,
                int(cnt),
                float(avg_d or 0), float(civ_r or 0),
                float(dur or 0), float(hi_r or 1),
                fd, ld,
            ))
            upserted += 1

        vlad.commit()
        print(f"  Записано/обновлено типов: {upserted}")

        # Итоговая таблица
        vc.execute(f"""
            SELECT id, violence_class, death_class, region,
                   occurrence_count,
                   ROUND(avg_deaths, 1)          AS avg_d,
                   ROUND(civilian_ratio, 3)      AS civ_r,
                   ROUND(high_estimate_ratio, 2) AS hi_r
            FROM `{CTX_TABLE}`
            ORDER BY occurrence_count DESC
            LIMIT 20
        """)
        print(f"\n{'id':>4} {'violence':<14} {'death':<8} {'region':<14} "
              f"{'count':>7} {'avg_d':>7} {'civ_r':>7} {'hi_r':>6}")
        print("─" * 72)
        for r in vc.fetchall():
            print(f"{r[0]:>4} {r[1]:<14} {r[2]:<8} {r[3]:<14} "
                  f"{r[4]:>7} {r[5]:>7} {r[6]:>7} {r[7]:>6}")
        print("\nГотово.")

    finally:
        brain.close()
        vlad.close()


if __name__ == "__main__":
    main()
