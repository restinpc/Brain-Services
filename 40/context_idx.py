"""
context_idx.py — Buybacks Security Details.
============================================
Строит таблицу типов buyback-событий vlad_tr_buybacks_context_idx.

Группировка: maturity_bucket × accepted
  maturity_bucket : short (<5yr) | medium (5-15yr) | long (>15yr)
      Остаточная дюрация на дату операции — определяет какой участок кривой
      доходности затрагивает buyback.
  accepted        : 0 (offered, not accepted) | 1 (par_amt_accepted > 0)
      Принятые buybacks дают прямое ценовое давление.
      Отклонённые — dealers не захотели продавать (ждут роста цен).

Максимум типов: 3 × 2 = 6 (реально 5–6).

Датасет НЕ строится здесь — model.py читает brain.vlad_tr_buybacks_security_details
напрямую через DATASET_QUERY с кросс-БД JOIN (vlad видит brain.*).

Два режима:
  1. python context_idx.py           — полная пересборка (TRUNCATE + INSERT)
  2. from context_idx import build_index
     await build_index(engine_vlad, engine_brain)  — инкрементальное обновление

КРИТИЧНО: логика матч матurity_bucket здесь и в DATASET_QUERY model.py должна совпадать.
  Любое расхождение → JOIN не матчит → ctx_id = NULL → пустые сигналы.
  Пороги: residual_yr < 5 → short; < 15 → medium; иначе long.
"""

from __future__ import annotations

import os
import sys

import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

CTX_TABLE = "vlad_tr_buybacks_context_idx"

_DDL_CTX = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
    id                 INT          NOT NULL AUTO_INCREMENT,
    maturity_bucket    VARCHAR(8)   NOT NULL COMMENT 'short|medium|long',
    accepted           TINYINT      NOT NULL COMMENT '0=rejected|1=accepted',
    occurrence_count   INT          NOT NULL DEFAULT 0,
    avg_par_accepted   DOUBLE       NULL     COMMENT 'Средний total_par этого типа (тыс. $)',
    avg_coupon         DOUBLE       NULL     COMMENT 'Средний купон (%)',
    avg_residual_yr    DOUBLE       NULL     COMMENT 'Средняя остаточная дюрация (лет)',
    avg_price          DOUBLE       NULL     COMMENT 'Средняя weighted_avg_accepted_price',
    acceptance_ratio   DOUBLE       NULL     COMMENT 'Доля операций где хоть что-то принято',
    first_dt           DATE         NULL,
    last_dt            DATE         NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_ctx (maturity_bucket, accepted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Buybacks: ctx_index для Brain microservice'
"""

# Агрегируем сырую таблицу до уровня (maturity_bucket, accepted)
# для построения типов событий. Это только для ctx_index —
# датасет model.py строит самостоятельно через DATASET_QUERY.
_AGGREGATE_SQL = """
SELECT
    CASE
        WHEN DATEDIFF(
            STR_TO_DATE(maturity_date,  '%Y-%m-%d'),
            STR_TO_DATE(operation_date, '%Y-%m-%d')
        ) / 365.25 < 5  THEN 'short'
        WHEN DATEDIFF(
            STR_TO_DATE(maturity_date,  '%Y-%m-%d'),
            STR_TO_DATE(operation_date, '%Y-%m-%d')
        ) / 365.25 < 15 THEN 'medium'
        ELSE 'long'
    END AS maturity_bucket,

    CASE
        WHEN COALESCE(
            CAST(NULLIF(NULLIF(par_amt_accepted, ''), 'NULL') AS DECIMAL(20,2)),
            0
        ) > 0 THEN 1 ELSE 0
    END AS accepted,

    COUNT(DISTINCT operation_date)                                    AS occurrence_count,

    AVG(COALESCE(
        CAST(NULLIF(NULLIF(par_amt_accepted, ''), 'NULL') AS DECIMAL(20,2)),
        0
    ))                                                                AS avg_par_accepted,

    AVG(CAST(NULLIF(NULLIF(coupon_rate_pct, ''), 'NULL') AS DECIMAL(10,3)))
                                                                      AS avg_coupon,
    AVG(DATEDIFF(
        STR_TO_DATE(maturity_date,  '%Y-%m-%d'),
        STR_TO_DATE(operation_date, '%Y-%m-%d')
    ) / 365.25)                                                       AS avg_residual_yr,

    AVG(CAST(
        NULLIF(NULLIF(weighted_avg_accepted_price, ''), 'NULL')
        AS DECIMAL(10,3)
    ))                                                                AS avg_price,

    SUM(CASE WHEN COALESCE(
            CAST(NULLIF(NULLIF(par_amt_accepted, ''), 'NULL') AS DECIMAL(20,2)),
            0
        ) > 0 THEN 1 ELSE 0 END) / COUNT(DISTINCT operation_date)   AS acceptance_ratio,

    MIN(STR_TO_DATE(operation_date, '%Y-%m-%d'))                      AS first_dt,
    MAX(STR_TO_DATE(operation_date, '%Y-%m-%d'))                      AS last_dt

FROM vlad_tr_buybacks_security_details
WHERE operation_date IS NOT NULL
  AND maturity_date  IS NOT NULL
  AND STR_TO_DATE(maturity_date,  '%Y-%m-%d') >
      STR_TO_DATE(operation_date, '%Y-%m-%d')
GROUP BY maturity_bucket, accepted
ORDER BY maturity_bucket, accepted
"""


# ──────────────────────────────────────────────────────────────
# РЕЖИМ 1: вызов фреймворком (async)
# ──────────────────────────────────────────────────────────────

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    Полная пересборка ctx_index.
    Вызывается фреймворком при /rebuild_index.
    """
    # Создаём таблицу
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL_CTX))

    # Агрегируем из brain
    async with engine_brain.connect() as conn:
        res  = await conn.execute(text(_AGGREGATE_SQL))
        rows = res.fetchall()

    if not rows:
        return {
            "ctx_total": 0,
            "warning": "vlad_tr_buybacks_security_details пуста или нет данных",
        }

    # TRUNCATE + INSERT
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))
        for row in rows:
            (mb, acc, occ, avg_par, avg_coupon,
             avg_res, avg_price, acc_ratio, fd, ld) = row
            await conn.execute(text(f"""
                INSERT INTO `{CTX_TABLE}`
                    (maturity_bucket, accepted, occurrence_count,
                     avg_par_accepted, avg_coupon, avg_residual_yr,
                     avg_price, acceptance_ratio, first_dt, last_dt)
                VALUES (:mb, :acc, :occ,
                        :avg_par, :avg_coupon, :avg_res,
                        :avg_price, :acc_ratio, :fd, :ld)
            """), {
                "mb": str(mb), "acc": int(acc or 0), "occ": int(occ or 0),
                "avg_par": float(avg_par or 0) if avg_par is not None else None,
                "avg_coupon": float(avg_coupon or 0) if avg_coupon is not None else None,
                "avg_res": float(avg_res or 0) if avg_res is not None else None,
                "avg_price": float(avg_price or 0) if avg_price is not None else None,
                "acc_ratio": float(acc_ratio or 0) if acc_ratio is not None else None,
                "fd": fd, "ld": ld,
            })

    return {"ctx_total": len(rows)}


# ──────────────────────────────────────────────────────────────
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# ──────────────────────────────────────────────────────────────

def main():
    required = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD',
                'MASTER_HOST', 'MASTER_USER', 'MASTER_PASSWORD']
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"\n Ошибка: не найдены переменные окружения: {', '.join(missing)}")
        sys.exit(1)

    print(f"Подключение к brain ({os.getenv('MASTER_HOST')})...")
    try:
        brain = mysql.connector.connect(
            host=os.getenv("MASTER_HOST"),
            port=int(os.getenv("MASTER_PORT", 3306)),
            user=os.getenv("MASTER_USER"),
            password=os.getenv("MASTER_PASSWORD"),
            database="brain",
            use_pure=True, allow_local_infile=False,
            autocommit=False, connection_timeout=10,
        )
        print(" Подключено к brain")
    except Exception as e:
        print(f" Ошибка: {e}")
        sys.exit(1)

    print(f"Подключение к vlad ({os.getenv('DB_HOST')})...")
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
        print(" Подключено к vlad\n")
    except Exception as e:
        print(f" Ошибка: {e}")
        brain.close()
        sys.exit(1)

    try:
        bc = brain.cursor()
        vc = vlad.cursor()

        print(f"Создание `{CTX_TABLE}`...")
        vc.execute(_DDL_CTX)
        vlad.commit()

        print("Агрегируем vlad_tr_buybacks_security_details...")
        bc.execute(_AGGREGATE_SQL)
        rows = bc.fetchall()
        print(f"  Типов найдено: {len(rows)}")

        if not rows:
            print("  Нет данных — проверь таблицу в brain")
            return

        print(f"\n  {'bucket':<8} {'acc':>4} {'occ':>6} "
              f"{'avg_par':>12} {'avg_res_yr':>11} {'acc_ratio':>10}")
        print("  " + "─" * 58)
        for row in rows:
            mb, acc, occ, avg_par, avg_c, avg_r, avg_p, ar, fd, ld = row
            print(f"  {str(mb):<8} {int(acc or 0):>4} {int(occ or 0):>6} "
                  f"{float(avg_par or 0):>12.0f} "
                  f"{float(avg_r or 0):>11.1f} "
                  f"{float(ar or 0):>10.3f}")

        print(f"\nПересборка `{CTX_TABLE}` (TRUNCATE + INSERT)...")
        vc.execute(f"TRUNCATE TABLE `{CTX_TABLE}`")
        vlad.commit()

        for row in rows:
            (mb, acc, occ, avg_par, avg_coupon,
             avg_res, avg_price, acc_ratio, fd, ld) = row
            vc.execute(f"""
                INSERT INTO `{CTX_TABLE}`
                    (maturity_bucket, accepted, occurrence_count,
                     avg_par_accepted, avg_coupon, avg_residual_yr,
                     avg_price, acceptance_ratio, first_dt, last_dt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(mb), int(acc or 0), int(occ or 0),
                float(avg_par    or 0) if avg_par    is not None else None,
                float(avg_coupon or 0) if avg_coupon is not None else None,
                float(avg_res    or 0) if avg_res    is not None else None,
                float(avg_price  or 0) if avg_price  is not None else None,
                float(acc_ratio  or 0) if acc_ratio  is not None else None,
                fd, ld,
            ))
        vlad.commit()
        print(f"   Записано типов: {len(rows)}")
        print(f"\n Готово. ctx={len(rows)}")

    except Exception as e:
        print(f"\n Ошибка: {e}")
        vlad.rollback()
        raise
    finally:
        brain.close()
        vlad.close()


if __name__ == "__main__":
    main()
