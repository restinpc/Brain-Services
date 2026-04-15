"""
context_idx.py — Сервис DTS: Daily Treasury Statement.
=======================================================
Строит таблицу типов Treasury-дней vlad_tr_context_idx.

Группировка: debt_regime × tga_level_class
  debt_regime     : suspended | normal | stress   (из Statutory Debt Limit headroom)
  tga_level_class : critical | low | adequate | elevated  (из TGA closing balance)

Максимум типов: 3 × 4 = 12 (реально ~8-10).

Также строит таблицу vlad_tr_dts_dataset — агрегированные DTS-данные
с уже проставленным ctx_id. Именно её читает DATASET_QUERY в model.py
(DATASET_ENGINE = "vlad"), исключая кросс-БД запросы.

Два режима:
  1. python context_idx.py            — полная пересборка с нуля (TRUNCATE + INSERT)
  2. from context_idx import build_index
     await build_index(engine_vlad, engine_brain)  — инкрементальное обновление (UPSERT)

КРИТИЧНО: логика классификации здесь и в DATASET_QUERY model.py должна совпадать.
  Любое расхождение → JOIN не матчит → ctx_id = NULL → пустые сигналы.
  Пороги: headroom < 200_000 → stress; tga < 100_000 → critical; и т.д.
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
DATASET_TABLE = "vlad_tr_dts_dataset"

_DDL = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
    id                INT NOT NULL AUTO_INCREMENT,
    debt_regime       VARCHAR(12) NOT NULL COMMENT 'suspended|normal|stress',
    tga_level_class   VARCHAR(10) NOT NULL COMMENT 'critical|low|adequate|elevated',
    occurrence_count  INT     NOT NULL DEFAULT 0,
    avg_tga_closing   DOUBLE  NULL COMMENT 'Средний TGA closing (млн $)',
    avg_daily_change  DOUBLE  NULL COMMENT 'Средний Δ TGA day-over-day (млн $)',
    avg_net_issuance  DOUBLE  NULL COMMENT 'Среднее (issues - redemptions), Table III-B',
    avg_ftd_withheld  DOUBLE  NULL COMMENT 'Средние withheld taxes (payroll proxy)',
    avg_headroom      DOUBLE  NULL COMMENT 'Среднее расстояние до потолка',
    tax_month_ratio   DOUBLE  NULL COMMENT 'Доля дней в налоговых месяцах',
    first_dt          DATE    NULL,
    last_dt           DATE    NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_ctx (debt_regime, tga_level_class)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='DTS Treasury: ctx_index для Brain microservice'
"""

# Таблица-датасет: одна строка на торговый день, ctx_id уже проставлен.
# Читается через DATASET_ENGINE="vlad" в model.py — никакого кросс-БД запроса.
_DDL_DATASET = f"""
CREATE TABLE IF NOT EXISTS `{DATASET_TABLE}` (
    date              DATE        NOT NULL,
    ctx_id            INT         NOT NULL,
    tga_closing       DOUBLE      NULL,
    net_issuance      DOUBLE      NULL,
    ftd_withheld      DOUBLE      NULL,
    tax_refunds_eft   DOUBLE      NULL,
    headroom          DOUBLE      NULL,
    calendar_month    TINYINT     NULL,
    occurrence_count  INT         NULL,
    avg_tga_closing   DOUBLE      NULL,
    avg_daily_change  DOUBLE      NULL,
    avg_net_issuance  DOUBLE      NULL,
    avg_ftd_withheld  DOUBLE      NULL,
    avg_headroom      DOUBLE      NULL,
    tax_month_ratio   DOUBLE      NULL,
    debt_regime       VARCHAR(12) NULL,
    tga_level_class   VARCHAR(10) NULL,
    PRIMARY KEY (date),
    INDEX idx_ctx_id (ctx_id),
    INDEX idx_date   (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='DTS датасет с ctx_id — читается model.py через DATASET_ENGINE=vlad'
"""

# SQL для brain — один проход, conditional MAX по всем нужным таблицам DTS
# Выполняется напрямую на brain или через cross-DB (зависит от движка)
_AGGREGATE_SQL = """
    SELECT
        record_date,
        MAX(CAST(NULLIF(record_calendar_month,'') AS UNSIGNED)) AS cal_month,

        /* TGA closing: pre-Apr 2022 = closing_bal_mil_amt,
           post-Apr 2022 = today_mil_amt (Treasury переложил closing туда) */
        COALESCE(
            MAX(CASE
                WHEN (table_nbr = 'I' OR table_nm = 'Operating Cash Balance')
                     AND (group_desc LIKE '%Federal Reserve Account%'
                          OR group_desc LIKE '%Treasury General Account%')
                     AND group_desc NOT LIKE '%Total%'
                     AND closing_bal_mil_amt IS NOT NULL
                     AND closing_bal_mil_amt NOT IN ('','null','NULL')
                THEN CAST(closing_bal_mil_amt AS SIGNED) END),
            MAX(CASE
                WHEN (table_nbr = 'I' OR table_nm = 'Operating Cash Balance')
                     AND (group_desc LIKE '%Federal Reserve Account%'
                          OR group_desc LIKE '%Treasury General Account%'
                          OR group_desc LIKE '%TGA Closing%')
                     AND group_desc NOT LIKE '%Total%'
                     AND closing_bal_mil_amt IS NULL
                     AND today_mil_amt IS NOT NULL
                     AND today_mil_amt NOT IN ('','null','NULL')
                THEN CAST(today_mil_amt AS SIGNED) END)
        ) AS tga_closing,

        /* Net issuance: issues − redemptions (Table III-B) */
        (
            COALESCE(MAX(CASE
                WHEN table_nbr IN ('III-B','IIIB')
                     AND group_desc LIKE '%Total Public Debt Cash Issues%'
                THEN CAST(NULLIF(today_mil_amt,'') AS SIGNED) END), 0)
            -
            COALESCE(MAX(CASE
                WHEN table_nbr IN ('III-B','IIIB')
                     AND group_desc LIKE '%Total Public Debt Cash Redemptions%'
                THEN CAST(NULLIF(today_mil_amt,'') AS SIGNED) END), 0)
        ) AS net_issuance,

        /* FTD withheld taxes (два формата Table IV: до/после Feb 2023) */
        COALESCE(
            MAX(CASE
                WHEN table_nbr = 'IV'
                     AND table_nm LIKE '%Federal Tax%'
                     AND group_desc LIKE '%Withheld Income and Employment%'
                THEN CAST(NULLIF(today_mil_amt,'') AS SIGNED) END),
            MAX(CASE
                WHEN table_nbr = 'IV'
                     AND table_nm LIKE '%Inter-agency%'
                     AND group_desc LIKE '%Withheld%'
                THEN CAST(NULLIF(today_mil_amt,'') AS SIGNED) END)
        ) AS ftd_withheld,

        /* Individual tax refunds EFT (два формата Table V/VI) */
        COALESCE(
            MAX(CASE
                WHEN table_nbr IN ('V','VI')
                     AND group_desc LIKE '%Individual%'
                     AND group_desc LIKE '%EFT%'
                THEN CAST(NULLIF(today_mil_amt,'') AS SIGNED) END),
            MAX(CASE
                WHEN table_nbr = 'V'
                     AND group_desc LIKE '%Individual Tax Refunds (EFT)%%'
                THEN CAST(NULLIF(today_mil_amt,'') AS SIGNED) END)
        ) AS tax_refunds_eft,

        /* Statutory Debt Limit (NULL = приостановлен) */
        MAX(CASE
            WHEN table_nbr IN ('III-C','IIIC')
                 AND group_desc LIKE '%Statutory Debt Limit%'
                 AND closing_bal_mil_amt IS NOT NULL
                 AND closing_bal_mil_amt NOT IN ('','null','NULL')
            THEN CAST(closing_bal_mil_amt AS SIGNED) END) AS statutory_limit,

        /* Total Public Debt Outstanding */
        MAX(CASE
            WHEN table_nbr IN ('III-C','IIIC')
                 AND group_desc LIKE '%Total Public Debt Outstanding%'
                 AND closing_bal_mil_amt IS NOT NULL
                 AND closing_bal_mil_amt NOT IN ('','null','NULL')
            THEN CAST(closing_bal_mil_amt AS SIGNED) END) AS total_debt

    FROM vlad_tr_daily_treasury_statement_all
    WHERE record_date IS NOT NULL
    GROUP BY record_date
    HAVING tga_closing IS NOT NULL
    ORDER BY record_date
"""

_TAX_MONTHS = {1, 3, 4, 6, 9, 12}


# ══════════════════════════════════════════════════════════════════════════════
# КЛАССИФИКАТОРЫ — используются и в standalone, и в async
# ══════════════════════════════════════════════════════════════════════════════

def _debt_regime(headroom) -> str:
    """suspended / stress / normal — та же логика что в DATASET_QUERY JOIN."""
    if headroom is None:
        return "suspended"
    if headroom < 200_000:
        return "stress"
    return "normal"


def _tga_level(tga_closing) -> str:
    """critical / low / adequate / elevated — та же логика что в DATASET_QUERY JOIN."""
    if tga_closing is None:
        return "adequate"
    if tga_closing < 100_000:
        return "critical"
    if tga_closing < 300_000:
        return "low"
    if tga_closing < 600_000:
        return "adequate"
    return "elevated"


# ══════════════════════════════════════════════════════════════════════════════
# АГРЕГАЦИЯ СТРОК В ГРУППЫ (общая логика для обоих режимов)
# ══════════════════════════════════════════════════════════════════════════════

def _aggregate_rows(raw_rows) -> dict:
    """
    Принимает строки из _AGGREGATE_SQL, возвращает словарь групп:
    {(debt_regime, tga_level_class): {cnt, sum_*, ...}}
    """
    from collections import defaultdict

    groups: dict = defaultdict(lambda: {
        "cnt": 0, "sum_tga": 0.0, "sum_delta": 0.0,
        "sum_ni": 0.0, "sum_ftd": 0.0,
        "sum_headroom": 0.0, "headroom_cnt": 0,
        "tax_cnt": 0, "first_dt": None, "last_dt": None,
    })
    prev_tga: float | None = None

    for row in raw_rows:
        rec_date, cal_month, tga_cl, net_iss, ftd, tax_ref, stat_lim, tot_debt = (
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])

        if tga_cl is None:
            prev_tga = None
            continue

        tga_val = float(tga_cl)
        delta   = (tga_val - prev_tga) if prev_tga is not None else 0.0
        prev_tga = tga_val

        headroom = (float(stat_lim) - float(tot_debt)
                    if stat_lim is not None and tot_debt is not None
                    else None)

        dr  = _debt_regime(headroom)
        tlc = _tga_level(tga_val)
        key = (dr, tlc)

        g = groups[key]
        g["cnt"]         += 1
        g["sum_tga"]     += tga_val
        g["sum_delta"]   += delta
        g["sum_ni"]      += float(net_iss or 0)
        g["sum_ftd"]     += float(ftd or 0)
        if headroom is not None:
            g["sum_headroom"] += headroom
            g["headroom_cnt"] += 1
        cal_m = int(cal_month) if cal_month else 0
        if cal_m in _TAX_MONTHS:
            g["tax_cnt"] += 1
        if g["first_dt"] is None or rec_date < g["first_dt"]:
            g["first_dt"] = rec_date
        if g["last_dt"]  is None or rec_date > g["last_dt"]:
            g["last_dt"]  = rec_date

    return dict(groups)


def _group_to_params(debt_regime: str, tga_cls: str, g: dict) -> dict:
    cnt   = g["cnt"]
    h_cnt = g["headroom_cnt"]
    return {
        "dr":        debt_regime,
        "tc":        tga_cls,
        "cnt":       cnt,
        "avg_tga":   round(g["sum_tga"]      / cnt, 2),
        "avg_delta": round(g["sum_delta"]     / cnt, 2),
        "avg_ni":    round(g["sum_ni"]        / cnt, 2),
        "avg_ftd":   round(g["sum_ftd"]       / cnt, 2),
        "avg_h":     round(g["sum_headroom"] / h_cnt, 2) if h_cnt else None,
        "tmr":       round(g["tax_cnt"]       / cnt, 4),
        "fd":        g["first_dt"],
        "ld":        g["last_dt"],
    }


_UPSERT_SQL = f"""
    INSERT INTO `{CTX_TABLE}`
        (debt_regime, tga_level_class, occurrence_count,
         avg_tga_closing, avg_daily_change,
         avg_net_issuance, avg_ftd_withheld,
         avg_headroom, tax_month_ratio,
         first_dt, last_dt)
    VALUES (:dr, :tc, :cnt,
            :avg_tga, :avg_delta,
            :avg_ni, :avg_ftd,
            :avg_h, :tmr,
            :fd, :ld)
    ON DUPLICATE KEY UPDATE
        occurrence_count  = VALUES(occurrence_count),
        avg_tga_closing   = VALUES(avg_tga_closing),
        avg_daily_change  = VALUES(avg_daily_change),
        avg_net_issuance  = VALUES(avg_net_issuance),
        avg_ftd_withheld  = VALUES(avg_ftd_withheld),
        avg_headroom      = VALUES(avg_headroom),
        tax_month_ratio   = VALUES(tax_month_ratio),
        last_dt           = GREATEST(last_dt, VALUES(last_dt))
"""


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 1: вызов фреймворком (async, UPSERT)
# ══════════════════════════════════════════════════════════════════════════════

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    1. Пересчитывает агрегаты vlad_tr_context_idx из brain (UPSERT).
    2. Перезаписывает vlad_tr_dts_dataset — одна строка на день с ctx_id.

    engine_brain: читает из vlad_tr_daily_treasury_statement_all (brain DB)
    engine_vlad:  пишет обе таблицы (vlad DB)
    """
    # Создаём таблицы если нет
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL))
        await conn.execute(text(_DDL_DATASET))

    # Читаем агрегаты из brain
    async with engine_brain.connect() as conn:
        res  = await conn.execute(text(_AGGREGATE_SQL))
        rows = res.fetchall()

    if not rows:
        return {"contexts_total": 0, "warning": "нет данных — проверь Table I / group_desc"}

    groups = _aggregate_rows(rows)

    # ── Шаг 1: UPSERT в vlad_tr_context_idx ──────────────────────────────────
    upserted = 0
    async with engine_vlad.begin() as conn:
        for (dr, tlc), g in groups.items():
            await conn.execute(text(_UPSERT_SQL), _group_to_params(dr, tlc, g))
            upserted += 1

    # Читаем актуальные ctx_id из vlad (после upsert id стабильны)
    ctx_map: dict[tuple, dict] = {}   # (debt_regime, tga_level_class) → ctx row
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(f"SELECT * FROM `{CTX_TABLE}`"))
        for r in res.mappings().all():
            ctx_map[(r["debt_regime"], r["tga_level_class"])] = dict(r)

    # ── Шаг 2: перезаписываем vlad_tr_dts_dataset ────────────────────────────
    # Строим строки: одна на день, ctx_id проставлен через классификацию
    dataset_rows = []
    prev_tga: float | None = None

    for row in rows:
        rec_date, cal_month, tga_cl, net_iss, ftd, tax_ref, stat_lim, tot_debt = (
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])

        if tga_cl is None:
            prev_tga = None
            continue

        tga_val  = float(tga_cl)
        headroom = (float(stat_lim) - float(tot_debt)
                    if stat_lim is not None and tot_debt is not None
                    else None)

        dr  = _debt_regime(headroom)
        tlc = _tga_level(tga_val)
        ctx = ctx_map.get((dr, tlc))
        if ctx is None:
            prev_tga = tga_val
            continue

        dataset_rows.append({
            "date":             rec_date,
            "ctx_id":           ctx["id"],
            "tga_closing":      tga_val,
            "net_issuance":     float(net_iss or 0),
            "ftd_withheld":     float(ftd     or 0),
            "tax_refunds_eft":  float(tax_ref or 0) if tax_ref is not None else None,
            "headroom":         headroom,
            "calendar_month":   int(cal_month) if cal_month else None,
            "occurrence_count": ctx["occurrence_count"],
            "avg_tga_closing":  ctx["avg_tga_closing"],
            "avg_daily_change": ctx["avg_daily_change"],
            "avg_net_issuance": ctx["avg_net_issuance"],
            "avg_ftd_withheld": ctx["avg_ftd_withheld"],
            "avg_headroom":     ctx["avg_headroom"],
            "tax_month_ratio":  ctx["tax_month_ratio"],
            "debt_regime":      dr,
            "tga_level_class":  tlc,
        })
        prev_tga = tga_val

    # TRUNCATE + batch INSERT (датасет полностью пересчитывается)
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{DATASET_TABLE}`"))
        for i in range(0, len(dataset_rows), 500):
            batch = dataset_rows[i:i + 500]
            await conn.execute(text(f"""
                INSERT INTO `{DATASET_TABLE}`
                    (date, ctx_id, tga_closing, net_issuance, ftd_withheld,
                     tax_refunds_eft, headroom, calendar_month,
                     occurrence_count, avg_tga_closing, avg_daily_change,
                     avg_net_issuance, avg_ftd_withheld, avg_headroom,
                     tax_month_ratio, debt_regime, tga_level_class)
                VALUES
                    (:date, :ctx_id, :tga_closing, :net_issuance, :ftd_withheld,
                     :tax_refunds_eft, :headroom, :calendar_month,
                     :occurrence_count, :avg_tga_closing, :avg_daily_change,
                     :avg_net_issuance, :avg_ftd_withheld, :avg_headroom,
                     :tax_month_ratio, :debt_regime, :tga_level_class)
            """), batch)

    async with engine_vlad.connect() as conn:
        total_ctx = (await conn.execute(
            text(f"SELECT COUNT(*) FROM `{CTX_TABLE}`")
        )).scalar()
        total_ds = (await conn.execute(
            text(f"SELECT COUNT(*) FROM `{DATASET_TABLE}`")
        )).scalar()

    return {
        "raw_days_parsed":  len(rows),
        "upserted":         upserted,
        "total_contexts":   int(total_ctx or 0),
        "dataset_rows":     int(total_ds  or 0),
        "dimensions":       "debt_regime × tga_level_class",
    }


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка, TRUNCATE + INSERT)
# ══════════════════════════════════════════════════════════════════════════════

def main():
    required_vars = {
        'MASTER_HOST': 'MASTER_HOST',
        'MASTER_PORT': 'MASTER_PORT',
        'MASTER_USER': 'MASTER_USER',
        'MASTER_PASSWORD': 'MASTER_PASSWORD',
        'DB_HOST': 'DB_HOST',
        'DB_PORT': 'DB_PORT',
        'DB_USER': 'DB_USER',
        'DB_PASSWORD': 'DB_PASSWORD',
    }

    missing = []
    for var_name, env_key in required_vars.items():
        value = os.getenv(env_key)
        if not value:
            missing.append(env_key)
        else:
            print(f"✓ {env_key}: {value}")

    if missing:
        print(f"\n❌ Ошибка: Не найдены переменные окружения: {', '.join(missing)}")
        sys.exit(1)

    print("\nПодключение к базам данных...")

    try:
        brain = mysql.connector.connect(
            host=os.getenv("MASTER_HOST"),
            port=int(os.getenv("MASTER_PORT", 3306)),
            user=os.getenv("MASTER_USER"),
            password=os.getenv("MASTER_PASSWORD"),
            database="brain",
            use_pure=True,
            allow_local_infile=False,
            autocommit=False,
            connection_timeout=10,
        )
        print("✓ Подключено к brain")
    except Exception as e:
        print(f"❌ Ошибка подключения к brain: {e}")
        sys.exit(1)

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
        print("✓ Подключено к vlad\n")
    except Exception as e:
        print(f"❌ Ошибка подключения к vlad: {e}")
        brain.close()
        sys.exit(1)

    try:
        bc = brain.cursor()
        vc = vlad.cursor()

        # ── Создаём таблицы ───────────────────────────────────────────────────
        print(f"Создание таблицы `{CTX_TABLE}` в vlad...")
        vc.execute(_DDL)
        vlad.commit()
        print("  ✓ Таблица создана")

        print(f"Создание таблицы `{DATASET_TABLE}` в vlad...")
        vc.execute(_DDL_DATASET)
        vlad.commit()
        print("  ✓ Таблица создана")

        # ── Агрегация из brain ────────────────────────────────────────────────
        print("Агрегация из brain.vlad_tr_daily_treasury_statement_all...")
        bc.execute(_AGGREGATE_SQL)
        rows = bc.fetchall()
        print(f"  ✓ Дней обработано: {len(rows)}")

        if not rows:
            print("  ⚠ Нет данных — проверь подключение к brain и фильтры SQL")
            return

        groups = _aggregate_rows(rows)
        print(f"  ✓ Типов найдено: {len(groups)}")

        # ── TRUNCATE + INSERT в vlad_tr_context_idx ───────────────────────────
        print("Пересборка vlad_tr_context_idx (TRUNCATE + INSERT)...")
        vc.execute(f"TRUNCATE TABLE `{CTX_TABLE}`")
        vlad.commit()

        inserted_ctx = 0
        for (dr, tlc), g in groups.items():
            p = _group_to_params(dr, tlc, g)
            vc.execute(f"""
                INSERT INTO `{CTX_TABLE}`
                    (debt_regime, tga_level_class, occurrence_count,
                     avg_tga_closing, avg_daily_change,
                     avg_net_issuance, avg_ftd_withheld,
                     avg_headroom, tax_month_ratio,
                     first_dt, last_dt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                p["dr"], p["tc"], p["cnt"],
                p["avg_tga"], p["avg_delta"],
                p["avg_ni"], p["avg_ftd"],
                p["avg_h"], p["tmr"],
                p["fd"], p["ld"],
            ))
            inserted_ctx += 1
        vlad.commit()
        print(f"  ✓ Записано типов: {inserted_ctx}")

        # Читаем ctx_id после вставки
        vc.execute(f"SELECT id, debt_regime, tga_level_class, "
                   f"occurrence_count, avg_tga_closing, avg_daily_change, "
                   f"avg_net_issuance, avg_ftd_withheld, avg_headroom, "
                   f"tax_month_ratio FROM `{CTX_TABLE}`")
        ctx_map: dict[tuple, dict] = {}
        for r in vc.fetchall():
            ctx_map[(r[1], r[2])] = {
                "id": r[0], "occurrence_count": r[3],
                "avg_tga_closing": r[4], "avg_daily_change": r[5],
                "avg_net_issuance": r[6], "avg_ftd_withheld": r[7],
                "avg_headroom": r[8], "tax_month_ratio": r[9],
            }

        # ── TRUNCATE + INSERT в vlad_tr_dts_dataset ───────────────────────────
        print("Пересборка vlad_tr_dts_dataset (TRUNCATE + INSERT)...")
        vc.execute(f"TRUNCATE TABLE `{DATASET_TABLE}`")
        vlad.commit()

        inserted_ds = 0
        prev_tga: float | None = None

        for row in rows:
            rec_date, cal_month, tga_cl, net_iss, ftd, tax_ref, stat_lim, tot_debt = (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])

            if tga_cl is None:
                prev_tga = None
                continue

            tga_val  = float(tga_cl)
            headroom = (float(stat_lim) - float(tot_debt)
                        if stat_lim is not None and tot_debt is not None
                        else None)

            dr  = _debt_regime(headroom)
            tlc = _tga_level(tga_val)
            ctx = ctx_map.get((dr, tlc))
            if ctx is None:
                prev_tga = tga_val
                continue

            vc.execute(f"""
                INSERT INTO `{DATASET_TABLE}`
                    (date, ctx_id, tga_closing, net_issuance, ftd_withheld,
                     tax_refunds_eft, headroom, calendar_month,
                     occurrence_count, avg_tga_closing, avg_daily_change,
                     avg_net_issuance, avg_ftd_withheld, avg_headroom,
                     tax_month_ratio, debt_regime, tga_level_class)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                rec_date,
                ctx["id"],
                tga_val,
                float(net_iss or 0),
                float(ftd     or 0),
                float(tax_ref or 0) if tax_ref is not None else None,
                headroom,
                int(cal_month) if cal_month else None,
                ctx["occurrence_count"],
                ctx["avg_tga_closing"],
                ctx["avg_daily_change"],
                ctx["avg_net_issuance"],
                ctx["avg_ftd_withheld"],
                ctx["avg_headroom"],
                ctx["tax_month_ratio"],
                dr,
                tlc,
            ))
            inserted_ds += 1
            prev_tga = tga_val

            if inserted_ds % 500 == 0:
                vlad.commit()

        vlad.commit()
        print(f"  ✓ Записано строк датасета: {inserted_ds}")

        # ── Итоговая таблица ctx_index ────────────────────────────────────────
        vc.execute(f"""
            SELECT id, debt_regime, tga_level_class,
                   occurrence_count,
                   ROUND(avg_tga_closing / 1000, 0) AS avg_tga_b,
                   ROUND(avg_daily_change / 1000, 1) AS avg_delta_b,
                   ROUND(avg_ftd_withheld / 1000, 1) AS avg_ftd_b,
                   ROUND(COALESCE(avg_headroom, 0) / 1000, 0) AS avg_h_b,
                   ROUND(tax_month_ratio, 2) AS tmr,
                   first_dt, last_dt
            FROM `{CTX_TABLE}`
            ORDER BY debt_regime, tga_level_class
        """)
        print(f"\n{'id':>3} {'regime':<12} {'level':<10} {'cnt':>6} "
              f"{'tga_B$':>8} {'dΔ_B$':>7} {'ftd_B$':>7} "
              f"{'h_B$':>8} {'tax_r':>6} {'first':>12} {'last':>12}")
        print("─" * 95)
        for r in vc.fetchall():
            h_str = f"{r[7]:>8.0f}" if r[7] else "    N/A "
            print(f"{r[0]:>3} {r[1]:<12} {r[2]:<10} {r[3]:>6} "
                  f"{r[4] or 0:>8.0f} {r[5] or 0:>7.1f} {r[6] or 0:>7.1f} "
                  f"{h_str} {r[8] or 0:>6.2f} "
                  f"{str(r[9] or ''):>12} {str(r[10] or ''):>12}")

        print(f"\n✅ Готово. ctx={inserted_ctx} dataset={inserted_ds}")

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        vlad.rollback()
        raise
    finally:
        brain.close()
        vlad.close()


if __name__ == "__main__":
    main()