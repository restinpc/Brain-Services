"""
context_idx.py — build_index для brain-atomic-extremum

Что делает build_index():
  1. Запускает atomic_extremum_model.py через subprocess:
       - пересчитывает vlad_atomic_feature_matrix из 22 датасетов;
       - пересчитывает vlad_atomic_cooccurrence (lift каждого сигнала);
       - пересчитывает vlad_atomic_combinations (фингерпринты).
  2. Агрегирует vlad_atomic_cooccurrence → vlad_atomic_svc52_index:
       - для каждой пары (feature_col, bucket_val) собирает lift по всем парам;
       - вычисляет bull_ratio = max_lift / (max_lift + min_lift);
       - записывает в индексную таблицу, которую фреймворк кладёт в ctx_index.

Вызывается фреймворком через /rebuild_index.
Частота: rebuild_interval = 86400 (раз в сутки).

Почему ежесуточно (а не ежечасно как у service 51):
  Атомарная модель работает на дневных данных (FRED, Alpha Vantage, ECB и др.).
  Пересчёт чаще суток не меняет индекс — источники обновляются раз в день.
  «Цифровой отпечаток» устраняется TRUNCATE + полным пересчётом индекса,
  а не временнóй ротацией (как в hourly-сервисах).
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime

from sqlalchemy import text

log = logging.getLogger("brain-framework")

# ── Импортируем константы из model.py ─────────────────────────────────────────
import importlib.util as _ilu

def _import_model():
    here = os.path.dirname(os.path.abspath(__file__))
    spec = _ilu.spec_from_file_location("atomic_model", os.path.join(here, "model.py"))
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_mdl = _import_model()
SERVICE_ID   = _mdl.SERVICE_ID
RATES_TABLE  = _mdl.RATES_TABLE

INDEX_TABLE  = f"vlad_atomic_svc{SERVICE_ID}_index"
SOURCE_COOC  = "vlad_atomic_cooccurrence"
SOURCE_FM    = "vlad_atomic_feature_matrix"

# Минимальный lift для включения в индекс
MIN_LIFT = 1.15
BATCH_SIZE = 500


# ── DDL ──────────────────────────────────────────────────────────────────────

async def _ensure_index_table(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{INDEX_TABLE}` (
                `id`               INT          NOT NULL AUTO_INCREMENT,
                `weight_code`      VARCHAR(80)  NOT NULL,
                `feature_col`      VARCHAR(80)  NOT NULL,
                `bucket_val`       TINYINT      NOT NULL,
                `btc_max_lift`     FLOAT        NOT NULL DEFAULT 1.0,
                `btc_min_lift`     FLOAT        NOT NULL DEFAULT 1.0,
                `eth_max_lift`     FLOAT        NOT NULL DEFAULT 1.0,
                `eth_min_lift`     FLOAT        NOT NULL DEFAULT 1.0,
                `eur_max_lift`     FLOAT        NOT NULL DEFAULT 1.0,
                `eur_min_lift`     FLOAT        NOT NULL DEFAULT 1.0,
                `btc_bull_ratio`   FLOAT        NOT NULL DEFAULT 0.5,
                `eth_bull_ratio`   FLOAT        NOT NULL DEFAULT 0.5,
                `eur_bull_ratio`   FLOAT        NOT NULL DEFAULT 0.5,
                `support`          FLOAT        NOT NULL DEFAULT 0.0,
                `occurrence_count` INT          NOT NULL DEFAULT 0,
                `date_updated`     DATETIME     NULL,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_wc`      (`weight_code`),
                INDEX `idx_btc_bull`   (`btc_bull_ratio`),
                INDEX `idx_feature`    (`feature_col`(50))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


# ── Запуск atomic_extremum_model.py ───────────────────────────────────────────

def _run_atomic_model(engine_vlad, engine_brain, min_date: str = "2020-01-01") -> bool:
    """
    Запускаем atomic_extremum_model.py через subprocess.
    Параметры подключения берутся из .env — аргументы не передаём.
    """
    here   = os.path.dirname(os.path.abspath(__file__))
    script = os.path.join(here, "atomic_extremum_model.py")

    if not os.path.exists(script):
        log.error(f"[{INDEX_TABLE}] atomic_extremum_model.py не найден: {script}")
        return False

    cmd = [
        sys.executable, script,
        "--min-date", min_date,
        "--window",   "7",
        "--lookback", "7",   # узкое окно → высокий lift
    ]

    log.info(f"[{INDEX_TABLE}] запуск atomic_extremum_model.py (min_date={min_date})...")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            log.error(f"[{INDEX_TABLE}] atomic_extremum_model.py вернул {result.returncode}")
            log.error(f"stderr: {result.stderr[-2000:]}")
            return False

        for line in result.stdout.strip().split("\n")[-10:]:
            log.info(f"[atomic_model] {line}")
        return True

    except subprocess.TimeoutExpired:
        log.error(f"[{INDEX_TABLE}] таймаут 3600с")
        return False
    except Exception as e:
        log.error(f"[{INDEX_TABLE}] ошибка subprocess: {e}")
        return False


# ── Агрегация cooccurrence → index ───────────────────────────────────────────

async def _populate_index(engine_vlad) -> int:
    """
    Читает vlad_atomic_cooccurrence, агрегирует по (feature_col, bucket_val),
    вычисляет bull_ratio для каждой пары (btc/eth/eur), пишет в INDEX_TABLE.

    Логика bull_ratio:
      bull_ratio = max_lift / (max_lift + min_lift)
      lift_max >> lift_min → bull_ratio > 0.5 (бычий сигнал)
      lift_min >> lift_max → bull_ratio < 0.5 (медвежий сигнал)
    """
    # Загружаем всю таблицу cooccurrence
    async with engine_vlad.connect() as conn:
        rows = (await conn.execute(text(f"""
            SELECT feature_col, bucket_val, extremum_type, lift, support, active_count
            FROM `{SOURCE_COOC}`
            WHERE lift > {MIN_LIFT}
        """))).mappings().all()

    if not rows:
        log.warning(f"[{INDEX_TABLE}] {SOURCE_COOC} пуст или lift < {MIN_LIFT}")
        return 0

    # Агрегируем по (feature_col, bucket_val)
    # Структура: agg[(fc, bkt)] = {btc_max: X, btc_min: Y, ...}
    agg: dict[tuple, dict] = {}
    for r in rows:
        fc  = r["feature_col"]
        bkt = int(r["bucket_val"])
        ext = r["extremum_type"].lower()      # "btc_ext_max", "btc_ext_min", etc.
        key = (fc, bkt)

        if key not in agg:
            agg[key] = {
                "btc_max": 1.0, "btc_min": 1.0,
                "eth_max": 1.0, "eth_min": 1.0,
                "eur_max": 1.0, "eur_min": 1.0,
                "support": 0.0, "cnt": 0,
            }

        lift    = float(r["lift"])
        support = float(r["support"] or 0)

        for pair in ("btc", "eth", "eur"):
            if f"{pair}_ext_max" in ext:
                agg[key][f"{pair}_max"] = max(agg[key][f"{pair}_max"], lift)
                agg[key]["support"]     = max(agg[key]["support"], support)
                agg[key]["cnt"]         = max(agg[key]["cnt"], int(r["active_count"] or 0))
            elif f"{pair}_ext_min" in ext:
                agg[key][f"{pair}_min"] = max(agg[key][f"{pair}_min"], lift)

    # Строим строки для вставки
    def _bull_ratio(mx: float, mn: float) -> float:
        total = mx + mn + 1e-9
        return round(mx / total, 6)

    insert_rows = []
    for (fc, bkt), v in agg.items():
        wc = f"{fc}_{bkt}"[:80]

        # Включаем только сигналы с хотя бы одним значимым lift
        max_any_lift = max(v["btc_max"], v["btc_min"],
                           v["eth_max"], v["eth_min"],
                           v["eur_max"], v["eur_min"])
        if max_any_lift < MIN_LIFT:
            continue

        insert_rows.append({
            "wc":     wc,
            "fc":     fc[:80],
            "bkt":    bkt,
            "btc_mx": round(v["btc_max"], 6),
            "btc_mn": round(v["btc_min"], 6),
            "eth_mx": round(v["eth_max"], 6),
            "eth_mn": round(v["eth_min"], 6),
            "eur_mx": round(v["eur_max"], 6),
            "eur_mn": round(v["eur_min"], 6),
            "btc_br": _bull_ratio(v["btc_max"], v["btc_min"]),
            "eth_br": _bull_ratio(v["eth_max"], v["eth_min"]),
            "eur_br": _bull_ratio(v["eur_max"], v["eur_min"]),
            "sup":    round(v["support"], 6),
            "cnt":    v["cnt"],
        })

    if not insert_rows:
        log.warning(f"[{INDEX_TABLE}] нет строк для записи после фильтрации lift < {MIN_LIFT}")
        return 0

    # TRUNCATE + batch INSERT
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{INDEX_TABLE}`"))

    total = 0
    for i in range(0, len(insert_rows), BATCH_SIZE):
        batch = insert_rows[i : i + BATCH_SIZE]
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO `{INDEX_TABLE}`
                    (weight_code, feature_col, bucket_val,
                     btc_max_lift, btc_min_lift, eth_max_lift, eth_min_lift,
                     eur_max_lift, eur_min_lift,
                     btc_bull_ratio, eth_bull_ratio, eur_bull_ratio,
                     support, occurrence_count, date_updated)
                VALUES
                    (:wc, :fc, :bkt,
                     :btc_mx, :btc_mn, :eth_mx, :eth_mn, :eur_mx, :eur_mn,
                     :btc_br, :eth_br, :eur_br,
                     :sup, :cnt, NOW())
            """), batch)
        total += len(batch)

    return total


async def _stats(engine_vlad) -> dict:
    """Статистика — не падает если таблицы ещё не созданы."""
    stats = {"feature_matrix_rows": 0, "cooccurrence_rows": 0, "index_rows": 0}
    for key, table in [
        ("feature_matrix_rows", SOURCE_FM),
        ("cooccurrence_rows",   SOURCE_COOC),
        ("index_rows",          INDEX_TABLE),
    ]:
        try:
            async with engine_vlad.connect() as conn:
                stats[key] = (await conn.execute(
                    text(f"SELECT COUNT(*) FROM `{table}`")
                )).scalar() or 0
        except Exception:
            stats[key] = 0
    return stats


# ── Гарантируем существование таблицы при старте сервиса ─────────────────────

async def on_startup(engine_vlad, engine_brain) -> None:
    """
    Вызывается фреймворком при старте сервиса (до первого reload).
    Создаёт пустую индексную таблицу если она не существует,
    чтобы запросы /weights и /ctx_index не падали с ошибкой 1146.
    """
    await _ensure_index_table(engine_vlad)


# ── Точка входа ───────────────────────────────────────────────────────────────

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    Точка входа, вызываемая фреймворком через /rebuild_index.

    Алгоритм:
      1. CREATE TABLE IF NOT EXISTS (индексная таблица)
      2. subprocess: python atomic_extremum_model.py → rebuild all vlad_atomic_* tables
      3. Агрегация vlad_atomic_cooccurrence → vlad_atomic_svc52_index
      4. Возврат статистики

    Время выполнения: ~3-10 мин (зависит от размера истории и числа датасетов).
    """
    log.info(f"[{INDEX_TABLE}] build_index: start")

    await _ensure_index_table(engine_vlad)

    # ── 1. Запускаем atomic_extremum_model.py ─────────────────────────────────
    success = _run_atomic_model(engine_vlad, engine_brain)
    if not success:
        return {"error": "atomic_extremum_model.py завершился с ошибкой"}

    # ── 2. Строим индексную таблицу ───────────────────────────────────────────
    log.info(f"[{INDEX_TABLE}] агрегируем cooccurrence → index...")
    n_written = await _populate_index(engine_vlad)
    log.info(f"[{INDEX_TABLE}] записано {n_written} строк в {INDEX_TABLE}")

    # ── 3. Статистика ──────────────────────────────────────────────────────────
    stats = await _stats(engine_vlad)
    stats.update({
        "index_table":        INDEX_TABLE,
        "atomic_model_ok":    success,
        "index_entries":      n_written,
    })

    log.info(f"[{INDEX_TABLE}] build_index: done — {stats}")
    return stats