"""
model.py — brain-atomic-extremum (сервис 50)

Многодатасетный индексатор экстремумов на основе lift-анализа.

Алгоритм model():
  1. Определяем пару (BTC / ETH / EUR) по last_close котировок.
  2. Из dataset (vlad_atomic_feature_matrix) берём строку за нужный день.
  3. Для каждой _bkt-колонки читаем bucket_val (0..4).
  4. Строим weight_code = "{feature_col}_{bucket_val}".
  5. Ищем bull_ratio в ctx_index (vlad_atomic_svc52_index).

Возвращает {weight_code: bull_ratio}:
  bull_ratio > 0.5 → исторически этот сигнал предшествовал росту;
  bull_ratio < 0.5 → исторически предшествовал падению.

/weights — возвращает фиксированный список всех weight_code из индекса.
/values  — возвращает bull_ratio для активных кодов на текущую дату.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, date as date_type
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger("brain-framework")

# ── Константы, импортируемые context_idx.py ──────────────────────────────────
SERVICE_ID   = 52                       # brain_models.id — ПОДСТАВИТЬ перед деплоем
RATES_TABLE  = "brain_rates_btc_usd"   # базовая таблица котировок (BTC)

# Ключи: порог для включения weight_code в результат.
# Коды с bull_ratio близким к 0.5 неинформативны — их пропускаем.
MIN_SIGNAL_DIFF = 0.03    # |bull_ratio - 0.5| >= 0.03


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _detect_pair(rates: list[dict]) -> str:
    """
    Определяем торговую пару по last_close.
    BTC ~10K–200K, ETH ~100–10K, EUR ~0.8–1.3.
    """
    if not rates:
        return "btc"
    last_close = float(rates[-1].get("close") or rates[-1].get("t1") or 0)
    if last_close > 10_000:
        return "btc"
    if last_close > 100:
        return "eth"
    return "eur"


def _to_date(val) -> Optional[date_type]:
    """Конвертирует любой формат даты в date."""
    if val is None:
        return None
    if isinstance(val, date_type):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _find_feature_row(dataset: list[dict], target: date_type) -> Optional[dict]:
    """
    Ищет строку feature_matrix для target или ближайшую до неё.
    dataset пришёл из config.toml query (только date + ext флаги).
    Полные _bkt данные читаются отдельно через _load_feature_row_from_db().
    """
    best_row  = None
    best_date = None
    for row in dataset:
        d = _to_date(row.get("date") or row.get("record_date"))
        if d is None:
            continue
        if d == target:
            return row
        if d < target:
            if best_date is None or d > best_date:
                best_date = d
                best_row  = row
    return best_row


def _load_feature_row_from_db(target: date_type) -> Optional[dict]:
    """
    Читает одну строку vlad_atomic_feature_matrix из БД напрямую.
    Используется в model() вместо dataset который содержит только
    date + ext колонки (для совместимости с pretest фреймворка).
    """
    import mysql.connector
    try:
        cfg  = {
            "host":     os.getenv("DB_HOST",     "127.0.0.1"),
            "port":     int(os.getenv("DB_PORT", "3306")),
            "user":     os.getenv("DB_USER",     "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME",     "vlad"),
        }
        conn = mysql.connector.connect(**cfg)
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM vlad_atomic_feature_matrix "
            "WHERE record_date <= %s ORDER BY record_date DESC LIMIT 1",
            (target,)
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        return row
    except Exception as e:
        log.warning(f"[atomic] _load_feature_row_from_db error: {e}")
        return None


def _load_index_from_db(pair: str, min_lift: float = 0.0) -> dict[str, float]:
    """
    Читает vlad_atomic_svc52_index из БД напрямую.
    Надёжнее чем ctx_index — гарантирует актуальные данные после rebuild_index.
    """
    col_br = f"{pair}_bull_ratio"
    col_mx = f"{pair}_max_lift"
    col_mn = f"{pair}_min_lift"
    idx_table = f"vlad_atomic_svc{SERVICE_ID}_index"

    import mysql.connector
    try:
        cfg = {
            "host":     os.getenv("DB_HOST",     "127.0.0.1"),
            "port":     int(os.getenv("DB_PORT", "3306")),
            "user":     os.getenv("DB_USER",     "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME",     "vlad"),
        }
        conn = mysql.connector.connect(**cfg)
        cur  = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT weight_code, `{col_br}`, `{col_mx}`, `{col_mn}` "
            f"FROM `{idx_table}`"
        )
        rows = cur.fetchall()
        cur.close(); conn.close()

        lookup = {}
        for r in rows:
            wc = r.get("weight_code")
            br = r.get(col_br)
            mx = float(r.get(col_mx) or 1.0)
            mn = float(r.get(col_mn) or 1.0)
            if wc and br is not None and max(mx, mn) >= min_lift:
                lookup[wc] = float(br)
        log.debug(f"[atomic] loaded {len(lookup)} index entries for pair={pair} min_lift={min_lift}")
        return lookup

    except Exception as e:
        log.warning(f"[atomic] _load_index_from_db error: {e}")
        return {}


def _build_lift_lookup(ctx_index: dict, pair: str, min_lift: float = 0.0) -> dict[str, float]:
    """
    Строим {weight_code: bull_ratio}.
    Сначала пробуем прочитать из БД напрямую (надёжнее),
    при ошибке — fallback на ctx_index от фреймворка.
    """
    # Приоритет: прямой запрос к БД
    lookup = _load_index_from_db(pair, min_lift)
    if lookup:
        return lookup

    # Fallback: ctx_index переданный фреймворком
    col_br = f"{pair}_bull_ratio"
    col_mx = f"{pair}_max_lift"
    col_mn = f"{pair}_min_lift"
    for _, info in ctx_index.items():
        wc = info.get("weight_code")
        br = info.get(col_br)
        if not wc or br is None:
            continue
        mx = float(info.get(col_mx) or 1.0)
        mn = float(info.get(col_mn) or 1.0)
        if max(mx, mn) >= min_lift:
            lookup[wc] = float(br)
    return lookup


# ══════════════════════════════════════════════════════════════════════════════
# model() — точка входа фреймворка
# ══════════════════════════════════════════════════════════════════════════════

def model(
    rates:         list[dict],
    dataset:       list[dict],   # vlad_atomic_feature_matrix (строки за ~2 года)
    date:          datetime,
    *,
    type:          int  = 0,
    var:           int  = 0,
    param:         str  = "",
    dataset_index: dict | None = None,
    **kw,
) -> dict[str, float]:
    """
    Вычисляет lift-based сигналы для всех активных (feature, bucket) на дату `date`.

    Возвращает {weight_code: bull_ratio}:
      weight_code = "{feature_col}_{bucket_val}"  напр. "ds0_value_bkt_4"
      bull_ratio  = вероятность роста при данном состоянии сигнала (0.0–1.0)

    Пустой dict → данных на эту дату нет (нейросеть использует нейтральные веса).
    """
    if not dataset:
        return {}

    ctx_index = (dataset_index or {}).get("ctx_index") or {}

    # ── 1. Определяем пару по type ────────────────────────────────────────────
    # type=0 → авто (по last_close котировок)
    # type=1 → BTC   type=2 → ETH   type=3 → EUR
    pair_map = {1: "btc", 2: "eth", 3: "eur"}
    if type in pair_map:
        pair = pair_map[type]
    else:
        pair = _detect_pair(rates)

    # ── 2. Определяем минимальный lift по var ─────────────────────────────────
    # var=0 → lift > 1.15 (все значимые сигналы, максимальный охват)
    # var=1 → lift > 1.5  (только сильные сигналы)
    # var=2 → lift > 2.0  (только очень значимые)
    lift_map  = {0: 1.15, 1: 1.5, 2: 2.0}
    min_lift  = lift_map.get(var, 1.15)

    # ── 2. Строим lookup из индекса ───────────────────────────────────────────
    lift_lookup = _build_lift_lookup(ctx_index, pair, min_lift) if ctx_index else {}

    # ── 3. Находим строку feature_matrix ─────────────────────────────────────
    target_date = date.date() if isinstance(date, datetime) else date

    # dataset из config.toml содержит только date + ext (для pretest фреймворка).
    # Полные _bkt данные читаем из БД напрямую.
    feature_row = _load_feature_row_from_db(target_date)

    if feature_row is None:
        log.debug(f"[atomic] no feature_row for {target_date}")
        return {}

    # ── 4. Собираем активные bucket-значения ──────────────────────────────────
    result: dict[str, float] = {}

    for col, val in feature_row.items():
        # Берём только _bkt-колонки (дискретизированные сигналы)
        if not col.endswith("_bkt"):
            continue
        if val is None:
            continue

        try:
            bkt = int(val)
        except (TypeError, ValueError):
            continue

        weight_code = f"{col}_{bkt}"

        # ── 5. Ищем bull_ratio: из индекса или нейтральный 0.5 ───────────────
        bull_ratio = lift_lookup.get(weight_code, 0.5)

        # Пропускаем нейтральные сигналы (|br - 0.5| < порога)
        if abs(bull_ratio - 0.5) < MIN_SIGNAL_DIFF and weight_code not in lift_lookup:
            continue

        result[weight_code] = round(bull_ratio, 6)

    log.debug(f"[atomic] {target_date} {pair}: {len(result)} signals")
    return result