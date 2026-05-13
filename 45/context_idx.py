"""
extremum_ctx.py — framework builder for brain_extremum_ctx / brain_extremum_events.

Аналог context_idx.py, но вместо календарных событий — ZigZag-экстремумы
на котировках (brain_rates_*).

Фреймворк вызывает:
    await build_index(engine_vlad, engine_brain)

Что строим:
    brain_extremum_events — все ZigZag-экстремумы по каждому инструменту/порогу/типу.
        Это DATASET для фреймворка (DATASET_KEY = event_key).
    brain_extremum_ctx — паттерны «нырок→подъём» на гистограмме интервалов.
        Это CTX для фреймворка.

Контекст для model():
    dataset_index["by_key"][event_key]     → список строк extremum_events
    dataset_index["key_dates"][event_key]  → отсортированные даты экстремумов
    dataset_index["ctx_index"][(table, thr, ext_type)] → {entry_x, exit_x, ...}
"""

from __future__ import annotations

import os
from typing import Any

import numpy as np
from sqlalchemy import text


# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════

# Таблицы котировок для анализа.
# Формат: "brain_rates_btc_usd,brain_rates_eth_usd,brain_rates_eur_usd"
_RATES_TABLES_RAW = os.getenv(
    "EXTREMUM_RATES_TABLES",
    "brain_rates_btc_usd,brain_rates_eth_usd,brain_rates_eur_usd"
    ",brain_rates_btc_usd_day,brain_rates_eth_usd_day,brain_rates_eur_usd_day",
)
RATES_TABLES: list[str] = [t.strip() for t in _RATES_TABLES_RAW.split(",") if t.strip()]

# Пороги ZigZag на инструмент (через запятую, для каждой таблицы — свои).
# Формат: "table:p1,p2,p3|table2:p1,p2,p3"
# Если таблицы нет в маппинге — берём дефолт.
_THRESHOLDS_RAW = os.getenv(
    "EXTREMUM_THRESHOLDS",
    "brain_rates_btc_usd:3.0,5.0,8.0"
    "|brain_rates_eth_usd:3.0,5.0,8.0"
    "|brain_rates_eur_usd:0.3,0.5,0.8"
    "|brain_rates_btc_usd_day:3.0,5.0,8.0"
    "|brain_rates_eth_usd_day:3.0,5.0,8.0"
    "|brain_rates_eur_usd_day:0.3,0.5,0.8",
)

def _parse_thresholds(raw: str) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for part in raw.split("|"):
        part = part.strip()
        if ":" not in part:
            continue
        table, vals = part.split(":", 1)
        out[table.strip()] = [float(v) for v in vals.split(",") if v.strip()]
    return out

THRESHOLDS_MAP: dict[str, list[float]] = _parse_thresholds(_THRESHOLDS_RAW)
DEFAULT_THRESHOLDS: list[float] = [3.0, 5.0, 8.0]

# Типы экстремумов для анализа.
EXT_TYPES: list[str] = ["all", "max", "min"]

# Параметры гистограммы и WMA.
HISTOGRAM_BIN_SIZE_HOURLY: float = float(os.getenv("EXTREMUM_BIN_SIZE_HOURLY", "0.5"))
HISTOGRAM_BIN_SIZE_DAILY:  float = float(os.getenv("EXTREMUM_BIN_SIZE_DAILY",  "1.0"))
WMA_WINDOW: int = int(os.getenv("EXTREMUM_WMA_WINDOW", "3"))  # окно скользящей средней

# Паттерн «нырок→подъём» (dip then rise): фильтр на min. размах.
MIN_DIP_ABS: float = float(os.getenv("EXTREMUM_MIN_DIP_ABS", "0.0"))
ONLY_RECURRING: bool = os.getenv("EXTREMUM_ONLY_RECURRING", "1") == "1"

# Движок откуда читать котировки.
SRC_ENGINE: str = os.getenv("EXTREMUM_SRC_ENGINE", "brain").lower()

EVENTS_TABLE: str = os.getenv("EXTREMUM_EVENTS_TABLE", "brain_extremum_events")
CTX_TABLE: str = os.getenv("EXTREMUM_CTX_TABLE", "brain_extremum_ctx")


# ══════════════════════════════════════════════════════════════════════════════
# DDL
# ══════════════════════════════════════════════════════════════════════════════

DDL_EVENTS = f"""
CREATE TABLE IF NOT EXISTS `{EVENTS_TABLE}` (
  `id`            BIGINT       NOT NULL AUTO_INCREMENT,
  `event_key`     VARCHAR(64)  NOT NULL,
  `table_name`    VARCHAR(64)  NOT NULL,
  `threshold_pct` DOUBLE       NOT NULL,
  `ext_type`      VARCHAR(8)   NOT NULL,
  `date`          DATETIME     NOT NULL,
  `close_price`   DOUBLE       NULL,
  `updated_at`    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                               ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_event_date` (`event_key`, `date`),
  INDEX `idx_evt_table`  (`table_name`),
  INDEX `idx_evt_thr`    (`threshold_pct`),
  INDEX `idx_evt_type`   (`ext_type`),
  INDEX `idx_evt_date`   (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_CTX = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  `id`               BIGINT       NOT NULL AUTO_INCREMENT,
  `event_key`        VARCHAR(64)  NOT NULL,
  `table_name`       VARCHAR(64)  NOT NULL,
  `threshold_pct`    DOUBLE       NOT NULL,
  `ext_type`         VARCHAR(8)   NOT NULL,
  `frame`            VARCHAR(16)  NOT NULL DEFAULT 'hourly',
  `divisor`          INT          NOT NULL DEFAULT 24,
  `entry_x`          DOUBLE       NOT NULL,
  `exit_x`           DOUBLE       NOT NULL,
  `max_dip_x`        DOUBLE       NOT NULL,
  `max_diff_abs`     DOUBLE       NOT NULL DEFAULT 0,
  `occurrence_count` INT          NOT NULL DEFAULT 0,
  `updated_at`       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctx_pattern`
    (`table_name`, `threshold_pct`, `ext_type`, `entry_x`),
  INDEX `idx_ctx_table`   (`table_name`),
  INDEX `idx_ctx_thr`     (`threshold_pct`),
  INDEX `idx_ctx_exttype` (`ext_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ══════════════════════════════════════════════════════════════════════════════
# ZIGZAG
# ══════════════════════════════════════════════════════════════════════════════

def _zigzag(prices: np.ndarray, threshold_pct: float) -> list[tuple[int, str]]:
    """
    Возвращает список (index, 'max'|'min') — ZigZag-экстремумы.
    threshold_pct: минимальное изменение цены в процентах для нового экстремума.
    """
    if len(prices) < 3:
        return []

    thr = threshold_pct / 100.0
    result: list[tuple[int, str]] = []

    # Инициализация: ищем первый значимый шаг
    last_ext_idx  = 0
    last_ext_price = prices[0]
    direction: str | None = None  # 'up' | 'down'

    for i in range(1, len(prices)):
        p = prices[i]
        if direction is None:
            change = (p - last_ext_price) / last_ext_price if last_ext_price != 0 else 0
            if change > thr:
                result.append((last_ext_idx, "min"))
                direction = "up"
                last_ext_idx   = i
                last_ext_price = p
            elif change < -thr:
                result.append((last_ext_idx, "max"))
                direction = "down"
                last_ext_idx   = i
                last_ext_price = p
        elif direction == "up":
            if p > last_ext_price:
                last_ext_idx   = i
                last_ext_price = p
            elif last_ext_price != 0 and (last_ext_price - p) / last_ext_price > thr:
                result.append((last_ext_idx, "max"))
                direction      = "down"
                last_ext_idx   = i
                last_ext_price = p
        else:  # direction == "down"
            if p < last_ext_price:
                last_ext_idx   = i
                last_ext_price = p
            elif last_ext_price != 0 and (p - last_ext_price) / last_ext_price > thr:
                result.append((last_ext_idx, "min"))
                direction      = "up"
                last_ext_idx   = i
                last_ext_price = p

    # Добавляем последний незакрытый экстремум
    if result:
        last_type = "min" if direction == "up" else "max"
        if last_ext_idx != result[-1][0]:
            result.append((last_ext_idx, last_type))

    return result


# ══════════════════════════════════════════════════════════════════════════════
# ГИСТОГРАММА + WMA + ПАТТЕРНЫ
# ══════════════════════════════════════════════════════════════════════════════

def _wma(values: list[float], half: int = 3) -> list[float]:
    """
    Центрированная линейно-взвешенная скользящая средняя.

    Для каждого бина i берём окно [i-half .. i+half], обрезаем по границам.
    Веса линейно растут от 0.5 до 1.5 внутри окна.

    При half=3: максимальное окно = 7 бинов.
    Формула воспроизводит WMA из оригинального анализа (проверено на всех датасетах).
    """
    n = len(values)
    result = []
    for i in range(n):
        start = max(0, i - half)
        end   = min(n - 1, i + half)
        chunk = values[start:end + 1]
        w     = len(chunk)
        ws    = [0.5 + j * (1.0 / (w - 1)) for j in range(w)] if w > 1 else [1.0]
        total_w = sum(ws)
        total_v = sum(ww * vv for ww, vv in zip(ws, chunk))
        result.append(total_v / total_w if total_w > 0 else 0.0)
    return result


def _build_histogram(
    intervals: list[float],
    bin_size: float,
) -> list[tuple[float, int]]:
    """
    Возвращает [(bin_center, count), ...].
    bin_center = i * bin_size + bin_size/2.
    """
    if not intervals:
        return []
    max_val = max(intervals)
    n_bins  = int(max_val / bin_size) + 1
    counts  = [0] * n_bins
    for v in intervals:
        idx = int(v / bin_size)
        if idx < n_bins:
            counts[idx] += 1
    result = []
    for i, cnt in enumerate(counts):
        center = i * bin_size + bin_size / 2
        result.append((round(center, 4), cnt))
    return result


def _find_dip_then_rise(
    hist: list[tuple[float, int]],
    wma_values: list[float],
    min_dip_abs: float = MIN_DIP_ABS,
) -> list[dict]:
    """
    Паттерн «нырок→подъём»: diff = count - wma.
    Ищем участки где diff меняет знак с «+» на «−» на «+».
    Возвращает список {entry_x, exit_x, max_dip_x, max_diff_abs, occ_count}.

    max_dip_x = bin с max |diff| на переходном участке (entry_x + первый дип-бин).
    Это соответствует алгоритму оригинального анализа.
    """
    if len(hist) < 3:
        return []

    patterns = []
    diffs = [cnt - wma for (_, cnt), wma in zip(hist, wma_values)]

    i = 0
    while i < len(diffs) - 1:
        # Ищем переход + → − (начало нырка)
        if diffs[i] >= 0 and diffs[i + 1] < 0:
            entry_x   = hist[i][0]
            dip_start = i + 1

            # max_dip_x = argmax |diff| среди entry_x и первого дип-бина
            # (именно так работает оригинал)
            if abs(diffs[i]) >= abs(diffs[dip_start]):
                max_dip_x = hist[i][0]
            else:
                max_dip_x = hist[dip_start][0]

            # Идём по отрицательной зоне до следующего пересечения
            j = dip_start + 1
            while j < len(diffs) and diffs[j] < 0:
                j += 1

            if j < len(diffs) and diffs[j] >= 0:
                exit_x    = hist[j][0]
                dip_depth = abs(min(diffs[dip_start:j], default=0))
                if dip_depth >= min_dip_abs:
                    n_extrema = sum(cnt for _, cnt in hist[dip_start:j + 1])
                    patterns.append({
                        "entry_x":          entry_x,
                        "exit_x":           exit_x,
                        "max_dip_x":        max_dip_x,
                        "max_diff_abs":     abs(min(diffs[dip_start:j], default=0)),
                        "occurrence_count": n_extrema,
                    })
                i = j
            else:
                i = j
        else:
            i += 1

    return patterns


# ══════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА КОТИРОВОК ИЗ БД
# ══════════════════════════════════════════════════════════════════════════════

async def _load_rates(engine, table: str) -> list[dict]:
    """Загрузка OHLC-котировок из brain_rates_* таблицы."""
    try:
        async with engine.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT `date`, `open`, `close`, `min`, `max`
                FROM `{table}`
                WHERE `close` IS NOT NULL
                  AND `close` > 0
                ORDER BY `date`
            """))
            return [dict(r) for r in res.mappings().all()]
    except Exception as e:
        print(f"[extremum_ctx] load_rates error ({table}): {e}")
        return []


def _detect_frame(rates: list[dict]) -> tuple[str, int]:
    """Определяет таймфрейм по шагу между барами. Возвращает (frame, divisor)."""
    if len(rates) < 2:
        return "hourly", 24
    delta = (rates[-1]["date"] - rates[0]["date"]).total_seconds() / max(1, len(rates) - 1)
    if delta >= 86_400 * 0.9:
        return "daily", 7
    return "hourly", 24


# ══════════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА АНАЛИЗА
# ══════════════════════════════════════════════════════════════════════════════

def _analyze_table(
    table_name: str,
    rates: list[dict],
    thresholds: list[float],
) -> tuple[list[dict], list[dict]]:
    """
    Запускает полный анализ для одной таблицы.
    Возвращает (event_rows, ctx_rows).
    """
    if not rates:
        return [], []

    frame, divisor = _detect_frame(rates)
    bin_size = HISTOGRAM_BIN_SIZE_DAILY if frame == "daily" else HISTOGRAM_BIN_SIZE_HOURLY
    prices = np.array([float(r["close"]) for r in rates], dtype=np.float64)
    dates  = [r["date"] for r in rates]

    event_rows: list[dict] = []
    ctx_rows:   list[dict] = []

    for thr in thresholds:
        zigzag_points = _zigzag(prices, thr)
        if len(zigzag_points) < 4:
            continue

        # Группируем по ext_type: all / max / min
        extrema_by_type: dict[str, list[tuple]] = {
            "all": zigzag_points,
            "max": [(i, t) for i, t in zigzag_points if t == "max"],
            "min": [(i, t) for i, t in zigzag_points if t == "min"],
        }

        for ext_type, pts in extrema_by_type.items():
            if len(pts) < 4:
                continue

            event_key = f"{table_name}_{thr}_{ext_type}"

            # ── Строки событий ─────────────────────────────────────────────
            for idx, _etype in pts:
                event_rows.append({
                    "event_key":     event_key,
                    "table_name":    table_name,
                    "threshold_pct": thr,
                    "ext_type":      ext_type,
                    "date":          dates[idx],
                    "close_price":   float(prices[idx]),
                })

            # ── Интервалы и гистограмма ────────────────────────────────────
            pt_dates  = [dates[idx] for idx, _ in pts]
            intervals = []
            for k in range(1, len(pt_dates)):
                raw_delta = (pt_dates[k] - pt_dates[k - 1]).total_seconds() / 3600
                intervals.append(raw_delta / divisor)  # в днях или неделях

            hist = _build_histogram(intervals, bin_size)
            if not hist:
                continue

            counts    = [cnt for _, cnt in hist]
            wma_vals  = _wma(counts)  # centered WMA, half=3
            patterns  = _find_dip_then_rise(hist, wma_vals)

            if ONLY_RECURRING:
                patterns = [p for p in patterns if p["occurrence_count"] > 1]

            for pat in patterns:
                ctx_rows.append({
                    "event_key":        event_key,
                    "table_name":       table_name,
                    "threshold_pct":    thr,
                    "ext_type":         ext_type,
                    "frame":            frame,
                    "divisor":          divisor,
                    "entry_x":          pat["entry_x"],
                    "exit_x":           pat["exit_x"],
                    "max_dip_x":        pat["max_dip_x"],
                    "max_diff_abs":     pat["max_diff_abs"],
                    "occurrence_count": pat["occurrence_count"],
                })

    return event_rows, ctx_rows


# ══════════════════════════════════════════════════════════════════════════════
# DDL + ЗАПИСЬ В БД
# ══════════════════════════════════════════════════════════════════════════════

async def _ensure_tables(engine_vlad) -> None:
    async with engine_vlad.begin() as conn:
        await conn.execute(text(DDL_EVENTS))
        await conn.execute(text(DDL_CTX))


async def _write_events(engine_vlad, rows: list[dict]) -> int:
    if not rows:
        return 0
    BATCH = 1000
    written = 0
    async with engine_vlad.begin() as conn:
        for i in range(0, len(rows), BATCH):
            batch = rows[i:i + BATCH]
            await conn.execute(text(f"""
                INSERT INTO `{EVENTS_TABLE}`
                    (event_key, table_name, threshold_pct, ext_type, date, close_price)
                VALUES
                    (:event_key, :table_name, :threshold_pct, :ext_type, :date, :close_price)
                ON DUPLICATE KEY UPDATE
                    close_price = VALUES(close_price)
            """), batch)
            written += len(batch)
    return written


async def _write_ctx(engine_vlad, rows: list[dict]) -> int:
    if not rows:
        return 0
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))
        await conn.execute(text(f"""
            INSERT INTO `{CTX_TABLE}`
                (event_key, table_name, threshold_pct, ext_type,
                 frame, divisor, entry_x, exit_x, max_dip_x,
                 max_diff_abs, occurrence_count)
            VALUES
                (:event_key, :table_name, :threshold_pct, :ext_type,
                 :frame, :divisor, :entry_x, :exit_x, :max_dip_x,
                 :max_diff_abs, :occurrence_count)
        """), rows)
    return len(rows)


# ══════════════════════════════════════════════════════════════════════════════
# ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════════════════════

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    Основная функция — вызывается фреймворком.
    """
    src_engine = engine_vlad if SRC_ENGINE == "vlad" else engine_brain

    await _ensure_tables(engine_vlad)

    all_event_rows: list[dict] = []
    all_ctx_rows:   list[dict] = []
    per_table_stats: list[dict] = []

    for table in RATES_TABLES:
        rates = await _load_rates(src_engine, table)
        if not rates:
            per_table_stats.append({"table": table, "rates": 0, "events": 0, "patterns": 0})
            continue

        thresholds = THRESHOLDS_MAP.get(table, DEFAULT_THRESHOLDS)
        event_rows, ctx_rows = _analyze_table(table, rates, thresholds)

        all_event_rows.extend(event_rows)
        all_ctx_rows.extend(ctx_rows)
        per_table_stats.append({
            "table":    table,
            "rates":    len(rates),
            "events":   len(event_rows),
            "patterns": len(ctx_rows),
        })

    # Сброс событий — частичный (не TRUNCATE, чтобы не терять историю).
    # Для полного перестроения можно использовать TRUNCATE вручную.
    events_written = await _write_events(engine_vlad, all_event_rows)
    ctx_written    = await _write_ctx(engine_vlad, all_ctx_rows)

    return {
        "events_table":  EVENTS_TABLE,
        "ctx_table":     CTX_TABLE,
        "tables":        per_table_stats,
        "total_events":  events_written,
        "total_patterns": ctx_written,
    }