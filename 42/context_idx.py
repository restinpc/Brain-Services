"""
context_idx.py — framework builder for brain_calendar_context_idx.

Фреймворк вызывает:
    await build_index(engine_vlad, engine_brain)

Контекст:
    event_id × currency_code × importance × forecast_dir × surprise_dir × revision_dir
"""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text


SRC_TABLE = os.getenv("SRC_TABLE", "brain_calendar")
CTX_TABLE = os.getenv("CTX_TABLE", "brain_calendar_context_idx")

# Обычно события берутся из engine_brain.
# Если нужно строить из engine_vlad, поставь SRC_ENGINE=vlad.
SRC_ENGINE = os.getenv("SRC_ENGINE", "brain").lower()

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"

DIRECTION_THRESHOLD = float(os.getenv("DIRECTION_THRESHOLD", "0.01"))

SKIP_EVENT_TYPES = {2}


DDL = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  `id`                  BIGINT         NOT NULL AUTO_INCREMENT,
  `event_id`            INT            NOT NULL,
  `currency_code`       VARCHAR(4)     NOT NULL,
  `importance`          VARCHAR(10)    NOT NULL,
  `forecast_dir`        VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `surprise_dir`        VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `revision_dir`        VARCHAR(8)     NOT NULL DEFAULT 'NONE',
  `occurrence_count`    INT            NOT NULL DEFAULT 0,
  `first_dt`            DATETIME       NULL,
  `last_dt`             DATETIME       NULL,
  `avg_actual`          DOUBLE         NULL,
  `avg_surprise_abs`    DOUBLE         NULL,
  `avg_revision_abs`    DOUBLE         NULL,
  `updated_at`          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctx_event_dirs`
    (`event_id`, `currency_code`, `importance`,
     `forecast_dir`, `surprise_dir`, `revision_dir`),
  INDEX `idx_ctx_event`    (`event_id`),
  INDEX `idx_ctx_currency` (`currency_code`),
  INDEX `idx_ctx_imp`      (`importance`),
  INDEX `idx_ctx_forecast` (`forecast_dir`),
  INDEX `idx_ctx_surprise` (`surprise_dir`),
  INDEX `idx_ctx_revision` (`revision_dir`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _source_engine(engine_vlad, engine_brain):
    return engine_vlad if SRC_ENGINE == "vlad" else engine_brain


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _rel_direction(
    actual: Any,
    reference: Any,
    threshold: float,
    *,
    up_label: str = "UP",
    down_label: str = "DOWN",
    flat_label: str = "FLAT",
) -> str:
    actual_f = _to_float(actual)
    ref_f = _to_float(reference)

    if actual_f is None or ref_f is None:
        return "UNKNOWN"

    if ref_f == 0:
        if actual_f > 0:
            return up_label
        if actual_f < 0:
            return down_label
        return flat_label

    pct = (actual_f - ref_f) / abs(ref_f)

    if pct > threshold:
        return up_label
    if pct < -threshold:
        return down_label
    return flat_label


def classify_event(
    forecast: Any,
    previous: Any,
    old_previous: Any,
    actual: Any,
    threshold: float = DIRECTION_THRESHOLD,
) -> tuple[str, str, str]:
    forecast_f = _to_float(forecast)

    if forecast_f is None or forecast_f == 0:
        forecast_dir = "UNKNOWN"
    else:
        forecast_dir = _rel_direction(
            actual,
            forecast,
            threshold,
            up_label="BEAT",
            down_label="MISS",
            flat_label="INLINE",
        )

    surprise_dir = _rel_direction(
        actual,
        previous,
        threshold,
        up_label="UP",
        down_label="DOWN",
        flat_label="FLAT",
    )

    old_prev_f = _to_float(old_previous)
    previous_f = _to_float(previous)

    if old_prev_f is None or old_prev_f == 0 or previous_f is None:
        revision_dir = "NONE"
    elif previous_f == old_prev_f:
        revision_dir = "FLAT"
    else:
        revision_dir = _rel_direction(
            previous,
            old_previous,
            threshold,
            up_label="UP",
            down_label="DOWN",
            flat_label="FLAT",
        )

    return forecast_dir, surprise_dir, revision_dir


async def _ensure_table(engine_vlad) -> None:
    async with engine_vlad.begin() as conn:
        await conn.execute(text(DDL))


async def _load_url_to_event_id(engine_vlad) -> dict[str, int]:
    out: dict[str, int] = {}

    try:
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT Url, EventId
                FROM `{SRC_TABLE}`
                WHERE Url IS NOT NULL
                  AND Url != ''
                  AND EventId IS NOT NULL
                GROUP BY Url, EventId
            """))

            for r in res.mappings().all():
                out[str(r["Url"])] = int(r["EventId"])

    except Exception:
        return {}

    return out


async def _fetch_rows_with_event_id(src_engine) -> list[dict]:
    async with src_engine.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT
                EventId          AS event_id,
                CurrencyCode     AS currency_code,
                Importance       AS importance,
                ForecastValue    AS forecast_value,
                PreviousValue    AS previous_value,
                OldPreviousValue AS old_previous_value,
                ActualValue      AS actual_value,
                FullDate         AS full_date,
                EventType        AS event_type
            FROM `{SRC_TABLE}`
            WHERE ActualValue IS NOT NULL
              AND Processed = 1
              AND FullDate IS NOT NULL
            ORDER BY FullDate
        """))

        return [dict(r) for r in res.mappings().all()]


async def _fetch_rows_with_url_mapping(src_engine, engine_vlad) -> list[dict]:
    url_to_event_id = await _load_url_to_event_id(engine_vlad)

    async with src_engine.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT
                Url              AS url,
                CurrencyCode     AS currency_code,
                Importance       AS importance,
                ForecastValue    AS forecast_value,
                PreviousValue    AS previous_value,
                OldPreviousValue AS old_previous_value,
                ActualValue      AS actual_value,
                FullDate         AS full_date,
                EventType        AS event_type
            FROM `{SRC_TABLE}`
            WHERE ActualValue IS NOT NULL
              AND Processed = 1
              AND FullDate IS NOT NULL
            ORDER BY FullDate
        """))

        rows = []
        for r in res.mappings().all():
            row = dict(r)
            url = row.get("url")
            row["event_id"] = url_to_event_id.get(str(url)) if url else None
            rows.append(row)

        return rows


async def _fetch_source_rows(engine_vlad, engine_brain) -> tuple[list[dict], str]:
    src_engine = _source_engine(engine_vlad, engine_brain)

    try:
        rows = await _fetch_rows_with_event_id(src_engine)
        return rows, f"{SRC_ENGINE}.{SRC_TABLE}:direct_event_id"
    except Exception:
        rows = await _fetch_rows_with_url_mapping(src_engine, engine_vlad)
        return rows, f"{SRC_ENGINE}.{SRC_TABLE}:url_to_event_id"


def _aggregate_rows(rows: list[dict]) -> tuple[dict[tuple, dict], dict]:
    aggregates: dict[tuple, dict] = {}

    skipped_event_type = 0
    skipped_no_event_id = 0
    skipped_no_currency = 0
    classified = 0

    for row in rows:
        event_type = row.get("event_type")

        if event_type in SKIP_EVENT_TYPES:
            skipped_event_type += 1
            continue

        event_id = row.get("event_id")
        currency = row.get("currency_code")

        if event_id is None:
            skipped_no_event_id += 1
            continue

        if currency is None:
            skipped_no_currency += 1
            continue

        forecast = row.get("forecast_value")
        previous = row.get("previous_value")
        old_previous = row.get("old_previous_value")
        actual = row.get("actual_value")
        full_date = row.get("full_date")

        forecast_dir, surprise_dir, revision_dir = classify_event(
            forecast,
            previous,
            old_previous,
            actual,
            DIRECTION_THRESHOLD,
        )

        importance = str(row.get("importance") or "none").lower()

        key = (
            int(event_id),
            str(currency),
            importance,
            forecast_dir,
            surprise_dir,
            revision_dir,
        )

        if key not in aggregates:
            aggregates[key] = {
                "count": 0,
                "first_dt": full_date,
                "last_dt": full_date,
                "sum_actual": 0.0,
                "sum_surprise": 0.0,
                "sum_revision": 0.0,
            }

        agg = aggregates[key]
        agg["count"] += 1

        if full_date is not None:
            if agg["first_dt"] is None or full_date < agg["first_dt"]:
                agg["first_dt"] = full_date
            if agg["last_dt"] is None or full_date > agg["last_dt"]:
                agg["last_dt"] = full_date

        actual_f = _to_float(actual)
        forecast_f = _to_float(forecast)
        previous_f = _to_float(previous)
        old_previous_f = _to_float(old_previous)

        if actual_f is not None:
            agg["sum_actual"] += actual_f

        if actual_f is not None and forecast_f is not None:
            agg["sum_surprise"] += abs(actual_f - forecast_f)

        if (
            previous_f is not None
            and old_previous_f is not None
            and old_previous_f != 0
        ):
            agg["sum_revision"] += abs(previous_f - old_previous_f)

        classified += 1

    stats = {
        "source_rows": len(rows),
        "classified": classified,
        "skipped_event_type": skipped_event_type,
        "skipped_no_event_id": skipped_no_event_id,
        "skipped_no_currency": skipped_no_currency,
        "contexts_before_filter": len(aggregates),
    }

    if ONLY_RECURRING:
        aggregates = {
            key: agg
            for key, agg in aggregates.items()
            if int(agg["count"] or 0) > 1
        }

    stats["contexts_after_filter"] = len(aggregates)
    stats["only_recurring"] = ONLY_RECURRING

    return aggregates, stats


def _to_insert_rows(aggregates: dict[tuple, dict]) -> list[dict]:
    rows: list[dict] = []

    for key, agg in sorted(aggregates.items()):
        event_id, currency, importance, forecast_dir, surprise_dir, revision_dir = key
        count = int(agg["count"] or 0)

        rows.append({
            "event_id": event_id,
            "currency_code": currency,
            "importance": importance,
            "forecast_dir": forecast_dir,
            "surprise_dir": surprise_dir,
            "revision_dir": revision_dir,
            "occurrence_count": count,
            "first_dt": agg["first_dt"],
            "last_dt": agg["last_dt"],
            "avg_actual": (agg["sum_actual"] / count) if count else None,
            "avg_surprise_abs": (agg["sum_surprise"] / count) if count else None,
            "avg_revision_abs": (agg["sum_revision"] / count) if count else None,
        })

    return rows


async def build_index(engine_vlad, engine_brain) -> dict:
    await _ensure_table(engine_vlad)

    rows, source = await _fetch_source_rows(engine_vlad, engine_brain)
    aggregates, stats = _aggregate_rows(rows)
    insert_rows = _to_insert_rows(aggregates)

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))

        if insert_rows:
            await conn.execute(text(f"""
                INSERT INTO `{CTX_TABLE}` (
                    event_id,
                    currency_code,
                    importance,
                    forecast_dir,
                    surprise_dir,
                    revision_dir,
                    occurrence_count,
                    first_dt,
                    last_dt,
                    avg_actual,
                    avg_surprise_abs,
                    avg_revision_abs
                )
                VALUES (
                    :event_id,
                    :currency_code,
                    :importance,
                    :forecast_dir,
                    :surprise_dir,
                    :revision_dir,
                    :occurrence_count,
                    :first_dt,
                    :last_dt,
                    :avg_actual,
                    :avg_surprise_abs,
                    :avg_revision_abs
                )
            """), insert_rows)

    stats.update({
        "table": CTX_TABLE,
        "source": source,
        "inserted": len(insert_rows),
    })

    return stats
