"""
weights.py — framework builder for brain_calendar_weights.

Фреймворк вызывает:
    await build_weights(engine_vlad)

Сохраняет старый формат weight_code:
    E{event_id}_{currency}_{importance}_{forecast}_{surprise}_{revision}_{mode}
    E{event_id}_{currency}_{importance}_{forecast}_{surprise}_{revision}_{mode}_{shift}
"""

from __future__ import annotations

import os

from sqlalchemy import text


CTX_TABLE = os.getenv("CTX_TABLE", "brain_calendar_context_idx")
OUT_TABLE = os.getenv("OUT_TABLE", "brain_calendar_weights")

SHIFT_MIN = int(os.getenv("SHIFT_MIN", "-12"))
SHIFT_MAX = int(os.getenv("SHIFT_MAX", "12"))

# Новый ML-режим обычно использует только mode=0.
# Если нужна старая совместимость с mode=0 и mode=1, поставь WEIGHT_MODES=0,1.
_WEIGHT_MODES_RAW = os.getenv("WEIGHT_MODES", "0")
MODES = tuple(
    int(x.strip())
    for x in _WEIGHT_MODES_RAW.split(",")
    if x.strip() != ""
)

TRUNCATE_OUT = os.getenv("TRUNCATE_OUT", "1") == "1"


FORECAST_MAP = {
    "UNKNOWN": "X",
    "BEAT": "B",
    "MISS": "M",
    "INLINE": "I",
}

SURPRISE_MAP = {
    "UNKNOWN": "X",
    "UP": "U",
    "DOWN": "D",
    "FLAT": "F",
}

REVISION_MAP = {
    "NONE": "N",
    "FLAT": "T",
    "UP": "U",
    "DOWN": "D",
    "UNKNOWN": "X",
}

IMPORTANCE_MAP = {
    "high": "H",
    "medium": "M",
    "low": "L",
    "none": "N",
}


DDL = f"""
CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
  `weight_code`       VARCHAR(64)   NOT NULL,
  `event_id`          INT           NOT NULL,
  `currency_code`     VARCHAR(4)    NOT NULL,
  `importance`        VARCHAR(10)   NOT NULL,
  `forecast_dir`      VARCHAR(8)    NOT NULL,
  `surprise_dir`      VARCHAR(8)    NOT NULL,
  `revision_dir`      VARCHAR(8)    NOT NULL,
  `mode_val`          TINYINT       NOT NULL,
  `hour_shift`        SMALLINT      NULL,
  `occurrence_count`  INT           NULL,
  `created_at`        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (`weight_code`),
  KEY `idx_cw_event`    (`event_id`),
  KEY `idx_cw_currency` (`currency_code`),
  KEY `idx_cw_imp`      (`importance`),
  KEY `idx_cw_forecast` (`forecast_dir`),
  KEY `idx_cw_surprise` (`surprise_dir`),
  KEY `idx_cw_revision` (`revision_dir`),
  KEY `idx_cw_mode`     (`mode_val`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def encode(value: str, direction_type: str) -> str:
    maps = {
        "forecast": FORECAST_MAP,
        "surprise": SURPRISE_MAP,
        "revision": REVISION_MAP,
        "importance": IMPORTANCE_MAP,
    }
    return maps.get(direction_type, {}).get(value, "X")


def make_weight_code(
    event_id: int,
    currency: str,
    importance: str,
    fcd: str,
    scd: str,
    rcd: str,
    mode: int,
    hour_shift: int | None = None,
) -> str:
    imp_c = encode(importance, "importance")
    fcd_c = encode(fcd, "forecast")
    scd_c = encode(scd, "surprise")
    rcd_c = encode(rcd, "revision")

    base = f"E{event_id}_{currency}_{imp_c}_{fcd_c}_{scd_c}_{rcd_c}_{mode}"
    return base if hour_shift is None else f"{base}_{hour_shift}"


def decode_weight_code(code: str) -> dict:
    forecast_map_rev = {v: k for k, v in FORECAST_MAP.items()}
    surprise_map_rev = {v: k for k, v in SURPRISE_MAP.items()}
    revision_map_rev = {v: k for k, v in REVISION_MAP.items()}
    importance_map_rev = {v: k for k, v in IMPORTANCE_MAP.items()}

    parts = code.split("_")

    if len(parts) < 7:
        return {}

    try:
        event_id = int(parts[0][1:])
        mode = int(parts[6])
        shift = int(parts[7]) if len(parts) > 7 else None
    except Exception:
        return {}

    return {
        "event_id": event_id,
        "currency_code": parts[1],
        "importance": importance_map_rev.get(parts[2], parts[2]),
        "forecast_dir": forecast_map_rev.get(parts[3], parts[3]),
        "surprise_dir": surprise_map_rev.get(parts[4], parts[4]),
        "revision_dir": revision_map_rev.get(parts[5], parts[5]),
        "mode_val": mode,
        "hour_shift": shift,
    }


async def _ensure_table(engine_vlad) -> None:
    async with engine_vlad.begin() as conn:
        await conn.execute(text(DDL))


def _generate_rows(ctx: dict):
    event_id = int(ctx["event_id"])
    currency = str(ctx["currency_code"])
    importance = str(ctx["importance"] or "none").lower()
    fcd = str(ctx["forecast_dir"] or "UNKNOWN")
    scd = str(ctx["surprise_dir"] or "UNKNOWN")
    rcd = str(ctx["revision_dir"] or "NONE")
    occ = int(ctx["occurrence_count"] or 0)

    is_recurring = occ > 1

    # Базовые строки без shift — всегда.
    for mode in MODES:
        yield {
            "weight_code": make_weight_code(
                event_id,
                currency,
                importance,
                fcd,
                scd,
                rcd,
                mode,
                None,
            ),
            "event_id": event_id,
            "currency_code": currency,
            "importance": importance,
            "forecast_dir": fcd,
            "surprise_dir": scd,
            "revision_dir": rcd,
            "mode_val": mode,
            "hour_shift": None,
            "occurrence_count": occ,
        }

    # Shift-строки — только для повторяющихся контекстов.
    if is_recurring:
        for shift in range(SHIFT_MIN, SHIFT_MAX + 1):
            for mode in MODES:
                yield {
                    "weight_code": make_weight_code(
                        event_id,
                        currency,
                        importance,
                        fcd,
                        scd,
                        rcd,
                        mode,
                        shift,
                    ),
                    "event_id": event_id,
                    "currency_code": currency,
                    "importance": importance,
                    "forecast_dir": fcd,
                    "surprise_dir": scd,
                    "revision_dir": rcd,
                    "mode_val": mode,
                    "hour_shift": shift,
                    "occurrence_count": occ,
                }


async def build_weights(engine_vlad) -> dict:
    await _ensure_table(engine_vlad)

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT
                event_id,
                currency_code,
                importance,
                forecast_dir,
                surprise_dir,
                revision_dir,
                occurrence_count
            FROM `{CTX_TABLE}`
            ORDER BY
                event_id,
                currency_code,
                importance,
                forecast_dir,
                surprise_dir,
                revision_dir
        """))

        ctx_rows = [dict(r) for r in res.mappings().all()]

    rows: list[dict] = []
    recurring = 0
    non_recurring = 0

    for ctx in ctx_rows:
        occ = int(ctx.get("occurrence_count") or 0)

        if occ > 1:
            recurring += 1
        else:
            non_recurring += 1

        rows.extend(_generate_rows(ctx))

    async with engine_vlad.begin() as conn:
        if TRUNCATE_OUT:
            await conn.execute(text(f"TRUNCATE TABLE `{OUT_TABLE}`"))

        if rows:
            await conn.execute(text(f"""
                INSERT INTO `{OUT_TABLE}` (
                    weight_code,
                    event_id,
                    currency_code,
                    importance,
                    forecast_dir,
                    surprise_dir,
                    revision_dir,
                    mode_val,
                    hour_shift,
                    occurrence_count
                )
                VALUES (
                    :weight_code,
                    :event_id,
                    :currency_code,
                    :importance,
                    :forecast_dir,
                    :surprise_dir,
                    :revision_dir,
                    :mode_val,
                    :hour_shift,
                    :occurrence_count
                )
                ON DUPLICATE KEY UPDATE
                    occurrence_count = VALUES(occurrence_count)
            """), rows)

    return {
        "table": OUT_TABLE,
        "ctx_table": CTX_TABLE,
        "contexts": len(ctx_rows),
        "recurring": recurring,
        "non_recurring": non_recurring,
        "weights": len(rows),
        "modes": list(MODES),
        "shift_min": SHIFT_MIN,
        "shift_max": SHIFT_MAX,
        "truncate": TRUNCATE_OUT,
    }

