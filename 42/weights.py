"""
weights.py — framework builder for brain_calendar_weights.

Фреймворк вызывает:
    await build_weights(engine_vlad)

Сохраняет старый формат weight_code:
    E{event_id}_{currency}_{importance}_{forecast}_{surprise}_{revision}_{mode}
    E{event_id}_{currency}_{importance}_{forecast}_{surprise}_{revision}_{mode}_{shift}
"""

from __future__ import annotations

import asyncio
import logging
import os

from sqlalchemy import text

log = logging.getLogger(__name__)


CTX_TABLE = os.getenv("CTX_TABLE", "brain_calendar_context_idx")
OUT_TABLE = os.getenv("OUT_TABLE", "brain_calendar_weights")

# brain_calendar_weights / brain_calendar_context_idx — ОБЩИЕ таблицы,
# в которые независимо пишут несколько сервисов (s42/s44/s49,
# все с дефолтным CTX_TABLE/OUT_TABLE, без переопределения в .env).
# Имя advisory lock привязано к имени таблицы, а не к SERVICE_ID,
# чтобы сериализовать rebuild между ВСЕМИ сервисами, делящими эту таблицу.
REBUILD_LOCK_NAME = f"rebuild_{OUT_TABLE}"

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

    # Fill a connection-private table before locking the published dictionary.
    # Session lifetime preserves TEMP rows across the bounded staging commits.
    stage = "_brain_calendar_weights_stage"
    columns = ("weight_code", "event_id", "currency_code", "importance",
               "forecast_dir", "surprise_dir", "revision_dir", "mode_val",
               "hour_shift", "occurrence_count")
    names = ", ".join("`" + name + "`" for name in columns)
    async with engine_vlad.connect() as conn:
        async def reject_warnings(context):
            result = await conn.execute(text("SHOW WARNINGS"))
            if result.fetchall():
                raise RuntimeError(f"Calendar weights {context} produced SQL warnings")

        got_lock = 0
        try:
            # Every requested rebuild runs. Wait cooperatively for the shared
            # dictionary publisher instead of discarding a contending cycle.
            while True:
                got_lock = (await conn.execute(
                    text("SELECT GET_LOCK(:name, 0)"),
                    {"name": REBUILD_LOCK_NAME},
                )).scalar()
                await conn.commit()
                if got_lock == 1:
                    break
                if got_lock != 0:
                    raise RuntimeError("Calendar weights could not acquire rebuild lock")
                await asyncio.sleep(0.1)

            await conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{stage}`"))
            await conn.execute(text(f"CREATE TEMPORARY TABLE `{stage}` LIKE `{OUT_TABLE}`"))
            await reject_warnings("staging setup")
            await conn.commit()
            stage_statements = {}
            for offset in range(0, len(rows), 500):
                batch = rows[offset:offset + 500]
                if len(batch) not in stage_statements:
                    groups = ["(" + ", ".join(f":r{i}_{name}" for name in columns) + ")"
                              for i in range(len(batch))]
                    stage_statements[len(batch)] = text(f"""
                        INSERT INTO `{stage}` ({names}) VALUES {", ".join(groups)} AS incoming
                        ON DUPLICATE KEY UPDATE occurrence_count = incoming.occurrence_count
                    """)
                parameters = {f"r{i}_{name}": row[name]
                              for i, row in enumerate(batch) for name in columns}
                # One physical statement per batch: executemany with a row alias
                # falls back to separate statements and loses earlier warnings.
                await conn.execute(stage_statements[len(batch)], parameters)
                await reject_warnings("staging insert")
                await conn.commit()

            # Discover changes with ordinary nonlocking reads. INSERT SELECT,
            # UPDATE JOIN and DELETE JOIN can lock the whole published table
            # even for an unchanged snapshot under InnoDB REPEATABLE READ.
            payload = columns[1:] if TRUNCATE_OUT else ("occurrence_count",)
            comparison = " OR ".join(
                f"NOT (t.`{name}` <=> s.`{name}`)" for name in payload
            )
            old_names = ", ".join(f"t.`{name}` AS `old_{name}`" for name in columns[1:])
            new_rows = [dict(row) for row in (await conn.execute(text(f"""
                SELECT {", ".join(f's.`{name}`' for name in columns)}
                FROM `{stage}` AS s LEFT JOIN `{OUT_TABLE}` AS t
                ON t.weight_code = s.weight_code WHERE t.weight_code IS NULL
            """))).mappings().all()]
            updates = [dict(row) for row in (await conn.execute(text(f"""
                SELECT {", ".join(f's.`{name}`' for name in columns)}, {old_names}
                FROM `{stage}` AS s JOIN `{OUT_TABLE}` AS t
                ON t.weight_code = s.weight_code WHERE {comparison}
            """))).mappings().all()]
            deletes = []
            if TRUNCATE_OUT:
                deletes = [dict(row) for row in (await conn.execute(text(f"""
                    SELECT t.weight_code, {old_names}
                    FROM `{OUT_TABLE}` AS t LEFT JOIN `{stage}` AS s
                    ON s.weight_code = t.weight_code WHERE s.weight_code IS NULL
                """))).mappings().all()]
            await conn.commit()

            # The table-scoped session lock still excludes other builders.
            # Each permanent write is additionally guarded by its exact old
            # payload and positive affected-row count. Unchanged definitions
            # retain their real creation timestamp and generate no undo.
            async with conn.begin():
                for offset in range(0, len(new_rows), 500):
                    batch = new_rows[offset:offset + 500]
                    values = ["(" + ", ".join(f":n{i}_{name}" for name in columns) + ")"
                              for i in range(len(batch))]
                    parameters = {f"n{i}_{name}": row[name]
                                  for i, row in enumerate(batch) for name in columns}
                    result = await conn.execute(text(
                        f"INSERT INTO `{OUT_TABLE}` ({names}) VALUES " + ", ".join(values)
                    ), parameters)
                    await reject_warnings("delta insert")
                    if result.rowcount != len(batch):
                        raise RuntimeError("Calendar weights insert count mismatch")

                for offset in range(0, len(updates), 250):
                    batch = updates[offset:offset + 250]
                    parameters = {}
                    for i, row in enumerate(batch):
                        parameters[f"k{i}"] = row["weight_code"]
                        for name in payload:
                            parameters[f"v{i}_{name}"] = row[name]
                        for name in columns[1:]:
                            parameters[f"o{i}_{name}"] = row[f"old_{name}"]
                    assignments = []
                    for name in payload:
                        cases = " ".join(f"WHEN :k{i} THEN :v{i}_{name}" for i in range(len(batch)))
                        assignments.append(f"`{name}` = CASE weight_code {cases} ELSE `{name}` END")
                    # created_at describes the creation of a dictionary row;
                    # feature values and trained model bindings do not use it.
                    predicates = []
                    for i in range(len(batch)):
                        old = " AND ".join(f"`{name}` <=> :o{i}_{name}" for name in columns[1:])
                        predicates.append(f"(weight_code = :k{i} AND {old})")
                    keys = ", ".join(f":k{i}" for i in range(len(batch)))
                    result = await conn.execute(text(
                        f"UPDATE `{OUT_TABLE}` SET " + ", ".join(assignments)
                        + f" WHERE weight_code IN ({keys}) AND (" + " OR ".join(predicates) + ")"
                    ), parameters)
                    await reject_warnings("delta update")
                    if result.rowcount != len(batch):
                        raise RuntimeError("Calendar weights update preimage conflict")

                for offset in range(0, len(deletes), 250):
                    batch = deletes[offset:offset + 250]
                    parameters = {}
                    predicates = []
                    for i, row in enumerate(batch):
                        parameters[f"k{i}"] = row["weight_code"]
                        for name in columns[1:]:
                            parameters[f"o{i}_{name}"] = row[f"old_{name}"]
                        old = " AND ".join(f"`{name}` <=> :o{i}_{name}" for name in columns[1:])
                        predicates.append(f"(weight_code = :k{i} AND {old})")
                    keys = ", ".join(f":k{i}" for i in range(len(batch)))
                    result = await conn.execute(text(
                        f"DELETE FROM `{OUT_TABLE}` WHERE weight_code IN ({keys}) AND ("
                        + " OR ".join(predicates) + ")"
                    ), parameters)
                    await reject_warnings("delta delete")
                    if result.rowcount != len(batch):
                        raise RuntimeError("Calendar weights delete preimage conflict")
        finally:
            try:
                if conn.in_transaction():
                    await conn.rollback()
                await conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{stage}`"))
                await conn.commit()
            finally:
                try:
                    # Also release after cancellation during GET_LOCK: the
                    # server may have acquired it before the await was cancelled.
                    released = (await conn.execute(text("SELECT RELEASE_LOCK(:name)"),
                                                   {"name": REBUILD_LOCK_NAME})).scalar()
                    await conn.commit()
                    if got_lock == 1 and released != 1:
                        raise RuntimeError("Calendar weights rebuild lock was not released")
                except BaseException:
                    # A pooled rollback does not release MySQL session locks.
                    await conn.invalidate()
                    raise

    return {
        "table": OUT_TABLE,
        "ctx_table": CTX_TABLE,
        "skipped": False,
        "contexts": len(ctx_rows),
        "recurring": recurring,
        "non_recurring": non_recurring,
        "weights": len(rows),
        "modes": list(MODES),
        "shift_min": SHIFT_MIN,
        "shift_max": SHIFT_MAX,
        "truncate": TRUNCATE_OUT,
    }
