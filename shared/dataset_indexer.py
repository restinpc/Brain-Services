"""
dataset_indexer.py — Dataset indexing system for Brain Framework v1.0

Четыре async-функции для автоматического построения индексов парсерных таблиц:

  parse_fields(engine, table_name)                         -> list[str]
  build_indexes(engine, table_name, fields)                -> bool
  parse_indexes(engine, table_name, date_field, date=None) -> bool
  index_parser(engine_vlad, engine_brain, parser_id, date=None) -> dict

Пример использования (standalone):
    from dataset_indexer import index_parser
    result = await index_parser(engine_vlad, engine_brain, parser_id=7)

Пример использования (incremental refresh из model.py):
    from dataset_indexer import parse_indexes
    await parse_indexes(engine_vlad, "vlad_test", "date", date=last_seen_date)

Подключение в brain_framework.py — см. в конце файла раздел
«ИНТЕГРАЦИЯ В BRAIN_FRAMEWORK».
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from itertools import combinations
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger("dataset_indexer")

# ──────────────────────────────────────────────────────────────────────────────
# Настройки критериев отбора полей
# ──────────────────────────────────────────────────────────────────────────────

# Поле годится как индекс, если COUNT > COUNT(DISTINCT) * REPEAT_RATIO
REPEAT_RATIO: int = 10

# И при этом COUNT(DISTINCT) > MIN_DISTINCT (отсекаем малые словари)
MIN_DISTINCT: int = 100

# Паттерны в имени поля, которые указывают на дату/время → пропускаем
_SKIP_NAME_PATTERNS: tuple[str, ...] = (
    "date", "time", "_at", "timestamp", "created", "updated",
)

# Типы данных MySQL, непригодные для индексирования значений
_SKIP_TYPES: frozenset[str] = frozenset({
    "datetime", "date", "timestamp", "time",
    "tinyblob", "blob", "mediumblob", "longblob",
    "tinytext", "text", "mediumtext", "longtext",
    "binary", "varbinary",
})


# ══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИЯ 1 — parse_fields
# ══════════════════════════════════════════════════════════════════════════════

async def parse_fields(engine, table_name: str) -> list[str]:
    """
    Возвращает список полей таблицы table_name, пригодных для индексирования.

    Поле включается в результат, если выполнены оба условия:
      a) COUNT(поле) > COUNT(DISTINCT поле) * 10   — достаточно повторений
      b) COUNT(DISTINCT поле) > 100                — не чисто уникальный ряд

    Автоматически исключаются:
      • поля с именем «id» / «ID»
      • blob/text/datetime-типы
      • поля, в имени которых есть date / time / _at / timestamp и т.п.

    Пример для vlad_test {id, date, a, b, c, x, y, z}:
      → предположим, что a, x, y, z прошли критерии → ['a', 'x', 'y', 'z']
    """
    # 1. Получаем список колонок из information_schema
    async with engine.connect() as conn:
        res = await conn.execute(text("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM   information_schema.COLUMNS
            WHERE  TABLE_SCHEMA = DATABASE()
              AND  TABLE_NAME   = :tbl
            ORDER  BY ORDINAL_POSITION
        """), {"tbl": table_name})
        columns: list[tuple[str, str]] = [
            (r["COLUMN_NAME"], r["DATA_TYPE"]) for r in res.mappings().all()
        ]

    if not columns:
        logger.warning("parse_fields: таблица %s не найдена или пустая схема", table_name)
        return []

    # 2. Предварительная фильтрация по типу и имени
    candidates: list[str] = []
    for col, dtype in columns:
        col_lower = col.lower()
        if col_lower == "id":
            continue
        if dtype.lower() in _SKIP_TYPES:
            continue
        if any(pat in col_lower for pat in _SKIP_NAME_PATTERNS):
            continue
        candidates.append(col)

    if not candidates:
        logger.info("parse_fields: нет кандидатов после предфильтрации в %s", table_name)
        return []

    # 3. Проверяем статистические критерии для каждого кандидата
    result: list[str] = []
    async with engine.connect() as conn:
        for col in candidates:
            try:
                row = (await conn.execute(text(
                    f"SELECT COUNT(`{col}`), COUNT(DISTINCT `{col}`) FROM `{table_name}`"
                ))).fetchone()
                if row is None:
                    continue
                cnt, dist = int(row[0] or 0), int(row[1] or 0)
                if dist > MIN_DISTINCT and cnt > dist * REPEAT_RATIO:
                    result.append(col)
                    logger.debug(
                        "parse_fields: %s.%s — cnt=%d dist=%d → PASS", table_name, col, cnt, dist
                    )
                else:
                    logger.debug(
                        "parse_fields: %s.%s — cnt=%d dist=%d → SKIP", table_name, col, cnt, dist
                    )
            except Exception as exc:
                logger.warning("parse_fields: ошибка проверки %s.%s: %s", table_name, col, exc)

    logger.info(
        "parse_fields: %s → кандидатов %d, прошло %d: %s",
        table_name, len(candidates), len(result), result,
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИЯ 2 — build_indexes
# ══════════════════════════════════════════════════════════════════════════════

async def build_indexes(engine, table_name: str, fields: list[str]) -> bool:
    """
    Создаёт таблицу {table_name}_mask с уникальными комбинациями индексов.

    Структура: {id INT AUTO_INCREMENT, json TEXT}
    Наполнение:
      • 1 полная маска  : [a, x, y, z]        (все поля из fields)
      • C(N,N-1) неполных масок               (все подмножества длиной N-1)

    Для fields = [a, x, y, z] (N=4) → 5 строк:
      id=1  json='["a","x","y","z"]'
      id=2  json='["a","x","y"]'
      id=3  json='["a","x","z"]'
      id=4  json='["a","y","z"]'
      id=5  json='["x","y","z"]'

    Таблица полностью перезаписывается при каждом вызове (TRUNCATE + INSERT).

    Возвращает True  — когда работа выполнена.
    Возвращает False — когда fields пуст (нечего делать).
    """
    if not fields:
        logger.info("build_indexes: пустой список полей, пропуск")
        return False

    mask_table = f"{table_name}_mask"

    # Генерируем комбинации: полная + все (N-1)-подмножества
    combos: list[list[str]] = [list(fields)]
    if len(fields) > 1:
        for subset in combinations(fields, len(fields) - 1):
            combos.append(list(subset))

    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{mask_table}` (
                `id`   INT  NOT NULL AUTO_INCREMENT,
                `json` TEXT NOT NULL,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Index masks for {table_name}'
        """))
        await conn.execute(text(f"TRUNCATE TABLE `{mask_table}`"))
        await conn.execute(
            text(f"INSERT INTO `{mask_table}` (`json`) VALUES (:v)"),
            [{"v": json.dumps(combo, ensure_ascii=False)} for combo in combos],
        )

    logger.info(
        "build_indexes: %s → создано %d масок: %s",
        mask_table, len(combos), combos,
    )
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИЯ 3 — parse_indexes
# ══════════════════════════════════════════════════════════════════════════════

def _combo_hash(mask_id: int, values: tuple) -> str:
    """MD5 от (mask_id | v1 | v2 | ...) для UNIQUE KEY в таблице индексов."""
    raw = f"{mask_id}|" + "|".join("" if v is None else str(v) for v in values)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


async def _get_col_definitions(engine, table_name: str, fields: list[str]) -> dict[str, str]:
    """
    Возвращает {col_name: 'COLUMN_TYPE NULL'} для указанных полей.
    Используется при динамическом DDL таблицы индексов.
    """
    if not fields:
        return {}
    placeholders = ", ".join(f":c{i}" for i in range(len(fields)))
    params = {"tbl": table_name, **{f"c{i}": f for i, f in enumerate(fields)}}
    try:
        async with engine.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT COLUMN_NAME, COLUMN_TYPE
                FROM   information_schema.COLUMNS
                WHERE  TABLE_SCHEMA = DATABASE()
                  AND  TABLE_NAME   = :tbl
                  AND  COLUMN_NAME  IN ({placeholders})
            """), params)
            return {
                r["COLUMN_NAME"]: f"`{r['COLUMN_NAME']}` {r['COLUMN_TYPE']} NULL"
                for r in res.mappings().all()
            }
    except Exception as exc:
        logger.warning("_get_col_definitions: %s", exc)
        return {}


async def parse_indexes(
    engine,
    table_name: str,
    date_field: str,
    date: Optional[datetime] = None,
) -> bool:
    """
    Создаёт/обновляет таблицу {table_name}_indexes.

    Структура: {id, mask_id, <index_fields из parse_fields>, date_added, combo_hash}
    combo_hash — UNIQUE KEY, гарантирует идемпотентность INSERT IGNORE.

    Алгоритм:
      Для каждой маски из {table_name}_mask:
        SELECT DISTINCT <mask_fields>, MIN(date_field) AS date_added
        FROM table_name
        [WHERE date_field >= :date]   ← инкрементальный режим
        GROUP BY <mask_fields>
        → INSERT IGNORE INTO {table_name}_indexes

    Режим date=None    → полный перестрой (TRUNCATE + INSERT)
    Режим date=<дата>  → дешёвый «подтяг» новых комбо за период

    Возвращает True  — успех.
    Возвращает False — критическая ошибка (маски не найдены и т.п.).
    """
    mask_table    = f"{table_name}_mask"
    indexes_table = f"{table_name}_indexes"

    # 1. Загружаем маски
    try:
        async with engine.connect() as conn:
            res = await conn.execute(text(
                f"SELECT id, `json` FROM `{mask_table}` ORDER BY id"
            ))
            masks: list[tuple[int, list[str]]] = [
                (int(r[0]), json.loads(r[1])) for r in res.fetchall()
            ]
    except Exception as exc:
        logger.error("parse_indexes: не удалось загрузить %s: %s", mask_table, exc)
        return False

    if not masks:
        logger.warning("parse_indexes: %s пуста, нечего индексировать", mask_table)
        return False

    # 2. Собираем union всех полей по всем маскам (для единой DDL)
    all_fields: list[str] = []
    seen: set[str] = set()
    for _, flds in masks:
        for f in flds:
            if f not in seen:
                seen.add(f)
                all_fields.append(f)

    # 3. Получаем типы полей из source-таблицы для точного DDL
    col_defs = await _get_col_definitions(engine, table_name, all_fields)
    field_ddl = "\n".join(
        f"    {col_defs.get(f, f'`{f}` VARCHAR(255) NULL')}," for f in all_fields
    )

    # 4. Создаём таблицу индексов, если не существует
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{indexes_table}` (
                `id`         BIGINT   NOT NULL AUTO_INCREMENT,
                `mask_id`    INT      NOT NULL,
                {field_ddl}
                `date_added` DATETIME NULL,
                `combo_hash` CHAR(32) NOT NULL DEFAULT '',
                PRIMARY KEY (`id`),
                UNIQUE KEY  `uk_combo`     (`combo_hash`),
                INDEX       `idx_mask_id`  (`mask_id`),
                INDEX       `idx_date`     (`date_added`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='Indexed dataset combinations for {table_name}'
        """))

    # При полном перестрое — очищаем таблицу
    if date is None:
        async with engine.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{indexes_table}`"))
        logger.info("parse_indexes: %s очищена (полный перестрой)", indexes_table)

    # 5. Наполняем — по одной маске за раз
    date_filter = f"AND `{date_field}` >= :from_date" if date else ""
    date_params = {"from_date": date} if date else {}

    total_inserted = 0
    all_insert_cols = ["mask_id"] + all_fields + ["date_added", "combo_hash"]
    col_str = ", ".join(f"`{c}`" for c in all_insert_cols)
    val_str = ", ".join(f":{c}" for c in all_insert_cols)

    for mask_id, mask_fields in masks:
        if not mask_fields:
            continue

        sel_cols   = ", ".join(f"`{f}`" for f in mask_fields)
        group_cols = sel_cols  # GROUP BY те же поля

        try:
            async with engine.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT {sel_cols},
                           MIN(`{date_field}`) AS _date_added
                    FROM   `{table_name}`
                    WHERE  1=1 {date_filter}
                    GROUP  BY {group_cols}
                    ORDER  BY _date_added
                """), date_params)
                source_rows = res.fetchall()
        except Exception as exc:
            logger.error(
                "parse_indexes: SELECT для mask_id=%d не удался: %s", mask_id, exc
            )
            continue

        if not source_rows:
            continue

        insert_rows: list[dict] = []
        for row in source_rows:
            field_values = tuple(row[i] for i in range(len(mask_fields)))
            date_added   = row[-1]  # _date_added — последний столбец

            record: dict = {
                "mask_id":    mask_id,
                "date_added": date_added,
                "combo_hash": _combo_hash(mask_id, field_values),
            }
            # Заполняем поля текущей маски
            for i, f in enumerate(mask_fields):
                record[f] = field_values[i]
            # Поля других масок, которых нет в текущей → NULL
            for f in all_fields:
                record.setdefault(f, None)

            insert_rows.append(record)

        if not insert_rows:
            continue

        try:
            async with engine.begin() as conn:
                r = await conn.execute(text(f"""
                    INSERT IGNORE INTO `{indexes_table}` ({col_str})
                    VALUES ({val_str})
                """), insert_rows)
                total_inserted += r.rowcount
        except Exception as exc:
            logger.error(
                "parse_indexes: INSERT для mask_id=%d не удался: %s", mask_id, exc
            )
            continue

    logger.info(
        "parse_indexes: %s — добавлено %d записей (режим: %s)",
        indexes_table,
        total_inserted,
        f"incremental from {date}" if date else "full rebuild",
    )
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ФУНКЦИЯ 4 — index_parser (оркестратор)
# ══════════════════════════════════════════════════════════════════════════════

async def index_parser(
    engine_vlad,
    engine_brain,
    parser_id: int,
    date: Optional[datetime] = None,
) -> dict:
    """
    Оркестратор: читает конфиг парсера из brain_parsers и последовательно вызывает
    parse_fields → build_indexes → parse_indexes.

    Ожидаемая схема brain_parsers:
        id          INT
        table_name  VARCHAR   — имя пром-таблицы (напр. 'vlad_test')
        date_field  VARCHAR   — поле с датой в этой таблице (напр. 'date')
        engine_name VARCHAR   — 'vlad' или 'brain' (по умолчанию 'vlad')

    Параметр date:
        None         → полный перестрой индексов (parse_indexes в режиме TRUNCATE)
        <datetime>   → инкрементальный режим (добавить новые комбо с date_field >= date)

    Возвращает dict со статусом и метаданными.
    """
    # 1. Читаем конфиг парсера
    try:
        async with engine_brain.connect() as conn:
            res = await conn.execute(text("""
                SELECT table_name, date_field, engine_name
                FROM   brain_parsers
                WHERE  id = :pid
                LIMIT  1
            """), {"pid": parser_id})
            row = res.mappings().fetchone()
    except Exception as exc:
        return {"ok": False, "error": f"brain_parsers query failed: {exc}"}

    if row is None:
        return {"ok": False, "error": f"parser_id={parser_id} not found in brain_parsers"}

    table_name  = str(row["table_name"])
    date_field  = str(row["date_field"])
    engine_name = str(row.get("engine_name") or "vlad").lower()

    data_engine = engine_brain if engine_name == "brain" else engine_vlad

    logger.info(
        "index_parser: parser_id=%d table=%s date_field=%s engine=%s mode=%s",
        parser_id, table_name, date_field, engine_name,
        f"incremental from {date}" if date else "full rebuild",
    )

    # 2. parse_fields — определяем, какие поля годятся для индекса
    fields = await parse_fields(data_engine, table_name)
    if not fields:
        return {
            "ok":         False,
            "parser_id":  parser_id,
            "table_name": table_name,
            "fields":     [],
            "reason":     "no indexable fields found (criteria: distinct > 100 AND count > distinct * 10)",
        }

    # 3. build_indexes — создаём/перезаписываем таблицу масок
    if not await build_indexes(engine_vlad, table_name, fields):
        return {
            "ok":         False,
            "parser_id":  parser_id,
            "table_name": table_name,
            "fields":     fields,
            "reason":     "build_indexes returned False",
        }

    masks_count = 1 + len(fields) if len(fields) > 1 else 1  # full + C(N,N-1)

    # 4. parse_indexes — наполняем таблицу проиндексированных данных
    ok = await parse_indexes(engine_vlad, table_name, date_field, date)

    return {
        "ok":             ok,
        "parser_id":      parser_id,
        "table_name":     table_name,
        "date_field":     date_field,
        "engine":         engine_name,
        "fields":         fields,
        "masks_count":    masks_count,
        "mask_table":     f"{table_name}_mask",
        "indexes_table":  f"{table_name}_indexes",
        "mode":           "incremental" if date else "full_rebuild",
        "from_date":      date.isoformat() if date else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ИНТЕГРАЦИЯ В BRAIN_FRAMEWORK
# ══════════════════════════════════════════════════════════════════════════════
#
# В brain_framework.py добавить после импортов:
#
#     from dataset_indexer import index_parser, parse_indexes
#
# Внутри build_app(), перед «return app», добавить два endpoint-а:
#
#
# @app.get("/index_dataset")
# async def ep_index_dataset(
#     parser_id: int = Query(..., description="ID парсера из brain_parsers"),
#     date_from:  str = Query("", description="Инкрементальный режим: от YYYY-MM-DD"),
# ):
#     """
#     Запускает полный цикл индексирования для указанного парсера.
#
#     GET /index_dataset?parser_id=7              — полный перестрой
#     GET /index_dataset?parser_id=7&date_from=2025-05-01  — дешёвый «подтяг»
#     """
#     from_dt = _parse_date(date_from) if date_from.strip() else None
#     try:
#         result = await index_parser(s.engine_vlad, s.engine_brain, parser_id, from_dt)
#         if result.get("ok"):
#             return ok_response(result)
#         return err_response(result.get("reason") or result.get("error") or "unknown error")
#     except Exception as e:
#         send_error_trace(e, s.NODE_NAME, "index_dataset")
#         return err_response(str(e))
#
#
# @app.get("/index_dataset_refresh")
# async def ep_index_dataset_refresh(
#     parser_id: int  = Query(...),
#     hours_back: int = Query(48, description="Инкрементальный горизонт в часах"),
# ):
#     """
#     Быстрый incremental-refresh: добавляет индексы за последние N часов.
#     Вызывается периодически из PHP или планировщика.
#     """
#     from datetime import timedelta
#     from_dt = datetime.now() - timedelta(hours=hours_back)
#     try:
#         result = await index_parser(s.engine_vlad, s.engine_brain, parser_id, from_dt)
#         return ok_response(result) if result.get("ok") else err_response(result.get("reason", ""))
#     except Exception as e:
#         return err_response(str(e))
