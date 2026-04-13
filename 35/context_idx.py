"""
context_idx.py — Сервис 35: построение vlad_news_context_idx + vlad_news_ctx_map.

Два режима запуска:
  1. Самостоятельный скрипт (полная пересборка):
         python context_idx.py
     Читает .env, пересобирает таблицы с нуля (TRUNCATE + INSERT).

  2. Вызов фреймворком (инкрементальное обновление):
         from context_idx import build_index
         stats = await build_index(engine_vlad, engine_brain)
     Обновляет только новые news_id > MAX(news_id) из vlad_news_ctx_map.

Источники: CNN, NYT, TWP, TGD, WSJ (5 NER-моделей)
Консенсус сущностей >= MIN_MODELS моделей.
Fingerprint = MD5(feed_cat | person | location | misc)[:16]
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections import Counter

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════════════════════════════════════

CTX_TABLE  = "vlad_news_context_idx"
MAP_TABLE  = "vlad_news_ctx_map"

NER_SOURCES = [
    ("brain_cnn_ner",  "brain_cnn_news"),
    ("brain_nyt_ner",  "brain_nyt_news"),
    ("brain_twp_ner",  "brain_twp_news"),
    ("brain_tgd_ner",  "brain_tgd_news"),
    ("brain_wsj_ner",  "brain_wsj_news"),
]

FEED_CAT_MAP = {
    "cnn_money":       "F",
    "nyt_business":    "F",
    "wsj":             "F",
    "brain_wsj":       "F",
    "cnn_allpolitics": "P",
    "nyt_politics":    "P",
    "cnn_tech":        "T",
    "nyt_technology":  "T",
    "nyt_world":       "W",
    "tgd":             "W",
    "twp":             "W",
    "cnn_us":          "P",
    "nyt_us":          "P",
    "nyt_main":        "G",
    "cnn_world":       "W",
}

MIN_MODELS     = 2
MIN_OCCURRENCE = 2
BATCH_SIZE     = 2000


# ══════════════════════════════════════════════════════════════════════════════
# DDL
# ══════════════════════════════════════════════════════════════════════════════

_DDL_CTX = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
    `id`               INT         NOT NULL AUTO_INCREMENT,
    `feed_cat`         CHAR(1)     NOT NULL DEFAULT 'G',
    `person_token`     VARCHAR(64) NOT NULL DEFAULT '',
    `location_token`   VARCHAR(64) NOT NULL DEFAULT '',
    `misc_token`       VARCHAR(64) NOT NULL DEFAULT '',
    `fingerprint_hash` CHAR(16)    NOT NULL DEFAULT '',
    `occurrence_count` INT         NOT NULL DEFAULT 0,
    `first_dt`         DATETIME    NULL,
    `last_dt`          DATETIME    NULL,
    `updated_at`       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_fingerprint` (`fingerprint_hash`),
    INDEX idx_feed_cat       (`feed_cat`),
    INDEX idx_person_token   (`person_token`),
    INDEX idx_location_token (`location_token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_DDL_MAP = f"""
CREATE TABLE IF NOT EXISTS `{MAP_TABLE}` (
    `news_id`   BIGINT   NOT NULL,
    `ctx_id`    INT      NOT NULL,
    `news_date` DATETIME NULL,
    PRIMARY KEY (`news_id`),
    INDEX idx_ctx_id    (`ctx_id`),
    INDEX idx_news_date (`news_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


# ══════════════════════════════════════════════════════════════════════════════
# NER-УТИЛИТЫ
# ══════════════════════════════════════════════════════════════════════════════

def _get_feed_cat(feed: str) -> str:
    feed_lower = (feed or "").lower()
    for key, cat in FEED_CAT_MAP.items():
        if key in feed_lower:
            return cat
    return "G"


def _normalize_token(raw: str) -> str:
    if not raw:
        return ""
    return " ".join(raw.strip().lower().split()[:2])[:64]


def _consensus_entities(ner_rows: list[dict]) -> dict:
    """
    Консенсус сущностей из нескольких NER-моделей.
    Сущность включается если встречается в >= MIN_MODELS моделях.
    Возвращает топ-2 слова для каждого типа (детерминировано: sorted).
    """
    persons   = Counter()
    locations = Counter()
    miscs     = Counter()

    for row in ner_rows:
        for token in (row.get("person") or "").lower().split():
            if len(token) > 2:
                persons[token] += 1
        for token in (row.get("location") or "").lower().split():
            if len(token) > 2:
                locations[token] += 1
        for token in (row.get("misc") or "").lower().split():
            if len(token) > 2:
                miscs[token] += 1

    def top2(counter: Counter) -> str:
        words = [k for k, v in counter.items() if v >= MIN_MODELS]
        return " ".join(sorted(words)[:2])

    return {
        "person":   top2(persons),
        "location": top2(locations),
        "misc":     top2(miscs),
    }


def _make_fingerprint(feed_cat: str, person: str, location: str, misc: str) -> str:
    raw = f"{feed_cat}|{person}|{location}|{misc}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════════
# ОБЩАЯ ЛОГИКА (используется обоими режимами)
# ══════════════════════════════════════════════════════════════════════════════

async def _load_ner(engine_brain, max_id: int) -> dict[int, list[dict]]:
    """Загружает NER-данные для news_id > max_id со всех источников."""
    new_ner: dict[int, list[dict]] = {}
    for ner_table, news_table in NER_SOURCES:
        try:
            async with engine_brain.connect() as conn:
                res = await conn.execute(text(f"""
                    SELECT n.news_id, n.person, n.location, n.misc,
                           a.date, a.feed
                    FROM `{ner_table}` n
                    JOIN `{news_table}` a ON a.id = n.news_id
                    WHERE a.date IS NOT NULL
                      AND n.news_id > :max_id
                    ORDER BY n.news_id
                """), {"max_id": max_id})
                for r in res.mappings().all():
                    nid = int(r["news_id"])
                    new_ner.setdefault(nid, []).append({
                        "person":   r.get("person")   or "",
                        "location": r.get("location") or "",
                        "misc":     r.get("misc")     or "",
                        "date":     r["date"],
                        "feed":     r.get("feed")     or "",
                    })
        except Exception as e:
            print(f"  Пропуск {ner_table}: {e}")
    return new_ner


def _build_fp_map(new_ner: dict[int, list[dict]]) -> dict[str, dict]:
    """Строит словарь fingerprint → агрегированные данные."""
    fp_map: dict[str, dict] = {}
    for news_id, ner_rows in new_ner.items():
        if not ner_rows:
            continue
        feed_cat  = _get_feed_cat(ner_rows[0]["feed"])
        news_date = ner_rows[0]["date"]
        ents      = _consensus_entities(ner_rows)
        fp        = _make_fingerprint(
            feed_cat, ents["person"], ents["location"], ents["misc"]
        )
        if fp not in fp_map:
            fp_map[fp] = {
                "feed_cat":   feed_cat,
                "person":     _normalize_token(ents["person"]),
                "location":   _normalize_token(ents["location"]),
                "misc":       _normalize_token(ents["misc"]),
                "count":      0,
                "first_dt":   news_date,
                "last_dt":    news_date,
                "news_items": [],
            }
        d = fp_map[fp]
        d["count"] += 1
        if news_date:
            if d["first_dt"] is None or news_date < d["first_dt"]:
                d["first_dt"] = news_date
            if d["last_dt"]  is None or news_date > d["last_dt"]:
                d["last_dt"]  = news_date
        d["news_items"].append((news_id, news_date))
    return fp_map


async def _upsert_ctx(engine_vlad, fp_map: dict[str, dict]) -> dict[str, int]:
    """UPSERT fingerprints в CTX_TABLE. Возвращает {fp: ctx_id}."""
    async with engine_vlad.begin() as conn:
        for fp, d in fp_map.items():
            await conn.execute(text(f"""
                INSERT INTO `{CTX_TABLE}`
                    (feed_cat, person_token, location_token, misc_token,
                     fingerprint_hash, occurrence_count, first_dt, last_dt)
                VALUES (:fc, :p, :l, :m, :fp, :cnt, :fd, :ld)
                ON DUPLICATE KEY UPDATE
                    occurrence_count = occurrence_count + :cnt,
                    last_dt  = IF(:ld > last_dt  OR last_dt  IS NULL, :ld,  last_dt),
                    first_dt = IF(:fd < first_dt OR first_dt IS NULL, :fd, first_dt)
            """), {
                "fc": d["feed_cat"], "p": d["person"],
                "l":  d["location"], "m": d["misc"],
                "fp": fp,            "cnt": d["count"],
                "fd": d["first_dt"], "ld": d["last_dt"],
            })

    fp_list   = list(fp_map.keys())
    fp_to_id: dict[str, int] = {}
    async with engine_vlad.connect() as conn:
        for i in range(0, len(fp_list), 500):
            batch        = fp_list[i:i + 500]
            placeholders = ", ".join(f":fp{j}" for j in range(len(batch)))
            params       = {f"fp{j}": fp for j, fp in enumerate(batch)}
            res = await conn.execute(text(f"""
                SELECT id, fingerprint_hash FROM `{CTX_TABLE}`
                WHERE fingerprint_hash IN ({placeholders})
            """), params)
            for r in res.fetchall():
                fp_to_id[r[1]] = r[0]
    return fp_to_id


async def _insert_map(engine_vlad, fp_map: dict[str, dict],
                      fp_to_id: dict[str, int]) -> int:
    """INSERT IGNORE news → ctx mapping. Возвращает количество новых строк."""
    new_events = 0
    async with engine_vlad.begin() as conn:
        for fp, d in fp_map.items():
            ctx_id = fp_to_id.get(fp)
            if ctx_id is None:
                continue
            for news_id, news_date in d["news_items"]:
                r = await conn.execute(text(f"""
                    INSERT IGNORE INTO `{MAP_TABLE}` (news_id, ctx_id, news_date)
                    VALUES (:nid, :cid, :nd)
                """), {"nid": news_id, "cid": ctx_id, "nd": news_date})
                new_events += r.rowcount
    return new_events


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 1: вызов фреймворком (инкрементальный)
# ══════════════════════════════════════════════════════════════════════════════

async def build_index(engine_vlad, engine_brain) -> dict:
    """
    Инкрементально обновляет CTX_TABLE и MAP_TABLE.
    Обрабатывает только news_id > MAX(news_id из MAP_TABLE).
    Идемпотентна: безопасно запускать часто.
    """
    await engine_vlad.dispose()
    await engine_brain.dispose()

    # Создаём таблицы если не существуют
    async with engine_vlad.begin() as conn:
        await conn.execute(text(_DDL_CTX))
        await conn.execute(text(_DDL_MAP))

    # Определяем инкремент
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(
            text(f"SELECT MAX(news_id) FROM `{MAP_TABLE}`")
        )).fetchone()
    max_known_id: int = int(row[0]) if row and row[0] else 0

    new_ner = await _load_ner(engine_brain, max_id=max_known_id)
    if not new_ner:
        return {
            "processed_news": 0, "new_contexts": 0,
            "new_events": 0, "max_news_id": max_known_id,
        }

    fp_map   = _build_fp_map(new_ner)
    fp_to_id = await _upsert_ctx(engine_vlad, fp_map)
    new_events = await _insert_map(engine_vlad, fp_map, fp_to_id)

    return {
        "processed_news":   len(new_ner),
        "new_contexts":     len(fp_to_id),
        "new_events":       new_events,
        "prev_max_news_id": max_known_id,
        "new_max_news_id":  max(new_ner.keys()) if new_ner else max_known_id,
    }


# ══════════════════════════════════════════════════════════════════════════════
# РЕЖИМ 2: самостоятельный скрипт (полная пересборка)
# ══════════════════════════════════════════════════════════════════════════════

async def _run_standalone():
    """Полная пересборка с нуля: TRUNCATE + INSERT (не инкрементально)."""

    def _make_engine(url: str):
        return create_async_engine(url, pool_pre_ping=True, echo=False)

    brain_url = (
        f"mysql+aiomysql://{os.getenv('MASTER_USER','root')}:"
        f"{os.getenv('MASTER_PASSWORD','')}@"
        f"{os.getenv('MASTER_HOST','localhost')}:"
        f"{os.getenv('MASTER_PORT','3307')}/"
        f"{os.getenv('MASTER_NAME','rss_db')}?charset=utf8mb4"
    )
    vlad_url = (
        f"mysql+aiomysql://{os.getenv('DB_USER','root')}:"
        f"{os.getenv('DB_PASSWORD','')}@"
        f"{os.getenv('DB_HOST','localhost')}:"
        f"{os.getenv('DB_PORT','3306')}/"
        f"{os.getenv('DB_NAME','vlad')}?charset=utf8mb4"
    )

    engine_brain = _make_engine(brain_url)
    engine_vlad  = _make_engine(vlad_url)

    try:
        # Создаём таблицы
        print("Создание таблиц...")
        async with engine_vlad.begin() as conn:
            await conn.execute(text(_DDL_CTX))
            await conn.execute(text(_DDL_MAP))

        # Загружаем ВСЕ NER-данные (max_id=0)
        print("Загрузка NER-данных со всех источников...")
        new_ner = await _load_ner(engine_brain, max_id=0)
        print(f"  Загружено новостей: {len(new_ner)}")

        fp_map = _build_fp_map(new_ner)
        print(f"  Уникальных fingerprints: {len(fp_map)}")

        # Фильтрация по MIN_OCCURRENCE (только для standalone — полная пересборка)
        fp_filtered = {fp: d for fp, d in fp_map.items()
                       if d["count"] >= MIN_OCCURRENCE}
        print(f"  После фильтра (>= {MIN_OCCURRENCE}): {len(fp_filtered)}")

        # TRUNCATE перед полной пересборкой
        print("Очистка таблиц...")
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))
            await conn.execute(text(f"TRUNCATE TABLE `{MAP_TABLE}`"))

        fp_to_id = await _upsert_ctx(engine_vlad, fp_filtered)
        new_events = await _insert_map(engine_vlad, fp_filtered, fp_to_id)

        # Итоги
        async with engine_vlad.connect() as conn:
            ctx_cnt = (await conn.execute(
                text(f"SELECT COUNT(*) FROM `{CTX_TABLE}`"))).scalar()
            map_cnt = (await conn.execute(
                text(f"SELECT COUNT(*) FROM `{MAP_TABLE}`"))).scalar()

        print(f"\nOK: context_idx rows = {ctx_cnt}")
        print(f"OK: news_ctx_map rows = {map_cnt}")

        # Топ-10 контекстов
        print("\n-- Топ-10 контекстов по частоте --")
        async with engine_vlad.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT id, feed_cat, person_token, location_token,
                       misc_token, occurrence_count
                FROM `{CTX_TABLE}`
                ORDER BY occurrence_count DESC LIMIT 10
            """))
            for row in res.fetchall():
                print(
                    f"  ctx={row[0]:5} | cat={row[1]} "
                    f"| person={row[2]:<20} | loc={row[3]:<20} "
                    f"| misc={row[4]:<20} | n={row[5]}"
                )
        print("\nГотово.")

    finally:
        await engine_brain.dispose()
        await engine_vlad.dispose()


if __name__ == "__main__":
    asyncio.run(_run_standalone())
