"""
brain_news_context_idx.py
Шаг 1: Строит таблицу vlad_news_context_idx из NER-таблиц и brain_all_news.
Логика: консенсус по 3 NER-моделям (>=2/3) → fingerprint → агрегация
"""

import os
import hashlib
from collections import Counter, defaultdict
from datetime import datetime

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# --- Конфиг -------------------------------------------------------------------
CTX_TABLE   = os.getenv("CTX_TABLE",   "vlad_news_context_idx")
BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "2000"))
MIN_MODELS  = 2          # сущность попадает в fingerprint если встречается в >= MIN_MODELS моделях
MIN_OCCURRENCE = 2       # минимум появлений контекста для включения в таблицу

NER_SOURCES = [
    ("brain", "brain_cnn_ner",  "brain_cnn_news"),
    ("brain", "brain_nyt_ner",  "brain_nyt_news"),
    ("brain", "brain_twp_ner",  "brain_twp_news"),
    ("brain", "brain_tgd_ner",  "brain_tgd_news"),
    ("brain", "brain_wsj_ner",  "brain_wsj_news"),
]

# Маппинг feed → категория: F=Financial, P=Political, T=Tech, W=World, G=General
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

# DDL
DDL = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  `id`               INT            NOT NULL AUTO_INCREMENT,
  `feed_cat`         CHAR(1)        NOT NULL DEFAULT 'G',
  `person_token`     VARCHAR(64)    NOT NULL DEFAULT '',
  `location_token`   VARCHAR(64)    NOT NULL DEFAULT '',
  `misc_token`       VARCHAR(64)    NOT NULL DEFAULT '',
  `fingerprint_hash` CHAR(16)       NOT NULL DEFAULT '',
  `occurrence_count` INT            NOT NULL DEFAULT 0,
  `first_dt`         DATETIME       NULL,
  `last_dt`          DATETIME       NULL,
  `updated_at`       TIMESTAMP      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_fingerprint` (`fingerprint_hash`),
  INDEX idx_feed_cat      (`feed_cat`),
  INDEX idx_person_token  (`person_token`),
  INDEX idx_location_token(`location_token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# NEWS_CTX таблица: news_id → ctx_id (для быстрого RAM-поиска в сервере)
NEWS_CTX_DDL = """
CREATE TABLE IF NOT EXISTS `vlad_news_ctx_map` (
  `news_id`   BIGINT NOT NULL,
  `ctx_id`    INT    NOT NULL,
  `news_date` DATETIME NULL,
  PRIMARY KEY (`news_id`),
  INDEX idx_ctx_id   (`ctx_id`),
  INDEX idx_news_date(`news_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# --- DB -----------------------------------------------------------------------

def get_brain_conn():
    return mysql.connector.connect(
        host=os.getenv("MASTER_HOST", "localhost"),
        port=int(os.getenv("MASTER_PORT", "3307")),
        user=os.getenv("MASTER_USER", "root"),
        password=os.getenv("MASTER_PASSWORD", ""),
        database=os.getenv("MASTER_NAME", "rss_db"),
        autocommit=False,
        charset="utf8mb4",
    )

def get_vlad_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "vlad"),
        autocommit=False,
        charset="utf8mb4",
    )


# --- NER обработка ------------------------------------------------------------

def get_feed_cat(feed: str) -> str:
    """Определяет категорию по полю feed."""
    feed_lower = (feed or "").lower()
    for key, cat in FEED_CAT_MAP.items():
        if key in feed_lower:
            return cat
    return "G"


def normalize_token(raw: str) -> str:
    """
    Нормализует строку сущности: берёт первые 2 слова, lowercase, strip.
    Цель: 'wall street white house' → 'wall street'
    """
    if not raw:
        return ""
    words = raw.strip().lower().split()
    return " ".join(words[:2])[:64]


def consensus_entities(ner_rows: list) -> dict:
    """
    Принимает список NER-записей для одного news_id (от разных моделей).
    Возвращает сущности с консенсусом >= MIN_MODELS.

    Считаем "токены" после split+lower, затем голосуем по каждому слову.
    Берём самый частотный консенсусный токен каждого типа.
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

    threshold = MIN_MODELS
    p_words  = [k for k, v in persons.items()   if v >= threshold]
    l_words  = [k for k, v in locations.items() if v >= threshold]
    m_words  = [k for k, v in miscs.items()     if v >= threshold]

    # Берём топ-2 слова, сортируем для детерминизма
    def top2(words):
        return " ".join(sorted(words)[:2])

    return {
        "person":   top2(p_words),
        "location": top2(l_words),
        "misc":     top2(m_words),
    }


def make_fingerprint(feed_cat: str, person: str, location: str, misc: str) -> str:
    """MD5 первые 16 символов — уникальный ключ контекста."""
    raw = f"{feed_cat}|{person}|{location}|{misc}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# --- Загрузка NER-данных ------------------------------------------------------

def load_ner_for_source(brain_conn, ner_table: str, news_table: str) -> dict:
    """
    Загружает NER-данные из одной пары таблиц.
    Возвращает словарь: news_id → [ner_row, ...]
    """
    print(f"  Загрузка {ner_table} + {news_table}...")
    cur = brain_conn.cursor(dictionary=True)

    cur.execute(f"""
        SELECT n.news_id, n.person, n.location, n.misc,
               a.date, a.feed
        FROM `{ner_table}` n
        JOIN `{news_table}` a ON a.id = n.news_id
        WHERE a.date IS NOT NULL
    """)
    rows = cur.fetchall()
    cur.close()

    # Группируем по news_id
    by_news = defaultdict(list)
    for row in rows:
        by_news[row["news_id"]].append(row)

    print(f"    Загружено: {len(rows)} NER-строк по {len(by_news)} новостям")
    return by_news


def build_fingerprint_map(ner_by_news: dict) -> list:
    """
    Принимает словарь {news_id: [ner_rows]}.
    Возвращает список [(news_id, news_date, fingerprint_key, feed_cat, person, loc, misc)].
    """
    result = []
    for news_id, rows in ner_by_news.items():
        if not rows:
            continue
        # Берём date и feed из первой записи (одинаковы для всех моделей)
        news_date = rows[0].get("date")
        feed_cat  = get_feed_cat(rows[0].get("feed", ""))

        ents = consensus_entities(rows)
        fp   = make_fingerprint(feed_cat, ents["person"], ents["location"], ents["misc"])

        result.append({
            "news_id":  news_id,
            "date":     news_date,
            "fp":       fp,
            "feed_cat": feed_cat,
            "person":   normalize_token(ents["person"]),
            "location": normalize_token(ents["location"]),
            "misc":     normalize_token(ents["misc"]),
        })
    return result


# --- Агрегация ----------------------------------------------------------------

def aggregate_contexts(fp_records: list) -> dict:
    """
    Группирует fingerprint-записи в контексты.
    Возвращает: {fp_hash: {feed_cat, person, location, misc, count, first_dt, last_dt}}
    """
    ctx_map = {}
    for rec in fp_records:
        fp = rec["fp"]
        if fp not in ctx_map:
            ctx_map[fp] = {
                "feed_cat":  rec["feed_cat"],
                "person":    rec["person"],
                "location":  rec["location"],
                "misc":      rec["misc"],
                "count":     0,
                "first_dt":  rec["date"],
                "last_dt":   rec["date"],
                "news_ids":  [],
            }
        ctx = ctx_map[fp]
        ctx["count"] += 1
        if rec["date"]:
            if ctx["first_dt"] is None or rec["date"] < ctx["first_dt"]:
                ctx["first_dt"] = rec["date"]
            if ctx["last_dt"] is None or rec["date"] > ctx["last_dt"]:
                ctx["last_dt"] = rec["date"]
        ctx["news_ids"].append((rec["news_id"], rec["date"]))
    return ctx_map


# --- main ---------------------------------------------------------------------

def main():
    brain_conn = get_brain_conn()
    vlad_conn  = get_vlad_conn()

    try:
        vcur = vlad_conn.cursor()

        print("Создание таблиц...")
        vcur.execute(DDL)
        vcur.execute(NEWS_CTX_DDL)
        vlad_conn.commit()

        # Собираем NER со всех источников
        all_fp_records = []
        for db_name, ner_table, news_table in NER_SOURCES:
            conn = brain_conn  # всё в brain DB
            try:
                by_news = load_ner_for_source(conn, ner_table, news_table)
                fp_recs = build_fingerprint_map(by_news)
                all_fp_records.extend(fp_recs)
                print(f"    {ner_table}: {len(fp_recs)} fingerprint-записей")
            except Exception as e:
                print(f"  Пропуск {ner_table}: {e}")

        print(f"\nВсего NER-fingerprint записей: {len(all_fp_records)}")

        # Дедупликация по news_id (один и тот же news_id из нескольких источников)
        seen_news = {}
        deduped = []
        for rec in all_fp_records:
            if rec["news_id"] not in seen_news:
                seen_news[rec["news_id"]] = rec
                deduped.append(rec)
        print(f"После дедупликации: {len(deduped)}")

        # Агрегируем контексты
        ctx_map = aggregate_contexts(deduped)
        print(f"Уникальных контекстов: {len(ctx_map)}")

        # Фильтруем по минимальному числу появлений
        ctx_filtered = {fp: ctx for fp, ctx in ctx_map.items()
                        if ctx["count"] >= MIN_OCCURRENCE}
        print(f"Контекстов после фильтра (>= {MIN_OCCURRENCE}): {len(ctx_filtered)}")

        # Записываем context_idx
        print("\nЗапись в vlad_news_context_idx...")
        vcur.execute(f"TRUNCATE TABLE `{CTX_TABLE}`;")
        vlad_conn.commit()

        ctx_insert = f"""
        INSERT INTO `{CTX_TABLE}`
          (feed_cat, person_token, location_token, misc_token,
           fingerprint_hash, occurrence_count, first_dt, last_dt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch = []
        for fp, ctx in ctx_filtered.items():
            batch.append((
                ctx["feed_cat"], ctx["person"], ctx["location"], ctx["misc"],
                fp, ctx["count"], ctx["first_dt"], ctx["last_dt"],
            ))
            if len(batch) >= BATCH_SIZE:
                vcur.executemany(ctx_insert, batch)
                vlad_conn.commit()
                batch.clear()
        if batch:
            vcur.executemany(ctx_insert, batch)
            vlad_conn.commit()

        # Читаем обратно ctx_id для маппинга news → ctx
        vcur.execute(f"SELECT id, fingerprint_hash FROM `{CTX_TABLE}`")
        fp_to_id = {row[1]: row[0] for row in vcur.fetchall()}

        # Записываем news_ctx_map
        print("Запись в vlad_news_ctx_map...")
        vcur.execute("TRUNCATE TABLE `vlad_news_ctx_map`;")
        vlad_conn.commit()

        map_insert = "INSERT INTO `vlad_news_ctx_map` (news_id, ctx_id, news_date) VALUES (%s, %s, %s)"
        batch = []
        for rec in deduped:
            ctx_id = fp_to_id.get(rec["fp"])
            if ctx_id:
                batch.append((rec["news_id"], ctx_id, rec["date"]))
                if len(batch) >= BATCH_SIZE:
                    vcur.executemany(map_insert, batch)
                    vlad_conn.commit()
                    batch.clear()
        if batch:
            vcur.executemany(map_insert, batch)
            vlad_conn.commit()

        vcur.execute(f"SELECT COUNT(*) FROM `{CTX_TABLE}`")
        print(f"\nOK: context_idx rows = {vcur.fetchone()[0]}")
        vcur.execute("SELECT COUNT(*) FROM `vlad_news_ctx_map`")
        print(f"OK: news_ctx_map rows = {vcur.fetchone()[0]}")

        # Топ-10 контекстов
        print("\n-- Топ-10 контекстов по частоте --")
        vcur.execute(f"""
            SELECT id, feed_cat, person_token, location_token, misc_token, occurrence_count
            FROM `{CTX_TABLE}`
            ORDER BY occurrence_count DESC LIMIT 10
        """)
        for row in vcur.fetchall():
            print(f"  ctx={row[0]:5} | cat={row[1]} | person={row[2]:<20} | loc={row[3]:<20} | misc={row[4]:<20} | n={row[5]}")

        vcur.close()
        print("\nГотово.")

    finally:
        brain_conn.close()
        vlad_conn.close()


if __name__ == "__main__":
    main()
