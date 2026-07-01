"""
news_index_updater.py
Инкрементальное обновление индекса NER-контекстов и весовых кодов.

Цель: сделать таблицы vlad_news_context_idx и vlad_news_weights_table
      динамическими — они должны обновляться по мере поступления новых новостей,
      а не только при полной пересборке (context_idx.py + weights.py).

Вызывается из фонового reload сервера (каждые 3600 сек) — через run_incremental_update()
"""

import os
import hashlib
import logging
from collections import Counter, defaultdict
from typing import Optional

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# 
# КОНФИГУРАЦИЯ
# 

CTX_TABLE   = os.getenv("CTX_TABLE",  "vlad_news_context_idx")
MAP_TABLE   = "vlad_news_ctx_map"
WGT_TABLE   = os.getenv("OUT_TABLE",  "vlad_news_weights_table")
BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "2000"))
SHIFT_MAX   = int(os.getenv("SHIFT_MAX",  "24"))
MIN_MODELS  = 2   # порог консенсуса NER (из 5 источников)
RECURRING_THRESHOLD = 2  # начиная с этого count → генерируем все сдвиги

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

log = logging.getLogger("news_updater")


# 
# DB СОЕДИНЕНИЯ
# 

def get_brain_conn():
    """
    Соединение с brain БД (источник NER-данных).

    @return mysql.connector.connection
    """
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
    """
    Соединение с vlad БД (целевые таблицы ctx_idx, ctx_map, weights).

    @return mysql.connector.connection
    """
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "vlad"),
        autocommit=False,
        charset="utf8mb4",
    )


# 
# УТИЛИТЫ 
# 

def get_feed_cat(feed: str) -> str:
    """
    Определяет категорию источника по полю feed.
    """
    feed_lower = (feed or "").lower()
    for key, cat in FEED_CAT_MAP.items():
        if key in feed_lower:
            return cat
    return "G"


def normalize_token(raw: str) -> str:
    """
    Нормализует строку NER-сущности: первые 2 слова, lowercase, до 64 символов.
    """
    if not raw:
        return ""
    words = raw.strip().lower().split()
    return " ".join(words[:2])[:64]


def consensus_entities(ner_rows: list) -> dict:
    """
    Строит NER-консенсус из записей нескольких моделей для одной новости.
    Сущность включается если встречается в >= MIN_MODELS строках.
    """
    persons = Counter()
    locations = Counter()
    miscs = Counter()

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

    def top2(counter):
        words = [k for k, v in counter.items() if v >= MIN_MODELS]
        return " ".join(sorted(words)[:2])

    return {
        "person":   top2(persons),
        "location": top2(locations),
        "misc":     top2(miscs),
    }


def make_fingerprint(feed_cat: str, person: str, location: str, misc: str) -> str:
    """
    Создаёт 16-символьный MD5-хэш контекста (уникальный ключ fingerprint).

    """
    raw = f"{feed_cat}|{person}|{location}|{misc}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def make_weight_code(ctx_id: int, mode: int, shift: int) -> str:
    """
    Формирует строковый код веса в формате NW{ctx_id}_{mode}_{shift}.
    """
    return f"NW{ctx_id}_{mode}_{shift}"


# 
# ШАГ 1: НАЙТИ НОВЫЕ NEWS_ID
# 

def find_new_news_ids(brain_conn, vlad_conn) -> set:
    """
    Возвращает множество news_id, которые есть в NER-таблицах brain,
    но ещё отсутствуют в vlad_news_ctx_map (то есть ещё не обработаны).

    Логика: LEFT JOIN ctx_map → WHERE ctx_map.news_id IS NULL
    """
    # Читаем уже обработанные news_id из ctx_map
    vcur = vlad_conn.cursor()
    vcur.execute(f"SELECT news_id FROM `{MAP_TABLE}`")
    already_processed = {row[0] for row in vcur.fetchall()}
    vcur.close()
    log.info(f"  Уже в ctx_map: {len(already_processed)} новостей")

    # Собираем все news_id из NER-источников
    new_ids = set()
    bcur = brain_conn.cursor()
    for ner_table, news_table in NER_SOURCES:
        try:
            bcur.execute(f"SELECT DISTINCT news_id FROM `{ner_table}`")
            source_ids = {row[0] for row in bcur.fetchall()}
            new_for_source = source_ids - already_processed
            new_ids |= new_for_source
            log.info(f"  {ner_table}: всего {len(source_ids)}, новых {len(new_for_source)}")
        except Exception as e:
            log.warning(f"  Пропуск {ner_table}: {e}")
    bcur.close()

    log.info(f"  Итого новых news_id для обработки: {len(new_ids)}")
    return new_ids


# 
# ШАГ 2: ЗАГРУЗИТЬ NER ДЛЯ НОВЫХ NEWS_ID
# 

def load_ner_for_new_ids(brain_conn, new_ids: set) -> dict:
    """
    Загружает NER-данные из всех источников только для заданных news_id.
    Возвращает словарь news_id → [ner_row, ...] для построения fingerprints.
    """
    if not new_ids:
        return {}

    by_news = defaultdict(list)
    bcur = brain_conn.cursor(dictionary=True)

    # Разбиваем new_ids на чанки для IN (...) — MySQL лимит
    id_list = list(new_ids)
    chunk_size = 500

    for ner_table, news_table in NER_SOURCES:
        try:
            for i in range(0, len(id_list), chunk_size):
                chunk = id_list[i: i + chunk_size]
                placeholders = ",".join(["%s"] * len(chunk))
                bcur.execute(f"""
                    SELECT n.news_id, n.person, n.location, n.misc,
                           a.date, a.feed
                    FROM `{ner_table}` n
                    JOIN `{news_table}` a ON a.id = n.news_id
                    WHERE n.news_id IN ({placeholders})
                      AND a.date IS NOT NULL
                """, chunk)
                for row in bcur.fetchall():
                    by_news[row["news_id"]].append(row)
        except Exception as e:
            log.warning(f"  Ошибка при загрузке {ner_table}: {e}")

    bcur.close()
    log.info(f"  NER загружен для {len(by_news)} из {len(new_ids)} news_id")
    return dict(by_news)


# 
# ШАГ 3: ПОСТРОИТЬ FINGERPRINTS
# 

def build_fp_records(ner_by_news: dict) -> list:
    """
    Для каждого news_id строит fingerprint на основе NER-консенсуса.

    @param ner_by_news  dict   {news_id: [ner_rows]}
    @return list               Список dict:
                               {news_id, date, fp (hex16), feed_cat,
                                person, location, misc}
    """
    result = []
    for news_id, rows in ner_by_news.items():
        if not rows:
            continue
        news_date = rows[0].get("date")
        feed_cat  = get_feed_cat(rows[0].get("feed", ""))
        ents = consensus_entities(rows)
        fp   = make_fingerprint(
            feed_cat, ents["person"], ents["location"], ents["misc"]
        )
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


# 
# ШАГ 4: UPSERT В vlad_news_context_idx
# 

def upsert_context_idx(vlad_conn, fp_records: list) -> dict:
    """
    Для каждого уникального fingerprint:
      - Если новый → INSERT (occurrence_count = кол-во вхождений в fp_records)
      - Если существующий → UPDATE occurrence_count += N, last_dt

    Возвращает актуальный маппинг {fp_hash: ctx_id} для дальнейшего использования.
    """
    if not fp_records:
        return {}

    # Группируем fp_records по fingerprint
    by_fp = defaultdict(list)
    for rec in fp_records:
        by_fp[rec["fp"]].append(rec)

    vcur = vlad_conn.cursor()

    # Читаем существующие fingerprints → id
    vcur.execute(f"SELECT id, fingerprint_hash, occurrence_count FROM `{CTX_TABLE}`")
    existing = {row[1]: (row[0], row[2]) for row in vcur.fetchall()}
    # existing: {fp_hash: (ctx_id, current_count)}

    inserted = 0
    updated  = 0

    for fp_hash, recs in by_fp.items():
        n = len(recs)  # сколько новых событий с этим fingerprint
        dates = [r["date"] for r in recs if r["date"]]
        last_dt = max(dates) if dates else None

        if fp_hash not in existing:
            # Новый контекст — INSERT
            sample = recs[0]
            vcur.execute(f"""
                INSERT INTO `{CTX_TABLE}`
                  (feed_cat, person_token, location_token, misc_token,
                   fingerprint_hash, occurrence_count, first_dt, last_dt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  occurrence_count = occurrence_count + VALUES(occurrence_count),
                  last_dt = GREATEST(COALESCE(last_dt, VALUES(last_dt)), VALUES(last_dt))
            """, (
                sample["feed_cat"], sample["person"],
                sample["location"], sample["misc"],
                fp_hash, n,
                min(dates) if dates else None,
                last_dt,
            ))
            inserted += 1
        else:
            # Существующий — UPDATE count и last_dt
            ctx_id, _ = existing[fp_hash]
            vcur.execute(f"""
                UPDATE `{CTX_TABLE}`
                SET occurrence_count = occurrence_count + %s,
                    last_dt = GREATEST(COALESCE(last_dt, %s), %s)
                WHERE id = %s
            """, (n, last_dt, last_dt, ctx_id))
            updated += 1

    vlad_conn.commit()
    log.info(f"  ctx_idx: +{inserted} новых, ~{updated} обновлено")

    # Читаем обновлённый маппинг fp → ctx_id
    vcur.execute(f"SELECT id, fingerprint_hash FROM `{CTX_TABLE}`")
    fp_to_id = {row[1]: row[0] for row in vcur.fetchall()}
    vcur.close()

    return fp_to_id


# 
# ШАГ 5: ДОБАВИТЬ СТРОКИ В vlad_news_ctx_map
# 

def insert_ctx_map(vlad_conn, fp_records: list, fp_to_id: dict):
    """
    Добавляет новые строки в vlad_news_ctx_map (news_id → ctx_id).
    Использует INSERT IGNORE — не затрагивает уже существующие записи.
    """
    vcur = vlad_conn.cursor()
    sql  = f"INSERT IGNORE INTO `{MAP_TABLE}` (news_id, ctx_id, news_date) VALUES (%s, %s, %s)"

    batch = []
    skipped = 0
    for rec in fp_records:
        ctx_id = fp_to_id.get(rec["fp"])
        if ctx_id is None:
            # fingerprint не прошёл фильтр MIN_OCCURRENCE — пропускаем
            skipped += 1
            continue
        batch.append((rec["news_id"], ctx_id, rec["date"]))
        if len(batch) >= BATCH_SIZE:
            vcur.executemany(sql, batch)
            vlad_conn.commit()
            batch.clear()

    if batch:
        vcur.executemany(sql, batch)
        vlad_conn.commit()

    vcur.close()
    log.info(f"  ctx_map: +{len(fp_records) - skipped} строк добавлено, {skipped} пропущено (нет ctx_id)")


# 
# ШАГ 6: ДОПОЛНИТЬ vlad_news_weights_table
# 

def update_weight_codes(vlad_conn):
    """
    Дополняет vlad_news_weights_table для двух случаев:

    Случай A — новые ctx_id (вообще нет кодов в weights_table):
      Если count < RECURRING_THRESHOLD → генерировать только shift=0 (mode 0 и 1)
      Если count >= RECURRING_THRESHOLD → генерировать shift 0..SHIFT_MAX (mode 0 и 1)

    Случай B — ctx_id перешли порог recurring (были одиночными, стали регулярными):
      Уже есть shift=0, надо дописать shift 1..SHIFT_MAX для обоих mode.

    Использует INSERT IGNORE — безопасен для повторного запуска.
    """
    vcur = vlad_conn.cursor()

    # Читаем текущее состояние ctx_idx
    vcur.execute(f"SELECT id, occurrence_count FROM `{CTX_TABLE}`")
    all_ctx = {row[0]: row[1] for row in vcur.fetchall()}
    # all_ctx: {ctx_id: occurrence_count}

    # Читаем уже существующие коды: {ctx_id → set(shift для mode=0)}
    # Нас интересует: какие ctx_id вообще есть и какой max shift у них
    vcur.execute(f"SELECT ctx_id, mode, shift FROM `{WGT_TABLE}`")
    existing_codes = defaultdict(lambda: defaultdict(set))
    for ctx_id, mode, shift in vcur.fetchall():
        existing_codes[ctx_id][mode].add(shift)

    sql = f"INSERT IGNORE INTO `{WGT_TABLE}` (weight_code, ctx_id, mode, shift) VALUES (%s, %s, %s, %s)"
    batch = []
    new_codes = 0
    upgraded  = 0

    for ctx_id, occ_count in all_ctx.items():
        is_recurring = occ_count >= RECURRING_THRESHOLD
        max_shift    = SHIFT_MAX if is_recurring else 0

        for mode in (0, 1):
            existing_shifts = existing_codes[ctx_id][mode]

            # Генерируем нужные сдвиги
            needed_shifts = set(range(0, max_shift + 1))
            missing = needed_shifts - existing_shifts

            for shift in missing:
                wc = make_weight_code(ctx_id, mode, shift)
                batch.append((wc, ctx_id, mode, shift))
                if shift == 0:
                    new_codes += 1
                else:
                    upgraded += 1

                if len(batch) >= BATCH_SIZE:
                    vcur.executemany(sql, batch)
                    vlad_conn.commit()
                    batch.clear()

    if batch:
        vcur.executemany(sql, batch)
        vlad_conn.commit()

    vcur.close()
    log.info(f"  weights: +{new_codes} новых (shift=0), +{upgraded} upgraded (shift 1..{SHIFT_MAX} для recurring)")


# 
# ГЛАВНАЯ ФУНКЦИЯ
# 

def run_incremental_update(
    brain_conn=None,
    vlad_conn=None,
    close_after: bool = True,
) -> dict:
    """
    Запускает полный инкрементальный цикл обновления индекса.
    """
    _own_connections = brain_conn is None

    if brain_conn is None:
        brain_conn = get_brain_conn()
    if vlad_conn is None:
        vlad_conn = get_vlad_conn()

    log.info("=== Инкрементальное обновление новостного индекса ===")

    try:
        # Шаг 1: найти новые news_id
        new_ids = find_new_news_ids(brain_conn, vlad_conn)
        if not new_ids:
            log.info("  Новых новостей нет — обновление не требуется.")
            return {"new_news": 0, "new_ctx": 0, "new_codes": 0}

        # Шаг 2: загрузить NER для новых новостей
        ner_by_news = load_ner_for_new_ids(brain_conn, new_ids)

        # Шаг 3: построить fingerprints
        fp_records = build_fp_records(ner_by_news)
        log.info(f"  Построено fingerprints: {len(fp_records)}")

        # Шаг 4: upsert в ctx_idx
        fp_to_id = upsert_context_idx(vlad_conn, fp_records)

        # Шаг 5: добавить в ctx_map
        insert_ctx_map(vlad_conn, fp_records, fp_to_id)

        # Шаг 6: дополнить weight_codes
        update_weight_codes(vlad_conn)

        log.info("=== Обновление завершено ===")
        return {
            "new_news": len(new_ids),
            "new_ctx":  len(fp_records),
        }

    except Exception as e:
        log.error(f"  ОШИБКА при обновлении: {e}", exc_info=True)
        raise
    finally:
        if close_after or _own_connections:
            try:
                brain_conn.close()
            except Exception:
                pass
            try:
                vlad_conn.close()
            except Exception:
                pass

# 
# ТОЧКА ВХОДА
# 

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    result = run_incremental_update()
    print(f"\nРезультат: {result}")
