"""
model.py — Сервис 36: Веса новостей на основе NER-контекстов.

Sources: CNN, NYT, TWP, TGD, WSJ (5 NER-моделей)
Context = NER fingerprint (feed_cat | person | location | misc)
Consensus >= 2/3 моделей по каждой сущности.

Weight code: NW{ctx_id}_{mode}_{shift}
  mode=0 → T1 sum
  mode=1 → Extremum probability
  shift  → сдвиг в часах (0..24 для recurring)

Критичные переменные (PORT, NODE_NAME, SERVICE_ID, SERVICE_TEXT)
лежат в .env рядом с этим файлом и НЕ дублируются здесь.
"""

from __future__ import annotations

import hashlib
from collections import Counter

from sqlalchemy import text

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГ ДЛЯ ФРЕЙМВОРКА (опционально, добавляй только то, что нужно)
# ══════════════════════════════════════════════════════════════════════════════

WEIGHTS_TABLE   = "vlad_news_weights_table"
CTX_TABLE       = "vlad_news_context_idx"
CTX_KEY_COLUMNS = ["id"]

EVENTS_QUERY = """
    SELECT ctx_id, news_date
    FROM vlad_news_ctx_map
    WHERE news_date IS NOT NULL
    ORDER BY news_date
"""
EVENTS_ENGINE     = "vlad"
EVENT_KEY_COLUMNS = ["ctx_id"]
EVENT_DATE_COLUMN = "news_date"

SHIFT_WINDOW         = 24
RECURRING_MIN_COUNT  = 2
FILTER_FUTURE_EVENTS = True

REBUILD_INTERVAL = 3600   # секунд между автоматическими вызовами rebuild_index

# ── Вариации ─────────────────────────────────────────────────────────────────
#
#   var=0  простая сумма T1 / базовый Extremum
#   var=1  только крупные свечи (range > avg_range)
#   var=2  T1 × |T1| — квадратичное усиление
#   var=3  крупные свечи + квадрат
#   var=4  амплитуда: (range − avg_range) вместо T1

VAR_RANGE = [0, 1, 2, 3, 4]

# ══════════════════════════════════════════════════════════════════════════════
# NER-ПАЙПЛАЙН (специфика этого сервиса, не трогается фреймворком)
# ══════════════════════════════════════════════════════════════════════════════

_NER_SOURCES = [
    ("brain", "brain_cnn_ner",  "brain_cnn_news"),
    ("brain", "brain_nyt_ner",  "brain_nyt_news"),
    ("brain", "brain_twp_ner",  "brain_twp_news"),
    ("brain", "brain_tgd_ner",  "brain_tgd_news"),
    ("brain", "brain_wsj_ner",  "brain_wsj_news"),
]

_FEED_CAT_MAP = {
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

_MIN_MODELS     = 2
_MIN_OCCURRENCE = 2
_SHIFT_MAX      = 24


def _get_feed_cat(feed: str) -> str:
    feed_lower = (feed or "").lower()
    for key, cat in _FEED_CAT_MAP.items():
        if key in feed_lower:
            return cat
    return "G"


def _normalize_token(raw: str) -> str:
    if not raw:
        return ""
    return " ".join(raw.strip().lower().split()[:2])[:64]


def _consensus_entities(ner_rows: list[dict]) -> dict:
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
        words = [k for k, v in counter.items() if v >= _MIN_MODELS]
        return " ".join(sorted(words)[:2])

    return {"person": top2(persons), "location": top2(locations), "misc": top2(miscs)}


def _make_fingerprint(feed_cat: str, person: str, location: str, misc: str) -> str:
    raw = f"{feed_cat}|{person}|{location}|{misc}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════════
# model() — ОСНОВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════════════════════════════════════════

def model(rates, dataset, date, *, type=0, var=0, param="", ctx):
    """
    T1 + Extremum по NER-контекстам новостей.
    Weight code: NW{ctx_id}_{mode}_{shift}
    """
    if not rates:
        return {}

    observations = ctx.find_events()
    if not observations:
        return {}

    result = {}
    prev   = ctx.prev_candle
    du     = ctx.delta_unit

    rates_t1      = ctx.rates_t1
    candle_ranges = ctx.candle_ranges
    avg_range     = ctx.avg_range

    for event_key, obs_dt, shift in observations:
        valid_dts = ctx.event_history(event_key)
        if not valid_dts:
            continue

        t_dates = [d + du * shift for d in valid_dts
                   if (d + du * shift) < date]
        if not t_dates:
            continue

        ctx_id = event_key[0]

        # ── T1 (type=0 или type=1) ────────────────────────────────────────────
        if type in (0, 1):
            if var == 0:
                t1 = ctx.compute_t1(t_dates)
            elif var == 1:
                t1 = sum(
                    rates_t1.get(d, 0.0) for d in t_dates
                    if candle_ranges.get(d, 0.0) > avg_range
                )
            elif var == 2:
                t1 = sum(
                    (v := rates_t1.get(d, 0.0)) * abs(v)
                    for d in t_dates
                )
            elif var == 3:
                t1 = sum(
                    (v := rates_t1.get(d, 0.0)) * abs(v)
                    for d in t_dates
                    if candle_ranges.get(d, 0.0) > avg_range
                )
            elif var == 4:
                t1 = sum(
                    candle_ranges.get(d, 0.0) - avg_range
                    for d in t_dates
                    if candle_ranges.get(d, 0.0) > avg_range
                )
            else:
                t1 = 0.0

            if t1 != 0.0:
                wc = f"NW{ctx_id}_0_{shift}"
                result[wc] = result.get(wc, 0.0) + t1

        # ── Extremum (type=0 или type=2) ──────────────────────────────────────
        if type in (0, 2) and prev is not None:
            _, is_bull = prev
            ext_set    = ctx.extremums["max" if is_bull else "min"]
            total_hist = len(valid_dts)

            if var == 0:
                ext = ctx.compute_extremum(t_dates, is_bull=is_bull, total_hist=total_hist)
            elif var == 1:
                pool = [d for d in t_dates if candle_ranges.get(d, 0.0) > avg_range]
                if pool and total_hist > 0:
                    ext = (
                        (sum(1 for d in pool if d in ext_set) / total_hist) * 2 - 1
                    ) * ctx.modification
                    ext = ext if ext != 0 else None
                else:
                    ext = None
            elif var == 2:
                pool = [d for d in t_dates if d in ext_set]
                if pool and total_hist > 0:
                    v = rates_t1.get(pool[0], 0.0)
                    ext = sum(
                        (v := rates_t1.get(d, 0.0)) * abs(v)
                        for d in pool
                    ) / total_hist * ctx.modification
                    ext = ext if ext != 0 else None
                else:
                    ext = None
            elif var == 3:
                pool = [
                    d for d in t_dates
                    if d in ext_set and candle_ranges.get(d, 0.0) > avg_range
                ]
                if pool and total_hist > 0:
                    ext = (len(pool) / total_hist * 2 - 1) * ctx.modification
                    ext = ext if ext != 0 else None
                else:
                    ext = None
            elif var == 4:
                pool = [d for d in t_dates if d in ext_set]
                ext = sum(
                    candle_ranges.get(d, 0.0) - avg_range
                    for d in pool
                    if candle_ranges.get(d, 0.0) > avg_range
                )
                ext = ext if ext != 0 else None
            else:
                ext = None

            if ext is not None:
                wc = f"NW{ctx_id}_1_{shift}"
                result[wc] = result.get(wc, 0.0) + ext

    return {k: round(v, 6) for k, v in result.items() if v != 0}


# ══════════════════════════════════════════════════════════════════════════════
# rebuild_index() — ИНКРЕМЕНТАЛЬНАЯ ПЕРЕСБОРКА
# ══════════════════════════════════════════════════════════════════════════════

async def rebuild_index(engine_vlad, engine_brain, engine_super=None) -> dict:
    """
    Инкрементально обновляет три таблицы по новым news_id:
      vlad_news_context_idx   — контексты (fingerprints)
      vlad_news_weights_table — weight codes
      vlad_news_ctx_map       — маппинг news_id → ctx_id

    Идемпотентна: безопасно запускать часто.
    Вызывается фреймворком автоматически раз в REBUILD_INTERVAL секунд,
    а также вручную через POST /rebuild_index.
    """
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(
            text("SELECT MAX(news_id) FROM vlad_news_ctx_map")
        )).fetchone()
    max_known_id: int = int(row[0]) if row and row[0] else 0

    # Загружаем новые NER-данные
    new_ner: dict[int, list[dict]] = {}
    for _engine_name, ner_table, news_table in _NER_SOURCES:
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
                """), {"max_id": max_known_id})
                for r in res.mappings().all():
                    nid = int(r["news_id"])
                    new_ner.setdefault(nid, []).append({
                        "person":   r.get("person")   or "",
                        "location": r.get("location") or "",
                        "misc":     r.get("misc")     or "",
                        "date":     r["date"],
                        "feed":     r.get("feed")     or "",
                    })
        except Exception:
            pass

    if not new_ner:
        return {
            "processed_news": 0, "new_contexts": 0,
            "new_weights": 0,    "new_events":   0,
            "max_news_id": max_known_id,
        }

    # Строим fingerprints
    fp_map: dict[str, dict] = {}
    for news_id, ner_rows in new_ner.items():
        if not ner_rows:
            continue
        feed_cat  = _get_feed_cat(ner_rows[0]["feed"])
        news_date = ner_rows[0]["date"]
        ents      = _consensus_entities(ner_rows)
        fp        = _make_fingerprint(feed_cat, ents["person"], ents["location"], ents["misc"])

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

    # UPSERT контексты
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
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
                `updated_at`       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
                                               ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_fingerprint` (`fingerprint_hash`),
                INDEX idx_feed_cat (`feed_cat`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        for fp, d in fp_map.items():
            await conn.execute(text("""
                INSERT INTO `vlad_news_context_idx`
                    (feed_cat, person_token, location_token, misc_token,
                     fingerprint_hash, occurrence_count, first_dt, last_dt)
                VALUES (:fc, :p, :l, :m, :fp, :cnt, :fd, :ld)
                ON DUPLICATE KEY UPDATE
                    occurrence_count = occurrence_count + :cnt,
                    last_dt  = IF(:ld  > last_dt  OR last_dt  IS NULL, :ld,  last_dt),
                    first_dt = IF(:fd  < first_dt OR first_dt IS NULL, :fd, first_dt)
            """), {
                "fc": d["feed_cat"], "p": d["person"],
                "l":  d["location"], "m": d["misc"],
                "fp": fp, "cnt": d["count"],
                "fd": d["first_dt"], "ld": d["last_dt"],
            })

    # Читаем ctx_id для новых fingerprints
    fp_list  = list(fp_map.keys())
    fp_to_id: dict[str, int] = {}
    async with engine_vlad.connect() as conn:
        for i in range(0, len(fp_list), 500):
            batch = fp_list[i:i + 500]
            placeholders = ", ".join(f":fp{j}" for j in range(len(batch)))
            params = {f"fp{j}": fp for j, fp in enumerate(batch)}
            res = await conn.execute(text(f"""
                SELECT id, fingerprint_hash FROM `{CTX_TABLE}`
                WHERE fingerprint_hash IN ({placeholders})
            """), params)
            for r in res.fetchall():
                fp_to_id[r[1]] = r[0]

    # INSERT IGNORE новые weight codes
    new_weights = 0
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{WEIGHTS_TABLE}` (
                `id`          INT         NOT NULL AUTO_INCREMENT,
                `weight_code` VARCHAR(40) NOT NULL,
                `ctx_id`      INT         NOT NULL,
                `mode`        TINYINT     NOT NULL DEFAULT 0,
                `shift`       SMALLINT    NOT NULL DEFAULT 0,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_weight_code` (`weight_code`),
                INDEX idx_ctx_id (`ctx_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        for fp, ctx_id in fp_to_id.items():
            total_count = fp_map.get(fp, {}).get("count", 0)
            max_shift   = _SHIFT_MAX if total_count >= _MIN_OCCURRENCE else 0
            for mode in (0, 1):
                for shift in range(0, max_shift + 1):
                    wc = f"NW{ctx_id}_{mode}_{shift}"
                    r  = await conn.execute(text("""
                        INSERT IGNORE INTO `vlad_news_weights_table`
                            (weight_code, ctx_id, mode, shift)
                        VALUES (:wc, :cid, :mode, :shift)
                    """), {"wc": wc, "cid": ctx_id, "mode": mode, "shift": shift})
                    new_weights += r.rowcount

    # INSERT IGNORE новые события
    new_events = 0
    async with engine_vlad.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `vlad_news_ctx_map` (
                `news_id`   BIGINT   NOT NULL,
                `ctx_id`    INT      NOT NULL,
                `news_date` DATETIME NULL,
                PRIMARY KEY (`news_id`),
                INDEX idx_ctx_id    (`ctx_id`),
                INDEX idx_news_date (`news_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        for fp, d in fp_map.items():
            ctx_id = fp_to_id.get(fp)
            if ctx_id is None:
                continue
            for news_id, news_date in d["news_items"]:
                r = await conn.execute(text("""
                    INSERT IGNORE INTO `vlad_news_ctx_map`
                        (news_id, ctx_id, news_date)
                    VALUES (:nid, :cid, :nd)
                """), {"nid": news_id, "cid": ctx_id, "nd": news_date})
                new_events += r.rowcount

    return {
        "processed_news":   len(new_ner),
        "new_contexts":     len(fp_to_id),
        "new_weights":      new_weights,
        "new_events":       new_events,
        "prev_max_news_id": max_known_id,
        "new_max_news_id":  max(new_ner.keys()) if new_ner else max_known_id,
    }