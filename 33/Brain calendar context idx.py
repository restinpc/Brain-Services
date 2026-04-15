import os
from collections import defaultdict

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

SRC_TABLE  = os.getenv("SRC_TABLE",  "brain_calendar")
CTX_TABLE  = os.getenv("CTX_TABLE",  "brain_calendar_context_idx")

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "5000"))

# Относительный порог для определения направления события
# |(actual - reference) / |reference|| > threshold  →  BEAT / UP
# Если reference == 0: используем абсолютное сравнение знаков
DIRECTION_THRESHOLD = float(os.getenv("DIRECTION_THRESHOLD", "0.01"))

# EventType=2 — праздники/выходные, их пропускаем
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
  `avg_surprise_abs`    DOUBLE         NULL,   -- avg |actual - forecast|
  `avg_revision_abs`    DOUBLE         NULL,   -- avg |previous - oldPrevious|
  `updated_at`          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctx_event_dirs`
    (`event_id`, `currency_code`, `importance`,
     `forecast_dir`, `surprise_dir`, `revision_dir`),
  INDEX idx_ctx_event    (`event_id`),
  INDEX idx_ctx_currency (`currency_code`),
  INDEX idx_ctx_imp      (`importance`),
  INDEX idx_ctx_forecast (`forecast_dir`),
  INDEX idx_ctx_surprise (`surprise_dir`),
  INDEX idx_ctx_revision (`revision_dir`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


#  DB 

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rss_db"),
        autocommit=False,
    )


#  Классификаторы 

def _rel_direction(actual, reference, threshold,
                   up_label="UP", down_label="DOWN", flat_label="FLAT"):
    """Относительное направление actual vs reference с порогом."""
    if actual is None or reference is None:
        return "UNKNOWN"
    if reference == 0:
        # Нет делителя — сравниваем знаки
        if actual > 0:
            return up_label
        if actual < 0:
            return down_label
        return flat_label
    pct = (actual - reference) / abs(reference)
    if pct > threshold:
        return up_label
    if pct < -threshold:
        return down_label
    return flat_label


def classify_event(forecast, previous, old_previous, actual, threshold):
    """
    Возвращает (forecast_dir, surprise_dir, revision_dir).

    forecast_dir  : BEAT / MISS / INLINE / UNKNOWN  — actual vs forecast
    surprise_dir  : UP   / DOWN / FLAT   / UNKNOWN  — actual vs previous
    revision_dir  : UP   / DOWN / FLAT   / NONE      — previous vs oldPrevious
    """
    # actual vs forecast
    if forecast is None or forecast == 0:
        fcd = "UNKNOWN"
    else:
        fcd = _rel_direction(actual, forecast, threshold,
                             up_label="BEAT", down_label="MISS",
                             flat_label="INLINE")

    # actual vs previous
    scd = _rel_direction(actual, previous, threshold)

    # revision: old_previous == 0 означает «ревизии не было / данных нет»
    if old_previous is None or old_previous == 0 or previous is None:
        rcd = "NONE"
    elif previous == old_previous:
        rcd = "FLAT"
    else:
        rcd = _rel_direction(previous, old_previous, threshold)

    return fcd, scd, rcd


#  main 

def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        print(f"Создание таблицы `{CTX_TABLE}`...")
        cur.execute(DDL)
        conn.commit()

        print(f"\nЗагрузка событий из `{SRC_TABLE}`...")
        cur.execute(f"""
            SELECT
                EventId, CurrencyCode, Importance,
                ForecastValue, PreviousValue, OldPreviousValue, ActualValue,
                FullDate, EventType
            FROM `{SRC_TABLE}`
            WHERE ActualValue IS NOT NULL
              AND Processed = 1
            ORDER BY FullDate
        """)
        raw = cur.fetchall()
        print(f"  Строк загружено: {len(raw)}")

        all_aggregates = {}
        skipped = 0
        classified = 0

        for (event_id, currency, importance,
             forecast, previous, old_prev, actual,
             full_date, event_type) in raw:

            if event_type in SKIP_EVENT_TYPES:
                skipped += 1
                continue

            if event_id is None or currency is None:
                skipped += 1
                continue

            fcd, scd, rcd = classify_event(
                forecast, previous, old_prev, actual, DIRECTION_THRESHOLD
            )
            imp = (importance or "none").lower()

            key = (event_id, currency, imp, fcd, scd, rcd)
            if key not in all_aggregates:
                all_aggregates[key] = {
                    "count":        0,
                    "first_dt":     full_date,
                    "last_dt":      full_date,
                    "sum_actual":   0.0,
                    "sum_surprise": 0.0,
                    "sum_revision": 0.0,
                }
            agg = all_aggregates[key]
            agg["count"]   += 1
            agg["last_dt"]  = full_date
            if actual is not None:
                agg["sum_actual"] += float(actual)
            if forecast is not None and actual is not None:
                agg["sum_surprise"] += abs(float(actual) - float(forecast))
            if old_prev is not None and old_prev != 0 and previous is not None:
                agg["sum_revision"] += abs(float(previous) - float(old_prev))
            classified += 1

        print(f"  Обработано: {classified}, пропущено: {skipped}")
        print(f"  Уникальных контекстов: {len(all_aggregates)}")

        if ONLY_RECURRING:
            all_aggregates = {k: v for k, v in all_aggregates.items()
                              if v["count"] > 1}
            print(f"  После фильтрации (count > 1): {len(all_aggregates)}")

        cur.execute(f"TRUNCATE TABLE `{CTX_TABLE}`;")
        conn.commit()

        sql = f"""
        INSERT INTO `{CTX_TABLE}` (
            event_id, currency_code, importance,
            forecast_dir, surprise_dir, revision_dir,
            occurrence_count, first_dt, last_dt,
            avg_actual, avg_surprise_abs, avg_revision_abs
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch = []
        total_inserted = 0
        for (eid, cur_code, imp, fcd, scd, rcd), agg in all_aggregates.items():
            cnt = agg["count"]
            batch.append((
                eid, cur_code, imp, fcd, scd, rcd,
                cnt,
                agg["first_dt"], agg["last_dt"],
                agg["sum_actual"]   / cnt if cnt else None,
                agg["sum_surprise"] / cnt if cnt else None,
                agg["sum_revision"] / cnt if cnt else None,
            ))
            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                total_inserted += len(batch)
                batch.clear()

        if batch:
            cur.executemany(sql, batch)
            total_inserted += len(batch)

        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{CTX_TABLE}`;")
        (table_cnt,) = cur.fetchone()
        print(f"\nOK: inserted={total_inserted}, table_rows={table_cnt}")

        print(f"\n Топ-20 контекстов по числу наблюдений ")
        cur.execute(f"""
            SELECT event_id, currency_code, importance,
                   forecast_dir, surprise_dir, revision_dir,
                   occurrence_count
            FROM `{CTX_TABLE}`
            ORDER BY occurrence_count DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        hdr = f"  {'evt_id':>7} {'curr':<5} {'imp':<8} {'fcast':<8} {'surp':<8} {'rev':<8} {'count':>6}"
        print(hdr)
        print("  " + "" * 56)
        for eid, cur_code, imp, fcd, scd, rcd, cnt in rows:
            print(f"  {eid:>7} {cur_code:<5} {imp:<8} {fcd:<8} {scd:<8} {rcd:<8} {cnt:>6}")

        print(f"\n Распределение по валютам ")
        cur.execute(f"""
            SELECT currency_code,
                   COUNT(*)              AS distinct_contexts,
                   SUM(occurrence_count) AS total_obs
            FROM `{CTX_TABLE}`
            GROUP BY currency_code
            ORDER BY total_obs DESC
            LIMIT 20
        """)
        for cur_code, ctx_cnt, obs in cur.fetchall():
            print(f"  {cur_code:<6}  contexts={ctx_cnt:<5}  observations={obs}")

        print(f"\n Распределение по важности ")
        cur.execute(f"""
            SELECT importance,
                   COUNT(*)              AS distinct_contexts,
                   SUM(occurrence_count) AS total_obs
            FROM `{CTX_TABLE}`
            GROUP BY importance
            ORDER BY total_obs DESC
        """)
        for imp, ctx_cnt, obs in cur.fetchall():
            print(f"  {imp:<10}  contexts={ctx_cnt:<5}  observations={obs}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()