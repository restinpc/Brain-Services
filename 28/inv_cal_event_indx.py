import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CAL_TABLE = "vlad_investing_calendar"
CTX_TABLE = "vlad_investing_event_context_idx"

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"


def get_db_connection():
    """Создаёт и возвращает подключение к базе данных MySQL."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rss_db"),
        autocommit=False,
    )


DDL_CONTEXT_INDEX = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  event_id              INT          NOT NULL,
  forecast_direction    VARCHAR(8)   NOT NULL DEFAULT 'UNKNOWN',
  surprise_direction    VARCHAR(8)   NOT NULL DEFAULT 'UNKNOWN',
  actual_direction      VARCHAR(8)   NOT NULL DEFAULT 'UNKNOWN',
  occurrence_count      INT          NOT NULL DEFAULT 0,
  first_occurrence_utc  DATETIME     NULL,
  last_occurrence_utc   DATETIME     NULL,
  avg_actual            DOUBLE       NULL,
  avg_forecast          DOUBLE       NULL,
  avg_previous          DOUBLE       NULL,
  avg_surprise_abs      DOUBLE       NULL,
  currency              VARCHAR(8)   NULL,
  importance            TINYINT      NULL,
  event_name            VARCHAR(255) NULL,
  country_id            INT          NULL,
  category              VARCHAR(64)  NULL,
  unit                  VARCHAR(16)  NULL,
  updated_at            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (event_id, forecast_direction, surprise_direction, actual_direction),
  INDEX idx_ctx_event    (event_id),
  INDEX idx_ctx_currency (currency),
  INDEX idx_ctx_category (category),
  INDEX idx_ctx_importance (importance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def sql_upsert_context_index(having: str) -> str:
    """Формирует SQL-запрос для заполнения контекстной таблицы агрегатами из календаря."""
    return f"""
    INSERT INTO `{CTX_TABLE}` (
        event_id,
        forecast_direction, surprise_direction, actual_direction,
        occurrence_count,
        first_occurrence_utc, last_occurrence_utc,
        avg_actual, avg_forecast, avg_previous, avg_surprise_abs,
        currency, importance, event_name, country_id, category, unit
    )
    SELECT
        c.event_id,
        CASE
          WHEN CAST(c.forecast AS DECIMAL(18,6)) IS NULL
            OR CAST(c.previous AS DECIMAL(18,6)) IS NULL
              THEN 'UNKNOWN'
          WHEN CAST(c.forecast AS DECIMAL(18,6))
               > CAST(c.previous AS DECIMAL(18,6)) + 0.0001  THEN 'UP'
          WHEN CAST(c.forecast AS DECIMAL(18,6))
               < CAST(c.previous AS DECIMAL(18,6)) - 0.0001  THEN 'DOWN'
          ELSE 'FLAT'
        END AS forecast_direction,
        CASE
          WHEN CAST(c.actual   AS DECIMAL(18,6)) IS NULL
            OR CAST(c.forecast AS DECIMAL(18,6)) IS NULL
              THEN 'UNKNOWN'
          WHEN CAST(c.actual   AS DECIMAL(18,6))
               > CAST(c.forecast AS DECIMAL(18,6)) + 0.0001  THEN 'BEAT'
          WHEN CAST(c.actual   AS DECIMAL(18,6))
               < CAST(c.forecast AS DECIMAL(18,6)) - 0.0001  THEN 'MISS'
          ELSE 'INLINE'
        END AS surprise_direction,
        CASE
          WHEN CAST(c.actual   AS DECIMAL(18,6)) IS NULL
            OR CAST(c.previous AS DECIMAL(18,6)) IS NULL
              THEN 'UNKNOWN'
          WHEN CAST(c.actual   AS DECIMAL(18,6))
               > CAST(c.previous AS DECIMAL(18,6)) + 0.0001  THEN 'UP'
          WHEN CAST(c.actual   AS DECIMAL(18,6))
               < CAST(c.previous AS DECIMAL(18,6)) - 0.0001  THEN 'DOWN'
          ELSE 'FLAT'
        END AS actual_direction,
        COUNT(*)                       AS occurrence_count,
        MIN(c.occurrence_time_utc)     AS first_occurrence_utc,
        MAX(c.occurrence_time_utc)     AS last_occurrence_utc,
        AVG(CAST(c.actual   AS DECIMAL(18,6)))   AS avg_actual,
        AVG(CAST(c.forecast AS DECIMAL(18,6)))   AS avg_forecast,
        AVG(CAST(c.previous AS DECIMAL(18,6)))   AS avg_previous,
        AVG(ABS(  CAST(c.actual   AS DECIMAL(18,6))
                - CAST(c.forecast AS DECIMAL(18,6))
               ))                                AS avg_surprise_abs,
        MAX(c.currency)    AS currency,
        MAX(c.importance)  AS importance,
        MAX(c.event_name)  AS event_name,
        MAX(c.country_id)  AS country_id,
        MAX(c.category)    AS category,
        MAX(c.unit)        AS unit
    FROM `{CAL_TABLE}` c
    WHERE c.event_id IS NOT NULL
    GROUP BY
        c.event_id,
        forecast_direction,
        surprise_direction,
        actual_direction
    {having}
    ON DUPLICATE KEY UPDATE
        occurrence_count     = VALUES(occurrence_count),
        first_occurrence_utc = VALUES(first_occurrence_utc),
        last_occurrence_utc  = VALUES(last_occurrence_utc),
        avg_actual           = VALUES(avg_actual),
        avg_forecast         = VALUES(avg_forecast),
        avg_previous         = VALUES(avg_previous),
        avg_surprise_abs     = VALUES(avg_surprise_abs),
        currency             = VALUES(currency),
        importance           = VALUES(importance),
        event_name           = VALUES(event_name),
        country_id           = VALUES(country_id),
        category             = VALUES(category),
        unit                 = VALUES(unit);
    """


SQL_DIAG_CONTEXT = f"""
SELECT
    forecast_direction,
    surprise_direction,
    actual_direction,
    COUNT(*)           AS distinct_events,
    SUM(occurrence_count) AS total_occurrences
FROM `{CTX_TABLE}`
GROUP BY forecast_direction, surprise_direction, actual_direction
ORDER BY total_occurrences DESC;
"""


def main():
    """Основная логика: создание контекстной таблицы, её заполнение и вывод статистики."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        having = "HAVING COUNT(*) > 1" if ONLY_RECURRING else ""

        print("Создание таблицы контекста событий...")
        cur.execute(DDL_CONTEXT_INDEX)
        conn.commit()

        print(f"Заполнение `{CTX_TABLE}`...")
        cur.execute(sql_upsert_context_index(having))
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM `{CTX_TABLE}`;")
        print(f"  → {cur.fetchone()[0]} строк в `{CTX_TABLE}`")

        print("\n── Распределение по контекстам ──────────────────────────────")
        cur.execute(SQL_DIAG_CONTEXT)
        rows = cur.fetchall()
        print(f"  {'forecast_dir':<14} {'surprise_dir':<14} {'actual_dir':<12}"
              f" {'events':>8} {'occurrences':>12}")
        print("  " + "─" * 64)
        for forecast_d, surprise_d, actual_d, ev, occ in rows:
            print(f"  {forecast_d:<14} {surprise_d:<14} {actual_d:<12}"
                  f" {ev:>8} {occ:>12}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()