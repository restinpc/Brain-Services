import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()
CAL_TABLE = "vlad_investing_calendar"
IDX_TABLE = "vlad_investing_event_index"

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"  # 1 = только COUNT(*)>1

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rss_db"),
        autocommit=False,
    )

DDL = f"""
CREATE TABLE IF NOT EXISTS `{IDX_TABLE}` (
  event_id INT NOT NULL,
  currency VARCHAR(8) NULL,
  importance TINYINT NULL,
  event_name VARCHAR(255) NULL,
  country_id INT NULL,
  category VARCHAR(64) NULL,
  page_link VARCHAR(255) NULL,
  unit VARCHAR(16) NULL,
  reference_period VARCHAR(32) NULL,

  first_occurrence_utc DATETIME NULL,
  last_occurrence_utc  DATETIME NULL,
  occurrence_count INT NOT NULL DEFAULT 0,

  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (event_id),
  INDEX idx_country (country_id),
  INDEX idx_currency (currency)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # 1) Таблица-справочник событий
        cur.execute(DDL)
        conn.commit()

        # 2) Наполнение/обновление справочника из occurrences
        having = "HAVING COUNT(*) > 1" if ONLY_RECURRING else ""

        sql = f"""
        INSERT INTO `{IDX_TABLE}` (
            event_id,
            currency, importance, event_name, country_id, category, page_link, unit, reference_period,
            first_occurrence_utc, last_occurrence_utc, occurrence_count
        )
        SELECT
            c.event_id AS event_id,

            -- Берём "какое-то" значение из группы; MAX() часто достаточно, если данные внутри event_id согласованы
            MAX(c.currency) AS currency,
            MAX(c.importance) AS importance,
            MAX(c.event_name) AS event_name,
            MAX(c.country_id) AS country_id,
            MAX(c.category) AS category,
            MAX(c.page_link) AS page_link,
            MAX(c.unit) AS unit,
            MAX(c.reference_period) AS reference_period,

            MIN(c.occurrence_time_utc) AS first_occurrence_utc,
            MAX(c.occurrence_time_utc) AS last_occurrence_utc,
            COUNT(*) AS occurrence_count
        FROM `{CAL_TABLE}` c
        WHERE c.event_id IS NOT NULL
        GROUP BY c.event_id
        {having}
        ON DUPLICATE KEY UPDATE
            currency = VALUES(currency),
            importance = VALUES(importance),
            event_name = VALUES(event_name),
            country_id = VALUES(country_id),
            category = VALUES(category),
            page_link = VALUES(page_link),
            unit = VALUES(unit),
            reference_period = VALUES(reference_period),
            first_occurrence_utc = VALUES(first_occurrence_utc),
            last_occurrence_utc = VALUES(last_occurrence_utc),
            occurrence_count = VALUES(occurrence_count);
        """

        cur.execute(sql)
        conn.commit()

        # 3) Диагностика (сколько строк получилось)
        cur.execute(f"SELECT COUNT(*) FROM `{IDX_TABLE}`;")
        (cnt,) = cur.fetchone()
        print(f"OK: `{IDX_TABLE}` rows = {cnt}")

        cur.close()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
