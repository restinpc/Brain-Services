import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CAL_TABLE = os.getenv("CAL_TABLE", "vlad_investing_calendar")
OUT_TABLE = os.getenv("OUT_TABLE", "vlad_investing_weights_table")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
TRUNCATE_OUT = os.getenv("TRUNCATE_OUT", "1") == "1"


def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rss_db"),
        autocommit=False,
    )


def ensure_out_table(cur):
    # Структура как на твоём скрине: weight_code, EventId, event_type, mode_val, hour_shift
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
      `weight_code` VARCHAR(80) NOT NULL,
      `EventId` INT NOT NULL,
      `event_type` TINYINT NOT NULL,   -- 0/1
      `mode_val` TINYINT NOT NULL,     -- 0/1
      `hour_shift` SMALLINT NULL,      -- -12..12 или NULL
      `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

      PRIMARY KEY (`weight_code`),
      KEY `idx_event` (`EventId`),
      KEY `idx_type` (`event_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def fetch_event_types(cur):
    # event_type = 1 если у event_id больше одного появления, иначе 0
    cur.execute(f"""
        SELECT event_id, COUNT(*) AS cnt
        FROM `{CAL_TABLE}`
        WHERE event_id IS NOT NULL
        GROUP BY event_id
    """)
    out = []
    for event_id, cnt in cur.fetchall():
        etype = 1 if int(cnt) > 1 else 0
        out.append((int(event_id), etype))
    return out


def generate_rows_for_event(event_id: int, event_type: int):
    # базовые коды — для всех
    for mode in (0, 1):
        yield (f"{event_id}_{event_type}_{mode}", event_id, event_type, mode, None)

    # часовые коды — только для циклических (event_type=1)
    if event_type == 1:
        for hour in range(-12, 13):
            for mode in (0, 1):
                yield (f"{event_id}_{event_type}_{mode}_{hour}", event_id, event_type, mode, hour)


def insert_rows(cur, rows):
    sql = f"""
    INSERT INTO `{OUT_TABLE}` (`weight_code`, `EventId`, `event_type`, `mode_val`, `hour_shift`)
    VALUES (%s, %s, %s, %s, %s)
    """
    total = 0
    batch = []
    for r in rows:
        batch.append(r)
        if len(batch) >= BATCH_SIZE:
            cur.executemany(sql, batch)  # batch insert [web:31]
            total += len(batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)      # batch insert [web:31]
        total += len(batch)
    return total


def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        ensure_out_table(cur)
        conn.commit()

        if TRUNCATE_OUT:
            cur.execute(f"TRUNCATE TABLE `{OUT_TABLE}`;")
            conn.commit()

        event_types = fetch_event_types(cur)

        # Генерация и вставка
        def all_rows():
            for event_id, etype in event_types:
                yield from generate_rows_for_event(event_id, etype)

        written = insert_rows(cur, all_rows())
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`;")
        (cnt,) = cur.fetchone()

        print(f"OK: events={len(event_types)}, inserted_rows={written}, table_rows={cnt}")

        # Примеры (как у тебя на скрине)
        cur.execute(f"""
            SELECT weight_code, EventId, event_type, mode_val, hour_shift
            FROM `{OUT_TABLE}`
            ORDER BY EventId, event_type, mode_val, hour_shift
            LIMIT 30
        """)
        for row in cur.fetchall():
            print(row)

        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
