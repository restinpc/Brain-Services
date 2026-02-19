import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CTX_TABLE = os.getenv("CTX_TABLE", "vlad_investing_event_context_idx")
OUT_TABLE  = os.getenv("OUT_TABLE",  "vlad_investing_weights")

BATCH_SIZE   = int(os.getenv("BATCH_SIZE",   "5000"))
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
    """
    weight_code — детерминированный строковый ключ вида:
        {event_id}__{fdir}__{sdir}__{adir}__{mode}[__{hour}]

    Контекст берётся из PK таблицы vlad_investing_event_context_idx:
        (event_id, forecast_direction, surprise_direction, actual_direction)
    """
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
      `weight_code`         VARCHAR(120) NOT NULL,
      `event_id`            INT          NOT NULL,
      `forecast_direction`  VARCHAR(8)   NOT NULL,
      `surprise_direction`  VARCHAR(8)   NOT NULL,
      `actual_direction`    VARCHAR(8)   NOT NULL,
      `mode_val`            TINYINT      NOT NULL,   -- 0 / 1
      `hour_shift`          SMALLINT     NULL,        -- -12..12 или NULL
      `occurrence_count`    INT          NULL,        -- из контекстного индекса
      `importance`          TINYINT      NULL,
      `currency`            VARCHAR(8)   NULL,
      `created_at`          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

      PRIMARY KEY (`weight_code`),
      KEY `idx_w_event`    (`event_id`),
      KEY `idx_w_forecast` (`forecast_direction`),
      KEY `idx_w_surprise` (`surprise_direction`),
      KEY `idx_w_actual`   (`actual_direction`),
      KEY `idx_w_currency` (`currency`),
      KEY `idx_w_importance` (`importance`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def fetch_context_rows(cur):
    """
    Читаем все строки из контекстного индекса.
    Каждая строка — уникальная комбинация (event_id, forecast_dir, surprise_dir, actual_dir).
    """
    cur.execute(f"""
        SELECT
            event_id,
            forecast_direction,
            surprise_direction,
            actual_direction,
            occurrence_count,
            importance,
            currency
        FROM `{CTX_TABLE}`
    """)
    return cur.fetchall()


def make_weight_code(event_id, fdir, sdir, adir, mode, hour=None):
    base = f"{event_id}__{fdir}__{sdir}__{adir}__{mode}"
    return base if hour is None else f"{base}__{hour}"


def generate_rows_for_context(event_id, fdir, sdir, adir, occ_count, importance, currency):
    """
    Для каждого контекста генерируем:
      - 2 базовых строки (mode=0, mode=1, hour=NULL) — для всех
      - 2×25 часовых строк (mode=0/1, hour=-12..12) — только для recurring (occ_count > 1)
    """
    is_recurring = (occ_count is not None and occ_count > 1)

    for mode in (0, 1):
        yield (
            make_weight_code(event_id, fdir, sdir, adir, mode),
            event_id, fdir, sdir, adir,
            mode, None,
            occ_count, importance, currency,
        )

    if is_recurring:
        for hour in range(-12, 13):
            for mode in (0, 1):
                yield (
                    make_weight_code(event_id, fdir, sdir, adir, mode, hour),
                    event_id, fdir, sdir, adir,
                    mode, hour,
                    occ_count, importance, currency,
                )


def insert_rows(cur, rows):
    sql = f"""
    INSERT INTO `{OUT_TABLE}` (
        `weight_code`,
        `event_id`, `forecast_direction`, `surprise_direction`, `actual_direction`,
        `mode_val`, `hour_shift`,
        `occurrence_count`, `importance`, `currency`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        occurrence_count = VALUES(occurrence_count),
        importance       = VALUES(importance),
        currency         = VALUES(currency)
    """
    total = 0
    batch = []
    for r in rows:
        batch.append(r)
        if len(batch) >= BATCH_SIZE:
            cur.executemany(sql, batch)
            total += len(batch)
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
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

        ctx_rows = fetch_context_rows(cur)
        print(f"Контекстов загружено: {len(ctx_rows)}")

        def all_rows():
            for event_id, fdir, sdir, adir, occ, imp, curr in ctx_rows:
                yield from generate_rows_for_context(
                    int(event_id), fdir, sdir, adir, occ, imp, curr
                )

        written = insert_rows(cur, all_rows())
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`;")
        (cnt,) = cur.fetchone()
        print(f"OK: contexts={len(ctx_rows)}, inserted_rows={written}, table_rows={cnt}")

        # Диагностика: топ-10 строк
        cur.execute(f"""
            SELECT weight_code, event_id,
                   forecast_direction, surprise_direction, actual_direction,
                   mode_val, hour_shift, occurrence_count
            FROM `{OUT_TABLE}`
            ORDER BY event_id, forecast_direction, surprise_direction,
                     actual_direction, mode_val, hour_shift
            LIMIT 10
        """)
        print(f"\n{'weight_code':<55} {'eid':>6} {'fdir':<10} {'sdir':<8} {'adir':<8} {'mode':>4} {'hour':>5} {'occ':>5}")
        print("─" * 110)
        for row in cur.fetchall():
            wc, eid, fd, sd, ad, mv, hs, oc = row
            print(f"{wc:<55} {eid:>6} {fd:<10} {sd:<8} {ad:<8} {mv:>4} {str(hs):>5} {str(oc):>5}")

        cur.close()
        print("\nГотово.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()