import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CTX_TABLE    = os.getenv("CTX_TABLE",    "brain_calendar_context_idx")
OUT_TABLE    = os.getenv("OUT_TABLE",    "brain_calendar_weights")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE",   "5000"))
TRUNCATE_OUT = os.getenv("TRUNCATE_OUT", "1") == "1"

SHIFT_MIN = int(os.getenv("SHIFT_MIN", "-12"))
SHIFT_MAX = int(os.getenv("SHIFT_MAX",  "12"))


#  Однобуквенное кодирование 

FORECAST_MAP = {
    "UNKNOWN": "X",
    "BEAT":    "B",
    "MISS":    "M",
    "INLINE":  "I",
}

SURPRISE_MAP = {
    "UNKNOWN": "X",
    "UP":      "U",
    "DOWN":    "D",
    "FLAT":    "F",
}

REVISION_MAP = {
    "NONE":    "N",
    "FLAT":    "T",   # aT — не путать с N
    "UP":      "U",
    "DOWN":    "D",
    "UNKNOWN": "X",
}

IMPORTANCE_MAP = {
    "high":   "H",
    "medium": "M",
    "low":    "L",
    "none":   "N",
}

# Обратные словари (для decode)
FORECAST_MAP_REV  = {v: k for k, v in FORECAST_MAP.items()}
SURPRISE_MAP_REV  = {v: k for k, v in SURPRISE_MAP.items()}
REVISION_MAP_REV  = {v: k for k, v in REVISION_MAP.items()}
IMPORTANCE_MAP_REV = {v: k for k, v in IMPORTANCE_MAP.items()}


#  DDL 

DDL = f"""
CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
  `weight_code`       VARCHAR(64)   NOT NULL,
  `event_id`          INT           NOT NULL,
  `currency_code`     VARCHAR(4)    NOT NULL,
  `importance`        VARCHAR(10)   NOT NULL,
  `forecast_dir`      VARCHAR(8)    NOT NULL,
  `surprise_dir`      VARCHAR(8)    NOT NULL,
  `revision_dir`      VARCHAR(8)    NOT NULL,
  `mode_val`          TINYINT       NOT NULL,   -- 0=T1, 1=Extremum
  `hour_shift`        SMALLINT      NULL,        -- NULL = без сдвига; SHIFT_MIN..SHIFT_MAX
  `occurrence_count`  INT           NULL,
  `created_at`        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (`weight_code`),
  KEY `idx_cw_event`    (`event_id`),
  KEY `idx_cw_currency` (`currency_code`),
  KEY `idx_cw_imp`      (`importance`),
  KEY `idx_cw_forecast` (`forecast_dir`),
  KEY `idx_cw_surprise` (`surprise_dir`),
  KEY `idx_cw_revision` (`revision_dir`),
  KEY `idx_cw_mode`     (`mode_val`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


#  Подключение 

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rss_db"),
        autocommit=False,
    )


#  Кодирование / декодирование 

def encode(value: str, direction_type: str) -> str:
    maps = {
        "forecast":   FORECAST_MAP,
        "surprise":   SURPRISE_MAP,
        "revision":   REVISION_MAP,
        "importance": IMPORTANCE_MAP,
    }
    return maps.get(direction_type, {}).get(value, "X")


def make_weight_code(event_id: int, currency: str, importance: str,
                     fcd: str, scd: str, rcd: str,
                     mode: int, hour_shift: int | None = None) -> str:
    """
    Генерирует компактный weight_code для события экономического календаря.

    >>> make_weight_code(14, 'USD', 'high', 'BEAT', 'UP', 'NONE', 0)
    'E14_USD_H_B_U_N_0'
    >>> make_weight_code(14, 'USD', 'high', 'BEAT', 'UP', 'NONE', 1, -6)
    'E14_USD_H_B_U_N_1_-6'
    """
    imp_c = encode(importance, "importance")
    fcd_c = encode(fcd, "forecast")
    scd_c = encode(scd, "surprise")
    rcd_c = encode(rcd, "revision")
    base  = f"E{event_id}_{currency}_{imp_c}_{fcd_c}_{scd_c}_{rcd_c}_{mode}"
    return base if hour_shift is None else f"{base}_{hour_shift}"


def decode_weight_code(code: str) -> dict:
    """
    Обратная функция: weight_code → словарь полей.

    >>> decode_weight_code('E14_USD_H_B_U_N_0_5')
    {'event_id': 14, 'currency_code': 'USD', 'importance': 'high',
     'forecast_dir': 'BEAT', 'surprise_dir': 'UP', 'revision_dir': 'NONE',
     'mode_val': 0, 'hour_shift': 5}
    """
    parts = code.split("_")
    # Минимум: E{id} _ {curr} _ {imp} _ {fcd} _ {scd} _ {rcd} _ {mode}
    if len(parts) < 7:
        return {}
    try:
        event_id = int(parts[0][1:])          # убираем 'E'
    except ValueError:
        return {}
    currency   = parts[1]
    importance = IMPORTANCE_MAP_REV.get(parts[2], parts[2])
    fcd        = FORECAST_MAP_REV.get(parts[3], parts[3])
    scd        = SURPRISE_MAP_REV.get(parts[4], parts[4])
    rcd        = REVISION_MAP_REV.get(parts[5], parts[5])
    mode       = int(parts[6])
    shift      = int(parts[7]) if len(parts) > 7 else None
    return {
        "event_id":      event_id,
        "currency_code": currency,
        "importance":    importance,
        "forecast_dir":  fcd,
        "surprise_dir":  scd,
        "revision_dir":  rcd,
        "mode_val":      mode,
        "hour_shift":    shift,
    }


#  Генерация строк 

def generate_rows(event_id: int, currency: str, importance: str,
                  fcd: str, scd: str, rcd: str, occ: int):
    """
    Для одного контекста генерирует:
    - 2 базовые строки (mode=0, mode=1, hour_shift=NULL) — всегда
    - 2*(SHIFT_MAX-SHIFT_MIN+1) строк со сдвигом — только если occ > 1
    """
    is_recurring = occ is not None and occ > 1

    for mode in (0, 1):
        yield (
            make_weight_code(event_id, currency, importance, fcd, scd, rcd, mode),
            event_id, currency, importance, fcd, scd, rcd,
            mode, None, occ,
        )

    if is_recurring:
        for shift in range(SHIFT_MIN, SHIFT_MAX + 1):
            for mode in (0, 1):
                yield (
                    make_weight_code(event_id, currency, importance, fcd, scd, rcd,
                                     mode, shift),
                    event_id, currency, importance, fcd, scd, rcd,
                    mode, shift, occ,
                )


#  Вставка 

def insert_rows(cur, rows):
    sql = f"""
    INSERT INTO `{OUT_TABLE}` (
        weight_code,
        event_id, currency_code, importance,
        forecast_dir, surprise_dir, revision_dir,
        mode_val, hour_shift, occurrence_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        occurrence_count = VALUES(occurrence_count)
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


#  main 

def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        print(f"Создание таблицы `{OUT_TABLE}`...")
        cur.execute(DDL)
        conn.commit()

        if TRUNCATE_OUT:
            cur.execute(f"TRUNCATE TABLE `{OUT_TABLE}`;")
            conn.commit()

        cur.execute(f"""
            SELECT event_id, currency_code, importance,
                   forecast_dir, surprise_dir, revision_dir,
                   occurrence_count
            FROM `{CTX_TABLE}`
        """)
        ctx_rows = cur.fetchall()
        print(f"Контекстов загружено: {len(ctx_rows)}")

        recurring     = sum(1 for *_, occ in ctx_rows if occ and occ > 1)
        non_recurring = len(ctx_rows) - recurring
        shifts_per    = SHIFT_MAX - SHIFT_MIN + 1

        print(f"  Recurring     (occ > 1): {recurring}")
        print(f"  Non-recurring (occ = 1): {non_recurring}")
        print(f"  Диапазон сдвигов: {SHIFT_MIN}..{SHIFT_MAX} ({shifts_per} значений)")

        estimated = non_recurring * 2 + recurring * (2 + 2 * shifts_per)
        print(f"  Ожидаемое кол-во weight_code: ~{estimated:,}")

        def all_rows():
            for event_id, currency, importance, fcd, scd, rcd, occ in ctx_rows:
                yield from generate_rows(event_id, currency, importance,
                                         fcd, scd, rcd, occ)

        written = insert_rows(cur, all_rows())
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`;")
        (cnt,) = cur.fetchone()
        print(f"\nOK: contexts={len(ctx_rows)}, inserted={written}, table_rows={cnt}")

        print(f"\n Строк по валютам ")
        cur.execute(f"""
            SELECT currency_code,
                   SUM(hour_shift IS NULL)     AS base_rows,
                   SUM(hour_shift IS NOT NULL) AS shift_rows,
                   COUNT(*)                    AS total
            FROM `{OUT_TABLE}`
            GROUP BY currency_code
            ORDER BY total DESC
        """)
        print(f"  {'currency':<10} {'base':>8} {'shifted':>10} {'total':>8}")
        print("  " + "" * 40)
        for cur_code, base, shifted, total in cur.fetchall():
            print(f"  {cur_code:<10} {base:>8} {shifted:>10} {total:>8}")

        print(f"\n Первые 20 weight_codes ")
        cur.execute(f"""
            SELECT weight_code, event_id, currency_code, importance,
                   forecast_dir, surprise_dir, revision_dir,
                   mode_val, hour_shift, occurrence_count
            FROM `{OUT_TABLE}`
            ORDER BY event_id, currency_code, importance,
                     forecast_dir, surprise_dir, revision_dir,
                     mode_val, hour_shift IS NULL DESC, hour_shift
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"  {'weight_code':<38} {'evt':>7} {'curr':<5} {'imp':<5} "
              f"{'fcd':<7} {'scd':<7} {'rcd':<6} {'mode':>4} {'shift':>6} {'occ':>5}")
        print("  " + "" * 96)
        for wc, eid, cur_c, imp, fcd, scd, rcd, mv, ds, oc in rows:
            print(f"  {wc:<38} {eid:>7} {cur_c:<5} {imp:<5} "
                  f"{fcd:<7} {scd:<7} {rcd:<6} {mv:>4} {str(ds):>6} {str(oc):>5}")

        if rows:
            sample = rows[0][0]
            print(f"\n Пример декодирования ")
            print(f"  Код: {sample}")
            print(f"  Расшифровка: {decode_weight_code(sample)}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
