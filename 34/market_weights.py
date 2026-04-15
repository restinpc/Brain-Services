import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CTX_TABLE    = os.getenv("CTX_TABLE",    "vlad_market_tech_context_idx")
OUT_TABLE    = os.getenv("OUT_TABLE",    "vlad_market_tech_weights")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE",   "5000"))
TRUNCATE_OUT = os.getenv("TRUNCATE_OUT", "1") == "1"

SHIFT_MIN = int(os.getenv("SHIFT_MIN", "-12"))
SHIFT_MAX = int(os.getenv("SHIFT_MAX",  "12"))


#  Числовое кодирование 
# Формат weight_code: {instr}_{rcd}_{td}_{md}_{vol}_{bb}_{mode}[_{shift}]
# Пример: 1_1_1_1_1_2_0       (без сдвига)
#         1_1_1_1_1_2_0_-6    (со сдвигом)

INSTRUMENT_MAP = {
    "EURUSD": 1,
    "BTC":    3,
    "ETH":    4,
    "DXY":    5,
    "GOLD":   6,
    "OIL":    7,
}

RATE_CHANGE_MAP = {
    "UNKNOWN": 0,
    "UP":      1,
    "DOWN":    2,
    "FLAT":    3,
}

TREND_MAP = {
    "UNKNOWN": 0,
    "ABOVE":   1,
    "BELOW":   2,
    "AT":      3,
}

MOMENTUM_MAP = {
    "UNKNOWN": 0,
    "UP":      1,
    "DOWN":    2,
    "FLAT":    3,
}

VOL_MAP = {
    "UNKNOWN": 0,
    "HIGH":    1,
    "LOW":     2,
}

BB_MAP = {
    "UNKNOWN": 0,
    "UPPER":   1,
    "MID":     2,
    "LOWER":   3,
}

# Обратные словари (для decode)
INSTRUMENT_MAP_REV  = {v: k for k, v in INSTRUMENT_MAP.items()}
RATE_CHANGE_MAP_REV = {v: k for k, v in RATE_CHANGE_MAP.items()}
TREND_MAP_REV       = {v: k for k, v in TREND_MAP.items()}
MOMENTUM_MAP_REV    = {v: k for k, v in MOMENTUM_MAP.items()}
VOL_MAP_REV         = {v: k for k, v in VOL_MAP.items()}
BB_MAP_REV          = {v: k for k, v in BB_MAP.items()}


#  DDL 

DDL = f"""
CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
  `weight_code`       VARCHAR(40)   NOT NULL,
  `instrument`        VARCHAR(12)   NOT NULL,
  `rate_change_dir`   VARCHAR(8)    NOT NULL,
  `trend_dir`         VARCHAR(8)    NOT NULL,
  `momentum_dir`      VARCHAR(8)    NOT NULL,
  `vol_zone`          VARCHAR(8)    NOT NULL,
  `bb_zone`           VARCHAR(8)    NOT NULL,
  `mode_val`          TINYINT       NOT NULL,
  `hour_shift`        SMALLINT      NULL,
  `occurrence_count`  INT           NULL,
  `created_at`        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (`weight_code`),
  KEY `idx_mw_instr`    (`instrument`),
  KEY `idx_mw_change`   (`rate_change_dir`),
  KEY `idx_mw_trend`    (`trend_dir`),
  KEY `idx_mw_momentum` (`momentum_dir`),
  KEY `idx_mw_vol`      (`vol_zone`),
  KEY `idx_mw_bb`       (`bb_zone`),
  KEY `idx_mw_mode`     (`mode_val`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


#  Подключение 

def get_db_connection():
    return mysql.connector.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "3306")),
        user     = os.getenv("DB_USER",     "root"),
        password = os.getenv("DB_PASSWORD", ""),
        database = os.getenv("DB_NAME",     "rss_db"),
        autocommit = False,
    )


#  Кодирование / декодирование 

def make_weight_code(instrument: str, rcd: str, td: str, md: str,
                     vol: str, bb: str,
                     mode: int, hour_shift: int | None = None) -> str:
    """
    Генерирует числовой weight_code.

    >>> make_weight_code('EURUSD', 'UP', 'ABOVE', 'UP', 'HIGH', 'MID', 0)
    '1_1_1_1_1_2_0'
    >>> make_weight_code('BTC', 'DOWN', 'BELOW', 'FLAT', 'LOW', 'LOWER', 1, -6)
    '3_2_2_3_2_3_1_-6'
    """
    instr_c = INSTRUMENT_MAP.get(instrument, 9)
    rcd_c   = RATE_CHANGE_MAP.get(rcd, 0)
    td_c    = TREND_MAP.get(td,         0)
    md_c    = MOMENTUM_MAP.get(md,      0)
    vol_c   = VOL_MAP.get(vol,          0)
    bb_c    = BB_MAP.get(bb,            0)
    base    = f"{instr_c}_{rcd_c}_{td_c}_{md_c}_{vol_c}_{bb_c}_{mode}"
    return base if hour_shift is None else f"{base}_{hour_shift}"


def decode_weight_code(code: str) -> dict:
    """
    Обратная функция: числовой weight_code → словарь полей.

    >>> decode_weight_code('1_1_1_1_1_2_0_5')
    {'instrument': 'EURUSD', 'rate_change_dir': 'UP', 'trend_dir': 'ABOVE',
     'momentum_dir': 'UP', 'vol_zone': 'HIGH', 'bb_zone': 'MID',
     'mode_val': 0, 'hour_shift': 5}
    """
    parts = code.split("_")
    # Минимум 7 частей: instr rcd td md vol bb mode
    if len(parts) < 7:
        return {}
    try:
        instr_id = int(parts[0])
        rcd_id   = int(parts[1])
        td_id    = int(parts[2])
        md_id    = int(parts[3])
        vol_id   = int(parts[4])
        bb_id    = int(parts[5])
        mode     = int(parts[6])
        shift    = int(parts[7]) if len(parts) > 7 else None
    except ValueError:
        return {}

    return {
        "instrument":      INSTRUMENT_MAP_REV.get(instr_id, str(instr_id)),
        "rate_change_dir": RATE_CHANGE_MAP_REV.get(rcd_id,  str(rcd_id)),
        "trend_dir":       TREND_MAP_REV.get(td_id,         str(td_id)),
        "momentum_dir":    MOMENTUM_MAP_REV.get(md_id,      str(md_id)),
        "vol_zone":        VOL_MAP_REV.get(vol_id,          str(vol_id)),
        "bb_zone":         BB_MAP_REV.get(bb_id,            str(bb_id)),
        "mode_val":        mode,
        "hour_shift":      shift,
    }


#  Генерация строк 

def generate_rows(instrument: str, rcd: str, td: str, md: str,
                  vol: str, bb: str, occ: int):
    """
    Для одного контекста генерирует:
    - 2 базовые строки (mode=0, mode=1, hour_shift=NULL)
    - 2*(SHIFT_MAX-SHIFT_MIN+1) строк со сдвигом — только если occ > 1
    """
    is_recurring = occ is not None and occ > 1

    for mode in (0, 1):
        yield (
            make_weight_code(instrument, rcd, td, md, vol, bb, mode),
            instrument, rcd, td, md, vol, bb,
            mode, None, occ,
        )

    if is_recurring:
        for shift in range(SHIFT_MIN, SHIFT_MAX + 1):
            for mode in (0, 1):
                yield (
                    make_weight_code(instrument, rcd, td, md, vol, bb, mode, shift),
                    instrument, rcd, td, md, vol, bb,
                    mode, shift, occ,
                )


#  Вставка 

def insert_rows(cur, rows):
    sql = f"""
    INSERT INTO `{OUT_TABLE}` (
        weight_code,
        instrument, rate_change_dir, trend_dir, momentum_dir,
        vol_zone, bb_zone,
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
            SELECT instrument, rate_change_dir, trend_dir, momentum_dir,
                   vol_zone, bb_zone, occurrence_count
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
            for instr, rcd, td, md, vol, bb, occ in ctx_rows:
                yield from generate_rows(instr, rcd, td, md, vol, bb, occ)

        written = insert_rows(cur, all_rows())
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`;")
        (cnt,) = cur.fetchone()
        print(f"\nOK: contexts={len(ctx_rows)}, inserted={written}, table_rows={cnt}")

        print(f"\n Строк по инструментам ")
        cur.execute(f"""
            SELECT instrument,
                   SUM(hour_shift IS NULL)     AS base_rows,
                   SUM(hour_shift IS NOT NULL) AS shift_rows,
                   COUNT(*)                    AS total
            FROM `{OUT_TABLE}`
            GROUP BY instrument
            ORDER BY instrument
        """)
        print(f"  {'instr':<10} {'base':>8} {'shifted':>10} {'total':>8}")
        print("  " + "" * 40)
        for instr, base, shifted, total in cur.fetchall():
            print(f"  {instr:<10} {base:>8} {shifted:>10} {total:>8}")

        print(f"\n Первые 20 weight_codes ")
        cur.execute(f"""
            SELECT weight_code, instrument,
                   rate_change_dir, trend_dir, momentum_dir,
                   vol_zone, bb_zone,
                   mode_val, hour_shift, occurrence_count
            FROM `{OUT_TABLE}`
            ORDER BY instrument, rate_change_dir, trend_dir, momentum_dir,
                     vol_zone, bb_zone,
                     mode_val, hour_shift IS NULL DESC, hour_shift
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"  {'weight_code':<26} {'instr':<8} {'chg':<6} {'trd':<8} "
              f"{'mom':<8} {'vol':<6} {'bb':<8} {'mode':>4} {'shift':>6} {'occ':>5}")
        print("  " + "" * 96)
        for wc, instr, rcd, td, md, vol, bb, mv, ds, oc in rows:
            print(f"  {wc:<26} {instr:<8} {rcd:<6} {td:<8} "
                  f"{md:<8} {vol:<6} {bb:<8} {mv:>4} {str(ds):>6} {str(oc):>5}")

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