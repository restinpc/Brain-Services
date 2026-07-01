import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CTX_TABLE  = os.getenv("CTX_TABLE",  "vlad_market_context_idx")
OUT_TABLE  = os.getenv("OUT_TABLE",  "vlad_market_weights")
BATCH_SIZE = int(os.getenv("BATCH_SIZE",  "5000"))
TRUNCATE_OUT = os.getenv("TRUNCATE_OUT", "1") == "1"

SHIFT_MIN = int(os.getenv("SHIFT_MIN", "-12"))
SHIFT_MAX = int(os.getenv("SHIFT_MAX",  "12"))


#  Однобуквенное кодирование 

RATE_CHANGE_MAP = {
    "UNKNOWN": "X",
    "UP":      "U",
    "DOWN":    "D",
    "FLAT":    "F",
}

TREND_MAP = {
    "UNKNOWN": "X",
    "ABOVE":   "A",
    "BELOW":   "B",
    "AT":      "T",  # aT (не A, чтобы не путать с ABOVE)
}

MOMENTUM_MAP = {
    "UNKNOWN": "X",
    "UP":      "U",
    "DOWN":    "D",
    "FLAT":    "F",
}

# Обратные словари (для decode)
RATE_CHANGE_MAP_REV = {v: k for k, v in RATE_CHANGE_MAP.items()}
TREND_MAP_REV       = {v: k for k, v in TREND_MAP.items()}
MOMENTUM_MAP_REV    = {v: k for k, v in MOMENTUM_MAP.items()}


#  DDL 

DDL = f"""
CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
  `weight_code`       VARCHAR(40)   NOT NULL,
  `instrument`        VARCHAR(12)   NOT NULL,
  `rate_change_dir`   VARCHAR(8)    NOT NULL,
  `trend_dir`         VARCHAR(8)    NOT NULL,
  `momentum_dir`      VARCHAR(8)    NOT NULL,
  `mode_val`          TINYINT       NOT NULL,   -- 0=T1, 1=Extremum
  `hour_shift`        SMALLINT      NULL,        -- NULL = без сдвига; -12..12
  `occurrence_count`  INT           NULL,
  `created_at`        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (`weight_code`),
  KEY `idx_mw_instr`    (`instrument`),
  KEY `idx_mw_change`   (`rate_change_dir`),
  KEY `idx_mw_trend`    (`trend_dir`),
  KEY `idx_mw_momentum` (`momentum_dir`),
  KEY `idx_mw_mode`     (`mode_val`)
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
        "change":   RATE_CHANGE_MAP,
        "trend":    TREND_MAP,
        "momentum": MOMENTUM_MAP,
    }
    return maps.get(direction_type, {}).get(value, "X")


def make_weight_code(instrument: str, rcd: str, td: str, md: str,
                     mode: int, hour_shift: int | None = None) -> str:
    """
    Генерирует компактный weight_code.

    >>> make_weight_code('EURUSD', 'UP', 'ABOVE', 'UP', 0)
    'EURUSD_U_A_U_0'
    >>> make_weight_code('BTC', 'DOWN', 'BELOW', 'FLAT', 1, -6)
    'BTC_D_B_F_1_-6'
    """
    rcd_c = encode(rcd, "change")
    td_c  = encode(td,  "trend")
    md_c  = encode(md,  "momentum")
    base  = f"{instrument}_{rcd_c}_{td_c}_{md_c}_{mode}"
    return base if hour_shift is None else f"{base}_{hour_shift}"


def decode_weight_code(code: str) -> dict:
    """
    Обратная функция: weight_code → словарь полей.

    >>> decode_weight_code('EURUSD_U_A_U_0_5')
    {'instrument': 'EURUSD', 'rate_change_dir': 'UP', 'trend_dir': 'ABOVE',
     'momentum_dir': 'UP', 'mode_val': 0, 'hour_shift': 5}
    """
    parts = code.split("_")
    if len(parts) < 5:
        return {}
    instrument = parts[0]
    rcd  = RATE_CHANGE_MAP_REV.get(parts[1], parts[1])
    td   = TREND_MAP_REV.get(parts[2], parts[2])
    md   = MOMENTUM_MAP_REV.get(parts[3], parts[3])
    mode = int(parts[4])
    shift = int(parts[5]) if len(parts) > 5 else None
    return {
        "instrument":      instrument,
        "rate_change_dir": rcd,
        "trend_dir":       td,
        "momentum_dir":    md,
        "mode_val":        mode,
        "hour_shift":      shift,
    }


#  Генерация строк 

def generate_rows(instrument: str, rcd: str, td: str, md: str, occ: int):
    """
    Для одного контекста (instrument, rcd, td, md) генерирует:
    - 2 базовые строки (mode=0, mode=1, hour_shift=NULL) — всегда
    - 2*(SHIFT_MAX-SHIFT_MIN+1) строк со сдвигом — только если occ > 1
    """
    is_recurring = occ is not None and occ > 1

    for mode in (0, 1):
        yield (
            make_weight_code(instrument, rcd, td, md, mode),
            instrument, rcd, td, md,
            mode, None, occ,
        )

    if is_recurring:
        for shift in range(SHIFT_MIN, SHIFT_MAX + 1):
            for mode in (0, 1):
                yield (
                    make_weight_code(instrument, rcd, td, md, mode, shift),
                    instrument, rcd, td, md,
                    mode, shift, occ,
                )


#  Вставка 

def insert_rows(cur, rows):
    sql = f"""
    INSERT INTO `{OUT_TABLE}` (
        weight_code,
        instrument, rate_change_dir, trend_dir, momentum_dir,
        mode_val, hour_shift, occurrence_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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

        # 1) Таблица весов
        print(f"Создание таблицы `{OUT_TABLE}`...")
        cur.execute(DDL)
        conn.commit()

        # 2) Очистка
        if TRUNCATE_OUT:
            cur.execute(f"TRUNCATE TABLE `{OUT_TABLE}`;")
            conn.commit()

        # 3) Загружаем контексты
        cur.execute(f"""
            SELECT instrument, rate_change_dir, trend_dir, momentum_dir,
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

        # 4) Генерация и вставка
        def all_rows():
            for instrument, rcd, td, md, occ in ctx_rows:
                yield from generate_rows(instrument, rcd, td, md, occ)

        written = insert_rows(cur, all_rows())
        conn.commit()

        # 5) Диагностика
        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`;")
        (cnt,) = cur.fetchone()
        print(f"\nOK: contexts={len(ctx_rows)}, inserted={written}, table_rows={cnt}")

        # Распределение по инструментам
        print(f"\n Строк по инструментам ")
        cur.execute(f"""
            SELECT instrument,
                   SUM(hour_shift IS NULL) AS base_rows,
                   SUM(hour_shift IS NOT NULL) AS shift_rows,
                   COUNT(*) AS total
            FROM `{OUT_TABLE}`
            GROUP BY instrument
            ORDER BY instrument
        """)
        print(f"  {'instr':<10} {'base':>8} {'shifted':>10} {'total':>8}")
        print("  " + "" * 40)
        for instr, base, shifted, total in cur.fetchall():
            print(f"  {instr:<10} {base:>8} {shifted:>10} {total:>8}")

        # Первые 20 строк
        print(f"\n Первые 20 weight_codes ")
        cur.execute(f"""
            SELECT weight_code, instrument,
                   rate_change_dir, trend_dir, momentum_dir,
                   mode_val, hour_shift, occurrence_count
            FROM `{OUT_TABLE}`
            ORDER BY instrument, rate_change_dir, trend_dir, momentum_dir,
                     mode_val, hour_shift IS NULL DESC, hour_shift
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"  {'weight_code':<35} {'instr':<8} {'chg':<6} {'trd':<8} "
              f"{'mom':<8} {'mode':>4} {'shift':>6} {'occ':>5}")
        print("  " + "" * 85)
        for wc, instr, rcd, td, md, mv, ds, oc in rows:
            print(f"  {wc:<35} {instr:<8} {rcd:<6} {td:<8} "
                  f"{md:<8} {mv:>4} {str(ds):>6} {str(oc):>5}")

        # Пример декодирования
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
