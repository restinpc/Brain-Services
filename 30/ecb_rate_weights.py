import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ── Конфигурация ──────────────────────────────────────────────────────────────
CTX_TABLE = os.getenv("CTX_TABLE", "vlad_ecb_rate_context_idx")
OUT_TABLE = os.getenv("OUT_TABLE", "vlad_ecb_rate_weights")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
TRUNCATE_OUT = os.getenv("TRUNCATE_OUT", "1") == "1"

# ── Словари кодировки ─────────────────────────

RATE_CHANGE_MAP = {
    "UNKNOWN": "X",  # X = eXception/неопределено
    "UP": "U",
    "DOWN": "D",
    "FLAT": "F",
}

TREND_MAP = {
    "UNKNOWN": "X",
    "ABOVE": "A",  # A = Above
    "BELOW": "B",  # B = Below
    "AT": "T",  # T = aT (чтобы не путать с Above)
}

MOMENTUM_MAP = {
    "UNKNOWN": "X",
    "UP": "U",
    "DOWN": "D",
    "FLAT": "F",
}

# Обратные словари для декодировки
RATE_CHANGE_MAP_REV = {v: k for k, v in RATE_CHANGE_MAP.items()}
TREND_MAP_REV = {v: k for k, v in TREND_MAP.items()}
MOMENTUM_MAP_REV = {v: k for k, v in MOMENTUM_MAP.items()}

# ── Окно сдвига (ECB — дневные данные, не часовые) ───────────────────────────
SHIFT_MIN = -12
SHIFT_MAX = 12


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
    Создаёт таблицу весов.
    weight_code — компактный строковый ключ с буквами и одинарным '_'.
    Пример: USD_U_A_U_0_5 (валюта_изменение_тренд_импульс_режим_сдвиг)
    """
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
      `weight_code`       VARCHAR(40)   NOT NULL,
      `currency`          CHAR(3)       NOT NULL,
      `rate_change_dir`   VARCHAR(8)    NOT NULL,  -- в БД остаётся слово
      `trend_dir`         VARCHAR(8)    NOT NULL,
      `momentum_dir`      VARCHAR(8)    NOT NULL,
      `mode_val`          TINYINT       NOT NULL,   -- 0 = T1, 1 = Extremum
      `day_shift`         SMALLINT      NULL,        -- -12..12 или NULL
      `occurrence_count`  INT           NULL,
      `created_at`        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

      PRIMARY KEY (`weight_code`),
      KEY `idx_w_ccy`       (`currency`),
      KEY `idx_w_change`    (`rate_change_dir`),
      KEY `idx_w_trend`     (`trend_dir`),
      KEY `idx_w_momentum`  (`momentum_dir`),
      KEY `idx_w_mode`      (`mode_val`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def fetch_context_rows(cur):
    """
    Читает все строки из контекстного индекса.
    Каждая строка — уникальная комбинация:
    (currency, rate_change_dir, trend_dir, momentum_dir)
    Значения направлений — ЧИТАЕМЫЕ СЛОВА из БД.
    """
    cur.execute(f"""
        SELECT
            currency,
            rate_change_dir,
            trend_dir,
            momentum_dir,
            occurrence_count
        FROM `{CTX_TABLE}`
    """)
    return cur.fetchall()


def encode_direction(value, direction_type: str) -> str:
    """
    Кодирует слово из БД в одну букву для weight_code.

    Args:
        value: Значение из БД (например, 'UP', 'ABOVE', 'UNKNOWN')
        direction_type: Тип направления ('change', 'trend', 'momentum')

    Returns:
        Однобуквенный код (например, 'U', 'A', 'X')
    """
    maps = {
        "change": RATE_CHANGE_MAP,
        "trend": TREND_MAP,
        "momentum": MOMENTUM_MAP,
    }
    return maps.get(direction_type, {}).get(value, "X")


def make_weight_code(currency: str, rcd: str, td: str, md: str,
                     mode: int, day_shift: int = None) -> str:
    """
    Генерирует компактный weight_code с буквами и одинарным '_'.

    Логика:
    1. Берёт слова из БД (rcd, td, md)
    2. Конвертирует каждое в букву через словари
    3. Соединяет через одинарный '_'

    Примеры:
        ('USD', 'UP', 'ABOVE', 'UP', 0, None)      → 'USD_U_A_U_0'
        ('EUR', 'DOWN', 'BELOW', 'FLAT', 1, 5)     → 'EUR_D_B_F_1_5'
        ('GBP', 'UNKNOWN', 'AT', 'UNKNOWN', 0, -3) → 'GBP_X_T_X_0_-3'
    """
    rcd_c = encode_direction(rcd, "change")
    td_c = encode_direction(td, "trend")
    md_c = encode_direction(md, "momentum")

    base = f"{currency}_{rcd_c}_{td_c}_{md_c}_{mode}"
    return base if day_shift is None else f"{base}_{day_shift}"


def decode_weight_code(weight_code: str) -> dict:
    """
    Обратная функция: из weight_code восстанавливает исходные значения.
    Полезно для отладки и анализа результатов.

    Пример:
        'USD_U_A_U_0_5' → {
            'currency': 'USD',
            'rate_change_dir': 'UP',
            'trend_dir': 'ABOVE',
            'momentum_dir': 'UP',
            'mode_val': 0,
            'day_shift': 5
        }
    """
    parts = weight_code.split("_")
    if len(parts) < 5:
        return {}

    currency = parts[0]
    rcd = RATE_CHANGE_MAP_REV.get(parts[1], parts[1])
    td = TREND_MAP_REV.get(parts[2], parts[2])
    md = MOMENTUM_MAP_REV.get(parts[3], parts[3])
    mode = int(parts[4])
    shift = int(parts[5]) if len(parts) > 5 else None

    return {
        "currency": currency,
        "rate_change_dir": rcd,
        "trend_dir": td,
        "momentum_dir": md,
        "mode_val": mode,
        "day_shift": shift,
    }


def generate_rows_for_context(currency: str, rcd: str, td: str, md: str, occ_count: int):
    """
    Для каждого контекста генерирует строки таблицы весов:

    1. Базовые строки (mode=0, mode=1, day_shift=NULL) — для ВСЕХ валют
    2. Строки со сдвигом (mode=0/1, day_shift=-12..12) — только для recurring

    Recurring = occurrence_count > 1 (контекст встречался более 1 раза)
    """
    is_recurring = (occ_count is not None and occ_count > 1)

    # Базовые строки (без сдвига) — всегда генерируем
    for mode in (0, 1):
        yield (
            make_weight_code(currency, rcd, td, md, mode),
            currency, rcd, td, md,
            mode, None,
            occ_count,
        )

    # Строки со сдвигом — только для recurring контекстов
    if is_recurring:
        for day_shift in range(SHIFT_MIN, SHIFT_MAX + 1):
            for mode in (0, 1):
                yield (
                    make_weight_code(currency, rcd, td, md, mode, day_shift),
                    currency, rcd, td, md,
                    mode, day_shift,
                    occ_count,
                )


def insert_rows(cur, rows):
    """Пакетная вставка с UPSERT (ON DUPLICATE KEY UPDATE)."""
    sql = f"""
    INSERT INTO `{OUT_TABLE}` (
        `weight_code`,
        `currency`, `rate_change_dir`, `trend_dir`, `momentum_dir`,
        `mode_val`, `day_shift`,
        `occurrence_count`
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


def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # 1) Создаём таблицу весов
        ensure_out_table(cur)
        conn.commit()

        # 2) Очистка при необходимости
        if TRUNCATE_OUT:
            cur.execute(f"TRUNCATE TABLE `{OUT_TABLE}`;")
            conn.commit()

        # 3) Загрузка контекстов из БД (со словами)
        ctx_rows = fetch_context_rows(cur)
        print(f"Контекстов загружено: {len(ctx_rows)}")

        # Подсчёт recurring vs non-recurring
        recurring = sum(1 for _, _, _, _, occ in ctx_rows
                        if occ is not None and occ > 1)
        non_recurring = len(ctx_rows) - recurring
        print(f"  Recurring (count>1): {recurring}")
        print(f"  Non-recurring:       {non_recurring}")

        # Оценка количества строк
        estimated = non_recurring * 2 + recurring * (2 + 2 * (SHIFT_MAX - SHIFT_MIN + 1))
        print(f"  Ожидаемое кол-во weight_code: ~{estimated}")

        # 4) Генерация и вставка
        def all_rows():
            for currency, rcd, td, md, occ in ctx_rows:
                yield from generate_rows_for_context(
                    currency, rcd, td, md, occ
                )

        written = insert_rows(cur, all_rows())
        conn.commit()

        # 5) Диагностика
        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`;")
        (cnt,) = cur.fetchone()
        print(f"\nOK: contexts={len(ctx_rows)}, inserted={written}, table_rows={cnt}")

        # Вывод первых 20 строк
        cur.execute(f"""
            SELECT weight_code, currency,
                   rate_change_dir, trend_dir, momentum_dir,
                   mode_val, day_shift, occurrence_count
            FROM `{OUT_TABLE}`
            ORDER BY currency, rate_change_dir, trend_dir,
                     momentum_dir, mode_val,
                     day_shift IS NULL DESC, day_shift
            LIMIT 20
        """)
        rows = cur.fetchall()

        print(f"\n{'weight_code':<40} {'ccy':<5} {'change':<8} "
              f"{'trend':<8} {'mom':<8} {'mode':>4} {'shift':>6} {'occ':>5}")
        print("─" * 95)
        for wc, ccy, rcd, td, md, mv, ds, oc in rows:
            print(f"{wc:<40} {ccy:<5} {rcd:<8} "
                  f"{td:<8} {md:<8} {mv:>4} {str(ds):>6} {str(oc):>5}")

        # Пример декодирования
        print(f"\n── Пример декодирования weight_code ─────────────────────────")
        if rows:
            sample_code = rows[0][0]
            decoded = decode_weight_code(sample_code)
            print(f"  Код: {sample_code}")
            print(f"  Расшифровка: {decoded}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()