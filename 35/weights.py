"""
brain_news_weights.py
Шаг 2: Генерирует weight_codes из vlad_news_context_idx.
Формат: NW{ctx_id}_{mode}_{shift}
  mode=0  → T1 sum (линейный)
  mode=1  → extremum probability
  shift   → 0..SHIFT_MAX часов после события

Для recurring (count > 1) контекстов генерируем shift 0..SHIFT_MAX.
Для одиночных — только shift=0.
"""

import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

CTX_TABLE  = os.getenv("CTX_TABLE",  "vlad_news_context_idx")
OUT_TABLE  = os.getenv("OUT_TABLE",  "vlad_news_weights_table")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
SHIFT_MAX  = int(os.getenv("SHIFT_MAX",  "24"))   # максимальный сдвиг в часах

# ─── DDL ──────────────────────────────────────────────────────────────────────

DDL = f"""
CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
  `id`          INT            NOT NULL AUTO_INCREMENT,
  `weight_code` VARCHAR(40)    NOT NULL,
  `ctx_id`      INT            NOT NULL,
  `mode`        TINYINT        NOT NULL DEFAULT 0,
  `shift`       SMALLINT       NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_weight_code` (`weight_code`),
  INDEX idx_ctx_id (`ctx_id`),
  INDEX idx_mode   (`mode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_vlad_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST",       "localhost"),
        port=int(os.getenv("DB_PORT",   "3306")),
        user=os.getenv("DB_USER",       "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME",   "vlad"),
        autocommit=False,
        charset="utf8mb4",
    )


# ─── Генерация кодов ──────────────────────────────────────────────────────────

def make_weight_code(ctx_id: int, mode: int, shift: int) -> str:
    """
    NW{ctx_id}_{mode}_{shift}
    Примеры:
      NW142_0_0   ctx=142, T1,       сдвиг 0ч
      NW142_1_4   ctx=142, extremum, сдвиг 4ч
      NW7_0_12    ctx=7,   T1,       сдвиг 12ч
    """
    return f"NW{ctx_id}_{mode}_{shift}"


def generate_codes(ctx_id: int, occurrence_count: int) -> list:
    """
    Для каждого контекста генерирует список (weight_code, ctx_id, mode, shift).
    Recurring (count > 1): все сдвиги 0..SHIFT_MAX × 2 режима
    Одиночные: только shift=0 × 2 режима
    """
    codes = []
    max_shift = SHIFT_MAX if occurrence_count > 1 else 0
    for mode in (0, 1):
        for shift in range(0, max_shift + 1):
            codes.append((make_weight_code(ctx_id, mode, shift), ctx_id, mode, shift))
    return codes


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    conn = get_vlad_conn()
    try:
        cur = conn.cursor()

        print(f"Создание таблицы `{OUT_TABLE}`...")
        cur.execute(DDL)
        conn.commit()

        print(f"Загрузка контекстов из `{CTX_TABLE}`...")
        cur.execute(f"SELECT id, occurrence_count FROM `{CTX_TABLE}` ORDER BY id")
        contexts = cur.fetchall()
        print(f"  Загружено контекстов: {len(contexts)}")

        print("Генерация weight_codes...")
        cur.execute(f"TRUNCATE TABLE `{OUT_TABLE}`;")
        conn.commit()

        sql = f"""
        INSERT IGNORE INTO `{OUT_TABLE}` (weight_code, ctx_id, mode, shift)
        VALUES (%s, %s, %s, %s)
        """

        batch = []
        total_codes = 0
        recurring   = 0

        for ctx_id, occ_count in contexts:
            codes = generate_codes(ctx_id, occ_count)
            batch.extend(codes)
            total_codes += len(codes)
            if occ_count > 1:
                recurring += 1

            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                conn.commit()
                batch.clear()

        if batch:
            cur.executemany(sql, batch)
            conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{OUT_TABLE}`")
        (table_cnt,) = cur.fetchone()

        print(f"\nOK:")
        print(f"  Контекстов всего:     {len(contexts)}")
        print(f"  Recurring (count>1):  {recurring}")
        print(f"  Одиночных:            {len(contexts) - recurring}")
        print(f"  Weight codes всего:   {total_codes}")
        print(f"  В таблице:            {table_cnt}")
        print(f"  Shift max:            {SHIFT_MAX}h → {(SHIFT_MAX+1)*2} кодов на recurring")

        # Примеры кодов
        print("\n-- Примеры weight_code --")
        cur.execute(f"""
            SELECT weight_code, ctx_id, mode, shift
            FROM `{OUT_TABLE}`
            ORDER BY ctx_id, mode, shift
            LIMIT 20
        """)
        for wc, cid, mode, shift in cur.fetchall():
            print(f"  {wc:<20}  ctx={cid:<5}  mode={mode}  shift={shift}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
