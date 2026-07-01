import os
from collections import defaultdict

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

SRC_TABLE = os.getenv("SRC_TABLE", "vlad_market_history")
CTX_TABLE = os.getenv("CTX_TABLE", "vlad_market_context_idx")

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

SMA_SHORT = int(os.getenv("SMA_SHORT", "24"))
SMA_LONG  = int(os.getenv("SMA_LONG",  "168"))

THRESHOLD_BY_INSTRUMENT = {
    "EURUSD": 0.0003,
    "DXY":    0.0003,
    "BTC":    0.002,
    "ETH":    0.003,
    "GOLD":   0.001,
    "OIL":    0.002,
}
DEFAULT_THRESHOLD = 0.001

INSTRUMENT_COLUMNS = {
    "EURUSD": "EURUSD_Close",
    "BTC":    "BTC_Close",
    "ETH":    "ETH_Close",
    "DXY":    "DXY_Close",
    "GOLD":   "Gold_Close",
    "OIL":    "Oil_Close",
}

DDL = f"""
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  `id`                  BIGINT         NOT NULL AUTO_INCREMENT,
  `instrument`          VARCHAR(12)    NOT NULL,
  `rate_change_dir`     VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `trend_dir`           VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `momentum_dir`        VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `occurrence_count`    INT            NOT NULL DEFAULT 0,
  `first_dt`            DATETIME       NULL,
  `last_dt`             DATETIME       NULL,
  `avg_close`           DOUBLE         NULL,
  `avg_hourly_change`   DOUBLE         NULL,
  `avg_abs_change`      DOUBLE         NULL,
  `updated_at`          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctx_instrument_dirs` (`instrument`, `rate_change_dir`, `trend_dir`, `momentum_dir`),
  INDEX idx_ctx_instr    (`instrument`),
  INDEX idx_ctx_change   (`rate_change_dir`),
  INDEX idx_ctx_trend    (`trend_dir`),
  INDEX idx_ctx_momentum (`momentum_dir`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Устанавливает соединение с базой данных MySQL используя параметры из .env
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rss_db"),
        autocommit=False,
    )

# Сравнивает два значения a и b с учётом порога и возвращает метку направления
def direction_label(a, b, threshold,
                    up_label="UP", down_label="DOWN", flat_label="FLAT"):
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct > threshold:
        return up_label
    if pct < -threshold:
        return down_label
    return flat_label

# Вычисляет простое скользящее среднее для указанного окна
def compute_sma(series, idx, window):
    if idx < window - 1:
        return None
    return sum(v for _, v in series[idx - window + 1: idx + 1]) / window

# Классифицирует каждое наблюдение по трём направлениям: изменение цены, тренд и моментум
def classify_observations(series, threshold):
    results = []
    for i, (dt, close) in enumerate(series):
        if i == 0:
            rcd = "UNKNOWN"
            hourly_change = None
        else:
            prev = series[i - 1][1]
            rcd = direction_label(close, prev, threshold)
            hourly_change = close - prev

        sma_long = compute_sma(series, i, SMA_LONG)
        td = (direction_label(close, sma_long, threshold, "ABOVE", "BELOW", "AT")
              if sma_long is not None else "UNKNOWN")

        sma_short = compute_sma(series, i, SMA_SHORT)
        md = (direction_label(sma_short, sma_long, threshold)
              if (sma_short is not None and sma_long is not None) else "UNKNOWN")

        results.append((dt, close, rcd, td, md, hourly_change))

    return results

# Основная функция: загружает данные, классифицирует и заполняет итоговую таблицу
def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        print(f"Создание таблицы `{CTX_TABLE}`...")
        cur.execute(DDL)
        conn.commit()

        all_aggregates = {}
        grand_total_obs = 0

        for instrument, col in INSTRUMENT_COLUMNS.items():
            threshold = THRESHOLD_BY_INSTRUMENT.get(instrument, DEFAULT_THRESHOLD)

            print(f"\nОбработка инструмента {instrument} (колонка: {col}, порог: {threshold*100:.3f}%)...")

            cur.execute(f"""
                SELECT `datetime`, `{col}`
                FROM `{SRC_TABLE}`
                WHERE `{col}` IS NOT NULL
                ORDER BY `datetime`
            """)
            raw = cur.fetchall()
            if not raw:
                print(f"    Нет данных для {instrument}, пропускаем.")
                continue

            series = [(dt, float(v)) for dt, v in raw]
            print(f"  Строк загружено: {len(series)}")

            observations = classify_observations(series, threshold)
            grand_total_obs += len(observations)

            for dt, close, rcd, td, md, hourly_change in observations:
                key = (instrument, rcd, td, md)
                if key not in all_aggregates:
                    all_aggregates[key] = {
                        "count":       0,
                        "first_dt":    dt,
                        "last_dt":     dt,
                        "sum_close":   0.0,
                        "sum_change":  0.0,
                        "sum_abs":     0.0,
                    }
                agg = all_aggregates[key]
                agg["count"]     += 1
                agg["last_dt"]    = dt
                agg["sum_close"] += close
                if hourly_change is not None:
                    agg["sum_change"] += hourly_change
                    agg["sum_abs"]    += abs(hourly_change)

        print(f"\nВсего наблюдений: {grand_total_obs}")
        print(f"Уникальных контекстов: {len(all_aggregates)}")

        if ONLY_RECURRING:
            all_aggregates = {k: v for k, v in all_aggregates.items()
                              if v["count"] > 1}
            print(f"После фильтрации (count > 1): {len(all_aggregates)}")

        cur.execute(f"TRUNCATE TABLE `{CTX_TABLE}`;")
        conn.commit()

        sql = f"""
        INSERT INTO `{CTX_TABLE}` (
            instrument, rate_change_dir, trend_dir, momentum_dir,
            occurrence_count, first_dt, last_dt,
            avg_close, avg_hourly_change, avg_abs_change
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch = []
        total_inserted = 0
        for (instr, rcd, td, md), agg in all_aggregates.items():
            cnt = agg["count"]
            batch.append((
                instr, rcd, td, md,
                cnt,
                agg["first_dt"], agg["last_dt"],
                agg["sum_close"]  / cnt if cnt else None,
                agg["sum_change"] / cnt if cnt else None,
                agg["sum_abs"]    / cnt if cnt else None,
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
            SELECT instrument, rate_change_dir, trend_dir, momentum_dir,
                   occurrence_count
            FROM `{CTX_TABLE}`
            ORDER BY occurrence_count DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"  {'instr':<8} {'change':<8} {'trend':<8} {'momentum':<10} {'count':>8}")
        print("  " + "" * 46)
        for instr, rcd, td, md, cnt in rows:
            print(f"  {instr:<8} {rcd:<8} {td:<8} {md:<10} {cnt:>8}")

        print(f"\n Распределение по инструментам ")
        cur.execute(f"""
            SELECT instrument,
                   COUNT(*) AS distinct_contexts,
                   SUM(occurrence_count) AS total_obs
            FROM `{CTX_TABLE}`
            GROUP BY instrument
            ORDER BY instrument
        """)
        for instr, ctx_cnt, obs in cur.fetchall():
            print(f"  {instr:<8}  contexts={ctx_cnt:<4}  observations={obs}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()