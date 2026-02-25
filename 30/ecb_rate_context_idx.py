import os
import mysql.connector
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

SRC_TABLE = "vlad_ecb_exchange_rates"
CTX_TABLE = "vlad_ecb_rate_context_idx"

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# Пороги для определения направления
THRESHOLD_PCT = 0.0003  # 0.03% — для дневного изменения и моментума
SMA_SHORT = 5
SMA_LONG = 20


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
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  `currency`            CHAR(3)        NOT NULL,
  `rate_change_dir`     VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `trend_dir`           VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `momentum_dir`        VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',

  `occurrence_count`    INT            NOT NULL DEFAULT 0,
  `first_rate_date`     DATE           NULL,
  `last_rate_date`      DATE           NULL,

  `avg_rate`            DECIMAL(22,12) NULL,
  `avg_daily_change`    DOUBLE         NULL,
  `avg_abs_change`      DOUBLE         NULL,

  `updated_at`          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (`currency`, `rate_change_dir`, `trend_dir`, `momentum_dir`),
  INDEX idx_ctx_ccy       (`currency`),
  INDEX idx_ctx_change    (`rate_change_dir`),
  INDEX idx_ctx_trend     (`trend_dir`),
  INDEX idx_ctx_momentum  (`momentum_dir`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def direction_label(a, b, threshold,
                    up_label="UP", down_label="DOWN", flat_label="FLAT"):
    """Сравнивает a и b с порогом, возвращает метку направления."""
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct > threshold:
        return up_label
    if pct < -threshold:
        return down_label
    return flat_label


def compute_sma(rates_list, idx, window):
    """Простая скользящая средняя по последним `window` значениям до idx включительно."""
    if idx < window - 1:
        return None
    return sum(r for _, r in rates_list[idx - window + 1: idx + 1]) / window


def classify_observations(rates_sorted):
    """
    Для отсортированного списка [(date, rate), ...] одной валюты
    возвращает список (date, rate, rate_change_dir, trend_dir, momentum_dir, daily_change).
    """
    results = []
    for i, (dt, rate) in enumerate(rates_sorted):
        # 1) rate_change_dir — сегодня vs вчера
        if i == 0:
            rcd = "UNKNOWN"
            daily_change = None
        else:
            prev_rate = rates_sorted[i - 1][1]
            rcd = direction_label(rate, prev_rate, THRESHOLD_PCT)
            daily_change = rate - prev_rate

        # 2) trend_dir — rate vs SMA(20): ABOVE / BELOW / AT
        sma_long = compute_sma(rates_sorted, i, SMA_LONG)
        if sma_long is None:
            td = "UNKNOWN"
        else:
            td = direction_label(rate, sma_long, THRESHOLD_PCT,
                                 "ABOVE", "BELOW", "AT")

        # 3) momentum_dir — SMA(5) vs SMA(20): UP / DOWN / FLAT
        sma_short = compute_sma(rates_sorted, i, SMA_SHORT)
        if sma_short is None or sma_long is None:
            md = "UNKNOWN"
        else:
            md = direction_label(sma_short, sma_long, THRESHOLD_PCT)

        results.append((dt, rate, rcd, td, md, daily_change))

    return results


def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # 1) Создаём таблицу
        print(f"Создание таблицы `{CTX_TABLE}`...")
        cur.execute(DDL)
        conn.commit()

        # 2) Загружаем все курсы, группируем по валюте
        print(f"Загрузка данных из `{SRC_TABLE}`...")
        cur.execute(f"""
            SELECT currency, rate_date, rate
            FROM `{SRC_TABLE}`
            ORDER BY currency, rate_date
        """)

        by_currency = defaultdict(list)
        for ccy, rd, rate in cur.fetchall():
            by_currency[ccy].append((rd, float(rate)))

        print(f"  Валют загружено: {len(by_currency)}")

        # 3) Классифицируем каждое наблюдение и агрегируем по контекстам
        # Ключ: (currency, rate_change_dir, trend_dir, momentum_dir)
        # Значение: {count, dates, rates, changes}
        aggregates = {}

        for ccy, rates_sorted in by_currency.items():
            observations = classify_observations(rates_sorted)

            for dt, rate, rcd, td, md, daily_change in observations:
                key = (ccy, rcd, td, md)

                if key not in aggregates:
                    aggregates[key] = {
                        "count": 0,
                        "first_date": dt,
                        "last_date": dt,
                        "sum_rate": 0.0,
                        "sum_change": 0.0,
                        "sum_abs_change": 0.0,
                    }

                agg = aggregates[key]
                agg["count"] += 1
                agg["last_date"] = dt
                agg["sum_rate"] += rate
                if daily_change is not None:
                    agg["sum_change"] += daily_change
                    agg["sum_abs_change"] += abs(daily_change)

        print(f"  Контекстов (уникальных комбинаций): {len(aggregates)}")

        # 4) Фильтрация (ONLY_RECURRING)
        if ONLY_RECURRING:
            aggregates = {k: v for k, v in aggregates.items() if v["count"] > 1}
            print(f"  После фильтрации (count > 1): {len(aggregates)}")

        # 5) Вставка / обновление
        cur.execute(f"TRUNCATE TABLE `{CTX_TABLE}`;")
        conn.commit()

        sql = f"""
        INSERT INTO `{CTX_TABLE}` (
            currency, rate_change_dir, trend_dir, momentum_dir,
            occurrence_count, first_rate_date, last_rate_date,
            avg_rate, avg_daily_change, avg_abs_change
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch = []
        total = 0
        for (ccy, rcd, td, md), agg in aggregates.items():
            cnt = agg["count"]
            avg_rate = agg["sum_rate"] / cnt if cnt > 0 else None
            avg_change = agg["sum_change"] / cnt if cnt > 0 else None
            avg_abs = agg["sum_abs_change"] / cnt if cnt > 0 else None

            batch.append((
                ccy, rcd, td, md,
                cnt, agg["first_date"], agg["last_date"],
                avg_rate, avg_change, avg_abs,
            ))

            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                total += len(batch)
                batch.clear()

        if batch:
            cur.executemany(sql, batch)
            total += len(batch)

        conn.commit()

        # 6) Диагностика
        cur.execute(f"SELECT COUNT(*) FROM `{CTX_TABLE}`;")
        (table_cnt,) = cur.fetchone()
        print(f"\nOK: inserted={total}, table_rows={table_cnt}")

        print(f"\n── Распределение по контекстам ──────────────────────────────")
        cur.execute(f"""
            SELECT rate_change_dir, trend_dir, momentum_dir,
                   COUNT(*)              AS distinct_ccys,
                   SUM(occurrence_count) AS total_obs
            FROM `{CTX_TABLE}`
            GROUP BY rate_change_dir, trend_dir, momentum_dir
            ORDER BY total_obs DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"  {'change_dir':<12} {'trend_dir':<10} {'momentum':<10} "
              f"{'ccys':>6} {'observations':>14}")
        print("  " + "─" * 56)
        for rcd, td, md, ccys, obs in rows:
            print(f"  {rcd:<12} {td:<10} {md:<10} {ccys:>6} {obs:>14}")

        cur.close()
        print("\nГотово.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()