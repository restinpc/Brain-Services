import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

SRC_TABLE = os.getenv("SRC_TABLE", "vlad_market_history")           # brain DB
CTX_TABLE = os.getenv("CTX_TABLE", "vlad_market_tech_context_idx")  # vlad DB

ONLY_RECURRING = os.getenv("ONLY_RECURRING", "1") == "1"
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "5000"))

SMA_SHORT  = int(os.getenv("SMA_SHORT",  "24"))
SMA_LONG   = int(os.getenv("SMA_LONG",   "168"))
BB_PERIOD  = int(os.getenv("BB_PERIOD",  "20"))
VOL_PERIOD = int(os.getenv("VOL_PERIOD", "24"))

THRESHOLD_BY_INSTRUMENT = {
    "EURUSD": 0.0003, "DXY": 0.0003, "BTC": 0.002,
    "ETH": 0.003, "GOLD": 0.001, "OIL": 0.002,
}
DEFAULT_THRESHOLD = 0.001

INSTRUMENT_COLUMNS = {
    "EURUSD": ("EURUSD_Close", "EURUSD_Volume"),
    "BTC":    ("BTC_Close",    "BTC_Volume"),
    "ETH":    ("ETH_Close",    "ETH_Volume"),
    "DXY":    ("DXY_Close",    "DXY_Volume"),
    "GOLD":   ("Gold_Close",   "Gold_Volume"),
    "OIL":    ("Oil_Close",    "Oil_Volume"),
}

DDL = """
CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
  `id`                  BIGINT         NOT NULL AUTO_INCREMENT,
  `instrument`          VARCHAR(12)    NOT NULL,
  `rate_change_dir`     VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `trend_dir`           VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `momentum_dir`        VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `vol_zone`            VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `bb_zone`             VARCHAR(8)     NOT NULL DEFAULT 'UNKNOWN',
  `occurrence_count`    INT            NOT NULL DEFAULT 0,
  `first_dt`            DATETIME       NULL,
  `last_dt`             DATETIME       NULL,
  `avg_close`           DOUBLE         NULL,
  `avg_hourly_change`   DOUBLE         NULL,
  `avg_abs_change`      DOUBLE         NULL,
  `updated_at`          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_ctx_instrument_dirs`
    (`instrument`, `rate_change_dir`, `trend_dir`, `momentum_dir`,
     `vol_zone`, `bb_zone`),
  INDEX idx_ctx_instr    (`instrument`),
  INDEX idx_ctx_change   (`rate_change_dir`),
  INDEX idx_ctx_trend    (`trend_dir`),
  INDEX idx_ctx_momentum (`momentum_dir`),
  INDEX idx_ctx_vol      (`vol_zone`),
  INDEX idx_ctx_bb       (`bb_zone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".replace("{CTX_TABLE}", CTX_TABLE)


#  Два соединения 

def get_brain_connection():
    """Читаем vlad_market_history из БД brain (MASTER_* в .env)."""
    return mysql.connector.connect(
        host     = os.getenv("MASTER_HOST",     os.getenv("DB_HOST", "localhost")),
        port     = int(os.getenv("MASTER_PORT", os.getenv("DB_PORT", "3306"))),
        user     = os.getenv("MASTER_USER",     os.getenv("DB_USER", "root")),
        password = os.getenv("MASTER_PASSWORD", os.getenv("DB_PASSWORD", "")),
        database = os.getenv("MASTER_NAME",     "brain"),
        autocommit = False,
    )


def get_vlad_connection():
    """Пишем vlad_market_tech_context_idx в БД vlad (DB_* в .env)."""
    return mysql.connector.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "3306")),
        user     = os.getenv("DB_USER",     "root"),
        password = os.getenv("DB_PASSWORD", ""),
        database = os.getenv("DB_NAME",     "vlad"),
        autocommit = False,
    )


#  Вычислители 

def direction_label(a, b, threshold, up_label="UP", down_label="DOWN", flat_label="FLAT"):
    if a is None or b is None or b == 0:
        return "UNKNOWN"
    pct = (a - b) / abs(b)
    if pct >  threshold: return up_label
    if pct < -threshold: return down_label
    return flat_label

def compute_sma(series, idx, window):
    if idx < window - 1: return None
    return sum(v for _, v in series[idx - window + 1: idx + 1]) / window

def compute_sma_raw(values, idx, window):
    if idx < window - 1: return None
    return sum(values[idx - window + 1: idx + 1]) / window

def compute_std_raw(values, idx, window):
    if idx < window - 1: return None
    sl  = values[idx - window + 1: idx + 1]
    avg = sum(sl) / window
    return (sum((x - avg) ** 2 for x in sl) / window) ** 0.5

def classify_vol_zone(volume, vol_ma):
    if volume is None or vol_ma is None or vol_ma == 0: return "UNKNOWN"
    return "HIGH" if float(volume) > float(vol_ma) else "LOW"

def classify_bb_zone(close, bb_upper, bb_lower):
    if close is None or bb_upper is None or bb_lower is None: return "UNKNOWN"
    bw = bb_upper - bb_lower
    if bw == 0: return "UNKNOWN"
    p = (close - bb_lower) / bw
    if p >= 0.8: return "UPPER"
    if p <= 0.2: return "LOWER"
    return "MID"

def classify_observations(close_series, volume_series, threshold):
    closes  = [v for _, v in close_series]
    results = []
    for i, (dt, close) in enumerate(close_series):
        if i == 0:
            rcd, hourly_change = "UNKNOWN", None
        else:
            prev = close_series[i-1][1]
            rcd  = direction_label(close, prev, threshold)
            hourly_change = close - prev

        sma_long  = compute_sma(close_series, i, SMA_LONG)
        td = (direction_label(close, sma_long, threshold, "ABOVE", "BELOW", "AT")
              if sma_long is not None else "UNKNOWN")

        sma_short = compute_sma(close_series, i, SMA_SHORT)
        md = (direction_label(sma_short, sma_long, threshold)
              if (sma_short is not None and sma_long is not None) else "UNKNOWN")

        vol_ma   = compute_sma_raw(volume_series, i, VOL_PERIOD)
        vol_zone = classify_vol_zone(volume_series[i], vol_ma)

        bb_mid = compute_sma_raw(closes, i, BB_PERIOD)
        bb_std = compute_std_raw(closes, i, BB_PERIOD)
        if bb_mid is not None and bb_std is not None and bb_std > 0:
            bb_zone = classify_bb_zone(close, bb_mid + 2*bb_std, bb_mid - 2*bb_std)
        else:
            bb_zone = "UNKNOWN"

        results.append((dt, close, rcd, td, md, vol_zone, bb_zone, hourly_change))
    return results


#  main 

def main():
    brain_conn = get_brain_connection()
    vlad_conn  = get_vlad_connection()
    try:
        brain_cur = brain_conn.cursor()
        vlad_cur  = vlad_conn.cursor()

        print(f"Создание таблицы `{CTX_TABLE}` в vlad...")
        vlad_cur.execute(DDL)
        vlad_conn.commit()

        all_aggregates  = {}
        grand_total_obs = 0

        for instrument, (close_col, volume_col) in INSTRUMENT_COLUMNS.items():
            threshold = THRESHOLD_BY_INSTRUMENT.get(instrument, DEFAULT_THRESHOLD)
            print(f"\nОбработка {instrument}  (порог: {threshold*100:.3f}%)...")

            brain_cur.execute(f"""
                SELECT `datetime`, `{close_col}`, `{volume_col}`
                FROM `{SRC_TABLE}`
                WHERE `{close_col}` IS NOT NULL
                ORDER BY `datetime`
            """)
            raw = brain_cur.fetchall()
            if not raw:
                print(f"    Нет данных для {instrument}, пропускаем.")
                continue

            close_series  = [(row[0], float(row[1])) for row in raw]
            volume_series = [float(row[2]) if row[2] is not None else None for row in raw]
            print(f"  Строк загружено: {len(close_series)}")

            observations = classify_observations(close_series, volume_series, threshold)
            grand_total_obs += len(observations)

            for dt, close, rcd, td, md, vol_zone, bb_zone, hourly_change in observations:
                key = (instrument, rcd, td, md, vol_zone, bb_zone)
                if key not in all_aggregates:
                    all_aggregates[key] = {
                        "count": 0, "first_dt": dt, "last_dt": dt,
                        "sum_close": 0.0, "sum_change": 0.0, "sum_abs": 0.0,
                    }
                agg = all_aggregates[key]
                agg["count"]     += 1
                agg["last_dt"]    = dt
                agg["sum_close"] += close
                if hourly_change is not None:
                    agg["sum_change"] += hourly_change
                    agg["sum_abs"]    += abs(hourly_change)

        print(f"\nВсего наблюдений: {grand_total_obs}")
        print(f"Уникальных контекстов (5D): {len(all_aggregates)}")

        if ONLY_RECURRING:
            all_aggregates = {k: v for k, v in all_aggregates.items() if v["count"] > 1}
            print(f"После фильтрации (count > 1): {len(all_aggregates)}")

        vlad_cur.execute(f"TRUNCATE TABLE `{CTX_TABLE}`;")
        vlad_conn.commit()

        sql = f"""
        INSERT INTO `{CTX_TABLE}` (
            instrument, rate_change_dir, trend_dir, momentum_dir,
            vol_zone, bb_zone,
            occurrence_count, first_dt, last_dt,
            avg_close, avg_hourly_change, avg_abs_change
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch = []
        total_inserted = 0
        for (instr, rcd, td, md, vol_zone, bb_zone), agg in all_aggregates.items():
            cnt = agg["count"]
            batch.append((
                instr, rcd, td, md, vol_zone, bb_zone, cnt,
                agg["first_dt"], agg["last_dt"],
                agg["sum_close"]  / cnt if cnt else None,
                agg["sum_change"] / cnt if cnt else None,
                agg["sum_abs"]    / cnt if cnt else None,
            ))
            if len(batch) >= BATCH_SIZE:
                vlad_cur.executemany(sql, batch)
                total_inserted += len(batch)
                batch.clear()

        if batch:
            vlad_cur.executemany(sql, batch)
            total_inserted += len(batch)

        vlad_conn.commit()

        vlad_cur.execute(f"SELECT COUNT(*) FROM `{CTX_TABLE}`;")
        (table_cnt,) = vlad_cur.fetchone()
        print(f"\nOK: inserted={total_inserted}, table_rows={table_cnt}")

        #  Статистика 
        print(f"\n Топ-20 контекстов по числу наблюдений ")
        vlad_cur.execute(f"""
            SELECT instrument, rate_change_dir, trend_dir, momentum_dir,
                   vol_zone, bb_zone, occurrence_count
            FROM `{CTX_TABLE}` ORDER BY occurrence_count DESC LIMIT 20
        """)
        print(f"  {'instr':<8} {'chg':<8} {'trend':<8} {'mom':<8} {'vol':<6} {'bb':<8} {'count':>8}")
        print("  " + "" * 60)
        for instr, rcd, td, md, vol, bb, cnt in vlad_cur.fetchall():
            print(f"  {instr:<8} {rcd:<8} {td:<8} {md:<8} {vol:<6} {bb:<8} {cnt:>8}")

        print(f"\n По инструментам ")
        vlad_cur.execute(f"""
            SELECT instrument, COUNT(*) AS ctx, SUM(occurrence_count) AS obs
            FROM `{CTX_TABLE}` GROUP BY instrument ORDER BY instrument
        """)
        for instr, ctx_cnt, obs in vlad_cur.fetchall():
            print(f"  {instr:<8}  contexts={ctx_cnt:<5}  observations={obs}")

        print(f"\n По vol_zone ")
        vlad_cur.execute(f"""
            SELECT vol_zone, COUNT(*) AS ctx, SUM(occurrence_count) AS obs
            FROM `{CTX_TABLE}` GROUP BY vol_zone ORDER BY obs DESC
        """)
        for vol, ctx_cnt, obs in vlad_cur.fetchall():
            print(f"  {vol:<8}  contexts={ctx_cnt:<5}  observations={obs}")

        print(f"\n По bb_zone ")
        vlad_cur.execute(f"""
            SELECT bb_zone, COUNT(*) AS ctx, SUM(occurrence_count) AS obs
            FROM `{CTX_TABLE}` GROUP BY bb_zone ORDER BY obs DESC
        """)
        for bb, ctx_cnt, obs in vlad_cur.fetchall():
            print(f"  {bb:<8}  contexts={ctx_cnt:<5}  observations={obs}")

        brain_cur.close()
        vlad_cur.close()
        print("\nГотово.")

    finally:
        brain_conn.close()
        vlad_conn.close()


if __name__ == "__main__":
    main()