import os
import sys
import datetime
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "vlad_market_history"
SQLALCHEMY_URL = (
    f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', '3306')}"
    f"/{os.getenv('DB_NAME')}"
)

ASSETS = {
    'EURUSD': 'EURUSD=X',
    'BTC':    'BTC-USD',
    'ETH':    'ETH-USD',
    'DXY':    'DX-Y.NYB',
    'SP500':  '^GSPC',
    'Nasdaq': '^IXIC',
    'VIX':    '^VIX',
    'Oil':    'CL=F',
    'Gold':   'GC=F',
    'US10Y':  '^TNX',
}

OHLCV_COLS = {
    'Close':  'Close',
    'High':   'High',
    'Low':    'Low',
    'Open':   'Open',
    'Volume': 'Volume',
}

engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)


# ── Миграция: добавляем недостающие колонки ───────────────────────────────────

def migrate_add_columns():
    new_cols = [
        f'{name}_{suffix}'
        for name in ASSETS
        for suffix in ('High', 'Low', 'Open')
    ]
    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{TABLE_NAME}'"
        ))
        existing = {row[0] for row in result.fetchall()}

    missing = [c for c in new_cols if c not in existing]
    if not missing:
        print(f"  ✅ Все колонки уже существуют")
        return

    print(f"  ⚙️  Добавляем {len(missing)} колонок: "
          f"{missing[:6]}{'...' if len(missing) > 6 else ''}...")
    alter_parts = ", ".join(f"ADD COLUMN `{col}` DOUBLE NULL" for col in missing)
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE `{TABLE_NAME}` {alter_parts}"))
    print(f"  ✅ ALTER TABLE выполнен")


# ── Даты ─────────────────────────────────────────────────────────────────────

def get_last_datetime():
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(`datetime`) FROM `{TABLE_NAME}`"))
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print(f"⚠️ Не удалось получить последнюю дату: {e}")
        return None


def get_first_datetime():
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MIN(`datetime`) FROM `{TABLE_NAME}`"))
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print(f"⚠️ Не удалось получить первую дату: {e}")
        return None


# ── Скачивание данных ─────────────────────────────────────────────────────────

def download_ohlcv(start_date=None, period="2y") -> pd.DataFrame:
    """
    Качает OHLCV для всех тикеров.
    start_date — datetime, качаем с этой даты.
    period     — строка "2y"/"1y", используется если start_date=None.
    """
    tickers = list(ASSETS.values())
    try:
        if start_date:
            data = yf.download(
                tickers,
                start=start_date.strftime('%Y-%m-%d'),
                interval="1h",
                group_by='ticker',
                progress=False,
            )
        else:
            data = yf.download(
                tickers,
                period=period,
                interval="1h",
                group_by='ticker',
                progress=False,
            )
    except Exception as e:
        print(f"  ❌ Ошибка Yahoo Finance: {e}")
        return pd.DataFrame()

    if data is None or data.empty:
        return pd.DataFrame()

    dfs = {}
    for name, ticker in ASSETS.items():
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if ticker not in data.columns.get_level_values(0):
                    continue
                df = data[ticker].copy()
            else:
                df = data.copy()

            cols_map = {}
            for src, suffix in OHLCV_COLS.items():
                if src in df.columns:
                    cols_map[src] = f'{name}_{suffix}'

            if not cols_map:
                continue

            df = df.rename(columns=cols_map)[list(cols_map.values())]
            df.index = pd.to_datetime(df.index).tz_localize(None)
            dfs[name] = df

        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    full_df = pd.concat(dfs.values(), axis=1)
    full_df.sort_index(inplace=True)
    full_df.dropna(how='all', inplace=True)
    return full_df


# ── On-Chain метрики ──────────────────────────────────────────────────────────

def get_crypto_metrics() -> pd.DataFrame:
    print("[*] Скачивание On-Chain метрик (BTC Hashrate)...")
    try:
        r = crequests.get(
            "https://api.blockchain.info/charts/hash-rate?timespan=2years&format=json",
            timeout=10,
        )
        df = pd.DataFrame(r.json()['values'])
        df['x'] = pd.to_datetime(df['x'], unit='s')
        df.set_index('x', inplace=True)
        df.columns = ['BTC_Hashrate']
        return df.resample('1h').ffill()
    except Exception:
        return pd.DataFrame()


# ── Бэкфилл через temp table + UPDATE JOIN ────────────────────────────────────

def backfill_missing_columns():
    """
    Заполняет NULL в High/Low/Open для уже существующих строк.
    Использует временную таблицу + UPDATE JOIN — намного быстрее
    построчного UPDATE на 25k+ строк.
    """
    check_col = 'EURUSD_High'
    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT COUNT(*) FROM `{TABLE_NAME}` WHERE `{check_col}` IS NULL"
        ))
        null_count = result.fetchone()[0]

    if null_count == 0:
        print(f"  ✅ Бэкфилл не нужен — исторические данные уже заполнены")
        return

    print(f"  ⚙️  Нужно заполнить {null_count} строк историческими High/Low/Open...")

    # ── Скачиваем: сначала по дате, fallback на period="2y" ──────────────────
    hist_df = pd.DataFrame()

    first_dt = get_first_datetime()
    if first_dt:
        start = first_dt - datetime.timedelta(days=1)
        print(f"  [*] Пробуем скачать с {start.date()}...")
        hist_df = download_ohlcv(start_date=start)

    if hist_df.empty:
        # yfinance не всегда отдаёт данные по start= для дат старше 730 дней
        print(f"  [*] start= не сработал → пробуем period='2y'...")
        hist_df = download_ohlcv(period="2y")

    if hist_df.empty:
        print("  ⚠️  Не удалось скачать данные для бэкфилла, пропускаем")
        return

    # ── Нормализуем индекс ────────────────────────────────────────────────────
    hist_df = hist_df.copy()
    hist_df.index.name = 'datetime'
    hist_df = hist_df.reset_index()

    # Страховка от разных названий индекса в разных версиях yfinance
    for alias in ('Datetime', 'Date', 'index'):
        if alias in hist_df.columns and 'datetime' not in hist_df.columns:
            hist_df.rename(columns={alias: 'datetime'}, inplace=True)

    hist_df['datetime'] = pd.to_datetime(hist_df['datetime']).dt.tz_localize(None)

    # ── Оставляем только High/Low/Open колонки ────────────────────────────────
    update_cols = [
        f'{name}_{suffix}'
        for name in ASSETS
        for suffix in ('High', 'Low', 'Open')
        if f'{name}_{suffix}' in hist_df.columns
    ]

    if not update_cols:
        print("  ⚠️  High/Low/Open колонок нет в скачанных данных")
        return

    keep = ['datetime'] + update_cols
    hist_df = hist_df[keep].dropna(how='all', subset=update_cols)
    print(f"  [*] Скачано {len(hist_df)} строк, "
          f"обновляем {len(update_cols)} колонок через UPDATE JOIN...")

    # ── Временная таблица + UPDATE JOIN ──────────────────────────────────────
    TMP = "_backfill_tmp"
    try:
        # 1. Пишем в temp-таблицу (быстро через to_sql)
        hist_df.to_sql(
            name=TMP, con=engine,
            if_exists='replace', index=False,
            chunksize=2000, method='multi',
        )

        # 2. Один UPDATE JOIN — обновляет все совпадающие строки за один запрос
        set_clause = ",\n    ".join(
            f"t.`{col}` = s.`{col}`" for col in update_cols
        )
        sql = f"""
            UPDATE `{TABLE_NAME}` t
            JOIN   `{TMP}` s ON s.`datetime` = t.`datetime`
            SET    {set_clause}
            WHERE  t.`{check_col}` IS NULL
        """
        with engine.begin() as conn:
            result  = conn.execute(text(sql))
            updated = result.rowcount

        print(f"  ✅ Бэкфилл завершён: обновлено {updated} строк")

    except Exception as e:
        print(f"  ❌ Ошибка при бэкфилле: {e}")
    finally:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS `{TMP}`"))
        except Exception:
            pass


# ── Сохранение новых данных ───────────────────────────────────────────────────

def save_data(df: pd.DataFrame):
    if df.empty:
        print("  ℹ️  Нет новых данных для сохранения")
        return
    try:
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='append',
            index=True,
            index_label='datetime',
            chunksize=1000,
            method='multi',
        )
        print(f"  ✅ Добавлено {len(df)} строк в '{TABLE_NAME}'")
    except Exception as e:
        print(f"  ❌ Ошибка записи: {e}")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"🚀 Загрузка рыночных данных → {TABLE_NAME}\n")

    # 1. Миграция структуры таблицы
    print("[1] Проверка структуры таблицы...")
    migrate_add_columns()

    # 2. Бэкфилл исторических High/Low/Open
    print("\n[2] Бэкфилл исторических данных...")
    backfill_missing_columns()

    # 3. Инкрементальное обновление — только новые строки
    print("\n[3] Обновление свежих данных...")
    last_dt    = get_last_datetime()
    start_date = (last_dt - datetime.timedelta(days=1)) if last_dt else None

    df_market = download_ohlcv(start_date=start_date)
    if df_market.empty:
        print("  ⚠️  Нет новых рыночных данных")
        sys.exit(0)

    if last_dt is not None:
        df_market = df_market[df_market.index > last_dt]

    # 4. On-Chain метрики
    df_onchain = get_crypto_metrics()
    if not df_onchain.empty:
        df_market = df_market.join(df_onchain, how='left').ffill()

    save_data(df_market)
    print("\n🏁 Готово!")
