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

# Колонки которые качаем — добавили High, Low, Open
OHLCV_COLS = {
    'Close':  'Close',
    'High':   'High',
    'Low':    'Low',
    'Open':   'Open',
    'Volume': 'Volume',
}

engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)


# ── Миграция: добавляем недостающие колонки в существующую таблицу ────────────

def migrate_add_columns():
    """
    Проверяет существующие колонки таблицы.
    Если High/Low/Open отсутствуют — добавляет их через ALTER TABLE.
    Запускается один раз при старте, безопасно при повторном запуске.
    """
    new_cols = []
    for name in ASSETS:
        for suffix in ('High', 'Low', 'Open'):
            new_cols.append(f'{name}_{suffix}')

    with engine.connect() as conn:
        # Получаем список существующих колонок
        result = conn.execute(text(
            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{TABLE_NAME}'"
        ))
        existing = {row[0] for row in result.fetchall()}

    missing = [c for c in new_cols if c not in existing]
    if not missing:
        print(f"  ✅ Все колонки уже существуют ({len(new_cols)} штук)")
        return

    print(f"  ⚙️  Добавляем {len(missing)} колонок: {missing[:6]}{'...' if len(missing) > 6 else ''}")
    alter_parts = ", ".join(f"ADD COLUMN `{col}` DOUBLE NULL" for col in missing)
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE `{TABLE_NAME}` {alter_parts}"))
    print(f"  ✅ ALTER TABLE выполнен")


# ── Получение крайних дат ─────────────────────────────────────────────────────

def get_last_datetime():
    """Последняя datetime в таблице — для инкрементального обновления."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(`datetime`) FROM `{TABLE_NAME}`"))
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print(f"⚠️ Не удалось получить последнюю дату: {e}")
        return None


def get_first_datetime():
    """Первая datetime в таблице — для бэкфилла исторических колонок."""
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
    Качает OHLCV для всех тикеров из ASSETS.
    Возвращает wide DataFrame с колонками: {Name}_{Close/High/Low/Open/Volume}
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
        print(f"❌ Ошибка Yahoo Finance: {e}")
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


# ── Бэкфилл: заполнение исторических High/Low/Open ───────────────────────────

def backfill_missing_columns():
    """
    Проверяет, есть ли строки где High/Low/Open = NULL.
    Если есть — качает историю с самого начала таблицы и обновляет через UPDATE.
    Вызывается один раз после migrate_add_columns().
    """
    # Проверяем нужен ли бэкфилл (есть ли NULL в первой попавшейся High-колонке)
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

    first_dt = get_first_datetime()
    if not first_dt:
        print("  ⚠️  Не удалось получить начальную дату, пропускаем бэкфилл")
        return

    start = first_dt - datetime.timedelta(days=1)
    print(f"  [*] Скачиваем историю с {start.date()} для бэкфилла...")

    hist_df = download_ohlcv(start_date=start)
    if hist_df.empty:
        print("  ⚠️  Не удалось скачать историю для бэкфилла")
        return

    # Строим список колонок для UPDATE
    update_cols = []
    for name in ASSETS:
        for suffix in ('High', 'Low', 'Open'):
            col = f'{name}_{suffix}'
            if col in hist_df.columns:
                update_cols.append(col)

    if not update_cols:
        print("  ⚠️  Нет колонок для обновления")
        return

    print(f"  [*] Обновляем {len(update_cols)} колонок по {len(hist_df)} строкам...")

    # Обновляем батчами по 500 строк
    hist_df = hist_df.reset_index()  # datetime становится колонкой
    hist_df = hist_df.rename(columns={'index': 'datetime', 'Datetime': 'datetime'})

    batch_size = 500
    updated = 0
    with engine.begin() as conn:
        for i in range(0, len(hist_df), batch_size):
            batch = hist_df.iloc[i:i + batch_size]
            for _, row in batch.iterrows():
                dt = row.get('datetime')
                if dt is None:
                    continue
                set_parts = []
                values = {'dt': dt}
                for col in update_cols:
                    val = row.get(col)
                    if val is not None and not pd.isna(val):
                        set_parts.append(f"`{col}` = :{col.replace(' ', '_')}")
                        values[col.replace(' ', '_')] = float(val)
                if not set_parts:
                    continue
                sql = f"UPDATE `{TABLE_NAME}` SET {', '.join(set_parts)} WHERE `datetime` = :dt"
                conn.execute(text(sql), values)
                updated += 1

    print(f"  ✅ Бэкфилл завершён: обновлено {updated} строк")


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

    # 1. Миграция: добавляем новые колонки если их нет
    print("[1] Проверка структуры таблицы...")
    migrate_add_columns()

    # 2. Бэкфилл: заполняем историческими High/Low/Open уже существующие строки
    print("\n[2] Бэкфилл исторических данных...")
    backfill_missing_columns()

    # 3. Инкрементальное обновление — как раньше
    print("\n[3] Обновление свежих данных...")
    last_dt = get_last_datetime()
    start_date = (last_dt - datetime.timedelta(days=1)) if last_dt else None

    df_market = download_ohlcv(start_date=start_date)
    if df_market.empty:
        print("  ⚠️  Нет новых рыночных данных")
        sys.exit(0)

    # Отрезаем только новые строки
    if last_dt is not None:
        df_market = df_market[df_market.index > last_dt]

    # 4. On-Chain метрики
    df_onchain = get_crypto_metrics()
    if not df_onchain.empty:
        df_market = df_market.join(df_onchain, how='left').ffill()

    save_data(df_market)
    print("\n🏁 Готово!")
