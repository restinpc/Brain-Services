#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import datetime
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "vlad_macro_calendar_events"  # ← но данные — рыночные!
SQLALCHEMY_URL = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"

ASSETS = {
    'EURUSD': 'EURUSD=X',
    'BTC': 'BTC-USD',
    'ETH': 'ETH-USD',
    'DXY': 'DX-Y.NYB',
    'SP500': '^GSPC',
    'Nasdaq': '^IXIC',
    'VIX': '^VIX',
    'Oil': 'CL=F',
    'Gold': 'GC=F',
    'US10Y': '^TNX',
}

engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)

def get_last_datetime():
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(`datetime`) FROM `{TABLE_NAME}`"))
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print(f"⚠️ Не удалось получить последнюю дату: {e}")
        return None

def get_market_data(last_dt):
    print("[*] Скачивание рыночных данных (Yahoo Finance)...")
    start_date = (last_dt - datetime.timedelta(days=1)) if last_dt else None
    tickers = list(ASSETS.values())
    try:
        if start_date:
            data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), interval="1h", group_by='ticker', progress=False)
        else:
            data = yf.download(tickers, period="2y", interval="1h", group_by='ticker', progress=False)
    except Exception as e:
        print(f"❌ Ошибка Yahoo: {e}")
        return None

    dfs = {}
    for name, ticker in ASSETS.items():
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if ticker in data.columns.levels[0]:
                    df = data[ticker].copy()
                else:
                    continue
            else:
                df = data.copy()
            cols_map = {}
            if 'Close' in df.columns: cols_map['Close'] = f'{name}_Close'
            if 'Volume' in df.columns: cols_map['Volume'] = f'{name}_Volume'
            if not cols_map:
                continue
            df = df.rename(columns=cols_map)[list(cols_map.values())]
            df.index = pd.to_datetime(df.index).tz_localize(None)
            dfs[name] = df
        except Exception:
            continue

    if not dfs:
        return None

    full_df = pd.concat(dfs.values(), axis=1)
    full_df.sort_index(inplace=True)
    full_df.dropna(how='all', inplace=True)
    if last_dt:
        full_df = full_df[full_df.index > last_dt]
    return full_df if not full_df.empty else None

def get_crypto_metrics():
    print("[*] Скачивание On-Chain метрик...")
    try:
        r = crequests.get("https://api.blockchain.info/charts/hash-rate?timespan=2years&format=json", timeout=10)
        df = pd.DataFrame(r.json()['values'])
        df['x'] = pd.to_datetime(df['x'], unit='s')
        df.set_index('x', inplace=True)
        df.columns = ['BTC_Hashrate']
        return df.resample('1h').ffill()
    except Exception:
        return pd.DataFrame()

def save_data(df_matrix):
    if df_matrix.empty:
        return
    try:
        df_matrix.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='append',
            index=True,
            index_label='datetime',
            chunksize=1000,
            method='multi'
        )
        print(f"✅ Добавлено {len(df_matrix)} строк в '{TABLE_NAME}'")
    except Exception as e:
        print(f"❌ Ошибка записи: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print(f"🚀 Загрузка рыночных данных в: {TABLE_NAME}")
    last_dt = get_last_datetime()
    df_market = get_market_data(last_dt)
    if df_market is None:
        print("⚠️ Нет рыночных данных")
        sys.exit(0)
    df_onchain = get_crypto_metrics()
    final_df = df_market.join(df_onchain, how='left').ffill() if not df_onchain.empty else df_market
    save_data(final_df)
    print("🏁 Готово!")