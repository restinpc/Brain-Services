#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import pandas as pd
import yfinance as yf
import mysql.connector
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
import traceback
from dotenv import load_dotenv

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "tradingview_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception, script_name: str = "TradingView.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {
        "url": "cli_script",
        "node": NODE_NAME,
        "email": EMAIL,
        "logs": logs,
    }
    print(f"\n📤 [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        import requests
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"✅ [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")

# === Аргументы командной строки + .env fallback ===
parser = argparse.ArgumentParser(description="TradingView Data Collector → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

SQLALCHEMY_URL = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"

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

class TradingViewCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)

    def get_last_datetime(self) -> datetime.datetime | None:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(`datetime`) FROM `{self.table_name}`"))
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            print(f"   ⚠️ Не удалось получить последнюю дату из {self.table_name}: {e}")
            return None

    def get_market_data(self, last_dt: datetime.datetime | None) -> pd.DataFrame | None:
        print("[*] Скачивание рыночных данных (Yahoo Finance)...")
        if last_dt:
            start_date = last_dt - datetime.timedelta(days=1)
            period_str = None
            start_str = start_date.strftime('%Y-%m-%d')
        else:
            period_str = "2y"
            start_str = None
        tickers = list(ASSETS.values())
        try:
            if period_str:
                data = yf.download(tickers, period=period_str, interval="1h", group_by='ticker', progress=False)
            else:
                data = yf.download(tickers, start=start_str, interval="1h", group_by='ticker', progress=False)
        except Exception as e:
            print(f"   -> Ошибка Yahoo: {e}")
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

    def get_crypto_metrics(self) -> pd.DataFrame | None:
        print("[*] Скачивание On-Chain метрик...")
        metrics = {}
        try:
            url = "https://api.blockchain.info/charts/hash-rate?timespan=2years&format=json"
            r = crequests.get(url)
            df = pd.DataFrame(r.json()['values'])
            df['x'] = pd.to_datetime(df['x'], unit='s')
            df.set_index('x', inplace=True)
            df.columns = ['BTC_Hashrate']
            metrics['Hashrate'] = df.resample('1h').ffill()
        except Exception:
            pass
        if metrics:
            return pd.concat(metrics.values(), axis=1)
        return pd.DataFrame()

    def save_market_data_incremental(self, df_matrix: pd.DataFrame):
        if df_matrix.empty:
            return
        try:
            df_matrix.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists='append',
                index=True,
                index_label='datetime',
                chunksize=1000,
                method='multi'
            )
            print(f"   -> Матрица: добавлено {len(df_matrix)} строк в '{self.table_name}'")
        except Exception as e:
            print(f"   -> Ошибка записи матрицы: {e}")

def main():
    collector = TradingViewCollector(args.table_name)
    last_dt = collector.get_last_datetime()
    df_market = collector.get_market_data(last_dt)
    df_onchain = collector.get_crypto_metrics()
    if df_market is not None:
        if not df_onchain.empty:
            final_df = df_market.join(df_onchain, how='left').ffill()
        else:
            final_df = df_market
        collector.save_market_data_incremental(final_df)
    print("✅ Готово! Данные добавлены в базу.")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)