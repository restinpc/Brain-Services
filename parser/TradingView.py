import os
import datetime
import pandas as pd
import yfinance as yf
import mysql.connector
from sqlalchemy import create_engine
from curl_cffi import requests as crequests
from dotenv import load_dotenv

load_dotenv()

# --- ТЕ ЖЕ НАСТРОЙКИ ---
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


class MLDataToSQL:
    def __init__(self):
        # Используем SQLAlchemy для удобной записи DataFrame в MySQL
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}")

    def get_market_data(self):
        print("[*] Скачивание рыночных данных (Yahoo Finance)...")
        tickers = list(ASSETS.values())

        try:
            # Для продакшна в БД берем 1 месяц, чтобы не перегружать каждый раз
            # Если нужно залить историю с нуля - поставьте "2y"
            data = yf.download(tickers, period="2y", interval="1h", group_by='ticker', progress=False)
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

                cols = {}
                if 'Close' in df.columns: cols['Close'] = f'{name}_Close'
                if 'Volume' in df.columns: cols['Volume'] = f'{name}_Volume'

                if not cols: continue

                df = df.rename(columns=cols)[list(cols.values())]
                df.index = df.index.tz_localize(None)
                dfs[name] = df
            except Exception:
                continue

        if not dfs: return None

        full_df = pd.concat(dfs.values(), axis=1)
        full_df.sort_index(inplace=True)
        full_df.dropna(how='all', inplace=True)

        return full_df

    def get_crypto_metrics(self):
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

    def get_economic_calendar(self):
        print("[*] Скачивание календаря событий...")
        url = "https://economic-calendar.tradingview.com/events"
        payload = {
            "from": (datetime.datetime.now() - datetime.timedelta(days=730)).isoformat() + "Z",
            "to": datetime.datetime.now().isoformat() + "Z",
            "countries": "US,EU,DE",
            "min_importance": "1"
        }
        headers = {'origin': 'https://ru.tradingview.com', 'referer': 'https://ru.tradingview.com/'}

        try:
            r = crequests.get(url, params=payload, headers=headers, impersonate="chrome120")
            data = r.json().get('result', [])

            rows = []
            for i in data:
                rows.append({
                    'datetime': pd.to_datetime(i['date']).replace(tzinfo=None),
                    'Country': i['country'],
                    'Title': i['title'],
                    'Actual': i['actual'],
                    'Previous': i['previous'],
                    'Forecast': i['forecast'],
                    'Importance': i['importance']
                })
            return pd.DataFrame(rows)
        except Exception:
            return pd.DataFrame()

    def save_to_mysql(self, df_matrix, df_events):
        print(f"[*] Запись в MySQL (DB: {self.engine.url.database})...")

        # 1. Сохраняем МАТРИЦУ (Training Set)
        # Таблица: vlad_ml_training_set
        # index=True сохранит колонку datetime как индекс
        try:
            df_matrix.to_sql(
                name='vlad_market_history',
                con=self.engine,
                if_exists='replace',  # ПЕРЕЗАПИСЫВАЕМ таблицу полностью (самый надежный способ для матрицы)
                index=True,
                index_label='datetime',
                chunksize=1000
            )
            print(f"   -> Матрица: {len(df_matrix)} строк загружено в 'vlad_market_history'")
        except Exception as e:
            print(f"   -> Ошибка записи матрицы: {e}")

        # 2. Сохраняем КАЛЕНДАРЬ
        # Таблица: vlad_ml_events_log
        try:
            if not df_events.empty:
                df_events.to_sql(
                    name='vlad_macro_calendar_events',
                    con=self.engine,
                    if_exists='replace',
                    index=False,
                    chunksize=1000
                )
                print(f"   -> Календарь: {len(df_events)} событий загружено в 'vlad_macro_calendar_events'")
        except Exception as e:
            print(f"   -> Ошибка записи календаря: {e}")


# --- ЗАПУСК ---
if __name__ == "__main__":
    bot = MLDataToSQL()

    # 1. Собираем данные (как раньше)
    df_market = bot.get_market_data()
    df_onchain = bot.get_crypto_metrics()

    if df_market is not None:
        # Склеиваем матрицу
        if not df_onchain.empty:
            final_df = df_market.join(df_onchain, how='left').ffill()
        else:
            final_df = df_market

        # Качаем события
        df_events = bot.get_economic_calendar()

        # 2. ПИШЕМ В MYSQL
        bot.save_to_mysql(final_df, df_events)

    print("✅ Готово! Данные в базе.")
