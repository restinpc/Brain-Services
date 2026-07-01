#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
import traceback
from dotenv import load_dotenv

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "tradingview_macro_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception, script_name: str = "TradingViewMacro.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {
        "url": "cli_script",
        "node": NODE_NAME,
        "email": EMAIL,
        "logs": logs,
    }
    print(f"\n [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        import requests
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f" [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f" [POST] Не удалось отправить отчёт: {e}")

# === Аргументы командной строки + .env fallback ===
parser = argparse.ArgumentParser(description="TradingView Economic Calendar → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД (например, vlad_macro_calendar_events)")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

SQLALCHEMY_URL = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"

class TradingViewMacroCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)
        self.session = crequests.Session()

    def get_last_datetime(self) -> datetime.datetime | None:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(`datetime`) FROM `{self.table_name}`"))
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            print(f"    Не удалось получить последнюю дату из {self.table_name}: {e}")
            return None

    def get_economic_calendar(self, start_dt: datetime.datetime, end_dt: datetime.datetime) -> pd.DataFrame:
        print(f"[*] Загрузка календаря с {start_dt.date()} по {end_dt.date()}...")
        url = "https://economic-calendar.tradingview.com/events"

        # TradingView ожидает ISO 8601 с 'Z' на конце
        params = {
            "from": start_dt.isoformat() + "Z",
            "to": end_dt.isoformat() + "Z",
            "countries": "US,EU,DE,GB,CA,AU,CN,JP,RU,CH",  # можно расширить
            "min_importance": 1,  # 1 = low, 2 = medium, 3 = high → но API принимает 1 как минимум
        }

        headers = {
            "origin": "https://ru.tradingview.com",
            "referer": "https://ru.tradingview.com/",
        }

        try:
            r = self.session.get(url, params=params, headers=headers, impersonate="chrome120", timeout=15)
            r.raise_for_status()
            data = r.json().get('result', [])
            if not data:
                return pd.DataFrame()

            rows = []
            for ev in data:
                rows.append({
                    "datetime": pd.to_datetime(ev['date']).tz_localize(None),
                    "Country": ev.get('country', ''),
                    "Title": ev.get('title', ''),
                    "Actual": self._parse_number(ev.get('actual')),
                    "Previous": self._parse_number(ev.get('previous')),
                    "Forecast": self._parse_number(ev.get('forecast')),
                    "Importance": int(ev.get('importance', -1)),
                })
            df = pd.DataFrame(rows)
            df.sort_values('datetime', inplace=True)
            return df.reset_index(drop=True)
        except Exception as e:
            print(f"   → Ошибка при загрузке календаря: {e}")
            return pd.DataFrame()

    @staticmethod
    def _parse_number(val):
        if val is None or val == "":
            return None
        try:
            return float(str(val).replace(',', '.'))
        except (ValueError, TypeError):
            return None

    def save_incremental(self, df: pd.DataFrame):
        if df.empty:
            return
        try:
            df.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=500,
                method='multi'
            )
            print(f"   → Добавлено {len(df)} строк в '{self.table_name}'")
        except Exception as e:
            print(f"   → Ошибка записи: {e}")
            raise

    def run(self):
        last_dt = self.get_last_datetime()
        now = datetime.datetime.utcnow()

        if last_dt:
            # Загружаем с момента последней записи минус 1 день (на случай пропущенных)
            start = last_dt - datetime.timedelta(days=1)
        else:
            # Первый запуск — последние 2 года
            start = now - datetime.timedelta(days=730)

        end = now + datetime.timedelta(days=1)  # включая будущие события

        df = self.get_economic_calendar(start, end)

        if not df.empty:
            # Фильтруем только новые события (строго > last_dt)
            if last_dt:
                df = df[df['datetime'] > last_dt]
            if not df.empty:
                self.save_incremental(df)
                print(" Готово! Новые события добавлены.")
            else:
                print("ℹ Новых событий нет.")
        else:
            print("ℹ Нет данных от TradingView.")

def main():
    collector = TradingViewMacroCollector(args.table_name)
    collector.run()

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n Прервано пользователем")
    except Exception as e:
        print(f"\n Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)