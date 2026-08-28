#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TradingView Economic Calendar -> MySQL.

Исправления относительно старой версии:
- больше НЕ фильтрует ответ API через datetime > MAX(datetime);
- каждый обычный запуск перечитывает скользящий хвост истории и будущие события;
- существующие события обновляются по ключу (datetime, Country, Title);
- Actual/Forecast/Previous не затираются NULL-ами;
- поддерживает однократный исторический backfill блоками;
- новые дубли по одному событию не создаются;
- добавляет индекс по datetime, если его ещё нет;
- время TradingView нормализуется в UTC перед записью в MySQL.

Пример обычного запуска (совместим со старой командой):
    python3 TradingView_2.py vlad_macro_calendar_events HOST 3306 USER PASS brain

Однократное восстановление истории:
    python3 TradingView_2.py vlad_macro_calendar_events HOST 3306 USER PASS brain \
        --backfill-from 2026-03-01 --backfill-to 2026-08-26
"""

import os
import sys
import re
import argparse
import datetime as dt
import traceback
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "tradingview_macro_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

DEFAULT_COUNTRIES = os.getenv("TV_CAL_COUNTRIES", "US,EU,DE,GB,CA,AU,CN,JP,RU,CH")
DEFAULT_LOOKBACK_DAYS = int(os.getenv("TV_CAL_LOOKBACK_DAYS", "7"))
DEFAULT_FUTURE_DAYS = int(os.getenv("TV_CAL_FUTURE_DAYS", "7"))
DEFAULT_CHUNK_DAYS = int(os.getenv("TV_CAL_CHUNK_DAYS", "14"))


def send_error_trace(exc: Exception, script_name: str = "TradingView_2.py"):
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
    print(f"\n[POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        import requests
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"[POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"[POST] Не удалось отправить отчёт: {e}")


def parse_iso_date(value: Optional[str], *, end_of_day: bool = False) -> Optional[dt.datetime]:
    if not value:
        return None
    parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    if end_of_day:
        return parsed + dt.timedelta(days=1)
    return parsed


def safe_table_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value or ""):
        raise ValueError(f"Недопустимое имя таблицы: {value!r}")
    return value


parser = argparse.ArgumentParser(description="TradingView Economic Calendar -> MySQL")
parser.add_argument("table_name", help="Целевая таблица, например vlad_macro_calendar_events")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                    help="Сколько прошлых дней перечитывать при обычном запуске (default: 7)")
parser.add_argument("--future-days", type=int, default=DEFAULT_FUTURE_DAYS,
                    help="Сколько будущих дней хранить в календаре (default: 7)")
parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                    help="Размер блока для исторического backfill (default: 14)")
parser.add_argument("--backfill-from", metavar="YYYY-MM-DD",
                    help="Однократно перечитать историю с этой даты")
parser.add_argument("--backfill-to", metavar="YYYY-MM-DD",
                    help="Конец backfill включительно; по умолчанию сегодня")
parser.add_argument("--countries", default=DEFAULT_COUNTRIES,
                    help=f"Страны TradingView (default: {DEFAULT_COUNTRIES})")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("Ошибка: не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

args.table_name = safe_table_name(args.table_name)
if args.lookback_days < 1 or args.future_days < 0 or args.chunk_days < 1:
    raise ValueError("lookback-days/chunk-days должны быть >= 1, future-days >= 0")

SQLALCHEMY_URL = (
    f"mysql+mysqlconnector://{args.user}:{args.password}"
    f"@{args.host}:{args.port}/{args.database}"
)


class TradingViewMacroCollector:
    KEY_COLUMNS = ("datetime", "Country", "Title")

    def __init__(self, table_name: str):
        self.table_name = safe_table_name(table_name)
        self.engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600, pool_pre_ping=True)
        self.session = crequests.Session()

    def ensure_table(self):
        """Создаёт таблицу при первом запуске и индексирует datetime."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            `id` INT NOT NULL AUTO_INCREMENT,
            `datetime` DATETIME DEFAULT NULL,
            `Country` TEXT,
            `Title` TEXT,
            `Actual` DOUBLE DEFAULT NULL,
            `Previous` DOUBLE DEFAULT NULL,
            `Forecast` DOUBLE DEFAULT NULL,
            `Importance` BIGINT DEFAULT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_sql))

        # На существующей таблице из дампа был только PRIMARY(id).
        # Индекс нужен для быстрых rolling-upsert/backfill по диапазонам дат.
        with self.engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT INDEX_NAME, COLUMN_NAME
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = :db
                  AND TABLE_NAME = :tbl
                  AND COLUMN_NAME = 'datetime'
            """), {"db": args.database, "tbl": self.table_name}).fetchall()
            if not rows:
                print(f"[*] Добавляю индекс idx_{self.table_name}_datetime ...")
                conn.execute(text(
                    f"ALTER TABLE `{self.table_name}` "
                    f"ADD INDEX `idx_{self.table_name}_datetime` (`datetime`)"
                ))

    @staticmethod
    def _parse_number(val):
        """
        Сохраняем совместимость со старой таблицей: TradingView для используемого
        endpoint обычно отдаёт уже нормализованные числовые значения.
        Не масштабируем K/M/B, чтобы не смешать единицы со старой историей.
        """
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        s = str(val).strip().replace("\u2212", "-").replace("\xa0", "")
        # Безопасные декоративные символы. Суффиксы K/M/B намеренно не пересчитываем.
        s = s.replace("%", "")
        s = s.replace(",", ".")
        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_event_datetime(value) -> Optional[dt.datetime]:
        if value is None or value == "":
            return None
        try:
            # utc=True одинаково обрабатывает Z и явные offsets.
            stamp = pd.to_datetime(value, utc=True, errors="coerce")
            if pd.isna(stamp):
                return None
            # MySQL DATETIME хранится без tz; храним именно UTC wall-clock.
            return stamp.tz_convert(None).to_pydatetime()
        except Exception:
            return None

    def get_economic_calendar(self, start_dt: dt.datetime, end_dt: dt.datetime) -> pd.DataFrame:
        print(f"[*] Загрузка календаря {start_dt:%Y-%m-%d %H:%M} -> {end_dt:%Y-%m-%d %H:%M} UTC")
        url = "https://economic-calendar.tradingview.com/events"
        params = {
            "from": start_dt.replace(microsecond=0).isoformat() + "Z",
            "to": end_dt.replace(microsecond=0).isoformat() + "Z",
            "countries": args.countries,
            "min_importance": 1,
        }
        headers = {
            "origin": "https://ru.tradingview.com",
            "referer": "https://ru.tradingview.com/",
        }

        r = self.session.get(
            url,
            params=params,
            headers=headers,
            impersonate="chrome120",
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        data = payload.get("result", []) if isinstance(payload, dict) else []
        if not data:
            return pd.DataFrame(columns=[
                "datetime", "Country", "Title", "Actual", "Previous", "Forecast", "Importance"
            ])

        rows = []
        for ev in data:
            event_dt = self._parse_event_datetime(ev.get("date"))
            country = str(ev.get("country", "") or "").strip()
            title = str(ev.get("title", "") or "").strip()
            if event_dt is None or not country or not title:
                continue

            try:
                importance = ev.get("importance", None)
                importance = int(importance) if importance is not None and str(importance) != "" else None
            except (ValueError, TypeError):
                importance = None

            rows.append({
                "datetime": event_dt,
                "Country": country,
                "Title": title,
                "Actual": self._parse_number(ev.get("actual")),
                "Previous": self._parse_number(ev.get("previous")),
                "Forecast": self._parse_number(ev.get("forecast")),
                "Importance": importance,
            })

        if not rows:
            return pd.DataFrame(columns=[
                "datetime", "Country", "Title", "Actual", "Previous", "Forecast", "Importance"
            ])

        df = pd.DataFrame(rows)
        df = self._collapse_api_duplicates(df)
        df.sort_values(["datetime", "Country", "Title"], inplace=True, kind="stable")
        return df.reset_index(drop=True)

    @staticmethod
    def _collapse_api_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """Если API вернул один event несколько раз, оставляем наиболее полный вариант."""
        if df.empty:
            return df
        out = df.copy()
        value_cols = ["Actual", "Previous", "Forecast", "Importance"]
        out["__completeness"] = out[value_cols].notna().sum(axis=1)
        out["__order"] = range(len(out))
        out.sort_values(
            ["datetime", "Country", "Title", "__completeness", "__order"],
            inplace=True,
            kind="stable",
        )
        out = out.drop_duplicates(subset=["datetime", "Country", "Title"], keep="last")
        return out.drop(columns=["__completeness", "__order"])

    def _existing_keys(self, start_dt: dt.datetime, end_dt: dt.datetime) -> set:
        sql = text(f"""
            SELECT `datetime`, `Country`, `Title`
            FROM `{self.table_name}`
            WHERE `datetime` >= :start_dt
              AND `datetime` <= :end_dt
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"start_dt": start_dt, "end_dt": end_dt}).fetchall()
        return {(row[0], row[1] or "", row[2] or "") for row in rows}

    @staticmethod
    def _records(df: pd.DataFrame):
        records = []
        for row in df.to_dict(orient="records"):
            clean = {}
            for key, value in row.items():
                if pd.isna(value):
                    clean[key] = None
                elif hasattr(value, "item"):
                    try:
                        clean[key] = value.item()
                    except Exception:
                        clean[key] = value
                else:
                    clean[key] = value
            records.append(clean)
        return records

    def upsert_events(self, df: pd.DataFrame) -> tuple[int, int]:
        if df.empty:
            return 0, 0

        start_dt = df["datetime"].min().to_pydatetime() if hasattr(df["datetime"].min(), "to_pydatetime") else df["datetime"].min()
        end_dt = df["datetime"].max().to_pydatetime() if hasattr(df["datetime"].max(), "to_pydatetime") else df["datetime"].max()
        existing = self._existing_keys(start_dt, end_dt)

        inserts = []
        updates = []
        for row in self._records(df):
            key = (row["datetime"], row["Country"], row["Title"])
            if key in existing:
                updates.append(row)
            else:
                inserts.append(row)
                existing.add(key)  # не создать дубль внутри того же batch

        update_sql = text(f"""
            UPDATE `{self.table_name}`
            SET
                `Actual` = COALESCE(:Actual, `Actual`),
                `Previous` = COALESCE(:Previous, `Previous`),
                `Forecast` = COALESCE(:Forecast, `Forecast`),
                `Importance` = COALESCE(:Importance, `Importance`)
            WHERE `datetime` = :datetime
              AND `Country` = :Country
              AND `Title` = :Title
        """)
        insert_sql = text(f"""
            INSERT INTO `{self.table_name}`
                (`datetime`, `Country`, `Title`, `Actual`, `Previous`, `Forecast`, `Importance`)
            VALUES
                (:datetime, :Country, :Title, :Actual, :Previous, :Forecast, :Importance)
        """)

        with self.engine.begin() as conn:
            if updates:
                # UPDATE затронет и старые дубли одного ключа одинаково, но новых дублей не создаст.
                conn.execute(update_sql, updates)
            if inserts:
                conn.execute(insert_sql, inserts)

        print(f"   -> Обновлено существующих событий: {len(updates)}")
        print(f"   -> Добавлено новых событий:       {len(inserts)}")
        return len(updates), len(inserts)

    def process_range(self, start_dt: dt.datetime, end_dt: dt.datetime) -> tuple[int, int, int]:
        df = self.get_economic_calendar(start_dt, end_dt)
        if df.empty:
            print("   -> TradingView не вернул событий в этом диапазоне")
            return 0, 0, 0
        updated, inserted = self.upsert_events(df)
        return len(df), updated, inserted

    def run_live(self):
        now = dt.datetime.utcnow().replace(microsecond=0)
        start = now - dt.timedelta(days=args.lookback_days)
        end = now + dt.timedelta(days=args.future_days)
        print(
            f"[*] LIVE: перечитываем {args.lookback_days} дн. назад "
            f"и {args.future_days} дн. вперёд"
        )
        got, updated, inserted = self.process_range(start, end)
        print(f"[OK] LIVE завершён: API={got}, updated={updated}, inserted={inserted}")

    def run_backfill(self, start_dt: dt.datetime, end_dt: dt.datetime):
        if end_dt <= start_dt:
            raise ValueError("backfill-to должен быть позже backfill-from")

        print(
            f"[*] BACKFILL: {start_dt:%Y-%m-%d} -> {end_dt:%Y-%m-%d}, "
            f"chunk={args.chunk_days} дней"
        )
        cursor = start_dt
        total_api = total_updated = total_inserted = 0
        chunk_no = 0

        while cursor < end_dt:
            chunk_no += 1
            chunk_end = min(cursor + dt.timedelta(days=args.chunk_days), end_dt)
            # Час overlap на границе безопасен: upsert не создаёт дублей.
            request_start = cursor if chunk_no == 1 else cursor - dt.timedelta(hours=1)
            print(f"\n=== BACKFILL chunk {chunk_no}: {request_start} -> {chunk_end} ===")
            got, updated, inserted = self.process_range(request_start, chunk_end)
            total_api += got
            total_updated += updated
            total_inserted += inserted
            cursor = chunk_end

        print(
            f"\n[OK] BACKFILL завершён: API={total_api}, "
            f"updated={total_updated}, inserted={total_inserted}"
        )

    def run(self):
        self.ensure_table()

        if args.backfill_from:
            start = parse_iso_date(args.backfill_from)
            if args.backfill_to:
                end = parse_iso_date(args.backfill_to, end_of_day=True)
            else:
                end = dt.datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
            self.run_backfill(start, end)
        else:
            self.run_live()


def main():
    collector = TradingViewMacroCollector(args.table_name)
    collector.run()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
    except Exception as e:
        print(f"\nКритическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
