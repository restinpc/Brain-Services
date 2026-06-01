#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
atomic_extremum_model.py  —  Атомарная модель: Экстремумный анализ  v2

ИДЕЯ:
  Каждый датасет получает индекс (0..N).
  Каждый числовой сигнал дискретизируется в 5 квинтильных бакетов (0=очень_низкий … 4=очень_высокий).
  Комбинация бакетов всех сигналов на каждую дату = «состояние рынка» (fingerprint).
  Детектируем локальные экстремумы BTC / ETH / EUR по скользящему окну.
  Считаем: насколько каждое состояние (или отдельный бакет) чаще встречалось
  в N-дневном окне ДО экстремума, чем в обычные дни (lift).

ДАТАСЕТЫ (34 штуки, idx 0..33):
   0  sasha_fred_vixcls            — VIX индекс волатильности
   1  sasha_fred_dff               — Ставка ФРС (Fed Funds Rate)
   2  sasha_fred_dgs10             — Доходность 10-летних US Treasuries
   3  sasha_fred_dexuseu           — EUR/USD (FRED spot)
   4  sasha_fred_t10yie            — 10-летний breakeven inflation
   5  sasha_fred_cbbtcusd          — BTC/USD (Coinbase via FRED)
   6  sasha_alpha_daily_btcusd_ind — BTC технические индикаторы (Alpha Vantage)
   7  sasha_alpha_daily_ethusd_ind — ETH технические индикаторы
   8  sasha_alpha_daily_eurusd_ind — EUR/USD технические индикаторы
   9  vlad_fear_greed_index        — Crypto Fear & Greed Index
  10  vlad_treasury_nominal_yield  — Номинальная кривая доходности (Treasury.gov)
  11  vlad_treasury_real_yield     — Реальная кривая доходности (Treasury.gov)
  12  vlad_coingecko_stablecoin_history — Стейблкоин peg (USDT/USDC/DAI)
  13  vlad_market_history          — Рыночные цены (BTC/ETH/SP500/VIX часовые→дневные)
  14  vlad_investing_calendar      — Экономический календарь (сюрпризы, пересмотры)
  15  vlad_ecb_exchange_rates      — Курсы ЕЦБ (pivot по валюте)
  16  brain_calendar               — Brain calendar (агрегация событий + ImpactValue)
  17  brain_llm                    — LLM sentiment по валютам (BTC/ETH/USD/EUR)
  18  sasha_imf_international_reserves — МВФ международные резервы (IRFCL)
  19  vlad_usgs_earthquake         — Сейсмические события (max_magnitude, count)
  20  vlad_tr_daily_treasury_statement_all — Ежедневный баланс Казначейства США
  21  vlad_tr_buybacks_security_details    — Buyback ценных бумаг Казначейства
  22  sasha_btc_daily_*            — On-chain BTC (active_addresses/fee/tx/volume)
  23  sasha_eth_daily_*            — On-chain ETH (active_addresses/burned/fee/gas/tx)
  24  vlad_ucdp                    — Геополитические конфликты (UCDP GED, с 1989)
  25  vlad_macro_calendar_events   — Макро-календарь actual/forecast (с 2024)
  26  brain_news_signals           — Именованные числовые сигналы Brain
  27  vlad_polymarket              — Рынки предсказаний (Polymarket, с 2026)
  28  brain_fng                    — Fear & Greed из brain DB (второй источник)
  29  brain_tgd_ner                — NER Telegram (person/location/misc)
  30  brain_cnn_ner                — NER CNN
  31  brain_nyt_ner                — NER New York Times
  32  brain_twp_ner                — NER Washington Post
  33  brain_wsj_ner                — NER Wall Street Journal

ВЫХОДНЫЕ ТАБЛИЦЫ (vlad DB):
  vlad_atomic_registry        — реестр датасетов (idx 0..N, name, db)
  vlad_atomic_extremums       — даты локальных MAX/MIN цен
  vlad_atomic_feature_matrix  — плоская матрица бакетов (вход для NN)
  vlad_atomic_cooccurrence    — lift (feature, bucket) при каждом типе экстремума
  vlad_atomic_combinations    — уникальные комбинации состояний + lift (BTC_MAX/MIN)

ЗАПУСК:
  python atomic_extremum_model.py [host] [port] [user] [password] [vlad_database]

ОПЦИИ:
  --brain-db   DB     база с brain_* таблицами (default: brain)
  --window     N      полушина окна для поиска экстремума, дни (default: 7)
  --lookback   N      на сколько дней смотрим назад от экстремума (default: 21)
  --min-date   DATE   начало истории (default: 2020-01-01)
"""

import os, sys, argparse, hashlib, traceback, warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ══════════════════════════════════════════════════════════════════
#  Конфигурация
# ══════════════════════════════════════════════════════════════════
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "atomic_extremum")
EMAIL     = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")
N_BUCKETS = 5        # квинтили 0..4

# Глобальные конфиги подключения — заполняются либо через configure(), либо через CLI.
# Позволяет импортировать модуль без argparse (context_idx.py вызывает subprocess).
CFG_VLAD:  dict = {}
CFG_BRAIN: dict = {}


def configure(cfg_vlad: dict, cfg_brain: dict) -> None:
    """Программная установка конфигов подключения (для импорта без CLI)."""
    global CFG_VLAD, CFG_BRAIN
    CFG_VLAD  = cfg_vlad
    CFG_BRAIN = cfg_brain


def configure_from_env() -> None:
    """
    Читает параметры подключения из .env — точно так же как Brain Framework.

    Переменные:
      DB_HOST      — хост MySQL                (default: 127.0.0.1)
      DB_PORT      — порт MySQL                (default: 3306)
      DB_USER      — пользователь              (default: root)
      DB_PASSWORD  — пароль
      DB_NAME      — база vlad_* таблиц        (default: vlad)
      MASTER_NAME  — база sasha_*/brain_* таблиц (default: brain)
    """
    host      = os.getenv("DB_HOST",      "127.0.0.1")
    port      = int(os.getenv("DB_PORT",  "3306"))
    user      = os.getenv("DB_USER",      "root")
    password  = os.getenv("DB_PASSWORD",  "")
    vlad_db   = os.getenv("DB_NAME",      "vlad")    # vlad_* источники + vlad_atomic_* вывод
    brain_db  = os.getenv("MASTER_NAME",  "brain")   # sasha_*, brain_* источники
    base      = dict(host=host, port=port, user=user, password=password)
    configure(
        cfg_vlad  = {**base, "database": vlad_db},
        cfg_brain = {**base, "database": brain_db},
    )

# ══════════════════════════════════════════════════════════════════
#  Реестр датасетов
#  idx       → уникальный индекс (тот самый «проиндексировать датасеты»)
#  name      → имя таблицы MySQL
#  db        → "vlad" или "brain"
#  date_col  → столбец с датой
#  value_cols→ список числовых столбцов (None = автообнаружение)
#  pivot_col → для таблиц «несколько строк на дату» — pivot по этому столбцу
#  pivot_vals→ значения pivot_col, которые сохраняем
#  special   → нестандартный загрузчик
# ══════════════════════════════════════════════════════════════════
REGISTRY = [
    # ── FRED однострочные серии ────────────────────────────────────────
    dict(idx=0,  name="sasha_fred_vixcls",                 db="brain", date_col="date_iso",     value_cols=["value"]),
    dict(idx=1,  name="sasha_fred_dff",                    db="brain", date_col="date_iso",     value_cols=["value"]),
    dict(idx=2,  name="sasha_fred_dgs10",                  db="brain", date_col="date_iso",     value_cols=["value"]),
    dict(idx=3,  name="sasha_fred_dexuseu",                db="brain", date_col="date_iso",     value_cols=["value"]),
    dict(idx=4,  name="sasha_fred_t10yie",                 db="brain", date_col="date_iso",     value_cols=["value"]),
    dict(idx=5,  name="sasha_fred_cbbtcusd",               db="brain", date_col="date_iso",     value_cols=["value"]),
    # ── Alpha Vantage технические индикаторы ──────────────────────────
    dict(idx=6,  name="sasha_alpha_daily_btcusd_ind",      db="brain", date_col="datetime_iso", value_cols=["sma_20","ema_12","ema_26","rsi_14","bb_middle","bb_upper","bb_lower"]),
    dict(idx=7,  name="sasha_alpha_daily_ethusd_ind",      db="brain", date_col="datetime_iso", value_cols=["sma_20","ema_12","ema_26","rsi_14","bb_middle","bb_upper","bb_lower"]),
    dict(idx=8,  name="sasha_alpha_daily_eurusd_ind",      db="brain", date_col="datetime_iso", value_cols=["sma_20","ema_12","ema_26","rsi_14","bb_middle","bb_upper","bb_lower"]),
    # ── Crypto Fear & Greed ────────────────────────────────────────────
    dict(idx=9,  name="vlad_fear_greed_index",             db="vlad", date_col="record_date",  value_cols=["value"]),
    # ── Treasury кривые доходности (динамические столбцы) ─────────────
    dict(idx=10, name="vlad_treasury_nominal_yield",       db="vlad", date_col="record_date",  value_cols=None),
    dict(idx=11, name="vlad_treasury_real_yield",          db="vlad", date_col="record_date",  value_cols=None),
    # ── CoinGecko Stablecoins (pivot по coin symbol) ───────────────────
    dict(idx=12, name="vlad_coingecko_stablecoin_history", db="vlad", date_col="record_date",
         value_cols=["price", "peg_deviation"],
         pivot_col="symbol", pivot_vals=["USDT", "USDC", "DAI"]),
    # ── vlad_market_history (hourly → daily, источник BTC/ETH цен) ────
    dict(idx=13, name="vlad_market_history",               db="vlad", date_col="datetime",
         value_cols=["BTC_Close","ETH_Close","EURUSD_Close","VIX_Close","SP500_Close","Nasdaq_Close"],
         special="market_history"),
    # ── Инвест-календарь (агрегация по дате + сюрпризы) ───────────────
    dict(idx=14, name="vlad_investing_calendar",           db="vlad", date_col=None,
         special="investing_cal"),
    # ── ECB курсы (pivot по currency) ─────────────────────────────────
    dict(idx=15, name="vlad_ecb_exchange_rates",           db="vlad", date_col=None,
         special="ecb_rates"),
    # ── brain_calendar (агрегация событий по дате) ────────────────────
    dict(idx=16, name="brain_calendar",                    db="brain", date_col=None,
         special="brain_calendar"),
    # ── brain_llm (средний sentiment по дате и валюте) ────────────
    dict(idx=17, name="brain_llm",                        db="brain", date_col=None,
         special="brain_llm"),
    # ── IMF International Reserves (IRFCL, pivot по series_key) ──
    dict(idx=18, name="sasha_imf_international_reserves", db="brain",  date_col=None,
         special="imf"),
    # ── USGS Earthquake (дневная сейсмическая активность) ─────────
    dict(idx=19, name="vlad_usgs_earthquakes",            db="vlad",  date_col=None,
         special="usgs_earthquake"),
    # ── Treasury Daily Statement (ежедневный баланс Казначейства) ─
    dict(idx=20, name="vlad_tr_daily_treasury_statement_all", db="vlad", date_col=None,
         special="treasury_table"),
    # ── Treasury Buybacks (детали выкупа ценных бумаг) ───────────
    dict(idx=21, name="vlad_tr_buybacks_security_details", db="vlad", date_col=None,
         special="treasury_table"),
    # ── On-chain BTC (4 таблицы: active_addresses/fee/tx_count/volume) ─
    dict(idx=22, name="sasha_btc_daily_*",  db="brain", date_col=None,
         special="btc_onchain"),
    # ── On-chain ETH (5 таблиц: active_addresses/burned/fee/gas/tx) ────
    dict(idx=23, name="sasha_eth_daily_*",  db="brain", date_col=None,
         special="eth_onchain"),
    # ── UCDP конфликты (геополитический риск с 1989) ─────────────────
    dict(idx=24, name="vlad_ucdp",           db="vlad", date_col=None,
         special="ucdp"),
    # ── Макро-календарь (actual/forecast с 2024) ──────────────────────
    dict(idx=25, name="vlad_macro_calendar_events", db="vlad", date_col=None,
         special="macro_calendar"),
    # ── Brain News Signals (именованные числовые сигналы) ─────────────
    dict(idx=26, name="brain_news_signals",  db="brain", date_col=None,
         special="news_signals"),
    # ── Polymarket (рынки предсказаний) ───────────────────────────────
    dict(idx=27, name="vlad_polymarket",     db="vlad", date_col=None,
         special="polymarket"),
    # ── Brain Fear & Greed (второй источник FnG) ─────────────────────
    dict(idx=28, name="brain_fng",           db="brain", date_col=None,
         special="brain_fng"),
    # ── NER (named entity recognition по источникам новостей) ─────────
    dict(idx=29, name="brain_tgd_ner",  db="brain", date_col=None, special="ner"),
    dict(idx=30, name="brain_cnn_ner",  db="brain", date_col=None, special="ner"),
    dict(idx=31, name="brain_nyt_ner",  db="brain", date_col=None, special="ner"),
    dict(idx=32, name="brain_twp_ner",  db="brain", date_col=None, special="ner"),
    dict(idx=33, name="brain_wsj_ner",  db="brain", date_col=None, special="ner"),
]


# ══════════════════════════════════════════════════════════════════
#  Вспомогательные утилиты
# ══════════════════════════════════════════════════════════════════

def send_error_trace(exc, script="atomic_extremum_model.py"):
    import threading
    try:
        import requests as req
    except ImportError:
        return
    logs = f"Node: {NODE_NAME}\nScript: {script}\n{repr(exc)}\n\n{traceback.format_exc()}"
    threading.Thread(
        target=lambda: req.post(TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
            timeout=10),
        daemon=True
    ).start()


def mk_conn(cfg):
    return mysql.connector.connect(**cfg)


def table_exists(cfg, name):
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute("SHOW TABLES LIKE %s", (name,))
        ok = bool(cur.fetchone())
        cur.close(); c.close()
        return ok
    except Exception:
        return False


def resolve_cfg(table_name: str, preferred_cfg: dict) -> dict:
    """
    Возвращает конфиг той БД, где таблица реально существует.
    Сначала проверяет preferred_cfg, при неудаче — другую базу.
    Это нужно потому что некоторые vlad_* таблицы физически
    находятся в brain DB, и наоборот.
    """
    if table_exists(preferred_cfg, table_name):
        return preferred_cfg
    fallback = CFG_BRAIN if preferred_cfg is CFG_VLAD else CFG_VLAD
    if table_exists(fallback, table_name):
        return fallback
    return preferred_cfg   # вернём preferred, load-функция сама выдаст SKIP


def numeric_columns(cfg, table, exclude=None):
    """Возвращает список числовых столбцов таблицы через SHOW COLUMNS."""
    excl = set(exclude or [])
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        rows = cur.fetchall(); cur.close(); c.close()
        return [r[0] for r in rows
                if r[0] not in excl
                and any(t in r[1].lower() for t in ("int", "float", "double", "decimal"))]
    except Exception:
        return []


def safe_col(name: str, max_len: int = 64) -> str:
    """Санитизация имени для MySQL (без точек, пробелов, тире)."""
    return name.replace(".", "_").replace("-", "_").replace(" ", "_")[:max_len]


def _read_table(cfg, table, date_col, value_cols, min_date) -> pd.DataFrame:
    """Базовый SELECT date_col + value_cols WHERE date >= min_date."""
    cols_sql = f"`{date_col}`, " + ", ".join(f"`{c}`" for c in value_cols)
    sql = f"SELECT {cols_sql} FROM `{table}` WHERE `{date_col}` >= %s ORDER BY `{date_col}`"
    c = mk_conn(cfg); cur = c.cursor()
    cur.execute(sql, (min_date,))
    rows = cur.fetchall(); cur.close(); c.close()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=[date_col] + value_cols)
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    df = df.set_index(date_col).apply(pd.to_numeric, errors="coerce")
    # Защита от дублей дат (несколько записей на день)
    if df.index.duplicated().any():
        df = df.groupby(level=0).last()
    return df


# ══════════════════════════════════════════════════════════════════
#  Загрузчики по типам датасетов
# ══════════════════════════════════════════════════════════════════

def load_simple(entry, cfg, min_date) -> pd.DataFrame:
    """Один ряд на дату, фиксированные числовые колонки."""
    t = entry["name"]
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t} (таблица не найдена)")
        return pd.DataFrame()
    vcols = entry.get("value_cols") or numeric_columns(
        cfg, t, exclude=[entry["date_col"], "id", "loaded_at", "timestamp_unix"])
    if not vcols:
        return pd.DataFrame()
    try:
        df = _read_table(cfg, t, entry["date_col"], vcols, min_date)
        if df.empty:
            return df
        df.columns = [f"ds{entry['idx']}_{safe_col(c)}" for c in df.columns]
        print(f"  [{entry['idx']:>2}] {t}: {len(df)} строк, {len(df.columns)} сигналов")
        return df
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()


def load_pivot(entry, cfg, min_date) -> pd.DataFrame:
    """Несколько строк на дату — pivot по pivot_col."""
    t = entry["name"]
    if not table_exists(cfg, t):
        return pd.DataFrame()
    pivot_col  = entry["pivot_col"]
    pivot_vals = entry["pivot_vals"]
    vcols      = entry["value_cols"]
    ph  = ", ".join(["%s"] * len(pivot_vals))
    sql = (f"SELECT `{entry['date_col']}`, `{pivot_col}`, "
           f"{', '.join(f'`{c}`' for c in vcols)} "
           f"FROM `{t}` WHERE `{entry['date_col']}` >= %s AND `{pivot_col}` IN ({ph})")
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date, *pivot_vals))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=[entry["date_col"], pivot_col] + vcols)
    df[entry["date_col"]] = pd.to_datetime(df[entry["date_col"]]).dt.normalize()
    frames = []
    for pv in pivot_vals:
        sub = (df[df[pivot_col] == pv]
               .set_index(entry["date_col"])[vcols]
               .apply(pd.to_numeric, errors="coerce"))
        sub.columns = [f"ds{entry['idx']}_{safe_col(pv)}_{safe_col(c)}" for c in vcols]
        frames.append(sub)
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, axis=1).groupby(level=0).first()
    print(f"  [{entry['idx']:>2}] {t} (pivot {pivot_col}): {len(result)} строк")
    return result


def load_market_history(entry, cfg, min_date) -> pd.DataFrame:
    """vlad_market_history: часовые OHLCV → дневные close + производные."""
    t = entry["name"]
    if not table_exists(cfg, t):
        return pd.DataFrame()
    vcols = entry["value_cols"]
    sql = (f"SELECT `datetime`, {', '.join(f'`{c}`' for c in vcols)} "
           f"FROM `{t}` WHERE `datetime` >= %s ORDER BY `datetime`")
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["dt"] + vcols)
    df["dt"] = pd.to_datetime(df["dt"])
    df[vcols] = df[vcols].apply(pd.to_numeric, errors="coerce")
    daily = df.groupby(df["dt"].dt.normalize())[vcols].last()
    daily.index.name = "dt"
    # Добавляем доходность BTC 1d / 7d как отдельные сигналы
    if "BTC_Close" in daily.columns:
        daily["BTC_ret1d"] = daily["BTC_Close"].pct_change(1) * 100
        daily["BTC_ret7d"] = daily["BTC_Close"].pct_change(7) * 100
    daily.columns = [f"ds{entry['idx']}_{safe_col(c)}" for c in daily.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(daily)} дней (hourly→daily)")
    return daily


def load_investing_cal(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_investing_calendar → агрегация по дате.
    Реальная схема: occurrence_time_utc DATETIME,
                    importance TINYINT (1=low/2=med/3=high),
                    actual_to_forecast VARCHAR(16)  ('above'/'below'/NULL)
                    revised_to_previous VARCHAR(16) ('higher'/'lower'/NULL)
    Сигналы:
      cnt_high, cnt_med  — кол-во событий по важности
      pos_surp_high      — 'above' среди высоко-важных
      neg_surp_high      — 'below' среди высоко-важных
      pos_surp_all       — 'above' по всем событиям
      neg_surp_all       — 'below' по всем событиям
      rev_up / rev_down  — пересмотры предыдущих данных вверх/вниз
    """
    t = "vlad_investing_calendar"
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()

    # Определяем реальные столбцы
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{t}`")
        all_cols = {r[0] for r in cur.fetchall()}; cur.close(); c.close()
    except Exception:
        return pd.DataFrame()

    # Выбираем правильный date column
    if "occurrence_time_utc" in all_cols:
        date_col = "occurrence_time_utc"
    elif "datetime" in all_cols:
        date_col = "datetime"
    else:
        dc = [c for c in all_cols if "date" in c.lower()]
        if not dc:
            return pd.DataFrame()
        date_col = dc[0]

    has_imp = "importance" in all_cols
    has_atf = "actual_to_forecast" in all_cols      # VARCHAR: 'above'/'below'/NULL
    has_rtp = "revised_to_previous" in all_cols     # VARCHAR: 'higher'/'lower'/NULL

    # Строим агрегацию
    # actual_to_forecast — VARCHAR, сравниваем как строки
    agg_parts = [f"DATE(`{date_col}`) AS d"]
    if has_imp:
        agg_parts.append("SUM(importance >= 3)         AS cnt_high")
        agg_parts.append("SUM(importance  = 2)         AS cnt_med")
    if has_atf:
        agg_parts.append(
            "SUM(importance >= 3 AND LOWER(TRIM(actual_to_forecast)) = 'above') AS pos_surp_high")
        agg_parts.append(
            "SUM(importance >= 3 AND LOWER(TRIM(actual_to_forecast)) = 'below') AS neg_surp_high")
        agg_parts.append(
            "SUM(LOWER(TRIM(actual_to_forecast)) = 'above')             AS pos_surp_all")
        agg_parts.append(
            "SUM(LOWER(TRIM(actual_to_forecast)) = 'below')             AS neg_surp_all")
        # Нетто-сюрприз = (above - below) / max(cnt_high, 1) → диапазон [-1, +1]
        agg_parts.append(
            "SUM(CASE WHEN importance >= 3 THEN "
            "  CASE WHEN LOWER(TRIM(actual_to_forecast))='above' THEN 1 "
            "       WHEN LOWER(TRIM(actual_to_forecast))='below' THEN -1 "
            "       ELSE 0 END ELSE 0 END) AS net_surp_high")
    if has_rtp:
        agg_parts.append(
            "SUM(LOWER(TRIM(revised_to_previous)) = 'higher') AS rev_up")
        agg_parts.append(
            "SUM(LOWER(TRIM(revised_to_previous)) = 'lower')  AS rev_down")

    sql = (f"SELECT {', '.join(agg_parts)} FROM `{t}` "
           f"WHERE `{date_col}` >= %s "
           f"GROUP BY DATE(`{date_col}`) ORDER BY d")
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall()
        col_names = ["d"] + [p.split(" AS ")[-1].strip() for p in agg_parts[1:]]
        cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=col_names)
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
    return df


def load_ecb_rates(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_ecb_exchange_rates: (id, currency, rate_date, rate)
    Pivot → один столбец на валюту.
    """
    t = "vlad_ecb_exchange_rates"
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    # Ограничиваемся основными кросс-курсами к USD
    target_currencies = ("USD", "GBP", "JPY", "CHF", "CNY", "CAD")
    ph  = ", ".join(["%s"] * len(target_currencies))
    sql = f"SELECT rate_date, currency, rate FROM `{t}` WHERE rate_date >= %s AND currency IN ({ph}) ORDER BY rate_date"
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date, *target_currencies))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "currency", "rate"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["rate"]  = pd.to_numeric(df["rate"], errors="coerce")
    pivoted = df.pivot_table(index="date", columns="currency", values="rate", aggfunc="last")
    pivoted.columns = [f"ds{entry['idx']}_ecb_{safe_col(c)}" for c in pivoted.columns]
    print(f"  [{entry['idx']:>2}] {t} (pivot currency): {len(pivoted)} строк")
    return pivoted


def load_brain_calendar(entry, cfg_brain, min_date) -> pd.DataFrame:
    """brain_calendar → агрегация сюрпризов по дате."""
    t = "brain_calendar"
    if not table_exists(cfg_brain, t):
        print(f"  [{entry['idx']:>2}] SKIP {t} (brain DB)")
        return pd.DataFrame()
    sql = """
        SELECT DATE(FullDate) AS d,
               SUM(Importance = 'high')                                                    AS cnt_high,
               SUM(Importance = 'medium')                                                  AS cnt_med,
               SUM(Importance = 'high' AND ActualValue > ForecastValue AND ForecastValue != 0) AS pos_surp,
               SUM(Importance = 'high' AND ActualValue < ForecastValue AND ForecastValue != 0) AS neg_surp,
               AVG(CASE WHEN Importance = 'high' AND ForecastValue != 0
                        THEN ActualValue - ForecastValue END)                              AS avg_surp
        FROM `brain_calendar`
        WHERE FullDate >= %s AND ActualValue IS NOT NULL
        GROUP BY DATE(FullDate)
        ORDER BY d
    """
    try:
        c = mk_conn(cfg_brain); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА brain_calendar: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["d","cnt_high","cnt_med","pos_surp","neg_surp","avg_surp"])
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] brain_calendar: {len(df)} дней")
    return df


def load_brain_llm(entry, cfg_brain, min_date) -> pd.DataFrame:
    """brain_llm × brain_calendar → средний LLM-sentiment по (дата, валюта)."""
    if not table_exists(cfg_brain, "brain_llm") or not table_exists(cfg_brain, "brain_calendar"):
        print(f"  [{entry['idx']:>2}] SKIP brain_llm")
        return pd.DataFrame()
    sql = """
        SELECT DATE(bc.FullDate) AS d,
               AVG(CASE WHEN bl.currency = 'btc' THEN bl.result END) AS btc_sent,
               AVG(CASE WHEN bl.currency = 'eth' THEN bl.result END) AS eth_sent,
               AVG(CASE WHEN bl.currency = 'usd' THEN bl.result END) AS usd_sent,
               AVG(CASE WHEN bl.currency = 'eur' THEN bl.result END) AS eur_sent
        FROM brain_llm bl
        JOIN brain_calendar bc ON bl.news_id = bc.Id
        WHERE bc.FullDate >= %s
        GROUP BY DATE(bc.FullDate)
        ORDER BY d
    """
    try:
        c = mk_conn(cfg_brain); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА brain_llm: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["d","btc_sent","eth_sent","usd_sent","eur_sent"])
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] brain_llm: {len(df)} дней")
    return df


def load_imf(entry, cfg, min_date) -> pd.DataFrame:
    """
    sasha_imf_international_reserves: (date_iso DATE, series_key VARCHAR(255), value DOUBLE)
    Данные ежемесячные (резервы США/ЕС от МВФ IRFCL).
    Выбираем top-N серий по частоте появления, делаем pivot.
    """
    t = entry["name"]
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    # Находим самые частые series_key
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(
            f"SELECT series_key, COUNT(*) AS cnt FROM `{t}` "
            f"WHERE date_iso >= %s GROUP BY series_key ORDER BY cnt DESC LIMIT 10",
            (min_date,))
        top_series = [r[0] for r in cur.fetchall()]; cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not top_series:
        return pd.DataFrame()
    ph  = ", ".join(["%s"] * len(top_series))
    sql = (f"SELECT date_iso, series_key, value FROM `{t}` "
           f"WHERE date_iso >= %s AND series_key IN ({ph}) ORDER BY date_iso")
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date, *top_series))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "series_key", "value"])
    df["date"]  = pd.to_datetime(df["date"]).dt.normalize()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # Укорачиваем имя серии до 20 символов для колонок
    df["sk_short"] = df["series_key"].str.replace(r"[^A-Za-z0-9]", "_", regex=True).str[:20]
    # Убираем строки с пустым/NaN ключом серии
    df = df[df["sk_short"].notna() & (df["sk_short"] != "") & (df["sk_short"] != "nan")]
    if df.empty:
        return pd.DataFrame()
    pivoted = df.pivot_table(index="date", columns="sk_short", values="value", aggfunc="last")
    # Убираем NaN-колонки если они всё равно попали
    pivoted = pivoted[[c for c in pivoted.columns if str(c) != "nan" and str(c).strip() != ""]]
    pivoted.columns = [f"ds{entry['idx']}_{c}" for c in pivoted.columns]
    # Monthly → ffill до 35 дней (один месяц)
    all_days = pd.date_range(pivoted.index.min(), pivoted.index.max(), freq="D")
    pivoted  = pivoted.reindex(all_days).ffill(limit=35)
    print(f"  [{entry['idx']:>2}] {t} (IMF pivot): {len(pivoted)} дней, {len(pivoted.columns)} серий")
    return pivoted


def load_usgs_earthquake(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_usgs_earthquake → дневная агрегация сейсмической активности.
    Автоматически ищет дату и magnitude.
    Сигналы: max_magnitude, count_all, count_significant (mag >= 5)
    """
    t = entry["name"]
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{t}`")
        cols = {r[0]: r[1] for r in cur.fetchall()}; cur.close(); c.close()
    except Exception:
        return pd.DataFrame()
    # Находим столбец с датой
    date_cand = [c for c in cols if any(k in c.lower() for k in ("time", "date"))]
    if not date_cand:
        return pd.DataFrame()
    date_col  = date_cand[0]
    # Находим столбец magnitude
    mag_cand  = [c for c in cols if "mag" in c.lower()]
    mag_col   = mag_cand[0] if mag_cand else None

    if mag_col:
        sql = (f"SELECT DATE(`{date_col}`) AS d, "
               f"MAX(`{mag_col}`) AS max_mag, "
               f"COUNT(*) AS cnt_all, "
               f"SUM(`{mag_col}` >= 5) AS cnt_sig "
               f"FROM `{t}` WHERE `{date_col}` >= %s "
               f"GROUP BY DATE(`{date_col}`) ORDER BY d")
        col_names = ["d", "max_mag", "cnt_all", "cnt_sig"]
    else:
        sql = (f"SELECT DATE(`{date_col}`) AS d, COUNT(*) AS cnt_all "
               f"FROM `{t}` WHERE `{date_col}` >= %s "
               f"GROUP BY DATE(`{date_col}`) ORDER BY d")
        col_names = ["d", "cnt_all"]

    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=col_names)
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
    return df


def load_treasury_table(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_tr_daily_treasury_statement_all / vlad_tr_buybacks_security_details
    Автообнаружение числовых колонок + даты.
    """
    t = entry["name"]
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{t}`")
        all_cols_info = cur.fetchall(); cur.close(); c.close()
    except Exception:
        return pd.DataFrame()

    # Дата
    date_cands = [r[0] for r in all_cols_info if any(k in r[0].lower() for k in ("date", "time"))]
    num_cols   = [r[0] for r in all_cols_info
                  if any(tp in r[1].lower() for tp in ("int", "float", "double", "decimal", "numeric"))
                  and r[0] not in {"id", "loaded_at"}
                  and r[0] not in (date_cands or [])]
    if not date_cands or not num_cols:
        return pd.DataFrame()
    date_col = date_cands[0]
    # Берём не более 15 числовых колонок
    vcols = num_cols[:15]
    try:
        df = _read_table(cfg, t, date_col, vcols, min_date)
        if df.empty:
            return df
        df.columns = [f"ds{entry['idx']}_{safe_col(c)}" for c in df.columns]
        print(f"  [{entry['idx']:>2}] {t}: {len(df)} строк, {len(df.columns)} сигналов")
        return df
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()


def load_btc_onchain(entry, cfg, min_date) -> pd.DataFrame:
    """
    4 on-chain таблицы BTC → один датасет.
    sasha_btc_daily_active_addresses, _fee_btc, _tx_count, _volume_btc
    Схема: date_iso DATE, value FLOAT/DOUBLE
    """
    tables = {
        "sasha_btc_daily_active_addresses": "btc_active_addr",
        "sasha_btc_daily_fee_btc":          "btc_fee",
        "sasha_btc_daily_tx_count":         "btc_tx_count",
        "sasha_btc_daily_volume_btc":       "btc_volume",
    }
    frames = []
    for tbl, col_name in tables.items():
        if not table_exists(cfg, tbl):
            continue
        try:
            c = mk_conn(cfg); cur = c.cursor()
            cur.execute(f"SELECT date_iso, value FROM `{tbl}` WHERE date_iso >= %s ORDER BY date_iso", (min_date,))
            rows = cur.fetchall(); cur.close(); c.close()
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=["d", col_name])
            df["d"] = pd.to_datetime(df["d"]).dt.normalize()
            df = df.set_index("d")
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            frames.append(df)
        except Exception as e:
            print(f"  [{entry['idx']:>2}] ОШИБКА {tbl}: {e}")
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, axis=1)
    result.columns = [f"ds{entry['idx']}_{c}" for c in result.columns]
    print(f"  [{entry['idx']:>2}] BTC on-chain: {len(result)} дней, {len(result.columns)} сигналов")
    return result


def load_eth_onchain(entry, cfg, min_date) -> pd.DataFrame:
    """
    5 on-chain таблиц ETH → один датасет.
    sasha_eth_daily_active_addresses, _eth_burned, _fee_usd, _gas_used, _tx_count
    """
    tables = {
        "sasha_eth_daily_active_addresses": "eth_active_addr",
        "sasha_eth_daily_eth_burned":       "eth_burned",
        "sasha_eth_daily_fee_usd":          "eth_fee_usd",
        "sasha_eth_daily_gas_used":         "eth_gas_used",
        "sasha_eth_daily_tx_count":         "eth_tx_count",
    }
    frames = []
    for tbl, col_name in tables.items():
        if not table_exists(cfg, tbl):
            continue
        try:
            c = mk_conn(cfg); cur = c.cursor()
            cur.execute(f"SELECT date_iso, value FROM `{tbl}` WHERE date_iso >= %s ORDER BY date_iso", (min_date,))
            rows = cur.fetchall(); cur.close(); c.close()
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=["d", col_name])
            df["d"] = pd.to_datetime(df["d"]).dt.normalize()
            df = df.set_index("d")
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            frames.append(df)
        except Exception as e:
            print(f"  [{entry['idx']:>2}] ОШИБКА {tbl}: {e}")
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, axis=1)
    result.columns = [f"ds{entry['idx']}_{c}" for c in result.columns]
    print(f"  [{entry['idx']:>2}] ETH on-chain: {len(result)} дней, {len(result.columns)} сигналов")
    return result


def load_ucdp(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_ucdp — геополитические конфликты (UCDP GED).
    Схема: date_end DATE, deaths_best INT, deaths_low INT, deaths_high INT, ...
    Агрегируем по дате: суммарные жертвы + количество событий.
    Используем date_end как дату события.
    """
    t = "vlad_ucdp"
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    # Находим числовые столбцы с deaths
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{t}`")
        all_cols = {r[0]: r[1] for r in cur.fetchall()}; cur.close(); c.close()
    except Exception:
        return pd.DataFrame()

    death_cols = [c for c in all_cols if "death" in c.lower() and "best" in c.lower()]
    death_col  = death_cols[0] if death_cols else None

    if death_col:
        sql = (f"SELECT DATE(date_end) AS d, "
               f"SUM(`{death_col}`) AS deaths_total, "
               f"COUNT(*) AS conflict_events "
               f"FROM `{t}` WHERE date_end >= %s "
               f"GROUP BY DATE(date_end) ORDER BY d")
        col_names = ["d", "deaths_total", "conflict_events"]
    else:
        sql = (f"SELECT DATE(date_end) AS d, COUNT(*) AS conflict_events "
               f"FROM `{t}` WHERE date_end >= %s "
               f"GROUP BY DATE(date_end) ORDER BY d")
        col_names = ["d", "conflict_events"]

    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=col_names)
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
    return df


def load_macro_calendar(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_macro_calendar_events — макро-календарь (actual/forecast/previous).
    Схема: datetime DATETIME, actual FLOAT, forecast FLOAT, previous FLOAT, importance INT, ...
    Агрегируем по дате: avg сюрприз (actual-forecast), кол-во событий по важности.
    """
    t = "vlad_macro_calendar_events"
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{t}`")
        cols = {r[0] for r in cur.fetchall()}; cur.close(); c.close()
    except Exception:
        return pd.DataFrame()

    has_actual     = "actual" in cols
    has_forecast   = "forecast" in cols
    has_importance = "importance" in cols

    agg_parts = ["DATE(`datetime`) AS d", "COUNT(*) AS cnt_all"]
    if has_importance:
        agg_parts.append("SUM(importance >= 3) AS cnt_high")
        agg_parts.append("SUM(importance = 2)  AS cnt_med")
    if has_actual and has_forecast:
        agg_parts.append("AVG(CAST(actual AS DECIMAL(12,4)) - CAST(forecast AS DECIMAL(12,4))) AS avg_surprise")
        agg_parts.append("SUM(CAST(actual AS DECIMAL(12,4)) > CAST(forecast AS DECIMAL(12,4))) AS cnt_beat")
        agg_parts.append("SUM(CAST(actual AS DECIMAL(12,4)) < CAST(forecast AS DECIMAL(12,4))) AS cnt_miss")

    sql = (f"SELECT {', '.join(agg_parts)} FROM `{t}` "
           f"WHERE `datetime` >= %s GROUP BY DATE(`datetime`) ORDER BY d")
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall()
        col_names = ["d"] + [p.split(" AS ")[-1].strip() for p in agg_parts[1:]]
        cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=col_names)
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
    return df


def load_news_signals(entry, cfg_brain, min_date) -> pd.DataFrame:
    """
    brain_news_signals — именованные числовые сигналы.
    Схема: pair INT, code VARCHAR(20), date DATETIME, postfix VARCHAR(12), value FLOAT
    Pivot по code → числовые колонки на дату.
    Берём top-20 кодов по частоте.
    """
    t = "brain_news_signals"
    if not table_exists(cfg_brain, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    try:
        c = mk_conn(cfg_brain); cur = c.cursor()
        # top-20 самых частых code
        cur.execute(f"SELECT code, COUNT(*) AS cnt FROM `{t}` "
                    f"WHERE `date` >= %s GROUP BY code ORDER BY cnt DESC LIMIT 20",
                    (min_date,))
        top_codes = [r[0] for r in cur.fetchall()]; cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not top_codes:
        return pd.DataFrame()

    ph  = ", ".join(["%s"] * len(top_codes))
    sql = (f"SELECT DATE(`date`) AS d, code, AVG(value) AS val "
           f"FROM `{t}` WHERE `date` >= %s AND code IN ({ph}) "
           f"GROUP BY DATE(`date`), code ORDER BY d")
    try:
        c = mk_conn(cfg_brain); cur = c.cursor()
        cur.execute(sql, (min_date, *top_codes))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()

    df_raw = pd.DataFrame(rows, columns=["d", "code", "val"])
    df_raw["d"] = pd.to_datetime(df_raw["d"])
    df_raw["val"] = pd.to_numeric(df_raw["val"], errors="coerce")
    df_raw["code_safe"] = df_raw["code"].str.replace(r"[^A-Za-z0-9]", "_", regex=True).str[:20]
    pivoted = df_raw.pivot_table(index="d", columns="code_safe", values="val", aggfunc="last")
    pivoted.columns = [f"ds{entry['idx']}_{c}" for c in pivoted.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(pivoted)} дней, {len(pivoted.columns)} сигналов")
    return pivoted


def load_polymarket(entry, cfg, min_date) -> pd.DataFrame:
    """
    vlad_polymarket — рынки предсказаний Polymarket.
    Схема: price_timestamp DATETIME, price DECIMAL(5,4), volume DECIMAL, liquidity DECIMAL,
           spread DECIMAL, tags VARCHAR, question VARCHAR, active TINYINT
    Агрегируем по дате:
      - avg_price_all   — средняя вероятность всех активных контрактов
      - avg_price_crypto — средняя вероятность крипто-контрактов
      - avg_spread      — средний спред (мера неопределённости рынка)
      - total_liquidity — суммарная ликвидность
      - total_vol_24h   — суммарный объём за 24ч
    """
    t = "vlad_polymarket"
    if not table_exists(cfg, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    sql = """
        SELECT
            DATE(price_timestamp)                                                    AS d,
            AVG(price)                                                               AS avg_price_all,
            AVG(CASE WHEN LOWER(IFNULL(tags,'')) LIKE '%crypto%'
                          OR LOWER(IFNULL(question,'')) LIKE '%bitcoin%'
                          OR LOWER(IFNULL(question,'')) LIKE '%btc%'
                          OR LOWER(IFNULL(question,'')) LIKE '%eth%'
                     THEN price END)                                                 AS avg_price_crypto,
            AVG(spread)                                                              AS avg_spread,
            SUM(liquidity)                                                           AS total_liquidity,
            SUM(volume_24h)                                                          AS total_vol_24h,
            COUNT(*)                                                                 AS contract_count
        FROM vlad_polymarket
        WHERE active = 1 AND price_timestamp >= %s
        GROUP BY DATE(price_timestamp)
        ORDER BY d
    """
    try:
        c = mk_conn(cfg); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall()
        col_names = ["d", "avg_price_all", "avg_price_crypto", "avg_spread",
                     "total_liquidity", "total_vol_24h", "contract_count"]
        cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=col_names)
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    df.columns = [f"ds{entry['idx']}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
    return df


def load_brain_fng(entry, cfg_brain, min_date) -> pd.DataFrame:
    """
    brain_fng — Fear & Greed из brain DB (второй источник).
    Схема: date DATE/DATETIME, value FLOAT (+ возможно другие поля)
    """
    t = "brain_fng"
    if not table_exists(cfg_brain, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    try:
        c = mk_conn(cfg_brain); cur = c.cursor()
        cur.execute(f"SHOW COLUMNS FROM `{t}`")
        cols = {r[0]: r[1] for r in cur.fetchall()}; cur.close(); c.close()
    except Exception:
        return pd.DataFrame()

    date_cand = [c for c in cols if "date" in c.lower()]
    if not date_cand:
        return pd.DataFrame()
    date_col = date_cand[0]

    num_cols = [c for c in cols if any(tp in cols[c].lower()
                for tp in ("float", "double", "decimal", "int"))
                and c not in {"id"}]
    if not num_cols:
        return pd.DataFrame()

    try:
        df = _read_table(cfg_brain, t, date_col, num_cols[:5], min_date)
        if df.empty:
            return df
        # brain_fng может иметь несколько записей на день
        df = df[~df.index.duplicated(keep="last")]
        df.columns = [f"ds{entry['idx']}_{safe_col(c)}" for c in df.columns]
        print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
        return df
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()


def load_ner(entry, cfg_brain, min_date) -> pd.DataFrame:
    """
    brain_*_ner — Named Entity Recognition по источникам новостей.
    Схема: news_id INT → brain_calendar.Id → FullDate DATETIME
           person VARCHAR(80), location VARCHAR(80), misc VARCHAR(80)
    Агрегируем по дате: COUNT(person), COUNT(location), COUNT(misc), COUNT(*) итого.
    """
    t = entry["name"]
    if not table_exists(cfg_brain, t):
        print(f"  [{entry['idx']:>2}] SKIP {t}")
        return pd.DataFrame()
    # Определяем источник из имени таблицы: brain_tgd_ner → tgd
    parts = t.split("_")   # ['brain', 'tgd', 'ner']
    src   = parts[1] if len(parts) >= 3 else "unk"

    sql = f"""
        SELECT
            DATE(bc.FullDate)      AS d,
            COUNT(n.person)        AS ner_person,
            COUNT(n.location)      AS ner_location,
            COUNT(n.misc)          AS ner_misc,
            COUNT(*)               AS ner_total
        FROM `{t}` n
        JOIN brain_calendar bc ON n.news_id = bc.Id
        WHERE bc.FullDate >= %s
        GROUP BY DATE(bc.FullDate)
        ORDER BY d
    """
    try:
        c = mk_conn(cfg_brain); cur = c.cursor()
        cur.execute(sql, (min_date,))
        rows = cur.fetchall(); cur.close(); c.close()
    except Exception as e:
        print(f"  [{entry['idx']:>2}] ОШИБКА {t}: {e}")
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["d", "ner_person", "ner_location", "ner_misc", "ner_total"])
    df["d"] = pd.to_datetime(df["d"])
    df = df.set_index("d").apply(pd.to_numeric, errors="coerce")
    # Префикс включает источник: ds29_tgd_ner_person
    df.columns = [f"ds{entry['idx']}_{src}_{c}" for c in df.columns]
    print(f"  [{entry['idx']:>2}] {t}: {len(df)} дней")
    return df


def load_dataset(entry, min_date) -> pd.DataFrame:
    """
    Диспетчер загрузчиков.
    resolve_cfg определяет реальную БД таблицы (vlad или brain),
    независимо от префикса имени — на случай когда таблицы
    физически лежат не там где ожидается по названию.
    """
    special = entry.get("special")
    name    = entry.get("name", "")

    # Предпочтительная БД по названию таблицы, fallback — другая
    preferred = CFG_VLAD if entry.get("db") == "vlad" else CFG_BRAIN

    # Для специальных загрузчиков с известной БД — resolve по первой таблице
    def r(table=None):
        return resolve_cfg(table or name, preferred)

    if special == "market_history":  return load_market_history(entry, r(), min_date)
    if special == "investing_cal":   return load_investing_cal(entry,  r("vlad_investing_calendar"), min_date)
    if special == "ecb_rates":       return load_ecb_rates(entry,      r("vlad_ecb_exchange_rates"), min_date)
    if special == "brain_calendar":  return load_brain_calendar(entry, r("brain_calendar"), min_date)
    if special == "brain_llm":       return load_brain_llm(entry,      r("brain_llm"), min_date)
    if special == "imf":             return load_imf(entry,            r("sasha_imf_international_reserves"), min_date)
    if special == "usgs_earthquake": return load_usgs_earthquake(entry, r("vlad_usgs_earthquakes"), min_date)
    if special == "treasury_table":  return load_treasury_table(entry, r(), min_date)
    if special == "btc_onchain":     return load_btc_onchain(entry,    r("sasha_btc_daily_active_addresses"), min_date)
    if special == "eth_onchain":     return load_eth_onchain(entry,    r("sasha_eth_daily_active_addresses"), min_date)
    if special == "ucdp":            return load_ucdp(entry,           r("vlad_ucdp"), min_date)
    if special == "macro_calendar":  return load_macro_calendar(entry, r("vlad_macro_calendar_events"), min_date)
    if special == "news_signals":    return load_news_signals(entry,   r("brain_news_signals"), min_date)
    if special == "polymarket":      return load_polymarket(entry,     r("vlad_polymarket"), min_date)
    if special == "brain_fng":       return load_brain_fng(entry,      r("brain_fng"), min_date)
    if special == "ner":             return load_ner(entry,            r(), min_date)
    if "pivot_col" in entry:         return load_pivot(entry,          r(), min_date)
    if not entry.get("date_col"):    return pd.DataFrame()
    return load_simple(entry, r(), min_date)


# ══════════════════════════════════════════════════════════════════
#  Дискретизация: непрерывные сигналы → квинтильные бакеты 0..4
#  Это и есть «индексация уникальных комбинаций событий»:
#  каждое числовое значение получает категорию от «очень низкий»
#  до «очень высокий» относительно всей истории сигнала.
# ══════════════════════════════════════════════════════════════════

def discretize(df: pd.DataFrame, n: int = N_BUCKETS) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    for col in df.columns:
        # Пропускаем колонки с NaN-именем (артефакты pivot-загрузчиков)
        if not isinstance(col, str) or col == "nan" or col.strip() == "":
            continue
        s = df[col].dropna()
        if len(s) < 20:
            continue
        try:
            bkt = pd.qcut(df[col], q=n, labels=False, duplicates="drop")
        except Exception:
            mn, mx = s.min(), s.max()
            if mn == mx:
                bkt = pd.Series(n // 2, index=df.index, dtype="float64")
            else:
                bkt = pd.cut(df[col], bins=n, labels=False, include_lowest=True)
        result[col + "_bkt"] = bkt.astype("Int64")
    return result


# ══════════════════════════════════════════════════════════════════
#  Обнаружение локальных экстремумов
#  Ищем даты, где цена является локальным MAX или MIN
#  в пределах ±window дней.
# ══════════════════════════════════════════════════════════════════

def find_extremums(price: pd.Series, window: int) -> pd.Series:
    """Возвращает Series: +1=local MAX, -1=local MIN, 0=neutral."""
    s     = price.dropna()
    out   = pd.Series(0, index=price.index, dtype="int8")
    v     = s.values
    n     = len(v)
    if n < window * 2 + 1:
        return out
    try:
        from scipy.signal import argrelextrema
        max_i = argrelextrema(v, np.greater, order=window)[0]
        min_i = argrelextrema(v, np.less,    order=window)[0]
    except ImportError:
        # Fallback без scipy
        max_i = [i for i in range(window, n - window)
                 if v[i] == v[max(0, i-window):i+window+1].max()]
        min_i = [i for i in range(window, n - window)
                 if v[i] == v[max(0, i-window):i+window+1].min()]
    for i in max_i:
        out.loc[s.index[i]] = 1
    for i in min_i:
        out.loc[s.index[i]] = -1
    return out


# ══════════════════════════════════════════════════════════════════
#  Co-occurrence: событие × экстремум
#
#  Для каждого экстремума типа T на дату t* рассматриваем окно
#  [t* - lookback, t*]. Считаем: насколько чаще каждый (feature, bucket)
#  встречается в этих окнах, чем в произвольный день (lift).
#
#  lift = P(feature=bucket | в окне экстремума)
#       / P(feature=bucket | в любой день)
#
#  lift > 1.5 → событие статистически связано с данным типом экстремума
# ══════════════════════════════════════════════════════════════════

def _window_mask(all_dates: pd.DatetimeIndex,
                 ext_dates: pd.DatetimeIndex,
                 lookback: int) -> np.ndarray:
    """Бул. вектор: True если дата попадает в [t-lookback, t] для хотя бы одного t ∈ ext_dates."""
    a  = all_dates.values.astype("datetime64[D]")
    e  = ext_dates.values.astype("datetime64[D]")
    lb = np.timedelta64(lookback, "D")
    mask = np.zeros(len(a), dtype=bool)
    for t in e:
        mask |= (a >= t - lb) & (a <= t)
    return mask


def cooccurrence_stats(features: pd.DataFrame,
                       extremums: pd.DataFrame,
                       lookback: int) -> pd.DataFrame:
    """
    Вычисляет lift для каждой тройки (feature_col, bucket_val, extremum_type).
    """
    idx = features.index.intersection(extremums.index)
    F   = features.reindex(idx)
    E   = extremums.reindex(idx).fillna(0)
    n_total = len(idx)
    rows = []

    for ext_col in E.columns:
        ext_s = E[ext_col]
        for ext_type, label in ((1, "MAX"), (-1, "MIN")):
            ext_dates = idx[ext_s == ext_type]
            if len(ext_dates) < 2:
                continue
            win_mask = _window_mask(idx, ext_dates, lookback)
            n_win    = win_mask.sum()
            if n_win == 0:
                continue
            base_rate = n_win / n_total

            for col in F.columns:
                for bkt in F[col].dropna().unique():
                    active  = (F[col] == bkt).values
                    n_act   = int(active.sum())
                    if n_act == 0:
                        continue
                    n_cooc  = int((active & win_mask).sum())
                    cond    = n_cooc / n_act
                    lift    = cond / base_rate if base_rate > 0 else 0
                    rows.append({
                        "feature_col":   col,
                        "bucket_val":    int(bkt),
                        "extremum_type": f"{ext_col}_{label}",
                        "cooc_count":    n_cooc,
                        "active_count":  n_act,
                        "base_rate":     round(base_rate, 6),
                        "cond_rate":     round(cond, 6),
                        "lift":          round(lift, 4),
                        "support":       round(n_cooc / n_total, 6),
                    })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def combination_stats(features: pd.DataFrame,
                      extremums: pd.DataFrame,
                      lookback: int) -> pd.DataFrame:
    """
    Для каждой уникальной комбинации бакетов (fingerprint):
      - occurrence_count   сколько раз встречалась
      - btc_max/min_count  сколько раз была в окне экстремума BTC
      - btc_max/min_lift   lift = (доля в окне / доля всего)
    """
    idx = features.index.intersection(extremums.index)
    F   = features.reindex(idx).fillna(-1).astype("Int64")
    E   = extremums.reindex(idx).fillna(0)

    # MD5-fingerprint каждой даты (12 символов)
    col_keys = list(F.columns)
    fps = np.array([
        hashlib.md5(
            "|".join(f"{col_keys[j]}:{int(F.iloc[i, j])}"
                     for j in range(len(col_keys))).encode()
        ).hexdigest()[:12]
        for i in range(len(idx))
    ])

    # Уникальные fingerprints → их occurrence
    unique_fps, counts = np.unique(fps, return_counts=True)
    stats: dict = {
        fp: {"combination_hash": fp, "occurrence_count": int(cnt)}
        for fp, cnt in zip(unique_fps, counts)
    }

    # Накапливаем вхождения в окна экстремумов
    for ext_col in E.columns:
        ext_s  = E[ext_col]
        prefix = ext_col.split("_")[0]      # btc / eth / eur
        for ext_type, suffix in ((1, "max"), (-1, "min")):
            ext_dates = idx[ext_s == ext_type]
            win_mask  = _window_mask(idx, ext_dates, lookback)
            fps_win   = fps[win_mask]
            key       = f"{prefix}_{suffix}_count"
            for fp in fps_win:
                if fp in stats:
                    stats[fp][key] = stats[fp].get(key, 0) + 1

    df      = pd.DataFrame(list(stats.values()))
    n_total = len(idx)

    # Lifts
    for prefix in ("btc", "eth", "eur"):
        for suffix in ("max", "min"):
            cnt_col  = f"{prefix}_{suffix}_count"
            lift_col = f"{prefix}_{suffix}_lift"
            if cnt_col not in df.columns:
                df[cnt_col] = 0
            base = df[cnt_col].sum() / n_total if n_total > 0 else 1e-9
            df[lift_col] = (
                (df[cnt_col] / df["occurrence_count"].clip(lower=1)) / (base + 1e-9)
            ).round(4)

    # Текстовое описание: берём первую дату каждого fingerprint
    fp_series = pd.Series(fps, index=idx, name="fp")
    fp_first  = fp_series.reset_index().groupby("fp")["index"].first()
    desc_map  = {}
    for fp in df["combination_hash"]:
        if fp in fp_first.index:
            row = F.loc[fp_first[fp]]
            desc_map[fp] = "|".join(f"{c}:{int(v)}" for c, v in row.items())[:500]
        else:
            desc_map[fp] = ""
    df["combination_desc"] = df["combination_hash"].map(desc_map)

    return df.sort_values("btc_max_lift", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════
#  Запись в MySQL
# ══════════════════════════════════════════════════════════════════

def _safe_val(v):
    """Конвертирует float NaN и pandas NA в None для MySQL INSERT.
    mysql-connector форматирует float('nan') как литерал 'nan',
    который MySQL воспринимает как имя колонки → ошибка 1054.
    """
    if v is None:
        return None
    try:
        import pandas as _pd
        if _pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _row_to_tuple(row) -> tuple:
    """Конвертирует pandas Series или iterable в tuple с NaN → None."""
    return tuple(_safe_val(v) for v in row)


def _exec_ddl(sql):
    c = mk_conn(CFG_VLAD); cur = c.cursor()
    cur.execute(sql)
    c.commit(); cur.close(); c.close()


def _bulk_insert(sql, rows, chunk=500):
    c = mk_conn(CFG_VLAD); cur = c.cursor()
    total = 0
    for i in range(0, len(rows), chunk):
        cur.executemany(sql, rows[i:i+chunk])
        total += cur.rowcount
    c.commit(); cur.close(); c.close()
    return total


def write_registry():
    _exec_ddl("""
        CREATE TABLE IF NOT EXISTS `vlad_atomic_registry` (
            idx        INT         NOT NULL PRIMARY KEY COMMENT 'Индекс датасета 0..N',
            table_name VARCHAR(100)         COMMENT 'Имя таблицы MySQL',
            db_name    VARCHAR(50)          COMMENT 'vlad или brain',
            special    VARCHAR(40)          COMMENT 'Тип загрузчика',
            updated_at TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Реестр датасетов: ind 0..N → table_name'
    """)
    sql = """
        INSERT INTO `vlad_atomic_registry` (idx, table_name, db_name, special)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE table_name=VALUES(table_name), updated_at=CURRENT_TIMESTAMP
    """
    rows = [(e["idx"], e["name"], e.get("db","vlad"), e.get("special","") or "")
            for e in REGISTRY]
    _bulk_insert(sql, rows)
    print(f"   vlad_atomic_registry: {len(rows)} датасетов")


def write_extremums(ext_df: pd.DataFrame):
    _exec_ddl("""
        CREATE TABLE IF NOT EXISTS `vlad_atomic_extremums` (
            record_date DATE    NOT NULL PRIMARY KEY,
            btc_ext     TINYINT DEFAULT 0 COMMENT '+1=local_max  -1=local_min  0=нейтр',
            eth_ext     TINYINT DEFAULT 0,
            eur_ext     TINYINT DEFAULT 0,
            btc_price   DOUBLE,
            eth_price   DOUBLE,
            eur_price   DOUBLE,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Локальные экстремумы цен'
    """)
    df = ext_df.copy()
    df.index.name = "record_date"
    df = df.reset_index()
    df["record_date"] = pd.to_datetime(df["record_date"]).dt.date
    cols = list(df.columns)
    ph   = ", ".join(["%s"] * len(cols))
    sql  = f"INSERT IGNORE INTO `vlad_atomic_extremums` ({', '.join(f'`{c}`' for c in cols)}) VALUES ({ph})"
    rows = [_row_to_tuple(r) for _, r in df.iterrows()]
    n = _bulk_insert(sql, rows)
    print(f"   vlad_atomic_extremums: {n} строк")


def write_feature_matrix(features: pd.DataFrame, extremums: pd.DataFrame):
    """
    Одна строка = один день.
    Столбцы = бакетизированные сигналы + флаги экстремумов.
    Это основной вход для нейросети.
    """
    idx    = features.index.intersection(extremums.index)
    F      = features.reindex(idx)
    E      = extremums.reindex(idx).fillna(0)
    merged = pd.concat([F, E], axis=1)

    # Карта: оригинальное имя → sanitized DB-имя (с дедупликацией)
    # Пропускаем NaN-имена — артефакты pivot-загрузчиков
    feat_safe: dict[str, str] = {}
    for col in F.columns:
        if not isinstance(col, str) or col == "nan" or col.strip() == "":
            continue
        sc = safe_col(col)
        if sc in feat_safe.values():
            sc = sc[:60] + f"_{len(feat_safe)}"
        feat_safe[col] = sc

    ext_cols = list(E.columns)

    col_defs  = ["`record_date` DATE NOT NULL PRIMARY KEY"]
    col_defs += [f"`{sc}` TINYINT" for sc in feat_safe.values()]
    col_defs += [f"`{c}` TINYINT DEFAULT 0" for c in ext_cols]
    col_defs += ["`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP"]

    _exec_ddl(f"""
        CREATE TABLE IF NOT EXISTS `vlad_atomic_feature_matrix` (
            {', '.join(col_defs)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Бакетизированная матрица признаков — вход для NN'
    """)

    all_db_cols = ["record_date"] + list(feat_safe.values()) + ext_cols
    ph  = ", ".join(["%s"] * len(all_db_cols))
    sql = (f"INSERT IGNORE INTO `vlad_atomic_feature_matrix` "
           f"({', '.join(f'`{c}`' for c in all_db_cols)}) VALUES ({ph})")

    rows = []
    for dt, row in merged.iterrows():
        r = [dt.date()]
        for orig, sc in feat_safe.items():
            v = row.get(orig)
            r.append(int(v) if pd.notna(v) else None)
        for ec in ext_cols:
            v = row.get(ec)
            r.append(int(v) if pd.notna(v) else 0)
        rows.append(tuple(r))

    n = _bulk_insert(sql, rows)
    print(f"   vlad_atomic_feature_matrix: {n} строк × {len(feat_safe)} признаков")


def write_cooccurrence(df: pd.DataFrame):
    if df.empty:
        return
    # Убираем строки с NaN feature_col или bucket_val (артефакты NaN-колонок)
    df = df.dropna(subset=["feature_col", "bucket_val"])
    df = df[df["feature_col"].astype(str) != "nan"]
    if df.empty:
        return
    _exec_ddl("""
        CREATE TABLE IF NOT EXISTS `vlad_atomic_cooccurrence` (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            feature_col    VARCHAR(80),
            bucket_val     TINYINT,
            extremum_type  VARCHAR(40),
            cooc_count     INT,
            active_count   INT,
            base_rate      FLOAT,
            cond_rate      FLOAT,
            lift           FLOAT COMMENT 'lift > 1 → событие чаще при экстремуме',
            support        FLOAT,
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq (feature_col(70), bucket_val, extremum_type(30))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Lift: (feature, bucket) при каждом типе экстремума'
    """)
    sql = """
        INSERT INTO `vlad_atomic_cooccurrence`
            (feature_col, bucket_val, extremum_type, cooc_count, active_count, base_rate, cond_rate, lift, support)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            cooc_count=VALUES(cooc_count), lift=VALUES(lift), updated_at=CURRENT_TIMESTAMP
    """
    rows = [_row_to_tuple(r) for _, r in
            df[["feature_col","bucket_val","extremum_type",
                "cooc_count","active_count","base_rate","cond_rate","lift","support"]].iterrows()]
    n = _bulk_insert(sql, rows)
    print(f"   vlad_atomic_cooccurrence: {n} записей")


def write_combinations(df: pd.DataFrame):
    if df.empty:
        return
    _exec_ddl("""
        CREATE TABLE IF NOT EXISTS `vlad_atomic_combinations` (
            combination_hash CHAR(12)  NOT NULL PRIMARY KEY,
            combination_desc TEXT      COMMENT 'Описание: col:bucket|col:bucket|...',
            occurrence_count INT,
            btc_max_count    INT DEFAULT 0,
            btc_min_count    INT DEFAULT 0,
            eth_max_count    INT DEFAULT 0,
            eth_min_count    INT DEFAULT 0,
            eur_max_count    INT DEFAULT 0,
            eur_min_count    INT DEFAULT 0,
            btc_max_lift     FLOAT,
            btc_min_lift     FLOAT,
            eth_max_lift     FLOAT,
            eth_min_lift     FLOAT,
            eur_max_lift     FLOAT,
            eur_min_lift     FLOAT,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_btc_max (btc_max_lift),
            INDEX idx_btc_min (btc_min_lift)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='Уникальные комбинации состояний: fingerprint + lift vs экстремумы'
    """)
    cols = ["combination_hash","combination_desc","occurrence_count",
            "btc_max_count","btc_min_count","eth_max_count","eth_min_count",
            "eur_max_count","eur_min_count",
            "btc_max_lift","btc_min_lift","eth_max_lift","eth_min_lift",
            "eur_max_lift","eur_min_lift"]
    ph  = ", ".join(["%s"] * len(cols))
    sql = (f"INSERT INTO `vlad_atomic_combinations` ({', '.join(f'`{c}`' for c in cols)}) "
           f"VALUES ({ph}) ON DUPLICATE KEY UPDATE "
           f"occurrence_count=VALUES(occurrence_count), btc_max_lift=VALUES(btc_max_lift), "
           f"updated_at=CURRENT_TIMESTAMP")
    rows = []
    for _, row in df.iterrows():
        r = []
        for col in cols:
            v = row.get(col, None)
            if isinstance(v, float) and np.isnan(v):
                v = None
            r.append(v)
        rows.append(tuple(r))
    n = _bulk_insert(sql, rows)
    print(f"   vlad_atomic_combinations: {n} уникальных комбинаций")


# ══════════════════════════════════════════════════════════════════
#  main
# ══════════════════════════════════════════════════════════════════

def main(min_date="2020-01-01", window=7, lookback=7):
    print(" Atomic Extremum Model — Экстремумный анализ всех датасетов")
    print(f" READ  brain: {CFG_BRAIN.get('host')}:{CFG_BRAIN.get('port')}/{CFG_BRAIN.get('database')}")
    print(f" WRITE vlad:  {CFG_VLAD.get('host')}:{CFG_VLAD.get('port')}/{CFG_VLAD.get('database')}")
    print(f" Период: {min_date} → сегодня  |  окно ±{window}d  |  lookback {lookback}d")
    print("═" * 68)

    # ── 1. Загрузка ────────────────────────────────────────────────────
    print("\n[1] Загрузка датасетов...")
    frames = []
    for entry in REGISTRY:
        df = load_dataset(entry, min_date)
        if not df.empty:
            frames.append(df)

    if not frames:
        print(" Нет данных. Проверьте параметры подключения и --min-date"); sys.exit(1)

    # ── 2. Объединение ─────────────────────────────────────────────────
    print(f"\n[2] Объединение {len(frames)} датасетов в единую матрицу...")

    # Дедупликация дат в каждом фрейме перед склейкой
    clean = []
    for df in frames:
        df.index = pd.to_datetime(df.index).normalize()
        if df.index.duplicated().any():
            df = df[~df.index.duplicated(keep="last")]
        clean.append(df)

    full = pd.concat(clean, axis=1)
    full.sort_index(inplace=True)
    full = full.loc[full.index >= min_date]
    full = full.ffill(limit=3)
    print(f"  Матрица: {full.shape[0]} дней × {full.shape[1]} сигналов")
    print(f"  Период:  {full.index.min().date()} → {full.index.max().date()}")

    # ── 3. Дискретизация ────────────────────────────────────────────────
    print(f"\n[3] Дискретизация: каждый сигнал → квинтильный бакет 0..{N_BUCKETS-1}...")
    bkt = discretize(full, n=N_BUCKETS)
    print(f"  Бакетизировано: {len(bkt.columns)} признаков")
    print(f"  (Это «уникальные комбинации событий» = возможные состояния каждого датасета)")

    # ── 4. Обнаружение экстремумов ──────────────────────────────────────
    print(f"\n[4] Поиск локальных экстремумов (окно ±{window} дней)...")
    ext_map:   dict = {}
    price_map: dict = {}

    # Приоритет: длинная история первой.
    # BTC: sasha_fred_cbbtcusd (с 2014) > vlad_market_history (с 2024)
    btc_candidates = (
        [c for c in full.columns if "ds5"  in c and "value"    in c.lower()] +
        [c for c in full.columns if "ds13" in c and "btc_close" in c.lower()]
    )
    # ETH: только vlad_market_history (более короткая история — нормально)
    eth_candidates = (
        [c for c in full.columns if "ds13" in c and "eth_close" in c.lower()]
    )
    # EUR: sasha_fred_dexuseu (с 1999) > vlad_market_history (с 2024)
    eur_candidates = (
        [c for c in full.columns if "ds3"  in c and "value"    in c.lower()] +
        [c for c in full.columns if "ds13" in c and "eurusd"   in c.lower()]
    )

    for label, candidates in [("btc", btc_candidates),
                               ("eth", eth_candidates),
                               ("eur", eur_candidates)]:
        if not candidates:
            print(f"  {label.upper()}: источник не найден — пропущено")
            continue
        col = candidates[0]
        ext_s = find_extremums(full[col], window=window)
        n_max = int((ext_s == 1).sum())
        n_min = int((ext_s == -1).sum())
        print(f"  {label.upper()}: {n_max} MAX + {n_min} MIN  (источник: {col})")
        ext_map[f"{label}_ext"]   = ext_s
        price_map[f"{label}_price"] = full[col]

    if not ext_map:
        print(" Ценовые данные для обнаружения экстремумов не найдены"); sys.exit(1)

    ext_df   = pd.DataFrame(ext_map)
    extremums_out = pd.concat([ext_df, pd.DataFrame(price_map)], axis=1)

    # ── 5. Co-occurrence анализ ─────────────────────────────────────────
    print(f"\n[5] Co-occurrence: lift каждого (feature, bucket) при экстремумах (lookback={lookback}d)...")
    cooc = cooccurrence_stats(bkt, ext_df, lookback=lookback)
    print(f"  Вычислено {len(cooc)} троек (feature × bucket × extremum_type)")

    # Топ для BTC MAX
    if not cooc.empty:
        top = (cooc[cooc["extremum_type"].str.endswith("_MAX") & (cooc["lift"] > 1.5)]
               .sort_values("lift", ascending=False).head(10))
        if not top.empty:
            print("\n  Топ (BTC MAX, lift > 1.5):")
            for _, r in top[top["extremum_type"].str.startswith("btc")].head(8).iterrows():
                print(f"    {r['feature_col']}  bkt={r['bucket_val']}  lift={r['lift']:.2f}  cooc={r['cooc_count']}")

    # ── 6. Комбинации состояний ─────────────────────────────────────────
    print("\n[6] Статистика уникальных комбинаций состояний...")
    combos = combination_stats(bkt, ext_df, lookback=lookback)
    print(f"  Уникальных комбинаций: {len(combos)}")
    top_c = combos[combos["btc_max_lift"] > 2.0].head(5)
    if not top_c.empty:
        print("  Топ-5 (BTC_MAX lift > 2.0):")
        for _, r in top_c.iterrows():
            print(f"    [{r['combination_hash']}]  lift={r['btc_max_lift']:.2f}  n={r['occurrence_count']}")

    # ── 7. Запись в MySQL ───────────────────────────────────────────────
    print("\n[7] Запись результатов в MySQL...")
    write_registry()
    write_extremums(extremums_out)
    write_feature_matrix(bkt, ext_df)
    write_cooccurrence(cooc)
    write_combinations(combos)

    print("\n" + "═" * 68)
    print(" ГОТОВО")
    print(f"\n  Созданные таблицы в [{CFG_VLAD.get('database')}]:")
    print("  vlad_atomic_registry        — реестр датасетов (idx 0..N → имя таблицы)")
    print("  vlad_atomic_extremums       — даты локальных MAX/MIN цен BTC/ETH/EUR")
    print("  vlad_atomic_feature_matrix  — матрица признаков (вход для NN)")
    print("  vlad_atomic_cooccurrence    — lift каждого события при каждом типе экстремума")
    print("  vlad_atomic_combinations    — уникальные комбинации состояний + lift")
    print()
    print("  SQL для NN: SELECT * FROM vlad_atomic_feature_matrix ORDER BY record_date")
    print("  Топ событий: SELECT * FROM vlad_atomic_cooccurrence WHERE lift > 2 ORDER BY lift DESC")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Atomic Extremum Model — экстремумный анализ",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Без аргументов читает подключение из .env:\n"
            "  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, BRAIN_DB\n\n"
            "Пример (из .env):\n"
            "  python atomic_extremum_model.py\n\n"
            "Пример (явные параметры — переопределяют .env):\n"
            "  python atomic_extremum_model.py --host 127.0.0.1 --user root --password secret --db brain"
        )
    )
    # Все параметры опциональны — берутся из .env если не указаны
    ap.add_argument("--host",      default=None, help="MySQL host     (default: DB_HOST из .env)")
    ap.add_argument("--port",      default=None, help="MySQL port     (default: DB_PORT из .env)")
    ap.add_argument("--user",      default=None, help="MySQL user     (default: DB_USER из .env)")
    ap.add_argument("--password",  default=None, help="MySQL password (default: DB_PASSWORD из .env)")
    ap.add_argument("--db",        default=None, help="MySQL database (default: DB_NAME из .env)")
    ap.add_argument("--brain-db",  default=None, help="Brain database (default: BRAIN_DB из .env)")
    ap.add_argument("--window",    type=int, default=7,            help="Полушина окна экстремума, дней (default: 7)")
    ap.add_argument("--lookback",  type=int, default=7,            help="Ретроспектива перед экстремумом, дней (default: 7)")
    ap.add_argument("--min-date",  default="2020-01-01",           help="Начало истории (default: 2020-01-01)")
    _args = ap.parse_args()

    # Сначала грузим из .env, потом перекрываем явными аргументами
    configure_from_env()
    if any([_args.host, _args.user, _args.password, _args.db]):
        host     = _args.host     or CFG_VLAD["host"]
        port     = int(_args.port or CFG_VLAD["port"])
        user     = _args.user     or CFG_VLAD["user"]
        password = _args.password or CFG_VLAD["password"]
        db_name  = _args.db       or CFG_VLAD["database"]
        brain_db = _args.brain_db or CFG_BRAIN["database"]
        base     = dict(host=host, port=port, user=user, password=password)
        configure(
            cfg_vlad  = {**base, "database": db_name},
            cfg_brain = {**base, "database": brain_db},
        )

    try:
        main(min_date=_args.min_date, window=_args.window, lookback=_args.lookback)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n Прервано"); sys.exit(1)
    except Exception as e:
        print(f"\n Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)