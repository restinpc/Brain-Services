"""
model.py — разность потенциалов инструментов по momentum последней свечки.

Идея. У целевой пары две ноги: например у EUR/USD это EUR и USD. Каждая нога
входит ещё в 15 инструментов (у EUR это 15 кроссов, у USD — 15 остальных
базовых таблиц; сама целевая пара из обоих наборов исключена). Берём momentum
последней закрытой свечки в каждом из этих инструментов, приводим знак к
анализируемой ноге и получаем картину вида «EUR вырос на 10 графиках из 15».

Разность потенциалов — это net первой ноги минус net второй: EUR +5 и USD -5
дают +10, потому что для роста EUR/USD нужны и укрепление EUR, и слабость USD.
Значения второй ноги отдаются уже инвертированными, поэтому всё множество
складывается в одну сумму.

Смысл несёт именно знаковый счёт: кроссы — точные отношения базовых котировок,
поэтому bp кросса равен разности bp его ног, и непрерывная сумма sum_bp
алгебраически сворачивается в 15 тел самой целевой пары (годится как проверка
целостности, но новой информации не даёт). Знаки же нелинейны: net ноги — это
её ранг среди 17 активов, то есть на скольких инструментах она сильнее рынка.

Модель не использует стандартный enriched-конвейер: momentum по 136 парам за
всю историю — это порядка двух миллионов строк, которые фреймворк держал бы в
памяти как list[dict]. Вместо этого серии лежат в numpy (см. momentum.py).
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta

import momentum as mom
from brain_framework import get_service_config

# Модель не работает с историей brain_rates_* — ей нужны свои таблицы.
MODEL_USES_RATE_HISTORY = False

_REFRESH_LOCK = threading.Lock()
_REFRESH_STARTED_AT = 0.0
_CONFIGURED = False


# ── Конфигурация ─────────────────────────────────────────────────────────────

def _model_cfg() -> dict:
    cfg = get_service_config() or {}
    return dict(cfg.get("model") or {})


def _ensure_configured() -> dict:
    """Глубина истории берётся из cache.date_from с запасом на shift_window."""
    global _CONFIGURED
    model_cfg = _model_cfg()
    if _CONFIGURED:
        return model_cfg

    cfg = get_service_config() or {}
    raw = str((cfg.get("cache") or {}).get("date_from") or "").strip()
    history_from = None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            history_from = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue
    if history_from is not None:
        history_from -= timedelta(days=int(model_cfg.get("history_margin_days", 60)))
    mom.configure(history_from)
    _CONFIGURED = True
    return model_cfg


# ── Подкачка свежих котировок ────────────────────────────────────────────────

def _run_parsers(python_exe: str = "") -> dict:
    """Запускает SashaRates.py: сначала реальные котировки, затем кроссы."""
    results = {}
    env = dict(os.environ)
    interpreter = mom.parser_python(python_exe)
    for script, table in mom.PARSER_JOBS:
        try:
            completed = subprocess.run(
                [interpreter, script, table],
                cwd=mom.parsers_dir(),
                env=env,
                capture_output=True,
                text=True,
                timeout=int(env.get("QUOTES_JOB_TIMEOUT", "1800")),
            )
            results[script] = completed.returncode
        except Exception as exc:
            results[script] = repr(exc)
    mom.invalidate_series()
    return results


def _maybe_refresh_in_background(model_cfg: dict, day: bool) -> None:
    """Обновляет котировки, не задерживая ответ.

    model() вызывается синхронно внутри событийного цикла, поэтому запускать
    парсеры прямо здесь нельзя — сервис встал бы на десятки секунд. Поток
    отдаёт свежие данные следующим запросам.
    """
    if str(model_cfg.get("quotes_refresh", "background")).lower() != "background":
        return
    cooldown = float(model_cfg.get("refresh_cooldown_minutes", 30)) * 60.0

    global _REFRESH_STARTED_AT
    if time.time() - _REFRESH_STARTED_AT < cooldown:
        return
    if not mom.quotes_stale(day, float(model_cfg.get("quotes_max_age_minutes", 90))):
        return
    if not _REFRESH_LOCK.acquire(blocking=False):
        return
    _REFRESH_STARTED_AT = time.time()

    def _worker():
        try:
            _run_parsers(str(model_cfg.get("parser_python", "")))
        finally:
            _REFRESH_LOCK.release()

    threading.Thread(target=_worker, daemon=True, name="quotes-refresh").start()


# ── Обогащение: полный цикл обновления данных ────────────────────────────────

async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    """Выкачивает новые котировки, генерирует производные и сбрасывает кеш серий.

    Вызывается фреймворком на /rebuild_index и каждые cache.rebuild_interval
    секунд. Стандартная enriched-таблица не создаётся: модель считает momentum
    напрямую по таблицам котировок.
    """
    del engine_brain
    model_cfg = _ensure_configured()
    stats: dict = {"mode": "noop", "source": "sasha_rates_*"}

    refresh = str(model_cfg.get("quotes_refresh", "background")).lower()
    slack = float(model_cfg.get("quotes_max_age_minutes", 90))
    if refresh != "off" and (
        mom.quotes_stale(False, slack) or mom.quotes_stale(True, slack)
    ):
        loop = asyncio.get_running_loop()
        stats["parsers"] = await loop.run_in_executor(
            None, _run_parsers, str(model_cfg.get("parser_python", ""))
        )
    else:
        stats["parsers"] = "skipped-fresh"

    mom.invalidate_series()
    stats.update(mom.discover(force=True))
    stats["last_bar_age_minutes"] = {
        "hour": mom.last_bar_age_minutes(False),
        "day": mom.last_bar_age_minutes(True),
    }
    return stats


# ── Расчёт ───────────────────────────────────────────────────────────────────

def _value(bp: float, value_mode: str) -> float:
    if value_mode == "sign":
        return 1.0 if bp > 0.0 else (-1.0 if bp < 0.0 else 0.0)
    return round(bp, 6)


def _leg_values(
    asset_id: int,
    exclude: tuple[int, int],
    day: bool,
    cutoff_ts: int,
    threshold: float,
) -> tuple[dict[str, float], int, int]:
    """Momentum ноги по всем её инструментам, кроме целевой пары.

    Возвращает значения в базисных пунктах, знаковый net и число найденных бар.
    """
    values: dict[str, float] = {}
    net = 0
    bars = 0
    for key in mom.pairs_with(asset_id):
        if key == exclude:
            continue
        found = mom.asset_momentum(key, day, cutoff_ts, asset_id)
        if found is None:
            continue
        bp = found[0]
        bars += 1
        if abs(bp) < threshold:
            continue
        values[mom.mask(key)] = bp
        net += 1 if bp > 0.0 else (-1 if bp < 0.0 else 0)
    return values, net, bars


def model(rates, dataset, date, *, type=0, var=0, param="", dataset_index=None):
    del rates, dataset, param
    model_cfg = _ensure_configured()

    if not isinstance(date, datetime):
        return {}

    index = dataset_index or {}
    table = str(index.get("rates_table") or "")
    day = bool(index.get("is_daily", table.endswith("_day")))
    cutoff_ts = mom.to_epoch(date)

    first_id, second_id = mom.TARGET_PAIRS.get(
        _pair_from_index(index), mom.TARGET_PAIRS[mom.DEFAULT_TARGET]
    )
    exclude = mom.pair_key(first_id, second_id)

    value_mode = str(model_cfg.get("value_mode", "bp")).lower()
    if int(type) == 4:
        value_mode = "sign"
    threshold = float(var)

    first_values, first_net, first_bars = _leg_values(
        first_id, exclude, day, cutoff_ts, threshold
    )
    second_values, second_net, second_bars = _leg_values(
        second_id, exclude, day, cutoff_ts, threshold
    )

    result: dict[str, float] = {}
    if int(type) in (0, 1, 4):
        for key, bp in first_values.items():
            result[key] = _value(bp, value_mode)
    if int(type) in (0, 2, 4):
        # Вторая нога инвертируется: слабость USD толкает EUR/USD вверх так же,
        # как укрепление EUR. После инверсии всё множество складывается.
        for key, bp in second_values.items():
            result[key] = _value(-bp, value_mode)

    if model_cfg.get("include_totals", True):
        result["net_first"] = float(first_net)
        result["net_second"] = float(second_net)
        result["potential"] = float(first_net - second_net)
        result["sum_bp"] = round(
            sum(first_values.values()) - sum(second_values.values()), 6
        )
        result["bars_first"] = float(first_bars)
        result["bars_second"] = float(second_bars)

    _maybe_refresh_in_background(model_cfg, day)
    return result


def _pair_from_index(index: dict) -> int:
    """pair из /values фреймворк передаёт через имя таблицы котировок."""
    table = str(index.get("rates_table") or "")
    if "btc" in table:
        return 3
    if "eth" in table:
        return 4
    return mom.DEFAULT_TARGET
