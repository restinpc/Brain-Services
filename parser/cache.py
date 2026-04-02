import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import math
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

# ── ID моделей и маппинг на папки сервисов ────────────────────────────────────
# model_id → абсолютный путь к папке сервиса (где лежит server.py и model.py)
MODEL_IDS = [30]

SERVICE_FOLDER_MAP: dict[int, str] = {
    30: "/brain/Brain-Services/30",
    # 31: "/brain/Brain-Server/31",
}

# ── Параллельность ────────────────────────────────────────────────────────────
SLOT_CONCURRENCY = 4    # параллельных вычислений на один слот
SLOT_BATCH_SIZE  = 50   # свечей в батче (локальный вызов быстрее HTTP, можно больше)

# ── Стратегия retry: collect-then-retry ──────────────────────────────────────
# Основной проход — не останавливается на ошибках, собирает все упавшие.
# После — до RETRY_PASSES перепрогонов по упавшим.
# Между перепрогонами — пауза (даёт время восстановиться если БД или модель)
RETRY_PASSES      = 3
RETRY_PASS_DELAYS = [10, 30, 60]   # сек; локальные вызовы восстанавливаются быстрее

# ── Логирование ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Трассировка ───────────────────────────────────────────────────────────────
_HANDLER    = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL   = f"{_HANDLER}/trace.php"
NODE_NAME   = os.getenv("NODE_NAME",   "cache_runner")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_trace(subject: str, body: str, is_error: bool = False) -> None:
    level = "ERROR" if is_error else "INFO"
    full_body = f"[{level}] {subject}\n\nNode: {NODE_NAME}\n\n{body}"
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME,
                  "email": ALERT_EMAIL, "logs": full_body},
            timeout=10,
        )
        log.info(f"📤 Трассировка отправлена: {subject}")
    except Exception as e:
        log.warning(f"⚠️  Не удалось отправить трассировку: {e}")


def send_error_trace(exc: Exception, context: str = "") -> None:
    body = (
        f"Context: {context}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    send_trace(f"❌ Ошибка: {type(exc).__name__}", body, is_error=True)


# ── Торговые константы ─────────────────────────────────────────────────────────
INITIAL_BALANCE = 10_000.0
MIN_LOT         = 0.01

PAIR_CFG = {
    1: (0.0002, 100_000.0, 50_000.0),
    3: (60.0,        1.0, 100_000.0),
    4: (10.0,        1.0,   5_000.0),
}

RATES_TABLE = {
    (1, 0): "brain_rates_eur_usd",
    (1, 1): "brain_rates_eur_usd_day",
    (3, 0): "brain_rates_btc_usd",
    (3, 1): "brain_rates_btc_usd_day",
    (4, 0): "brain_rates_eth_usd",
    (4, 1): "brain_rates_eth_usd_day",
}

PAIR_NAMES = {1: "EUR/USD", 3: "BTC/USD", 4: "ETH/USD"}
DAY_NAMES  = {0: "hourly",  1: "daily"}


def _slot_label(pair: int, day: int) -> str:
    return f"{PAIR_NAMES.get(pair, f'pair{pair}')}-{'day' if day else 'hour'}"


# ── DDL ────────────────────────────────────────────────────────────────────────
DDL_CACHE = """
CREATE TABLE IF NOT EXISTS vlad_values_cache (
    id          BIGINT       NOT NULL AUTO_INCREMENT,
    service_url VARCHAR(255) NOT NULL,
    pair        TINYINT      NOT NULL,
    day_flag    TINYINT      NOT NULL,
    date_val    DATETIME     NOT NULL,
    params_hash CHAR(32)     NOT NULL,
    params_json TEXT         NOT NULL,
    result_json TEXT         NOT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_cache (service_url(100), pair, day_flag, date_val, params_hash),
    INDEX idx_lookup (service_url(100), pair, day_flag, params_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_BACKTEST = """
CREATE TABLE IF NOT EXISTS vlad_backtest_results (
    id              BIGINT         NOT NULL AUTO_INCREMENT,
    service_url     VARCHAR(255)   NOT NULL,
    model_id        INT            NOT NULL DEFAULT 0,
    pair            TINYINT        NOT NULL,
    day_flag        TINYINT        NOT NULL,
    tier            TINYINT        NOT NULL,
    params_hash     CHAR(32)       NOT NULL,
    params_json     TEXT           NOT NULL,
    date_from       DATETIME       NOT NULL,
    date_to         DATETIME       NOT NULL,
    balance_final   DECIMAL(18,4)  NOT NULL DEFAULT 0,
    total_result    DECIMAL(18,4)  NOT NULL DEFAULT 0,
    summary_lost    DECIMAL(18,6)  NOT NULL DEFAULT 0,
    value_score     DECIMAL(18,4)  NOT NULL DEFAULT 0,
    trade_count     INT            NOT NULL DEFAULT 0,
    win_count       INT            NOT NULL DEFAULT 0,
    accuracy        DECIMAL(7,4)   NOT NULL DEFAULT 0,
    created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_backtest (service_url(100), pair, day_flag, tier,
                            params_hash, date_from, date_to),
    INDEX idx_score (service_url(100), pair, day_flag, tier, value_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_SUMMARY = """
CREATE TABLE IF NOT EXISTS vlad_backtest_summary (
    id                 INT            NOT NULL AUTO_INCREMENT,
    model_id           INT            NOT NULL,
    service_url        VARCHAR(255)   NOT NULL,
    pair               TINYINT        NOT NULL,
    day_flag           TINYINT        NOT NULL,
    tier               TINYINT        NOT NULL,
    date_from          DATETIME       NOT NULL,
    date_to            DATETIME       NOT NULL,
    total_combinations INT            NOT NULL DEFAULT 0,
    best_score         DECIMAL(18,4)  NOT NULL DEFAULT 0,
    avg_score          DECIMAL(18,4)  NOT NULL DEFAULT 0,
    best_accuracy      DECIMAL(7,4)   NOT NULL DEFAULT 0,
    avg_accuracy       DECIMAL(7,4)   NOT NULL DEFAULT 0,
    best_params_json   TEXT,
    computed_at        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                       ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_summary (model_id, pair, day_flag, tier, date_from, date_to)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


class Deadline:
    def __init__(self, hours: float):
        self.limit    = timedelta(hours=hours)
        self.started  = datetime.now()
        self.deadline = self.started + self.limit

    def exceeded(self) -> bool:
        return datetime.now() >= self.deadline

    def remaining_str(self) -> str:
        rem = self.deadline - datetime.now()
        if rem.total_seconds() <= 0:
            return "0s"
        h, rem_s = divmod(int(rem.total_seconds()), 3600)
        m, s     = divmod(rem_s, 60)
        return f"{h}h {m}m {s}s"

    def elapsed_str(self) -> str:
        el   = datetime.now() - self.started
        h, r = divmod(int(el.total_seconds()), 3600)
        m, s = divmod(r, 60)
        return f"{h}h {m}m {s}s"


# ══════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА СЕРВИСА — динамический импорт server.py из папки
# ══════════════════════════════════════════════════════════════════════════════

class ServiceRunner:
    """
    Загружает server.py из папки сервиса и держит его состояние в памяти.
    Вызывает calculate() напрямую — без HTTP, без сети.
    """

    def __init__(self, model_id: int, folder: str):
        self.model_id  = model_id
        self.folder    = Path(folder)
        self.module    = None
        self.service_url = f"local:{model_id}"  # псевдо-url для кеш-таблицы
        self._initialized = False

    def load_module(self) -> None:
        """
        Динамически импортирует server.py из папки сервиса.
        Добавляет папку в sys.path чтобы server.py мог найти model.py и shared/.
        """
        server_path = self.folder / "server.py"
        if not server_path.exists():
            raise FileNotFoundError(
                f"server.py не найден: {server_path}\n"
                f"Проверь SERVICE_FOLDER_MAP для model_id={self.model_id}"
            )

        # Добавляем папку сервиса и shared в sys.path
        service_dir = str(self.folder)
        shared_dir  = str(self.folder.parent / "shared")
        for p in (service_dir, shared_dir):
            if p not in sys.path:
                sys.path.insert(0, p)

        module_name = f"service_{self.model_id}_server"

        # Если уже импортирован ранее — берём из кеша
        if module_name in sys.modules:
            self.module = sys.modules[module_name]
            log.info(f"  [model{self.model_id}] server.py уже в sys.modules")
            return

        spec = importlib.util.spec_from_file_location(module_name, str(server_path))
        if spec is None or spec.loader is None:
            raise ImportError(
                f"Не удалось создать spec для {server_path}\n"
                f"Возможно, файл повреждён или недоступен"
            )

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            del sys.modules[module_name]
            raise ImportError(
                f"Ошибка при выполнении server.py (model={self.model_id}):\n"
                f"  {type(e).__name__}: {e}\n\n"
                f"Полный traceback:\n{traceback.format_exc()}"
            )

        self.module = module
        log.info(f"  [model{self.model_id}] server.py загружен из {server_path}")

    async def initialize(self) -> None:
        """
        Загружает все данные в RAM: котировки, датасет, синхронизирует универсум.
        Вызывается один раз перед началом работы.
        """
        if self._initialized:
            return

        if self.module is None:
            self.load_module()

        if not hasattr(self.module, "preload_all_data"):
            raise AttributeError(
                f"server.py (model={self.model_id}) не экспортирует preload_all_data()\n"
                f"Убедись что это dummy-фреймворк нужной версии"
            )

        log.info(f"  [model{self.model_id}] Загрузка данных в RAM (preload_all_data)...")
        try:
            await self.module.preload_all_data()
            self._initialized = True
        except Exception as e:
            raise RuntimeError(
                f"Ошибка при preload_all_data (model={self.model_id}):\n"
                f"  {type(e).__name__}: {e}\n\n"
                f"{traceback.format_exc()}"
            )

        # Проверяем что данные загружены
        rates = getattr(self.module, "GLOBAL_RATES", {})
        dataset_len = len(getattr(self.module, "GLOBAL_DATASET", []))
        for table, rows in rates.items():
            log.info(f"    {table}: {len(rows)} строк")
        log.info(f"    dataset: {dataset_len} строк")

        if not any(rates.values()):
            log.warning(
                f"  ⚠️  [model{self.model_id}] GLOBAL_RATES пуст — "
                f"все расчёты вернут пустой результат"
            )

    def calculate(
        self,
        pair: int,
        day: int,
        date_str: str,
        extra_params: dict,
    ) -> tuple[dict | None, str | None]:
        """
        Вызывает calculate() из server.py напрямую.
        Возвращает (result | None, error_reason | None).
        error_reason содержит подробное описание причины ошибки.
        """
        if self.module is None:
            return None, "Модуль не загружен (вызови load_module() сначала)"

        if not hasattr(self.module, "calculate"):
            return None, (
                f"server.py (model={self.model_id}) не экспортирует calculate()\n"
                f"Ожидается функция: calculate(pair, day, date_str, type_, var, param)"
            )

        try:
            result = self.module.calculate(
                pair     = pair,
                day      = day,
                date_str = date_str,
                type_    = extra_params.get("type", 0),
                var      = extra_params.get("var",  0),
                param    = extra_params.get("param", ""),
            )
        except ImportError as e:
            # model.py не найден или не импортируется
            return None, (
                f"ImportError при вызове calculate(): {e}\n"
                f"Скорее всего model.py отсутствует или содержит ошибку импорта\n"
                f"Путь: {self.folder / 'model.py'}"
            )
        except SyntaxError as e:
            return None, (
                f"SyntaxError в model.py (строка {e.lineno}): {e.msg}\n"
                f"Исправь синтаксис и перезапусти"
            )
        except TypeError as e:
            return None, (
                f"TypeError в model() или calculate(): {e}\n"
                f"Возможно, сигнатура model() не совпадает с контрактом\n"
                f"Ожидается: model(rates, dataset, date, *, type, var, param)"
            )
        except ZeroDivisionError as e:
            return None, (
                f"ZeroDivisionError в model(): {e}\n"
                f"Модель делит на 0 — добавь проверку перед делением"
            )
        except Exception as e:
            return None, (
                f"{type(e).__name__} в calculate(pair={pair}, day={day}, "
                f"date={date_str}, params={extra_params}):\n"
                f"  {e}\n\n"
                f"Traceback (последние 3 кадра):\n"
                f"{''.join(traceback.format_tb(e.__traceback__)[-3:])}"
            )

        if result is None:
            return None, (
                f"calculate() вернул None для date={date_str}\n"
                f"Возможная причина: parse_date() не распознал формат даты\n"
                f"Поддерживаемые форматы: YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DD"
            )

        if not isinstance(result, dict):
            return None, (
                f"calculate() вернул {type(result).__name__} вместо dict\n"
                f"Значение: {repr(result)[:200]}"
            )

        # Пустой dict — нормально (нет сигнала), не ошибка
        return result, None


# ── Кеш SERVICE_RUNNERS: один экземпляр на model_id ──────────────────────────
_SERVICE_RUNNERS: dict[int, ServiceRunner] = {}


async def get_service_runner(model_id: int) -> ServiceRunner:
    if model_id not in _SERVICE_RUNNERS:
        folder = SERVICE_FOLDER_MAP.get(model_id)
        if not folder:
            raise KeyError(
                f"model_id={model_id} не найден в SERVICE_FOLDER_MAP\n"
                f"Добавь: SERVICE_FOLDER_MAP[{model_id}] = '/path/to/service/folder'"
            )
        runner = ServiceRunner(model_id, folder)
        runner.load_module()
        await runner.initialize()
        _SERVICE_RUNNERS[model_id] = runner
        log.info(f"  [model{model_id}] ServiceRunner готов")
    return _SERVICE_RUNNERS[model_id]


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def get_service_url_from_db(sync_engine, model_id: int) -> str:
    with sync_engine.connect() as conn:
        row = conn.execute(
            sa_text("SELECT url FROM brain_service WHERE id = :mid"),
            {"mid": model_id},
        ).fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"URL для модели {model_id} не найден в brain_service")
    url = row[0].rstrip("/")
    log.info(f"  URL модели {model_id}: {url}")
    return url


def discover_param_combos(sync_engine, model_id: int) -> list[dict]:
    table = f"brain_signal{model_id}"
    with sync_engine.connect() as conn:
        try:
            desc = conn.execute(sa_text(f"DESCRIBE `{table}`")).fetchall()
        except Exception:
            log.warning(f"  ⚠️  Таблица {table} не найдена — одна комбинация {{}}")
            return [{}]

        available  = {row[0] for row in desc}
        param_cols = [c for c in ("type", "var") if c in available]

        if not param_cols:
            log.info(f"  Модель {model_id}: нет type/var → одна комбинация {{}}")
            return [{}]

        cols_str = ", ".join(param_cols)
        try:
            rows = conn.execute(
                sa_text(
                    f"SELECT DISTINCT {cols_str} FROM `{table}` ORDER BY {cols_str}"
                )
            ).fetchall()
        except Exception as e:
            log.warning(f"  ⚠️  Ошибка чтения {table}: {e} → одна комбинация")
            return [{}]

    combos = [
        {col: int(row[i]) for i, col in enumerate(param_cols) if row[i] is not None}
        for row in rows
    ] or [{}]

    log.info(f"  Модель {model_id}: {len(combos)} комбинаций (колонки: {param_cols})")
    return combos


def _params_hash(params: dict) -> str:
    return hashlib.md5(
        json.dumps(params, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Неверный формат даты: {s!r}")


def _compute_signal(values: dict, tier: int) -> int:
    if not values:
        return 0
    total = (
        sum(math.copysign(1.0, v) for v in values.values() if v != 0)
        if tier == 0
        else sum(values.values())
    )
    return 1 if total > 0 else (-1 if total < 0 else 0)


# ══════════════════════════════════════════════════════════════════════════════
# ВЫЧИСЛЕНИЕ ОДНОЙ СВЕЧИ — замена HTTP на прямой вызов
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_and_store_one(
    candle: dict,
    runner: ServiceRunner,
    pair: int,
    day: int,
    extra_params: dict,
    p_hash: str,
    sem: asyncio.Semaphore,
    engine_vlad,
) -> tuple[str, str, str | None]:
    """
    Вычисляет calculate() для одной свечи и сохраняет в кеш.
    Возвращает ('ok' | 'error', date_str, error_reason | None).
    error_reason содержит подробное описание причины.
    """
    date_val = candle["date"]
    date_str = date_val.strftime("%Y-%m-%d %H:%M:%S")

    async with sem:
        # Запускаем синхронный calculate() в потоке — не блокируем event loop
        result, err = await asyncio.to_thread(
            runner.calculate, pair, day, date_str, extra_params
        )

    if err is not None:
        return "error", date_str, err

    try:
        async with engine_vlad.begin() as conn:
            await conn.execute(text("""
                INSERT IGNORE INTO vlad_values_cache
                    (service_url, pair, day_flag, date_val,
                     params_hash, params_json, result_json)
                VALUES (:url, :pair, :day, :dv, :ph, :pj, :rj)
            """), {
                "url":  runner.service_url,
                "pair": pair,
                "day":  day,
                "dv":   date_val,
                "ph":   p_hash,
                "pj":   json.dumps(extra_params, ensure_ascii=False),
                "rj":   json.dumps(result,       ensure_ascii=False),
            })
    except Exception as e:
        # Ошибка записи в кеш не является критической — логируем и продолжаем
        log.debug(f"[CACHE] DB insert warn date={date_str}: {e}")

    return "ok", date_str, None


# ── Один проход по списку свечей ──────────────────────────────────────────────

FailedItem = dict  # {"candle": candle, "reason": str}


async def _run_pass(
    candles: list[dict],
    runner: ServiceRunner,
    pair: int,
    day: int,
    extra_params: dict,
    p_hash: str,
    sem: asyncio.Semaphore,
    engine_vlad,
    deadline: Deadline,
    prefix: str,
    pass_label: str,
) -> tuple[int, list[FailedItem]]:
    """
    Проходит по candles батчами. Возвращает (ok_count, failed_list).
    Не останавливается на ошибках — собирает все упавшие с причиной.
    """
    if not candles:
        return 0, []

    ok_count = 0
    failed: list[FailedItem] = []

    for batch_start in range(0, len(candles), SLOT_BATCH_SIZE):
        if deadline.exceeded():
            log.warning(f"{prefix} ⏰ {pass_label}: дедлайн, прерываем")
            break

        batch = candles[batch_start : batch_start + SLOT_BATCH_SIZE]

        results = await asyncio.gather(*[
            _fetch_and_store_one(
                c, runner, pair, day,
                extra_params, p_hash,
                sem, engine_vlad,
            )
            for c in batch
        ], return_exceptions=True)

        for i, r in enumerate(results):
            c = batch[i]
            if isinstance(r, Exception):
                # Неожиданное исключение в gather — максимально подробно
                failed.append({
                    "candle": c,
                    "reason": (
                        f"Неожиданное исключение gather: {type(r).__name__}: {r}\n"
                        f"{''.join(traceback.format_tb(r.__traceback__)[-2:])}"
                    ),
                })
            elif r[0] == "ok":
                ok_count += 1
            else:
                _, date_str, reason = r
                failed.append({"candle": c, "reason": reason or "unknown"})

        done = batch_start + len(batch)
        pct  = done / len(candles) * 100
        log.info(
            f"{prefix} {pass_label} [{done}/{len(candles)}] {pct:.1f}%  "
            f"ok={ok_count}  err={len(failed)}"
        )

    return ok_count, failed


def _log_error_details(prefix: str, failed: list[FailedItem], pass_label: str) -> None:
    """
    Логирует подробности ошибок — один пример на каждую уникальную причину.
    Показывает первые строки reason чтобы было понятно что случилось.
    """
    if not failed:
        return

    log.warning(f"{prefix} ── Детали ошибок [{pass_label}] ({'─' * 40})")

    # Группируем по первой строке reason (тип ошибки)
    by_type: dict[str, list[FailedItem]] = {}
    for item in failed:
        first_line = item["reason"].split("\n")[0][:120]
        by_type.setdefault(first_line, []).append(item)

    for error_type, items in sorted(by_type.items(), key=lambda x: -len(x[1])):
        count = len(items)
        sample = items[0]
        date_str = sample["candle"]["date"].strftime("%Y-%m-%d %H:%M:%S")

        log.warning(f"{prefix}   [{count}x] {error_type}")
        log.warning(f"{prefix}          Пример: date={date_str}")

        # Если reason многострочный — выводим полностью для первого примера
        reason_lines = sample["reason"].split("\n")
        if len(reason_lines) > 1:
            for line in reason_lines[1:6]:  # до 5 дополнительных строк
                if line.strip():
                    log.warning(f"{prefix}          {line}")

    log.warning(f"{prefix} {'─' * 50}")


def _error_summary(failed: list[FailedItem]) -> list[tuple[str, int]]:
    """Группирует ошибки по первой строке причины → топ-10 по частоте."""
    counts: dict[str, int] = {}
    for item in failed:
        key = item["reason"].split("\n")[0][:100]
        counts[key] = counts.get(key, 0) + 1
    return sorted(counts.items(), key=lambda x: -x[1])[:10]


# ══════════════════════════════════════════════════════════════════════════════
# fill_cache — основной проход + collect-then-retry
# ══════════════════════════════════════════════════════════════════════════════

async def fill_cache(
    engine_vlad,
    candles: list[dict],
    runner: ServiceRunner,
    pair: int,
    day: int,
    extra_params: dict,
    deadline: Deadline,
    sem: asyncio.Semaphore,
    slot: str = "",
) -> dict:
    """
    Заполняет кеш для одного слота.
    Основной проход — без остановок, все ошибки собираем.
    Затем до RETRY_PASSES перепрогонов по упавшим датам.
    """
    if not candles:
        return {"done": 0, "total": 0, "errors": 0, "skipped": 0, "new": 0,
                "error_summary": [], "failed_jobs": []}

    prefix = f"[{slot}]" if slot else "[cache]"
    p_hash = _params_hash(extra_params)
    total  = len(candles)

    # Загружаем уже закешированные даты
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT date_val FROM vlad_values_cache
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND params_hash = :ph
        """), {"url": runner.service_url, "pair": pair, "day": day, "ph": p_hash})
        cached_dates = {row[0] for row in res.fetchall()}

    skipped  = sum(1 for c in candles if c["date"] in cached_dates)
    to_fetch = [c for c in candles if c["date"] not in cached_dates]

    log.info(
        f"{prefix} Начало: {total} свечей  "
        f"в кеше={len(cached_dates)}  нужно={len(to_fetch)}  "
        f"params={json.dumps(extra_params)}"
    )

    if not to_fetch:
        return {"done": total, "total": total, "errors": 0,
                "skipped": skipped, "new": 0, "error_summary": [], "failed_jobs": []}

    # ── Основной проход ────────────────────────────────────────────────────────
    ok, failed = await _run_pass(
        to_fetch, runner, pair, day, extra_params, p_hash,
        sem, engine_vlad, deadline, prefix, pass_label="▶ Основной",
    )

    if failed:
        _log_error_details(prefix, failed, "Основной проход")

    # ── Retry-проходы по упавшим ───────────────────────────────────────────────
    for pass_num in range(1, RETRY_PASSES + 1):
        if not failed or deadline.exceeded():
            break

        delay = RETRY_PASS_DELAYS[min(pass_num - 1, len(RETRY_PASS_DELAYS) - 1)]
        log.info(
            f"{prefix} 🔄 Retry-проход {pass_num}/{RETRY_PASSES}: "
            f"{len(failed)} дат, пауза {delay}s..."
        )
        await asyncio.sleep(delay)

        retry_candles = [item["candle"] for item in failed]
        ok_retry, failed = await _run_pass(
            retry_candles, runner, pair, day, extra_params, p_hash,
            sem, engine_vlad, deadline, prefix,
            pass_label=f"♻ Retry {pass_num}/{RETRY_PASSES}",
        )
        ok += ok_retry

        if failed:
            _log_error_details(prefix, failed, f"Retry {pass_num}")
            log.info(
                f"{prefix} Retry {pass_num} результат: "
                f"восстановлено={ok_retry}  осталось={len(failed)}"
            )
        else:
            log.info(f"{prefix} ✅ Retry {pass_num}: все ошибки устранены")

    errors  = len(failed)
    new_cnt = ok
    done    = skipped + new_cnt + errors
    summary = _error_summary(failed)

    if errors == 0:
        log.info(
            f"{prefix} ✅ Готово: total={total}  new={new_cnt}  "
            f"skip={skipped}  err=0"
        )
    else:
        log.warning(
            f"{prefix} ⚠️  Готово с ошибками: total={total}  new={new_cnt}  "
            f"skip={skipped}  err={errors}"
        )
        # Остаточные ошибки — финальный детальный лог
        _log_error_details(prefix, failed, "Остаток после всех retry")

    return {
        "done":          done,
        "total":         total,
        "errors":        errors,
        "skipped":       skipped,
        "new":           new_cnt,
        "error_summary": summary,
        "failed_jobs":   failed,
    }


# ══════════════════════════════════════════════════════════════════════════════
# fetch_candles
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_candles(
    engine_brain, pair: int, day: int,
    date_from: datetime, date_to: datetime,
) -> list[dict]:
    table = RATES_TABLE.get((pair, day))
    if not table:
        return []
    async with engine_brain.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT date, open, close FROM {table}
            WHERE date >= :df AND date < :dt
            ORDER BY date ASC
        """), {"df": date_from, "dt": date_to})
        return [
            {"date": r[0], "open": float(r[1] or 0), "close": float(r[2] or 0)}
            for r in res.fetchall()
        ]


# ══════════════════════════════════════════════════════════════════════════════
# run_slot_cache
# ══════════════════════════════════════════════════════════════════════════════

async def run_slot_cache(
    pair: int, day: int,
    candles: list[dict],
    param_combos: list[dict],
    engine_vlad,
    runner: ServiceRunner,
    args, date_from: datetime, date_to: datetime,
    deadline: Deadline,
) -> dict:
    slot    = _slot_label(pair, day)
    label   = f"[{slot}]"
    sem     = asyncio.Semaphore(SLOT_CONCURRENCY)
    stats   = {"new": 0, "skipped": 0, "errors": 0}
    timed_out = False

    if not candles:
        log.warning(f"{label} ⚠️  Нет свечей — пропускаем слот")
        return {"pair": pair, "day": day, "slot": slot, "cache": stats, "timed_out": False}

    log.info(f"{label} ▶  Кеш: {len(candles)} свечей, {len(param_combos)} комбинаций")

    if not args.skip_fill:
        for idx, combo in enumerate(param_combos, 1):
            if deadline.exceeded():
                timed_out = True
                break

            combo_str = json.dumps(combo) if combo else "{}"
            log.info(f"{label} 📥 Кеш [{idx}/{len(param_combos)}] params={combo_str}")

            r = await fill_cache(
                engine_vlad, candles, runner, pair, day,
                combo, deadline, sem, slot=slot,
            )
            stats["new"]     += r["new"]
            stats["skipped"] += r["skipped"]
            stats["errors"]  += r["errors"]

            if r["errors"] > 0:
                summary_lines = "\n".join(
                    f"  {cnt:>4}x  {reason}"
                    for reason, cnt in r.get("error_summary", [])
                )
                send_trace(
                    f"⚠️  Ошибки кеша — {slot} params={combo_str}",
                    f"model={runner.model_id}  pair={PAIR_NAMES.get(pair)}  "
                    f"day={DAY_NAMES.get(day)}\nparams={combo_str}\n\n"
                    f"Ошибок после всех retry: {r['errors']} из {r['total']}\n\n"
                    f"Топ причин:\n{summary_lines}\n\n"
                    f"Прошло: {deadline.elapsed_str()}  "
                    f"Осталось: {deadline.remaining_str()}",
                    is_error=True,
                )

        if not timed_out:
            log.info(
                f"{label} ✅ Кеш завершён: "
                f"new={stats['new']}  skip={stats['skipped']}  err={stats['errors']}"
            )
        else:
            log.warning(f"{label} ⏰ Кеш прерван по таймауту")
    else:
        log.info(f"{label} ⏭️  Кеш пропущен (--skip-fill)")

    return {"pair": pair, "day": day, "slot": slot, "cache": stats, "timed_out": timed_out}


# ══════════════════════════════════════════════════════════════════════════════
# run_slot_backtest
# ══════════════════════════════════════════════════════════════════════════════

async def run_backtest(
    engine_vlad, candles: list[dict],
    runner: ServiceRunner, pair: int, day: int, tier: int,
    extra_params: dict, date_from: datetime, date_to: datetime,
    deadline: Deadline, slot: str = "",
) -> dict:
    if not candles:
        return {"error": "no candles"}

    prefix = f"[{slot}]" if slot else "[backtest]"
    p_hash = _params_hash(extra_params)

    async with engine_vlad.connect() as conn:
        existing = (await conn.execute(text("""
            SELECT value_score, accuracy, trade_count FROM vlad_backtest_results
            WHERE service_url = :url AND pair = :pair AND day_flag = :day
              AND tier = :tier AND params_hash = :ph
              AND date_from = :df AND date_to = :dt
        """), {
            "url": runner.service_url, "pair": pair, "day": day, "tier": tier,
            "ph": p_hash, "df": date_from, "dt": date_to,
        })).fetchone()

    if existing:
        return {
            "value_score": float(existing[0]),
            "accuracy":    float(existing[1]),
            "trade_count": int(existing[2]),
            "params":      extra_params,
            "params_hash": p_hash,
            "skipped":     True,
        }

    if deadline.exceeded():
        return {"error": "timeout"}

    async with engine_vlad.connect() as conn:
        res = await conn.execute(text("""
            SELECT date_val, result_json FROM vlad_values_cache
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day AND params_hash = :ph
        """), {"url": runner.service_url, "pair": pair, "day": day, "ph": p_hash})
        cache_map = {row[0]: json.loads(row[1]) for row in res.fetchall()}

    spread, modification, lot_divisor = PAIR_CFG.get(pair, (0.0002, 100_000.0, 10_000.0))

    balance = INITIAL_BALANCE
    highest = INITIAL_BALANCE
    summary_lost = 0.0
    trade_count = win_count = 0

    for candle in candles:
        values = cache_map.get(candle["date"])
        if not values:
            continue
        signal = _compute_signal(values, tier)
        if signal == 0:
            continue

        raw_move = candle["close"] - candle["open"]
        lot      = max(round(balance / lot_divisor, 2), MIN_LOT)
        amount   = (signal * raw_move - spread) * lot * modification

        balance += amount
        trade_count += 1
        if signal * raw_move > spread:
            win_count += 1

        if balance > highest:
            highest = balance
        drawdown = highest - balance
        if drawdown > 0:
            summary_lost += drawdown / highest

    if trade_count < 10:
        return {"error": "not enough trades", "trade_count": trade_count}

    total_result = balance - INITIAL_BALANCE
    value_score  = total_result - summary_lost
    accuracy     = win_count / trade_count

    result = {
        "balance_final": round(balance,      4),
        "total_result":  round(total_result, 4),
        "summary_lost":  round(summary_lost, 6),
        "value_score":   round(value_score,  4),
        "trade_count":   trade_count,
        "win_count":     win_count,
        "accuracy":      round(accuracy,     4),
        "params":        extra_params,
        "params_hash":   p_hash,
    }

    try:
        async with engine_vlad.begin() as conn:
            await conn.execute(text("""
                INSERT INTO vlad_backtest_results
                    (service_url, model_id, pair, day_flag, tier,
                     params_hash, params_json, date_from, date_to,
                     balance_final, total_result, summary_lost,
                     value_score, trade_count, win_count, accuracy)
                VALUES
                    (:url, :mid, :pair, :day, :tier,
                     :ph, :pj, :df, :dt,
                     :bf, :tr, :sl, :vs, :tc, :wc, :acc)
                ON DUPLICATE KEY UPDATE
                    balance_final = VALUES(balance_final),
                    total_result  = VALUES(total_result),
                    summary_lost  = VALUES(summary_lost),
                    value_score   = VALUES(value_score),
                    trade_count   = VALUES(trade_count),
                    win_count     = VALUES(win_count),
                    accuracy      = VALUES(accuracy),
                    created_at    = CURRENT_TIMESTAMP
            """), {
                "url": runner.service_url, "mid": runner.model_id,
                "pair": pair, "day": day, "tier": tier, "ph": p_hash,
                "pj": json.dumps(extra_params, ensure_ascii=False),
                "df": date_from, "dt": date_to,
                "bf": result["balance_final"], "tr": result["total_result"],
                "sl": result["summary_lost"],  "vs": result["value_score"],
                "tc": trade_count, "wc": win_count, "acc": result["accuracy"],
            })
    except Exception as e:
        log.warning(f"{prefix} ⚠️  Не удалось сохранить бэктест: {e}")

    return result


async def upsert_summary(
    engine_vlad, runner: ServiceRunner,
    pair: int, day: int, tier: int,
    date_from: datetime, date_to: datetime,
) -> None:
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text("""
            SELECT COUNT(*), MAX(value_score), AVG(value_score),
                   MAX(accuracy), AVG(accuracy),
                   MAX(CASE WHEN value_score = (
                       SELECT MAX(value_score) FROM vlad_backtest_results r2
                       WHERE r2.service_url = :url AND r2.pair = :pair
                         AND r2.day_flag = :day  AND r2.tier = :tier
                         AND r2.date_from = :df  AND r2.date_to = :dt
                   ) THEN params_json END)
            FROM vlad_backtest_results
            WHERE service_url = :url AND pair = :pair
              AND day_flag = :day  AND tier = :tier
              AND date_from = :df  AND date_to = :dt
        """), {
            "url": runner.service_url, "pair": pair, "day": day,
            "tier": tier, "df": date_from, "dt": date_to,
        })).fetchone()

    if not row or not row[0]:
        return

    async with engine_vlad.begin() as conn:
        await conn.execute(text("""
            INSERT INTO vlad_backtest_summary
                (model_id, service_url, pair, day_flag, tier,
                 date_from, date_to,
                 total_combinations, best_score, avg_score,
                 best_accuracy, avg_accuracy, best_params_json)
            VALUES (:mid, :url, :pair, :day, :tier, :df, :dt,
                    :cnt, :bs, :as_, :ba, :aa, :bpj)
            ON DUPLICATE KEY UPDATE
                total_combinations = VALUES(total_combinations),
                best_score         = VALUES(best_score),
                avg_score          = VALUES(avg_score),
                best_accuracy      = VALUES(best_accuracy),
                avg_accuracy       = VALUES(avg_accuracy),
                best_params_json   = VALUES(best_params_json),
                computed_at        = CURRENT_TIMESTAMP
        """), {
            "mid": runner.model_id, "url": runner.service_url,
            "pair": pair, "day": day, "tier": tier,
            "df": date_from, "dt": date_to,
            "cnt": row[0],
            "bs": float(row[1] or 0), "as_": float(row[2] or 0),
            "ba": float(row[3] or 0), "aa":  float(row[4] or 0),
            "bpj": row[5],
        })


async def run_slot_backtest(
    pair: int, day: int,
    candles: list[dict],
    param_combos: list[dict],
    engine_vlad, runner: ServiceRunner,
    args, date_from: datetime, date_to: datetime,
    deadline: Deadline, slot: str,
) -> dict:
    label  = f"[{slot}]"
    stats  = {"done": 0, "skipped": 0, "failed": 0}
    timed_out = False

    if args.only_fill or not candles:
        return {"slot": slot, "backtest": stats, "timed_out": timed_out}

    for tier in args.tiers:
        if deadline.exceeded():
            timed_out = True
            break

        log.info(f"{label} 🧪 Бэктест tier={tier} ({len(param_combos)} комбинаций)")
        results = []

        for idx, combo in enumerate(param_combos, 1):
            if deadline.exceeded():
                timed_out = True
                break

            combo_str = json.dumps(combo) if combo else "{}"
            r = await run_backtest(
                engine_vlad, candles, runner, pair, day, tier,
                combo, date_from, date_to, deadline, slot=slot,
            )

            if r.get("skipped"):
                stats["skipped"] += 1
                log.info(
                    f"{label} [{idx}/{len(param_combos)}] tier={tier} ⏭  "
                    f"уже: score={r.get('value_score')}  acc={r.get('accuracy')}"
                )
            elif "error" in r:
                stats["failed"] += 1
                log.info(
                    f"{label} [{idx}/{len(param_combos)}] tier={tier} ✗ "
                    f"{r['error']} (trades={r.get('trade_count', 0)})"
                )
            else:
                stats["done"] += 1
                results.append(r)
                log.info(
                    f"{label} [{idx}/{len(param_combos)}] tier={tier} ✓ "
                    f"score={r['value_score']:>10.2f}  acc={r['accuracy']:.3f}  "
                    f"trades={r['trade_count']}  params={combo_str}"
                )

        await upsert_summary(engine_vlad, runner, pair, day, tier, date_from, date_to)

        log.info(
            f"{label} 📊 tier={tier}: done={len(results)}  "
            f"skip={stats['skipped']}  fail={stats['failed']}"
        )
        if results:
            best = max(results, key=lambda x: x["value_score"])
            log.info(
                f"{label} 🏆 tier={tier} лучший: "
                f"score={best['value_score']}  acc={best['accuracy']}  "
                f"trades={best['trade_count']}  params={best['params']}"
            )

    return {"slot": slot, "backtest": stats, "timed_out": timed_out}


# ══════════════════════════════════════════════════════════════════════════════
# run_model
# ══════════════════════════════════════════════════════════════════════════════

async def run_model(
    model_id: int, param_combos: list[dict],
    engine_vlad, engine_brain, args,
    date_from: datetime, date_to: datetime,
    deadline: Deadline,
) -> tuple[dict, dict, bool]:
    # Инициализируем ServiceRunner
    runner = await get_service_runner(model_id)

    slots   = [(p, d) for p in args.pairs for d in args.days]
    n_slots = len(slots)

    log.info(
        f"\n{'#'*60}\n"
        f"  🤖 model_id={model_id}  local={runner.folder}\n"
        f"  Слотов: {n_slots}  Комбинаций: {len(param_combos)}\n"
        f"  SLOT_CONCURRENCY={SLOT_CONCURRENCY}  SLOT_BATCH_SIZE={SLOT_BATCH_SIZE}\n"
        f"  Retry: passes={RETRY_PASSES}  delays={RETRY_PASS_DELAYS}s\n"
        f"{'#'*60}"
    )

    log.info("  📊 Загружаем свечи для всех слотов параллельно...")
    candles_list = await asyncio.gather(*[
        fetch_candles(engine_brain, p, d, date_from, date_to)
        for p, d in slots
    ])
    candles_map = dict(zip(slots, candles_list))
    for (p, d), cc in candles_map.items():
        log.info(f"  [{_slot_label(p, d)}] свечей: {len(cc)}")

    # Фаза 1: кеш (параллельно по слотам)
    log.info(f"\n  🚀 Запускаем {n_slots} слотов параллельно...")
    slot_cache_results = await asyncio.gather(*[
        run_slot_cache(
            pair=p, day=d, candles=candles_map[(p, d)],
            param_combos=param_combos, engine_vlad=engine_vlad,
            runner=runner, args=args,
            date_from=date_from, date_to=date_to,
            deadline=deadline,
        )
        for p, d in slots
    ], return_exceptions=True)

    sc = {"new": 0, "skipped": 0, "errors": 0}
    timed_out = False

    for res in slot_cache_results:
        if isinstance(res, Exception):
            log.error(f"  ❌ Исключение в слоте кеша: {res!r}")
            send_error_trace(res, f"run_slot_cache model={model_id}")
            continue
        sl = res["slot"]
        c  = res["cache"]
        sc["new"]     += c.get("new",     0)
        sc["skipped"] += c.get("skipped", 0)
        sc["errors"]  += c.get("errors",  0)
        if res.get("timed_out"):
            timed_out = True
        log.info(
            f"  [{sl}] итог: new={c.get('new',0)}  "
            f"skip={c.get('skipped',0)}  err={c.get('errors',0)}"
        )

    # Фаза 2: бэктест
    sb = {"done": 0, "skipped": 0, "failed": 0}
    if not args.only_fill and not timed_out and not deadline.exceeded():
        log.info("\n  🧪 Запуск бэктеста (параллельно по слотам)...")
        slot_back_results = await asyncio.gather(*[
            run_slot_backtest(
                pair=p, day=d, candles=candles_map[(p, d)],
                param_combos=param_combos, engine_vlad=engine_vlad,
                runner=runner, args=args,
                date_from=date_from, date_to=date_to,
                deadline=deadline, slot=_slot_label(p, d),
            )
            for p, d in slots
        ], return_exceptions=True)

        for res in slot_back_results:
            if isinstance(res, Exception):
                log.error(f"  ❌ Исключение в бэктесте: {res!r}")
                send_error_trace(res, f"run_slot_backtest model={model_id}")
                continue
            b = res["backtest"]
            sb["done"]    += b.get("done",    0)
            sb["skipped"] += b.get("skipped", 0)
            sb["failed"]  += b.get("failed",  0)
            if res.get("timed_out"):
                timed_out = True

    return sc, sb, timed_out


# ══════════════════════════════════════════════════════════════════════════════
# run / main
# ══════════════════════════════════════════════════════════════════════════════

async def run(args) -> None:
    deadline = Deadline(hours=args.timeout_hours)
    log.info(
        f"⏰ Таймаут: {args.timeout_hours}h  "
        f"(дедлайн: {deadline.deadline.strftime('%Y-%m-%d %H:%M:%S')})"
    )

    vlad_host     = os.getenv("VLAD_HOST",     os.getenv("DB_HOST",     "localhost"))
    vlad_port     = os.getenv("VLAD_PORT",     os.getenv("DB_PORT",     "3306"))
    vlad_user     = os.getenv("VLAD_USER",     os.getenv("DB_USER",     "root"))
    vlad_password = os.getenv("VLAD_PASSWORD", os.getenv("DB_PASSWORD", ""))
    vlad_database = os.getenv("VLAD_DATABASE", os.getenv("DB_NAME",     "vlad"))

    super_host     = os.getenv("SUPER_HOST",     vlad_host)
    super_port     = os.getenv("SUPER_PORT",     vlad_port)
    super_user     = os.getenv("SUPER_USER",     vlad_user)
    super_password = os.getenv("SUPER_PASSWORD", vlad_password)
    super_name     = os.getenv("SUPER_NAME",     "brain")

    brain_host     = os.getenv("MASTER_HOST",     vlad_host)
    brain_port     = os.getenv("MASTER_PORT",     vlad_port)
    brain_user     = os.getenv("MASTER_USER",     vlad_user)
    brain_password = os.getenv("MASTER_PASSWORD", vlad_password)
    brain_name     = os.getenv("MASTER_NAME",     "brain")

    vlad_url  = (f"mysql+aiomysql://{vlad_user}:{vlad_password}"
                 f"@{vlad_host}:{vlad_port}/{vlad_database}")
    brain_url = (f"mysql+aiomysql://{brain_user}:{brain_password}"
                 f"@{brain_host}:{brain_port}/{brain_name}")
    super_sync_url = (f"mysql+mysqlconnector://{super_user}:{super_password}"
                      f"@{super_host}:{super_port}/{super_name}")

    log.info("=" * 60)
    log.info(f"🚀 models={MODEL_IDS}  SLOT_CONCURRENCY={SLOT_CONCURRENCY}")
    log.info(f"   vlad  DB : {vlad_user}@{vlad_host}:{vlad_port}/{vlad_database}")
    log.info(f"   brain DB : {brain_user}@{brain_host}:{brain_port}/{brain_name}")
    log.info("=" * 60)

    model_configs: list[tuple[int, list[dict]]] = []
    try:
        sync_engine = create_engine(
            super_sync_url, pool_recycle=3600,
            connect_args={"auth_plugin": "caching_sha2_password"},
        )
        for model_id in MODEL_IDS:
            try:
                param_combos = discover_param_combos(sync_engine, model_id)
                model_configs.append((model_id, param_combos))
            except Exception as e:
                log.error(f"❌ Модель {model_id}: {e}")
                send_error_trace(e, f"discover_params model={model_id}")
        sync_engine.dispose()
    except Exception as e:
        log.critical(f"❌ super DB: {e}")
        send_error_trace(e, "sync_engine_connect")
        sys.exit(1)

    if not model_configs:
        log.critical("❌ Ни одна модель не загружена.")
        sys.exit(1)

    n_slots   = len(args.pairs) * len(args.days)
    vlad_pool = min(n_slots * SLOT_CONCURRENCY + 5, 60)

    engine_vlad  = create_async_engine(vlad_url,  pool_size=vlad_pool, max_overflow=10, echo=False)
    engine_brain = create_async_engine(brain_url, pool_size=max(n_slots, 6), max_overflow=0, echo=False)

    log.info(f"  DB pool: vlad={vlad_pool}(+10)  brain={max(n_slots,6)}  слотов={n_slots}")

    try:
        async with engine_vlad.begin() as conn:
            for ddl in (DDL_CACHE, DDL_BACKTEST, DDL_SUMMARY):
                await conn.execute(text(ddl))
        log.info("✅ Таблицы проверены/созданы")
    except Exception as e:
        log.critical(f"❌ Ошибка DDL: {e}")
        send_error_trace(e, "ensure_tables")
        sys.exit(1)

    date_from = _parse_dt(args.date_from) if args.date_from else datetime(2025, 1, 15)
    date_to   = (
        _parse_dt(args.date_to) if args.date_to
        else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    )

    log.info(f"  Период : {date_from} → {date_to}")
    log.info(f"  Модели : {MODEL_IDS}")
    log.info(f"  Пары   : {args.pairs}")
    log.info(f"  Дни    : {args.days}")
    log.info(f"  Тиры   : {args.tiers}")
    log.info(f"  Слотов : {n_slots} (параллельно)")

    all_timed_out = False
    try:
        for model_id, param_combos in model_configs:
            if deadline.exceeded():
                all_timed_out = True
                break

            sc, sb, timed_out = await run_model(
                model_id=model_id,
                param_combos=param_combos,
                engine_vlad=engine_vlad,
                engine_brain=engine_brain,
                args=args,
                date_from=date_from,
                date_to=date_to,
                deadline=deadline,
            )

            elapsed    = deadline.elapsed_str()
            body_stats = (
                f"Кеш  : new={sc['new']}  skip={sc['skipped']}  err={sc['errors']}\n"
                f"Бэктест: done={sb['done']}  skip={sb['skipped']}  fail={sb['failed']}"
            )

            if timed_out:
                all_timed_out = True
                msg = (
                    f"⏰ Модель {model_id} остановлена по таймауту.\n"
                    f"Прошло: {elapsed}\n\n{body_stats}\n\n"
                    f"Запусти повторно — продолжит с места остановки."
                )
                log.warning(f"\n{msg}")
                send_trace(f"⏰ Таймаут — model={model_id}", msg)
                break
            else:
                msg = f"✅ Модель {model_id}.\nПрошло: {elapsed}\n\n{body_stats}"
                log.info(f"\n{'='*55}\n{msg}\n{'='*55}")
                send_trace(f"✅ Готово — model={model_id}", msg)

        elapsed   = deadline.elapsed_str()
        completed = [m[0] for m in model_configs]

        if all_timed_out:
            final = (f"⏰ Прогон остановлен по таймауту {args.timeout_hours}h.\n"
                     f"Прошло: {elapsed}\nМодели: {completed}\nЗапусти повторно.")
            log.warning(f"\n{final}")
            send_trace(f"⏰ Таймаут — models={MODEL_IDS}", final)
        else:
            final = f"✅ Все модели завершены.\nПрошло: {elapsed}\nМодели: {completed}"
            log.info(f"\n{'='*55}\n{final}\n{'='*55}")
            send_trace(f"✅ Готово — {MODEL_IDS}", final)

    except Exception as e:
        log.critical(f"❌ Критическая ошибка: {e!r}")
        send_error_trace(e, "main_loop")
        raise
    finally:
        await engine_vlad.dispose()
        await engine_brain.dispose()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Кеш + бэктест. Прямой вызов calculate() из server.py сервисов.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Конфигурация (в коде):
  SERVICE_FOLDER_MAP — маппинг model_id → папка с server.py и model.py
  SLOT_CONCURRENCY   = {SLOT_CONCURRENCY}
  SLOT_BATCH_SIZE    = {SLOT_BATCH_SIZE}
  RETRY_PASSES       = {RETRY_PASSES}
  RETRY_PASS_DELAYS  = {RETRY_PASS_DELAYS}s

Примеры:
  python cache.py
  python cache.py --date-from 2025-01-15 --date-to 2026-03-12
  python cache.py --pair 1 --day 0 --only-fill
        """,
    )
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to",   default=None)
    parser.add_argument("--pair",  type=int, nargs="+", default=[1, 3, 4],
                        choices=[1, 3, 4], dest="pairs")
    parser.add_argument("--day",   type=int, nargs="+", default=[0, 1],
                        choices=[0, 1], dest="days")
    parser.add_argument("--tier",  type=int, nargs="+", default=[0, 1],
                        choices=[0, 1], dest="tiers")
    parser.add_argument("--skip-fill",     action="store_true")
    parser.add_argument("--only-fill",     action="store_true")
    parser.add_argument("--timeout-hours", type=float, default=24.0)

    args, unknown = parser.parse_known_args()
    if unknown:
        log.warning(f"⚠️  Игнорируем неизвестные аргументы: {unknown}")
    return args


def main():
    args    = parse_args()
    n_slots = len(args.pairs) * len(args.days)
    slots   = [_slot_label(p, d) for p in args.pairs for d in args.days]

    log.info("=" * 60)
    log.info("⚙️  Параметры запуска")
    log.info(f"   models            : {MODEL_IDS}")
    log.info(f"   date_from         : {args.date_from or '2025-01-15 (default)'}")
    log.info(f"   date_to           : {args.date_to   or 'today (default)'}")
    log.info(f"   pairs             : {args.pairs}")
    log.info(f"   days              : {args.days}")
    log.info(f"   tiers             : {args.tiers}")
    log.info(f"   skip_fill         : {args.skip_fill}")
    log.info(f"   only_fill         : {args.only_fill}")
    log.info(f"   timeout_hours     : {args.timeout_hours}")
    log.info(f"   слотов            : {n_slots} → {slots}")
    log.info(f"   SLOT_CONCURRENCY  : {SLOT_CONCURRENCY}")
    log.info(f"   SLOT_BATCH_SIZE   : {SLOT_BATCH_SIZE}")
    log.info(f"   RETRY_PASSES      : {RETRY_PASSES}")
    log.info(f"   RETRY_PASS_DELAYS : {RETRY_PASS_DELAYS}s")
    log.info("=" * 60)

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("🛑 Прервано пользователем (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        log.critical(f"❌ Завершено с ошибкой: {e!r}")
        send_error_trace(e, "main")
        sys.exit(1)


if __name__ == "__main__":
    main()
