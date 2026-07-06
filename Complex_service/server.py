import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import json
import httpx
from urllib.parse import urlencode
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from dotenv import load_dotenv

from common import (
    MODE, IS_DEV,
    log, send_error_trace,
    ok_response, err_response,
    resolve_workers,
    build_engines,
)
from cache_helper import ensure_cache_table, cached_values

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════
#  ЕДИНСТВЕННОЕ, ЧТО НУЖНО МЕНЯТЬ ДЛЯ НОВОЙ СОСТАВНОЙ МОДЕЛИ
# ═══════════════════════════════════════════════════════════════════════════
CHILD_MODEL_IDS = [44, 31]   # ID дочерних моделей (2, 3, N — сколько угодно)
SERVICE_ID      = 45         # ID самой составной модели (новый, свободный)
PORT            = 8898       # свободный порт
# ═══════════════════════════════════════════════════════════════════════════

BEST_URL     = "https://server.brain-project.online/best.php"
NODE_NAME    = f"brain-complex-{'-'.join(map(str, CHILD_MODEL_IDS))}-microservice"
HTTP_TIMEOUT = httpx.Timeout(10.0, connect=5.0)

# PHP-монолит: используется для дочерних моделей, у которых нет активного URL
# в brain_service, либо если модель явно указана в CHILD_MODEL_BACKENDS как "monolith".
# ВАЖНО: по задаче #172 монолит вызывается НЕ по HTTP, а через штатный shell-wrapper:
#   bash /brain/Brain-Server/centos/php.sh 'neuronet_cache.php?neuronet_id=...&signal_id=...'
# В Python мы НЕ используем shell=True и передаём весь query-string одним argv-аргументом,
# поэтому амперсанды не нужно экранировать вручную — они не попадают в shell-парсер.
MONOLITH_PHP_SH = os.getenv("MONOLITH_PHP_SH", "/brain/Brain-Server/centos/php.sh")
MONOLITH_CACHE_SCRIPT = os.getenv("MONOLITH_CACHE_SCRIPT", "neuronet_cache.php")
MONOLITH_SECRET = os.getenv("MONOLITH_SECRET", "3m19m1")
MONOLITH_TIMEOUT_SECONDS = float(os.getenv("MONOLITH_TIMEOUT_SECONDS", "30"))

# Необязательный ручной override. Обычно можно оставить пустым:
#   {2: "monolith", 44: "microservice"}
# Если override не задан: есть active URL в brain_service => microservice, иначе monolith.
CHILD_MODEL_BACKENDS: dict[int, str] = {}

PAIR_POSTFIX = {
    1: "eur_usd",
    3: "btc_usd",
    4: "eth_usd",
}

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)


def _extract_weights_payload(data: dict, model_id: int) -> list[str]:
    """
    Универсально достаёт список кодов весов из ответа /weights.
    Поддерживает:
      Legacy-стиль:    {"status":"ok","payLoad": [...]}
      Framework-стиль: {"status":"ok","payLoad": {"codes": [...], ...}}
      Fallback:        {"weights": [...]}
    """
    payload = data.get("payLoad")
    if payload is not None:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("codes", "weights", "keys"):
                if isinstance(payload.get(key), list):
                    return payload[key]
            raise ValueError(
                f"Model {model_id}: payLoad is dict but no list field "
                f"(codes/weights/keys) found: {list(payload.keys())}"
            )
        raise ValueError(f"Model {model_id}: unsupported payLoad type {type(payload)}")

    if isinstance(data.get("weights"), list):
        return data["weights"]

    raise ValueError(f"Model {model_id}: cannot extract weights from response: {data}")


def _extract_result_dict(res: dict, model_id: int) -> dict:
    """
    Универсально достаёт словарь {key: value} из ответа дочернего /values.
    """
    if "error" in res:
        raise ValueError(f"Model {model_id} returned error: {res['error']}")
    if "payLoad" in res:
        payload = res["payLoad"]
        if not isinstance(payload, dict):
            raise ValueError(f"Model {model_id}: payLoad is not a dict: {payload!r}")
        return payload
    if "status" in res and res.get("status") != "ok":
        raise ValueError(f"Model {model_id}: status not ok: {res}")
    return {k: v for k, v in res.items() if k not in ("status", "error")}


def _rates_table(pair: int, day: int) -> str:
    """Имя таблицы котировок по pair/day для получения rate_id по date."""
    postfix = PAIR_POSTFIX.get(int(pair))
    if not postfix:
        raise ValueError(f"Unsupported pair={pair}; add it to PAIR_POSTFIX")
    return f"brain_rates_{postfix}{'_day' if int(day) else ''}"


def _normalize_backend(value: str | None) -> str | None:
    if value is None:
        return None
    value = str(value).strip().lower()
    aliases = {
        "php": "monolith",
        "legacy": "monolith",
        "mono": "monolith",
        "service": "microservice",
        "micro": "microservice",
    }
    return aliases.get(value, value)


def _as_float(value) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"Cannot convert child value to float: {value!r}") from exc


async def _resolve_rate_id(pair: int, day: int, date: str) -> int:
    """Преобразует date из /values в id из brain_rates_* для PHP-монолита."""
    table = _rates_table(pair, day)
    async with engine_brain.connect() as conn:
        row = (await conn.execute(
            text(f"SELECT id FROM `{table}` WHERE `date` = :date LIMIT 1"),
            {"date": date},
        )).fetchone()
    if not row:
        raise ValueError(f"Rate not found: table={table}, date={date}")
    return int(row[0])


async def _fetch_weights_from_microservice(
    client: httpx.AsyncClient,
    url: str,
    model_id: int,
    pair: int | None = None,
    day: int | None = None,
) -> list[str]:
    """Асинхронный запрос /weights у дочернего Python/FastAPI-сервиса."""
    params = {}
    if pair is not None:
        params["pair"] = pair
    if day is not None:
        params["day"] = day
    r = await client.get(f"{url}/weights", params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return _extract_weights_payload(r.json(), model_id)


async def _fetch_weights_from_monolith(model_id: int, pair: int, day: int) -> list[str]:
    """
    Достаёт возможные ключи кеша PHP-модели из brain_input*.

    Runtime-ключи для монолита имеют вид:
      <child_model_id>_<child_signal_id>_<child_input_id>
    Поэтому здесь возвращаем хвосты <signal_id>_<input_id>; общий префикс
    model_id добавляется ниже так же, как для обычных микросервисов.
    """
    postfix = PAIR_POSTFIX.get(int(pair))
    if not postfix:
        raise ValueError(f"Unsupported pair={pair}; add it to PAIR_POSTFIX")
    if int(day):
        postfix += "_day"

    table_signal = f"brain_signal{int(model_id)}"
    weights: list[str] = []

    async with engine_brain.connect() as conn:
        signal_rows = (await conn.execute(
            text(
                f"SELECT id, only_neurons, layer "
                f"FROM `{table_signal}` "
                f"WHERE pair = :pair AND is_day = :day AND active = 1"
            ),
            {"pair": int(pair), "day": int(day)},
        )).fetchall()

        for sid, only_neurons, layer in signal_rows:
            sid = int(sid)
            if int(only_neurons or 0) == 0 or int(layer or 0) > 0:
                input_table = f"brain_input{int(model_id)}_res_{sid}"
            else:
                input_table = f"brain_input{int(model_id)}_{postfix}"

            try:
                rows = (await conn.execute(
                    text(f"SELECT id FROM `{input_table}` ORDER BY id ASC")
                )).fetchall()
            except Exception as exc:
                log(
                    f"Monolith weights: skip {input_table}: {exc}",
                    NODE_NAME, level="warn", force=True,
                )
                continue

            weights.extend(f"{sid}_{int(row[0])}" for row in rows)

    return list(dict.fromkeys(weights))


async def _fetch_weights_for_child(
    client: httpx.AsyncClient,
    state,
    model_id: int,
    pair: int | None = None,
    day: int | None = None,
) -> list[str]:
    backend = state.child_backends.get(model_id, "microservice")
    if backend == "monolith":
        if pair is None or day is None:
            raise ValueError(f"Model {model_id}: monolith weights require pair/day")
        return await _fetch_weights_from_monolith(model_id, pair, day)
    return await _fetch_weights_from_microservice(
        client, state.child_urls[model_id], model_id, pair=pair, day=day
    )


async def _fetch_all_weights(
    client: httpx.AsyncClient,
    state,
    pair: int | None = None,
    day: int | None = None,
) -> dict[int, list[str] | Exception]:
    """
    Параллельно получает /weights у ВСЕХ дочерних моделей.
    Поддерживает и Python/FastAPI-сервисы, и PHP-монолит.
    """
    ids = CHILD_MODEL_IDS
    results = await asyncio.gather(
        *[_fetch_weights_for_child(client, state, mid, pair=pair, day=day) for mid in ids],
        return_exceptions=True,
    )
    return dict(zip(ids, results))


async def _fetch_one_microservice_values(
    client: httpx.AsyncClient,
    model_id: int,
    url: str,
    pair: int,
    day: int,
    date: str,
    subp: dict,
) -> dict:
    """Один запрос /values к дочернему Python/FastAPI-сервису + масштабирование на k."""
    k = subp.get("k")
    if k is None:
        raise ValueError(f"Model {model_id}: missing 'k' in params")

    query_params = {key: val for key, val in subp.items() if key != "k"}
    query_params.update({"pair": pair, "day": day, "date": date})

    r = await client.get(f"{url}/values", params=query_params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    values = _extract_result_dict(r.json(), model_id)
    return {f"{model_id}_{key}": round(_as_float(val) * k, 6) for key, val in values.items()}


def _build_monolith_cache_request(query_params: dict) -> str:
    """
    Формирует аргумент для php.sh в виде:
      neuronet_cache.php?neuronet_id=...&signal_id=...&pair=...&day=...&rate_id=...

    В subprocess аргумент передаётся одним элементом argv, поэтому `&` не становится
    shell-оператором. Если запускать руками в терминале, строку надо взять в кавычки
    или экранировать амперсанды.
    """
    return f"{MONOLITH_CACHE_SCRIPT}?{urlencode(query_params)}"


async def _run_monolith_cache_php(query_params: dict, model_id: int) -> dict:
    """
    Запускает PHP-монолит через штатный wrapper php.sh.

    Без shell=True: это защищает от проблем с `&`, пробелами и спецсимволами.
    Все ошибки wrapper/PHP/JSON поднимаются как exceptions — выше они логируются
    и превращаются в отсутствие результата комплексной модели.
    """
    request_arg = _build_monolith_cache_request(query_params)

    try:
        proc = await asyncio.create_subprocess_exec(
            "bash",
            MONOLITH_PHP_SH,
            request_arg,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"php.sh not found: {MONOLITH_PHP_SH}") from exc
    except Exception as exc:
        raise RuntimeError(
            f"Cannot start monolith PHP command: bash {MONOLITH_PHP_SH} {request_arg!r}: {exc}"
        ) from exc

    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(),
            timeout=MONOLITH_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise TimeoutError(
            f"Monolith PHP timeout after {MONOLITH_TIMEOUT_SECONDS}s: {request_arg}"
        ) from exc

    stdout = stdout_b.decode("utf-8", errors="replace").strip()
    stderr = stderr_b.decode("utf-8", errors="replace").strip()

    if proc.returncode != 0:
        raise RuntimeError(
            "Monolith PHP failed "
            f"rc={proc.returncode}, request={request_arg!r}, "
            f"stderr={stderr[:1000]!r}, stdout={stdout[:1000]!r}"
        )

    # php.sh/engine иногда может напечатать служебные строки до JSON.
    # Берём JSON-объект с первого символа `{`, иначе показываем короткий вывод.
    json_text = stdout
    start = json_text.find("{")
    if start > 0:
        json_text = json_text[start:]

    try:
        return json.loads(json_text)
    except Exception as exc:
        raise ValueError(
            f"Model {model_id}: monolith PHP returned non-JSON output. "
            f"request={request_arg!r}, stdout={stdout[:1500]!r}, stderr={stderr[:1000]!r}"
        ) from exc


async def _fetch_one_monolith_values(
    client: httpx.AsyncClient,
    model_id: int,
    pair: int,
    day: int,
    date: str,
    subp: dict,
) -> dict:
    """
    Один вызов PHP-монолита через:
      bash /brain/Brain-Server/centos/php.sh 'neuronet_cache.php?...'

    Для монолита нужен конкретный signal_id, потому что cache(rate_id)
    считается внутри конкретного neuronetX(signal_id).
    """
    k = subp.get("k")
    if k is None:
        raise ValueError(f"Model {model_id}: missing 'k' in params")

    signal_id = subp.get("signal_id", subp.get("sid", subp.get("id")))
    if signal_id is None:
        raise ValueError(
            f"Model {model_id}: monolith child requires signal_id in params"
        )
    signal_id = int(signal_id)
    rate_id = await _resolve_rate_id(pair, day, date)

    query_params = {
        "neuronet_id": int(model_id),
        "signal_id": int(signal_id),
        "pair": int(pair),
        "day": int(day),
        "rate_id": int(rate_id),
        "secret": MONOLITH_SECRET,
    }
    response = await _run_monolith_cache_php(query_params, model_id)
    values = _extract_result_dict(response, model_id)

    # signal_id добавлен в ключ специально: у PHP-монолита input_id часто
    # локален для таблицы brain_inputX_res_<signal_id>, значит без signal_id
    # разные дочерние сигналы могут конфликтовать по одинаковым id.
    return {
        f"{model_id}_{signal_id}_{key}": round(_as_float(val) * k, 6)
        for key, val in values.items()
    }


async def _fetch_one_child_values(
    client: httpx.AsyncClient,
    model_id: int,
    url: str,
    pair: int,
    day: int,
    date: str,
    subp: dict,
    state,
) -> dict:
    backend = state.child_backends.get(model_id, "microservice")
    if backend == "monolith":
        return await _fetch_one_monolith_values(client, model_id, pair, day, date, subp)
    return await _fetch_one_microservice_values(client, model_id, url, pair, day, date, subp)


async def _compute_composite(client: httpx.AsyncClient, pair, day, date, param_dict, state) -> dict | None:
    """
    Логика вычисления составной модели.
    КЛЮЧЕВОЕ: все обращения к дочерним моделям запускаются ОДНОВРЕМЕННО
    через asyncio.gather, а не последовательно друг за другом.
    Это критично при вложенных составных моделях (composite of composite) —
    иначе задержка растёт линейно с глубиной вложенности и числом детей.
    """
    tasks = []
    task_model_ids = []

    for model_str, subp in param_dict.items():
        model_id = int(model_str)
        url = state.child_urls.get(model_id)
        if url is None:
            log(f" Unknown child model {model_id} in params — skipped", NODE_NAME,
                level="warn", force=True)
            continue
        tasks.append(_fetch_one_child_values(client, model_id, url, pair, day, date, subp, state))
        task_model_ids.append(model_id)

    if not tasks:
        return None

    results = await asyncio.gather(*tasks, return_exceptions=True)

    combined: dict = {}
    for model_id, result in zip(task_model_ids, results):
        if isinstance(result, Exception):
            log(f" Child model {model_id} error: {result}", NODE_NAME,
                level="error", force=True)
            return None
        combined.update(result)

    return combined


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

    # 1. URL-ы дочерних Python-сервисов из brain_service.
    # Если активного URL нет, считаем модель PHP-монолитом и дёргаем neuronet_cache.php через php.sh.
    async with engine_super.connect() as conn:
        placeholders = ", ".join(f":id{i}" for i in range(len(CHILD_MODEL_IDS)))
        params = {f"id{i}": mid for i, mid in enumerate(CHILD_MODEL_IDS)}
        res = await conn.execute(text(
            f"SELECT id, url FROM brain_service WHERE id IN ({placeholders}) AND active = 1"
        ), params)
        urls = {row[0]: row[1].rstrip("/") for row in res.fetchall() if row[1]}

    app.state.child_urls = {}
    app.state.child_backends = {}
    for mid in CHILD_MODEL_IDS:
        override = _normalize_backend(CHILD_MODEL_BACKENDS.get(mid))
        url = urls.get(mid)

        if override == "microservice":
            if not url:
                raise RuntimeError(f"URL for model {mid} not found in brain_service (active=1)")
            backend = "microservice"
            child_url = url
        elif override == "monolith" or not url:
            backend = "monolith"
            child_url = f"shell:{MONOLITH_PHP_SH} {MONOLITH_CACHE_SCRIPT}"
        else:
            backend = "microservice"
            child_url = url

        app.state.child_backends[mid] = backend
        app.state.child_urls[mid] = child_url
        log(f"CHILD[{mid}]: backend={backend}, url={child_url}", NODE_NAME, force=True)

    # 2. Колонки параметров сигналов (для /params — если таблица есть)
    app.state.cols = {}
    async with engine_brain.connect() as conn:
        for mid in CHILD_MODEL_IDS:
            try:
                result = await conn.execute(text(f"DESCRIBE `brain_signal{mid}`"))
                app.state.cols[mid] = [row[0] for row in result.fetchall()
                                        if row[0] in ("type", "var", "param")]
            except Exception as e:
                app.state.cols[mid] = []
                log(f"  DESCRIBE brain_signal{mid} failed: {e}", NODE_NAME,
                    level="warn", force=True)
    log(f"Cols: {app.state.cols}", NODE_NAME)

    # 3. Флаги static — одним запросом
    async with engine_brain.connect() as conn:
        placeholders = ", ".join(f":id{i}" for i in range(len(CHILD_MODEL_IDS)))
        params = {f"id{i}": mid for i, mid in enumerate(CHILD_MODEL_IDS)}
        res   = await conn.execute(text(
            f"SELECT id, static FROM brain_models WHERE id IN ({placeholders})"
        ), params)
        flags = {row[0]: bool(row[1]) for row in res.fetchall()}

    app.state.child_static = {mid: flags.get(mid, True) for mid in CHILD_MODEL_IDS}
    log(f"static: {app.state.child_static}", NODE_NAME, force=True)

    # 4. Предзагрузка весов только для нестатичных Python-сервисов.
    # PHP-монолит pair/day-зависим, поэтому его веса читаются на /weights(pair, day).
    app.state.child_weights = {mid: None for mid in CHILD_MODEL_IDS}
    nonstatic_ids = [
        mid for mid in CHILD_MODEL_IDS
        if app.state.child_backends.get(mid) == "microservice" and not app.state.child_static[mid]
    ]
    if nonstatic_ids:
        results = await asyncio.gather(
            *[_fetch_weights_for_child(app.state.http_client, app.state, mid)
              for mid in nonstatic_ids],
            return_exceptions=True,
        )
        for mid, result in zip(nonstatic_ids, results):
            if isinstance(result, Exception):
                log(f"  Pre-load weights model {mid}: {result}", NODE_NAME,
                    level="error", force=True)
                send_error_trace(result, NODE_NAME)
            else:
                app.state.child_weights[mid] = result
                log(f"Pre-loaded {len(result)} weights for model {mid}", NODE_NAME, force=True)

    # 5. URL этого сервиса и таблица кеша
    async with engine_super.connect() as conn:
        row = (await conn.execute(
            text("SELECT url FROM brain_service WHERE id = :sid"), {"sid": SERVICE_ID}
        )).fetchone()
    app.state.service_url = row[0].rstrip("/") if row and row[0] else f"http://localhost:{PORT}"
    await ensure_cache_table(engine_vlad)
    log(f"SERVICE_URL (self): {app.state.service_url}", NODE_NAME, force=True)

    yield

    await app.state.http_client.aclose()
    await engine_vlad.dispose()
    await engine_brain.dispose()
    await engine_super.dispose()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def get_metadata():
    async with engine_vlad.connect() as conn:
        res     = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :sid"),
            {"sid": SERVICE_ID},
        )
        row     = res.fetchone()
        version = row[0] if row else 0
    return {
        "status":  "ok",
        "version": f"1.{version}.0",
        "name":    NODE_NAME,
        "mode":    MODE,
        "text":    f"Composite model {'+'.join(map(str, CHILD_MODEL_IDS))}",
        "child_urls":     {str(mid): app.state.child_urls[mid] for mid in CHILD_MODEL_IDS},
        "child_backends": {str(mid): app.state.child_backends[mid] for mid in CHILD_MODEL_IDS},
        "static":         {str(mid): app.state.child_static[mid] for mid in CHILD_MODEL_IDS},
        "weights_cached": {str(mid): app.state.child_weights[mid] is not None for mid in CHILD_MODEL_IDS},
        "metadata": {"child_models": CHILD_MODEL_IDS, "monolith_php_sh": MONOLITH_PHP_SH, "monolith_cache_script": MONOLITH_CACHE_SCRIPT},
    }


@app.get("/weights")
async def get_weights(pair: int = Query(1), day: int = Query(1)):
    client = app.state.http_client
    to_fetch = [
        mid for mid in CHILD_MODEL_IDS
        if (
            app.state.child_backends.get(mid) == "monolith"
            or app.state.child_static[mid]
            or app.state.child_weights[mid] is None
        )
    ]
    try:
        if to_fetch:
            fetched = await asyncio.gather(
                *[_fetch_weights_for_child(client, app.state, mid, pair=pair, day=day)
                  for mid in to_fetch]
            )
            fetched_map = dict(zip(to_fetch, fetched))
        else:
            fetched_map = {}
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(f"Child model unreachable: {e}")

    combined = []
    for mid in CHILD_MODEL_IDS:
        w = fetched_map.get(mid, app.state.child_weights.get(mid)) or []
        combined += [f"{mid}_" + str(code) for code in w]
    deduped = list(dict.fromkeys(combined))
    return ok_response(deduped)


@app.get("/new_weights")
async def new_weights(pair: int = Query(1), day: int = Query(1)):
    try:
        results = await _fetch_all_weights(app.state.http_client, app.state, pair=pair, day=day)
        all_weights: list[str] = []
        for mid in CHILD_MODEL_IDS:
            result = results[mid]
            if isinstance(result, Exception):
                send_error_trace(result, NODE_NAME)
                weights = app.state.child_weights.get(mid) or []
            else:
                weights = result
                if (
                    app.state.child_backends.get(mid) == "microservice"
                    and not app.state.child_static[mid]
                ):
                    app.state.child_weights[mid] = weights
            all_weights += [f"{mid}_" + str(w) for w in weights]
        deduped = list(dict.fromkeys(all_weights))
        return ok_response(deduped)
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="new_weights")
        return err_response(str(e))


@app.get("/values")
async def get_values(
    pair:   int = Query(1),
    day:    int = Query(1),
    date:   str = Query(...),
    params: str = Query(...),
):
    try:
        param_dict = json.loads(params)
    except Exception:
        return err_response("Invalid JSON in params")

    try:
        return await cached_values(
            engine_vlad=engine_vlad,
            service_url  = app.state.service_url,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = param_dict,
            compute_fn   = lambda: _compute_composite(
                app.state.http_client, pair, day, date, param_dict, app.state
            ),
            node         = NODE_NAME,
        )
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_values")
        return err_response(str(e))


@app.get("/params")
async def get_params(
    pair: int = Query(1),
    day:  int = Query(1),
    tier: int = Query(..., ge=0, le=1),
):
    """
    Автогенерация комбинаций параметров для обучения. Поддерживает ровно
    2 дочерние модели (декартово произведение type/var A × B). Для N>2
    детей используйте ручной complex_json в brain_models.
    """
    if len(CHILD_MODEL_IDS) != 2:
        return err_response("/params auto-generation supports exactly 2 child models")
    model_a_id, model_b_id = CHILD_MODEL_IDS
    max_per_model = 4 if tier == 0 else 3

    client = app.state.http_client
    try:
        best_a, best_b = await asyncio.gather(
            client.get(f"{BEST_URL}?neuronet_id={model_a_id}&pair={pair}&day={day}", timeout=HTTP_TIMEOUT),
            client.get(f"{BEST_URL}?neuronet_id={model_b_id}&pair={pair}&day={day}", timeout=HTTP_TIMEOUT),
        )
        best_a = best_a.json()
        best_b = best_b.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"best.php error: {e}")

    sorted_a = sorted(best_a, key=best_a.get, reverse=True)
    sorted_b = sorted(best_b, key=best_b.get, reverse=True)

    TABLE_A = f"brain_signal{model_a_id}"
    TABLE_B = f"brain_signal{model_b_id}"
    cols_a  = app.state.cols.get(model_a_id, [])
    cols_b  = app.state.cols.get(model_b_id, [])

    params_a, params_b = [], []

    async with engine_brain.connect() as conn:
        for sid in sorted_a:
            if len(params_a) >= max_per_model:
                break
            sid_int = int(sid)
            select_cols = cols_a if cols_a else ["id"]
            cols_str = ", ".join(select_cols)
            res = await conn.execute(
                text(f"SELECT {cols_str} FROM `{TABLE_A}` "
                     f"WHERE id = :sid AND tier = :tier AND is_day = :day"),
                {"sid": sid_int, "tier": tier, "day": day},
            )
            row = res.fetchone()
            if row:
                param_obj = {
                    col: ([row[i]] if col == "param" and row[i] is not None else row[i])
                    for i, col in enumerate(select_cols)
                    if col != "id"
                }
                param_obj["signal_id"] = sid_int
                params_a.append(param_obj)

        for sid in sorted_b:
            if len(params_b) >= max_per_model:
                break
            sid_int = int(sid)
            select_cols = cols_b if cols_b else ["id"]
            cols_str = ", ".join(select_cols)
            res = await conn.execute(
                text(f"SELECT {cols_str} FROM `{TABLE_B}` "
                     f"WHERE id = :sid AND tier = :tier AND is_day = :day"),
                {"sid": sid_int, "tier": tier, "day": day},
            )
            row = res.fetchone()
            if row:
                param_obj = {
                    col: ([row[i]] if col == "param" and row[i] is not None else row[i])
                    for i, col in enumerate(select_cols)
                    if col != "id"
                }
                param_obj["signal_id"] = sid_int
                params_b.append(param_obj)

    combs = []
    if tier == 0:
        for pa in params_a:
            for pb in params_b:
                combs.append({
                    str(model_a_id): {**pa, "k": 0.5},
                    str(model_b_id): {**pb, "k": 0.5},
                })
    else:
        for pa in params_a:
            for pb in params_b:
                for ki in range(1, 10):
                    k = round(ki / 10, 1)
                    combs.append({
                        str(model_a_id): {**pa, "k": k},
                        str(model_b_id): {**pb, "k": round(1 - k, 1)},
                    })

    return combs[:150]


@app.post("/patch")
async def patch_service():
    async with engine_vlad.begin() as conn:
        res = await conn.execute(
            text("SELECT version FROM version_microservice WHERE microservice_id = :id"),
            {"id": SERVICE_ID},
        )
        row = res.fetchone()
        old = row[0] if row else 0
        new = max(old, 1)
        if new != old:
            await conn.execute(
                text("UPDATE version_microservice SET version = :v WHERE microservice_id = :id"),
                {"v": new, "id": SERVICE_ID},
            )
    return {"status": "ok", "version": new}


if __name__ == "__main__":
    import asyncio as _asyncio
    async def _get_workers():
        return await resolve_workers(engine_super, SERVICE_ID, default=1)
    _workers = _asyncio.run(_get_workers())
    log(f"Starting with {_workers} worker(s) in {MODE} mode", NODE_NAME, force=True)
    try:
        uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False, workers=_workers)
    except KeyboardInterrupt:
        log("Server stopped", NODE_NAME, force=True)
    except SystemExit:
        pass
    except Exception as e:
        log(f"Critical: {e!r}", NODE_NAME, level="error", force=True)
        send_error_trace(e, NODE_NAME)
