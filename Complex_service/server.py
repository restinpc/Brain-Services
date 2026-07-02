import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import json
import httpx
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

load_dotenv()

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


async def _fetch_weights_from_child(client: httpx.AsyncClient, url: str, model_id: int) -> list[str]:
    """Асинхронный запрос /weights у одного дочернего сервиса."""
    r = await client.get(f"{url}/weights", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return _extract_weights_payload(r.json(), model_id)


async def _fetch_all_weights(client: httpx.AsyncClient, state) -> dict[int, list[str] | Exception]:
    """
    Параллельно опрашивает /weights у ВСЕХ дочерних сервисов разом.
    Возвращает {model_id: list_of_weights | Exception}.
    """
    ids = CHILD_MODEL_IDS
    results = await asyncio.gather(
        *[_fetch_weights_from_child(client, state.child_urls[mid], mid) for mid in ids],
        return_exceptions=True,
    )
    return dict(zip(ids, results))


async def _fetch_one_child_values(
    client: httpx.AsyncClient,
    model_id: int,
    url: str,
    pair: int,
    day: int,
    date: str,
    subp: dict,
) -> dict:
    """Один асинхронный запрос /values к дочернему сервису + масштабирование на k."""
    k = subp.get("k")
    if k is None:
        raise ValueError(f"Model {model_id}: missing 'k' in params")

    query_params = {key: val for key, val in subp.items() if key != "k"}
    query_params.update({"pair": pair, "day": day, "date": date})

    r   = await client.get(f"{url}/values", params=query_params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    res = r.json()
    values = _extract_result_dict(res, model_id)
    return {f"{model_id}_{key}": round(val * k, 6) for key, val in values.items()}


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
        tasks.append(_fetch_one_child_values(client, model_id, url, pair, day, date, subp))
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

    # 1. URL-ы всех дочерних моделей из brain_service — одним запросом
    async with engine_super.connect() as conn:
        placeholders = ", ".join(f":id{i}" for i in range(len(CHILD_MODEL_IDS)))
        params = {f"id{i}": mid for i, mid in enumerate(CHILD_MODEL_IDS)}
        res  = await conn.execute(text(
            f"SELECT id, url FROM brain_service WHERE id IN ({placeholders}) AND active = 1"
        ), params)
        urls = {row[0]: row[1].rstrip("/") for row in res.fetchall()}

    app.state.child_urls = {}
    for mid in CHILD_MODEL_IDS:
        url = urls.get(mid)
        if not url:
            raise RuntimeError(f"URL for model {mid} not found in brain_service (active=1)")
        app.state.child_urls[mid] = url
        log(f"URL[{mid}]: {url}", NODE_NAME, force=True)

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

    # 4. Предзагрузка весов для нестатичных моделей — ПАРАЛЛЕЛЬНО
    app.state.child_weights = {mid: None for mid in CHILD_MODEL_IDS}
    nonstatic_ids = [mid for mid in CHILD_MODEL_IDS if not app.state.child_static[mid]]
    if nonstatic_ids:
        results = await asyncio.gather(
            *[_fetch_weights_from_child(app.state.http_client, app.state.child_urls[mid], mid)
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
        "static":         {str(mid): app.state.child_static[mid] for mid in CHILD_MODEL_IDS},
        "weights_cached": {str(mid): app.state.child_weights[mid] is not None for mid in CHILD_MODEL_IDS},
        "metadata": {"child_models": CHILD_MODEL_IDS},
    }


@app.get("/weights")
async def get_weights():
    client = app.state.http_client
    to_fetch = [mid for mid in CHILD_MODEL_IDS
                if app.state.child_static[mid] or app.state.child_weights[mid] is None]
    try:
        if to_fetch:
            fetched = await asyncio.gather(
                *[_fetch_weights_from_child(client, app.state.child_urls[mid], mid) for mid in to_fetch]
            )
            fetched_map = dict(zip(to_fetch, fetched))
        else:
            fetched_map = {}
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(f"Child service unreachable: {e}")

    combined = []
    for mid in CHILD_MODEL_IDS:
        w = fetched_map.get(mid, app.state.child_weights.get(mid))
        combined += [f"{mid}_" + code for code in w]
    deduped = list(dict.fromkeys(combined))
    return ok_response(deduped)


@app.get("/new_weights")
async def new_weights():
    try:
        results = await _fetch_all_weights(app.state.http_client, app.state)
        all_weights: list[str] = []
        for mid in CHILD_MODEL_IDS:
            result = results[mid]
            if isinstance(result, Exception):
                send_error_trace(result, NODE_NAME)
                weights = app.state.child_weights.get(mid) or []
            else:
                weights = result
                if not app.state.child_static[mid]:
                    app.state.child_weights[mid] = weights
            all_weights += [f"{mid}_" + w for w in weights]
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
            if cols_a:
                cols_str = ", ".join(cols_a)
                res = await conn.execute(
                    text(f"SELECT {cols_str} FROM `{TABLE_A}` "
                         f"WHERE id = :sid AND tier = :tier AND is_day = :day"),
                    {"sid": sid, "tier": tier, "day": day},
                )
                row = res.fetchone()
                if row:
                    param_obj = {
                        col: ([row[i]] if col == "param" and row[i] is not None else row[i])
                        for i, col in enumerate(cols_a)
                    }
                    params_a.append(param_obj)

        for sid in sorted_b:
            if len(params_b) >= max_per_model:
                break
            if cols_b:
                cols_str = ", ".join(cols_b)
                res = await conn.execute(
                    text(f"SELECT {cols_str} FROM `{TABLE_B}` "
                         f"WHERE id = :sid AND tier = :tier AND is_day = :day"),
                    {"sid": sid, "tier": tier, "day": day},
                )
                row = res.fetchone()
                if row:
                    param_obj = {
                        col: ([row[i]] if col == "param" and row[i] is not None else row[i])
                        for i, col in enumerate(cols_b)
                    }
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
