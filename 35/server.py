import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import uvicorn
import asyncio
import json
import requests
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

MODEL_A_ID = 32
MODEL_B_ID = 31
SERVICE_ID = 34
PORT       = 8897
BEST_URL   = "https://server.brain-project.online/best.php"
NODE_NAME  = f"brain-complex-{MODEL_A_ID}-{MODEL_B_ID}-microservice"

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)


def _fetch_weights_from_child(url: str, model_id: int) -> list[str]:
    """Синхронный запрос /weights у дочернего сервиса."""
    r = requests.get(f"{url}/weights", timeout=10)
    r.raise_for_status()
    data = r.json()
    if "payLoad" in data:
        return data["payLoad"]
    if "weights" in data:
        return data["weights"]
    raise ValueError(f"Model {model_id}: no 'weights' in response")


async def _compute_composite(pair, day, date, param_dict, state) -> dict | None:
    """Логика вычисления составной модели — вызывает дочерние сервисы."""
    combined = {}
    for model_str, subp in param_dict.items():
        model_id = int(model_str)
        k = subp.get("k")
        if k is None:
            return None
        url = (state.URL_A if model_id == MODEL_A_ID
               else state.URL_B if model_id == MODEL_B_ID
               else None)
        if url is None:
            continue
        query_params = {key: val for key, val in subp.items() if key != "k"}
        query_params.update({"pair": pair, "day": day, "date": date})
        try:
            r   = requests.get(f"{url}/values", params=query_params, timeout=10)
            res = r.json()
            if "payLoad" in res:
                res = res["payLoad"]
            if "error" in res:
                return None
            for key, val in res.items():
                combined[f"{model_id}_{key}"] = round(val * k, 6)
        except Exception as e:
            log(f"❌ Child model {model_id} error: {e}", NODE_NAME, level="error", force=True)
            return None
    return combined


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. URL-ы из brain_service
    async with engine_super.connect() as conn:
        res  = await conn.execute(text(
            "SELECT id, url FROM brain_service WHERE id IN (:a, :b) AND active = 1"
        ), {"a": MODEL_A_ID, "b": MODEL_B_ID})
        urls = {row[0]: row[1].rstrip("/") for row in res.fetchall()}

    app.state.URL_A = urls.get(MODEL_A_ID)
    app.state.URL_B = urls.get(MODEL_B_ID)

    if not app.state.URL_A:
        raise RuntimeError(f"URL for model {MODEL_A_ID} not found in brain_service (active=1)")
    if not app.state.URL_B:
        raise RuntimeError(f"URL for model {MODEL_B_ID} not found in brain_service (active=1)")

    log(f"URL_A ({MODEL_A_ID}): {app.state.URL_A}", NODE_NAME, force=True)
    log(f"URL_B ({MODEL_B_ID}): {app.state.URL_B}", NODE_NAME, force=True)

    # 2. Колонки параметров сигналов
    async with engine_brain.connect() as conn:
        result_a = await conn.execute(text(f"DESCRIBE `brain_signal{MODEL_A_ID}`"))
        app.state.cols_A = [row[0] for row in result_a.fetchall()
                            if row[0] in ("type", "var", "param")]
        result_b = await conn.execute(text(f"DESCRIBE `brain_signal{MODEL_B_ID}`"))
        app.state.cols_B = [row[0] for row in result_b.fetchall()
                            if row[0] in ("type", "var", "param")]

    log(f"Cols A: {app.state.cols_A}", NODE_NAME)
    log(f"Cols B: {app.state.cols_B}", NODE_NAME)

    # 3. Флаги static
    async with engine_brain.connect() as conn:
        res   = await conn.execute(text(
            "SELECT id, static FROM brain_models WHERE id IN (:a, :b)"
        ), {"a": MODEL_A_ID, "b": MODEL_B_ID})
        flags = {row[0]: bool(row[1]) for row in res.fetchall()}

    app.state.static_A = flags.get(MODEL_A_ID, True)
    app.state.static_B = flags.get(MODEL_B_ID, True)
    log(f"static_A={app.state.static_A}, static_B={app.state.static_B}", NODE_NAME, force=True)

    # 4. Предзагрузка весов для нестатичных моделей
    app.state.weights_A = None
    app.state.weights_B = None

    for model_id, url, is_static, attr in [
        (MODEL_A_ID, app.state.URL_A, app.state.static_A, "weights_A"),
        (MODEL_B_ID, app.state.URL_B, app.state.static_B, "weights_B"),
    ]:
        if not is_static:
            try:
                w = _fetch_weights_from_child(url, model_id)
                setattr(app.state, attr, w)
                log(f"Pre-loaded {len(w)} weights for model {model_id}", NODE_NAME, force=True)
            except Exception as e:
                log(f"⚠️  Pre-load weights model {model_id}: {e}", NODE_NAME,
                    level="error", force=True)
                send_error_trace(e, NODE_NAME)

    # 5. URL этого сервиса и таблица кеша
    # Для составной модели SERVICE_URL читается из brain_service по SERVICE_ID=34
    async with engine_super.connect() as conn:
        row = (await conn.execute(
            text("SELECT url FROM brain_service WHERE id = :sid"), {"sid": SERVICE_ID}
        )).fetchone()
    app.state.service_url = row[0].rstrip("/") if row and row[0] else f"http://localhost:{PORT}"
    await ensure_cache_table(engine_vlad)
    log(f"SERVICE_URL (self): {app.state.service_url}", NODE_NAME, force=True)

    yield
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
        "text":    f"Composite model {MODEL_A_ID} + {MODEL_B_ID}",
        "child_urls": {
            str(MODEL_A_ID): app.state.URL_A,
            str(MODEL_B_ID): app.state.URL_B,
        },
        "static": {
            str(MODEL_A_ID): app.state.static_A,
            str(MODEL_B_ID): app.state.static_B,
        },
        "weights_cached": {
            str(MODEL_A_ID): app.state.weights_A is not None,
            str(MODEL_B_ID): app.state.weights_B is not None,
        },
        "metadata": {"child_models": [MODEL_A_ID, MODEL_B_ID]},
    }


@app.get("/weights")
async def get_weights():
    try:
        w_a = (
            app.state.weights_A
            if (not app.state.static_A and app.state.weights_A is not None)
            else _fetch_weights_from_child(app.state.URL_A, MODEL_A_ID)
        )
        w_b = (
            app.state.weights_B
            if (not app.state.static_B and app.state.weights_B is not None)
            else _fetch_weights_from_child(app.state.URL_B, MODEL_B_ID)
        )
    except Exception as e:
        send_error_trace(e, node=NODE_NAME, script="get_weights")
        return err_response(f"Child service unreachable: {e}")
    combined = [f"{MODEL_A_ID}_" + w for w in w_a] + [f"{MODEL_B_ID}_" + w for w in w_b]
    deduped  = list(dict.fromkeys(combined))
    return ok_response(deduped)


@app.get("/new_weights")
async def new_weights():
    try:
        all_weights: list[str] = []
        for model_id, url, is_static, attr in [
            (MODEL_A_ID, app.state.URL_A, app.state.static_A, "weights_A"),
            (MODEL_B_ID, app.state.URL_B, app.state.static_B, "weights_B"),
        ]:
            try:
                weights = _fetch_weights_from_child(url, model_id)
                if not is_static:
                    setattr(app.state, attr, weights)
            except Exception as e:
                send_error_trace(e, NODE_NAME)
                weights = getattr(app.state, attr) or []
            all_weights += [f"{model_id}_" + w for w in weights]
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

    # Кешируем весь составной результат: ключ кеша включает params JSON
    try:
        return await cached_values(
            engine_vlad=engine_vlad,
            service_url  = app.state.service_url,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = param_dict,   # весь params как ключ
            compute_fn   = lambda: _compute_composite(pair, day, date, param_dict, app.state),
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
    max_per_model = 4 if tier == 0 else 3
    try:
        best_a = requests.get(
            f"{BEST_URL}?neuronet_id={MODEL_A_ID}&pair={pair}&day={day}", timeout=10).json()
        best_b = requests.get(
            f"{BEST_URL}?neuronet_id={MODEL_B_ID}&pair={pair}&day={day}", timeout=10).json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"best.php error: {e}")

    sorted_a = sorted(best_a, key=best_a.get, reverse=True)
    sorted_b = sorted(best_b, key=best_b.get, reverse=True)

    TABLE_A = f"brain_signal{MODEL_A_ID}"
    TABLE_B = f"brain_signal{MODEL_B_ID}"

    params_a, params_b = [], []

    async with engine_brain.connect() as conn:
        for sid in sorted_a:
            if len(params_a) >= max_per_model:
                break
            if app.state.cols_A:
                cols_str = ", ".join(app.state.cols_A)
                res = await conn.execute(
                    text(f"SELECT {cols_str} FROM `{TABLE_A}` "
                         f"WHERE id = :sid AND tier = :tier AND is_day = :day"),
                    {"sid": sid, "tier": tier, "day": day},
                )
                row = res.fetchone()
                if row:
                    param_obj = {
                        col: ([row[i]] if col == "param" and row[i] is not None else row[i])
                        for i, col in enumerate(app.state.cols_A)
                    }
                    params_a.append(param_obj)

        for sid in sorted_b:
            if len(params_b) >= max_per_model:
                break
            if app.state.cols_B:
                cols_str = ", ".join(app.state.cols_B)
                res = await conn.execute(
                    text(f"SELECT {cols_str} FROM `{TABLE_B}` "
                         f"WHERE id = :sid AND tier = :tier AND is_day = :day"),
                    {"sid": sid, "tier": tier, "day": day},
                )
                row = res.fetchone()
                if row:
                    param_obj = {
                        col: ([row[i]] if col == "param" and row[i] is not None else row[i])
                        for i, col in enumerate(app.state.cols_B)
                    }
                    params_b.append(param_obj)

    combs = []
    if tier == 0:
        for pa in params_a:
            for pb in params_b:
                combs.append({
                    str(MODEL_A_ID): {**pa, "k": 0.5},
                    str(MODEL_B_ID): {**pb, "k": 0.5},
                })
    else:
        for pa in params_a:
            for pb in params_b:
                for ki in range(1, 10):
                    k = round(ki / 10, 1)
                    combs.append({
                        str(MODEL_A_ID): {**pa, "k": k},
                        str(MODEL_B_ID): {**pb, "k": round(1 - k, 1)},
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