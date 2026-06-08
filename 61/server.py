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

MODEL_A_ID = 47   # brain-sasha_fred_cbbtcusd  (BTC events, type+var)
MODEL_B_ID = 53   # brain-sasha_fred_dgs10      (10Y Treasury, type+var)
SERVICE_ID = 61   # TODO: уточнить — свободный ID в brain_service / brain_models
PORT       = 8923 # TODO: уточнить (формула: 8862 + SERVICE_ID = 8920)
BEST_URL   = "https://server.brain-project.online/best.php"
NODE_NAME  = f"brain-complex-{MODEL_A_ID}-{MODEL_B_ID}-microservice"

# Fallback-диапазоны параметров на случай, если brain_signal<ID> ещё не заполнена
# (сервис 47 — старая архитектура без автоматической brain_signal таблицы).
# Используются в /params ТОЛЬКО если DESCRIBE brain_signal<ID> провалился.
_FALLBACK_TYPE_VAR = [
    {"type": t, "var": v}
    for t in range(3)   # type 0, 1, 2
    for v in range(4)   # var  0, 1, 2, 3
]

load_dotenv()

engine_vlad, engine_brain, engine_super = build_engines()

log(f"MODE={MODE}", NODE_NAME, force=True)
log(f"engines built via build_engines()", NODE_NAME)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

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


async def _compute_single(pair, day, date, combo: dict, state) -> dict | None:
    """
    Вычисляет результат для ОДНОЙ комбинации параметров.
    combo = {"47": {"type": 0, "var": 0, "k": 0.5}, "53": {"type": 0, "var": 0, "k": 0.5}}
    """
    combined = {}
    for model_str, subp in combo.items():
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
            log(f"Child model {model_id} error: {e}", NODE_NAME, level="error", force=True)
            return None
    return combined


async def _compute_composite(pair, day, date, param_dict, state) -> dict | None:
    """
    Вычисляет составной результат.

    Поддерживает два формата params (в зависимости от того, как PHP создал сигнал):

    1. Одна комбинация (neuronet_x2 стиль — один сигнал на одну комбо):
       {"47": {"type": 0, "var": 0, "k": 0.5}, "53": {...}}
       → возвращает значения для этой конкретной комбинации.

    2. Массив комбинаций (neuronet_python стиль — один сигнал на тир):
       [{"47": {...}, "53": {...}}, {"47": {...}, "53": {...}}, ...]
       → считает значения для каждой комбинации и усредняет ансамблем.
    """
    # Определяем формат
    if isinstance(param_dict, dict):
        # Формат 1: одна комбинация
        return await _compute_single(pair, day, date, param_dict, state)

    if isinstance(param_dict, list):
        # Формат 2: массив комбинаций — ансамблевое усреднение
        results: list[dict] = []
        for combo in param_dict:
            if not isinstance(combo, dict):
                continue
            r = await _compute_single(pair, day, date, combo, state)
            if r:
                results.append(r)
        if not results:
            return None
        # Собираем все ключи и усредняем
        all_keys = set(k for r in results for k in r)
        averaged = {}
        n = len(results)
        for key in all_keys:
            total = sum(r.get(key, 0.0) for r in results)
            averaged[key] = round(total / n, 6)
        return averaged

    return None


def _safe_describe_cols(conn_sync_result, model_id: int) -> list[str]:
    """Извлекает нужные колонки из результата DESCRIBE; без исключений."""
    return [row[0] for row in conn_sync_result if row[0] in ("type", "var", "param")]


# ---------------------------------------------------------------------------
# lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. URL дочерних сервисов из brain_service
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

    # 2. Колонки параметров сигналов (type / var / param)
    # Сервис 47 — старая архитектура, brain_signal47 может отсутствовать →
    # в таком случае cols будут [], и /params применит fallback-перебор.
    async with engine_brain.connect() as conn:
        try:
            result_a = await conn.execute(text(f"DESCRIBE `brain_signal{MODEL_A_ID}`"))
            app.state.cols_A = _safe_describe_cols(result_a.fetchall(), MODEL_A_ID)
        except Exception as e:
            log(f"brain_signal{MODEL_A_ID} не найдена: {e} → /params будет использовать fallback",
                NODE_NAME, level="warning", force=True)
            app.state.cols_A = []

        try:
            result_b = await conn.execute(text(f"DESCRIBE `brain_signal{MODEL_B_ID}`"))
            app.state.cols_B = _safe_describe_cols(result_b.fetchall(), MODEL_B_ID)
        except Exception as e:
            log(f"brain_signal{MODEL_B_ID} не найдена: {e} → /params будет использовать fallback",
                NODE_NAME, level="warning", force=True)
            app.state.cols_B = []

    log(f"Cols A ({MODEL_A_ID}): {app.state.cols_A or 'FALLBACK type+var'}", NODE_NAME, force=True)
    log(f"Cols B ({MODEL_B_ID}): {app.state.cols_B or 'FALLBACK type+var'}", NODE_NAME, force=True)

    # 3. Флаги static из brain_models (таблица в super/master, не в brain)
    async with engine_super.connect() as conn:
        res   = await conn.execute(text(
            "SELECT id, static FROM brain_models WHERE id IN (:a, :b)"
        ), {"a": MODEL_A_ID, "b": MODEL_B_ID})
        flags = {row[0]: bool(row[1]) for row in res.fetchall()}

    app.state.static_A = flags.get(MODEL_A_ID, True)
    app.state.static_B = flags.get(MODEL_B_ID, True)
    log(f"static_A={app.state.static_A}, static_B={app.state.static_B}", NODE_NAME, force=True)

    # 4. Предзагрузка весов (только для non-static моделей)
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
                log(f"Pre-load weights model {model_id}: {e}", NODE_NAME,
                    level="error", force=True)
                send_error_trace(e, NODE_NAME)

    # 5. URL этого сервиса и таблица кеша
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


# ---------------------------------------------------------------------------
# app
# ---------------------------------------------------------------------------

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
        "text":    f"Composite model {MODEL_A_ID} (BTC events) + {MODEL_B_ID} (10Y Treasury)",
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
        "metadata": {
            "child_models": [MODEL_A_ID, MODEL_B_ID],
            "cols_A": app.state.cols_A or "fallback",
            "cols_B": app.state.cols_B or "fallback",
        },
    }


@app.get("/weights")
async def get_weights():
    """Объединяет веса обоих дочерних сервисов с префиксом {model_id}_."""
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
    """Принудительно перезагружает веса с дочерних сервисов."""
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
    """
    Ключевой endpoint для комплексной модели.
    params — JSON вида: {"47": {"type": 1, "var": 2, "k": 0.6}, "53": {"type": 0, "var": 1, "k": 0.4}}
    """
    try:
        param_dict = json.loads(params)
    except Exception:
        return err_response("Invalid JSON in params")

    try:
        return await cached_values(
            engine_vlad  = engine_vlad,
            service_url  = app.state.service_url,
            pair         = pair,
            day          = day,
            date         = date,
            extra_params = param_dict,
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
    """
    Возвращает список комбинаций параметров для исследования.
    tier=0 → k=0.5 константно, до 20 комбинаций.
    tier=1 → k плавающий (0.1..0.9), до 100 комбинаций.
    Если brain_signal<ID> не существует → использует fallback перебор type×var.
    """
    max_per_model = 4 if tier == 0 else 3

    # --- Получаем рейтинг сигналов через best.php ---
    try:
        best_a = requests.get(
            f"{BEST_URL}?neuronet_id={MODEL_A_ID}&pair={pair}&day={day}", timeout=10).json()
        best_b = requests.get(
            f"{BEST_URL}?neuronet_id={MODEL_B_ID}&pair={pair}&day={day}", timeout=10).json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"best.php error: {e}")

    sorted_a = sorted(best_a, key=best_a.get, reverse=True) if isinstance(best_a, dict) else []
    sorted_b = sorted(best_b, key=best_b.get, reverse=True) if isinstance(best_b, dict) else []

    TABLE_A = f"brain_signal{MODEL_A_ID}"
    TABLE_B = f"brain_signal{MODEL_B_ID}"

    params_a: list[dict] = []
    params_b: list[dict] = []

    async with engine_brain.connect() as conn:

        # --- Модель A (47) ---
        if app.state.cols_A:
            # Нормальный путь: читаем из brain_signal47
            cols_str = ", ".join(app.state.cols_A)
            for sid in sorted_a:
                if len(params_a) >= max_per_model:
                    break
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
        else:
            # Fallback: brain_signal47 не найдена → перебираем type×var
            log(f"/params fallback для модели {MODEL_A_ID}: перебор type×var", NODE_NAME, level="warning")
            for p in _FALLBACK_TYPE_VAR[:max_per_model]:
                params_a.append(p)

        # --- Модель B (53) ---
        if app.state.cols_B:
            cols_str = ", ".join(app.state.cols_B)
            for sid in sorted_b:
                if len(params_b) >= max_per_model:
                    break
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
        else:
            log(f"/params fallback для модели {MODEL_B_ID}: перебор type×var", NODE_NAME, level="warning")
            for p in _FALLBACK_TYPE_VAR[:max_per_model]:
                params_b.append(p)

    # --- Строим комбинации ---
    combs: list[dict] = []

    if tier == 0:
        # k константно = 0.5
        for pa in params_a:
            for pb in params_b:
                combs.append({
                    str(MODEL_A_ID): {**pa, "k": 0.5},
                    str(MODEL_B_ID): {**pb, "k": 0.5},
                })
        return combs[:20]

    else:
        # k плавающий: 0.1..0.9 (шаг 0.1, сумма всегда 1.0)
        for pa in params_a:
            for pb in params_b:
                for ki in range(1, 10):
                    k = round(ki / 10, 1)
                    combs.append({
                        str(MODEL_A_ID): {**pa, "k": k},
                        str(MODEL_B_ID): {**pb, "k": round(1 - k, 1)},
                    })
        return combs[:100]


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


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    async def _get_workers():
        return await resolve_workers(engine_super, SERVICE_ID, default=1)
    _workers = asyncio.run(_get_workers())
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
