"""
common.py — shared utilities for all brain-* microservices.

Usage:
    from common import MODE, IS_DEV, log, ok_response, err_response, send_error_trace
"""

import os
import traceback
import requests

# ── Режим запуска ─────────────────────────────────────────────────────────────
MODE   = os.getenv("MODE", "dev").lower()   # "dev" | "prod"
IS_DEV = MODE == "dev"

# ── Трассировка ошибок ────────────────────────────────────────────────────────
_HANDLER  = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL = f"{_HANDLER}/trace.php"
EMAIL     = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, node: str, script: str = "server.py") -> None:
    logs = (
        f"Node: {node}\nScript: {script}\n"
        f"Exception: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    )
    try:
        log(f"📤 Sending error trace to {TRACE_URL}", node, force=True)
        r = requests.post(
            TRACE_URL,
            data={"url": "fastapi_microservice", "node": node, "email": EMAIL, "logs": logs},
            timeout=10,
        )
        log(f"✅ Trace sent, status={r.status_code}", node, force=True)
    except Exception as e:
        log(f"⚠️  Failed to send trace: {e}", node, force=True)


# ── Логирование ───────────────────────────────────────────────────────────────
def log(msg: str, node: str = "", level: str = "info", force: bool = False) -> None:
    """
    DEV  — печатает всё.
    PROD — печатает только level="error" или force=True.
    """
    if IS_DEV or force or level == "error":
        prefix = f"[{node}] " if node else ""
        print(f"{prefix}{msg}")


# ── Стандартные ответы ────────────────────────────────────────────────────────
def ok_response(payload: dict | list) -> dict:
    """{"status": "ok", "payLoad": payload}"""
    return {"status": "ok", "payLoad": payload}


def err_response(
    message: str,
    exc: Exception | None = None,
    node: str = "",
    script: str = "server.py",
) -> dict:
    """
    {"status": "error", "error": message, "payLoad": {}}

    Если передан exc — отправляет трассировку и перебрасывает исключение,
    чтобы FastAPI вернул 500.
    """
    if exc is not None:
        send_error_trace(exc, node=node, script=script)
        raise exc
    return {"status": "error", "error": message, "payLoad": {}}


# ── Engine builder ────────────────────────────────────────────────────────────
def build_engines():
    """
    Создаёт три AsyncEngine из переменных окружения:
      - engine_vlad  : локальная БД сервиса  (DB_*)
      - engine_brain : мастер-нода проекта   (MASTER_*)
      - engine_super : супер-нода (таблицы brain_service и т.п.) (SUPER_*)

    Возвращает (engine_vlad, engine_brain, engine_super).
    """
    from sqlalchemy.ext.asyncio import create_async_engine

    def _url(host, port, user, password, name):
        return f"mysql+aiomysql://{user}:{password}@{host}:{port}/{name}"

    engine_vlad = create_async_engine(
        _url(
            os.getenv("DB_HOST",     ""),
            os.getenv("DB_PORT",     "3306"),
            os.getenv("DB_USER",     ""),
            os.getenv("DB_PASSWORD", ""),
            os.getenv("DB_NAME",     ""),
        ),
        pool_size=20, echo=False,
    )

    engine_brain = create_async_engine(
        _url(
            os.getenv("MASTER_HOST",     ""),
            os.getenv("MASTER_PORT",     "3306"),
            os.getenv("MASTER_USER",     ""),
            os.getenv("MASTER_PASSWORD", ""),
            os.getenv("MASTER_NAME",     "brain"),
        ),
        pool_size=5, echo=False,
    )

    engine_super = create_async_engine(
        _url(
            os.getenv("SUPER_HOST",     ""),
            os.getenv("SUPER_PORT",     "3306"),
            os.getenv("SUPER_USER",     ""),
            os.getenv("SUPER_PASSWORD", ""),
            os.getenv("SUPER_NAME",     "brain"),
        ),
        pool_size=3, echo=False,
    )

    return engine_vlad, engine_brain, engine_super


# ── Workers ───────────────────────────────────────────────────────────────────
async def resolve_workers(engine_super, service_id: int, default: int = 1) -> int:
    """
    DEV  → всегда 1.
    PROD → читает brain_service.workers из engine_super;
           если =0, использует brain_models.priority.
    """
    if IS_DEV:
        return 1
    try:
        from sqlalchemy import text
        async with engine_super.connect() as conn:
            res = await conn.execute(
                text("SELECT workers FROM brain_service WHERE id = :sid"),
                {"sid": service_id},
            )
            row = res.fetchone()
            if row and row[0] and int(row[0]) > 0:
                return int(row[0])
            res2 = await conn.execute(
                text("SELECT priority FROM brain_models WHERE id = :sid"),
                {"sid": service_id},
            )
            row2 = res2.fetchone()
            if row2 and row2[0]:
                return max(1, int(row2[0]))
    except Exception as e:
        print(f"⚠️  resolve_workers error: {e}")
    return default