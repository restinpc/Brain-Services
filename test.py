import os
import traceback
import httpx

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "Node 1")

app = FastAPI()


@app.get("/test")
async def test():
    print("🔥 [/test] Эндпоинт вызван, сейчас будет деление на ноль...")
    result = 1 / 0  # инициируем исключение
    return {"ok": True}


@app.exception_handler(Exception)
async def on_any_exception(request: Request, exc: Exception):
    print(f"\n❌ [Exception Handler] Поймали исключение: {exc!r}")
    print(f"📍 URL: {request.url}")
    print(f"🔧 Method: {request.method}")

    logs = (
        f"Exception: {exc!r}\n"
        f"Method: {request.method}\n"
        f"URL: {request.url}\n\n"
        f"{traceback.format_exc()}"
    )

    payload = {
        "url": str(request.url),
        "node": NODE_NAME,
        "logs": logs,
    }

    print(f"\n📤 [POST] Отправляем данные на {TRACE_URL}")
    print(f"   - node: {NODE_NAME}")
    print(f"   - url: {request.url}")

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(TRACE_URL, data=payload)
            print(f"✅ [POST] Успешно отправлено! Status: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Ошибка отправки: {e}")

    return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


if __name__ == "__main__":
    import uvicorn

    print("🚀 Запускаем сервер на http://localhost:8000")
    print("📋 Тестовый эндпоинт: http://localhost:8000/test")
    uvicorn.run(app, host="0.0.0.0", port=8000)
