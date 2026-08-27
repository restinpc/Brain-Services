"""
server.py
  1. `import model` imports local model.py.
  2. brain_framework.py from shared/ bootstraps FastAPI app.
"""
import os
import sys

from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.normpath(os.path.join(_HERE, ".."))
# Явный путь: при `cd 91 && python server.py` find_dotenv() не всегда
# поднимается до корневого .env, и сервис стартует без DB_*.
load_dotenv(os.path.join(_ROOT, ".env"))
load_dotenv(os.path.join(_HERE, ".env"), override=True)

_shared = os.path.join(_ROOT, "shared")
sys.path.insert(1, _shared)

import model
from brain_framework import build_app, get_service_config

app = build_app(model)

if __name__ == "__main__":
    import uvicorn

    cfg = get_service_config() or {}
    port = int((cfg.get("service") or {}).get("port", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
