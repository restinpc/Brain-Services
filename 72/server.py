"""Точка запуска модели 28 через общий Brain Framework."""
import os
import sys

from dotenv import load_dotenv

load_dotenv()

_shared = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared")
sys.path.insert(1, _shared)

import model
from brain_framework import build_app, get_service_config

app = build_app(model)

if __name__ == "__main__":
    import uvicorn

    cfg = get_service_config() or {}
    port = int((cfg.get("service") or {}).get("port", 8892))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
