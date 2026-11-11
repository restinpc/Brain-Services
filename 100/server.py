"""FastAPI entry point for GDELT service 100."""
from pathlib import Path
import sys

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE.parent / ".env")
load_dotenv(HERE / ".env", override=True)
sys.path.insert(1, str(HERE.parent / "shared"))

import model
from brain_framework import build_app, get_service_config

app = build_app(model)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=int(get_service_config()["service"]["port"]), reload=False)
