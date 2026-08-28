"""
server.py
  1. `import model` подхватывает model.py из ТЕКУЩЕЙ директории.
     service36/server.py → service36/model.py
     dummy/server.py     → dummy/model.py
  2. brain_framework_simple.py из shared/ делает всё остальное.
Переменные окружения (PORT, NODE_NAME, SERVICE_ID, SERVICE_TEXT)
читаются из .env файла, находящегося в папке сервиса.
"""
import sys
import os
from dotenv import load_dotenv

load_dotenv()

_shared = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared")
sys.path.insert(1, _shared)

import model
from brain_framework import build_app

app = build_app(model)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
