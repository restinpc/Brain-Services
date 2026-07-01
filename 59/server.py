import os, sys

from dotenv import load_dotenv
load_dotenv()

_here   = os.path.dirname(os.path.abspath(__file__))
_shared = os.path.join(_here, "..", "shared")
sys.path.insert(1, _shared)

_cfg_port = 8916
_cfg_path = os.path.join(_here, "config.toml")
if os.path.exists(_cfg_path):
    with open(_cfg_path, "r", encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line.startswith("port") and "=" in _line:
                try:
                    _cfg_port = int(_line.split("=")[1].strip().split("#")[0].strip())
                    break
                except ValueError:
                    pass

import model
from brain_framework import build_app

app = build_app(model)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", _cfg_port))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
