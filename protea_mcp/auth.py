"""Auth opcional (activa solo con transporte HTTP/SSE).

Configurable por env:
  PROTEA_MCP_API_KEYS="key1,key2"   claves válidas separadas por coma
  PROTEA_MCP_REQUIRE_AUTH=1         bloquea si no hay header
"""

from __future__ import annotations

import os
from typing import Optional


def _allowed_keys() -> set[str]:
    raw = os.environ.get("PROTEA_MCP_API_KEYS", "")
    return {k.strip() for k in raw.split(",") if k.strip()}


def require_auth() -> bool:
    return os.environ.get("PROTEA_MCP_REQUIRE_AUTH") == "1"


def validate(api_key: Optional[str]) -> bool:
    if not require_auth():
        return True
    keys = _allowed_keys()
    return bool(api_key) and api_key in keys
