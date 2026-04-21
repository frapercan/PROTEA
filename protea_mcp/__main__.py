"""Entrypoint. Elige transporte por env var PROTEA_MCP_TRANSPORT: stdio|sse|http."""

from __future__ import annotations

import os

from protea_mcp.server import mcp


def main() -> None:
    transport = os.environ.get("PROTEA_MCP_TRANSPORT", "stdio")
    if transport == "stdio":
        mcp.run()
    elif transport in ("sse", "http"):
        host = os.environ.get("PROTEA_MCP_HOST", "127.0.0.1")
        port = int(os.environ.get("PROTEA_MCP_PORT", "8765"))
        mcp.run(transport=transport, host=host, port=port)
    else:
        raise SystemExit(f"Unknown transport: {transport}")


if __name__ == "__main__":
    main()
