"""FastMCP instance + registro de tools/resources/prompts."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("protea")

# --- Tools -------------------------------------------------------------------
from protea_mcp.tools import (  # noqa: E402,F401
    admin,
    notes,
    operations,
    prediction_sets,
    queues,
)

# --- Resources ---------------------------------------------------------------
from protea_mcp.resources import protea_resources  # noqa: E402,F401

# --- Prompts -----------------------------------------------------------------
from protea_mcp.prompts import debugging  # noqa: E402,F401
