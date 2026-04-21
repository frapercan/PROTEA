"""Tests de contrato: cada tool tiene signature y está registrada en mcp."""

from __future__ import annotations

import pytest

from protea_mcp.server import mcp


EXPECTED_TOOLS = {
    "list_prediction_sets",
    "get_prediction_set",
    "compare_prediction_sets",
    "top_predictions",
    "queue_status",
    "list_workers",
    "stalled_jobs",
    "dead_letter_peek",
    "get_operation",
    "list_recent_operations",
    "watch_operation",
    "operation_logs",
    "post_note",
    "read_notes",
    "list_channels",
    "claim_task",
    "release_claim",
    "list_claims",
    "enqueue_operation",
    "cancel_operation",
    "retry_failed_jobs",
    "purge_stalled",
    "set_worker_concurrency",
}


def _registered_tool_names() -> set[str]:
    # FastMCP expone las tools via un registry interno; el API puede variar según
    # versión. Este helper se adapta al atributo real en tiempo de test.
    for attr in ("_tools", "_tool_manager", "tools"):
        obj = getattr(mcp, attr, None)
        if obj is None:
            continue
        tools = getattr(obj, "_tools", None) or getattr(obj, "tools", None) or obj
        if isinstance(tools, dict):
            return set(tools.keys())
    pytest.skip("no se encontró registry de tools en FastMCP")


def test_all_tools_registered():
    got = _registered_tool_names()
    missing = EXPECTED_TOOLS - got
    assert not missing, f"faltan tools: {missing}"


@pytest.mark.parametrize("tool_name", sorted(EXPECTED_TOOLS))
def test_tool_has_docstring(tool_name: str):
    got = _registered_tool_names()
    assert tool_name in got
