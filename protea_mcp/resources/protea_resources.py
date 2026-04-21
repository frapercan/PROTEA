"""Resources MCP — URIs protea://* para exponer datos direccionables."""

from __future__ import annotations

import json
from uuid import UUID

from protea_mcp.server import mcp
from protea_mcp.tools.notes import read_notes
from protea_mcp.tools.operations import get_job, job_events
from protea_mcp.tools.prediction_sets import get_prediction_set
from protea_mcp.tools.queues import queue_status


def _j(obj) -> str:
    return json.dumps(obj, default=str, indent=2, ensure_ascii=False)


@mcp.resource("protea://prediction_set/{prediction_set_id}")
def prediction_set_resource(prediction_set_id: str) -> str:
    return _j(get_prediction_set(UUID(prediction_set_id)))


@mcp.resource("protea://queue/status")
def queue_status_resource() -> str:
    return _j(queue_status())


@mcp.resource("protea://job/{job_id}")
def job_resource(job_id: str) -> str:
    return _j(get_job(UUID(job_id)))


@mcp.resource("protea://job/{job_id}/events")
def job_events_resource(job_id: str) -> str:
    return _j(job_events(UUID(job_id)))


@mcp.resource("protea://notes/{channel}")
def notes_channel_resource(channel: str) -> str:
    return _j(read_notes(channel))
