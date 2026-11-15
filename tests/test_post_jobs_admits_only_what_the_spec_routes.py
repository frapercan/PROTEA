"""``POST /jobs`` admits an operation only as its spec routes it (ADR-D60, decision 6).

Before the operation descriptor the endpoint checked neither the operation, nor
the queue, nor the kind it was given. An unknown name failed only on dequeue; a
message stage or a write got a Job row whose id its ``OperationConsumer`` cannot
read; and a job posted to the wrong queue waited where no consumer, or the wrong
one, would take it. Now each is a 422 before anything is written, counted or
published, and every request the specs route behaves as it did.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.jobs import CreateJobRequest, router
from protea.core import queues
from protea.core.operation_catalog import build_operation_registry
from protea.core.operation_spec import job_queues, refuse_job_request
from protea.core.operations.compute_embeddings import ComputeEmbeddingsPayload

REGISTRY = build_operation_registry()
SPECS = REGISTRY.specs()
JOBS = sorted(name for name, spec in SPECS.items() if spec.carries_jobs)
NOT_JOBS = sorted(name for name, spec in SPECS.items() if not spec.carries_jobs)


class _Posted:
    """What one request did: its response, and whether it wrote or published."""

    def __init__(self, response: Any, session: MagicMock, publish: MagicMock) -> None:
        self.status = response.status_code
        self.body = response.json()
        self.wrote = session.add.called
        self.published = publish.call_args_list


@contextmanager
def _scope(session: MagicMock) -> Iterator[MagicMock]:
    yield session


def _post(operation: str, queue: str) -> _Posted:
    session = MagicMock()
    session.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = None
    job = MagicMock(id=uuid4())
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.state.amqp_url = "amqp://guest:guest@localhost/"
    app.state.operation_registry = REGISTRY
    app.include_router(router)
    with (
        patch("protea.api.routers.jobs.session_scope", side_effect=lambda _f: _scope(session)),
        patch("protea.api.routers.jobs.publish_job") as publish,
        patch("protea.api.routers.jobs.Job", return_value=job),
    ):
        response = TestClient(app).post("/jobs", json={"operation": operation, "queue_name": queue})
    return _Posted(response, session, publish)


# Admitted, as today --------------------------------------------------------------------


@pytest.mark.parametrize("operation", JOBS)
def test_a_job_on_its_spec_queue_is_created_and_published(operation: str) -> None:
    queue = SPECS[operation].queue
    posted = _post(operation, queue)
    assert posted.status == 200 and posted.body["status"] == "queued"
    assert posted.wrote
    assert [call.args[1] for call in posted.published] == [queue]


@pytest.mark.parametrize("queue", sorted(job_queues(SPECS.values())))
def test_the_probe_is_admitted_on_any_job_queue(queue: str) -> None:
    """``scripts/test_deploy_e2e.sh`` posts ping to protea.jobs: its slice has no ping worker."""
    posted = _post("ping", queue)
    assert posted.status == 200
    assert [call.args[1] for call in posted.published] == [queue]


def test_the_request_example_is_admitted_and_its_payload_validates() -> None:
    extra = cast(dict[str, Any], CreateJobRequest.model_config["json_schema_extra"])
    example = cast(dict[str, Any], extra["example"])
    assert refuse_job_request(SPECS, example["operation"], example["queue_name"]) is None
    ComputeEmbeddingsPayload.model_validate(example["payload"])


# Refused, before anything is written ---------------------------------------------------


def _refused(posted: _Posted, error: str) -> dict[str, Any]:
    assert posted.status == 422
    assert not posted.wrote and posted.published == []
    detail: dict[str, Any] = posted.body["detail"]
    assert detail["error"] == error
    return detail


def test_an_unknown_operation_is_refused() -> None:
    detail = _refused(_post("train_reranker", queues.TRAINING), "unknown_operation")
    assert detail["expected_queue"] is None


@pytest.mark.parametrize("operation", NOT_JOBS)
def test_a_message_stage_or_a_write_never_gets_a_job_row(operation: str) -> None:
    detail = _refused(_post(operation, SPECS[operation].queue), "not_a_job")
    assert SPECS[operation].kind in detail["message"]


@pytest.mark.parametrize(
    ("operation", "queue", "expected"),
    [
        ("insert_proteins", queues.EVALUATIONS, queues.JOBS),
        ("run_cafa_evaluation", queues.JOBS, queues.EVALUATIONS),
        # What scripts/run_experiments.py posted before this change.
        ("predict_go_terms", queues.JOBS, queues.PREDICTIONS),
        ("compute_embeddings", "protea.embedding", queues.EMBEDDINGS),
        ("run_lab_method", queues.JOBS, queues.METHODS),
    ],
)
def test_a_job_on_another_queue_is_refused(operation: str, queue: str, expected: str) -> None:
    detail = _refused(_post(operation, queue), "wrong_queue")
    assert (detail["queue_name"], detail["expected_queue"]) == (queue, expected)


@pytest.mark.parametrize("queue", [queues.EMBEDDINGS_BATCH, queues.PREDICTIONS_WRITE, "test.q"])
def test_the_probe_is_refused_where_no_job_consumer_listens(queue: str) -> None:
    detail = _refused(_post("ping", queue), "wrong_queue")
    assert detail["expected_queue"] == queues.PING


def test_a_refusal_spends_no_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    spent = MagicMock()
    monkeypatch.setattr("protea.api.routers.jobs._maybe_enforce_user_quota", spent)
    _refused(_post("run_cafa_evaluation", queues.JOBS), "wrong_queue")
    spent.assert_not_called()
