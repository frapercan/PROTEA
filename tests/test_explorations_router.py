"""The explorations router maps the service's refusals and keeps the decision the author's.

The service is patched: its own tests cover the moves. What is pinned here is
what HTTP adds: the status codes, the principal read as an actor, and the
decision refused to anything but a user session, an API key included whatever
its role.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.auth import require_api_key_or_bearer
from protea.api.bearer import BearerPrincipal
from protea.api.routers import explorations as router_module
from protea.services import explorations_service as svc

DESCRIBED = {"id": str(uuid.uuid4()), "number": 1, "state": "draft"}
AUTHOR = BearerPrincipal(sub="u1", claims={"role": "admin", "email": "author@example.org"})


@contextmanager
def _scope(_factory: Any) -> Any:
    yield MagicMock()


def _client(principal: Any = None) -> TestClient:
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(router_module.router)
    if principal is not None:
        app.dependency_overrides[require_api_key_or_bearer] = lambda: principal
    return TestClient(app)


@pytest.fixture(autouse=True)
def _patched() -> Any:
    with (
        patch.object(router_module, "session_scope", side_effect=_scope),
        patch.object(svc, "find", return_value=MagicMock()),
        patch.object(svc, "describe", return_value=DESCRIBED),
    ):
        yield


def test_a_draft_is_created_with_the_principal_as_proposer() -> None:
    body = {
        "title": "t",
        "question": "q",
        "hypothesis": "h",
        "kind": "instrument",
        "evaluation_set_id": str(uuid.uuid4()),
    }
    with patch.object(svc, "create", return_value=MagicMock()) as create:
        response = _client().post("/explorations", json=body)
    assert response.status_code == 201
    assert response.json() == DESCRIBED
    assert create.call_args.args[2] == svc.Actor(name="local", type="user")


@pytest.mark.parametrize(
    ("raised", "status"),
    [
        (svc.ExplorationError("refused"), 422),
        (svc.ExplorationNotFound("no exploration"), 404),
        (svc.ExplorationConflict("wrong state"), 409),
    ],
)
def test_the_service_refusals_map_to_status_codes(raised: Exception, status: int) -> None:
    with patch.object(svc, "launch", side_effect=raised):
        assert _client().post("/explorations/1/launch").status_code == status


def test_the_decision_is_refused_without_a_user_session() -> None:
    with patch.object(svc, "decide") as decide:
        response = _client().post(
            "/explorations/1/decision", json={"decision": "accepted", "text": "holds"}
        )
    assert response.status_code == 403
    decide.assert_not_called()


def test_the_decision_is_refused_to_an_api_key_whatever_its_role() -> None:
    key = SimpleNamespace(name="laptop", role="admin")
    with patch.object(svc, "decide") as decide:
        response = _client(key).post(
            "/explorations/1/decision", json={"decision": "accepted", "text": "holds"}
        )
    assert response.status_code == 403
    decide.assert_not_called()


def test_the_author_decides_from_a_user_session() -> None:
    with patch.object(svc, "decide") as decide:
        response = _client(AUTHOR).post(
            "/explorations/1/decision", json={"decision": "accepted", "text": "holds"}
        )
    assert response.status_code == 200
    assert decide.call_args.args[-1] == svc.Actor(name="author@example.org", type="user")


class TestThePrincipalAsAnActor:
    def test_a_user_session_is_a_user_named_by_email(self) -> None:
        assert router_module.actor_of(AUTHOR) == svc.Actor(name="author@example.org", type="user")

    def test_an_api_key_is_an_api_key_named_by_its_label(self) -> None:
        key = SimpleNamespace(name="laptop")
        assert router_module.actor_of(key) == svc.Actor(name="key:laptop", type="api_key")

    def test_the_open_development_gate_is_local(self) -> None:
        assert router_module.actor_of(None) == svc.Actor(name="local", type="user")


def test_the_listing_is_public_and_passes_its_filters() -> None:
    window = uuid.uuid4()
    with patch.object(svc, "listing", return_value=[DESCRIBED]) as listing:
        response = _client().get(
            "/explorations", params={"state": "running", "evaluation_set_id": str(window)}
        )
    assert response.status_code == 200
    assert listing.call_args.kwargs == {
        "state": "running",
        "evaluation_set_id": window,
        "include_archived": False,
    }
