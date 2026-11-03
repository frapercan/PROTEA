"""``/model-weights``: the service's answers as status codes (ADR-D54).

The service is pinned in ``test_model_weights_service.py`` and against Postgres in
``test_model_weights_pg.py``. This file pins the mapping a client depends on: a wrong
request is 422, a missing upload or object 404, a key already registered 409 with the id
of the row that holds it, and a part is the raw body of a PUT.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers import model_weights as router_module
from protea.services import model_weights_service as svc

ROW = {
    "id": "0d3f6c1e-2b7a-4f0e-9a51-3c2d1e0f9b8a",
    "kind": "residue_vocabulary",
    "name": "vocabulary-v1",
    "uri": "residue-vocabulary/vocabulary-v1-06fed75ca76d17be.npz",
    "sha256": "06fed75ca76d17be" + "0" * 48,
    "size_bytes": 260233730,
}


@contextmanager
def _scope(_factory):
    yield MagicMock()


@pytest.fixture()
def client():
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.state.settings = MagicMock()
    app.include_router(router_module.router)
    with (
        patch.object(router_module, "session_scope", side_effect=_scope),
        patch.object(router_module, "get_artifact_store", return_value=MagicMock()) as store,
    ):
        client = TestClient(app)
        client.store_factory = store  # type: ignore[attr-defined]
        yield client


def test_the_listing_passes_the_kind_through(client) -> None:
    with patch.object(svc, "list_weights", return_value=[ROW]) as listing:
        response = client.get("/model-weights", params={"kind": "residue_vocabulary"})
    assert response.status_code == 200
    assert response.json() == [ROW]
    assert listing.call_args.args[1] == "residue_vocabulary"


def test_an_unknown_kind_in_the_listing_is_422(client) -> None:
    with patch.object(svc, "list_weights", side_effect=svc.WeightsError("unknown kind 'x'")):
        assert client.get("/model-weights", params={"kind": "x"}).status_code == 422


def test_a_missing_weight_is_404(client) -> None:
    with patch.object(svc, "weights_detail", return_value=None):
        assert client.get(f"/model-weights/{uuid.uuid4()}").status_code == 404


def test_a_reference_already_registered_is_409_with_the_existing_id(client) -> None:
    existing = uuid.uuid4()
    with patch.object(
        svc, "register_by_reference", side_effect=svc.WeightsConflict("taken", existing)
    ):
        response = client.post(
            "/model-weights/by-reference",
            json={"kind": "residue_vocabulary", "name": "v", "uri": "k.npz", "sha256": "a" * 64},
        )
    assert response.status_code == 409
    assert response.json()["detail"]["existing_id"] == str(existing)


def test_a_reference_the_store_does_not_hold_is_404(client) -> None:
    with patch.object(
        svc, "register_by_reference", side_effect=svc.WeightsNotFound("not in the store")
    ):
        response = client.post(
            "/model-weights/by-reference",
            json={"kind": "reader", "name": "r", "uri": "r.pt", "sha256": "a" * 64},
        )
    assert response.status_code == 404


def test_a_registered_reference_is_201_with_its_row(client) -> None:
    with (
        patch.object(svc, "register_by_reference", return_value=MagicMock()),
        patch.object(svc, "weights_dict", return_value=ROW),
    ):
        response = client.post(
            "/model-weights/by-reference",
            json={
                "kind": "residue_vocabulary",
                "name": "vocabulary-v1",
                "uri": ROW["uri"],
                "sha256": "a" * 64,
            },
        )
    assert response.status_code == 201
    assert response.json() == ROW


def test_a_part_is_the_raw_body_of_a_put(client) -> None:
    with patch.object(svc, "put_part", return_value={"received": 1, "parts": 9}) as put:
        response = client.put(
            "/model-weights/uploads/6b1f0c9e-1d3a-4c55-9a0e-6e0c5d7f2a11/parts/3",
            content=b"\x00\x01\x02",
            headers={"Content-Type": "application/octet-stream"},
        )
    assert response.status_code == 200
    assert put.call_args.args == ("6b1f0c9e-1d3a-4c55-9a0e-6e0c5d7f2a11", 3, b"\x00\x01\x02")


def test_a_part_of_the_wrong_size_is_422(client) -> None:
    with patch.object(svc, "put_part", side_effect=svc.WeightsError("part 3 has 3 bytes")):
        response = client.put("/model-weights/uploads/x/parts/3", content=b"abc")
    assert response.status_code == 422


def test_the_store_comes_from_the_application_settings(client) -> None:
    """Not from a settings load without a project root, which raises: the store is built
    from the settings the application was started with."""
    with (
        patch.object(svc, "complete_upload", return_value=MagicMock()),
        patch.object(svc, "weights_dict", return_value=ROW),
    ):
        client.post("/model-weights/uploads/6b1f0c9e-1d3a-4c55-9a0e-6e0c5d7f2a11/complete")
    assert client.store_factory.call_args.args[0] is client.app.state.settings


def test_completing_an_upload_registers_it(client) -> None:
    with (
        patch.object(svc, "complete_upload", return_value=MagicMock()),
        patch.object(svc, "weights_dict", return_value=ROW),
    ):
        response = client.post(
            "/model-weights/uploads/6b1f0c9e-1d3a-4c55-9a0e-6e0c5d7f2a11/complete"
        )
    assert response.status_code == 201
    assert response.json()["uri"] == ROW["uri"]


def test_an_upload_opens_with_its_part_size(client) -> None:
    with patch.object(
        svc, "create_upload", return_value={"upload_id": "u", "part_bytes": 33554432, "parts": 8}
    ):
        response = client.post(
            "/model-weights/uploads",
            json={
                "kind": "residue_vocabulary",
                "name": "v",
                "filename": "v.npz",
                "size_bytes": 260233730,
            },
        )
    assert response.status_code == 201
    assert response.json()["parts"] == 8
