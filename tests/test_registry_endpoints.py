"""Tests for the plugin-registry endpoints (T2B.1-3 of master plan v3).

Three endpoints — ``GET /backends``, ``GET /sources``, ``GET /runners``
— each list the entry-point-discovered plugins for the corresponding
group. The plugins themselves are real (installed via the C-stack
sibling repos), so these tests run against the live discovery rather
than mocking ``importlib.metadata.entry_points``.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.registry import router


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestBackendsEndpoint:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/backends")
        assert resp.status_code == 200

    def test_returns_canonical_group(self, client: TestClient) -> None:
        body = client.get("/backends").json()
        assert body["group"] == "protea.backends"

    def test_lists_all_four_backends(self, client: TestClient) -> None:
        body = client.get("/backends").json()
        names = {p["name"] for p in body["plugins"]}
        assert {"esm", "t5", "ankh", "esm3c"} <= names

    def test_plugin_info_shape(self, client: TestClient) -> None:
        body = client.get("/backends").json()
        plugin = next(p for p in body["plugins"] if p["name"] == "esm")
        assert plugin["cls"] == "EsmBackend"
        assert plugin["module"] == "protea_backends.esm:plugin"
        assert "extra" in plugin

    def test_plugins_sorted_by_name(self, client: TestClient) -> None:
        body = client.get("/backends").json()
        names = [p["name"] for p in body["plugins"]]
        assert names == sorted(names)


class TestSourcesEndpoint:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/sources")
        assert resp.status_code == 200

    def test_returns_canonical_group(self, client: TestClient) -> None:
        body = client.get("/sources").json()
        assert body["group"] == "protea.sources"

    def test_lists_all_three_sources(self, client: TestClient) -> None:
        body = client.get("/sources").json()
        names = {p["name"] for p in body["plugins"]}
        assert names >= {"goa", "quickgo", "uniprot"}

    def test_extra_carries_version(self, client: TestClient) -> None:
        body = client.get("/sources").json()
        goa = next(p for p in body["plugins"] if p["name"] == "goa")
        assert goa["extra"].get("version") == "uniprot-goa"

    def test_uniprot_class_name_is_correct(self, client: TestClient) -> None:
        body = client.get("/sources").json()
        uniprot = next(p for p in body["plugins"] if p["name"] == "uniprot")
        assert uniprot["cls"] == "UniProtSource"


class TestRunnersEndpoint:
    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/runners")
        assert resp.status_code == 200

    def test_returns_canonical_group(self, client: TestClient) -> None:
        body = client.get("/runners").json()
        assert body["group"] == "protea.runners"

    def test_lists_all_three_runners(self, client: TestClient) -> None:
        body = client.get("/runners").json()
        names = {p["name"] for p in body["plugins"]}
        assert names >= {"baseline", "knn", "lightgbm"}

    def test_lightgbm_class_name(self, client: TestClient) -> None:
        body = client.get("/runners").json()
        lgbm = next(p for p in body["plugins"] if p["name"] == "lightgbm")
        assert lgbm["cls"] == "LightgbmRunner"


class TestResponseSchema:
    def test_plugin_list_response_keys(self, client: TestClient) -> None:
        body = client.get("/backends").json()
        assert set(body.keys()) == {"group", "plugins"}

    def test_plugin_info_keys(self, client: TestClient) -> None:
        body = client.get("/backends").json()
        plugin = body["plugins"][0]
        assert set(plugin.keys()) == {"name", "cls", "module", "extra"}
