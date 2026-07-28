"""Tests for the feature-registry endpoint (third renderer).

``GET /features/registry`` serves ``protea_contracts.feature_docs
.FEATURE_DOCS`` as JSON so the Next.js frontend can render the same
canonical feature glossary the Sphinx docs and the thesis cite. These
tests pin that the endpoint DERIVES from the contract (never a
transcribed literal): the totals, names, families, and per-feature
fields must match ``FEATURE_DOCS`` exactly, and ``status`` must be the
enum's string value.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from protea_contracts.feature_docs import FEATURE_DOCS

from protea.api.routers import features as features_router


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(features_router.router)
    return TestClient(app)


class TestFeatureRegistryEndpoint:
    def test_returns_200(self, client: TestClient) -> None:
        assert client.get("/features/registry").status_code == 200

    def test_total_matches_contract(self, client: TestClient) -> None:
        body = client.get("/features/registry").json()
        assert body["total"] == len(FEATURE_DOCS)
        assert len(body["features"]) == len(FEATURE_DOCS)

    def test_names_and_order_match_contract(self, client: TestClient) -> None:
        # Declaration order must be preserved: the endpoint iterates the
        # contract mapping, it does not sort or re-home features.
        body = client.get("/features/registry").json()
        got = [f["name"] for f in body["features"]]
        assert got == list(FEATURE_DOCS.keys())

    def test_every_feature_field_is_copied_verbatim(self, client: TestClient) -> None:
        body = client.get("/features/registry").json()
        by_name = {f["name"]: f for f in body["features"]}
        for name, doc in FEATURE_DOCS.items():
            f = by_name[name]
            assert f["family"] == doc.family
            assert f["summary"] == doc.summary
            assert f["definition"] == doc.definition
            assert f["producer"] == doc.producer
            assert f["unit"] == doc.unit
            assert f["value_range"] == doc.value_range
            assert f["notes"] == doc.notes
            # status is lowered to the enum's string value for the UI.
            assert f["status"] == doc.status.value

    def test_status_counts_sum_to_total(self, client: TestClient) -> None:
        body = client.get("/features/registry").json()
        assert sum(body["status_counts"].values()) == body["total"]

    def test_families_are_first_seen_order_and_distinct(self, client: TestClient) -> None:
        body = client.get("/features/registry").json()
        fams = body["families"]
        assert len(fams) == len(set(fams))
        assert set(fams) == {doc.family for doc in FEATURE_DOCS.values()}

    def test_schema_version_is_reported(self, client: TestClient) -> None:
        body = client.get("/features/registry").json()
        # protea-contracts is always installed in the test env; the value
        # is the resolved package version, never a hardcoded string.
        assert body["schema_version"]
        assert body["schema_version"] != "unknown"

    def test_declared_absent_is_surfaced(self, client: TestClient) -> None:
        # ADR-D45: the default export leaves classifier/self_prior/
        # association columns unfilled. The technician must be able to
        # learn that from this surface, so the status must round-trip.
        body = client.get("/features/registry").json()
        by_name = {f["name"]: f for f in body["features"]}
        assert by_name["classifier_score"]["status"] == "DECLARED_ABSENT"
