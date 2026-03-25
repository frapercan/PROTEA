"""Unit tests for the /proteins router.

Database is fully mocked -- no real infrastructure required.
"""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.proteins import router

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app(session_factory):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


def _make_protein(**overrides):
    defaults = {
        "accession": "P12345",
        "entry_name": "TEST_HUMAN",
        "gene_name": "TEST",
        "organism": "Homo sapiens",
        "taxonomy_id": 9606,
        "length": 100,
        "reviewed": True,
        "is_canonical": True,
        "canonical_accession": "P12345",
        "isoform_index": None,
        "sequence_id": 1,
    }
    defaults.update(overrides)
    p = MagicMock()
    for k, v in defaults.items():
        setattr(p, k, v)
    return p


def _make_metadata():
    meta = MagicMock()
    for attr in (
        "function_cc", "ec_number", "catalytic_activity", "pathway",
        "keywords", "cofactor", "activity_regulation", "absorption",
        "kinetics", "ph_dependence", "redox_potential", "temperature_dependence",
        "active_site", "binding_site", "dna_binding", "rhea_id", "site", "features",
    ):
        setattr(meta, attr, f"mock_{attr}")
    return meta


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def factory(session):
    return MagicMock()


@pytest.fixture()
def client(session, factory):
    app = _make_app(factory)
    with patch(
        "protea.api.routers.proteins.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ):
        with TestClient(app) as c:
            yield c, session


# ---------------------------------------------------------------------------
# GET /proteins/stats
# ---------------------------------------------------------------------------

class TestProteinStats:
    def test_returns_all_stat_keys(self, client):
        c, session = client
        # Each scalar() call returns a value in order:
        # total, canonical, reviewed, with_metadata, with_embeddings, with_go
        session.query.return_value.scalar.return_value = 10
        session.query.return_value.filter.return_value.scalar.return_value = 5
        session.query.return_value.join.return_value.scalar.return_value = 3

        resp = c.get("/proteins/stats")
        assert resp.status_code == 200
        data = resp.json()
        for key in (
            "total", "canonical", "isoforms", "reviewed",
            "unreviewed", "with_metadata", "with_embeddings", "with_go_annotations",
        ):
            assert key in data

    def test_stats_zero_values(self, client):
        c, session = client
        session.query.return_value.scalar.return_value = 0
        session.query.return_value.filter.return_value.scalar.return_value = 0
        session.query.return_value.join.return_value.scalar.return_value = 0

        resp = c.get("/proteins/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["isoforms"] == 0


# ---------------------------------------------------------------------------
# GET /proteins
# ---------------------------------------------------------------------------

class TestListProteins:
    def test_returns_paginated_list(self, client):
        c, session = client
        p = _make_protein()
        q_mock = MagicMock()
        session.query.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.count.return_value = 1
        q_mock.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [p]

        resp = c.get("/proteins")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["accession"] == "P12345"

    def test_search_filter(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.count.return_value = 0
        q_mock.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

        resp = c.get("/proteins", params={"search": "kinase"})
        assert resp.status_code == 200
        assert resp.json()["total"] == 0
        assert resp.json()["items"] == []

    def test_reviewed_filter(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.count.return_value = 0
        q_mock.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

        resp = c.get("/proteins", params={"reviewed": "true"})
        assert resp.status_code == 200

    def test_canonical_only_false(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.count.return_value = 0
        q_mock.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

        resp = c.get("/proteins", params={"canonical_only": "false"})
        assert resp.status_code == 200

    def test_pagination_params(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.count.return_value = 100
        q_mock.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

        resp = c.get("/proteins", params={"limit": 10, "offset": 20})
        assert resp.status_code == 200
        data = resp.json()
        assert data["limit"] == 10
        assert data["offset"] == 20

    def test_empty_list(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.count.return_value = 0
        q_mock.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

        resp = c.get("/proteins")
        assert resp.status_code == 200
        assert resp.json()["items"] == []


# ---------------------------------------------------------------------------
# GET /proteins/{accession}
# ---------------------------------------------------------------------------

class TestGetProtein:
    def test_returns_protein_with_metadata(self, client):
        c, session = client
        p = _make_protein()
        meta = _make_metadata()
        session.get.return_value = p
        session.query.return_value.filter.return_value.first.return_value = meta
        session.query.return_value.filter.return_value.scalar.return_value = 2
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        resp = c.get("/proteins/P12345")
        assert resp.status_code == 200
        data = resp.json()
        assert data["accession"] == "P12345"
        assert data["metadata"] is not None
        assert data["metadata"]["function_cc"] == "mock_function_cc"

    def test_returns_protein_without_metadata(self, client):
        c, session = client
        p = _make_protein()
        session.get.return_value = p
        session.query.return_value.filter.return_value.first.return_value = None
        session.query.return_value.filter.return_value.scalar.return_value = 0
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []

        resp = c.get("/proteins/P12345")
        assert resp.status_code == 200
        assert resp.json()["metadata"] is None

    def test_not_found_returns_404(self, client):
        c, session = client
        session.get.return_value = None

        resp = c.get("/proteins/UNKNOWN")
        assert resp.status_code == 404

    def test_canonical_lists_isoforms(self, client):
        c, session = client
        p = _make_protein(is_canonical=True)
        meta = _make_metadata()
        session.get.return_value = p
        session.query.return_value.filter.return_value.first.return_value = meta
        session.query.return_value.filter.return_value.scalar.return_value = 0

        iso1 = MagicMock()
        iso1.accession = "P12345-2"
        iso2 = MagicMock()
        iso2.accession = "P12345-3"
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [iso1, iso2]

        resp = c.get("/proteins/P12345")
        assert resp.status_code == 200
        assert resp.json()["isoforms"] == ["P12345-2", "P12345-3"]

    def test_non_canonical_no_isoform_list(self, client):
        c, session = client
        p = _make_protein(is_canonical=False, accession="P12345-2", sequence_id=None)
        session.get.return_value = p
        session.query.return_value.filter.return_value.first.return_value = None
        session.query.return_value.filter.return_value.scalar.return_value = 0

        resp = c.get("/proteins/P12345-2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["isoforms"] == []
        assert data["embedding_count"] == 0


# ---------------------------------------------------------------------------
# GET /proteins/{accession}/annotations
# ---------------------------------------------------------------------------

class TestGetProteinAnnotations:
    def _make_annotation_row(self, go_id="GO:0003674", name="molecular_function",
                              aspect="F", qualifier="enables", evidence="IDA",
                              assigned_by="UniProt", db_ref="PMID:123",
                              ann_set_id=None, source="goa", version="2024-01"):
        ann = MagicMock()
        ann.qualifier = qualifier
        ann.evidence_code = evidence
        ann.assigned_by = assigned_by
        ann.db_reference = db_ref
        ann.annotation_set_id = ann_set_id or uuid4()

        gt = MagicMock()
        gt.go_id = go_id
        gt.name = name
        gt.aspect = aspect

        aset = MagicMock()
        aset.source = source
        aset.source_version = version

        return (ann, gt, aset)

    def test_returns_annotations(self, client):
        c, session = client
        row = self._make_annotation_row()
        q_mock = MagicMock()
        session.query.return_value.join.return_value.join.return_value.filter.return_value = q_mock
        q_mock.order_by.return_value.all.return_value = [row]

        resp = c.get("/proteins/P12345/annotations")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["go_id"] == "GO:0003674"
        assert data[0]["evidence_code"] == "IDA"

    def test_empty_annotations(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value.join.return_value.join.return_value.filter.return_value = q_mock
        q_mock.order_by.return_value.all.return_value = []

        resp = c.get("/proteins/P12345/annotations")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_filter_by_annotation_set_id(self, client):
        c, session = client
        ann_set_id = uuid4()
        row = self._make_annotation_row(ann_set_id=ann_set_id)
        q_mock = MagicMock()
        session.query.return_value.join.return_value.join.return_value.filter.return_value = q_mock
        q_mock.filter.return_value = q_mock
        q_mock.order_by.return_value.all.return_value = [row]

        resp = c.get("/proteins/P12345/annotations", params={"annotation_set_id": str(ann_set_id)})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_invalid_annotation_set_id_returns_422(self, client):
        c, session = client
        q_mock = MagicMock()
        session.query.return_value.join.return_value.join.return_value.filter.return_value = q_mock
        q_mock.filter.side_effect = ValueError("bad uuid")

        resp = c.get("/proteins/P12345/annotations", params={"annotation_set_id": "not-a-uuid"})
        assert resp.status_code == 422
