"""
Unit tests for the FastAPI embeddings router.
Database and pika are fully mocked — no real infrastructure required.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.embeddings import router

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_AMQP = "amqp://guest:guest@localhost/"


def _make_config(config_id=None):
    c = MagicMock()
    c.id = config_id or uuid4()
    c.model_name = "facebook/esm2_t33_650M_UR50D"
    c.model_backend = "esm"
    c.layer_indices = [0]
    c.layer_agg = "mean"
    c.pooling = "mean"
    c.normalize_residues = False
    c.normalize = True
    c.max_length = 1022
    c.use_chunking = False
    c.chunk_size = 512
    c.chunk_overlap = 0
    c.description = None
    c.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return c


def _make_app(session_factory, amqp_url=FAKE_AMQP):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.amqp_url = amqp_url
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def client(session):
    factory = MagicMock()
    app = _make_app(factory)
    with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
        yield TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# POST /embeddings/configs — validation
# ---------------------------------------------------------------------------

_VALID_CONFIG_BODY = {
    "model_name": "facebook/esm2_t33_650M_UR50D",
    "model_backend": "esm",
    "layer_indices": [0],
    "layer_agg": "mean",
    "pooling": "mean",
    "normalize": True,
    "max_length": 1022,
}


class TestCreateEmbeddingConfigValidation:
    def test_valid_body_returns_201_ish(self, client, session):
        cfg = _make_config()
        session.get.return_value = cfg
        # Make flush() set cfg.id so the response can be built
        session.flush = MagicMock()
        # The router does session.add(config); session.flush(); _config_to_dict(config)
        # We patch session.add to capture the added object, then mock its attributes
        added_objects: list = []

        def _fake_add(obj):
            # Copy needed attributes from the validated body into the mock
            obj.id = uuid4()
            obj.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
            added_objects.append(obj)

        session.add.side_effect = _fake_add

        resp = client.post("/embeddings/configs", json=_VALID_CONFIG_BODY)
        assert resp.status_code == 200  # FastAPI default for non-201 POST

    def test_missing_model_name_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY}
        del body["model_name"]
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_invalid_backend_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "model_backend": "llama"}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_empty_layer_indices_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "layer_indices": []}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_invalid_layer_agg_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "layer_agg": "sum"}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_invalid_pooling_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "pooling": "attention"}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_negative_max_length_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "max_length": 0}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_chunk_overlap_equal_chunk_size_returns_422(self, client, session):
        """chunk_overlap >= chunk_size must be rejected (would produce O(L) chunks)."""
        body = {**_VALID_CONFIG_BODY, "use_chunking": True, "chunk_size": 10, "chunk_overlap": 10}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("chunk_overlap" in str(e) for e in detail)

    def test_chunk_overlap_greater_than_chunk_size_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "use_chunking": True, "chunk_size": 4, "chunk_overlap": 6}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422

    def test_valid_chunking_config_is_accepted(self, client, session):
        """chunk_overlap < chunk_size must be accepted."""
        body = {**_VALID_CONFIG_BODY, "use_chunking": True, "chunk_size": 512, "chunk_overlap": 64}
        # Just check it passes validation (not 422)
        cfg = _make_config()
        added: list = []

        def _fake_add(obj):
            obj.id = uuid4()
            obj.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
            added.append(obj)

        session.add.side_effect = _fake_add
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code != 422


# ---------------------------------------------------------------------------
# GET /embeddings/configs
# ---------------------------------------------------------------------------

class TestListEmbeddingConfigs:
    def test_returns_list(self, client, session):
        session.query.return_value.order_by.return_value.all.return_value = [_make_config()]
        resp = client.get("/embeddings/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert "model_name" in data[0]
        assert "normalize_residues" in data[0]
        assert "use_chunking" in data[0]

    def test_empty_list(self, client, session):
        session.query.return_value.order_by.return_value.all.return_value = []
        resp = client.get("/embeddings/configs")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# DELETE /embeddings/configs/{id}
# ---------------------------------------------------------------------------

class TestDeleteEmbeddingConfig:
    def test_delete_existing_returns_200(self, client, session):
        cfg = _make_config()
        session.get.return_value = cfg
        resp = client.delete(f"/embeddings/configs/{cfg.id}")
        assert resp.status_code == 200
        assert resp.json()["deleted"] == str(cfg.id)

    def test_delete_nonexistent_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.delete(f"/embeddings/configs/{uuid4()}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets/{set_id}/predictions.tsv
# ---------------------------------------------------------------------------

def _make_prediction_set(ps_id=None):
    ps = MagicMock()
    ps.id = ps_id or uuid4()
    ps.embedding_config_id = uuid4()
    ps.annotation_set_id = uuid4()
    ps.ontology_snapshot_id = uuid4()
    ps.query_set_id = None
    ps.limit_per_entry = 5
    ps.distance_threshold = None
    ps.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return ps


def _make_go_prediction(accession="P12345", distance=0.1):
    pred = MagicMock()
    pred.protein_accession = accession
    pred.distance = distance
    pred.ref_protein_accession = "QREF01"
    pred.qualifier = "enables"
    pred.evidence_code = "IDA"
    # alignment — not computed
    for col in ("identity_nw", "similarity_nw", "alignment_score_nw",
                "gaps_pct_nw", "alignment_length_nw",
                "identity_sw", "similarity_sw", "alignment_score_sw",
                "gaps_pct_sw", "alignment_length_sw",
                "length_query", "length_ref",
                "query_taxonomy_id", "ref_taxonomy_id",
                "taxonomic_lca", "taxonomic_distance",
                "taxonomic_common_ancestors"):
        setattr(pred, col, None)
    pred.taxonomic_relation = None
    return pred


def _make_go_term(go_id="GO:0003824", name="catalytic activity", aspect="F"):
    gt = MagicMock()
    gt.go_id = go_id
    gt.name = name
    gt.aspect = aspect
    return gt


class TestDownloadPredictionsTSV:
    def _get(self, client, session, set_id, rows, **params):
        ps = _make_prediction_set(set_id)
        session.get.return_value = ps

        # yield_per returns the rows iterable
        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value = q
        q.yield_per.return_value = iter(rows)
        session.query.return_value.join.return_value.filter.return_value = q

        return client.get(f"/embeddings/prediction-sets/{set_id}/predictions.tsv", params=params)

    def test_returns_200_with_tsv_content_type(self, client, session):
        set_id = uuid4()
        pred = _make_go_prediction()
        gt = _make_go_term()
        resp = self._get(client, session, set_id, [(pred, gt)])

        assert resp.status_code == 200
        assert "tab-separated" in resp.headers["content-type"]

    def test_content_disposition_has_filename(self, client, session):
        set_id = uuid4()
        resp = self._get(client, session, set_id, [])

        disposition = resp.headers.get("content-disposition", "")
        assert "attachment" in disposition
        assert str(set_id) in disposition

    def test_header_row_present(self, client, session):
        set_id = uuid4()
        resp = self._get(client, session, set_id, [])

        lines = resp.text.splitlines()
        assert lines[0].startswith("protein_accession\t")
        assert "go_id" in lines[0]
        assert "distance" in lines[0]

    def test_data_row_values(self, client, session):
        set_id = uuid4()
        pred = _make_go_prediction("P12345", distance=0.2345)
        gt = _make_go_term("GO:0003824", "catalytic activity", "F")
        resp = self._get(client, session, set_id, [(pred, gt)])

        lines = resp.text.splitlines()
        assert len(lines) == 2  # header + 1 data row
        row = lines[1].split("\t")
        assert row[0] == "P12345"
        assert row[1] == "GO:0003824"
        assert row[2] == "catalytic activity"
        assert row[3] == "F"
        assert float(row[4]) == pytest.approx(0.2345, abs=1e-4)
        assert row[5] == "QREF01"

    def test_empty_predictions_returns_header_only(self, client, session):
        set_id = uuid4()
        resp = self._get(client, session, set_id, [])

        lines = resp.text.splitlines()
        assert len(lines) == 1

    def test_null_alignment_fields_are_empty_string(self, client, session):
        set_id = uuid4()
        pred = _make_go_prediction()
        gt = _make_go_term()
        resp = self._get(client, session, set_id, [(pred, gt)])

        row = resp.text.splitlines()[1].split("\t")
        header = resp.text.splitlines()[0].split("\t")
        identity_nw_idx = header.index("identity_nw")
        assert row[identity_nw_idx] == ""

    def test_prediction_set_not_found_returns_404(self, client, session):
        # Both the preflight check and the generator use session.get → None
        session.get.return_value = None
        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = client.get(f"/embeddings/prediction-sets/{uuid4()}/predictions.tsv")
        assert resp.status_code == 404

    def test_multiple_rows_all_included(self, client, session):
        set_id = uuid4()
        rows = [(
            _make_go_prediction(f"PROT{i}", distance=i * 0.1),
            _make_go_term(f"GO:{i:07d}", f"term {i}", "P"),
        ) for i in range(5)]
        resp = self._get(client, session, set_id, rows)

        lines = resp.text.splitlines()
        assert len(lines) == 6  # 1 header + 5 data
