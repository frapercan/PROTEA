"""
Unit tests for the FastAPI embeddings router.
Database and pika are fully mocked — no real infrastructure required.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
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
    c.created_at = datetime(2024, 1, 1, tzinfo=UTC)
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
            obj.created_at = datetime(2024, 1, 1, tzinfo=UTC)
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
        _make_config()
        added: list = []

        def _fake_add(obj):
            obj.id = uuid4()
            obj.created_at = datetime(2024, 1, 1, tzinfo=UTC)
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
    ps.created_at = datetime(2024, 1, 1, tzinfo=UTC)
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
    # re-ranker features
    pred.vote_count = None
    pred.k_position = None
    pred.go_term_frequency = None
    pred.ref_annotation_density = None
    pred.neighbor_distance_std = None
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

    def test_filter_by_accession(self, client, session):
        """The accession query param should filter predictions."""
        set_id = uuid4()
        pred = _make_go_prediction("P99999")
        gt = _make_go_term()
        resp = self._get(client, session, set_id, [(pred, gt)], accession="P99999")
        assert resp.status_code == 200
        lines = resp.text.splitlines()
        assert len(lines) == 2
        assert "P99999" in lines[1]

    def test_filter_by_aspect(self, client, session):
        """The aspect query param should filter predictions."""
        set_id = uuid4()
        pred = _make_go_prediction()
        gt = _make_go_term(aspect="P")
        resp = self._get(client, session, set_id, [(pred, gt)], aspect="P")
        assert resp.status_code == 200

    def test_filter_by_max_distance(self, client, session):
        """The max_distance query param should filter predictions."""
        set_id = uuid4()
        pred = _make_go_prediction(distance=0.05)
        gt = _make_go_term()
        resp = self._get(client, session, set_id, [(pred, gt)], max_distance=0.5)
        assert resp.status_code == 200

    def test_alignment_fields_formatted(self, client, session):
        """Non-null alignment fields should be formatted with _fmt."""
        set_id = uuid4()
        pred = _make_go_prediction()
        pred.identity_nw = 0.95123456
        pred.similarity_nw = 0.88
        gt = _make_go_term()
        resp = self._get(client, session, set_id, [(pred, gt)])
        lines = resp.text.splitlines()
        row = lines[1].split("\t")
        header = lines[0].split("\t")
        identity_nw_idx = header.index("identity_nw")
        assert row[identity_nw_idx] == "0.951235"


# ---------------------------------------------------------------------------
# _fmt helper
# ---------------------------------------------------------------------------

class TestFmt:
    def test_none_returns_empty(self):
        from protea.api.routers.embeddings import _fmt
        assert _fmt(None) == ""

    def test_float_returns_formatted(self):
        from protea.api.routers.embeddings import _fmt
        assert _fmt(0.123456789) == "0.123457"

    def test_zero_returns_formatted(self):
        from protea.api.routers.embeddings import _fmt
        assert _fmt(0.0) == "0"


# ---------------------------------------------------------------------------
# get_session_factory / get_amqp_url — RuntimeError when not set
# ---------------------------------------------------------------------------

class TestDependencyGuards:
    def test_session_factory_missing_raises(self):
        from protea.api.routers.embeddings import get_session_factory
        req = MagicMock()
        req.app.state = MagicMock(spec=[])  # no session_factory attr
        with pytest.raises(RuntimeError, match="session_factory"):
            get_session_factory(req)

    def test_amqp_url_missing_raises(self):
        from protea.api.routers.embeddings import get_amqp_url
        req = MagicMock()
        req.app.state = MagicMock(spec=[])  # no amqp_url attr
        with pytest.raises(RuntimeError, match="amqp_url"):
            get_amqp_url(req)


# ---------------------------------------------------------------------------
# Additional validation edge cases
# ---------------------------------------------------------------------------

class TestValidationEdgeCases:
    def test_normalize_residues_non_bool_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "normalize_residues": "yes"}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        assert any("normalize_residues" in str(e) for e in resp.json()["detail"])

    def test_normalize_non_bool_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "normalize": "yes"}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        assert any("normalize" in str(e) for e in resp.json()["detail"])

    def test_use_chunking_non_bool_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "use_chunking": "yes"}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        assert any("use_chunking" in str(e) for e in resp.json()["detail"])

    def test_chunk_size_non_positive_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "chunk_size": -1}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        assert any("chunk_size" in str(e) for e in resp.json()["detail"])

    def test_chunk_overlap_negative_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "chunk_overlap": -1}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        assert any("chunk_overlap" in str(e) for e in resp.json()["detail"])

    def test_description_non_string_returns_422(self, client, session):
        body = {**_VALID_CONFIG_BODY, "description": 42}
        resp = client.post("/embeddings/configs", json=body)
        assert resp.status_code == 422
        assert any("description" in str(e) for e in resp.json()["detail"])


# ---------------------------------------------------------------------------
# GET /embeddings/configs/{config_id}
# ---------------------------------------------------------------------------

class TestGetEmbeddingConfig:
    def test_returns_config(self, client, session):
        cfg = _make_config()
        config_id = cfg.id
        session.get.return_value = cfg
        # Mock the embedding count query
        session.query.return_value.filter.return_value.scalar.return_value = 42

        resp = client.get(f"/embeddings/configs/{config_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(config_id)
        assert data["model_name"] == "facebook/esm2_t33_650M_UR50D"
        assert data["embedding_count"] == 42

    def test_not_found_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/embeddings/configs/{uuid4()}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /embeddings/configs/{config_id} — with prediction sets
# ---------------------------------------------------------------------------

class TestDeleteEmbeddingConfigCascade:
    def test_delete_with_prediction_sets(self, client, session):
        cfg = _make_config()
        config_id = cfg.id
        session.get.return_value = cfg

        pred_set_id = uuid4()
        # query(PredictionSet.id).filter(...).all() returns [(pred_set_id,)]
        session.query.return_value.filter.return_value.all.return_value = [(pred_set_id,)]
        # Bulk deletes return counts
        session.query.return_value.filter.return_value.delete.return_value = 10

        resp = client.delete(f"/embeddings/configs/{config_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == str(config_id)


# ---------------------------------------------------------------------------
# POST /embeddings/predict
# ---------------------------------------------------------------------------

class TestPredictGoTerms:
    def _make_predict_app(self, session):
        factory = MagicMock()
        app = _make_app(factory)
        return app

    def test_predict_success(self, session):
        app = self._make_predict_app(session)

        config_id = uuid4()
        ann_id = uuid4()
        onto_id = uuid4()

        # session.get returns objects for all three lookups
        session.get.return_value = MagicMock()
        # session.add captures Job and JobEvent
        job_mock = MagicMock()
        job_mock.id = 42
        added = []

        def _fake_add(obj):
            added.append(obj)
            # If it's a Job, set its id
            if hasattr(obj, 'operation'):
                obj.id = 42

        session.add.side_effect = _fake_add
        session.flush = MagicMock()

        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            with patch("protea.api.routers.embeddings.publish_job") as mock_pub:
                client = TestClient(app, raise_server_exceptions=True)
                resp = client.post("/embeddings/predict", json={
                    "embedding_config_id": str(config_id),
                    "annotation_set_id": str(ann_id),
                    "ontology_snapshot_id": str(onto_id),
                })

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "queued"
        mock_pub.assert_called_once()

    def test_predict_invalid_uuid_returns_422(self, session):
        app = self._make_predict_app(session)
        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post("/embeddings/predict", json={
                "embedding_config_id": "not-a-uuid",
                "annotation_set_id": str(uuid4()),
                "ontology_snapshot_id": str(uuid4()),
            })
        assert resp.status_code == 422

    def test_predict_config_not_found_returns_404(self, session):
        app = self._make_predict_app(session)
        # session.get returns None for EmbeddingConfig
        session.get.return_value = None
        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post("/embeddings/predict", json={
                "embedding_config_id": str(uuid4()),
                "annotation_set_id": str(uuid4()),
                "ontology_snapshot_id": str(uuid4()),
            })
        assert resp.status_code == 404

    def test_predict_annotation_set_not_found_returns_404(self, session):
        app = self._make_predict_app(session)

        def _get_side(model_cls, id_val):
            from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
            if model_cls is EmbeddingConfig:
                return MagicMock()
            return None

        session.get.side_effect = _get_side
        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post("/embeddings/predict", json={
                "embedding_config_id": str(uuid4()),
                "annotation_set_id": str(uuid4()),
                "ontology_snapshot_id": str(uuid4()),
            })
        assert resp.status_code == 404

    def test_predict_ontology_not_found_returns_404(self, session):
        app = self._make_predict_app(session)

        call_count = [0]
        def _get_side(model_cls, id_val):
            call_count[0] += 1
            from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
                OntologySnapshot,
            )
            if model_cls is OntologySnapshot:
                return None
            return MagicMock()

        session.get.side_effect = _get_side
        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            client = TestClient(app, raise_server_exceptions=True)
            resp = client.post("/embeddings/predict", json={
                "embedding_config_id": str(uuid4()),
                "annotation_set_id": str(uuid4()),
                "ontology_snapshot_id": str(uuid4()),
            })
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets
# ---------------------------------------------------------------------------

class TestListPredictionSets:
    @staticmethod
    def _wire_list_query(session, rows):
        """Wire the mock chain for the correlated-subquery list query."""
        # query(PredictionSet, EmbeddingConfig, AnnotationSet, OntologySnapshot, count_subq)
        #   .join(...).join(...).join(...).order_by(...).limit(...).all()
        # The count subquery is built via session.query().filter().correlate().scalar_subquery()
        # but all that matters for the mock is the final .all() result.
        session.query.return_value.join.return_value.join.return_value.join.return_value \
            .order_by.return_value.limit.return_value.all.return_value = rows

    def test_returns_list(self, client, session):
        ps = _make_prediction_set()
        ec = _make_config()
        ann = MagicMock()
        ann.source = "goa"
        ann.source_version = "2024-01"
        snap = MagicMock()
        snap.obo_version = "2024-01-01"

        self._wire_list_query(session, [(ps, ec, ann, snap, 100)])

        resp = client.get("/embeddings/prediction-sets")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == str(ps.id)
        assert data[0]["embedding_config_name"] == ec.model_name
        assert data[0]["annotation_set_label"] == "goa 2024-01"
        assert data[0]["ontology_snapshot_version"] == "2024-01-01"
        assert data[0]["prediction_count"] == 100

    def test_annotation_set_without_version(self, client, session):
        ps = _make_prediction_set()
        ec = _make_config()
        ann = MagicMock()
        ann.source = "goa"
        ann.source_version = None
        snap = MagicMock()
        snap.obo_version = "2024-01-01"

        self._wire_list_query(session, [(ps, ec, ann, snap, 0)])

        resp = client.get("/embeddings/prediction-sets")
        assert resp.status_code == 200
        assert resp.json()[0]["annotation_set_label"] == "goa"

    def test_empty_list(self, client, session):
        self._wire_list_query(session, [])
        resp = client.get("/embeddings/prediction-sets")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets/{set_id}
# ---------------------------------------------------------------------------

class TestGetPredictionSet:
    def test_returns_details(self, client, session):
        ps = _make_prediction_set()
        ps_id = ps.id
        session.get.return_value = ps
        session.query.return_value.filter.return_value.scalar.return_value = 50
        session.query.return_value.filter.return_value.group_by.return_value.all.return_value = [
            ("P12345", 30), ("Q67890", 20),
        ]

        resp = client.get(f"/embeddings/prediction-sets/{ps_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(ps_id)
        assert data["prediction_count"] == 50
        assert data["per_protein_counts"]["P12345"] == 30
        assert data["per_protein_counts"]["Q67890"] == 20

    def test_not_found_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/embeddings/prediction-sets/{uuid4()}")
        assert resp.status_code == 404

    def test_with_query_set_id(self, client, session):
        ps = _make_prediction_set()
        ps.query_set_id = uuid4()
        session.get.return_value = ps
        session.query.return_value.filter.return_value.scalar.return_value = 0
        session.query.return_value.filter.return_value.group_by.return_value.all.return_value = []

        resp = client.get(f"/embeddings/prediction-sets/{ps.id}")
        assert resp.status_code == 200
        assert resp.json()["query_set_id"] == str(ps.query_set_id)


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets/{set_id}/proteins
# ---------------------------------------------------------------------------

class TestListPredictionSetProteins:
    def _setup_proteins_mocks(self, session, ps, rows_data):
        """Set up the complex mock chain for the proteins endpoint."""
        # We need to carefully control the mock chain.
        # The endpoint does multiple session.query(...) calls with different args.
        # Use a side_effect on session.query to return different mocks per call.
        call_idx = [0]
        main_q = MagicMock()
        main_q.filter.return_value = main_q
        main_q.group_by.return_value = main_q
        main_q.count.return_value = len(rows_data)
        main_q.order_by.return_value = main_q
        main_q.offset.return_value = main_q
        main_q.limit.return_value = main_q
        main_q.all.return_value = rows_data

        prot_q = MagicMock()
        prot_mock = MagicMock()
        prot_mock.accession = rows_data[0][0] if rows_data else "X"
        prot_q.filter.return_value = prot_q
        prot_q.all.return_value = [prot_mock] if rows_data else []

        ann_q = MagicMock()
        ann_q.filter.return_value = ann_q
        ann_q.group_by.return_value = ann_q
        ann_q.all.return_value = [(rows_data[0][0], 5)] if rows_data else []

        match_q = MagicMock()
        match_q.join.return_value = match_q
        match_q.filter.return_value = match_q
        match_q.group_by.return_value = match_q
        match_q.all.return_value = [(rows_data[0][0], 3)] if rows_data else []

        queries = [main_q, prot_q, ann_q, match_q]

        def _query_side(*args, **kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            if idx < len(queries):
                return queries[idx]
            return MagicMock()

        session.query.side_effect = _query_side

    def test_returns_paginated_proteins(self, client, session):
        ps = _make_prediction_set()
        ps_id = ps.id
        session.get.return_value = ps
        self._setup_proteins_mocks(session, ps, [("P12345", 10, 0.05)])

        resp = client.get(f"/embeddings/prediction-sets/{ps_id}/proteins")
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "items" in data
        assert len(data["items"]) == 1
        item = data["items"][0]
        assert item["accession"] == "P12345"
        assert item["go_count"] == 10
        assert item["in_db"] is True

    def test_not_found_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/embeddings/prediction-sets/{uuid4()}/proteins")
        assert resp.status_code == 404

    def test_search_filter(self, client, session):
        ps = _make_prediction_set()
        session.get.return_value = ps

        call_idx = [0]
        main_q = MagicMock()
        main_q.filter.return_value = main_q
        main_q.group_by.return_value = main_q
        main_q.count.return_value = 0
        main_q.order_by.return_value = main_q
        main_q.offset.return_value = main_q
        main_q.limit.return_value = main_q
        main_q.all.return_value = []

        prot_q = MagicMock()
        prot_q.filter.return_value = prot_q
        prot_q.all.return_value = []

        queries = [main_q, prot_q]

        def _query_side(*args, **kwargs):
            idx = call_idx[0]
            call_idx[0] += 1
            if idx < len(queries):
                return queries[idx]
            return MagicMock()

        session.query.side_effect = _query_side

        resp = client.get(
            f"/embeddings/prediction-sets/{ps.id}/proteins",
            params={"search": "P123"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets/{set_id}/proteins/{accession}
# ---------------------------------------------------------------------------

class TestGetProteinPredictions:
    def test_returns_predictions(self, client, session):
        ps = _make_prediction_set()
        ps_id = ps.id
        session.get.return_value = ps

        pred = _make_go_prediction("P12345", distance=0.1)
        gt = _make_go_term("GO:0003824", "catalytic activity", "F")

        session.query.return_value.join.return_value.filter.return_value \
            .order_by.return_value.all.return_value = [(pred, gt)]

        resp = client.get(f"/embeddings/prediction-sets/{ps_id}/proteins/P12345")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["go_id"] == "GO:0003824"
        assert data[0]["name"] == "catalytic activity"
        assert data[0]["aspect"] == "F"
        assert data[0]["distance"] == pytest.approx(0.1, abs=1e-4)
        assert data[0]["ref_protein_accession"] == "QREF01"
        # Alignment fields should be None
        assert data[0]["identity_nw"] is None
        assert data[0]["taxonomic_relation"] is None

    def test_not_found_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/embeddings/prediction-sets/{uuid4()}/proteins/P12345")
        assert resp.status_code == 404

    def test_empty_predictions_returns_empty_list(self, client, session):
        ps = _make_prediction_set()
        session.get.return_value = ps
        session.query.return_value.join.return_value.filter.return_value \
            .order_by.return_value.all.return_value = []
        resp = client.get(f"/embeddings/prediction-sets/{ps.id}/proteins/UNKNOWN")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets/{set_id}/go-terms
# ---------------------------------------------------------------------------

class TestGoTermDistribution:
    def test_returns_distribution(self, client, session):
        ps = _make_prediction_set()
        ps_id = ps.id
        session.get.return_value = ps

        # Top terms query
        session.query.return_value.join.return_value.filter.return_value \
            .group_by.return_value.order_by.return_value.limit.return_value \
            .all.return_value = [
                ("GO:0003824", "catalytic activity", "F", 50),
                ("GO:0005515", "protein binding", "F", 30),
                ("GO:0008150", "biological_process", "P", 20),
            ]

        # Aspect counts query
        session.query.return_value.join.return_value.filter.return_value \
            .group_by.return_value.all.return_value = [
                ("F", 80), ("P", 20),
            ]

        resp = client.get(f"/embeddings/prediction-sets/{ps_id}/go-terms")
        assert resp.status_code == 200
        data = resp.json()
        assert "by_aspect" in data
        assert "aspect_totals" in data
        assert "top_terms" in data

    def test_not_found_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.get(f"/embeddings/prediction-sets/{uuid4()}/go-terms")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /embeddings/prediction-sets/{set_id}/predictions-cafa.tsv
# ---------------------------------------------------------------------------

class TestDownloadPredictionsCafa:
    def _get_cafa(self, client, session, set_id, rows, **params):
        """Wire mocks for the CAFA download endpoint.

        ``rows`` should be a list of (protein_accession, go_id, distance) tuples,
        matching the subquery-based query output.
        """
        ps = _make_prediction_set(set_id)
        session.get.return_value = ps

        q = MagicMock()
        q.join.return_value = q
        q.filter.return_value = q
        q.group_by.return_value = q
        q.subquery.return_value = q
        q.c = q  # subquery column access
        q.order_by.return_value = q
        q.yield_per.return_value = iter(rows)
        session.query.return_value = q

        return client.get(
            f"/embeddings/prediction-sets/{set_id}/predictions-cafa.tsv",
            params=params,
        )

    def test_returns_cafa_format(self, client, session):
        set_id = uuid4()
        # New format: (accession, go_id, distance) tuples
        resp = self._get_cafa(client, session, set_id, [("P12345", "GO:0003824", 0.3)])
        assert resp.status_code == 200
        assert "tab-separated" in resp.headers["content-type"]
        lines = resp.text.splitlines()
        assert len(lines) == 1
        parts = lines[0].split("\t")
        assert parts[0] == "P12345"
        assert parts[1] == "GO:0003824"
        # score = max(0, 1 - 0.3) = 0.7
        assert float(parts[2]) == pytest.approx(0.7, abs=1e-3)

    def test_cafa_deduplicates_go_terms(self, client, session):
        """Deduplication now happens at DB level via GROUP BY + MIN(distance).
        The query returns already-unique rows, so a single row is expected."""
        set_id = uuid4()
        # DB-level dedup means only the best (min distance) row is returned
        resp = self._get_cafa(client, session, set_id, [("P12345", "GO:0003824", 0.2)])
        assert resp.status_code == 200
        lines = resp.text.splitlines()
        assert len(lines) == 1

    def test_cafa_not_found_returns_404(self, client, session):
        session.get.return_value = None
        with patch("protea.api.routers.embeddings.session_scope", side_effect=lambda _: _mock_scope(session)):
            resp = client.get(f"/embeddings/prediction-sets/{uuid4()}/predictions-cafa.tsv")
        assert resp.status_code == 404

    def test_cafa_content_disposition(self, client, session):
        set_id = uuid4()
        resp = self._get_cafa(client, session, set_id, [])
        disposition = resp.headers.get("content-disposition", "")
        assert "attachment" in disposition
        assert "cafa" in disposition

    def test_cafa_filter_by_aspect(self, client, session):
        set_id = uuid4()
        resp = self._get_cafa(client, session, set_id, [("P12345", "GO:0003824", 0.1)], aspect="F")
        assert resp.status_code == 200

    def test_cafa_filter_by_max_distance(self, client, session):
        set_id = uuid4()
        resp = self._get_cafa(client, session, set_id, [("P12345", "GO:0003824", 0.05)], max_distance=0.5)
        assert resp.status_code == 200

    def test_cafa_score_clamps_at_zero(self, client, session):
        """When distance > 1.0 the score should be 0.0, not negative."""
        set_id = uuid4()
        resp = self._get_cafa(client, session, set_id, [("P12345", "GO:0003824", 2.5)])
        lines = resp.text.splitlines()
        assert len(lines) == 1
        score = float(lines[0].split("\t")[2])
        assert score == 0.0


# ---------------------------------------------------------------------------
# DELETE /embeddings/prediction-sets/{set_id}
# ---------------------------------------------------------------------------

class TestDeletePredictionSet:
    def test_delete_existing_returns_200(self, client, session):
        ps = _make_prediction_set()
        ps_id = ps.id
        session.get.return_value = ps
        session.query.return_value.filter.return_value.delete.return_value = 25

        resp = client.delete(f"/embeddings/prediction-sets/{ps_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] == str(ps_id)
        assert data["predictions_deleted"] == 25
        session.delete.assert_called_once_with(ps)

    def test_delete_nonexistent_returns_404(self, client, session):
        session.get.return_value = None
        resp = client.delete(f"/embeddings/prediction-sets/{uuid4()}")
        assert resp.status_code == 404
