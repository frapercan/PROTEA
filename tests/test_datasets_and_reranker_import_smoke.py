"""Smoke tests for the PROTEA ↔ protea-reranker-lab decoupling surface.

Exercises the three HTTP boundaries that the lab drives against PROTEA:

* ``POST /datasets`` — enqueues an ``export_research_dataset`` job. Does
  not run the operation; just confirms validation, duplicate rejection,
  and AMQP publish.
* ``GET /datasets`` / ``GET /datasets/{id_or_name}`` — read path used by
  the lab's ``pull_dataset.py``.
* ``POST /reranker-models/import-by-reference`` — lab registers a
  pre-uploaded booster; PROTEA persists a ``RerankerModel`` row linked
  to its ``Dataset``.
* ``POST /reranker-models/import`` — multipart variant; PROTEA uploads
  the booster through its artifact store.

No live stack is required: FastAPI ``TestClient`` drives a minimal in-
process app wired to a MagicMock session and a fake artifact store.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.datasets import router as datasets_router
from protea.api.routers.reranker_models import router as reranker_models_router

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


@contextmanager
def _scope(session):
    yield session


def _make_dataset_row(
    *,
    name="bench-v1-K5",
    backend="local",
    train_uri="file:///tmp/bench-v1-K5/train.parquet",
    eval_uri="file:///tmp/bench-v1-K5/eval.parquet",
    manifest_uri="file:///tmp/bench-v1-K5/manifest.json",
):
    ds = MagicMock()
    ds.id = uuid.uuid4()
    ds.name = name
    ds.operation = "export_research_dataset"
    ds.job_id = None
    ds.storage_backend = backend
    ds.key_prefix = f"datasets/{name}/"
    ds.train_uri = train_uri
    ds.eval_uri = eval_uri
    ds.manifest_uri = manifest_uri
    ds.schema_sha = "abc123def456"
    ds.manifest_sha = "f" * 64
    ds.n_train_rows = 12345
    ds.n_eval_rows = 678
    ds.k = 5
    ds.annotation_source = "goa"
    ds.embedding_config_id = uuid.uuid4()
    ds.ontology_snapshot_id = uuid.uuid4()
    ds.train_snapshot_pairs = ["v160-v165", "v165-v170"]
    ds.eval_snapshot_pair = "v220-v230"
    ds.producer_version = "0.1.0"
    ds.producer_git_sha = "deadbeef"
    ds.meta = {}
    ds.created_at = datetime(2026, 4, 20, tzinfo=UTC)
    return ds


# ---------------------------------------------------------------------------
# /datasets — enqueue, list, lookup
# ---------------------------------------------------------------------------


@pytest.fixture()
def datasets_client():
    session = MagicMock()

    # Simulate SQLAlchemy's primary-key default: when the router adds a Job
    # instance, stamp a real UUID on it so ``job.id`` post-``flush`` returns
    # something that actually round-trips through ``UUID(...)``.
    def _stamp(obj):
        if getattr(obj, "id", None) is None or isinstance(obj.id, MagicMock):
            obj.id = uuid.uuid4()

    session.add.side_effect = _stamp

    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.state.amqp_url = "amqp://stub"
    app.include_router(datasets_router)

    with patch(
        "protea.api.routers.datasets.session_scope",
        side_effect=lambda _: _scope(session),
    ), patch(
        "protea.api.routers.datasets.publish_job"
    ) as publish:
        yield TestClient(app, raise_server_exceptions=True), session, publish


class TestDatasetsRouter:
    def _valid_body(self, **overrides):
        body = {
            "output_name": "bench-v1-K5",
            "embedding_config_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "train_versions": [160, 165, 170],
            "test_versions": [230],
            "k": 5,
        }
        body.update(overrides)
        return body

    def test_post_enqueues_training_job(self, datasets_client):
        client, session, publish = datasets_client
        # No duplicate — initial conflict check returns None
        session.query.return_value.filter.return_value.first.return_value = None

        resp = client.post("/datasets", json=self._valid_body())
        assert resp.status_code == 200, resp.text

        body = resp.json()
        assert body["queue"] == "protea.training"
        assert body["status"] == "queued"
        uuid.UUID(body["job_id"])  # valid UUID

        # Job + JobEvent persisted, publish_job invoked with the right queue
        assert session.add.call_count >= 2
        assert publish.call_count == 1
        publish_args = publish.call_args
        assert publish_args[0][0] == "amqp://stub"
        assert publish_args[0][1] == "protea.training"

    def test_post_rejects_duplicate_name(self, datasets_client):
        client, session, publish = datasets_client
        # Name already exists
        session.query.return_value.filter.return_value.first.return_value = (uuid.uuid4(),)

        resp = client.post("/datasets", json=self._valid_body())
        assert resp.status_code == 409
        assert "already exists" in resp.json()["detail"]
        publish.assert_not_called()

    def test_post_rejects_too_few_train_versions(self, datasets_client):
        client, _, publish = datasets_client
        resp = client.post(
            "/datasets", json=self._valid_body(train_versions=[160])
        )
        assert resp.status_code == 422
        publish.assert_not_called()

    def test_get_list_returns_dataset_dicts(self, datasets_client):
        client, session, _ = datasets_client
        rows = [_make_dataset_row(name=n) for n in ("bench-A", "bench-B")]
        q = session.query.return_value
        q.order_by.return_value.limit.return_value.all.return_value = rows

        resp = client.get("/datasets")
        assert resp.status_code == 200
        payload = resp.json()
        assert [d["name"] for d in payload] == ["bench-A", "bench-B"]
        assert payload[0]["storage_backend"] == "local"
        assert payload[0]["k"] == 5

    def test_get_by_name_resolves_non_uuid(self, datasets_client):
        client, session, _ = datasets_client
        row = _make_dataset_row(name="bench-by-name")
        # First lookup (UUID path) raises ValueError on ``UUID("bench-by-name")``,
        # then name path returns the row.
        session.query.return_value.filter.return_value.first.return_value = row

        resp = client.get("/datasets/bench-by-name")
        assert resp.status_code == 200
        assert resp.json()["name"] == "bench-by-name"

    def test_get_by_name_404(self, datasets_client):
        client, session, _ = datasets_client
        session.get.return_value = None
        session.query.return_value.filter.return_value.first.return_value = None

        resp = client.get("/datasets/missing")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /reranker-models — import paths
# ---------------------------------------------------------------------------


@pytest.fixture()
def reranker_client(tmp_path):
    session = MagicMock()
    # No duplicate name
    session.query.return_value.filter.return_value.first.return_value = None

    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.include_router(reranker_models_router)

    fake_settings = MagicMock()
    fake_settings.storage_backend = "local"

    fake_store = MagicMock()
    fake_store.put.side_effect = lambda key, _bytes: f"file:///fake/{key}"

    with patch(
        "protea.api.routers.reranker_models.session_scope",
        side_effect=lambda _: _scope(session),
    ), patch(
        "protea.api.routers.reranker_models.load_settings",
        return_value=fake_settings,
    ), patch(
        "protea.api.routers.reranker_models.get_artifact_store",
        return_value=fake_store,
    ):
        yield TestClient(app, raise_server_exceptions=True), session, fake_store


class TestRerankerModelsImport:
    def _spec_yaml(self, cell="pk-bpo"):
        return f"training:\n  cell: {cell}\n"

    def _run_json(self, run_id="run-abc", dataset_name="bench-v1-K5"):
        return {
            "run_id": run_id,
            "features": {"families_enabled": ["knn"], "drop_features": []},
            "dataset": {
                "name": dataset_name,
                "schema_sha": "abc123def456",
                "embedding_config_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
            },
            "metrics": {"fmax": 0.42},
            "feature_importance": {"distance": 0.5},
        }

    def test_import_multipart_persists_row(self, reranker_client):
        client, session, store = reranker_client
        files = {
            "model_file": ("model.txt", b"tree\n--\n", "text/plain"),
            "spec_yaml": ("spec.yaml", self._spec_yaml().encode(), "text/yaml"),
            "run_json": (
                "run.json",
                json.dumps(self._run_json()).encode(),
                "application/json",
            ),
        }
        resp = client.post("/reranker-models/import", files=files)
        assert resp.status_code == 201, resp.text

        body = resp.json()
        assert body["name"] == "run-abc"
        assert body["artifact_uri"].endswith("rerankers/run-abc/model.txt")
        assert body["storage_backend"] == "local"

        # Booster was uploaded + RerankerModel row persisted
        store.put.assert_called_once()
        assert session.add.call_count == 1

    def test_import_by_reference_persists_row(self, reranker_client):
        client, session, store = reranker_client
        resp = client.post(
            "/reranker-models/import-by-reference",
            json={
                "artifact_uri": "s3://protea/rerankers/run-xyz/model.txt",
                "spec_yaml": self._spec_yaml("lk-mfo"),
                "run": self._run_json(run_id="run-xyz"),
            },
        )
        assert resp.status_code == 201, resp.text

        body = resp.json()
        assert body["name"] == "run-xyz"
        assert body["artifact_uri"] == "s3://protea/rerankers/run-xyz/model.txt"

        # By-reference path must NOT hit the store
        store.put.assert_not_called()
        assert session.add.call_count == 1

    def test_duplicate_name_conflicts_without_force(self, reranker_client):
        client, session, _ = reranker_client
        session.query.return_value.filter.return_value.first.return_value = MagicMock(
            id=uuid.uuid4()
        )
        resp = client.post(
            "/reranker-models/import-by-reference",
            json={
                "artifact_uri": "s3://protea/rerankers/run-dup/model.txt",
                "spec_yaml": self._spec_yaml(),
                "run": self._run_json(run_id="run-dup"),
            },
        )
        assert resp.status_code == 409
        assert "already exists" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# End-to-end wiring: both routers coexist + the export operation is in the
# registry; TrainReranker* are *not*.
# ---------------------------------------------------------------------------


class TestOperationCatalogWiring:
    def test_export_dataset_registered_training_ops_not(self):
        from protea.core.operation_catalog import build_operation_registry

        registry = build_operation_registry()
        names = set(registry._ops.keys())  # type: ignore[attr-defined]

        assert "export_research_dataset" in names
        assert "train_reranker" not in names
        assert "train_reranker_auto" not in names
