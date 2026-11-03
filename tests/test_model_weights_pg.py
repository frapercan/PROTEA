"""The model weights registry against real rows (ADR-D54).

The companion file pins the checks; this one pins what lands in Postgres and what a
weight's detail says uses it:

* a file registered by reference is a row with the hash the store gave, and the same key
  a second time is a conflict that names the existing row;
* an upload completed in parts is a row under its content-addressed key, and the same bytes
  uploaded again are a conflict, not a second row;
* a residue vocabulary's detail lists the configurations whose name carries its digest and
  the jobs whose payload names its key, and the listing counts the configurations.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.infrastructure.orm.models.job import Job, JobStatus
from protea.infrastructure.storage.local import LocalFsArtifactStore
from protea.services import model_weights_service as svc

KEY = "residue-vocabulary/vocabulary-v1-0123456789ab.npz"
DATA = b"vocabulary v1 bytes" * 4096
DIGEST = hashlib.sha256(DATA).hexdigest()


def _fresh(postgres_url: str) -> Any:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture()
def store(tmp_path: Path) -> LocalFsArtifactStore:
    s = LocalFsArtifactStore(tmp_path / "store")
    s.put(KEY, DATA)
    return s


def _register(session: Session, store: LocalFsArtifactStore) -> ModelWeights:
    info = svc.WeightsInfo(
        "residue_vocabulary",
        "vocabulary-v1",
        meta={"layers": [2, 10, 22, 40, 48], "dict_dim": 8192, "actives": 128, "levels": 256},
        created_by="operator",
    )
    return svc.register_by_reference(session, store, info, KEY, DIGEST)


def test_a_reference_becomes_a_row_and_a_second_one_is_a_conflict(
    postgres_url: str, store: LocalFsArtifactStore
) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        row = _register(session, store)
        session.commit()
        stored = session.scalars(select(ModelWeights)).one()
        assert (stored.uri, stored.sha256, stored.size_bytes) == (KEY, DIGEST, len(DATA))
        assert stored.meta["dict_dim"] == 8192
        with pytest.raises(svc.WeightsConflict) as conflict:
            _register(session, store)
        assert conflict.value.existing_id == row.id


def test_the_detail_says_what_uses_a_vocabulary(
    postgres_url: str, store: LocalFsArtifactStore
) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        row = _register(session, store)
        session.add(
            EmbeddingConfig(
                model_name=f"residue-vocabulary:{DIGEST[:16]}:f3a7fe16",
                model_backend="residue-vocabulary",
                layer_indices=[2, 10, 22, 40, 48],
                layer_agg="concat",
                pooling="per-residue",
                normalize=False,
                kind="learned",
            )
        )
        session.add(
            EmbeddingConfig(
                model_name="residue-vocabulary:ffffffffffffffff:f3a7fe16",
                model_backend="residue-vocabulary",
                layer_indices=[2, 10, 22, 40, 48],
                layer_agg="concat",
                pooling="per-residue",
                normalize=False,
                kind="learned",
            )
        )
        job = Job(
            id=uuid.uuid4(),
            operation="encode_residue_vocabulary",
            queue_name="protea.embeddings",
            status=JobStatus.RUNNING,
            payload={
                "vocabulary_artifact_uri": KEY,
                "source_embedding_config_id": str(uuid.uuid4()),
            },
            meta={},
        )
        session.add(job)
        session.add(
            Job(
                id=uuid.uuid4(),
                operation="encode_residue_vocabulary",
                queue_name="protea.embeddings",
                status=JobStatus.SUCCEEDED,
                payload={"vocabulary_artifact_uri": "residue-vocabulary/other-0123456789ab.npz"},
                meta={},
            )
        )
        session.flush()
        detail = svc.weights_detail(session, row.id)
        listing = svc.list_weights(session)

    assert detail is not None
    assert [c["name"] for c in detail["used_by"]["configurations"]] == [
        f"residue-vocabulary:{DIGEST[:16]}:f3a7fe16"
    ], "only the configuration that carries this vocabulary's digest"
    assert [j["id"] for j in detail["used_by"]["jobs"]] == [str(job.id)]
    assert detail["used_by"]["jobs"][0]["status"] == "RUNNING"
    assert listing[0]["configurations"] == 1


def test_an_upload_lands_once(
    postgres_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PROTEA_TUNING__api__weights_upload_dir", str(tmp_path / "uploads"))
    monkeypatch.setenv("PROTEA_TUNING__api__weights_part_bytes", "4096")
    get_tuning.cache_clear()
    try:
        engine = _fresh(postgres_url)
        store = LocalFsArtifactStore(tmp_path / "store")

        def upload() -> str:
            u = svc.create_upload(
                svc.WeightsInfo("reader", "reader-211"), "reader.pt", len(DATA), DIGEST
            )
            for n in range(u["parts"]):
                svc.put_part(u["upload_id"], n, DATA[n * 4096 : (n + 1) * 4096])
            return str(u["upload_id"])

        with Session(engine) as session:
            row = svc.complete_upload(session, store, upload())
            session.commit()
            assert row.uri == f"model-weights/reader/reader-211-{DIGEST[:16]}.pt"
            assert store.get(row.uri) == DATA
            with pytest.raises(svc.WeightsConflict):
                svc.complete_upload(session, store, upload())
            assert session.scalars(select(ModelWeights)).all() == [row]
    finally:
        get_tuning.cache_clear()
