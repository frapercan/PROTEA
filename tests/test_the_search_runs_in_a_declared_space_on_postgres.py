"""On Postgres: a run in a declared space reads its cut's statistics and maps both sides.

The unit tests feed the batch a space. Here the space comes from where a run
finds it: the photo that loaded the run's annotation set, the
``embedding_space_statistics`` row of (configuration, photo), and its artifact
in a store, verified against the row's sha256. The coordinator records the
statistics in the receipt and hands every batch the transform; the batch maps
its pool and its queries with them, and a member asked as a query lands on its
own row of the mapped pool.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core import disk_cache
from protea.core.embedding_space.transforms import transformed_copy
from protea.core.operations.predict_go_terms import _batch_op_reference as reference_module
from protea.core.operations.predict_go_terms import _search_space as space_module
from protea.core.operations.predict_go_terms._batch_op import (
    PredictGOTermsBatchOperation,
    _BatchExecCtx,
)
from protea.core.operations.predict_go_terms._common import PredictGOTermsBatchPayload
from protea.core.operations.predict_go_terms._coordinator import PredictGOTermsOperation
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.embedding_space_statistics import (
    EmbeddingSpaceStatistics,
)
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.storage.local import LocalFsArtifactStore
from tests.helpers.cut_photos import make_job, make_photo, member, sequence_ids
from tests.test_the_search_runs_in_a_declared_space import _artifact, _parameters

DIM = 8
MEMBERS = [f"M{i:02d}" for i in range(16)]


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    engine.dispose()
    factory = build_session_factory(postgres_url)
    with factory() as session:
        yield session
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    engine.dispose()


def _scene(
    session: Session, store: LocalFsArtifactStore
) -> tuple[CutPhoto, uuid.UUID, EmbeddingSpaceStatistics, np.ndarray]:
    """A photo whose members have vectors and rows, and the statistics of its space."""
    photo = make_photo(
        session, [member(acc, "ts", residues=f"MK{i:03d}") for i, acc in enumerate(MEMBERS)]
    )
    config = EmbeddingConfig(model_name="test-space", layer_indices=[])
    term = GOTerm(go_id="GO:0003674", aspect="F", ontology_snapshot_id=photo.ontology_snapshot_id)
    session.add_all([config, term])
    session.flush()
    rng = np.random.default_rng(11)
    stored = (rng.standard_normal((len(MEMBERS), DIM)) * np.geomspace(4.0, 0.2, DIM) + 1.0).astype(
        np.float16
    )
    sequences = sequence_ids(session, photo)
    for accession, vector in zip(MEMBERS, stored, strict=True):
        session.add(Protein(accession=accession, canonical_accession=accession))
        session.add(
            SequenceEmbedding(
                sequence_id=sequences[accession],
                embedding_config_id=config.id,
                embedding=[float(v) for v in vector],
                embedding_dim=DIM,
            )
        )
        session.add(
            ProteinGOAnnotation(
                annotation_set_id=photo.annotation_set_id,
                protein_accession=accession,
                go_term_id=term.id,
                evidence_code="EXP",
            )
        )
    bank = EmbeddingBank(
        embedding_config_id=config.id,
        photos=[{"cut": photo.cut_release, "manifest_sha256": photo.manifest_sha256}],
        sequences=len(MEMBERS),
        residues=5 * len(MEMBERS),
        dim=DIM,
        shards=1,
        sequences_sha256="a" * 64,
        model_revision="test",
        software={},
        card="cpu",
        token_budgets=[1],
        manifest={},
        manifest_uri="file:///bank.json",
        manifest_sha256="b" * 64,
        short_name="bank-test-space",
        description="A bank of the test's members.",
    )
    session.add(bank)
    session.flush()
    blob = _artifact(_parameters(stored.astype(np.float64), shrinkage=0.1, removed=2))
    sha = hashlib.sha256(blob).hexdigest()
    row = EmbeddingSpaceStatistics(
        embedding_config_id=config.id,
        cut_photo_id=photo.id,
        embedding_bank_id=bank.id,
        n=len(MEMBERS),
        d=DIM,
        artifact_uri=store.put(
            f"embedding-space-statistics/{config.id}/{photo.id}/{sha}.npz", blob
        ),
        artifact_sha256=sha,
        description="The statistics of the test's space.",
    )
    session.add(row)
    session.flush()
    return photo, config.id, row, stored


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LocalFsArtifactStore:
    """A store, a disk cache and process caches of this test's own."""
    store = LocalFsArtifactStore(tmp_path / "store")
    monkeypatch.setattr(space_module, "_store", lambda: store)
    monkeypatch.setattr(space_module, "_PARAMETERS", {})
    monkeypatch.setattr(reference_module, "_REF_CACHE", {})
    monkeypatch.setattr(disk_cache, "_DISK_CACHE_DIR", tmp_path / "ref-cache")
    return store


def _quiet(*_args: Any, **_kwargs: Any) -> None:
    return None


def test_a_member_asked_as_a_query_lands_on_its_row_of_the_mapped_pool(
    pg: Session, isolated: LocalFsArtifactStore
) -> None:
    photo, config, row, stored = _scene(pg, isolated)
    p = PredictGOTermsBatchPayload.model_validate(
        {
            "embedding_config_id": str(config),
            "annotation_set_id": str(photo.annotation_set_id),
            "ontology_snapshot_id": str(photo.ontology_snapshot_id),
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["M03", "M11"],
            "metric": "l2",
            "aspect_separated_knn": False,
            "embedding_transform": "whiten",
            "compute_alignments": False,
            "compute_taxonomy": False,
        }
    )
    ctx = _BatchExecCtx(
        p=p,
        parent_job_id=uuid.UUID(p.parent_job_id),
        prediction_set_id=uuid.UUID(p.prediction_set_id),
        embedding_config_id=config,
        annotation_set_id=photo.annotation_set_id,
    )
    op = PredictGOTermsBatchOperation()

    pool = op._ensure_reference_cache(pg, ctx, _quiet)
    queries, asked = op._load_query_embeddings(pg, ["M03", "M11"], config, p, _quiet)

    assert pool["accessions"] == MEMBERS
    assert asked == ["M03", "M11"]
    rows = [MEMBERS.index(accession) for accession in asked]
    assert np.max(np.abs(queries - pool["embeddings_f32"][rows])) <= 1e-6
    parameters = space_module.search_space(pg, p).parameters  # type: ignore[union-attr]
    expected = transformed_copy(stored.astype(np.float32), "whiten", parameters)
    assert np.allclose(pool["embeddings_f32"], expected, atol=1e-5)
    written, _ = disk_cache._disk_cache_paths(config, photo.annotation_set_id)
    assert np.load(written).tobytes() == stored.tobytes(), "the disk cache keeps the raw pool"


def test_the_coordinator_records_the_statistics_and_hands_the_batches_the_space(
    pg: Session, isolated: LocalFsArtifactStore
) -> None:
    photo, config, row, _stored = _scene(pg, isolated)
    payload = {
        "embedding_config_id": str(config),
        "annotation_set_id": str(photo.annotation_set_id),
        "ontology_snapshot_id": str(photo.ontology_snapshot_id),
        "query_accessions": ["M01", "M02"],
        "embedding_transform": "remove_dominant_directions",
        "compute_alignments": False,
        "_job_id": str(make_job(pg, "predict_go_terms")),
    }

    result = PredictGOTermsOperation().execute(pg, payload, emit=_quiet)

    prediction_set = pg.get(PredictionSet, uuid.UUID(result.result["prediction_set_id"]))
    assert prediction_set is not None
    block = prediction_set.meta["embedding_transform"]
    assert block["name"] == "remove_dominant_directions"
    assert (block["statistics_id"], block["statistics_sha256"]) == (
        str(row.id),
        row.artifact_sha256,
    )
    assert (block["n"], block["d"], block["shrinkage"], block["removed_directions"]) == (
        len(MEMBERS),
        DIM,
        0.1,
        2,
    )
    (_queue, message), *_rest = result.publish_operations
    assert message["payload"]["embedding_transform"] == "remove_dominant_directions"
