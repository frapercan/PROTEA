"""Integration tests against real PostgreSQL + pgvector.

Run with: poetry run pytest --with-postgres -m integration
"""
from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register all models
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence

_noop_emit = lambda *_: None  # noqa: E731

_OBO_SAMPLE = """\
format-version: 1.2
data-version: releases/2024-01-17

[Term]
id: GO:0003674
name: molecular_function
namespace: molecular_function

[Term]
id: GO:0008150
name: biological_process
namespace: biological_process

[Term]
id: GO:0005575
name: cellular_component
namespace: cellular_component

[Term]
id: GO:0003824
name: catalytic activity
namespace: molecular_function
is_a: GO:0003674

[Typedef]
id: part_of
"""


@pytest.fixture()
def db(postgres_url: str):
    """Create a clean database for each test."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# Load ontology snapshot — full round-trip
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_load_ontology_snapshot_roundtrip(db):
    from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotOperation

    op = LoadOntologySnapshotOperation()

    with patch.object(op, "_download", return_value=_OBO_SAMPLE):
        with Session(db, future=True) as session:
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=_noop_emit,
            )
            session.commit()

    assert result.result["terms_inserted"] == 4
    assert result.result["obo_version"] == "releases/2024-01-17"

    # Verify data in DB
    with Session(db, future=True) as session:
        snapshot = session.query(OntologySnapshot).one()
        assert snapshot.obo_version == "releases/2024-01-17"

        terms = session.query(GOTerm).filter_by(ontology_snapshot_id=snapshot.id).all()
        assert len(terms) == 4

        go_ids = {t.go_id for t in terms}
        assert "GO:0003674" in go_ids
        assert "GO:0003824" in go_ids

        # Aspect mapping
        mf_term = session.query(GOTerm).filter_by(go_id="GO:0003674").one()
        assert mf_term.aspect == "F"


@pytest.mark.integration
def test_load_ontology_snapshot_idempotent(db):
    from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotOperation

    op = LoadOntologySnapshotOperation()

    with patch.object(op, "_download", return_value=_OBO_SAMPLE):
        with Session(db, future=True) as session:
            op.execute(session, {"obo_url": "http://example.org/go.obo"}, emit=_noop_emit)
            session.commit()

    # Second run — should skip
    op2 = LoadOntologySnapshotOperation()
    with patch.object(op2, "_download", return_value=_OBO_SAMPLE):
        with Session(db, future=True) as session:
            result = op2.execute(session, {"obo_url": "http://example.org/go.obo"}, emit=_noop_emit)
            session.commit()

    assert result.result["skipped"] is True

    # Still only one snapshot
    with Session(db, future=True) as session:
        assert session.query(OntologySnapshot).count() == 1


# ---------------------------------------------------------------------------
# Store embeddings — pgvector round-trip
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_store_embeddings_roundtrip(db):
    from protea.core.operations.compute_embeddings import StoreEmbeddingsOperation

    with Session(db, future=True) as session:
        # Setup: create EmbeddingConfig + Sequence + parent Job
        config = EmbeddingConfig(
            model_name="test/model",
            model_backend="esm",
            layer_indices=[0],
            layer_agg="mean",
            pooling="mean",
            normalize_residues=False,
            normalize=True,
            max_length=1022,
            use_chunking=False,
            chunk_size=512,
            chunk_overlap=0,
        )
        session.add(config)

        seq = Sequence(sequence="MKVLWAGS", sequence_hash=Sequence.compute_hash("MKVLWAGS"))
        session.add(seq)

        parent = Job(operation="compute_embeddings", queue_name="protea.embeddings",
                     status=JobStatus.RUNNING, progress_current=0, progress_total=1)
        session.add(parent)
        session.flush()

        config_id = config.id
        seq_id = seq.id
        parent_id = parent.id
        session.commit()

    # Execute store_embeddings
    op = StoreEmbeddingsOperation()
    vec = [0.1, 0.2, 0.3, 0.4]
    payload = {
        "parent_job_id": str(parent_id),
        "embedding_config_id": str(config_id),
        "skip_existing": True,
        "sequences": [{
            "sequence_id": seq_id,
            "chunks": [{
                "chunk_index_s": 0,
                "chunk_index_e": None,
                "vector": vec,
                "embedding_dim": 4,
            }],
        }],
    }

    with Session(db, future=True) as session:
        result = op.execute(session, payload, emit=_noop_emit)
        session.commit()

    assert result.result["embeddings_stored"] == 1

    # Verify embedding in DB
    with Session(db, future=True) as session:
        emb = session.query(SequenceEmbedding).filter_by(sequence_id=seq_id).one()
        assert emb.embedding_config_id == config_id
        assert emb.embedding_dim == 4
        stored_vec = list(emb.embedding)
        np.testing.assert_allclose(stored_vec, vec, atol=1e-5)

    # Second run — skip_existing should prevent re-insert
    with Session(db, future=True) as session:
        result2 = op.execute(session, payload, emit=_noop_emit)
        session.commit()

    assert result2.result["sequences_skipped"] == 1
    assert result2.result["embeddings_stored"] == 0


# ---------------------------------------------------------------------------
# Store predictions — round-trip with parent progress
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_store_predictions_roundtrip(db):
    from protea.core.operations.predict_go_terms import StorePredictionsOperation

    with Session(db, future=True) as session:
        # Setup: EmbeddingConfig, AnnotationSet, OntologySnapshot, PredictionSet, GOTerm, Job
        config = EmbeddingConfig(
            model_name="test/model",
            model_backend="esm",
            layer_indices=[0],
            layer_agg="mean",
            pooling="mean",
            normalize_residues=False,
            normalize=True,
            max_length=1022,
            use_chunking=False,
            chunk_size=512,
            chunk_overlap=0,
        )
        session.add(config)

        snap = OntologySnapshot(obo_version="test-v1", obo_url="http://test")
        session.add(snap)
        session.flush()

        go_term1 = GOTerm(
            go_id="GO:0003674",
            name="molecular_function",
            aspect="F",
            ontology_snapshot_id=snap.id,
        )
        go_term2 = GOTerm(
            go_id="GO:0008150",
            name="biological_process",
            aspect="P",
            ontology_snapshot_id=snap.id,
        )
        session.add_all([go_term1, go_term2])

        ann_set = AnnotationSet(
            ontology_snapshot_id=snap.id,
            source="test",
            source_version="v1",
        )
        session.add(ann_set)
        session.flush()

        pred_set = PredictionSet(
            embedding_config_id=config.id,
            annotation_set_id=ann_set.id,
            ontology_snapshot_id=snap.id,
            limit_per_entry=5,
            meta={},
        )
        session.add(pred_set)

        parent = Job(
            operation="predict_go_terms",
            queue_name="protea.jobs",
            status=JobStatus.RUNNING,
            progress_current=0,
            progress_total=1,
        )
        session.add(parent)
        session.flush()

        pred_set_id = pred_set.id
        parent_id = parent.id
        go_term1_id = go_term1.id
        go_term2_id = go_term2.id
        session.commit()

    # Execute store_predictions
    op = StorePredictionsOperation()
    payload = {
        "parent_job_id": str(parent_id),
        "prediction_set_id": str(pred_set_id),
        "predictions": [
            {
                "protein_accession": "P12345",
                "go_term_id": go_term1_id,
                "ref_protein_accession": "Q99999",
                "distance": 0.15,
                "qualifier": "enables",
                "evidence_code": "IDA",
            },
            {
                "protein_accession": "P12345",
                "go_term_id": go_term2_id,
                "ref_protein_accession": "Q88888",
                "distance": 0.25,
            },
        ],
    }

    events = []
    def capture_emit(event, msg, fields, level):
        events.append(event)

    with Session(db, future=True) as session:
        result = op.execute(session, payload, emit=capture_emit)
        session.commit()

    assert result.result["predictions_inserted"] == 2

    # Parent job should be closed (progress_total=1, this was the only batch)
    assert "store_predictions.parent_succeeded" in events

    # Verify predictions in DB
    with Session(db, future=True) as session:
        preds = session.query(GOPrediction).filter_by(prediction_set_id=pred_set_id).all()
        assert len(preds) == 2
        distances = sorted(p.distance for p in preds)
        np.testing.assert_allclose(distances, [0.15, 0.25], atol=1e-5)

        # Parent job should be SUCCEEDED
        parent = session.get(Job, parent_id)
        assert parent.status == JobStatus.SUCCEEDED
        assert parent.finished_at is not None


# ---------------------------------------------------------------------------
# Job lifecycle — parent-child with atomic progress
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_job_parent_child_progress(db):
    with Session(db, future=True) as session:
        parent = Job(
            operation="compute_embeddings",
            queue_name="protea.embeddings",
            status=JobStatus.RUNNING,
            progress_current=0,
            progress_total=3,
        )
        session.add(parent)
        session.flush()
        parent_id = parent.id

        # Add events
        session.add(JobEvent(job_id=parent_id, event="job.created", fields={}))
        session.add(JobEvent(job_id=parent_id, event="job.started", fields={}))
        session.commit()

    # Simulate 3 child batches incrementing progress
    from sqlalchemy import update as sa_update

    from protea.core.utils import utcnow

    for i in range(3):
        with Session(db, future=True) as session:
            row = session.execute(
                sa_update(Job)
                .where(Job.id == parent_id, Job.status == JobStatus.RUNNING)
                .values(progress_current=Job.progress_current + 1)
                .returning(Job.progress_current, Job.progress_total)
            ).fetchone()
            assert row is not None
            assert row.progress_current == i + 1

            if row.progress_current == row.progress_total:
                session.execute(
                    sa_update(Job)
                    .where(Job.id == parent_id, Job.status == JobStatus.RUNNING)
                    .values(status=JobStatus.SUCCEEDED, finished_at=utcnow())
                )
                session.add(JobEvent(
                    job_id=parent_id,
                    event="job.succeeded",
                    fields={"via": "last_batch"},
                    level="info",
                ))
            session.commit()

    # Verify final state
    with Session(db, future=True) as session:
        job = session.get(Job, parent_id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.progress_current == 3
        assert job.progress_total == 3
        assert job.finished_at is not None
        assert len(job.events) == 3  # created, started, succeeded


# ---------------------------------------------------------------------------
# Load GOA annotations — round-trip
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_load_goa_annotations_roundtrip(db):
    from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsOperation
    from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotOperation

    # Step 1: Load ontology
    ont_op = LoadOntologySnapshotOperation()
    with patch.object(ont_op, "_download", return_value=_OBO_SAMPLE):
        with Session(db, future=True) as session:
            ont_result = ont_op.execute(
                session, {"obo_url": "http://example.org/go.obo"}, emit=_noop_emit,
            )
            session.commit()

    snapshot_id = ont_result.result["ontology_snapshot_id"]

    # Step 2: Insert proteins so annotations can be filtered
    with Session(db, future=True) as session:
        seq = Sequence(sequence="MKVLWAGS", sequence_hash=Sequence.compute_hash("MKVLWAGS"))
        session.add(seq)
        session.flush()
        protein = Protein(
            accession="P12345",
            canonical_accession="P12345",
            is_canonical=True,
            sequence_id=seq.id,
        )
        session.add(protein)
        session.commit()

    # Step 3: Build a GAF record (as _stream_gaf yields dicts)
    gaf_records = [
        {
            "accession": "P12345",
            "go_id": "GO:0003824",
            "qualifier": "enables",
            "evidence_code": "IDA",
            "db_reference": "PMID:123",
            "with_from": "",
            "assigned_by": "UniProt",
            "annotation_date": "20240101",
        },
    ]

    # Step 4: Load annotations
    goa_op = LoadGOAAnnotationsOperation()
    with patch.object(goa_op, "_stream_gaf", return_value=iter(gaf_records)):
        with Session(db, future=True) as session:
            result = goa_op.execute(
                session,
                {
                    "ontology_snapshot_id": snapshot_id,
                    "gaf_url": "http://example.org/goa.gaf.gz",
                    "source_version": "2024-03",
                },
                emit=_noop_emit,
            )
            session.commit()

    assert result.result["annotations_inserted"] > 0

    # Verify annotation in DB
    with Session(db, future=True) as session:
        ann_set = session.query(AnnotationSet).one()
        assert ann_set.source == "goa"

        annotations = session.query(ProteinGOAnnotation).all()
        assert len(annotations) >= 1
        assert annotations[0].protein_accession == "P12345"
        assert annotations[0].evidence_code == "IDA"


# ---------------------------------------------------------------------------
# Full pipeline: QuerySet → Embeddings → Predictions
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_full_pipeline_queryset_to_predictions(db):
    """End-to-end: create QuerySet, store embeddings, store predictions."""
    from protea.core.operations.compute_embeddings import StoreEmbeddingsOperation
    from protea.core.operations.predict_go_terms import StorePredictionsOperation

    dim = 4

    with Session(db, future=True) as session:
        # 1. Create EmbeddingConfig
        config = EmbeddingConfig(
            model_name="test/model", model_backend="esm",
            layer_indices=[0], layer_agg="mean", pooling="mean",
            normalize_residues=False, normalize=True,
            max_length=1022, use_chunking=False, chunk_size=512, chunk_overlap=0,
        )
        session.add(config)

        # 2. Create Ontology + GOTerm
        snap = OntologySnapshot(obo_version="pipeline-test", obo_url="http://test")
        session.add(snap)
        session.flush()

        go_mf = GOTerm(go_id="GO:0003674", name="molecular_function", aspect="F",
                       ontology_snapshot_id=snap.id)
        session.add(go_mf)

        # 3. Create AnnotationSet
        ann_set = AnnotationSet(ontology_snapshot_id=snap.id, source="test", source_version="v1")
        session.add(ann_set)

        # 4. Create sequences + proteins
        seq1 = Sequence(sequence="MKVLWAGS", sequence_hash=Sequence.compute_hash("MKVLWAGS"))
        seq2 = Sequence(sequence="ACDEFGHI", sequence_hash=Sequence.compute_hash("ACDEFGHI"))
        session.add_all([seq1, seq2])
        session.flush()

        p1 = Protein(accession="Q_QUERY", canonical_accession="Q_QUERY",
                      is_canonical=True, sequence_id=seq1.id)
        p2 = Protein(accession="R_REF", canonical_accession="R_REF",
                      is_canonical=True, sequence_id=seq2.id)
        session.add_all([p1, p2])

        # 5. Create QuerySet
        qs = QuerySet(name="pipeline-test", description="integration test")
        session.add(qs)
        session.flush()

        entry = QuerySetEntry(query_set_id=qs.id, sequence_id=seq1.id, accession="Q_QUERY")
        session.add(entry)

        # 6. Create embedding parent job
        embed_job = Job(operation="compute_embeddings", queue_name="protea.embeddings",
                        status=JobStatus.RUNNING, progress_current=0, progress_total=1)
        session.add(embed_job)
        session.flush()

        ids = {
            "config_id": config.id, "snap_id": snap.id, "ann_set_id": ann_set.id,
            "go_term_id": go_mf.id, "seq1_id": seq1.id, "seq2_id": seq2.id,
            "qs_id": qs.id, "embed_job_id": embed_job.id,
        }
        session.commit()

    # 7. Store embeddings for both sequences
    store_emb = StoreEmbeddingsOperation()
    emb_payload = {
        "parent_job_id": str(ids["embed_job_id"]),
        "embedding_config_id": str(ids["config_id"]),
        "sequences": [
            {"sequence_id": ids["seq1_id"], "chunks": [
                {"chunk_index_s": 0, "chunk_index_e": None,
                 "vector": [0.9, 0.1, 0.0, 0.0], "embedding_dim": dim}
            ]},
            {"sequence_id": ids["seq2_id"], "chunks": [
                {"chunk_index_s": 0, "chunk_index_e": None,
                 "vector": [0.0, 0.0, 0.1, 0.9], "embedding_dim": dim}
            ]},
        ],
    }
    with Session(db, future=True) as session:
        emb_result = store_emb.execute(session, emb_payload, emit=_noop_emit)
        session.commit()

    assert emb_result.result["embeddings_stored"] == 2

    # 8. Create prediction job + PredictionSet
    with Session(db, future=True) as session:
        pred_job = Job(operation="predict_go_terms", queue_name="protea.jobs",
                       status=JobStatus.RUNNING, progress_current=0, progress_total=1)
        session.add(pred_job)

        pred_set = PredictionSet(
            embedding_config_id=ids["config_id"],
            annotation_set_id=ids["ann_set_id"],
            ontology_snapshot_id=ids["snap_id"],
            query_set_id=ids["qs_id"],
            limit_per_entry=5, meta={},
        )
        session.add(pred_set)
        session.flush()
        pred_job_id = pred_job.id
        pred_set_id = pred_set.id
        session.commit()

    # 9. Store predictions
    store_pred = StorePredictionsOperation()
    pred_payload = {
        "parent_job_id": str(pred_job_id),
        "prediction_set_id": str(pred_set_id),
        "predictions": [{
            "protein_accession": "Q_QUERY",
            "go_term_id": ids["go_term_id"],
            "ref_protein_accession": "R_REF",
            "distance": 0.85,
            "qualifier": "enables",
            "evidence_code": "IDA",
        }],
    }
    with Session(db, future=True) as session:
        pred_result = store_pred.execute(session, pred_payload, emit=_noop_emit)
        session.commit()

    assert pred_result.result["predictions_inserted"] == 1

    # 10. Verify full chain in DB
    with Session(db, future=True) as session:
        # QuerySet has entry
        entries = session.query(QuerySetEntry).filter_by(query_set_id=ids["qs_id"]).all()
        assert len(entries) == 1

        # Embeddings exist
        embs = session.query(SequenceEmbedding).filter_by(
            embedding_config_id=ids["config_id"]
        ).all()
        assert len(embs) == 2

        # Predictions exist
        preds = session.query(GOPrediction).filter_by(prediction_set_id=pred_set_id).all()
        assert len(preds) == 1
        assert preds[0].protein_accession == "Q_QUERY"
        assert preds[0].distance == pytest.approx(0.85, abs=1e-5)

        # Predict job should be SUCCEEDED
        pred_job = session.get(Job, pred_job_id)
        assert pred_job.status == JobStatus.SUCCEEDED
