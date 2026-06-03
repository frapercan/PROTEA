"""Postgres-backed smoke test for ``predict_go_terms_from_interpro`` (IP.4b-PRED).

Acceptance criterion from the slice spec: smoke test on 10 proteins.

Sets up the minimal context needed to exercise the operation
end-to-end against a real Postgres instance:

* One :class:`OntologySnapshot` + GOTerm rows for the GO ids in the
  synthetic mapping.
* One :class:`AnnotationSet` (used only as an FK target — the
  InterPro path doesn't read its rows).
* One :class:`EmbeddingConfig` (FK target only).
* 10 :class:`Protein` rows, each with a few :class:`InterProAnnotation`
  hits.
* :class:`InterProGoMapping` rows tying each IPR to a GO id.

Then runs the operation and asserts:

1. One :class:`PredictionSet` was created with ``meta["algorithm"]``
   = ``"interpro_propagation"``.
2. At least one :class:`GOPrediction` row per protein with hits.
3. Vote-count aggregation works: proteins with two IPR hits mapped to
   the same GO term should have ``vote_count == 2``.
4. Re-running on the *same* :class:`PredictionSet` cannot happen
   (each call mints a new one), but the underlying conflict-do-nothing
   would protect a partial-failure retry — we cover that by
   re-running the bulk insert on a manually re-issued payload.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register tables
from protea.core.operations.predict_go_terms_from_interpro import (
    PredictGOTermsFromInterProOperation,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.interpro_annotation import (
    InterProAnnotation,
)
from protea.infrastructure.orm.models.annotation.interpro_go_mapping import (
    InterProGoMapping,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence

_IPR_RELEASE = "InterProScan-5.66-98.0"
_I2G_VERSION = "InterPro-104.0"
_N_PROTEINS = 10
_N_HITS_PER_PROTEIN = 3  # each protein has 3 InterPro hits


def _noop_emit(*_args, **_kwargs) -> None:
    return None


@pytest.fixture()
def pg_session(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)


@pytest.fixture()
def seeded_world(pg_session: Session) -> dict[str, uuid.UUID | list[str]]:
    """Seed the minimum fixture for the smoke test.

    Layout (one IPR per index lets us assert vote_count deterministically):

    * ``IPR000000`` → ``GO:0000000`` (shared by *every* protein → 1 vote each)
    * ``IPR{i+1:06d}`` → ``GO:{i+1:07d}`` (unique per protein → 1 vote each)
    * ``IPR{i+101:06d}`` → ``GO:0000000`` (shared GO again → bumps vote_count to 2)

    So each protein has 3 hits and ends up with:
    * 2 votes for ``GO:0000000`` (IPR000000 + IPR{i+101})
    * 1 vote for ``GO:{i+1:07d}``
    """
    # Ontology snapshot + GO terms
    snap = OntologySnapshot(obo_url="file:///stub.obo", obo_version="test-2026-05-12")
    pg_session.add(snap)
    pg_session.flush()

    go_terms: list[GOTerm] = [
        GOTerm(go_id="GO:0000000", aspect="F", ontology_snapshot_id=snap.id),
    ]
    for i in range(_N_PROTEINS):
        go_terms.append(
            GOTerm(go_id=f"GO:{i + 1:07d}", aspect="F", ontology_snapshot_id=snap.id)
        )
    pg_session.add_all(go_terms)
    pg_session.flush()

    # AnnotationSet (FK target, not actually read by the op)
    ann_set = AnnotationSet(
        source="stub",
        source_version="v1",
        ontology_snapshot_id=snap.id,
        meta={},
    )
    pg_session.add(ann_set)
    pg_session.flush()

    # EmbeddingConfig (FK target, not read)
    cfg = EmbeddingConfig(
        model_name="stub-model",
        model_backend="esm",
        layer_indices=[0],
        layer_agg="mean",
        pooling="mean",
    )
    pg_session.add(cfg)
    pg_session.flush()

    # Proteins + sequences
    seq = Sequence(
        sequence="MKTIIALSYIFCLVFA",
        sequence_hash=Sequence.compute_hash("MKTIIALSYIFCLVFA"),
    )
    pg_session.add(seq)
    pg_session.flush()
    accessions: list[str] = []
    for i in range(_N_PROTEINS):
        acc = f"P{i:05d}"
        accessions.append(acc)
        pg_session.add(
            Protein(
                accession=acc,
                entry_name=f"TST{i}_HUMAN",
                reviewed=True,
                canonical_accession=acc,
                is_canonical=True,
                length=16,
                sequence_id=seq.id,
            )
        )
    pg_session.flush()

    # InterProAnnotation rows
    for i, acc in enumerate(accessions):
        # Hit 1: shared IPR mapping to shared GO:0000000
        pg_session.add(
            InterProAnnotation(
                protein_accession=acc,
                source_db="Pfam",
                accession="IPR000000",
                start=1,
                end=10,
                ipr_release=_IPR_RELEASE,
            )
        )
        # Hit 2: unique IPR mapping to unique GO:{i+1:07d}
        pg_session.add(
            InterProAnnotation(
                protein_accession=acc,
                source_db="Pfam",
                accession=f"IPR{i + 1:06d}",
                start=11,
                end=14,
                ipr_release=_IPR_RELEASE,
            )
        )
        # Hit 3: second IPR also mapping to shared GO:0000000 → extra vote
        pg_session.add(
            InterProAnnotation(
                protein_accession=acc,
                source_db="Pfam",
                accession=f"IPR{i + 101:06d}",
                start=15,
                end=16,
                ipr_release=_IPR_RELEASE,
            )
        )

    # InterPro2GO mapping rows
    pg_session.add(
        InterProGoMapping(
            ipr_accession="IPR000000",
            go_id="GO:0000000",
            evidence="IEA",
            source_version=_I2G_VERSION,
        )
    )
    for i in range(_N_PROTEINS):
        pg_session.add(
            InterProGoMapping(
                ipr_accession=f"IPR{i + 1:06d}",
                go_id=f"GO:{i + 1:07d}",
                evidence="IEA",
                source_version=_I2G_VERSION,
            )
        )
        pg_session.add(
            InterProGoMapping(
                ipr_accession=f"IPR{i + 101:06d}",
                go_id="GO:0000000",
                evidence="IEA",
                source_version=_I2G_VERSION,
            )
        )

    pg_session.commit()

    return {
        "snapshot_id": snap.id,
        "annotation_set_id": ann_set.id,
        "embedding_config_id": cfg.id,
        "accessions": accessions,
    }


@pytest.mark.integration
def test_smoke_10_proteins(
    pg_session: Session, seeded_world: dict[str, uuid.UUID | list[str]]
) -> None:
    """Acceptance criterion: smoke test on 10 proteins."""
    op = PredictGOTermsFromInterProOperation()
    payload = {
        "embedding_config_id": str(seeded_world["embedding_config_id"]),
        "annotation_set_id": str(seeded_world["annotation_set_id"]),
        "ontology_snapshot_id": str(seeded_world["snapshot_id"]),
        "source_version": _I2G_VERSION,
        "query_accessions": list(seeded_world["accessions"]),
    }
    result = op.execute(pg_session, payload, emit=_noop_emit)
    pg_session.commit()

    # Each protein has 2 distinct GO term predictions:
    #   GO:0000000 (vote_count=2) + GO:{i:07d} (vote_count=1)
    expected_predictions = _N_PROTEINS * 2
    assert result.result["proteins_requested"] == _N_PROTEINS
    assert result.result["proteins_with_ipr"] == _N_PROTEINS
    assert result.result["proteins_with_predictions"] == _N_PROTEINS
    assert result.result["candidate_pairs"] == expected_predictions
    assert result.result["predictions_inserted"] == expected_predictions
    assert result.result["algorithm"] == "interpro_propagation"

    # PredictionSet was minted with the right algorithm tag.
    ps = pg_session.scalar(
        select(PredictionSet).where(
            PredictionSet.id == uuid.UUID(result.result["prediction_set_id"])
        )
    )
    assert ps is not None
    assert ps.meta["algorithm"] == "interpro_propagation"
    assert ps.meta["interpro_mapping_source_version"] == _I2G_VERSION
    assert ps.meta["ipr_releases_seen"] == [_IPR_RELEASE]

    # Vote-count aggregation: GO:0000000 has 2 votes per protein.
    shared_go = pg_session.scalar(
        select(GOTerm).where(
            GOTerm.go_id == "GO:0000000",
            GOTerm.ontology_snapshot_id == seeded_world["snapshot_id"],
        )
    )
    assert shared_go is not None
    shared_predictions = pg_session.scalars(
        select(GOPrediction).where(
            GOPrediction.prediction_set_id == ps.id,
            GOPrediction.go_term_id == shared_go.id,
        )
    ).all()
    assert len(shared_predictions) == _N_PROTEINS
    assert all(p.vote_count == 2 for p in shared_predictions)
    # Lower distance for higher vote_count (1/2 = 0.5).
    assert all(p.distance == pytest.approx(0.5) for p in shared_predictions)
    assert all(p.evidence_code == "IEA" for p in shared_predictions)


@pytest.mark.integration
def test_idempotent_partial_failure_retry(
    pg_session: Session, seeded_world: dict[str, uuid.UUID | list[str]]
) -> None:
    """Re-issuing the same insert against the same PredictionSet is a no-op.

    Models a worker restart: the first call inserted predictions; the
    rerun targets the *same* PredictionSet and the
    ``uq_go_prediction_set_protein_term`` constraint suppresses the
    duplicates. We replay the bulk insert through the private helper
    against the just-created prediction_set_id to confirm.
    """
    op = PredictGOTermsFromInterProOperation()
    payload = {
        "embedding_config_id": str(seeded_world["embedding_config_id"]),
        "annotation_set_id": str(seeded_world["annotation_set_id"]),
        "ontology_snapshot_id": str(seeded_world["snapshot_id"]),
        "source_version": _I2G_VERSION,
        "query_accessions": list(seeded_world["accessions"]),
    }
    first = op.execute(pg_session, payload, emit=_noop_emit)
    pg_session.commit()

    ps_id = uuid.UUID(first.result["prediction_set_id"])
    initial_count = pg_session.scalar(
        select(GOPrediction).where(GOPrediction.prediction_set_id == ps_id)
    )
    assert initial_count is not None

    # Construct rows identical to what the first execute() inserted and
    # replay through the bulk-insert helper to prove conflict-do-nothing.
    rows = []
    for acc in seeded_world["accessions"]:
        # The shared GO:0000000 row (2 votes)
        shared_go = pg_session.scalar(
            select(GOTerm).where(
                GOTerm.go_id == "GO:0000000",
                GOTerm.ontology_snapshot_id == seeded_world["snapshot_id"],
            )
        )
        assert shared_go is not None
        rows.append(
            {
                "prediction_set_id": ps_id,
                "protein_accession": acc,
                "go_term_id": shared_go.id,
                "ref_protein_accession": "IPR000000",
                "distance": 0.5,
                "evidence_code": "IEA",
                "vote_count": 2,
            }
        )

    re_inserted = PredictGOTermsFromInterProOperation._bulk_insert(
        pg_session, rows, chunk_size=1000
    )
    pg_session.commit()
    assert re_inserted == 0  # every row was a conflict


@pytest.mark.integration
def test_no_predictions_when_source_version_mismatches(
    pg_session: Session, seeded_world: dict[str, uuid.UUID | list[str]]
) -> None:
    """A wrong ``source_version`` joins to zero mapping rows → zero predictions."""
    op = PredictGOTermsFromInterProOperation()
    payload = {
        "embedding_config_id": str(seeded_world["embedding_config_id"]),
        "annotation_set_id": str(seeded_world["annotation_set_id"]),
        "ontology_snapshot_id": str(seeded_world["snapshot_id"]),
        "source_version": "Nonexistent-Release-999",
        "query_accessions": list(seeded_world["accessions"]),
    }
    result = op.execute(pg_session, payload, emit=_noop_emit)
    pg_session.commit()

    assert result.result["candidate_pairs"] == 0
    assert result.result["predictions_inserted"] == 0
    # A PredictionSet is still created (acts as an audit trail of the
    # zero-result run); the algorithm tag is intact.
    ps = pg_session.scalar(
        select(PredictionSet).where(
            PredictionSet.id == uuid.UUID(result.result["prediction_set_id"])
        )
    )
    assert ps is not None
    assert ps.meta["algorithm"] == "interpro_propagation"


@pytest.mark.integration
def test_ipr_release_floor_filters_old_hits(
    pg_session: Session, seeded_world: dict[str, uuid.UUID | list[str]]
) -> None:
    """``ipr_release_floor`` skips InterProAnnotation rows below the threshold."""
    op = PredictGOTermsFromInterProOperation()
    payload = {
        "embedding_config_id": str(seeded_world["embedding_config_id"]),
        "annotation_set_id": str(seeded_world["annotation_set_id"]),
        "ontology_snapshot_id": str(seeded_world["snapshot_id"]),
        "source_version": _I2G_VERSION,
        "query_accessions": list(seeded_world["accessions"]),
        "ipr_release_floor": "InterProScan-9.99-99.9",  # lex-greater than seeded release
    }
    result = op.execute(pg_session, payload, emit=_noop_emit)
    pg_session.commit()
    assert result.result["candidate_pairs"] == 0
    assert result.result["predictions_inserted"] == 0
