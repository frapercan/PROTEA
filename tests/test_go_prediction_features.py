"""Tests for the ``GOPrediction`` JSONB dual-write helper (T3.1a).

Two tiers:

1. Pure-function tests of ``build_feature_jsonb`` and ``from_json`` —
   stable shape, identity-key exclusion, NaN-safety (caller's job;
   helper just mirrors), round-trip equivalence.
2. End-to-end integration through ``StorePredictionsOperation`` against
   a real Postgres container, asserting that every typed feature column
   on the inserted ``GOPrediction`` row has a matching value in the
   ``features`` JSONB blob (gated by ``--with-postgres``).

The integration test is the load-bearing one: it catches any future
writer site that bypasses ``_row_from_prediction`` (which is the only
path through which the dual-write runs).
"""

from __future__ import annotations

import math
import uuid
from unittest.mock import MagicMock

import numpy as np
import pytest

from protea.core.operations.predict_go_terms import (
    StorePredictionsOperation,
    _row_from_prediction,
)
from protea.infrastructure.orm.models.embedding.go_prediction_features import (
    FEATURE_JSONB_KEYS,
    build_feature_jsonb,
    from_json,
)
from protea.infrastructure.orm.models.job import JobStatus

# ---------------------------------------------------------------------------
# Pure-function tier
# ---------------------------------------------------------------------------


class TestBuildFeatureJsonb:
    def test_identity_keys_are_dropped(self) -> None:
        row = {
            "prediction_set_id": uuid.uuid4(),
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
        }
        blob = build_feature_jsonb(row)
        for k in (
            "prediction_set_id",
            "protein_accession",
            "go_term_id",
            "ref_protein_accession",
        ):
            assert k not in blob
        assert blob["distance"] == 0.15

    def test_missing_feature_keys_default_to_none(self) -> None:
        # Realistic predict-side dict: only a handful of features set.
        row = {
            "prediction_set_id": uuid.uuid4(),
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
            "qualifier": "enables",
            "evidence_code": "IDA",
        }
        blob = build_feature_jsonb(row)
        # Every canonical key must be present (stable JSONB shape).
        assert set(blob.keys()) == set(FEATURE_JSONB_KEYS)
        # Unset features are mirrored as None.
        assert blob["vote_count"] is None
        assert blob["anc2vec_neighbor_cos"] is None
        assert blob["emb_pca_query_15"] is None

    def test_mirrors_every_feature_value(self) -> None:
        # Construct a row touching one column from every family so the
        # JSONB shape lock-step is enforced.
        row: dict[str, object] = {
            "prediction_set_id": uuid.uuid4(),
            "protein_accession": "P00001",
            "go_term_id": 1,
            "ref_protein_accession": "Q00001",
            "distance": 0.10,
            "qualifier": "enables",
            "evidence_code": "IDA",
            "identity_nw": 0.5,
            "identity_sw": 0.6,
            "length_query": 200,
            "length_ref": 250,
            "vote_count": 4,
            "k_position": 2,
            "neighbor_vote_fraction": 0.8,
            "neighbor_min_distance": 0.05,
            "neighbor_mean_distance": 0.12,
            "query_taxonomy_id": 9606,
            "ref_taxonomy_id": 10090,
            "taxonomic_distance": 4,
            "taxonomic_relation": "sibling",
            "anc2vec_neighbor_cos": 0.77,
            "tax_voters_same_frac": 0.25,
            "emb_pca_query_0": 0.1,
            "emb_pca_query_15": -0.2,
        }
        blob = build_feature_jsonb(row)
        for key, value in row.items():
            if key in {
                "prediction_set_id",
                "protein_accession",
                "go_term_id",
                "ref_protein_accession",
            }:
                continue
            assert blob[key] == value, f"mismatch on {key}"

    def test_full_canonical_key_count(self) -> None:
        # Lock the canonical key count: 3 (distance + 2 categoricals)
        # + 10 alignment + 2 lengths + 5 reranker + 3 consensus
        # + 6 taxonomy + 6 anc2vec + 3 tax_voters + 16 emb_pca = 54
        assert len(FEATURE_JSONB_KEYS) == 54
        assert len(set(FEATURE_JSONB_KEYS)) == 54  # no duplicates


class TestFromJson:
    def test_none_yields_empty_dict(self) -> None:
        assert from_json(None) == {}

    def test_round_trip_preserves_keys(self) -> None:
        row = {
            "prediction_set_id": uuid.uuid4(),
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.1,
            "vote_count": 5,
            "neighbor_min_distance": 0.05,
            "emb_pca_query_3": -0.4,
        }
        blob = build_feature_jsonb(row)
        restored = from_json(blob)
        # Round-trip preserves the full canonical shape.
        assert set(restored.keys()) == set(FEATURE_JSONB_KEYS)
        assert restored["distance"] == 0.1
        assert restored["vote_count"] == 5
        assert restored["neighbor_min_distance"] == 0.05
        assert restored["emb_pca_query_3"] == -0.4
        assert restored["anc2vec_has_emb"] is None

    def test_unknown_keys_in_blob_are_ignored(self) -> None:
        # Legacy / forward-compat blob with extra unknown keys.
        blob = {"distance": 0.2, "vote_count": 3, "garbage": "ignored"}
        restored = from_json(blob)
        assert "garbage" not in restored
        assert restored["distance"] == 0.2
        assert restored["vote_count"] == 3


# ---------------------------------------------------------------------------
# Writer-site integration (no DB)
# ---------------------------------------------------------------------------


class TestRowFromPredictionDualWrite:
    """``_row_from_prediction`` is the canonical writer; it must always
    attach a ``features`` blob whose values mirror the typed columns.
    """

    def test_row_includes_features_blob(self) -> None:
        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
            "qualifier": "enables",
            "evidence_code": "IDA",
            "vote_count": 3,
            "k_position": 1,
            "neighbor_min_distance": 0.05,
            "anc2vec_neighbor_cos": 0.8,
            "emb_pca_query_0": 0.1,
        }
        row = _row_from_prediction(pred, pred_set_id)
        assert "features" in row
        blob = row["features"]
        assert isinstance(blob, dict)
        # Mirror invariant: every feature key the row carries must
        # match the blob exactly. Identity keys must NOT leak in.
        for k in (
            "prediction_set_id",
            "protein_accession",
            "go_term_id",
            "ref_protein_accession",
        ):
            assert k not in blob
        assert blob["distance"] == row["distance"]
        assert blob["vote_count"] == row["vote_count"]
        assert blob["k_position"] == row["k_position"]
        assert blob["neighbor_min_distance"] == row["neighbor_min_distance"]
        assert blob["anc2vec_neighbor_cos"] == row["anc2vec_neighbor_cos"]
        assert blob["emb_pca_query_0"] == row["emb_pca_query_0"]
        # Unset features mirror as None in both row and blob.
        assert row["vote_count"] == 3
        assert row["alignment_score_nw"] is None
        assert blob["alignment_score_nw"] is None

    def test_nan_cleaned_in_blob(self) -> None:
        """NaN / inf are scrubbed by ``_clean_float`` BEFORE the blob is
        built, so the JSONB never carries a non-finite float (would
        break Postgres JSONB serialisation in some clients)."""
        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
            "neighbor_distance_std": float("nan"),
            "anc2vec_neighbor_cos": float("inf"),
        }
        row = _row_from_prediction(pred, pred_set_id)
        assert row["neighbor_distance_std"] is None
        assert row["anc2vec_neighbor_cos"] is None
        blob = row["features"]
        assert blob["neighbor_distance_std"] is None
        assert blob["anc2vec_neighbor_cos"] is None
        # ``math.isnan`` guard to make the intent unmistakable.
        assert not isinstance(blob["neighbor_distance_std"], float) or not math.isnan(
            blob["neighbor_distance_std"]
        )


class TestLafaFeaturePersistence:
    """LAFA per-category families (classifier / self_prior / association)
    have no typed ``GOPrediction`` column; they must ride the ``features``
    JSONB blob, and ONLY when the prediction dict carries them (i.e. the
    matching compute flag was on at predict time).
    """

    def test_lafa_keys_persisted_when_present(self) -> None:
        from protea.core.operations.predict_go_terms._common import (
            _LAFA_JSONB_FEATURE_KEYS,
        )

        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
            # A compute_* flag was on, so the family keys ride the dict.
            "classifier_score": 0.73,
            "classifier_present": 1.0,
            "self_prior_score": 1.0,
            "association_total": 0.4,
            "association_cross": 0.1,
            "association_present": 1.0,
        }
        blob = _row_from_prediction(pred, pred_set_id)["features"]
        for key in _LAFA_JSONB_FEATURE_KEYS:
            assert blob[key] == pred[key], f"mismatch on {key}"

    def test_default_run_writes_no_lafa_keys(self) -> None:
        """Flags off: the dict carries no LAFA keys, so the blob keeps its
        canonical shape exactly (golden / parity stay unchanged)."""
        from protea.core.operations.predict_go_terms._common import (
            _LAFA_JSONB_FEATURE_KEYS,
        )

        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
            "vote_count": 3,
        }
        blob = _row_from_prediction(pred, pred_set_id)["features"]
        # No LAFA key leaked in; the canonical key set is unchanged.
        assert set(blob.keys()) == set(FEATURE_JSONB_KEYS)
        for key in _LAFA_JSONB_FEATURE_KEYS:
            assert key not in blob

    def test_lafa_keys_partial_presence(self) -> None:
        """Only the families whose flag was on are written; the rest stay
        absent (a self_prior-only run carries just ``self_prior_score``)."""
        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P1",
            "go_term_id": 7,
            "ref_protein_accession": "Q1",
            "distance": 0.2,
            "self_prior_score": 1.0,
        }
        blob = _row_from_prediction(pred, pred_set_id)["features"]
        assert blob["self_prior_score"] == 1.0
        assert "classifier_score" not in blob
        assert "association_total" not in blob

    def test_lafa_nan_cleaned(self) -> None:
        """Non-finite LAFA values are scrubbed to ``None`` like the typed
        dual-write, so the JSONB never carries a non-finite float."""
        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P1",
            "go_term_id": 7,
            "ref_protein_accession": "Q1",
            "distance": 0.2,
            "association_total": float("nan"),
        }
        blob = _row_from_prediction(pred, pred_set_id)["features"]
        assert blob["association_total"] is None


# ---------------------------------------------------------------------------
# Unit test of StorePredictionsOperation hand-off (no DB)
# ---------------------------------------------------------------------------


class TestStorePredictionsDualWriteSurface:
    """At the ORM-call level, the rows passed to the bulk insert must
    each carry the ``features`` JSONB blob alongside the typed columns.
    Catches regressions where a future writer bypasses
    ``_row_from_prediction``.
    """

    def test_bulk_insert_rows_carry_features_blob(self) -> None:
        op = StorePredictionsOperation()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [
                {
                    "protein_accession": "P12345",
                    "go_term_id": 42,
                    "ref_protein_accession": "Q99999",
                    "distance": 0.15,
                    "vote_count": 3,
                },
            ],
        }
        op.execute(session, payload, emit=lambda *_args, **_kw: None)
        # First execute is the bulk insert; its second positional arg
        # is the list of row dicts.
        first_call = session.execute.call_args_list[0]
        rows = first_call.args[1]
        assert len(rows) == 1
        assert "features" in rows[0]
        assert rows[0]["features"]["distance"] == 0.15
        assert rows[0]["features"]["vote_count"] == 3


# ---------------------------------------------------------------------------
# Integration round-trip (--with-postgres)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dual_write_roundtrip_features_blob_matches_typed(postgres_url: str):
    """End-to-end: feed ``StorePredictionsOperation`` real rows, verify
    the persisted ``features`` JSONB blob matches every typed column
    value on the same row."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    import protea.infrastructure.orm.models  # noqa: F401
    from protea.core.operations.predict_go_terms import StorePredictionsOperation
    from protea.infrastructure.orm.base import Base
    from protea.infrastructure.orm.models.annotation.annotation_set import (
        AnnotationSet,
    )
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )
    from protea.infrastructure.orm.models.embedding.embedding_config import (
        EmbeddingConfig,
    )
    from protea.infrastructure.orm.models.embedding.go_prediction import (
        GOPrediction,
    )
    from protea.infrastructure.orm.models.embedding.go_prediction_features import (
        FEATURE_JSONB_KEYS,
    )
    from protea.infrastructure.orm.models.embedding.prediction_set import (
        PredictionSet,
    )
    from protea.infrastructure.orm.models.job import Job, JobStatus

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine, future=True) as session:
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
        snap = OntologySnapshot(obo_version="t3-1a-v1", obo_url="http://test")
        session.add(snap)
        session.flush()

        go_term = GOTerm(
            go_id="GO:0003674",
            name="molecular_function",
            aspect="F",
            ontology_snapshot_id=snap.id,
        )
        session.add(go_term)

        ann_set = AnnotationSet(ontology_snapshot_id=snap.id, source="test", source_version="v1")
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
        go_term_id = go_term.id
        session.commit()

    payload = {
        "parent_job_id": str(parent_id),
        "prediction_set_id": str(pred_set_id),
        "predictions": [
            {
                "protein_accession": "P_DUAL_1",
                "go_term_id": go_term_id,
                "ref_protein_accession": "Q_REF_1",
                "distance": 0.15,
                "qualifier": "enables",
                "evidence_code": "IDA",
                "identity_nw": 0.42,
                "alignment_length_nw": 180.0,
                "length_query": 200,
                "length_ref": 250,
                "vote_count": 4,
                "k_position": 1,
                "neighbor_min_distance": 0.05,
                "neighbor_vote_fraction": 0.8,
                "taxonomic_distance": 3,
                "taxonomic_relation": "sibling",
                "anc2vec_neighbor_cos": 0.77,
                "tax_voters_same_frac": 0.25,
                "emb_pca_query_0": 0.1,
                "emb_pca_query_15": -0.2,
            },
        ],
    }

    op = StorePredictionsOperation()
    with Session(engine, future=True) as session:
        op.execute(session, payload, emit=lambda *_a, **_k: None)
        session.commit()

    with Session(engine, future=True) as session:
        rows = session.query(GOPrediction).filter_by(prediction_set_id=pred_set_id).all()
        assert len(rows) == 1
        row = rows[0]

        # JSONB blob is populated.
        assert row.features is not None
        blob = row.features

        # Stable shape: every canonical key present.
        assert set(blob.keys()) == set(FEATURE_JSONB_KEYS)

        # Each typed column value matches the JSONB mirror.
        for key in FEATURE_JSONB_KEYS:
            typed = getattr(row, key)
            mirrored = blob[key]
            if typed is None:
                assert mirrored is None, f"{key}: typed is None but JSONB has {mirrored}"
            elif isinstance(typed, float):
                np.testing.assert_allclose(
                    mirrored, typed, atol=1e-9, err_msg=f"{key}: typed={typed} blob={mirrored}"
                )
            else:
                assert mirrored == typed, f"{key}: typed={typed} blob={mirrored}"
