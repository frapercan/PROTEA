"""Tests for the ``GOPrediction`` feature writer + JSONB helper.

Two tiers:

1. Pure-function tests of ``build_feature_jsonb`` and ``from_json`` — stable
   shape, identity-key exclusion, NaN-safety, round-trip equivalence. These
   helpers are retained for legacy blob rows; the live writer no longer calls
   them.
2. Writer-site + end-to-end integration through ``StorePredictionsOperation``,
   asserting that after the signal-store code-switch every feature (base +
   LAFA + IA) lands in a typed ``GOPrediction`` column and the redundant
   ``features`` JSONB blob is no longer written (gated by ``--with-postgres``
   for the DB round-trip).

The integration test is the load-bearing one: it catches any future writer
site that bypasses ``_row_from_prediction`` (the only path that builds the row).
"""

from __future__ import annotations

import math
import uuid
from unittest.mock import MagicMock

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

    def test_non_finite_floats_scrubbed_to_none(self) -> None:
        # The classifier path adds candidates whose KNN distance is NaN
        # (a deliberate "no KNN neighbor" marker); +/-inf can also slip in.
        # The typed float8 column keeps them, but the mirrored JSONB blob
        # must scrub them to None or Postgres rejects the JSON.
        row: dict[str, object] = {
            "prediction_set_id": uuid.uuid4(),
            "protein_accession": "P00001",
            "go_term_id": 1,
            "ref_protein_accession": "Q00001",
            "distance": float("nan"),
            "neighbor_distance_std": float("inf"),
            "neighbor_min_distance": float("-inf"),
            # Finite floats / ints / strings pass through unchanged.
            "neighbor_mean_distance": 0.12,
            "vote_count": 4,
            "qualifier": "enables",
        }
        blob = build_feature_jsonb(row)
        assert blob["distance"] is None
        assert blob["neighbor_distance_std"] is None
        assert blob["neighbor_min_distance"] is None
        # No non-finite float survives anywhere in the blob.
        for value in blob.values():
            assert not (isinstance(value, float) and not math.isfinite(value))
        # Finite / non-float values untouched.
        assert blob["neighbor_mean_distance"] == 0.12
        assert blob["vote_count"] == 4
        assert blob["qualifier"] == "enables"

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


class TestRowFromPredictionTypedWrite:
    """Signal-store code-switch: ``_row_from_prediction`` writes every feature
    to a typed column and no longer emits the redundant ``features`` JSONB blob.
    """

    def test_row_has_no_features_blob(self) -> None:
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
        assert "features" not in row
        # Typed columns carry the values.
        assert row["distance"] == 0.15
        assert row["vote_count"] == 3
        assert row["k_position"] == 1
        assert row["neighbor_min_distance"] == 0.05
        assert row["anc2vec_neighbor_cos"] == 0.8
        assert row["emb_pca_query_0"] == 0.1
        # Unset feature -> None typed column.
        assert row["alignment_score_nw"] is None

    def test_nan_cleaned_in_typed_columns(self) -> None:
        """NaN / inf are scrubbed by ``_clean_float`` before the row lands in
        Postgres. (The typed float8 columns could hold NaN, but ``_clean_float``
        keeps them out so LightGBM's native missing branch is used downstream.)
        """
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
        assert "features" not in row
        # No non-finite float survives anywhere in the row.
        for value in row.values():
            assert not (isinstance(value, float) and not math.isfinite(value))


class TestLafaFeaturePersistence:
    """LAFA per-category families (classifier / self_prior / association) + IA
    now write to typed ``GOPrediction`` columns (signal-store code-switch), and
    ONLY when the prediction dict carries them (the matching compute flag was on
    at predict time). IA maps the predict-dict ``IA`` key to the typed ``ia``
    column.
    """

    def test_lafa_columns_persisted_when_present(self) -> None:
        from protea.core.operations.predict_go_terms._common import (
            _LAFA_TYPED_FEATURE_KEYS,
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
            "IA": 7.5,
        }
        row = _row_from_prediction(pred, pred_set_id)
        for key in _LAFA_TYPED_FEATURE_KEYS:
            assert row[key] == pred[key], f"mismatch on {key}"
        assert row["ia"] == 7.5
        assert "features" not in row

    def test_default_run_leaves_lafa_columns_none(self) -> None:
        """Flags off: the dict carries no LAFA/IA keys, so those typed columns
        are NULL (LightGBM missing branch), the same as legacy rows."""
        from protea.core.operations.predict_go_terms._common import (
            _LAFA_TYPED_FEATURE_KEYS,
        )

        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P12345",
            "go_term_id": 42,
            "ref_protein_accession": "Q99999",
            "distance": 0.15,
            "vote_count": 3,
        }
        row = _row_from_prediction(pred, pred_set_id)
        for key in _LAFA_TYPED_FEATURE_KEYS:
            assert row[key] is None
        assert row["ia"] is None

    def test_lafa_columns_partial_presence(self) -> None:
        """Only the families whose flag was on are set; the rest stay None
        (a self_prior-only run carries just ``self_prior_score``)."""
        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P1",
            "go_term_id": 7,
            "ref_protein_accession": "Q1",
            "distance": 0.2,
            "self_prior_score": 1.0,
        }
        row = _row_from_prediction(pred, pred_set_id)
        assert row["self_prior_score"] == 1.0
        assert row["classifier_score"] is None
        assert row["association_total"] is None
        assert row["ia"] is None

    def test_lafa_nan_cleaned(self) -> None:
        """Non-finite LAFA / IA values are scrubbed to ``None`` by
        ``_clean_float`` before the row is inserted."""
        pred_set_id = uuid.uuid4()
        pred = {
            "protein_accession": "P1",
            "go_term_id": 7,
            "ref_protein_accession": "Q1",
            "distance": 0.2,
            "association_total": float("nan"),
            "IA": float("inf"),
        }
        row = _row_from_prediction(pred, pred_set_id)
        assert row["association_total"] is None
        assert row["ia"] is None


# ---------------------------------------------------------------------------
# Unit test of StorePredictionsOperation hand-off (no DB)
# ---------------------------------------------------------------------------


class TestStorePredictionsTypedWriteSurface:
    """At the ORM-call level, the rows passed to the bulk insert carry the
    typed feature columns and no longer carry a ``features`` JSONB blob.
    Catches regressions where a future writer bypasses ``_row_from_prediction``.
    """

    def test_bulk_insert_rows_carry_typed_columns(self) -> None:
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
        assert "features" not in rows[0]
        assert rows[0]["distance"] == 0.15
        assert rows[0]["vote_count"] == 3


# ---------------------------------------------------------------------------
# Integration round-trip (--with-postgres)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_typed_write_roundtrip_no_features_blob(postgres_url: str):
    """End-to-end: feed ``StorePredictionsOperation`` real rows, verify the
    feature values (base + LAFA + IA) land in typed columns and the ``features``
    JSONB blob is NOT written (signal-store code-switch)."""
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
                # LAFA + IA now land in typed columns.
                "classifier_score": 0.73,
                "classifier_present": 1.0,
                "self_prior_score": 1.0,
                "association_total": 0.4,
                "association_cross": 0.1,
                "association_present": 1.0,
                "IA": 7.5,
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

        # Signal-store code-switch: the redundant JSONB blob is NOT written.
        assert row.features is None

        # Base features landed in their typed columns.
        assert row.distance == pytest.approx(0.15)
        assert row.identity_nw == pytest.approx(0.42)
        assert row.vote_count == 4
        assert row.taxonomic_distance == 3
        assert row.anc2vec_neighbor_cos == pytest.approx(0.77)
        assert row.emb_pca_query_0 == pytest.approx(0.1)
        assert row.emb_pca_query_15 == pytest.approx(-0.2)

        # LAFA families landed in their typed columns.
        assert row.classifier_score == pytest.approx(0.73)
        assert row.classifier_present == pytest.approx(1.0)
        assert row.self_prior_score == pytest.approx(1.0)
        assert row.association_total == pytest.approx(0.4)
        assert row.association_cross == pytest.approx(0.1)
        assert row.association_present == pytest.approx(1.0)

        # IA landed in the typed ``ia`` column (predict-dict ``IA`` key).
        assert row.ia == pytest.approx(7.5)
