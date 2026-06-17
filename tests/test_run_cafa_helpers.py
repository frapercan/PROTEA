"""Unit tests for ``protea.core.operations._run_cafa_helpers``.

Pure helpers extracted from the CAFA evaluator. The anc2vec scoring
helpers need a stubbed ``get_anc2vec_index`` so they don't load the
real Anc2Vec artifact.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from protea.core.operations import _run_cafa_helpers as helpers
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction


class TestEvalArtifactKey:
    def test_strips_leading_slash_from_relpath(self) -> None:
        rid = uuid.uuid4()
        assert helpers.eval_artifact_key(rid, "/sub/file.tsv") == (
            f"eval_artifacts/{rid}/sub/file.tsv"
        )

    def test_preserves_relative_path(self) -> None:
        rid = uuid.uuid4()
        assert helpers.eval_artifact_key(rid, "sub/file.tsv") == (
            f"eval_artifacts/{rid}/sub/file.tsv"
        )

    def test_empty_relpath_returns_trailing_slash(self) -> None:
        rid = uuid.uuid4()
        assert helpers.eval_artifact_key(rid, "") == f"eval_artifacts/{rid}/"


class TestNamespaceLabelMaps:
    def test_ns_short_contains_three_cafa_codes(self) -> None:
        # BPO / MFO / CCO are the CAFA aspect codes used by cafaeval.
        assert helpers._NS_SHORT == {"BPO", "MFO", "CCO"}

    def test_ns_labels_maps_obo_names_to_cafa_codes(self) -> None:
        assert helpers._NS_LABELS["biological_process"] == "BPO"
        assert helpers._NS_LABELS["molecular_function"] == "MFO"
        assert helpers._NS_LABELS["cellular_component"] == "CCO"


class TestRecordFromPred:
    def _make_pred(self, **overrides: Any) -> GOPrediction:
        """Construct a GOPrediction-like object with all expected attributes."""
        base: dict[str, Any] = {
            "protein_accession": "P12345",
            "qualifier": "enables",
            "evidence_code": "IBA",
            "taxonomic_relation": "same_species",
        }
        for col in helpers._NUMERIC_ORM_COLS:
            base[col] = 0.5
        base.update(overrides)
        # SimpleNamespace duck-types as GOPrediction for the helper's
        # getattr-only access pattern; cast keeps mypy happy.
        return cast(GOPrediction, SimpleNamespace(**base))

    def test_returns_core_string_fields(self) -> None:
        pred = self._make_pred()
        rec = helpers._record_from_pred(pred, "GO:0001234", aspect="F")
        assert rec["protein_accession"] == "P12345"
        assert rec["go_id"] == "GO:0001234"
        assert rec["aspect"] == "F"
        assert rec["qualifier"] == "enables"
        assert rec["evidence_code"] == "IBA"
        assert rec["taxonomic_relation"] == "same_species"

    def test_default_aspect_is_empty_string(self) -> None:
        pred = self._make_pred()
        rec = helpers._record_from_pred(pred, "GO:0001234")
        assert rec["aspect"] == ""

    def test_includes_every_numeric_column(self) -> None:
        pred = self._make_pred()
        rec = helpers._record_from_pred(pred, "GO:0001234")
        for col in helpers._NUMERIC_ORM_COLS:
            assert col in rec
            assert rec[col] == 0.5

    def test_none_qualifier_falls_back_to_empty_string(self) -> None:
        pred = self._make_pred(qualifier=None, evidence_code=None, taxonomic_relation=None)
        rec = helpers._record_from_pred(pred, "GO:X")
        assert rec["qualifier"] == ""
        assert rec["evidence_code"] == ""
        assert rec["taxonomic_relation"] == ""

    def test_missing_numeric_column_yields_none(self) -> None:
        # SimpleNamespace without the column should produce None via getattr default.
        pred = cast(
            GOPrediction,
            SimpleNamespace(
                protein_accession="P1",
                qualifier="",
                evidence_code="",
                taxonomic_relation="",
            ),
        )
        rec = helpers._record_from_pred(pred, "GO:Y")
        # Every numeric column resolves to None.
        for col in helpers._NUMERIC_ORM_COLS:
            assert rec[col] is None

    def test_lafa_families_read_from_jsonb_blob(self) -> None:
        # The LAFA families have no typed column; they are read back from
        # ``pred.features`` (the JSONB blob the store wrote).
        blob = {
            "classifier_score": 0.73,
            "classifier_present": 1.0,
            "self_prior_score": 1.0,
            "association_total": 0.4,
            "association_cross": 0.1,
            "association_present": 1.0,
        }
        pred = self._make_pred(features=blob)
        rec = helpers._record_from_pred(pred, "GO:1")
        for col in helpers._LAFA_JSONB_FEATURE_COLS:
            assert rec[col] == blob[col], f"mismatch on {col}"

    def test_lafa_families_default_to_none_without_blob(self) -> None:
        # Legacy / default-run rows: no blob (or no LAFA keys) -> None,
        # which LightGBM routes through its native missing branch.
        pred = self._make_pred(features=None)
        rec = helpers._record_from_pred(pred, "GO:1")
        for col in helpers._LAFA_JSONB_FEATURE_COLS:
            assert rec[col] is None

    def test_lafa_families_partial_blob_fills_missing_with_none(self) -> None:
        # A self_prior-only run: only that key is in the blob; the rest None.
        pred = self._make_pred(features={"self_prior_score": 1.0})
        rec = helpers._record_from_pred(pred, "GO:1")
        assert rec["self_prior_score"] == 1.0
        assert rec["classifier_score"] is None
        assert rec["association_total"] is None


class TestLafaPersistEvalRoundTrip:
    """End-to-end (in-memory) parity: a prediction dict carrying the LAFA
    families round-trips store -> ``GOPrediction.features`` -> the eval
    reranker record, so the per-category booster sees the real values it
    trained on (not zero / missing).
    """

    def test_lafa_values_survive_store_to_eval_record(self) -> None:
        from protea.core.operations.predict_go_terms._common import (
            _row_from_prediction,
        )

        pred_dict = {
            "protein_accession": "P42",
            "go_term_id": 99,
            "ref_protein_accession": "Q7",
            "distance": 0.12,
            "qualifier": "enables",
            "evidence_code": "IBA",
            "classifier_score": 0.81,
            "classifier_present": 1.0,
            "self_prior_score": 1.0,
            "association_total": 0.55,
            "association_cross": 0.2,
            "association_present": 1.0,
        }
        # Half 1: persist -> the JSONB blob the store column receives.
        blob = _row_from_prediction(pred_dict, uuid.uuid4())["features"]

        # Reconstruct the GOPrediction the eval reads, carrying that blob.
        pred = cast(
            GOPrediction,
            SimpleNamespace(
                protein_accession="P42",
                qualifier="enables",
                evidence_code="IBA",
                taxonomic_relation="",
                features=blob,
            ),
        )
        # Half 2: read back -> the eval reranker record carries the families.
        rec = helpers._record_from_pred(pred, "GO:99", aspect="F")
        for col in helpers._LAFA_JSONB_FEATURE_COLS:
            assert rec[col] == pred_dict[col], f"round-trip lost {col}"

    def test_default_run_yields_missing_lafa_in_eval_record(self) -> None:
        from protea.core.operations.predict_go_terms._common import (
            _row_from_prediction,
        )

        pred_dict = {
            "protein_accession": "P42",
            "go_term_id": 99,
            "ref_protein_accession": "Q7",
            "distance": 0.12,
        }
        blob = _row_from_prediction(pred_dict, uuid.uuid4())["features"]
        pred = cast(
            GOPrediction,
            SimpleNamespace(
                protein_accession="P42",
                qualifier="",
                evidence_code="",
                taxonomic_relation="",
                features=blob,
            ),
        )
        rec = helpers._record_from_pred(pred, "GO:99")
        # No LAFA families were computed, so the eval record carries None
        # (LightGBM missing branch) for each.
        for col in helpers._LAFA_JSONB_FEATURE_COLS:
            assert rec[col] is None


def _patch_anc2vec(monkeypatch: pytest.MonkeyPatch, embeddings: dict[str, np.ndarray]) -> None:
    """Patch ``get_anc2vec_index`` to return a stub with a ``batch`` method."""

    def fake_batch(go_list):
        return np.array(
            [embeddings.get(g, np.zeros(4, dtype=np.float32)) for g in go_list],
            dtype=np.float32,
        )

    stub = MagicMock()
    stub.batch.side_effect = fake_batch
    monkeypatch.setattr(helpers, "get_anc2vec_index", lambda: stub)


class TestBuildAnc2vecLookup:
    def test_empty_df_returns_none(self) -> None:
        df = pd.DataFrame(columns=["go_id", "protein_accession"])
        assert helpers._build_anc2vec_lookup(df, {"P1": {"GO:1"}}) is None

    def test_empty_known_gos_returns_none(self) -> None:
        df = pd.DataFrame({"go_id": ["GO:1"], "protein_accession": ["P1"]})
        assert helpers._build_anc2vec_lookup(df, {}) is None

    def test_returns_go_list_and_normalised_matrix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        embeddings = {
            "GO:1": np.array([2.0, 0.0, 0.0, 0.0], dtype=np.float32),
            "GO:2": np.array([0.0, 3.0, 0.0, 0.0], dtype=np.float32),
        }
        _patch_anc2vec(monkeypatch, embeddings)
        df = pd.DataFrame({"go_id": ["GO:1"], "protein_accession": ["P1"]})
        out = helpers._build_anc2vec_lookup(df, {"P1": {"GO:2"}})
        assert out is not None
        go_list, idx_of_go, all_norm, has_emb_mask = out
        # Both GO ids land in the joint sorted list.
        assert go_list == ["GO:1", "GO:2"]
        assert idx_of_go == {"GO:1": 0, "GO:2": 1}
        # Embeddings are unit-normalised.
        np.testing.assert_allclose(all_norm[0], [1.0, 0.0, 0.0, 0.0])
        np.testing.assert_allclose(all_norm[1], [0.0, 1.0, 0.0, 0.0])
        assert has_emb_mask.tolist() == [True, True]

    def test_zero_norm_embedding_marked_unknown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # An all-zero embedding (missing in Anc2Vec) gets has_emb=False
        # and the row is zeroed (not NaN).
        _patch_anc2vec(monkeypatch, {})
        df = pd.DataFrame({"go_id": ["GO:99"], "protein_accession": ["P1"]})
        out = helpers._build_anc2vec_lookup(df, {"P1": {"GO:99"}})
        assert out is not None
        _, _, all_norm, has_emb_mask = out
        assert has_emb_mask.tolist() == [False]
        np.testing.assert_array_equal(all_norm[0], np.zeros(4, dtype=np.float32))


class TestPatchQueryKnownFeatures:
    def test_no_known_gos_leaves_df_untouched_when_lookup_returns_none(
        self,
    ) -> None:
        df = pd.DataFrame(columns=["go_id", "protein_accession"])
        # Empty df short-circuits via _build_anc2vec_lookup returning None.
        helpers._patch_query_known_features(df, {"P1": {"GO:1"}})
        assert df.empty

    def test_populates_expected_columns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        embeddings = {
            "GO:1": np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
            "GO:2": np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
            "GO:KNOWN": np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
        }
        _patch_anc2vec(monkeypatch, embeddings)
        df = pd.DataFrame(
            {
                "go_id": ["GO:1", "GO:2"],
                "protein_accession": ["P1", "P1"],
            }
        )
        with patch("pandas.Series.replace", lambda self, *a, **kw: self):
            helpers._patch_query_known_features(df, {"P1": {"GO:KNOWN"}})
        for col in (
            "anc2vec_query_known_cos",
            "anc2vec_query_known_maxcos",
            "anc2vec_query_known_count",
        ):
            assert col in df.columns
        # Count equals number of known GOs for the protein.
        assert df["anc2vec_query_known_count"].tolist() == [1.0, 1.0]
