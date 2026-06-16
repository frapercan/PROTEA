"""Unit tests for the INT-5 per-category (NK / LK / PK) machinery.

Two surfaces are covered:

* :func:`attach_query_category` - derives the CAFA category for every
  ``(protein, candidate aspect)`` from the protein's pre-cutoff EXPERIMENTAL
  known terms across aspects.
* :meth:`RerankerScorer.apply` per-category dispatch - splits candidates by
  category and applies the matching booster; falls back cleanly to the
  single-booster path when the per-category binding is absent.

Both are unit-shaped: the DB-bound annotation + aspect loaders are stubbed so
no Postgres is needed.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.operations.predict_go_terms import _category_dispatch as cd
from protea.core.operations.predict_go_terms._category_dispatch import (
    CATEGORY_LK,
    CATEGORY_NK,
    CATEGORY_PK,
    attach_query_category,
)
from protea.core.operations.predict_go_terms._common import PredictGOTermsBatchPayload
from protea.core.operations.predict_go_terms._reranker_scorer import (
    RerankerScorer,
    SchemaShaMismatchError,
)

_ANN_SET_ID = str(uuid.uuid4())
_SNAPSHOT_ID = str(uuid.uuid4())

# Synthetic GO-term-id -> aspect catalogue used across the tests.
#   1 = P (BPO), 2 = F (MFO), 3 = C (CCO)
_ASPECT_BY_ID = {1: "P", 2: "F", 3: "C"}


def _payload(**kwargs: Any) -> PredictGOTermsBatchPayload:
    defaults = dict(
        embedding_config_id=str(uuid.uuid4()),
        annotation_set_id=_ANN_SET_ID,
        ontology_snapshot_id=_SNAPSHOT_ID,
        prediction_set_id=str(uuid.uuid4()),
        parent_job_id=str(uuid.uuid4()),
        query_accessions=["Q1"],
        limit_per_entry=2,
    )
    defaults.update(kwargs)
    return PredictGOTermsBatchPayload.model_validate(defaults)


def _emit_capture():
    events: list[tuple[str, dict]] = []

    def _emit(name, _msg, fields, _sev):
        events.append((name, fields or {}))

    return _emit, events


class _FakeOp:
    """Stand-in for the batch op exposing only ``_load_annotations_for``.

    ``own_exp`` maps accession -> list of (go_term_id, evidence_code). Only
    experimental evidence becomes "known"; the loader hands back the raw rows
    the way :meth:`_FeatureLoadingMixin._load_annotations_for` would.
    """

    def __init__(self, own: dict[str, list[tuple[int, str]]]) -> None:
        self._own = own

    def _load_annotations_for(self, _session, _ann_set_id, accessions):
        return {
            acc: [{"go_term_id": gid, "evidence_code": ev} for gid, ev in self._own.get(acc, [])]
            for acc in accessions
        }


def _patch_aspect_loader():
    """Patch the module-level aspect loader to the synthetic catalogue."""
    return patch.object(
        cd,
        "_load_known_aspects",
        side_effect=lambda _s, ids: {i: _ASPECT_BY_ID.get(i, "") for i in ids},
    )


class TestAttachQueryCategory:
    def test_no_known_terms_is_nk(self) -> None:
        """A protein with no t0 experimental annotation is NK in every aspect."""
        op = _FakeOp({})  # no known terms at all
        dicts = [
            {"protein_accession": "Q1", "go_term_id": 1},  # P candidate
            {"protein_accession": "Q1", "go_term_id": 2},  # F candidate
        ]
        with _patch_aspect_loader():
            hist = attach_query_category(op, MagicMock(), uuid.UUID(_ANN_SET_ID), dicts)
        assert [d["category"] for d in dicts] == [CATEGORY_NK, CATEGORY_NK]
        assert hist == {CATEGORY_NK: 2, CATEGORY_LK: 0, CATEGORY_PK: 0}

    def test_known_in_same_aspect_is_pk_other_aspect_is_lk(self) -> None:
        """Known term in aspect F: F candidates are PK, P / C candidates are LK."""
        op = _FakeOp({"Q1": [(2, "EXP")]})  # experimental known term in aspect F
        dicts = [
            {"protein_accession": "Q1", "go_term_id": 1},  # P candidate -> LK
            {"protein_accession": "Q1", "go_term_id": 2},  # F candidate -> PK
            {"protein_accession": "Q1", "go_term_id": 3},  # C candidate -> LK
        ]
        with _patch_aspect_loader():
            attach_query_category(op, MagicMock(), uuid.UUID(_ANN_SET_ID), dicts)
        assert [d["category"] for d in dicts] == [CATEGORY_LK, CATEGORY_PK, CATEGORY_LK]

    def test_non_experimental_known_does_not_count(self) -> None:
        """Only EXPERIMENTAL evidence makes a protein 'known' (CAFA partition)."""
        op = _FakeOp({"Q1": [(2, "IEA")]})  # electronic evidence only
        dicts = [{"protein_accession": "Q1", "go_term_id": 1}]
        with _patch_aspect_loader():
            attach_query_category(op, MagicMock(), uuid.UUID(_ANN_SET_ID), dicts)
        assert dicts[0]["category"] == CATEGORY_NK

    def test_prefers_already_stamped_aspect(self) -> None:
        """Candidate aspect already on the dict is reused without a DB lookup."""
        op = _FakeOp({"Q1": [(2, "EXP")]})
        dicts = [{"protein_accession": "Q1", "go_term_id": 99, "aspect": "F"}]
        # _load_known_aspects is called only for the known terms, never for 99.
        seen_ids: list[set[int]] = []

        def _record(_s, ids):
            seen_ids.append(set(ids))
            return {i: _ASPECT_BY_ID.get(i, "") for i in ids}

        with patch.object(cd, "_load_known_aspects", side_effect=_record):
            attach_query_category(op, MagicMock(), uuid.UUID(_ANN_SET_ID), dicts)
        assert dicts[0]["category"] == CATEGORY_PK
        assert all(99 not in ids for ids in seen_ids)

    def test_mixed_proteins(self) -> None:
        """NK / LK / PK can co-exist in one batch across proteins."""
        op = _FakeOp({"K1": [(2, "EXP")]})  # K1 known in F; N1 unknown
        dicts = [
            {"protein_accession": "N1", "go_term_id": 2},  # NK (N1 has no known)
            {"protein_accession": "K1", "go_term_id": 2},  # PK (known in F)
            {"protein_accession": "K1", "go_term_id": 1},  # LK (known F, cand P)
        ]
        with _patch_aspect_loader():
            hist = attach_query_category(op, MagicMock(), uuid.UUID(_ANN_SET_ID), dicts)
        assert [d["category"] for d in dicts] == [CATEGORY_NK, CATEGORY_PK, CATEGORY_LK]
        assert hist == {CATEGORY_NK: 1, CATEGORY_LK: 1, CATEGORY_PK: 1}


def _attach_category_stub(by_acc_aspect: dict[tuple[str, int], str]):
    """Return an attach_category callable that stamps a fixed category map."""

    def _attach(_session, _ann_set_id, dicts):
        hist = {CATEGORY_NK: 0, CATEGORY_LK: 0, CATEGORY_PK: 0}
        for rec in dicts:
            cat = by_acc_aspect[(rec["protein_accession"], rec["go_term_id"])]
            rec["category"] = cat
            hist[cat] += 1
        return hist

    return _attach


class TestPerCategoryDispatch:
    """The scorer routes each category subset to its own booster."""

    _CAT_KWARGS = dict(
        reranker_nk_artifact_uri="file:///tmp/nk.txt",
        reranker_nk_feature_schema_sha="sha",
        reranker_lk_artifact_uri="file:///tmp/lk.txt",
        reranker_lk_feature_schema_sha="sha",
        reranker_pk_artifact_uri="file:///tmp/pk.txt",
        reranker_pk_feature_schema_sha="sha",
        compute_alignments=False,
        compute_taxonomy=False,
        compute_v6_features=False,
    )

    def _scorer(self, by_key):
        return RerankerScorer(
            attach_aspect=lambda _s, _d: None,
            attach_category=_attach_category_stub(by_key),
        )

    def test_three_boosters_score_their_own_category(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """NK booster scores NK rows, LK booster LK rows, PK booster PK rows."""
        dicts = [
            {"protein_accession": "P", "go_term_id": 10, "distance": 0.1},  # NK
            {"protein_accession": "P", "go_term_id": 11, "distance": 0.2},  # LK
            {"protein_accession": "P", "go_term_id": 12, "distance": 0.3},  # PK
        ]
        by_key = {
            ("P", 10): CATEGORY_NK,
            ("P", 11): CATEGORY_LK,
            ("P", 12): CATEGORY_PK,
        }
        # Each "booster" is identified by its artifact uri; the fake scorer
        # records which uri scored which rows and writes a category-coded score.
        scorer = self._scorer(by_key)
        marker = {"file:///tmp/nk.txt": 1.0, "file:///tmp/lk.txt": 2.0, "file:///tmp/pk.txt": 3.0}

        def _fake_score_with(self, subset, *, artifact_uri, feature_schema_sha):
            val = marker[artifact_uri]
            for rec in subset:
                rec["reranker_score"] = val
            return np.full(len(subset), val, dtype=np.float64)

        monkeypatch.setattr(RerankerScorer, "_score_with", _fake_score_with)
        monkeypatch.setattr(RerankerScorer, "resolve_live_schema_sha", lambda self, p, e: "sha")

        emit, events = _emit_capture()
        p = _payload(**self._CAT_KWARGS)
        stats = scorer.apply(MagicMock(), dicts, p, emit)

        assert dicts[0]["reranker_score"] == 1.0  # NK booster
        assert dicts[1]["reranker_score"] == 2.0  # LK booster
        assert dicts[2]["reranker_score"] == 3.0  # PK booster
        assert stats is not None and stats["per_category"] is True
        assert stats["rows"] == 3
        assert stats["by_category"] == {CATEGORY_NK: 1, CATEGORY_LK: 1, CATEGORY_PK: 1}
        assert any(name == "reranker.per_category_applied" for name, _ in events)

    def test_missing_binding_falls_back_to_single_booster(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No per-category fields -> single-booster ``apply_if_aligned`` path."""
        scorer = RerankerScorer(attach_aspect=lambda _s, _d: None)
        called: dict[str, bool] = {}

        def _fake_single(self, session, dicts, p, emit):
            called["single"] = True
            return {"applied": True, "rows": len(dicts)}

        monkeypatch.setattr(RerankerScorer, "apply_if_aligned", _fake_single)
        emit, _ = _emit_capture()
        p = _payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/single.txt",
            reranker_feature_schema_sha="sha",
        )
        stats = scorer.apply(MagicMock(), [{"protein_accession": "P", "go_term_id": 1}], p, emit)
        assert called.get("single") is True
        assert stats == {"applied": True, "rows": 1}

    def test_partial_binding_falls_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Only NK + LK set (PK missing) -> not per-category; single path."""
        scorer = RerankerScorer(attach_aspect=lambda _s, _d: None)
        called: dict[str, bool] = {}
        monkeypatch.setattr(
            RerankerScorer,
            "apply_if_aligned",
            lambda self, s, d, p, e: called.setdefault("single", True),
        )
        emit, _ = _emit_capture()
        p = _payload(
            reranker_nk_artifact_uri="file:///tmp/nk.txt",
            reranker_nk_feature_schema_sha="sha",
            reranker_lk_artifact_uri="file:///tmp/lk.txt",
            reranker_lk_feature_schema_sha="sha",
        )
        scorer.apply(MagicMock(), [{"protein_accession": "P", "go_term_id": 1}], p, emit)
        assert called.get("single") is True

    def test_schema_sha_drift_on_category_booster_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A per-category booster whose recorded sha drifted is a hard failure."""
        scorer = self._scorer({("P", 1): CATEGORY_NK})
        monkeypatch.setattr(
            RerankerScorer, "resolve_live_schema_sha", lambda self, p, e: "live_sha"
        )
        emit, events = _emit_capture()
        # NK booster carries the stale "sha"; live is "live_sha".
        p = _payload(**self._CAT_KWARGS)
        with pytest.raises(SchemaShaMismatchError):
            scorer.apply(MagicMock(), [{"protein_accession": "P", "go_term_id": 1}], p, emit)
        assert any(name == "reranker.schema_mismatch" for name, _ in events)

    def test_empty_category_subset_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A category with no candidates does not invoke its booster."""
        dicts = [{"protein_accession": "P", "go_term_id": 10, "distance": 0.1}]  # NK only
        scorer = self._scorer({("P", 10): CATEGORY_NK})
        loaded: list[str] = []

        def _fake_score_with(self, subset, *, artifact_uri, feature_schema_sha):
            loaded.append(artifact_uri)
            for rec in subset:
                rec["reranker_score"] = 1.0
            return np.ones(len(subset))

        monkeypatch.setattr(RerankerScorer, "_score_with", _fake_score_with)
        monkeypatch.setattr(RerankerScorer, "resolve_live_schema_sha", lambda self, p, e: "sha")
        emit, _ = _emit_capture()
        p = _payload(**self._CAT_KWARGS)
        scorer.apply(MagicMock(), dicts, p, emit)
        # Only the NK booster loaded; LK + PK never touched.
        assert loaded == ["file:///tmp/nk.txt"]
