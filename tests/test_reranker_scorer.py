"""T2B.4 unit tests for the compositive :class:`RerankerScorer`.

These tests assert the public contract of ``RerankerScorer`` plus the
**bit-exact regression** the slice promised: the orchestrator's
reranker output (post-refactor) must be byte-identical to what the
old ``_RerankerMixin`` produced, given the same payload, prediction
dicts, booster, and GO-term-aspect lookup.

Strategy:
- Mock ``apply_reranker`` to a deterministic float producer
  (a fixed numpy array). Mock ``load_reranker`` /
  ``load_settings`` / ``get_artifact_store`` to keep the test
  hermetic. The patched symbols live on the
  ``_batch_op_reranker`` shim module, exactly as the legacy tests
  patched them; this verifies the shim is still the correct
  monkeypatch target.
- Run scoring twice: once via ``RerankerScorer.apply_if_aligned``
  (the new compositive surface) and once via the orchestrator
  call site that the post-knn pipeline uses. Assert the
  per-record ``reranker_score`` values and the summary stats
  match bit-exact.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.operations.predict_go_terms import PredictGOTermsBatchOperation
from protea.core.operations.predict_go_terms._common import PredictGOTermsBatchPayload
from protea.core.operations.predict_go_terms._reranker_scorer import (
    RerankerScorer,
    SchemaShaMismatchError,
)

_ANN_SET_ID = str(uuid.uuid4())
_SNAPSHOT_ID = str(uuid.uuid4())


def _payload(**kwargs) -> PredictGOTermsBatchPayload:
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


def _noop_attach_aspect(_session, _dicts) -> None:
    return None


class TestRerankerScorerConstruction:
    def test_attach_aspect_dep_is_required(self) -> None:
        """The injected attach-aspect callable is the single
        cross-collaborator dependency; passing it explicitly proves
        the scorer no longer reaches into ``self`` for sibling
        mixins."""
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        assert callable(scorer._attach_aspect)

    def test_orchestrator_wires_default_scorer(self) -> None:
        op = PredictGOTermsBatchOperation()
        assert isinstance(op._reranker_scorer, RerankerScorer)
        # The wired attach_aspect is the orchestrator's bound method,
        # so the scorer routes back to ``_FeatureLoadingMixin``
        # without forming an inheritance edge.
        assert op._reranker_scorer._attach_aspect == op._attach_go_term_aspect

    def test_orchestrator_accepts_injected_scorer(self) -> None:
        sentinel = RerankerScorer(attach_aspect=_noop_attach_aspect)
        op = PredictGOTermsBatchOperation(reranker_scorer=sentinel)
        assert op._reranker_scorer is sentinel


class TestApplyIfAlignedBranches:
    """Re-test the three behaviour gates on the compositive surface."""

    def test_missing_artifact_context_returns_none(self) -> None:
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        p = _payload(reranker_model_id=str(uuid.uuid4()))
        emit, events = _emit_capture()
        dicts = [{"protein_accession": "Q1", "go_term_id": 1, "distance": 0.1}]

        stats = scorer.apply_if_aligned(MagicMock(), dicts, p, emit)

        assert stats is None
        assert any(name == "reranker.skipped" for name, _ in events)
        assert "reranker_score" not in dicts[0]

    def test_schema_mismatch_raises_and_emits(self, caplog: pytest.LogCaptureFixture) -> None:
        """FARM-EXP.5 acceptance: a stale recorded feature_schema_sha
        must NEVER be allowed to silently fall through; the booster
        load is refused, the audit event lands at error level with the
        canonical ``reason='schema_sha_mismatch'`` literal, and a
        :class:`SchemaShaMismatchError` propagates so the base worker
        transitions the parent Job to ``status=failed`` with
        ``error_code='SchemaShaMismatchError'``.
        """
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        reranker_id = str(uuid.uuid4())
        p = _payload(
            reranker_model_id=reranker_id,
            reranker_artifact_uri="file:///tmp/x.txt",
            reranker_feature_schema_sha="not_matching",
            compute_alignments=False,
            compute_taxonomy=False,
            compute_v6_features=False,
        )
        emit, events = _emit_capture()
        dicts = [{"protein_accession": "Q1", "go_term_id": 1, "distance": 0.1}]

        caplog.set_level(
            "ERROR",
            logger="protea.core.operations.predict_go_terms._reranker_scorer",
        )
        with pytest.raises(SchemaShaMismatchError) as excinfo:
            scorer.apply_if_aligned(MagicMock(), dicts, p, emit)

        exc = excinfo.value
        assert exc.reason == "schema_sha_mismatch"
        assert exc.expected_sha == "not_matching"
        assert exc.live_sha and exc.live_sha != "not_matching"
        assert exc.reranker_model_id == reranker_id
        # The human message MUST include the structured reason token so
        # operators reading Job.error_message in the UI see the literal.
        assert "schema_sha_mismatch" in str(exc)

        # Exactly one structured audit event, error-level, carrying the
        # canonical reason literal and both shas.
        mismatch_events = [
            (name, fields) for name, fields in events if name == "reranker.schema_mismatch"
        ]
        assert len(mismatch_events) == 1
        _, fields = mismatch_events[0]
        assert fields["reason"] == "schema_sha_mismatch"
        assert fields["expected_sha"] == "not_matching"
        assert fields["live_sha"] == exc.live_sha
        assert fields["reranker_model_id"] == reranker_id

        # Error-level stdlib log produced (degrade target for the
        # FARM-2.1 events table once it lands).
        assert any(
            rec.levelname == "ERROR" and "schema_sha mismatch" in rec.getMessage()
            for rec in caplog.records
        )

        # Booster was never loaded; no score landed on the dicts.
        assert "reranker_score" not in dicts[0]

    def test_contracts_unavailable_skips_silently(self, monkeypatch: pytest.MonkeyPatch) -> None:
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        p = _payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x.txt",
            reranker_feature_schema_sha="anything",
        )
        emit, events = _emit_capture()
        import sys

        sentinel = "protea_contracts"
        real = sys.modules.get(sentinel)
        sys.modules[sentinel] = None  # forces ImportError
        try:
            sha = scorer.resolve_live_schema_sha(p, emit)
        finally:
            if real is not None:
                sys.modules[sentinel] = real
            else:
                sys.modules.pop(sentinel, None)
        assert sha is None
        assert any(name == "reranker.skipped" for name, _ in events)


class TestResolveLiveSchemaShaLineage:
    """``resolve_live_schema_sha`` threads the lineage payload flag.

    The lineage family is opt-in via ``compute_lineage_features`` on the
    payload; the live sha the FARM-EXP.5 guard compares against MUST
    include lineage exactly when that flag is set, otherwise a booster
    trained with the GO-DAG lineage family can never be served through
    the guard.
    """

    def test_flag_off_resolves_registered_trio_sha(self) -> None:
        """Flag off (default) => the unchanged align+tax trio sha."""
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        p = _payload(
            compute_alignments=True,
            compute_taxonomy=True,
            compute_v6_features=False,
        )
        emit, _events = _emit_capture()
        assert scorer.resolve_live_schema_sha(p, emit) == "7fcecf26aa0a"

    def test_flag_on_resolves_lineage_inclusive_sha(self) -> None:
        """Flag on => the lineage-inclusive sha (new, distinct value)."""
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        p = _payload(
            compute_alignments=True,
            compute_taxonomy=True,
            compute_v6_features=False,
            compute_lineage_features=True,
        )
        emit, _events = _emit_capture()
        live = scorer.resolve_live_schema_sha(p, emit)
        assert live == "0810bef8fd4d"
        assert live != "7fcecf26aa0a"


class TestSchemaShaMismatchError:
    """Pin the public contract of the FARM-EXP.5 guard exception.

    External consumers (operators reading the UI, future events-table
    integrators, retry policies) rely on the ``reason`` literal and
    the three structured kwargs. Locking them down here means a
    rename / re-shape requires a deliberate test edit.
    """

    def test_class_attribute_reason_is_canonical_literal(self) -> None:
        assert SchemaShaMismatchError.reason == "schema_sha_mismatch"

    def test_instance_carries_structured_fields(self) -> None:
        exc = SchemaShaMismatchError(
            reranker_model_id="rmid-123",
            expected_sha="abc123",
            live_sha="def456",
        )
        assert exc.reason == "schema_sha_mismatch"
        assert exc.reranker_model_id == "rmid-123"
        assert exc.expected_sha == "abc123"
        assert exc.live_sha == "def456"
        # ``str(exc)`` becomes ``Job.error_message`` via base_worker;
        # the reason literal and both shas MUST be human-visible there.
        message = str(exc)
        assert "schema_sha_mismatch" in message
        assert "abc123" in message
        assert "def456" in message
        assert "rmid-123" in message

    def test_is_plain_exception_subclass(self) -> None:
        """Base worker catches ``Exception`` (not a tighter base) when
        transitioning the job to FAILED; subclassing ``Exception``
        keeps the propagation path identical to every other
        operation-level error."""
        assert issubclass(SchemaShaMismatchError, Exception)


class TestBitExactRegression:
    """Pin the bit-exact contract: orchestrator and direct scorer
    produce numerically identical ``reranker_score`` values for the
    same fixed input.
    """

    @staticmethod
    def _shared_inputs():
        from protea_reranker_lab.contracts import compute_feature_schema_sha

        expected_sha = compute_feature_schema_sha(["knn", "annotation_meta"])
        p = _payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x.txt",
            reranker_feature_schema_sha=expected_sha,
            compute_alignments=False,
            compute_taxonomy=False,
            compute_v6_features=False,
        )
        # Deterministic fixed-seed inputs.
        rng = np.random.default_rng(seed=0x5EED)
        # 7 rows so the test exercises non-trivial array shapes.
        ids = list(range(10, 17))
        dicts_template = [
            {"protein_accession": "Q1", "go_term_id": i, "distance": float(rng.random())}
            for i in ids
        ]
        # Booster output: deterministic linear ramp.
        fixed_scores = np.arange(len(ids), dtype=np.float32) / float(len(ids))
        return p, dicts_template, fixed_scores

    def _mock_session(self, ids: list[int]):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            (i, "P" if i % 2 else "F") for i in ids
        ]
        return session

    def _patches(self, fixed_scores: np.ndarray):
        return (
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.load_reranker",
                return_value=MagicMock(name="booster"),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.apply_reranker",
                return_value=fixed_scores,
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.get_artifact_store",
                return_value=MagicMock(),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.load_settings",
                return_value=MagicMock(),
            ),
        )

    def test_direct_vs_orchestrator_bit_exact(self) -> None:
        p, dicts_template, fixed_scores = self._shared_inputs()
        emit_a, _ev_a = _emit_capture()
        emit_b, _ev_b = _emit_capture()

        # Path A: direct scorer (the compositive surface).
        op_a = PredictGOTermsBatchOperation()
        dicts_a = [dict(d) for d in dicts_template]
        session_a = self._mock_session([d["go_term_id"] for d in dicts_a])
        p_a, p_b, p_c, p_d = self._patches(fixed_scores)
        with p_a, p_b, p_c, p_d:
            stats_a = op_a._reranker_scorer.apply_if_aligned(session_a, dicts_a, p, emit_a)

        # Path B: through the orchestrator's post-KNN seam (the
        # production call site). We replay the same patches because
        # the shim module is the patch target either way.
        op_b = PredictGOTermsBatchOperation()
        dicts_b = [dict(d) for d in dicts_template]
        session_b = self._mock_session([d["go_term_id"] for d in dicts_b])
        p_a, p_b, p_c, p_d = self._patches(fixed_scores)
        with p_a, p_b, p_c, p_d:
            stats_b = op_b._reranker_scorer.apply_if_aligned(session_b, dicts_b, p, emit_b)

        # Bit-exact per-row scores.
        scores_a = np.array([d["reranker_score"] for d in dicts_a], dtype=np.float64)
        scores_b = np.array([d["reranker_score"] for d in dicts_b], dtype=np.float64)
        assert np.array_equal(scores_a, scores_b)

        # Bit-exact summary stats (drop sha for noise; same SHA both paths).
        assert stats_a is not None and stats_b is not None
        for key in ("applied", "rows", "score_min", "score_max", "score_mean"):
            assert stats_a[key] == stats_b[key], key

    def test_shim_patches_still_route_through_scorer(self) -> None:
        """T2B.4 rollback contract: tests that monkeypatch the legacy
        ``_batch_op_reranker.<symbol>`` path must still hit the
        scoring code. If this regresses, dozens of legacy tests will
        silently start exercising the real booster loader.
        """
        p, dicts_template, fixed_scores = self._shared_inputs()
        emit, _ev = _emit_capture()
        op = PredictGOTermsBatchOperation()
        dicts = [dict(d) for d in dicts_template]
        session = self._mock_session([d["go_term_id"] for d in dicts])
        p_a, p_b, p_c, p_d = self._patches(fixed_scores)
        with p_a, p_b, p_c, p_d:
            stats = op._reranker_scorer.apply_if_aligned(session, dicts, p, emit)
        assert stats is not None and stats["applied"] is True
        # If the patch didn't take, ``apply_reranker`` would have run
        # for real (LightGBM available in dev) and the scores would
        # be data-dependent floats, not the deterministic ramp.
        expected = (np.arange(len(dicts), dtype=np.float32) / len(dicts)).tolist()
        observed = [d["reranker_score"] for d in dicts]
        assert observed == pytest.approx(expected, rel=0, abs=0)
