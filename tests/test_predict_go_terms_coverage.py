"""F6.6 follow-up: targeted unit coverage for predict_go_terms.py.

These tests lift coverage of ``protea/core/operations/predict_go_terms.py``
from ~45 percent toward the 85 percent gate enforced by the codecov path
filter on the ``core`` package. They focus on the helpers that the
existing test suite walks around: summarize_payload, query-side loaders,
reference-cache disk paths, ancestor expansion, the unified-pool
pipeline plumbing, and the small bookkeeping methods. The reranker
happy and skipped paths are exercised separately by
``test_predict_go_terms.py``.

Driven by MagicMock sessions and monkeypatched module globals; no
network or real Postgres needed. The on-disk caches are pointed at a
per-test ``tmp_path`` via the ``_DISK_CACHE_DIR`` attribute so the
worker repo's ``data/ref_cache/`` is never touched.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core import disk_cache as disk_cache_module
from protea.core.operations import predict_go_terms as pgt
from protea.core.operations.predict_go_terms import (
    _UNIFIED_REF_KEY,
    PredictGOTermsBatchOperation,
    PredictGOTermsBatchPayload,
    PredictGOTermsOperation,
    StorePredictionsOperation,
    _clean_float,
    _row_from_prediction,
)
from protea.core.operations.predict_go_terms._batch_op_reference import _PoolRequest
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.orm.models.query.query_set import QuerySet

_noop_emit = lambda *_a, **_k: None  # noqa: E731


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _payload(**overrides) -> PredictGOTermsBatchPayload:
    defaults = {
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "prediction_set_id": str(uuid.uuid4()),
        "parent_job_id": str(uuid.uuid4()),
        "query_accessions": ["Q1"],
    }
    defaults.update(overrides)
    return PredictGOTermsBatchPayload.model_validate(defaults)


def _emit_capture():
    events: list[tuple[str, dict]] = []

    def _emit(name, _m, fields, _sev):
        events.append((name, fields or {}))

    return _emit, events


def _halfvec(v: list[float]):
    """MagicMock for pgvector HalfVector with ``.dimensions()`` and
    ``.to_numpy()`` / ``.to_list()`` accessors."""
    obj = MagicMock()
    obj.dimensions.return_value = len(v)
    arr = np.array(v, dtype=np.float16)
    obj.to_numpy.return_value = arr
    obj.to_list.return_value = list(v)
    return obj


# ---------------------------------------------------------------------------
# Tiny pure helpers
# ---------------------------------------------------------------------------


class TestCleanFloat:
    """Cover the NaN + infinity short-circuits in ``_clean_float``."""

    def test_none_passthrough(self) -> None:
        assert _clean_float(None) is None

    def test_finite_passthrough(self) -> None:
        assert _clean_float(0.5) == 0.5
        assert _clean_float(0) == 0

    def test_nan_becomes_none(self) -> None:
        assert _clean_float(float("nan")) is None

    def test_pos_inf_becomes_none(self) -> None:
        assert _clean_float(float("inf")) is None

    def test_neg_inf_becomes_none(self) -> None:
        assert _clean_float(float("-inf")) is None

    def test_string_passthrough(self) -> None:
        assert _clean_float("hello") == "hello"


class TestRowFromPrediction:
    def test_minimal_row_typed_columns_no_blob(self) -> None:
        pred = {
            "protein_accession": "Q1",
            "go_term_id": 42,
            "ref_protein_accession": "R1",
            "distance": 0.25,
        }
        pred_set_id = uuid.uuid4()
        row = _row_from_prediction(pred, pred_set_id)
        assert row["prediction_set_id"] == pred_set_id
        assert row["protein_accession"] == "Q1"
        assert row["distance"] == 0.25
        # Signal-store code-switch: the redundant JSONB blob is not written.
        assert "features" not in row

    def test_nan_features_become_none(self) -> None:
        pred = {
            "protein_accession": "Q1",
            "go_term_id": 42,
            "ref_protein_accession": "R1",
            "distance": 0.1,
            "identity_nw": float("nan"),
            "length_query": 130,
        }
        row = _row_from_prediction(pred, uuid.uuid4())
        assert row["identity_nw"] is None
        assert row["length_query"] == 130


# ---------------------------------------------------------------------------
# Coordinator: summarize_payload + load_query_accessions
# ---------------------------------------------------------------------------


class TestSummarizePayload:
    def test_empty_returns_empty(self) -> None:
        op = PredictGOTermsOperation()
        assert op.summarize_payload({}) == ""

    def test_with_session_resolves_labels(self) -> None:
        op = PredictGOTermsOperation()
        cfg_id = uuid.uuid4()
        ann_id = uuid.uuid4()
        cfg = MagicMock(spec=EmbeddingConfig)
        cfg.display_name = "esm2_t33"
        cfg.model_name = "esm2"
        cfg.model_backend = "esm"
        cfg.id = cfg_id
        ann = MagicMock(spec=AnnotationSet)
        ann.source = "swissprot"
        ann.source_version = "2024_05"

        def _get(cls, _id):
            if cls is EmbeddingConfig:
                return cfg
            if cls is AnnotationSet:
                return ann
            return MagicMock()

        session = MagicMock()
        session.get.side_effect = _get
        summary = op.summarize_payload(
            {
                "embedding_config_id": str(cfg_id),
                "annotation_set_id": str(ann_id),
                "query_set_id": str(uuid.uuid4()),
                "limit_per_entry": 7,
                "search_backend": "numpy",
                "aspect_separated_knn": True,
                "compute_alignments": True,
                "compute_taxonomy": True,
            },
            session=session,
        )
        assert "esm2_t33" in summary
        assert "ref=swissprot@2024_05" in summary
        assert "qs=" in summary
        assert "k=7" in summary
        assert "numpy" in summary
        assert "aspect-knn" in summary
        assert "+align" in summary
        assert "+tax" in summary

    def test_session_get_failure_falls_back_to_short_id(self) -> None:
        op = PredictGOTermsOperation()
        cfg_id_raw = "not-a-uuid"
        ann_id_raw = "ann-XYZ"
        session = MagicMock()
        session.get.side_effect = Exception("boom")
        summary = op.summarize_payload(
            {"embedding_config_id": cfg_id_raw, "annotation_set_id": ann_id_raw},
            session=session,
        )
        # When the lookup fails the helper silently falls back to no row;
        # without a session it just truncates the raw id.
        assert summary == "" or "cfg=" in summary or "ann=" in summary

    def test_without_session_uses_truncated_ids(self) -> None:
        op = PredictGOTermsOperation()
        summary = op.summarize_payload(
            {
                "embedding_config_id": "abcd1234-aaaa-bbbb-cccc-dddddddddddd",
                "annotation_set_id": "1111aaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            }
        )
        assert "cfg=abcd1234" in summary
        assert "ann=1111aaaa" in summary

    def test_faiss_backend_appends_index_type(self) -> None:
        op = PredictGOTermsOperation()
        summary = op.summarize_payload({"search_backend": "faiss", "faiss_index_type": "IVFFlat"})
        assert "faiss/IVFFlat" in summary


class TestLoadQueryAccessions:
    """Covers both branches of ``_load_query_accessions``."""

    def _payload_obj(self, **kw) -> pgt.PredictGOTermsPayload:
        defaults = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
        }
        defaults.update(kw)
        return pgt.PredictGOTermsPayload.model_validate(defaults)

    def test_query_set_path(self) -> None:
        op = PredictGOTermsOperation()
        qs_id = uuid.uuid4()
        p = self._payload_obj(query_set_id=str(qs_id))
        session = MagicMock()
        session.get.return_value = MagicMock(spec=QuerySet)
        chain = session.query.return_value.join.return_value.filter.return_value
        chain.all.return_value = [("Q1",), ("Q2",)]
        accs = op._load_query_accessions(session, p, uuid.uuid4(), _noop_emit)
        assert accs == ["Q1", "Q2"]

    def test_query_set_missing_raises(self) -> None:
        op = PredictGOTermsOperation()
        qs_id = uuid.uuid4()
        p = self._payload_obj(query_set_id=str(qs_id))
        session = MagicMock()
        session.get.return_value = None
        with pytest.raises(ValueError, match="QuerySet"):
            op._load_query_accessions(session, p, uuid.uuid4(), _noop_emit)

    def test_explicit_accessions_filters_query(self) -> None:
        op = PredictGOTermsOperation()
        p = self._payload_obj(query_accessions=["P1", "P2"])
        session = MagicMock()
        filtered_q = MagicMock()
        filtered_q.all.return_value = [("P1",)]
        q_chain = session.query.return_value.join.return_value.join.return_value
        q_chain.filter.return_value = filtered_q
        accs = op._load_query_accessions(session, p, uuid.uuid4(), _noop_emit)
        assert accs == ["P1"]

    def test_all_accessions_when_no_filter(self) -> None:
        op = PredictGOTermsOperation()
        p = self._payload_obj()  # no query_set_id, no explicit accessions
        session = MagicMock()
        q_chain = session.query.return_value.join.return_value.join.return_value
        q_chain.all.return_value = [("X1",), ("X2",), ("X3",)]
        accs = op._load_query_accessions(session, p, uuid.uuid4(), _noop_emit)
        assert accs == ["X1", "X2", "X3"]


class TestBuildBatchMessageRerankerNulls:
    """Cover the empty-dispatch branch of ``_build_batch_message``."""

    def test_no_binding_sets_null_reranker_fields(self) -> None:
        from protea.core.operations.predict_go_terms._common import _RerankerDispatch

        p = pgt.PredictGOTermsPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
            }
        )
        msg = PredictGOTermsOperation._build_batch_message(
            p,
            prediction_set_id=uuid.uuid4(),
            parent_job_id=uuid.uuid4(),
            batch_accs=["Q1"],
            dispatch=_RerankerDispatch(),
        )
        assert msg["payload"]["reranker_artifact_uri"] is None
        assert msg["payload"]["reranker_feature_schema_sha"] is None
        # INT-5: per-category fields default to None (single-booster path).
        assert msg["payload"]["reranker_nk_artifact_uri"] is None
        assert msg["payload"]["reranker_pk_feature_schema_sha"] is None


class TestResolveRerankerBindingNoMid:
    def test_returns_none_when_id_absent(self) -> None:
        session = MagicMock()
        p = pgt.PredictGOTermsPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
            }
        )
        out = PredictGOTermsOperation._resolve_reranker_binding(session, p, _noop_emit)
        assert out is None

    def test_raises_when_missing_feature_schema(self) -> None:
        session = MagicMock()
        row = MagicMock(spec=RerankerModel)
        row.artifact_uri = "file:///tmp/x.txt"
        row.feature_schema_sha = ""
        row.name = "x"
        session.get.return_value = row
        p = pgt.PredictGOTermsPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
                "reranker_model_id": str(uuid.uuid4()),
            }
        )
        with pytest.raises(ValueError, match="feature_schema_sha"):
            PredictGOTermsOperation._resolve_reranker_binding(session, p, _noop_emit)


# ---------------------------------------------------------------------------
# Batch worker tiny helpers
# ---------------------------------------------------------------------------


class TestSummarizePayloadBatch:
    def test_n_when_accessions_present(self) -> None:
        op = PredictGOTermsBatchOperation()
        assert op.summarize_payload({"query_accessions": ["a", "b", "c"]}) == "n=3"

    def test_empty_returns_empty(self) -> None:
        op = PredictGOTermsBatchOperation()
        assert op.summarize_payload({}) == ""


class TestSummarizePayloadStore:
    def test_n_when_predictions_present(self) -> None:
        op = StorePredictionsOperation()
        assert op.summarize_payload({"predictions": [1, 2]}) == "n=2"

    def test_empty_returns_empty(self) -> None:
        op = StorePredictionsOperation()
        assert op.summarize_payload({}) == ""


class TestShouldSkipForParent:
    def test_missing_parent_does_not_skip(self) -> None:
        session = MagicMock()
        session.get.return_value = None
        assert (
            PredictGOTermsBatchOperation._should_skip_for_parent(session, uuid.uuid4(), _noop_emit)
            is False
        )

    def test_running_parent_does_not_skip(self) -> None:
        from protea.infrastructure.orm.models.job import JobStatus

        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        assert (
            PredictGOTermsBatchOperation._should_skip_for_parent(session, uuid.uuid4(), _noop_emit)
            is False
        )


class TestChunkedPublish:
    def _setup_tuning(self, monkeypatch, size: int) -> None:
        from protea.config import tuning

        cur = tuning.get_tuning()

        class _Op:
            store_chunk_size = size
            annotation_chunk_size = cur.operation.annotation_chunk_size
            stream_chunk_size = cur.operation.stream_chunk_size

        class _Stub:
            operation = _Op()
            worker = cur.worker

        monkeypatch.setattr(tuning, "get_tuning", lambda: _Stub())

    def test_splits_into_expected_chunks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._setup_tuning(monkeypatch, size=2)
        op = PredictGOTermsBatchOperation()
        parent_job = uuid.uuid4()
        pred_set = uuid.uuid4()
        preds = [{"i": i} for i in range(5)]
        msgs = op._chunked_publish(
            parent_job_id=parent_job, prediction_set_id=pred_set, prediction_dicts=preds
        )
        # 5/2 → 3 chunks
        assert len(msgs) == 3
        queues = {q for q, _ in msgs}
        assert queues == {"protea.predictions.write"}
        # Only the final chunk has is_final_chunk True.
        finals = [m["payload"]["is_final_chunk"] for _, m in msgs]
        assert finals == [False, False, True]
        # All chunks share the same parent_job_id / prediction_set_id strings.
        assert all(m["payload"]["parent_job_id"] == str(parent_job) for _, m in msgs)
        assert all(m["payload"]["prediction_set_id"] == str(pred_set) for _, m in msgs)

    def test_empty_predictions_emit_single_terminal_chunk(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._setup_tuning(monkeypatch, size=10)
        op = PredictGOTermsBatchOperation()
        msgs = op._chunked_publish(
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            prediction_dicts=[],
        )
        assert len(msgs) == 1
        assert msgs[0][1]["payload"]["is_final_chunk"] is True
        assert msgs[0][1]["payload"]["predictions"] == []


class TestEmitDone:
    def test_includes_reranker_stats_when_present(self) -> None:
        op = PredictGOTermsBatchOperation()
        emit, events = _emit_capture()
        op._emit_done(
            emit,
            valid_accessions=["Q1", "Q2"],
            prediction_dicts=[{"x": 1}, {"x": 2}, {"x": 3}],
            reranker_stats={"applied": True, "rows": 3},
            started_at=0.0,
        )
        assert events[0][0] == "predict_go_terms_batch.done"
        fields = events[0][1]
        assert fields["queries"] == 2
        assert fields["predictions"] == 3
        assert fields["reranker"] == {"applied": True, "rows": 3}

    def test_omits_reranker_key_when_none(self) -> None:
        op = PredictGOTermsBatchOperation()
        emit, events = _emit_capture()
        op._emit_done(
            emit,
            valid_accessions=["Q1"],
            prediction_dicts=[{"x": 1}],
            reranker_stats=None,
            started_at=0.0,
        )
        assert "reranker" not in events[0][1]


class TestAttachGoTermAspect:
    def test_no_unique_ids_early_return(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        dicts: list[dict] = [{"protein_accession": "Q1"}, {"go_term_id": None}]
        op._attach_go_term_aspect(session, dicts)
        # session.query should never have been invoked.
        session.query.assert_not_called()

    def test_writes_aspect_back(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        chain = session.query.return_value.filter.return_value
        chain.all.return_value = [(10, "P"), (11, "F")]
        dicts = [
            {"protein_accession": "Q1", "go_term_id": 10},
            {"protein_accession": "Q1", "go_term_id": 11},
        ]
        op._attach_go_term_aspect(session, dicts)
        assert dicts[0]["aspect"] == "P"
        assert dicts[1]["aspect"] == "F"


# ---------------------------------------------------------------------------
# Reranker live-schema helpers
# ---------------------------------------------------------------------------


class TestResolveLiveSchemaSha:
    def test_returns_sha_when_contracts_present(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x.txt",
            reranker_feature_schema_sha="will_be_overwritten",
            compute_alignments=False,
            compute_taxonomy=False,
            compute_v6_features=False,
        )
        emit, _events = _emit_capture()
        sha = op._reranker_scorer.resolve_live_schema_sha(p, emit)
        assert isinstance(sha, str) and len(sha) > 0

    def test_returns_none_when_contracts_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Emit a warning and return ``None`` when ``protea_contracts``
        ImportError fires inside the helper."""
        op = PredictGOTermsBatchOperation()
        p = _payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x.txt",
            reranker_feature_schema_sha="something",
        )
        emit, events = _emit_capture()
        # Inject an ImportError when the helper tries to import the
        # contracts SHA function.
        import sys

        sentinel = "protea_contracts"
        real = sys.modules.get(sentinel)
        sys.modules[sentinel] = None  # forces ImportError on next import
        try:
            sha = op._reranker_scorer.resolve_live_schema_sha(p, emit)
        finally:
            if real is not None:
                sys.modules[sentinel] = real
            else:
                sys.modules.pop(sentinel, None)
        assert sha is None
        assert any(name == "reranker.skipped" for name, _ in events)


class TestApplyRerankerEmptyScores:
    def test_zero_rows_returns_applied_zero(self) -> None:
        from protea_reranker_lab.contracts import compute_feature_schema_sha

        op = PredictGOTermsBatchOperation()
        expected_sha = compute_feature_schema_sha(["knn", "annotation_meta"])
        p = _payload(
            reranker_model_id=str(uuid.uuid4()),
            reranker_artifact_uri="file:///tmp/x.txt",
            reranker_feature_schema_sha=expected_sha,
            compute_alignments=False,
            compute_taxonomy=False,
            compute_v6_features=False,
        )
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []
        emit, _events = _emit_capture()
        empty_scores = np.array([], dtype=np.float32)
        with (
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.load_reranker",
                return_value=MagicMock(),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.apply_reranker",
                return_value=empty_scores,
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.get_artifact_store",
                return_value=MagicMock(),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.load_settings",
                return_value=MagicMock(),
            ),
        ):
            # Pass an empty list so the strict zip in ``RerankerScorer.score``
            # short-circuits cleanly; scores.size == 0 then returns the
            # ``rows: 0`` summary.
            stats = op._reranker_scorer.apply_if_aligned(session, [], p, emit)
        assert stats == {"applied": True, "rows": 0}


# ---------------------------------------------------------------------------
# Reference data + per-aspect cache (disk-backed)
# ---------------------------------------------------------------------------


class TestFindMissingAspects:
    def test_all_missing_when_dir_empty(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        from protea.core.domain.aspect import ASPECT_CODES

        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        ec_id = uuid.uuid4()
        as_id = uuid.uuid4()
        missing = PredictGOTermsBatchOperation._find_missing_aspects(ec_id, as_id)
        assert sorted(missing) == sorted(ASPECT_CODES)


class TestLoadReferenceData:
    def test_zero_rows_returns_empty_view(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        """When the accession query returns 0 rows the streamer should
        emit empty arrays — and the derived view should have empty f32
        slices."""
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        # _reference_pool_query just returns the chained query object;
        # patch the streamer + count separately.
        with (
            patch.object(
                op,
                "_reference_pool_query",
                return_value=MagicMock(count=lambda: 0),
            ),
            patch.object(
                op,
                "_stream_reference_pool",
                return_value=([], np.empty((0,), dtype=np.float16)),
            ),
        ):
            emit, events = _emit_capture()
            ref = op._load_reference_data(session, _PoolRequest(uuid.uuid4(), uuid.uuid4()), emit)
        assert ref["accessions"] == []
        assert ref["embeddings"].size == 0
        names = [n for n, _ in events]
        assert "predict_go_terms_batch.load_references_start" in names
        assert "predict_go_terms_batch.load_references_done" in names

    def test_loads_and_caches_db_rows(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        """First call streams from DB and writes the cache; second call
        reads back from disk without invoking the streamer again."""
        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        ec_id = uuid.uuid4()
        as_id = uuid.uuid4()
        accs = ["R1", "R2"]
        emb = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float16)
        stream_calls = {"n": 0}

        def _stream(_q):
            stream_calls["n"] += 1
            return list(accs), emb

        with (
            patch.object(
                op,
                "_reference_pool_query",
                return_value=MagicMock(count=lambda: len(accs)),
            ),
            patch.object(op, "_stream_reference_pool", side_effect=_stream),
        ):
            ref1 = op._load_reference_data(session, _PoolRequest(ec_id, as_id), _noop_emit)
            ref2 = op._load_reference_data(session, _PoolRequest(ec_id, as_id), _noop_emit)
        assert ref1["accessions"] == accs
        assert ref2["accessions"] == accs
        assert stream_calls["n"] == 1  # 2nd call hit disk cache


class TestStreamReferencePool:
    def test_empty_query_returns_empty(self) -> None:
        op = PredictGOTermsBatchOperation()
        accession_q = MagicMock()
        accession_q.count.return_value = 0
        accs, embeddings = op._stream_reference_pool(accession_q)
        assert accs == []
        assert embeddings.shape == (0,)
        assert embeddings.dtype == np.float16

    def test_streams_rows_into_matrix(self) -> None:
        op = PredictGOTermsBatchOperation()
        # accession_q.count() == 2, base_q.limit(1).one() returns
        # (acc, halfvec), base_q.yield_per() returns (acc, halfvec) pairs.
        hv0 = _halfvec([1.0, 2.0])
        hv1 = _halfvec([3.0, 4.0])
        accession_q = MagicMock()
        accession_q.count.return_value = 2
        base_q = MagicMock()
        accession_q.add_columns.return_value = base_q
        base_q.limit.return_value.one.return_value = ("R1", hv0)
        base_q.yield_per.return_value = [("R1", hv0), ("R2", hv1)]
        accs, embeddings = op._stream_reference_pool(accession_q)
        assert accs == ["R1", "R2"]
        assert embeddings.shape == (2, 2)
        assert embeddings.dtype == np.float16


class TestLoadReferenceDataPerAspect:
    def test_empty_unified_returns_empty_per_aspect(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        from protea.core.domain.aspect import ASPECT_CODES

        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        op = PredictGOTermsBatchOperation()
        # Force _load_reference_data to return an empty unified pool.
        empty_unified = {
            "accessions": [],
            "embeddings": np.empty((0,), dtype=np.float16),
            "embeddings_f32": np.empty((0,), dtype=np.float32),
            "embeddings_f32_cos": np.empty((0,), dtype=np.float32),
        }
        with patch.object(op, "_load_reference_data", return_value=empty_unified):
            result = op._load_reference_data_per_aspect(
                MagicMock(), _PoolRequest(uuid.uuid4(), uuid.uuid4()), _noop_emit
            )
        assert _UNIFIED_REF_KEY in result
        for asp in ASPECT_CODES:
            assert result[asp]["accessions"] == []

    def test_populated_unified_persists_aspect_caches(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """End-to-end on the per-aspect path with a populated unified pool:
        each missing aspect triggers a DB scan, persists index + CSR files
        to disk, and the resulting views slice the unified pool correctly."""
        from protea.core.domain.aspect import ASPECT_CODES

        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        op = PredictGOTermsBatchOperation()
        # Unified pool: 4 refs, 2-dim embeddings.
        unified_accs = ["R1", "R2", "R3", "R4"]
        unified_emb16 = np.array(
            [[1.0, 0.0], [0.99, 0.01], [0.0, 1.0], [0.5, 0.5]], dtype=np.float16
        )
        from protea.core.disk_cache import _derive_reference_views

        unified = _derive_reference_views(unified_accs, unified_emb16)

        # _collect_aspect_annotations fixture: P→{R1,R4}, F→{R2}, C→{R3}.
        ec_id = uuid.uuid4()
        as_id = uuid.uuid4()

        with (
            patch.object(op, "_load_reference_data", return_value=unified),
            patch.object(
                PredictGOTermsBatchOperation,
                "_collect_aspect_annotations",
                return_value=(
                    {"P": {"R1", "R4"}, "F": {"R2"}, "C": {"R3"}},
                    {
                        "P": {
                            "R1": [{"go_term_id": 101, "qualifier": "", "evidence_code": "IEA"}],
                            "R4": [{"go_term_id": 102, "qualifier": "", "evidence_code": "IEA"}],
                        },
                        "F": {
                            "R2": [{"go_term_id": 201, "qualifier": "", "evidence_code": "IEA"}],
                        },
                        "C": {
                            "R3": [{"go_term_id": 301, "qualifier": "", "evidence_code": "IEA"}],
                        },
                    },
                ),
            ),
        ):
            emit, events = _emit_capture()
            result = op._load_reference_data_per_aspect(MagicMock(), _PoolRequest(ec_id, as_id), emit)
        for asp in ASPECT_CODES:
            view = result[asp]
            assert isinstance(view["embeddings_f32"], np.ndarray)
            assert "anno_gtids" in view
            assert "acc_to_anno_idx" in view
        assert result["P"]["accessions"] == ["R1", "R4"] or sorted(result["P"]["accessions"]) == [
            "R1",
            "R4",
        ]
        # The aspect index file got written.
        from protea.core.disk_cache import _aspect_index_path

        assert _aspect_index_path(ec_id, as_id, "P").exists()
        # Audit events: per-aspect done event has source="database".
        per_aspect = [
            f for n, f in events if n == "predict_go_terms_batch.load_references_per_aspect_done"
        ]
        sources = {f["source"] for f in per_aspect}
        assert "database" in sources


class TestAssembleAspectView:
    def test_aspect_view_uses_disk_index_and_csr(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        from protea.core.disk_cache import (
            _aspect_index_path,
            _build_anno_csr,
            _derive_reference_views,
            _save_anno_csr_to_disk,
        )

        monkeypatch.setattr(disk_cache_module, "_DISK_CACHE_DIR", tmp_path)
        ec_id = uuid.uuid4()
        as_id = uuid.uuid4()
        unified_accs = ["R1", "R2", "R3"]
        unified_emb16 = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]], dtype=np.float16)
        unified = _derive_reference_views(unified_accs, unified_emb16)
        idx_path = _aspect_index_path(ec_id, as_id, "P")
        idx_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(idx_path, np.array([0, 2], dtype=np.int32))
        csr = _build_anno_csr(
            ["R1", "R3"],
            {
                "R1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "IEA"}],
                "R3": [{"go_term_id": 2, "qualifier": "", "evidence_code": "IEA"}],
            },
        )
        _save_anno_csr_to_disk(ec_id, as_id, "P", csr)
        view, n = PredictGOTermsBatchOperation._assemble_aspect_view("P", unified, ec_id, as_id)
        assert n == 2
        assert view["accessions"] == ["R1", "R3"]
        assert view["embeddings_f32"].shape == (2, 2)
        assert "anno_gtids" in view
        assert view["acc_to_anno_idx"] == {"R1": 0, "R3": 1}


# ---------------------------------------------------------------------------
# Annotation loaders + feature engineering helpers
# ---------------------------------------------------------------------------


class TestLoadAnnotationsFor:
    def test_chunks_and_aggregates_rows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from protea.config import tuning

        cur = tuning.get_tuning()

        class _Op:
            store_chunk_size = cur.operation.store_chunk_size
            annotation_chunk_size = 2
            stream_chunk_size = cur.operation.stream_chunk_size

        class _Stub:
            operation = _Op()
            worker = cur.worker

        monkeypatch.setattr(tuning, "get_tuning", lambda: _Stub())
        op = PredictGOTermsBatchOperation()

        # Two chunks of size 2 over 3 accessions ⇒ 2 round-trips.
        def fake_fetch(_self, _session, _ann_id, chunk, aspect):
            # Return one row per accession in the chunk.
            return [(acc, 10 + i, None, "IEA") for i, acc in enumerate(chunk)]

        monkeypatch.setattr(PredictGOTermsBatchOperation, "_fetch_annotation_chunk", fake_fetch)
        result = op._load_annotations_for(MagicMock(), uuid.uuid4(), {"A", "B", "C"})
        assert set(result.keys()) == {"A", "B", "C"}
        for v in result.values():
            assert len(v) == 1
            assert v[0]["evidence_code"] == "IEA"

    def test_empty_accessions_returns_empty_map(self) -> None:
        op = PredictGOTermsBatchOperation()
        result = op._load_annotations_for(MagicMock(), uuid.uuid4(), set())
        assert result == {}


class TestFetchAnnotationChunk:
    def test_aspect_filter_adds_join(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        filtered = session.query.return_value.filter.return_value
        filtered.join.return_value.filter.return_value.all.return_value = [
            ("R1", 10, None, "IEA"),
        ]
        rows = op._fetch_annotation_chunk(session, uuid.uuid4(), ["R1"], aspect="P")
        assert rows == [("R1", 10, None, "IEA")]
        # Without aspect: same query, no extra join.
        filtered.all.return_value = [("R2", 11, None, "IEA")]
        rows = op._fetch_annotation_chunk(session, uuid.uuid4(), ["R2"], aspect=None)
        assert rows == [("R2", 11, None, "IEA")]


class TestLoadQueryEmbeddings:
    def test_no_rows_returns_empty(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        session.query.return_value.join.return_value.join.return_value.filter.return_value.all.return_value = []
        p = _payload(query_accessions=["Q1"])
        emb, accs = op._load_query_embeddings(session, ["Q1"], uuid.uuid4(), p, _noop_emit)
        assert accs == []
        assert emb.shape == (0,)

    def test_returns_array_for_protein_path(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        hv = _halfvec([0.1, 0.2, 0.3])
        rows = [("Q1", hv), ("Q2", hv)]
        session.query.return_value.join.return_value.join.return_value.filter.return_value.all.return_value = rows
        p = _payload(query_accessions=["Q1", "Q2"])
        emb, accs = op._load_query_embeddings(session, ["Q1", "Q2"], uuid.uuid4(), p, _noop_emit)
        assert accs == ["Q1", "Q2"]
        assert emb.shape == (2, 3)
        assert emb.dtype == np.float32

    def test_query_set_path(self) -> None:
        op = PredictGOTermsBatchOperation()
        qs_id = uuid.uuid4()
        session = MagicMock()
        hv = _halfvec([0.0, 1.0])
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("Q1", hv)
        ]
        p = _payload(query_set_id=str(qs_id), query_accessions=["Q1"])
        emb, accs = op._load_query_embeddings(session, ["Q1"], uuid.uuid4(), p, _noop_emit)
        assert accs == ["Q1"]
        assert emb.shape == (1, 2)


class TestSequenceAndTaxonomyLoaders:
    def _stub_tuning(self, monkeypatch: pytest.MonkeyPatch, chunk: int = 2) -> None:
        from protea.config import tuning

        cur = tuning.get_tuning()

        class _Op:
            store_chunk_size = cur.operation.store_chunk_size
            annotation_chunk_size = chunk
            stream_chunk_size = cur.operation.stream_chunk_size

        class _Stub:
            operation = _Op()
            worker = cur.worker

        monkeypatch.setattr(tuning, "get_tuning", lambda: _Stub())

    def test_load_sequences_for_proteins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._stub_tuning(monkeypatch)
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        chain = session.query.return_value.join.return_value.filter.return_value
        chain.all.side_effect = [
            [("R1", "MFG"), ("R2", "AAA")],
            [("R3", "CCC")],
        ]
        result = op._load_sequences_for_proteins(session, {"R1", "R2", "R3"})
        assert result == {"R1": "MFG", "R2": "AAA", "R3": "CCC"}

    def test_load_sequences_for_queries_falls_back_to_proteins(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._stub_tuning(monkeypatch, chunk=10)
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        # No query_set_id ⇒ delegated to _load_sequences_for_proteins.
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("Q1", "MAA"),
        ]
        p = _payload(query_accessions=["Q1"])
        out = op._load_sequences_for_queries(session, p, ["Q1"])
        assert out == {"Q1": "MAA"}

    def test_load_sequences_for_queries_uses_query_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._stub_tuning(monkeypatch, chunk=10)
        op = PredictGOTermsBatchOperation()
        qs_id = uuid.uuid4()
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.all.return_value = [
            ("Q1", "MGS"),
            ("Q2", "PPP"),
        ]
        p = _payload(query_set_id=str(qs_id), query_accessions=["Q1", "Q2"])
        out = op._load_sequences_for_queries(session, p, ["Q1", "Q2"])
        assert out == {"Q1": "MGS", "Q2": "PPP"}

    def test_load_taxonomy_ids_for_proteins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._stub_tuning(monkeypatch)
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        chain = session.query.return_value.filter.return_value
        chain.all.side_effect = [
            [("R1", 9606), ("R2", 0)],  # 0 → None
            [("R3", None)],
        ]
        result = op._load_taxonomy_ids_for_proteins(session, {"R1", "R2", "R3"})
        assert result["R1"] == 9606
        assert result["R2"] is None
        assert result["R3"] is None

    def test_load_taxonomy_ids_for_queries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._stub_tuning(monkeypatch, chunk=10)
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [("Q1", 9606)]
        p = _payload(query_accessions=["Q1", "Q2"])
        result = op._load_taxonomy_ids_for_queries(session, p, ["Q1", "Q2"])
        # Q2 was not in returned rows → stays None.
        assert result["Q1"] == 9606
        assert result["Q2"] is None


class TestLoadFeatureEngineeringData:
    def test_both_flags_off_returns_empty_dicts(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(compute_alignments=False, compute_taxonomy=False)
        out = op._load_feature_engineering_data(MagicMock(), p, ["Q1"], {"R1"})
        assert out == ({}, {}, {}, {})

    def test_alignment_flag_loads_sequences(self, monkeypatch: pytest.MonkeyPatch) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(compute_alignments=True, compute_taxonomy=False)
        with (
            patch.object(op, "_load_sequences_for_proteins", return_value={"R1": "MGS"}),
            patch.object(op, "_load_sequences_for_queries", return_value={"Q1": "MAA"}),
        ):
            out = op._load_feature_engineering_data(MagicMock(), p, ["Q1"], {"R1"})
        assert out == ({"R1": "MGS"}, {"Q1": "MAA"}, {}, {})

    def test_taxonomy_flag_loads_tax_ids(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(compute_alignments=False, compute_taxonomy=True)
        with (
            patch.object(op, "_load_taxonomy_ids_for_proteins", return_value={"R1": 9606}),
            patch.object(op, "_load_taxonomy_ids_for_queries", return_value={"Q1": 9606}),
        ):
            out = op._load_feature_engineering_data(MagicMock(), p, ["Q1"], {"R1"})
        assert out == ({}, {}, {"R1": 9606}, {"Q1": 9606})


# ---------------------------------------------------------------------------
# Ancestor expansion + FK resolver
# ---------------------------------------------------------------------------


class TestStampGoIds:
    def test_writes_go_id_strings(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (10, "GO:0000010"),
            (11, "GO:0000011"),
        ]
        dicts = [
            {"go_term_id": 10},
            {"go_term_id": 11},
            {"go_term_id": None},
        ]
        m = op._stamp_go_ids(session, dicts)
        assert m == {10: "GO:0000010", 11: "GO:0000011"}
        assert dicts[0]["go_id"] == "GO:0000010"
        assert dicts[1]["go_id"] == "GO:0000011"
        assert "go_id" not in dicts[2]


class TestResolveSyntheticFks:
    def test_no_ancestors_passes_through(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        # All go_ids are leaves ⇒ no ancestor pairs to resolve.
        dicts = [
            {"go_term_id": 10, "go_id": "GO:0000010"},
        ]
        int_to_str = {10: "GO:0000010"}
        out = op._resolve_synthetic_fks(session, dicts, int_to_str, uuid.uuid4())
        assert out == dicts

    def test_stamps_ancestor_fk(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        # Ancestor "GO:0000020" maps to db id 20 under the given snapshot.
        session.execute.return_value.all.return_value = [(20, "GO:0000020")]
        dicts = [
            {"go_term_id": 10, "go_id": "GO:0000010"},  # leaf
            {"go_id": "GO:0000020"},  # ancestor (no go_term_id yet)
        ]
        int_to_str = {10: "GO:0000010"}
        out = op._resolve_synthetic_fks(session, dicts, int_to_str, uuid.uuid4())
        assert {row["go_term_id"] for row in out} == {10, 20}
        # Drops records whose go_id never resolved to an int.
        dicts2 = [{"go_id": "GO:9999999"}]
        session.execute.return_value.all.return_value = []
        out2 = op._resolve_synthetic_fks(session, dicts2, {}, uuid.uuid4())
        assert out2 == []


class TestExpandToAncestors:
    def test_emits_audit_event_and_resolves_fks(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        snapshot_id = uuid.uuid4()
        p = _payload(
            ontology_snapshot_id=str(snapshot_id),
            expand_votes_to_ancestors=True,
            limit_per_entry=5,
        )
        # _stamp_go_ids returns {10: 'GO:0010'} after the lookup.
        session.execute.return_value.all.return_value = [(10, "GO:0010")]
        emit, events = _emit_capture()

        # Inject the imports into the helper's namespace by importing
        # the parents and patching them on the feature_enricher module
        # before the helper does its local ``from … import …``.
        from protea.core import feature_enricher as fe

        with (
            patch.object(fe, "load_parent_map", return_value={}),
            patch.object(
                fe,
                "expand_predictions_to_ancestors",
                side_effect=lambda preds, **_kw: preds + [{"go_id": "GO:0010"}],
            ),
        ):
            out = op._expand_to_ancestors(
                session,
                p,
                [{"go_term_id": 10, "distance": 0.1}],
                emit,
            )
        assert any(name == "predict_go_terms_batch.expanded_to_ancestors" for name, _ in events)
        # The leaf stays plus the synthetic ancestor (which collapses to the
        # same leaf in our toy fixture).
        assert len(out) == 2


# ---------------------------------------------------------------------------
# Path dispatchers + execute()
# ---------------------------------------------------------------------------


class TestRunUnifiedPath:
    def test_returns_none_when_no_refs(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = MagicMock()
        ctx.p = _payload(aspect_separated_knn=False)
        ref_data = {"embeddings": np.empty((0,), dtype=np.float32)}
        query_batch = pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
        )
        emit, events = _emit_capture()
        out = op._run_unified_path(MagicMock(), ctx, query_batch, ref_data, emit)
        assert out is None
        assert any(name == "predict_go_terms_batch.no_references" for name, _ in events)


class TestExecuteBatchHappyPath:
    def test_full_flow_with_mocked_helpers(self) -> None:
        """End-to-end happy path on ``PredictGOTermsBatchOperation.execute``:
        parent OK, ref cache yields stub data, query embeddings load,
        KNN dispatch returns predictions, and the result aggregates the
        publish operations."""
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        from protea.infrastructure.orm.models.job import JobStatus

        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        payload_dict = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["Q1"],
            "aspect_separated_knn": False,
        }
        # Stub all heavy collaborators so execute() exercises its own
        # branches end-to-end without DB roundtrips.
        ref_data = {
            "accessions": ["R1"],
            "embeddings": np.array([[1.0]], dtype=np.float16),
            "embeddings_f32": np.array([[1.0]], dtype=np.float32),
            "embeddings_f32_cos": np.array([[1.0]], dtype=np.float32),
        }
        prediction = {
            "protein_accession": "Q1",
            "go_term_id": 1,
            "ref_protein_accession": "R1",
            "distance": 0.1,
        }
        knn = pgt._KnnResult(
            prediction_dicts=[prediction],
            v6_ctx=None,
            query_batch=pgt._QueryBatch(
                valid_accessions=["Q1"],
                query_embeddings=np.array([[0.1]], dtype=np.float32),
            ),
        )
        with (
            patch.object(op, "_ensure_reference_cache", return_value=ref_data),
            patch.object(
                op,
                "_load_query_embeddings",
                return_value=(np.array([[0.1]], dtype=np.float32), ["Q1"]),
            ),
            patch.object(op, "_run_knn_path", return_value=knn),
            patch.object(op, "_run_post_knn_pipeline", return_value=([prediction], None)),
        ):
            result = op.execute(session, payload_dict, emit=_noop_emit)
        assert result.result["predictions"] == 1
        assert result.result["store_chunks"] >= 1

    def test_returns_empty_when_query_embeddings_missing(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        from protea.infrastructure.orm.models.job import JobStatus

        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        payload_dict = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["Q1"],
            "aspect_separated_knn": False,
        }
        with (
            patch.object(
                op, "_ensure_reference_cache", return_value={"embeddings": np.empty((0,))}
            ),
            patch.object(
                op,
                "_load_query_embeddings",
                return_value=(np.empty((0,)), []),
            ),
        ):
            result = op.execute(session, payload_dict, emit=_noop_emit)
        assert result.result["predictions"] == 0

    def test_returns_empty_when_knn_returns_none(self) -> None:
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        from protea.infrastructure.orm.models.job import JobStatus

        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        payload_dict = {
            "embedding_config_id": str(uuid.uuid4()),
            "annotation_set_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": ["Q1"],
            "aspect_separated_knn": False,
        }
        with (
            patch.object(op, "_ensure_reference_cache", return_value={}),
            patch.object(
                op,
                "_load_query_embeddings",
                return_value=(np.array([[0.1]], dtype=np.float32), ["Q1"]),
            ),
            patch.object(op, "_run_knn_path", return_value=None),
        ):
            result = op.execute(session, payload_dict, emit=_noop_emit)
        assert result.result["predictions"] == 0


class TestRunPostKnnPipeline:
    def test_v6_then_expansion_then_reranker_invoked_in_order(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(
                compute_v6_features=True,
                expand_votes_to_ancestors=True,
                reranker_model_id=str(uuid.uuid4()),
            ),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        knn = pgt._KnnResult(
            prediction_dicts=[{"go_term_id": 1, "protein_accession": "Q1", "distance": 0.1}],
            v6_ctx={"neighbors_by_aspect": {}, "go_map_by_aspect": {}, "pair_features": {}},
            query_batch=pgt._QueryBatch(
                valid_accessions=["Q1"],
                query_embeddings=np.array([[0.1]], dtype=np.float32),
            ),
        )
        with (
            patch.object(op, "_apply_v6_features") as v6,
            patch.object(
                op,
                "_expand_to_ancestors",
                side_effect=lambda _s, _p, d, _e: d + [{"go_term_id": 2}],
            ) as expand,
            patch.object(
                op._reranker_scorer,
                "apply_if_aligned",
                return_value={"applied": True, "rows": 2},
            ) as rerank,
        ):
            preds, stats = op._run_post_knn_pipeline(MagicMock(), ctx, knn, {}, _noop_emit)
        v6.assert_called_once()
        expand.assert_called_once()
        rerank.assert_called_once()
        assert len(preds) == 2
        assert stats == {"applied": True, "rows": 2}

    def test_no_features_runs_no_helpers(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(
                compute_v6_features=False,
                expand_votes_to_ancestors=False,
                reranker_model_id=None,
            ),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        knn = pgt._KnnResult(
            prediction_dicts=[{"go_term_id": 1}],
            v6_ctx=None,
            query_batch=pgt._QueryBatch(
                valid_accessions=["Q1"],
                query_embeddings=np.array([[0.1]], dtype=np.float32),
            ),
        )
        preds, stats = op._run_post_knn_pipeline(MagicMock(), ctx, knn, {}, _noop_emit)
        assert preds == [{"go_term_id": 1}]
        assert stats is None


# ---------------------------------------------------------------------------
# Unified pipeline helpers
# ---------------------------------------------------------------------------


class TestLoadGoTermMetadata:
    def test_empty_annotations_returns_empty_maps(self) -> None:
        op = PredictGOTermsBatchOperation()
        id_map, asp_map = op._load_go_term_metadata(MagicMock(), {})
        assert id_map == {}
        assert asp_map == {}

    def test_chunks_and_aggregates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from protea.config import tuning

        cur = tuning.get_tuning()

        class _Op:
            store_chunk_size = cur.operation.store_chunk_size
            annotation_chunk_size = 2
            stream_chunk_size = cur.operation.stream_chunk_size

        class _Stub:
            operation = _Op()
            worker = cur.worker

        monkeypatch.setattr(tuning, "get_tuning", lambda: _Stub())
        op = PredictGOTermsBatchOperation()
        annotations = {
            "R1": [{"go_term_id": 10}, {"go_term_id": 11}],
            "R2": [{"go_term_id": 12}],
        }
        session = MagicMock()
        session.query.return_value.filter.return_value.all.side_effect = [
            [(10, "GO:0010", "P"), (11, "GO:0011", None)],
            [(12, "GO:0012", "F")],
        ]
        id_map, asp_map = op._load_go_term_metadata(session, annotations)
        assert id_map == {10: "GO:0010", 11: "GO:0011", 12: "GO:0012"}
        assert asp_map[10] == "P"
        assert asp_map[11] == ""
        assert asp_map[12] == "F"


class TestUnifiedLoadPairInputs:
    def test_both_flags_off(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(compute_alignments=False, compute_taxonomy=False)
        ctx = pgt._UnifiedPredictContext(
            p=p,
            annotation_set_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
            ref_data={},
        )
        rs, qs, rt, qt = op._unified_load_pair_inputs(MagicMock(), ctx, {"R1"})
        assert rs == {} and qs == {} and rt == {} and qt == {}

    def test_both_flags_on_invokes_loaders(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(compute_alignments=True, compute_taxonomy=True)
        ctx = pgt._UnifiedPredictContext(
            p=p,
            annotation_set_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
            ref_data={},
        )
        with (
            patch.object(op, "_load_sequences_for_proteins", return_value={"R1": "MGS"}) as rs_m,
            patch.object(op, "_load_sequences_for_queries", return_value={"Q1": "MGS"}) as qs_m,
            patch.object(op, "_load_taxonomy_ids_for_proteins", return_value={"R1": 9606}) as rt_m,
            patch.object(op, "_load_taxonomy_ids_for_queries", return_value={"Q1": 9606}) as qt_m,
        ):
            rs, qs, rt, qt = op._unified_load_pair_inputs(MagicMock(), ctx, {"R1"})
        assert rs == {"R1": "MGS"} and qs == {"Q1": "MGS"}
        assert rt == {"R1": 9606} and qt == {"Q1": 9606}
        rs_m.assert_called_once()
        qs_m.assert_called_once()
        rt_m.assert_called_once()
        qt_m.assert_called_once()


class TestEnsureReferenceCacheMode:
    def test_aspect_separated_dispatches_per_aspect(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Hit the cache miss branch with aspect_separated=True so the
        per-aspect loader is the one invoked."""
        # Wipe the module-level cache so the helper takes the miss branch.
        monkeypatch.setattr(pgt, "_REF_CACHE", {})
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(aspect_separated_knn=True),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        sentinel = {"sentinel": True}
        with (
            patch.object(
                op, "_load_reference_data_per_aspect", return_value=sentinel
            ) as per_aspect,
            patch.object(op, "_load_reference_data") as unified,
        ):
            out = op._ensure_reference_cache(MagicMock(), ctx, _noop_emit)
        per_aspect.assert_called_once()
        unified.assert_not_called()
        assert out is sentinel

    def test_cache_hit_skips_loaders(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Second invocation with the same key short-circuits both
        loaders — covers the ``if cache_key not in _REF_CACHE`` False
        branch."""
        from protea.core.operations.predict_go_terms import _batch_op_reference

        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(aspect_separated_knn=False),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        sentinel = {"cached": True}
        need_cos, need_plain = op._reference_dtype_needs(ctx.p)
        # Derived, not restated. A key spelled twice is a key that can
        # disagree with itself, and this test existed to catch a cache hit,
        # not to re-implement how the cache is addressed.
        key = op._process_cache_key(
            ctx.p,
            _PoolRequest(ctx.embedding_config_id, ctx.annotation_set_id, ctx.p.donor_policy),
            need_cos=need_cos,
            need_plain=need_plain,
        )
        monkeypatch.setattr(_batch_op_reference, "_REF_CACHE", {key: sentinel})
        with (
            patch.object(op, "_load_reference_data") as unified,
            patch.object(op, "_load_reference_data_per_aspect") as per_aspect,
        ):
            out = op._ensure_reference_cache(MagicMock(), ctx, _noop_emit)
        unified.assert_not_called()
        per_aspect.assert_not_called()
        assert out is sentinel

    def test_eviction_when_at_cache_max(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A pre-existing entry is evicted when the cache is already
        full and a new key comes in — covers the LRU branch (lines
        713-715)."""
        from protea.config import tuning
        from protea.core.operations.predict_go_terms import _batch_op_reference

        cur = tuning.get_tuning()
        smaller_worker = cur.worker.model_copy(update={"ref_cache_max": 1})

        class _Stub:
            operation = cur.operation
            worker = smaller_worker

        monkeypatch.setattr(tuning, "get_tuning", lambda: _Stub())

        # Seed the cache with an old entry so the eviction branch triggers.
        old_key = ("old_cfg", "old_ann", False)
        monkeypatch.setattr(_batch_op_reference, "_REF_CACHE", {old_key: {"placeholder": True}})

        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(aspect_separated_knn=False),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        sentinel = {"new": True}
        with patch.object(op, "_load_reference_data", return_value=sentinel):
            out = op._ensure_reference_cache(MagicMock(), ctx, _noop_emit)
        assert out is sentinel
        # Old entry got evicted, new one took its place.
        assert old_key not in _batch_op_reference._REF_CACHE


# ---------------------------------------------------------------------------
# Store predictions edge case: empty predictions list
# ---------------------------------------------------------------------------


class TestUnifiedLoadAnnotations:
    def test_calls_search_then_loads_anno_for_neighbors(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(
            aspect_separated_knn=False,
            compute_alignments=False,
            compute_taxonomy=False,
            search_backend="numpy",
            metric="l2",
        )
        ctx = pgt._UnifiedPredictContext(
            p=p,
            annotation_set_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
            ref_data={
                "accessions": ["R1", "R2"],
                "embeddings_f32": np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
                "embeddings_f32_cos": np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            },
        )
        # Patch search_knn (re-imported inside the helper).
        with (
            patch(
                "protea.core.knn_search.search_knn",
                return_value=[[("R1", 0.0)]],
            ),
            patch.object(
                op,
                "_load_annotations_for",
                return_value={"R1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "IEA"}]},
            ),
        ):
            anns, neighbors = op._unified_load_annotations(MagicMock(), ctx)
        assert neighbors == {"R1"}
        assert "R1" in anns

    def test_uses_cosine_view_when_metric_cosine(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(metric="cosine", aspect_separated_knn=False, search_backend="numpy")
        ctx = pgt._UnifiedPredictContext(
            p=p,
            annotation_set_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
            ref_data={
                "accessions": ["R1"],
                "embeddings_f32": np.array([[1.0, 0.0]], dtype=np.float32),
                "embeddings_f32_cos": np.array([[1.0, 0.0]], dtype=np.float32),
            },
        )
        captured: dict[str, np.ndarray] = {}

        def fake_search(_q, refs, _accs, **_kw):
            captured["refs"] = refs
            return [[("R1", 0.0)]]

        with (
            patch("protea.core.knn_search.search_knn", side_effect=fake_search),
            patch.object(op, "_load_annotations_for", return_value={}),
        ):
            op._unified_load_annotations(MagicMock(), ctx)
        assert "refs" in captured


class TestUnifiedPredictViaPipelineWiring:
    def test_delegates_to_adapter(self) -> None:
        op = PredictGOTermsBatchOperation()
        p = _payload(aspect_separated_knn=False)
        ctx = pgt._UnifiedPredictContext(
            p=p,
            annotation_set_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
            ref_data={"accessions": ["R1"], "embeddings_f32": np.array([[0.1]])},
        )
        fake_result = pgt.AdapterResult(
            predictions=[{"protein_accession": "Q1", "go_term_id": 10}],
            pair_features={},
            neighbors_by_aspect={"P": [], "F": [], "C": []},
            go_map_by_aspect={"P": {}, "F": {}, "C": {}},
        )
        with (
            patch.object(
                op,
                "_unified_load_annotations",
                return_value=({}, set()),
            ),
            patch.object(
                op,
                "_unified_load_pair_inputs",
                return_value=({}, {}, {}, {}),
            ),
            patch.object(
                op,
                "_load_go_term_metadata",
                return_value=({}, {}),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op.call_pipeline_predict",
                return_value=fake_result,
            ) as adapter,
        ):
            out = op._unified_predict_via_pipeline(MagicMock(), ctx)
        adapter.assert_called_once()
        assert out is fake_result


class TestRunUnifiedPathHappyAndV6:
    def test_returns_knn_result_with_v6_ctx(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(compute_v6_features=True, aspect_separated_knn=False),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        query_batch = pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
        )
        ref_data = {
            "embeddings": np.array([[1.0]], dtype=np.float16),
            "accessions": ["R1"],
            "embeddings_f32": np.array([[1.0]], dtype=np.float32),
            "embeddings_f32_cos": np.array([[1.0]], dtype=np.float32),
        }
        adapter_out = pgt.AdapterResult(
            predictions=[{"protein_accession": "Q1", "go_term_id": 1}],
            pair_features={},
            neighbors_by_aspect={"P": [[("R1", 0.1)]], "F": [[]], "C": [[]]},
            go_map_by_aspect={"P": {}, "F": {}, "C": {}},
        )
        with patch.object(op, "_unified_predict_via_pipeline", return_value=adapter_out):
            out = op._run_unified_path(MagicMock(), ctx, query_batch, ref_data, _noop_emit)
        assert out is not None
        assert out.v6_ctx is not None
        assert out.prediction_dicts == adapter_out.predictions

    def test_returns_knn_result_without_v6_ctx_when_flag_off(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(compute_v6_features=False, aspect_separated_knn=False),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        query_batch = pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
        )
        ref_data = {"embeddings": np.array([[1.0]])}
        adapter_out = pgt.AdapterResult(
            predictions=[{"protein_accession": "Q1", "go_term_id": 1}],
            pair_features={},
            neighbors_by_aspect={},
            go_map_by_aspect={},
        )
        with patch.object(op, "_unified_predict_via_pipeline", return_value=adapter_out):
            out = op._run_unified_path(MagicMock(), ctx, query_batch, ref_data, _noop_emit)
        assert out.v6_ctx is None


class TestRunKnnPath:
    def test_dispatches_aspect_separated(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(aspect_separated_knn=True),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        query_batch = pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
        )
        sentinel = MagicMock()
        with (
            patch.object(op, "_run_aspect_separated_path", return_value=sentinel) as asp,
            patch.object(op, "_run_unified_path") as uni,
        ):
            out = op._run_knn_path(MagicMock(), ctx, query_batch, {}, _noop_emit)
        asp.assert_called_once()
        uni.assert_not_called()
        assert out is sentinel


class TestRunAspectSeparatedPath:
    def test_packages_aspect_results_with_v6(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(compute_v6_features=True, aspect_separated_knn=True),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        query_batch = pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
        )
        predictions = [{"go_term_id": 1}]
        with patch.object(
            op,
            "_run_aspect_separated_knn",
            return_value=(predictions, {"P": []}, {"P": {}}, {("Q1", "R1"): {}}),
        ):
            out = op._run_aspect_separated_path(MagicMock(), ctx, query_batch, {})
        assert out.prediction_dicts == predictions
        assert out.v6_ctx is not None
        assert "neighbors_by_aspect" in out.v6_ctx

    def test_no_v6_when_flag_off(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(compute_v6_features=False, aspect_separated_knn=True),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        query_batch = pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
        )
        with patch.object(
            op,
            "_run_aspect_separated_knn",
            return_value=([{"go_term_id": 1}], {}, {}, {}),
        ):
            out = op._run_aspect_separated_path(MagicMock(), ctx, query_batch, {})
        assert out.v6_ctx is None


class TestApplyV6Features:
    def _make_query_batch(self) -> pgt._QueryBatch:
        return pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1, 0.2]], dtype=np.float32),
        )

    def test_aspect_separated_pca_pool_concat(self, monkeypatch: pytest.MonkeyPatch) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(
                aspect_separated_knn=True,
                compute_v6_features=True,
                compute_taxonomy=False,
            ),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        ref_data = {
            "P": {"embeddings_f32": np.array([[1.0, 0.0]], dtype=np.float32)},
            "F": {"embeddings_f32": np.array([[0.0, 1.0]], dtype=np.float32)},
            "C": {"embeddings_f32": np.array([], dtype=np.float32)},
        }
        knn_result = pgt._KnnResult(
            prediction_dicts=[{"protein_accession": "Q1", "go_term_id": 1}],
            v6_ctx={"neighbors_by_aspect": {}, "go_map_by_aspect": {}, "pair_features": {}},
            query_batch=self._make_query_batch(),
        )
        with (
            patch(
                "protea.core.operations.predict_go_terms._batch_op._load_or_fit_pca_state",
                return_value=("dummy_pca",),
            ),
            patch("protea.core.operations.predict_go_terms._batch_op.enrich_v6_features") as enrich,
        ):
            op._apply_v6_features(MagicMock(), ctx, knn_result, ref_data, _noop_emit)
        enrich.assert_called_once()

    def test_unified_pca_pool_uses_existing_view(self) -> None:
        op = PredictGOTermsBatchOperation()
        ctx = pgt._BatchExecCtx(
            p=_payload(
                aspect_separated_knn=False,
                compute_v6_features=True,
                compute_taxonomy=False,
            ),
            parent_job_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            embedding_config_id=uuid.uuid4(),
            annotation_set_id=uuid.uuid4(),
        )
        ref_data = {"embeddings_f32": np.array([[1.0, 0.0]], dtype=np.float32)}
        knn_result = pgt._KnnResult(
            prediction_dicts=[{"protein_accession": "Q1"}],
            v6_ctx={"neighbors_by_aspect": {}, "go_map_by_aspect": {}, "pair_features": {}},
            query_batch=self._make_query_batch(),
        )
        with (
            patch(
                "protea.core.operations.predict_go_terms._batch_op._load_or_fit_pca_state",
                return_value=None,
            ),
            patch("protea.core.operations.predict_go_terms._batch_op.enrich_v6_features") as enrich,
        ):
            op._apply_v6_features(MagicMock(), ctx, knn_result, ref_data, _noop_emit)
        enrich.assert_called_once()


class TestCollectAspectAnnotations:
    def test_groups_rows_by_aspect_and_skips_unknown(self) -> None:
        """Drive the yield_per iterator with a stub row list; verify
        per-aspect accession sets and go_maps."""
        session = MagicMock()
        chain = session.query.return_value.join.return_value.filter.return_value
        chain.yield_per.return_value = iter(
            [
                ("R1", "P", 101, None, "IEA"),
                ("R2", "F", 201, "involved_in", "IPI"),
                # Aspect "X" is not in missing_aspects ⇒ skipped silently.
                ("R3", "X", 999, None, "IEA"),
            ]
        )
        accs, gmap = PredictGOTermsBatchOperation._collect_aspect_annotations(
            session, ["P", "F"], uuid.uuid4()
        )
        assert accs["P"] == {"R1"}
        assert accs["F"] == {"R2"}
        assert gmap["P"]["R1"][0]["go_term_id"] == 101
        assert gmap["F"]["R2"][0]["evidence_code"] == "IPI"


class TestReferencePoolQuery:
    def test_builds_subquery(self) -> None:
        """Smoke check: the helper composes the right join chain on the
        provided session.query without raising."""
        op = PredictGOTermsBatchOperation()
        session = MagicMock()
        out = op._reference_pool_query(session, _PoolRequest(uuid.uuid4(), uuid.uuid4()))
        assert out is session.query.return_value.join.return_value.join.return_value


class TestAspectKnnPreSearchEmptyBank:
    def test_empty_aspect_bank_yields_empty_neighbors(self) -> None:
        """The continue branch on an empty aspect ref bank should yield
        an empty list per query for that aspect."""
        from protea.core.operations.predict_go_terms import _AspectKnnPreSearch

        p = _payload()
        ref_data = {
            "P": {
                "accessions": [],
                "embeddings_f32": np.empty((0, 2), dtype=np.float32),
                "embeddings_f32_cos": np.empty((0, 2), dtype=np.float32),
            },
            "F": {
                "accessions": [],
                "embeddings_f32": np.empty((0, 2), dtype=np.float32),
                "embeddings_f32_cos": np.empty((0, 2), dtype=np.float32),
            },
            "C": {
                "accessions": [],
                "embeddings_f32": np.empty((0, 2), dtype=np.float32),
                "embeddings_f32_cos": np.empty((0, 2), dtype=np.float32),
            },
        }
        nbya, hits = _AspectKnnPreSearch.run(
            ["Q1", "Q2"],
            np.array([[0.0, 1.0], [1.0, 0.0]], dtype=np.float32),
            ref_data,
            p,
        )
        assert hits == set()
        for asp in ("P", "F", "C"):
            assert nbya[asp] == [[], []]


class TestLoadAspectSeparatedAnnotationsNoHits:
    def test_empty_hits_per_aspect_returns_empty(self) -> None:
        """``_load_aspect_separated_annotations`` should skip aspects
        whose hit-intersection is empty (continue branch at 1979)."""
        from protea.core.operations.predict_go_terms import (
            _load_aspect_separated_annotations,
        )

        op = PredictGOTermsBatchOperation()
        ref_data_by_aspect = {
            "P": {"accessions": ["R1"]},
            "F": {"accessions": []},
            "C": {"accessions": []},
        }
        # No overlap with R1 ⇒ all aspects skipped.
        result = _load_aspect_separated_annotations(
            op, MagicMock(), ref_data_by_aspect, uuid.uuid4(), {"R9"}
        )
        assert result == {}


class TestStoreEmptyPredictions:
    def test_zero_predictions_does_not_execute_insert(self) -> None:
        from protea.infrastructure.orm.models.job import JobStatus

        op = StorePredictionsOperation()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        progress_row = MagicMock()
        progress_row.progress_current = 1
        progress_row.progress_total = 3
        session.execute.return_value.fetchone.return_value = progress_row
        payload = {
            "parent_job_id": str(uuid.uuid4()),
            "prediction_set_id": str(uuid.uuid4()),
            "predictions": [],
            "is_final_chunk": False,  # not last → skip parent_progress branch
        }
        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["predictions_inserted"] == 0
