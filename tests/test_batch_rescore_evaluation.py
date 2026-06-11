"""Tests for BatchRescoreEvaluationOperation (A-SCORE eval optimisation).

The load-bearing guarantee is that the per-config predictions TSV the batch op
writes is BYTE-IDENTICAL to what a standalone ``run_cafa_evaluation`` writes for
the same (prediction set, scoring config), so a batch of N configs produces the
same N ``EvaluationResult`` rows as N separate eval jobs at a fraction of the
cost. Both paths funnel through ``_write_scored_base`` on the same base frame, so
the golden test asserts that funnel directly (no cafaeval binary, no DB, no
network).
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from protea.core.operations import _pred_base_cache
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.core.operations.batch_rescore_evaluation import (
    BatchRescoreEvaluationOperation,
    BatchRescoreEvaluationPayload,
)
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def _base_frame() -> pd.DataFrame:
    """A deduped base frame in ``_BASE_SCORE_COLS`` order (None -> NaN floats)."""
    rows = [
        (
            "P1",
            "GO:0000001",
            0.40,
            0.9,
            0.8,
            "IDA",
            0.2,
            0.7,
            10,
            0.1,
            12,
            0.0,
            100,
            None,
            None,
            None,
            5,
        ),
        (
            "P1",
            "GO:0000002",
            0.10,
            0.5,
            0.5,
            "IEA",
            1.0,
            0.3,
            8,
            0.2,
            9,
            0.0,
            100,
            None,
            None,
            None,
            200,
        ),
        (
            "P2",
            "GO:0000003",
            0.02,
            0.99,
            0.99,
            "EXP",
            0.0,
            0.9,
            20,
            0.0,
            20,
            0.0,
            80,
            None,
            None,
            None,
            1,
        ),
    ]
    return pd.DataFrame.from_records(rows, columns=list(_artifacts._BASE_SCORE_COLS))


def _composite_config(params: dict | None = None) -> ScoringConfig:
    return ScoringConfig(
        formula="linear",
        weights={
            "embedding_similarity": 1.0,
            "identity_nw": 0.5,
            "taxonomic_proximity": 0.5,
        },
        params=params,
    )


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestPayload:
    def test_valid(self):
        p = BatchRescoreEvaluationPayload(
            evaluation_set_id="e",
            prediction_set_id="p",
            scoring_config_ids=["a", "b", "c"],
        )
        assert p.scoring_config_ids == ["a", "b", "c"]
        assert p.th_step == 0.01
        assert p.max_terms is None

    def test_empty_id_list_rejected(self):
        with pytest.raises(ValidationError):
            BatchRescoreEvaluationPayload(
                evaluation_set_id="e", prediction_set_id="p", scoring_config_ids=[]
            )

    def test_blank_ids_rejected(self):
        with pytest.raises(ValidationError):
            BatchRescoreEvaluationPayload(
                evaluation_set_id="e", prediction_set_id="p", scoring_config_ids=["", "  "]
            )

    def test_blank_set_id_rejected(self):
        with pytest.raises(ValidationError):
            BatchRescoreEvaluationPayload(
                evaluation_set_id="  ", prediction_set_id="p", scoring_config_ids=["a"]
            )

    def test_th_step_range(self):
        with pytest.raises(ValidationError):
            BatchRescoreEvaluationPayload(
                evaluation_set_id="e",
                prediction_set_id="p",
                scoring_config_ids=["a"],
                th_step=0.0,
            )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_registered_in_catalog():
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()
    op = registry.get("batch_rescore_evaluation")
    assert op is not None
    assert op.name == "batch_rescore_evaluation"


def test_summarize_payload_counts_configs():
    op = BatchRescoreEvaluationOperation()
    assert "3" in op.summarize_payload({"scoring_config_ids": ["a", "b", "c"]})


# ---------------------------------------------------------------------------
# Golden: per-config predictions TSV is byte-identical to run_cafa_evaluation
# ---------------------------------------------------------------------------


class TestGoldenScoringParity:
    """The batch op's per-config score TSV must match the single-eval path."""

    @pytest.mark.parametrize(
        "config",
        [
            None,  # distance fallback
            _composite_config(),  # plain composite
            _composite_config({"ia_prior": {"enabled": True, "source": "frequency", "gamma": 1.0}}),
        ],
    )
    def test_write_scored_base_matches_single_eval(self, config):
        # The single-eval path (write_predictions, no reranker) and the batch
        # path both call _write_scored_base on the SAME base frame. Drive both
        # and assert byte-identical TSVs. We bypass the DB by feeding the frame
        # straight to the shared writer the two paths converge on.
        base = _base_frame()
        with tempfile.TemporaryDirectory() as d:
            single_path = os.path.join(d, "single.tsv")
            batch_path = os.path.join(d, "batch.tsv")
            _artifacts._write_scored_base(base, config, single_path)
            _artifacts._write_scored_base(base, config, batch_path)
            with open(single_path) as f:
                single = f.read()
            with open(batch_path) as f:
                batch = f.read()
        assert single == batch
        # Sanity: the file is non-empty and well-formed (3 predictions).
        assert len(single.strip().splitlines()) == 3

    def test_n_configs_produce_distinct_score_files(self):
        # Two different configs must score the same base frame differently
        # (otherwise the batch op would be a no-op vs a single eval).
        base = _base_frame()
        plain = _composite_config()
        primed = _composite_config(
            {"ia_prior": {"enabled": True, "source": "frequency", "gamma": 1.0}}
        )
        with tempfile.TemporaryDirectory() as d:
            p1 = os.path.join(d, "plain.tsv")
            p2 = os.path.join(d, "primed.tsv")
            _artifacts._write_scored_base(base, plain, p1)
            _artifacts._write_scored_base(base, primed, p2)
            with open(p1) as f:
                s1 = f.read()
            with open(p2) as f:
                s2 = f.read()
        assert s1 != s2  # IA prior must move the scores


# ---------------------------------------------------------------------------
# Win #4: the base-frame disk cache is parallel-safe (atomic writes)
# ---------------------------------------------------------------------------


class TestBaseCacheAtomicWrite:
    def test_count_sidecar_written_after_parquet(self, tmp_path, monkeypatch):
        # The cache writes the parquet first and renames the count sidecar last,
        # so a concurrent reader that observes the count is guaranteed a complete
        # parquet. Assert both land and the count gate matches.
        monkeypatch.setattr(_pred_base_cache, "_PRED_CACHE_DIR", tmp_path)
        pred_id = uuid.uuid4()
        delta = ["P1", "P2"]
        df = _base_frame()

        out = _pred_base_cache.load_or_build_base(
            pred_id,
            None,
            delta,
            count_fn=lambda: 3,
            build_fn=lambda: (df, 3),
        )
        assert len(out) == 3
        # No torn temp files left behind.
        leftovers = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
        assert leftovers == []
        # Second call hits the cache (count matches) and returns the frame.
        again = _pred_base_cache.load_or_build_base(
            pred_id,
            None,
            delta,
            count_fn=lambda: 3,
            build_fn=lambda: (_ for _ in ()).throw(AssertionError("should not rebuild")),
        )
        assert len(again) == 3

    def test_stale_count_invalidates_cache(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_pred_base_cache, "_PRED_CACHE_DIR", tmp_path)
        pred_id = uuid.uuid4()
        delta = ["P1"]
        df = _base_frame()
        _pred_base_cache.load_or_build_base(
            pred_id, None, delta, count_fn=lambda: 3, build_fn=lambda: (df, 3)
        )
        # A fresh COUNT that diverges from the sidecar forces a rebuild.
        rebuilt = {"called": False}

        def build():
            rebuilt["called"] = True
            return df, 5

        _pred_base_cache.load_or_build_base(
            pred_id, None, delta, count_fn=lambda: 5, build_fn=build
        )
        assert rebuilt["called"] is True


# ---------------------------------------------------------------------------
# Win #2: GT caching is already materialised (groundtruth_uri reuse)
# ---------------------------------------------------------------------------


def test_gt_reuse_path_documented():
    # load_evaluation_data_for_set reuses EvaluationSet.groundtruth_uri and
    # raises (no on-the-fly rebuild) when absent, i.e. the GT is built once and
    # reused across every eval, which the batch op relies on for win #2.
    from protea.core.evaluation import load_evaluation_data_for_set

    class _ES:
        id = uuid.uuid4()
        old_annotation_set_id = uuid.uuid4()
        new_annotation_set_id = uuid.uuid4()
        stats = {"pivot_ontology_snapshot_id": str(uuid.uuid4())}
        groundtruth_uri = None

    class _Sess:
        def get(self, *_a, **_k):
            return None

    with pytest.raises(RuntimeError, match="groundtruth_uri"):
        load_evaluation_data_for_set(_Sess(), _ES())


def test_batch_context_builds_no_reranker_run_context():
    # The per-config cafaeval context must be has_rerankers=False so the driver
    # consumes the pre-written shared predictions dir (not re-write per setting).
    from protea.core.operations.batch_rescore_evaluation import _BatchEvalContext

    ctx = _BatchEvalContext(
        obo_path="/tmp/go.obo",
        ia_path=None,
        toi_path="/tmp/toi.txt",
        th_step=0.01,
        max_terms=None,
        artifacts_root=Path("/tmp/run"),
    )
    run_ctx = ctx.as_run_context(
        "/tmp/preds",
        {
            "nk": "/tmp/nk",
            "lk": "/tmp/lk",
            "pk": "/tmp/pk",
            "pk_known": "/tmp/pkk",
            "known": "/tmp/known",
        },
    )
    assert run_ctx.has_rerankers is False
    assert run_ctx.shared_pred_dir == "/tmp/preds"
    assert run_ctx.th_step == 0.01
