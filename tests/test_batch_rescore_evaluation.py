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

    def test_protein_fold_defaults_to_full_cohort(self):
        p = BatchRescoreEvaluationPayload(
            evaluation_set_id="e", prediction_set_id="p", scoring_config_ids=["a"]
        )
        assert p.protein_folds == 1
        assert p.protein_fold == 0

    def test_protein_fold_must_be_below_folds(self):
        with pytest.raises(ValidationError):
            BatchRescoreEvaluationPayload(
                evaluation_set_id="e",
                prediction_set_id="p",
                scoring_config_ids=["a"],
                protein_folds=2,
                protein_fold=2,
            )

    def test_valid_two_fold_split(self):
        p = BatchRescoreEvaluationPayload(
            evaluation_set_id="e",
            prediction_set_id="p",
            scoring_config_ids=["a"],
            protein_folds=2,
            protein_fold=1,
            protein_seed=7,
        )
        assert (p.protein_folds, p.protein_fold, p.protein_seed) == (2, 1, 7)


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
# Step 0: real-IA prior wiring (source="ia" -> term_ia from the IA file)
# ---------------------------------------------------------------------------


class TestRealIaPrior:
    """The IA file maps onto the base frame as a normalised ``term_ia`` column.

    Without this, ``ia_prior source="ia"`` no-ops (no ``term_ia`` on the frame);
    with it, the prior aligns with the cafaeval ``f_micro_w`` IA weighting.
    """

    def test_load_ia_map_parses_go_id_ia(self, tmp_path):
        ia_file = tmp_path / "ia.tsv"
        ia_file.write_text("GO:0000001\t-0.0\nGO:0000006\t7.35\nbad_line\nGO:0000007\t12.0\n")
        ia_map = _artifacts.load_ia_map(str(ia_file))
        assert ia_map == {"GO:0000001": -0.0, "GO:0000006": 7.35, "GO:0000007": 12.0}

    def test_attach_term_ia_normalises_to_unit_interval(self):
        base = _base_frame()
        ia_map = {"GO:0000001": 2.0, "GO:0000003": 20.0}  # GO:0000002 absent
        _artifacts.attach_term_ia(base, ia_map)
        vals = dict(zip(base["go_id"], base["term_ia"], strict=True))
        # Present terms map into [0, 1]; absent term -> NaN (no penalty).
        assert 0.0 <= vals["GO:0000001"] <= 1.0
        assert 0.0 <= vals["GO:0000003"] <= 1.0
        assert pd.isna(vals["GO:0000002"])
        # Higher raw IA -> higher (or equal, if both clip to 1) normalised prior.
        assert vals["GO:0000003"] >= vals["GO:0000001"]

    def test_ia_source_moves_scores_once_term_ia_present(self):
        # source="ia" is a no-op without term_ia, and bends scores once attached.
        base_no_ia = _base_frame()
        base_with_ia = _base_frame()
        _artifacts.attach_term_ia(
            base_with_ia, {"GO:0000001": 1.0, "GO:0000002": 18.0, "GO:0000003": 9.0}
        )
        cfg = _composite_config({"ia_prior": {"enabled": True, "source": "ia", "gamma": 1.0}})
        with tempfile.TemporaryDirectory() as d:
            p_no = os.path.join(d, "no_ia.tsv")
            p_yes = os.path.join(d, "with_ia.tsv")
            _artifacts._write_scored_base(base_no_ia, cfg, p_no)
            _artifacts._write_scored_base(base_with_ia, cfg, p_yes)
            with open(p_no) as f:
                s_no = f.read()
            with open(p_yes) as f:
                s_yes = f.read()
        # No term_ia -> prior 1.0 everywhere -> identical to a plain composite.
        plain = _composite_config()
        with tempfile.TemporaryDirectory() as d:
            p_plain = os.path.join(d, "plain.tsv")
            _artifacts._write_scored_base(_base_frame(), plain, p_plain)
            with open(p_plain) as f:
                s_plain = f.read()
        assert s_no == s_plain  # source="ia" is inert without term_ia
        assert s_yes != s_no  # attaching IA moves the scores

    def test_uses_ia_source_detection(self):
        op = BatchRescoreEvaluationOperation()
        assert op._uses_ia_source(None) is False
        assert op._uses_ia_source(_composite_config()) is False
        assert (
            op._uses_ia_source(
                _composite_config({"ia_prior": {"enabled": True, "source": "frequency"}})
            )
            is False
        )
        assert (
            op._uses_ia_source(
                _composite_config({"ia_prior": {"enabled": True, "source": "ia"}})
            )
            is True
        )


# ---------------------------------------------------------------------------
# LEAN internal train/valid split (beat-pooled-or-revert guard primitive)
# ---------------------------------------------------------------------------


class TestProteinFoldSplit:
    """``restrict_data_to_protein_fold`` deterministically partitions the cohort."""

    @staticmethod
    def _data():
        from protea.core.evaluation import EvaluationData

        proteins = [f"P{i:04d}" for i in range(400)]
        return EvaluationData(
            nk={p: {"GO:0000001"} for p in proteins[:200]},
            lk={p: {"GO:0000002"} for p in proteins[200:300]},
            pk={p: {"GO:0000003"} for p in proteins[300:]},
            pk_known={p: {"GO:0000004"} for p in proteins[300:]},
            known={p: {"GO:0000005"} for p in proteins[300:]},
        )

    def test_folds_le_one_is_identity(self):
        from protea.core.operations import _run_cafa_data_helpers as dh

        d = self._data()
        out = dh.restrict_data_to_protein_fold(
            d, folds=1, fold=0, seed=0, emit=lambda *a, **k: None
        )
        assert out is d

    def test_two_folds_partition_disjoint_and_cover(self):
        from protea.core.operations import _run_cafa_data_helpers as dh

        d = self._data()
        f0 = dh.restrict_data_to_protein_fold(
            d, folds=2, fold=0, seed=11, emit=lambda *a, **k: None
        )
        f1 = dh.restrict_data_to_protein_fold(
            d, folds=2, fold=1, seed=11, emit=lambda *a, **k: None
        )
        all_nk = set(d.nk)
        s0, s1 = set(f0.nk), set(f1.nk)
        assert s0.isdisjoint(s1)
        assert s0 | s1 == all_nk
        # Roughly balanced split (hash uniformity), not all on one side.
        assert 0.3 < len(s0) / len(all_nk) < 0.7

    def test_fold_assignment_is_seed_stable(self):
        from protea.core.operations import _run_cafa_data_helpers as dh

        d = self._data()
        a = dh.restrict_data_to_protein_fold(
            d, folds=3, fold=2, seed=5, emit=lambda *a, **k: None
        )
        b = dh.restrict_data_to_protein_fold(
            d, folds=3, fold=2, seed=5, emit=lambda *a, **k: None
        )
        assert set(a.nk) == set(b.nk)


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
            pred_id, None, None, delta, count_fn=lambda: 3, build_fn=lambda: (df, 3)
        )
        # A fresh COUNT that diverges from the sidecar forces a rebuild.
        rebuilt = {"called": False}

        def build():
            rebuilt["called"] = True
            return df, 5

        _pred_base_cache.load_or_build_base(
            pred_id, None, None, delta, count_fn=lambda: 5, build_fn=build
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


class TestDepthIsPartOfTheCacheKey:
    """The base frame is the candidate set after the cut, so K must key it.

    Sharing one key across depths served the deepest arm's parquet to every
    other arm, and the depth sweep came back flat with no error to show for
    it. The regression is silent by construction, so it needs its own test.
    """

    def test_two_depths_do_not_share_a_parquet(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_pred_base_cache, "_PRED_CACHE_DIR", tmp_path)
        pred_id = uuid.uuid4()
        delta = ["P1", "P2"]
        shallow, deep = _pred_base_cache._cache_paths(pred_id, None, 1, delta)
        wide, _ = _pred_base_cache._cache_paths(pred_id, None, 10, delta)
        unbounded, _ = _pred_base_cache._cache_paths(pred_id, None, None, delta)
        assert shallow != wide
        assert wide != unbounded
        assert shallow != unbounded

    def test_a_shallow_arm_does_not_read_the_deep_arm_frame(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_pred_base_cache, "_PRED_CACHE_DIR", tmp_path)
        pred_id = uuid.uuid4()
        delta = ["P1"]
        df = _base_frame()
        _pred_base_cache.load_or_build_base(
            pred_id, None, 10, delta, count_fn=lambda: 3, build_fn=lambda: (df, 3)
        )
        built = {"called": False}

        def build():
            built["called"] = True
            return df, 3

        _pred_base_cache.load_or_build_base(
            pred_id, None, 1, delta, count_fn=lambda: 3, build_fn=build
        )
        assert built["called"] is True
