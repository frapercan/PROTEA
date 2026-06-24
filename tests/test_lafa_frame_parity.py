"""LAFA-frame parity regression tests for ``run_cafa_evaluation``.

These exercise the *real* ``cafaeval`` binary (a hard dependency of
PROTEA) rather than mocking it, to pin that PROTEA's per-setting driver
(:func:`protea.core.operations._run_cafa_eval_driver._invoke_cafaeval_signal_safe`)
scores predictions in the **exact same frame** as the LAFA
(CAFA_forever) leaderboard scorer:

    cafaeval <OBO> <PREDS_DIR> <gt> -ia <IA> -toi <TOI> \
        -prop fill -norm cafa -no_orphans [-known <PK_known>]   # th_step=0.01

There are two layers:

1. A hermetic synthetic-ontology test (always runs in CI). It builds a
   tiny OBO + IA + ground truth + predictions, scores them once through
   PROTEA's driver and once through a direct LAFA-style ``cafa_eval``
   call, and asserts the two agree to the bit. It also asserts that the
   ``th_step`` knob is load-bearing (a finer grid changes the number),
   so a future regression that silently re-pins ``th_step`` away from
   LAFA's default ``0.01`` is caught.

2. An opt-in real-leaderboard parity test (skipped unless the LAFA
   release files and the PROTEA-KNN prediction are present on disk). It
   re-scores ``protea-lafa-knn/predictions_7401.tsv`` on the
   ``Sep_2025_Mar_2026`` window and asserts every one of the 9 NK/LK/PK
   x BP/CC/MF cells matches the published LAFA leaderboard
   ``f_micro_w`` within LAFA's own 3-decimal rounding. This is the
   end-to-end proof of frame parity; see ``docs/EVAL_LAFA_PARITY.md``.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

import pytest

from protea.core.evaluation import EvaluationData
from protea.core.operations import _run_cafa_eval_driver as driver

pytest.importorskip("cafaeval")


# ---------------------------------------------------------------------------
# Synthetic ontology fixtures
# ---------------------------------------------------------------------------

# A tiny single-namespace ontology with a root, a mid node, and three
# leaves so True-Path-Rule propagation and IA weighting have something
# non-trivial to do.
_OBO = """format-version: 1.2

[Term]
id: GO:0000001
name: bp_root
namespace: biological_process

[Term]
id: GO:0000002
name: bp_mid
namespace: biological_process
is_a: GO:0000001 ! bp_root

[Term]
id: GO:0000003
name: bp_leaf_a
namespace: biological_process
is_a: GO:0000002 ! bp_mid

[Term]
id: GO:0000004
name: bp_leaf_b
namespace: biological_process
is_a: GO:0000002 ! bp_mid

[Term]
id: GO:0000005
name: bp_leaf_c
namespace: biological_process
is_a: GO:0000002 ! bp_mid
"""

# Information accretion: root collapses to 0, deeper terms accrete more.
_IA = "GO:0000001\t0.0\nGO:0000002\t1.0\nGO:0000003\t2.5\nGO:0000004\t2.0\nGO:0000005\t3.0\n"

# Terms of interest = the full term universe (LAFA passes a release file;
# here the synthetic universe is small enough to enumerate).
_TOI = "GO:0000001\nGO:0000002\nGO:0000003\nGO:0000004\nGO:0000005\n"

# Ground truth (leaf annotations; cafaeval propagates up under prop=fill).
_GT_NK = "P1\tGO:0000003\nP2\tGO:0000004\nP3\tGO:0000003\nP3\tGO:0000005\nP4\tGO:0000004\n"

# A protein's *known* (t0) terms, used for the PK -known exclusion path.
# These overlap the propagated ground truth, so excluding them must move
# the score (see ``test_known_exclusion_changes_the_score``).
_PK_KNOWN = "P1\tGO:0000003\nP3\tGO:0000003\n"

# Predictions: protein, term, score (CAFA 3-col format). Scores are
# deliberately *off* the 0.01 threshold grid (e.g. 0.9035, 0.5512) and
# carry false positives at intermediate scores, so the optimal tau is
# sensitive to the grid resolution and the th_step knob is load-bearing
# (see ``test_th_step_is_load_bearing``).
_PREDS = (
    "P1\tGO:0000003\t0.9035\n"
    "P1\tGO:0000004\t0.5512\n"
    "P1\tGO:0000005\t0.2071\n"
    "P2\tGO:0000004\t0.8123\n"
    "P2\tGO:0000003\t0.6234\n"
    "P2\tGO:0000005\t0.3157\n"
    "P3\tGO:0000003\t0.9567\n"
    "P3\tGO:0000005\t0.5543\n"
    "P3\tGO:0000004\t0.4089\n"
    "P4\tGO:0000004\t0.7088\n"
    "P4\tGO:0000003\t0.4512\n"
    "P4\tGO:0000005\t0.2533\n"
)


@pytest.fixture()
def synthetic_frame(tmp_path: Path) -> dict[str, str]:
    """Materialise a tiny self-contained LAFA-style scoring frame on disk."""
    obo = tmp_path / "go.obo"
    obo.write_text(_OBO)
    ia = tmp_path / "IA.tsv"
    ia.write_text(_IA)
    toi = tmp_path / "toi.txt"
    toi.write_text(_TOI)
    gt = tmp_path / "gt_NK.tsv"
    gt.write_text(_GT_NK)
    pk_known = tmp_path / "pk_known.tsv"
    pk_known.write_text(_PK_KNOWN)

    pred_dir = tmp_path / "predictions"
    pred_dir.mkdir()
    (pred_dir / "model.tsv").write_text(_PREDS)

    return {
        "obo": str(obo),
        "ia": str(ia),
        "toi": str(toi),
        "gt": str(gt),
        "pk_known": str(pk_known),
        "pred_dir": str(pred_dir),
    }


def _make_ctx(frame: dict[str, str], **overrides):
    base = dict(
        pred_set_id=uuid.uuid4(),
        delta_proteins=set(),
        max_distance=None,
        artifacts_root=Path("/tmp"),
        has_rerankers=False,
        reranker_models={},
        scoring_config_snapshot=None,
        data=EvaluationData(),
        obo_path=frame["obo"],
        nk_path=frame["gt"],
        lk_path=frame["gt"],
        pk_path=frame["gt"],
        pk_known_path=frame["pk_known"],
        ia_path=frame["ia"],
        toi_path=frame["toi"],
        shared_pred_dir=frame["pred_dir"],
    )
    base.update(overrides)
    return driver.CafaEvalRunContext(**base)


def _f_micro_w_by_ns(dfs_best) -> dict[str, float]:
    best = dfs_best["f_micro_w"].reset_index()
    return {str(r["ns"]): round(float(r["f_micro_w"]), 6) for _, r in best.iterrows()}


def _lafa_reference(frame: dict[str, str], *, known: str | None) -> dict[str, float]:
    """Score the synthetic frame the way CAFA_forever's evaluation.nf does."""
    from cafaeval.evaluation import cafa_eval

    _df, dfs_best = cafa_eval(
        frame["obo"],
        frame["pred_dir"],
        frame["gt"],
        ia=frame["ia"],
        exclude=known,
        prop="fill",
        norm="cafa",
        no_orphans=True,
        toi_file=frame["toi"],
        max_terms=None,
        th_step=0.01,
        n_cpu=1,
    )
    return _f_micro_w_by_ns(dfs_best)


class TestSyntheticFrameParity:
    """PROTEA's driver call must equal a direct LAFA-flag ``cafa_eval`` call."""

    def test_nk_driver_matches_lafa_invocation(self, synthetic_frame):
        ctx = _make_ctx(synthetic_frame)
        _df, dfs_best = driver._invoke_cafaeval_signal_safe(
            ctx=ctx,
            pred_dir=synthetic_frame["pred_dir"],
            gt_file=synthetic_frame["gt"],
            known_file=None,
        )
        protea = _f_micro_w_by_ns(dfs_best)
        reference = _lafa_reference(synthetic_frame, known=None)
        assert protea == reference
        # Sanity: the frame actually produced non-trivial IA-weighted scores
        # (otherwise an all-zero result would match vacuously).
        assert any(v > 0.0 for v in protea.values())

    def test_pk_driver_matches_lafa_invocation_with_known_exclusion(self, synthetic_frame):
        ctx = _make_ctx(synthetic_frame)
        _df, dfs_best = driver._invoke_cafaeval_signal_safe(
            ctx=ctx,
            pred_dir=synthetic_frame["pred_dir"],
            gt_file=synthetic_frame["gt"],
            known_file=synthetic_frame["pk_known"],
        )
        protea = _f_micro_w_by_ns(dfs_best)
        reference = _lafa_reference(synthetic_frame, known=synthetic_frame["pk_known"])
        assert protea == reference

    def test_known_exclusion_changes_the_score(self, synthetic_frame):
        """The PK ``-known`` path must actually exclude known terms.

        Guards against a regression where the known file is dropped on the
        floor (which would silently turn PK into NK).
        """
        ctx = _make_ctx(synthetic_frame)
        _df_nk, best_nk = driver._invoke_cafaeval_signal_safe(
            ctx=ctx,
            pred_dir=synthetic_frame["pred_dir"],
            gt_file=synthetic_frame["gt"],
            known_file=None,
        )
        _df_pk, best_pk = driver._invoke_cafaeval_signal_safe(
            ctx=ctx,
            pred_dir=synthetic_frame["pred_dir"],
            gt_file=synthetic_frame["gt"],
            known_file=synthetic_frame["pk_known"],
        )
        assert _f_micro_w_by_ns(best_nk) != _f_micro_w_by_ns(best_pk)

    def test_th_step_is_load_bearing(self, synthetic_frame):
        """A finer ``th_step`` than LAFA's 0.01 must change the number.

        This is the dominant historical gap (PROTEA ran ``th_step=0.001``,
        inflating ``f_micro_w`` by up to +0.014). If this assertion ever
        starts failing, the threshold grid no longer affects scoring and
        the parity guarantee is meaningless.
        """
        ctx_lafa = _make_ctx(synthetic_frame, th_step=0.01)
        ctx_fine = _make_ctx(synthetic_frame, th_step=0.001)
        _d1, best_lafa = driver._invoke_cafaeval_signal_safe(
            ctx=ctx_lafa,
            pred_dir=synthetic_frame["pred_dir"],
            gt_file=synthetic_frame["gt"],
            known_file=None,
        )
        _d2, best_fine = driver._invoke_cafaeval_signal_safe(
            ctx=ctx_fine,
            pred_dir=synthetic_frame["pred_dir"],
            gt_file=synthetic_frame["gt"],
            known_file=None,
        )
        assert _f_micro_w_by_ns(best_lafa) != _f_micro_w_by_ns(best_fine)


# ---------------------------------------------------------------------------
# Opt-in: real LAFA leaderboard parity (skipped in CI; the proof on record)
# ---------------------------------------------------------------------------

_THESIS_ROOT = Path("/home/frapercan/Thesis2")
_LAFA_RELEASE = _THESIS_ROOT / "CAFA_forever/data/releases/Sep_2025_Mar_2026"
_LAFA_T0 = _THESIS_ROOT / "protea-lafa-knn/lafa_t0_Sep_2025"
_LAFA_PRED = _THESIS_ROOT / "protea-lafa-knn/predictions_7401.tsv"

# Published LAFA leaderboard f_micro_w for the PROTEA-KNN method
# (protea-knn-v1.tsv), read from results_{NK,LK,PK}/
# evaluation_best_f_micro_w.tsv column 31 on the Sep_2025_Mar_2026 window.
_LEADERBOARD = {
    "NK": {"biological_process": 0.263, "cellular_component": 0.407, "molecular_function": 0.579},
    "LK": {"biological_process": 0.284, "cellular_component": 0.332, "molecular_function": 0.489},
    "PK": {"biological_process": 0.075, "cellular_component": 0.184, "molecular_function": 0.191},
}

_LAFA_FILES_PRESENT = _LAFA_RELEASE.is_dir() and _LAFA_T0.is_dir() and _LAFA_PRED.is_file()


@pytest.mark.skipif(
    not _LAFA_FILES_PRESENT,
    reason=(
        "real LAFA leaderboard files not present; this is the opt-in "
        "end-to-end proof, run locally with the Sep_2025_Mar_2026 release "
        "and protea-lafa-knn checkout on disk"
    ),
)
def test_real_lafa_leaderboard_parity(tmp_path: Path):
    """PROTEA's driver reproduces the LAFA leaderboard within 3-decimal rounding."""
    obo = str(_LAFA_T0 / "go-basic.obo")
    ia = str(_LAFA_T0 / "IA.tsv")
    toi = str(_LAFA_RELEASE / "groundtruth_terms_of_interest.txt")

    for tier, gt_name, known_name in [
        ("NK", "groundtruth_NK.tsv", None),
        ("LK", "groundtruth_LK.tsv", None),
        ("PK", "groundtruth_PK.tsv", "groundtruth_PK_known.tsv"),
    ]:
        pred_dir = tmp_path / f"pred_{tier}"
        pred_dir.mkdir()
        shutil.copy(str(_LAFA_PRED), os.path.join(str(pred_dir), "protea-knn-v1.tsv"))
        gt = str(_LAFA_RELEASE / gt_name)
        known = str(_LAFA_RELEASE / known_name) if known_name else None

        ctx = driver.CafaEvalRunContext(
            pred_set_id=uuid.uuid4(),
            delta_proteins=set(),
            max_distance=None,
            artifacts_root=tmp_path,
            has_rerankers=False,
            reranker_models={},
            scoring_config_snapshot=None,
            data=EvaluationData(),
            obo_path=obo,
            nk_path=gt,
            lk_path=gt,
            pk_path=gt,
            pk_known_path=known or gt,
            ia_path=ia,
            toi_path=toi,
            shared_pred_dir=str(pred_dir),
        )
        _df, dfs_best = driver._invoke_cafaeval_signal_safe(
            ctx=ctx, pred_dir=str(pred_dir), gt_file=gt, known_file=known
        )
        got = _f_micro_w_by_ns(dfs_best)
        for ns, expected in _LEADERBOARD[tier].items():
            assert ns in got, f"{tier}/{ns} missing from PROTEA result"
            assert got[ns] == pytest.approx(expected, abs=1e-3), (
                f"{tier}/{ns}: PROTEA-aligned {got[ns]:.4f} vs leaderboard {expected:.3f}"
            )
