"""Unit tests for the soft Pmin/Pmax DAG propagation post-processing.

Targets ``_soft_prop_protein`` / ``apply_softprop`` in
``protea.core.operations._run_cafa_softprop``. A tiny 3-term chain
(GO:child is_a GO:mid is_a GO:root) exercises the recursive Pmin (root->leaf)
and Pmax (leaf->root) blends and the in-place TSV rewrite.
"""

from __future__ import annotations

from protea.core.operations import _run_cafa_softprop as sp


def _emit(*_args, **_kwargs) -> None:
    return None


PAR = {"GO:child": {"GO:mid"}, "GO:mid": {"GO:root"}}
CH = {"GO:root": {"GO:mid"}, "GO:mid": {"GO:child"}}


def test_root_with_strong_child_keeps_high_score() -> None:
    # root: Pmin = own (no parents) = 0.2; Pmax = max(child Pmax)*0.7 + 0.2*0.3.
    scores = {"GO:root": 0.2, "GO:mid": 0.5, "GO:child": 0.9}
    out = sp._soft_prop_protein(scores, PAR, CH, {})
    # child is a leaf: Pmax(child)=0.9 (own); Pmin(child)=min(Pmin(mid))*0.7+0.9*0.3.
    assert set(out) == {"GO:root", "GO:mid", "GO:child"}
    # root gets lifted by its strong descendants via Pmax.
    assert out["GO:root"] > 0.2


def test_child_with_weak_ancestors_is_pulled_down() -> None:
    # child strong but ancestors weak -> Pmin pulls the child down below its own score.
    scores = {"GO:root": 0.05, "GO:mid": 0.05, "GO:child": 0.9}
    out = sp._soft_prop_protein(scores, PAR, CH, {})
    assert out["GO:child"] < 0.9  # discriminative suppression of an ill-supported leaf


def test_apply_softprop_rewrites_tsv(tmp_path) -> None:
    pred_dir = tmp_path / "preds"
    pred_dir.mkdir()
    f = pred_dir / "m.tsv"
    f.write_text("P1\tGO:root\t0.2\nP1\tGO:mid\t0.5\nP1\tGO:child\t0.9\n", encoding="utf-8")
    obo = tmp_path / "go.obo"
    obo.write_text(
        "[Term]\nid: GO:root\n\n"
        "[Term]\nid: GO:mid\nis_a: GO:root\n\n"
        "[Term]\nid: GO:child\nis_a: GO:mid\n",
        encoding="utf-8",
    )
    sp.apply_softprop(str(pred_dir), str(obo), _emit)
    lines = [ln for ln in f.read_text(encoding="utf-8").splitlines() if ln]
    assert len(lines) == 3
    # every line is protein\tgo_id\tscore with a finite positive score
    for ln in lines:
        p, g, s = ln.split("\t")
        assert p == "P1" and g.startswith("GO:") and float(s) > 0


def test_apply_softprop_missing_obo_is_noop(tmp_path) -> None:
    pred_dir = tmp_path / "preds"
    pred_dir.mkdir()
    f = pred_dir / "m.tsv"
    f.write_text("P1\tGO:root\t0.2\n", encoding="utf-8")
    sp.apply_softprop(str(pred_dir), str(tmp_path / "nope.obo"), _emit)
    assert f.read_text(encoding="utf-8") == "P1\tGO:root\t0.2\n"  # unchanged
