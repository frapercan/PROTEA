"""Unit tests for the InterPro2GO BP-only graft post-processing.

Targets ``_interpro_graded`` / ``_graft_protein`` / ``apply_interpro_graft`` in
``protea.core.operations._run_cafa_interpro_graft``. A tiny synthetic
protein2ipr + ipr2go map over a 3-term OBO (one BP, one MF, one CC) exercises
the naive-max BP blend, the BP-term addition, the MF/CC untouched invariant,
and the noisy-OR variant, plus the missing-file skip.
"""

from __future__ import annotations

import json

from protea.core.operations import _run_cafa_interpro_graft as ig


def _emit(*_args, **_kwargs) -> None:
    return None


# GO:bp = biological_process, GO:mf = molecular_function, GO:cc = cellular_component
OBO_TEXT = (
    "[Term]\nid: GO:bp\nnamespace: biological_process\n\n"
    "[Term]\nid: GO:bp2\nnamespace: biological_process\n\n"
    "[Term]\nid: GO:mf\nnamespace: molecular_function\n\n"
    "[Term]\nid: GO:cc\nnamespace: cellular_component\n"
)


def _write_obo(tmp_path):
    obo = tmp_path / "go.obo"
    obo.write_text(OBO_TEXT, encoding="utf-8")
    return obo


def test_bp_go_ids_parses_only_biological_process(tmp_path) -> None:
    obo = _write_obo(tmp_path)
    assert ig._bp_go_ids(str(obo)) == {"GO:bp", "GO:bp2"}


def test_interpro_graded_is_support_fraction() -> None:
    protein2ipr = {"P1": ["IPR1", "IPR2"], "P2": ["IPRX"]}
    ipr2go = {"IPR1": ["GO:bp", "GO:mf"], "IPR2": ["GO:bp"]}
    graded = ig._interpro_graded(protein2ipr, ipr2go)
    # P1 maps to 2 entries; GO:bp supported by both (2/2), GO:mf by one (1/2).
    assert graded["P1"]["GO:bp"] == 1.0
    assert graded["P1"]["GO:mf"] == 0.5
    # P2's only IPR has no GO mapping -> dropped entirely.
    assert "P2" not in graded


def test_graft_protein_naive_max_bp_only() -> None:
    base = {"GO:bp": 0.3, "GO:mf": 0.4, "GO:cc": 0.6}
    graded = {"GO:bp": 0.8, "GO:bp2": 0.5, "GO:mf": 0.9, "GO:cc": 0.9}
    out = ig._graft_protein(base, graded, {"GO:bp", "GO:bp2"}, weight=None)
    assert out["GO:bp"] == 0.8  # max(0.3, 0.8)
    assert out["GO:bp2"] == 0.5  # new BP candidate added
    assert out["GO:mf"] == 0.4  # MF untouched (interpro ignored)
    assert out["GO:cc"] == 0.6  # CC untouched


def test_graft_protein_noisy_or_bp() -> None:
    base = {"GO:bp": 0.5}
    out = ig._graft_protein(base, {"GO:bp": 0.5}, {"GO:bp"}, weight=0.5)
    # 1 - (1 - 0.5)(1 - 0.5*0.5) = 1 - 0.5*0.75 = 0.625
    assert abs(out["GO:bp"] - 0.625) < 1e-9


def test_apply_off_is_identity_when_files_missing(tmp_path) -> None:
    pred_dir = tmp_path / "preds"
    pred_dir.mkdir()
    f = pred_dir / "m.tsv"
    original = "P1\tGO:bp\t0.3\nP1\tGO:mf\t0.4\n"
    f.write_text(original, encoding="utf-8")
    obo = _write_obo(tmp_path)
    ig.apply_interpro_graft(str(pred_dir), str(obo), None, None, None, _emit)
    assert f.read_text(encoding="utf-8") == original  # unchanged, no crash


def test_apply_grafts_bp_and_leaves_mf_cc(tmp_path) -> None:
    pred_dir = tmp_path / "preds"
    pred_dir.mkdir()
    f = pred_dir / "m.tsv"
    f.write_text("P1\tGO:bp\t0.3\nP1\tGO:mf\t0.4\nP1\tGO:cc\t0.6\n", encoding="utf-8")
    obo = _write_obo(tmp_path)
    p2i = tmp_path / "p2i.json"
    p2i.write_text(json.dumps({"P1": ["IPR1", "IPR2"]}), encoding="utf-8")
    i2g = tmp_path / "i2g.json"
    # Both IPRs vote GO:bp (2/2 -> 1.0); IPR1 also adds GO:bp2 (1/2 -> 0.5) and
    # GO:mf (1/2 -> 0.5) and GO:cc (1/2 -> 0.5).
    i2g.write_text(
        json.dumps({"IPR1": ["GO:bp", "GO:bp2", "GO:mf", "GO:cc"], "IPR2": ["GO:bp"]}),
        encoding="utf-8",
    )
    ig.apply_interpro_graft(str(pred_dir), str(obo), str(p2i), str(i2g), None, _emit)
    rows = {}
    for ln in f.read_text(encoding="utf-8").splitlines():
        if not ln:
            continue
        prot, go_id, score = ln.split("\t")
        assert prot == "P1"
        rows[go_id] = float(score)
    assert abs(rows["GO:bp"] - 1.0) < 1e-9  # max(0.3, 1.0)
    assert abs(rows["GO:bp2"] - 0.5) < 1e-9  # new BP candidate grafted
    assert abs(rows["GO:mf"] - 0.4) < 1e-9  # MF untouched (no interpro graft)
    assert abs(rows["GO:cc"] - 0.6) < 1e-9  # CC untouched
