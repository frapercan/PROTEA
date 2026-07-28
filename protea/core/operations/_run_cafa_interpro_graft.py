"""InterPro2GO BP-only graft post-processing for cafaeval (offline ``naivemax_bponly``).

A scorer-agnostic post-processing arm over the prediction frame, applied per
protein right before cafaeval, mirroring :func:`apply_softprop`. For each protein
it grafts InterPro2GO evidence onto the biological-process (BP) aspect only:

* BP terms are blended with the InterPro graded score, ``max(base, graded)`` by
  default (parameter-free naive max), or noisy-OR ``1 - (1 - base)(1 - w*graded)``
  when a per-aspect weight ``w`` is supplied.
* BP terms InterPro predicts but the base scorer missed are ADDED as new
  candidates (union, not just rescore).
* MF / CC terms are left byte-identical (the graft never touches them).

Ported from the reranker-lab offline reference
(``interpro_lib.interpro_preds`` + ``apply_and_score.build_blend_rows`` with
``rule='max'`` and BP-only weights), the shipped ``naivemax_bponly`` graft that
lifts the board-faithful 9-cell mean f_micro_w from 0.3884 to 0.4063.

The InterPro graded score for a protein reproduces the offline recipe: with the
protein mapping to ``n`` InterPro entries that carry a GO mapping, a GO term
supported by ``c`` of those entries scores ``c / n`` (fraction of the protein's
InterPro signatures that vote for the term, over the propagated InterPro2GO map).
"""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict

from protea.core.contracts.operation import EmitFn


def _bp_go_ids(obo_path: str) -> set[str]:
    """Parse the biological_process GO ids from an OBO file.

    Uses the same OBO the evaluation already loads (no hardcoded aspect map).
    """
    bp: set[str] = set()
    cur: str | None = None
    with open(obo_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line == "[Term]":
                cur = None
            elif line.startswith("id: GO:"):
                cur = line[4:]
            elif line.startswith("namespace:") and cur:
                if line.split(":", 1)[1].strip() == "biological_process":
                    bp.add(cur)
    return bp


def _interpro_graded(
    protein2ipr: dict[str, list[str]],
    ipr2go: dict[str, list[str]],
) -> dict[str, dict[str, float]]:
    """Build ``acc -> {go: graded_score}`` from a protein->IPR map + IPR->[GO] map.

    Faithful port of ``interpro_lib.interpro_preds``: graded score is the
    fraction of the protein's mapping InterPro entries that support each GO term.
    ``ipr2go`` is the propagated, namespaced InterPro2GO map (``ipr2go_prop.json``).
    """
    out: dict[str, dict[str, float]] = {}
    for acc, iprs in protein2ipr.items():
        mapped = [i for i in iprs if i in ipr2go]
        if not mapped:
            continue
        support: Counter[str] = Counter()
        for ipr in mapped:
            for go_id in ipr2go[ipr]:
                support[go_id] += 1
        n = len(mapped)
        out[acc] = {go_id: c / n for go_id, c in support.items()}
    return out


def _graft_protein(
    base: dict[str, float],
    graded: dict[str, float],
    bp_terms: set[str],
    weight: float | None,
) -> dict[str, float]:
    """Blend one protein's base scores with InterPro BP evidence.

    Starts from the base scores (MF / CC and any non-BP term stay untouched),
    then for every BP InterPro term applies naive max (``weight is None``) or
    noisy-OR, adding BP terms the base scorer missed.
    """
    out = dict(base)
    for go_id, g in graded.items():
        if go_id not in bp_terms:
            continue
        b = base.get(go_id, 0.0)
        if weight is None:
            out[go_id] = max(b, g)
        else:
            out[go_id] = 1.0 - (1.0 - b) * (1.0 - weight * g)
    return out


def _skip_reason(
    obo_path: str, protein2ipr_file: str | None, ipr2go_file: str | None
) -> str | None:
    """Return a skip reason when a required artefact is missing, else ``None``."""
    if not os.path.isfile(obo_path):
        return "obo missing"
    if not protein2ipr_file or not os.path.isfile(protein2ipr_file):
        return "protein2ipr file missing"
    if not ipr2go_file or not os.path.isfile(ipr2go_file):
        return "ipr2go file missing"
    return None


def _graft_file(
    path: str, graded: dict[str, dict[str, float]], bp_terms: set[str], weight: float | None
) -> None:
    """Rewrite one CAFA-format prediction TSV in place with the BP graft."""
    by_prot: dict[str, dict[str, float]] = defaultdict(dict)
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 3:
                continue
            prot, go_id, score = cols[0], cols[1], cols[2]
            try:
                val = float(score)
            except ValueError:
                continue
            if val > by_prot[prot].get(go_id, -1.0):
                by_prot[prot][go_id] = val
    with open(path, "w", encoding="utf-8") as fh:
        for prot, scores in by_prot.items():
            post = _graft_protein(scores, graded.get(prot, {}), bp_terms, weight)
            for go_id, val in post.items():
                if val > 0:
                    fh.write(f"{prot}\t{go_id}\t{val:.6f}\n")


def apply_interpro_graft(
    pred_dir: str,
    obo_path: str,
    protein2ipr_file: str | None,
    ipr2go_file: str | None,
    weight: float | None,
    emit: EmitFn,
) -> None:
    """Rewrite every prediction TSV in ``pred_dir`` with the InterPro BP graft.

    Prediction files are CAFA-format (``protein\\tgo_id\\tscore``, no header).
    Operates in place. No-op (warning) when the OBO or either InterPro artefact
    is missing, so a misconfigured opt-in never crashes the evaluation.
    """
    reason = _skip_reason(obo_path, protein2ipr_file, ipr2go_file)
    if reason is not None:
        emit("run_cafa_evaluation.interpro_graft_skipped", None, {"reason": reason}, "warning")
        return
    assert protein2ipr_file is not None and ipr2go_file is not None  # narrowed by _skip_reason

    with open(protein2ipr_file, encoding="utf-8") as fh:
        protein2ipr = json.load(fh)
    with open(ipr2go_file, encoding="utf-8") as fh:
        ipr2go = json.load(fh)
    bp_terms = _bp_go_ids(obo_path)
    graded = _interpro_graded(protein2ipr, ipr2go)

    files = [f for f in os.listdir(pred_dir) if f.endswith(".tsv")]
    for fname in files:
        _graft_file(os.path.join(pred_dir, fname), graded, bp_terms, weight)
    emit(
        "run_cafa_evaluation.interpro_graft_done",
        None,
        {
            "files": len(files),
            "bp_terms": len(bp_terms),
            "interpro_proteins": len(graded),
            "rule": "noisyor" if weight is not None else "max",
        },
        "info",
    )
