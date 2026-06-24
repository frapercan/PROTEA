"""Soft Pmin/Pmax DAG propagation post-processing for cafaeval (ProtBoost 4.5).

A scorer-agnostic post-processing step over the prediction frame, applied per
protein right before cafaeval. It smooths each candidate's score with its GO-DAG
neighbours, which the threshold-swept f_micro_w rewards (validated offline:
+0.0141 PK on the 220->227 frame, sanity-gated). It REPLACES the hard CAFA
propagation conceptually, but cafaeval ``prop=fill`` only overwrites zeros, so
applying this first and then ``fill`` preserves the softened non-zero scores.

Formulas (recursive over the GO DAG):
    Pmin(N) = min(Pmin(parent) : parent in Parents(N)) * 0.7 + P(N) * 0.3   (root->leaf)
    Pmax(N) = max(Pmax(child)  : child  in Children(N)) * 0.7 + P(N) * 0.3   (leaf->root)
    P_post(N) = (Pmin(N) + Pmax(N)) / 2
Empty parents/children keep the own score (the blend only applies with neighbours).
"""

from __future__ import annotations

import os
from collections import defaultdict

from protea.core.contracts.operation import EmitFn


def _parents_children(obo_path: str) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Parse is_a + part_of edges from an OBO file into parent/child maps."""
    par: dict[str, set[str]] = defaultdict(set)
    cur: str | None = None
    with open(obo_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line == "[Term]":
                cur = None
            elif line.startswith("id: GO:"):
                cur = line[4:]
            elif line.startswith("is_a:") and cur:
                par[cur].add(line.split()[1])
            elif line.startswith("relationship: part_of") and cur:
                parts = line.split()
                if len(parts) >= 3:
                    par[cur].add(parts[2])
    ch: dict[str, set[str]] = defaultdict(set)
    for c, ps in par.items():
        for p in ps:
            ch[p].add(c)
    return par, ch


def _ancestors(t: str, par: dict[str, set[str]], cache: dict[str, set[str]]) -> set[str]:
    if t in cache:
        return cache[t]
    out: set[str] = set()
    stack = list(par.get(t, ()))
    while stack:
        a = stack.pop()
        if a in out:
            continue
        out.add(a)
        stack.extend(par.get(a, ()))
    cache[t] = out
    return out


def _soft_prop_protein(
    scores: dict[str, float],
    par: dict[str, set[str]],
    ch: dict[str, set[str]],
    anc_cache: dict[str, set[str]],
) -> dict[str, float]:
    """Apply averaged soft Pmin/Pmax to one protein's candidate scores."""
    terms = set(scores)
    universe = set(terms)
    for t in terms:
        universe |= _ancestors(t, par, anc_cache)  # ancestors (score 0 if not a candidate) gate Pmin
    order = sorted(universe, key=lambda t: len(_ancestors(t, par, anc_cache)))
    pmin: dict[str, float] = {}
    for t in order:  # root -> leaf
        ps = [pmin[p] for p in par.get(t, ()) if p in pmin]
        base = scores.get(t, 0.0)
        pmin[t] = (min(ps) * 0.7 + base * 0.3) if ps else base
    pmax: dict[str, float] = {}
    for t in reversed(order):  # leaf -> root
        cs = [pmax[c] for c in ch.get(t, ()) if c in pmax]
        base = scores.get(t, 0.0)
        pmax[t] = (max(cs) * 0.7 + base * 0.3) if cs else base
    return {t: (pmin[t] + pmax[t]) / 2 for t in terms}


def apply_softprop(pred_dir: str, obo_path: str, emit: EmitFn) -> None:
    """Rewrite every prediction TSV in ``pred_dir`` with soft-propagated scores.

    Prediction files are CAFA-format (``protein\\tgo_id\\tscore``, no header).
    Idempotent per call; operates in place. No-op when the OBO is missing.
    """
    if not os.path.isfile(obo_path):
        emit("run_cafa_evaluation.softprop_skipped", None, {"reason": "obo missing"}, "warning")
        return
    par, ch = _parents_children(obo_path)
    anc_cache: dict[str, set[str]] = {}
    files = [f for f in os.listdir(pred_dir) if f.endswith(".tsv")]
    rewritten = 0
    for fname in files:
        path = os.path.join(pred_dir, fname)
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
                post = _soft_prop_protein(scores, par, ch, anc_cache)
                for go_id, val in post.items():
                    if val > 0:
                        fh.write(f"{prot}\t{go_id}\t{val:.6f}\n")
        rewritten += 1
    emit("run_cafa_evaluation.softprop_done", None,
         {"files": rewritten, "terms_with_parents": len(par)}, "info")
