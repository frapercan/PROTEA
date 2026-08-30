"""The ontology encoder as a graph over its own text, against what it replaces.

Run:  poetry run python scripts/analysis/graph_ontology_encoder.py

Three things are compared on the same held-out edges:

  - the free lookup table of Part 1, one vector per term, learned from edges
  - the graph encoder, whose term positions are computed from the term's text
    and its neighbourhood
  - lexical containment, which answers "is the candidate's name inside the
    child's name" and needs no model at all

The third exists because GO names are compositional. The parent's name appears
verbatim inside the child's on 23.4 per cent of edges, so a quarter of this
graph is recoverable by string matching, and a model reporting a high number
without that comparison has measured str.find.

Everything is therefore reported twice: over all held-out edges, and over the
subset whose parent and child names share no content token. That subset is
6.7 per cent of the ontology and it is where the claim actually lives.
"""

from __future__ import annotations

import os
import re
import sys
import time

import numpy as np
import torch
from sqlalchemy import create_engine, text

from protea.core.ontology.dag import Dag
from protea.core.ontology.evaluation import rank_held_out_parents, separates_ancestors
from protea.core.ontology.graph_encoder import (
    GRAPH_RELATIONS,
    GraphConfig,
    GraphOrderEncoder,
    build_adjacency,
)
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.term_features import (
    TextFeatureConfig,
    aspect_features,
    text_features,
)
from protea.core.ontology.training import fit

SNAPSHOT = "36038118-37ba-4858-8677-f5b5d730bf56"
SAMPLE = 400
STOP = {"of", "the", "a", "an", "in", "to", "and", "or", "by", "from", "with",
        "into", "via", "on", "at", "for", "as"}


def _tok(s: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]+", s.lower()) if w not in STOP and len(w) > 2}


class LexicalBaseline:
    """Rank a candidate parent by how much of its name is inside the child's.

    The control that keeps the graph encoder honest. It needs no training and
    no features beyond the strings the ontology already ships.
    """

    def __init__(self, names: dict[str, str]) -> None:
        self.tok = {g: _tok(n) for g, n in names.items()}

    def rank_parents(self, child: str, candidates: list[str]) -> np.ndarray:
        c = self.tok.get(child, set())
        score = [
            -(len(self.tok.get(t, set()) & c) / max(len(self.tok.get(t, set())), 1))
            for t in candidates
        ]
        return np.argsort(score, kind="stable")


def load(url: str) -> tuple[Dag, dict, dict, dict, dict]:
    with create_engine(url, future=True).connect() as con:
        terms = con.execute(
            text("SELECT go_id, name, definition, aspect FROM go_term "
                 "WHERE ontology_snapshot_id = :s AND NOT is_obsolete"), {"s": SNAPSHOT}
        ).all()
        edges = con.execute(
            text("SELECT pt.go_id, c.go_id, r.relation_type FROM go_term_relationship r "
                 "JOIN go_term c ON c.id = r.child_go_term_id "
                 "JOIN go_term pt ON pt.id = r.parent_go_term_id "
                 "WHERE r.ontology_snapshot_id = :s"), {"s": SNAPSHOT}
        ).all()
    names = {g: n or "" for g, n, _, _ in terms}
    defs = {g: d or "" for g, _, d, _ in terms}
    asp = {g: a or "" for g, _, _, a in terms}
    typed: dict[str, list[tuple[str, str]]] = {}
    for p, c, rel in edges:
        typed.setdefault(rel, []).append((p, c))
    sub = [e for r in ("is_a", "part_of") for e in typed.get(r, [])]
    return Dag.from_pairs(sub), names, defs, asp, typed


def _report(tag: str, model, dag: Dag, edges: list[tuple[str, str]]) -> None:
    rep = rank_held_out_parents(model, dag, edges, sample=SAMPLE,
                                against_non_ancestors_only=True)
    print(rep.line(tag))


def main() -> int:
    url = os.environ.get("PROTEA_DB_URL", "")
    if not url:
        print("PROTEA_DB_URL no esta definido", file=sys.stderr)
        return 2
    t0 = time.time()
    dag, names, defs, asp, typed = load(url)
    closure = set(dag.closure())
    print(f"  GO: {len(dag.terms):,} terminos, {len(dag.edges):,} aristas de subsuncion, "
          f"{sum(len(v) for v in typed.values()):,} aristas en total")

    order = list(dag.terms)
    X = np.hstack([
        text_features([names.get(g, "") for g in order], [defs.get(g, "") for g in order],
                      TextFeatureConfig(dim=256)),
        aspect_features([asp.get(g, "") for g in order]),
    ])
    print(f"  rasgos de texto: {X.shape} ({time.time() - t0:.0f}s)")

    split = dag.split_edges(held_out=0.05, seed=0)
    held = set(split.test)
    train_pairs = sorted(p for p in closure if p not in held)
    lean = [(p, c) for p, c in split.test
            if _tok(names.get(p, "")) and not (_tok(names.get(p, "")) & _tok(names.get(c, "")))]
    print(f"  aristas retenidas: {len(split.test):,}, de ellas sin solape lexico: {len(lean):,}\n")

    tcfg = TrainConfig(dim=64, epochs=12, batch=8192, lr=0.05, negatives=4)
    table = fit(OrderEncoder(dag, tcfg), train_pairs, closure, tcfg, log=print).frozen()

    mats = build_adjacency(dag, typed, GRAPH_RELATIONS)
    gcfg = GraphConfig(in_dim=X.shape[1], out_dim=64, hidden=256, layers=3)
    # A large batch, because the cost is per STEP and not per pair: every step
    # recomputes all 40,214 term vectors whether the batch touches ten thousand
    # of them or a hundred thousand. Eight steps an epoch instead of sixty.
    # The learning rate rises with it, a larger batch giving a less noisy
    # gradient, and the epochs rise to keep the step count sane.
    #
    # The table encoder above is deliberately NOT changed. It is the baseline
    # this is measured against, and retuning it would make the comparison say
    # something other than what it claims.
    gtrain = TrainConfig(dim=64, epochs=30, batch=65536, lr=3e-3, negatives=4)
    graph = fit(
        GraphOrderEncoder(dag, torch.tensor(X), mats, gcfg),  # type: ignore[arg-type]
        train_pairs, closure, gtrain, log=print,
    )
    gfrozen = graph.frozen()

    lex = LexicalBaseline(names)
    for title, edges in (("todas las retenidas", list(split.test)),
                         (f"SIN solape lexico ({len(lean):,})", lean)):
        print(f"\n  --- padre retenido entre los no-ancestros, {title} ---")
        for tag, m in (("grafo (texto+DAG)", gfrozen), ("tabla libre (Parte 1)", table),
                       ("contencion lexica", lex)):
            _report(tag, m, dag, edges)

    print("\n  --- subsuncion verdadera contra falsa, negativos duros ---")
    for tag, m in (("grafo (texto+DAG)", gfrozen), ("tabla libre (Parte 1)", table)):
        acc, _ = separates_ancestors(m, dag, closure, n=20000, seed=1, hard=True)
        print(f"  {tag:24s} exactitud {acc:6.2%}")
    print(f"\n  total {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    # Line-buffered on purpose: these runs take tens of minutes and a
    # buffered stdout means no progress is readable until they end.
    sys.stdout.reconfigure(line_buffering=True)
    raise SystemExit(main())
