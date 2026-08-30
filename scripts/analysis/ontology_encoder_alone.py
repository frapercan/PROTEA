"""Train the ontology encoder on GO and measure it with nothing else attached.

Run:  poetry run python scripts/analysis/ontology_encoder_alone.py

The point is a number to beat, and the baselines are the reason the number can
be read at all. An untrained encoder says what the architecture gives for free.
Ranking by how many descendants a term has says what a bare degree heuristic
gives. Neither is a strawman: both are what you get without training anything.
"""

from __future__ import annotations

import os
import random
import sys
import time

import numpy as np
from sqlalchemy import create_engine, text

from protea.core.ontology.dag import Dag
from protea.core.ontology.evaluation import (
    rank_held_out_parents,
    separates_ancestors,
)
from protea.core.ontology.order_encoder import OrderEncoder, TrainConfig
from protea.core.ontology.training import fit

SNAPSHOT = "36038118-37ba-4858-8677-f5b5d730bf56"
SAMPLE = 400


def load_go(url: str) -> Dag:
    q = text(
        "SELECT pt.go_id, c.go_id FROM go_term_relationship r "
        "JOIN go_term c ON c.id = r.child_go_term_id "
        "JOIN go_term pt ON pt.id = r.parent_go_term_id "
        "WHERE r.ontology_snapshot_id = :s AND r.relation_type IN ('is_a','part_of')"
    )
    with create_engine(url, future=True).connect() as con:
        return Dag.from_pairs([(a, b) for a, b in con.execute(q, {"s": SNAPSHOT})])


class DegreeBaseline:
    """Rank a candidate parent by how many descendants it has.

    Not a strawman. Subsumption correlates strongly with generality, so a term
    that subsumes a lot is a good guess for subsuming this one too. Whatever
    the encoder is worth, it is worth over this.
    """

    def __init__(self, dag: Dag, closure: set[tuple[str, str]]) -> None:
        self.dag = dag
        self.size: dict[str, int] = {}
        for up, _ in closure:
            self.size[up] = self.size.get(up, 0) + 1

    def rank_parents(self, child: str, candidates: list[str]) -> np.ndarray:
        return np.argsort([-self.size.get(c, 0) for c in candidates], kind="stable")


def _report(dag: Dag, closure: set[tuple[str, str]], model: OrderEncoder,
            untrained: OrderEncoder, test: list[tuple[str, str]]) -> None:
    cands = [("encoder entrenado", model), ("encoder sin entrenar", untrained),
             ("grado (descendientes)", DegreeBaseline(dag, closure))]
    print("\n  --- contra los 40.214 terminos (incluye empates legitimos) ---")
    for tag, m in cands:
        print(rank_held_out_parents(m, dag, test, sample=SAMPLE).line(tag))
    print("\n  --- contra los NO-ancestros: solo el error ---")
    for tag, m in cands:
        rep = rank_held_out_parents(
            m, dag, test, sample=SAMPLE, against_non_ancestors_only=True)
        print(rep.line(tag))

    for hard, title in ((False, "negativos aleatorios (facil)"),
                        (True, "negativos duros: pares invertidos y hermanos")):
        print(f"\n  --- subsuncion verdadera contra falsa, {title} ---")
        for tag, m in (("entrenado", model), ("sin entrenar", untrained)):
            acc, ratio = separates_ancestors(m, dag, closure, n=20000, seed=1, hard=hard)
            print(f"  {tag:22s} exactitud {acc:6.2%}   "
                  f"penalizacion falsa/verdadera {min(ratio, 9.99e9):10.1f}x")


def main() -> int:
    url = os.environ.get("PROTEA_DB_URL", "")
    if not url:
        print("PROTEA_DB_URL no esta definido", file=sys.stderr)
        return 2

    t0 = time.time()
    dag = load_go(url)
    closure = set(dag.closure())
    print(f"  GO {SNAPSHOT[:8]}: {len(dag.terms):,} terminos, {len(dag.edges):,} aristas")
    print(f"  cierre transitivo: {len(closure):,} pares  ({time.time() - t0:.0f}s)")

    split = dag.split_edges(held_out=0.05, seed=0)
    held = set(split.test)
    train_pairs = sorted(p for p in closure if p not in held)
    print(f"  aristas retenidas: {len(split.test):,}")
    print(f"  pares de entrenamiento: {len(train_pairs):,}\n")

    cfg = TrainConfig(dim=64, epochs=12, batch=8192, lr=0.05, negatives=4)
    untrained = OrderEncoder(dag, cfg)
    model = fit(dag, train_pairs, closure, cfg, log=print)
    _report(dag, closure, model, untrained, list(split.test))
    print(f"\n  total {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    random.seed(0)
    raise SystemExit(main())
