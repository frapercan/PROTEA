#!/usr/bin/env python
"""Compute Information Accretion (Clark & Radivojac 2013) for an OntologySnapshot.

IA(v) = -log2( P(v | parents(v)) )
      = -log2( |proteins[v]| / |intersect proteins[p] for p in parents(v)| )

Implementation notes
--------------------
* Corpus: a reference AnnotationSet (typically GOA at the pivot date). Annotations
  may be pinned to a *different* snapshot, so we join through ``go_term.go_id``
  to translate to the target (pivot) snapshot. Terms absent from the target
  snapshot are dropped.
* DAG edges: ``is_a`` and ``part_of`` on the target snapshot (the two True Path
  Rule edges CAFA/cafaeval use for propagation).
* Propagation: for each (protein, term) pair we add the protein to ``term`` and
  all its ancestors. Ancestors per term are precomputed once via iterative BFS.
* Roots: IA = 0 (no parents).
* Terms with no annotation after propagation: IA = 0 (skipped when writing,
  treated as 0 by cafaeval).

Outputs
-------
* ``data/benchmarks/IA_<snapshot_id>.tsv``  (go_id\\tia_value, no header)
* Updates ``ontology_snapshot.ia_url`` to the ``file://`` URL of that TSV.
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

from sqlalchemy import text

from protea.core.ia import PROPAGATE_RELATIONS, build_ancestors, term_ia
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings


def _log(msg: str) -> None:
    print(f"[ia] {msg}", flush=True)


def compute_ia(
    session,
    snapshot_id: uuid.UUID,
    annotation_set_id: uuid.UUID,
) -> dict[str, float]:
    t0 = time.time()

    # 1. go_id -> pivot term_id and term_id -> go_id maps
    rows = session.execute(
        text(
            "select id, go_id from go_term "
            "where ontology_snapshot_id = :s and is_obsolete = false"
        ),
        {"s": str(snapshot_id)},
    ).all()
    pivot_goid_by_id: dict[str, str] = {str(r[0]): r[1] for r in rows}
    pivot_id_by_goid: dict[str, str] = {r[1]: str(r[0]) for r in rows}
    _log(f"pivot terms: {len(pivot_goid_by_id)}")

    # 2. parent map on pivot DAG (child_go_id -> set(parent_go_id))
    from sqlalchemy import bindparam
    rels_stmt = text(
        "select c.go_id, p.go_id "
        "from go_term_relationship r "
        "join go_term c on c.id = r.child_go_term_id "
        "join go_term p on p.id = r.parent_go_term_id "
        "where r.ontology_snapshot_id = :s "
        "  and r.relation_type in :rels"
    ).bindparams(bindparam("rels", expanding=True))
    rels = session.execute(
        rels_stmt,
        {"s": str(snapshot_id), "rels": list(PROPAGATE_RELATIONS)},
    ).all()
    parents: dict[str, set[str]] = defaultdict(set)
    for child_go, parent_go in rels:
        parents[child_go].add(parent_go)
    _log(f"parent edges: {sum(len(v) for v in parents.values())} "
         f"({len(parents)} terms with parents)")

    # 3. precompute ancestors (including self) via the shared IA module so the
    #    script and the unit-tested core never drift.
    ancestors_cache = build_ancestors(parents)
    for go_id in pivot_goid_by_id.values():
        if go_id not in ancestors_cache:
            ancestors_cache[go_id] = frozenset({go_id})
    _log(f"ancestor closures computed: {len(ancestors_cache)}")

    # 4. load corpus annotations — stream to avoid loading 5M rows into mem at once
    _log(f"loading annotations from annotation_set {annotation_set_id} …")
    stmt = text(
        "select pga.protein_accession, gt.go_id "
        "from protein_go_annotation pga "
        "join go_term gt on gt.id = pga.go_term_id "
        "where pga.annotation_set_id = :a"
    )
    # proteins_per_term[go_id] = set(protein_accession)
    proteins_per_term: dict[str, set[str]] = defaultdict(set)
    count_raw = 0
    count_used = 0
    result = session.execute(stmt, {"a": str(annotation_set_id)})
    for protein, go_id in result.yield_per(50000):
        count_raw += 1
        if go_id not in pivot_id_by_goid:
            continue
        count_used += 1
        for anc in ancestors_cache[go_id]:
            proteins_per_term[anc].add(protein)
        if count_raw % 500000 == 0:
            _log(f"  processed {count_raw:,} raw annotations "
                 f"({count_used:,} kept; elapsed {time.time()-t0:.0f}s)")
    _log(f"raw={count_raw:,} used={count_used:,} "
         f"terms_with_proteins={len(proteins_per_term)}")

    # 5. compute IA via the shared core (LAFA-faithful dummy-protein formula)
    ia: dict[str, float] = {
        go_id: term_ia(go_id, parents.get(go_id), proteins_per_term)
        for go_id in pivot_goid_by_id.values()
    }

    nonzero = sum(1 for v in ia.values() if v > 0)
    total = sum(ia.values())
    _log(f"IA computed: {len(ia)} terms ({nonzero} non-zero, "
         f"sum={total:.1f}, elapsed {time.time()-t0:.0f}s)")
    return ia


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True,
                    help="Target OntologySnapshot id (pivot).")
    ap.add_argument("--annotation-set", required=True,
                    help="Reference corpus AnnotationSet id (e.g. GOA 220).")
    ap.add_argument("--out-dir", default="data/benchmarks")
    ap.add_argument("--update-snapshot", action="store_true",
                    help="Also update ontology_snapshot.ia_url to the output file.")
    args = ap.parse_args()

    settings = load_settings(Path("."))
    factory = build_session_factory(settings.db_url)
    with factory() as sess:
        ia = compute_ia(
            sess,
            uuid.UUID(args.snapshot),
            uuid.UUID(args.annotation_set),
        )

        out_dir = Path(args.out_dir).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"IA_{args.snapshot}.tsv"
        with out_path.open("w") as fh:
            for go_id in sorted(ia):
                fh.write(f"{go_id}\t{ia[go_id]}\n")
        _log(f"wrote {out_path}")

        if args.update_snapshot:
            ia_url = f"file://{out_path}"
            sess.execute(
                text("update ontology_snapshot set ia_url=:u where id=:s"),
                {"u": ia_url, "s": args.snapshot},
            )
            sess.commit()
            _log(f"ontology_snapshot.ia_url = {ia_url}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
