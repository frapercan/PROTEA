"""A platform prediction set against an oracle's scores: the same terms, the same scores.

Reads the set's stored scores from the store (``go_prediction`` rows or compact
``prediction_score`` rows, whichever it holds) and the oracle's TSV (``accession, term,
score``, no header, as democafa's baselines write it), and prints what agrees: the
pairs on both sides, on one side only, the largest score difference and the pairs past
the tolerance (the first twenty of each). It exits 1 when the term sets differ or a
score differs by more than the tolerance, outside the pairs named as departures.

* ``--vector``: a term frequency set gives every target one vector; compare the oracle's
  first target's vector with the set's (and check every target of the set holds it).
* ``--drop-roots``: leave the three roots out of both sides.
* ``--close-under <evaluation set id>``: name the oracle's terms under the window's t0
  ontology and close them upwards, scores carried as the largest over the descendants,
  roots out, before comparing (democafa's goa_nonexp writes direct terms).
* ``--departures <tsv>``: ``accession, term`` pairs allowed to differ, the recorded
  departures of a literal.

Read only: the transaction is declared so. Run it with the store's environment::

    python scripts/lafa_check/baselines/compare_with_set.py <set id> <oracle.tsv> [options]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core import lafa_frame
from protea.core._lafa_frame_data import load_end_ontology
from protea.core.term_frequency import ROOTS
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

Scores = dict[tuple[str, str], float]

_SET = text(
    """
    SELECT p.protein_accession, t.go_id, p.method_score
    FROM go_prediction p JOIN go_term t ON t.id = p.go_term_id
    WHERE p.prediction_set_id = :s
    UNION ALL
    SELECT s.protein_accession, t.go_id, u.score
    FROM prediction_score s
    CROSS JOIN LATERAL unnest(s.go_term_ids, s.scores) AS u(go_term_id, score)
    JOIN go_term t ON t.id = u.go_term_id
    WHERE s.prediction_set_id = :s
    """
)


def read_oracle(path: Path) -> Scores:
    opener = gzip.open if path.suffix == ".gz" else open
    out: Scores = {}
    with opener(path, "rt") as handle:  # type: ignore[operator]
        for row in csv.reader(handle, delimiter="\t"):
            if len(row) >= 3 and row[0] != "EntryID":
                out[(row[0], row[1])] = max(float(row[2]), out.get((row[0], row[1]), 0.0))
    return out


def closed(session: Session, evaluation_set_id: str, oracle: Scores) -> Scores:
    """The oracle's terms named and closed under the window's t0 ontology, roots out."""
    eset = session.get(EvaluationSet, uuid.UUID(evaluation_set_id))
    if eset is None:
        raise SystemExit(f"no window {evaluation_set_id}")
    t0 = load_end_ontology(session, eset.old_native_snapshot_id)
    out: Scores = {}
    for (accession, go_id), score in oracle.items():
        by_aspect = lafa_frame.end_truth(t0, {accession: {go_id}}, {}, Counter(), "oracle")
        for terms in by_aspect.get(accession, {}).values():
            for term in terms - ROOTS:
                out[(accession, term)] = max(score, out.get((accession, term), 0.0))
    return out


def as_vector(scores: Scores) -> tuple[dict[str, float], int]:
    """One target's vector, and how many targets hold a different one."""
    by_target: dict[str, dict[str, float]] = defaultdict(dict)
    for (accession, term), score in scores.items():
        by_target[accession][term] = score
    first = by_target[min(by_target)] if by_target else {}
    return first, sum(1 for v in by_target.values() if v != first)


def report(ours: dict, theirs: dict, tolerance: float, departures: set) -> int:
    both = set(ours) & set(theirs)
    only_ours = sorted(set(ours) - set(theirs) - departures)
    only_theirs = sorted(set(theirs) - set(ours) - departures)
    beyond = sorted(k for k in both - departures if abs(ours[k] - theirs[k]) > tolerance)
    largest = max((abs(ours[k] - theirs[k]) for k in both), default=0.0)
    print(
        f"pairs on both sides {len(both):,}; only in the set {len(only_ours):,}; only in the oracle {len(only_theirs):,}"
    )
    print(f"largest difference {largest:.3g}; beyond {tolerance:g}: {len(beyond):,}")
    for name, items in (
        ("only in the set", only_ours),
        ("only in the oracle", only_theirs),
        ("beyond", beyond),
    ):
        for item in items[:20]:
            print(f"  {name}: {item} set={ours.get(item)} oracle={theirs.get(item)}")
    return 1 if only_ours or only_theirs or beyond else 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="compare_with_set.py")
    parser.add_argument("prediction_set_id")
    parser.add_argument("oracle", type=Path)
    parser.add_argument("--tolerance", type=float, default=1e-7)
    parser.add_argument("--vector", action="store_true")
    parser.add_argument("--drop-roots", action="store_true")
    parser.add_argument("--close-under", default=None)
    parser.add_argument("--departures", type=Path, default=None)
    args = parser.parse_args(argv)
    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    with Session(engine) as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        ours: Scores = {
            (a, g): float(s)
            for a, g, s in session.execute(_SET, {"s": uuid.UUID(args.prediction_set_id)})
        }
        theirs = read_oracle(args.oracle)
        if args.close_under:
            theirs = closed(session, args.close_under, theirs)
    if args.drop_roots:
        ours = {k: v for k, v in ours.items() if k[1] not in ROOTS}
        theirs = {k: v for k, v in theirs.items() if k[1] not in ROOTS}
    departures: set = set()
    if args.departures:
        departures = {
            tuple(r[:2]) for r in csv.reader(open(args.departures), delimiter="\t") if len(r) >= 2
        }
    if args.vector:
        mine, differing = as_vector(ours)
        print(f"targets of the set whose vector differs from the first's: {differing}")
        oracle_vector, _ = as_vector(theirs)
        return report(mine, oracle_vector, args.tolerance, set()) or (1 if differing else 0)
    return report(ours, theirs, args.tolerance, departures)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
