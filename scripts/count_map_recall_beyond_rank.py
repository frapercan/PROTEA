"""Read only: how much of its window's truth a count map's set holds past rank 100.

The count map's reading at the grid floor (``run_count_map`` with ``top`` null) keeps
every term the evaluator can count, where the laboratory kept the 100 best per target
and aspect. For one stored set, this reads its rows and its window's truth (the
groundtruth parquet, fetched and checked against its sha256) and reports, per bucket
(``nk``, ``lk``, ``pk`` and all) and per aspect, the true pairs the set holds within
the first ``--rank`` terms of a target, past them, and not at all
(``protea.core.count_map.recall``). The count map never reads the truth itself: this is
the measurement the ladder's toy records beside it.

Opens a READ ONLY transaction with a statement timeout.

Usage:
    python scripts/count_map_recall_beyond_rank.py --prediction-set <id> [--rank 100] [--out f.json]
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from protea.core._evaluation_groundtruth import load_evaluation_data_for_set  # noqa: E402
from protea.core.count_map.reading import TOP  # noqa: E402
from protea.core.count_map.recall import recall_by_rank  # noqa: E402
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet  # noqa: E402
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet  # noqa: E402
from protea.infrastructure.session import build_session_factory  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402

_ROWS = text(
    "SELECT gp.protein_accession, gt.go_id, gp.method_score FROM go_prediction gp "
    "JOIN go_term gt ON gt.id = gp.go_term_id WHERE gp.prediction_set_id = :p"
)
_ASPECTS = text("SELECT go_id, aspect FROM go_term WHERE ontology_snapshot_id = :s")


def measure(session: Session, prediction_set_id: uuid.UUID, rank: int = TOP) -> dict[str, Any]:
    """The recall within and past ``rank`` of one count-map set, per bucket and aspect."""
    pset = session.get(PredictionSet, prediction_set_id)
    if pset is None or not (pset.meta or {}).get("evaluation_set_id"):
        raise SystemExit(f"{prediction_set_id} is not a set that read a window")
    eset = session.get(EvaluationSet, uuid.UUID(str(pset.meta["evaluation_set_id"])))
    if eset is None:
        raise SystemExit(f"the window of {prediction_set_id} is gone")
    rows = [(str(a), str(g), float(s)) for a, g, s in session.execute(_ROWS, {"p": pset.id})]
    aspect_of = {
        str(g): str(a) for g, a in session.execute(_ASPECTS, {"s": eset.pivot_snapshot_id})
    }
    truth, _ = load_evaluation_data_for_set(session, eset)
    buckets = {"nk": truth.nk, "lk": truth.lk, "pk": truth.pk}
    every: dict[str, set[str]] = {}
    for bucket in buckets.values():
        for accession, terms in bucket.items():
            every.setdefault(accession, set()).update(terms)
    report = {name: recall_by_rank(rows, b, aspect_of, rank) for name, b in buckets.items()}
    report["all"] = recall_by_rank(rows, every, aspect_of, rank)
    return {
        "prediction_set_id": str(pset.id),
        "evaluation_set_id": str(eset.id),
        "limit_per_entry": pset.limit_per_entry,
        "rows": len(rows),
        "rank": rank,
        "recall": report,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--prediction-set", type=uuid.UUID, required=True)
    parser.add_argument("--rank", type=int, default=TOP)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)
    factory = build_session_factory(load_settings(PROJECT_ROOT).db_url)
    with factory() as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout = '600s'"))
        record = measure(session, args.prediction_set, args.rank)
        session.rollback()
    body = json.dumps(record, indent=2, sort_keys=True)
    if args.out is not None:
        args.out.write_text(body + "\n", encoding="utf-8")
    print(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
