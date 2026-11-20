"""On a real store: a scored set's prediction file is the evaluator's reading of its candidates.

For each candidate set and ScoringConfig named, inside ONE transaction that is
rolled back at the end, so nothing is left in the store:

1. writes the prediction file the evaluator writes today from the candidates
   (``_run_cafa_artifacts.write_predictions`` with the config), over every
   protein of the set;
2. scores the set with ``score_prediction_set`` (its rows are inserted, not
   committed);
3. writes the prediction file the evaluator writes from a scored set
   (``_method_score_predictions.write_method_predictions``) over the same
   proteins, reading the uncommitted rows on the same connection;
4. compares the two files byte for byte.

    PROTEA_DB_URL=... PROTEA_PRED_CACHE_DIR=<scratch> \\
    python scripts/score_parity/compare_on_store.py record.json \\
        --set <id> [--set <id> ...] --config embedding_only --config vote_fraction \\
        [--cut <k>]

The DSN is read from the environment and never printed. Point
``PROTEA_PRED_CACHE_DIR`` at a scratch directory: the evaluator's path caches
its frames there, and a store's own cache is not the place for a check's.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from protea.core.operations import _method_score_predictions
from protea.core.operations import _run_cafa_artifacts as artifacts
from protea.core.operations.score_prediction_set import ScorePredictionSetOperation
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def _one_case(
    session: Session, set_id: uuid.UUID, config: ScoringConfig, cut: int | None, tmp: Path
) -> dict[str, Any]:
    """Both files for one (set, config, cut), compared, inside the caller's transaction."""
    proteins = {
        row[0]
        for row in session.execute(
            text(
                "SELECT DISTINCT protein_accession FROM go_prediction WHERE prediction_set_id = :s"
            ),
            {"s": set_id},
        )
    }
    started = time.perf_counter()
    before = tmp / "evaluator.tsv"
    artifacts.write_predictions(
        session,
        artifacts.WritePredictionsContext(
            pred_set_id=set_id,
            delta_proteins=proteins,
            max_distance=None,
            path=str(before),
            max_k_position=cut,
        ),
        scoring_config=config,
    )
    evaluator_seconds = time.perf_counter() - started
    payload = {
        "prediction_set_id": str(set_id),
        "scoring_config_id": str(config.id),
        "max_k_position": cut,
    }
    started = time.perf_counter()
    result = (
        ScorePredictionSetOperation()
        .execute(session, payload, emit=lambda *_args, **_kwargs: None)
        .result
    )
    scoring_seconds = time.perf_counter() - started
    after = tmp / "scored.tsv"
    _method_score_predictions.write_method_predictions(
        session,
        artifacts.WritePredictionsContext(
            pred_set_id=uuid.UUID(result["prediction_set_id"]),
            delta_proteins=proteins,
            max_distance=None,
            path=str(after),
        ),
    )
    return {
        "prediction_set_id": str(set_id),
        "scoring_config": config.name,
        "max_k_position": cut,
        "proteins": len(proteins),
        "candidates_read": result["candidates_read"],
        "denied_removed": result["denied_removed"],
        "evaluator_file": {"lines": _lines(before), "sha256": _sha256(before)},
        "scored_file": {"lines": _lines(after), "sha256": _sha256(after)},
        "identical": before.read_bytes() == after.read_bytes(),
        "seconds": {"evaluator": round(evaluator_seconds, 1), "scoring": round(scoring_seconds, 1)},
    }


#: The code the check exercises. Their digests survive a rebase that leaves
#: them unchanged, which the commit id does not.
_CODE = (
    "protea/core/denials.py",
    "protea/core/operations/_candidate_scoring.py",
    "protea/core/operations/_method_score_predictions.py",
    "protea/core/operations/_run_cafa_artifacts.py",
    "protea/core/operations/score_prediction_set.py",
)


def _code() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {"commit": commit, "files": {path: _sha256(root / path) for path in _CODE}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("record", type=Path)
    parser.add_argument("--set", dest="sets", action="append", required=True)
    parser.add_argument("--config", dest="configs", action="append", required=True)
    parser.add_argument("--cut", type=int, default=None)
    parser.add_argument("--store", default="development store")
    args = parser.parse_args()

    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    cases: list[dict[str, Any]] = []
    with Session(engine, future=True) as session:
        configs = {
            row.name: row
            for row in session.scalars(
                select(ScoringConfig).where(ScoringConfig.name.in_(args.configs))
            )
        }
        missing = sorted(set(args.configs) - set(configs))
        if missing:
            raise SystemExit(f"no scoring config named {missing}")
        for set_id in args.sets:
            for name in args.configs:
                with tempfile.TemporaryDirectory(prefix="score_parity_") as tmp:
                    case = _one_case(session, uuid.UUID(set_id), configs[name], args.cut, Path(tmp))
                session.rollback()
                cases.append(case)
                print(json.dumps(case), flush=True)
    record = {
        "what": (
            "A scored set's prediction file against the file the evaluator writes "
            "from the candidates with the same ScoringConfig, byte for byte, over "
            "every protein of each candidate set. Each case ran in a transaction "
            "that was rolled back: nothing was left in the store."
        ),
        "store": args.store,
        "code": _code(),
        "date": datetime.now(UTC).date().isoformat(),
        "all_identical": all(case["identical"] for case in cases),
        "cases": cases,
    }
    args.record.write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
