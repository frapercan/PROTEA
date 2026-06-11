"""Measure the load cost the batch re-score op amortizes (Deliverable 2).

Each standalone ``run_cafa_evaluation`` pays a fixed per-eval cost: fetch +
dedup the prediction base frame, load the ground truth, and (cheaply) re-score.
The batch op pays the base-frame + GT load ONCE for the whole batch and then
only the cheap per-config re-score.

This harness measures, against the live read-only DB:

* T_load  : wall-clock to fetch + dedup the column-pruned base frame.
* T_score : wall-clock for ONE _vectorized_scores + TSV write (the per-config
            cost the batch op repeats).
* RSS     : peak resident memory of the column-pruned load.

It then projects N-separate-evals (N x (T_load + T_score)) vs the batch
(T_load + N x T_score). cafaeval's tau-sweep is identical in both paths and is
excluded (it is the same constant per config either way), so this isolates the
structural win.

Read-only: SELECTs only, no writes.
"""

from __future__ import annotations

import argparse
import os
import resource
import tempfile
import time
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def _peak_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred-set", required=True)
    ap.add_argument("--db-url", default=os.environ.get("PROTEA_PROBE_DB_URL"))
    ap.add_argument("--configs", type=int, default=8, help="N (batch size) to project")
    args = ap.parse_args()
    if not args.db_url:
        print("ERROR: set --db-url or PROTEA_PROBE_DB_URL")
        return 2

    engine = create_engine(args.db_url)
    pred_id = uuid.UUID(args.pred_set)

    with Session(engine) as session:
        # Distinct delta-protein cohort for this set (the eval filter).
        from sqlalchemy import distinct, select

        from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

        delta = set(
            session.execute(
                select(distinct(GOPrediction.protein_accession)).where(
                    GOPrediction.prediction_set_id == pred_id
                )
            )
            .scalars()
            .all()
        )
        ctx = _artifacts.WritePredictionsContext(
            pred_set_id=pred_id, delta_proteins=delta, max_distance=None, path=""
        )

        rss_before = _peak_rss_mb()
        t0 = time.perf_counter()
        base, raw = _artifacts._build_base_frame(session, ctx)
        t_load = time.perf_counter() - t0
        rss_after_load = _peak_rss_mb()

        cfg = ScoringConfig(
            formula="linear",
            weights={"embedding_similarity": 1.0, "identity_nw": 0.5, "taxonomic_proximity": 0.5},
            params={"ia_prior": {"enabled": True, "source": "frequency", "gamma": 0.5}},
        )
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=True) as f:
            t0 = time.perf_counter()
            _artifacts._write_scored_base(base, cfg, f.name)
            t_score = time.perf_counter() - t0

    n = args.configs
    n_sep = n * (t_load + t_score)
    batch = t_load + n * t_score
    speedup = n_sep / batch if batch else float("inf")

    print("=" * 64)
    print(f"pred_set            : {args.pred_set}")
    print(f"delta proteins      : {len(delta)}")
    print(f"base rows (raw/dedup): {raw} / {len(base)}")
    print(f"cols loaded         : {len(_artifacts._BASE_SCORE_COLS)} (column-pruned)")
    print("-" * 64)
    print(f"T_load  (fetch+dedup): {t_load:8.3f} s")
    print(f"T_score (1 re-score) : {t_score:8.3f} s")
    print(
        f"RSS delta for load   : {rss_after_load - rss_before:8.1f} MB (peak {rss_after_load:.0f})"
    )
    print("-" * 64)
    print(f"N = {n} configs")
    print(f"  N separate evals   : {n_sep:8.3f} s  (N x (T_load + T_score))")
    print(f"  batch op           : {batch:8.3f} s  (T_load + N x T_score)")
    print(f"  speedup (load part): {speedup:8.2f} x")
    print("=" * 64)
    engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
