"""Offline de-saturation probe for the A-SCORE IA prior (Deliverable 1).

Loads a sample of one prediction_set's go_prediction feature rows, applies the
canonical vectorised scorer (``protea.core.operations._run_cafa_artifacts._vectorized_scores``)
under several ScoringConfig variants, and reports the score distribution
(min / mean / % at exactly 1.0 / coarse histogram) per config.

The point: the base composite SATURATES (a large fraction of scores == 1.0
because cosine distances are tiny on this set). #628's IA prior multiplies the
composite by ``prior_t ** gamma``; this probe proves it spreads the mass off 1.0.

Read-only: a single SELECT, no DDL, no writes. Run with the live read creds.
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from protea.core.operations._run_cafa_artifacts import _BASE_SCORE_COLS, _vectorized_scores
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig

# Score columns the scorer reads, mapped to go_prediction columns. go_id is not
# needed for the score itself but kept for parity with the eval base frame.
_SELECT_COLS = [c for c in _BASE_SCORE_COLS if c not in ("protein_accession", "go_id")]


def load_sample(db_url: str, pred_set_id: str, limit: int) -> pd.DataFrame:
    """Load up to ``limit`` go_prediction rows as the scorer's base frame."""
    engine = create_engine(db_url)
    cols = ", ".join(_SELECT_COLS)
    sql = text(f"SELECT {cols} FROM go_prediction WHERE prediction_set_id = :psid LIMIT :lim")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"psid": pred_set_id, "lim": limit})
    engine.dispose()
    # Cast numerics to float64 (None -> NaN) so the vectorised path matches eval.
    for c in df.columns:
        if c != "evidence_code":
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    return df


def composite_config(params: dict | None = None) -> ScoringConfig:
    """A multi-signal composite mirroring the A-SCORE default (not embedding-only).

    Uses the signals that are actually populated on this set so the composite is
    not trivially distance-only. ``params`` carries the optional IA prior block.
    """
    weights = {
        "embedding_similarity": 1.0,
        "identity_nw": 0.5,
        "identity_sw": 0.5,
        "taxonomic_proximity": 0.5,
        "neighbor_vote_fraction": 0.5,
    }
    return ScoringConfig(formula="linear", weights=weights, params=params)


def embedding_only_config(params: dict | None = None) -> ScoringConfig:
    """The embedding-only composite (the 82.6%-saturated baseline)."""
    return ScoringConfig(formula="linear", weights={"embedding_similarity": 1.0}, params=params)


def describe(scores: np.ndarray) -> dict:
    """min / mean / %==1.0 / coarse histogram for a score array."""
    n = len(scores)
    pct_one = 100.0 * float(np.sum(scores >= 1.0 - 1e-9)) / n if n else 0.0
    edges = [0.0, 0.5, 0.8, 0.9, 0.95, 0.99, 0.999, 1.0 + 1e-9]
    hist, _ = np.histogram(scores, bins=edges)
    labels = ["[0,.5)", "[.5,.8)", "[.8,.9)", "[.9,.95)", "[.95,.99)", "[.99,.999)", "[.999,1]"]
    return {
        "n": n,
        "min": float(np.min(scores)) if n else 0.0,
        "mean": float(np.mean(scores)) if n else 0.0,
        "pct_at_1": pct_one,
        "hist": dict(zip(labels, [int(h) for h in hist], strict=True)),
    }


def fmt(name: str, stats: dict) -> str:
    """Markdown table row + histogram line for one config."""
    h = stats["hist"]
    histline = "  ".join(f"{k}={v}" for k, v in h.items())
    return (
        f"| {name} | {stats['n']} | {stats['min']:.4f} | {stats['mean']:.4f} | "
        f"**{stats['pct_at_1']:.1f}%** |\n"
        f"  hist: {histline}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred-set", required=True)
    ap.add_argument("--limit", type=int, default=200_000)
    ap.add_argument("--db-url", default=os.environ.get("PROTEA_PROBE_DB_URL"))
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    if not args.db_url:
        print("ERROR: set --db-url or PROTEA_PROBE_DB_URL", file=sys.stderr)
        return 2

    df = load_sample(args.db_url, args.pred_set, args.limit)
    print(f"Loaded {len(df)} rows for pred_set {args.pred_set}", file=sys.stderr)

    def prior(source: str, gamma: float) -> dict:
        return {"ia_prior": {"enabled": True, "source": source, "gamma": gamma}}

    configs: list[tuple[str, ScoringConfig]] = [
        ("embedding_only (plain)", embedding_only_config()),
        ("composite (plain)", composite_config()),
        ("composite + freq-prior gamma=0.5", composite_config(prior("frequency", 0.5))),
        ("composite + freq-prior gamma=1.0", composite_config(prior("frequency", 1.0))),
        ("composite + freq-prior gamma=2.0", composite_config(prior("frequency", 2.0))),
        ("embedding_only + freq-prior gamma=1.0", embedding_only_config(prior("frequency", 1.0))),
    ]

    rows = []
    results = []
    for name, cfg in configs:
        scores = _vectorized_scores(df, cfg)
        scores = np.round(scores, 6)
        stats = describe(scores)
        results.append((name, stats))
        rows.append(fmt(name, stats))

    header = "| config | n | min | mean | % at 1.0 |\n| --- | --- | --- | --- | --- |\n"
    body = "\n".join(rows)
    report = header + body + "\n"
    print(report)

    if args.out:
        with open(args.out, "w") as f:
            f.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
