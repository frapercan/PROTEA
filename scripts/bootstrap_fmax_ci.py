"""Protein-level paired bootstrap CIs for cafaeval Fmax.

Loads ``predictions.tsv`` and ``gt_<tier>.tsv`` from the cafaeval
artifact bundle of one or two ``evaluation_result`` rows, computes the
per-protein best-F1 across thresholds, and reports the mean with a
95% bootstrap confidence interval.

Two modes:

* ``--eval-result-id A`` reports the marginal CI for cell A.
* ``--eval-result-id A --baseline-eval-result-id B`` reports the
  paired delta (A − B) and its CI; protein indices match across the
  two cells so the bootstrap captures the per-protein paired
  difference (the right thing for "rerank improves the cell" claims).

The script is CLI-only and reads from MinIO. It does not touch the
PROTEA database.

Usage::

    poetry run python scripts/bootstrap_fmax_ci.py \\
        --eval-result-id 94451038-9993-4dcc-83dd-2df0b328f2ca \\
        --baseline-eval-result-id 63ba289b-...                  \\
        --tier NK --n-resamples 1000
"""

from __future__ import annotations

import argparse
from collections import defaultdict

import numpy as np
from minio import Minio


def _load_predictions(client: Minio, bucket: str, key: str) -> list[tuple[str, str, float]]:
    obj = client.get_object(bucket, key)
    rows: list[tuple[str, str, float]] = []
    try:
        text = obj.read().decode()
    finally:
        obj.close()
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            try:
                rows.append((parts[0], parts[1], float(parts[2])))
            except ValueError:
                continue
    return rows


def _load_gt(client: Minio, bucket: str, key: str) -> list[tuple[str, str]]:
    obj = client.get_object(bucket, key)
    rows: list[tuple[str, str]] = []
    try:
        text = obj.read().decode()
    finally:
        obj.close()
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            rows.append((parts[0], parts[1]))
    return rows


def per_protein_fmax(
    pred_rows: list[tuple[str, str, float]],
    gt_rows: list[tuple[str, str]],
    *,
    n_thresholds: int = 100,
) -> dict[str, float]:
    """Compute the best F1 across ``n_thresholds`` per protein.

    Proteins present in ``gt_rows`` but with no predictions get 0.0;
    proteins present only in predictions are ignored (cafaeval's
    ``no_orphans=True`` semantics).
    """
    gt_by_prot: dict[str, set[str]] = defaultdict(set)
    for prot, go in gt_rows:
        gt_by_prot[prot].add(go)
    pred_by_prot: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for prot, go, score in pred_rows:
        pred_by_prot[prot].append((go, score))

    thresholds = np.linspace(0.001, 1.0, n_thresholds)
    fmax: dict[str, float] = {}
    for prot, gt_set in gt_by_prot.items():
        cands = pred_by_prot.get(prot, [])
        if not gt_set:
            fmax[prot] = 0.0
            continue
        best = 0.0
        for tau in thresholds:
            predicted = {go for go, score in cands if score >= tau}
            if not predicted:
                continue
            tp = len(predicted & gt_set)
            if tp == 0:
                continue
            pr = tp / len(predicted)
            rc = tp / len(gt_set)
            f = 2 * pr * rc / (pr + rc) if (pr + rc) > 0 else 0.0
            if f > best:
                best = f
        fmax[prot] = best
    return fmax


def bootstrap_ci(
    values: np.ndarray,
    *,
    n_resamples: int = 1000,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple[float, float, float]:
    """Return ``(point_mean, ci_low, ci_high)`` from ``n_resamples`` resamples."""
    rng = np.random.default_rng(seed)
    n = values.size
    means = np.empty(n_resamples, dtype=np.float64)
    for i in range(n_resamples):
        idx = rng.integers(0, n, size=n)
        means[i] = values[idx].mean()
    return float(values.mean()), float(np.quantile(means, alpha / 2)), float(
        np.quantile(means, 1 - alpha / 2)
    )


def paired_bootstrap_delta(
    a_values: np.ndarray,
    b_values: np.ndarray,
    *,
    n_resamples: int = 1000,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple[float, float, float]:
    """Paired bootstrap: same protein indices in both arrays.

    Returns ``(point_delta, ci_low, ci_high)`` where ``point_delta =
    mean(a) - mean(b)``.
    """
    if a_values.size != b_values.size:
        raise ValueError(
            f"paired bootstrap needs same length: a={a_values.size} b={b_values.size}"
        )
    rng = np.random.default_rng(seed)
    n = a_values.size
    deltas = np.empty(n_resamples, dtype=np.float64)
    for i in range(n_resamples):
        idx = rng.integers(0, n, size=n)
        deltas[i] = a_values[idx].mean() - b_values[idx].mean()
    return (
        float(a_values.mean() - b_values.mean()),
        float(np.quantile(deltas, alpha / 2)),
        float(np.quantile(deltas, 1 - alpha / 2)),
    )


def _make_client(args: argparse.Namespace) -> Minio:
    return Minio(
        args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure,
    )


def _fmax_for_eval(
    client: Minio,
    bucket: str,
    eval_id: str,
    tier: str,
    n_thresholds: int,
) -> dict[str, float]:
    base = f"eval_artifacts/{eval_id}"
    preds = _load_predictions(client, bucket, f"{base}/predictions/predictions.tsv")
    gt = _load_gt(client, bucket, f"{base}/gt_{tier}.tsv")
    print(f"  {eval_id} {tier}: {len(preds)} pred rows, {len(gt)} gt rows")
    return per_protein_fmax(preds, gt, n_thresholds=n_thresholds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eval-result-id", required=True)
    parser.add_argument(
        "--baseline-eval-result-id",
        default=None,
        help="If set, paired delta vs this baseline cell.",
    )
    parser.add_argument("--tier", required=True, choices=["NK", "LK", "PK"])
    parser.add_argument("--n-resamples", type=int, default=1000)
    parser.add_argument("--n-thresholds", type=int, default=100)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--bucket", default="protea")
    parser.add_argument("--endpoint", default="localhost:9000")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--secure", action="store_true")
    args = parser.parse_args()

    client = _make_client(args)

    print(f"Computing per-protein Fmax (tier={args.tier}, thresholds={args.n_thresholds})")
    fmax_a = _fmax_for_eval(client, args.bucket, args.eval_result_id, args.tier, args.n_thresholds)

    if args.baseline_eval_result_id:
        fmax_b = _fmax_for_eval(
            client, args.bucket, args.baseline_eval_result_id, args.tier, args.n_thresholds
        )
        common = sorted(set(fmax_a.keys()) & set(fmax_b.keys()))
        if not common:
            raise SystemExit("no proteins shared between the two eval_result rows")
        a_arr = np.array([fmax_a[p] for p in common])
        b_arr = np.array([fmax_b[p] for p in common])
        point, lo, hi = paired_bootstrap_delta(
            a_arr, b_arr, n_resamples=args.n_resamples, seed=args.seed
        )
        print(
            f"\nPaired delta ({args.tier}, n={len(common)} proteins, "
            f"{args.n_resamples} resamples):"
        )
        print(f"  A={a_arr.mean():.4f}  B={b_arr.mean():.4f}")
        print(f"  Δ = {point:+.4f}  95% CI [{lo:+.4f}, {hi:+.4f}]")
    else:
        arr = np.array(list(fmax_a.values()))
        point, lo, hi = bootstrap_ci(arr, n_resamples=args.n_resamples, seed=args.seed)
        print(
            f"\nMarginal ({args.tier}, n={arr.size} proteins, "
            f"{args.n_resamples} resamples):"
        )
        print(f"  Fmax = {point:.4f}  95% CI [{lo:.4f}, {hi:.4f}]")


if __name__ == "__main__":
    main()
