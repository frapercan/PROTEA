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
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
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


def parse_obo_parents(obo_path: str) -> dict[str, list[str]]:
    """Return ``go_id → list of immediate parents`` from an OBO file.

    Edges followed: ``is_a`` and ``relationship: part_of`` (matches
    cafaeval's ``prop="fill"`` propagation rules). Obsolete terms are
    dropped (no parents recorded). Unknown relationship types are
    ignored.

    Parents are buffered per-term and only committed at end-of-term so
    that ``is_obsolete`` flags appearing after ``is_a`` lines still
    drop the term cleanly.
    """
    parents: dict[str, list[str]] = {}
    current_id: str | None = None
    pending_parents: list[str] = []
    in_term = False
    obsolete = False

    def _flush() -> None:
        nonlocal current_id, obsolete, pending_parents
        if current_id and not obsolete:
            parents[current_id] = list(pending_parents)
        current_id = None
        pending_parents = []
        obsolete = False

    with open(obo_path) as f:
        for raw in f:
            line = raw.rstrip("\n").strip()
            if line == "[Term]":
                _flush()
                in_term = True
                continue
            if line.startswith("[") and line != "[Term]":
                _flush()
                in_term = False
                continue
            if not in_term or not line:
                continue
            if line.startswith("id: GO:"):
                current_id = line.split(None, 1)[1].strip()
            elif line == "is_obsolete: true":
                obsolete = True
            elif line.startswith("is_a: GO:"):
                p = line.split(None, 1)[1].split("!")[0].strip()
                pending_parents.append(p)
            elif line.startswith("relationship: part_of GO:"):
                tail = line[len("relationship: part_of "):]
                p = tail.split("!")[0].strip()
                pending_parents.append(p)
    _flush()
    return parents


def all_ancestors(go_id: str, parents: dict[str, list[str]]) -> set[str]:
    """Return ``{go_id} ∪ all reachable ancestors`` (BFS over ``parents``)."""
    out = {go_id}
    stack = [go_id]
    while stack:
        node = stack.pop()
        for p in parents.get(node, ()):
            if p not in out:
                out.add(p)
                stack.append(p)
    return out


def propagate_predictions(
    pred_rows: list[tuple[str, str, float]],
    parents: dict[str, list[str]],
) -> list[tuple[str, str, float]]:
    """Propagate predicted GO terms up the ancestor closure.

    Each ``(prot, go, score)`` row contributes its score to every
    ancestor of ``go``. When the same ``(prot, ancestor)`` pair is
    reached from multiple leaves, the maximum score wins (matches
    cafaeval's ``prop="fill"`` semantics).
    """
    best: dict[tuple[str, str], float] = {}
    cache: dict[str, set[str]] = {}
    for prot, go, score in pred_rows:
        anc = cache.get(go)
        if anc is None:
            anc = all_ancestors(go, parents)
            cache[go] = anc
        for a in anc:
            key = (prot, a)
            cur = best.get(key)
            if cur is None or score > cur:
                best[key] = score
    return [(p, g, s) for (p, g), s in best.items()]


def propagate_gt(
    gt_rows: list[tuple[str, str]],
    parents: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """Propagate ground-truth GO terms up the ancestor closure (deduped)."""
    out: set[tuple[str, str]] = set()
    cache: dict[str, set[str]] = {}
    for prot, go in gt_rows:
        anc = cache.get(go)
        if anc is None:
            anc = all_ancestors(go, parents)
            cache[go] = anc
        for a in anc:
            out.add((prot, a))
    return list(out)


_ASPECT_LETTER_TO_CAFA = {
    "P": "BPO",
    "F": "MFO",
    "C": "CCO",
}


def _load_aspect_map(path: str) -> dict[str, str]:
    """Read a 2-col ``(go_id, aspect)`` TSV.

    Aspect may be the single-letter PROTEA encoding (``P``/``F``/``C``)
    or the CAFA code (``BPO``/``MFO``/``CCO``); both are normalised to
    the CAFA code in the returned dict. Lines that don't have at least
    two columns are skipped silently.
    """
    out: dict[str, str] = {}
    with open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            go_id, aspect_raw = parts[0].strip(), parts[1].strip()
            if not go_id or not aspect_raw:
                continue
            aspect = _ASPECT_LETTER_TO_CAFA.get(aspect_raw, aspect_raw)
            out[go_id] = aspect
    return out


def filter_by_aspect(
    pred_rows: list[tuple[str, str, float]],
    gt_rows: list[tuple[str, str]],
    aspect_map: dict[str, str],
    aspect: str,
) -> tuple[list[tuple[str, str, float]], list[tuple[str, str]]]:
    """Keep only rows whose GO id maps to ``aspect`` (CAFA code)."""
    keep_ids = {go for go, asp in aspect_map.items() if asp == aspect}
    preds = [(p, g, s) for (p, g, s) in pred_rows if g in keep_ids]
    gt = [(p, g) for (p, g) in gt_rows if g in keep_ids]
    return preds, gt


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
    from minio import Minio  # lazy: keeps the script importable without the storage extra

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
    aspect_map: dict[str, str] | None = None,
    aspect: str | None = None,
    parents: dict[str, list[str]] | None = None,
) -> dict[str, float]:
    base = f"eval_artifacts/{eval_id}"
    preds = _load_predictions(client, bucket, f"{base}/predictions/predictions.tsv")
    gt = _load_gt(client, bucket, f"{base}/gt_{tier}.tsv")
    if parents is not None:
        before = len(preds), len(gt)
        preds = propagate_predictions(preds, parents)
        gt = propagate_gt(gt, parents)
        print(f"  {eval_id} {tier}: propagated {before[0]}→{len(preds)} preds, "
              f"{before[1]}→{len(gt)} gt rows")
    # Aspect filter must run AFTER propagation so cross-aspect ancestors
    # (e.g. the all-aspects root) get pruned away.
    if aspect_map and aspect:
        preds, gt = filter_by_aspect(preds, gt, aspect_map, aspect)
        suffix = f" filtered to {aspect}"
    else:
        suffix = ""
    print(f"  {eval_id} {tier}: {len(preds)} pred rows, {len(gt)} gt rows{suffix}")
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
    parser.add_argument(
        "--aspect",
        default=None,
        choices=["MFO", "BPO", "CCO"],
        help="If set, filter predictions + gt to this aspect; requires --aspect-tsv.",
    )
    parser.add_argument(
        "--aspect-tsv",
        default=None,
        help="Path to a 2-col (go_id, aspect) TSV; "
        "needed when --aspect is set. Aspect may be P/F/C or BPO/MFO/CCO.",
    )
    parser.add_argument(
        "--obo-path",
        default=None,
        help="If set, propagate predictions + gt to ancestors via the OBO "
        "DAG (is_a + part_of edges) before computing per-protein Fmax. "
        "Use this to approach cafaeval's `prop=fill` numbers.",
    )
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

    if (args.aspect is None) != (args.aspect_tsv is None):
        raise SystemExit("--aspect and --aspect-tsv must be set together (or both unset)")
    aspect_map = _load_aspect_map(args.aspect_tsv) if args.aspect_tsv else None
    parents = parse_obo_parents(args.obo_path) if args.obo_path else None

    suffix_parts = []
    if args.aspect:
        suffix_parts.append(args.aspect)
    if parents is not None:
        suffix_parts.append(f"propagated ({len(parents)} OBO terms)")
    suffix = (" / " + ", ".join(suffix_parts)) if suffix_parts else ""
    print(
        f"Computing per-protein Fmax (tier={args.tier}{suffix}, "
        f"thresholds={args.n_thresholds})"
    )
    fmax_a = _fmax_for_eval(
        client,
        args.bucket,
        args.eval_result_id,
        args.tier,
        args.n_thresholds,
        aspect_map=aspect_map,
        aspect=args.aspect,
        parents=parents,
    )

    if args.baseline_eval_result_id:
        fmax_b = _fmax_for_eval(
            client,
            args.bucket,
            args.baseline_eval_result_id,
            args.tier,
            args.n_thresholds,
            aspect_map=aspect_map,
            aspect=args.aspect,
            parents=parents,
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
