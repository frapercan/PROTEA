"""Artifact-IO helpers extracted from ``run_cafa_evaluation``.

These pure functions handle downloading inputs (OBO, IA TSV), writing the
ground-truth and prediction TSVs that ``cafaeval`` consumes, and parsing the
metrics it produces. The prediction file is the method's stored scores, read
as they are (``_method_score_predictions``): the evaluator scores what a method
stored and nothing else.
"""

from __future__ import annotations

import gzip
import logging
import shutil
import uuid
from dataclasses import dataclass
from typing import Any

import requests
from sqlalchemy.orm import Session

from protea.core.operations import _method_score_predictions
from protea.core.operations._run_cafa_helpers import _NS_LABELS
from protea.core.row_alignment import assert_unique_key

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class WritePredictionsContext:
    """Which set's scores, for which proteins, written where."""

    pred_set_id: uuid.UUID
    delta_proteins: set[str]
    path: str


def download_obo(url: str, dest: str) -> None:
    """Download OBO file to dest, decompressing gzip if needed."""
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    if url.endswith(".gz"):
        with open(dest, "wb") as f:
            f.write(gzip.decompress(resp.content))
    else:
        with open(dest, "w", encoding="utf-8") as f:
            f.write(resp.text)


def _copy_local(local_path: str, dest: str, *, gzipped: bool) -> None:
    """Copy a path already on this filesystem, decompressing if needed."""
    if gzipped:
        with gzip.open(local_path, "rb") as src, open(dest, "wb") as f:
            shutil.copyfileobj(src, f)
    else:
        shutil.copy2(local_path, dest)


def _fetch_from_store(url: str, dest: str) -> None:
    """Read an ``s3://`` URI through the configured artifact store.

    The bucket in the URI is checked against the configured one rather than
    trusted. A key that exists in two buckets is two different files, and
    reading the wrong one would produce a plausible artifact under the right
    name, which is the failure this whole module exists to prevent.
    """
    from pathlib import Path as _Path

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    bucket, _, key = url[len("s3://") :].partition("/")
    if not key:
        raise ValueError(f"malformed s3 URI (no key): {url}")
    store = get_artifact_store(load_settings(_Path(__file__).resolve().parents[3]))
    configured = getattr(store, "bucket", None)
    if configured is not None and configured != bucket:
        raise ValueError(
            f"s3 URI bucket {bucket!r} does not match the configured "
            f"artifact-store bucket {configured!r}; refusing to read a "
            f"same-named key out of a different bucket"
        )
    blob = store.get(key)
    if url.endswith(".gz"):
        blob = gzip.decompress(blob)
    with open(dest, "wb") as f:
        f.write(blob)


def download_tsv(url: str, dest: str) -> None:
    """Copy or download a plain-text TSV file (gzip-transparent) to dest.

    Accepts HTTP(S) URLs, ``s3://bucket/key`` artifact-store URIs, and local
    filesystem paths (absolute or ``file://`` scheme).  Local paths are
    resolved without any network request, which is useful during development
    when the IA file lives inside the repository
    (``data/benchmarks/IA_cafa6.tsv``) and ``ia_url`` is set to its absolute
    path.  Once the file is pushed to GitHub the URL can be switched to the
    raw.githubusercontent.com address and the same code path handles it
    transparently.

    ``s3://`` is resolved through the configured ``ArtifactStore`` rather than
    over HTTP, which is what makes an artifact published by
    ``compute_information_accretion`` readable from any machine that shares the
    object store.  The bucket in the URI must match the configured bucket: a
    mismatch means the URI was produced against a different deployment, and
    silently reading the same key out of the local bucket would hand back a
    different file under the right-looking name.
    """
    local_path: str | None = None
    if url.startswith("file://"):
        local_path = url[len("file://") :]
    elif url.startswith("/"):
        local_path = url

    if local_path is not None:
        _copy_local(local_path, dest, gzipped=url.endswith(".gz"))
        return

    if url.startswith("s3://"):
        _fetch_from_store(url, dest)
        return

    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    if url.endswith(".gz"):
        with open(dest, "wb") as f:
            f.write(gzip.decompress(resp.content))
    else:
        with open(dest, "w", encoding="utf-8") as f:
            f.write(resp.text)


def write_gt(annotations: dict[str, set[str]], path: str) -> None:
    """Write {protein: {go_id}} to a 2-column TSV (no header)."""
    with open(path, "w") as f:
        for protein in sorted(annotations):
            for go_id in sorted(annotations[protein]):
                f.write(f"{protein}\t{go_id}\n")


def write_predictions(session: Session, ctx: WritePredictionsContext) -> int:
    """Write the method's stored scores (``protein\\tgo_id\\tscore``) for the window's targets.

    The scores are read as they are. A set that stores candidates without
    scores was refused before anything was staged
    (``_method_score_predictions.refuse_unless_scored``).
    """
    return _method_score_predictions.write_method_predictions(session, ctx)


def parse_results(dfs_best: dict) -> dict[str, Any]:
    """Extract per-namespace Fmax metrics from cafaeval dfs_best.

    The unweighted metrics are read from ``dfs_best["f"]`` (one row
    per namespace at the threshold optimising the protein-mean
    Fmax). When an IA file was supplied to cafaeval, the IA-weighted
    equivalents land in ``dfs_best["f_w"]`` (best by IA-weighted
    Fmax) and the micro-averaged Fmax in ``dfs_best["f_micro"]`` /
    ``dfs_best["f_micro_w"]``; these are surfaced here as the
    ``_w`` / ``_micro`` / ``_micro_w`` keys so chapter-6 tables can
    pull them without re-reading the per-tier TSV artifacts.
    """
    ns_results: dict[str, Any] = {}

    df_f = dfs_best.get("f")
    if df_f is None or df_f.empty:
        return ns_results

    df_f = df_f.reset_index()
    for _, row in df_f.iterrows():
        ns_long = str(row.get("ns", ""))
        ns = _NS_LABELS.get(ns_long)
        if ns is None:
            continue
        ns_results[ns] = {
            "fmax": round(float(row.get("f", 0)), 4),
            "precision": round(float(row.get("pr", 0)), 4),
            "recall": round(float(row.get("rc", 0)), 4),
            "tau": round(float(row.get("tau", 0)), 4),
            "coverage": round(float(row.get("cov_max", row.get("cov", 0))), 4),
            # Coverage at the threshold Fmax actually landed on, which is
            # the one that reconciles with n_proteins. ``coverage`` above is
            # cafaeval's cov_max, the maximum over every threshold, so the
            # two describe different points and reading them as a pair makes
            # the population look as though it moved. It did not: n is the
            # count of proteins with a prediction at this tau, and a single
            # 0.98 -> 0.99 step moved it by 17 per cent in rung 1 while the
            # scored cohort was provably identical across all 32 runs.
            "coverage_at_tau": (
                round(float(row["cov"]), 4) if row.get("cov") is not None else None
            ),
            "n_proteins": int(row.get("n", 0)) if "n" in row else None,
        }

    _merge_weighted_metrics(dfs_best, ns_results)
    return ns_results


# Maps each extra cafaeval best-frame to the ``cafaeval column -> persisted
# field`` it contributes. The IA-weighted micro frame (the LAFA headline
# metric) also carries the weighted micro precision / recall / coverage;
# persisting those per aspect (not only the unweighted pr/rc) is what makes
# the blob LAFA-comparable. See docs/EVAL_LAFA_PARITY.md and FIX-METRIC-IA.
#
# Two coverages, as in the unweighted pair ``coverage`` / ``coverage_at_tau``.
# ``coverage_w`` is cafaeval's ``cov_max``, the largest coverage over every
# threshold. ``coverage_w_at_tau`` is ``cov_w``, the coverage at the threshold
# ``f_micro_w`` was reported at, which is the coverage LAFA prints beside it.
# Until 2026-11-21 only the first was persisted, under a name that read as the
# second: on 211 -> 227, PK biological process, 0.444 against LAFA's 0.441.
_EXTRA_METRIC_SPECS: tuple[tuple[str, dict[str, str]], ...] = (
    ("f_w", {"f_w": "fmax_w"}),
    ("f_micro", {"f_micro": "f_micro"}),
    (
        "f_micro_w",
        {
            "f_micro_w": "f_micro_w",
            "pr_micro_w": "precision_w",
            "rc_micro_w": "recall_w",
            "cov_max": "coverage_w",
            "cov_w": "coverage_w_at_tau",
        },
    ),
)


def _merge_weighted_metrics(dfs_best: dict, ns_results: dict[str, Any]) -> None:
    """Fold the IA-weighted / micro best-frames into ``ns_results`` in place.

    Each frame is keyed by namespace; only namespaces already present from the
    unweighted ``f`` frame are touched. Missing frames (uniform IC=1 fallback)
    are skipped, leaving the unweighted keys alone.

    The namespace key must be unique per frame. If it were not, the last row
    for a namespace would overwrite the earlier ones and the published metric
    would silently belong to a different row than the one it is attributed to,
    with the result keeping its expected shape throughout. That is checked
    rather than assumed, because it is the failure mode that produces no error.
    """
    for key, col_to_field in _EXTRA_METRIC_SPECS:
        df_extra = dfs_best.get(key)
        if df_extra is None or df_extra.empty:
            continue
        df_extra = df_extra.reset_index()
        assert_unique_key(
            (str(r.get("ns", "")) for _, r in df_extra.iterrows()),
            lambda ns: ns,
            context=f"folding the {key!r} best-frame into the per-namespace results",
        )
        for _, row in df_extra.iterrows():
            ns = _NS_LABELS.get(str(row.get("ns", "")))
            if ns is None or ns not in ns_results:
                continue
            for col, field in col_to_field.items():
                if col in row:
                    ns_results[ns][field] = round(float(row.get(col, 0)), 4)
