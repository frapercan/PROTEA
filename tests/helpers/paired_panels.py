"""A reference producer for the per-protein threshold-grid artefact.

``compare_paired_panels`` defines a contract whose producer does not exist yet.
This writes files that satisfy it, so the consumer's gates are exercised against
something real rather than against a mock, and so the contract is expressed once
as bytes on disk instead of twice in prose.

Deliberately parameterised for corruption: several tests need a file that is
right in every respect but one.

A row carries ``tp``/``pred``/``n_gt`` for the weighted variant and, when the
file declares the unweighted one, ``tp_u``/``pred_u``/``n_gt_u`` for it. They
default to the weighted values so most fixtures stay short, but they are
separate on purpose: a helper that wrote one set of numbers under both names
would make the two variants bit-identical, and then no test could tell a
consumer reading the wrong columns from one reading the right ones.

``n_gt`` is written as its own column in every file whatever variants it
declares. It is the unweighted ground-truth count, which the contract requires
as the eligibility marker and the population denominator.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

DEFAULT_METADATA: dict[str, Any] = {
    "normalization": "cafa",
    "prop": "fill",
    "no_orphans": "true",
    "max_terms": "null",
    "producer": "run_cafa_evaluation",
    "producer_git_sha": "0" * 40,
    "ontology_snapshot_id": "11111111-1111-1111-1111-111111111111",
    "evaluation_set_id": "22222222-2222-2222-2222-222222222222",
    "information_accretion_set_id": "33333333-3333-3333-3333-333333333333",
}


def tau_grid_for(th_step: float) -> list[float]:
    """cafaeval's own grid, which is what the producer must declare."""
    return [float(x) for x in np.arange(th_step, 1, th_step)]


def write_grid_parquet(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    th_step: float,
    setting: str,
    variants: tuple[str, ...] = ("weighted",),
    list_type: str = "fixed",
    version: str | None = "1",
    tau_grid: list[float] | None = None,
    extra_columns: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Write one setting's grid file. ``rows`` carry ``tp``/``pred`` as sequences."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    grid = tau_grid_for(th_step) if tau_grid is None else tau_grid
    width = len(rows[0]["tp"]) if rows else len(grid)
    fields: list[pa.Field] = [
        pa.field("protein_accession", pa.string()),
        pa.field("namespace", pa.string()),
    ]
    arrays: list[pa.Array] = [
        pa.array([r["accession"] for r in rows], pa.string()),
        pa.array([r["namespace"] for r in rows], pa.string()),
    ]
    written: set[str] = set()
    for variant, (tp_col, pred_col, gt_col), suffix in (
        ("weighted", ("tp_w", "pred_w", "n_gt_w"), ""),
        ("unweighted", ("tp", "pred", "n_gt"), "_u"),
    ):
        if variant not in variants:
            continue
        for name, key in ((tp_col, "tp"), (pred_col, "pred")):
            values = [np.asarray(r.get(key + suffix, r[key]), dtype=np.float32) for r in rows]
            flat = pa.array(
                np.concatenate(values) if rows else np.zeros(0, dtype=np.float32), pa.float32()
            )
            if list_type == "fixed":
                fields.append(pa.field(name, pa.list_(pa.float32(), width)))
                arrays.append(pa.FixedSizeListArray.from_arrays(flat, width))
            else:
                offsets = np.cumsum([0, *[len(v) for v in values]]).astype(np.int32)
                fields.append(pa.field(name, pa.list_(pa.float32())))
                arrays.append(pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), flat))
        fields.append(pa.field(gt_col, pa.float64()))
        arrays.append(
            pa.array([float(r.get("n_gt" + suffix, r["n_gt"])) for r in rows], pa.float64())
        )
        written.add(gt_col)
    if "n_gt" not in written:
        # Mandatory whatever the variants: the contract's eligibility marker.
        fields.append(pa.field("n_gt", pa.float64()))
        arrays.append(
            pa.array([float(r.get("n_gt_u", r["n_gt"])) for r in rows], pa.float64())
        )
    for name, values in (extra_columns or {}).items():
        fields.append(pa.field(name, pa.float64()))
        arrays.append(pa.array(values, pa.float64()))

    meta = {**DEFAULT_METADATA, **(metadata or {})}
    meta.update(
        {
            "tau_grid": json.dumps(grid),
            "th_step": repr(float(th_step)),
            "variants": json.dumps(list(variants)),
            "setting": setting,
        }
    )
    if version is not None:
        meta["version"] = version
    encoded = {f"protea.per_protein_grid.{k}".encode(): str(v).encode() for k, v in meta.items()}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata=encoded))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def write_panel(
    root: Path,
    setting: str,
    namespace: str,
    *,
    accessions: list[str],
    tp: np.ndarray,
    pred: np.ndarray,
    n_gt: np.ndarray,
    th_step: float,
    unweighted: tuple[np.ndarray, np.ndarray, np.ndarray] | None = None,
    **kwargs: Any,
) -> Path:
    """One namespace's worth of arrays, written as that setting's whole file."""
    rows = [
        {
            "accession": acc,
            "namespace": namespace,
            "tp": tp[i].tolist(),
            "pred": pred[i].tolist(),
            "n_gt": float(n_gt[i]),
            **(
                {}
                if unweighted is None
                else {
                    "tp_u": unweighted[0][i].tolist(),
                    "pred_u": unweighted[1][i].tolist(),
                    "n_gt_u": float(unweighted[2][i]),
                }
            ),
        }
        for i, acc in enumerate(accessions)
    ]
    target = root / setting / "per_protein_grid.parquet"
    return write_grid_parquet(target, rows, th_step=th_step, setting=setting, **kwargs)


def write_legacy_parquet(path: Path, *, namespace: str = "molecular_function") -> Path:
    """The artefact that exists today: one tau per (protein, namespace)."""
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "protein_accession": f"P{i:05d}",
                "namespace": namespace,
                "tau": 0.31,
                "tp_w": 1.0,
                "pred_w": 2.0,
                "n_gt_w": 2.0,
                "precision_w": 0.5,
                "recall_w": 0.5,
                "f_w": 0.5,
            }
            for i in range(4)
        ]
    ).to_parquet(path, index=False)
    return path
