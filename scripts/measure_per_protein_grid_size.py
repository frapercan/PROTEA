#!/usr/bin/env python
"""Measure what one setting's whole-grid per-protein artefact costs on disk.

The size figure in ``_run_cafa_per_protein`` is a claim about bytes, and a
claim about bytes that lives only in a comment goes stale within two pyarrow
releases with nobody the wiser. This reproduces it.

Not a test. The number depends on the pyarrow version, the compression codec
and, most of all, the score distribution, so pinning it as an assertion would
fail on an upgrade that changed nothing about this producer. Run it when the
figure is quoted and when a dependency moves.

Synthetic and stated as such: predicted terms per protein uniform on [15, 120),
scores Beta(1.5, 3), information-accretion weights Gamma(3, 2), a third of the
predictions true. Real predictions whose scores concentrate on few distinct
values compress better than this, and ones that spread them compress worse.

    python scripts/measure_per_protein_grid_size.py [--rows 20000] [--seed 7]
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import numpy as np

from protea.core.operations._run_cafa_per_protein import (
    GRID_SCHEMA_VERSION,
    GridArtifact,
    tau_grid_for,
    write_grid_parquet,
)

TH_STEP = 0.01


def _rows(n: int, n_tau: int, tau: np.ndarray, rng: np.random.Generator) -> list[dict]:
    rows = []
    for i in range(n):
        k = int(rng.integers(15, 121))
        scores = rng.beta(1.5, 3.0, size=k)
        weights = rng.gamma(3.0, 2.0, size=k)
        is_tp = rng.random(k) < 0.35
        idx = np.clip(np.searchsorted(tau, scores, side="right") - 1, 0, n_tau - 1)

        def curve(w: np.ndarray | None, at: np.ndarray = idx) -> np.ndarray:
            delta = np.bincount(at, weights=w, minlength=n_tau).astype(np.float64)
            return np.cumsum(delta[::-1])[::-1]

        rows.append(
            {
                "protein_accession": f"P{i:06d}",
                "namespace": "biological_process",
                "tp_w": curve(weights * is_tp),
                "pred_w": curve(weights),
                "n_gt_w": float(weights.sum()),
                "tp": curve(is_tp.astype(np.float64)),
                "pred": curve(None),
                "n_gt": float(k),
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    grid = tau_grid_for(TH_STEP)
    n_tau = len(grid)
    tau = np.asarray(grid)
    rows = _rows(args.rows, n_tau, tau, np.random.default_rng(args.seed))
    footer = {
        "version": GRID_SCHEMA_VERSION,
        "tau_grid": json.dumps(grid),
        "th_step": repr(TH_STEP),
        "normalization": "cafa",
        "prop": "fill",
        "no_orphans": "true",
        "max_terms": "null",
        "information_accretion_set_id": "measurement",
        "ontology_snapshot_id": "measurement",
        "evaluation_set_id": "measurement",
    }
    out = Path(tempfile.mkdtemp())
    print(f"{args.rows} rows, {n_tau} thresholds, seed {args.seed}")
    for variants in (("weighted", "unweighted"), ("weighted",)):
        artifact = GridArtifact(rows, variants, n_tau, [])
        path = write_grid_parquet(
            out / f"{'_'.join(variants)}.parquet",
            artifact,
            {**footer, "variants": json.dumps(list(variants))},
        )
        size = path.stat().st_size
        label = " + ".join(variants)
        print(f"  {label:<22} {size / 1e6:6.2f} MB   {size / args.rows:6.1f} B/row")


if __name__ == "__main__":
    main()
