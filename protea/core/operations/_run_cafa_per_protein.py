"""Persist the per-protein scores cafaeval would otherwise reduce away.

The evaluation reports one number per (category, aspect) cell. Three of the axes
this project is required to report, sequence length, identity to the nearest
donor and taxonomic relation to it, are properties of a PROTEIN, not of a cell,
so none of them can be recovered from the stored aggregate however it is sliced.

cafaeval computes the per-protein vectors and collapses them with
``.sum(axis=0)``. Its ``PerProteinSink`` hands them over before that happens.
This module turns one sink into a parquet artifact next to the run's other
outputs, keyed by accession so it joins to ``protein.length`` and to the donor
identities in ``go_prediction``.

A parquet artifact rather than a table on purpose: a table would need a
migration, and migrations here are validated offline and never applied to the
live store as a side effect of an evaluation.
"""

from __future__ import annotations

from typing import Any

import numpy as np


#: cafaeval builds its thresholds as ``np.arange(th_step, 1, th_step)``. We
#: rebuild the same vector to find which column the reported tau sits in, and
#: assert the width matches rather than trusting the reconstruction: a silent
#: off-by-one here would attribute every protein the score of a neighbouring
#: threshold, which reads as a plausible number.
def _tau_array(th_step: float) -> np.ndarray:
    return np.arange(th_step, 1, th_step)


class PerProteinShapeError(RuntimeError):
    """The rebuilt threshold vector does not match the emitted arrays."""


def _column_for_tau(th_step: float, width: int, tau: float) -> int:
    tau_arr = _tau_array(th_step)
    if len(tau_arr) != width:
        raise PerProteinShapeError(
            f"rebuilt {len(tau_arr)} thresholds from th_step={th_step} but the "
            f"emitted arrays are {width} wide; refusing to guess which column "
            f"the reported tau is in"
        )
    return int(np.argmin(np.abs(tau_arr - tau)))


def _accessions_for(record: dict[str, Any], n_rows: int) -> list[str] | None:
    """Name each array row, or return None when the record cannot be keyed.

    ``ids`` maps accession to the ground truth's row number; ``row_index`` maps
    array row to that same numbering, and the two differ whenever the kernel was
    handed a subset. Both are required: a record carrying one without the other
    cannot be joined and is dropped rather than written under a guess.
    """
    ids, row_index = record.get("ids"), record.get("row_index")
    if not ids or row_index is None or len(row_index) != n_rows:
        return None
    by_index = {int(v): k for k, v in ids.items()}
    out = [by_index.get(int(i)) for i in row_index]
    return None if any(a is None for a in out) else out  # type: ignore[return-value]


def rows_from_sink(sink: Any, *, th_step: float, tau_by_ns: dict[str, float]) -> list[dict]:
    """Flatten a sink into per-(namespace, protein) rows at the reported tau.

    Only the weighted variant is kept: it is the one carrying ``f_micro_w``,
    which is the metric every published cell in this project reports.
    """
    rows: list[dict] = []
    for rec in getattr(sink, "records", []):
        if rec.get("variant") != "weighted":
            continue
        ns = rec.get("ns")
        tau = tau_by_ns.get(str(ns))
        if tau is None:
            continue
        tp, pred = rec["tp_at_tau"], rec["pred_at_tau"]
        col = _column_for_tau(th_step, tp.shape[1], tau)
        accs = _accessions_for(rec, tp.shape[0])
        if accs is None:
            continue
        n_gt = np.asarray(rec["n_gt"], dtype=np.float64)
        tp_c, pred_c = tp[:, col], pred[:, col]
        prec = np.where(pred_c > 0, tp_c / np.where(pred_c > 0, pred_c, 1.0), 0.0)
        rec_c = np.where(n_gt > 0, tp_c / np.where(n_gt > 0, n_gt, 1.0), 0.0)
        denom = prec + rec_c
        f = np.where(denom > 0, 2 * prec * rec_c / np.where(denom > 0, denom, 1.0), 0.0)
        for i, acc in enumerate(accs):
            rows.append(
                {
                    "protein_accession": acc,
                    "namespace": str(ns),
                    "tau": float(tau),
                    "tp_w": float(tp_c[i]),
                    "pred_w": float(pred_c[i]),
                    "n_gt_w": float(n_gt[i]),
                    "precision_w": float(prec[i]),
                    "recall_w": float(rec_c[i]),
                    "f_w": float(f[i]),
                }
            )
    return rows
