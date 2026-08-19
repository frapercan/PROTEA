"""Resolve each query's neighbourhood, and pool per-protein scores into cells.

Two halves that the strata vocabulary in :mod:`protea.core.strata` was written
for and had no consumer:

* :func:`neighbourhoods_for` reads what the retrieval actually returned, so a
  protein can be placed on the homology, donor-evidence and taxonomy axes;
* :func:`micro_cells` pools the per-protein rows into one number per cell.

Pooling is MICRO, summing tp / pred / n_gt and then dividing, not the mean of
each protein's F. The two differ whenever proteins carry different numbers of
terms, which they always do, and `f_micro_w` is the micro one: every published
cell in this project reports it. Averaging per-protein F would silently publish
a macro average under a micro name.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, NamedTuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.ia_regimes import EXPERIMENTAL_EVIDENCE
from protea.core.strata import Neighbourhood

# The self hit is excluded by SEQUENCE, not by accession. Two accessions can
# carry the identical sequence (isoforms, re-annotations, cross-references),
# and such a donor is the same twin under a different name. Excluding only the
# matching accession would leave those in and the axes would still be reading
# the protocol.
_NEIGHBOURHOOD_SQL = """
WITH donors AS (
    SELECT g.protein_accession           AS acc,
           g.identity_nw * 100.0         AS identity_pct,
           g.distance                    AS distance,
           g.evidence_code               AS evidence_code,
           g.taxonomic_relation          AS taxonomic_relation
    FROM go_prediction g
    JOIN protein qp ON qp.accession = g.protein_accession
    JOIN protein rp ON rp.accession = g.ref_protein_accession
    WHERE g.prediction_set_id = :pset
      AND qp.sequence_id <> rp.sequence_id
),
nearest AS (
    SELECT DISTINCT ON (acc) acc, evidence_code, taxonomic_relation, distance
    FROM donors ORDER BY acc, distance ASC
),
nearest_exp AS (
    SELECT DISTINCT ON (acc) acc, distance
    FROM donors WHERE evidence_code = ANY(:exp_codes)
    ORDER BY acc, distance ASC
),
best AS (
    SELECT acc, max(identity_pct) AS best_identity FROM donors GROUP BY acc
)
SELECT b.acc,
       b.best_identity,
       n.evidence_code,
       n.taxonomic_relation,
       n.distance          AS nearest_any,
       e.distance          AS nearest_experimental
FROM best b
JOIN nearest n ON n.acc = b.acc
LEFT JOIN nearest_exp e ON e.acc = b.acc
"""


def neighbourhoods_for(session: Session, prediction_set_id: str) -> dict[str, Neighbourhood]:
    """One :class:`Neighbourhood` per query that retrieved a non-self donor.

    Queries absent from the result retrieved nothing but themselves. They are
    absent rather than defaulted, because "no neighbour" and "a neighbour we
    did not look up" are different facts and only the caller knows which it is
    looking at.
    """
    rows = session.execute(
        text(_NEIGHBOURHOOD_SQL),
        {"pset": prediction_set_id, "exp_codes": list(EXPERIMENTAL_EVIDENCE)},
    ).mappings()
    out: dict[str, Neighbourhood] = {}
    for r in rows:
        out[r["acc"]] = Neighbourhood(
            best_identity=None if r["best_identity"] is None else float(r["best_identity"]),
            donor_is_experimental=r["evidence_code"] in EXPERIMENTAL_EVIDENCE,
            taxonomic_relation=r["taxonomic_relation"],
            nearest_any=None if r["nearest_any"] is None else float(r["nearest_any"]),
            nearest_experimental=(
                None if r["nearest_experimental"] is None else float(r["nearest_experimental"])
            ),
        )
    return out


class Cell(NamedTuple):
    """One pooled result, with the population it was pooled over.

    ``n_proteins`` travels with the score because a cell is only readable
    beside its population: 0.61 over 85 proteins and 0.61 over 3,886 are not
    the same claim, and a table that prints only the number hides which is
    which.
    """

    n_proteins: int
    tp_w: float
    pred_w: float
    n_gt_w: float
    precision_w: float
    recall_w: float
    f_micro_w: float


def _micro(tp: float, pred: float, n_gt: float, n: int) -> Cell:
    precision = tp / pred if pred > 0 else 0.0
    recall = tp / n_gt if n_gt > 0 else 0.0
    denom = precision + recall
    f = (2 * precision * recall / denom) if denom > 0 else 0.0
    return Cell(n, tp, pred, n_gt, precision, recall, f)


def micro_cells(
    rows: Iterable[Mapping[str, Any]],
    key_for: Callable[[Mapping[str, Any]], Any],
) -> dict[Any, Cell]:
    """Pool per-protein rows into one micro-averaged cell per key.

    ``key_for`` returns whatever the caller groups by, which is how a run
    chooses its axes: the full stratum, or a projection onto the two or three
    that a given table reports.
    """
    sums: dict[Any, list[float]] = {}
    for row in rows:
        key = key_for(row)
        if key is None:
            continue
        acc = sums.setdefault(key, [0.0, 0.0, 0.0, 0.0])
        acc[0] += float(row["tp_w"])
        acc[1] += float(row["pred_w"])
        acc[2] += float(row["n_gt_w"])
        acc[3] += 1.0
    return {k: _micro(v[0], v[1], v[2], int(v[3])) for k, v in sums.items()}


def project(stratum: Any, axes: Sequence[str]) -> tuple:
    """The stratum restricted to ``axes``, for tables that report a face of it.

    Crossing all seven axes leaves most cells empty; a report usually wants
    two or three. Projecting here rather than at each call site keeps the
    axis names in one place and makes an unknown axis an error instead of a
    silently missing column.
    """
    unknown = [a for a in axes if a not in stratum._fields]
    if unknown:
        raise ValueError(f"unknown stratum axes {unknown}; known are {list(stratum._fields)}")
    return tuple(getattr(stratum, a) for a in axes)
