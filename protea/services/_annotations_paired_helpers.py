"""The paired comparisons of a window's results, read from the jobs that ran them.

``compare_paired_panels`` writes no row of its own: it is read-only by design, and
its inputs, seed and refusals are recovered from its job. So a comparison's record
is its job, whose payload names the two results and whose succeeded event carries
the result. This module reads, for one window, the latest comparison of every pair
of its results over the whole panel or restricted to one stratum, and the part of
each panel the window page shows.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

#: The latest succeeded comparison of each pair whose two results are on the
#: window, restricted to exactly the stratum asked for: ``'null'`` asks for the
#: whole panel, so a comparison restricted to a stratum, which is another
#: question, never stands in for it, nor one stratum for another.
_COMPARISONS = text(
    """
    SELECT DISTINCT ON (
               j.payload ->> 'evaluation_result_id', j.payload ->> 'baseline_evaluation_result_id'
           )
           j.id::text AS job_id,
           j.payload ->> 'evaluation_result_id' AS evaluation_result_id,
           j.payload ->> 'baseline_evaluation_result_id' AS baseline_evaluation_result_id,
           j.finished_at AS finished_at,
           j.payload -> 'restrict_to_stratum' AS restrict_to_stratum,
           e.fields -> 'result' AS result
    FROM job j
    JOIN job_event e ON e.job_id = j.id AND e.event = 'job.succeeded'
    WHERE j.operation = 'compare_paired_panels'
      AND j.status = 'SUCCEEDED'
      AND j.payload ->> 'evaluation_result_id' = ANY(:results)
      AND j.payload ->> 'baseline_evaluation_result_id' = ANY(:results)
      AND COALESCE(j.payload -> 'restrict_to_stratum', 'null'::jsonb) = CAST(:stratum AS jsonb)
    ORDER BY j.payload ->> 'evaluation_result_id',
             j.payload ->> 'baseline_evaluation_result_id',
             j.finished_at DESC
    """
)

#: What the page shows of each panel; the job keeps everything else.
PANEL_FIELDS: tuple[str, ...] = (
    "delta",
    "ci_low",
    "ci_high",
    "interval_method",
    "verdict",
    "status",
    "n_paired",
    # What a null is read against: the difference the comparison declared worth
    # detecting, and the smallest one it could have detected. Without them a page
    # cannot tell sameness within the effect (null_with_power) from too little
    # data to tell (underpowered).
    "effect_of_interest",
    "minimum_detectable_effect",
)

#: What the page shows of the comparison as a whole. ``ladder_declaration`` is the rung
#: the comparison was declared to belong to before it was read, or None.
COMPARISON_FIELDS: tuple[str, ...] = (
    "method_axis",
    "operating_point",
    "weighting",
    "n_resamples",
    "seed",
    "confidence",
    "ladder_declaration",
)


def comparison_to_dict(row: Any) -> dict[str, Any]:
    """One comparison as the page reads it, from its job's row and result."""
    result = row["result"] or {}
    panels = result.get("panels") or {}
    finished = row["finished_at"]
    return {
        "job_id": row["job_id"],
        "evaluation_result_id": row["evaluation_result_id"],
        "baseline_evaluation_result_id": row["baseline_evaluation_result_id"],
        "finished_at": finished.isoformat() if finished is not None else None,
        "restrict_to_stratum": row.get("restrict_to_stratum"),
        **{field: result.get(field) for field in COMPARISON_FIELDS},
        "panels": {
            key: {field: panel.get(field) for field in PANEL_FIELDS}
            for key, panel in panels.items()
        },
    }


def list_paired_comparisons_data(
    session: Session, eval_id: uuid.UUID, stratum: dict[str, str] | None = None
) -> list[dict[str, Any]]:
    """The latest comparison of each pair of the window's results, on ``stratum``.

    ``None`` asks for the comparisons over the whole panel; ``{"homology": ">90"}``
    for those restricted to that band, as ``compare_paired_panels`` was asked.
    Raises :class:`ValueError` for a stratum outside the closed vocabularies, read
    by the same rule the comparison's payload is (``validate_stratum``), and
    :class:`EntityNotFoundError` when the window does not resolve.
    """
    from protea.core.operations._paired_panels_stratum import validate_stratum

    stratum = validate_stratum(stratum)
    from protea.services.annotations_service import EntityNotFoundError

    if session.get(EvaluationSet, eval_id) is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    results = [
        str(i)
        for (i,) in session.query(EvaluationResult.id).filter(
            EvaluationResult.evaluation_set_id == eval_id
        )
    ]
    if not results:
        return []
    rows = session.execute(
        _COMPARISONS, {"results": results, "stratum": json.dumps(stratum)}
    ).mappings()
    return [comparison_to_dict(row) for row in rows]
