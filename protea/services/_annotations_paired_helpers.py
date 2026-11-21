"""The paired comparisons of a window's results, read from the jobs that ran them.

``compare_paired_panels`` writes no row of its own: it is read-only by design, and
its inputs, seed and refusals are recovered from its job. So a comparison's record
is its job, whose payload names the two results and whose succeeded event carries
the result. This module reads, for one window, the latest whole-panel comparison of
every pair of its results, and the part of each panel the window page shows.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

#: The latest succeeded comparison of each pair whose two results are on the
#: window. A comparison restricted to a stratum is another question and is left out.
_COMPARISONS = text(
    """
    SELECT DISTINCT ON (
               j.payload ->> 'evaluation_result_id', j.payload ->> 'baseline_evaluation_result_id'
           )
           j.id::text AS job_id,
           j.payload ->> 'evaluation_result_id' AS evaluation_result_id,
           j.payload ->> 'baseline_evaluation_result_id' AS baseline_evaluation_result_id,
           j.finished_at AS finished_at,
           e.fields -> 'result' AS result
    FROM job j
    JOIN job_event e ON e.job_id = j.id AND e.event = 'job.succeeded'
    WHERE j.operation = 'compare_paired_panels'
      AND j.status = 'SUCCEEDED'
      AND j.payload ->> 'evaluation_result_id' = ANY(:results)
      AND j.payload ->> 'baseline_evaluation_result_id' = ANY(:results)
      AND COALESCE(j.payload -> 'restrict_to_stratum', 'null'::jsonb) = 'null'::jsonb
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

#: What the page shows of the comparison as a whole.
COMPARISON_FIELDS: tuple[str, ...] = (
    "method_axis",
    "operating_point",
    "weighting",
    "n_resamples",
    "seed",
    "confidence",
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
        **{field: result.get(field) for field in COMPARISON_FIELDS},
        "panels": {
            key: {field: panel.get(field) for field in PANEL_FIELDS}
            for key, panel in panels.items()
        },
    }


def list_paired_comparisons_data(session: Session, eval_id: uuid.UUID) -> list[dict[str, Any]]:
    """The latest whole-panel comparison of each pair of the window's results.

    Raises :class:`EntityNotFoundError` when the window does not resolve.
    """
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
    rows = session.execute(_COMPARISONS, {"results": results}).mappings()
    return [comparison_to_dict(row) for row in rows]
