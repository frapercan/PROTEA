# protea/core/operations/audit_evaluation_frames.py
"""Count what the evaluation layer actually holds, before anything rewrites it.

A number is meaningless without its frame. The registry says so in its own
words: the same reranker reads 0.3433 in one row and 0.117 in another and both
are correct, because the frame differs in ontology, IA source, propagation,
normalisation, term cap, threshold step, and above all whether known annotations
were excluded from the prior-knowledge cell.

``evaluation_result`` records a two-value ``frame`` label, ``lafa`` or
``internal``. That marks which harness, not which parameters, so two rows both
labelled ``lafa`` can still be incomparable. All four provenance markers are
nullable with no default, so the guarantee is opt-in, which means it is not a
guarantee.

This operation changes nothing. It answers the questions that decide whether
making the frame mandatory is an afternoon of recomputation or a week of it, and
it exists as an operation rather than a script because a procedure outside the
platform is a capability that dies with the disk. It will be run again after the
migration and again after any deletion, and its output is the before-and-after
that makes those safe.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult

_TOTALS = (
    "SELECT count(*) AS n_rows,"
    "       count(frame) AS with_frame,"
    "       count(temporal_window) AS with_window,"
    "       count(leakage_role) AS with_role,"
    "       count(arms_enabled) AS with_arms,"
    "       count(*) FILTER (WHERE job_id IS NULL) AS without_job"
    "  FROM evaluation_result"
)

#: Recomputable means both parents are still reachable. A row whose prediction
#: set or evaluation set is gone can be described but not reproduced, so it can
#: only be deleted, never re-framed.
_REACHABLE = (
    "SELECT count(*) AS recomputable"
    "  FROM evaluation_result r"
    "  JOIN prediction_set p ON p.id = r.prediction_set_id"
    "  JOIN evaluation_set e ON e.id = r.evaluation_set_id"
)

#: How many distinct frames exist in fact, as opposed to how many we imagine.
#: Grouped on all four markers because any one of them can make two rows
#: incomparable.
_COMBINATIONS = (
    "SELECT frame, temporal_window, leakage_role,"
    "       (arms_enabled IS NOT NULL) AS has_arms,"
    "       count(*) AS n"
    "  FROM evaluation_result"
    " GROUP BY 1, 2, 3, 4"
    " ORDER BY n DESC"
)


def _read_totals(session: Session) -> dict[str, int]:
    row = session.execute(text(_TOTALS)).mappings().one()
    return {key: int(row[key]) for key in row.keys()}


def _read_recomputable(session: Session) -> int:
    return int(session.execute(text(_REACHABLE)).mappings().one()["recomputable"])


def _read_combinations(session: Session) -> list[dict[str, Any]]:
    return [
        {
            "frame": row["frame"],
            "temporal_window": row["temporal_window"],
            "leakage_role": row["leakage_role"],
            "has_arms": bool(row["has_arms"]),
            "n": int(row["n"]),
        }
        for row in session.execute(text(_COMBINATIONS)).mappings().all()
    ]


class AuditEvaluationFramesOperation(Operation):
    name = "audit_evaluation_frames"
    description = (
        "Read-only census of evaluation_result: how many rows, how many carry a "
        "frame, how many are recomputable, and which provenance combinations "
        "actually occur. Writes nothing."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        emit("audit.start", "Counting the evaluation layer", {}, "info")

        totals = _read_totals(session)
        recomputable = _read_recomputable(session)
        combinations = _read_combinations(session)

        n_rows = totals["n_rows"]
        with_frame = totals["with_frame"]

        emit(
            "audit.totals",
            f"{n_rows} results, {with_frame} carry a frame, {recomputable} recomputable",
            {**totals, "recomputable": recomputable, "unreachable_parents": n_rows - recomputable},
            "info",
        )
        emit(
            "audit.combinations",
            f"{len(combinations)} distinct provenance combinations",
            {"combinations": combinations},
            "info",
        )

        # The number that decides the shape of the work, stated once and plainly
        # so nobody has to derive it from the rest. A row can carry a frame label
        # and still have lost its parents, so this is clamped rather than
        # subtracted blind.
        re_framable = max(0, recomputable - with_frame)
        deletable_only = n_rows - recomputable
        emit(
            "audit.verdict",
            f"{re_framable} rows can be re-framed by recomputing; {deletable_only} can only be deleted",
            {"re_framable_by_recompute": re_framable, "deletable_only": deletable_only},
            "info",
        )

        return OperationResult(
            result={
                "rows": n_rows,
                "with_frame": with_frame,
                "recomputable": recomputable,
                "combinations": combinations,
                "re_framable_by_recompute": re_framable,
                "deletable_only": deletable_only,
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return "read-only census of evaluation_result"
