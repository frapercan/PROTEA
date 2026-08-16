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

This operation changes nothing. It answers the four questions that decide
whether making the frame mandatory is an afternoon of recomputation or a week of
it, and it exists as an operation rather than a script because a procedure
outside the platform is a capability that dies with the disk. It will be run
again after the migration and again after any deletion, and its output is the
before-and-after that makes those safe.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult


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

        totals = (
            session.execute(
                text(
                    "SELECT count(*) AS rows,"
                    "       count(frame) AS with_frame,"
                    "       count(temporal_window) AS with_window,"
                    "       count(leakage_role) AS with_role,"
                    "       count(arms_enabled) AS with_arms"
                    "  FROM evaluation_result"
                )
            )
            .mappings()
            .one()
        )

        # Recomputable means both parents are still reachable. A row whose
        # prediction set or evaluation set is gone can be described but not
        # reproduced, so it can only be deleted, never re-framed.
        reachable = (
            session.execute(
                text(
                    "SELECT count(*) AS recomputable"
                    "  FROM evaluation_result r"
                    "  JOIN prediction_set p ON p.id = r.prediction_set_id"
                    "  JOIN evaluation_set e ON e.id = r.evaluation_set_id"
                )
            )
            .mappings()
            .one()
        )

        # How many distinct frames exist in fact, as opposed to how many we
        # imagine. Grouped on all four markers because any of them can make two
        # rows incomparable.
        combos = (
            session.execute(
                text(
                    "SELECT frame, temporal_window, leakage_role,"
                    "       (arms_enabled IS NOT NULL) AS has_arms,"
                    "       count(*) AS n"
                    "  FROM evaluation_result"
                    " GROUP BY 1, 2, 3, 4"
                    " ORDER BY n DESC"
                )
            )
            .mappings()
            .all()
        )

        # Anything that would break if a row were deleted. A result referenced
        # by a published figure is not ours to remove quietly.
        orphan_risk = (
            session.execute(
                text(
                    "SELECT count(*) AS results_without_job"
                    "  FROM evaluation_result WHERE job_id IS NULL"
                )
            )
            .mappings()
            .one()
        )

        rows = int(totals["rows"])
        with_frame = int(totals["with_frame"])
        recomputable = int(reachable["recomputable"])

        emit(
            "audit.totals",
            f"{rows} results, {with_frame} carry a frame, {recomputable} recomputable",
            {
                "rows": rows,
                "with_frame": with_frame,
                "with_temporal_window": int(totals["with_window"]),
                "with_leakage_role": int(totals["with_role"]),
                "with_arms_enabled": int(totals["with_arms"]),
                "recomputable": recomputable,
                "unreachable_parents": rows - recomputable,
                "results_without_job": int(orphan_risk["results_without_job"]),
            },
            "info",
        )

        combinations = [
            {
                "frame": c["frame"],
                "temporal_window": c["temporal_window"],
                "leakage_role": c["leakage_role"],
                "has_arms": bool(c["has_arms"]),
                "n": int(c["n"]),
            }
            for c in combos
        ]
        emit(
            "audit.combinations",
            f"{len(combinations)} distinct provenance combinations",
            {"combinations": combinations},
            "info",
        )

        # The number that decides the shape of the work, stated once and plainly
        # so nobody has to derive it from the rest.
        unframed_but_recomputable = max(0, recomputable - with_frame)
        emit(
            "audit.verdict",
            (
                f"{unframed_but_recomputable} rows can be re-framed by recomputing; "
                f"{rows - recomputable} can only be deleted"
            ),
            {
                "re_framable_by_recompute": unframed_but_recomputable,
                "deletable_only": rows - recomputable,
            },
            "info",
        )

        return OperationResult(
            result={
                "rows": rows,
                "with_frame": with_frame,
                "recomputable": recomputable,
                "combinations": combinations,
                "re_framable_by_recompute": unframed_but_recomputable,
                "deletable_only": rows - recomputable,
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return "read-only census of evaluation_result"
