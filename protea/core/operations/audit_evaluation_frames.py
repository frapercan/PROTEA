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

Nothing in the platform derives ``frame`` from a run. It is a payload field:
``run_cafa_evaluation`` stamps onto the row whatever its caller passed, and
``batch_rescore_evaluation`` forwards that same value unchanged to every config
in the batch. So a row without a frame is missing a *declaration*, not a
computation, and re-running it produces another unframed row unless whoever
dispatches it declares one. The census says so in those words, because a verdict
that promises recomputation will supply the frame describes work that does not
exist.

This operation changes nothing. It answers the questions that decide whether
making the frame mandatory is an afternoon of recomputation or a week of it, and
it exists as an operation rather than a script because a procedure outside the
platform is a capability that dies with the disk. It will be run again after the
migration and again after any deletion, and its output is the before-and-after
that makes those safe.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import contract_payload

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
#: only be deleted, never re-run.
#:
#: The second column is counted here, in the same statement, rather than derived
#: afterwards from two totals. ``recomputable`` and ``with_frame`` describe
#: different populations: the first is a count over the join, the second a count
#: over the whole table, and a row can be reachable and already framed, or
#: neither. Subtracting one from the other therefore answers no question about
#: any row -- it yields the smallest number of unframed reachable rows the two
#: totals permit, which equals the truth only when the populations happen to
#: coincide. Asking the database for the conjunction costs the same join and is
#: right for every mixture.
_REACHABLE = (
    "SELECT count(*) AS recomputable,"
    "       count(*) FILTER (WHERE r.frame IS NULL) AS needs_frame_declaration"
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


def _read_reachable(session: Session) -> tuple[int, int]:
    """Return (rows whose parents survive, of those, rows carrying no frame).

    Both come out of one statement so the two numbers cannot end up describing
    different populations, which is the whole reason the second one is not
    computed from the totals.
    """
    row = session.execute(text(_REACHABLE)).mappings().one()
    return int(row["recomputable"]), int(row["needs_frame_declaration"])


def _read_combinations(session: Session, limit: int) -> tuple[list[dict[str, Any]], int]:
    """Return at most ``limit`` combinations, and how many were dropped.

    The full list goes into a ``JobEvent`` as JSONB, so it is unbounded input
    to a row. The count is returned alongside rather than logged and forgotten:
    a truncated census that does not say it was truncated reads exactly like a
    complete one, and this operation exists to be trusted as a before and after.
    """
    rows = [
        {
            "frame": row["frame"],
            "temporal_window": row["temporal_window"],
            "leakage_role": row["leakage_role"],
            "has_arms": bool(row["has_arms"]),
            "n": int(row["n"]),
        }
        for row in session.execute(text(_COMBINATIONS)).mappings().all()
    ]
    return rows[:limit], max(0, len(rows) - limit)


PositiveInt = Annotated[int, Field(gt=0)]


class AuditEvaluationFramesPayload(ProteaPayload, frozen=True):
    """Inputs for the census.

    The census needs nothing to run, which is why it originally took a bare
    dict. That is also why it accepted any dict silently, and an operation with
    no contract cannot be told from one whose contract is never checked. The
    one knob it does need is a cap, because the combination list is written into
    a JobEvent as JSONB and nothing bounded it.
    """

    max_combinations: PositiveInt = 200


def _emit_verdict(emit: EmitFn, needs_declaration: int, deletable_only: int) -> int:
    """Say what the work is, and return the count of what cannot be redone.

    Split out of :meth:`AuditEvaluationFramesOperation.execute` only because the
    method outgrew the smell budget; the reasoning is the point and belongs
    beside the sentence it produces.

    The verdict names a missing DECLARATION, not a missing computation. Nothing
    in this platform computes ``frame``: it is a payload field that
    ``run_cafa_evaluation`` and ``batch_rescore_evaluation`` stamp from whatever
    the dispatcher declared. So these rows need someone to decide which frame
    they belong to before re-running them means anything. The wording this
    replaced promised that a recomputation would supply the frame, which sends a
    reader looking for a step in the pipeline that has never existed.
    """
    emit(
        "audit.verdict",
        f"{needs_declaration} rows carry no frame and can still be re-run with one "
        f"declared; {deletable_only} can only be deleted",
        {
            "needs_frame_declaration": needs_declaration,
            "deletable_only": deletable_only,
        },
        "info",
    )
    return deletable_only


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
        p = AuditEvaluationFramesPayload.model_validate(contract_payload(payload))
        emit("audit.start", "Counting the evaluation layer", {}, "info")

        totals = _read_totals(session)
        recomputable, needs_declaration = _read_reachable(session)
        combinations, dropped = _read_combinations(session, p.max_combinations)
        if dropped:
            emit(
                "audit.combinations_truncated",
                f"{dropped} combinations beyond the cap of {p.max_combinations} are not reported",
                {"dropped": dropped, "max_combinations": p.max_combinations},
                "warning",
            )

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

        deletable_only = _emit_verdict(emit, needs_declaration, n_rows - recomputable)

        return OperationResult(
            result={
                "rows": n_rows,
                "with_frame": with_frame,
                "recomputable": recomputable,
                "combinations": combinations,
                "needs_frame_declaration": needs_declaration,
                "deletable_only": deletable_only,
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return "read-only census of evaluation_result"
