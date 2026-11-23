"""Mark windows as superseded by a successor, deleting nothing.

WHY NOT RETIRE THEM. ``retire_evaluation_sets`` archives a window's truth and
deletes the row, and it refuses a window with a role or with anything pointing
at it. The windows a frame leaves behind have both: a role in the frame they
belonged to, and evaluations computed on them that stay records of what was
measured. So they are kept, and marked: which window supersedes each one, when,
and why. A reader of any of their results can follow the pointer to the window
that replaced it.

WHAT A SUCCESSOR IS. A window over the same two corpora, the same two annotation
sets, that is not superseded itself. The operation refuses a successor over other
corpora, a chain (a successor already superseded: mark the newest window), a
window already superseded, the successor named among the windows, and an empty
note: the mark says why, or it is not made.

Every refusal is listed, not only the first, and ``dry_run`` does every check and
emits what it would mark, touching nothing.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


class SupersedeEvaluationSetsPayload(ProteaPayload, frozen=True):
    """Which windows one successor supersedes, and why. Named one by one."""

    successor_id: str
    evaluation_set_ids: list[str]
    #: Why the successor replaces them: the decision, with its date.
    note: str
    #: Do every check and emit what would be marked, touching nothing.
    dry_run: bool = False

    @field_validator("successor_id")
    @classmethod
    def _an_id(cls, value: str) -> str:
        return str(uuid.UUID(value))

    @field_validator("evaluation_set_ids")
    @classmethod
    def _named_and_distinct(cls, value: list[str]) -> list[str]:
        """Refuse an empty list and repeats: the scope is what the caller read."""
        if not value:
            raise ValueError(
                "evaluation_set_ids cannot be empty: this operation marks the windows it "
                "is given and takes no predicate."
            )
        ids = [str(uuid.UUID(raw)) for raw in value]
        repeated = sorted({raw for raw in ids if ids.count(raw) > 1})
        if repeated:
            raise ValueError(f"evaluation_set_ids repeats {repeated}")
        return ids

    @field_validator("note")
    @classmethod
    def _says_why(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("note cannot be empty: the mark says why, or it is not made")
        return value.strip()


def refusals(
    successor_id: uuid.UUID,
    successor: EvaluationSet | None,
    windows: dict[uuid.UUID, EvaluationSet | None],
) -> list[str]:
    """Every reason not to mark these windows, in one list; empty means go."""
    found: list[str] = []
    if successor is None:
        found.append(f"successor {successor_id} does not exist")
    elif successor.superseded_by_id is not None:
        found.append(
            f"successor {successor_id} is itself superseded by {successor.superseded_by_id}; "
            "mark the newest window, never a chain"
        )
    for window_id, window in windows.items():
        if window is None:
            found.append(f"window {window_id} does not exist")
            continue
        if window_id == successor_id:
            found.append(f"window {window_id} is the successor itself")
        if window.superseded_by_id is not None:
            found.append(f"window {window_id} is already superseded by {window.superseded_by_id}")
        if successor is not None and _corpora(window) != _corpora(successor):
            found.append(f"window {window_id} spans other corpora than its successor")
    return found


def _corpora(window: EvaluationSet) -> tuple[uuid.UUID, uuid.UUID]:
    return (window.old_annotation_set_id, window.new_annotation_set_id)


class SupersedeEvaluationSetsOperation(Operation):
    name = "supersede_evaluation_sets"
    description = (
        "Mark named EvaluationSet rows as superseded by a successor over the same two "
        "corpora, with the date and a note, deleting nothing. Refuses a missing or "
        "superseded successor, a window over other corpora, one already superseded, the "
        "successor itself and an empty note, listing every refusal; dry_run does every "
        "check and touches nothing."
    )

    def execute(
        self,
        session: Session,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        p = SupersedeEvaluationSetsPayload.model_validate(contract_payload(payload))
        successor_id = uuid.UUID(p.successor_id)
        successor = session.get(EvaluationSet, successor_id)
        windows = {
            uuid.UUID(raw): session.get(EvaluationSet, uuid.UUID(raw))
            for raw in p.evaluation_set_ids
        }
        refused = refusals(successor_id, successor, windows)
        if refused:
            emit("supersede_evaluation_sets.refused", None, {"refusals": refused}, "error")
            raise ValueError("; ".join(refused))

        marked_at = datetime.now(UTC)
        for window_id, window in windows.items():
            emit(
                "supersede_evaluation_sets.mark",
                None,
                {
                    "evaluation_set_id": str(window_id),
                    "successor_id": str(successor_id),
                    "dry_run": p.dry_run,
                },
                "info",
            )
            if not p.dry_run and window is not None:
                window.superseded_by_id = successor_id
                window.superseded_at = marked_at
                window.supersession_note = p.note
        if not p.dry_run:
            session.flush()
        result = {
            "successor_id": str(successor_id),
            "superseded": [str(window_id) for window_id in windows],
            "note": p.note,
            "dry_run": p.dry_run,
        }
        emit("supersede_evaluation_sets.done", None, result, "info")
        return OperationResult(result=result)
