"""Retire a window of a superseded frame, leaving a job behind.

WHY AN OPERATION AND NOT A SCRIPT, which is the whole point of this file. A
raw ``DELETE`` against the database is how 124 campaign rows disappear with
nothing recording that they did, who asked, or what they were. This project's
rule is that every operation goes through ``POST /v1/jobs``, and the reason
given for it is that a direct ``execute()`` writes rows and leaves no job. A
DELETE is the case where that matters most: afterwards there is no row left to
ask.

WHAT IT REFUSES, structurally rather than by care:

* a window carrying a ``window_role``. That is the holdout and the two
  declared bands, and ``cb46395a`` has ZERO results in PROTEA because its
  single declared look was spent in the laboratory, so a "no results" filter
  would have swept it up. The role is the protection, not the result count;
* a window any ``evaluation_result``, ``exploration``,
  ``reference_reproduction`` or ``reranker_model`` points at. Those are
  evidence, and evidence is not retired;
* a window of the LIVE frame. This operation exists for the superseded one,
  and naming a campaign window here is far more likely to be a mistake than
  an intention.

Each refusal names the window and the reason, and the run stops: a partial
retirement of a named list leaves nobody able to say which half went.

THE ARTEFACT GOES WITH THE ROW. A parquet whose row is gone is a file nobody
can attribute, and the store has no other index. It is deleted after the row,
so a failure leaves an orphan file rather than a row pointing at nothing.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: The frame this operation is for. Anything else is refused.
SUPERSEDED = ("as_closed", "pivot_terms", "any")

#: Everything that can point at a window. Each one means evidence.
REFERRERS = (
    "evaluation_result",
    "exploration",
    "reference_reproduction",
    "reranker_model",
)


class RetireEvaluationSetsPayload(ProteaPayload, frozen=True):
    """Which windows to retire. Named one by one, never a predicate."""

    evaluation_set_ids: list[str]

    @field_validator("evaluation_set_ids")
    @classmethod
    def _at_least_one(cls, value: list[str]) -> list[str]:
        """Refuse an empty list rather than read it as "everything matching".

        A delete whose scope is a predicate is a delete whose scope nobody
        read. The caller derives the list, looks at it, and passes it.
        """
        if not value:
            raise ValueError(
                "evaluation_set_ids cannot be empty. This operation retires the "
                "windows it is given and takes no predicate: a delete whose scope is "
                "a query is a delete nobody read before it ran."
            )
        return value


def _refusal(session: Session, eset: EvaluationSet) -> str | None:
    """Why this window must not be retired, or ``None``."""
    if eset.window_role is not None:
        return (
            f"it carries window_role={eset.window_role!r}, so it is the holdout or a "
            "declared band. The role is the protection and the result count is not: "
            "the holdout has zero results here because its look was spent in the "
            "laboratory"
        )
    rules = (eset.truth_profile, eset.toi_rule, eset.not_codes)
    if rules != SUPERSEDED:
        return (
            f"its rules are {rules}, not the superseded frame {SUPERSEDED}. This "
            "operation retires the old frame; naming a live window here is far more "
            "likely to be a mistake than an intention"
        )
    for table in REFERRERS:
        n = session.execute(
            text(f"SELECT count(*) FROM {table} WHERE evaluation_set_id = :v"),
            {"v": eset.id},
        ).scalar_one()
        if n:
            return f"{n} row(s) in {table} point at it, and evidence is not retired"
    return None


class RetireEvaluationSetsOperation(Operation):
    name = "retire_evaluation_sets"
    description = (
        "Delete named EvaluationSet rows of the superseded as_closed/pivot_terms/any "
        "frame together with their ground-truth parquets, leaving a job behind. "
        "Refuses any window that carries a window_role, belongs to the live frame, or "
        "is pointed at by a result, an exploration, a reproduction or a model."
    )

    def execute(
        self,
        session: Session,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        from protea.core.evaluation import groundtruth_key_for

        p = RetireEvaluationSetsPayload.model_validate(contract_payload(payload))
        store = get_artifact_store(load_settings(_project_root()))

        objetivos: list[tuple[uuid.UUID, str]] = []
        for raw in p.evaluation_set_ids:
            eset = session.get(EvaluationSet, uuid.UUID(raw))
            if eset is None:
                raise ValueError(f"EvaluationSet {raw} does not exist")
            motivo = _refusal(session, eset)
            if motivo is not None:
                raise ValueError(f"refusing to retire {raw}: {motivo}")
            objetivos.append((eset.id, groundtruth_key_for(eset.id)))

        # Checked them ALL before deleting ANY: a partial retirement of a
        # named list leaves nobody able to say which half went.
        emit(
            "retire_evaluation_sets.checked",
            None,
            {"asked": len(objetivos)},
            "info",
        )

        huerfanos = 0
        for vid, key in objetivos:
            session.execute(
                text("DELETE FROM evaluation_set WHERE id = :v"), {"v": vid}
            )
            try:
                store.delete(key)
            except Exception:  # noqa: BLE001 -- an orphan file is the safe failure
                huerfanos += 1
            emit(
                "retire_evaluation_sets.retired",
                None,
                {"evaluation_set_id": str(vid), "key": key},
                "info",
            )

        result = {
            "retired": len(objetivos),
            "orphaned_artifacts": huerfanos,
            "asked": len(p.evaluation_set_ids),
        }
        emit("retire_evaluation_sets.done", None, result, "warning")
        return OperationResult(result=result)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        n = len(payload.get("evaluation_set_ids") or [])
        return f"retire {n} evaluation set(s) of the superseded frame"


def _project_root():
    from pathlib import Path

    return Path(__file__).resolve().parents[3]
