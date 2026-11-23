"""Count every prediction set not yet counted, so its pages read the counts.

A set's summary (its rows, proteins, totals per aspect, most frequent terms, and
each protein's terms) is counted on the set's first read and kept
(``protea.services._prediction_set_summary``): a set does not change after it is
written. This counts ahead of the reader, so no one waits for the first read of
a set that already exists. One set per transaction, so a set of millions of
rows commits on its own and a failure keeps every set counted before it.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import contract_payload
from protea.services._prediction_set_summary import summarize


class SummarizePredictionSetsPayload(ProteaPayload, frozen=True):
    """The sets to count; none named means every set that has no summary."""

    prediction_set_ids: list[str] | None = None

    @field_validator("prediction_set_ids")
    @classmethod
    def _each_is_a_set_id(cls, value: list[str] | None) -> list[str] | None:
        for set_id in value or []:
            uuid.UUID(set_id)
        return value


class SummarizePredictionSetsOperation(Operation):
    """Count the sets not yet counted and keep their summaries."""

    name = "summarize_prediction_sets"
    description = (
        "Count the rows, proteins and terms of every prediction set not yet counted, "
        "and keep them for the set's pages."
    )
    payload_model = SummarizePredictionSetsPayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        ids = payload.get("prediction_set_ids")
        return "every set without a summary" if not ids else f"{len(ids)} named sets"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = SummarizePredictionSetsPayload.model_validate(contract_payload(payload))
        ids = p.prediction_set_ids or [
            str(r[0])
            for r in session.execute(
                text("SELECT id FROM prediction_set WHERE summary IS NULL ORDER BY created_at")
            )
        ]
        emit("summarize_prediction_sets.start", None, {"sets": len(ids)}, "info")
        rows = 0
        writing: list[str] = []
        for set_id in ids:
            started = time.monotonic()
            summary = summarize(session, uuid.UUID(set_id))
            session.commit()
            if summary.get("provisional"):
                # Its job is still writing it: counted, not kept; the next sweep keeps it.
                writing.append(set_id)
                continue
            rows += summary["rows"]
            emit(
                "summarize_prediction_sets.set",
                None,
                {
                    "prediction_set_id": set_id,
                    "rows": summary["rows"],
                    "proteins": summary["proteins"],
                    "seconds": round(time.monotonic() - started, 1),
                },
                "info",
            )
        if writing:
            emit(
                "summarize_prediction_sets.still_writing",
                f"{len(writing)} sets are still being written and were not kept",
                {"prediction_set_ids": writing},
                "info",
            )
        return OperationResult(
            result={"sets": len(ids) - len(writing), "still_writing": len(writing), "rows": rows}
        )
