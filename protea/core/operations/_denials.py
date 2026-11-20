"""The evaluator's application of the denial policy, until it stops applying it.

The policy lives in :mod:`protea.core.denials`, where the methods read it:
PROTEA's own transfer methods honour denials when they predict and say so in
their record. The evaluator still applies the same policy to every set it
scores, through the three writers here, so that a set written before its method
honoured denials is scored as it always was. The next step of the closure
retires this module, and the evaluator then scores what a method stored and
nothing else.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core import denials

if TYPE_CHECKING:  # importing it for real would close a cycle: the writers
    # import this module, and the context is declared beside them.
    from protea.core.operations._run_cafa_artifacts import WritePredictionsContext

_LOG = logging.getLogger(__name__)


def _denial_sources(
    session: Session, ctx: WritePredictionsContext
) -> tuple[uuid.UUID | None, uuid.UUID | None]:
    """Where the denials come from, resolved rather than passed.

    The prediction set already records the annotation set it transferred from
    and the ontology it was built under, so the check needs nothing from the
    caller. That is on purpose. The depth cut spent a campaign silently
    unapplied because it was a field two construction sites both had to
    remember to fill, and neither did. A policy that reads its own inputs off
    the row it is about cannot be forgotten into being off.

    The explicit context fields stay as an override, for tests that need to
    aim the check at a specific bank.
    """
    if ctx.bank_annotation_set_id is not None and ctx.denial_snapshot_id is not None:
        return ctx.bank_annotation_set_id, ctx.denial_snapshot_id
    row = session.execute(
        text("SELECT annotation_set_id, ontology_snapshot_id FROM prediction_set WHERE id = :i"),
        {"i": ctx.pred_set_id},
    ).first()
    if row is None:
        return None, None
    return (ctx.bank_annotation_set_id or row[0], ctx.denial_snapshot_id or row[1])


def denied_pairs(session: Session, ctx: WritePredictionsContext) -> set[tuple[str, str]]:
    """Everything the bank explicitly says the window's targets do NOT do.

    :func:`protea.core.denials.denied_pairs` on the prediction set's own bank
    and ontology, for the targets of the window being scored. Returns an empty
    set only when the prediction set records no bank or no ontology, which no
    set produced by this pipeline does.
    """
    bank, snap = _denial_sources(session, ctx)
    if bank is None or snap is None:
        return set()
    return denials.denied_pairs(session, bank, snap, ctx.delta_proteins)


def apply_denials(session: Session, ctx: WritePredictionsContext, df: Any) -> Any:
    """The base-frame entry point: resolve the denials and drop them.

    Called AFTER the depth guard, on purpose. The guard compares the frame's
    length against a fresh COUNT over the same filter, so removing rows before
    it would make it fire on its own bookkeeping.
    """
    return drop_contradictions(df, denied_pairs(session, ctx))[0]


def drop_contradictions(df: Any, denied: set[tuple[str, str]]) -> tuple[Any, int]:
    """Remove candidates the bank denies, and say how many.

    Applied AFTER the depth guard rather than inside the SELECT, deliberately.
    The guard compares the frame's length against a fresh COUNT over the same
    filter, so a filter applied in one and not the other would make the guard
    fire on its own bookkeeping. Filtering here keeps that arithmetic intact
    and makes the removal a number the run reports about itself.
    """
    return denials.drop_denied_frame(df, denied)


def drop_denied_records(
    session: Session,
    ctx: WritePredictionsContext,
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """The record-list twin of :func:`drop_contradictions`.

    The reranker paths never build the cached base frame, so they need their
    own application of the same policy. Every writer in this module goes
    through one of the two, which is the point: the depth cut was once missed
    because it had to be passed by hand at each construction site, and a
    filter that some writers apply and others do not is the same defect
    wearing a different name.
    """
    kept, _ = denials.drop_denied(records, denied_pairs(session, ctx), lambda r: r["go_id"])
    return kept
