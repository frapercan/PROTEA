"""What the bank explicitly denies about a query, and the filter that honours it.

A NOT annotation is a curator saying, with evidence, that this protein does not
do this. It is usually written BECAUSE homology suggests otherwise, which makes
it the one record in the corpus aimed squarely at the error a homology-transfer
method makes.

Kept apart from the writers deliberately. The policy is one idea (which pairs
are denied, and how a denial descends the ontology) and it has to be applied
identically by three writers that otherwise share no code path.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

if TYPE_CHECKING:  # importing it for real would close a cycle: the writers
    # import this module, and the context is declared beside them.
    from protea.core.operations._run_cafa_artifacts import WritePredictionsContext

_LOG = logging.getLogger(__name__)


#: Denials propagate DOWN the DAG and only under the relations that carry a GO
#: annotation at all. A regulates edge does not transmit a denial.
_DENIAL_RELATIONS = ("is_a", "part_of")

_DENIED_PAIRS = text(
    """
    WITH RECURSIVE edge AS (
        SELECT pt.go_id AS parent, c.go_id AS child
          FROM go_term_relationship r
          JOIN go_term c  ON c.id = r.child_go_term_id
          JOIN go_term pt ON pt.id = r.parent_go_term_id
         WHERE r.ontology_snapshot_id = :snap
           AND r.relation_type = ANY(:rels)
    ),
    seed AS (
        SELECT DISTINCT a.protein_accession AS p, t.go_id AS g
          FROM protein_go_annotation a
          JOIN go_term t ON t.id = a.go_term_id
         WHERE a.annotation_set_id = :bank
           AND a.qualifier ILIKE '%NOT%'
           AND a.protein_accession = ANY(:proteins)
    ),
    denied(p, g, depth) AS (
            SELECT p, g, 0 FROM seed
        UNION
            SELECT d.p, e.child, d.depth + 1
              FROM denied d JOIN edge e ON e.parent = d.g
             WHERE d.depth < 12
    )
    SELECT DISTINCT p, g FROM denied
    """
)


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
        text(
            "SELECT annotation_set_id, ontology_snapshot_id "
            "FROM prediction_set WHERE id = :i"
        ),
        {"i": ctx.pred_set_id},
    ).first()
    if row is None:
        return None, None
    return (ctx.bank_annotation_set_id or row[0], ctx.denial_snapshot_id or row[1])


def denied_pairs(session: Session, ctx: WritePredictionsContext) -> set[tuple[str, str]]:
    """Everything the bank explicitly says this query does NOT do.

    WHY THIS EXISTS. A NOT annotation is a curator saying, with evidence, that
    this protein does not do this. It is usually written BECAUSE homology
    suggests otherwise, which makes it the one record aimed squarely at the
    error this method makes.

    Measured on the 2026-08-30 campaign before this existed: of 956 direct NOT
    annotations on query proteins, 478 were predicted anyway, 298 of those
    against experimental evidence, at a median k_position of 4, and 289 of them
    inside the query's own top five. Propagated down the DAG the denied set is
    39,134 pairs and 1,190 of them were predicted.

    The example worth remembering is O94526, fission yeast PTEN. A curator
    determined by direct assay that it does NOT have
    phosphatidylinositol-4,5-bisphosphate 5-phosphatase activity. The run
    predicted exactly that, in first place, from a homologue at distance 0.015.
    The annotation exists because the homology is misleading there, and the run
    walked into the trap the annotation documents.

    NO LEAKAGE. The denials come from the BANK's annotation set, the corpus as
    of t0, which is available at prediction time. Never from the evaluation
    truth: t0 carries 5,603 NOT annotations and t1 carries 5,397, and reading
    the second would be reading the answer.

    Returns an empty set only when the prediction set records no bank or no
    ontology, which no set produced by this pipeline does.
    """
    bank, snap = _denial_sources(session, ctx)
    if bank is None or snap is None:
        return set()
    rows = session.execute(
        _DENIED_PAIRS,
        {
            "snap": snap,
            "rels": list(_DENIAL_RELATIONS),
            "bank": bank,
            "proteins": sorted(ctx.delta_proteins),
        },
    ).all()
    return {(r[0], r[1]) for r in rows}


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
    if df.empty or not denied:
        return df, 0
    keys = list(zip(df["protein_accession"], df["go_id"], strict=True))
    keep = [k not in denied for k in keys]
    removed = len(keep) - sum(keep)
    _report_dropped(removed)
    return (df[keep] if removed else df), removed


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
    denied = denied_pairs(session, ctx)
    if not denied or not records:
        return records
    kept = [r for r in records if (r["protein_accession"], r["go_id"]) not in denied]
    _report_dropped(len(records) - len(kept))
    return kept


def _report_dropped(n: int) -> None:
    if n:
        _LOG.warning(
            "dropped %d candidate(s) the bank explicitly denies about the query; "
            "a NOT annotation is curated evidence against exactly this inference",
            n,
        )
