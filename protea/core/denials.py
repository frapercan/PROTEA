"""What the bank explicitly denies about a protein, and the filter that honours it.

A NOT annotation is a curator saying, with evidence, that this protein does not
do this. It is usually written BECAUSE homology suggests otherwise, which makes
it the one record in the corpus aimed squarely at the error a transfer method
makes.

WHO APPLIES IT. The method that predicts, when it predicts. The evaluator scores
what a method stored and nothing else (the closure's question 6, decided on
2026-11-20), so a method that honours denials does it before it writes and says
so in its record: :data:`POLICY` is what it records. PROTEA's own transfer
methods, the k-nearest-neighbour transfer and the InterPro mirror, honour them
always, as the evaluator used to do for every method.

THE POLICY. The denials of a protein are the NOT rows of the bank, the
annotation set the prediction transfers from, whatever their evidence code; and
a denial descends the ontology, because "does not have the parent" implies
"does not have any descendant", through ``is_a`` and ``part_of`` only, under
the ontology the prediction set was built under.

NO LEAKAGE. The bank is the corpus as of the prediction's start, available when
predicting. The denials are never read from the evaluation truth.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

_LOG = logging.getLogger(__name__)

#: Denials propagate DOWN the DAG and only under the relations that carry a GO
#: annotation at all. A regulates edge does not transmit a denial.
DENIAL_RELATIONS: tuple[str, ...] = ("is_a", "part_of")

#: How far down a denial is followed. Twelve levels reach every descendant on
#: the ontologies of this project; the bound keeps a cycle from running away.
MAX_DEPTH = 12

#: What a method that honours denials records about it, so that its record
#: says which rule removed which candidates.
POLICY: dict[str, Any] = {
    "source": "the NOT rows of the prediction set's annotation set, any evidence code",
    "propagation": "down is_a and part_of of the prediction set's ontology",
    "max_depth": MAX_DEPTH,
}

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
             WHERE d.depth < :max_depth
    )
    SELECT DISTINCT p, g FROM denied
    """
)


def denied_pairs(
    session: Session,
    bank_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    proteins: Iterable[str],
) -> set[tuple[str, str]]:
    """Every (protein, term) the bank denies for these proteins, propagated down.

    WHY THIS EXISTS. Measured on the 2026-08-30 campaign, before any writer
    honoured denials: of 956 NOT rows on query proteins, which are 936 distinct
    (protein, term) denials, 478 rows and 465 distinct denials were predicted
    anyway, at a median k_position of 4 and with 289 inside the query's own top
    five. 298 of the violated rows carry one of the thirteen codes
    ``evidence_codes.EXPERIMENTAL`` calls experimental; under the eleven
    strictly experimental and high-throughput codes the figure is 286, and under
    the six strict experimental ones 285. Propagated down the DAG the denied set
    was 39,105 pairs and 1,190 of them were predicted, on the prediction set's
    own ontology; the same measurement on the evaluation pivot gave 39,134, and
    that gap is why the ontology is the prediction set's and not a pivot.

    The example worth remembering is O94526, fission yeast PTEN. A curator
    determined by direct assay that it does NOT have
    phosphatidylinositol-4,5-bisphosphate 5-phosphatase activity. The run
    predicted exactly that, in first place, from a homologue at distance 0.015:
    the annotation exists because the homology is misleading there.

    NO LEAKAGE. The bank is the corpus as of the prediction's start. Never the
    evaluation truth: on that campaign t0 carried 5,603 NOT annotations and t1
    5,397, and reading the second would be reading the answer.
    """
    accessions = sorted(set(proteins))
    if not accessions:
        return set()
    rows = session.execute(
        _DENIED_PAIRS,
        {
            "snap": ontology_snapshot_id,
            "rels": list(DENIAL_RELATIONS),
            "bank": bank_annotation_set_id,
            "proteins": accessions,
            "max_depth": MAX_DEPTH,
        },
    ).all()
    return {(row[0], row[1]) for row in rows}


def drop_denied(
    rows: list[dict[str, Any]],
    denied: set[tuple[str, str]],
    go_id_of: Callable[[dict[str, Any]], str | None],
) -> tuple[list[dict[str, Any]], int]:
    """The rows the bank does not deny, and how many it did.

    ``go_id_of`` reads a row's term: some writers carry the GO id, others the
    id of its ``go_term`` row. A row whose term cannot be read is kept, because
    nothing denies it.
    """
    if not rows or not denied:
        return rows, 0
    kept = [row for row in rows if (row["protein_accession"], go_id_of(row)) not in denied]
    removed = len(rows) - len(kept)
    if removed:
        _LOG.warning(
            "dropped %d candidate(s) the bank explicitly denies about the query; "
            "a NOT annotation is curated evidence against exactly this inference",
            removed,
        )
    return kept, removed


def honour_denials(
    session: Session,
    bank_annotation_set_id: uuid.UUID,
    ontology_snapshot_id: uuid.UUID,
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """A prediction writer's step: drop the rows the bank denies, and say how many.

    The rows carry ``protein_accession`` and either ``go_id`` or the integer
    ``go_term_id`` of the prediction set's ontology, which is read back to a GO
    id here in one query over the ids present.
    """
    if not rows:
        return rows, 0
    denied = denied_pairs(
        session,
        bank_annotation_set_id,
        ontology_snapshot_id,
        (row["protein_accession"] for row in rows),
    )
    if not denied:
        return rows, 0
    term_ids = sorted(
        {
            int(row["go_term_id"])
            for row in rows
            if not row.get("go_id") and row.get("go_term_id") is not None
        }
    )
    go_id_by_term: dict[int, str] = {}
    if term_ids:
        go_id_by_term = {
            term_id: go_id
            for term_id, go_id in session.execute(
                text("SELECT id, go_id FROM go_term WHERE id = ANY(:ids)"), {"ids": term_ids}
            )
        }

    def go_id_of(row: dict[str, Any]) -> str | None:
        if row.get("go_id"):
            return str(row["go_id"])
        term_id = row.get("go_term_id")
        return go_id_by_term.get(int(term_id)) if term_id is not None else None

    return drop_denied(rows, denied, go_id_of)


__all__ = [
    "DENIAL_RELATIONS",
    "MAX_DEPTH",
    "POLICY",
    "denied_pairs",
    "drop_denied",
    "honour_denials",
]
