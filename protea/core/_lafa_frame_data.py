"""What LAFA's frame reads from the store: one end's ontology and one end's rows.

Split from :mod:`protea.core.lafa_frame` so that every step there is a function
of these two reads, which its tests build by hand.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evidence_codes import ECO_TO_CODE, EXPERIMENTAL

#: The thirteen experimental codes and the ECO ids that stand for them, which
#: is how every other truth in the platform reads "experimental".
_EXPERIMENTAL = sorted(
    EXPERIMENTAL | {eco for eco, code in ECO_TO_CODE.items() if code in EXPERIMENTAL}
)


@dataclass(frozen=True)
class EndOntology:
    """One end's ontology, read the way LAFA reads that end's go-basic.

    ``live`` maps every non-obsolete id to its aspect (``P``, ``F`` or ``C``).
    ``parents`` holds the ``is_a`` and ``part_of`` edges between two live terms
    of one aspect, the only edges LAFA propagates through. ``alt_ids`` and
    ``replaced_by`` are the two ways the ontology names an id it no longer uses
    as a primary one.
    """

    live: Mapping[str, str]
    parents: Mapping[str, frozenset[str]]
    alt_ids: Mapping[str, str]
    replaced_by: Mapping[str, str]


def load_end_ontology(session: Session, snapshot_id: uuid.UUID) -> EndOntology:
    """One ontology snapshot as LAFA reads a go-basic: live terms, two edge types, two renamings.

    An edge enters only between two live terms of one aspect; go-basic has no
    other kind, and the test keeps it that way if a snapshot ever does.
    Refused when the snapshot has no live term or no alias rows: without
    ``load_ontology_aliases`` step 2 would drop every renamed id in silence.
    """
    live = dict(
        session.execute(
            text(
                "SELECT go_id, aspect FROM go_term WHERE ontology_snapshot_id = :s AND NOT is_obsolete"
            ),
            {"s": snapshot_id},
        ).all()
    )
    if not live:
        raise ValueError(f"ontology snapshot {snapshot_id} has no live term")
    parents: dict[str, set[str]] = defaultdict(set)
    edges = session.execute(
        text(
            """
            SELECT child.go_id, parent.go_id
            FROM go_term_relationship rel
            JOIN go_term child ON child.id = rel.child_go_term_id
            JOIN go_term parent ON parent.id = rel.parent_go_term_id
            WHERE rel.ontology_snapshot_id = :s AND rel.relation_type IN ('is_a', 'part_of')
            """
        ),
        {"s": snapshot_id},
    ).all()
    for child, parent in edges:
        if live.get(child) is not None and live.get(child) == live.get(parent):
            parents[child].add(parent)
    renamings: dict[str, dict[str, str]] = {"alt_id": {}, "replaced_by": {}}
    aliases = session.execute(
        text(
            "SELECT kind, alias_go_id, primary_go_id FROM go_term_alias "
            "WHERE ontology_snapshot_id = :s AND kind IN ('alt_id', 'replaced_by')"
        ),
        {"s": snapshot_id},
    ).all()
    for kind, alias, primary in aliases:
        renamings[kind][alias] = primary
    if not aliases:
        raise ValueError(
            f"ontology snapshot {snapshot_id} has no alias rows; run load_ontology_aliases first"
        )
    return EndOntology(
        live=live,
        parents={child: frozenset(found) for child, found in parents.items()},
        alt_ids=renamings["alt_id"],
        replaced_by=renamings["replaced_by"],
    )


def load_end_rows(
    session: Session, annotation_set_id: uuid.UUID, experimental_not_only: bool
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """One end's experimental rows without ``NOT``, by protein, and its ``NOT`` rows.

    The ``NOT`` rows are the experimental ones when ``experimental_not_only``,
    every one otherwise: that is the window's ``not_codes``.
    """
    positives: dict[str, set[str]] = defaultdict(set)
    for protein, go_id in session.execute(
        text(
            """
            SELECT pga.protein_accession, gt.go_id
            FROM protein_go_annotation pga
            JOIN go_term gt ON gt.id = pga.go_term_id
            WHERE pga.annotation_set_id = :set_id
              AND pga.evidence_code = ANY(:codes)
              AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')
            """
        ),
        {"set_id": annotation_set_id, "codes": _EXPERIMENTAL},
    ):
        positives[protein].add(go_id)
    negations: dict[str, set[str]] = defaultdict(set)
    for protein, go_id in session.execute(
        text(
            """
            SELECT pga.protein_accession, gt.go_id
            FROM protein_go_annotation pga
            JOIN go_term gt ON gt.id = pga.go_term_id
            WHERE pga.annotation_set_id = :set_id
              AND pga.qualifier LIKE '%NOT%'
              AND (NOT :experimental_only OR pga.evidence_code = ANY(:codes))
            """
        ),
        {
            "set_id": annotation_set_id,
            "codes": _EXPERIMENTAL,
            "experimental_only": experimental_not_only,
        },
    ):
        negations[protein].add(go_id)
    return dict(positives), dict(negations)
