"""Which proteins a window may score: its target population.

``every_protein`` is what every window built before this rule did: any protein
that gained an experimental annotation between the two ends is a target,
whatever it was at the start.

``superset_at_t0`` fixes the targets before the window opens, as CAFA fixes
its target list: the superset of the photo of the window's start (ADR-D59),
which is the Swiss-Prot list of the UniProt release paired with that cut. The
author chose it for the four windows of the frame on 2026-11-16 (ADR-D60). A
protein that entered Swiss-Prot after the start, a TrEMBL entry, a neighbour
or an accession that only the end's release uses is not a target, however
much it gained.

The rule is a filter on proteins, not on terms, and it runs after the delta is
classified. That order is safe because the classification is per protein: a
protein's NK, LK or PK bucket depends on its own annotations at the two ends
and on nothing another protein holds. Every bucket keeps the population's
proteins only, ``known`` and ``removed`` included, so the window is about one
population in everything it publishes. The proteins it leaves out are counted
by bucket in the stats, so a reader can see what the restriction cost.

The photo is found by its annotation set: a cut has one photo and the photo
names the annotation set it loaded, so the window's t0 corpus leads to exactly
one photo or to none, and none is refused.
"""

from __future__ import annotations

import uuid
from typing import NamedTuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evaluation import EvaluationData

#: ``every_protein`` first: it is the default, so no stored window moves.
TARGET_POPULATIONS: tuple[str, ...] = ("every_protein", "superset_at_t0")


class Population(NamedTuple):
    """The accessions a window may score, and the photo they come from."""

    cut_release: int
    photo_id: uuid.UUID
    accessions: frozenset[str]


def refuse_unknown_population(value: str) -> str:
    """The value itself, or a refusal naming the vocabulary."""
    if value not in TARGET_POPULATIONS:
        raise ValueError(
            f"unknown target_population {value!r}; expected one of {TARGET_POPULATIONS}"
        )
    return value


def superset_at_start(session: Session, old_annotation_set_id: uuid.UUID) -> Population:
    """The superset of the photo whose corpus is the window's start.

    Refused when no photo loaded that annotation set (a window over a corpus
    that is not a photo has no superset at t0 to restrict to), when more than
    one did, and when the photo's superset is empty.
    """
    photos = session.execute(
        text("SELECT id, cut_release FROM cut_photo WHERE annotation_set_id = :set_id"),
        {"set_id": old_annotation_set_id},
    ).all()
    if not photos:
        raise ValueError(
            f"no cut photo has annotation set {old_annotation_set_id} as its corpus, so the "
            "window's start has no superset to restrict the targets to"
        )
    if len(photos) > 1:
        raise ValueError(
            f"{len(photos)} cut photos have annotation set {old_annotation_set_id} as their "
            "corpus; a cut has one photo, so the start of the window is ambiguous"
        )
    photo_id, cut_release = photos[0]
    accessions = frozenset(
        session.execute(
            text("SELECT accession FROM cut_photo_member WHERE photo_id = :photo_id AND superset"),
            {"photo_id": photo_id},
        ).scalars()
    )
    if not accessions:
        raise ValueError(f"the photo of cut {cut_release} has an empty superset")
    return Population(int(cut_release), photo_id, accessions)


def restricted_to(
    data: EvaluationData, population: frozenset[str]
) -> tuple[EvaluationData, dict[str, int]]:
    """``data`` with every bucket kept to ``population``, and what was left out.

    The projection and binding counters are carried over as the build reported
    them: they describe the delta over every protein, before the restriction.
    """

    def kept(bucket: dict[str, set[str]]) -> dict[str, set[str]]:
        return {protein: terms for protein, terms in bucket.items() if protein in population}

    restricted = EvaluationData(
        nk=kept(data.nk),
        lk=kept(data.lk),
        pk=kept(data.pk),
        known=kept(data.known),
        pk_known=kept(data.pk_known),
        removed=kept(data.removed),
        projection=data.projection,
        binding_dropped=data.binding_dropped,
    )
    outside = {
        "outside_population_delta_proteins": data.delta_proteins - restricted.delta_proteins,
        "outside_population_nk_proteins": data.nk_proteins - restricted.nk_proteins,
        "outside_population_lk_proteins": data.lk_proteins - restricted.lk_proteins,
        "outside_population_pk_proteins": data.pk_proteins - restricted.pk_proteins,
    }
    return restricted, outside
