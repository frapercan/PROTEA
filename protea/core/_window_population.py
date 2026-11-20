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

``swissprot_same_sequence`` is LAFA's (:mod:`protea.core.lafa_frame`): the
accessions in Swiss-Prot at BOTH ends, in the release paired with each end's
photo, whose sequence has the same md5 at both. Membership at both ends and the
same sequence are two conditions: of the 573,640 accessions in both 2025_03 and
2026_01, 180 changed sequence. A protein whose sequence changed is not the
protein the start asked about, so LAFA does not score it.

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
from typing import Any, NamedTuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.evaluation import EvaluationData

#: ``every_protein`` first: it is the default, so no stored window moves.
TARGET_POPULATIONS: tuple[str, ...] = ("every_protein", "superset_at_t0", "swissprot_same_sequence")


class Population(NamedTuple):
    """The accessions a window may score, and the photo they come from."""

    cut_release: int
    photo_id: uuid.UUID
    accessions: frozenset[str]

    def described(self) -> dict[str, Any]:
        """What a window's stats say about this population."""
        return {
            "population_cut": self.cut_release,
            "population_photo_id": str(self.photo_id),
            "population_proteins": len(self.accessions),
        }


class SameSequence(NamedTuple):
    """LAFA's targets of one window and the two Swiss-Prot releases they come from."""

    start_release: str
    end_release: str
    #: Accessions in both releases, whatever their sequence.
    in_both: int
    accessions: frozenset[str]

    def described(self) -> dict[str, Any]:
        """What a window's stats say about this population."""
        return {
            "population_swissprot_releases": [self.start_release, self.end_release],
            "population_in_both": self.in_both,
            "population_sequence_changed": self.in_both - len(self.accessions),
            "population_proteins": len(self.accessions),
        }


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


def _swissprot_release_of(session: Session, annotation_set_id: uuid.UUID, end: str) -> uuid.UUID:
    """The Swiss-Prot release paired with the photo whose corpus is this end (never NULL)."""
    photos = session.execute(
        text(
            "SELECT cut_release, swissprot_release_id FROM cut_photo WHERE annotation_set_id = :set_id"
        ),
        {"set_id": annotation_set_id},
    ).all()
    if len(photos) != 1:
        raise ValueError(
            f"{len(photos)} cut photos have annotation set {annotation_set_id} as their corpus; "
            f"the window's {end} needs exactly one to find its Swiss-Prot release"
        )
    return photos[0][1]


def same_sequence_at_both_ends(
    session: Session, old_annotation_set_id: uuid.UUID, new_annotation_set_id: uuid.UUID
) -> SameSequence:
    """LAFA's targets: in Swiss-Prot at both ends, with the same sequence md5 at both.

    The join walks the primary key ``(release_id, accession)`` of the end's
    release for each accession of the start's. Refused when either end has no
    single photo, and when nothing is left.
    """
    start = _swissprot_release_of(session, old_annotation_set_id, "start")
    end = _swissprot_release_of(session, new_annotation_set_id, "end")
    pairs = session.execute(
        text(
            """
            SELECT a.accession, a.sequence_md5 = b.sequence_md5
            FROM swissprot_membership a
            JOIN swissprot_membership b ON b.release_id = :end AND b.accession = a.accession
            WHERE a.release_id = :start
            """
        ),
        {"start": start, "end": end},
    ).all()
    accessions = frozenset(accession for accession, same in pairs if same)
    if not accessions:
        raise ValueError(
            f"no accession has the same sequence in Swiss-Prot releases {start} and {end}"
        )
    names: dict[uuid.UUID, str] = {
        release_id: release
        for release_id, release in session.execute(
            text("SELECT id, release FROM swissprot_release WHERE id IN (:start, :end)"),
            {"start": start, "end": end},
        )
    }
    return SameSequence(names[start], names[end], len(pairs), accessions)


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
