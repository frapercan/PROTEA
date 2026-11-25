"""The sequence a protein had at a photo's cut, for a run that transfers from a photo (ADR-D59, ADR-D62).

``protein.sequence_id`` is the protein's sequence TODAY, as UniProt serves it.
A photo's members carry their own: ``cut_photo_member.sequence_md5``, the
sequence of the paired release at the cut, which is the one every bank of the
photos was computed from. A run whose annotation set is a photo's (the photo
loaded it: ``cut_photo.annotation_set_id``) transfers the cut's knowledge, so
it must also search with the cut's sequences, queries and donors alike. Joined
through today's sequence it finds nothing: in development every member is an
accession-only protein row, and the first run over 211 -> 227 loaded 0 of
20,811 queries (2026-11-18).

A run over an annotation set no photo loaded keeps today's sequences: every
reader here takes the photo as optional and does what it did before when it is
None.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence


def photo_of(session: Session, annotation_set_id: object) -> uuid.UUID | None:
    """The photo that loaded ``annotation_set_id``, or None for a set no photo loaded.

    Anything that is not an annotation set id (a malformed value) names no photo,
    and so does an answer that is not a photo id.
    """
    try:
        set_id = uuid.UUID(str(annotation_set_id))
    except ValueError:
        return None
    found = session.execute(
        select(CutPhoto.id).where(CutPhoto.annotation_set_id == set_id)
    ).scalar_one_or_none()
    if isinstance(found, uuid.UUID):
        return found
    if isinstance(found, str):
        try:
            return uuid.UUID(found)
        except ValueError:
            return None
    return None


def member_sequences(photo_id: uuid.UUID) -> Any:
    """(accession, sequence_id): every member of the photo with its sequence at the cut."""
    return (
        select(
            CutPhotoMember.accession.label("accession"),
            Sequence.id.label("sequence_id"),
        )
        .join(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
        .where(CutPhotoMember.photo_id == photo_id)
        .subquery("member_sequences")
    )


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def photo_sequences(
    session: Session, photo_id: uuid.UUID, accessions: list[str], chunk: int = 10_000
) -> dict[str, tuple[int, str]]:
    """Accession to ``(sequence id, residues)`` at the photo's cut, for members that have one.

    The sequence is the member's (``cut_photo_member.sequence_md5``), never the
    protein's row: on a photo's run, an alignment, a length, a target FASTA or a
    transitions table built from today's sequence would read the future in
    production and nothing at all in development.
    """
    found: dict[str, tuple[int, str]] = {}
    for part in _chunks(sorted(set(accessions)), chunk):
        rows = session.execute(
            select(CutPhotoMember.accession, Sequence.id, Sequence.sequence)
            .join(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
            .where(CutPhotoMember.photo_id == photo_id, CutPhotoMember.accession.in_(part))
        ).all()
        found.update(
            {accession: (sequence_id, residues) for accession, sequence_id, residues in rows}
        )
    return found


def photo_taxa(
    session: Session, photo_id: uuid.UUID, accessions: list[str], chunk: int = 10_000
) -> dict[str, int | None]:
    """Accession to the member's taxon at the photo's cut (``cut_photo_member.taxon_id``).

    Every asked accession is a key; a member with no recorded taxon, or an
    accession the photo does not hold, maps to None, as an unknown taxon did
    before.
    """
    found: dict[str, int | None] = dict.fromkeys(accessions)
    for part in _chunks(sorted(set(accessions)), chunk):
        rows = session.execute(
            select(CutPhotoMember.accession, CutPhotoMember.taxon_id).where(
                CutPhotoMember.photo_id == photo_id, CutPhotoMember.accession.in_(part)
            )
        ).all()
        found.update({accession: (int(taxon) if taxon else None) for accession, taxon in rows})
    return found


def donor_pool_base(
    session: Session, annotation_set_id: object, embedding_config_id: uuid.UUID, annotated: Any
) -> tuple[Any, Any]:
    """The donors with a vector of the config and an annotation, and the column naming them.

    On a photo's run every donor searches with its sequence at the cut and the
    bank is ordered by the photo's accession column, with ``protein`` joined for
    the donor policy's filters; otherwise today's sequence, as before.
    """
    photo = photo_of(session, annotation_set_id)
    if photo is not None:
        members = member_sequences(photo)
        accession = members.c.accession
        query = (
            session.query(accession)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == members.c.sequence_id)
                & (SequenceEmbedding.embedding_config_id == embedding_config_id),
            )
            .join(annotated, accession == annotated.c.protein_accession)
            .join(Protein, Protein.accession == accession)
        )
        return query, accession
    query = (
        session.query(Protein.accession)
        .join(
            SequenceEmbedding,
            (SequenceEmbedding.sequence_id == Protein.sequence_id)
            & (SequenceEmbedding.embedding_config_id == embedding_config_id),
        )
        .join(annotated, Protein.accession == annotated.c.protein_accession)
    )
    return query, Protein.accession


__all__ = ["donor_pool_base", "member_sequences", "photo_of", "photo_sequences", "photo_taxa"]
