"""Which annotation rows admit a donor to the reference pool, and which it may donate.

Three readers apply a donor policy to the rows of an annotation set: the pool's
own query (``_ReferenceMixin._reference_pool_query``), the build of the
per-aspect indices and their annotation arrays (``_collect_aspect_annotations``)
and the per-batch donation loader (``_fetch_annotation_chunk``). Each predicate
they share is written here once, so the three cannot read one policy two ways.

A ROW ADMITS ONLY WHEN IT IS POSITIVE. A row whose qualifier carries ``NOT``
says the protein does not have the term, so it donates nothing, and the aspect
build and the donation loader always skipped it. The pool's query did not: it
admitted a protein through any row of the set, so on unified retrieval a donor
whose admitted rows were all ``NOT`` rows took a neighbour slot and donated
nothing. It now admits through a positive row that passes the policy's other
filters, as the aspect indices always did; those indices only ever held donors
with a positive row in their aspect, so aspect-separated retrieval is unchanged.
The pool cache's epoch moved with it (``protea.core.disk_cache._CACHE_EPOCH``).

THE CHANNEL OF A ROW IS READ AT THE PHOTO'S CUT. ``evidence_channels`` admits a
row whose evidence channel (``protea.core.cut_photo.channels.CHANNELS``) is
listed. A photo's rows are stored without their channel, which is a function of
the row's code, its reference class and whether the protein was reviewed at the
cut (``cut_photo_member.superset``), declared as the channel table the photo was
classified with; ``evidence_channel_of`` reads it in SQL. An annotation set no
photo loaded has no cut to read a channel at, so a policy naming channels over
one is refused rather than read against another photo's table.

A DECLARED SET RESTRICTS DONORS, NOT ROWS. ``donor_set_id`` names a set of photo
members (``declare_photo_member_set``); the pool admits only the proteins it
lists, by one ``EXISTS`` semi-join on ``query_set_entry`` (unique on the set and
the accession). Donations are read only for the pool's neighbours, so the
loader needs no second copy of it.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any, NamedTuple

from sqlalchemy import and_, exists, func, select
from sqlalchemy.orm import Session, aliased

from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.query.query_set import QuerySetEntry

__all__ = [
    "ChannelSource",
    "admit_channels",
    "admit_donor_set",
    "channel_source",
    "channels_of_the_cut",
    "positive_rows",
]


def positive_rows() -> Any:
    """The rows whose qualifier carries no ``NOT``: the rows that can be donated.

    ``qualifier IS NULL`` is kept explicitly, because ``LIKE`` against a null is
    null and would drop the row, and most rows carry no qualifier at all
    (measured on GOA 156: 871,484 of its 875,990 reliable rows).
    """
    return ProteinGOAnnotation.qualifier.is_(None) | ~ProteinGOAnnotation.qualifier.like("%NOT%")


class ChannelSource(NamedTuple):
    """Where the channels of an annotation set's rows are read.

    The photo that loaded the set, whose members say whether each protein was
    reviewed at the cut, and the digest of the channel table that photo was
    classified with.
    """

    photo_id: uuid.UUID
    table_sha256: str


def channel_source(session: Session, annotation_set_id: object) -> ChannelSource | None:
    """The photo that loaded ``annotation_set_id`` and its channel table, or None."""
    try:
        set_id = uuid.UUID(str(annotation_set_id))
    except ValueError:
        return None
    row = session.execute(
        select(CutPhoto.id, CutPhoto.channel_table_sha256).where(
            CutPhoto.annotation_set_id == set_id
        )
    ).first()
    return None if row is None else ChannelSource(row[0], str(row[1]))


def channels_of_the_cut(
    session: Session, annotation_set_id: object, donor_policy: Any
) -> ChannelSource | None:
    """Where the policy's channels are read, or None when it names no channel.

    Asks nothing of the database when the policy names none, so every policy
    stored before channels existed resolves exactly the queries it did.

    :raises ValueError: when the policy names channels over an annotation set
        no photo loaded.
    """
    if not getattr(donor_policy, "evidence_channels", None):
        return None
    source = channel_source(session, annotation_set_id)
    if source is None:
        raise ValueError(
            f"the donor policy admits annotations by their evidence channel, and annotation "
            f"set {annotation_set_id} was loaded by no cut photo: a channel is read at a "
            "photo's cut, with the channel table it was classified with"
        )
    return source


def admit_channels(rows: Any, channels: Sequence[str], source: ChannelSource | None) -> Any:
    """Narrow ``rows`` (a query over ``protein_go_annotation``) to the listed channels.

    Joins the photo's member row of each annotated protein, one per accession
    by the member table's key, for the review status the channel depends on.

    :raises ValueError: when no photo was resolved for the rows. A caller that
        forgets to resolve it is refused here instead of reading every channel.
    """
    if source is None:
        raise ValueError(
            "the donor policy admits annotations by their evidence channel, and no cut "
            "photo was resolved for the rows it restricts"
        )
    member = aliased(CutPhotoMember, name="channel_member")
    channel = func.evidence_channel_of(
        source.table_sha256,
        ProteinGOAnnotation.evidence_code,
        ProteinGOAnnotation.db_reference,
        ProteinGOAnnotation.with_from,
        member.superset,
    )
    return rows.join(
        member,
        and_(
            member.photo_id == source.photo_id,
            member.accession == ProteinGOAnnotation.protein_accession,
        ),
    ).filter(channel.in_(list(channels)))


def admit_donor_set(query: Any, accession: Any, donor_set_id: str | None) -> Any:
    """Keep the donors ``donor_set_id`` lists; ``query`` unchanged when no set is named.

    ``accession`` is the column naming the pool's donors: the photo's member
    accession on a photo's run.
    """
    if donor_set_id is None:
        return query
    listed = exists().where(
        QuerySetEntry.query_set_id == uuid.UUID(str(donor_set_id)),
        QuerySetEntry.accession == accession,
    )
    return query.filter(listed)
