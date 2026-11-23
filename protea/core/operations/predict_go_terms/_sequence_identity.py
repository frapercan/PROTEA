"""The map that lets depth be counted in sequences.

``k_position`` numbers a neighbour list by protein. This corpus holds
616,846 proteins over 528,294 distinct sequences: 38,694 sequences
belong to more than one protein and one belongs to 114, so a cut at
protein position ``d`` does not admit ``d`` distinct points of the
embedding space. It admits however many the bank happened to duplicate
near that query, which differs per query and makes the arms of a depth
sweep incomparable.

``protea_method`` numbers the list by distinct sequence when it is
handed this map. The map is the whole of PROTEA's side of that: one
identity per neighbour, read from the column the schema already keeps
non-unique precisely because proteins share sequences.

The load is deliberately unconditional. ``ref_sequences`` next door is
gated on ``compute_alignments`` and is empty in most runs, so reusing it
would have left ``sequence_rank`` null whenever alignments were off,
which is the failure this project keeps meeting: the system goes on
working while doing nothing, and the null reads as "old row" rather than
"the feature was silently off".
"""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from typing import TYPE_CHECKING

from sqlalchemy import select

from protea.core.operations.predict_go_terms._photo_sequences import member_sequences, photo_of
from protea.infrastructure.orm.models.protein.protein import Protein

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = ("forget_photo_identities", "load_sequence_identities")

#: The identities of every member of a photo, by photo, as this process read them.
#:
#: A photo is written once and never changed (ADR-D59), so a member's identity
#: under it is fixed, and a worker reads the photo once rather than once per batch.
#: A run that excludes each query's own sequence asks for the identity of every
#: accession in the pool, in every batch: on the bank of cut 211 that is 1.5 M
#: accessions, read in chunks in about 100 s of each batch's two minutes, while
#: the photo whole reads in about 4 s. Only the photos used most recently are kept.
_PHOTO_IDENTITIES: OrderedDict[uuid.UUID, dict[str, str]] = OrderedDict()
_PHOTOS_KEPT = 4
_LOCK = threading.Lock()


def forget_photo_identities() -> None:
    """Drop every photo this process has read; the next call reads its photo again."""
    with _LOCK:
        _PHOTO_IDENTITIES.clear()


def _photo_identities(session: Session, photo: uuid.UUID) -> dict[str, str]:
    """Every member of ``photo`` with its sequence identity, read once per process."""
    with _LOCK:
        known = _PHOTO_IDENTITIES.get(photo)
        if known is not None:
            _PHOTO_IDENTITIES.move_to_end(photo)
            return known
    members = member_sequences(photo)
    rows = session.execute(
        select(members.c.accession, members.c.sequence_id).execution_options(yield_per=100_000)
    )
    known = {accession: str(sequence_id) for accession, sequence_id in rows}
    with _LOCK:
        _PHOTO_IDENTITIES[photo] = known
        while len(_PHOTO_IDENTITIES) > _PHOTOS_KEPT:
            _PHOTO_IDENTITIES.popitem(last=False)
    return known


def load_sequence_identities(
    session: Session, accessions: set[str], annotation_set_id: object = None
) -> dict[str, str] | None:
    """One sequence identity per accession.

    Under a photo, from the photo's members, read once per process; otherwise
    from today's proteins, read in chunks.

    Args:
        session: An open session on the store holding ``protein``.
        accessions: The neighbour accessions to map. Order is irrelevant;
            the rank is taken from the neighbour list, not from here.

    Returns:
        Accession to sequence identity, as a string so the method side
        stays free of PROTEA's key types. Every protein in this corpus
        has a sequence row (measured: 0 of 616,846 are null), so a
        missing entry means the accession is not a protein here, not that
        a protein has no sequence.

        None when nothing mapped at all, which is a different statement
        from a partial map and has to be made differently. Nothing mapped
        means this bank cannot be counted in sequences, uniformly, and
        the method leaves the rank empty on every row. A partial map
        means some neighbours can be ranked and others cannot, and the
        method raises rather than ranking half a list, because a rank
        that skips its unmappable neighbours is a rank counted over a
        bank the caller does not think it has.
    """
    from protea.config.tuning import get_tuning

    # A run that transfers from a photo counts sequences at the photo's cut, and
    # reads the photo once per process (``_PHOTO_IDENTITIES``).
    photo = photo_of(session, annotation_set_id) if annotation_set_id is not None else None
    if photo is not None:
        known = _photo_identities(session, photo)
        by_photo = {a: known[a] for a in accessions if a in known}
        return by_photo or None
    chunk_size = get_tuning().operation.annotation_chunk_size
    identities: dict[str, str] = {}
    acc_list = list(accessions)
    for start in range(0, len(acc_list), chunk_size):
        chunk = acc_list[start : start + chunk_size]
        rows = (
            session.query(Protein.accession, Protein.sequence_id)
            .filter(Protein.accession.in_(chunk))
            .filter(Protein.sequence_id.isnot(None))
            .all()
        )
        for accession, sequence_id in rows:
            identities[accession] = str(sequence_id)
    return identities or None
