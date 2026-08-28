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

from typing import TYPE_CHECKING

from protea.infrastructure.orm.models.protein.protein import Protein

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__all__ = ("load_sequence_identities",)


def load_sequence_identities(session: Session, accessions: set[str]) -> dict[str, str]:
    """Read one sequence identity per accession, in chunks.

    Args:
        session: An open session on the store holding ``protein``.
        accessions: The neighbour accessions to map. Order is irrelevant;
            the rank is taken from the neighbour list, not from here.

    Returns:
        Accession to sequence identity, as a string so the method side
        stays free of PROTEA's key types. Every protein in this corpus
        has a sequence row (measured: 0 of 616,846 are null), so a
        missing entry means the accession is not a protein here, not
        that a protein has no sequence.
    """
    from protea.config.tuning import get_tuning

    chunk_size = get_tuning().operation.annotation_chunk_size
    identities: dict[str, str] = {}
    acc_list = list(accessions)
    for start in range(0, len(acc_list), chunk_size):
        rows = (
            session.query(Protein.accession, Protein.sequence_id)
            .filter(Protein.accession.in_(acc_list[start : start + chunk_size]))
            .filter(Protein.sequence_id.isnot(None))
            .all()
        )
        for accession, sequence_id in rows:
            identities[accession] = str(sequence_id)
    return identities
