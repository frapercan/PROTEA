"""Dropping the query protein from its own neighbourhood.

Its own module because both retrieval paths need it and neither has room: the
unified path and the per-aspect path each call ``search_knn`` and each turns the
result into candidate rows, and the rule about who may be a neighbour belongs to
neither of them in particular.

WHY IT EXISTS. Measured on this campaign, read-only against the live store on
2026-08-28: with self-retrieval allowed, the nearest neighbour is the query
protein itself for 95.0 per cent of candidate rows at depth 1, and 81.8 per cent
of the 14,032 query proteins have no other neighbour at that depth at all. A
neighbourhood of one, where the one is yourself, is not a transfer. The depth
sweep run over that pool therefore measured how much self-retrieval each cut
contained rather than what depth costs, and the shallow arms won because they
predicted almost nothing.

WHY IT ASKS FOR ONE MORE. Filtering after the search would silently turn a
requested depth of ten into nine for every protein present in its own donor
corpus, and into ten for every protein absent from it, so the same payload would
mean two different depths depending on the corpus. Asking for k+1 and dropping
the self hit keeps ``limit_per_entry`` meaning the number of real donors.
"""

from __future__ import annotations

__all__ = ["search_k_for", "without_self"]


def search_k_for(k: int, exclude_self: bool) -> int:
    """How many neighbours to ask the index for, to end up with ``k`` real ones."""
    return k + 1 if exclude_self else k


def without_self(
    neighbours: list[list[tuple[str, float]]],
    query_accessions: list[str],
    k: int,
    exclude_self: bool,
) -> list[list[tuple[str, float]]]:
    """Drop each query's own accession from its neighbour list, then trim to ``k``.

    ``neighbours`` is one list of ``(accession, distance)`` per query, in the
    same order as ``query_accessions``; that ordering is the contract
    ``search_knn`` already keeps with every caller here.

    Trimming happens after the drop, never before, so a query that did not
    retrieve itself keeps ``k`` neighbours rather than being punished with k-1
    for the extra slot the search asked for on its behalf.

    A mismatch in length is not silently tolerated. It would mean the neighbour
    lists and the accessions had drifted out of order, and dropping "self" by
    position would then remove a real donor from the wrong protein, which is a
    corrupted candidate set that still looks well formed.
    """
    if not exclude_self:
        return neighbours
    if len(neighbours) != len(query_accessions):
        raise ValueError(
            f"{len(neighbours)} neighbour lists against {len(query_accessions)} "
            "query accessions; the two are positionally paired and dropping the "
            "self hit by position would remove a real donor from another protein"
        )
    return [
        [pair for pair in top if pair[0] != acc][:k]
        for top, acc in zip(neighbours, query_accessions, strict=True)
    ]
