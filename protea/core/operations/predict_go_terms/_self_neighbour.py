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
mean two different depths depending on the corpus. Asking for more than k and
dropping the self hit keeps ``limit_per_entry`` meaning the number of real
donors.

WHY THIS MODULE NO LONGER IMPLEMENTS IT. The first version dropped by
ACCESSION and asked for exactly k+1. The method downstream drops by SEQUENCE
and asks for k plus a margin measured from the bank, so a query with twins in
the pool loses more entries than the pre-search anticipated and the method
reaches past what it was handed. That was fixed in ``protea_method`` and
migrated into the aspect path alone; the naive pair stayed here, callable,
unmarked, under a header saying both paths need it. On 2026-09-07 every
``exclude_self_neighbour`` arm of axis C died on
``SequenceIdentityMissingError`` against a bank where nothing was unmappable,
because the unified path was still the one that had not migrated.

So this module now hands out the sequence-aware pair and implements nothing.
The header above is true again: both paths take their margin and their drop
from the same two functions.
"""

from __future__ import annotations

from protea_method._self_by_sequence import extra_neighbours_for, without_own_sequence

__all__ = ["extra_neighbours_for", "without_own_sequence"]
