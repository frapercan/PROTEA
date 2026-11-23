"""When an aspect's bank is cut out of the unified pool, and when it is not.

A fancy-index slice is a full copy. The three aspect corpora together are
1,777,762 rows, which at 1536 dimensions is 10.92 GB of float32, and with
``aspect_knn_workers`` at its default of 3 the three searches run concurrently
in threads and all three copies are alive at once.

Only one of them is read at a time when those searches run in series. So the
view keeps the index array and the pool it would be cut from, and the cut
happens when a search asks for it: the caller holds one aspect's bank for one
search and lets it go. On the serial path the peak carries 3.81 GB, the largest
aspect, instead of 10.92 GB.

This is a memory layout and not a method. The rows and their order are exactly
what the eager cut produced, because it is the same index array applied to the
same pool, so nothing a run writes or scores changes.

ON THE NUMPY BACKEND AN UNCUT ASPECT IS NOT CUT AT ALL. With every evidence code
but ND, aspect P of photo 211 is 1,383,730 donors, 92 % of the pool, so even one
cut is nearly a second copy of it: 13.2 GiB of float32 at ESM-2 3B's 2,560
dimensions, beside the pool's own 21.6 GiB (half precision plus its normalised
copy), and the run did not fit in the desktop's memory. :func:`search_aspect_in_blocks`
searches the aspect block by block straight from the pool and merges the blocks'
nearest, so only one block is copied at a time.

The neighbours are the ones one search over the whole aspect returns, to the
bit. The numpy search accumulates each distance in float64 and casts it to
float32, reading the bank in blocks of its own (``_REF_BLOCK``, 50,000 rows).
That cast does not erase everything: near zero, a query against its own sequence
or a twin's, one float32 step is about 1e-15, and the float64 product of the
same two rows can move by that much when it is computed in a matrix of another
shape (measured: a one-row block, which BLAS computes as a matrix-vector
product, moved a query's distance to its twin from -3.5076411e-08 to
-3.5076408e-08; blocks of 4, 5, 11 and 13 rows of the same 42 did not). So the
blocks are cut at multiples of the kernel's block, and every float64 product is
computed in the very call the whole search makes, with the same shapes and the
same rows. The search ranks by distance and then by column position, and a
donor's position here is its index in the aspect, the column it has in the whole
cut. Each block returns its ``k`` best under that order, the whole aspect's
``k`` best are among them, and merging the candidates by (distance, position)
keeps exactly those, in that order. Measured on the all-evidence pool of ESM-2
150M (1,507,676 donors), 300 targets, the three aspects, k 5 and 10: every list
identical, including the 340 neighbours at distance about 0, in the same time.

Its own module because ``_batch_op_reference`` is at its size budget, which is
the same reason ``_reference_binary`` and ``_reference_needs`` came out of it.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NamedTuple

#: Where an aspect view keeps what it needs to cut its own bank: the index
#: array into the unified pool, and the two pools it may be cut from.
_ASPECT_SLICE = "__slice_source__"


class UncutAspect(NamedTuple):
    """An aspect still inside the pool: its rows there, the pool, their accessions."""

    indices: Any
    pool: Any
    accessions: list[str]


def aspect_bank(view: Any, *, use_cos: bool) -> Any:
    """The aspect's f32 bank, gathered from the unified pool when asked for.

    An already-materialised view is returned as it stands, so a caller that
    built one eagerly still works and a test that hands over a plain dict does
    not have to know this module exists. ``None`` means there is no bank of
    that dtype to read, which is what the eager cut also returned.
    """
    ready = view.get("embeddings_f32_cos" if use_cos else "embeddings_f32")
    if ready is not None:
        return ready
    source = view.get(_ASPECT_SLICE)
    if source is None:
        return None
    indices, plain, cos = source
    pool = cos if use_cos else plain
    return None if pool is None else pool[indices]


def uncut_aspect(view: Any, *, use_cos: bool) -> UncutAspect | None:
    """The aspect as it sits in the pool, or ``None`` when there is nothing to search there.

    ``None`` when the view already holds its bank (an eager view, or a test's
    plain dict) or has no pool of that dtype: the caller then searches the bank
    :func:`aspect_bank` returns, as before.
    """
    if view.get("embeddings_f32_cos" if use_cos else "embeddings_f32") is not None:
        return None
    source = view.get(_ASPECT_SLICE)
    if source is None:
        return None
    indices, plain, cos = source
    pool = cos if use_cos else plain
    return None if pool is None else UncutAspect(indices, pool, list(view["accessions"]))


def kernel_aligned(block_rows: int) -> int:
    """``block_rows`` rounded up to a multiple of the numpy search kernel's block."""
    from protea_method import _order_invariance

    kernel_block = int(_order_invariance._REF_BLOCK)
    return max(1, -(-int(block_rows) // kernel_block)) * kernel_block


def _keep_best(best: list[list[tuple[float, int]]], hits: list, k: int) -> None:
    """Fold one block's hits into each query's ``k`` best by (distance, position)."""
    for kept, row in zip(best, hits, strict=True):
        kept.extend((distance, position) for position, distance in row)
        kept.sort()
        del kept[k:]


def search_aspect_in_blocks(
    queries: Any,
    aspect: UncutAspect,
    *,
    k: int,
    block_rows: int,
    distance_threshold: float | None,
    search: Callable[..., list[list[tuple[Any, float]]]],
) -> list[list[tuple[str, float]]]:
    """The aspect's ``k`` nearest donors, searched block by block from the pool.

    ``search`` is the numpy search with its metric bound. The same neighbours as
    one search over the whole aspect, and why, are in the module docstring. The
    threshold is applied after the merge, where the search applies it after its
    selection.
    """
    rows_per_block = kernel_aligned(block_rows)
    best: list[list[tuple[float, int]]] = [[] for _ in range(int(queries.shape[0]))]
    for start in range(0, len(aspect.indices), rows_per_block):
        rows = aspect.indices[start : start + rows_per_block]
        block = aspect.pool[rows]
        # Positions instead of accessions: the search hands back whatever it is
        # given for a column, and the merge needs the column.
        hits = search(queries, block, range(start, start + len(rows)), k, distance_threshold=None)
        del block
        _keep_best(best, hits, k)
    return [
        [
            (aspect.accessions[position], distance)
            for distance, position in kept
            if distance_threshold is None or distance <= distance_threshold
        ]
        for kept in best
    ]


__all__ = [
    "_ASPECT_SLICE",
    "UncutAspect",
    "aspect_bank",
    "kernel_aligned",
    "search_aspect_in_blocks",
    "uncut_aspect",
]
