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

Its own module because ``_batch_op_reference`` is at its size budget, which is
the same reason ``_reference_binary`` and ``_reference_needs`` came out of it.
"""

from __future__ import annotations

from typing import Any

#: Where an aspect view keeps what it needs to cut its own bank: the index
#: array into the unified pool, and the two pools it may be cut from.
_ASPECT_SLICE = "__slice_source__"


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


__all__ = ["_ASPECT_SLICE", "aspect_bank"]
