"""What a cache holds can change without its parameters changing.

On 2026-08-19 the reference pools were built with a donor policy that gated
which PROTEINS entered the pool and not which annotations they could donate.
On 2026-08-29 that was fixed, so the same policy now admits strictly fewer
annotations. The policy object did not change, so its discriminator did not
change, so the key did not change, and 240 files sat on disk under exactly
the name the corrected run would look for.

Read from one of them before the epoch was added: 87,709 IEA, 26,044 IBA,
10,767 ISO, 9,691 ISS. The retrieval would have found that pool, reported
success, and produced the rows the fix was written to prevent.

It happened again on 2026-10-12, with a different kind of content change: the
reference pool and the per-aspect index arrays became ORDERED, where before the
order came from the query plan and, for the aspect indices, from per-process
string hashing. The ORDER is part of what a pool contains, because ties at the
k-th neighbour are resolved by bank position -- worth ~1e-3 per cell in
f_micro_w -- and it is invisible to every guard this cache has: the disk load
checks only ``len(accessions) == expected_count`` and the row SET did not
change, the freshness branch skips even that, and the per-aspect indices are
guarded by a fingerprint computed over the accessions just loaded from the same
cache, which is self-referential.

Measured before that bump, on the live cache for
``e3__e7ba37da...__a4517ed0...__donor-f7658ad59248``: 142,422 accessions not in
accession order, with all three ``.pool`` files recording the fingerprint of
that stale order. Without the bump the ordering fix was a silent no-op.

The epoch is the thing that has to move when the meaning does. These pin
that it is in every key, that moving it moves every key, and that it has
moved for each content change so far.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core import disk_cache
from protea.core.disk_cache import _cache_key

_A = uuid.UUID("11111111-1111-1111-1111-111111111111")
_B = uuid.UUID("22222222-2222-2222-2222-222222222222")
_POLICY = "reviewed=0|codes=EXP,IDA|norefs=*"


def test_the_epoch_is_in_the_key() -> None:
    assert _cache_key(_A, _B).startswith(f"e{disk_cache._CACHE_EPOCH}__")


@pytest.mark.parametrize("discriminator", ["", _POLICY])
def test_moving_the_epoch_moves_the_key_even_for_a_permissive_pool(
    monkeypatch: pytest.MonkeyPatch, discriminator: str
) -> None:
    """Including the permissive one, whose contents did not change.

    An epoch that applies only where somebody judged it necessary needs that
    judgement to be right every time, and the judgement is what was wrong the
    day this was added.
    """
    before = _cache_key(_A, _B, discriminator)
    monkeypatch.setattr(disk_cache, "_CACHE_EPOCH", disk_cache._CACHE_EPOCH + 1)
    assert _cache_key(_A, _B, discriminator) != before


def test_the_policy_still_separates_two_pools() -> None:
    """The epoch is on top of the discriminator, not instead of it."""
    assert _cache_key(_A, _B, "") != _cache_key(_A, _B, _POLICY)


def test_no_key_from_this_epoch_can_collide_with_a_pre_epoch_file() -> None:
    """Every file written before this change begins with a raw UUID.

    That is what makes the epoch a clean break rather than an invalidation
    that has to be applied by hand to the right subset.
    """
    key = _cache_key(_A, _B, _POLICY)
    assert not key.startswith(str(_A))
    assert key.startswith("e")


def test_the_epoch_moved_for_the_ordered_pools() -> None:
    """Epoch 3 names pools whose order came from the plan and the hash seed.

    Bumped on 2026-10-12. A pool cached under ``e3`` is in the pre-ordering
    order, and nothing in the cache can tell, so ``e4`` is what stops it being
    served to code that now expects accession order.
    """
    assert disk_cache._CACHE_EPOCH >= 4
