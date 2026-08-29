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

The epoch is the thing that has to move when the meaning does. These pin
that it is in every key and that moving it moves every key.
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
