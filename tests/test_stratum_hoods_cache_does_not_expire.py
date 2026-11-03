"""The neighbourhood cache must outlive the thing it is expensive for.

MEASURED on the live store, 2026-10-28: ``neighbourhoods_for`` over the
retrieval arm of the 211 -> 227 window, 1.145.411 candidate rows, took
**635,0 s** and returned 37.467 neighbourhoods. The entry lived 300 s.

So the cache could not hit even once on that arm. Every visitor started a
rebuild that had already expired before the next request arrived and paid the
whole ten and a half minutes again. The stratified grid -- 96 cells, 78
reportable, already computed and stored -- was unreachable from the page that
exists to show it.

An unbounded lifetime is correct here rather than merely convenient, and that
turns on the key. It names a ``prediction_set``, whose candidate rows are
written once and never updated; a rescore does not mutate one, it writes a new
``evaluation_result`` that names it. So the entry can never be wrong, and there
is no invalidation to get right because there is none to do.

The panel slice keeps its five minutes. With the neighbourhoods resident that
half was 0,110 s, and its entry is the big one in memory: one dict per protein,
eleven thousand of them on a real cell.
"""

from __future__ import annotations

import protea.api.cache as cache_module
from protea.api.cache import cached, invalidate
from protea.api.routers._stratum_hoods import HOODS_TTL
from protea.api.routers.stratum_proteins import _TTL


def test_the_two_lifetimes_are_not_the_same_number():
    """They were, and that is the defect. The expensive half and the big half
    are different halves and want different answers."""
    assert _TTL == 300.0
    assert HOODS_TTL != _TTL
    assert HOODS_TTL == float("inf")


def test_an_entry_under_the_hoods_lifetime_survives_a_rebuild_that_outlasts_the_panel_ttl():
    """The exact shape of the live failure: a producer slower than the TTL.

    635 s of work under a 300 s lifetime means the value is stale the moment it
    is stored. Simulated by advancing the clock past the panel TTL, which is
    what 635 s of real work does to a 300 s entry.
    """
    invalidate()
    runs = []

    def produce() -> str:
        runs.append(1)
        return "neighbourhoods"

    real = cache_module.time.monotonic
    try:
        assert cached("stratum:hoods:ps-1", HOODS_TTL, produce) == "neighbourhoods"
        # Well past the panel TTL, and past the 635 s the real producer takes.
        cache_module.time.monotonic = lambda: real() + 10 * _TTL
        assert cached("stratum:hoods:ps-1", HOODS_TTL, produce) == "neighbourhoods"
    finally:
        cache_module.time.monotonic = real

    assert len(runs) == 1


def test_the_panel_entry_still_expires_under_its_own_ttl():
    """The other half keeps a bounded life, so the memory it holds is bounded.

    Asserted rather than assumed, because making both unbounded would be the
    easy change and the wrong one: this entry is one row per protein in the
    panel and there is one per (result, setting, namespace).
    """
    invalidate()
    runs = []

    def produce() -> str:
        runs.append(1)
        return "panel"

    real = cache_module.time.monotonic
    try:
        assert cached("stratum:proteins:r-1:NK:biological_process", _TTL, produce) == "panel"
        cache_module.time.monotonic = lambda: real() + _TTL + 1.0
        assert cached("stratum:proteins:r-1:NK:biological_process", _TTL, produce) == "panel"
    finally:
        cache_module.time.monotonic = real

    assert len(runs) == 2


def test_the_hoods_key_names_only_the_prediction_set():
    """What makes the unbounded lifetime safe, pinned so a future key change
    that folded in something mutable would fail here.

    A key carrying, say, the evaluation result would be wrong in the other
    direction: two results over one prediction set share their neighbourhoods
    exactly, and splitting them would pay 635 s twice for one answer.
    """
    import inspect

    from protea.api.routers import _stratum_hoods

    source = inspect.getsource(_stratum_hoods.hoods)
    assert 'f"stratum:hoods:{prediction_set_id}"' in source
