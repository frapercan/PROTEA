"""The aggregate cache must run ONE producer per key at a time.

``cached`` released the store lock before calling the producer, so every
concurrent caller on a cold or expired key ran its own copy. The producers this
cache exists for are DISTINCT-over-JOIN queries on hundred-million-row tables.

MEASURED on the live API, by sampling ``pg_stat_activity`` for 30 minutes
without issuing any query: the GO annotation matrix ran 8 times, median 137 s,
max 171 s, 1.167 s of 1.800 -- 65 per cent of wall time recomputing one
aggregate, each run with two parallel workers. The refresh loop accounts for
about 2,6 of those; the rest were public page views arriving while it was
already in flight.
"""

from __future__ import annotations

import threading
import time

from protea.api.cache import cached, get_last_known, invalidate


def _concurrent(n: int, key: str, ttl: float, producer, delay: float = 0.0):
    """Fire ``n`` concurrent ``cached`` calls and return (results, seconds)."""
    out: list = []
    hilos = [threading.Thread(target=lambda: out.append(cached(key, ttl, producer))) for _ in range(n)]
    t = time.perf_counter()
    for h in hilos:
        h.start()
        if delay:
            time.sleep(delay)
    for h in hilos:
        h.join()
    return out, time.perf_counter() - t


class TestOneProducerPerKey:
    def setup_method(self) -> None:
        invalidate()

    def test_a_cold_stampede_runs_the_producer_once(self) -> None:
        """Six callers, one query. This ran six times before."""
        veces: list[int] = []

        def lento():
            veces.append(1)
            time.sleep(0.4)
            return "v"

        out, _ = _concurrent(6, "k", 60, lento)
        assert len(veces) == 1
        assert out == ["v"] * 6

    def test_the_waiters_do_not_serialise(self) -> None:
        """Six 0,4 s producers would take 2,4 s if each caller ran its own."""
        _, segundos = _concurrent(6, "k", 60, lambda: time.sleep(0.4) or "v")
        assert segundos < 1.0

    def test_an_expired_key_also_runs_the_producer_once(self) -> None:
        """The live case: the refresh loop and a page view arrive together."""
        cached("k", 0.001, lambda: "viejo")
        time.sleep(0.01)
        veces: list[int] = []

        def lento():
            veces.append(1)
            time.sleep(0.3)
            return "nuevo"

        _concurrent(6, "k", 60, lento)
        assert len(veces) == 1

    def test_a_caller_with_a_stale_value_gets_it_instead_of_blocking(self) -> None:
        """Blocking a page view on a 137 s aggregate yields a timeout, not an answer.

        ``app._refresh_interval``'s own comment names the 30 s ngrok upstream
        deadline, so the waiter is served the previous value.
        """
        cached("k", 0.001, lambda: "viejo")
        time.sleep(0.01)
        out, segundos = _concurrent(4, "k", 60, lambda: time.sleep(0.4) or "nuevo", delay=0.02)
        assert "viejo" in out, "a waiter should have been served the stale value"
        assert segundos < 1.0

    def test_a_cold_caller_with_nothing_stale_waits_rather_than_duplicating(self) -> None:
        """With no previous value there is nothing to serve, so it waits."""
        veces: list[int] = []

        def lento():
            veces.append(1)
            time.sleep(0.3)
            return "v"

        out, _ = _concurrent(4, "k", 60, lento, delay=0.02)
        assert len(veces) == 1
        assert out == ["v"] * 4


class TestTheRestOfTheContractIsUnchanged:
    def setup_method(self) -> None:
        invalidate()

    def test_a_hit_does_not_call_the_producer(self) -> None:
        veces: list[int] = []
        for _ in range(3):
            cached("k", 60, lambda: veces.append(1) or "v")
        assert len(veces) == 1

    def test_an_expired_entry_is_recomputed(self) -> None:
        assert cached("k", 0.001, lambda: "a") == "a"
        time.sleep(0.01)
        assert cached("k", 60, lambda: "b") == "b"

    def test_the_producer_error_propagates_by_default(self) -> None:
        import pytest

        with pytest.raises(RuntimeError):
            cached("k", 60, lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    def test_serve_stale_on_error_returns_the_previous_value(self) -> None:
        cached("k", 0.001, lambda: "viejo")
        time.sleep(0.01)
        got = cached(
            "k", 60,
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            serve_stale_on_error=True,
        )
        assert got == "viejo"

    def test_a_failed_producer_does_not_keep_the_key_locked(self) -> None:
        """A raise must release the producer lock, or the key deadlocks forever."""
        import pytest

        with pytest.raises(RuntimeError):
            cached("k", 60, lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        assert cached("k", 60, lambda: "v") == "v"

    def test_two_keys_do_not_block_each_other(self) -> None:
        orden: list[str] = []

        def a():
            time.sleep(0.3)
            orden.append("a")
            return "a"

        def b():
            orden.append("b")
            return "b"

        ha = threading.Thread(target=lambda: cached("ka", 60, a))
        ha.start()
        time.sleep(0.05)
        cached("kb", 60, b)
        ha.join()
        assert orden == ["b", "a"], "kb waited for ka"

    def test_invalidate_still_drops_the_value(self) -> None:
        cached("k", 60, lambda: "v")
        invalidate("k")
        assert get_last_known("k") is None


class TestTheRecheckUnderTheProducerLock:
    """The narrow race: the holder finishes between our miss and our acquire.

    A caller checks the store, misses, and only then takes the producer lock.
    If the previous holder stored a fresh value in that gap, producing again
    would be a second copy of the query this whole change exists to avoid.

    Driven through the ``_fresh`` seam rather than by timing, because a test
    that has to win a race by sleeping is a test that fails on a loaded
    machine -- and this machine spends a third of its time recomputing
    aggregates, which is the very thing being fixed.
    """

    def setup_method(self) -> None:
        invalidate()

    def test_it_does_not_produce_when_the_value_appeared_while_we_queued(self) -> None:
        from unittest.mock import patch

        from protea.api import cache as mod

        llamadas = {"n": 0}

        def falso_fresh(key, now):
            llamadas["n"] += 1
            # Primera: miss, como el estado real al entrar.
            # Segunda: la recomprobacion, con el valor que dejo el otro.
            return (False, None) if llamadas["n"] == 1 else (True, "del otro")

        veces: list[int] = []
        with patch.object(mod, "_fresh", falso_fresh):
            got = mod.cached("k", 60, lambda: veces.append(1) or "mio")

        assert got == "del otro"
        assert veces == [], "produced although a fresh value had just appeared"

    def test_it_does_produce_when_nothing_appeared(self) -> None:
        from unittest.mock import patch

        from protea.api import cache as mod

        veces: list[int] = []
        with patch.object(mod, "_fresh", lambda key, now: (False, None)):
            got = mod.cached("k", 60, lambda: veces.append(1) or "mio")

        assert got == "mio"
        assert veces == [1]
