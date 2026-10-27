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

from protea.api.cache import cached, get_last_known, invalidate, refresh


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


class TestRefreshDoesNotEmptyTheStore:
    """A prewarm must not drop a good value before it has a better one.

    Every ``prewarm_*`` used to be ``invalidate(key)`` then ``cached(...)``.
    The drop happens FIRST, so for however long the producer runs the key is
    absent. Measured on the live API: the annotation-set listing takes a median
    of 145 s and the loop fires every 385 s, so the public endpoint had nothing
    to serve for ~38 per cent of wall time on a key that was never missing.
    """

    def setup_method(self) -> None:
        invalidate()

    def test_the_previous_value_is_servable_for_the_whole_recompute(self) -> None:
        """The regression. With invalidate-then-cached this read None."""
        cached("k", 60, lambda: "viejo")
        visto: list = []
        arrancado = threading.Event()

        def lento():
            arrancado.set()
            time.sleep(0.4)
            return "nuevo"

        t = threading.Thread(target=lambda: refresh("k", 60, lento))
        t.start()
        arrancado.wait(2)
        time.sleep(0.05)
        visto.append(get_last_known("k"))
        t.join()

        assert visto == ["viejo"], "the key was empty while the producer ran"
        assert get_last_known("k") == "nuevo"

    def test_a_reader_gets_the_stale_value_rather_than_blocking(self) -> None:
        """Together with one-producer-per-key: the reader is served, not queued."""
        cached("k", 0.001, lambda: "viejo")
        time.sleep(0.01)
        arrancado = threading.Event()

        def lento():
            arrancado.set()
            time.sleep(0.4)
            return "nuevo"

        t = threading.Thread(target=lambda: refresh("k", 60, lento))
        t.start()
        arrancado.wait(2)
        time.sleep(0.05)
        inicio = time.perf_counter()
        got = cached("k", 60, lambda: "del lector")
        espera = time.perf_counter() - inicio
        t.join()

        assert got == "viejo"
        assert espera < 0.2, "the reader blocked on the refresh"

    def test_it_always_produces_even_when_the_entry_is_live(self) -> None:
        """Skipping on a live entry would let it expire before the next tick."""
        cached("k", 60, lambda: "viejo")
        assert refresh("k", 60, lambda: "nuevo") == "nuevo"
        assert get_last_known("k") == "nuevo"

    def test_only_one_producer_runs_per_key(self) -> None:
        veces: list[int] = []

        def lento():
            veces.append(1)
            time.sleep(0.3)
            return "v"

        hilos = [threading.Thread(target=lambda: refresh("k", 60, lento)) for _ in range(3)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()
        assert len(veces) == 3, "refresh always produces; the lock serialises them"
        assert get_last_known("k") == "v"

    def test_a_failing_producer_leaves_the_previous_value(self) -> None:
        """And releases the lock, or the key deadlocks forever."""
        import pytest

        cached("k", 60, lambda: "viejo")
        with pytest.raises(RuntimeError):
            refresh("k", 60, lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        assert get_last_known("k") == "viejo"
        assert refresh("k", 60, lambda: "nuevo") == "nuevo"


class TestNoPrewarmDropsItsKeyFirst:
    """The wiring across the seven prewarms, not just the primitive."""

    FICHEROS = (
        "protea/api/routers/proteins_stats.py",
        "protea/api/routers/proteins.py",
        "protea/api/routers/annotations/sets.py",
        "protea/api/routers/annotations/snapshots.py",
        "protea/api/routers/embeddings.py",
        "protea/api/routers/benchmark.py",
    )

    def test_no_prewarm_calls_invalidate_before_producing(self) -> None:
        import pathlib
        import re

        raiz = pathlib.Path(__file__).resolve().parents[1]
        culpables = []
        for f in self.FICHEROS:
            src = (raiz / f).read_text()
            for m in re.finditer(r"def (prewarm_\w+)\(", src):
                cuerpo = src[m.start() : src.find("\ndef ", m.end())]
                if "invalidate(" in cuerpo:
                    culpables.append(f"{f}::{m.group(1)}")
        assert culpables == [], f"these drop the key before producing: {culpables}"

    def test_every_prewarm_that_caches_goes_through_refresh(self) -> None:
        """Only the ones that write the in-process cache are in scope.

        ``prewarm_benchmark_matrix`` is deliberately not: it holds no key and
        warms Postgres's buffer cache by running the query, so there is nothing
        to drop and nothing to swap. An earlier version of this test required
        ``refresh`` of every ``prewarm_*`` and flagged it, which is the test
        being wrong rather than the code.
        """
        import pathlib
        import re

        raiz = pathlib.Path(__file__).resolve().parents[1]
        sin_refresh = []
        for f in self.FICHEROS:
            src = (raiz / f).read_text()
            for m in re.finditer(r"def (prewarm_\w+)\(", src):
                cuerpo = src[m.start() : src.find("\ndef ", m.end())]
                toca_la_cache = "cached(" in cuerpo or "refresh(" in cuerpo or "invalidate(" in cuerpo
                if toca_la_cache and "refresh(" not in cuerpo:
                    sin_refresh.append(f"{f}::{m.group(1)}")
        assert sin_refresh == [], f"these cache without refresh(): {sin_refresh}"

    def test_the_scope_is_not_empty(self) -> None:
        """Guards the test above: a filter that matched nothing would pass."""
        import pathlib
        import re

        raiz = pathlib.Path(__file__).resolve().parents[1]
        con_refresh = 0
        for f in self.FICHEROS:
            src = (raiz / f).read_text()
            for m in re.finditer(r"def prewarm_\w+\(", src):
                cuerpo = src[m.start() : src.find("\ndef ", m.end())]
                if "refresh(" in cuerpo:
                    con_refresh += 1
        assert con_refresh == 7, f"expected the seven caching prewarms, found {con_refresh}"
