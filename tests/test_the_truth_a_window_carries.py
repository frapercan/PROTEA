"""``gains``: which terms each query gained, with its stratum and its closure.

THIS IS THE TABLE THAT CARRIES THE TRUTH, so the subject of this file is the
two ways it could quietly stop being a copy of it.

ONE: its population. ``queries`` excludes a protein with no stored sequence,
because the exported FASTA excludes it and the training rows have to be about
the scored population. A gain for a protein outside that set would be a gain
for a query the loader does not know, and the two tables would disagree about
who the queries are without either being wrong on its own. Measured 2026-11-08
over the 65 admissible windows: **zero** delta proteins lack a sequence, so the
guard changes nothing today and says so when that stops being true.

TWO: its closure. ``projected`` means the gain is expressed on the pivot and
``as_closed`` means on each end's own graph, and the same window under two
closures is two different truths. The mapping lives in one place and a profile
with no entry is refused rather than guessed.

THE SHAPE, measured before the code: 9.596.563 rows over those 65 windows, the
largest window 691.553. That is the one place where a tuple per row would hurt,
so the columns are six parallel lists.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from protea.core.transitions_export import _gains as G
from protea.core.transitions_export._window_facts import WindowFacts

ASPECTO = {"GO:1": "BPO", "GO:2": "MFO", "GO:3": "CCO"}


PIVOTE = uuid.uuid4()
CORPUS_T = uuid.uuid4()
CORPUS_T1 = uuid.uuid4()


class _Eset:
    def __init__(self, perfil="projected", viejo=None, nuevo=None) -> None:
        self.id = uuid.uuid4()
        self.truth_profile = perfil
        self.pivot_snapshot_id = PIVOTE
        # Por defecto (A): los dos nativos en el pivote.
        self.old_native_snapshot_id = PIVOTE if viejo is None else viejo
        self.new_native_snapshot_id = PIVOTE if nuevo is None else nuevo


class _Data:
    def __init__(self, nk=None, lk=None, pk=None, removed=None) -> None:
        self.nk = nk or {}
        self.lk = lk or {}
        self.pk = pk or {}
        self.removed = removed or {}


def _facts(data, perfil="projected", viejo=None, nuevo=None):
    e = _Eset(perfil, viejo, nuevo)
    return WindowFacts(
        eset=e,
        window=str(e.id),
        data=data,
        version_t="200",
        version_t_prime="201",
        horizon_days=31,
        old_end_snapshot=CORPUS_T,
        new_end_snapshot=CORPUS_T1,
    )


# ------------------------------------------------------------------ el cierre


class TestTheClosure:
    """IT IS DERIVED FROM THE NATIVES, AND THE FIRST VERSION GOT IT WRONG.

    It mapped ``projected`` to ``pivot`` and ``as_closed`` to ``native``, which
    ADR-D56 says in as many words is wrong: ``truth_profile`` says HOW the
    truth is derived and ``closure`` says WHICH GRAPHS the two ends close in,
    and they vary independently.

    So these tests walk combinations of NATIVES and not values of
    ``TRUTH_PROFILES``, and the one that matters is
    ``test_two_projected_windows_can_have_different_closures``: the (A) and (B)
    twins of ``160 -> 177`` in the database are both ``projected`` and differ
    only there, so under the mapping the (B) one would have been written as
    ``pivot``. The desktop session caught it by reading the ADR this project's
    own earlier PR had written.
    """

    def test_both_natives_on_the_pivot_is_pivot(self):
        assert G.closure_of(_facts(_Data())) == "pivot"

    def test_each_native_on_its_own_era_is_native(self):
        assert G.closure_of(_facts(_Data(), viejo=CORPUS_T, nuevo=CORPUS_T1)) == "native"

    def test_two_projected_windows_can_have_DIFFERENT_closures(self):
        """THE TEST THIS CLASS EXISTS FOR, and the ADR's own proof."""
        a = _facts(_Data(), perfil="projected")
        b = _facts(_Data(), perfil="projected", viejo=CORPUS_T, nuevo=CORPUS_T1)

        assert a.eset.truth_profile == b.eset.truth_profile == "projected"
        assert G.closure_of(a) == "pivot"
        assert G.closure_of(b) == "native"

    def test_as_closed_on_the_pivot_is_still_pivot(self):
        """The other direction of the same independence."""
        assert G.closure_of(_facts(_Data(), perfil="as_closed")) == "pivot"

    def test_a_third_combination_is_refused(self):
        """Neither both-on-the-pivot nor each-on-its-own-era has a name yet."""
        with pytest.raises(ValueError, match="neither both-on-the-pivot"):
            G.closure_of(_facts(_Data(), viejo=PIVOTE, nuevo=uuid.uuid4()))

    def test_the_profile_is_not_read_at_all(self):
        import inspect

        fuente = inspect.getsource(G.closure_of)

        assert "truth_profile" not in fuente.split('"""')[2]


# ------------------------------------------------------------- lo que escribe


def test_it_writes_one_row_per_query_and_term_with_its_stratum():
    cols = G.new_columns()
    data = _Data(nk={"P1": {"GO:2", "GO:1"}}, pk={"P2": {"GO:3"}})

    g = G.gains_of(_facts(data), ASPECTO, {"P1", "P2"}, cols)

    assert g.rows == 3
    assert g.by_stratum == {"nk": 2, "lk": 0, "pk": 1}
    assert g.distinct_terms == 3
    assert g.closure == "pivot"
    # Orden estable por construccion: estratos declarados, consulta ordenada,
    # termino ordenado. Sin un sort final de 9,6 millones de filas.
    assert cols["go_id"] == ["GO:1", "GO:2", "GO:3"]
    assert cols["stratum"] == ["nk", "nk", "pk"]
    assert cols["aspect"] == ["BPO", "MFO", "CCO"]
    assert cols["closure"] == ["pivot", "pivot", "pivot"]


def test_removed_is_not_a_stratum():
    """Those proteins only LOST annotation: reported by the evaluation, never scored."""
    cols = G.new_columns()
    data = _Data(nk={"P1": {"GO:1"}}, removed={"P9": {"GO:2"}})

    g = G.gains_of(_facts(data), ASPECTO, {"P1", "P9"}, cols)

    assert g.rows == 1
    assert "removed" not in G.STRATA
    assert "P9" not in cols["query"]


def test_several_windows_accumulate_into_the_same_columns():
    """Appending rather than returning rows is the measured decision."""
    cols = G.new_columns()
    a = G.gains_of(_facts(_Data(nk={"P1": {"GO:1"}})), ASPECTO, {"P1"}, cols)
    b = G.gains_of(_facts(_Data(nk={"P2": {"GO:2"}})), ASPECTO, {"P2"}, cols)

    assert len(cols["window"]) == 2
    assert {a.window, b.window} == set(cols["window"])


# ------------------------------------------------------------------ los guardas


class TestWhatItRefusesRatherThanDrops:
    """Both guards refuse rather than drop, and neither filters anything today.

    Dropping would make ``gains`` quietly smaller than the truth it copies, and
    a row count is the only thing a reader could compare it against.
    """

    def test_a_gain_for_a_protein_queries_excludes(self):
        """It would be a gain for a query the loader does not know."""
        cols = G.new_columns()
        data = _Data(nk={"P1": {"GO:1"}}, pk={"P_sin_secuencia": {"GO:2"}})

        with pytest.raises(ValueError, match="not in the query population"):
            G.gains_of(_facts(data), ASPECTO, {"P1"}, cols)

    def test_a_gain_on_a_term_the_axis_cannot_name(self):
        """The loader groups by aspect, so it is a row it cannot place."""
        cols = G.new_columns()
        data = _Data(nk={"P1": {"GO:1", "GO:9999"}})

        with pytest.raises(ValueError, match="not on the declared axis"):
            G.gains_of(_facts(data), ASPECTO, {"P1"}, cols)

    def test_the_message_names_the_window_and_a_sample(self):
        cols = G.new_columns()
        facts = _facts(_Data(nk={"P_fuera": {"GO:1"}}))

        with pytest.raises(ValueError) as exc:
            G.gains_of(facts, ASPECTO, set(), cols)

        assert facts.window in str(exc.value)
        assert "P_fuera" in str(exc.value)


# ------------------------------------------------------------------- el parquet


def test_the_six_columns_are_the_loader_s():
    assert G.SCHEMA.names == ["window", "query", "aspect", "go_id", "closure", "stratum"]


def test_the_table_is_written_from_the_accumulated_columns(tmp_path: Path):
    cols = G.new_columns()
    G.gains_of(_facts(_Data(nk={"P1": {"GO:1"}}, lk={"P2": {"GO:2"}})), ASPECTO, {"P1", "P2"}, cols)
    destino = tmp_path / "gains.parquet"

    assert G.write_table(cols, str(destino)) == 2

    tabla = pq.read_table(destino).to_pydict()
    assert tabla["query"] == ["P1", "P2"]
    assert tabla["stratum"] == ["nk", "lk"]


def test_an_empty_run_writes_an_empty_table_rather_than_failing(tmp_path: Path):
    """A window set with no gains is a real answer, and the sha describes it."""
    destino = tmp_path / "vacio.parquet"

    assert G.write_table(G.new_columns(), str(destino)) == 0
    assert pq.read_table(destino).num_rows == 0
