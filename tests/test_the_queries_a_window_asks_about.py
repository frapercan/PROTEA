"""The queries table, the window's facts, and the cut that bounds them.

THE LEAKAGE THIS FILE EXISTS FOR. The first bundle written here declared
``cut_version`` 211 over 64 windows, and **15 of them ended after 211**: every
transition from 211 to 212 through 225 to 226. All fifteen fall INSIDE the span
of the long validation window, 211 to 227, so their gains are part of the truth
that window scores. The table that run published holds ontology terms rather
than annotations, so nothing leaked, but the bundle declared a scope that was
not its own and the next table would have leaked. The desktop session found it
by reading the manifest against the design.

So ``test_a_window_ending_after_the_cut_is_refused`` is the test this file is
written around. The laboratory's loader checks the same rule and would refuse
the whole bundle, which means it would not have passed in silence -- but a
bundle that gets built and then rejected is still a bundle somebody has to
diagnose.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from protea.core.transitions_export import _queries as Q
from protea.core.transitions_export import _window_facts as F


class _Aset:
    def __init__(self, version="200", fecha=datetime(2022, 1, 1)) -> None:
        self.id = uuid.uuid4()
        self.source_version = version
        self.source_published_at = fecha


class _Eset:
    def __init__(self) -> None:
        self.id = uuid.uuid4()
        self.old_annotation_set_id = uuid.uuid4()
        self.new_annotation_set_id = uuid.uuid4()


class _Session:
    """Answers ``get`` for the two annotation sets, and the sequence query."""

    def __init__(self, viejo=None, nuevo=None, secuencias=None) -> None:
        self.viejo = viejo if viejo is not None else _Aset("200", datetime(2022, 1, 1))
        self.nuevo = nuevo if nuevo is not None else _Aset("201", datetime(2022, 2, 1))
        self.secuencias = secuencias or {}
        self._primero = True

    def get(self, _model, ident):
        if self.viejo is not None and ident == getattr(self, "_old_id", None):
            return self.viejo
        # The first get is the old end, the second the new one: that is the
        # order ``facts_of`` asks in, and asserting it here would couple the
        # test to the order, so both are answered by position.
        if self._primero:
            self._primero = False
            return self.viejo
        return self.nuevo

    def execute(self, _statement):
        return _Rows([(a, sid, seq) for a, (sid, seq) in self.secuencias.items()])


class _Rows:
    def __init__(self, rows) -> None:
        self._rows = rows

    def all(self):
        return self._rows


class _Data:
    def __init__(self, por_proteina) -> None:
        self.nk = por_proteina
        self.lk: dict[str, set[str]] = {}
        self.pk: dict[str, set[str]] = {}


def _facts(version_t="200", version_t_prime="201", dias=31, data=None):
    e = _Eset()
    return F.WindowFacts(
        eset=e,
        window=str(e.id),
        data=data if data is not None else _Data({"P1": {"GO:1"}, "P2": {"GO:2"}}),
        version_t=version_t,
        version_t_prime=version_t_prime,
        horizon_days=dias,
    )


# ------------------------------------------------------------------- el corte


class TestTheCutBoundsTheWindows:
    def test_a_window_ending_after_the_cut_is_refused(self):
        """THE TEST THIS FILE IS WRITTEN AROUND.

        215 to 216 is one of the fifteen: its gain is in the future of a model
        cut at 211, and it is also inside the span the long validation window
        scores.
        """
        with pytest.raises(ValueError, match="cannot train a model cut there"):
            F.refuse_after_the_cut(_facts("215", "216"), "211")

    def test_a_window_ending_exactly_at_the_cut_is_allowed(self):
        """A model cut at 211 is entitled to the gains up to 211."""
        F.refuse_after_the_cut(_facts("205", "211"), "211")

    def test_a_window_entirely_before_the_cut_is_allowed(self):
        F.refuse_after_the_cut(_facts("156", "157"), "211")

    def test_the_message_names_both_versions_and_the_cut(self):
        with pytest.raises(ValueError) as exc:
            F.refuse_after_the_cut(_facts("225", "226"), "211")

        texto = str(exc.value)
        assert "225" in texto and "226" in texto and "211" in texto


# -------------------------------------------------------- los hechos, una vez


class TestWhatTheFactsRefuse:
    def _facts_of(self, session, monkeypatch, data=None):
        import protea.core.evaluation as ev

        monkeypatch.setattr(
            ev, "load_evaluation_data_for_set", lambda _s, _e: (data or _Data({}), None)
        )
        return F.facts_of(session, _Eset())

    def test_an_end_with_no_release_is_refused(self, monkeypatch):
        """A NULL version does not FAIL the leakage rule, it SKIPS it.

        And a bundle whose rule was never evaluated looks exactly like one
        that passed. A window's release is indeterminate in 23 of the 130
        evaluation sets here, so this case exists.
        """
        session = _Session(viejo=_Aset(version=None))

        with pytest.raises(ValueError, match="no source_version"):
            self._facts_of(session, monkeypatch)

    def test_an_end_with_no_publication_date_is_refused(self, monkeypatch):
        session = _Session(nuevo=_Aset("201", None))

        with pytest.raises(ValueError, match="no\\s+publication date"):
            self._facts_of(session, monkeypatch)

    def test_a_window_spanning_no_days_is_refused(self, monkeypatch):
        """Releases 192 and 193 are byte-identical and share a date.

        A transition between them is a window over one file counted twice. The
        64 windows of this campaign skip it, going 191 to 192 and then 193 to
        194, so somebody already handled it; this keeps it handled.
        """
        mismo = datetime(2022, 3, 1)
        session = _Session(viejo=_Aset("192", mismo), nuevo=_Aset("193", mismo))

        with pytest.raises(ValueError, match="byte-identical"):
            self._facts_of(session, monkeypatch)

    def test_the_horizon_is_the_days_between_the_two_publications(self, monkeypatch):
        session = _Session(
            viejo=_Aset("200", datetime(2022, 1, 1)), nuevo=_Aset("201", datetime(2022, 2, 1))
        )

        facts = self._facts_of(session, monkeypatch)

        assert facts.horizon_days == 31
        assert (facts.version_t, facts.version_t_prime) == ("200", "201")
        # The join key is the FULL id as a string, never a prefix or a name.
        assert len(facts.window) == 36


# ------------------------------------------------------------------- queries


def test_the_population_is_the_target_export_s_rule_and_not_a_second_one():
    """Imported, not rewritten.

    Writing the union again here would make the training set's population a
    second rule that agrees with the first until it does not.
    """
    import inspect

    fuente = inspect.getsource(Q.queries_of)

    assert "delta_accessions" in fuente


def test_a_protein_with_no_sequence_is_excluded_and_counted():
    """Excluded for comparability and not for convenience.

    The exported FASTA excludes it, so including it here would make the
    training rows about a population the scored file does not contain. The
    count is emitted so the exclusion is visible rather than inferred from a
    row count that does not add up.
    """
    session = _Session(secuencias={"P1": (11, "MKV")})

    q = Q.queries_of(session, _facts(), ["nk", "lk", "pk"])

    assert [r.query for r in q.rows] == ["P1"]
    assert q.asked == 2
    assert q.without_sequence == 1


def test_the_length_comes_from_the_stored_sequence_and_not_from_the_file():
    """The published FASTA is canonicalised, so its length can differ."""
    session = _Session(secuencias={"P1": (11, "MKV-MK"), "P2": (22, "MK")})

    q = Q.queries_of(session, _facts(dias=62), ["nk"])

    por_query = {r.query: r for r in q.rows}
    assert por_query["P1"].length == 6
    assert por_query["P1"].sequence_id == 11
    assert por_query["P1"].horizon_days == 62
    assert por_query["P1"].version_t == "200"


def test_a_window_with_no_delta_is_refused():
    """A window nothing asks about should not be in the bundle at all."""
    session = _Session(secuencias={})

    with pytest.raises(ValueError, match="no delta proteins"):
        Q.queries_of(session, _facts(data=_Data({})), ["nk", "lk", "pk"])


def test_the_same_windows_give_the_same_bytes_whatever_order(tmp_path: Path):
    session = _Session(secuencias={"P1": (11, "MKV"), "P2": (22, "MK")})
    a = Q.queries_of(session, _facts(), ["nk"])
    session2 = _Session(secuencias={"P3": (33, "MKVA")})
    b = Q.queries_of(session2, _facts(data=_Data({"P3": {"GO:1"}})), ["nk"])

    uno, otro = tmp_path / "a.parquet", tmp_path / "b.parquet"
    assert Q.write_table([a, b], str(uno)) == 3
    assert Q.write_table([b, a], str(otro)) == 3

    assert hashlib.sha256(uno.read_bytes()).hexdigest() == (
        hashlib.sha256(otro.read_bytes()).hexdigest()
    )
    tabla = pq.read_table(uno).to_pydict()
    assert tabla["query"] == sorted(tabla["query"]) or len(set(tabla["window"])) == 2
    assert set(Q.SCHEMA.names) == {
        "window",
        "query",
        "sequence_id",
        "length",
        "version_t",
        "horizon_days",
    }


# ------------------------------------------------------ la afirmacion cruzada


class TestTheTwoTablesAgreeAboutTheirWindows:
    """The loader joins them by the ``window`` string, as text.

    If one table carried the full id and the other a name or a prefix, the
    negatives restriction would simply not apply and **nothing would fail**.
    """

    def _computed(self, ventanas_terminos, ventanas_consultas):
        from protea.core.operations.export_transitions_dataset import _Computed
        from protea.core.transitions_export._window_terms_out import WindowTerms

        return _Computed(
            terms=[
                WindowTerms(
                    window=w,
                    out=[],
                    axis_size=1,
                    toi_size=1,
                    toi_off_axis=0,
                    restricts=True,
                    gain_terms=1,
                )
                for w in ventanas_terminos
            ],
            queries=[
                Q.WindowQueries(window=w, rows=[], asked=1, without_sequence=0)
                for w in ventanas_consultas
            ],
        )

    def _payload(self, tablas):
        from protea.core.operations.export_transitions_dataset import (
            ExportTransitionsDatasetPayload,
        )

        return ExportTransitionsDatasetPayload(
            windows=["w"], cut="c", cut_version="211", tables=tablas
        )

    def test_a_disagreement_is_refused(self):
        from protea.core.operations.export_transitions_dataset import (
            _refuse_disagreeing_windows,
        )

        hecho = self._computed(["a", "b"], ["a"])

        with pytest.raises(ValueError, match="silently stops restricting"):
            _refuse_disagreeing_windows(
                hecho, self._payload(["window_terms_out", "queries"]), lambda *_a: None
            )

    def test_agreement_is_emitted_rather_than_assumed(self):
        from protea.core.operations.export_transitions_dataset import (
            _refuse_disagreeing_windows,
        )

        eventos: list[str] = []
        _refuse_disagreeing_windows(
            self._computed(["a", "b"], ["b", "a"]),
            self._payload(["window_terms_out", "queries"]),
            lambda e, *_a: eventos.append(e),
        )

        assert eventos == ["transitions.windows_agree"]

    def test_one_table_alone_has_nothing_to_agree_with(self):
        from protea.core.operations.export_transitions_dataset import (
            _refuse_disagreeing_windows,
        )

        _refuse_disagreeing_windows(
            self._computed(["a"], []), self._payload(["window_terms_out"]), lambda *_a: None
        )
