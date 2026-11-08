"""The two tables that depend on no window: ``terms`` and ``term_ancestors``.

WHY THEY COULD BE WRITTEN BEFORE THE WINDOWS WERE CHOSEN. Every other table is
keyed by a window or a sequence. These two are keyed by ``cut`` alone: the
vocabulary a model may name, and the subsumption between those names. Which
transitions train the model changes neither. I had said there was nothing left
to advance without that decision, and I was wrong; the desktop session pointed
at these.

MEASURED on the axis ``releases/2022-07-01``, 2026-11-08:

    terms in the snapshot      43.558, all live, all with an aspect
    direct edges               77.055
    closure pairs             595.072, both ends live in every one

``Dag.closure``'s own docstring says 783.477 "on GO", which is a different
release. That number was quoted and this one was measured.

AND THE VECTORS ARE VERIFIED, NEVER QUOTED. The artefact was NOT in the
platform's store on 2026-11-08: 6.426 objects over twelve prefixes and nothing
matching it, so ``terms`` refuses until it is published rather than writing a
sha for bytes nobody here can check.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from protea.core._artifact_identity import ArtifactMismatch
from protea.core.operations.export_transitions_dataset import (
    AXIS_ONLY,
    ExportTransitionsDatasetPayload,
)
from protea.core.transitions_export import _terms as T

CUT = "15f002ea-a6f1-4ab2-918a-832f79b082c0"
VECTORES = b"PK\x03\x04 no es un npz de verdad, y no hace falta"
SHA = hashlib.sha256(VECTORES).hexdigest()


def _axis(aspect=None, edges=()):
    return T.Axis(
        cut=CUT,
        aspect=aspect if aspect is not None else {"GO:1": "BPO", "GO:2": "MFO"},
        edges=edges,
    )


class _Store:
    def __init__(self, objetos) -> None:
        self.objetos = objetos

    def get(self, key):
        if key not in self.objetos:
            raise OSError(f"no such key {key}")
        return self.objetos[key]

    def url(self, key):
        return f"s3://protea/{key}"


# --------------------------------------------------------------------- el eje


class TestWhatTheAxisRefuses:
    def test_an_axis_that_can_name_nothing(self, monkeypatch):
        monkeypatch.setattr(T, "_live_terms_with_aspect", lambda _s, _c: (set(), {}))

        with pytest.raises(ValueError, match="no live terms"):
            T.axis_of(object(), uuid.uuid4())

    def test_a_live_term_with_no_aspect(self, monkeypatch):
        """The loader groups by aspect, so a term without one cannot be grouped.

        Measured: on this axis all 43.558 have one, and this refusal is what
        keeps that a fact rather than an assumption.
        """
        monkeypatch.setattr(
            T, "_live_terms_with_aspect", lambda _s, _c: ({"GO:1", "GO:2"}, {"GO:1": "BPO"})
        )
        monkeypatch.setattr(T, "_load_children_by_go_id", lambda _s, _c: {})

        with pytest.raises(ValueError, match="have no aspect"):
            T.axis_of(object(), uuid.uuid4())

    def test_the_aspect_comes_from_the_evaluation_s_own_function(self):
        """So a term's namespace here is the one the truth was classified under."""
        import inspect

        assert "_live_terms_with_aspect" in inspect.getsource(T.axis_of)


# ------------------------------------------------------------------- vectores


class TestTheVectorsAreVerified:
    def test_bytes_that_do_not_hash_to_the_declared_sha_are_refused(self):
        store = _Store({"vectors/axis.npz": b"otros bytes"})

        with pytest.raises(ArtifactMismatch):
            T.verified_vectors(store, "vectors/axis.npz", SHA)

    def test_an_absent_artefact_is_refused_and_names_the_key(self):
        """It was absent on 2026-11-08, so this is the live case and not a guard."""
        store = _Store({})

        with pytest.raises(OSError, match="vectors/axis.npz"):
            T.verified_vectors(store, "vectors/axis.npz", SHA)

    def test_verified_bytes_give_the_store_s_own_uri(self):
        """``store.url`` and not a built string: building one guesses the bucket."""
        store = _Store({"vectors/axis.npz": VECTORES})

        assert T.verified_vectors(store, "vectors/axis.npz", SHA) == (
            "s3://protea/vectors/axis.npz"
        )

    def test_it_downloads_to_check_rather_than_asking_whether_something_is_there(self):
        """``exists`` answers a different question from the one the column asks."""
        import inspect

        fuente = inspect.getsource(T.verified_vectors)

        assert "fetch_and_verify" in fuente
        assert ".exists(" not in fuente


# --------------------------------------------------------------------- tablas


def test_terms_is_one_row_per_live_term_sorted(tmp_path: Path):
    destino = tmp_path / "terms.parquet"

    assert T.write_terms(_axis(), "s3://protea/v.npz", SHA, str(destino)) == 2

    tabla = pq.read_table(destino).to_pydict()
    assert tabla["go_id"] == ["GO:1", "GO:2"]
    assert tabla["aspect"] == ["BPO", "MFO"]
    assert tabla["cut"] == [CUT, CUT]
    assert set(tabla["vector_sha256"]) == {SHA}


def test_the_closure_is_transitive_and_excludes_the_term_itself(tmp_path: Path):
    # GO:3 -> GO:2 -> GO:1, parent to child.
    axis = _axis(
        aspect={"GO:1": "BPO", "GO:2": "BPO", "GO:3": "BPO"},
        edges=(("GO:1", "GO:2"), ("GO:2", "GO:3")),
    )
    destino = tmp_path / "anc.parquet"

    filas = T.write_ancestors(axis, str(destino))

    tabla = pq.read_table(destino).to_pydict()
    pares = set(zip(tabla["go_id"], tabla["ancestor"], strict=True))
    assert pares == {("GO:2", "GO:1"), ("GO:3", "GO:2"), ("GO:3", "GO:1")}
    assert filas == 3
    assert not any(g == a for g, a in pares)


def test_a_pair_reaching_outside_the_axis_is_dropped(tmp_path: Path):
    """The loader draws from ``terms``, so an ancestor it cannot name joins to nothing.

    On this axis there are none, measured: 595.072 of 595.072 pairs have both
    ends live. The filter is what keeps that true rather than assumed.
    """
    axis = _axis(aspect={"GO:2": "BPO"}, edges=(("GO:1", "GO:2"),))
    destino = tmp_path / "anc.parquet"

    assert T.write_ancestors(axis, str(destino)) == 0


def test_the_same_axis_gives_the_same_bytes(tmp_path: Path):
    axis = _axis(
        aspect={"GO:1": "BPO", "GO:2": "BPO", "GO:3": "CCO"},
        edges=(("GO:2", "GO:3"), ("GO:1", "GO:2")),
    )
    uno, otro = tmp_path / "a.parquet", tmp_path / "b.parquet"

    T.write_ancestors(axis, str(uno))
    T.write_ancestors(
        T.Axis(cut=axis.cut, aspect=axis.aspect, edges=(("GO:1", "GO:2"), ("GO:2", "GO:3"))),
        str(otro),
    )

    assert hashlib.sha256(uno.read_bytes()).hexdigest() == (
        hashlib.sha256(otro.read_bytes()).hexdigest()
    )


# ------------------------------------------------- lo que el payload exige o no


class TestWhatTheTablesAskOfThePayload:
    def test_the_axis_tables_need_no_windows(self):
        """Demanding ids would be asking for a scope the table does not use."""
        p = ExportTransitionsDatasetPayload(
            windows=[],
            cut=CUT,
            cut_version="211",
            tables=["term_ancestors"],
        )

        assert p.windows == []
        assert AXIS_ONLY == {"terms", "term_ancestors"}

    def test_a_window_keyed_table_still_needs_them(self):
        with pytest.raises(ValueError, match="keyed by a window"):
            ExportTransitionsDatasetPayload(
                windows=[], cut=CUT, cut_version="211", tables=["queries"]
            )

    def test_mixing_the_two_needs_them(self):
        with pytest.raises(ValueError, match="keyed by a window"):
            ExportTransitionsDatasetPayload(
                windows=[], cut=CUT, cut_version="211", tables=["terms", "queries"]
            )

    def test_terms_needs_the_vectors_named(self):
        """Its two vector columns are published only after the bytes are checked."""
        with pytest.raises(ValueError, match="term_vectors_key"):
            ExportTransitionsDatasetPayload(
                windows=[], cut=CUT, cut_version="211", tables=["terms"]
            )

    def test_terms_with_the_vectors_named_is_accepted(self):
        p = ExportTransitionsDatasetPayload(
            windows=[],
            cut=CUT,
            cut_version="211",
            tables=["terms"],
            term_vectors_key="vectors/axis.npz",
            term_vectors_sha256=SHA,
        )

        assert p.term_vectors_key == "vectors/axis.npz"
