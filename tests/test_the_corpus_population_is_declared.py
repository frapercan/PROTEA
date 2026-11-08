"""The corpus population: its digest, its order, and what it refuses.

WHY IT EXISTS. A vector bank is a RECIPE applied to a POPULATION, and until
this operation only the recipe had an identity. Measured on 2026-11-08: the
bank of ``f3a7fe16`` held 683.887 of the corpus's 684.048 sequences, and the
161 missing were the LONGEST in the corpus. 0,02 per cent, and nothing in the
schema could say it.

THE TWO UNITS. The corpus is 792.535 accessions over 684.048 distinct
sequences, because production's bank is one row per ACCESSION while a vector is
per SEQUENCE. The operation reports both, so a figure cannot be read in the
wrong unit, which is exactly what happened the afternoon it was written.
"""

from __future__ import annotations

import hashlib

import pytest

from protea.core.operations.declare_corpus_population import (
    POPULATION_RULE,
    _Extracted,
    _refuse_a_second_row,
    _serialise,
    population_key_for,
)
from protea.infrastructure.orm.models.query.query_set import QUERY_SET_SOURCES


# ------------------------------------------------- la serializacion y su huella


def test_the_bytes_are_one_accession_per_line_and_nothing_else():
    """Reproducible by ``sha256sum`` on a file anybody can write.

    No header and no separator ambiguity, because the digest is the only thing
    that says WHICH population a bank covered, and a format with options is a
    digest with options.
    """
    raw = _serialise(["A0A001", "P12345", "Q9XYZ0"])

    assert raw == b"A0A001\nP12345\nQ9XYZ0\n"
    assert raw.endswith(b"\n")
    assert raw.count(b"\n") == 3


def test_the_digest_is_of_those_bytes_and_of_nothing_else():
    raw = _serialise(["P00001", "P00002"])

    assert hashlib.sha256(raw).hexdigest() == hashlib.sha256(b"P00001\nP00002\n").hexdigest()


def test_the_key_is_derived_and_carries_the_digest():
    """One spelling, so a later reader never splits a uri to get at the bytes."""
    clave = population_key_for("abcdef0123456789" * 4)

    assert clave == "corpus-population/corpus-abcdef012345.txt"


def test_an_empty_population_would_be_a_digest_of_nothing():
    """The refusal is in ``execute``; this pins that empty bytes are not valid.

    ``sha256`` of an empty file is a perfectly good hexadecimal string, which
    is the whole problem: a declared set with a digest and no members reads as
    verified.
    """
    assert _serialise([]) == b"\n"
    assert hashlib.sha256(b"\n").hexdigest() != hashlib.sha256(b"").hexdigest()


# ------------------------------------------------------ el orden es por codepoint


def test_the_order_is_codepoint_and_not_this_database_s_collation():
    """The case from the module docstring, as a test rather than as prose.

    This Postgres runs ``en_US.utf8``, whose glibc collation ignores
    punctuation on a first pass and puts ``A0A18`` BEFORE ``A0A1-9``. Python's
    ``sorted`` is codepoint, where ``-`` (0x2D) precedes ``8`` (0x38). The
    operation asks the database for ``COLLATE "C"`` precisely so the two agree,
    and a digest taken under the default collation would not be reproducible by
    anybody sorting the same accessions outside Postgres.
    """
    assert sorted(["A0A18", "A0A1-9"]) == ["A0A1-9", "A0A18"]


# ------------------------------------------------------------- lo que se niega


class _Fila:
    def __init__(self, ident: str) -> None:
        self.ident = ident

    def __getitem__(self, i: int) -> str:
        return self.ident


class _Session:
    """Answers the one lookup ``_refuse_a_second_row`` makes."""

    def __init__(self, existente: str | None) -> None:
        self._existente = existente

    def execute(self, _stmt):
        fila = _Fila(self._existente) if self._existente else None

        class _R:
            def first(self_inner):
                return fila

        return _R()


def test_a_second_row_with_the_same_bytes_is_refused_here_not_by_the_constraint():
    """The constraint CANNOT catch this one, and the message has to say so.

    ``uq_query_set_window_content`` is on ``(evaluation_set_id,
    content_sha256)`` and this kind has ``evaluation_set_id`` NULL by its own
    CHECK. Two NULLs do not collide in Postgres, so that constraint would admit
    a second row for one set of bytes. A reader who assumes the database is
    guarding this would be wrong, so the refusal says where the guard is.
    """
    with pytest.raises(ValueError) as exc:
        _refuse_a_second_row(_Session("ya-existe"), "f" * 64)

    mensaje = str(exc.value)
    assert "already declared" in mensaje
    assert "NULL" in mensaje
    assert "f" * 64 in mensaje


def test_declaring_the_first_one_is_allowed():
    _refuse_a_second_row(_Session(None), "a" * 64)


# ------------------------------------------------------- el vocabulario y la regla


def test_the_kind_is_in_the_closed_vocabulary():
    assert "corpus_population" in QUERY_SET_SOURCES


def test_the_rule_is_a_predicate_and_not_a_label():
    """A description a reader can ACT on, which is what the set rule asks for.

    It has to name the filter and the order, because those are the two choices
    that decide which bytes come out, and a description saying "the corpus"
    would leave both unstated.
    """
    assert "sequence" in POPULATION_RULE
    assert 'COLLATE "C"' in POPULATION_RULE
    assert "accession" in POPULATION_RULE


# ----------------------------------------------- las dos unidades, que no son una


def test_accessions_and_sequences_are_counted_apart():
    """The trap this operation closes, in the shape it actually took.

    One sequence can carry several accessions: 54.153 of the corpus's do, over
    162.640 accessions, up to 127 on one sequence. So a population is counted
    in accessions and a coverage in sequences, and a single number quoted
    without its unit is how "the extended population" got said about a
    population that had not changed by one row.
    """
    got = _Extracted(
        accessions=["P1", "P2", "P3"],
        sequence_of={"P1": 10, "P2": 10, "P3": 11},
    )

    assert len(got.accessions) == 3
    assert len(set(got.sequence_of.values())) == 2
