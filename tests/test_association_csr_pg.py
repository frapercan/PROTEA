"""Bit-exactness test for the CSR cooccurrence loader (--with-postgres).

Proves that the in-memory ``scipy.sparse`` CSR path
(:mod:`protea.core.operations.predict_go_terms._association_csr`) returns results
BYTE-IDENTICAL to the production per-chunk COPY/DB loader
(:mod:`protea.core.operations.predict_go_terms._association_loader`) when run
against a real Postgres, at two levels:

1. the loader return value ``(cooc_by_known, freq)`` itself, and
2. the downstream protein -> candidate ``association_*`` feature values produced
   by ``_score_association_candidates`` over the same prediction records.

The fixture stands up an EPHEMERAL pgvector container (``postgres_url``, torn
down after the test) and seeds a SMALL standalone ``term_cooccurrence`` /
``term_frequency`` fixture (a handful of rows). It never touches the live
database and never loads a large result set.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms import _association_csr as csr
from protea.core.operations.predict_go_terms import _association_loader as loader
from protea.core.operations.predict_go_terms._post_knn_pipeline import (
    _score_association_candidates,
)


def _make_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS term_cooccurrence"))
        conn.execute(text("DROP TABLE IF EXISTS term_frequency"))
        conn.execute(
            text(
                "CREATE TABLE term_cooccurrence ("
                "annotation_set_id uuid NOT NULL, "
                "known_go_id varchar(15), "
                "candidate_go_id varchar(15), "
                "cooccurrence_count integer NOT NULL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE term_frequency ("
                "annotation_set_id uuid NOT NULL, "
                "go_id varchar(15), "
                "freq integer NOT NULL)"
            )
        )


def _seed(engine, set_id: uuid.UUID, other_set: uuid.UUID) -> None:
    cooc = {
        "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7, "GO:0000101": 1},
        "GO:0000002": {"GO:0000099": 2, "GO:0000102": 5},
        "GO:0000003": {"GO:0000101": 11},
    }
    freq = {"GO:0000001": 9, "GO:0000002": 3, "GO:0000003": 4, "GO:0000004": 5}
    with engine.begin() as conn:
        for k, row in cooc.items():
            for t, c in row.items():
                conn.execute(
                    text("INSERT INTO term_cooccurrence VALUES (:sid, :k, :t, :c)"),
                    {"sid": set_id, "k": k, "t": t, "c": c},
                )
        for k, f in freq.items():
            conn.execute(
                text("INSERT INTO term_frequency VALUES (:sid, :k, :f)"),
                {"sid": set_id, "k": k, "f": f},
            )
        # Decoy under a DIFFERENT set + legacy NULL rows that must NOT leak.
        conn.execute(
            text("INSERT INTO term_cooccurrence VALUES (:sid, :k, :t, :c)"),
            {"sid": other_set, "k": "GO:0000001", "t": "GO:0000099", "c": 999},
        )
        conn.execute(
            text("INSERT INTO term_cooccurrence VALUES (:sid, NULL, NULL, :c)"),
            {"sid": set_id, "c": 123},
        )
        conn.execute(
            text("INSERT INTO term_frequency VALUES (:sid, NULL, :f)"),
            {"sid": set_id, "f": 456},
        )


@pytest.mark.integration
def test_csr_loader_bit_identical_to_copy_loader(postgres_url: str) -> None:
    """CSR ``(cooc_by_known, freq)`` == COPY/DB loader output, byte-for-byte."""
    engine = create_engine(postgres_url, future=True)
    _make_tables(engine)
    set_id = uuid.uuid4()
    other_set = uuid.uuid4()
    _seed(engine, set_id, other_set)

    known = {"GO:0000001", "GO:0000002", "GO:0000004", "GO:0000005"}

    with Session(engine, future=True) as session:
        loader.clear_cooccurrence_cache()
        copy_out = loader.load_cooccurrence_for_known(session, set_id, known)
    loader.clear_cooccurrence_cache()

    with Session(engine, future=True) as session:
        csr.clear_csr_cooccurrence_cache()
        csr_out = csr.load_cooccurrence_for_known(session, set_id, known)
    csr.clear_csr_cooccurrence_cache()

    assert csr_out == copy_out
    # And the concrete expected structure (legacy NULL + other-set decoys absent).
    assert csr_out == (
        {
            "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7, "GO:0000101": 1},
            "GO:0000002": {"GO:0000099": 2, "GO:0000102": 5},
        },
        {"GO:0000001": 9, "GO:0000002": 3, "GO:0000004": 5},
    )


@pytest.mark.integration
def test_csr_association_scores_bit_identical(postgres_url: str) -> None:
    """End-to-end association feature values match between CSR and COPY loaders.

    Runs ``_score_association_candidates`` over the SAME prediction records using
    each loader's ``(cooc_by_known, freq)`` and asserts the per-record
    ``association_total`` / ``association_cross`` / ``association_present`` floats
    are bit-identical (``repr`` equality), proving the swap is invisible to the
    scored feature.
    """
    engine = create_engine(postgres_url, future=True)
    _make_tables(engine)
    set_id = uuid.uuid4()
    _seed(engine, set_id, uuid.uuid4())

    # Two query proteins with known terms; candidates spanning both aspects.
    own_exp_go = {
        "P00001": {"GO:0000001", "GO:0000002"},
        "P00002": {"GO:0000003"},
    }
    all_known = {g for gos in own_exp_go.values() for g in gos}
    aspect_by_go = {
        "GO:0000001": "P",
        "GO:0000002": "F",
        "GO:0000003": "C",
        "GO:0000099": "F",  # cross from GO:0000001(P), same as GO:0000002(F)
        "GO:0000100": "P",
        "GO:0000101": "C",
        "GO:0000102": "P",
    }

    def _records() -> list[dict[str, object]]:
        # go_id stamped on the rec so go_id_by_int is unused; gtid distinct.
        recs: list[dict[str, object]] = []
        gid = 1
        for acc, cands in {
            "P00001": ["GO:0000099", "GO:0000100", "GO:0000101", "GO:0000102"],
            "P00002": ["GO:0000101", "GO:0000099"],
        }.items():
            for t in cands:
                recs.append(
                    {
                        "protein_accession": acc,
                        "go_term_id": gid,
                        "go_id": t,
                        "association_total": 0.0,
                        "association_cross": 0.0,
                        "association_present": 0.0,
                    }
                )
                gid += 1
        return recs

    with Session(engine, future=True) as session:
        loader.clear_cooccurrence_cache()
        cooc_copy, freq_copy = loader.load_cooccurrence_for_known(session, set_id, all_known)
    loader.clear_cooccurrence_cache()

    with Session(engine, future=True) as session:
        csr.clear_csr_cooccurrence_cache()
        cooc_csr, freq_csr = csr.load_cooccurrence_for_known(session, set_id, all_known)
    csr.clear_csr_cooccurrence_cache()

    recs_copy = _records()
    recs_csr = _records()
    _score_association_candidates(recs_copy, own_exp_go, cooc_copy, freq_copy, {}, aspect_by_go)
    _score_association_candidates(recs_csr, own_exp_go, cooc_csr, freq_csr, {}, aspect_by_go)

    # Bit-identical floats: compare repr of each feature triple per record.
    def _feat(recs: list[dict[str, object]]) -> list[tuple[str, str, str]]:
        return [
            (
                repr(r["association_total"]),
                repr(r["association_cross"]),
                repr(r["association_present"]),
            )
            for r in recs
        ]

    assert _feat(recs_csr) == _feat(recs_copy)
    # Sanity: at least one record was actually scored (non-zero total).
    assert any(r["association_present"] == 1.0 for r in recs_copy)
