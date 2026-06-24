"""Integration test for the parameterized COPY association loader (--with-postgres).

``_association_loader._load_into_cache`` bulk-reads ``term_cooccurrence`` /
``term_frequency`` with ``COPY (SELECT ... WHERE known_go_id = ANY(%s)) TO
STDOUT``, binding the known go_id list as a psycopg3 array PARAMETER instead of
inlining a quoted IN-list literal. The unit tests in
``test_cooccurrence_loader_cache`` use a fake connection; this test proves the
real psycopg3 / Postgres path actually accepts the bound COPY parameters and
returns rows byte-identical to a plain ``SELECT`` reference over the same data.

The fixture stands up an EPHEMERAL pgvector container (``postgres_url``, torn
down after the test) and creates minimal standalone tables with exactly the
columns the loader SELECTs. It never touches the live database.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms import _association_loader as loader


def _reference_load(
    session: Session,
    annotation_set_id: uuid.UUID,
    known_go_ids: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int]]:
    """No-cache oracle: a plain parameterized SELECT, mirroring the loader's WHERE."""
    cooc_by_known: dict[str, dict[str, int]] = {}
    freq: dict[str, int] = {}
    if not known_go_ids:
        return cooc_by_known, freq
    kl = list(known_go_ids)
    cooc_rows = session.execute(
        text(
            "SELECT known_go_id, candidate_go_id, cooccurrence_count "
            "FROM term_cooccurrence "
            "WHERE annotation_set_id = :sid AND known_go_id = ANY(:kl) "
            "AND known_go_id IS NOT NULL AND candidate_go_id IS NOT NULL"
        ),
        {"sid": annotation_set_id, "kl": kl},
    ).fetchall()
    for known_go, candidate_go, count in cooc_rows:
        cooc_by_known.setdefault(known_go, {})[candidate_go] = int(count)
    freq_rows = session.execute(
        text(
            "SELECT go_id, freq FROM term_frequency "
            "WHERE annotation_set_id = :sid AND go_id = ANY(:kl) AND go_id IS NOT NULL"
        ),
        {"sid": annotation_set_id, "kl": kl},
    ).fetchall()
    for go_id, f in freq_rows:
        freq[go_id] = int(f)
    return cooc_by_known, freq


def _make_tables(engine) -> None:
    """Minimal standalone tables with just the loader-SELECTed columns.

    The production tables carry FKs to ``annotation_set`` / ``go_term``; the
    loader never joins them, so a flat table with the same name + columns is a
    faithful stand-in that isolates the COPY-parameter behaviour under test.
    """
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


@pytest.mark.integration
def test_copy_any_param_matches_select_reference(postgres_url: str) -> None:
    """The ANY(%s)-bound COPY load is byte-identical to a plain SELECT oracle."""
    engine = create_engine(postgres_url, future=True)
    _make_tables(engine)

    set_id = uuid.uuid4()
    other_set = uuid.uuid4()  # rows that must NOT leak into the result
    cooc = {
        "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7},
        "GO:0000002": {"GO:0000099": 2},
        "GO:0000003": {"GO:0000101": 11},
    }
    freq = {"GO:0000001": 9, "GO:0000002": 3, "GO:0000004": 5}

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
        # Decoys under a DIFFERENT annotation set + a legacy NULL go_id row.
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

    known = {"GO:0000001", "GO:0000002", "GO:0000004", "GO:0000005"}

    with Session(engine, future=True) as session:
        expected = _reference_load(session, set_id, known)

    with Session(engine, future=True) as session:
        loader.clear_cooccurrence_cache()
        got = loader.load_cooccurrence_for_known(session, set_id, known)

    loader.clear_cooccurrence_cache()

    # Byte-identical to the plain-SELECT oracle: same cooc map, same freq map.
    assert got == expected
    assert got == (
        {
            "GO:0000001": {"GO:0000099": 4, "GO:0000100": 7},
            "GO:0000002": {"GO:0000099": 2},
        },
        {"GO:0000001": 9, "GO:0000002": 3, "GO:0000004": 5},
    )


@pytest.mark.integration
def test_copy_any_param_large_list_no_query_parse_blowup(postgres_url: str) -> None:
    """A large known list binds as ONE array param (the perf motivation).

    The prior path inlined every go_id into a quoted IN-list, so a big known set
    produced a megabyte-class SQL string re-parsed server-side every call. The
    ANY(%s) form sends the list as a single bound array; this exercises that path
    with a 2000-element list and asserts correctness against the SELECT oracle.
    """
    engine = create_engine(postgres_url, future=True)
    _make_tables(engine)

    set_id = uuid.uuid4()
    known_ids = [f"GO:{i:07d}" for i in range(1, 2001)]
    # Populate cooc/freq for a sparse subset.
    with engine.begin() as conn:
        for i in range(1, 2001, 7):
            k = f"GO:{i:07d}"
            conn.execute(
                text("INSERT INTO term_cooccurrence VALUES (:sid, :k, :t, :c)"),
                {"sid": set_id, "k": k, "t": f"GO:{i + 5000:07d}", "c": i},
            )
            conn.execute(
                text("INSERT INTO term_frequency VALUES (:sid, :k, :f)"),
                {"sid": set_id, "k": k, "f": i * 2},
            )

    known = set(known_ids)
    with Session(engine, future=True) as session:
        expected = _reference_load(session, set_id, known)
    with Session(engine, future=True) as session:
        loader.clear_cooccurrence_cache()
        got = loader.load_cooccurrence_for_known(session, set_id, known)
    loader.clear_cooccurrence_cache()

    assert got == expected
    assert got[1]  # non-empty freq map proves rows were actually matched
