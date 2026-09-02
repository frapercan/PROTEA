"""The three surfaces that name an arm name it by the same fields.

WHY THIS TEST EXISTS. The graph panels, the strata list and the per-protein
stratum view each carried their own copy of the expression that turns a stored
donor policy into a level name. Three copies of one rule is how a field comes
to be added to two of them, which is the same shape as the KNN step that gained
a self-exclusion on one path and not the other, and it fails in the direction
that does not raise: an arm renders under a name that omits what it varied in,
and a reader compares two experiments believing they are one.

So the fields live in one fragment and this test asserts all three read it,
that the fragment distinguishes what it claims to, and that no surface has
grown a private copy again.

THE SAME SHAPE CAUGHT DEPTH. Two of the three surfaces read a RESULT and so
can report a depth, and both carried their own copy of a COALESCE that folded
three different quantities into one column: the retrieval depth on the
prediction set, and the sequence and k-position cuts taken at evaluation time
over a list that was already retrieved and scored. Only the sequence cut was
marked, so a cut at k-position 10 and a retrieval depth of 10 rendered as the
same string. The fragment now names the quantity, and the tests below pin both
halves: that the three render as three, and that a comparison refuses to
ladder one against another.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.api.routers._arm_identity import (
    ARM_IDENTITY_COLUMNS,
    DEPTH_IDENTITY_COLUMN,
    EVALUATION_CUT,
    RETRIEVAL_DEPTH,
    UNRECORDED_DEPTH,
    depth_kind,
    with_arm_identity,
)
from protea.api.routers._graph_panels import _LEVEL_FIELDS
from protea.api.routers._graph_reads import _Q_PANELS
from protea.api.routers.strata import _ARMS
from protea.api.routers.stratum_proteins import _ARM
from tests.helpers.prediction_sets import make_parents, make_prediction_set

#: Every name the shared fragment produces.
_IDENTITY_ALIASES = ("donor_policy", "self_exclusion", "features", "code_revision")

_SURFACES = {
    "graph panels": _Q_PANELS,
    "strata list": _ARMS,
    "stratum proteins": _ARM,
}

#: The surfaces that read an evaluation result and can therefore say which
#: depth a number was read at. The strata list is absent on purpose: it names
#: the retrieval K under the name ``k`` and makes no claim about a cut, so
#: there is nothing there to tell apart.
_DEPTH_SURFACES = {
    "graph panels": _Q_PANELS,
    "stratum proteins": _ARM,
}


def test_every_surface_reads_the_shared_fragment() -> None:
    for name, query in _SURFACES.items():
        sql = str(query)
        for alias in _IDENTITY_ALIASES:
            assert f"AS {alias}" in sql, f"{name} does not name an arm by {alias}"
        assert "{ARM_IDENTITY" not in sql, f"{name} left the fragment unsubstituted"


def test_no_surface_keeps_a_private_copy_of_the_rule() -> None:
    """One occurrence each, and it came from the fragment.

    A surface that reintroduced its own CASE would still satisfy the test
    above, and would drift the moment the rule changes.
    """
    for name, query in _SURFACES.items():
        sql = str(query)
        assert sql.count("AS donor_policy") == 1, name
        assert ARM_IDENTITY_COLUMNS.strip() in sql, f"{name} does not use the fragment"


def test_every_surface_that_reports_a_depth_reads_the_shared_fragment() -> None:
    for name, query in _DEPTH_SURFACES.items():
        sql = str(query)
        assert "AS depth" in sql, f"{name} does not report a depth at all"
        assert DEPTH_IDENTITY_COLUMN.strip() in sql, f"{name} does not use the fragment"
        assert "{DEPTH_IDENTITY" not in sql, f"{name} left the fragment unsubstituted"


def test_no_surface_folds_the_three_depths_into_one_column() -> None:
    """The private copy that caused this, in the shape it had.

    A COALESCE over the three columns produces a name for each of them and a
    mark for at most one, which is how a k-position cut of 10 and a retrieval
    depth of 10 came to be the same level. Asserting on the shape rather than
    on a rendered value because the shape is what a surface reintroduces when
    it decides it needs its own depth column.
    """
    for name, query in _SURFACES.items():
        sql = str(query)
        assert "COALESCE(er.max_sequence_rank" not in sql, name
        assert sql.count("AS depth") <= 1, name


@pytest.mark.parametrize(
    ("rendered", "kind"),
    [
        ("retrieval depth 30", RETRIEVAL_DEPTH),
        ("cut at sequence rank 30", EVALUATION_CUT),
        ("cut at protein rank 10", EVALUATION_CUT),
        ("unrecorded", UNRECORDED_DEPTH),
        # What the column used to hold, and what fixtures written against it
        # still pass. A bare integer says a number and not which quantity the
        # number is of, so it is placed on no axis rather than on the likelier
        # one.
        ("10", UNRECORDED_DEPTH),
        (None, UNRECORDED_DEPTH),
    ],
)
def test_a_rendered_depth_says_which_quantity_it_is(rendered: str | None, kind: str) -> None:
    assert depth_kind(rendered) == kind


def test_the_depth_fragment_separates_the_three_quantities(postgres_url: str) -> None:
    """Three quantities, three names, against real Postgres.

    The four cases the record actually contains. The pair that has to separate
    is the last two: prediction set d5b634b2 was retrieved at depth 10 and
    evaluated both uncut and cut at k-position 10, and until this fragment
    those sixteen results shared one level name while being readings of two
    different things.

    Driven off literals rather than off stored rows because the fragment reads
    four columns and nothing else, and building an evaluation result to carry
    them would test the ORM's defaults rather than the CASE.
    """
    engine = create_engine(postgres_url, future=True)
    query = text(
        "SELECT er.label AS label,\n" + DEPTH_IDENTITY_COLUMN + "\n"
        "FROM (VALUES\n"
        "    ('sequence cut', 30::int, NULL::int, 30::int),\n"
        "    ('k-position cut', NULL, 10, 10),\n"
        "    ('uncut', NULL, NULL, 10),\n"
        "    ('no retrieval recorded', NULL, NULL, NULL)\n"
        ") AS er(label, max_sequence_rank, max_k_position, limit_per_entry)\n"
        "CROSS JOIN LATERAL (SELECT er.limit_per_entry AS limit_per_entry) AS ps"
    )
    with Session(engine) as session:
        rendered = {r["label"]: r["depth"] for r in session.execute(query).mappings()}

    assert rendered == {
        "sequence cut": "cut at sequence rank 30",
        "k-position cut": "cut at protein rank 10",
        "uncut": "retrieval depth 10",
        "no retrieval recorded": "unrecorded",
    }
    # The collision the record holds today, gone in both directions: the two
    # names differ, and each says which quantity it is.
    assert rendered["k-position cut"] != rendered["uncut"]
    assert depth_kind(rendered["k-position cut"]) == EVALUATION_CUT
    assert depth_kind(rendered["uncut"]) == RETRIEVAL_DEPTH
    assert len(set(rendered.values())) == 4


def test_the_fragment_is_one_of_the_fields_a_level_is_named_by() -> None:
    """Producing the column is not enough; the naming has to consume it."""
    for alias in _IDENTITY_ALIASES:
        assert alias in _LEVEL_FIELDS, f"{alias} is produced and never named by"


def test_a_query_that_does_not_ask_for_the_columns_is_refused() -> None:
    with pytest.raises(ValueError):
        with_arm_identity("SELECT 1")


def test_the_fragment_separates_the_regimes_it_claims_to(postgres_url: str) -> None:
    """The three cases the record actually contains, against real Postgres.

    ``8a75f84e`` and a corrected run store byte-identical donor policies while
    having gated different things. They must not render as one level, and the
    only field that separates them is the revision.
    """
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    gated = {"evidence_codes": ["EXP", "IDA"]}
    regimes = {
        "old permissive": {"donor_policy": {}, "features": ["compute_alignments"]},
        "old gated": {"donor_policy": gated, "features": ["compute_alignments"]},
        "corrected": {
            "donor_policy": gated,
            "exclude_self_neighbour": True,
            "features": ["compute_alignments", "compute_taxonomy"],
            "code_revision": "a" * 40,
        },
    }

    with Session(engine) as session:
        parents = make_parents(session)
        ids = {
            name: make_prediction_set(session, parents, meta)
            for name, meta in regimes.items()
        }
        rows = _identity_rows(session, ids)

        assert len({tuple(r) for r in rows.values()}) == 3, rows
        # The pair the campaign actually has to separate.
        assert rows["old gated"][0] == rows["corrected"][0]
        assert rows["old gated"] != rows["corrected"]
        assert rows["corrected"][3] == "aaaaaaa"
        assert rows["old gated"][3] == "unrecorded"
        # False and absent are different statements about the retriever.
        assert rows["old gated"][1] == "self-unrecorded"
        assert rows["corrected"][1] == "self-excluded"
        assert rows["corrected"][2] == "alignments+taxonomy"

        session.rollback()


def _identity_rows(
    session: Session, ids: dict[str, uuid.UUID]
) -> dict[str, tuple[str, ...]]:
    query = text(
        f"SELECT ps.id::text AS sid,\n{ARM_IDENTITY_COLUMNS}\n"
        "FROM prediction_set ps WHERE ps.id = ANY(:ids)"
    )
    by_id = {str(v): k for k, v in ids.items()}
    out: dict[str, tuple[str, ...]] = {}
    for row in session.execute(query, {"ids": list(ids.values())}).mappings():
        out[by_id[row["sid"]]] = tuple(row[a] for a in _IDENTITY_ALIASES)
    return out
