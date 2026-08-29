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
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.api.routers._arm_identity import ARM_IDENTITY_COLUMNS, with_arm_identity
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
