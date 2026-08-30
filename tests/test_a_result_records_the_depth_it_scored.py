"""A result says which depth it scored, on itself.

WHY THIS TEST EXISTS. A depth cut used to leave no trace on the result. All
three reading surfaces rendered depth from ``prediction_set.limit_per_entry``,
which is the RETRIEVAL depth and is 30 for every cut of a k=30 set, so the five
point depth series run on 2026-08-30 appeared as five results at depth 30 with
different numbers and no visible reason for the difference.

The only witness was ``job.payload``, reached through
``evaluation_result.job_id``, declared ON DELETE SET NULL. Deleting a job would
have erased the one field saying what its result measured, with no foreign key
error and no other trace.

The frame seal cannot substitute for it. Depth is a LEVEL, not a frame: two
depths of one retrieval belong under one digest and differ in their level. That
is precisely why five correct results and five that had scored no cut at all
shared the digest ``f-1c245d41f26ff70c3b0a9247`` and could not be separated by
it.
"""

from __future__ import annotations

import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.api.routers._graph_panels import _LEVEL_FIELDS
from protea.api.routers._graph_reads import _Q_PANELS, _Q_PREDICTION_SETS
from protea.api.routers.stratum_proteins import _ARM
from protea.infrastructure.orm.models.annotation.evaluation_result import (
    EvaluationResult,
)


def test_the_result_carries_both_depth_units() -> None:
    columns = {c.name for c in EvaluationResult.__table__.columns}
    assert "max_sequence_rank" in columns
    assert "max_k_position" in columns


def test_the_surfaces_that_read_a_result_read_its_own_cut() -> None:
    """Not the retrieval k, which is the same for every cut of one set."""
    for name, query in (("panels", _Q_PANELS), ("stratum arm", _ARM)):
        sql = str(query)
        assert "er.max_sequence_rank" in sql, name
        assert "er.max_k_position" in sql, name


def test_a_prediction_set_still_reports_the_retrieval_depth() -> None:
    """One set is evaluated at many depths, so it has no single scored depth.

    Reading a result's cut here would be wrong in the other direction, and the
    first attempt at this change did exactly that, producing SQL referring to
    an alias the query does not join.
    """
    # Strip SQL comments first. The explanation above the column mentions the
    # result's field by name, and asserting on raw text catches the prose
    # rather than the query, which is a test that fails for the wrong reason.
    sql = "\n".join(
        line for line in str(_Q_PREDICTION_SETS).splitlines()
        if not line.strip().startswith("--")
    )
    assert "ps.limit_per_entry::text" in sql
    assert "er.max_sequence_rank" not in sql


def test_depth_is_one_of_the_fields_a_level_is_named_by() -> None:
    """Producing it is not enough; the naming has to consume it."""
    assert "depth" in _LEVEL_FIELDS


def test_the_writer_carries_the_cut_from_payload_to_row() -> None:
    """Both halves of the journey, since it now crosses two modules.

    Constructing a real result needs a staged run, a session and an artifact
    store, so this reads the two hops from source: the operation must put the
    payload's cut into the bundle, and the builder must put the bundle's cut
    onto the row. Checking only one hop passes while the other drops it.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "protea/core/operations"

    op = (root / "run_cafa_evaluation.py").read_text()
    at = op.index("ResultRow(")
    bundle = op[at : op.index(")", op.index("max_k_position", at))]
    assert "max_sequence_rank=p.max_sequence_rank" in bundle
    assert "max_k_position=p.max_k_position" in bundle

    helper = (root / "_run_cafa_helpers.py").read_text()
    at = helper.index("return EvaluationResult(")
    row = helper[at : helper.index("job_id=", at)]
    assert "max_sequence_rank=row.max_sequence_rank" in row
    assert "max_k_position=row.max_k_position" in row


def test_the_queries_are_valid_against_the_real_schema(postgres_url: str) -> None:
    """The clause has to survive contact with the schema, not just a string test.

    The first version of this change put the result's cut into a query that
    never joins evaluation_result, which reads fine and fails only when
    executed.
    """
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.execute(_Q_PANELS).fetchall()
        session.execute(_ARM, {"rid": str(uuid.uuid4())}).fetchall()
        session.execute(_Q_PREDICTION_SETS).fetchall()


def test_a_result_with_no_cut_reports_the_retrieval_depth(postgres_url: str) -> None:
    """Null in both units means the whole neighbourhood, and must not read as 0.

    The COALESCE order is what delivers this, and it is the case that covers
    every result written before this change.
    """
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        depth = session.execute(
            text(
                "SELECT COALESCE(er.max_sequence_rank::text || 'seq', "
                "er.max_k_position::text, ps.limit_per_entry::text) "
                "FROM (SELECT NULL::int AS max_sequence_rank, "
                "             NULL::int AS max_k_position) er, "
                "     (SELECT 30 AS limit_per_entry) ps"
            )
        ).scalar()
        assert depth == "30"

        cut = session.execute(
            text(
                "SELECT COALESCE(er.max_sequence_rank::text || 'seq', "
                "er.max_k_position::text, ps.limit_per_entry::text) "
                "FROM (SELECT 2::int AS max_sequence_rank, "
                "             NULL::int AS max_k_position) er, "
                "     (SELECT 30 AS limit_per_entry) ps"
            )
        ).scalar()
        # The unit is in the label. A bare "2" beside a bare "30" invites
        # reading two different units as one axis.
        assert cut == "2seq"
