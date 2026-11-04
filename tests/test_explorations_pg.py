"""On Postgres: whether an arm already had results in the window, read for real.

``launch`` marks an exploration ``post_hoc`` when an arm linked to it already
has an evaluation result in its window, because rules frozen after a number
was readable are not rules written before it. The question is one semi-join
over ``evaluation_result`` and ``prediction_set``, which SQLite cannot build.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.services import explorations_service as svc
from tests.helpers.prediction_sets import make_parents, make_prediction_set
from tests.helpers.windows import make_window

LAB = svc.Actor(name="key:lab", type="api_key")
SERVER = svc.Actor(name="key:server", type="api_key")


@pytest.fixture()
def pg(postgres_url: str) -> Any:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _launched_with_an_arm(pg: Session, *, already_scored: bool, by_config: bool) -> Any:
    window = make_window(pg)
    parents = make_parents(pg)
    arm_set = make_prediction_set(pg, parents, {})
    if already_scored:
        pg.add(EvaluationResult(evaluation_set_id=window, prediction_set_id=arm_set, results={}))
        pg.flush()
    draft = svc.Draft(
        title="t",
        question="q",
        hypothesis="h",
        kind="one_axis",
        evaluation_set_id=window,
        rules={"axis": "donor_policy"},
    )
    row = svc.create(pg, draft, LAB)
    target = (
        {"embedding_config_id": parents["embedding"]}
        if by_config
        else {"prediction_set_id": arm_set}
    )
    svc.link(pg, row, svc.LinkSpec(role="arm", arm="candidate", target=target), LAB)
    svc.launch(pg, row, SERVER)
    return row


@pytest.mark.parametrize("by_config", [False, True], ids=["by_set", "by_config"])
def test_an_arm_already_scored_in_the_window_makes_it_post_hoc(
    pg: Session, by_config: bool
) -> None:
    assert _launched_with_an_arm(pg, already_scored=True, by_config=by_config).post_hoc is True


@pytest.mark.parametrize("by_config", [False, True], ids=["by_set", "by_config"])
def test_an_arm_not_yet_scored_leaves_it_clean(pg: Session, by_config: bool) -> None:
    assert _launched_with_an_arm(pg, already_scored=False, by_config=by_config).post_hoc is False


def test_the_whole_cycle_runs_on_postgres(pg: Session) -> None:
    row = _launched_with_an_arm(pg, already_scored=False, by_config=False)
    row.state = "reported"  # the report operation is its own change
    pg.flush()
    svc.review(pg, row, SERVER)
    svc.decide(pg, row, "accepted", "the measure and its reading hold", svc.Actor("me", "user"))
    pg.commit()
    described = svc.describe(pg, row)
    assert described["state"] == "decided"
    assert [e["event"] for e in described["events"]] == [
        "created",
        "launched",
        "reviewed",
        "decision",
    ]


def _arm_pair(pg: Session, *, crossed: bool) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """Two prediction sets over one config and snapshot that differ in the bank.

    With ``crossed`` the candidate also expands votes to ancestors, which is
    the second field the comparison of 2026-08-28 moved without declaring it.
    """
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

    window = make_window(pg)
    parents = make_parents(pg)
    other_bank = AnnotationSet(
        source="goa", source_version="165", ontology_snapshot_id=parents["snapshot"]
    )
    pg.add(other_bank)
    pg.flush()
    meta = {"code_revision": "8699bfd7", "dependency_revisions": {"protea-method": "a120f613"}}
    base = make_prediction_set(pg, parents, {**meta, "expand_votes_to_ancestors": False})
    candidate = make_prediction_set(
        pg,
        {**parents, "annotation": other_bank.id},
        {**meta, "expand_votes_to_ancestors": crossed},
    )
    # The seal refuses a set that does not record its population, so both
    # arms score one query set, as a real comparison would.
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
    from protea.infrastructure.orm.models.query.query_set import QuerySet

    population = QuerySet(name="exploration-arms")
    pg.add(population)
    pg.flush()
    for pset in (base, candidate):
        pg.get(PredictionSet, pset).query_set_id = population.id
    pg.flush()
    return window, base, candidate


def _one_axis_with_arms(pg: Session, *, crossed: bool) -> Any:
    window, base, candidate = _arm_pair(pg, crossed=crossed)
    draft = svc.Draft(
        title="the bank, one axis",
        question="q",
        hypothesis="h",
        kind="one_axis",
        evaluation_set_id=window,
        rules={"axis": "annotation_set_id"},
    )
    row = svc.create(pg, draft, LAB)
    svc.link(pg, row, svc.LinkSpec(role="arm", arm="base", target={"prediction_set_id": base}), LAB)
    svc.link(
        pg,
        row,
        svc.LinkSpec(role="arm", arm="candidate", target={"prediction_set_id": candidate}),
        LAB,
    )
    return row


def test_the_receipt_reads_the_arms_for_real(pg: Session) -> None:
    from protea.services.exploration_receipt import compute

    row = _one_axis_with_arms(pg, crossed=False)
    layers = compute(pg, row)["layers"]
    assert layers["method"] == {"status": "one axis", "axis": ["annotation_set_id"]}
    assert layers["recipe"]["status"] == "reproduced"
    assert layers["recipe"]["differs_in"] == []
    assert layers["population"]["status"] == "not recorded"


def test_arms_that_cross_the_axis_cannot_be_launched(pg: Session) -> None:
    row = _one_axis_with_arms(pg, crossed=True)
    with pytest.raises(svc.ExplorationError, match="declared axis"):
        svc.launch(pg, row, SERVER)


def test_the_report_is_built_from_real_rows_and_blocks_an_undeclared_population(
    pg: Session,
) -> None:
    # Two clean arms over one axis, but neither declares the corpus membership
    # of its bank, which is the leak D55's checklist exists to block.
    row = _one_axis_with_arms(pg, crossed=False)
    svc.launch(pg, row, SERVER)
    svc.report(pg, row, SERVER)
    body = svc.describe(pg, row)["events"][-1]["body"]
    items = body["report"]["checklist"]["items"]
    assert items["one_axis"]["answer"] == "pass"
    assert items["no_leak"]["answer"] == "fail"
    assert any("membership is not declared" in p for p in items["no_leak"]["problems"])
    assert (row.state, row.verdict, row.grade) == ("reported", "blocked", "exploratory")
    assert len(body["sha256"]) == 64
