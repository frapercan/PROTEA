"""On Postgres: whether an arm already had results in the window, read for real.

``launch`` marks an exploration ``post_hoc`` when an arm linked to it already
has an evaluation result in its window, because rules frozen after a number
was readable are not rules written before it. The question is one semi-join
over ``evaluation_result`` and ``prediction_set``, which SQLite cannot build.
"""

from __future__ import annotations

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
