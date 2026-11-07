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


def _set_with_rows(pg: Session, parents: dict[str, Any], rows: dict[tuple[str, int], float]) -> Any:
    from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

    pset = make_prediction_set(pg, parents, {})
    for (accession, term), distance in rows.items():
        pg.add(
            GOPrediction(
                prediction_set_id=pset,
                protein_accession=accession,
                go_term_id=term,
                ref_protein_accession="R1",
                distance=distance,
            )
        )
    pg.flush()
    return pset


def _terms(pg: Session, parents: dict[str, Any], n: int) -> list[Any]:
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm

    terms = [
        GOTerm(go_id=f"GO:000000{i}", ontology_snapshot_id=parents["snapshot"]) for i in range(n)
    ]
    pg.add_all(terms)
    pg.flush()
    return terms


def test_row_identity_sees_one_double_move(pg: Session) -> None:
    from protea.services.exploration_identity import row_identity

    parents = make_parents(pg)
    terms = _terms(pg, parents, 3)
    rows = {("P1", terms[0].id): 0.1, ("P1", terms[1].id): 0.2, ("P2", terms[2].id): 0.3}
    a = _set_with_rows(pg, parents, rows)
    b = _set_with_rows(pg, parents, rows)
    assert row_identity(pg, str(a), str(b)) == {
        "identical": True,
        "rows": {"a": 3, "b": 3},
        "first_difference": None,
    }
    # One distance one unit in the last place away: the same panels, a different run.
    c = _set_with_rows(pg, parents, {**rows, ("P2", terms[2].id): 0.30000000000000004})
    moved = row_identity(pg, str(a), str(c))
    assert moved["identical"] is False
    assert moved["first_difference"]["protein_accession"] == "P2"
    assert moved["first_difference"]["go_term_id"] == terms[2].id
    assert moved["first_difference"]["a"] != moved["first_difference"]["b"]
    assert moved["first_difference"]["a_exact"] != moved["first_difference"]["b_exact"]


def test_the_same_double_is_decided_by_its_bytes_not_by_a_session_setting(pg: Session) -> None:
    # With extra_float_digits at 0 both doubles print as "0.3"; their bytes
    # still differ, and the identity claim must not move with a GUC.
    from sqlalchemy import text as sql_text

    from protea.services.exploration_identity import row_identity

    parents = make_parents(pg)
    terms = _terms(pg, parents, 1)
    a = _set_with_rows(pg, parents, {("P1", terms[0].id): 0.3})
    b = _set_with_rows(pg, parents, {("P1", terms[0].id): 0.30000000000000004})
    pg.execute(sql_text("SET extra_float_digits = 0"))
    moved = row_identity(pg, str(a), str(b))
    assert moved["identical"] is False
    assert moved["first_difference"]["a"] == moved["first_difference"]["b"]  # same text
    assert moved["first_difference"]["a_exact"] != moved["first_difference"]["b_exact"]


def test_two_proteins_that_swap_their_predictions_are_not_identical(pg: Session) -> None:
    # The shape a linear sum of row hashes is blind to, and the one that
    # matters: retrieval handing one protein's predictions to another.
    from protea.services.exploration_identity import row_identity

    parents = make_parents(pg)
    terms = _terms(pg, parents, 2)
    a = _set_with_rows(pg, parents, {("P1", terms[0].id): 0.1, ("P2", terms[1].id): 0.2})
    b = _set_with_rows(pg, parents, {("P1", terms[1].id): 0.2, ("P2", terms[0].id): 0.1})
    swapped = row_identity(pg, str(a), str(b))
    assert swapped["identical"] is False
    assert swapped["rows"] == {"a": 2, "b": 2}
    assert swapped["first_difference"]["protein_accession"] == "P1"
    # The lowest key, P1 with the first term, exists only in the first set.
    assert swapped["first_difference"]["b"] is None


def _job_with_loads(pg: Session, sources: list[str]) -> Any:
    from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus

    job = Job(
        operation="predict_go_terms", queue_name="protea.predictions", status=JobStatus.SUCCEEDED
    )
    pg.add(job)
    pg.flush()
    for source in sources:
        pg.add(
            JobEvent(
                job_id=job.id,
                event="child.predict_go_terms_batch.load_references_done",
                fields={"source": source, "host": "node"},
            )
        )
    pg.flush()
    return job.id


def test_an_instrument_with_a_cold_and_a_warm_run_is_not_read(pg: Session) -> None:
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

    window = make_window(pg)
    parents = make_parents(pg)
    terms = _terms(pg, parents, 1)
    first = _set_with_rows(pg, parents, {("P1", terms[0].id): 0.1})
    second = _set_with_rows(pg, parents, {("P1", terms[0].id): 0.1})
    for pset, source in ((first, "database"), (second, "disk_cache")):
        pg.get(PredictionSet, pset).meta = {"job_id": str(_job_with_loads(pg, [source]))}
    pg.flush()
    draft = svc.Draft(
        title="the instrument repeats",
        question="q",
        hypothesis="h",
        kind="instrument",
        evaluation_set_id=window,
        rules={"bank_load": "database"},
    )
    row = svc.create(pg, draft, LAB)
    svc.link(
        pg, row, svc.LinkSpec(role="arm", arm="first", target={"prediction_set_id": first}), LAB
    )
    svc.link(
        pg, row, svc.LinkSpec(role="arm", arm="second", target={"prediction_set_id": second}), LAB
    )
    svc.launch(pg, row, SERVER)
    svc.report(pg, row, SERVER)
    body = svc.describe(pg, row)["events"][-1]["body"]["report"]
    # The rows are identical, and still no verdict: the second bank came warm.
    assert body["identity"]["identical"] is True
    assert row.verdict == "blocked"
    assert any("different sources" in b for b in body["blocks_verdict"])


def _instrument_of(pg: Session, window: Any, arms: dict[str, Any], source: str = "database") -> Any:
    """An instrument exploration with one arm per prediction set, all loaded the same way."""
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

    for pset in arms.values():
        pg.get(PredictionSet, pset).meta = {"job_id": str(_job_with_loads(pg, [source]))}
    pg.flush()
    draft = svc.Draft(
        title="the instrument repeats",
        question="q",
        hypothesis="h",
        kind="instrument",
        evaluation_set_id=window,
        rules={"bank_load": source},
    )
    row = svc.create(pg, draft, LAB)
    for arm, pset in arms.items():
        svc.link(
            pg, row, svc.LinkSpec(role="arm", arm=arm, target={"prediction_set_id": pset}), LAB
        )
    svc.launch(pg, row, SERVER)
    svc.report(pg, row, SERVER)
    return row, svc.describe(pg, row)["events"][-1]["body"]["report"]


def test_an_instrument_reads_four_arms_and_not_only_two(pg: Session) -> None:
    # Exploration 3 ran four arms on one revision and the report refused them,
    # so its reading had to be written by hand as an event.
    window = make_window(pg)
    parents = make_parents(pg)
    terms = _terms(pg, parents, 1)
    rows = {("P1", terms[0].id): 0.1}
    arms = {name: _set_with_rows(pg, parents, rows) for name in ("a", "b", "c", "d")}

    row, body = _instrument_of(pg, window, arms)

    identity = body["identity"]
    assert identity["identical"] is True
    assert identity["arms"] == ["a", "b", "c", "d"]
    assert identity["reference"] == "a"
    # N-1 comparisons against the reference, not N(N-1)/2: equality is transitive.
    assert sorted(identity["pairs"]) == ["b", "c", "d"]
    assert identity["rows"] == {"a": 1, "b": 1, "c": 1, "d": 1}
    assert identity["first_difference"] is None
    assert row.verdict == "for"


def test_an_instrument_names_which_of_four_arms_moved(pg: Session) -> None:
    window = make_window(pg)
    parents = make_parents(pg)
    terms = _terms(pg, parents, 1)
    same = {("P1", terms[0].id): 0.1}
    arms = {
        "a": _set_with_rows(pg, parents, same),
        "b": _set_with_rows(pg, parents, same),
        "c": _set_with_rows(pg, parents, {("P1", terms[0].id): 0.2}),
        "d": _set_with_rows(pg, parents, same),
    }

    row, body = _instrument_of(pg, window, arms)

    identity = body["identity"]
    assert identity["identical"] is False
    assert identity["pairs"]["c"]["identical"] is False
    assert identity["pairs"]["b"]["identical"] is True
    assert identity["pairs"]["d"]["identical"] is True
    # The conclusion alone would not say which arm to go and look at.
    assert identity["pairs"]["c"]["first_difference"]["protein_accession"] == "P1"
    assert row.verdict == "against"


def test_one_arm_is_not_an_instrument(pg: Session) -> None:
    window = make_window(pg)
    parents = make_parents(pg)
    terms = _terms(pg, parents, 1)
    arms = {"a": _set_with_rows(pg, parents, {("P1", terms[0].id): 0.1})}

    row, body = _instrument_of(pg, window, arms)

    assert body["identity"]["identical"] is None
    assert "at least two runs" in body["identity"]["detail"]
    assert row.verdict is None
