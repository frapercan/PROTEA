"""Tests for the experiment graph at ``GET /v1/graph``.

Three properties are pinned here, and all three are about the surface refusing
to overstate what the record holds. They exist because the endpoint this one
replaces failed on exactly these points: it reported a full grid of successes
from a table whose evidence had been deleted.

1. A node with a single instantiated level can never come out ``measured``. One
   level means no contrast, and no contrast means no separation, whatever the
   numbers attached to it look like.
2. A blocked node always carries a reason, both on the node and in the top-level
   blocked list, and the two agree because they are built from one source.
3. The endpoint writes nothing. Checked three ways: every statement it can issue
   is a SELECT, the session handed to it fails loudly on every write method, and
   the modules never reach for the committing session helper.

No test here opens a database, with one exception. Whether a declared floor is
still standing is a claim about SQL, and a string assertion on a query cannot
tell a predicate that filters from one that matches nothing, so
``TestAWithdrawnDeclarationGovernsNothing`` runs that one statement against the
suite's own Postgres inside a transaction it rolls back. It is marked
``integration`` and takes ``conftest``'s ``postgres_url``, which is what the
integration workflow's ``pytest --with-postgres`` fills; keyed on its own env
var instead, both halves of that pair skipped in every workflow and said
nothing. Everywhere else the session is a fake that answers the endpoint's
statements by identity, which also means a statement the endpoint did not
declare up front cannot be answered at all.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from protea.api.routers._graph_edges import (
    BLOCKED,
    CHOSEN,
    INHERITED,
    MEASURED,
    SPECS,
    UNPOWERED,
    Edge,
    strength_of,
)
from protea.api.routers._graph_panels import (
    CrossedFrames,
    PANEL_KEYS,
    CrossedDepthAxes,
    build_panels,
    panel_units_from_groundtruth,
    separated_from_floor,
)
from protea.api.routers._graph_reads import (
    _PIVOT_ASPECTS,
    _Q_FLOORS,
    PARAM_QUERIES,
    QUERIES,
    UnclassifiedRunStatus,
    read_record,
    standing_floor_statuses,
)
from protea.api.routers.graph import build_graph, router
from protea.infrastructure.orm.models.experiment_run import ExperimentRun, ExperimentRunStatus
from protea.infrastructure.settings import load_settings

_STRENGTHS = {MEASURED, CHOSEN, INHERITED, UNPOWERED, BLOCKED}


# ── A fake session that cannot be written to ──────────────────────────────────


class WriteAttempted(AssertionError):
    """Raised the moment the endpoint touches any write path on the session."""


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeResult:
        return self

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    """Answers the declared statements and refuses everything else.

    Lookup is by object identity, not by SQL text: a statement the endpoint did
    not declare in ``QUERIES`` has no answer here and raises, so the read
    surface stays exactly as wide as the module says it is.
    """

    def __init__(self, record: dict[str, list[dict[str, Any]]]) -> None:
        self._by_id = {id(QUERIES[name]): rows for name, rows in record.items()}
        # The parameterised reads answer empty unless the record names them. A
        # statement outside both registries still raises, which is the point.
        for name, clause in PARAM_QUERIES.items():
            self._by_id.setdefault(id(clause), record.get(name, []))
        # The pivot aspect lookup is issued outside read_record, by the endpoint
        # itself, so it is not in QUERIES and would read as undeclared. It is
        # declared here explicitly rather than by relaxing the guard: a guard
        # that stops naming what it allows stops being one.
        self._by_id[id(_PIVOT_ASPECTS)] = record.get("pivot_aspects", [])
        self.statements: list[Any] = []

    def execute(self, clause: Any, *args: Any, **kwargs: Any) -> _FakeResult:
        rows = self._by_id.get(id(clause))
        if rows is None:
            raise AssertionError(f"undeclared statement issued: {clause}")
        self.statements.append(clause)
        return _FakeResult(rows)

    def __enter__(self) -> FakeSession:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def _write(self, *args: Any, **kwargs: Any) -> None:
        raise WriteAttempted("the graph endpoint attempted a write")

    add = add_all = delete = merge = flush = commit = _write


def _ground_truth_bytes(rows: list[tuple[str, str, str]]) -> bytes:
    """A ground-truth artefact in memory, in the shape the window writes."""
    import io

    import pandas as pd

    frame = pd.DataFrame(rows, columns=["protein_accession", "go_id", "bucket"])
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def empty_record() -> dict[str, list[dict[str, Any]]]:
    """A record in which every table is empty. The shape, with nothing in it."""
    return {name: [] for name in QUERIES}


def _client(record: dict[str, list[dict[str, Any]]]) -> tuple[TestClient, FakeSession]:
    session = FakeSession(record)
    app = FastAPI()
    app.state.session_factory = lambda: session
    # The endpoint reads the window's ground-truth artefact to count the panel
    # populations, so it asks for settings. Set here rather than mocked away: a
    # test that skips the dependency stops exercising the path that resolves the
    # artefact store, which is where an unreadable artefact has to turn into an
    # absent population instead of an exception.
    app.state.settings = load_settings(Path(__file__).resolve().parents[1])
    app.include_router(router, prefix="/v1")
    return TestClient(app), session


# ── Fixtures shaped like the record ───────────────────────────────────────────


def _panel_rows(levels: dict[str, float]) -> list[dict[str, Any]]:
    """One scored row per (level, panel), all nine panels, one shared cohort.

    And one shared frame seal. A separation is asked inside a seal, so a fixture
    without one is asking a question that cannot be answered rather than one
    whose answer happens to be no.
    """
    return [
        {
            "result_id": f"r-{name}",
            "frame_digest": "f-one-frame",
            "scoring_name": name,
            "embedding_name": "esm2_650m",
            "depth": "10",
            "category": category,
            "aspect": aspect,
            "f_micro_w": value,
            "tau": 0.5,
            "n_at_tau": 950,
            "coverage_at_tau": 0.95,
        }
        for name, value in levels.items()
        for category, aspect in PANEL_KEYS
    ]


def populated_record(levels: dict[str, float]) -> dict[str, list[dict[str, Any]]]:
    """A record with one frame, one substrate, one bank and ``levels`` scorers."""
    record = empty_record()
    record["evaluation_sets"] = [
        {
            "id": "eval-1",
            "window_role": "valid",
            "mode": "reconciled",
            "pivot_snapshot_id": "pivot-1",
            "pivot_version": "releases/2025-07-22",
            "window_from": "220",
            "window_to": "227",
            "bank_annotation_set_id": "bank-1",
        }
    ]
    record["accretion"] = [
        {
            "id": "ia-1",
            "ontology_snapshot_id": "pivot-1",
            "regime": "lafa",
            "sha256": "abc",
            "in_use": True,
        }
    ]
    record["query_sets"] = [{"id": "qs-1", "name": "targets", "entries": 14032, "in_use": True}]
    record["substrates"] = [
        {
            "id": "ec-1",
            "model_name": "esm2",
            "label": "esm2_650m",
            "in_use": True,
            "producible": True,
        },
        {
            "id": "ec-2",
            "model_name": "ankh",
            "label": "ankh_base",
            "in_use": False,
            "producible": True,
        },
    ]
    record["banks"] = [{"id": "bank-1", "source": "goa", "source_version": "220", "in_use": True}]
    record["prediction_sets"] = [
        {
            "id": "ps-1",
            "embedding_config_id": "ec-1",
            "annotation_set_id": "bank-1",
            "bank_source": "goa",
            "bank_version": "220",
            "depth": "10",
            "distance_threshold": None,
            "metric": "cosine",
            "search_backend": "numpy",
            "aspect_separated": "true",
            "expand_to_ancestors": "false",
            "donor_reviewed_only": "false",
            "donor_evidence_codes": None,
            "donor_exclusions": "[]",
            "features": "compute_alignments, compute_taxonomy",
        }
    ]
    record["scoring"] = [
        {
            "id": f"sc-{name}",
            "name": name,
            "formula": "linear",
            "weights": f'{{"w": {value}}}',
            "evidence_weights": "{}",
            "params": "{}",
            "results": 1,
        }
        for name, value in levels.items()
    ]
    record["results"] = [
        {
            "id": f"r-{name}",
            "evaluation_set_id": "eval-1",
            "prediction_set_id": "ps-1",
            "scoring_config_id": f"sc-{name}",
            "reranker_model_id": None,
            "frame": None,
            "embedding_config_id": "ec-1",
            "annotation_set_id": "bank-1",
        }
        for name in levels
    ]
    record["panels"] = _panel_rows(levels)
    record["candidates"] = [{"prediction_set_id": "ps-1", "candidates": 752786}]
    record["donor_column"] = [{"is_nullable": "NO"}]
    record["artifacts"] = [
        {
            "reranker_model": 0,
            "interpro_annotation": 0,
            "interpro_go_mapping": 0,
            "reranked_results": 0,
        }
    ]
    return record


# ── One level is never a measurement ──────────────────────────────────────────


@pytest.mark.parametrize("scored", [0, 1, 2, 8])
@pytest.mark.parametrize("results", [0, 1, 8])
@pytest.mark.parametrize("separated", [None, False, True])
def test_a_single_level_is_never_measured(
    scored: int, results: int, separated: bool | None
) -> None:
    """One level means no contrast, so nothing about it can be a separation.

    Swept over every combination of scored levels, surviving results and claimed
    separation, including a floor that says the comparison won: the single-level
    branch is reached first and no combination gets past it.
    """
    for forced in (True, False):
        edge = Edge(
            instantiated=1,
            available=13,
            scored=scored,
            results=results,
            forced=forced,
            floor="a-floor",
            separated=separated,
        )
        assert strength_of(edge) != MEASURED
        assert strength_of(edge) == (CHOSEN if forced else INHERITED)


def test_a_single_scored_level_reports_inherited_through_the_endpoint() -> None:
    """The same rule holds end to end, not only on the helper.

    The record here has one weighting with a surviving result and nine scored
    panels. A surface that ranked levels without asking how many there were
    would happily call that a winner.
    """
    client, _ = _client(populated_record({"embedding_only": 0.31}))
    nodes = {n["key"]: n for n in client.get("/v1/graph").json()["nodes"]}
    assert nodes["scoring"]["levels_instantiated"] == 1
    assert nodes["scoring"]["strength"] == INHERITED
    assert nodes["scoring"]["results"] == 1


def test_no_node_is_measured_without_a_declared_floor() -> None:
    """Eight scored levels with a real spread still fall short of a measurement.

    This is the state the record is actually in. The contrast is genuine, the
    numbers differ by a wide margin, and none of that is a separation until
    something says what the floor was.
    """
    levels = {f"preset-{i}": 0.10 + 0.02 * i for i in range(8)}
    client, _ = _client(populated_record(levels))
    body = client.get("/v1/graph").json()
    scoring = next(n for n in body["nodes"] if n["key"] == "scoring")
    assert scoring["levels_instantiated"] == 8
    assert scoring["strength"] == CHOSEN
    assert not [n for n in body["nodes"] if n["strength"] == MEASURED]


def test_a_declared_floor_that_is_cleared_reaches_measured() -> None:
    """The strongest edge is reachable, so the rules above are not vacuous."""
    levels = {"floor-level": 0.10, "rival": 0.30}
    record = populated_record(levels)
    record["floors"] = [{"node": "scoring", "floor": "floor-level", "name": "run-1"}]
    client, _ = _client(record)
    scoring = next(n for n in client.get("/v1/graph").json()["nodes"] if n["key"] == "scoring")
    assert scoring["strength"] == MEASURED
    assert scoring["blocked_reason"] is None


def test_two_levels_with_one_scored_is_unpowered() -> None:
    """A contrast needs two scored levels, and that is knowable before reading one."""
    assert strength_of(Edge(instantiated=2, available=2, scored=1, results=1)) == UNPOWERED
    assert strength_of(Edge(instantiated=8, available=8, scored=0, results=0)) == UNPOWERED


# ── A blocked node always brings a reason ─────────────────────────────────────


def test_every_blocked_node_carries_a_reason_on_an_empty_record() -> None:
    """With nothing in the record, every node is blocked and every one says why."""
    client, _ = _client(empty_record())
    body = client.get("/v1/graph").json()
    blocked = [n for n in body["nodes"] if n["strength"] == BLOCKED]
    assert len(blocked) == len(SPECS)
    for node in blocked:
        assert node["blocked_reason"]
        assert node["blocked_reason"].strip() == node["blocked_reason"]


def test_every_blocked_node_carries_a_reason_on_a_populated_record() -> None:
    """The rule is about the strength, not about the record being empty."""
    client, _ = _client(populated_record({"a": 0.1, "b": 0.2}))
    body = client.get("/v1/graph").json()
    for node in body["nodes"]:
        if node["strength"] == BLOCKED:
            assert node["blocked_reason"], f"{node['key']} is blocked with no reason"


def test_the_blocked_list_mirrors_the_blocked_nodes_exactly() -> None:
    """A node cannot be blocked and missing from the list, or listed without one.

    The two are built from one source, and this is what says so. It also pins
    that each entry names a precondition, because a blocked node that does not
    say what would unblock it is a dead end rather than a next step.
    """
    for record in (empty_record(), populated_record({"a": 0.1, "b": 0.2})):
        body = build_graph(record)
        listed = {b["node"] for b in body["blocked"]}
        blocked = {n["key"] for n in body["nodes"] if n["strength"] == BLOCKED}
        assert listed == blocked
        for entry in body["blocked"]:
            assert entry["why"] and entry["what"] and entry["precondition"]
            node = next(n for n in body["nodes"] if n["key"] == entry["node"])
            assert entry["why"] == node["blocked_reason"]


# ── Nothing is written ────────────────────────────────────────────────────────


def test_every_declared_statement_is_a_select() -> None:
    """The read surface cannot express a write, whatever a caller asks for."""
    forbidden = ("insert", "update", "delete", "create", "drop", "alter", "truncate", "grant")
    for name, clause in QUERIES.items():
        sql = str(clause).strip().lower()
        assert sql.startswith("select"), f"{name} does not start with SELECT"
        for word in forbidden:
            assert f" {word} " not in f" {sql} ", f"{name} contains {word}"


def test_the_endpoint_never_touches_a_write_path() -> None:
    """Every write method on the session raises, and the request still succeeds.

    A single ``session.add`` or ``session.commit`` anywhere under the endpoint
    turns this green test red, which is stronger than reading the source and
    concluding there is none.
    """
    client, session = _client(populated_record({"a": 0.1, "b": 0.2}))
    assert client.get("/v1/graph").status_code == 200
    with pytest.raises(WriteAttempted):
        session.commit()


def test_the_endpoint_issues_the_declared_statements_and_no_others() -> None:
    """One read each, nothing undeclared. The fake raises on anything else."""
    session = FakeSession(empty_record())
    read_record(session)
    assert len(session.statements) == len(QUERIES)
    assert {id(s) for s in session.statements} == {id(q) for q in QUERIES.values()}


def test_the_modules_do_not_import_the_committing_session_helper() -> None:
    """``session_scope`` commits on exit, so a reporting surface must not use it."""
    from pathlib import Path

    import protea.api.routers.graph as graph_module

    root = Path(graph_module.__file__).parent
    for name in ("graph.py", "_graph_reads.py", "_graph_panels.py"):
        source = (root / name).read_text()
        assert "session_scope(" not in source, name
        assert "import session_scope" not in source, name
        assert ".commit(" not in source, name


# ── Shape and honesty of the payload ──────────────────────────────────────────


def test_the_payload_has_the_five_declared_blocks_and_ten_nodes() -> None:
    client, _ = _client(populated_record({"a": 0.1, "b": 0.2}))
    body = client.get("/v1/graph").json()
    # A subset rather than an equality. The four are the blocks the model
    # declares and every one of them has to be there; blocks added since
    # (the timeline, the floors) are additive and must not make this fail.
    assert {"frame", "nodes", "panels", "blocked"} <= set(body)
    assert [n["key"] for n in body["nodes"]] == [s.key for s in SPECS]
    assert [n["stage"] for n in body["nodes"]] == list(range(len(SPECS)))
    assert {n["strength"] for n in body["nodes"]} <= _STRENGTHS


def test_all_nine_panels_are_reported_even_when_none_is_scored() -> None:
    """A panel nobody scored is a fact about the record, not a row to drop."""
    client, _ = _client(empty_record())
    panels = client.get("/v1/graph").json()["panels"]
    assert [(p["category"], p["aspect"]) for p in panels] == [list(k) and k for k in PANEL_KEYS]
    assert all(p["results"] == [] for p in panels)
    assert all(p["units"] is None for p in panels)


def test_an_unscored_panel_reports_no_population_rather_than_zero() -> None:
    """Null, never a zero: the two say different things and only one is true."""
    panels = build_panels([], None)
    assert all(p["units"] is None for p in panels)
    assert all(p["detectable_effect"] is None for p in panels)


def test_the_population_is_counted_from_the_ground_truth_and_not_inferred() -> None:
    """Counted, because the one number here that must not be guessed is this one.

    An earlier version derived it, by inverting a stored coverage against the
    protein count at the optimum threshold and intersecting the intervals the
    four-decimal rounding allows. That reads as careful and is not: every result
    inverts the same quantity the same way, so the intervals agree with each
    other while all being wrong together, and the guard that returns nothing
    when they disagree can never fire. It put two of the nine panels out by
    eleven and eight units with the same confidence as the seven it got right.
    """
    payload = _ground_truth_bytes(
        [
            ("P1", "GO:1", "nk"),
            ("P2", "GO:1", "nk"),
            ("P2", "GO:2", "nk"),
            ("P3", "GO:9", "known"),
        ]
    )
    counted = panel_units_from_groundtruth(payload, {"GO:1": "P", "GO:2": "F", "GO:9": "P"})
    assert counted[("NK", "BPO")] == 2
    assert counted[("NK", "MFO")] == 1
    assert ("NK", "CCO") not in counted


def test_a_term_outside_the_pivot_places_no_protein() -> None:
    """A term the pivot cannot type belongs to no panel, and is not a zero."""
    payload = _ground_truth_bytes([("P1", "GO:404", "nk")])
    assert panel_units_from_groundtruth(payload, {"GO:1": "P"}) == {}


def test_only_scored_buckets_carry_a_population() -> None:
    """``known`` and ``removed`` are reported and never scored, so neither is one."""
    payload = _ground_truth_bytes(
        [("P1", "GO:1", "known"), ("P2", "GO:1", "removed"), ("P3", "GO:1", "lk")]
    )
    counted = panel_units_from_groundtruth(payload, {"GO:1": "P"})
    assert counted == {("LK", "BPO"): 1}


def test_the_frame_is_undeclared_while_any_published_result_is_unsealed() -> None:
    """A frame that is recoverable is not a frame that was declared.

    Every field can resolve and the block still reports false, because no
    published result states which scoring frame it lives in. The two counters
    beside it are what tells a reader which of the two situations they are in.
    """
    record = populated_record({"a": 0.1, "b": 0.2})
    body = build_graph(record)
    assert body["frame"]["window"] == "220->227"
    assert body["frame"]["unsealed_rows"] == 2
    assert body["frame"]["sealed_rows"] == 0
    assert body["frame"]["declared"] is False

    for row in record["results"]:
        row["frame"] = "lafa"
    sealed = build_graph(record)
    assert sealed["frame"]["sealed_rows"] == 2
    assert sealed["frame"]["declared"] is True


def test_an_empty_record_invents_no_numbers() -> None:
    """Nothing in the record means zero levels and no results, never a placeholder."""
    body = build_graph(empty_record())
    assert body["frame"]["evaluation_set_id"] is None
    assert body["frame"]["window"] is None
    assert body["frame"]["declared"] is False
    for node in body["nodes"]:
        assert node["levels_instantiated"] == 0
        assert node["results"] == 0
        assert node["varying_fields"] == []
        assert node["constant_fields"] == []


def test_the_floors_are_served_with_the_record_and_not_left_to_the_client() -> None:
    """A cell marked too thin has to be able to say too thin for WHAT.

    Two classes, because the same cell is routinely reportable and unroutable
    at once, and a surface given one number would draw one question where the
    record poses two.
    """
    client, _ = _client(populated_record({"a": 0.1, "b": 0.2}))
    floors = client.get("/v1/graph").json()["floors"]
    keys = [c["key"] for c in floors["classes"]]
    assert keys == ["reporting", "routing"]
    populations = [c["population"] for c in floors["classes"]]
    # Ascending, so a reader meets the permissive floor before the strict one,
    # and strictly so: two classes that priced the same are one class.
    assert populations == sorted(populations)
    assert populations[0] < populations[1]
    assert floors["target_effect"] > 0
    assert all(c["contrast"] for c in floors["classes"])


# ── A retrieval depth and an evaluation cut are not one axis ──────────────────


def _depth_rows(depths: dict[str, str]) -> list[dict[str, Any]]:
    """One scored row per (arm, panel), the arms differing only in depth.

    Depth is the only field that moves, so it is the only field the level is
    named by and a level name here IS a depth. The scores rise with the key so
    the first arm is always the loser: whatever else the comparison does, it
    cannot come out False for want of a winner, which keeps the tests below
    about the refusal and not about the arithmetic.
    """
    return [
        {
            "result_id": f"r-{name}",
            # One frame, so these tests stay about the depth refusal. A separation
            # now happens inside a seal, and a fixture without one is asking a
            # question that cannot be answered rather than one whose answer is no.
            "frame_digest": "f-one-frame",
            "scoring_name": "composite",
            "embedding_name": "esm2_650m",
            "depth": depth,
            "category": category,
            "aspect": aspect,
            "f_micro_w": 0.1 + 0.1 * i,
            "tau": 0.5,
            "n_at_tau": 950,
            "coverage_at_tau": 0.95,
        }
        for i, (name, depth) in enumerate(depths.items())
        for category, aspect in PANEL_KEYS
    ]


def test_a_ladder_of_cuts_over_one_retrieval_is_still_a_comparison() -> None:
    """Cuts against cuts is one axis, and the surface answers it as before.

    The refusal is about crossing two quantities, not about disliking one of
    them. A truncation ladder is a real contrast between real numbers; what it
    must not do is stand in for a retrieval result.
    """
    rows = _depth_rows({"shallow": "cut at sequence rank 2", "deep": "cut at sequence rank 30"})
    assert separated_from_floor(rows, "cut at sequence rank 2") is True


def test_a_floor_at_a_retrieval_depth_refuses_its_rivals_evaluation_cuts() -> None:
    """The comparison this endpoint must not answer.

    A retrieval depth decides which candidates were ever fetched; a cut only
    truncates a list that was already retrieved and scored. Put on one ladder
    they read as five settings of one knob, and the campaign published exactly
    that reading once.
    """
    rows = _depth_rows({"retrieved": "retrieval depth 30", "cut": "cut at sequence rank 30"})
    with pytest.raises(CrossedDepthAxes) as refusal:
        separated_from_floor(rows, "retrieval depth 30")
    message = str(refusal.value)
    assert "retrieval depth" in message
    assert "evaluation cut" in message
    # The message has to say WHY, not merely that it declined: a reader who is
    # told only 'refused' declares the other floor next and meets it again.
    assert "truncat" in message


def test_the_two_cuts_that_used_to_render_alike_are_two_levels() -> None:
    """A k-position cut of 10 and a retrieval depth of 10 were both '10'.

    Sixteen results of prediction set d5b634b2 sit in exactly this shape in the
    record: retrieved at depth 10, evaluated both uncut and cut at k-position
    10. Under the old rendering they shared a level name, so the floor matched
    both sides and nothing was ever put against anything.
    """
    rows = _depth_rows({"cut": "cut at protein rank 10", "uncut": "retrieval depth 10"})
    with pytest.raises(CrossedDepthAxes):
        separated_from_floor(rows, "cut at protein rank 10")


def test_a_crossed_ladder_reaches_the_reader_as_a_reason_and_not_a_500() -> None:
    """The refusal is caught where it can be shown, and shown.

    A page that answers with a stack trace teaches nobody anything, and one
    that answers None teaches them that the record established nothing without
    saying which question was wrong. The node comes out unmeasured, carries the
    refusal as its reason, and the endpoint still serves.
    """
    record = populated_record({"composite": 0.1})
    record["panels"] = _depth_rows(
        {"retrieved": "retrieval depth 30", "cut": "cut at sequence rank 30"}
    )
    record["scoring"] = [
        {
            "id": "sc-composite",
            "name": "composite",
            "formula": "linear",
            "weights": "{}",
            "evidence_weights": "{}",
            "params": "{}",
            "results": 1,
        },
        {
            "id": "sc-other",
            "name": "other",
            "formula": "linear",
            "weights": "{}",
            "evidence_weights": "{}",
            "params": "{}",
            "results": 1,
        },
    ]
    record["floors"] = [
        {
            "node": "scoring",
            "floor": "retrieval depth 30",
            "name": "a ladder that crosses two quantities",
        }
    ]
    client, _ = _client(record)
    response = client.get("/v1/graph")
    assert response.status_code == 200
    node = next(n for n in response.json()["nodes"] if n["key"] == "scoring")
    assert node["strength"] != MEASURED
    assert "evaluation cut" in node["blocked_reason"]
    assert "truncat" in node["blocked_reason"]



class TestEveryNodeCanReachAMeasurement:
    """The vocabulary promised `measured` and nine of ten nodes could not reach it.

    Until 2026-09-02 `build_nodes` handed the floors dict to `_scoring_node`
    alone. Every other builder constructed its Edge with `floor` left at its
    default of None, so `strength_of` short-circuited to CHOSEN before it ever
    reached the separation test -- whatever an `experiment_run` declared.

    It was invisible from outside because the reasons a node gives are about its
    levels, not about its floor, so a node with a declared floor and a real
    contrast reported exactly what a node with neither reported. The first time
    it mattered, a retriever contrast with nine of nine panels resolved and a
    floor named in an experiment_run still read `chosen`, and the declaration
    looked wrong when the surface was.

    These tests are structural on purpose. Asserting the strength of a fixture
    would pass again the moment somebody added an eleventh node and forgot it;
    asserting that every builder takes the floors and that every one is given
    them cannot.
    """

    def test_every_node_builder_accepts_the_floors(self) -> None:
        import inspect

        from protea.api.routers import _graph_nodes
        from protea.api.routers._graph_edges import SPECS

        builders = [
            getattr(_graph_nodes, f"_{spec.key.replace('-', '_')}_node")
            for spec in SPECS
        ]
        assert len(builders) == 10
        for builder in builders:
            params = inspect.signature(builder).parameters
            assert "floors" in params, f"{builder.__name__} cannot see a declared floor"

    def test_every_builder_is_handed_them_at_the_call_site(self) -> None:
        """Taking the argument is worthless if the caller does not pass it."""
        import ast
        import inspect

        from protea.api.routers import graph as graph_module
        from protea.api.routers._graph_edges import SPECS

        tree = ast.parse(inspect.getsource(graph_module))  # build_graph is where they are called
        called_with_floors = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id.endswith("_node")
            and any(isinstance(a, ast.Name) and a.id == "floors" for a in node.args)
        }
        expected = {f"_{spec.key.replace('-', '_')}_node" for spec in SPECS}
        assert expected - called_with_floors == set(), (
            f"these builders are never handed the floors: {sorted(expected - called_with_floors)}"
        )


class TestASeparationHappensInsideOneFrame:
    """Two arms that were not scored under the same seal are not two levels.

    Until 2026-09-02 `separated_from_floor` tested the declared floor against
    every level that landed in the same panel, which is every arm the record
    holds regardless of the window, the corpus or the accretion table behind it.
    The first time a second frame existed it bit immediately: a retriever floor
    on the 226->227 tune frame was being compared against arms from 220->227,
    and the rivals that beat it in all nine panels were the other window's.

    A panel key is a category and an aspect. It is not a frame.
    """

    def _rows(self, seal: str, levels: dict[str, float]) -> list[dict[str, Any]]:
        rows = _panel_rows(levels)
        for row in rows:
            row["frame_digest"] = seal
        return rows

    def test_a_rival_from_another_frame_cannot_clear_the_floor(self) -> None:
        """The defect, pinned: the outsider wins on every panel and counts for nothing."""
        rows = self._rows("f-ours", {"floor-level": 0.10}) + self._rows(
            "f-theirs", {"outsider": 0.90}
        )
        assert separated_from_floor(rows, "floor-level") is False

    def test_a_rival_inside_the_frame_still_clears_it(self) -> None:
        """The restriction is about provenance, not about refusing comparisons."""
        rows = self._rows("f-ours", {"floor-level": 0.10, "rival": 0.30}) + self._rows(
            "f-theirs", {"outsider": 0.90}
        )
        assert separated_from_floor(rows, "floor-level") is True

    def test_an_unsealed_floor_refuses_rather_than_reporting_no_separation(self) -> None:
        """Returning False would report a comparison that was never made.

        An unstamped marker is not a matching one -- the rule
        seal_evaluation_frames states and refuses on -- so the honest answer is
        that the question cannot be asked yet.
        """
        rows = _panel_rows({"floor-level": 0.10, "rival": 0.30})
        for row in rows:
            row.pop("frame_digest", None)
        with pytest.raises(CrossedFrames, match="no frame seal"):
            separated_from_floor(rows, "floor-level")

    def test_a_floor_that_spans_two_frames_is_not_one_population(self) -> None:
        """Picking one seal would publish a number whose population nobody chose."""
        # A second level in each frame so the level name is the scoring name and
        # the two floors render alike; with one level nothing varies and the name
        # falls back to the whole tuple.
        rows = self._rows("f-ours", {"floor-level": 0.10, "rival": 0.30}) + self._rows(
            "f-theirs", {"floor-level": 0.90, "rival": 0.95}
        )
        with pytest.raises(CrossedFrames, match="different frame"):
            separated_from_floor(rows, "floor-level")


@pytest.mark.integration
class TestAWithdrawnDeclarationGovernsNothing:
    """A floor is a declaration, and a declaration can be taken back.

    Until 2026-09-14 the floors query read ``graph_node`` and ``floor`` off any
    ``experiment_run`` whatever state it was in. The single row in the store
    carrying both keys is `campana-limpia-eje-A-sustrato-220-227`: its status is
    `abandoned` and its own findings say it was never dispatched and that its
    floor stops being a starting point. The retraction sat one column away from
    the two the query read, and nothing read it, so a run that had withdrawn its
    floor still handed that floor to the substrate node.

    These are prophylaxis and not a repair. That comparison does not clear the
    floor, and `strength_of` answers `chosen` both with a floor a node fails and
    with no floor at all, so no published word moves. They are here so the next
    declared floor cannot be governed by a declaration somebody took back.
    """

    @pytest.fixture()
    def disposable_conn(self, postgres_url: str) -> Iterator[Connection]:
        """A connection to the suite's own Postgres, in a transaction rolled back.

        On ``postgres_url`` and not on a target of its own, because a target of
        its own is one no workflow fills. Keyed on ``PROTEA_DB_URL`` this pair
        skipped in BOTH: the unit workflow runs ``pytest`` with no Postgres at
        all, and the integration workflow supplies ``PROTEA_PG_*`` and never
        ``PROTEA_DB_URL``. Two tests that skip are two tests that do not exist,
        and they report green while doing it. ``postgres_url`` is the plumbing
        every other Postgres test here already uses, and ``pytest
        --with-postgres`` is what the integration workflow already runs.

        The schema is the ORM's own ``experiment_run`` table, created inside the
        transaction and discarded with it, so the target is left as it was
        found. The table and its enum type are dropped first, also inside the
        transaction: this Postgres is shared with every other module in the
        session and one of them may have left the table behind, and a
        ``CREATE TABLE`` that fails is a test that errors instead of a test that
        answers. The rollback puts back whatever was there.
        """
        engine = create_engine(postgres_url, future=True)
        conn = engine.connect()
        try:
            conn.execute(text("DROP TABLE IF EXISTS experiment_run CASCADE"))
            conn.execute(text("DROP TYPE IF EXISTS experiment_run_status CASCADE"))
            ExperimentRun.__table__.create(conn)
            yield conn
        finally:
            conn.rollback()
            conn.close()
            engine.dispose()

    @staticmethod
    def _declare_floor(conn: Connection, *, name: str, status: str, floor: str) -> None:
        """One run declaring a floor for the substrate node, in the given state."""
        conn.execute(
            text(
                "INSERT INTO experiment_run"
                " (id, name, status, config, provenance, tags, created_at)"
                " VALUES (CAST(:id AS uuid), :name,"
                " CAST(:status AS experiment_run_status), CAST(:config AS jsonb),"
                " '{}'::jsonb, '{}'::text[], now())"
            ),
            {
                "id": str(uuid.uuid4()),
                "name": name,
                "status": status,
                "config": json.dumps({"graph_node": "substrate", "floor": floor}),
            },
        )

    def test_a_floor_declared_by_a_withdrawn_run_is_not_read(
        self, disposable_conn: Connection
    ) -> None:
        """The defect itself: the run took its floor back and the query kept it."""
        self._declare_floor(
            disposable_conn,
            name="campana-limpia-eje-A-sustrato-220-227",
            status=str(ExperimentRunStatus.ABANDONED),
            floor="esm2_8m",
        )
        assert disposable_conn.execute(_Q_FLOORS).mappings().all() == []

    def test_a_floor_declared_by_a_standing_run_is_read(self, disposable_conn: Connection) -> None:
        """The other half, and it is not optional.

        Read alone, the refusal above is also what a predicate matching nothing
        would produce, and a floor that never reaches the graph makes every node
        `chosen` for a reason the reader cannot see. So every standing state is
        declared here beside the withdrawn one, and only the withdrawn one is
        missing from the answer.
        """
        for state in standing_floor_statuses(ExperimentRunStatus):
            self._declare_floor(
                disposable_conn, name=f"declared-{state}", status=state, floor=f"floor-{state}"
            )
        self._declare_floor(
            disposable_conn,
            name="declared-abandoned",
            status=str(ExperimentRunStatus.ABANDONED),
            floor="floor-abandoned",
        )
        rows = [dict(r) for r in disposable_conn.execute(_Q_FLOORS).mappings().all()]
        standing = standing_floor_statuses(ExperimentRunStatus)
        assert {r["name"] for r in rows} == {f"declared-{state}" for state in standing}
        assert {r["floor"] for r in rows} == {f"floor-{state}" for state in standing}
        assert all(r["node"] == "substrate" for r in rows)

    def test_a_lifecycle_state_nobody_classified_refuses_the_read(self) -> None:
        """Drift in either direction is refused where it can still be read.

        A state added to `ExperimentRunStatus` would otherwise decide whether a
        declaration governs by whichever way the two sets happen to be written,
        which is the same silence this change is about; and a state classified
        here that the column cannot hold would put a literal in the query that
        Postgres rejects halfway through serving a request.
        """
        with pytest.raises(UnclassifiedRunStatus, match="superseded"):
            standing_floor_statuses([*ExperimentRunStatus, "superseded"])
        with pytest.raises(UnclassifiedRunStatus, match="cannot hold it"):
            standing_floor_statuses(["planned", "done", "abandoned"])

    def test_the_states_this_column_can_hold_are_classified_end_to_end(self) -> None:
        """And the query is generated from that classification, not beside it."""
        standing = standing_floor_statuses(ExperimentRunStatus)
        assert set(standing) == {"planned", "running", "done"}
        clause = "er.status IN (" + ", ".join(f"'{state}'" for state in standing) + ")"
        assert clause in str(_Q_FLOORS)
        assert str(ExperimentRunStatus.ABANDONED) not in str(_Q_FLOORS)
class TestTheComparisonBehindTheStrengthIsPublished:
    """`chosen` is a sink, and until now the response held nothing that drained it.

    Four situations came out as that one word -- a single level the frame fixed,
    a powered contrast with no floor declared, a declared floor that REFUSED the
    comparison, and a comparison that was made and lost -- and a reader had no
    way to tell which one they were looking at.

    The third was the worst of them. `_floor_for` called `_separation`, which
    catches CrossedFrames and CrossedDepthAxes so the page keeps serving, and
    then threw the caught text away. Nine of the ten nodes turned a refusal into
    a bare `chosen`, and no field anywhere in the response said a refusal had
    happened. A caught refusal nobody sees is a silent None.

    Nothing here changes a strength. The three keys report the comparison the
    strength was already decided on.
    """

    _KEYS = ("floor", "separated", "floor_refusal")

    def test_every_node_publishes_the_three(self) -> None:
        """Structural over SPECS, and over a blocked record as well as a live one.

        Asserting it of one fixture's scoring node would pass again the moment an
        eleventh node was added and forgotten, and a blocked node needs the keys
        as much as any other: a floor can be declared for a node whose artifact
        has no producer, and that is worth seeing.
        """
        for record in (empty_record(), populated_record({"a": 0.1, "b": 0.2})):
            nodes = {n["key"]: n for n in build_graph(record)["nodes"]}
            assert set(nodes) == {spec.key for spec in SPECS}
            for key, node in nodes.items():
                missing = [f for f in self._KEYS if f not in node]
                assert not missing, f"{key} publishes no {missing}"

    def test_a_floor_that_was_asked_publishes_the_answer_it_got(self) -> None:
        """Both answers, because a key that only ever holds None is not published.

        The cleared floor is the case
        `test_a_declared_floor_that_is_cleared_reaches_measured` already pins on
        the strength; what it could not show is that the verdict itself reaches a
        reader. The floor that was NOT cleared is the one that matters here: it
        reads `chosen`, exactly as a contrast with no floor at all does, and only
        `separated` false says a comparison was made and lost.
        """
        asked = (("floor-level", MEASURED, True), ("rival", CHOSEN, False))
        for floor, strength, verdict in asked:
            record = populated_record({"floor-level": 0.10, "rival": 0.30})
            record["floors"] = [{"node": "scoring", "floor": floor, "name": "run-1"}]
            client, _ = _client(record)
            node = next(n for n in client.get("/v1/graph").json()["nodes"] if n["key"] == "scoring")
            assert node["strength"] == strength
            assert node["floor"] == floor
            assert node["separated"] is verdict
            assert node["floor_refusal"] is None

    def test_a_crossed_frames_refusal_reaches_the_payload_instead_of_vanishing(self) -> None:
        """The refusal the helper used to drop, on a node that is not scoring.

        `_scoring_node` reads the refusal itself and prints it in its own prose,
        so it never showed the defect. Every other node went through
        `_floor_for`, which dropped it. This declares a retriever floor on
        unsealed panels -- the shape `seal_evaluation_frames` refuses on -- and
        asks the endpoint for the retriever node.
        """
        record = populated_record({"floor-level": 0.10, "rival": 0.30})
        for row in record["panels"]:
            row.pop("frame_digest", None)
        without = next(n for n in build_graph(record)["nodes"] if n["key"] == "retriever")
        record["floors"] = [{"node": "retriever", "floor": "floor-level", "name": "run-1"}]
        client, _ = _client(record)
        response = client.get("/v1/graph")
        assert response.status_code == 200
        node = next(n for n in response.json()["nodes"] if n["key"] == "retriever")
        assert node["floor"] == "floor-level"
        assert node["separated"] is None
        assert "frame seal" in node["floor_refusal"]
        assert "floor-level" in node["floor_refusal"]
        # Declaring the floor decided nothing it had not already decided. The
        # refusal is published, not acted on.
        assert node["strength"] == without["strength"]

    def test_an_edge_cannot_hold_a_verdict_and_a_refusal_at_once(self) -> None:
        """A refused comparison has no verdict, so an edge holding both is unreadable.

        It would leave a reader to pick which half of one node's payload to
        believe, which is the state this change exists to end rather than to
        re-create in a new pair of keys.
        """
        with pytest.raises(ValueError, match="both a verdict"):
            Edge(instantiated=2, scored=2, floor="a-floor", separated=False, refusal="crossed")
        with pytest.raises(ValueError, match="no floor"):
            Edge(instantiated=2, scored=2, refusal="crossed")

    def test_the_edges_the_builders_actually_construct_are_accepted(self) -> None:
        """The guard must not refuse any shape the builders can hand it.

        A guard tested only on what it rejects can be a function that always
        raises, and this one sits in front of every node on the page.
        """
        for floor, separated, refusal in (
            (None, None, None),  # nothing declared
            ("a-floor", True, None),  # asked and cleared
            ("a-floor", False, None),  # asked and lost
            ("a-floor", None, "crossed"),  # refused
            ("a-floor", None, None),  # declared, no panel could testify
        ):
            edge = Edge(
                instantiated=2,
                scored=2,
                floor=floor,
                separated=separated,
                refusal=refusal,
            )
            assert strength_of(edge) in _STRENGTHS
