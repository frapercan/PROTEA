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

No test here opens a database. The session is a fake that answers the endpoint's
statements by identity, which also means a statement the endpoint did not
declare up front cannot be answered at all.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
    PANEL_KEYS,
    build_panels,
    panel_units_from_groundtruth,
)
from protea.api.routers._graph_reads import (
    _PIVOT_ASPECTS,
    PARAM_QUERIES,
    QUERIES,
    read_record,
)
from protea.api.routers.graph import build_graph, router
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
    """One scored row per (level, panel), all nine panels, one shared cohort."""
    return [
        {
            "result_id": f"r-{name}",
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
