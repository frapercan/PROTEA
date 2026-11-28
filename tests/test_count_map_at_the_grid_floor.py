"""The count map at the grid floor and by accession (the literature ladder's rung 12).

``top`` null keeps every term the evaluator can count (a four-decimal rendering at or
above the grid's first threshold) instead of a hand-fixed 100, checked here against a
brute force that renders every score; ``exclusion`` ``accession`` leaves out of a
target's reading its own accession only, so a twin under another accession stays a
donor; the recall past rank 100 is measured from a reading and a truth; and the
variants are methods of their own, while the default reading keeps the spec it had.
"""

from __future__ import annotations

import math
import uuid
from types import SimpleNamespace

import numpy as np
import pytest
import scipy.sparse as sp
from pydantic import ValidationError

from protea.core.count_map import reading
from protea.core.count_map.fit import unit_counts
from protea.core.count_map.inputs import Columns, Who, aspect_problem
from protea.core.count_map.reading import (
    FLOOR_CUT,
    GRID_FLOOR,
    CountMap,
    Rows,
    first_score_rendered_at_or_above,
    ranks,
    read,
)
from protea.core.count_map.recall import rank_of_rows, recall_by_rank
from protea.core.count_map.run import AspectResult
from protea.core.operations import run_count_map as op
from protea.core.operations._run_cafa_grid_artifact import tau_grid_for
from protea.core.operations.run_count_map import RunCountMapPayload
from tests.test_count_map import _brute_reading, _targets

ESET = str(uuid.uuid4())
BANK = str(uuid.uuid4())


def _renders_at_or_above(score: float, floor: float = GRID_FLOOR) -> bool:
    return float(f"{score:.4f}") >= floor


# ---------------------------------------------------------------- the floor


def test_the_floor_is_the_evaluators_first_threshold() -> None:
    assert GRID_FLOOR == tau_grid_for(0.01)[0]


def test_the_cut_is_the_renderings_own_rule_around_its_boundary() -> None:
    near = [FLOOR_CUT]
    for _ in range(50):
        near.append(math.nextafter(near[-1], math.inf))
        near.insert(0, math.nextafter(near[0], -math.inf))
    for score in [*near, 0.0099, 0.00994999, 0.0100, 0.011, 0.5, 1.0, 0.0]:
        assert (score >= FLOOR_CUT) == _renders_at_or_above(score), score
    for floor in (0.5, 0.33, 0.99):
        cut = first_score_rendered_at_or_above(floor)
        assert _renders_at_or_above(cut, floor)
        assert not _renders_at_or_above(math.nextafter(cut, -math.inf), floor)


def _sparse_problem(seed: int):
    """Many donors and rare terms, so scores fall on both sides of the floor."""
    rng = np.random.default_rng(seed)
    donors, units, terms = 400, 10, 40
    group = rng.integers(0, donors // 2, size=donors)
    seq_units = rng.random((donors // 2, units)) < 0.3
    carriers = sp.csr_matrix(seq_units[group].astype(np.float64))
    rates = np.geomspace(0.002, 0.4, terms)
    states = sp.csr_matrix((rng.random((donors, terms)) < rates).astype(np.float64))
    keep = np.flatnonzero(np.diff(states.indptr) > 0)
    carriers, states, group = carriers[keep], states[keep], group[keep]
    base_rate = np.asarray(states.sum(0)).ravel() / states.shape[0]
    return carriers, states, group, base_rate, seq_units


def _brute_floor(carriers, states, group, base_rate, m, targets, target_seq):
    """The brute force's rows before any depth, then every one the evaluator counts."""
    everything = _brute_reading(
        carriers, states, group, base_rate, m, targets, target_seq, top=10**6
    )
    return {
        q: [(t, v, u) for t, v, u in rows if _renders_at_or_above(v)]
        for q, rows in everything.items()
    }


@pytest.mark.parametrize("seed", [21, 22])
def test_the_reading_at_the_grid_floor_equals_the_brute_force(seed: int, monkeypatch) -> None:
    carriers, states, group, base_rate, seq_units = _sparse_problem(seed)
    counts, carried = unit_counts(carriers, states)
    cmap = CountMap(counts, carried, base_rate, 40.0)
    targets = _targets(seed, seq_units, carriers, states, group, len(base_rate))
    target_seq = np.r_[np.arange(seq_units.shape[0]), -1 - np.arange(6)]
    monkeypatch.setattr(reading, "CHUNK_SUPPORT", 200)  # many chunks
    rows = read(cmap, targets, top=None)
    want = _brute_floor(carriers, states, group, base_rate, 40.0, targets, target_seq)
    dropped = 0
    for q, expected in want.items():
        got = np.flatnonzero(rows.target == q)
        got = got[np.lexsort((rows.term[got], -rows.score[got]))]
        assert [int(t) for t in rows.term[got]] == [t for t, _, _ in expected], q
        assert np.allclose(rows.score[got], [v for _, v, _ in expected])
        assert [int(u) for u in rows.unit[got]] == [u for _, _, u in expected], q
        dropped += len(base_rate) - len(expected)
    assert dropped > 0  # the floor took terms out
    assert (rows.score >= FLOOR_CUT).all()


def test_the_floor_keeps_more_than_100_terms_where_the_reading_has_them() -> None:
    terms = 150
    counts = sp.csr_matrix(np.full((1, terms), 3.0))
    base_rate = np.linspace(0.02, 0.2, terms)
    cmap = CountMap(counts, np.array([4.0]), base_rate, 1.0)
    targets = reading.Targets(
        sp.csr_matrix(np.array([[1.0]])),
        sp.csr_matrix((1, terms)),
        np.zeros(1),
        sp.csr_matrix((1, terms)),
    )
    assert len(read(cmap, targets, top=100)) == 100
    assert len(read(cmap, targets, top=None)) == terms


# ---------------------------------------------------------------- exclusion by accession


def _cut():
    positives = {
        "A": {"P": {"GO:1"}},
        "B": {"P": {"GO:2"}},
        "D": {"P": {"GO:1", "GO:2"}},
    }
    md5_of = {"A": "m1", "B": "m1", "D": "m3", "Q": "m1"}
    units = ({"m1": np.array([0]), "m3": np.array([0])}, 1)
    return positives, md5_of, units


def test_by_accession_a_target_leaves_out_its_own_row_and_keeps_its_twins() -> None:
    positives, md5_of, units = _cut()
    columns = Columns("P", ("GO:1", "GO:2"))
    by_sequence = aspect_problem(
        columns, positives, {}, md5_of, units, (["A", "B", "D"], ["Q", "A"])
    )
    by_accession = aspect_problem(
        columns, positives, {}, md5_of, units, Who(["A", "B", "D"], ["Q", "A"], "accession")
    )
    # Q is A's and B's twin under another accession; A is a donor itself.
    assert by_sequence.target_view.excluded.toarray().tolist() == [[1, 1], [1, 1]]
    assert by_accession.target_view.excluded.toarray().tolist() == [[0, 0], [1, 0]]
    assert by_accession.target_view.excluded_n.tolist() == [0, 1]
    # A and B share a sequence: one group by sequence, two by accession.
    assert by_sequence.group.tolist() == [0, 0, 1]
    assert by_accession.group.tolist() == [0, 1, 2]
    with pytest.raises(ValueError, match="exclusion"):
        aspect_problem(columns, positives, {}, md5_of, units, Who(["A"], ["Q"], "twins"))


def test_by_accession_a_twin_votes_for_its_target() -> None:
    positives, md5_of, units = _cut()
    columns = Columns("P", ("GO:1", "GO:2"))
    problem = aspect_problem(
        columns, positives, {}, md5_of, units, Who(["A", "B", "D"], ["A"], "accession")
    )
    counts, carried = unit_counts(problem.carriers, problem.states)
    rows = read(CountMap(counts, carried, problem.base_rate, 1.0), problem.target_view, top=None)
    # A's unit holds A, B and D; A itself is out: GO:2 has B and D (2 of 2), GO:1 has D
    # (1 of 2), which reads below its base rate and is floored there, losing its witness.
    pi = problem.base_rate
    assert rows.term.tolist() == [1, 0] and rows.unit.tolist() == [0, -1]
    assert rows.score.tolist() == pytest.approx([(2 + pi[1]) / 3, pi[0]])
    assert (rows.k[0], rows.n[0]) == (2.0, 2.0)


# ---------------------------------------------------------------- ranks and the recall past 100


def test_ranks_follow_the_readings_order_within_each_target() -> None:
    rows = Rows(
        np.array([0, 0, 0, 1, 1]),
        np.array([3, 1, 2, 0, 1]),
        np.array([0.5, 0.9, 0.5, 0.2, 0.3]),
        np.zeros(5, np.int64),
        np.zeros(5),
        np.zeros(5),
    )
    assert ranks(rows).tolist() == [3, 1, 2, 2, 1]


def test_the_recall_past_rank_counts_what_only_the_floor_holds() -> None:
    aspect_of = {f"GO:{i:03d}": "P" for i in range(120)} | {"GO:F": "F"}
    scores = {f"GO:{i:03d}": 1.0 - i / 200 for i in range(120)}
    rows = [("T1", term, score) for term, score in scores.items()] + [("T1", "GO:F", 0.4)]
    truth = {"T1": {"GO:005", "GO:110", "GO:F", "GO:999"}, "T2": {"GO:001"}}
    report = recall_by_rank(rows, truth, aspect_of, rank=100)
    assert rank_of_rows(rows, aspect_of)[("T1", "GO:110")] == 111
    assert report["P"] == {
        "true_pairs": 3,
        "within_rank": 1,
        "past_rank": 1,
        "missed": 1,
        "recall_within_rank": pytest.approx(1 / 3),
        "recall_at_the_floor": pytest.approx(2 / 3),
        "recall_past_rank": pytest.approx(1 / 3),
    }
    assert report["F"]["within_rank"] == 1 and report["all"]["true_pairs"] == 4
    assert "GO:999" not in aspect_of  # a term the pivot does not place is not counted


# ---------------------------------------------------------------- the payload and the method


def test_the_payload_takes_the_grid_floor_and_the_exclusion() -> None:
    p = RunCountMapPayload(
        evaluation_set_id=ESET, lexicon_bank_id=BANK, top=None, exclusion="accession"
    )
    assert p.top is None and p.exclusion == "accession"
    default = RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK)
    assert default.top == 100 and default.exclusion == "sequence"
    with pytest.raises(ValidationError):
        RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK, top=0)
    with pytest.raises(ValidationError):
        RunCountMapPayload(evaluation_set_id=ESET, lexicon_bank_id=BANK, exclusion="twins")


def _sources(lexicon: str = "lexico-93k", profile: str = "own_graph") -> op.Sources:
    lexicon_row = SimpleNamespace(id=uuid.uuid4(), sha256="a" * 64, name=lexicon)
    bank = SimpleNamespace(id=uuid.uuid4(), manifest_sha256="c" * 64)
    photo = SimpleNamespace(cut_release=211, ontology_snapshot_id=uuid.uuid4())
    families = SimpleNamespace(id=uuid.uuid4(), manifest_sha256="f" * 64)
    window = SimpleNamespace(truth_profile=profile)
    return op.Sources(window, photo, bank, lexicon_row, families, "permissive")  # type: ignore[arg-type]


def _payload(**fields: object) -> RunCountMapPayload:
    base = {
        "evaluation_set_id": ESET,
        "lexicon_bank_id": BANK,
        "units": "families",
        "segment_families_id": str(uuid.uuid4()),
        "scope": "permissive",
    }
    return RunCountMapPayload(**{**base, **fields})


WEIGHTS = SimpleNamespace(id=uuid.uuid4(), sha256="d" * 64)


def test_each_variant_is_a_method_of_its_own_and_the_default_keeps_its_spec() -> None:
    src = _sources()
    default = op.method_spec(src, WEIGHTS, _payload())  # type: ignore[arg-type]
    floor = op.method_spec(src, WEIGHTS, _payload(top=None))  # type: ignore[arg-type]
    accession = op.method_spec(src, WEIGHTS, _payload(exclusion="accession"))  # type: ignore[arg-type]
    both = op.method_spec(src, WEIGHTS, _payload(top=None, exclusion="accession"))  # type: ignore[arg-type]
    assert default.code == "count-map-families-permissive-lexico-93k-lafa"
    assert "floor" not in default.recipe and default.recipe["top"] == 100
    assert default.recipe["exclusion"] == "the donors that share the target's sequence md5"
    assert floor.code == default.code + "-grid" and floor.recipe["top"] is None
    assert floor.recipe["floor"].startswith("every term whose four-decimal rendering")
    assert accession.code == default.code + "-accession"
    assert accession.recipe["exclusion"].startswith("the target's own accession only")
    assert both.code == "count-map-families-permissive-lexico-93k-lafa-accession-grid"
    assert len({default.method_id, floor.method_id, accession.method_id, both.method_id}) == 4
    # The map's weights follow the fit, which the depth does not change.
    assert op.map_code(src, _payload(top=None)) == op.map_code(src, _payload())


def test_a_short_name_too_long_for_a_method_is_refused_before_anything_runs() -> None:
    op.refuse_a_name_too_long(_sources(), _payload(top=None, exclusion="accession"))
    with pytest.raises(op.CountMapRefused, match="longer than 64"):
        op.refuse_a_name_too_long(
            _sources(lexicon="lexico-final-b"), _payload(top=None, exclusion="accession")
        )


def test_the_depth_written_is_three_times_top_or_the_most_rows_a_target_got() -> None:
    def result(targets: list[int]) -> AspectResult:
        n = len(targets)
        rows = Rows(
            np.array(targets),
            np.arange(n),
            np.ones(n),
            np.zeros(n, np.int64),
            np.ones(n),
            np.ones(n),
        )
        return AspectResult(Columns("P", ()), None, rows, {})  # type: ignore[arg-type]

    results = [result([0, 0, 1]), result([0, 2, 2, 2, 2])]
    assert op.written_depth(results, 100) == 300
    assert op.written_depth(results, None) == 4
    assert op.written_depth([], None) == 0


def test_the_summary_names_the_floor_and_the_exclusion() -> None:
    summary = op.RunCountMapOperation().summarize_payload(
        {"evaluation_set_id": ESET, "units": "themes", "top": None, "exclusion": "accession"}
    )
    assert summary.endswith("(at the grid floor, by accession)")
    plain = op.RunCountMapOperation().summarize_payload({"evaluation_set_id": ESET})
    assert plain == f"count map of themes for window {ESET[:8]}"
