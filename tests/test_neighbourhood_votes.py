"""Two votes weigh each neighbour by its distance, over a neighbourhood that must be whole.

``neighbor_similarity_vote`` is, for a term, the summed similarity ``1 - d/2`` of
the neighbours within the depth that carry it over the summed similarity of
every neighbour within the depth. ``neighbor_kernel_fraction`` is the sum of
goPredSim's ``0.5 / (0.5 + d)`` over the neighbours within the depth that carry
it, divided by the depth. Neither is stored: both are taken at the cut from the
donor ledger, and the similarity vote's denominator is the neighbourhood, which
no row holds and which is rebuilt as the union of one protein's ledgers.

Every number here is worked by hand from the neighbourhoods below. The
neighbourhood assertion is pinned on the way a neighbour goes missing (it left
no candidate, for instance because every term it donated was a denial of the
query), and the presets are pinned as bodies the ordinary create endpoint
accepts and the seed-all endpoint never creates.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from protea.core.operations import score_prediction_set as scoring_operation
from protea.core.operations._base_frame_recount import (
    DONOR_LEDGER_COLS,
    LEDGER_SELECT_COLS,
    build_base_frame,
    ledger_columns,
    recount_frame_aggregates,
)
from protea.core.operations._candidate_scoring import BASE_SCORE_COLS, vectorized_scores
from protea.core.operations._donor_recount import (
    KERNEL_BANDWIDTH,
    KERNEL_FRACTION,
    SIMILARITY_VOTE,
    DepthCut,
    NeighbourhoodIncomplete,
    NeighbourhoodRequest,
    kernel_fraction,
    neighbourhood_request,
    similarity_votes,
)
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.embedding.scoring_config import (
    DEFAULT_WEIGHTS,
    ScoringConfig,
)
from protea.services._scoring_models import (
    NEIGHBOURHOOD_VOTE_PRESETS,
    PRESET_CONFIGS,
    ScoringConfigCreate,
)
from protea.services._scoring_validation_helpers import (
    _SIGNAL_TO_COLUMN,
    compute_missing_signals,
)


def _ledger(k: list[int], d: list[float], seq: list[int] | None = None) -> dict[str, Any]:
    """One candidate's donor ledger: k positions, distances, and sequence ranks."""
    return {
        "donor_k_positions": k,
        "donor_sequence_ranks": seq if seq is not None else list(k),
        "donor_distances": d,
    }


def _s(d: float) -> float:
    """The platform's similarity."""
    return 1.0 - d / 2.0


def _kernel(d: float) -> float:
    """goPredSim's reliability weight."""
    return 0.5 / (0.5 + d)


#: Protein Q1, aspect F: four neighbours, at distances 0.10, 0.20, 0.30 and
#: 0.50, so similarities 0.95, 0.90, 0.85 and 0.75. No term is carried by all
#: four, so the neighbourhood exists only as the union of the ledgers.
_F = {1: 0.10, 2: 0.20, 3: 0.30, 4: 0.50}
_Q1_F = {
    "T1": _ledger([1, 2], [_F[1], _F[2]]),
    "T2": _ledger([1, 3], [_F[1], _F[3]]),
    "T3": _ledger([2, 4], [_F[2], _F[4]]),
    "T4": _ledger([3], [_F[3]]),
}

#: Protein Q1, aspect P, ranked on its own: positions 1 and 2 again, other
#: neighbours, at 0.40 and 0.60.
_P = {1: 0.40, 2: 0.60}
_Q1_P = {"T5": _ledger([1], [_P[1]]), "T6": _ledger([2], [_P[2]])}


class TestTheSimilarityVote:
    def test_at_the_whole_depth(self) -> None:
        """Over 0.95 + 0.90 + 0.85 + 0.75 = 3.45."""
        got = similarity_votes(list(_Q1_F.values()), DepthCut(max_k_position=4), "Q1")
        assert got == pytest.approx([1.85 / 3.45, 1.80 / 3.45, 1.65 / 3.45, 0.85 / 3.45])

    def test_at_a_cut_the_neighbourhood_is_the_cut(self) -> None:
        """At depth 2 only T1, T2 and T3 survive, over 0.95 + 0.90 = 1.85, and the
        term both neighbours carry scores exactly 1."""
        rows = [_Q1_F["T1"], _Q1_F["T2"], _Q1_F["T3"]]
        got = similarity_votes(rows, DepthCut(max_k_position=2), "Q1")
        assert got == pytest.approx([1.0, 0.95 / 1.85, 0.90 / 1.85])
        assert got[0] == 1.0

    def test_it_is_a_weighted_vote_fraction(self) -> None:
        """With every neighbour at one distance it is the vote fraction."""
        rows = [_ledger([1, 2], [0.3, 0.3]), _ledger([2, 3], [0.3, 0.3]), _ledger([3], [0.3])]
        got = similarity_votes(rows, DepthCut(max_k_position=3), "Q")
        assert got == pytest.approx([2 / 3, 2 / 3, 1 / 3])

    def test_a_neighbourhood_smaller_than_the_depth_divides_by_what_there_is(self) -> None:
        """Two neighbours retrieved, depth 5: the sum is over the two."""
        rows = [_ledger([1], [0.2]), _ledger([1, 2], [0.2, 0.4])]
        got = similarity_votes(rows, DepthCut(max_k_position=5), "Q")
        assert got == pytest.approx([0.9 / 1.7, 1.0])

    def test_a_row_without_a_ledger_has_none(self) -> None:
        rows = [_ledger([1], [0.2]), {"donor_k_positions": None, "donor_distances": None}]
        assert similarity_votes(rows, DepthCut(max_k_position=1), "Q") == [1.0, None]

    def test_no_ledger_at_all_has_no_neighbourhood_to_check(self) -> None:
        rows = [{"donor_k_positions": None, "donor_distances": None}] * 2
        assert similarity_votes(rows, DepthCut(max_k_position=3), "Q") == [None, None]

    def test_a_neighbourhood_of_no_similarity_has_no_vote(self) -> None:
        """Every neighbour at distance 2 sums to 0: no fraction, rather than a division."""
        rows = [_ledger([1], [2.0]), _ledger([1, 2], [2.0, 2.0])]
        assert similarity_votes(rows, DepthCut(max_k_position=2), "Q") == [None, None]


class TestTheNeighbourhoodAssertion:
    def test_a_neighbour_that_left_no_candidate_stops_it(self) -> None:
        """Position 2 is named by no ledger: its similarity would be missing from
        the denominator, so every vote of the protein would come out too high."""
        rows = [_ledger([1, 3], [0.1, 0.3]), _ledger([3], [0.3])]
        with pytest.raises(NeighbourhoodIncomplete, match=r"protein Q3 in aspect F.*\[2\]"):
            similarity_votes(rows, DepthCut(max_k_position=3), "protein Q3 in aspect F")

    def test_a_gap_below_the_deepest_position_known_stops_it_too(self) -> None:
        """Ledgers reach position 4, so the depth of 3 must hold 1, 2 and 3."""
        rows = [_ledger([1, 2, 4], [0.1, 0.2, 0.4])]
        with pytest.raises(NeighbourhoodIncomplete, match=r"\[3\]"):
            similarity_votes(rows, DepthCut(max_k_position=3), "Q")

    def test_a_gap_past_the_depth_does_not_matter(self) -> None:
        """The neighbour at 4 is missing, and the depth of 3 does not reach it."""
        rows = [_ledger([1, 2], [0.1, 0.2]), _ledger([3, 5], [0.3, 0.5])]
        got = similarity_votes(rows, DepthCut(max_k_position=3), "Q")
        assert got == pytest.approx([1.85 / 2.70, 0.85 / 2.70])

    def test_a_position_that_is_not_a_rank_stops_it(self) -> None:
        with pytest.raises(NeighbourhoodIncomplete, match="not neighbour positions"):
            similarity_votes([_ledger([0, 1], [0.1, 0.2])], DepthCut(max_k_position=2), "Q")

    def test_the_kernel_fraction_does_not_need_the_neighbourhood(self) -> None:
        """It reads each row alone, so a gap elsewhere changes nothing."""
        row = _ledger([1, 3], [0.1, 0.3])
        assert kernel_fraction(row, DepthCut(max_k_position=3)) == pytest.approx(
            (_kernel(0.1) + _kernel(0.3)) / 3
        )


class TestTheKernelFraction:
    @pytest.mark.parametrize(
        ("term", "expected"),
        [
            ("T1", (_kernel(0.10) + _kernel(0.20)) / 4),
            ("T2", (_kernel(0.10) + _kernel(0.30)) / 4),
            ("T3", (_kernel(0.20) + _kernel(0.50)) / 4),
            ("T4", _kernel(0.30) / 4),
        ],
    )
    def test_at_the_whole_depth(self, term: str, expected: float) -> None:
        assert kernel_fraction(_Q1_F[term], DepthCut(max_k_position=4)) == pytest.approx(expected)

    def test_the_bandwidth_is_gopredsims(self) -> None:
        assert KERNEL_BANDWIDTH == 0.5
        assert kernel_fraction(_ledger([1], [0.5]), DepthCut(max_k_position=1)) == 0.5

    def test_a_cut_keeps_only_the_donors_within_it_and_divides_by_it(self) -> None:
        got = kernel_fraction(_Q1_F["T3"], DepthCut(max_k_position=2))
        assert got == pytest.approx(_kernel(0.20) / 2)

    def test_it_reaches_one_only_when_every_slot_carries_the_term_at_distance_zero(self) -> None:
        assert kernel_fraction(_ledger([1, 2], [0.0, 0.0]), DepthCut(max_k_position=2)) == 1.0
        assert kernel_fraction(_ledger([1], [0.0]), DepthCut(max_k_position=2)) == 0.5

    def test_no_ledger_and_no_depth_have_none(self) -> None:
        assert kernel_fraction({"donor_k_positions": None}, DepthCut(max_k_position=2)) is None
        assert kernel_fraction(_ledger([1], [0.1]), DepthCut(max_k_position=0)) is None


#: Protein Q6: donors A and B share one sequence (one point of the space, at
#: 0.10), C has another (0.30). T1 comes from A and C, T2 from B.
_SHARED = {
    "T1": _ledger([1, 3], [0.10, 0.30], seq=[1, 2]),
    "T2": _ledger([2], [0.10], seq=[1]),
}


class TestADepthInSequences:
    def test_a_sequence_is_one_neighbour(self) -> None:
        """At two sequences the neighbourhood is 0.95 + 0.85 = 1.80, and T1
        carries both."""
        cut = DepthCut(max_sequence_rank=2)
        votes = similarity_votes(list(_SHARED.values()), cut, "Q6")
        assert votes == pytest.approx([1.0, 0.95 / 1.80])
        assert kernel_fraction(_SHARED["T1"], cut) == pytest.approx(
            (_kernel(0.10) + _kernel(0.30)) / 2
        )
        assert kernel_fraction(_SHARED["T2"], cut) == pytest.approx(_kernel(0.10) / 2)

    def test_the_same_number_in_proteins_is_another_neighbourhood(self) -> None:
        """At two proteins the neighbourhood is A and B, 0.95 + 0.95."""
        votes = similarity_votes(list(_SHARED.values()), DepthCut(max_k_position=2), "Q6")
        assert votes == pytest.approx([0.5, 0.5])


def _frame(rows: dict[str, dict[str, Any]], protein: str, aspect: str) -> list[dict[str, Any]]:
    return [
        {"protein_accession": protein, "go_id": term, "aspect": aspect, "distance": 0.0, **ledger}
        for term, ledger in rows.items()
    ]


class TestTheFrame:
    def _q1(self, depth: int) -> pd.DataFrame:
        """Q1's rows that survive a cut at ``depth``: the shallowest donor within it."""
        records = _frame(_Q1_F, "Q1", "F") + _frame(_Q1_P, "Q1", "P")
        kept = [r for r in records if min(r["donor_k_positions"]) <= depth]
        return pd.DataFrame(kept)

    def test_each_aspect_is_its_own_neighbourhood(self) -> None:
        request = NeighbourhoodRequest(frozenset({SIMILARITY_VOTE}), by_aspect=True)
        out = recount_frame_aggregates(self._q1(2), DepthCut(max_k_position=2), request)
        votes = dict(zip(out["go_id"], out[SIMILARITY_VOTE], strict=True))
        assert votes == pytest.approx(
            {
                "T1": 1.0,
                "T2": 0.95 / 1.85,
                "T3": 0.90 / 1.85,
                "T5": 0.80 / 1.50,
                "T6": 0.70 / 1.50,
            }
        )

    def test_one_list_for_every_aspect_is_one_neighbourhood(self) -> None:
        """A unified search ranks one list: T1 (F) and T5 (P) share Q2's
        neighbourhood, 1.0 + 0.9 + 0.8 = 2.7, which neither aspect holds alone."""
        records = [
            {
                "protein_accession": "Q2",
                "go_id": "T1",
                "aspect": "F",
                **_ledger([1, 3], [0.0, 0.4]),
            },
            {"protein_accession": "Q2", "go_id": "T5", "aspect": "P", **_ledger([2], [0.2])},
        ]
        cut = DepthCut(max_k_position=3)
        unified = NeighbourhoodRequest(frozenset({SIMILARITY_VOTE}), by_aspect=False)
        out = recount_frame_aggregates(pd.DataFrame(records), cut, unified)
        assert list(out[SIMILARITY_VOTE]) == pytest.approx([1.8 / 2.7, 0.9 / 2.7])
        by_aspect = NeighbourhoodRequest(frozenset({SIMILARITY_VOTE}), by_aspect=True)
        with pytest.raises(NeighbourhoodIncomplete, match="protein Q2 in aspect F"):
            recount_frame_aggregates(pd.DataFrame(records), cut, by_aspect)

    def test_both_signals_and_no_ledger_left(self) -> None:
        request = NeighbourhoodRequest(
            frozenset({SIMILARITY_VOTE, KERNEL_FRACTION}), by_aspect=True
        )
        out = recount_frame_aggregates(self._q1(4), DepthCut(max_k_position=4), request)
        kernels = dict(zip(out["go_id"], out[KERNEL_FRACTION], strict=True))
        assert kernels["T4"] == pytest.approx(_kernel(0.30) / 4)
        assert kernels["T6"] == pytest.approx(_kernel(0.60) / 4)
        assert not [c for c in LEDGER_SELECT_COLS if c in out.columns]

    def test_unasked_signals_are_not_computed(self) -> None:
        """A frame with a hole scores as before when nothing weights the similarity vote."""
        frame = pd.DataFrame(_frame({"T": _ledger([1, 3], [0.1, 0.3])}, "Q", "F"))
        out = recount_frame_aggregates(frame, DepthCut(max_k_position=3))
        assert SIMILARITY_VOTE not in out.columns and KERNEL_FRACTION not in out.columns
        kernel_only = NeighbourhoodRequest(frozenset({KERNEL_FRACTION}), by_aspect=None)
        out = recount_frame_aggregates(frame, DepthCut(max_k_position=3), kernel_only)
        assert SIMILARITY_VOTE not in out.columns
        assert out[KERNEL_FRACTION].iloc[0] == pytest.approx((_kernel(0.1) + _kernel(0.3)) / 3)

    def test_no_cut_adds_nothing(self) -> None:
        request = NeighbourhoodRequest(frozenset({KERNEL_FRACTION}), by_aspect=True)
        out = recount_frame_aggregates(self._q1(4), None, request)
        assert KERNEL_FRACTION not in out.columns
        assert not [c for c in DONOR_LEDGER_COLS if c in out.columns]

    def test_an_empty_frame_under_a_cut_drops_what_the_cut_fetched(self) -> None:
        columns = ["protein_accession", "go_id", "distance", *LEDGER_SELECT_COLS]
        request = NeighbourhoodRequest(frozenset({SIMILARITY_VOTE}), by_aspect=True)
        out = recount_frame_aggregates(
            pd.DataFrame(columns=columns), DepthCut(max_k_position=2), request
        )
        assert out.empty
        assert list(out.columns) == ["protein_accession", "go_id", "distance"]


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[Any, ...]]:
        return self._rows


class _Session:
    """Hands back whatever the SELECT builder built, as rows."""

    def execute(self, rows: list[tuple[Any, ...]]) -> _Result:
        return _Result(rows)


class TestTheFrameBuilder:
    _BASE = ("protein_accession", "go_id", "distance")

    def test_the_select_adds_the_ledger_then_the_aspect(self) -> None:
        """In the order the frame names them, or ranks and distances would swap."""
        assert ledger_columns(False) == ()
        assert [column.key for column in ledger_columns(True)] == list(LEDGER_SELECT_COLS)

    def _rows(self, depth: int) -> list[tuple[Any, ...]]:
        """Q1's candidates within ``depth``, in the SELECT's column order."""
        rows = []
        for term, (_term_id, aspect, ledger) in _TERMS.items():
            if min(ledger["donor_k_positions"]) <= depth:
                ledger_values = tuple(ledger[c] for c in DONOR_LEDGER_COLS)
                rows.append(("Q1", term, min(ledger["donor_distances"]), *ledger_values, aspect))
        return rows

    def test_under_a_cut_it_reads_the_ledger_and_the_aspect_and_passes_the_request_on(
        self,
    ) -> None:
        asked: list[bool] = []

        def select_of(_ctx: Any, *, with_ledger: bool) -> list[tuple[Any, ...]]:
            asked.append(with_ledger)
            return self._rows(2)

        ctx = MagicMock(
            max_k_position=2,
            max_sequence_rank=None,
            neighbourhood=NeighbourhoodRequest(frozenset({SIMILARITY_VOTE}), by_aspect=True),
        )
        frame, raw = build_base_frame(_Session(), ctx, select_of, self._BASE)
        assert asked == [True]
        assert raw == 5
        votes = dict(zip(frame["go_id"], frame[SIMILARITY_VOTE], strict=True))
        assert votes == pytest.approx(
            {"T1": 1.0, "T2": 0.95 / 1.85, "T3": 0.90 / 1.85, "T5": 0.80 / 1.50, "T6": 0.70 / 1.50}
        )
        assert not [c for c in LEDGER_SELECT_COLS if c in frame.columns]

    def test_a_context_without_a_request_adds_no_signal(self) -> None:
        ctx = MagicMock(spec=["max_k_position", "max_sequence_rank"])
        ctx.max_k_position, ctx.max_sequence_rank = 2, None
        frame, _raw = build_base_frame(_Session(), ctx, lambda _c, **_k: self._rows(2), self._BASE)
        assert SIMILARITY_VOTE not in frame.columns and KERNEL_FRACTION not in frame.columns
        assert list(frame["donor_count"]) == [2, 1, 1, 1, 1]


class TestTheOperationChecksBeforeItWrites:
    """``score_prediction_set`` asks what it can read before it creates the scored set."""

    @pytest.fixture(autouse=True)
    def _no_database(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(scoring_operation, "compute_missing_signals", lambda *a, **k: [])
        monkeypatch.setattr(
            scoring_operation, "assert_depth_unit_is_available", lambda *a, **k: None
        )

    @staticmethod
    def _source(meta: dict[str, Any]) -> Any:
        return MagicMock(id=uuid.uuid4(), meta=meta)

    @staticmethod
    def _config(weights: dict[str, float]) -> ScoringConfig:
        return ScoringConfig(id=uuid.uuid4(), name="votes", formula="linear", weights=weights)

    @staticmethod
    def _payload(**depth: Any) -> Any:
        return scoring_operation.ScorePredictionSetPayload(
            prediction_set_id=str(uuid.uuid4()), scoring_config_id=str(uuid.uuid4()), **depth
        )

    def test_it_returns_what_the_configuration_weights(self) -> None:
        got = scoring_operation.ScorePredictionSetOperation._what_it_can_read(
            MagicMock(),
            self._source({"aspect_separated_knn": True}),
            self._config({SIMILARITY_VOTE: 1.0}),
            self._payload(max_k_position=5),
        )
        assert got == NeighbourhoodRequest(frozenset({SIMILARITY_VOTE}), by_aspect=True)

    def test_a_refusal_names_the_configuration_and_the_set(self) -> None:
        source, config = self._source({}), self._config({KERNEL_FRACTION: 1.0})
        with pytest.raises(
            ValueError,
            match=rf"scoring config {config.id} \('votes'\) over prediction set {source.id}: "
            ".*at a depth",
        ):
            scoring_operation.ScorePredictionSetOperation._what_it_can_read(
                MagicMock(), source, config, self._payload()
            )

    def test_a_signal_the_set_does_not_carry_is_refused_first(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            scoring_operation,
            "compute_missing_signals",
            lambda *a, **k: ["neighbor_kernel_fraction (column 'donor_distances': 0/3 rows)"],
        )
        with pytest.raises(ValueError, match="does not carry: neighbor_kernel_fraction"):
            scoring_operation.ScorePredictionSetOperation._what_it_can_read(
                MagicMock(), self._source({}), self._config({KERNEL_FRACTION: 1.0}), self._payload()
            )


class TestWhatAConfigurationAsksFor:
    _CUT = DepthCut(max_k_position=5)

    def test_nothing_when_it_weights_neither(self) -> None:
        weights = {"neighbor_vote_fraction": 1.0, SIMILARITY_VOTE: 0.0}
        assert neighbourhood_request(weights, {}, self._CUT) is None

    def test_both_are_read_at_a_named_depth(self) -> None:
        with pytest.raises(ValueError, match="read over the neighbourhood at a depth"):
            neighbourhood_request({KERNEL_FRACTION: 1.0}, {"aspect_separated_knn": True}, None)

    def test_the_vote_needs_the_record_of_how_neighbours_were_ranked(self) -> None:
        with pytest.raises(ValueError, match="aspect_separated_knn"):
            neighbourhood_request({SIMILARITY_VOTE: 1.0}, {"metric": "cosine"}, self._CUT)

    def test_the_kernel_does_not(self) -> None:
        got = neighbourhood_request({KERNEL_FRACTION: 1.0}, None, self._CUT)
        assert got == NeighbourhoodRequest(frozenset({KERNEL_FRACTION}), by_aspect=None)

    @pytest.mark.parametrize("separated", [True, False])
    def test_the_record_says_whose_neighbourhood_it_is(self, separated: bool) -> None:
        meta = {"aspect_separated_knn": separated}
        got = neighbourhood_request({SIMILARITY_VOTE: 0.5, KERNEL_FRACTION: 0.5}, meta, self._CUT)
        assert got == NeighbourhoodRequest(
            frozenset({SIMILARITY_VOTE, KERNEL_FRACTION}), by_aspect=separated
        )


def _scored_row(**values: Any) -> dict[str, Any]:
    row = dict.fromkeys(BASE_SCORE_COLS)
    row.update(values)
    return row


class TestTheTwoScorersAgree:
    def test_on_the_two_signals_mixed_with_another(self) -> None:
        rows = [
            _scored_row(
                protein_accession="Q", go_id="T1", distance=0.2,
                **{SIMILARITY_VOTE: 0.7, KERNEL_FRACTION: 0.4},
            ),
            _scored_row(
                protein_accession="Q", go_id="T2", distance=0.6,
                **{SIMILARITY_VOTE: None, KERNEL_FRACTION: 0.1},
            ),
        ]  # fmt: skip
        config = ScoringConfig(
            formula="linear",
            weights={"embedding_similarity": 0.5, SIMILARITY_VOTE: 0.3, KERNEL_FRACTION: 0.2},
            evidence_weights=None,
        )
        frame = pd.DataFrame(rows)
        vectorised = [f"{s:.6f}" for s in vectorized_scores(frame, config)]
        scalar = [f"{compute_score(dict(r), config):.6f}" for r in rows]
        assert vectorised == scalar
        assert scalar == [
            f"{(0.5 * 0.9 + 0.3 * 0.7 + 0.2 * 0.4) / 1.0:.6f}",
            f"{(0.5 * 0.7 + 0.2 * 0.1) / 0.7:.6f}",
        ]

    def test_a_weighed_signal_the_frame_lacks_is_refused(self) -> None:
        frame = pd.DataFrame([_scored_row(protein_accession="Q", go_id="T", distance=0.2)])
        config = ScoringConfig(formula="linear", weights={SIMILARITY_VOTE: 1.0})
        with pytest.raises(ValueError, match="built without it"):
            vectorized_scores(frame, config)

    def test_an_unweighed_one_it_lacks_is_not(self) -> None:
        frame = pd.DataFrame([_scored_row(protein_accession="Q", go_id="T", distance=0.2)])
        config = ScoringConfig(formula="linear", weights={"embedding_similarity": 1.0})
        assert list(vectorized_scores(frame, config)) == pytest.approx([0.9])


class _CountingSession:
    """Answers the coverage query, and keeps its text."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, statement: Any, _params: Any) -> Any:
        self.sql.append(str(statement))
        answer = MagicMock()
        answer.mappings.return_value.one.return_value = {"total": 3, "cnt_donor_distances": 3}
        return answer


class TestTheirCoverageIsTheLedgers:
    def test_both_read_the_ledgers_distances(self) -> None:
        assert _SIGNAL_TO_COLUMN[SIMILARITY_VOTE] == "donor_distances"
        assert _SIGNAL_TO_COLUMN[KERNEL_FRACTION] == "donor_distances"

    def test_weighing_both_counts_the_column_once(self) -> None:
        session = _CountingSession()
        config = ScoringConfig(
            formula="linear", weights={SIMILARITY_VOTE: 1.0, KERNEL_FRACTION: 1.0}
        )
        assert compute_missing_signals(session, uuid.uuid4(), config) == []  # type: ignore[arg-type]
        assert session.sql[0].count("COUNT(donor_distances)") == 1

    def test_they_are_signals_a_configuration_may_name(self) -> None:
        assert DEFAULT_WEIGHTS[SIMILARITY_VOTE] == 0.0
        assert DEFAULT_WEIGHTS[KERNEL_FRACTION] == 0.0


class TestThePresets:
    def test_one_preset_per_signal(self) -> None:
        assert [p["name"] for p in NEIGHBOURHOOD_VOTE_PRESETS] == [
            "similarity_vote",
            "kernel_fraction",
        ]
        assert [p["weights"] for p in NEIGHBOURHOOD_VOTE_PRESETS] == [
            {SIMILARITY_VOTE: 1.0},
            {KERNEL_FRACTION: 1.0},
        ]
        for preset in NEIGHBOURHOOD_VOTE_PRESETS:
            assert preset["formula"] == "linear"
            assert preset["description"]

    def test_each_is_a_body_the_create_endpoint_validates(self) -> None:
        """Read from this module and sent as JSON, as a deploy sends them."""
        for preset in NEIGHBOURHOOD_VOTE_PRESETS:
            body = json.loads(json.dumps(preset))
            assert ScoringConfigCreate.model_validate(body).model_dump(exclude_unset=True) == body

    def test_the_create_endpoint_creates_each(self) -> None:
        from protea.api.routers.scoring import router

        session = MagicMock()

        def added(obj: Any) -> None:
            obj.id = uuid.uuid4()
            obj.created_at = "2026-11-28T00:00:00+00:00"

        session.add.side_effect = added

        @contextmanager
        def scope(_factory: Any) -> Any:
            yield session

        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        with patch("protea.api.routers.scoring.session_scope", side_effect=scope):
            client = TestClient(app, raise_server_exceptions=True)
            for preset in NEIGHBOURHOOD_VOTE_PRESETS:
                response = client.post("/scoring/configs", json=preset)
                assert response.status_code == 201, response.text
                assert response.json()["weights"] == preset["weights"]

    def test_the_seed_all_endpoint_never_creates_them(self) -> None:
        """It creates every preset an instance lacks, the deleted ones too, so
        these stay out of its list."""
        from protea.services.scoring_service import create_preset_configs_data

        session = MagicMock()
        session.query.return_value.all.return_value = []
        created = create_preset_configs_data(session)
        assert created == [p["name"] for p in PRESET_CONFIGS]
        assert {"similarity_vote", "kernel_fraction"}.isdisjoint(created)


# ---------------------------------------------------------------------------
# Through score_prediction_set, against Postgres.
# ---------------------------------------------------------------------------

#: Q1's terms: (go_term id, aspect, ledger). Their GO ids are GO:0009201...
_TERMS = {
    "T1": (9201, "F", _Q1_F["T1"]),
    "T2": (9202, "F", _Q1_F["T2"]),
    "T3": (9203, "F", _Q1_F["T3"]),
    "T4": (9204, "F", _Q1_F["T4"]),
    "T5": (9205, "P", _Q1_P["T5"]),
    "T6": (9206, "P", _Q1_P["T6"]),
}


@pytest.fixture()
def pg(postgres_url: str) -> Any:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    engine.dispose()


def _seed(session: Session, meta: dict[str, Any]) -> dict[str, Any]:
    from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
    from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
    from tests.helpers.prediction_sets import make_parents

    session.execute(text("SET session_replication_role = 'replica'"))
    parents = make_parents(session)
    for term_id, aspect, _ledger_of in _TERMS.values():
        session.execute(
            text(
                "INSERT INTO go_term (id, go_id, name, aspect, is_obsolete, ontology_snapshot_id) "
                "VALUES (:i, :g, :g, :a, false, :s)"
            ),
            {"i": term_id, "g": f"GO:000{term_id}", "a": aspect, "s": parents["snapshot"]},
        )
    source = PredictionSet(
        embedding_config_id=parents["embedding"],
        annotation_set_id=parents["annotation"],
        ontology_snapshot_id=parents["snapshot"],
        limit_per_entry=30,
        meta=meta,
    )
    session.add(source)
    session.flush()
    for term_id, _aspect, ledger in _TERMS.values():
        k = ledger["donor_k_positions"]
        session.add(
            GOPrediction(
                prediction_set_id=source.id,
                protein_accession="Q1",
                go_term_id=term_id,
                ref_protein_accession=f"D{k[0]}",
                distance=min(ledger["donor_distances"]),
                evidence_code="IDA",
                k_position=min(k),
                sequence_rank=min(k),
                neighbor_vote_fraction=len(k) / 30,
                donor_count=len(k),
                donor_accessions=[f"D{i}" for i in k],
                **ledger,
            )
        )
    configs = {preset["name"]: ScoringConfig(**preset) for preset in NEIGHBOURHOOD_VOTE_PRESETS}
    session.add_all(configs.values())
    session.flush()
    return {"source": source.id, "configs": configs}


def _score(session: Session, seeded: dict[str, Any], preset: str, **over: Any) -> dict[int, float]:
    from protea.core.operations.score_prediction_set import ScorePredictionSetOperation
    from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore

    payload = {
        "prediction_set_id": str(seeded["source"]),
        "scoring_config_id": str(seeded["configs"][preset].id),
        "_job_id": str(uuid.uuid4()),
        **over,
    }
    result = ScorePredictionSetOperation().execute(session, payload, emit=lambda *a, **k: None)
    scored = uuid.UUID(result.result["prediction_set_id"])
    row = session.execute(
        select(PredictionScore).where(PredictionScore.prediction_set_id == scored)
    ).scalar_one()
    return dict(zip(row.go_term_ids, row.scores, strict=True))


@pytest.mark.integration
class TestScoringACandidateSet:
    def test_the_similarity_vote_at_depth_two(self, pg: Session) -> None:
        seeded = _seed(pg, {"aspect_separated_knn": True})
        got = _score(pg, seeded, "similarity_vote", max_k_position=2)
        assert got == pytest.approx(
            {9201: 1.0, 9202: 0.95 / 1.85, 9203: 0.90 / 1.85, 9205: 0.80 / 1.50, 9206: 0.70 / 1.50}
        )

    def test_the_kernel_fraction_at_depth_four(self, pg: Session) -> None:
        seeded = _seed(pg, {"aspect_separated_knn": True})
        got = _score(pg, seeded, "kernel_fraction", max_k_position=4)
        assert got == pytest.approx(
            {
                9201: (_kernel(0.10) + _kernel(0.20)) / 4,
                9202: (_kernel(0.10) + _kernel(0.30)) / 4,
                9203: (_kernel(0.20) + _kernel(0.50)) / 4,
                9204: _kernel(0.30) / 4,
                9205: _kernel(0.40) / 4,
                9206: _kernel(0.60) / 4,
            }
        )

    def test_no_depth_is_refused_before_anything_is_written(self, pg: Session) -> None:
        seeded = _seed(pg, {"aspect_separated_knn": True})
        with pytest.raises(ValueError, match="read over the neighbourhood at a depth"):
            _score(pg, seeded, "kernel_fraction")
        assert pg.execute(text("SELECT count(*) FROM prediction_set")).scalar_one() == 1

    def test_a_set_that_does_not_say_how_it_ranked_is_refused_for_the_vote(
        self, pg: Session
    ) -> None:
        seeded = _seed(pg, {"metric": "cosine"})
        with pytest.raises(ValueError, match="aspect_separated_knn"):
            _score(pg, seeded, "similarity_vote", max_k_position=2)
        assert _score(pg, seeded, "kernel_fraction", max_k_position=2)

    def test_a_neighbour_that_left_no_candidate_stops_the_vote(self, pg: Session) -> None:
        """Q1's neighbour 2 in F is named by T1 and T3 only. With T3 gone and T1
        donated by 1 and 3, no candidate names it while the ledgers reach 3, as
        when every term it donated was a denial of the query."""
        seeded = _seed(pg, {"aspect_separated_knn": True})
        pg.execute(
            text(
                "UPDATE go_prediction SET donor_k_positions = '{1,3}', "
                "donor_sequence_ranks = '{1,3}', donor_distances = '{0.1,0.3}' "
                "WHERE go_term_id = 9201"
            )
        )
        pg.execute(text("DELETE FROM go_prediction WHERE go_term_id = 9203"))
        with pytest.raises(NeighbourhoodIncomplete, match=r"protein Q1 in aspect F.*\[2\]"):
            _score(pg, seeded, "similarity_vote", max_k_position=4)
        assert _score(pg, seeded, "kernel_fraction", max_k_position=4)
