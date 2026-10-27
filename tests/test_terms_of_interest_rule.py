"""Which terms cafaeval is told to score, and where that answer comes from.

The terms of interest decide which terms count toward precision and recall at
all, so two runs under different rules are scoring different quantities. Until
now there was one rule and it was implicit: every GO id under the pivot
snapshot, obsolete ones included. That is wrong in two separate ways for a
temporal window, and both show up on the 211 -> 227 validation window:

* it keeps obsolete terms. The pivot carries 47,328 of which 3,770 are
  obsolete, and cafaeval's own ``obo_parser`` drops obsolete terms from the
  graphs it builds, so the scorer was handed a universe 3,770 terms wider than
  anything it could score;
* it consults one snapshot. A term obsolete by t1 was scored as though it
  still existed.

The lab's rule is the ids non-obsolete in BOTH ends. On that window the two
rules are 43,558 terms against 39,028, and the 8,244 between them were the
whole of an unexplained pair surplus.

THE TRAP THESE TESTS EXIST FOR. ``EvaluationSet`` has
``old_native_snapshot_id`` and ``new_native_snapshot_id``, which look like the
obvious place to read the window's two ontologies from, and are the wrong one.
Those fields do two jobs at once: they say which graph closes each side, and
they are the only other record of which ontologies the window spans. Asking
for closure under t0's graph alone -- supported, and the request that made the
long window's PK count fall from 43,336 to 19,311 -- means setting both to t0,
which erases t1. The 211 -> 227 window has had that done: both fields read
``releases/2022-07-01`` while the annotation sets still read
``releases/2022-07-01`` and ``releases/2025-07-22``. A rule reading the native
fields would quietly degrade to the single-snapshot case on exactly the window
it was written for, while reporting that it had consulted two.

So the rule reads each end's ``AnnotationSet.ontology_snapshot_id``, fixed
when that corpus was loaded and independent of any later closure choice, and
:func:`test_the_rule_ignores_the_native_snapshot_fields` is the regression
that keeps it that way.

These test which fields are consulted and how the two sides combine, not SQL.
The DB round trip needs Postgres (``--with-postgres``) and a live window;
nothing here needs either, so the checks run everywhere.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.operations import _run_cafa_setup as setup
from protea.core.operations._run_cafa_setup import (
    TOI_RULES,
    _load_terms_of_interest,
    _window_end_snapshots,
)
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload

PIVOT = uuid.uuid4()
OLD_CORPUS_SNAPSHOT = uuid.uuid4()
NEW_CORPUS_SNAPSHOT = uuid.uuid4()
#: What the native fields say on a window closed under t0 alone: both ends
#: collapsed onto the old one.
COLLAPSED_NATIVE = OLD_CORPUS_SNAPSHOT


class _AnnotationSet:
    def __init__(self, snapshot_id: uuid.UUID) -> None:
        self.ontology_snapshot_id = snapshot_id


class _EvaluationSet:
    """A window whose native fields are collapsed, as the real one's are."""

    def __init__(self, *, old: uuid.UUID | None, new: uuid.UUID | None) -> None:
        self.id = uuid.uuid4()
        self.old_annotation_set_id = old
        self.new_annotation_set_id = new
        self.old_native_snapshot_id = COLLAPSED_NATIVE
        self.new_native_snapshot_id = COLLAPSED_NATIVE


class _Query:
    def __init__(self, rows: list[str]) -> None:
        self._rows = rows
        self.filtered = 0

    def filter(self, *_criteria: object) -> _Query:
        self.filtered += 1
        return self

    def all(self) -> list[tuple[str]]:
        return [(row,) for row in self._rows]


class _Session:
    """Answers the two things the loader asks of a session, and records both."""

    def __init__(self, pivot_rows: list[str], sets: dict[uuid.UUID, _AnnotationSet]) -> None:
        self._pivot_rows = pivot_rows
        self._sets = sets
        self.queries: list[_Query] = []
        self.fetched: list[uuid.UUID | None] = []

    def query(self, *_columns: object) -> _Query:
        q = _Query(self._pivot_rows)
        self.queries.append(q)
        return q

    def get(self, _model: type, ident: uuid.UUID | None) -> _AnnotationSet | None:
        self.fetched.append(ident)
        return self._sets.get(ident)


@pytest.fixture
def window() -> _EvaluationSet:
    return _EvaluationSet(old=uuid.uuid4(), new=uuid.uuid4())


@pytest.fixture
def session(window: _EvaluationSet) -> _Session:
    return _Session(
        pivot_rows=["GO:0000003", "GO:0000001", "GO:0000002"],
        sets={
            window.old_annotation_set_id: _AnnotationSet(OLD_CORPUS_SNAPSHOT),
            window.new_annotation_set_id: _AnnotationSet(NEW_CORPUS_SNAPSHOT),
        },
    )


@pytest.fixture
def live(monkeypatch: pytest.MonkeyPatch) -> dict[uuid.UUID, set[str]]:
    """Each snapshot's non-obsolete ids, and a record of which were asked for."""
    table = {
        OLD_CORPUS_SNAPSHOT: {"GO:0000001", "GO:0000002", "GO:0000004"},
        NEW_CORPUS_SNAPSHOT: {"GO:0000002", "GO:0000004", "GO:0000009"},
        # Deliberately different, so a rule reading the collapsed native
        # fields twice would produce this set and not the intersection.
        PIVOT: {"GO:0000777"},
    }
    asked: list[uuid.UUID] = []

    def _fake(_session: object, snapshot_id: uuid.UUID) -> set[str]:
        asked.append(snapshot_id)
        return table[snapshot_id]

    monkeypatch.setattr(setup, "_live_go_ids", _fake)
    table["asked"] = asked  # type: ignore[index]
    return table


class TestTheDefaultRuleIsUnchanged:
    def test_the_default_is_the_pivot_universe(self) -> None:
        payload = RunCafaEvaluationPayload(
            evaluation_set_id=str(uuid.uuid4()), prediction_set_id=str(uuid.uuid4())
        )
        assert payload.toi_rule == "pivot_terms"
        assert TOI_RULES[0] == "pivot_terms"

    def test_the_pivot_rule_returns_every_term_of_the_pivot(self, session: _Session) -> None:
        got = _load_terms_of_interest(session, PIVOT)
        assert got == ["GO:0000001", "GO:0000002", "GO:0000003"]

    def test_the_pivot_rule_needs_no_window(self, session: _Session) -> None:
        # The signature grew two optional arguments; the old call must keep
        # working, because every stored result was produced by one.
        assert _load_terms_of_interest(session, PIVOT, None, None)

    def test_the_result_is_sorted(self, session: _Session) -> None:
        # The query has no ORDER BY, so the file written from it was a
        # function of the plan. Nothing downstream happened to care, which is
        # the only reason it was harmless.
        got = _load_terms_of_interest(session, PIVOT)
        assert got == sorted(got)


class TestTheLiveIntersectionRule:
    def test_it_returns_the_sorted_intersection_of_both_ends(
        self, session: _Session, window: _EvaluationSet, live: dict
    ) -> None:
        got = _load_terms_of_interest(session, PIVOT, window, "live_in_both_ends")
        assert got == ["GO:0000002", "GO:0000004"]

    def test_the_rule_ignores_the_native_snapshot_fields(
        self, session: _Session, window: _EvaluationSet, live: dict
    ) -> None:
        # The regression. Both native fields are collapsed onto the old
        # snapshot, as the real 211 -> 227 window's are. A rule reading them
        # would ask for OLD twice and return OLD's own live set; a rule
        # reading the annotation sets asks for one of each.
        _load_terms_of_interest(session, PIVOT, window, "live_in_both_ends")
        assert live["asked"] == [OLD_CORPUS_SNAPSHOT, NEW_CORPUS_SNAPSHOT]
        assert window.old_native_snapshot_id == window.new_native_snapshot_id

    def test_it_reads_the_two_annotation_sets_and_nothing_else(
        self, session: _Session, window: _EvaluationSet, live: dict
    ) -> None:
        _load_terms_of_interest(session, PIVOT, window, "live_in_both_ends")
        assert session.fetched == [
            window.old_annotation_set_id,
            window.new_annotation_set_id,
        ]

    def test_it_never_returns_the_pivot_universe(
        self, session: _Session, window: _EvaluationSet, live: dict
    ) -> None:
        # The pivot's own live set is disjoint from both ends' here, so a
        # result containing it would mean the rule fell back to the default.
        got = _load_terms_of_interest(session, PIVOT, window, "live_in_both_ends")
        assert "GO:0000777" not in got

    def test_the_intersection_is_no_wider_than_either_end(
        self, session: _Session, window: _EvaluationSet, live: dict
    ) -> None:
        got = set(_load_terms_of_interest(session, PIVOT, window, "live_in_both_ends"))
        assert got <= live[OLD_CORPUS_SNAPSHOT]
        assert got <= live[NEW_CORPUS_SNAPSHOT]


class TestItRefusesRatherThanGuesses:
    def test_an_unknown_rule_raises(self, session: _Session) -> None:
        with pytest.raises(ValueError, match="unknown toi_rule"):
            _load_terms_of_interest(session, PIVOT, None, "whatever")

    def test_the_payload_refuses_an_unknown_rule(self) -> None:
        with pytest.raises(ValueError):
            RunCafaEvaluationPayload(
                evaluation_set_id=str(uuid.uuid4()),
                prediction_set_id=str(uuid.uuid4()),
                toi_rule="whatever",
            )

    def test_the_payload_accepts_every_rule_the_loader_knows(self) -> None:
        # Pinned to the loader's own tuple, so a rule cannot be added that no
        # payload can ask for, nor asked for by a name the loader rejects.
        for rule in TOI_RULES:
            payload = RunCafaEvaluationPayload(
                evaluation_set_id=str(uuid.uuid4()),
                prediction_set_id=str(uuid.uuid4()),
                toi_rule=rule,
            )
            assert payload.toi_rule == rule

    def test_the_two_end_rule_without_a_window_raises(self, session: _Session) -> None:
        with pytest.raises(ValueError, match="cannot be resolved"):
            _load_terms_of_interest(session, PIVOT, None, "live_in_both_ends")

    def test_a_window_whose_annotation_set_is_gone_raises(self, live: dict) -> None:
        orphan = _EvaluationSet(old=uuid.uuid4(), new=None)
        empty = _Session(pivot_rows=[], sets={})
        with pytest.raises(ValueError, match="names an AnnotationSet that is gone"):
            _window_end_snapshots(empty, orphan)


class TestTheRuleReachesTheSeal:
    def test_the_two_rules_do_not_share_a_frame(self) -> None:
        # A rule that changes the scored term set and is absent from the seal
        # would let 43,558 terms and 39,028 terms be published as one frame.
        from protea.core.operations.seal_evaluation_frames import frame_digest

        base = {
            "evaluation_set_id": "e",
            "pivot_snapshot_id": "p",
            "information_accretion_set_id": "i",
        }
        seals = {frame_digest({**base, "toi_rule": rule}) for rule in TOI_RULES}
        assert len(seals) == len(TOI_RULES)

    def test_an_absent_rule_seals_as_the_default(self) -> None:
        from protea.core.operations.seal_evaluation_frames import frame_digest

        base = {
            "evaluation_set_id": "e",
            "pivot_snapshot_id": "p",
            "information_accretion_set_id": "i",
        }
        assert frame_digest(base) == frame_digest({**base, "toi_rule": TOI_RULES[0]})

    def test_the_seal_names_the_rule(self) -> None:
        from protea.core.operations.seal_evaluation_frames import _FRAME_FIELDS, _ROWS

        assert "toi_rule" in _FRAME_FIELDS
        assert "AS toi_rule" in str(_ROWS)
