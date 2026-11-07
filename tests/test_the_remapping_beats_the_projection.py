"""What a t1 term is called at t0, and why that question comes first.

An annotation carries a GO id, and an id is not a term. Between the two ends
of this window GO merged terms, obsoleted them, named successors, and twice
REVIVED an id it had obsoleted, so "does this t1 term have a node at t0"
cannot be answered by looking the id up in t0's vocabulary.

THE ORDER IS THE POINT. The remapping runs before the projection and wins: a
term the ontology itself renamed is not a term to be generalised. PROTEA had
it the other way round and the difference was measurable -- nine entries
projected to their ancestors instead of being replaced by the successor GO
published, which put three extra proteins in the delta out of 40,531
(``A2AF31``, ``P62962``, ``Q8BPT6``).

Every clause below is copied from the lab's ``build_remap``, and every one of
them is a place where a reasonable alternative gives a different truth.
"""

from __future__ import annotations

import inspect
import random

from protea.core._truth_remap import Remap, apply_remap, build_remap
from protea.infrastructure.orm.models.annotation.go_term_alias import (
    KIND_ALT_ID,
    KIND_CONSIDER,
    KIND_REPLACED_BY,
)

#: ``(alias, primary, kind)`` as ``go_term_alias`` holds them.
ROWS: list[tuple[str, str, str]] = [
    # a secondary id of a live term
    ("GO:OLD1", "GO:LIVE1", KIND_ALT_ID),
    # an obsolete term with one live successor
    ("GO:DEAD1", "GO:LIVE2", KIND_REPLACED_BY),
    # an obsolete term with two successors
    ("GO:DEAD2", "GO:LIVE1", KIND_REPLACED_BY),
    ("GO:DEAD2", "GO:LIVE2", KIND_REPLACED_BY),
    # an obsolete term whose successor is obsolete AND a secondary id
    ("GO:DEAD3", "GO:GONE", KIND_REPLACED_BY),
    ("GO:GONE", "GO:LIVE3", KIND_ALT_ID),
    # an obsolete term whose successor is obsolete and nothing else
    ("GO:DEAD4", "GO:ALSOGONE", KIND_REPLACED_BY),
    # the ontology declining to name a successor
    ("GO:DEAD5", "GO:LIVE1", KIND_CONSIDER),
    # a term that is BOTH a secondary id and an obsolete with a successor
    ("GO:BOTH", "GO:LIVE1", KIND_ALT_ID),
    ("GO:BOTH", "GO:LIVE2", KIND_REPLACED_BY),
    # a secondary id whose term is obsolete at t0
    ("GO:OLD2", "GO:ALSOGONE", KIND_ALT_ID),
]
LIVE = {"GO:LIVE1", "GO:LIVE2", "GO:LIVE3"}


def _map() -> Remap:
    return build_remap(ROWS, LIVE)


class TestWhichStatementResolvesAnId:
    def test_a_secondary_id_of_a_live_term_is_that_term(self) -> None:
        assert _map().by_alias["GO:OLD1"] == ("GO:LIVE1", KIND_ALT_ID)

    def test_an_obsolete_with_one_live_successor_takes_it(self) -> None:
        assert _map().by_alias["GO:DEAD1"] == ("GO:LIVE2", KIND_REPLACED_BY)

    def test_two_successors_resolve_to_nothing(self) -> None:
        # Taking the first would be picking for the ontology; taking both
        # would turn one annotation into two.
        assert "GO:DEAD2" not in _map().by_alias

    def test_a_successor_that_is_itself_a_secondary_id_is_followed_once(self) -> None:
        assert _map().by_alias["GO:DEAD3"] == ("GO:LIVE3", KIND_REPLACED_BY)

    def test_and_only_once(self) -> None:
        # GO:DEAD4's successor is obsolete and is not a secondary id of
        # anything, so there is no second hop to take.
        assert "GO:DEAD4" not in _map().by_alias

    def test_consider_resolves_to_nothing(self) -> None:
        # The ontology declined to name a successor. Following it would be
        # inventing the decision it declined to make.
        assert "GO:DEAD5" not in _map().by_alias

    def test_a_secondary_id_of_an_obsolete_term_resolves_to_nothing(self) -> None:
        assert "GO:OLD2" not in _map().by_alias

    def test_the_secondary_id_wins_when_both_could_claim_it(self) -> None:
        # alt_id first and one map per id: "this IS that" is a stronger
        # statement than "that is what it became".
        assert _map().by_alias["GO:BOTH"] == ("GO:LIVE1", KIND_ALT_ID)

    def test_what_is_live_is_never_remapped(self) -> None:
        # An id the snapshot still uses is not a synonym of anything, even
        # when some other term lists it as a secondary id.
        rows = [*ROWS, ("GO:LIVE1", "GO:LIVE2", KIND_ALT_ID)]
        assert "GO:LIVE1" not in build_remap(rows, LIVE).by_alias

    def test_the_map_is_a_function_of_the_data_and_not_of_the_row_order(self) -> None:
        # Both loops sort. A map that depended on how the rows came back
        # would make the truth a function of the query plan, which this
        # campaign has already paid for once in the donor bank.
        shuffled = list(ROWS)
        random.Random(0).shuffle(shuffled)
        assert build_remap(shuffled, LIVE).by_alias == _map().by_alias

    def test_the_counts_say_which_half_resolved_what(self) -> None:
        # Three secondary ids resolve, not two: GO:GONE is itself one, of a
        # live term, so it resolves on its own account as well as being the
        # hop GO:DEAD3 takes. The lab's counters are per RESOLVED ID for the
        # same reason, which is why these are worth asserting rather than
        # eyeballing.
        # ``remap_aspect_crossed`` is 0 here and not absent: this fixture
        # carries no aspect map, so the rule resolved the same ids and the
        # counter has nothing to say. It is not a third kind of outcome.
        assert _map().counts == {
            "remap_alt_id": 3,
            "remap_replaced_by": 2,
            "remap_aspect_crossed": 0,
        }


class TestWhatItDoesToTheAnnotations:
    def test_a_live_term_is_left_alone(self) -> None:
        raw = {"P1": {"GO:LIVE1"}}
        report = apply_remap(raw, _map(), LIVE)
        assert raw == {"P1": {"GO:LIVE1"}}
        assert report == report._replace()  # nothing counted
        assert (report.alt_id, report.replaced_by, report.unmapped) == (0, 0, 0)

    def test_a_remappable_term_becomes_its_t0_identity(self) -> None:
        raw = {"P1": {"GO:OLD1"}}
        report = apply_remap(raw, _map(), LIVE)
        assert raw == {"P1": {"GO:LIVE1"}}
        assert (report.alt_id, report.replaced_by, report.proteins) == (1, 0, 1)

    def test_a_successor_is_counted_apart_from_a_synonym(self) -> None:
        raw = {"P1": {"GO:DEAD1"}}
        report = apply_remap(raw, _map(), LIVE)
        assert raw == {"P1": {"GO:LIVE2"}}
        assert (report.alt_id, report.replaced_by) == (0, 1)

    def test_what_nothing_can_place_is_dropped_and_counted(self) -> None:
        raw = {"P1": {"GO:DEAD2"}}
        report = apply_remap(raw, _map(), LIVE)
        assert raw == {"P1": set()}
        assert report.unmapped == 1

    def test_a_spared_term_is_left_for_the_projection(self) -> None:
        # ``spare`` is how the two rules compose: everything remappable is
        # remapped, what is left and projectable is left alone, and only what
        # neither can place is dropped.
        raw = {"P1": {"GO:DEAD2"}}
        report = apply_remap(raw, _map(), LIVE, spare={"GO:DEAD2"})
        assert raw == {"P1": {"GO:DEAD2"}}
        assert report.unmapped == 0

    def test_the_remapping_wins_over_the_projection(self) -> None:
        # THE DEFECT THIS FIXES, in one assertion. GO:DEAD1 is live at t1 and
        # obsolete at t0 with a published successor, so it is in ``spare``
        # too. The remap takes it; the projection never sees it. Nine terms
        # of the long window are exactly this case.
        raw = {"P1": {"GO:DEAD1"}}
        report = apply_remap(raw, _map(), LIVE, spare={"GO:DEAD1"})
        assert raw == {"P1": {"GO:LIVE2"}}
        assert report.replaced_by == 1

    def test_the_other_terms_of_the_protein_survive(self) -> None:
        raw = {"P1": {"GO:OLD1", "GO:LIVE3"}}
        apply_remap(raw, _map(), LIVE)
        assert raw == {"P1": {"GO:LIVE1", "GO:LIVE3"}}

    def test_two_terms_mapping_to_one_collapse(self) -> None:
        # The result is a set of terms, so an annotation that arrives twice
        # under two old names is one annotation.
        raw = {"P1": {"GO:OLD1", "GO:BOTH"}}
        apply_remap(raw, _map(), LIVE)
        assert raw == {"P1": {"GO:LIVE1"}}

    def test_the_report_names_the_side_it_is_about(self) -> None:
        report = apply_remap({"P1": {"GO:OLD1"}}, _map(), LIVE)
        assert set(report.as_dict("new_exp")) == {
            "new_exp_alt_id",
            "new_exp_replaced_by",
            "new_exp_unmapped",
            "new_exp_remapped_proteins",
            "new_exp_aspect_crossed",
        }


class TestWhereItRuns:
    def test_before_the_projection_and_before_the_binding_rule(self) -> None:
        import protea.core.evaluation as ev

        source = inspect.getsource(ev._reconcile_experimental_side)
        assert source.index("apply_remap") < source.index("project_direct_annotations")
        assert source.index("project_direct_annotations") < source.index("_drop_binding_only")

    def test_the_not_side_remaps_and_never_projects(self) -> None:
        # A NOT denies the term and everything under it, so climbing to an
        # ancestor would assert the protein lacks something it may well have.
        import protea.core.evaluation as ev

        source = inspect.getsource(ev._reconcile_not_side)
        assert "apply_remap" in source
        assert "project_direct_annotations" not in source

    def test_the_not_side_spares_nothing(self) -> None:
        # No ``spare`` there for the same reason: with no projection to hand
        # it to, an unmapped NOT is dropped and counted.
        import protea.core.evaluation as ev

        source = inspect.getsource(ev._reconcile_not_side)
        assert "spare=" not in source

    def test_the_profile_refuses_without_the_alias_map(self) -> None:
        # Otherwise ``projected`` would quietly mean "projected without
        # remapping" on a snapshot whose aliases nobody loaded, and the same
        # four letters would name two different truths.
        import protea.core._truth_rules as rules

        source = inspect.getsource(rules._frame_of)
        assert "load_ontology_aliases" in source
        assert "raise ValueError" in source


class TestTheSeams:
    """The wiring, not the rule. This is the class that was missing.

    Every clause of the remapping had a test and all of them passed while the
    rule was not being applied at all: ``_projection_request`` was building a
    ``Request`` without a ``remap``, so the delta came out of the projection
    alone. What caught it was the acceptance number -- 2,619 entries where
    2,610 were expected -- and a number is a slow way to find a missing
    assignment.

    So these exercise the joins. Each one would have failed on that commit:
    the request carries a remap, the counters reach the stats the operation
    writes, and the whole builder applies both rules in order over doubles.
    """

    class _Row:
        def __init__(self, snapshot: str) -> None:
            self.ontology_snapshot_id = snapshot

    class _Session:
        """Answers the four questions the resolvers ask, and nothing else."""

        def __init__(self, alias_rows: list[tuple[str, str, str]]) -> None:
            self._alias_rows = alias_rows
            self.sets = {
                "corpus-t0": TestTheSeams._Row("snap-t0"),
                "corpus-t1": TestTheSeams._Row("snap-t1"),
            }

        def get(self, _model: object, ident: str) -> object:
            return self.sets.get(ident)

        def execute(self, statement: object) -> object:
            text = str(statement)
            assert "go_term_alias" in text, f"unexpected query: {text[:60]}"

            class _Result:
                def __init__(self, rows: list[tuple[str, str, str]]) -> None:
                    self._rows = rows

                def all(self) -> list[tuple[str, str, str]]:
                    return self._rows

            return _Result(self._alias_rows)

    def _window(self, profile: str = "projected"):
        from protea.core._truth_rules import ReconciledWindow

        return ReconciledWindow(
            "corpus-t0", "corpus-t1", "snap-t0", "snap-t0", "snap-t0", profile
        )

    def test_each_side_gets_a_frame_carrying_the_map(self, monkeypatch) -> None:
        # THE MISSING ASSIGNMENT, in one assertion. It used to be a field of
        # the request; it is per side now, because one window can close its
        # two ends under two graphs.
        import protea.core._truth_rules as rules

        monkeypatch.setattr(
            rules, "_live_terms_with_aspect", lambda _s, _snap: (LIVE, {t: "F" for t in LIVE})
        )
        monkeypatch.setattr(
            rules, "_load_pivot_term_universe", lambda _s, _snap: (LIVE, {t: "F" for t in LIVE})
        )
        frame = rules._frame_of(self._Session(ROWS), "snap-t0")
        assert frame.snapshot_id == "snap-t0"
        assert frame.live == LIVE
        assert frame.remap.by_alias["GO:OLD1"] == ("GO:LIVE1", KIND_ALT_ID)

    def test_a_snapshot_with_no_alias_rows_is_refused(self, monkeypatch) -> None:
        # The other half of the same seam: the profile cannot silently mean
        # "projected without remapping".
        import pytest

        import protea.core._truth_rules as rules

        monkeypatch.setattr(rules, "_live_terms_with_aspect", lambda _s, _snap: (LIVE, {}))
        with pytest.raises(ValueError, match="load_ontology_aliases"):
            rules._frame_of(self._Session([]), "snap-t0")

    def test_the_counters_reach_the_stats_the_operation_writes(self) -> None:
        # The second half of the wiring: a report nobody reads is a report
        # that cannot be compared with the lab's.
        from protea.core._truth_remap import Applied
        from protea.core.evaluation import EvaluationData, _BothSides, _build_report

        sides = _BothSides(
            old_exp={},
            new_exp={},
            negatives={},
            projection=None,
            remapped={"new_exp": Applied(alt_id=0, replaced_by=9, unmapped=0, proteins=9)},
        )
        report = _build_report(sides)
        assert report == {
            "new_exp_alt_id": 0,
            "new_exp_replaced_by": 9,
            "new_exp_unmapped": 0,
            "new_exp_remapped_proteins": 9,
            "new_exp_aspect_crossed": 0,
        }
        # And out through the stats the window row stores.
        assert EvaluationData(projection=report).stats()["new_exp_replaced_by"] == 9

    def test_the_default_profile_reports_nothing(self) -> None:
        # No rule ran, so there is no report: an empty dict would read as
        # "ran and found nothing", which is a different claim.
        from protea.core.evaluation import _BothSides, _build_report

        sides = _BothSides(
            old_exp={}, new_exp={}, negatives={}, projection=None, remapped={"new_exp": None}
        )
        assert _build_report(sides) is None

    def test_both_rules_run_in_order_over_one_protein(self, monkeypatch) -> None:
        # The whole seam, end to end, with doubles instead of Postgres: a
        # protein carrying one remappable term and one projectable term comes
        # out with the successor and the ancestor, and both counters say so.
        import protea.core.evaluation as ev

        parents = {"GO:NEW": {"GO:LIVE3"}}
        aspect = {"GO:NEW": "F", "GO:LIVE3": "F", "GO:DEAD1": "F", "GO:LIVE2": "F"}
        live_t1 = {"GO:NEW", "GO:LIVE3", "GO:DEAD1", "GO:LIVE2"}

        monkeypatch.setattr(ev, "_load_parents_by_go_id", lambda _s, _snap: parents)
        monkeypatch.setattr(ev, "_load_children_by_go_id", lambda _s, _snap: {})
        monkeypatch.setattr(
            ev,
            "_load_experimental_raw_go_ids",
            lambda _s, _set: {"P1": {"GO:DEAD1", "GO:NEW"}},
        )
        monkeypatch.setattr(ev, "_drop_binding_only", lambda _s, _snap, _raw: 0)

        from protea.core._truth_projection import Graph
        from protea.core._truth_projection import Request as ProjectionRequest

        from protea.core._truth_remap import SideFrame
        from protea.core._truth_rules import _Side

        request = ProjectionRequest(
            graph=Graph(parents=parents, aspect=aspect, live=live_t1),
            target=LIVE,
        )
        side = _Side(
            "corpus-t1",
            "snap-t1",
            SideFrame(snapshot_id="snap-t1", live=LIVE, remap=_map()),
        )
        out, (projection, remapped) = ev._reconcile_experimental_side(
            self._Session(ROWS), side, LIVE, {t: "F" for t in LIVE}, request
        )
        # GO:DEAD1 was remapped to its successor; GO:NEW was projected to its
        # first live ancestor. Neither rule touched the other's term.
        assert remapped is not None and remapped.replaced_by == 1
        assert projection is not None and projection.entries == 1
        assert out["P1"]["F"] == {"GO:LIVE2", "GO:LIVE3"}


#: ``go_term_alias`` rows in the shapes the campaign's pivot really carries,
#: read out of ``releases/2022-07-01`` on 2026-11-07. Named so a reader can
#: go and check them rather than trust the fixture.
PIVOT_ROWS: list[tuple[str, str, str]] = [
    # No row at all in the pivot, and a secondary id of a live term: the
    # case that made a protein lose its molecular-function side entirely.
    ("GO:0004004", "GO:0003724", KIND_ALT_ID),
    # Two ids with no row, both secondary ids of the SAME live term: a merge
    # chain, and the reason "no entry" cannot be read as "lost".
    ("GO:0000071", "GO:0007052", KIND_ALT_ID),
    ("GO:0030472", "GO:0007052", KIND_ALT_ID),
    # Live in the pivot, and another term lists it: clause one wins.
    ("GO:0016218", "GO:0034081", KIND_CONSIDER),
    # Obsolete in the pivot, one live successor, in ANOTHER aspect.
    ("GO:0003719", "GO:0042994", KIND_REPLACED_BY),
]
#: Non-obsolete in the pivot. ``GO:0003789``, ``GO:0003925`` and
#: ``GO:0016218`` are the three ids GO reinstated after obsoleting them.
PIVOT_LIVE = {
    "GO:0003724",
    "GO:0007052",
    "GO:0042994",
    "GO:0003789",
    "GO:0003925",
    "GO:0016218",
}
#: The WHOLE universe's aspects, obsolete terms included, because the id whose
#: aspect is in question is the obsolete one being remapped.
PIVOT_ASPECT = {
    "GO:0003724": "F",
    "GO:0007052": "P",
    "GO:0042994": "P",
    "GO:0003789": "F",
    "GO:0003925": "F",
    "GO:0016218": "F",
    "GO:0003719": "F",
}


class TestWhichSnapshotTheMapIsReadFrom:
    """The sibling defect of ``TestTheSeams``, and the same shape.

    There, every clause had a test and the rule was never applied. Here,
    every clause had a test and the rule was applied against the WRONG
    SNAPSHOT -- the one the side's corpus was loaded under instead of the one
    that closes the side -- so it resolved nothing and the tests still
    passed, because a map that resolves nothing satisfies every clause about
    what it resolves.

    A corpus is always live in its own era. That one sentence is the whole
    bug: clause one answered "live, leave it alone" for every annotation,
    and the ids the window could not name died later at the pivot
    intersection with no counter. Measured on an early window, 772 terms over
    13,259 proteins on t0 and 562 over 9,098 on t1.
    """

    def test_an_anchor_in_which_everything_is_live_resolves_nothing(self) -> None:
        # The bug, as a property rather than as a window. Same rows, same
        # rule, and the only difference is which snapshot said what is live.
        era_live = LIVE | {alias for alias, _, _ in ROWS}
        assert build_remap(ROWS, era_live).by_alias == {}
        assert build_remap(ROWS, LIVE).by_alias != {}

    def test_the_frame_reads_one_snapshot_for_all_three_answers(self, monkeypatch) -> None:
        # The map, the liveness test and the aspects, from one snapshot. A
        # frame built from two is the defect in a new place.
        import protea.core._truth_rules as rules

        asked: list[str] = []

        def _record(_session, snapshot):
            asked.append(snapshot)
            return PIVOT_LIVE, PIVOT_ASPECT

        monkeypatch.setattr(rules, "_live_terms_with_aspect", _record)
        monkeypatch.setattr(rules, "_load_pivot_term_universe", _record)
        monkeypatch.setattr(rules, "alias_rows", lambda _s, _snap: PIVOT_ROWS)

        frame = rules._frame_of(object(), "the-closing-graph")
        assert asked == ["the-closing-graph", "the-closing-graph"]
        assert frame.snapshot_id == "the-closing-graph"

    def test_a_window_closing_both_ends_under_one_pivot_shares_one_frame(
        self, monkeypatch
    ) -> None:
        # Construction (A): the two sides are one call, and that falls out of
        # the key instead of being a case somebody has to remember.
        import protea.core._truth_rules as rules
        from protea.core._truth_rules import ReconciledWindow

        built: list[str] = []

        def _one(_session, snapshot):
            built.append(snapshot)
            return _frame(snapshot)

        monkeypatch.setattr(rules, "_frame_of", _one)
        window = ReconciledWindow("c0", "c1", "pivot", "pivot", "pivot", "projected")
        frames = rules._frames_by_snapshot(object(), window)
        assert built == ["pivot"]
        assert set(frames) == {"pivot"}

    def test_two_closing_graphs_give_two_frames(self, monkeypatch) -> None:
        # Construction (B): each end closes under its own era, so each end
        # reads its own alias table.
        import protea.core._truth_rules as rules
        from protea.core._truth_rules import ReconciledWindow

        monkeypatch.setattr(rules, "_frame_of", lambda _s, snap: _frame(snap))
        window = ReconciledWindow("c0", "c1", "nat-t0", "nat-t1", "pivot", "projected")
        frames = rules._frames_by_snapshot(object(), window)
        assert set(frames) == {"nat-t0", "nat-t1"}
        assert frames["nat-t0"].snapshot_id == "nat-t0"

    def test_the_default_profile_builds_no_frame_at_all(self, monkeypatch) -> None:
        # No projection, no remapping, and therefore no refusal over a
        # snapshot whose aliases nobody had any reason to load. The decision
        # is ``_sides_of``'s, which is where the two sides are built.
        import protea.core._truth_rules as rules
        from protea.core._truth_rules import ReconciledWindow

        monkeypatch.setattr(rules, "_frame_of", lambda _s, _snap: _must_not_be_called())
        window = ReconciledWindow("c0", "c1", "pivot", "pivot", "pivot", "as_closed")
        old, new = rules._sides_of(object(), window, None)
        assert old.frame is None and new.frame is None
        assert old.native_snapshot_id == "pivot"

    def test_the_landings_come_from_the_graph_that_closes_t0(self, monkeypatch) -> None:
        # The second half of the anchoring. Read from the t0 corpus's era
        # instead, 1,608 of an early window's 43,435 candidate landings were
        # ids the pivot has no row for, so a gain projected onto one of them
        # survived both rules and then vanished at the intersection.
        import protea.core._truth_rules as rules

        asked: list[str] = []

        def _live(_session, snapshot):
            asked.append(snapshot)
            return {f"live-of-{snapshot}"}, {f"live-of-{snapshot}": "F"}

        class _Set:
            def __init__(self, snapshot: str) -> None:
                self.ontology_snapshot_id = snapshot

        class _Session:
            def get(self, _model, ident):
                return {"c0": _Set("era-t0"), "c1": _Set("era-t1")}[ident]

        monkeypatch.setattr(rules, "_live_terms_with_aspect", _live)
        monkeypatch.setattr(rules, "_load_parents_by_go_id", lambda _s, _snap: {})
        monkeypatch.setattr(rules, "_load_children_by_go_id", lambda _s, _snap: {})

        from protea.core._truth_rules import ReconciledWindow

        window = ReconciledWindow("c0", "c1", "nat-t0", "nat-t1", "pivot", "projected")
        request = rules._projection_request(_Session(), window)

        # The landings are the closing graph's, the climb is still the t1
        # CORPUS's era, and "era-t0" is asked for by nobody.
        assert request.target == {"live-of-nat-t0"}
        assert request.graph.live == {"live-of-era-t1"}
        assert "era-t0" not in asked


def _frame(snapshot: str):
    from protea.core._truth_remap import Remap, SideFrame

    return SideFrame(snapshot_id=snapshot, live=set(), remap=Remap({}, {}, {}))


def _must_not_be_called():
    raise AssertionError("_frame_of must not be called without a projection")


class TestTheRealCasesFromThePivot:
    """The five shapes the campaign's own pivot has, as the rule sees them."""

    def _map(self) -> Remap:
        return build_remap(PIVOT_ROWS, PIVOT_LIVE, PIVOT_ASPECT)

    def test_an_id_with_no_row_resolves_to_its_live_primary(self) -> None:
        # GO:0004004 has no row in the pivot and is a secondary id of
        # GO:0003724, which is live. Under the corpus-era anchor it was left
        # alone and then dropped, which is how a protein lost its whole F
        # side while keeping its P one.
        assert self._map().by_alias["GO:0004004"] == ("GO:0003724", KIND_ALT_ID)

    def test_two_ids_merged_into_one_live_term_both_resolve_to_it(self) -> None:
        # GO:0000071 and GO:0030472 both alias to GO:0007052. Neither has a
        # row of its own, so "no entry" here would be a loss and is not.
        mapped = self._map().by_alias
        assert mapped["GO:0000071"] == ("GO:0007052", KIND_ALT_ID)
        assert mapped["GO:0030472"] == ("GO:0007052", KIND_ALT_ID)

    def test_the_reinstated_ids_are_left_exactly_as_they_are(self) -> None:
        # GO:0003789, GO:0003925 and GO:0016218 are live in the pivot: GO
        # obsoleted them and then brought them back. Clause one applies and
        # no later clause may touch them, which is why rewriting a live id
        # is a bug and not a conservative choice.
        mapped = self._map().by_alias
        for go_id in ("GO:0003789", "GO:0003925", "GO:0016218"):
            assert go_id not in mapped

    def test_consider_is_ignored_even_for_a_live_id(self) -> None:
        # GO:0016218's only row is a ``consider``. Following it would invent
        # the decision the ontology declined to make, and it is live anyway.
        assert "GO:0016218" not in self._map().by_alias

    def test_a_successor_in_another_aspect_is_followed_and_counted(self) -> None:
        # GO:0003719 is obsolete in the pivot with one live successor,
        # GO:0042994, and F does not equal P. The ontology named it, so the
        # substitution happens; the counter is what lets the alternative be
        # decided later rather than assumed now.
        remap = self._map()
        assert remap.by_alias["GO:0003719"] == ("GO:0042994", KIND_REPLACED_BY)
        assert remap.crossed_aspect == frozenset({"GO:0003719"})
        assert remap.counts["remap_aspect_crossed"] == 1

    def test_the_crossing_counter_reaches_the_side_report(self) -> None:
        # And out through the per-side counters the window row stores.
        raw = {"P1": {"GO:0003719"}, "P2": {"GO:0004004"}}
        applied = apply_remap(raw, self._map(), PIVOT_LIVE)
        assert applied.aspect_crossed == 1
        assert applied.alt_id == 1 and applied.replaced_by == 1
        assert applied.as_dict("new_exp")["new_exp_aspect_crossed"] == 1

    def test_an_id_the_graph_cannot_place_is_discarded_and_counted(self) -> None:
        # The counter the old anchoring never reached: nothing ever failed to
        # resolve when nothing was resolved. 6,354 entries of one early
        # window's t0 side land here.
        raw = {"P1": {"GO:NOTHING-KNOWS-THIS"}}
        applied = apply_remap(raw, self._map(), PIVOT_LIVE)
        assert applied.unmapped == 1
        assert raw["P1"] == set()

    def test_this_fixture_has_no_double_successor_and_neither_has_the_pivot(self) -> None:
        # Stated so nobody goes looking for a real case: the clause that
        # refuses two successors has NO instance in this pivot. Zero rows,
        # queried. Its test is the synthetic GO:DEAD2 above, on purpose, and
        # a fixture that quietly acquired a real double would make the two
        # tests say they cover the same thing when they do not.
        from collections import Counter

        per_alias = Counter(a for a, _, kind in PIVOT_ROWS if kind == KIND_REPLACED_BY)
        assert max(per_alias.values()) == 1
