"""The rule that moves a t1 gain into the ontology the window is scored in.

A temporal window is scored in one ontology, and the terms a protein gained
at t1 are not all expressible in it. Today those terms are dropped by the
``closure & pivot_go_ids`` intersection and the gain disappears with them: the
protein looks like it gained nothing where it gained something the older
ontology could only say less precisely.

These pin the lab's rule clause by clause, because every clause is a place
where a plausible alternative would quietly change the truth: climbing the
wrong graph, taking the nearest ancestor instead of all the first ones,
crossing an aspect, or landing on an obsolete node the scorer does not have.

The measured target on the 211 -> 227 window, from the lab's own counters:
2,610 entries projected over 3,821 pairs, 646 proteins added to the delta,
and zero landings inside the protein-binding subtree. The counters here are
the same ones, so the live run can be compared against those numbers rather
than inspected.
"""

from __future__ import annotations

import inspect

from protea.core._truth_projection import (
    Graph,
    first_live_ancestors,
    project_direct_annotations,
)

#: A small F-aspect graph. ``MID`` and ``NEW`` exist only at t1; ``A`` and
#: ``B`` are live at t0 and are the two landings; ``TOP`` sits above ``A``, so
#: a climb that did not stop would reach it.
PARENTS: dict[str, set[str]] = {
    "GO:NEW": {"GO:MID", "GO:OTHER"},
    "GO:MID": {"GO:A", "GO:B"},
    "GO:A": {"GO:TOP"},
    "GO:B": {"GO:TOP"},
    "GO:OTHER": {"GO:CROSS"},
    "GO:ORPHAN": {"GO:DEAD"},
    "GO:DEAD": set(),
}
ASPECT: dict[str, str] = {
    "GO:NEW": "F",
    "GO:MID": "F",
    "GO:A": "F",
    "GO:B": "F",
    "GO:TOP": "F",
    "GO:OTHER": "F",
    # The cross-aspect parent: reachable by an edge, in another CAFA task.
    "GO:CROSS": "P",
    "GO:ORPHAN": "F",
    "GO:DEAD": "F",
}
#: Live at t1: everything but the one deliberately obsolete there.
LIVE_T1 = set(ASPECT) - {"GO:OBSOLETE_AT_T1"}
#: Live at t0: the landings, their ancestor, and the cross-aspect term.
TARGET = {"GO:A", "GO:B", "GO:TOP", "GO:CROSS"}


def _graph(**over: object) -> Graph:
    base = {"parents": PARENTS, "aspect": ASPECT, "live": LIVE_T1, "binding": frozenset()}
    base.update(over)
    return Graph(**base)  # type: ignore[arg-type]


class TestWhereTheClimbStops:
    def test_it_takes_all_the_first_live_terms_and_not_one_of_them(self) -> None:
        # Two paths, two landings. Keeping the nearest would be a choice
        # nothing justifies, and a term can descend from two live ancestors.
        assert first_live_ancestors("GO:NEW", _graph(), TARGET) == {"GO:A", "GO:B"}

    def test_it_does_not_climb_past_a_landing(self) -> None:
        # GO:TOP is live at t0 and sits above both landings. Taking it as well
        # would add nothing (the True Path Rule reaches it from A and B) and
        # would bury the stop.
        assert "GO:TOP" not in first_live_ancestors("GO:NEW", _graph(), TARGET)

    def test_it_never_crosses_an_aspect(self) -> None:
        # GO:CROSS is live at t0 and reachable from GO:NEW through GO:OTHER,
        # and it is a P term. Landing there would move an annotation between
        # two tasks that are scored separately.
        assert "GO:CROSS" not in first_live_ancestors("GO:NEW", _graph(), TARGET)

    def test_a_term_with_no_live_ancestor_lands_nowhere(self) -> None:
        assert first_live_ancestors("GO:ORPHAN", _graph(), TARGET) == set()

    def test_a_term_with_no_aspect_lands_nowhere(self) -> None:
        # Without an aspect there is no "same aspect" to restrict the climb
        # to, so the climb does not start.
        assert first_live_ancestors("GO:NEW", _graph(aspect={}), TARGET) == set()

    def test_a_cycle_does_not_hang(self) -> None:
        # is_a and part_of are acyclic in a sane ontology, and this does not
        # depend on that being true.
        cyclic = {"GO:X": {"GO:Y"}, "GO:Y": {"GO:X"}}
        graph = _graph(parents=cyclic, aspect={"GO:X": "F", "GO:Y": "F"})
        assert first_live_ancestors("GO:X", graph, {"GO:Z"}) == set()


class TestWhatItDoesToTheDirectAnnotations:
    def test_a_term_live_at_t0_is_untouched(self) -> None:
        raw = {"P1": {"GO:A"}}
        report = project_direct_annotations(raw, _graph(), TARGET)
        assert raw == {"P1": {"GO:A"}}
        assert report.entries == 0
        assert report.proteins == 0

    def test_a_t1_only_term_becomes_its_landings(self) -> None:
        raw = {"P1": {"GO:NEW"}}
        report = project_direct_annotations(raw, _graph(), TARGET)
        assert raw == {"P1": {"GO:A", "GO:B"}}
        assert (report.entries, report.pairs, report.proteins) == (1, 2, 1)

    def test_what_lands_nowhere_is_discarded_and_counted(self) -> None:
        # A gain the old ontology cannot express at all. Losing it is the
        # rule; losing it in silence is what the counter is for.
        raw = {"P1": {"GO:ORPHAN"}}
        report = project_direct_annotations(raw, _graph(), TARGET)
        assert raw == {"P1": set()}
        assert (report.entries, report.pairs, report.discarded) == (1, 0, 1)

    def test_a_term_obsolete_at_t1_is_left_alone(self) -> None:
        # Not a gain to carry forward and not something to project: it is
        # kept exactly as today, so the closure trims it as it always did.
        graph = _graph(live=LIVE_T1 - {"GO:NEW"})
        raw = {"P1": {"GO:NEW"}}
        report = project_direct_annotations(raw, graph, TARGET)
        assert raw == {"P1": {"GO:NEW"}}
        assert (report.entries, report.not_live_at_t1) == (0, 1)

    def test_the_other_terms_of_the_same_protein_survive(self) -> None:
        raw = {"P1": {"GO:NEW", "GO:A"}}
        project_direct_annotations(raw, _graph(), TARGET)
        assert raw == {"P1": {"GO:A", "GO:B"}}

    def test_a_landing_the_protein_already_had_is_not_a_special_case(self) -> None:
        # The delta subtracts the two sides afterwards and a term present at
        # both ends vanishes there. Handling it here would duplicate the
        # subtraction in a second place.
        raw = {"P1": {"GO:NEW", "GO:B"}}
        report = project_direct_annotations(raw, _graph(), TARGET)
        assert raw == {"P1": {"GO:A", "GO:B"}}
        assert report.pairs == 2

    def test_it_counts_proteins_once_however_many_terms_moved(self) -> None:
        raw = {"P1": {"GO:NEW", "GO:ORPHAN"}, "P2": {"GO:A"}}
        report = project_direct_annotations(raw, _graph(), TARGET)
        assert (report.proteins, report.entries) == (1, 2)

    def test_a_landing_inside_the_binding_subtree_is_counted(self) -> None:
        # The lab reports zero of these on this window. A non-zero would mean
        # the projection is feeding the binding rule terms it did not see,
        # and the binding rule runs right after.
        graph = _graph(binding=frozenset({"GO:A"}))
        report = project_direct_annotations({"P1": {"GO:NEW"}}, graph, TARGET)
        assert report.binding_landings == 1

    def test_the_report_names_every_counter_the_lab_publishes(self) -> None:
        keys = project_direct_annotations({}, _graph(), TARGET).as_dict()
        assert set(keys) == {
            "projected_entries",
            "projected_pairs",
            "projected_discarded",
            "projected_proteins",
            "projected_not_live_at_t1",
            "projected_binding_landings",
        }


class TestWhereTheRuleIsAllowedToRun:
    def test_only_the_experimental_side_can_project(self) -> None:
        # NOT terms never project: the contrapositive of the True Path Rule
        # propagates them DOWN to descendants, and a projection climbing up
        # would assert the protein lacks an ancestor it may well have.
        from protea.core.evaluation import _reconcile_not_side

        assert "project_onto" not in inspect.signature(_reconcile_not_side).parameters

    def test_the_experimental_side_does_not_project_unless_asked(self) -> None:
        from protea.core.evaluation import _reconcile_experimental_side

        default = inspect.signature(_reconcile_experimental_side).parameters["project"].default
        assert default is None

    def test_it_runs_before_the_binding_rule(self) -> None:
        # The substitution is about DIRECT annotations. After the binding
        # rule, a protein whose F side was emptied would have nothing left to
        # project; after propagation, the ancestors are already there.
        import protea.core.evaluation as ev

        source = inspect.getsource(ev._reconcile_experimental_side)
        assert source.index("project_direct_annotations") < source.index("_drop_binding_only")

    def test_the_profile_vocabulary_is_closed_and_defaults_to_today(self) -> None:
        from protea.core.evaluation import TRUTH_PROFILES, ReconciledWindow

        assert TRUTH_PROFILES == ("as_closed", "projected")
        window = ReconciledWindow(1, 2, 3, 4, 5)  # type: ignore[arg-type]
        assert window.truth_profile == "as_closed"


class TestTheWindowsIdentityHoldsTheProfile:
    def test_the_unique_key_names_it(self) -> None:
        # Without it the two truths could not coexist: the five ids are the
        # same, so the projected window would be refused as a duplicate.
        from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

        unique = [
            c
            for c in EvaluationSet.__table__.constraints
            if getattr(c, "name", "") == "uq_evaluation_set_identity"
        ]
        assert unique, "the identity constraint is gone"
        assert "truth_profile" in {col.name for col in unique[0].columns}

    def test_the_column_cannot_be_null(self) -> None:
        # A NULL is distinct from every other NULL inside a unique
        # constraint, so two windows that declared no profile would both be
        # accepted and the key would stop being one.
        from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

        assert EvaluationSet.__table__.c.truth_profile.nullable is False

    def test_the_payload_refuses_a_profile_nobody_implements(self) -> None:
        import pytest

        from protea.core.operations.generate_evaluation_set import (
            GenerateEvaluationSetPayload,
        )

        with pytest.raises(ValueError, match="unknown truth_profile"):
            GenerateEvaluationSetPayload(
                old_annotation_set_id="a", new_annotation_set_id="b", truth_profile="remapped"
            )


class TestWhichGraphItClimbs:
    """The regression, and it is the one that actually happened.

    ``old_native_snapshot_id`` and ``new_native_snapshot_id`` do two jobs:
    they say which graph closes each side, and they are the only other record
    of which ontologies the window spans. Asking for a closure under t0's
    graph alone means setting BOTH to t0, which erases t1, and the 211 -> 227
    validation window has that done.

    The first wiring read them. On that window it climbed the 2022-07-01
    graph while believing it was climbing t1's, so "is this term live at t1"
    answered no for every term t0 does not have: 2,619 candidates classified
    as obsolete, nothing projected, populations unmoved. The lab projects
    2,610 of those and remaps 9 through ``replaced_by``, and that 2,610 + 9
    is what said the terms had been identified correctly and the graph had
    not.
    """

    T0_CORPUS = "corpus-of-t0"
    T1_CORPUS = "corpus-of-t1"
    T0_GRAPH = "graph-of-2022-07-01"
    T1_GRAPH = "graph-of-2025-07-22"

    def _window(self):
        from protea.core.evaluation import ReconciledWindow

        # Both native fields collapsed onto t0, as the real window's are.
        return ReconciledWindow(
            self.T0_CORPUS, self.T1_CORPUS, self.T0_GRAPH, self.T0_GRAPH, self.T0_GRAPH,
            "projected",
        )

    def _session(self):
        class _Set:
            def __init__(self, snapshot: str) -> None:
                self.ontology_snapshot_id = snapshot

        sets = {self.T0_CORPUS: _Set(self.T0_GRAPH), self.T1_CORPUS: _Set(self.T1_GRAPH)}

        class _Session:
            def get(self, _model, ident):
                return sets.get(ident)

        return _Session()

    def test_it_reads_the_annotation_sets_and_not_the_native_fields(self, monkeypatch) -> None:
        import protea.core.evaluation as ev

        asked: list[str] = []

        def _live(_session, snapshot):
            asked.append(snapshot)
            return {f"live-of-{snapshot}"}, {f"live-of-{snapshot}": "F"}

        monkeypatch.setattr(ev, "_live_terms_with_aspect", _live)
        monkeypatch.setattr(ev, "_load_parents_by_go_id", lambda _s, snap: {"graph": {snap}})
        monkeypatch.setattr(ev, "_load_children_by_go_id", lambda _s, _snap: {})

        request = ev._projection_request(self._session(), self._window())

        # One of each, in order: the landings come from t0, the climb from t1.
        assert asked == [self.T0_GRAPH, self.T1_GRAPH]
        assert request.target == {f"live-of-{self.T0_GRAPH}"}
        assert request.graph.live == {f"live-of-{self.T1_GRAPH}"}
        assert request.graph.parents == {"graph": {self.T1_GRAPH}}

    def test_a_window_whose_corpus_is_gone_raises(self) -> None:
        import pytest

        import protea.core.evaluation as ev

        class _Empty:
            def get(self, _model, _ident):
                return None

        with pytest.raises(ValueError, match="AnnotationSet that is gone"):
            ev._projection_request(_Empty(), self._window())

    def test_the_binding_closure_also_comes_from_t1(self, monkeypatch) -> None:
        # It feeds a counter about the binding rule, which runs on the t1
        # side right after; the subtree is not a constant across the series.
        import protea.core.evaluation as ev

        asked: list[str] = []
        monkeypatch.setattr(
            ev, "_live_terms_with_aspect", lambda _s, snap: (set(), {})
        )
        monkeypatch.setattr(ev, "_load_parents_by_go_id", lambda _s, _snap: {})

        def _children(_session, snapshot):
            asked.append(snapshot)
            return {}

        monkeypatch.setattr(ev, "_load_children_by_go_id", _children)
        ev._projection_request(self._session(), self._window())
        assert asked == [self.T1_GRAPH]
