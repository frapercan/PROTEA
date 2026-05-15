"""Unit tests for the pure ancestor-expansion helpers.

Covers ``protea.core._feature_enricher_helpers`` (the four pure
functions that back the ancestor-expansion loop) and the orchestrator
``expand_predictions_to_ancestors`` in ``protea.core.feature_enricher``.

These helpers are pure compute (no DB, no I/O) so the tests exercise
their full behaviour with small handcrafted inputs.
"""

from __future__ import annotations

from typing import Any

from protea.core._feature_enricher_helpers import (
    LabelConfig,
    compute_ia_weight,
    make_ancestor_closure,
    merge_into_existing_leaf,
    update_synth_entry,
)
from protea.core.feature_enricher import (
    AncestorLabelConfig,
    expand_predictions_to_ancestors,
)


class TestMakeAncestorClosure:
    def test_returns_empty_frozenset_for_unknown_id(self) -> None:
        ancestors = make_ancestor_closure({})
        assert ancestors("GO:0000001") == frozenset()

    def test_single_parent_edge(self) -> None:
        ancestors = make_ancestor_closure({"GO:CHILD": {"GO:PARENT"}})
        assert ancestors("GO:CHILD") == frozenset({"GO:PARENT"})

    def test_transitive_closure_across_multiple_levels(self) -> None:
        parents = {
            "GO:A": {"GO:B"},
            "GO:B": {"GO:C"},
            "GO:C": {"GO:D"},
        }
        ancestors = make_ancestor_closure(parents)
        assert ancestors("GO:A") == frozenset({"GO:B", "GO:C", "GO:D"})

    def test_accepts_list_parent_values(self) -> None:
        # Helper signature allows ``set`` or ``list`` values.
        parents: dict[str, list[str]] = {"GO:X": ["GO:Y", "GO:Z"]}
        ancestors = make_ancestor_closure(parents)
        assert ancestors("GO:X") == frozenset({"GO:Y", "GO:Z"})

    def test_none_parent_map_yields_empty_closure(self) -> None:
        ancestors = make_ancestor_closure(None)
        assert ancestors("GO:0000001") == frozenset()

    def test_handles_cycle_without_infinite_loop(self) -> None:
        # Real GO is a DAG, but the closure helper must still be safe
        # against accidental cycles in malformed input.
        parents = {"GO:A": {"GO:B"}, "GO:B": {"GO:A"}}
        ancestors = make_ancestor_closure(parents)
        assert ancestors("GO:A") == frozenset({"GO:A", "GO:B"})

    def test_results_are_memoized(self) -> None:
        # Memoization is a correctness property (cached results must be
        # identical to fresh ones) and a performance property; we
        # verify the former by asserting identity across calls.
        ancestors = make_ancestor_closure({"GO:A": {"GO:B"}})
        first = ancestors("GO:A")
        second = ancestors("GO:A")
        assert first is second

    def test_mutating_input_after_call_does_not_change_closure(self) -> None:
        parents: dict[str, set[str]] = {"GO:A": {"GO:B"}}
        ancestors = make_ancestor_closure(parents)
        # Mutate the caller's dict after the closure was constructed.
        parents["GO:A"].add("GO:C")
        # Internal normalisation copied parents into frozensets, so
        # the closure ignores the mutation.
        assert ancestors("GO:A") == frozenset({"GO:B"})


class TestComputeIaWeight:
    def test_returns_one_when_ia_weights_is_none(self) -> None:
        assert compute_ia_weight("GO:A", "GO:B", None) == 1.0

    def test_returns_one_when_ia_weights_is_empty(self) -> None:
        assert compute_ia_weight("GO:A", "GO:B", {}) == 1.0

    def test_returns_ratio_when_weights_present(self) -> None:
        weights = {"GO:ANC": 1.5, "GO:LEAF": 3.0}
        assert compute_ia_weight("GO:ANC", "GO:LEAF", weights) == 0.5

    def test_zero_leaf_weight_falls_back_to_one(self) -> None:
        # An incomplete IA file may leave a term at IA=0; that must
        # not blow up the division.
        weights = {"GO:ANC": 1.0, "GO:LEAF": 0.0}
        assert compute_ia_weight("GO:ANC", "GO:LEAF", weights) == 1.0

    def test_missing_ancestor_weight_treated_as_zero(self) -> None:
        weights = {"GO:LEAF": 2.0}
        assert compute_ia_weight("GO:ANC", "GO:LEAF", weights) == 0.0


class TestMergeIntoExistingLeaf:
    def test_increments_vote_fraction(self) -> None:
        leaf_anc: dict[str, Any] = {"neighbor_vote_fraction": 0.2}
        leaf_rec: dict[str, Any] = {"neighbor_min_distance": 0.5}
        merge_into_existing_leaf(leaf_anc, leaf_rec, 0.3, 0.5)
        assert leaf_anc["neighbor_vote_fraction"] == 0.5

    def test_vote_fraction_is_clamped_to_one(self) -> None:
        leaf_anc: dict[str, Any] = {"neighbor_vote_fraction": 0.8}
        leaf_rec: dict[str, Any] = {"neighbor_min_distance": 0.5}
        merge_into_existing_leaf(leaf_anc, leaf_rec, 0.5, 0.5)
        assert leaf_anc["neighbor_vote_fraction"] == 1.0

    def test_min_distance_lowered_when_contributor_is_closer(self) -> None:
        leaf_anc: dict[str, Any] = {
            "neighbor_vote_fraction": 0.0,
            "neighbor_min_distance": 0.7,
        }
        leaf_rec: dict[str, Any] = {"neighbor_min_distance": 0.3}
        merge_into_existing_leaf(leaf_anc, leaf_rec, 0.1, 0.3)
        assert leaf_anc["neighbor_min_distance"] == 0.3

    def test_min_distance_unchanged_when_contributor_is_farther(self) -> None:
        leaf_anc: dict[str, Any] = {
            "neighbor_vote_fraction": 0.0,
            "neighbor_min_distance": 0.3,
        }
        leaf_rec: dict[str, Any] = {"neighbor_min_distance": 0.7}
        merge_into_existing_leaf(leaf_anc, leaf_rec, 0.1, 0.7)
        assert leaf_anc["neighbor_min_distance"] == 0.3

    def test_uses_default_vote_fraction_when_absent(self) -> None:
        leaf_anc: dict[str, Any] = {}
        leaf_rec: dict[str, Any] = {"neighbor_min_distance": 0.5}
        merge_into_existing_leaf(leaf_anc, leaf_rec, 0.4, 0.5)
        assert leaf_anc["neighbor_vote_fraction"] == 0.4


class TestUpdateSynthEntry:
    def _label_ctx(self, present: bool = False, gt: set | None = None) -> LabelConfig:
        return LabelConfig(q_acc="Q1", gt_pairs=gt, column="label", present=present)

    def test_creates_new_entry_when_missing(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        leaf = {"go_id": "GO:LEAF", "distance": 0.3, "feat": 1}
        update_synth_entry(synth, "GO:ANC", leaf, 0.3, 0.2, self._label_ctx())
        assert "GO:ANC" in synth
        assert synth["GO:ANC"]["go_id"] == "GO:ANC"
        assert synth["GO:ANC"]["feat"] == 1
        assert synth["GO:ANC"]["neighbor_vote_fraction"] == 0.2

    def test_replaces_entry_when_closer_leaf_arrives(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        far_leaf = {"go_id": "GO:LEAF1", "distance": 0.6, "feat": "far"}
        near_leaf = {"go_id": "GO:LEAF2", "distance": 0.2, "feat": "near"}
        update_synth_entry(synth, "GO:ANC", far_leaf, 0.6, 0.1, self._label_ctx())
        update_synth_entry(synth, "GO:ANC", near_leaf, 0.2, 0.3, self._label_ctx())
        # New record wins (closer); vote fraction accumulates prior+new.
        assert synth["GO:ANC"]["feat"] == "near"
        assert synth["GO:ANC"]["neighbor_vote_fraction"] == 0.4

    def test_only_bumps_vote_when_existing_is_closer(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        near_leaf = {"go_id": "GO:LEAF1", "distance": 0.2, "feat": "near"}
        far_leaf = {"go_id": "GO:LEAF2", "distance": 0.6, "feat": "far"}
        update_synth_entry(synth, "GO:ANC", near_leaf, 0.2, 0.3, self._label_ctx())
        update_synth_entry(synth, "GO:ANC", far_leaf, 0.6, 0.1, self._label_ctx())
        assert synth["GO:ANC"]["feat"] == "near"
        assert synth["GO:ANC"]["neighbor_vote_fraction"] == 0.4

    def test_vote_fraction_capped_at_one_on_accumulation(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        leaf = {"go_id": "GO:LEAF", "distance": 0.3}
        update_synth_entry(synth, "GO:ANC", leaf, 0.3, 0.8, self._label_ctx())
        update_synth_entry(synth, "GO:ANC", leaf, 0.3, 0.5, self._label_ctx())
        assert synth["GO:ANC"]["neighbor_vote_fraction"] == 1.0

    def test_label_injection_uses_gt_pairs_membership(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        leaf = {"go_id": "GO:LEAF", "distance": 0.3}
        ctx = self._label_ctx(present=True, gt={("Q1", "GO:ANC")})
        update_synth_entry(synth, "GO:ANC", leaf, 0.3, 0.5, ctx)
        assert synth["GO:ANC"]["label"] == 1

    def test_label_zero_when_pair_not_in_gt(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        leaf = {"go_id": "GO:LEAF", "distance": 0.3}
        ctx = self._label_ctx(present=True, gt={("Q1", "GO:OTHER")})
        update_synth_entry(synth, "GO:ANC", leaf, 0.3, 0.5, ctx)
        assert synth["GO:ANC"]["label"] == 0

    def test_no_label_key_when_present_false(self) -> None:
        synth: dict[str, dict[str, Any]] = {}
        leaf = {"go_id": "GO:LEAF", "distance": 0.3}
        update_synth_entry(synth, "GO:ANC", leaf, 0.3, 0.5, self._label_ctx())
        assert "label" not in synth["GO:ANC"]


class TestExpandPredictionsToAncestors:
    def test_empty_predictions_returns_empty(self) -> None:
        out = expand_predictions_to_ancestors([], parent_map={}, k_limit=10)
        assert out == []

    def test_no_parents_returns_only_leaves(self) -> None:
        preds = [
            {
                "protein_accession": "P1",
                "aspect": "F",
                "go_id": "GO:LEAF",
                "distance": 0.4,
                "neighbor_vote_fraction": 0.5,
            }
        ]
        out = expand_predictions_to_ancestors(preds, parent_map={}, k_limit=10)
        assert len(out) == 1
        assert out[0]["go_id"] == "GO:LEAF"

    def test_synthesises_ancestor_records(self) -> None:
        preds = [
            {
                "protein_accession": "P1",
                "aspect": "F",
                "go_id": "GO:LEAF",
                "distance": 0.4,
                "neighbor_vote_fraction": 0.5,
            }
        ]
        parents: dict[str, set[str]] = {"GO:LEAF": {"GO:ANC"}}
        out = expand_predictions_to_ancestors(preds, parent_map=parents, k_limit=10)
        go_ids = {rec["go_id"] for rec in out}
        assert go_ids == {"GO:LEAF", "GO:ANC"}

    def test_groups_by_protein_and_aspect(self) -> None:
        # Two proteins, same parent map, must produce ancestors per
        # group rather than collapsing across proteins.
        preds = [
            {
                "protein_accession": "P1",
                "aspect": "F",
                "go_id": "GO:LEAF",
                "distance": 0.4,
                "neighbor_vote_fraction": 0.5,
            },
            {
                "protein_accession": "P2",
                "aspect": "F",
                "go_id": "GO:LEAF",
                "distance": 0.2,
                "neighbor_vote_fraction": 0.7,
            },
        ]
        parents: dict[str, set[str]] = {"GO:LEAF": {"GO:ANC"}}
        out = expand_predictions_to_ancestors(preds, parent_map=parents, k_limit=5)
        by_q = {(r["protein_accession"], r["go_id"]) for r in out}
        # Each (protein, GO:ANC) pair must be present independently.
        assert ("P1", "GO:ANC") in by_q
        assert ("P2", "GO:ANC") in by_q

    def test_ancestor_already_a_leaf_merges_in_place(self) -> None:
        # GO:B is both a parent of GO:A and an existing leaf prediction
        # in the same group — it must be merged, not duplicated.
        preds = [
            {
                "protein_accession": "P1",
                "aspect": "F",
                "go_id": "GO:A",
                "distance": 0.3,
                "neighbor_vote_fraction": 0.4,
                "neighbor_min_distance": 0.3,
            },
            {
                "protein_accession": "P1",
                "aspect": "F",
                "go_id": "GO:B",
                "distance": 0.5,
                "neighbor_vote_fraction": 0.2,
                "neighbor_min_distance": 0.5,
            },
        ]
        parents: dict[str, set[str]] = {"GO:A": {"GO:B"}}
        out = expand_predictions_to_ancestors(preds, parent_map=parents, k_limit=4)
        # No duplicate GO:B row; its vote was bumped by GO:A's contribution.
        b_rows = [r for r in out if r["go_id"] == "GO:B"]
        assert len(b_rows) == 1
        assert b_rows[0]["neighbor_vote_fraction"] > 0.2

    def test_ia_weight_scales_vote_increment(self) -> None:
        preds = [
            {
                "protein_accession": "P1",
                "aspect": "F",
                "go_id": "GO:LEAF",
                "distance": 0.4,
                "neighbor_vote_fraction": 0.0,
            }
        ]
        parents: dict[str, set[str]] = {"GO:LEAF": {"GO:ANC"}}
        ia_weights = {"GO:ANC": 1.0, "GO:LEAF": 4.0}
        out = expand_predictions_to_ancestors(
            preds, parent_map=parents, k_limit=4, ia_weights=ia_weights
        )
        anc_row = next(r for r in out if r["go_id"] == "GO:ANC")
        # increment = (1.0 / 4.0) / k_limit = 0.0625
        assert abs(anc_row["neighbor_vote_fraction"] - 0.0625) < 1e-9

    def test_label_propagation_via_ancestor_label_config(self) -> None:
        preds = [
            {
                "protein_accession": "Q1",
                "aspect": "F",
                "go_id": "GO:LEAF",
                "distance": 0.3,
            }
        ]
        parents: dict[str, set[str]] = {"GO:LEAF": {"GO:ANC"}}
        labels = AncestorLabelConfig(gt_pairs={("Q1", "GO:ANC")}, column="label", present=True)
        out = expand_predictions_to_ancestors(preds, parent_map=parents, k_limit=5, labels=labels)
        anc_row = next(r for r in out if r["go_id"] == "GO:ANC")
        assert anc_row["label"] == 1
