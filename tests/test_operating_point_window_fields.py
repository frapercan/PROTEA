"""One arm at several windows: the fields that may differ, listed once, and everything else held.

A threshold pooled over an arm's fitting windows has to be pooled over ONE arm.
Three sets of one arm differ in what each window's opening cut builds (the donor
corpus, the ontology, the query set, the window a native method read, the photo
pins and the instance of its method) and in nothing else; a triple that mixes
two arms is refused, naming the field.
"""

from __future__ import annotations

import copy
import uuid
from typing import Any

import pytest

from protea.core.operating_point.window_fields import (
    VARIES_BY_WINDOW,
    WINDOW_VARYING_PIN_ROLES,
    ArmAtWindow,
    WindowArmsDiffer,
    assert_one_arm,
    masked_identity,
)

_WINDOWS = ("160-194", "177-205", "194-211")


def _row(window: str, **meta: Any) -> dict[str, Any]:
    """A predicted set of one window, every field set the way the receipt writes it."""
    return {
        "id": uuid.uuid4(),
        "embedding_config_id": "ankh-base",
        "annotation_set_id": f"corpus-{window}",
        "ontology_snapshot_id": f"graph-{window}",
        "query_set_id": f"targets-{window}",
        "limit_per_entry": 30,
        "distance_threshold": None,
        "method_id": None,
        "created_at": "2026-11-28",
        "description": f"the arm at {window}",
        "job_id": uuid.uuid4(),
        "producer_git_sha": "abc",
        "summary": {},
        "meta": {
            "code_revision": "1" * 40,
            "dependency_revisions": {"protea-method": "69ea8af"},
            "search_backend": "numpy",
            "metric": "cosine",
            "aspect_separated_knn": False,
            "expand_votes_to_ancestors": True,
            "exclude_self_neighbour": True,
            "self_exclusion": "accession",
            "donor_policy": {"evidence_codes": ["EXP"]},
            "scoring": {"preset": "embedding_only", "k": 1},
            "job_id": str(uuid.uuid4()),
            **meta,
        },
    }


def _spec(window: str, **changes: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "family": "explainable-units",
        "code": "homology-vote",
        "instance": {"release": int(window.split("-")[0]), "ontology_snapshot_id": window},
        "recipe": {"rule": "bitscore_identity_vote", "denominator": "any_aspect"},
        "pins": [
            {"role": "homology-table", "sha256": window * 4, "refs": [f"homology_table:{window}"]},
            {"role": "photo", "sha256": window * 3, "refs": []},
            {"role": "code", "sha256": "c" * 64, "refs": []},
        ],
        "runtime": "native",
        "explains": True,
    }
    spec.update(changes)
    return spec


def _arms(**per_window: Any) -> dict[str, ArmAtWindow]:
    return {window: ArmAtWindow(row=_row(window, **per_window)) for window in _WINDOWS}


class TestOneArmAtThreeWindows:
    def test_the_window_fields_may_differ(self) -> None:
        assert_one_arm(_arms())

    def test_a_triple_mixing_two_arms_is_refused_naming_the_field(self) -> None:
        arms = _arms()
        row = copy.deepcopy(dict(arms["177-205"].row))
        row["meta"]["scoring"] = {"preset": "vote_fraction", "k": 5}
        arms["177-205"] = ArmAtWindow(row=row)
        with pytest.raises(WindowArmsDiffer, match="scoring") as refused:
            assert_one_arm(arms)
        assert "vote_fraction" in str(refused.value)

    def test_a_retrieval_depth_is_the_methods_and_not_the_windows(self) -> None:
        arms = _arms()
        row = dict(arms["194-211"].row)
        row["limit_per_entry"] = 5
        arms["194-211"] = ArmAtWindow(row=row)
        with pytest.raises(WindowArmsDiffer, match="limit_per_entry"):
            assert_one_arm(arms)

    def test_an_unrecorded_revision_is_refused_as_the_seal_refuses_it(self) -> None:
        arms = _arms()
        row = copy.deepcopy(dict(arms["160-194"].row))
        del row["meta"]["code_revision"]
        arms["160-194"] = ArmAtWindow(row=row)
        with pytest.raises(ValueError, match="code_revision"):
            assert_one_arm(arms)


class TestTheDonorSetIsComparedThroughItsRule:
    RULE = {"any_of": ["truth"], "all_of": [], "none_of": [], "sample": None}

    def _with_sets(self, rules: dict[str, dict[str, Any]]) -> dict[str, ArmAtWindow]:
        return {
            window: ArmAtWindow(
                row=_row(window, donor_policy={"donor_set_id": f"set-{window}"}),
                donor_rule=rules[window],
            )
            for window in _WINDOWS
        }

    def test_one_rule_declared_at_each_cut_is_one_arm(self) -> None:
        assert_one_arm(self._with_sets(dict.fromkeys(_WINDOWS, self.RULE)))

    def test_another_rule_at_one_window_is_another_arm(self) -> None:
        rules = dict.fromkeys(_WINDOWS, self.RULE)
        rules["194-211"] = {**self.RULE, "any_of": ["truth", "superset"]}
        with pytest.raises(WindowArmsDiffer, match="donor_policy"):
            assert_one_arm(self._with_sets(rules))


class TestAMethodsSpecDropsItsInstanceAndItsDatedPins:
    def _with_specs(self, specs: dict[str, dict[str, Any]]) -> dict[str, ArmAtWindow]:
        return {
            window: ArmAtWindow(row={**_row(window), "method_id": uuid.uuid4()}, spec=specs[window])
            for window in _WINDOWS
        }

    def test_the_instance_and_the_pins_dated_by_the_cut_vary(self) -> None:
        assert {"photo", "homology-table"} <= set(WINDOW_VARYING_PIN_ROLES)
        assert_one_arm(self._with_specs({w: _spec(w) for w in _WINDOWS}))

    def test_a_recipe_that_moves_is_another_method(self) -> None:
        specs = {w: _spec(w) for w in _WINDOWS}
        specs["177-205"] = _spec("177-205", recipe={"rule": "top_hit_identity"})
        with pytest.raises(WindowArmsDiffer, match="method_id"):
            assert_one_arm(self._with_specs(specs))

    def test_a_pin_not_dated_by_the_cut_must_agree(self) -> None:
        specs = {w: _spec(w) for w in _WINDOWS}
        moved = _spec("160-194")
        moved["pins"][2] = {"role": "code", "sha256": "d" * 64, "refs": []}
        specs["160-194"] = moved
        with pytest.raises(WindowArmsDiffer, match="method_id"):
            assert_one_arm(self._with_specs(specs))


class TestTheMasking:
    def test_the_window_fields_are_masked_and_the_method_fields_kept(self) -> None:
        identity = masked_identity(ArmAtWindow(row=_row("160-194")))
        for name in ("annotation_set_id", "ontology_snapshot_id", "query_set_id"):
            assert identity[name] == VARIES_BY_WINDOW
        assert identity["embedding_config_id"] == "ankh-base"
        assert identity["scoring"] == {"preset": "embedding_only", "k": 1}

    def test_a_single_window_is_trivially_one_arm(self) -> None:
        assert_one_arm({"only": ArmAtWindow(row=_row("only"))})
