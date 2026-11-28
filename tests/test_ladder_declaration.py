"""A reading or a comparison declares its rung of the literature ladder, before the read.

The vocabulary is constant in code and pinned here to a committed fixture of the
semantic model (``tests/fixtures/semantic_model_rungs.json``): the rung cards that
carry a ladder order, in that order, with the model's version, the commit it was
generated from and the sha256 of its ``data/concepts.yaml``. Nothing reads the
model at run time.

The fixture was generated from the model's branch ``concepts/rung-declarations``
(``git show <commit>:data/concepts.yaml``, the cards with a ``ladder_order`` sorted
by it); once the model's release ``v0.2.0`` is tagged, it is generated again from
the tag, and ``tag`` names it.

Then the declaration itself: unset, a comparison's payload and result are what they
were; set, the job's result carries it, the paired comparisons' serialiser returns
it, and the job's summary names the rung; and every declaration the accepted names
do not allow is refused before anything is read.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from protea.core.ladder_declaration import (
    ADOPTION_ROLES,
    COMPARISON_ROLES,
    LABELS,
    READING_VARIANTS,
    RUNG_CONCEPTS,
    STACK_VERSION_RUNGS,
    LadderDeclaration,
)
from protea.core.operations.compare_paired_panels import (
    ComparePairedPanelsOperation,
    ComparePairedPanelsPayload,
)
from protea.core.operations.read_operating_point import (
    ReadOperatingPointOperation,
    ReadOperatingPointPayload,
    _declared,
)
from protea.services._annotations_paired_helpers import comparison_to_dict
from tests.test_compare_paired_panels import _GUARD_N, _run, _write_reselection_panel

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "semantic_model_rungs.json"

_IDS = {
    "evaluation_result_id": "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a001",
    "baseline_evaluation_result_id": "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a002",
}
_READ = {
    "evaluation_result_id": "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a001",
    "declared_threshold_id": "4b8f0b48-1f0e-4f45-9c43-5d3e21b2a003",
}
_PICK = str(uuid.UUID("4b8f0b48-1f0e-4f45-9c43-5d3e21b2a004"))


@pytest.fixture(scope="module")
def model() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text())


class TestTheVocabularyIsTheModels:
    def test_the_rungs_are_the_model_cards_with_a_ladder_order_in_that_order(
        self, model: dict[str, Any]
    ) -> None:
        orders = [rung["ladder_order"] for rung in model["rungs"]]
        assert orders == sorted(orders)
        assert RUNG_CONCEPTS == tuple(rung["id"] for rung in model["rungs"])
        assert all(rung["status"] == "accepted" for rung in model["rungs"])

    def test_the_fixture_names_what_it_was_generated_from(self, model: dict[str, Any]) -> None:
        assert model["model"] == "frapercan/protein-function-prediction-semantic-model"
        assert model["version"] == "0.2.0"
        assert len(model["commit"]) == 40 and len(model["concepts_sha256"]) == 64

    def test_the_rungs_that_read_a_stack_version_are_rungs_with_a_method(
        self, model: dict[str, Any]
    ) -> None:
        families = {rung["id"]: rung["method_family"] for rung in model["rungs"]}
        assert set(STACK_VERSION_RUNGS) <= set(RUNG_CONCEPTS)
        assert all(families[rung] for rung in STACK_VERSION_RUNGS)

    def test_every_rung_lists_what_it_declares_and_nothing_else_is_listed(self) -> None:
        assert tuple(LABELS) == RUNG_CONCEPTS
        known = set(COMPARISON_ROLES) | set(READING_VARIANTS)
        for rung, rules in LABELS.items():
            assert set(rules) <= known, rung
        assert set(ADOPTION_ROLES) <= set(COMPARISON_ROLES)


def _declare(**fields: Any) -> LadderDeclaration:
    return LadderDeclaration.model_validate(fields)


class TestWhatADeclarationMaySay:
    @pytest.mark.parametrize(
        "fields",
        [
            {"rung": "term_frequency", "variant": "literal_variant", "label": "lafa_direct"},
            {"rung": "own_annotations", "role": "against_reference"},
            {
                "rung": "homology_vote",
                "variant": "comparison_arm",
                "label": "bitscore_identity_vote_same_aspect_hit_cap_100_by_sequence",
            },
            {"rung": "homology_vote", "variant": "comparison_arm", "label": "level_every_member"},
            {
                "rung": "homology_vote",
                "variant": "comparison_arm",
                "label": "channel_signature_electronic",
            },
            {"rung": "homology_vote", "role": "axis", "label": "breadth_plus_curated_inference"},
            {"rung": "homology_vote", "role": "axis_check", "label": "cross_family"},
            {"rung": "nearest_neighbour", "variant": "mirror_arm"},
            {"rung": "nearest_neighbour", "variant": "comparison_arm", "label": "truth_curated"},
            {
                "rung": "neighbourhood_vote",
                "variant": "comparison_arm",
                "label": "depth_30_kernel_fraction",
            },
            {
                "rung": "search_space_transform",
                "role": "diagnostic",
                "label": "identity_band_30_to_60",
            },
            {
                "rung": "recipe_across_language_models",
                "variant": "comparison_arm",
                "label": "prot_t5_xl_uniref50_whiten",
            },
            {
                "rung": "regime_calibration",
                "variant": "comparison_arm",
                "label": "homology_vote_truth",
                "stack_version": 1,
            },
            {
                "rung": "regime_calibration",
                "role": "sub_source_calibration",
                "label": "nearest_neighbour_curated",
                "stack_version": 2,
            },
            {
                "rung": "calibrated_maximum",
                "role": "comparator",
                "label": "convex_mix_per_aspect",
                "stack_version": 1,
            },
            {"rung": "calibrated_maximum", "role": "coverage_guarantee_cost", "stack_version": 3},
            {
                "rung": "supervised_head",
                "role": "against_reference",
                "label": "multilayer_perceptron",
            },
            {
                "rung": "reranker_over_sources",
                "role": "choice_confirmation",
                "label": "reranker_objective",
                "stack_version": 2,
                "choice_pick_job_id": _PICK,
            },
            {"rung": "count_map", "role": "against_secondary_reference"},
            {"rung": "segment_encoder", "variant": "recipe_variant"},
        ],
    )
    def test_the_accepted_names_declare(self, fields: dict[str, Any]) -> None:
        declared = _declare(**fields)
        assert declared.record() == {
            "rung": fields["rung"],
            "role": fields.get("role"),
            "variant": fields.get("variant"),
            "label": fields.get("label"),
            "stack_version": fields.get("stack_version"),
            "choice_pick_job_id": fields.get("choice_pick_job_id"),
        }

    @pytest.mark.parametrize(
        ("fields", "refusal"),
        [
            ({"rung": "rung_13", "role": "against_reference"}, "unknown rung"),
            ({"rung": "own_annotations"}, "never both and never neither"),
            (
                {
                    "rung": "own_annotations",
                    "role": "against_reference",
                    "variant": "recipe_variant",
                },
                "never both",
            ),
            ({"rung": "own_annotations", "role": "verdict"}, "is not one of"),
            ({"rung": "term_frequency", "role": "against_reference"}, "has no 'against_reference'"),
            (
                {"rung": "term_frequency", "variant": "literal_variant", "label": "naive"},
                "is not a label",
            ),
            (
                {
                    "rung": "homology_vote",
                    "variant": "comparison_arm",
                    "label": "bitscore_vote_some_aspect",
                },
                "is not a label",
            ),
            ({"rung": "nearest_neighbour", "variant": "comparison_arm"}, "names its label"),
            (
                {"rung": "own_annotations", "variant": "recipe_variant", "label": "recipe"},
                "takes no label",
            ),
            (
                {"rung": "own_annotations", "role": "against_reference", "stack_version": 1},
                "reads no stack version",
            ),
            ({"rung": "calibrated_maximum", "role": "against_reference"}, "declare stack_version"),
            (
                {"rung": "calibrated_maximum", "role": "against_reference", "stack_version": 0},
                "greater than or equal to 1",
            ),
            (
                {
                    "rung": "neighbourhood_vote",
                    "role": "choice_confirmation",
                    "label": "depth_and_preset",
                },
                "choice_pick_job_id",
            ),
            (
                {"rung": "count_map", "role": "against_reference", "choice_pick_job_id": _PICK},
                "confirmation only",
            ),
            (
                {
                    "rung": "count_map",
                    "role": "choice_confirmation",
                    "label": "lexicon_variant",
                    "choice_pick_job_id": "a pick",
                },
                "badly formed",
            ),
        ],
    )
    def test_what_the_names_do_not_allow_is_refused(
        self, fields: dict[str, Any], refusal: str
    ) -> None:
        with pytest.raises(ValidationError, match=refusal):
            _declare(**fields)


class TestTheDeclarationOnTheTwoOperations:
    def test_a_role_on_a_reading_and_a_variant_on_a_comparison_are_refused(self) -> None:
        with pytest.raises(ValidationError, match="a reading declares a variant"):
            ReadOperatingPointPayload.model_validate(
                {
                    **_READ,
                    "ladder_declaration": {"rung": "own_annotations", "role": "against_reference"},
                }
            )
        with pytest.raises(ValidationError, match="a comparison declares a role"):
            ComparePairedPanelsPayload.model_validate(
                {
                    **_IDS,
                    "ladder_declaration": {"rung": "own_annotations", "variant": "recipe_variant"},
                }
            )
        ok = ReadOperatingPointPayload.model_validate(
            {
                **_READ,
                "ladder_declaration": {"rung": "own_annotations", "variant": "recipe_variant"},
            }
        )
        assert ok.ladder_declaration is not None and ok.ladder_declaration.is_reading

    def test_unset_a_comparison_is_what_it_was_and_set_its_result_carries_it(
        self, tmp_path: Path
    ) -> None:
        _write_reselection_panel(tmp_path)
        declared = {"rung": "reading_of_an_arm", "role": "against_reference"}
        plain = _run(tmp_path, min_population=_GUARD_N, interval_method="percentile")
        marked = _run(
            tmp_path,
            min_population=_GUARD_N,
            interval_method="percentile",
            ladder_declaration=declared,
        )
        assert "ladder_declaration" not in plain
        assert marked.pop("ladder_declaration") == _declare(**declared).record()
        start = [e for e in marked.pop("_events") if e[0] == "compare_paired_panels.start"]
        assert start[0][2]["ladder_declaration"]["rung"] == "reading_of_an_arm"
        plain.pop("_events")
        assert marked == plain

    def test_a_readings_result_carries_its_declaration_and_only_then(self) -> None:
        result = {"evaluation_operating_point_id": "x"}
        unset = ReadOperatingPointPayload.model_validate(_READ)
        assert _declared(result, unset) is result
        declared = ReadOperatingPointPayload.model_validate(
            {
                **_READ,
                "ladder_declaration": {
                    "rung": "search_space_transform",
                    "variant": "comparison_arm",
                    "label": "whiten",
                },
            }
        )
        assert _declared(result, declared)["ladder_declaration"]["label"] == "whiten"

    def test_the_paired_comparisons_serialiser_returns_it(self) -> None:
        row = {
            "job_id": "j",
            "evaluation_result_id": "a",
            "baseline_evaluation_result_id": "b",
            "finished_at": None,
            "result": {"ladder_declaration": {"rung": "count_map", "role": "against_reference"}},
        }
        assert comparison_to_dict(row)["ladder_declaration"]["rung"] == "count_map"
        assert comparison_to_dict({**row, "result": {}})["ladder_declaration"] is None

    def test_the_jobs_summary_names_the_rung(self) -> None:
        comparison = ComparePairedPanelsOperation().summarize_payload(
            {
                **_IDS,
                "ladder_declaration": {"rung": "own_annotations", "role": "against_reference"},
            }
        )
        assert comparison.startswith("rung own_annotations, against_reference: 4b8f0b48")
        reading = ReadOperatingPointOperation().summarize_payload(
            {
                **_READ,
                "ladder_declaration": {
                    "rung": "term_frequency",
                    "variant": "literal_variant",
                    "label": "lafa_direct",
                },
            }
        )
        assert reading.startswith("rung term_frequency, literal_variant lafa_direct: result")
        assert ComparePairedPanelsOperation().summarize_payload(_IDS).startswith("4b8f0b48")
