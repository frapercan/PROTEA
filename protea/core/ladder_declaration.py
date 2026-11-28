"""Which rung of the literature ladder a reading or a comparison belongs to, declared before the read.

A reading (``read_operating_point``) or a comparison (``compare_paired_panels``)
may carry a ``ladder_declaration``: the rung it belongs to, what it is within the
rung (a reading's variant, a comparison's role), the label that tells it from its
siblings, the stack version for the rungs whose arms read one, and, for a choice's
confirmation, the job that picked the candidate it confirms. The declaration is
written before the read and recorded with the job (its payload, and its result),
so a resolver can find the record of each rung later; nothing about it changes
what the job computes.

**The vocabulary is constant here.** The rungs are the concept cards of the
semantic model that carry a ladder order, in that order (:data:`RUNG_CONCEPTS`);
the roles, the variants and the labels are the names the author accepted with
them. Nothing reads the semantic model at run time: a test pins
:data:`RUNG_CONCEPTS` to a committed fixture of the model's release
(``tests/fixtures/semantic_model_rungs.json``), so a rung added to the model and
not to the code, or the other way round, fails the build rather than a job.

**What a declaration may say.** A reading names a variant and no role; a
comparison names a role and no variant. Each rung lists, per variant and per role,
the labels it takes (:data:`LABELS`): a variant or role the rung does not list is
refused, a label outside the listed ones is refused, a label is required where the
list names some and refused where it names none. A stack version is required, a
positive integer, exactly for :data:`STACK_VERSION_RUNGS`. A choice's confirmation
names the job of its pick.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

#: The rung cards of the semantic model, in ladder order (their ``ladder_order``).
RUNG_CONCEPTS: tuple[str, ...] = (
    "term_frequency",
    "own_annotations",
    "top_hit_identity",
    "homology_vote",
    "nearest_neighbour",
    "neighbourhood_vote",
    "reading_of_an_arm",
    "search_space_transform",
    "recipe_across_language_models",
    "regime_calibration",
    "calibrated_maximum",
    "own_annotation_ranker",
    "supervised_head",
    "reranker_over_sources",
    "count_map",
    "segment_encoder",
)

#: What a comparison answers, in display order: the rung against its declared
#: reference or its secondary reference; an axis of the ladder; a choice's
#: confirmation; a sub-source's regime calibration against its per-cell reading; a
#: literature comparator against the recipe rule; a stack version against the
#: previous one; the coverage guarantee's cost; a dry comparison checking an
#: expected axis; a stratum diagnostic; a comparison reported and never adopted.
COMPARISON_ROLES: tuple[str, ...] = (
    "against_reference",
    "against_secondary_reference",
    "axis",
    "choice_confirmation",
    "sub_source_calibration",
    "comparator",
    "against_previous_stack_version",
    "coverage_guarantee_cost",
    "axis_check",
    "diagnostic",
    "reported_only",
)

#: The roles whose verdict line is shown; every other role decides nothing.
ADOPTION_ROLES: tuple[str, ...] = (
    "against_reference",
    "choice_confirmation",
    "sub_source_calibration",
    "comparator",
    "against_previous_stack_version",
)

#: The kind of arm a reading belongs to: a published method's literal, the variant
#: that may enter the recipe, the mirror arm, or any other arm a comparison reads.
READING_VARIANTS: tuple[str, ...] = (
    "literal_variant",
    "recipe_variant",
    "mirror_arm",
    "comparison_arm",
)

#: The rungs whose arms read a stack version, and so declare one.
STACK_VERSION_RUNGS: tuple[str, ...] = (
    "regime_calibration",
    "calibrated_maximum",
    "reranker_over_sources",
)


@dataclass(frozen=True)
class LabelRule:
    """The labels a variant or a role of a rung takes: listed ones, and patterns of built ones.

    A rule that lists none refuses every label; one that lists some requires one.
    """

    labels: tuple[str, ...] = ()
    patterns: tuple[str, ...] = ()

    @property
    def required(self) -> bool:
        return bool(self.labels or self.patterns)

    def admits(self, label: str) -> bool:
        return label in self.labels or any(re.fullmatch(p, label) for p in self.patterns)


_NONE = LabelRule()

#: The breadth levels, the evidence channels and the source kinds labels are built from.
_LEVELS = "(experimental|experimental_or_reviewed|plus_curated_inference|every_member)"
_CHANNELS = (
    "(truth|curated|phylogenetic|orthology_electronic|reviewed_field_electronic"
    "|signature_electronic|no_data)"
)
_SOURCE_KINDS = (
    "(own_annotations|homology_vote|nearest_neighbour|prior|own_annotations_ranked"
    "|supervised_head|count_map)"
)
#: A label read from an embedding configuration's model name: lowercase, other characters as underscores.
_MODEL_LABEL = "[a-z0-9]+(_[a-z0-9]+)*"
_PRESETS = "(embedding_only|vote_fraction|similarity_vote|kernel_fraction)"
_TRANSFORMS = (
    "raw",
    "center",
    "standardize",
    "remove_dominant_directions",
    "whiten",
    "whiten_above_storage_noise",
)
_CALIBRATED_MAXIMUM_COMPARATORS = (
    "identity_weighted_mix",
    "homology_then_neighbour_cascade",
    "convex_mix_per_aspect",
)
_RERANKER_COMPARATORS = ("weighted_vote_by_out_of_fold_average_precision", "consensus")
_RECIPE_CHANNEL_CANDIDATES = (
    "truth",
    "truth_curated",
    "truth_curated_phylogenetic",
    "truth_curated_phylogenetic_orthology_electronic",
    "recipe_channels",
)
_LEXICON_VARIANTS = (
    "lexico_93k_families_permissive",
    "lexico_93k_families_strict",
    "lexico_93k_themes",
    "lexico_final_families_permissive",
)

#: Per rung, per variant (a reading) or role (a comparison), the labels it takes.
#: A comparator's reading carries its comparison's label, and a choice's candidate
#: readings carry the labels its pick lists.
LABELS: Mapping[str, Mapping[str, LabelRule]] = {
    "term_frequency": {
        "literal_variant": LabelRule(("lafa_direct",)),
        "recipe_variant": LabelRule(("propagated",)),
    },
    "own_annotations": {
        "literal_variant": _NONE,
        "recipe_variant": _NONE,
        "against_reference": _NONE,
    },
    "top_hit_identity": {
        "literal_variant": LabelRule(("cafa5",)),
        "recipe_variant": _NONE,
        "against_reference": _NONE,
    },
    "homology_vote": {
        "recipe_variant": _NONE,
        "comparison_arm": LabelRule(
            patterns=(
                r"(bitscore_vote|bitscore_identity_vote)_(any_aspect|same_aspect)"
                r"(_hit_cap_100)?(_by_sequence)?",
                f"level_{_LEVELS}",
                f"channel_{_CHANNELS}",
            )
        ),
        "against_reference": _NONE,
        "axis": LabelRule(
            ("channels",),
            patterns=("breadth_(experimental|experimental_or_reviewed|plus_curated_inference)",),
        ),
        "choice_confirmation": LabelRule(("homology_variant",)),
        "axis_check": LabelRule(("cross_family",)),
    },
    "nearest_neighbour": {
        "literal_variant": LabelRule(("fantasia", "lafa", "cafa5")),
        "recipe_variant": _NONE,
        "mirror_arm": _NONE,
        "comparison_arm": LabelRule(
            ("level_experimental_truth_channel", *_RECIPE_CHANNEL_CANDIDATES),
            patterns=(f"level_{_LEVELS}",),
        ),
        "against_reference": _NONE,
        "axis": LabelRule(("mirror_arm", "level_experimental_truth_channel")),
        "choice_confirmation": LabelRule(("donor_pool_breadth", "recipe_channels")),
    },
    "neighbourhood_vote": {
        "comparison_arm": LabelRule(patterns=(f"depth_(1|2|3|5|10|20|30)_{_PRESETS}",)),
        "against_reference": _NONE,
        "choice_confirmation": LabelRule(("depth_and_preset",)),
    },
    "reading_of_an_arm": {
        "comparison_arm": LabelRule(("raw", "percentile_rank", "calibrated")),
        "against_reference": _NONE,
    },
    "search_space_transform": {
        "comparison_arm": LabelRule(_TRANSFORMS),
        "literal_variant": LabelRule(("remove_dominant_directions_fixed_count",)),
        "choice_confirmation": LabelRule(("search_space_transform",)),
        "reported_only": LabelRule(("remove_dominant_directions_fixed_count",)),
        "diagnostic": LabelRule(
            ("identity_band_none", "identity_band_30_to_60", "identity_band_60_to_90")
        ),
    },
    "recipe_across_language_models": {
        "comparison_arm": LabelRule(patterns=(_MODEL_LABEL,)),
        "axis": LabelRule(("lafa_literal_against_cafa5_literal",)),
        "reported_only": LabelRule(patterns=(_MODEL_LABEL,)),
    },
    "regime_calibration": {
        "comparison_arm": LabelRule(
            ("local", "global"), patterns=(f"{_SOURCE_KINDS}_{_CHANNELS}",)
        ),
        "sub_source_calibration": LabelRule(patterns=(f"{_SOURCE_KINDS}_{_CHANNELS}",)),
        "choice_confirmation": LabelRule(("calibration_identity",)),
    },
    "calibrated_maximum": {
        "recipe_variant": _NONE,
        "comparison_arm": LabelRule(_CALIBRATED_MAXIMUM_COMPARATORS),
        "against_reference": _NONE,
        "comparator": LabelRule(_CALIBRATED_MAXIMUM_COMPARATORS),
        "against_previous_stack_version": _NONE,
        "coverage_guarantee_cost": _NONE,
    },
    "own_annotation_ranker": {
        "recipe_variant": _NONE,
        "against_reference": _NONE,
    },
    "supervised_head": {
        "comparison_arm": LabelRule(("per_term_logistic", "multilayer_perceptron")),
        "against_reference": LabelRule(("per_term_logistic", "multilayer_perceptron")),
    },
    "reranker_over_sources": {
        "comparison_arm": LabelRule(("binary", "lambdarank", *_RERANKER_COMPARATORS)),
        "against_reference": _NONE,
        "choice_confirmation": LabelRule(("reranker_objective",)),
        "comparator": LabelRule(_RERANKER_COMPARATORS),
        "coverage_guarantee_cost": _NONE,
    },
    "count_map": {
        "comparison_arm": LabelRule(_LEXICON_VARIANTS),
        "against_reference": _NONE,
        "against_secondary_reference": _NONE,
        "choice_confirmation": LabelRule(("lexicon_variant",)),
    },
    "segment_encoder": {
        "recipe_variant": _NONE,
        "against_reference": _NONE,
    },
}


class LadderDeclaration(BaseModel):
    """The rung a reading or a comparison belongs to, and what it is within the rung."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    rung: str
    role: str | None = None
    variant: str | None = None
    label: str | None = None
    stack_version: Annotated[int | None, Field(default=None, ge=1)]
    choice_pick_job_id: str | None = None

    @model_validator(mode="after")
    def _within_its_rung(self) -> LadderDeclaration:
        if self.rung not in RUNG_CONCEPTS:
            raise ValueError(f"unknown rung {self.rung!r}; the rungs are {list(RUNG_CONCEPTS)}")
        if (self.role is None) == (self.variant is None):
            raise ValueError(
                "a declaration names a variant (a reading) or a role (a comparison), never "
                "both and never neither"
            )
        kind = self.role if self.role is not None else self.variant
        assert kind is not None
        vocabulary = COMPARISON_ROLES if self.role is not None else READING_VARIANTS
        if kind not in vocabulary:
            raise ValueError(f"{kind!r} is not one of {list(vocabulary)}")
        rule = LABELS[self.rung].get(kind)
        if rule is None:
            listed = sorted(k for k in LABELS[self.rung] if k in vocabulary)
            raise ValueError(
                f"rung {self.rung!r} has no {kind!r}; what it declares of that kind is {listed}"
            )
        self._label_within(rule, kind)
        self._stack_version_within()
        self._pick_within()
        return self

    def _label_within(self, rule: LabelRule, kind: str) -> None:
        if self.label is None:
            if rule.required:
                raise ValueError(
                    f"a {kind} of rung {self.rung!r} names its label (one of "
                    f"{list(rule.labels) or list(rule.patterns)})"
                )
            return
        if not rule.required:
            raise ValueError(f"a {kind} of rung {self.rung!r} takes no label, not {self.label!r}")
        if not rule.admits(self.label):
            raise ValueError(
                f"{self.label!r} is not a label of a {kind} of rung {self.rung!r}: the labels are "
                f"{list(rule.labels)}"
                + (f" and the forms {list(rule.patterns)}" if rule.patterns else "")
            )

    def _stack_version_within(self) -> None:
        takes = self.rung in STACK_VERSION_RUNGS
        if takes and self.stack_version is None:
            raise ValueError(f"rung {self.rung!r} reads a stack version: declare stack_version")
        if not takes and self.stack_version is not None:
            raise ValueError(f"rung {self.rung!r} reads no stack version; drop stack_version")

    def _pick_within(self) -> None:
        confirms = self.role == "choice_confirmation"
        if confirms and self.choice_pick_job_id is None:
            raise ValueError(
                "a choice's confirmation names the job that picked the candidate it confirms "
                "(choice_pick_job_id)"
            )
        if not confirms and self.choice_pick_job_id is not None:
            raise ValueError("choice_pick_job_id belongs to a choice's confirmation only")
        if self.choice_pick_job_id is not None:
            uuid.UUID(self.choice_pick_job_id)

    @property
    def is_reading(self) -> bool:
        return self.variant is not None

    @property
    def adopts(self) -> bool:
        """Whether this is a comparison whose verdict line is shown (an adoption role)."""
        return self.role in ADOPTION_ROLES

    def record(self) -> dict[str, Any]:
        """The declaration as the job records it: every key, the unset ones as null."""
        return self.model_dump()


def refuse_unless_reading(declaration: LadderDeclaration | None) -> None:
    """A reading's declaration names a variant; a role belongs to a comparison."""
    if declaration is not None and not declaration.is_reading:
        raise ValueError(
            f"a reading declares a variant, not a role ({declaration.role!r} is a comparison's)"
        )


def refuse_unless_comparison(declaration: LadderDeclaration | None) -> None:
    """A comparison's declaration names a role; a variant belongs to a reading."""
    if declaration is not None and declaration.is_reading:
        raise ValueError(
            f"a comparison declares a role, not a variant ({declaration.variant!r} is a reading's)"
        )


__all__ = [
    "ADOPTION_ROLES",
    "COMPARISON_ROLES",
    "LABELS",
    "READING_VARIANTS",
    "RUNG_CONCEPTS",
    "STACK_VERSION_RUNGS",
    "LabelRule",
    "LadderDeclaration",
    "refuse_unless_comparison",
    "refuse_unless_reading",
]
