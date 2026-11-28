"""The request of a paired comparison: which two results, which panels, at what precision.

Split out of :mod:`protea.core.operations.compare_paired_panels` when that file
reached its budget; the operation re-exports both names, so every import of
``ComparePairedPanelsPayload`` or ``ALL_PANELS`` from it keeps working.

Every field here is a decision a reader of the stored comparison has to be able
to recover from the job row: the operating point and the threshold it is read
at (one tau for every panel, or each arm's declared threshold per panel), the
interval family (each panel alone, or the nine together with simultaneous
bounds), the method axis the two arms are claimed to differ in. A closed
vocabulary refuses what no arithmetic would read.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Literal

from pydantic import ConfigDict, Field, field_validator, model_validator

from protea.core.contracts.operation import ProteaPayload
from protea.core.ladder_declaration import LadderDeclaration, refuse_unless_comparison
from protea.core.method_seal import METHOD_IDENTITY_FIELDS
from protea.core.operations._paired_panels_stratum import validate_stratum

#: The nine panels, category by aspect, in canonical report order. There is no
#: tenth entry and there is no aggregate over them.
ALL_PANELS: tuple[str, ...] = (
    "NK:MFO",
    "NK:BPO",
    "NK:CCO",
    "LK:MFO",
    "LK:BPO",
    "LK:CCO",
    "PK:MFO",
    "PK:BPO",
    "PK:CCO",
)


class ComparePairedPanelsPayload(ProteaPayload, frozen=True):
    """Which two results are being compared, over which panels, at what precision.

    ``weighting`` is a closed vocabulary and not a bool, and there is no
    ``auto``: asking for the weighted estimator against an artefact that carries
    no weighted components is a refusal, never a silent fallback. That is the
    requirement expressed in the type rather than in a comment somebody has to
    read.

    ``operating_point`` is a closed vocabulary for the same argument. Two
    semantics give two intervals from the same bytes and neither is recoverable
    from the numbers afterwards, so the choice belongs on the job row beside
    the seed. There is no ``auto`` here either, for a sharper version of the
    reason: the only thing an ``auto`` could do is take the argmax, which is
    not a fallback but the other semantics, silently. Hence the two refusals
    below, which are one rule read in both directions: a declared threshold
    exactly when something reads it.

    Unknown keys are refused. ``{"artifact_root": ...}`` is a plausible typo of
    ``artifacts_root``, and without this it validates, resolves to no local
    root, sends the operation to the object store, finds nothing there and
    returns a successful job full of nulls.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    evaluation_result_id: Annotated[str, Field(description="system A, the arm under test")]
    baseline_evaluation_result_id: Annotated[str, Field(description="system B, the baseline")]
    panels: Annotated[
        list[str],
        Field(default_factory=lambda: list(ALL_PANELS), description="category:aspect keys"),
    ]
    n_resamples: Annotated[int, Field(default=2000, ge=1000)]
    seed: Annotated[int, Field(default=0, ge=0)]
    confidence: Annotated[float, Field(default=0.95, gt=0.5, lt=1.0)]
    power: Annotated[float, Field(default=0.80, gt=0.5, lt=1.0)]
    weighting: Literal["ia_weighted", "unweighted"] = "ia_weighted"
    operating_point: Literal["reselected_per_resample", "fixed"] = "reselected_per_resample"
    declared_tau: Annotated[
        float | None,
        Field(
            default=None,
            gt=0.0,
            lt=1.0,
            description=(
                "the threshold a fixed operating point is read at, required by "
                "operating_point='fixed' and refused otherwise. It must be one the "
                "artefact's grid carries: a tau off the grid is refused and never snapped "
                "to its neighbour, because a comparison reported at a threshold it was not "
                "evaluated at is not recoverable from its own result."
            ),
        ),
    ]
    min_population: Annotated[int, Field(default=30, ge=1)]
    interval_method: Literal["bca", "percentile"] = "bca"
    effect_of_interest: Annotated[
        float | None,
        Field(
            default=None,
            gt=0.0,
            lt=1.0,
            description=(
                "the smallest difference worth detecting, on the estimator's own scale. A "
                "null is read against this and never against the observed difference: "
                "without it a panel whose interval covers zero is reported as null_unread, "
                "because nothing in the data says which effects matter."
            ),
        ),
    ]
    population_rule: Literal["intersect", "require_identical"] = "intersect"
    min_jaccard: Annotated[float, Field(default=0.95, gt=0.0, le=1.0)]
    restrict_to_stratum: Annotated[
        dict[str, str] | None,
        Field(
            default=None,
            description=(
                "axis -> band, restricting the paired population to one stratum. "
                "None compares the whole panel, which is what every result before "
                "this field existed did."
            ),
        ),
    ]
    allow_frame_mismatch: bool = False
    method_axis: Annotated[
        list[str],
        Field(
            default_factory=list,
            description=(
                "the prediction_set fields this comparison claims to be varying, e.g. "
                "['annotation_set_id'] for a bank contrast. Everything else in the method "
                "must agree, field by field, and a field that moved and was not named here "
                "is a refusal that names it with both its values. The empty default is the "
                "claim that the method is held entirely still, which is what a comparison "
                "of two scoring configs or two rerankers over one prediction method asserts."
            ),
        ),
    ]
    declared_threshold_id: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "the weights whose artifact declares the arm's threshold per panel "
                "(operating_threshold, or a reading's fit), with operating_point='fixed'. "
                "Each arm is then read at its own declared threshold in each panel, in "
                "place of one declared_tau for every panel and both arms."
            ),
        ),
    ]
    baseline_declared_threshold_id: Annotated[
        str | None,
        Field(
            default=None,
            description="the same for the baseline; given together with declared_threshold_id",
        ),
    ]
    interval_family: Annotated[
        Literal["per_panel", "simultaneous"],
        Field(
            default="per_panel",
            description=(
                "'per_panel' resamples each panel alone with a seed of its own (every comparison "
                "before this field). 'simultaneous' draws one stream of protein multiplicities "
                "over the union of the panels' paired proteins, shared by every panel, and "
                "reads balanced simultaneous percentile intervals whose joint coverage "
                "reaches the confidence; percentile only, so bca is refused with it."
            ),
        ),
    ]
    ladder_declaration: Annotated[
        LadderDeclaration | None,
        Field(
            default=None,
            description=(
                "the rung of the literature ladder this comparison belongs to, declared before "
                "the read: the rung, the role (what the comparison answers), its label, the stack "
                "version where the rung reads one and, for a choice's confirmation, the job of the "
                "pick. Recorded with the job; it changes nothing the comparison computes."
            ),
        ),
    ]
    artifacts_root: str | None = None
    baseline_artifacts_root: str | None = None

    @field_validator("evaluation_result_id", "baseline_evaluation_result_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("an evaluation result id is required; there is nothing to compare")
        return value.strip()

    @field_validator("panels")
    @classmethod
    def _known_panels(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("omit panels to take all nine; an empty list compares nothing")
        duplicated = sorted({key for key in value if value.count(key) > 1})
        if duplicated:
            raise ValueError(
                f"panels {duplicated} are listed more than once; each panel would be "
                "resampled twice and reported once, and the progress count would say 1/2 "
                "for one panel"
            )
        unknown = [key for key in value if key not in ALL_PANELS]
        if unknown:
            raise ValueError(
                f"unknown panels {unknown}; the nine are {list(ALL_PANELS)}. There is no "
                "aggregate over them: the nine panels are nine populations, so a mean over "
                "them is a different estimator needing a joint resample over the union with "
                "per-panel membership tracked, which this operation does not compute."
            )
        return value

    @field_validator("method_axis")
    @classmethod
    def _known_axis(cls, value: list[str]) -> list[str]:
        """An axis field that is not method identity is refused here, not later.

        There is no ``allow_method_mismatch`` to pair with ``allow_frame_mismatch``,
        and that is the design. A frame mismatch is waived because the two numbers
        were measured differently and somebody may still want to look; a method
        mismatch has a waiver already, and it is to NAME the field, which costs one
        list entry and makes the job row say what the comparison was of. A boolean
        that waived the whole seal at once would put the campaign back where D2
        found it, with a delta attributable to nothing.
        """
        unknown = sorted(set(value) - set(METHOD_IDENTITY_FIELDS))
        if unknown:
            raise ValueError(
                f"method_axis names {unknown}, which is not method identity. The fields "
                f"that can be an axis are {list(METHOD_IDENTITY_FIELDS)}. A misspelled "
                "axis declares nothing and would be refused one gate later under the "
                "wrong name."
            )
        repeated = sorted({key for key in value if value.count(key) > 1})
        if repeated:
            raise ValueError(f"method_axis lists {repeated} more than once")
        return value

    @field_validator("restrict_to_stratum")
    @classmethod
    def _known_stratum(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        return validate_stratum(value)

    @model_validator(mode="after")
    def _coherent(self) -> ComparePairedPanelsPayload:
        if self.evaluation_result_id == self.baseline_evaluation_result_id:
            raise ValueError(
                "a result compared against itself yields a zero delta and a zero-width "
                "interval, which reads as a decisive null"
            )
        if bool(self.artifacts_root) != bool(self.baseline_artifacts_root):
            raise ValueError(
                "give both artifacts_root and baseline_artifacts_root or neither; one local "
                "and one remote is two different resolutions of where the bytes are"
            )
        self._one_declared_threshold()
        refuse_unless_comparison(self.ladder_declaration)
        if self.interval_family == "simultaneous" and self.interval_method != "percentile":
            raise ValueError(
                "interval_family='simultaneous' reads balanced percentile bounds, one per-panel "
                "level for every panel; a bca interval corrects each panel on its own and has "
                "no joint coverage. Set interval_method='percentile' (it defaults to 'bca')."
            )
        return self

    def _one_declared_threshold(self) -> None:
        """A fixed operating point names its threshold exactly one way; a re-selected one, none."""
        by_weights = (self.declared_threshold_id, self.baseline_declared_threshold_id)
        if (by_weights[0] is None) != (by_weights[1] is None):
            raise ValueError(
                "give declared_threshold_id and baseline_declared_threshold_id together: each "
                "arm is read at its own declared threshold, so one without the other names "
                "half a comparison"
            )
        for value in by_weights:
            if value is not None:
                uuid.UUID(value)
        declared = by_weights[0] is not None
        if declared and self.declared_tau is not None:
            raise ValueError(
                "declared_tau and the declared threshold ids both name the threshold; a "
                "comparison is read at one of them"
            )
        if self.operating_point == "fixed" and self.declared_tau is None and not declared:
            raise ValueError(
                "operating_point='fixed' needs declared_tau or the two declared threshold ids: "
                "the threshold has to come from the caller. The only other place it could come "
                "from is the argmax of the panel curve, and taking that would reinstate the "
                "max-over-tau selection a fixed operating point exists to remove, under the name "
                "that says it was removed. Declare the tau the campaign reads its primary "
                "metric at."
            )
        if self.operating_point != "fixed" and (self.declared_tau is not None or declared):
            raise ValueError(
                f"a declared threshold was given with operating_point={self.operating_point!r}, "
                "which re-selects the threshold inside every resample and reads nothing at the "
                "declared one. Recording a threshold no arithmetic used tells a later reader "
                "that a decision was taken when it was not. Set operating_point='fixed', or "
                "drop the declared threshold."
            )

    @property
    def alpha(self) -> float:
        return 1.0 - self.confidence

    @property
    def variant(self) -> str:
        return "weighted" if self.weighting == "ia_weighted" else "unweighted"

    @property
    def estimator(self) -> str:
        """The metric's own name. ``f_micro_w`` never labels an unweighted number."""
        return "f_micro_w" if self.weighting == "ia_weighted" else "f_micro"
