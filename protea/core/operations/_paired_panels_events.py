"""What a paired comparison reports, separated from what it decides.

Three events at three levels, and the separation between them is the point.
:func:`_emit_pairing` says which population was resampled, :func:`_emit_panel`
says what was measured on it, and :func:`_emit_verdict` says what a reader
should conclude -- which is always a warning when the answer is "not enough
power", because a filtered log that drops info would otherwise drop the one
line saying the number cannot carry the claim.

Split out of :mod:`compare_paired_panels` when it passed the file budget. The
budget was right that two concerns had accumulated in one module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from protea.core.contracts.operation import EmitFn

if TYPE_CHECKING:
    from protea.core.operations._paired_panels_panel import PanelConfig
    from protea.core.operations.compare_paired_panels import ComparePairedPanelsPayload


def _emit_pairing(panel: dict[str, Any], key: str, p: ComparePairedPanelsPayload, emit: EmitFn) -> None:
    emit(
        "compare_paired_panels.pairing",
        None,
        {
            "panel": key,
            **{k: panel.get(k) for k in ("n_paired", "n_only_a", "n_only_b", "population_rule")},
        },
        "info",
    )
    if panel["status"] == "refused":
        emit(
            "compare_paired_panels.panel_refused",
            f"{key}: {panel['message']}",
            {"panel": key, "reason": panel["reason"], **{
                k: panel.get(k) for k in ("n_paired", "n_only_a", "n_only_b", "jaccard")
            }},
            "error",
        )
        return
    if panel["status"] == "empty":
        emit(
            "compare_paired_panels.panel_absent",
            f"{key}: no artefact for this panel on one or both sides, so it was not "
            "computed; it is reported as absent and never as a null",
            {"panel": key, "reason": panel["reason"]},
            "warning",
        )
        return
    if not panel["reportable"]:
        emit(
            "compare_paired_panels.withheld",
            f"{key} holds {panel['n_paired']} paired proteins, below the floor of "
            f"{p.min_population}; computed and flagged, never dropped",
            {"panel": key, "n_paired": panel["n_paired"], "min_population": p.min_population},
            "warning",
        )
    if panel.get("arm_silent"):
        emit(
            "compare_paired_panels.arm_silent",
            f"{key}: arm {panel['arm_silent']} predicts nothing on this panel and scores "
            "zero; the difference against it is a real difference, so it is named rather "
            "than dropped",
            {"panel": key, "arm": panel["arm_silent"]},
            "info",
        )
    if panel.get("interval_fallback_reason"):
        emit(
            "compare_paired_panels.interval_fallback",
            None,
            {"panel": key, "reason": panel["interval_fallback_reason"]},
            "warning",
        )


def _emit_panel(
    panel: dict[str, Any],
    key: str,
    p: ComparePairedPanelsPayload,
    cfg: PanelConfig,
    emit: EmitFn,
) -> None:
    """What the panel measured, at the level it belongs.

    What a reader should CONCLUDE from it is emitted separately, because the
    two answer different questions and are filtered at different levels: this
    one is always info, and a verdict about power is a warning.

    ``operating_point`` sits beside ``tau_a`` and ``tau_b`` because the three
    are one fact: two taus that may differ are each an argmax, two equal ones
    are the declared threshold. It comes off the config, the object the
    resampler was handed, so this event reports what ran and not what was asked.
    """
    _emit_pairing(panel, key, p, emit)
    reported = (
        "delta",
        "ci_low",
        "ci_high",
        "interval_method",
        "minimum_detectable_effect",
        "resolves",
        "status",
        "verdict",
    )
    emit(
        "compare_paired_panels.panel",
        None,
        {
            "panel": key,
            **{k: panel.get(k) for k in reported},
            "operating_point": cfg.operating_point,
            "tau_a": (panel["a"] or {}).get("tau"),
            "tau_b": (panel["b"] or {}).get("tau"),
            "tau_a_switched_fraction": panel["diagnostics"].get("tau_a_switched_fraction"),
        },
        "info",
    )
    _emit_verdict(panel, key, emit)


def _emit_verdict(panel: dict[str, Any], key: str, emit: EmitFn) -> None:
    """Which of the three readings of an interval covering zero this one is.

    The distinction is the operation's reason to exist. An interval covering
    zero can mean the two systems are the same down to a size worth caring
    about, or that the comparison could never have resolved anything, or that
    nobody declared what size was worth caring about so the question is
    unanswered. Those are different conclusions and only the first is evidence.

    The first is info and the other two are warnings, so a reader filtering the
    job event log on level sees the ones that need an action.
    """
    mde, effect = panel["minimum_detectable_effect"], panel.get("effect_of_interest")
    if panel["status"] == "underpowered" and mde is not None:
        emit(
            "compare_paired_panels.underpowered",
            f"{key} could not have resolved an effect smaller than {mde:.4f} on "
            f"{panel['n_paired']} proteins, and the effect of interest is {effect}; the "
            "interval covering zero here is about power, not about the two systems being "
            "the same",
            {
                "panel": key,
                "minimum_detectable_effect": mde,
                "effect_of_interest": effect,
                "n_paired": panel["n_paired"],
            },
            "warning",
        )
    elif panel["status"] == "null_unread" and mde is not None:
        emit(
            "compare_paired_panels.null_unread",
            f"{key} covers zero and could have resolved {mde:.4f}, but no effect_of_interest "
            "was declared, so nothing here says whether that is evidence of sameness or "
            "evidence of nothing. Declare the smallest difference worth detecting and re-run; "
            "this operation will not pick the campaign's standard for it",
            {"panel": key, "minimum_detectable_effect": mde},
            "warning",
        )
    elif panel["status"] == "null_with_power":
        emit(
            "compare_paired_panels.null_with_power",
            f"{key} covers zero and could have resolved {mde:.4f}, which is at or below the "
            f"declared effect of interest {effect}; this null is evidence of sameness down "
            "to that size, and is not an unanswered question",
            {"panel": key, "minimum_detectable_effect": mde, "effect_of_interest": effect},
            "info",
        )
