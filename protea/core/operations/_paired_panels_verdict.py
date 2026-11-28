"""What a panel's interval and detectable effect say, and the one-line tally of the nine.

Split out of :mod:`protea.core.operations._paired_panels_panel` when the panel
gained a second way to be resampled (the simultaneous family, whose bounds this
reads the same way); the panel module re-exports every name here.
"""

from __future__ import annotations

from typing import Any

from protea.core.operations import _paired_panels_bootstrap as boot


def verdict_for(panel: dict[str, Any]) -> tuple[str, str]:
    """``(status, verdict)`` from what the interval and the MDE actually say.

    **A null is read against the effect the caller came to detect, never against
    the observed difference.** The earlier rule compared the MDE to
    ``abs(delta)``, and that comparison can never fire: the MDE is built from
    the same bootstrap distribution as the interval, at power 0.80, so it is
    always about 1.43 times the half-width, while an interval covering zero
    forces ``abs(delta)`` to be at most the half-width. Measured over 2,000
    random panels the branch fired 0 times, so every null the campaign could
    ever produce was stamped "could not have resolved anything", which is the
    exact misreading the requirement exists to prevent, pointing the other way.
    It is also the wrong comparison on its own terms: an effect is worth
    detecting or not for reasons outside this run, so the threshold is a payload
    field and not a function of the answer.

    Three readings of an interval that covers zero, and they are different
    facts:

    ``null_with_power``   the comparison could have resolved an effect the size
                          the caller declared, and did not find one. That is
                          evidence of sameness, bounded by the MDE.
    ``underpowered``      the smallest effect this comparison could have found
                          is larger than the one the caller cares about. The
                          null says nothing about the two systems.
    ``null_unread``       no effect of interest was declared, so the null cannot
                          be read at all. The MDE is reported and the caller is
                          told what to declare; guessing a threshold here would
                          be this operation inventing the campaign's standard.

    A panel below the population floor is forced to ``not_resolved`` even when
    its interval excludes zero: an interval from a handful of units has coverage
    the bootstrap cannot vouch for, and reporting a win from one is how a
    fluctuation becomes a published finding. A degenerate resample distribution
    is never a win either, whatever its zero-width interval appears to exclude.

    Under the simultaneous family the interval and the minimum detectable effect
    this reads are the panel's simultaneous bounds and the effect read from them
    (:mod:`protea.core.operations._paired_panels_simultaneous`): a panel resolves
    when its simultaneous interval excludes zero, and a null is read against the
    effect the joint interval could have found.
    """
    mde = panel["minimum_detectable_effect"]
    effect = panel.get("effect_of_interest")
    if panel["interval_method"] == boot.DEGENERATE:
        return "unresolvable", "not_resolved"
    if not panel["reportable"]:
        return "underpowered", "not_resolved"
    if panel["excludes_zero"]:
        return "ok", ("a_greater" if panel["delta"] > 0 else "b_greater")
    if mde is None:
        return "unresolvable", "not_resolved"
    if effect is None:
        return "null_unread", "not_resolved"
    if mde <= effect:
        return "null_with_power", "not_resolved"
    return "underpowered", "not_resolved"


#: Every bucket a panel can land in, and they are six because they are six
#: different facts. Collapsing ``null_unread`` and ``null_with_power`` into
#: ``underpowered`` is what made a readable null unreportable; collapsing
#: ``refused`` into ``not_computed`` is what let an identifier-space bug look
#: like a missing file.
TALLY_KEYS: tuple[str, ...] = (
    "resolved",
    "null_with_power",
    "null_unread",
    "underpowered",
    "refused",
    "not_computed",
)


def tally(panels: dict[str, dict[str, Any]]) -> dict[str, int]:
    """The one-line reading of the nine, stated once and plainly."""
    counts = dict.fromkeys(TALLY_KEYS, 0)
    for panel in panels.values():
        if panel["status"] == "refused":
            counts["refused"] += 1
        elif panel["verdict"] == "not_computed":
            counts["not_computed"] += 1
        elif panel["resolves"]:
            counts["resolved"] += 1
        elif panel["status"] in ("null_with_power", "null_unread"):
            counts[panel["status"]] += 1
        else:
            counts["underpowered"] += 1
    return counts
