"""One panel, end to end: pairing, the two arms, the interval, the verdict.

Nine panels, nine populations. An aspect scores only the proteins that gained in
that aspect, so nothing here ever pools across panels and nothing here reports a
number without the population it was computed over.

This module holds the per-panel driver so that
:mod:`protea.core.operations.compare_paired_panels` can stay the payload, the
provenance gate and the emissions, and so that the arithmetic can be exercised
on a ten-protein panel written by hand without a job, a session or a store.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from protea.core.operations import _paired_panels_bootstrap as boot
from protea.core.operations._paired_panels_artifact import (
    NAMESPACE_TO_CAFA,
    GridMeta,
    PanelComparabilityError,
    SettingGrid,
)

# The exact-path control. ``_micro`` is the function behind every published
# ``f_micro_w`` cell in this project; the vectorised curve in the bootstrap
# module is a second implementation of the same metric, and two implementations
# of one metric drift. Comparing them at the full sample makes the drift an
# error rather than a discrepancy nobody notices.
from protea.core.operations._run_cafa_strata import _micro

CAFA_TO_NAMESPACE = {code: ns for ns, code in NAMESPACE_TO_CAFA.items()}

#: The stored blob rounds to four decimals, so parity against it is checked at
#: half a unit in the last place it holds.
STORED_PARITY_ATOL = 5e-5

#: Two implementations of one metric on bit-identical inputs. Anything above a
#: few units in the last place is a real disagreement.
EXACT_PATH_ATOL = 1e-12

#: Ground-truth mass is compared RELATIVELY. The contract lets one producer
#: store the grid as float32 and another as float64, and at an IA-weighted BPO
#: mass of a few hundred the two differ by more than any absolute 1e-6, so an
#: absolute tolerance would accuse two identical ground truths of being
#: different exclusion sets.
GROUND_TRUTH_RTOL = 1e-6
GROUND_TRUTH_ATOL = 1e-9


@dataclass(frozen=True)
class PanelConfig:
    """Everything the resampler needs that is not the data."""

    alpha: float
    power: float
    n_resamples: int
    seed: int
    min_population: int
    force_percentile: bool
    #: The effect the caller came to detect. Without it a null cannot be read:
    #: see :func:`verdict_for`.
    effect_of_interest: float | None = None


@dataclass
class Side:
    """One arm: its provenance row, where its bytes are, and its loaded grids."""

    result_id: str
    root: Path
    provenance: dict[str, Any]
    grids: dict[str, SettingGrid]

    def meta(self) -> GridMeta | None:
        for grid in self.grids.values():
            return grid.meta
        return None

    def panel(self, setting: str, namespace: str) -> boot.PanelArrays | None:
        grid = self.grids.get(setting)
        return None if grid is None else grid.panels.get(namespace)

    def stored_metric(self, setting: str, aspect: str) -> float | None:
        """The published cell for this panel, when the result row carries one."""
        blob = self.provenance.get("results") or {}
        cell = (blob.get(setting) or {}).get(aspect) or {}
        value = cell.get("f_micro_w")
        return None if value is None else float(value)


def population_stats(
    a: boot.PanelArrays, b: boot.PanelArrays, rule: str
) -> dict[str, Any]:
    """What the two arms share, always reported, whatever is decided about it."""
    set_a, set_b = set(a.accessions), set(b.accessions)
    shared = sorted(set_a & set_b)
    union = len(set_a | set_b)
    return {
        "population_rule": rule,
        "n_paired": len(shared),
        "n_only_a": len(set_a - set_b),
        "n_only_b": len(set_b - set_a),
        "jaccard": (len(shared) / union) if union else 1.0,
    }


def population_refusal(
    a: boot.PanelArrays,
    b: boot.PanelArrays,
    stats: dict[str, Any],
    rule: str,
    min_jaccard: float,
) -> tuple[str, str] | None:
    """``(reason, message)`` when this panel's two populations are not one, else None.

    Returned rather than raised. A population shift is a property of ONE panel,
    and raising took the other eight down with it, including panels that were
    perfectly paired; the only remedy was to loosen the global threshold, which
    is worse than degrading the panel that has the problem. The refusal is still
    a refusal: the panel reports no delta, no interval and no verdict, and the
    caller emits it at error level.

    Zero overlap is the loudest case, not the quietest. It was previously the
    single population state that did NOT refuse, landing in an empty panel
    beside a genuinely absent artefact, and it is the strongest possible
    evidence of an identifier-space bug: isoform suffixes on one side, entry
    names against accessions, a version suffix written by one producer.
    """
    set_a, set_b = set(a.accessions), set(b.accessions)
    if rule == "require_identical" and (stats["n_only_a"] or stats["n_only_b"]):
        return (
            "population_not_identical",
            f"population_rule=require_identical: {stats['n_only_a']} proteins only in A and "
            f"{stats['n_only_b']} only in B; examples {sorted(set_a - set_b)[:20]} and "
            f"{sorted(set_b - set_a)[:20]}",
        )
    if not stats["n_paired"]:
        return (
            "population_disjoint",
            f"the two arms share no protein at all on this panel: {a.n} accessions against "
            f"{b.n}, examples {sorted(set_a)[:5]} and {sorted(set_b)[:5]}. Two systems "
            "evaluated on the same frame do not have disjoint populations, so this is an "
            "identifier-space difference (isoform suffixes, entry names against accessions, "
            "a version suffix on one side) and not a result",
        )
    if stats["jaccard"] < min_jaccard:
        return (
            "population_overlap_below_floor",
            f"the two arms share only {stats['jaccard']:.3f} of their proteins on this panel, "
            f"below min_jaccard={min_jaccard}; that is a population difference arriving "
            "dressed as a method difference",
        )
    return None


def align(
    a: boot.PanelArrays, b: boot.PanelArrays
) -> tuple[boot.PanelArrays, boot.PanelArrays]:
    """One protein set for both arms, in one order.

    Union with zeros is not offered. Crediting a system with an empty prediction
    on a protein it was never asked about changes the number that system scores,
    so its own published cell would stop being recoverable from the comparison,
    and a difference in evaluation-set membership would arrive dressed as a
    difference in method.
    """
    shared = sorted(set(a.accessions) & set(b.accessions))
    index_a = {acc: i for i, acc in enumerate(a.accessions)}
    index_b = {acc: i for i, acc in enumerate(b.accessions)}
    order_a = np.array([index_a[acc] for acc in shared], dtype=np.int64)
    order_b = np.array([index_b[acc] for acc in shared], dtype=np.int64)
    return a.take(order_a), b.take(order_b)


def assert_same_ground_truth(a: boot.PanelArrays, b: boot.PanelArrays, panel: str) -> None:
    """``g_i`` is a property of the reference, so the two arms must agree on it.

    Compared relatively rather than absolutely: see :data:`GROUND_TRUTH_RTOL`.
    """
    if a.n and bool(
        np.any(
            np.abs(a.n_gt - b.n_gt)
            > GROUND_TRUTH_ATOL + GROUND_TRUTH_RTOL * np.abs(b.n_gt)
        )
    ):
        raise PanelComparabilityError(
            f"panel {panel}: the two arms carry different ground-truth mass for the same "
            "proteins, so they were scored against different ground truth or different "
            "exclusion sets and are not comparable, whatever their metadata says"
        )


def exact_path_control(arrays: boot.PanelArrays, op: boot.OperatingPoint) -> None:
    cell = _micro(
        float(arrays.tp[:, op.tau_index].sum()),
        float(arrays.pred[:, op.tau_index].sum()),
        float(arrays.n_gt.sum()),
        arrays.n,
    )
    if abs(cell.f_micro_w - op.value) > EXACT_PATH_ATOL:
        raise PanelComparabilityError(
            f"the vectorised panel estimator reads {op.value!r} where _run_cafa_strata._micro, "
            f"the function behind every published f_micro_w cell, reads {cell.f_micro_w!r}. "
            "Two implementations of one metric have drifted; refusing rather than reporting "
            "an interval around a number the rest of the platform would not reproduce."
        )


def arm_block(
    shared: boot.PanelArrays, own: boot.PanelArrays, meta: GridMeta, stored: float | None
) -> tuple[boot.OperatingPoint, dict[str, Any]]:
    """One arm's numbers, on the paired population and on its own.

    Both are carried because the delta is defined on the intersection while the
    published marginal is over each system's own coverage. Printing only the
    paired estimate would put a number in a table that does not match the one
    already published under the same panel name, which is a plausible number
    over the wrong population.
    """
    op_shared = boot.select_operating_point(boot.panel_curve(shared))
    op_own = boot.select_operating_point(boot.panel_curve(own))
    exact_path_control(shared, op_shared)
    exact_path_control(own, op_own)
    parity: bool | None = None
    if stored is not None:
        parity = abs(op_own.value - stored) <= STORED_PARITY_ATOL
        if not parity:
            raise PanelComparabilityError(
                f"recomposing the panel from the grid artefact gives {op_own.value:.6f} where "
                f"the evaluation result stores {stored:.4f}. The components and the published "
                "number do not describe the same run; refusing before any resampling, because "
                "this is the guard whose absence let a wrong-tau slice sit unnoticed."
            )
    scored = int(np.count_nonzero(shared.pred[:, op_shared.tau_index] > 0.0))
    scored_own = int(np.count_nonzero(own.pred[:, op_own.tau_index] > 0.0))
    return op_shared, {
        "estimate": float(op_shared.value),
        "tau": float(meta.tau_grid[op_shared.tau_index]),
        "tau_index": op_shared.tau_index,
        "tau_star_tied": op_shared.tied,
        "n_tau_at_max": op_shared.n_tau_at_max,
        "estimate_own_population": float(op_own.value),
        "tau_own_population": float(meta.tau_grid[op_own.tau_index]),
        "n_own": own.n,
        # The rows of the artefact ARE the proteins cafaeval scored, so the
        # coverage that sits beside every published cell is recoverable here
        # rather than being an unstated part of the frame. It is reported at the
        # selected tau because it moves with tau, which is the property this
        # project has already been burned by.
        "n_scored_at_tau": scored,
        "coverage_at_tau": (scored / shared.n) if shared.n else None,
        "n_scored_own_population": scored_own,
        "coverage_own_population": (scored_own / own.n) if own.n else None,
        "population_shift": float(op_shared.value - op_own.value),
        "stored_metric": stored,
        "estimator_parity_checked": parity,
        "silent": bool(float(shared.pred.sum()) == 0.0),
    }




def empty_panel(stats: dict[str, Any], reason: str, status: str) -> dict[str, Any]:
    """A panel that was not measured is a different fact from one that scored zero.

    Every number is null rather than zero, on purpose: one of the two is a real
    result, and writing zero for the other makes them indistinguishable.
    """
    return {
        **stats,
        "status": status,
        "verdict": "not_computed",
        "reason": reason,
        "message": None,
        "reportable": False,
        "a": None,
        "b": None,
        "delta": None,
        "ci_low": None,
        "ci_high": None,
        "interval_method": None,
        "interval_fallback_reason": None,
        "minimum_detectable_effect": None,
        "effect_of_interest": None,
        "excludes_zero": False,
        "arm_silent": None,
        "resolves": False,
        "underpowered": True,
        "diagnostics": {},
    }


def refused_panel(stats: dict[str, Any], reason: str, message: str) -> dict[str, Any]:
    """A panel whose precondition failed, named, with the other eight left alone.

    Status ``refused`` rather than ``empty``: the artefact was there and was
    read, and what failed is a stated precondition on the two populations. The
    caller emits this at error level, so the run cannot pass for clean.
    """
    return {**empty_panel(stats, reason, "refused"), "message": message}


def _diagnostics(
    draws: boot.Draws,
    interval: boot.Interval,
    mde: boot.DetectableEffect,
    ops: tuple[boot.OperatingPoint, boot.OperatingPoint],
) -> dict[str, Any]:
    return {
        "z0": interval.z0,
        "acceleration": interval.acceleration,
        "bootstrap_sd": float(draws.deltas.std(ddof=1)),
        "bootstrap_skew": boot.skewness(draws.deltas),
        "mde_positive": mde.positive,
        "mde_negative": mde.negative,
        "mde_basis": mde.basis,
        "mde_reason": mde.reason,
        "tau_a_switched_fraction": boot.switched_fraction(draws.a_tau_index, ops[0].tau_index),
        "tau_b_switched_fraction": boot.switched_fraction(draws.b_tau_index, ops[1].tau_index),
        "tau_a_distinct": int(np.unique(draws.a_tau_index).size),
        "tau_b_distinct": int(np.unique(draws.b_tau_index).size),
    }


def resample(
    a: boot.PanelArrays,
    b: boot.PanelArrays,
    cfg: PanelConfig,
    ops: tuple[boot.OperatingPoint, boot.OperatingPoint],
    panel_index: int,
) -> dict[str, Any]:
    """The bootstrap, the interval and the MDE for one aligned panel.

    The seed is spawned from the panel's canonical index rather than taken in
    sequence, so a run over three panels and a run over nine give the same
    numbers for the panels they share, whatever order they were requested in.
    """
    delta = float(ops[0].value - ops[1].value)
    seed = np.random.SeedSequence(cfg.seed, spawn_key=(panel_index,))
    draws = boot.paired_bootstrap(a, b, n_resamples=cfg.n_resamples, seed=seed)
    jack = boot.jackknife_deltas(a, b)
    interval = boot.build_interval(
        draws.deltas, delta, jack, alpha=cfg.alpha, force_percentile=cfg.force_percentile
    )
    mde = boot.minimum_detectable_effect(draws.deltas, delta, interval, power=cfg.power)
    return {
        "delta": delta,
        "ci_low": interval.low,
        "ci_high": interval.high,
        "interval_method": interval.method,
        "interval_fallback_reason": interval.fallback_reason,
        "minimum_detectable_effect": mde.mde,
        "effect_of_interest": cfg.effect_of_interest,
        "excludes_zero": interval.excludes_zero(),
        "diagnostics": _diagnostics(draws, interval, mde, ops),
    }


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


def absent_stats(rule: tuple[str, float]) -> dict[str, Any]:
    return {"population_rule": rule[0], "n_paired": 0, "n_only_a": 0, "n_only_b": 0, "jaccard": 0.0}


def _arms(
    sides: tuple[Side, Side], key: str, aligned: tuple[boot.PanelArrays, boot.PanelArrays],
    raw: tuple[boot.PanelArrays, boot.PanelArrays],
) -> tuple[tuple[boot.OperatingPoint, boot.OperatingPoint], dict[str, Any], dict[str, Any]]:
    setting, aspect = key.split(":")
    op_a, block_a = arm_block(
        aligned[0], raw[0], sides[0].grids[setting].meta, sides[0].stored_metric(setting, aspect)
    )
    op_b, block_b = arm_block(
        aligned[1], raw[1], sides[1].grids[setting].meta, sides[1].stored_metric(setting, aspect)
    )
    return (op_a, op_b), block_a, block_b


def panel_result(
    sides: tuple[Side, Side],
    key: str,
    cfg: PanelConfig,
    index: int,
    rule: tuple[str, float],
) -> dict[str, Any]:
    """One panel's whole answer, including the answers that are refusals to answer."""
    setting, aspect = key.split(":")
    namespace = CAFA_TO_NAMESPACE[aspect]
    raw_a, raw_b = sides[0].panel(setting, namespace), sides[1].panel(setting, namespace)
    if raw_a is None or raw_b is None:
        return empty_panel({"panel": key, **absent_stats(rule)}, "artefact_absent_for_panel", "empty")
    stats = {"panel": key, **population_stats(raw_a, raw_b, rule[0])}
    refusal = population_refusal(raw_a, raw_b, stats, rule[0], rule[1])
    if refusal is not None:
        return refused_panel(stats, *refusal)
    a, b = align(raw_a, raw_b)
    # Assert the population, never infer it from a count reported elsewhere.
    if not (a.n == b.n == stats["n_paired"]):
        raise PanelComparabilityError(
            f"panel {key}: declared {stats['n_paired']} paired proteins but the arrays hold "
            f"{a.n} and {b.n}; the number and the population it is over have come apart"
        )
    assert_same_ground_truth(a, b, key)
    ops, block_a, block_b = _arms(sides, key, (a, b), (raw_a, raw_b))
    delta = float(ops[0].value - ops[1].value)
    silent = [n for n, blk in (("A", block_a), ("B", block_b)) if blk["silent"]]
    arm_silent = silent[0] if len(silent) == 1 else None
    if a.n < 2:
        # Computed, and unresolvable. Not the same fact as an artefact that was
        # never read, so the verdict says "not resolved" and the tally counts it
        # among the panels that could not answer rather than among the panels
        # that were not asked.
        floor = empty_panel(stats, "population_below_bootstrap_floor", "unresolvable")
        return {
            **floor,
            "verdict": "not_resolved",
            "a": block_a,
            "b": block_b,
            "delta": delta,
            "arm_silent": arm_silent,
        }
    panel: dict[str, Any] = {
        **stats,
        "a": block_a,
        "b": block_b,
        # A system that predicts nothing genuinely scores zero, and the
        # difference against it is a real difference. The arm is named, not
        # dropped and not treated as missing data.
        "arm_silent": arm_silent,
        "reportable": a.n >= cfg.min_population,
        "reason": None,
        "message": None,
        **resample(a, b, cfg, ops, index),
    }
    panel["status"], panel["verdict"] = verdict_for(panel)
    panel["resolves"] = panel["verdict"] in ("a_greater", "b_greater")
    panel["underpowered"] = panel["status"] == "underpowered"
    return panel


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
