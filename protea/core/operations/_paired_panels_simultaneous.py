"""Simultaneous intervals for the panels of one comparison: one stream, one level, joint coverage.

**Why.** A per-panel 95 % interval read one panel at a time would resolve an
arm with no effect in its favour in some of nine panels about one time in six.
The adoption rule of the literature ladder reads the nine panels as one family,
so their intervals must hold together: the chance that any panel's interval
misses is held at ``1 - confidence``.

**One stream of resamples.** Each replicate draws protein multiplicities once,
over the union of the panels' paired proteins, and every panel computes its
difference with those multiplicities: a protein carries its rows into every
panel it belongs to (a protein can be partial knowledge in one aspect and
limited in another). Resampling each panel alone, with a seed of its own, gives
nine independent marginals and no joint coverage.

**Balanced percentile intervals** (Beran 1988, "Balanced simultaneous
confidence sets", JASA 83:679-686). Each panel's interval runs from the order
statistic of rank ``ceil(g/2 B)`` of its ``B`` replicate differences to the one
of rank ``ceil((1 - g/2) B)``, with one per-panel level ``g`` for every panel:
the largest between the Bonferroni ``alpha / k`` and ``alpha`` at which the
share of replicates with every panel inside its interval reaches the
confidence. It adapts to the dependence between panels: independent panels
need close to Bonferroni, identical ones need ``alpha``.

**Monte Carlo error of a bound, from its order statistics.** A bound is the
order statistic of rank ``r = ceil(q B)``. Its error is half the distance
between the order statistics of ranks ``r - s`` and ``r + s``, with ``s =
ceil(sqrt(B q (1 - q)))``, one binomial standard deviation of the rank. The
caller doubles ``B`` until every bound's error is below its tolerance; the
result records them.

**The detectable effect** is read with the bootstrap's own formula
(:func:`~protea.core.operations._paired_panels_bootstrap.minimum_detectable_effect`)
from the simultaneous bounds in place of a per-panel interval, so a null is read
against what the joint interval could have found.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

import numpy as np

from protea.core.operations import _paired_panels_bootstrap as boot
from protea.core.operations._paired_panels_panel import (
    PanelConfig,
    PreparedPanel,
    Side,
    resampled_block,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from protea.core.operating_point import DeclaredThreshold
    from protea.core.operations._paired_panels_payload import ComparePairedPanelsPayload

#: The largest multiplicity block drawn at once, in matrix elements: about 41 MB
#: of float64, the figure the per-panel resampler holds itself to.
_COUNTS_BUDGET = 5_242_880

#: Bisection steps on the level: far below any rank's width at any ``B``.
_BISECTIONS = 60


def _chunk_for(width: int) -> int:
    """Resamples per block over ``width`` proteins, a function of the population alone."""
    return max(1, min(boot._CHUNK, _COUNTS_BUDGET // max(width, 1)))


def shared_draws(
    pairs: Mapping[str, tuple[boot.PanelArrays, boot.PanelArrays, boot.TauIndex]],
    *,
    n_resamples: int,
    seed: int,
) -> dict[str, boot.Draws]:
    """Every panel's paired draws from one stream of multiplicities over the union of proteins.

    ``pairs`` maps each panel to its aligned arms and their columns. The union
    is sorted, so the stream depends on the proteins and the seed alone, not on
    the order the panels were asked in.
    """
    union = sorted(set().union(*(set(a.accessions) for a, _, _ in pairs.values())))
    where = {accession: i for i, accession in enumerate(union)}
    columns = {
        key: np.array([where[acc] for acc in a.accessions], dtype=np.int64)
        for key, (a, _, _) in pairs.items()
    }
    width = len(union)
    rng = np.random.default_rng(np.random.SeedSequence(seed))
    probabilities = np.full(width, 1.0 / width)
    chunk = _chunk_for(width)
    parts: dict[str, list[tuple[np.ndarray, ...]]] = {key: [] for key in pairs}
    done = 0
    while done < n_resamples:
        size = min(chunk, n_resamples - done)
        counts = rng.multinomial(width, probabilities, size=size).astype(np.float64)
        for key, (a, b, taus) in pairs.items():
            parts[key].append(boot.draw_pair(counts[:, columns[key]], a, b, taus))
        done += size
    return {
        key: boot.Draws(*(np.concatenate([block[i] for block in blocks]) for i in range(5)))
        for key, blocks in parts.items()
    }


def _rank(size: int, q: float) -> int:
    return min(max(math.ceil(q * size), 1), size)


def order_statistic(ordered: np.ndarray, q: float) -> float:
    """The bound at quantile ``q``: the order statistic of rank ``ceil(q B)``."""
    return float(ordered[_rank(ordered.size, q) - 1])


def bound_error(ordered: np.ndarray, q: float) -> float:
    """Half the distance between the order statistics one binomial deviation either side."""
    size = ordered.size
    rank = _rank(size, q)
    spread = math.ceil(math.sqrt(size * q * (1.0 - q)))
    low = ordered[max(rank - spread, 1) - 1]
    high = ordered[min(rank + spread, size) - 1]
    return float(high - low) / 2.0


def joint_coverage(
    deltas: Mapping[str, np.ndarray], ordered: Mapping[str, np.ndarray], level: float
) -> float:
    """The share of replicates with every panel's difference inside its interval at ``level``."""
    inside: np.ndarray | None = None
    for key, values in deltas.items():
        low = order_statistic(ordered[key], level / 2.0)
        high = order_statistic(ordered[key], 1.0 - level / 2.0)
        within = (values >= low) & (values <= high)
        inside = within if inside is None else inside & within
    return 1.0 if inside is None else float(np.mean(inside))


def balanced_level(deltas: Mapping[str, np.ndarray], confidence: float) -> tuple[float, float]:
    """``(g, joint coverage)``: the largest per-panel level in ``[alpha / k, alpha]`` that covers.

    When even the Bonferroni level does not reach the confidence (possible on a
    discrete resample distribution), the Bonferroni level is returned with the
    coverage it reached, which the result records rather than hides.
    """
    alpha = 1.0 - confidence
    ordered = {key: np.sort(values) for key, values in deltas.items()}
    floor_level = alpha / max(len(deltas), 1)
    widest = joint_coverage(deltas, ordered, alpha)
    if widest >= confidence:
        return alpha, widest
    floor_coverage = joint_coverage(deltas, ordered, floor_level)
    if floor_coverage < confidence:
        return floor_level, floor_coverage
    low, high = floor_level, alpha
    for _ in range(_BISECTIONS):
        middle = (low + high) / 2.0
        if joint_coverage(deltas, ordered, middle) >= confidence:
            low = middle
        else:
            high = middle
    return low, joint_coverage(deltas, ordered, low)


def _interval(deltas: np.ndarray, point: float, level: float) -> tuple[boot.Interval, dict]:
    """One panel's simultaneous interval at ``level`` and its bounds' Monte Carlo errors."""
    if np.unique(deltas).size < 2:
        interval = boot.Interval(
            point, point, boot.DEGENERATE, "bootstrap_distribution_degenerate", None, None
        )
        return interval, {"low": 0.0, "high": 0.0}
    ordered = np.sort(deltas)
    low_q, high_q = level / 2.0, 1.0 - level / 2.0
    interval = boot.Interval(
        order_statistic(ordered, low_q),
        order_statistic(ordered, high_q),
        boot.PERCENTILE,
        None,
        None,
        None,
    )
    return interval, {"low": bound_error(ordered, low_q), "high": bound_error(ordered, high_q)}


def simultaneous_resamples(
    prepared: Mapping[str, PreparedPanel], cfg: PanelConfig
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Each prepared panel's resample block under one stream, and the family's record.

    The family is the panels that were resampled; a panel absent, refused or
    below the bootstrap floor is not in it.
    """
    pairs = {key: (*panel.arms, panel.taus) for key, panel in prepared.items()}
    if not pairs:
        return {}, _family_record({}, 1.0 - cfg.alpha, (1.0 - cfg.alpha, 1.0), {})
    draws = shared_draws(pairs, n_resamples=cfg.n_resamples, seed=cfg.seed)
    deltas = {key: found.deltas for key, found in draws.items()}
    level, coverage = balanced_level(deltas, 1.0 - cfg.alpha)
    blocks: dict[str, dict[str, Any]] = {}
    errors: dict[str, dict[str, float]] = {}
    for key, panel in prepared.items():
        point = float(panel.ops[0].value - panel.ops[1].value)
        interval, errors[key] = _interval(deltas[key], point, level)
        blocks[key] = {
            **resampled_block(draws[key], interval, cfg, panel.ops),
            "monte_carlo_error": errors[key],
        }
    return blocks, _family_record(prepared, 1.0 - cfg.alpha, (level, coverage), errors)


def _family_record(
    prepared: Mapping[str, Any],
    confidence: float,
    level_coverage: tuple[float, float],
    errors: Mapping[str, Mapping[str, float]],
) -> dict[str, Any]:
    """What the result records about the family: its level, its coverage, its worst error."""
    worst = max((max(e.values()) for e in errors.values()), default=None)
    return {
        "interval_family": "simultaneous",
        "family_panels": sorted(prepared),
        "simultaneous_level": level_coverage[0],
        "bonferroni_level": (1.0 - confidence) / max(len(prepared), 1),
        "joint_coverage": level_coverage[1],
        "monte_carlo_error": worst,
    }


def declared_tau_indices(
    p: ComparePairedPanelsPayload,
    sides: tuple[Side, Side],
    load: Callable[[str], DeclaredThreshold],
) -> dict[str, tuple[int, int]] | None:
    """Each requested panel's ``(arm, baseline)`` declared threshold, as a column of the grid.

    Only the panels both sides carry are resolved: an absent panel is reported
    absent before any threshold is read. A declared threshold off the grid is
    refused by the grid's own lookup, which never snaps to a neighbour.
    """
    if p.declared_threshold_id is None or p.baseline_declared_threshold_id is None:
        return None
    arm, baseline = load(p.declared_threshold_id), load(p.baseline_declared_threshold_id)
    meta = sides[0].meta()
    if meta is None:  # pragma: no cover - a side with no grid is refused earlier
        return {}
    out: dict[str, tuple[int, int]] = {}
    for key in p.panels:
        setting, aspect = key.split(":")
        present = all(
            setting in side.grids and any(_cafa(ns) == aspect for ns in side.grids[setting].panels)
            for side in sides
        )
        if present:
            out[key] = (meta.tau_index(arm.tau(key)), meta.tau_index(baseline.tau(key)))
    return out


def _cafa(namespace: str) -> str:
    from protea.core.operations._paired_panels_artifact import NAMESPACE_TO_CAFA

    return NAMESPACE_TO_CAFA.get(namespace, namespace)


def simultaneous_block(family: Mapping[str, Any] | None) -> dict[str, Any]:
    """The family's record for the result, or nothing for a per-panel comparison."""
    return {} if family is None else dict(family)


__all__ = [
    "balanced_level",
    "bound_error",
    "declared_tau_indices",
    "joint_coverage",
    "order_statistic",
    "shared_draws",
    "simultaneous_block",
    "simultaneous_resamples",
]
