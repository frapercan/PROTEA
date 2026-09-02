"""The estimator, the resampler, the interval and the minimum detectable effect.

Everything in this module is pure numpy over arrays a caller has already
loaded. It knows nothing about parquet, the store or the database, because the
arithmetic is the part that has to be checkable on a ten-protein panel written
by hand.

Three properties are load-bearing and each one is here rather than at the call
site so it cannot be forgotten by a second caller.

**The estimator is the panel's ratio of sums.** ``2T / (P + G)`` is the exact
closed form of cafaeval's ``f_micro_w``, not an approximation of it: the kernel
stores ``tp``, ``fp = pred - tp`` and ``fn = gt - tp``, so
``pr = T/P``, ``rc = T/G`` and ``2 pr rc / (pr + rc) = 2T / (P + G)``. The mean
of per-protein F is a different statistic under the same word, and a protein
holding two hundred terms contributes equally to that mean and very unequally
to this ratio.

**The operating point is a declared property of the comparison, and the
default re-selects it inside every resample.** When the reported quantity is a
maximum over an estimated surface, a resample that replays only part of the
estimation procedure estimates a different functional. Holding the threshold at
the full-sample optimum understates the variance, and it does not understate it
equally for two systems whose score surfaces differ in flatness, so it is not
conservative and cannot be defended as conservative. Under
:data:`RESELECTED_PER_RESAMPLE`, :func:`paired_bootstrap` therefore recomputes
the whole threshold curve per resample and takes that resample's own argmax,
per arm, independently.

That is Fmax's selection semantics, and it is not the only defensible reading.
A caller reporting the estimator at a threshold fixed in advance, precisely so
that no max-over-tau selection is taken at all, is estimating a different
quantity and needs the resample to replay THAT procedure: the same column,
every time, in both arms. Under :data:`FIXED` the threshold is not estimated,
so re-selecting it inside the resample would inflate the variance of a
statistic that has none of that variance to begin with. Which of the two is in
force is a choice the caller states and this module never infers, because both
are correct procedures for different published quantities and the wrong one is
not detectable from the arrays.

**The sampling unit is the protein.** Rows and terms inside one protein are not
independent, so resampling them would produce an interval narrower than the
data supports.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from scipy.special import ndtr, ndtri

#: Resamples per matrix product. The count matrix is ``(chunk, n)`` float64,
#: so 256 x 20,000 is 41 MB per chunk. That bounds THIS module's own working
#: set and nothing else: the caller holds every panel's ``(n, n_tau)`` grids for
#: both arms at once, which on a large comparison is far larger than any chunk
#: here. The multinomial draw below is what keeps the chunk to that one matrix;
#: drawing indices first would allocate an ``(chunk, n)`` int64 beside it and
#: double the figure.
_CHUNK = 256

#: Interval method vocabulary. A result has no field in which the method can
#: be omitted, so these three are the whole space.
BCA = "bca"
PERCENTILE = "percentile"
DEGENERATE = "degenerate"

#: Operating-point vocabulary: which threshold the estimator is read at, and
#: whether that threshold is re-estimated inside the resample. Two values and
#: no third, for the same reason the weighting vocabulary has no ``auto``: an
#: operating point nobody declared is one the reader of a stored comparison
#: cannot recover, and the two semantics give different intervals on the same
#: bytes. Everywhere below, ``tau_index=None`` carries
#: :data:`RESELECTED_PER_RESAMPLE` and an integer carries :data:`FIXED`, so the
#: choice travels with the only parameter that can act on it and no call site
#: can hold a word that disagrees with what it computes.
RESELECTED_PER_RESAMPLE = "reselected_per_resample"
FIXED = "fixed"


@dataclass(frozen=True)
class PanelArrays:
    """One system's components for one panel, aligned on the protein axis.

    ``tp`` and ``pred`` are ``(n, n_tau)``; ``n_gt`` is ``(n,)`` because the
    ground-truth mass is a property of the reference and the exclusion set, not
    of a threshold. ``accessions`` travels with them so a population is
    asserted rather than inferred from a count reported elsewhere.
    """

    accessions: tuple[str, ...]
    tp: np.ndarray
    pred: np.ndarray
    n_gt: np.ndarray

    @property
    def n(self) -> int:
        return len(self.accessions)

    @property
    def n_tau(self) -> int:
        return int(self.tp.shape[1])

    def take(self, order: np.ndarray) -> PanelArrays:
        """The same panel restricted (and reordered) to ``order``."""
        accs = tuple(self.accessions[int(i)] for i in order)
        return PanelArrays(accs, self.tp[order], self.pred[order], self.n_gt[order])


def micro_curve(
    tp_total: np.ndarray, pred_total: np.ndarray, gt_total: np.ndarray | float
) -> np.ndarray:
    """``2T / (P + G)`` with cafaeval's zero-denominator convention.

    ``gt_total`` is a scalar in the normal case and deliberately so: the panel's
    ground-truth mass is a property of the panel, not of the threshold, so it
    broadcasts against the per-threshold totals rather than being repeated.

    ``compute_f`` divides ``where=d != 0`` and leaves zero elsewhere, so a
    panel with no predicted mass and no ground-truth mass scores 0.0 rather
    than raising or producing a nan that would later be maxed away silently.
    """
    denom = np.asarray(pred_total, dtype=np.float64) + np.asarray(gt_total, dtype=np.float64)
    numer = 2.0 * np.asarray(tp_total, dtype=np.float64)
    out = np.zeros(np.broadcast(numer, denom).shape, dtype=np.float64)
    np.divide(numer, denom, out=out, where=denom > 0)
    return out


def panel_curve(arrays: PanelArrays) -> np.ndarray:
    """The panel metric at every threshold, pooled over the whole population."""
    return micro_curve(arrays.tp.sum(axis=0), arrays.pred.sum(axis=0), arrays.n_gt.sum())


@dataclass(frozen=True)
class OperatingPoint:
    """The selected threshold and what the panel scores there."""

    value: float
    tau_index: int
    n_tau_at_max: int

    @property
    def tied(self) -> bool:
        return self.n_tau_at_max > 1


def select_operating_point(curve: np.ndarray) -> OperatingPoint:
    """Smallest tau among the maxima, by exact float equality.

    ``np.argmax`` returns the first occurrence and the grid is ascending, which
    is the rule ``pandas.idxmax`` applies to build cafaeval's ``dfs_best``. No
    tolerance: a tolerance would make the reported tau depend on evaluation
    order. The tie changes nothing about the value, the interval or the MDE, so
    it is reported and not warned about.
    """
    best = float(curve.max())
    return OperatingPoint(best, int(np.argmax(curve)), int(np.count_nonzero(curve == best)))


def read_operating_point(curve: np.ndarray, tau_index: int) -> OperatingPoint:
    """What the panel scores at a threshold the caller declared. No maximum.

    ``n_tau_at_max`` is 1 and :attr:`OperatingPoint.tied` is therefore False,
    which is a statement of fact rather than a convenience: nothing was
    selected here, so no tie was broken. Reporting a tie count from a curve
    that was never maximised would put a diagnostic about selection beside a
    number produced without any, and a reader would take it as evidence that
    the argmax happened to be flat.
    """
    return OperatingPoint(float(curve[tau_index]), int(tau_index), 1)


def operating_point(curve: np.ndarray, tau_index: int | None) -> OperatingPoint:
    """The one entry point, so the semantics cannot diverge between call sites.

    ``None`` re-selects, an integer reads the declared column. Written once
    here because the full sample, the jackknife and the resample all have to
    answer the same question the same way; a caller that got this right in two
    of the three places would produce an interval around a point estimate the
    interval is not about.
    """
    if tau_index is None:
        return select_operating_point(curve)
    return read_operating_point(curve, tau_index)


@dataclass(frozen=True)
class Draws:
    """What one paired bootstrap produced.

    ``deltas`` is the only thing the interval needs; the per-arm values and
    argmaxes are kept because ``tau_switched_fraction`` is the direct
    measurement of the effect re-selection exists to capture, and a panel where
    it is near zero is a panel where the shortcut would have been harmless.
    Reporting it is how that claim stops being an assumption.
    """

    deltas: np.ndarray
    a_values: np.ndarray
    b_values: np.ndarray
    a_tau_index: np.ndarray
    b_tau_index: np.ndarray


def _counts_matrix(rng: np.random.Generator, n: int, size: int) -> np.ndarray:
    """``(size, n)`` multiplicities from drawing ``n`` proteins with replacement.

    One draw per resample, and the SAME draw is applied to both arms below.
    That is what "paired" means here: the resample carries the correlation
    between the two systems' errors on the same proteins, which is large
    because the arms share the ground truth and the ontology.
    """
    return rng.multinomial(n, np.full(n, 1.0 / n), size=size).astype(np.float64)


def _read_curves(curves: np.ndarray, tau_index: int | None) -> tuple[np.ndarray, np.ndarray]:
    """Reduce a stack of threshold curves to one value and one index per row.

    The two branches are the two operating points, and this is the only place
    either is applied to a stack, so ``paired_bootstrap`` and
    ``jackknife_deltas`` cannot drift apart on it. The index column is returned
    in both cases, filled with the declared index under :data:`FIXED`, so the
    switched-fraction diagnostic downstream keeps its shape and reports the
    truth about a fixed run: nothing switched, because nothing was selected.
    """
    if tau_index is None:
        return curves.max(axis=1), curves.argmax(axis=1)
    return curves[:, tau_index], np.full(curves.shape[0], tau_index, dtype=np.int64)


def _resample_arm(
    counts: np.ndarray, arm: PanelArrays, tau_index: int | None
) -> tuple[np.ndarray, np.ndarray]:
    """Per-resample metric and threshold index for one arm, one BLAS call per component."""
    curves = micro_curve(counts @ arm.tp, counts @ arm.pred, (counts @ arm.n_gt)[:, None])
    return _read_curves(curves, tau_index)


def paired_bootstrap(
    a: PanelArrays,
    b: PanelArrays,
    *,
    n_resamples: int,
    seed: int | np.random.SeedSequence,
    tau_index: int | None = None,
) -> Draws:
    """Protein-level paired bootstrap at the caller's operating point.

    ``tau_index`` is ``None`` by default, which re-selects the threshold inside
    every resample: that is what the module docstring argues for and what every
    existing caller gets without saying anything. An integer holds the
    threshold at that column in both arms and in every resample, which is the
    procedure that matches an estimator reported at a threshold fixed in
    advance.

    The two arms must already be aligned on the protein axis; the caller owns
    the population rule. One count vector is built per resample and used twice,
    so the pairing is true by construction rather than by convention.
    """
    if a.n != b.n:
        raise ValueError(f"arms are not aligned: {a.n} proteins against {b.n}")
    rng = np.random.default_rng(seed)
    blocks: list[tuple[np.ndarray, ...]] = []
    done = 0
    while done < n_resamples:
        size = min(_CHUNK, n_resamples - done)
        counts = _counts_matrix(rng, a.n, size)
        va, ia = _resample_arm(counts, a, tau_index)
        vb, ib = _resample_arm(counts, b, tau_index)
        blocks.append((va - vb, va, vb, ia, ib))
        done += size
    stacked = [np.concatenate([block[i] for block in blocks]) for i in range(5)]
    return Draws(stacked[0], stacked[1], stacked[2], stacked[3], stacked[4])


def jackknife_deltas(
    a: PanelArrays, b: PanelArrays, *, tau_index: int | None = None
) -> np.ndarray:
    """Leave-one-protein-out deltas, at the same operating point as the resample.

    Exact and cheap because the estimator is a ratio of pooled sums: the
    leave-one-out totals are the totals minus that protein's row, so all ``n``
    replicates including their own argmax cost one ``(n, n_tau)`` subtraction
    per arm. Acceleration is therefore never skipped for cost.

    ``tau_index`` has to be the resample's, not a default: the acceleration is
    the skewness of the influence of one protein on the statistic being
    reported, and a jackknife that maximises over tau while the resample does
    not is measuring the influence on a different statistic. It would still
    produce a finite number, and the BCa endpoints would still move, so the
    only symptom would be an interval quietly corrected in the wrong direction.
    """
    out = []
    for arm in (a, b):
        totals_tp = arm.tp.sum(axis=0)
        totals_pred = arm.pred.sum(axis=0)
        total_gt = arm.n_gt.sum()
        curves = micro_curve(
            totals_tp - arm.tp, totals_pred - arm.pred, (total_gt - arm.n_gt)[:, None]
        )
        out.append(_read_curves(curves, tau_index)[0])
    return out[0] - out[1]


@dataclass(frozen=True)
class Interval:
    """An interval that states how it was built.

    ``method`` is never absent and never inferred from whether ``acceleration``
    happens to be null: an interval whose method is not stated is not
    reportable.
    """

    low: float
    high: float
    method: str
    fallback_reason: str | None
    z0: float | None
    acceleration: float | None

    def excludes_zero(self) -> bool:
        """Never true for a degenerate resample distribution.

        A zero-width interval around a nonzero point estimate satisfies the
        arithmetic of excluding zero and means nothing: every resample returned
        the same number, so there is no evidence about anything. The verdict
        already refuses to call it a win, and a downstream table that reads this
        boolean rather than re-deriving the verdict would publish one.
        """
        if self.method == DEGENERATE:
            return False
        return self.low > 0.0 or self.high < 0.0


def _bias_correction(deltas: np.ndarray, point: float) -> float:
    """``z0`` with the mid-rank convention.

    The plain ``#{D* < D}`` convention sends ``z0`` to minus infinity whenever
    no resample falls below the point estimate, which is common here because a
    maximum over a discrete grid ties heavily on small panels. Counting ties at
    half weight keeps the correction finite in exactly the cases where the
    plain form is least informative.
    """
    below = float(np.count_nonzero(deltas < point))
    equal = float(np.count_nonzero(deltas == point))
    return (below + 0.5 * equal) / float(deltas.size)


def _acceleration(jack: np.ndarray) -> tuple[float | None, str | None]:
    if jack.size < 3:
        return None, "jackknife_too_few_proteins"
    centred = jack.mean() - jack
    second = float(np.sum(centred**2))
    if second == 0.0:
        return None, "jackknife_degenerate"
    accel = float(np.sum(centred**3) / (6.0 * second**1.5))
    if not math.isfinite(accel):
        return None, "acceleration_not_finite"
    return accel, None


def _bca_alphas(z0: float, accel: float, alpha: float) -> tuple[float, float] | None:
    endpoints = []
    for z in (ndtri(alpha / 2.0), ndtri(1.0 - alpha / 2.0)):
        denom = 1.0 - accel * (z0 + z)
        if denom <= 0.0:
            return None
        endpoints.append(float(ndtr(z0 + (z0 + z) / denom)))
    return endpoints[0], endpoints[1]


def _percentile(deltas: np.ndarray, alpha: float, reason: str | None, z0: float | None) -> Interval:
    low = float(np.quantile(deltas, alpha / 2.0))
    high = float(np.quantile(deltas, 1.0 - alpha / 2.0))
    return Interval(low, high, PERCENTILE, reason, z0, None)


def build_interval(
    deltas: np.ndarray,
    point: float,
    jack: np.ndarray,
    *,
    alpha: float,
    force_percentile: bool = False,
) -> Interval:
    """BCa where the acceleration is computable, percentile with a named reason.

    Every fallback is tested explicitly and recorded by name, in the order the
    estimator specification fixes, because "we used the percentile interval"
    and "the acceleration blew through its pole on this panel" are different
    facts and only one of them is a reason to look at the panel again.
    """
    if np.unique(deltas).size < 2:
        return Interval(point, point, DEGENERATE, "bootstrap_distribution_degenerate", None, None)
    if force_percentile:
        # A deliberate choice is not a fallback. ``fallback_reason`` stays None
        # so "the caller asked for percentile" and "the acceleration blew
        # through its pole" stop arriving at the reader as one fact; what the
        # caller asked for is reported once, in the result, as
        # ``interval_method_requested``.
        return _percentile(deltas, alpha, None, None)

    accel, reason = _acceleration(jack)
    prop_low = _bias_correction(deltas, point)
    if reason is not None:
        return _percentile(deltas, alpha, reason, None)
    if prop_low <= 0.0 or prop_low >= 1.0:
        return _percentile(deltas, alpha, "bias_correction_infinite", None)
    z0 = float(ndtri(prop_low))
    alphas = _bca_alphas(z0, float(accel), alpha)
    if alphas is None:
        return _percentile(deltas, alpha, "bca_denominator_nonpositive", z0)
    resolution = 1.0 / float(deltas.size)
    if alphas[0] <= resolution or alphas[1] >= 1.0 - resolution:
        return _percentile(deltas, alpha, "bca_endpoint_beyond_resamples", z0)
    return Interval(
        float(np.quantile(deltas, alphas[0])),
        float(np.quantile(deltas, alphas[1])),
        BCA,
        None,
        z0,
        float(accel),
    )


@dataclass(frozen=True)
class DetectableEffect:
    """The smallest effect the comparison that actually ran could have found.

    Both directions are carried as well as the headline maximum. An
    asymmetric bootstrap distribution genuinely has different power in the two
    directions, and collapsing that to one number loses information about which
    way the panel is blind.
    """

    mde: float | None
    positive: float | None
    negative: float | None
    basis: str
    reason: str | None


def minimum_detectable_effect(
    deltas: np.ndarray, point: float, interval: Interval, *, power: float
) -> DetectableEffect:
    """Read the MDE off the bootstrap distribution, not off a normal approximation.

    The panel statistic is a maximum over 99 thresholds of a ratio of sums. The
    maximum is upward biased and right skewed, the ratio is not location-scale,
    and on a small panel the resample distribution is visibly discrete and can
    be bimodal when two thresholds trade the optimum. ``(z_{1-alpha/2} +
    z_power) * sd`` is a statement about a Gaussian that was never fitted, and
    it would be most wrong exactly where the null is most likely to be misread
    as sameness. Under symmetry the empirical form below reduces to that same
    ``2.80 * sigma``, so nothing is given up by not assuming it.

    The one assumption made is the standard bootstrap shift: the error
    distribution of the estimate is taken to be invariant to the true effect
    over the small range being probed. What comes out is achieved precision
    conditional on the observed data, not a design-stage power calculation.
    ``basis`` stays ``recentred_percentile`` even when the reported interval is
    BCa, because BCa corrects the interval's location while this is a statement
    about its width.

    A degenerate resample distribution returns ``None`` and a reason. The
    arithmetic would return zero, and a zero MDE beside a zero-width interval
    reads as an infinitely powerful comparison, which is the most dangerous
    output this operation could produce.
    """
    if interval.method == DEGENERATE or np.unique(deltas).size < 2:
        return DetectableEffect(
            None, None, None, "recentred_percentile", "bootstrap_distribution_degenerate"
        )
    errors = deltas - point
    beta = 1.0 - power
    half_low = point - interval.low
    half_high = interval.high - point
    positive = float(half_low - np.quantile(errors, beta))
    negative = float(half_high + np.quantile(errors, 1.0 - beta))
    return DetectableEffect(
        max(positive, negative), positive, negative, "recentred_percentile", None
    )


def switched_fraction(tau_index: np.ndarray, full_sample_index: int) -> float:
    """Share of resamples whose optimum threshold is not the full-sample one."""
    if tau_index.size == 0:
        return 0.0
    return float(np.count_nonzero(tau_index != full_sample_index) / tau_index.size)


def skewness(values: np.ndarray) -> float:
    centred = values - values.mean()
    variance = float(np.mean(centred**2))
    if variance <= 0.0:
        return 0.0
    return float(np.mean(centred**3) / variance**1.5)
