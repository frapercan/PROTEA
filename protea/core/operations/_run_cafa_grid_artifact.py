"""The whole-threshold-curve per-protein artefact: the producer for a contract.

``compare_paired_panels`` re-selects the operating point inside every resample,
because the threshold is itself estimated, so it needs the whole curve per
protein. :mod:`protea.core.operations._run_cafa_per_protein` writes the file
that exists today, sliced at one tau, and that file cannot carry an interval.
This module writes the one that can, beside it and never instead of it.

Split out of its sibling rather than added to it: the two produce two artefacts
with two schemas and two consumers, and one module doing both had grown past
the point where either could be read on its own. Everything public here is
re-exported by ``_run_cafa_per_protein``, which is the path the consumer's own
refusal message names.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# The threshold-grid artefact
# ---------------------------------------------------------------------------
#
# ``rows_from_sink`` above collapses the sink to one column, and the column it
# picks is the tau that maximises the UNWEIGHTED protein-mean Fmax while the
# masses it writes are weighted. That file cannot carry an interval:
# ``compare_paired_panels`` re-selects the operating point inside every
# resample, because the threshold is itself estimated, and a table sliced at one
# tau re-selects nothing.
#
# The whole grid is already in memory. ``tp_at_tau`` and ``pred_at_tau`` are
# ``[protein x threshold]`` reverse-cumulative sums along the threshold axis;
# only the selection here discarded them. So this is a second flattener beside
# the first, not a second evaluation, and the legacy file keeps its exact
# content because ``stratify_evaluation`` reads it.
#
# **Size, measured rather than assumed.** At cafaeval's default
# ``th_step=0.01`` the grid is 99 thresholds. Both variants means four mass
# columns, so a row holds 396 float32 values, 1,584 bytes before compression,
# and the scalars and the accession add about 30 more.
#
# Measured, not derived, and reproducible rather than asserted:
# ``scripts/measure_per_protein_grid_size.py`` is the measurement, so the figure
# below can be re-run when pyarrow moves instead of quietly going stale. At
# 20,000 rows of synthetic reverse-cumulative curves (15 to 120 predicted terms
# each, scores Beta(1.5, 3), information-accretion weights Gamma(3, 2), seed 7,
# pyarrow's default snappy) both variants come to 11.06 MB, 553 bytes per row,
# and weighted-only plus the mandatory ``n_gt`` to 9.15 MB, 458 bytes per row.
# The 1.2x ratio between them is not the 2x the column count suggests, because
# the unweighted masses are small integers and compress far better than the
# weighted floats do. The weighted-only figure is the narrower shape's cost, not
# a file this producer emits from a sink: ``grid_rows_from_sink`` refuses a sink
# with no unweighted records, so that width is reachable only by constructing a
# ``GridArtifact`` directly.
#
# One file per SETTING, each carrying every namespace that setting scored. NK,
# LK and PK are disjoint knowledge categories with separate ground-truth files,
# not three views of one population, so a 20,000-protein evaluation scoring all
# three aspects is about 60,000 rows across the three settings TOGETHER, not per
# setting: roughly 33 MB for the whole evaluation, against about 2 MB for its
# three legacy files. An earlier version of this paragraph read the 60,000 as
# per setting and tripled the figure.
#
# What that figure does NOT bound, stated because a size claim that hides its
# exclusions is worse than none:
#
# - It is not a memory bound for the reader, and the gap is larger than it
#   looks. ``compare_paired_panels`` keeps a ``SettingGrid`` per setting on
#   each of two arms, so SIX files are resident at once, not two, and each is
#   held as float64 against the 4 bytes stored. Measured on a 60,000-row file,
#   the retained arrays are about 3.3x the file and the transient pyarrow
#   buffers push the process high-water mark higher still during the read.
#   Budget several times the on-disk total, resident, before the resampler
#   allocates anything.
# - It does not bound compression, which depends on the real score
#   distribution. These curves are constant between adjacent distinct scores,
#   and a prediction whose scores concentrate on few distinct values compresses
#   better than this synthetic one while one that spreads them compresses
#   worse. Treat 560 bytes per row as an estimate with a real spread, not a
#   ceiling.
# - It says nothing about the store's own overhead or about how many
#   evaluations are kept.
#
# **Before this lands.** This project runs a new guard over everything before
# landing it, and a large hit count means the rule is wrong. Two numbers are
# needed and they are not the same number.
#
# The first is the recompute cost: how many stored evaluations hold only the
# legacy file. ``audit_per_protein_artifacts`` counts it, but its answer is
# knowable before it runs, because nothing has ever written the grid file. It is
# a migration estimate and reading it as a guard hit-rate would condemn a schema
# on a quantity that is large by construction.
#
# The second IS the calibration: how many namespaces this producer refuses,
# which is not knowable in advance. Most of it is readable today from artefacts
# already on the store, because ``rows_from_sink`` never filtered on ground
# truth: run the same operation with ``probe_legacy_rows`` and it counts the
# stored rows with ``n_gt_w <= 0`` and the predicted mass they carry. That is a
# lower bound, being the weighted variant at one threshold; the exact figure
# comes from a re-run, where every refusal is a
# ``run_cafa_evaluation.per_protein_grid_namespace_dropped`` event at error
# level carrying one of the ``DROP_*`` codes below. Both numbers go in the pull
# request body. Neither is invented here.


#: What the producer writes, beside ``per_protein.parquet`` and never instead
#: of it. Hard-coded rather than imported from the consumer: the two sides
#: agree through the FILE, and sharing a constant would make one module's typo
#: the other module's behaviour, which is exactly the coupling the round-trip
#: test exists to avoid.
GRID_FILENAME = "per_protein_grid.parquet"
GRID_META_PREFIX = "protea.per_protein_grid."
GRID_SCHEMA_VERSION = "1"

#: Canonical footer order. The consumer compares ``variants`` as a raw string,
#: so weighted-then-unweighted is not a preference, it is the wire format.
GRID_VARIANTS: tuple[str, ...] = ("weighted", "unweighted")

#: Column names per variant. The weighted branch scores ``toi_ia`` and the
#: unweighted branch scores ``toi``, which are DIFFERENT TERM SETS with
#: different row sets, so neither is ever derived from the other and an
#: unweighted number is never written under a weighted name.
GRID_VARIANT_COLUMNS: dict[str, tuple[str, str, str]] = {
    "weighted": ("tp_w", "pred_w", "n_gt_w"),
    "unweighted": ("tp", "pred", "n_gt"),
}

#: Keys the consumer gates on presence FIRST and equality second. An unstamped
#: key compares equal to another unstamped key, so a forgetful producer would
#: pass the gate a careful one fails. Refusing to write beats stamping a
#: plausible substitute.
GRID_SEMANTIC_KEYS: tuple[str, ...] = (
    "tau_grid",
    "normalization",
    "prop",
    "no_orphans",
    "max_terms",
    "information_accretion_set_id",
    "ontology_snapshot_id",
    "evaluation_set_id",
)


#: Machine-readable reasons a namespace is left out of the file. Prose alone
#: would make the landing calibration a grep over free text, and the number of
#: namespaces this producer refuses on a real corpus is exactly the number the
#: pull request has to state. Every drop carries one of these.
DROP_UNMAPPABLE_ROWS = "unmappable_row_numbering"
#: Retired with cafaeval e937e0e, which restricted the PK pooled sums to the
#: eligible rows. Kept exported because it names a real refusal that appears in
#: 54 job events on this deployment, and a reader chasing one of those needs
#: the code to still resolve to something.
DROP_POOLED_SUMS_DIFFER = "pooled_sums_do_not_reproduce"
DROP_VARIANT_POPULATIONS_DIFFER = "variant_populations_differ"
DROP_ELIGIBILITY_DISAGREES = "eligibility_mask_disagrees"
DROP_NO_UNWEIGHTED_RECORD = "no_unweighted_record"
DROP_NO_WEIGHTED_RECORD = "no_weighted_record"
DROP_COLUMN_SHAPE = "column_shape"


class GridProducerError(RuntimeError):
    """The grid artefact cannot be written from what the sink handed over."""

    #: Overridden per raise site. Carried on the exception rather than passed
    #: beside it so a reason cannot get separated from the message it explains.
    code: str = "grid_producer_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class UnstampableFrameError(GridProducerError):
    """A value the consumer gates on is not reachable at the write point."""

    code = "unstampable_frame"


class SinkAlignmentError(GridProducerError):
    """The sink's row numbering cannot be trusted to name the proteins."""

    code = DROP_UNMAPPABLE_ROWS


def tau_grid_for(th_step: float) -> list[float]:
    """cafaeval's own grid, floating-point accumulation included.

    ``np.arange(th_step, 1, th_step)`` is normative, not a description of one:
    it is the vector cafaeval evaluates on, and element 5 at ``th_step=0.01``
    really is ``0.060000000000000005``.

    Rounding those values to two decimals would NOT be caught, which is worth
    saying because an earlier version of this docstring claimed it would be.
    The deviation ``np.round`` introduces is about 1e-16, four orders inside
    the ``atol=1e-9`` of the consumer's cross-arm comparison, so a rounded file
    and an unrounded one compare equal and both load. What is caught, and the
    reason to emit float64 rather than anything narrower, is a grid built or
    stored in float32: near tau 0.99 that is off by about 3e-8, which sits
    INSIDE the file's own ``np.allclose`` cross-check against ``th_step`` (that
    one runs at numpy's default relative tolerance) and OUTSIDE the cross-arm
    comparison at ``rtol=0.0``. The file then loads on its own and is refused
    as an incomparable frame the moment it meets a float64 sibling, which is
    the worst place to find out. Emit the raw float64 values.
    """
    return [float(x) for x in np.arange(th_step, 1, th_step)]


@dataclass(frozen=True)
class GridArtifact:
    """One setting's rows, ready to write, with what the footer must declare."""

    rows: list[dict[str, Any]]
    variants: tuple[str, ...]
    n_tau: int
    #: One ``{"namespace", "code", "reason"}`` per namespace left out. Returned
    #: rather than logged inside, because a namespace silently absent from the
    #: file reaches the consumer as a null panel on a successful job. The code
    #: is there so the landing calibration counts refusals instead of grepping
    #: prose, and the prose is there so the operator knows what to do about one.
    dropped: list[dict[str, str]]


@dataclass(frozen=True)
class GridFrame:
    """Everything the footer states about what the numbers MEAN.

    Three of these are not properties of the cafaeval call and are not
    reachable from the evaluation driver's own inputs: which ontology snapshot,
    which evaluation set, which information-accretion table. They are threaded
    down from the operation that knows them. When one is missing this refuses
    rather than stamping a substitute, because the consumer's gate is on
    presence first: a file that omits a key compares EQUAL to another file that
    omits it, so a plausible substitute is worse than an absence and an absence
    is worse than a refusal.
    """

    setting: str
    th_step: float
    max_terms: int | None
    normalization: str
    prop: str
    no_orphans: bool
    ontology_snapshot_id: str | None
    evaluation_set_id: str | None
    #: The identity of the IA table, NOT necessarily a set id. An evaluation may
    #: pass ``information_accretion_set_id`` (a UUID), or a bare ``ia_file`` or a
    #: snapshot ``ia_url`` (a content hash, so two different tables do not
    #: compare equal), or no IA at all, in which case there is no weighted
    #: variant to compare and the literal ``"null"`` is honest rather than lossy.
    information_accretion_frame: str | None
    producer_git_sha: str | None = None

    def stamp(self, variants: tuple[str, ...]) -> dict[str, str]:
        """The footer, or a refusal naming exactly what could not be stamped."""
        values = {
            "version": GRID_SCHEMA_VERSION,
            "tau_grid": json.dumps(tau_grid_for(self.th_step)),
            "th_step": repr(float(self.th_step)),
            "variants": json.dumps(list(variants)),
            "normalization": self.normalization,
            "prop": self.prop,
            "no_orphans": "true" if self.no_orphans else "false",
            "max_terms": "null" if self.max_terms is None else str(self.max_terms),
            "information_accretion_set_id": self.information_accretion_frame or "",
            "ontology_snapshot_id": self.ontology_snapshot_id or "",
            "evaluation_set_id": self.evaluation_set_id or "",
            "producer": "run_cafa_evaluation",
            "producer_git_sha": self.producer_git_sha or "",
            "setting": self.setting,
        }
        absent = [key for key in GRID_SEMANTIC_KEYS if not values.get(key)]
        if absent:
            raise UnstampableFrameError(
                f"setting {self.setting}: {GRID_FILENAME} would not stamp {absent}, so the "
                "consumer's comparability gate could not fire on it. Those keys say which "
                "ontology, which reference and which information-accretion table a number "
                "was computed against, and an unstamped key compares equal to another "
                "unstamped key: two systems scored under different frames would be published "
                "as a method difference. Thread the value down from run_cafa_evaluation "
                "rather than defaulting it here. Nothing was written."
            )
        return values


def _index_records(sink: Any) -> dict[tuple[str, str], dict[str, Any]]:
    """Key the sink's flat record list on ``(namespace, variant)``.

    ``cafa_eval`` passes ONE sink to every prediction file it finds under the
    prediction directory, so the list is not self-describing about which file a
    record came from. PROTEA writes exactly one ``predictions.tsv`` per
    directory, which makes the key unique; a duplicate means that stopped being
    true and the arrays of two runs would be interleaved, so it refuses.
    """
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in getattr(sink, "records", []):
        key = (str(rec.get("ns")), str(rec.get("variant")))
        if key in out:
            raise GridProducerError(
                f"the sink holds two {key[1]} records for {key[0]}. One sink is shared across "
                "every prediction file in the directory, so two records mean two predictions "
                "were scored into one artefact and their rows would be interleaved under one "
                "set of accessions."
            )
        out[key] = rec
    return out


def _grid_accessions(record: dict[str, Any]) -> list[str]:
    """Name each array row, refusing a numbering that cannot be verified.

    ``ids`` numbers the FULL ground truth; ``row_index`` numbers the rows the
    kernel was actually handed. On the PK path cafaeval passes
    ``np.flatnonzero(proteins_has_gt)``, which is the real mapping. On the NK/LK
    path it passes ``np.arange(p.shape[0])`` against a ``p`` that was already
    filtered to the ground-truth-bearing proteins, so when any protein was
    dropped the mapping is positional in the filtered array while ``ids``
    numbers the unfiltered one, and every row after the first gap wears the
    wrong accession. It is not detectable after the fact and it is not
    recoverable from the record: ``proteins_has_gt`` is never handed over.

    So: a full-width record is provably right whichever branch produced it, and
    a PK record (the one carrying ``protein_rows``) is right because
    ``flatnonzero`` is the mapping. Anything else is refused rather than
    written under a guess. The fix is one line in cafaeval-protea, matching the
    PK branch, after which this gate stops firing on its own.
    """
    ids, row_index = record.get("ids"), record.get("row_index")
    n_rows = int(np.asarray(record["tp_at_tau"]).shape[0])
    if not ids or row_index is None or len(row_index) != n_rows:
        raise SinkAlignmentError(
            "the record carries no usable (ids, row_index) pair, so its rows cannot be named"
        )
    if record.get("protein_rows") is None and n_rows != len(ids):
        raise SinkAlignmentError(
            f"the record covers {n_rows} of {len(ids)} ground-truth proteins and numbers them "
            "0..n-1, which is what the NK/LK kernel passes after it has already filtered the "
            "rows. That numbering is positional in the filtered array while ids numbers the "
            "unfiltered ground truth, so every row after the first dropped protein would wear "
            "a neighbour's accession. Refusing rather than writing a mislabelled table; the "
            "fix is np.flatnonzero(proteins_has_gt) in cafaeval-protea, as the PK branch "
            "already does."
        )
    by_index = {int(v): k for k, v in ids.items()}
    out = [by_index.get(int(i)) for i in row_index]
    if any(a is None for a in out):
        raise SinkAlignmentError("row_index names a ground-truth row that ids does not carry")
    return [str(a) for a in out]


#: Relative slack on the pooled cross-check below. The two sums run over the
#: same float64 values in a different order, so they agree to within a few ulps
#: when the dropped rows really are zero and differ by a whole protein's mass
#: when they are not. Nothing lives between the two scales, so the tolerance is
#: not a judgement call.


def _eligible(record: dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    """Which rows cafaeval counts in its own population, and their GT mass.

    The rows of the file are exactly the scored population, so the row count IS
    the panel's coverage denominator, which is what
    ``cafaeval._count_proteins_in_toi`` computes: the proteins with at least one
    ground-truth term surviving the terms-of-interest and exclusion masks, i.e.
    ``n_gt > 0``.

    On the PK path the kernel also hands over ``protein_rows``, the mask behind
    its own coverage column. For the unweighted variant that mask and
    ``n_gt > 0`` are the same set by construction. For the weighted variant they
    can part company: ``protein_rows`` asks whether a surviving ground-truth
    term exists, ``n_gt`` sums information accretion over those terms, and a
    protein whose survivors all carry zero accretion is in one set and not the
    other. Either way a disagreement means the population is not the one the
    published cell was normalised by, so it is asserted rather than resolved by
    picking a side.
    """
    n_gt = np.asarray(record["n_gt"], dtype=np.float64)
    keep = n_gt > 0.0
    mask = record.get("protein_rows")
    if mask is not None:
        mask = np.asarray(mask, dtype=bool)
        if mask.shape != keep.shape or not bool(np.array_equal(mask, keep)):
            raise GridProducerError(
                f"the kernel's own eligibility mask keeps {int(np.count_nonzero(mask))} rows "
                f"and n_gt > 0 keeps {int(np.count_nonzero(keep))}. The published cell is "
                "normalised by the second and its coverage column is computed from the "
                "first, so when they differ there is no single row set whose count is the "
                "panel's denominator and the number beside every cell would be wrong.",
                code=DROP_ELIGIBILITY_DISAGREES,
            )
    return np.flatnonzero(keep), n_gt


def _population(
    record: dict[str, Any], namespace: str, variant: str
) -> tuple[list[str], np.ndarray, np.ndarray]:
    """One variant's scored population: row names, kept rows, ground-truth mass.

    A second gate used to stand here, refusing a namespace whose kept rows did
    not sum to the totals the kernel published. It was needed while the PK
    kernel pooled its confusion matrix over every row it was handed while
    normalising by the eligible ones alone, which left no row set that both
    reproduced ``f_micro_w`` and declared the population that was scored.
    cafaeval e937e0e restricts the pooled sums to the eligible rows, so the two
    requirements reconcile and the gate has nothing left to catch.

    What replaces it is already here and is stronger. :func:`_eligible` asserts
    that the kernel's own eligibility mask and this module's ``n_gt > 0`` keep
    the same rows, computed independently on the two sides. Given that
    agreement, summing the kept rows IS the published total, so the pooled
    check was implied by it rather than adding to it. The dependency is pinned
    by commit, so a kernel that changed its aggregation again would arrive as a
    deliberate repin and not as a silent drift.
    """
    accs = _grid_accessions(record)
    keep, n_gt = _eligible(record)
    return accs, keep, n_gt


def _check_populations_agree(namespace: str, unweighted: list[str], weighted: list[str]) -> None:
    """The two variants must have scored the same proteins, or one row set lies.

    Zero-padding the smaller variant keeps its ``f_micro_w`` exact, because zero
    adds nothing to ``T``, ``P`` or ``G``, and reports its coverage over the
    other variant's denominator. The estimator-parity guard downstream compares
    ``f_micro_w`` and so cannot see it, which is why it has to be caught here.
    """
    only_weighted = sorted(set(weighted) - set(unweighted))
    only_unweighted = sorted(set(unweighted) - set(weighted))
    if not (only_weighted or only_unweighted):
        return
    raise GridProducerError(
        f"{namespace}: the weighted branch scores {len(weighted)} proteins and the "
        f"unweighted branch {len(unweighted)} ({len(only_unweighted)} only unweighted, first "
        f"{only_unweighted[:3]}; {len(only_weighted)} only weighted, first "
        f"{only_weighted[:3]}). The file carries one row per (protein, namespace) and the "
        "consumer reads one variant at a time out of it, so the row count is the coverage "
        "denominator for both variants and two populations cannot share one row set. Padding "
        "the smaller variant with zero rows keeps its f_micro_w exact, because zero adds "
        "nothing to T, P or G, and reports its coverage over the other variant's denominator, "
        "which is a plausible number over the wrong population. The two sets part when a "
        "protein's whole ground-truth closure carries zero information accretion, so it has "
        "terms in toi and none in toi_ia. Refusing this namespace; the other aspects are "
        "unaffected.",
        code=DROP_VARIANT_POPULATIONS_DIFFER,
    )


def _namespace_rows(
    namespace: str,
    unweighted: dict[str, Any],
    weighted: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """One namespace's rows, one per protein, carrying both variants.

    The file has one row grain and the consumer reads one variant at a time out
    of it, so the row count is the population denominator for BOTH variants.
    That is only honest when the two variants scored the same proteins, and they
    do not always: the weighted branch scores ``toi_ia`` and the unweighted
    branch scores ``toi``, and a protein whose whole ground-truth closure carries
    zero information accretion has terms in the second and none in the first.

    An earlier version of this function keyed on the unweighted population and
    zero-filled the weighted-ineligible rows. The zeros really do leave the
    weighted ``T``, ``P`` and ``G`` alone, so ``f_micro_w`` reproduced and the
    parity guard stayed green, and the coverage printed beside it was quietly
    computed over a larger denominator than the one cafaeval normalised by. That
    is the defect shape this project keeps being bitten by, a plausible number
    over the wrong population, so the disagreement is now a refusal.

    Refusing the namespace rather than the file: the other two aspects are
    unaffected and there is no reason to lose them. The drop is emitted, because
    an absent namespace reaches the consumer as a null panel on a successful job.
    """
    accs, keep, n_gt = _population(unweighted, namespace, "unweighted")
    kept_accs = [accs[int(i)] for i in keep]
    by_acc: dict[str, tuple[np.ndarray, np.ndarray, float]] = {}
    if weighted is not None:
        w_accs, w_keep, w_gt = _population(weighted, namespace, "weighted")
        w_tp, w_pred = weighted["tp_at_tau"], weighted["pred_at_tau"]
        by_acc = {
            w_accs[int(i)]: (w_tp[int(i)], w_pred[int(i)], float(w_gt[int(i)])) for i in w_keep
        }
        _check_populations_agree(namespace, kept_accs, sorted(by_acc))
    rows: list[dict[str, Any]] = []
    for i in keep:
        acc = accs[int(i)]
        row: dict[str, Any] = {
            "protein_accession": acc,
            "namespace": namespace,
            "tp": unweighted["tp_at_tau"][int(i)],
            "pred": unweighted["pred_at_tau"][int(i)],
            "n_gt": float(n_gt[int(i)]),
        }
        if weighted is not None:
            w_tp_row, w_pred_row, w_gt_row = by_acc[acc]
            row.update({"tp_w": w_tp_row, "pred_w": w_pred_row, "n_gt_w": w_gt_row})
        rows.append(row)
    return rows


def _grid_widths(records: dict[tuple[str, str], dict[str, Any]]) -> int:
    """The one threshold count every record must share."""
    widths = {int(np.asarray(rec["tp_at_tau"]).shape[1]) for rec in records.values()}
    if len(widths) != 1:
        raise GridProducerError(
            f"the sink's records are {sorted(widths)} thresholds wide; one evaluation runs on "
            "one grid, so two widths mean two grids were mixed into one artefact",
            code=DROP_COLUMN_SHAPE,
        )
    return widths.pop()


def grid_rows_from_sink(sink: Any) -> GridArtifact:
    """Flatten a sink into per-(namespace, protein) rows carrying the WHOLE grid.

    This is the producer the consumer names when it refuses the legacy file. It
    reads the same records ``rows_from_sink`` reads and keeps what that one
    throws away: both variants instead of one, every threshold instead of the
    column at a single tau, and no ``tau`` on the row at all. ``parse_results``
    is not on this path, deliberately, because the tau it reports is the
    unweighted optimum and this artefact has no operating point to report.

    A namespace enters the file only when every variant present for it can be
    named and read. One that cannot is dropped and returned in ``dropped``,
    where the caller emits it: the consumer treats an absent namespace as a
    null panel on a successful job, so an unreported drop is a silent nine-null
    result, which is the failure this whole contract exists to prevent.
    """
    records = _index_records(sink)
    if not records:
        return GridArtifact([], (), 0, [])
    n_tau = _grid_widths(records)
    namespaces = sorted({ns for ns, _ in records})
    variants = tuple(v for v in GRID_VARIANTS if any(var == v for _, var in records))
    if "unweighted" not in variants:
        raise GridProducerError(
            "the sink carries no unweighted records, so the file would have no n_gt. That "
            "column is the eligibility marker and the population denominator, and the "
            "consumer requires it whatever variants a file declares."
        )
    rows: list[dict[str, Any]] = []
    dropped: list[dict[str, str]] = []

    def drop(namespace: str, code: str, reason: str) -> None:
        dropped.append({"namespace": namespace, "code": code, "reason": reason})

    for namespace in namespaces:
        base = records.get((namespace, "unweighted"))
        if base is None:
            drop(
                namespace,
                DROP_NO_UNWEIGHTED_RECORD,
                "no unweighted record, so the rows have no n_gt",
            )
            continue
        weighted = records.get((namespace, "weighted"))
        if weighted is None and "weighted" in variants:
            drop(
                namespace,
                DROP_NO_WEIGHTED_RECORD,
                "the file declares the weighted variant and this namespace has no weighted "
                "record",
            )
            continue
        try:
            rows.extend(_namespace_rows(namespace, base, weighted))
        except GridProducerError as exc:
            drop(namespace, exc.code, str(exc))
    return GridArtifact(rows, variants, n_tau, dropped)


def _mass_column(rows: list[dict[str, Any]], name: str, n_tau: int) -> tuple[Any, Any]:
    """One mass column as a fixed-width list of float32, per row.

    Fixed width rather than pyarrow's default variable-length list: the width
    then cannot go ragged at all, and a ragged column whose total happens to
    divide by the row count reshapes without complaint and gives every protein
    after the short row a neighbouring threshold's score.

    float32 for the grid and float64 for the scalars, computed in float64 and
    stored narrow. Rounding to nearest is monotone, so a non-increasing float64
    curve stays non-increasing in float32 and ``tp <= pred`` survives the cast;
    the consumer's tolerances are relative for exactly this reason.
    """
    import pyarrow as pa

    values = [np.asarray(r[name], dtype=np.float64) for r in rows]
    for i, block in enumerate(values):
        if block.shape != (n_tau,):
            raise GridProducerError(
                f"row {i} of column {name!r} holds {block.shape} against a declared grid of "
                f"{n_tau} thresholds"
            )
    flat = np.concatenate(values) if values else np.zeros(0, dtype=np.float64)
    array = pa.FixedSizeListArray.from_arrays(pa.array(flat.astype(np.float32), pa.float32()), n_tau)
    return pa.field(name, pa.list_(pa.float32(), n_tau)), array


def _check_footer(artifact: GridArtifact, metadata: dict[str, str]) -> None:
    """The writer's own gate on the footer it was handed.

    ``GridFrame.stamp`` refuses an unstampable frame, but it is a separate
    object that this function neither calls nor requires, so a second caller
    building a footer by hand, or threading a new field and dropping a key, gets
    a file written and uploaded. The consumer then refuses it with a message
    telling the operator to re-run with a producer that stamps the schema
    version, which is this module, which did write it. A gate beside the writer
    is not a gate, so it lives inside.

    The tau grid is cross-checked against the artefact's own width for the same
    reason the legacy path cross-checks it: the footer and the columns are two
    statements about one grid, and when they disagree the consumer reports it as
    a ragged list column, sending the operator to look for a producer bug in the
    mass columns, which are fine.
    """
    if metadata.get("version") != GRID_SCHEMA_VERSION:
        raise UnstampableFrameError(
            f"the footer declares version {metadata.get('version')!r} and this producer "
            f"writes {GRID_SCHEMA_VERSION!r}. The version is what says the columns were "
            "written under the contract, so a file carrying the wrong one is unreadable by "
            "design and there is no reason to put it on the store."
        )
    absent = [key for key in GRID_SEMANTIC_KEYS if not metadata.get(key)]
    if absent:
        raise UnstampableFrameError(
            f"the footer does not stamp {absent}, so the consumer's comparability gate could "
            "not fire on it. An unstamped key compares equal to another unstamped key, which "
            "means the gate would fire on the careful producer and never on the forgetful "
            "one. Nothing was written."
        )
    try:
        declared = json.loads(metadata["tau_grid"])
    except (ValueError, TypeError) as exc:
        raise UnstampableFrameError(
            f"the footer's tau_grid does not parse as JSON ({exc}), so the file would declare "
            "a grid nothing can read"
        ) from exc
    if len(declared) != artifact.n_tau:
        raise GridProducerError(
            f"the footer declares {len(declared)} thresholds and the rows are "
            f"{artifact.n_tau} wide. Two statements about one grid disagree, and the consumer "
            "reads that as a ragged list column, which sends whoever debugs it into the mass "
            "columns instead of into the th_step this footer was built from.",
            code=DROP_COLUMN_SHAPE,
        )
    try:
        stated = tuple(json.loads(metadata.get("variants", "[]")))
    except (ValueError, TypeError):
        stated = ()
    if stated != artifact.variants:
        raise UnstampableFrameError(
            f"the footer declares variants {list(stated)} and the artefact holds "
            f"{list(artifact.variants)}. The consumer refuses a variant the columns do not "
            "carry, so a footer that overstates them turns a readable file into a refusal "
            "naming this producer."
        )


def _variant_columns(artifact: GridArtifact) -> tuple[list[Any], list[Any], set[str]]:
    """The mass and ground-truth columns for every variant the artefact declares."""
    import pyarrow as pa

    fields: list[Any] = []
    arrays: list[Any] = []
    written: set[str] = set()
    for variant in artifact.variants:
        tp_col, pred_col, gt_col = GRID_VARIANT_COLUMNS[variant]
        for name in (tp_col, pred_col):
            field, array = _mass_column(artifact.rows, name, artifact.n_tau)
            fields.append(field)
            arrays.append(array)
        fields.append(pa.field(gt_col, pa.float64()))
        arrays.append(pa.array([float(r[gt_col]) for r in artifact.rows], pa.float64()))
        written.add(gt_col)
    return fields, arrays, written


def _eligibility_column(artifact: GridArtifact) -> tuple[Any, Any]:
    """``n_gt``, which a weighted-only file still has to carry.

    It is the eligibility marker and the population denominator, not a component
    of the unweighted metric, so a file holding only the weighted mass columns
    still holds it. Writing the weighted mass under this name instead, which is
    what the consumer's own test helper does, would make a
    zero-information-accretion protein look ineligible and get the whole file
    refused: the wrong number under the right name.

    Not reachable from a sink, because ``grid_rows_from_sink`` refuses one with
    no unweighted records. It is reachable from a hand-built ``GridArtifact``
    declaring only the weighted variant, which is a supported entry point.
    """
    import pyarrow as pa

    if any("n_gt" not in row for row in artifact.rows):
        raise GridProducerError(
            "the rows carry no unweighted n_gt, so the file would have no eligibility "
            "marker and no population denominator. The consumer requires that column "
            "whatever variants a file declares."
        )
    return (
        pa.field("n_gt", pa.float64()),
        pa.array([float(r["n_gt"]) for r in artifact.rows], pa.float64()),
    )


def write_grid_parquet(path: Path, artifact: GridArtifact, metadata: dict[str, str]) -> Path:
    """Write one setting's whole-grid file, footer and all.

    Refuses an empty artefact. A zero-row file passes every gate the consumer
    has and then degrades to "artefact absent for panel", so the job succeeds
    and reports a null where it should have reported a refusal.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not artifact.rows:
        # Checked before the footer, because an empty artefact declares no
        # variants and no width, so a footer gate would report the width
        # disagreement and bury the reason there is nothing to write.
        raise GridProducerError(
            "refusing to write an empty grid file: the consumer accepts it and silently "
            "reports a null panel, which is indistinguishable from an evaluation that was "
            "never run"
        )
    _check_footer(artifact, metadata)
    fields = [pa.field("protein_accession", pa.string()), pa.field("namespace", pa.string())]
    arrays: list[Any] = [
        pa.array([r["protein_accession"] for r in artifact.rows], pa.string()),
        pa.array([r["namespace"] for r in artifact.rows], pa.string()),
    ]
    variant_fields, variant_arrays, written = _variant_columns(artifact)
    fields.extend(variant_fields)
    arrays.extend(variant_arrays)
    if "n_gt" not in written:
        field, array = _eligibility_column(artifact)
        fields.append(field)
        arrays.append(array)
    encoded = {
        f"{GRID_META_PREFIX}{key}".encode(): str(value).encode()
        for key, value in metadata.items()
    }
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata=encoded))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path
