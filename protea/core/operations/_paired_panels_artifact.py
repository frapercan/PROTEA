"""The per-protein threshold-grid artefact: its contract, its gates, its refusals.

``compare_paired_panels`` re-selects the operating point inside every resample,
so it needs the whole threshold curve per protein. The artefact PROTEA writes
today, ``<SETTING>/per_protein.parquet``, is sliced at one tau, and that tau is
the unweighted Fmax optimum while its mass columns are weighted. There is no
correct interval to compute from it, so this module refuses and names the
producer that would satisfy it rather than computing something plausible.

This is the consumer side of a contract whose producer does not exist yet. The
column names, dtypes, row grain and file metadata below are the specification
that producer has to satisfy, and the gates in :func:`load_setting_grid` are
what makes the specification enforceable rather than aspirational. Every gate
here catches a producer mistake that would otherwise arrive as a believable
number: a grid written descending while declared ascending, a mass column that
exceeds its own denominator, a second row for a protein that silently doubles
its weight in the pooled sum, a single-threshold slice broadcast across the
columns.

**The contract, stated once.**

- One row per ``(protein_accession, namespace)``, and the rows are exactly the
  proteins cafaeval scores in that namespace: those carrying at least one
  ground-truth term after the term-of-interest and exclusion masks. Not a
  superset (a protein with no ground truth adds predicted mass to ``P`` and
  nothing to ``G``, which moves the published number while passing every other
  gate) and not a subset (the pooled sums would no longer reproduce the
  published cell). Because the row set is exactly the scored population, the
  panel's own coverage at a threshold is recoverable from the file, which is
  the number every published cell sits beside: the denominator is the row count
  and the numerator is the rows predicting anything at that threshold. A footer
  counter would say the same thing in a second place and could disagree with
  the rows, so the row rule is the contract and there is no ``ne`` key.
- ``tp_w`` / ``pred_w`` (and ``tp`` / ``pred`` for the unweighted variant) are
  list columns holding the whole ascending tau grid on the row, one value per
  declared threshold, fixed width. Reverse-cumulative sums, so they only fall.
- ``n_gt_w`` is the weighted ground-truth mass, a scalar per row, and may be
  zero when a protein's whole closure carries zero information accretion.
  ``n_gt`` is the UNWEIGHTED ground-truth term count and is mandatory in every
  file whatever variants it carries: it is the eligibility marker and the
  population denominator, and it is strictly positive by the row rule above.
- Floating dtypes are the producer's choice. The grids are expected to be
  float32 and the scalars float64, so every tolerance in this module is
  RELATIVE: an absolute 1e-5 is smaller than a float32 ulp once a weighted mass
  passes about 168, which would make a correct producer look like a broken one
  on any ordinary BPO closure.
- The tau grid is declared explicitly in the footer AND as ``th_step``. The
  normative construction is cafaeval's own ``np.arange(th_step, 1, th_step)``,
  including its floating-point accumulation; the two facts are required to
  confirm each other.
- Every key in :data:`SEMANTIC_COMPARABILITY_KEYS` must be stamped. A key that
  is absent from both files compares equal to itself, so an unstamped producer
  would pass the comparability gate that a careful one fails; the gate is on
  presence first and equality second.

A new filename rather than a widened old one, deliberately. Changing
``per_protein.parquet`` in place would break ``stratify_evaluation``, which is
the one thing reading it today, and it would make an old file and a new file
indistinguishable by name, which is the detection problem made permanently
unsolvable. The name is not the detection either: the legacy table renamed and
broadcast across the columns is caught by :func:`_check_curve_varies`, on the
content, because a name is a claim and a constant curve is evidence.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from protea.core.operations._paired_panels_bootstrap import PanelArrays

#: What the producer writes, per evaluation result, per CAFA setting.
GRID_FILENAME = "per_protein_grid.parquet"

#: What exists today and cannot support an interval.
LEGACY_FILENAME = "per_protein.parquet"

#: cafaeval's three settings. Not listed from the store: the store protocol has
#: no list, and these are the only settings an evaluation writes.
SETTINGS: tuple[str, ...] = ("NK", "LK", "PK")

#: File-level key/value metadata namespace and the version this consumer reads.
META_PREFIX = "protea.per_protein_grid."
SCHEMA_VERSION = "1"

#: Metadata that fixes what a number MEANS. A disagreement on any of these is a
#: refusal that ``allow_frame_mismatch`` does not waive, and so is an absence:
#: two files that both omit ``information_accretion_set_id`` would otherwise
#: compare equal, so the gate would fire on the producers careful enough to
#: declare their frame and never on the one that forgot.
SEMANTIC_COMPARABILITY_KEYS: tuple[str, ...] = (
    "tau_grid",
    "normalization",
    "prop",
    "no_orphans",
    "max_terms",
    "information_accretion_set_id",
    "ontology_snapshot_id",
    "evaluation_set_id",
)

#: Metadata that labels a file without changing what its numbers mean. A
#: disagreement here is recorded and, with ``allow_frame_mismatch``, permitted.
#: One flag cannot be allowed to waive both a label and the ontology snapshot,
#: which is why the two sets are separate rather than one list with a switch.
WAIVABLE_COMPARABILITY_KEYS: tuple[str, ...] = ("variants",)

#: Everything compared, in report order.
COMPARABILITY_KEYS: tuple[str, ...] = (
    *SEMANTIC_COMPARABILITY_KEYS,
    *WAIVABLE_COMPARABILITY_KEYS,
)

#: cafaeval emits the long namespace; every other table here speaks CAFA codes.
NAMESPACE_TO_CAFA: dict[str, str] = {
    "biological_process": "BPO",
    "molecular_function": "MFO",
    "cellular_component": "CCO",
}

#: The two variants are not the same numbers with weights of one: the weighted
#: branch scores ``toi_ia`` and the unweighted branch scores ``toi``, which are
#: different term sets. That is why an unweighted number is never labelled with
#: a weighted name.
VARIANT_COLUMNS: dict[str, tuple[str, str, str]] = {
    "weighted": ("tp_w", "pred_w", "n_gt_w"),
    "unweighted": ("tp", "pred", "n_gt"),
}

#: The unweighted ground-truth count, required whatever variants a file holds.
#: It is what says a row belongs in the file at all.
ELIGIBILITY_COLUMN = "n_gt"

#: Columns that only the single-threshold artefact carries. Their presence in a
#: file called ``per_protein_grid.parquet`` means a producer widened the rows
#: instead of the columns.
LEGACY_MARKER_COLUMNS = frozenset({"tau", "f_w", "precision_w", "recall_w"})

_KEY_COLUMNS = ("protein_accession", "namespace")

#: Relative and absolute slack for every mass comparison. ``_RTOL`` is eight
#: times float32's epsilon, so a producer computing in float64 and storing the
#: grid as float32 is inside it at any mass; ``_ATOL`` only covers comparisons
#: against zero.
_RTOL = 1e-6
_ATOL = 1e-9


class ThresholdGridUnavailableError(RuntimeError):
    """The per-protein artefact cannot support a re-selected operating point."""


class GridInvariantError(RuntimeError):
    """The grid artefact is present but violates its own contract."""


class PanelComparabilityError(RuntimeError):
    """Two artefacts describe frames that are not about the same thing."""


def exceeds(left: np.ndarray, right: np.ndarray) -> bool:
    """``left > right`` beyond a relative tolerance, elementwise, any."""
    return bool(np.any(left > right + _ATOL + _RTOL * np.abs(right)))


@dataclass(frozen=True)
class GridMeta:
    """The file-level metadata, parsed. Read from the footer, never the rows."""

    setting: str
    tau_grid: np.ndarray
    th_step: float
    variants: tuple[str, ...]
    values: dict[str, str]

    @property
    def n_tau(self) -> int:
        return int(self.tau_grid.size)

    def comparability(self) -> dict[str, str]:
        return {key: self.values.get(key, "") for key in COMPARABILITY_KEYS}


@dataclass(frozen=True)
class SettingGrid:
    """One setting's file, loaded and checked, keyed by cafaeval namespace."""

    setting: str
    meta: GridMeta
    variant: str
    panels: dict[str, PanelArrays]


def _decoded_metadata(path: Path) -> dict[str, str]:
    import pyarrow.parquet as pq

    raw = pq.read_schema(path).metadata or {}
    out: dict[str, str] = {}
    for key, value in raw.items():
        name = key.decode() if isinstance(key, bytes) else str(key)
        if name.startswith(META_PREFIX):
            out[name[len(META_PREFIX) :]] = (
                value.decode() if isinstance(value, bytes) else str(value)
            )
    return out


def _legacy_tau_hint(path: Path) -> str:
    """Name the single threshold the old file is sliced at, if it is readable.

    Best effort and one column wide. The refusal stands whether or not this
    succeeds; naming the tau just makes the message concrete for whoever has to
    decide what to re-run.
    """
    try:
        import pyarrow.parquet as pq

        values = sorted({float(v) for v in pq.read_table(path, columns=["tau"])["tau"].to_pylist()})
    except Exception:
        return "one threshold"
    if not values:
        return "one threshold"
    shown = ", ".join(f"{v:g}" for v in values[:3])
    return f"tau={shown}" + (", ..." if len(values) > 3 else "")


def _legacy_refusal(result_id: str, setting: str, legacy: Path) -> str:
    return (
        f"evaluation result {result_id}, setting {setting}: found "
        f"{LEGACY_FILENAME}, which stores tp_w and pred_w at ONE threshold "
        f"({_legacy_tau_hint(legacy)}), and no {GRID_FILENAME}.\n\n"
        "An interval on f_micro_w has to re-select the operating point inside "
        "every resample, because the threshold is itself estimated: holding it "
        "at the full-sample optimum understates the variance, and it does not "
        "understate it equally for two systems whose score distributions "
        "differ. A table sliced at a single tau cannot re-select anything, so "
        "there is no correct number to compute from it.\n\n"
        "Produce the grid artefact first: run_cafa_evaluation writes "
        f"<SETTING>/{GRID_FILENAME} through "
        "protea.core.operations._run_cafa_per_protein.grid_rows_from_sink, at "
        f"or after the version that declares {META_PREFIX}version. Refusing "
        "rather than computing a fixed-threshold interval, which is an "
        "interval for a different statistic."
    )


def resolve_setting_file(root: Path, setting: str, *, result_id: str) -> Path | None:
    """The grid file for one setting, or a refusal naming the producer.

    ``None`` means the setting was simply not evaluated, which is a normal
    absence: an evaluation writes what it scored. A legacy file standing where
    the grid file should be is not an absence, it is the wrong artefact, and it
    raises. The filename is the cheap half of the detection and not the whole
    of it: a legacy table renamed and broadcast across the grid columns passes
    here and is refused on its content by :func:`_check_curve_varies`.
    """
    grid = root / setting / GRID_FILENAME
    if grid.exists():
        return grid
    legacy = root / setting / LEGACY_FILENAME
    if legacy.exists():
        raise ThresholdGridUnavailableError(_legacy_refusal(result_id, setting, legacy))
    return None


def read_grid_metadata(path: Path, setting: str, *, result_id: str) -> GridMeta:
    """Parse and gate the footer. Nothing here reads a row."""
    values = _decoded_metadata(path)
    if values.get("version") != SCHEMA_VERSION:
        raise ThresholdGridUnavailableError(
            f"evaluation result {result_id}, setting {setting}: {path.name} declares "
            f"{META_PREFIX}version={values.get('version')!r}, and this operation reads "
            f"{SCHEMA_VERSION!r}. A producer wrote the columns without the contract, so "
            "nothing here can be trusted to mean what its name says. Re-run "
            "run_cafa_evaluation with a producer that stamps the schema version."
        )
    absent = [key for key in SEMANTIC_COMPARABILITY_KEYS if not values.get(key)]
    if absent:
        raise ThresholdGridUnavailableError(
            f"evaluation result {result_id}, setting {setting}: {path.name} does not stamp "
            f"{absent}. Those keys are what make two numbers about the same thing, and an "
            "absent key compares equal to another absent key: an unstamped file would pass "
            "the comparability gate that a stamped one fails, so two systems scored against "
            "different information-accretion sets would be published as a method difference. "
            "The gate is on presence first and equality second."
        )
    try:
        tau_grid = np.asarray(json.loads(values["tau_grid"]), dtype=np.float64)
        th_step = float(values["th_step"])
        variants = tuple(json.loads(values["variants"]))
    except (KeyError, ValueError, TypeError) as exc:
        raise ThresholdGridUnavailableError(
            f"evaluation result {result_id}, setting {setting}: {path.name} is missing or "
            f"cannot parse the grid metadata ({exc}). tau_grid, th_step and variants are "
            "required: the grid is a property of the file and reconstructing it from "
            "th_step alone was already rejected in this codebase, because a silent "
            "off-by-one attributes every protein a neighbouring threshold's score."
        ) from exc
    return GridMeta(setting, tau_grid, th_step, variants, values)


def require_variant(meta: GridMeta, variant: str, *, result_id: str) -> None:
    """Refuse rather than fall back when the requested weighting is absent."""
    if variant in meta.variants:
        return
    raise ThresholdGridUnavailableError(
        f'weighting="{"ia_weighted" if variant == "weighted" else "unweighted"}" was '
        f"requested, but eval_artifacts/{result_id}/{meta.setting}/{GRID_FILENAME} declares "
        f"variants {list(meta.variants)}: the evaluation that produced it has no {variant} "
        "components to sum. Re-run run_cafa_evaluation with information_accretion_set_id "
        "set, or ask for the variant it does carry and read the result under its own name. "
        "This operation does not compute an unweighted number and label it f_micro_w."
    )


def _check_grid_shape(meta: GridMeta, width: int, *, where: str) -> None:
    if meta.n_tau != width:
        raise GridInvariantError(
            f"{where}: the declared grid holds {meta.n_tau} thresholds and the arrays are "
            f"{width} wide; refusing to guess which column a threshold is in"
        )
    if not np.all(np.diff(meta.tau_grid) > 0):
        raise GridInvariantError(f"{where}: the declared tau grid is not strictly ascending")
    rebuilt = np.arange(meta.th_step, 1, meta.th_step)
    if rebuilt.size != meta.n_tau or not np.allclose(rebuilt, meta.tau_grid, atol=1e-9):
        raise GridInvariantError(
            f"{where}: the explicit tau grid and th_step={meta.th_step} disagree; the two "
            "facts that should confirm each other do not. np.arange(th_step, 1, th_step) is "
            "normative, floating-point accumulation included, because that is what cafaeval "
            "itself evaluates on"
        )


def _check_monotone(tp: np.ndarray, pred: np.ndarray, n_gt: np.ndarray, *, where: str) -> None:
    """The mass columns are reverse-cumulative sums, so they only ever fall.

    A producer that wrote the grid descending while declaring it ascending
    passes every other gate and is caught here and nowhere else. Every
    comparison is relative: the contract lets the grid be float32, whose ulp at
    a weighted mass of 1024 is 6e-5, so an absolute 1e-5 would accuse a correct
    producer of a bug it does not have on any ordinary BPO closure.
    """
    for name, block in (("tp", tp), ("pred", pred)):
        if block.shape[1] > 1 and exceeds(block[:, 1:], block[:, :-1]):
            raise GridInvariantError(
                f"{where}: {name} rises with the threshold; it is a reverse-cumulative sum "
                "and can only fall, so the grid was probably written in the other order"
            )
    if exceeds(tp, pred):
        raise GridInvariantError(f"{where}: true-positive mass exceeds predicted mass")
    if exceeds(tp, np.broadcast_to(n_gt[:, None], tp.shape)):
        raise GridInvariantError(f"{where}: true-positive mass exceeds ground-truth mass")


def _check_eligible(eligible: np.ndarray, *, where: str) -> None:
    """Every row is a protein cafaeval actually scored.

    cafaeval restricts its kernel to the ground-truth-bearing proteins, so a row
    with no ground truth is not in the published sums. Accepting one adds its
    predicted mass to ``P`` and nothing to ``G``, which moves the panel by more
    than the effects this campaign resolves while passing every structural gate:
    five such rows on a twenty-protein panel move an arm by 0.053 and the delta
    by 0.003.
    """
    if eligible.size and not bool(np.all(eligible > 0.0)):
        bad = int(np.count_nonzero(eligible <= 0.0))
        raise GridInvariantError(
            f"{where}: {bad} rows carry no ground truth ({ELIGIBILITY_COLUMN} <= 0). The file "
            "must hold exactly the proteins cafaeval scores, which are the ones with at least "
            "one ground-truth term after the term-of-interest and exclusion masks. A row "
            "without ground truth contributes predicted mass to P and nothing to G, so it "
            "moves the panel while every other gate stays green; and the population the panel "
            "reports would no longer be the population that was scored."
        )


def _check_curve_varies(panels: dict[str, PanelArrays], *, where: str) -> None:
    """The pooled curve has to move along the grid, or it is not a grid.

    This is the content-side detection of the legacy artefact. A producer that
    renames ``per_protein.parquet`` and broadcasts its single tau column across
    the declared thresholds passes the filename check, the width check, the
    monotonicity check (a constant row never rises) and the uniqueness check,
    and then reports the fixed-threshold interval this whole operation exists to
    refuse, as a resolved win with no warning.

    Judged over the whole file rather than per namespace, deliberately. One
    namespace CAN legitimately be flat, when every term it predicts scores above
    the top threshold, which is a real state on a small panel; the broadcast bug
    is not selective and flattens every namespace at once. Refusing per
    namespace would take a whole nine-panel run down for a legitimate small CCO
    file. An arm that predicted nothing is exempt everywhere: its curve is
    constant zero, that is a real result, and it is named downstream.
    """
    flat: list[str] = []
    varying = 0
    for namespace, arrays in sorted(panels.items()):
        if arrays.tp.shape[1] < 2 or float(arrays.pred.sum()) == 0.0:
            continue
        pooled_pred = arrays.pred.sum(axis=0)
        pooled_tp = arrays.tp.sum(axis=0)
        flat_pred = float(np.ptp(pooled_pred)) <= _RTOL * max(float(pooled_pred.max()), 1.0)
        flat_tp = float(np.ptp(pooled_tp)) <= _RTOL * max(float(pooled_tp.max()), 1.0)
        if flat_pred and flat_tp:
            flat.append(namespace)
        else:
            varying += 1
    if flat and not varying:
        width = next(iter(panels.values())).n_tau
        raise ThresholdGridUnavailableError(
            f"{where}: the pooled mass columns are identical at all {width} declared "
            f"thresholds in every namespace that predicts anything ({sorted(flat)}), so the "
            "file carries one operating point written out many times. That is the "
            "single-threshold table broadcast across the grid: it re-selects nothing, every "
            "resample returns the same threshold, and the interval would be the "
            "fixed-threshold one this operation refuses. Write the reverse-cumulative curve "
            "per protein, or, if every predicted term really does score above the top "
            "threshold, say so by re-running the evaluation on a grid that reaches it."
        )


def _list_matrix(table: object, column: str, n_rows: int, n_tau: int, *, where: str) -> np.ndarray:
    """One mass column as ``(n_rows, n_tau)``, refusing anything ragged.

    ``pa.list_`` is pyarrow's default list type and does not fix a width, so a
    producer writing rows of unequal length reaches here. Reshaping on the total
    is not enough: rows of 3, 5, 4 and 4 values on a declared 4-tau grid divide
    exactly, and every protein after the ragged one is then attributed a
    neighbouring threshold's score. That is the failure this contract exists to
    prevent, so the width is asserted per row.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    col = table[column]  # type: ignore[index]
    if not pa.types.is_list(col.type) and not pa.types.is_fixed_size_list(col.type):
        raise ThresholdGridUnavailableError(
            f"{where}: column {column!r} is {col.type}, not a list of per-threshold values. "
            "A producer widened the rows instead of the columns; this operation reads one "
            "row per (namespace, protein) with the whole grid on the row."
        )
    if n_rows == 0:
        return np.zeros((0, n_tau), dtype=np.float64)
    lengths = np.asarray(
        pc.list_value_length(col).to_numpy(zero_copy_only=False), dtype=np.int64
    )
    if lengths.size != n_rows or not bool(np.all(lengths == n_tau)):
        observed = sorted(set(int(v) for v in lengths))[:5]
        raise GridInvariantError(
            f"{where}: column {column!r} holds rows of length {observed} against a declared "
            f"grid of {n_tau} thresholds. A ragged list column whose total happens to divide "
            "by the row count reshapes without complaint and gives every protein after the "
            "short row a neighbouring threshold's score, which reads as a plausible number."
        )
    flat = np.asarray(pc.list_flatten(col).to_numpy(zero_copy_only=False), dtype=np.float64)
    return flat.reshape(n_rows, n_tau)


def load_setting_grid(path: Path, meta: GridMeta, *, variant: str, result_id: str) -> SettingGrid:
    """Read one setting's file into per-namespace arrays, gating on the way in.

    The gates are vectorised and cheap, and each one is a refusal rather than a
    warning: a producer bug that reaches the estimator arrives as a plausible
    number, which is the shape this project has been bitten by repeatedly.

    The schema is gated BEFORE the table is read. ``pq.read_table(columns=...)``
    raises on an absent column first, and what it raises is an arrow field-ref
    dump naming ``__fragment_index``, so a producer whose footer disagrees with
    its columns got a stack trace where this module has a message.
    """
    import pyarrow.parquet as pq

    where = f"evaluation result {result_id}, setting {meta.setting}"
    tp_col, pred_col, gt_col = VARIANT_COLUMNS[variant]
    present = set(pq.read_schema(path).names)
    stray = LEGACY_MARKER_COLUMNS & present
    if stray:
        raise ThresholdGridUnavailableError(
            f"{where}: {path.name} carries {sorted(stray)}, which only the single-threshold "
            f"artefact has. A file named {GRID_FILENAME} holding a per-row tau is the old "
            "table under a new name; refusing rather than computing a fixed-threshold "
            "interval, which is an interval for a different statistic."
        )
    wanted = [*_KEY_COLUMNS, tp_col, pred_col, gt_col, ELIGIBILITY_COLUMN]
    missing = [name for name in dict.fromkeys(wanted) if name not in present]
    if missing:
        raise GridInvariantError(
            f"{where}: {path.name} declares variants {list(meta.variants)} but does not carry "
            f"{missing}. The footer and the columns disagree, so the file does not hold the "
            f"variant it advertises. {ELIGIBILITY_COLUMN!r} is required whatever the variants: "
            "it is the unweighted ground-truth count that says which proteins were scored."
        )
    table = pq.read_table(path, columns=list(dict.fromkeys(wanted)))

    accs = [str(v) for v in table["protein_accession"].to_pylist()]
    spaces = [str(v) for v in table["namespace"].to_pylist()]
    if len(set(zip(accs, spaces, strict=True))) != len(accs):
        raise GridInvariantError(
            f"{where}: (protein_accession, namespace) is not unique; a repeated protein would "
            "carry double weight in the pooled sum and the panel would still look plausible"
        )
    _check_grid_shape(meta, meta.n_tau, where=where)
    tp = _list_matrix(table, tp_col, len(accs), meta.n_tau, where=where)
    pred = _list_matrix(table, pred_col, len(accs), meta.n_tau, where=where)
    n_gt = np.asarray(table[gt_col].to_numpy(zero_copy_only=False), dtype=np.float64)
    eligible = np.asarray(
        table[ELIGIBILITY_COLUMN].to_numpy(zero_copy_only=False), dtype=np.float64
    )
    _check_eligible(eligible, where=where)
    _check_monotone(tp, pred, n_gt, where=where)
    panels = _split_namespaces(accs, spaces, tp, pred, n_gt)
    _check_curve_varies(panels, where=where)
    return SettingGrid(meta.setting, meta, variant, panels)


def _split_namespaces(
    accs: list[str], spaces: list[str], tp: np.ndarray, pred: np.ndarray, n_gt: np.ndarray
) -> dict[str, PanelArrays]:
    panels: dict[str, PanelArrays] = {}
    for namespace in sorted(set(spaces)):
        rows = np.array([i for i, ns in enumerate(spaces) if ns == namespace], dtype=np.int64)
        panels[namespace] = PanelArrays(
            tuple(accs[int(i)] for i in rows), tp[rows], pred[rows], n_gt[rows]
        )
    return panels


def _same_value(key: str, left: str | None, right: str | None) -> bool:
    """Equality per key: the tau grid numerically, everything else as written.

    Two producers emitting the same grid with different float repr (``0.28``
    against ``0.28000000000000003``) describe one grid, and refusing that pair
    as incomparable frames would be a refusal about JSON formatting.
    """
    if left == right:
        return True
    if key != "tau_grid" or left is None or right is None:
        return False
    try:
        a = np.asarray(json.loads(left), dtype=np.float64)
        b = np.asarray(json.loads(right), dtype=np.float64)
    except (ValueError, TypeError):
        return False
    return a.shape == b.shape and bool(np.allclose(a, b, rtol=0.0, atol=1e-9))


def assert_comparable(a: GridMeta, b: GridMeta, *, allow_mismatch: bool) -> list[str]:
    """Which comparability markers disagree, refusing on the ones that matter.

    Returned rather than only raised so the caller can record the permitted
    mismatch in the result. A returned list that nobody reads is a check nobody
    acted on, so the caller writes it into ``artifact_mismatch``.
    """
    semantic = [
        key
        for key in SEMANTIC_COMPARABILITY_KEYS
        if not _same_value(key, a.values.get(key), b.values.get(key))
    ]
    if semantic:
        detail = ", ".join(
            f"{key}: {a.values.get(key)!r} against {b.values.get(key)!r}" for key in semantic
        )
        raise PanelComparabilityError(
            f"setting {a.setting}: the two artefacts disagree on {detail}. Two numbers "
            "computed under different frames are not a difference between two systems. "
            "allow_frame_mismatch does not waive these: it exists for a differing label on "
            "otherwise identical runs, and a differing ontology snapshot or "
            "information-accretion set is not a label."
        )
    waivable = [
        key
        for key in WAIVABLE_COMPARABILITY_KEYS
        if not _same_value(key, a.values.get(key), b.values.get(key))
    ]
    if waivable and not allow_mismatch:
        detail = ", ".join(
            f"{key}: {a.values.get(key)!r} against {b.values.get(key)!r}" for key in waivable
        )
        raise PanelComparabilityError(
            f"setting {a.setting}: the two artefacts disagree on {detail}. Set "
            "allow_frame_mismatch to record the mismatch and continue anyway."
        )
    return waivable


def assert_settings_agree(result_id: str, metas: dict[str, GridMeta]) -> None:
    """One evaluation result speaks with one frame across its own settings.

    The result block reports one ontology snapshot, one information-accretion
    set and one grid for the whole comparison. Nothing else compares a side's
    settings against each other, so without this a result whose NK and LK files
    were written under different frames would have one of them reported for
    both, and the reader would have no way to see it.
    """
    items = sorted(metas.items())
    if len(items) < 2:
        return
    first_setting, first = items[0]
    for setting, meta in items[1:]:
        differ = [
            key
            for key in SEMANTIC_COMPARABILITY_KEYS
            if not _same_value(key, first.values.get(key), meta.values.get(key))
        ]
        if differ:
            raise PanelComparabilityError(
                f"evaluation result {result_id}: settings {first_setting} and {setting} "
                f"disagree on {differ}. One result reports one frame, and the settings of a "
                "single evaluation were written by one run; two frames inside one result "
                "means two runs were mixed under one id, and the result block would report "
                "whichever file happened to load first as if it described both."
            )
