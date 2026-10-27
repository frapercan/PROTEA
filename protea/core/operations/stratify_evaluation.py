"""Turn a finished evaluation into cells, one number per stratum.

`run_cafa_evaluation` writes `<SETTING>/per_protein_grid.parquet` per setting;
this reads it back at the threshold the result published, places every protein
on the axes in :mod:`protea.core.strata`, and pools each cell. It is a separate
operation rather than a step inside the evaluation driver for three reasons:
the driver has no database session and should not grow one, the evaluation path
is long and should not get longer, and the evaluations already on disk deserve
to be stratified without being rerun.

IT USED TO READ `per_protein.parquet`, WHICH NOTHING WRITES. That file is the
legacy artefact `audit_per_protein_artifacts` exists to refuse: it was sliced
at one tau and its rows were never filtered on ground truth. The producer moved
to the grid and the paired-panels operation moved with it; this one did not, and
nobody noticed because `_settings_from_store` treats an absent file as a setting
that was not evaluated. So it returned an empty list, pooled nothing, and
reported success. Measured 2026-10-27: zero jobs had ever run it, and six run
that night over the five stored results found 0 settings on every one of them
while the store held three grid files per result. Hence the refusal at the end
of `execute`: an operation that pooled nothing now says so.

Pooling is micro. The mean of per-protein F is a macro average and would be a
different number under the same name; :func:`micro_cells` does the summing.

Which WEIGHTING is pooled is declared and never inferred. The grid carries its
components per variant -- `tp_w`/`pred_w`/`n_gt_w` for the information-accretion
weighting and `tp`/`pred`/`n_gt` without it -- over different term sets, and an
evaluation run with no `information_accretion_set_id` has no weighted
components at all. The payload names the variant, the footer gate refuses when
the file does not carry it, and the written table records it beside every cell,
so the one thing that cannot happen is an unweighted sum published under a
weighted name.
"""

from __future__ import annotations

import tempfile
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any, NamedTuple

from pydantic import Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._paired_panels_artifact import (
    GRID_FILENAME,
    read_grid_metadata,
    require_variant,
    resolve_setting_file,
)
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.core.operations._run_cafa_strata import (
    VARIANT_COLUMNS,
    micro_cells,
    neighbourhoods_for,
    project,
)
from protea.core.strata import (
    NEIGHBOURHOOD_AXES,
    Aspect,
    Category,
    Neighbourhood,
    Stratum,
    stratum_for,
)
from protea.core.utils import contract_payload

#: cafaeval reports the long namespace; the strata vocabulary speaks CAFA codes.
_ASPECT_FOR_NAMESPACE: dict[str, Aspect] = {
    "biological_process": Aspect.BIOLOGICAL_PROCESS,
    "molecular_function": Aspect.MOLECULAR_FUNCTION,
    "cellular_component": Aspect.CELLULAR_COMPONENT,
}

_LENGTHS = "SELECT accession, length FROM protein WHERE length IS NOT NULL"


class StratifyEvaluationPayload(ProteaPayload):
    """What to stratify, and how finely."""

    prediction_set_id: Annotated[str, Field(description="the run whose neighbourhood is read")]
    artifacts_root: Annotated[
        str | None,
        Field(
            default=None,
            description="local directory holding <setting>/per_protein_grid.parquet",
        ),
    ]
    evaluation_result_id: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "read and write the artefacts in the store under this result's "
                "prefix; the normal path, since a finished evaluation's files "
                "live there and its temporary directory is already gone"
            ),
        ),
    ]
    axes: Annotated[
        list[str],
        Field(
            default_factory=lambda: ["category", "aspect", "length", "homology"],
            description="stratum fields to cross; all seven leaves most cells empty",
        ),
    ]
    min_population: Annotated[
        int, Field(default=30, ge=1, description="cells below this are withheld, not dropped")
    ]
    variant: Annotated[
        str,
        Field(
            description=(
                "which weighting to pool: 'weighted' sums the "
                "information-accretion components and 'unweighted' the plain "
                "ones. Required, with no default and no fallback: the two are "
                "different term sets over different rows, so a default would "
                "quietly decide which quantity a published cell holds"
            ),
        ),
    ]

    @field_validator("variant")
    @classmethod
    def _known_variant(cls, value: str) -> str:
        if value not in VARIANT_COLUMNS:
            raise ValueError(
                f"unknown variant {value!r}; expected one of {sorted(VARIANT_COLUMNS)}"
            )
        return value


def _protein_lengths(session: Session) -> dict[str, int]:
    return {r[0]: int(r[1]) for r in session.execute(text(_LENGTHS))}


def _category_for(setting: str) -> Category | None:
    """cafaeval's setting names carry the knowledge category as a prefix."""
    head = setting.split("_", 1)[0].upper()
    try:
        return Category.from_code(head)
    except (ValueError, KeyError):
        return None


#: Stands in for the donor neighbourhood when none is needed, so that the one
#: placement path serves both kinds of run. Every field is ``None``, so the
#: four donor axes come out as "no donor" -- which is why it may only be used
#: when none of those axes was requested and none of them will be read. Asking
#: for a donor axis and getting this would report an empty neighbourhood as a
#: measurement, and :func:`_strata_for_rows` refuses instead.
_UNREAD_NEIGHBOURHOOD = Neighbourhood(best_identity=None, donor_is_experimental=None)


def _rows_at_published_tau(
    grid: Path,
    meta: Any,
    variant: str,
    taus: dict[str, float],
) -> list[dict[str, Any]]:
    """One scalar row per (protein, namespace), sliced at the reported tau.

    The grid holds the whole sweep: ``tp`` and ``pred`` are list columns, one
    value per threshold, and ``n_gt`` is a scalar because the ground truth does
    not move with the threshold. Slicing happens here and not in the pooling,
    so that ``micro_cells`` keeps summing scalars and the index a column came
    from is resolved once per namespace by the artefact's own
    :meth:`GridMeta.tau_index`, which refuses a threshold the file does not
    carry instead of reading the nearest one.

    A namespace with no published tau is dropped rather than defaulted: it was
    not in the best-F table, so the result never reported it.
    """
    import pandas as pd

    tp_col, pred_col, gt_col = VARIANT_COLUMNS[variant]
    frame = pd.read_parquet(grid)
    index_for = {ns: meta.tau_index(tau) for ns, tau in taus.items()}
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict("records"):
        namespace = str(record.get("namespace"))
        position = index_for.get(namespace)
        if position is None:
            continue
        rows.append(
            {
                "protein_accession": record["protein_accession"],
                "namespace": namespace,
                "tau": taus[namespace],
                tp_col: float(record[tp_col][position]),
                pred_col: float(record[pred_col][position]),
                gt_col: float(record[gt_col]),
            }
        )
    return rows


class _SettingWork(NamedTuple):
    """Everything pooling one setting needs, named.

    Seven values threaded through one call. They travel together because every
    one of them is read for every setting, and a signature taking them loose is
    a signature somebody can call with six.
    """

    root: Path
    setting_dir: Path
    result_id: str
    payload: StratifyEvaluationPayload
    store: Any
    lengths: dict[str, int]
    neighbourhoods: dict[str, Any]


def _setting_rows(work: _SettingWork, emit: EmitFn) -> list[dict[str, Any]] | None:
    """One setting's rows, sliced at the threshold its result published.

    ``None`` means the evaluation did not score this setting, which is the one
    honest absence here. Everything else raises: the resolver refuses a legacy
    table standing where the grid belongs, the footer gate refuses a file whose
    comparability keys are unstamped, ``require_variant`` refuses a weighting
    the file does not carry, and ``_published_taus`` refuses a sweep with
    nothing saying which column of it was reported.
    """
    p, setting_dir = work.payload, work.setting_dir
    setting = setting_dir.name
    grid = resolve_setting_file(work.root, setting, result_id=work.result_id)
    if grid is None:
        return None
    meta = read_grid_metadata(grid, setting, result_id=work.result_id)
    require_variant(meta, p.variant, result_id=work.result_id)
    taus = _published_taus(setting_dir, result_id=work.result_id)
    rows = _rows_at_published_tau(grid, meta, p.variant, taus)
    emit(
        "stratify_evaluation.sliced",
        None,
        {
            "setting": setting,
            "variant": p.variant,
            "rows": len(rows),
            "taus": {ns: taus[ns] for ns in sorted(taus)},
        },
        "info",
    )
    return rows


def _strata_for_rows(
    rows: list[dict[str, Any]],
    *,
    category: Category,
    lengths: dict[str, int],
    neighbourhoods: dict[str, Any],
    axes: Sequence[str],
) -> dict[int, Stratum]:
    """Index of row position -> stratum, skipping rows that cannot be placed.

    A protein with no length or no resolvable aspect is skipped rather than
    defaulted into a band, because every default here would move a published
    number by an amount nobody chose.

    A missing donor neighbourhood is only disqualifying when the run asks for
    an axis that reads one. It used to disqualify unconditionally, which meant
    a run crossing category x aspect x length -- three facts about the query's
    own sequence, needing no alignment at all -- silently reported on whichever
    proteins happened to have aligned donors. On this campaign that was a small
    and non-random minority, so the withheld-cell counts looked like a coverage
    floor and were really a filter nobody had asked for.
    """
    needs_donor = bool(NEIGHBOURHOOD_AXES & set(axes))
    placed: dict[int, Stratum] = {}
    for i, row in enumerate(rows):
        aspect = _ASPECT_FOR_NAMESPACE.get(str(row.get("namespace")))
        acc = str(row.get("protein_accession"))
        residues = lengths.get(acc)
        if aspect is None or not residues:
            continue
        neighbourhood = neighbourhoods.get(acc)
        if neighbourhood is None:
            if needs_donor:
                continue
            neighbourhood = _UNREAD_NEIGHBOURHOOD
        placed[i] = stratum_for(
            category=category, aspect=aspect, residues=residues, neighbourhood=neighbourhood
        )
    return placed


#: The table cafaeval writes with the threshold it reports each namespace at.
#: Fetched beside the grid because the operating point is not a property of the
#: grid -- the grid holds the whole sweep -- and reading it from here rather
#: than from the ``evaluation_result`` row keeps one source for both modes: a
#: local directory has no row to read.
BEST_F_FILENAME = "evaluation_best_f.tsv"


def _settings_from_store(store: Any, result_id: str, work: Path) -> list[str]:
    """Fetch every setting's grid and best-F table under a result's prefix.

    The settings are not listed from the store: the store protocol has no list,
    and the three CAFA knowledge categories are the only settings an evaluation
    writes. A setting whose grid is absent is simply not returned; a setting
    with a grid and no best-F table IS returned, and refused later by name,
    because that is a damaged result rather than an unevaluated setting.
    """
    found: list[str] = []
    for setting in ("NK", "LK", "PK"):
        grid_key = eval_artifact_key(uuid.UUID(result_id), f"{setting}/{GRID_FILENAME}")
        if not store.exists(grid_key):
            continue
        target = work / setting
        target.mkdir(parents=True, exist_ok=True)
        (target / GRID_FILENAME).write_bytes(store.get(grid_key))
        best_key = eval_artifact_key(uuid.UUID(result_id), f"{setting}/{BEST_F_FILENAME}")
        if store.exists(best_key):
            (target / BEST_F_FILENAME).write_bytes(store.get(best_key))
        found.append(setting)
    return found


def _published_taus(setting_dir: Path, *, result_id: str) -> dict[str, float]:
    """``{cafaeval namespace: tau}``, the operating point each one reports.

    Read from the file cafaeval wrote and not chosen here. A stratum pooled at
    a threshold other than the one its parent cell published would be a
    different measurement wearing the same label, and the difference would be
    invisible: both tables would say ``f_micro``.
    """
    path = setting_dir / BEST_F_FILENAME
    if not path.exists():
        raise ValueError(
            f"evaluation result {result_id}, setting {setting_dir.name}: there is a "
            f"{GRID_FILENAME} and no {BEST_F_FILENAME}, so the grid holds a sweep and "
            "nothing says which column of it the result published. Refusing rather than "
            "picking a threshold: every column is a defensible number and only one of "
            "them is the one already reported."
        )
    lines = path.read_text().splitlines()
    header = lines[0].split("\t")
    ns_at, tau_at = header.index("ns"), header.index("tau")
    taus: dict[str, float] = {}
    for line in lines[1:]:
        if not line.strip():
            continue
        fields = line.split("\t")
        taus[fields[ns_at]] = float(fields[tau_at])
    return taus


def _resolve_source(
    p: StratifyEvaluationPayload, emit: EmitFn
) -> tuple[Path, Any, Any]:
    """Where the per-protein tables are read from, and where results go back.

    Two modes on purpose. A local directory is what a test or an ad-hoc run
    has; a result id is what a finished evaluation has, since its temporary
    directory is long gone and the files live in the store.
    """
    if not p.artifacts_root and not p.evaluation_result_id:
        raise ValueError(
            "give either artifacts_root (a local directory) or "
            "evaluation_result_id (the store prefix); with neither there is "
            "nothing to read"
        )
    if not p.evaluation_result_id:
        return Path(str(p.artifacts_root)), None, None

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage.factory import get_artifact_store

    # protea/core/operations/<this>.py -> parents[3] is the repo root, the same
    # walk export_evaluation_targets and compute_information_accretion do.
    store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
    tmp = tempfile.TemporaryDirectory(prefix="protea_strata_")
    root = Path(tmp.name)
    emit(
        "stratify_evaluation.fetched",
        None,
        {"settings": _settings_from_store(store, p.evaluation_result_id, root)},
        "info",
    )
    return root, store, tmp


class StratifyEvaluationOperation(Operation):
    """Pool a finished evaluation into per-stratum cells."""

    name = "stratify_evaluation"
    description = (
        "Pool a finished evaluation's per-protein scores into one micro-averaged "
        "cell per stratum, withholding cells below the population floor."
    )
    payload_model = StratifyEvaluationPayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """One line naming what is being crossed, which is the thing that
        distinguishes two otherwise identical runs of this operation."""
        axes = payload.get("axes") or ["category", "aspect", "length", "homology"]
        pset = str(payload.get("prediction_set_id", "?"))[:8]
        return f"{pset} by {' x '.join(axes)} (floor {payload.get('min_population', 30)})"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = StratifyEvaluationPayload.model_validate(contract_payload(payload))
        root, store, tmp = _resolve_source(p, emit)
        neighbourhoods = neighbourhoods_for(session, p.prediction_set_id)
        lengths = _protein_lengths(session)
        emit(
            "stratify_evaluation.inputs",
            None,
            {"neighbourhoods": len(neighbourhoods), "lengths": len(lengths)},
            "info",
        )

        summary: dict[str, Any] = {"settings": {}, "axes": list(p.axes), "variant": p.variant}
        result_id = str(p.evaluation_result_id or root)
        for setting_dir in sorted(d for d in root.iterdir() if d.is_dir()):
            written = self._stratify_setting(
                _SettingWork(
                    root=root,
                    setting_dir=setting_dir,
                    result_id=result_id,
                    payload=p,
                    store=store,
                    lengths=lengths,
                    neighbourhoods=neighbourhoods,
                ),
                emit,
            )
            if written is not None:
                summary["settings"][setting_dir.name] = written
        if not any(w["cells"] for w in summary["settings"].values()):
            if tmp is not None:
                tmp.cleanup()
            raise ValueError(
                f"{result_id}: no setting yielded a single stratum, so there is nothing "
                "to write and nothing was written. This used to be reported as success, "
                "which is how the operation ran six times over five stored results and "
                "produced no artefact anybody could read. Either the result has no "
                f"<SETTING>/{GRID_FILENAME}, or no protein in it could be placed on the "
                "requested axes -- which is what happens when a run crosses a donor axis "
                "over a prediction set that has no donors at all, such as one ingested "
                "from a method's own file. The sliced and read-versus-placed counts in "
                "this job's events say which."
            )
        if tmp is not None:
            tmp.cleanup()
        return OperationResult(result=summary)

    def _stratify_setting(self, work: _SettingWork, emit: EmitFn) -> dict[str, Any] | None:
        """Pool one setting, or ``None`` when there is nothing of it to pool.

        ``None`` is for the two honest absences: a directory whose name is not
        a knowledge category, and a setting the evaluation did not score. Every
        other way of having nothing raises, in the resolver, the footer gate or
        the best-F reader, so that an unreadable artefact cannot arrive at the
        caller as a setting that simply was not there.
        """
        p, setting_dir = work.payload, work.setting_dir
        setting = setting_dir.name
        category = _category_for(setting)
        if category is None:
            emit(
                "stratify_evaluation.setting_skipped",
                None,
                {"setting": setting, "reason": "no knowledge category in the name"},
                "warning",
            )
            return None
        rows = _setting_rows(work, emit)
        if rows is None:
            return None
        placed = _strata_for_rows(
            rows,
            category=category,
            lengths=work.lengths,
            neighbourhoods=work.neighbourhoods,
            axes=p.axes,
        )
        # The stratum rides on the row rather than being looked up by
        # position: resolving it per row through list.index would be
        # quadratic and would compare dicts by value, which is both slow
        # and wrong when two proteins happen to score identically.
        placed_rows = [{**rows[i], "_stratum": st} for i, st in placed.items()]
        cells = micro_cells(
            placed_rows,
            lambda row: project(row["_stratum"], p.axes),
            VARIANT_COLUMNS[p.variant],
        )
        written = self._write(setting_dir, cells, p, emit, len(rows), len(placed))
        if work.store is not None and p.evaluation_result_id:
            key = eval_artifact_key(
                uuid.UUID(p.evaluation_result_id), f"{setting}/strata.parquet"
            )
            work.store.put(key, setting_dir / "strata.parquet")
            written["key"] = key
        return written

    @staticmethod
    def _write(
        setting_dir: Path,
        cells: dict[Any, Any],
        p: StratifyEvaluationPayload,
        emit: EmitFn,
        n_rows: int,
        n_placed: int,
    ) -> dict[str, Any]:
        import pandas as pd

        populations = {k: c.n_proteins for k, c in cells.items()}
        reportable = [k for k, n in populations.items() if n >= p.min_population]
        withheld = [k for k, n in populations.items() if n < p.min_population]
        frame = pd.DataFrame(
            [
                {
                    # ``.value``, not ``str()``. Category and Aspect are plain
                    # Enums, so str() yields "Aspect.MOLECULAR_FUNCTION" and the
                    # column stops matching the vocabulary every other table
                    # uses. The bands are StrEnum and agree either way.
                    **{
                        axis: getattr(value, "value", str(value))
                        for axis, value in zip(p.axes, key, strict=True)
                    },
                    "n_proteins": c.n_proteins,
                    # Neutral names with ``variant`` beside them, which is the
                    # same vocabulary the evaluation_result row publishes
                    # (``f_micro``, ``precision``, ``recall``). The previous
                    # names ended in ``_w`` whatever had been pooled, so an
                    # evaluation run without an information-accretion set --
                    # which is every evaluation stored on this deployment --
                    # would have had its unweighted sums published as
                    # ``f_micro_w``. Nothing read these columns yet: no
                    # strata.parquet had ever been written.
                    "variant": p.variant,
                    "precision": c.precision_w,
                    "recall": c.recall_w,
                    "f_micro": c.f_micro_w,
                    # Withheld cells are WRITTEN and flagged, never dropped: a
                    # table that prints only what survived looks identical to a
                    # table that covered everything.
                    "reportable": populations[key] >= p.min_population,
                }
                for key, c in cells.items()
            ]
        )
        setting = setting_dir.name
        setting_dir.mkdir(parents=True, exist_ok=True)
        out = setting_dir / "strata.parquet"
        frame.to_parquet(out, index=False)
        counts = {
            "rows_read": n_rows,
            "rows_placed": n_placed,
            "cells": len(cells),
            "reportable": len(reportable),
            "withheld": len(withheld),
        }
        emit("stratify_evaluation.written", None,
             {"setting": setting, "path": str(out), **counts}, "info")
        return counts
