"""The proteins behind one panel, each with what it scored and why it is there.

A panel says NK x BPO holds 1,509 proteins and that the leading level reaches
0.2652. Both numbers are pooled over the panel, so a reader who doubts one of
them has nowhere to go: the population is a count and the score is a micro sum,
and neither can be opened. This serves the rows underneath.

Where the rows come from. ``run_cafa_evaluation`` writes one
``<setting>/per_protein_grid.parquet`` beside each result, holding each
protein's components over the WHOLE threshold sweep; this slices it at the
threshold the run reported and divides. Its rows for one namespace under one
setting are the panel's population: the category is the directory and the
aspect is the namespace column, so nothing has to be joined and nothing can
drift out of a join.

IT USED TO READ ``per_protein.parquet``, AND THAT MADE A PANEL OPENABLE ONLY
WHEN THE RUN HAD BEEN WEIGHTED. ``rows_from_sink`` keeps the weighted variant
alone, so a run with no ``information_accretion_set_id`` produced no weighted
records, wrote no file, and emitted ``per_protein_empty`` as a warning. This
endpoint then answered 404 saying "this run wrote no per-protein scores for
this setting", which was false: it wrote the grid. Measured 2026-10-27 over the
seven scoring runs on this deployment, the correlation is perfect -- four runs
with an accretion set wrote three files each, three runs without wrote none and
emitted nine warnings between them -- so whether a reader could open a panel
depended on a weighting choice nobody connected to it.

The grid is also the better source. The legacy file's rows were never filtered
on ground truth, which is the defect ``audit_per_protein_artifacts`` exists to
count, so the two are not the same population and the grid's is the right one.

Measured against the graph's own panel counts on this campaign, eight of the
nine agree to the protein. The ninth, PK x BPO, is one short: 5,810 here
against 5,811 there, under both prediction sets, so it is a property of the
evaluation and not of one run. The two numbers are counted from different
places. The graph counts proteins that GAINED a term of that aspect in that
bucket, straight from the window's ground truth; this counts proteins cafaeval
actually SCORED. A query the kernel could not key is dropped when the artefact
is written, and it is dropped silently. So ``panel_population`` is named for
what it is, the scored population, and is not claimed to equal the ground
truth's count.

Why not ``/stratum/{prediction_set_id}/members``. That endpoint answers a
different question and says so: it places every query of a run on the length
and homology axes, and it does not filter on category or aspect because
whether a query is no-knowledge in an aspect is a fact about the evaluation's
ground truth and not about the retrieval. Its population for NK x BPO is
14,025, the whole query set, against the panel's 1,509. Drilling from a panel
into that number would show a reader nine times the proteins the panel is
made of. The per-protein artefact carries the category (it is the directory)
and the aspect (it is the namespace column), so this endpoint filters on both.

What it adds to the artefact. The two retrieval axes, length and homology, are
computed here with the same two functions the stratification itself uses, so a
band shown beside a protein cannot disagree with the band that protein was
pooled into. Nothing is recomputed that the artefact already holds: every score
on every row is read, not derived.
"""

from __future__ import annotations

import io
import json
import uuid
from dataclasses import dataclass
from typing import Any, NamedTuple

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.api.deps import get_session_factory, get_settings
from protea.api.routers import _stratum_drops as drops
from protea.api.routers._arm_identity import with_arm_identity
from protea.core.domain.category import Category
from protea.core.operations._run_cafa_grid_artifact import (
    GRID_FILENAME,
    GRID_META_PREFIX,
)
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.core.operations._run_cafa_strata import VARIANT_COLUMNS as _VARIANT_COLUMNS
from protea.core.operations._run_cafa_strata import neighbourhoods_for

# The namespace-to-aspect map is imported from the operation that WRITES the
# strata rather than restated here. A second spelling of it would be a second
# thing to keep in sync, and the failure it would produce is silent: a panel
# opened under one map and pooled under another disagrees about which proteins
# it holds without either side raising.
from protea.core.operations.stratify_evaluation import _ASPECT_FOR_NAMESPACE, BEST_F_FILENAME
from protea.core.strata import UNREAD_NEIGHBOURHOOD, stratum_for
from protea.infrastructure.storage.factory import ArtifactStoreUnavailable, get_artifact_store

router = APIRouter(prefix="/strata", tags=["strata"])

#: The knowledge settings an evaluation writes, which are also the directories
#: the per-protein artefacts sit in. Probed by name because the artifact store
#: has no list operation.
_SETTINGS: tuple[str, ...] = ("NK", "LK", "PK")

#: Namespace as cafaeval spells it, keyed by the aspect code a caller holds.
#: Both spellings are accepted: a screen holds the CAFA form (``BPO``) because
#: that is what the panels print, and a caller reading the strata artefact
#: holds the wire form (``P``) because that is what the column carries.
_NAMESPACE_FOR_ASPECT: dict[str, str] = {}
for _namespace, _aspect in _ASPECT_FOR_NAMESPACE.items():
    _NAMESPACE_FOR_ASPECT[_aspect.cafa] = _namespace
    _NAMESPACE_FOR_ASPECT[_aspect.code] = _namespace

#: Sequence length per query. Restricted to the accessions actually in the
#: artefact rather than read whole: the protein table holds the corpus and the
#: artefact holds a panel, and the panel is four orders of magnitude smaller.
_LENGTHS = text(
    """
    SELECT p.accession AS accession, p.length AS residues
    FROM protein p
    WHERE p.accession = ANY(:accessions) AND p.length IS NOT NULL
    """
)

#: The arm, named the way the panels name it. ``display_name`` alone is not an
#: identity here: the eight arms sharing a prediction set differ only in their
#: scoring configuration and donor policy, so a picker that showed the model
#: would offer eight rows reading ``esm2_650m`` and a reader choosing between
#: them would be choosing blind.
_ARM = text(
    with_arm_identity(
        """
    SELECT er.prediction_set_id::text          AS prediction_set_id,
           sc.name                             AS scoring_name,
           COALESCE(ec.display_name, ec.model_name) AS embedding_name,
{DEPTH_IDENTITY_COLUMN},
{ARM_IDENTITY_COLUMNS},
           ps.meta ->> 'metric'                AS metric
    FROM evaluation_result er
    LEFT JOIN scoring_config sc ON sc.id = er.scoring_config_id
    LEFT JOIN prediction_set ps ON ps.id = er.prediction_set_id
    LEFT JOIN embedding_config ec ON ec.id = ps.embedding_config_id
    WHERE er.id = :rid
    """
    )
)

#: Five minutes, matching the stratum comparison. The join behind a page is one
#: parquet read plus one neighbourhood query, and the neighbourhood query is the
#: expensive half: it runs over every candidate of a run and takes seconds. An
#: operator paging through a cell or switching sort order would otherwise pay it
#: again on every click, for an artefact that only changes when a run is rescored.
_TTL = 300.0

#: How rows may be ordered. Weakest first is the default because the reason to
#: open a cell is usually to see what makes it hard, and a page that opens on the
#: proteins that already worked answers a question nobody asked.
_SORTS: tuple[str, ...] = ("f_asc", "f_desc", "identity_asc", "identity_desc", "accession")


def _namespace_for(raw: str) -> str:
    """Map an aspect code, in either spelling, to cafaeval's namespace.

    Refused as a 422 rather than a 500, because an unrecognised aspect is a bad
    request and the reply should name the vocabulary it would have accepted.
    """
    ns = _NAMESPACE_FOR_ASPECT.get(raw.strip().upper())
    if ns is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"aspect {raw!r} is not a GO aspect. Accepted: "
                + ", ".join(sorted(_NAMESPACE_FOR_ASPECT))
            ),
        )
    return ns


def _hoods(factory: sessionmaker[Session], prediction_set_id: str) -> dict[str, Any]:
    """The retrieval neighbourhood of a run, cached across its arms.

    Eight evaluation results can share one prediction set, and the
    neighbourhood is a property of the retrieval rather than of the scoring, so
    switching arms inside a cell must not recompute it.
    """

    def produce() -> dict[str, Any]:
        with factory() as session:
            return neighbourhoods_for(session, prediction_set_id)

    return cached(f"stratum:hoods:{prediction_set_id}", _TTL, produce, serve_stale_on_error=True)


@dataclass
class _Placed:
    """The artefact's rows with the two retrieval axes attached.

    The reasons are broken down rather than totalled. A protein missing because
    the record holds no length is a corpus fact; one missing because a stored
    identity is not a percentage is a defect. Summing them would report a
    single number that two different actions would fix.

    ``no_donor`` is in this list but is NOT one of those reasons, and that is
    the distinction the first version got wrong. It counts rows whose donors
    were never aligned: they still place on the three sequence axes and read
    ``none`` on the four donor ones, so the number is a fact about the panel
    and not a gap in it. The response keeps it outside ``unplaced`` so a reader
    cannot add it to the others and get the shortfall.
    """

    rows: list[dict[str, Any]]
    tau: float | None
    no_donor: int
    no_length: int
    off_scale: int

    @property
    def placed(self) -> int:
        return sum(1 for r in self.rows if r["length_band"] is not None)


def _scored_zero(rows: list[dict[str, Any]]) -> int:
    """How many of the cell the run scored nothing at all on.

    A count and not an average, so it says nothing the pooled cell already
    says: a cell reading 0.09 over 375 proteins is one claim when the mass is
    spread and a different one when two thirds of it is exactly zero, and the
    pooled number cannot tell them apart. It is also what makes the default
    ordering readable: a page of zeros is a finding once the reader knows how
    many there are, and a mystery until then.

    Its own function because it was one line inside the endpoint reading
    ``r["f_w"]``, and when the row's metric lost that suffix nothing caught the
    stale key: ruff cannot see into a dict subscript and no test reached the
    line. A KeyError on every request, found by reading the function and not
    by running it.
    """
    return sum(1 for r in rows if r["f"] == 0.0)


class _Cell(NamedTuple):
    """Which panel was asked for, in the two vocabularies it has.

    ``setting`` is the knowledge category as the directory names it and
    ``namespace`` is the aspect as cafaeval names it. Both are needed and they
    are not derivable from each other without a table, so they travel
    together from the one place that validates them.
    """

    setting: str
    namespace: str


class _Counted(NamedTuple):
    """The page, and every count that frames it.

    Four populations and a page. Loose, this was five arguments of the ten
    that made the response builder the widest signature in the module; named,
    it is the thing the endpoint spent its whole body computing.
    """

    placed: _Placed
    rows: list[dict[str, Any]]
    page: list[dict[str, Any]]
    scored_zero: int


def _response(
    result_id: uuid.UUID,
    arm: Any,
    counted: _Counted,
    cell: _Cell,
    at: _Where,
    page_of: _Page,
) -> dict[str, Any]:
    """The page, and the counts that frame it.

    The counts are four and they are deliberately not one.
    ``panel_population`` is every protein the artefact scored in this cell,
    which is the number the panel prints. ``placed`` is how many of them could
    be put on the retrieval axes. ``matched`` is how many survive the bands the
    caller pinned. ``returned`` is how many are on this page. A single total
    would collapse a retrieval gap, a filter and a page size into one number
    that answers none of the three questions.
    """
    placed = counted.placed
    return {
        "evaluation_result_id": str(result_id),
        "arm": {
            "prediction_set_id": arm["prediction_set_id"],
            "embedding_name": arm["embedding_name"],
            "scoring_name": arm["scoring_name"],
            "donor_policy": arm["donor_policy"],
            "depth": arm["depth"],
            "metric": arm["metric"],
        },
        "setting": cell.setting,
        "where": {
            "category": cell.setting,
            "aspect": _ASPECT_FOR_NAMESPACE[cell.namespace].cafa,
            "length": at.length,
            "homology": at.homology,
        },
        "tau": placed.tau,
        "panel_population": len(placed.rows),
        "placed": placed.placed,
        "unplaced": {
            "no_length": placed.no_length,
            "off_scale": placed.off_scale,
        },
        # Outside ``unplaced`` on purpose: these rows ARE placed. See _Placed.
        "without_donor": placed.no_donor,
        "matched": len(counted.rows),
        "scored_zero": counted.scored_zero,
        "returned": len(counted.page),
        "offset": page_of.offset,
        "limit": page_of.limit,
        "sort": page_of.sort,
        "proteins": counted.page,
    }


def _validated_cell(at: _Where, sort: str) -> _Cell:
    """The setting, the namespace and the sort, or a 422 naming the vocabulary.

    Three refusals that were inline and are the same kind of thing: a caller
    asking for a cell this endpoint does not have. Together because a reader
    checking what a bad request returns should find them in one place.
    """
    if at.setting.upper() not in _SETTINGS:
        raise HTTPException(
            status_code=422,
            detail=f"setting {at.setting!r} is not one of {', '.join(_SETTINGS)}",
        )
    if sort not in _SORTS:
        raise HTTPException(
            status_code=422, detail=f"sort {sort!r} is not one of {', '.join(_SORTS)}"
        )
    return _Cell(at.setting.upper(), _namespace_for(at.aspect))


def _panel_keys(store: Any, result_id: uuid.UUID, setting: str) -> tuple[str, str]:
    """The grid and the best-F table, both present, or a 404 saying which is not.

    Existence is checked before the cache and not inside the producer. A
    missing artefact is a 404 and has to stay one: raised inside the producer
    it would be swallowed the moment a stale page for another cell was in the
    store.

    Two keys and two different refusals. No grid means the run scored nothing
    in this setting, which is an absence. A grid with no best-F table means the
    sweep is there and nothing says which column of it the panel published,
    which is a damaged result: every column is a defensible number and only one
    of them is the one already reported.
    """
    grid = eval_artifact_key(result_id, f"{setting}/{GRID_FILENAME}")
    if not store.exists(grid):
        raise HTTPException(
            status_code=404,
            detail=(
                f"no {setting}/{GRID_FILENAME} for evaluation result {result_id}; this "
                "run scored no proteins in this setting, so its panels cannot be opened"
            ),
        )
    best_f = eval_artifact_key(result_id, f"{setting}/{BEST_F_FILENAME}")
    if not store.exists(best_f):
        raise HTTPException(
            status_code=404,
            detail=(
                f"evaluation result {result_id} has a {setting}/{GRID_FILENAME} and no "
                f"{setting}/{BEST_F_FILENAME}, so the grid holds the whole sweep and "
                "nothing says which threshold the panel published. Refusing rather than "
                "picking one: every column is a defensible number and only one of them "
                "is the one already reported"
            ),
        )
    return grid, best_f


class _Footer(NamedTuple):
    """What the grid's own metadata says about the numbers inside it."""

    tau_grid: Any
    variant: str


def _grid_footer(table: Any, *, result_id: str, setting: str) -> _Footer:
    """The tau grid and the weighting, read from the file and never assumed.

    The grid is a property of the artefact: reconstructing it from ``th_step``
    alone attributes every protein a neighbouring threshold's score, which is
    a mistake this codebase has already rejected once. The variant comes from
    the same footer, because a run with no accretion set declares
    ``unweighted`` alone and its components are the only ones there are.
    """
    import numpy as np

    meta = {
        k.decode().split(GRID_META_PREFIX)[-1]: v.decode()
        for k, v in (table.schema.metadata or {}).items()
    }
    try:
        tau_grid = np.asarray(json.loads(meta["tau_grid"]), dtype=np.float64)
        variants = json.loads(meta["variants"])
        variant = "weighted" if "weighted" in variants else str(variants[0])
    except (KeyError, ValueError, TypeError, IndexError) as exc:
        raise HTTPException(
            status_code=409,
            detail=(
                f"evaluation result {result_id}, setting {setting}: {GRID_FILENAME} does "
                f"not carry a readable tau grid or variant list ({exc}). Both are what "
                "say which number each column is, and a file that does not stamp them "
                "cannot be read into a page that claims a threshold"
            ),
        ) from exc
    return _Footer(tau_grid, variant)


def _tau_column(footer: _Footer, best_f: str, namespace: str, *, where: str) -> tuple[float, int]:
    """The threshold this namespace published, and the column that holds it.

    Read from cafaeval's own best-F table and not chosen here. A page of
    proteins shown at a threshold other than its panel's would be a different
    measurement wearing the same label, and nothing downstream could tell.
    """
    import numpy as np

    lines = [line for line in best_f.strip().splitlines() if line.strip()]
    head = lines[0].split("\t")
    i_ns, i_tau = head.index("ns"), head.index("tau")
    tau = None
    for line in lines[1:]:
        fields = line.split("\t")
        if fields[i_ns] == namespace:
            tau = float(fields[i_tau])
    if tau is None:
        raise _NamespaceNotScored
    hits = np.flatnonzero(np.abs(footer.tau_grid - tau) <= 1e-9)
    if hits.size != 1:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{where}: the panel reports tau {tau}, which is not a threshold this "
                f"grid carries ({footer.tau_grid.size} from {float(footer.tau_grid.min())} "
                f"to {float(footer.tau_grid.max())}). Refusing rather than reading the "
                "nearest column: a page shown at a threshold it was not scored at is not "
                "recoverable from itself"
            ),
        )
    return tau, int(hits[0])


class _NamespaceNotScored(Exception):
    """This namespace is not in the best-F table, so the run never reported it."""


def _sliced_at_the_published_tau(
    grid: Any,
    best_f: str,
    namespace: str,
    *,
    result_id: str,
    setting: str,
) -> Any:
    """The grid's rows for one namespace, as scalars at the reported threshold.

    The grid holds ``tp`` and ``pred`` as list columns, one value per
    threshold, and ``n_gt`` as a scalar because the ground truth does not move
    with the threshold. The three ratios are divided from those components
    here, which is the same arithmetic over the same columns at the same tau
    that produces the panel's own cell -- so a row cannot disagree with the
    cell it is inside, which is a stronger property than reading a stored
    ratio from a second file.
    """
    import pandas as pd
    import pyarrow.parquet as pq

    table = pq.read_table(grid)
    footer = _grid_footer(table, result_id=result_id, setting=setting)
    try:
        tau, position = _tau_column(
            footer, best_f, namespace, where=f"evaluation result {result_id}, setting {setting}"
        )
    except _NamespaceNotScored:
        return pd.DataFrame(columns=["protein_accession", "namespace", "tau"])
    tp_col, pred_col, gt_col = _VARIANT_COLUMNS[footer.variant]

    frame = table.to_pandas()
    frame = frame[frame["namespace"] == namespace]
    rows = []
    for record in frame.to_dict("records"):
        tp = float(record[tp_col][position])
        pred = float(record[pred_col][position])
        gt = float(record[gt_col])
        precision = tp / pred if pred > 0 else 0.0
        recall = tp / gt if gt > 0 else 0.0
        denom = precision + recall
        rows.append(
            {
                "protein_accession": record["protein_accession"],
                "namespace": namespace,
                "tau": tau,
                "variant": footer.variant,
                "precision": precision,
                "recall": recall,
                "f": (2 * precision * recall / denom) if denom > 0 else 0.0,
                "n_gt": gt,
                "pred": pred,
                "tp": tp,
            }
        )
    drops.refuse_when_the_grid_and_the_record_disagree(
        rows, result_id=result_id, setting=setting, namespace=namespace)
    return pd.DataFrame(rows)


def _row(
    record: dict[str, Any],
    accession: str,
    residues: int | None,
    stratum: Any,
    hood: Any,
) -> dict[str, Any]:
    """One protein's row: where it sits, and what it scored.

    The three components come from the artefact and the three ratios are
    divided from them in :func:`_sliced_at_the_published_tau`, at the one
    threshold the panel published. ``variant`` says which weighting they are:
    these names used to end in ``_w`` whatever the run had done, and three of
    the seven scoring runs on this deployment were unweighted, so for those the
    suffix was a false label on a correct number.
    """
    return {
        "accession": accession,
        "residues": residues,
        "length_band": None if stratum is None else str(stratum.length),
        "homology_band": None if stratum is None else str(stratum.homology),
        "donor_evidence": None if stratum is None else str(stratum.donor_evidence),
        "best_identity": None if hood is None else hood.best_identity,
        "donor_is_experimental": None if hood is None else hood.donor_is_experimental,
        "taxonomic_relation": None if hood is None else hood.taxonomic_relation,
        "variant": str(record["variant"]),
        "precision": float(record["precision"]),
        "recall": float(record["recall"]),
        "f": float(record["f"]),
        "n_gt": float(record["n_gt"]),
        "pred": float(record["pred"]),
        "tp": float(record["tp"]),
    }


#: The closed set of reasons a scored protein ends up with no bands at all.
#: Written down rather than inferred, so adding a third reason has to name
#: itself here and in the response instead of landing in whichever counter was
#: nearest. A missing donor is deliberately NOT one of them: see :func:`_spot`.
_UNPLACED_REASONS: tuple[str, ...] = ("no_length", "off_scale")


def _spot(
    category: Category,
    aspect: Any,
    residues: int | None,
    hood: Any,
) -> tuple[Any, str | None]:
    """Where one protein sits, and the reason when it sits nowhere.

    A MISSING DONOR DOES NOT UNPLACE A ROW, which is the whole point of this
    function existing. The endpoint used to test the neighbourhood first and
    leave the stratum null, so the length band -- a fact about the query's own
    sequence, needing no alignment at all -- was discarded because a different
    axis could not be read. On the live record that silently emptied the drawer
    for every ingested arm and for 86,5% of all candidates: the endpoint
    answered 0 placed over a panel of thousands while the operation next door
    placed 100% of the same rows.

    Now the row places with :data:`UNREAD_NEIGHBOURHOOD`, so its length band is
    the band it always had and its four donor axes read ``none``. Filtering on
    a donor axis excludes it on its own, with no help from a counter.
    """
    if not residues:
        return None, "no_length"
    try:
        return (
            stratum_for(
                category=category,
                aspect=aspect,
                residues=residues,
                neighbourhood=hood if hood is not None else UNREAD_NEIGHBOURHOOD,
            ),
            None,
        )
    except ValueError:
        # A stored identity outside [0, 100] cannot be banded. Counted rather
        # than clamped: clamping would put the protein in a band nobody
        # measured it into.
        return None, "off_scale"


def _join(
    frame: Any,
    namespace: str,
    hoods: dict[str, Any],
    lengths: dict[str, int],
    setting: str,
) -> _Placed:
    """One row per protein in this panel, scored and placed.

    A protein that cannot be placed stays in the list with null bands. It
    scored, so it is part of the panel and dropping it would make the count
    printed here smaller than the count the panel prints; it just cannot answer
    the question, and a null says that where a zero or a band would not.

    :func:`_spot` decides each row and says why when it has no bands.
    """
    category = Category.from_code(setting)
    aspect = _ASPECT_FOR_NAMESPACE[namespace]

    taus = {round(float(t), 6) for t in frame["tau"].tolist()}
    rows: list[dict[str, Any]] = []
    unplaced = dict.fromkeys(_UNPLACED_REASONS, 0)
    no_donor = 0

    for record in frame.to_dict("records"):
        accession = str(record["protein_accession"])
        residues = lengths.get(accession)
        hood = hoods.get(accession)
        if hood is None:
            no_donor += 1
        stratum, why = _spot(category, aspect, residues, hood)
        if why is not None:
            unplaced[why] += 1
        rows.append(_row(record, accession, residues, stratum, hood))

    no_length = unplaced["no_length"]
    off_scale = unplaced["off_scale"]

    return _Placed(
        rows=rows,
        # One threshold per namespace is what the run reports. More than one
        # would mean the artefact mixes thresholds, and naming one of them
        # would attribute every row to a threshold it was not scored at.
        tau=taus.pop() if len(taus) == 1 else None,
        no_donor=no_donor,
        no_length=no_length,
        off_scale=off_scale,
    )


def _ordered(rows: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
    """Order the page, with absent values last under every ordering.

    A protein with no donor has no identity, and sorting it as if it had one of
    zero would put the retrieval failures at the head of a table about
    homology, where they read as the hardest cases rather than as the absent
    ones.
    """
    if sort == "accession":
        return sorted(rows, key=lambda r: r["accession"])
    field = "f" if sort.startswith("f_") else "best_identity"
    reverse = sort.endswith("_desc")
    # The accession is the last key under every ordering. Scores tie in bulk
    # here (every protein the run predicted nothing for scores exactly zero),
    # and without a tiebreak the order inside a tie is whatever the artefact
    # happened to hold, so page two could repeat a row from page one after the
    # cache was rebuilt.
    return sorted(
        rows,
        key=lambda r: (
            r[field] is None,
            -r[field] if (reverse and r[field] is not None) else (r[field] or 0.0),
            r["accession"],
        ),
    )


@dataclass
class _Page:
    """How much of the page to serve, and in what order.

    A dependency object rather than three loose query parameters, for the same
    reason :class:`_Where` is one: they are read together, they are validated
    together, and the endpoint's signature had seven arguments without them.
    """

    sort: str = Query(default="f_asc", description=f"One of: {', '.join(_SORTS)}.")
    limit: int = Query(default=100, ge=1, le=500, description="Rows on this page.")
    offset: int = Query(default=0, ge=0, description="Rows to skip.")


@dataclass
class _Where:
    """The cell to open, as query parameters."""

    setting: str = Query(
        default="NK",
        description=(
            "Knowledge setting, which is both the artefact directory and the "
            "category axis. One at a time: the three are different populations."
        ),
    )
    aspect: str = Query(description="GO aspect, in either spelling: BPO or P, MFO or F, CCO or C.")
    length: str | None = Query(
        default=None,
        description=(
            "Length band, or unset for the whole panel. A property of the "
            "sequence, so it is identical under every arm."
        ),
    )
    homology: str | None = Query(
        default=None,
        description=(
            "Identity band to the nearest non-self donor, or unset for the "
            "whole panel. A property of THIS arm's retrieval: K changes which "
            "donors come back, so the same protein can sit in different bands "
            "under two arms."
        ),
    )


@router.get(
    "/{evaluation_result_id}/proteins",
    summary="The proteins one panel is made of, with what each scored",
)
def stratum_proteins(
    evaluation_result_id: uuid.UUID,
    at: _Where = Depends(),
    page_of: _Page = Depends(),
    factory: sessionmaker[Session] = Depends(get_session_factory),
    settings: Any = Depends(get_settings),
) -> dict[str, Any]:
    """One page of a panel's proteins, and the counts that frame the page.

    The counts are four and they are deliberately not one. ``panel_population``
    is every protein the artefact scored in this cell, which is the number the
    panel prints. ``placed`` is how many of them could be put on the retrieval
    axes. ``matched`` is how many survive the bands the caller pinned.
    ``returned`` is how many are on this page. A single total would collapse a
    retrieval gap, a filter and a page size into one number that answers none
    of the three questions.
    """
    cell = _validated_cell(at, page_of.sort)
    setting, namespace = cell
    try:
        store = get_artifact_store(settings)
    except ArtifactStoreUnavailable as exc:  # pragma: no cover - config failure
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    with factory() as session:
        arm = drops.arm_for_an_openable_cell(
            session, _ARM, evaluation_result_id, setting, namespace)
    prediction_set_id = arm["prediction_set_id"]

    key, best_f_key = _panel_keys(store, evaluation_result_id, setting)

    def produce() -> _Placed:

        frame = _sliced_at_the_published_tau(
            io.BytesIO(store.get(key)),
            store.get(best_f_key).decode(),
            namespace,
            result_id=str(evaluation_result_id),
            setting=setting,
        )
        accessions = frame["protein_accession"].unique().tolist()
        with factory() as session:
            lengths = {
                r["accession"]: int(r["residues"])
                for r in session.execute(_LENGTHS, {"accessions": accessions}).mappings()
            }
        return _join(frame, namespace, _hoods(factory, prediction_set_id), lengths, setting)

    placed = cached(
        f"stratum:proteins:{evaluation_result_id}:{setting}:{namespace}",
        _TTL,
        produce,
        serve_stale_on_error=True,
    )

    rows = placed.rows
    if at.length is not None:
        rows = [r for r in rows if r["length_band"] == at.length]
    if at.homology is not None:
        rows = [r for r in rows if r["homology_band"] == at.homology]

    page = _ordered(rows, page_of.sort)[page_of.offset : page_of.offset + page_of.limit]

    scored_zero = _scored_zero(rows)

    return _response(
        evaluation_result_id,
        arm,
        _Counted(placed, rows, page, scored_zero),
        cell,
        at,
        page_of,
    )
