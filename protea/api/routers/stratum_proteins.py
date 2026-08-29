"""The proteins behind one panel, each with what it scored and why it is there.

A panel says NK x BPO holds 1,509 proteins and that the leading level reaches
0.2652. Both numbers are pooled over the panel, so a reader who doubts one of
them has nowhere to go: the population is a count and the score is a micro sum,
and neither can be opened. This serves the rows underneath.

Where the rows come from. ``run_cafa_evaluation`` writes one
``<setting>/per_protein.parquet`` beside each result, holding the per-protein
precision, recall and F at the threshold the run reported. Its rows for one
namespace under one setting are the panel's population: the category is the
directory and the aspect is the column, so nothing has to be joined and nothing
can drift out of a join.

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
import uuid
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached
from protea.api.deps import get_session_factory, get_settings
from protea.api.routers._arm_identity import with_arm_identity
from protea.core.domain.category import Category
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.core.operations._run_cafa_strata import neighbourhoods_for

# The namespace-to-aspect map is imported from the operation that WRITES the
# strata rather than restated here. A second spelling of it would be a second
# thing to keep in sync, and the failure it would produce is silent: a panel
# opened under one map and pooled under another disagrees about which proteins
# it holds without either side raising.
from protea.core.operations.stratify_evaluation import _ASPECT_FOR_NAMESPACE
from protea.core.strata import stratum_for
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
           ps.limit_per_entry                  AS depth,
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

    ``unplaced`` is broken down rather than totalled. A protein missing from
    this cell because no donor was retrieved is a retrieval fact; one missing
    because the record holds no length is a corpus fact; one missing because a
    stored identity is not a percentage is a defect. Summing the three would
    report a single number that three different actions would fix.
    """

    rows: list[dict[str, Any]]
    tau: float | None
    no_donor: int
    no_length: int
    off_scale: int

    @property
    def placed(self) -> int:
        return sum(1 for r in self.rows if r["length_band"] is not None)


def _join(
    frame: Any,
    namespace: str,
    hoods: dict[str, Any],
    lengths: dict[str, int],
    setting: str,
) -> _Placed:
    """One row per protein in this panel, scored and placed.

    A protein that cannot be placed on the retrieval axes stays in the list
    with null bands. It scored, so it is part of the panel and dropping it
    would make the count printed here smaller than the count the panel prints;
    it just cannot answer a question about length or homology, and a null says
    that where a zero or a band would not.
    """
    category = Category.from_code(setting)
    aspect = _ASPECT_FOR_NAMESPACE[namespace]

    taus = {round(float(t), 6) for t in frame["tau"].tolist()}
    rows: list[dict[str, Any]] = []
    no_donor = no_length = off_scale = 0

    for record in frame.to_dict("records"):
        accession = str(record["protein_accession"])
        residues = lengths.get(accession)
        hood = hoods.get(accession)
        stratum = None
        if hood is None:
            no_donor += 1
        elif not residues:
            no_length += 1
        else:
            try:
                stratum = stratum_for(
                    category=category, aspect=aspect, residues=residues, neighbourhood=hood
                )
            except ValueError:
                # A stored identity outside [0, 100] cannot be banded. Counted
                # rather than clamped: clamping would put the protein in a band
                # nobody measured it into.
                off_scale += 1
        rows.append(
            {
                "accession": accession,
                "residues": residues,
                "length_band": None if stratum is None else str(stratum.length),
                "homology_band": None if stratum is None else str(stratum.homology),
                "donor_evidence": None if stratum is None else str(stratum.donor_evidence),
                "best_identity": None if hood is None else hood.best_identity,
                "donor_is_experimental": None if hood is None else hood.donor_is_experimental,
                "taxonomic_relation": None if hood is None else hood.taxonomic_relation,
                # Read from the artefact, never recomputed. The panel's score is
                # a micro sum over exactly these three columns.
                "precision_w": float(record["precision_w"]),
                "recall_w": float(record["recall_w"]),
                "f_w": float(record["f_w"]),
                "n_gt_w": float(record["n_gt_w"]),
                "pred_w": float(record["pred_w"]),
                "tp_w": float(record["tp_w"]),
            }
        )

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
    field = "f_w" if sort.startswith("f_") else "best_identity"
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
    sort: str = Query(default="f_asc", description=f"One of: {', '.join(_SORTS)}."),
    limit: int = Query(default=100, ge=1, le=500, description="Rows on this page."),
    offset: int = Query(default=0, ge=0, description="Rows to skip."),
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
    if at.setting.upper() not in _SETTINGS:
        raise HTTPException(
            status_code=422,
            detail=f"setting {at.setting!r} is not one of {', '.join(_SETTINGS)}",
        )
    setting = at.setting.upper()
    if sort not in _SORTS:
        raise HTTPException(
            status_code=422, detail=f"sort {sort!r} is not one of {', '.join(_SORTS)}"
        )
    namespace = _namespace_for(at.aspect)

    try:
        store = get_artifact_store(settings)
    except ArtifactStoreUnavailable as exc:  # pragma: no cover - config failure
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    with factory() as session:
        arm = session.execute(_ARM, {"rid": evaluation_result_id}).mappings().first()
    if arm is None:
        raise HTTPException(status_code=404, detail=f"no evaluation result {evaluation_result_id}")
    prediction_set_id = arm["prediction_set_id"]
    if prediction_set_id is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"evaluation result {evaluation_result_id} names no prediction set, "
                "so its proteins cannot be placed on the retrieval axes"
            ),
        )

    # Existence is checked before the cache, not inside the producer. A missing
    # artefact is a 404 and has to stay one: raised inside the producer it would
    # be swallowed the moment a stale page for another cell was in the store.
    key = eval_artifact_key(evaluation_result_id, f"{setting}/per_protein.parquet")
    if not store.exists(key):
        raise HTTPException(
            status_code=404,
            detail=(
                f"no {setting}/per_protein.parquet for evaluation result "
                f"{evaluation_result_id}; this run wrote no per-protein scores for "
                f"this setting, so its panels cannot be opened"
            ),
        )

    def produce() -> _Placed:
        import pandas as pd

        frame = pd.read_parquet(io.BytesIO(store.get(key)))
        frame = frame[frame["namespace"] == namespace]
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

    page = _ordered(rows, sort)[offset : offset + limit]

    # How many of the cell the run scored nothing at all on. A count and not an
    # average, so it says nothing the pooled cell already says: a cell reading
    # 0.09 over 375 proteins is one claim when the mass is spread and a
    # different one when two thirds of it is exactly zero, and the pooled
    # number cannot tell them apart. It is also what makes the default
    # ordering readable: a page of zeros is a finding once the reader knows
    # how many there are, and a mystery until then.
    scored_zero = sum(1 for r in rows if r["f_w"] == 0.0)

    return {
        "evaluation_result_id": str(evaluation_result_id),
        "arm": {
            "prediction_set_id": prediction_set_id,
            "embedding_name": arm["embedding_name"],
            "scoring_name": arm["scoring_name"],
            "donor_policy": arm["donor_policy"],
            "depth": arm["depth"],
            "metric": arm["metric"],
        },
        "setting": setting,
        "where": {
            "category": setting,
            "aspect": _ASPECT_FOR_NAMESPACE[namespace].cafa,
            "length": at.length,
            "homology": at.homology,
        },
        "tau": placed.tau,
        "panel_population": len(placed.rows),
        "placed": placed.placed,
        "unplaced": {
            "no_donor": placed.no_donor,
            "no_length": placed.no_length,
            "off_scale": placed.off_scale,
        },
        "matched": len(rows),
        "scored_zero": scored_zero,
        "returned": len(page),
        "offset": offset,
        "limit": limit,
        "sort": sort,
        "proteins": page,
    }
