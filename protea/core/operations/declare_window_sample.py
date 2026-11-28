"""Declare a stratified sample of a window's targets.

The units are the window's benchmark pairs, every target in every aspect where it
gained terms, read from the window itself: the category of the pair (no, limited or
partial knowledge) from the window's truth, fetched from the store and verified
against the sha256 its row records, with each gained term's aspect from the window's
pivot; and the pair's stratum from the window's placement (``place_window_targets``),
the identity band of the closest truth member (a twin is 100, no homolog is the
``none`` band) and the length band of the target's sequence at the window's start.
The rule (:mod:`protea.core.sampling.window_sample`) draws the targets in hash order
from the payload's seed.

The set is a ``window_sample`` query set of the window: its entries are the targets
with their sequence at the window's opening cut, its artifact the sorted accession
list, one per line (its sha256 is the population digest of
:func:`protea.core.query_population.population_of`), and beside it in the store the
allocation table, per cell and stratum: the units, the quota, the units selected and
the units whose protein is a target. The description names the window, the rule, the
seed, the quotas and the realised counts.

It refuses before writing anything when the window does not exist, has no placement,
records no sha256 for its truth, names a term its pivot does not hold, or holds a
target its placement does not, when a target has no sequence at the opening cut, and
when the window already has a set with the same bytes. ``dry_run`` returns the
allocation table and the targets it would declare, and writes nothing, so the size
of a sample is reached by dry runs and only the final one is declared.
"""

from __future__ import annotations

import json
import tempfile
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pyarrow.parquet as pq
from pydantic import Field, field_validator
from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import fetch_groundtruth_to_file
from protea.core.operations.predict_go_terms._photo_sequences import photo_sequences
from protea.core.query_population import population_of
from protea.core.sampling.window_sample import Unit, WindowSampleDraw, draw_window_sample
from protea.core.strata import homology_band_for, length_band_for
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.window_placement import (
    WindowPlacement,
    WindowPlacementTarget,
)
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

__all__ = [
    "DeclareWindowSampleOperation",
    "DeclareWindowSamplePayload",
    "WindowSampleRefused",
    "benchmark_pairs",
    "window_sample_key_for",
]

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

#: The buckets of a window's truth that hold its benchmark pairs, by category.
SCORED_BUCKETS: dict[str, str] = {"nk": "NK", "lk": "LK", "pk": "PK"}


class WindowSampleRefused(ValueError):
    """A window sample the operation will not declare, with the reason."""


class DeclareWindowSamplePayload(ProteaPayload, frozen=True):
    """The window to sample, the rule, its quota per cell and its seed, and what to call the set."""

    evaluation_set_id: str
    units_per_cell: int = Field(ge=1)
    seed: str = Field(min_length=1)
    rule: Literal["equal_cells_proportional_strata"]
    dry_run: bool = False
    output_name: str

    @field_validator("evaluation_set_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @field_validator("output_name")
    @classmethod
    def a_name_a_reader_can_use(cls, value: str) -> str:
        name = value.strip()
        if not name or "/" in name:
            raise ValueError(
                "output_name is a non-empty name without '/': the store key is derived"
            )
        return name

    @property
    def window_id(self) -> uuid.UUID:
        return uuid.UUID(self.evaluation_set_id)


@dataclass(frozen=True)
class _Drawn:
    """A draw, the bytes that declare it and each target's sequence id at the opening cut."""

    draw: WindowSampleDraw
    body: bytes
    digest: str
    sequence_ids: dict[str, int]


def window_sample_key_for(window_id: uuid.UUID, digest: str, name: str) -> str:
    """Where a sample's files live in the store: ``accessions.txt`` and ``allocation.json``."""
    return f"query-sets/window-samples/{window_id}/{digest}/{name}"


def publish(key: str, blob: bytes) -> str:
    """Put the bytes in the store; return their URI."""
    return str(get_artifact_store(load_settings(_PROJECT_ROOT)).put(key, blob))


def _scratch_root() -> Path:
    root = load_settings(_PROJECT_ROOT).artifacts_dir / "window_sample"
    root.mkdir(parents=True, exist_ok=True)
    return root


def scored_truth(window: EvaluationSet) -> Iterator[tuple[str, str, str]]:
    """``(accession, go_id, category)`` of every gained term of the window's truth.

    Only the three scored buckets are read from the verified parquet; ``known`` and
    the rest never leave the file.
    """
    with tempfile.TemporaryDirectory(dir=_scratch_root()) as scratch:
        path = fetch_groundtruth_to_file(window, Path(scratch) / "groundtruth.parquet")
        table = pq.read_table(
            path,
            columns=["protein_accession", "go_id", "bucket"],
            filters=[("bucket", "in", list(SCORED_BUCKETS))],
        )
    for accession, go_id, bucket in zip(
        table.column("protein_accession").to_pylist(),
        table.column("go_id").to_pylist(),
        table.column("bucket").to_pylist(),
        strict=True,
    ):
        yield str(accession), str(go_id), SCORED_BUCKETS[str(bucket)]


def _aspects(session: Session, pivot_id: uuid.UUID) -> dict[str, str]:
    rows = session.execute(
        select(GOTerm.go_id, GOTerm.aspect).where(GOTerm.ontology_snapshot_id == pivot_id)
    ).all()
    return {go_id: aspect for go_id, aspect in rows if aspect}


def _placed(session: Session, placement: WindowPlacement) -> dict[str, tuple[str, str]]:
    """Each placed target's identity band and length band."""
    rows = session.execute(
        select(
            WindowPlacementTarget.accession,
            WindowPlacementTarget.closest_identity,
            WindowPlacementTarget.residues,
        ).where(WindowPlacementTarget.placement_id == placement.id)
    ).all()
    return {
        accession: (homology_band_for(identity).value, length_band_for(residues).value)
        for accession, identity, residues in rows
    }


def benchmark_pairs(
    window: EvaluationSet,
    truth: Iterator[tuple[str, str, str]],
    aspects: dict[str, str],
    placed: dict[str, tuple[str, str]],
) -> list[Unit]:
    """The window's benchmark pairs as sampling units, refusing what cannot be placed.

    A pair is one target in one aspect where it gained terms, with the category of the
    bucket that holds those terms; a pair held by two buckets is refused, since a
    category is one per target and aspect.
    """
    category_of: dict[tuple[str, str], str] = {}
    no_aspect: set[str] = set()
    for accession, go_id, category in truth:
        aspect = aspects.get(go_id)
        if aspect is None:
            no_aspect.add(go_id)
            continue
        if category_of.setdefault((accession, aspect), category) != category:
            raise WindowSampleRefused(
                f"target {accession} is in two categories in aspect {aspect} of window "
                f"{window.id}: its truth is not one the evaluator reads"
            )
    if no_aspect:
        raise WindowSampleRefused(
            f"{len(no_aspect)} terms of the truth of window {window.id} are not in its pivot "
            f"(first {sorted(no_aspect)[:3]}), so their aspect cannot be read"
        )
    unplaced = sorted({accession for accession, _ in category_of} - set(placed))
    if unplaced:
        raise WindowSampleRefused(
            f"{len(unplaced)} targets of window {window.id} are not in its placement "
            f"(first {unplaced[:3]}): the placement is not this truth's"
        )
    return [
        Unit(accession, aspect, category, *placed[accession])
        for (accession, aspect), category in sorted(category_of.items())
    ]


def _drawn(session: Session, window: EvaluationSet, p: DeclareWindowSamplePayload) -> _Drawn:
    placement = session.execute(
        select(WindowPlacement).where(WindowPlacement.evaluation_set_id == window.id)
    ).scalar_one_or_none()
    if placement is None:
        raise WindowSampleRefused(
            f"window {window.id} has no placement: its strata are read from it, so place its "
            "targets first (place_window_targets)"
        )
    units = benchmark_pairs(
        window,
        scored_truth(window),
        _aspects(session, window.pivot_snapshot_id),
        _placed(session, placement),
    )
    draw = draw_window_sample(
        units, units_per_cell=p.units_per_cell, seed=p.seed, window_id=str(window.id), rule=p.rule
    )
    sequences = photo_sequences(session, placement.cut_photo_id, list(draw.targets))
    missing = sorted(set(draw.targets) - set(sequences))
    if missing:
        raise WindowSampleRefused(
            f"{len(missing)} sampled targets have no sequence at the window's opening cut "
            f"(first {missing[:3]})"
        )
    return _Drawn(
        draw=draw,
        body="".join(f"{accession}\n" for accession in draw.targets).encode(),
        digest=population_of(draw.targets)["sha256"],
        sequence_ids={accession: sequence_id for accession, (sequence_id, _) in sequences.items()},
    )


def describe(window: EvaluationSet, drawn: _Drawn) -> str:
    """The set's description, derived from the draw, never typed."""
    draw = drawn.draw
    cells = "; ".join(
        f"{cell.category} {cell.aspect} {cell.quota:,} of {cell.population:,}, "
        f"realised {cell.realised:,}"
        for cell in draw.cells
    )
    return (
        f"A stratified sample of {len(draw.targets):,} targets of window {window.id}, drawn "
        f"by rule {draw.rule} with seed '{draw.seed}' and {draw.units_per_cell:,} units per "
        "cell: in each cell of category and aspect, its quota of benchmark pairs over the "
        "nonempty strata of identity band and length band, in ascending "
        "sha256(seed|window|accession); a target keeps every cell it has. Per cell, quota "
        f"of units, then realised: {cells}. The artifact is the sorted accession list, one "
        f"per line, sha256 {drawn.digest}, with its allocation table beside it."
    )


def _declare(
    session: Session,
    window: EvaluationSet,
    p: DeclareWindowSamplePayload,
    drawn: _Drawn,
    job_id: uuid.UUID | None,
) -> QuerySet:
    allocation = json.dumps(drawn.draw.allocation(), indent=1, sort_keys=True).encode()
    publish(window_sample_key_for(window.id, drawn.digest, "allocation.json"), allocation)
    uri = publish(window_sample_key_for(window.id, drawn.digest, "accessions.txt"), drawn.body)
    row = QuerySet(
        name=f"{p.output_name}-{drawn.digest[:12]}",
        source_kind="window_sample",
        evaluation_set_id=window.id,
        description=describe(window, drawn),
        artifact_uri=uri,
        content_sha256=drawn.digest,
        job_id=job_id,
        code_revision=code_revision(),
    )
    session.add(row)
    session.flush()
    session.execute(
        insert(QuerySetEntry),
        [
            {
                "query_set_id": row.id,
                "sequence_id": drawn.sequence_ids[accession],
                "accession": accession,
            }
            for accession in drawn.draw.targets
        ],
    )
    return row


class DeclareWindowSampleOperation:
    name = "declare_window_sample"
    description = (
        "Declare a stratified sample of a window's targets: each cell of category and "
        "aspect gets its quota of benchmark pairs, spread over the strata of identity band "
        "and length band and taken in hash order from a seed, and the targets are the "
        "union of their proteins. Read from the window's verified truth and its "
        "placement. dry_run returns the allocation table and writes nothing."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        window = str(payload.get("evaluation_set_id") or "")[:8]
        dry = " (dry run)" if payload.get("dry_run") else ""
        quota = payload.get("units_per_cell")
        return f"sample of window {window}, {quota} units per cell{dry}" if window else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclareWindowSamplePayload.model_validate(contract_payload(payload))
        window = session.get(EvaluationSet, p.window_id)
        if window is None:
            raise WindowSampleRefused(f"no window {p.window_id}")
        drawn = _drawn(session, window, p)
        existing = session.execute(
            select(QuerySet.id).where(
                QuerySet.evaluation_set_id == window.id, QuerySet.content_sha256 == drawn.digest
            )
        ).scalar_one_or_none()
        if existing is not None and not p.dry_run:
            raise WindowSampleRefused(
                f"window {window.id} already has these targets declared as query set "
                f"{existing} (sha256 {drawn.digest}): one window and one set of bytes are one set"
            )
        result: dict[str, Any] = {
            "evaluation_set_id": str(window.id),
            "targets": len(drawn.draw.targets),
            "content_sha256": drawn.digest,
            "allocation": drawn.draw.allocation(),
            "dry_run": p.dry_run,
        }
        if p.dry_run:
            result["already_declared"] = str(existing) if existing else None
            emit(
                "window_sample.counted",
                f"{len(drawn.draw.targets):,} targets (dry run)",
                result,
                "info",
            )
            return OperationResult(result=result)
        row = _declare(session, window, p, drawn, job_id_from_payload(payload))
        result.update(query_set_id=str(row.id), name=row.name, artifact_uri=row.artifact_uri)
        emit(
            "window_sample.declared",
            f"{len(drawn.draw.targets):,} targets as {row.name}",
            result,
            "info",
        )
        return OperationResult(result=result)
