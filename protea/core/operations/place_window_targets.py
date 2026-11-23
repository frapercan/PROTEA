"""Place each target of a window on the sequence axes, once, for every method to read.

The rule is :mod:`protea.core.window_placement`. This operation finds what the rule
reads and writes what it returns:

- the window's targets, the proteins of its truth in any knowledge category;
- the full photo of the window's opening cut, whose superset holds each target's
  sequence at that cut and whose ``truth`` members are the proteins measured there;
- the homology table of that cut (ADR-D61), whose hits it streams from the store,
  verified against the sha256 its row declares;
- each target sequence's length, from ``sequence`` by its md5.

It refuses before writing anything when one of them is missing: a window whose
opening cut has no full photo or no homology table, a target that is not a superset
member of that photo, or a target sequence with no length. A window has one
placement: the same window again is a no-op.
"""

from __future__ import annotations

import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy import String, any_, bindparam, func, insert, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_verified_to_file
from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.core.window_placement import (
    RULE,
    Placement,
    best_truth_hits,
    needs_a_hit,
    place,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from protea.infrastructure.orm.models.annotation.window_placement import (
    WindowPlacement,
    WindowPlacementTarget,
)
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _any_of(name: str, values: set[str]) -> Any:
    """One array parameter rather than one parameter per value: a window has up to
    about 44,000 targets, and Postgres takes at most 65,535 parameters a statement."""
    return any_(bindparam(name, value=sorted(values), type_=ARRAY(String)))


class WindowPlacementRefused(SourceGuardError):
    """A placement the operation will not write, with the reason."""


class PlaceWindowTargetsPayload(ProteaPayload, frozen=True):
    """The window to place; everything else is read from it."""

    evaluation_set_id: str

    @field_validator("evaluation_set_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))


def opening_photo(session: Session, window: EvaluationSet) -> CutPhoto:
    """The full photo of the cut that opens the window: the one on its start annotation set."""
    photos = (
        session.execute(
            select(CutPhoto).where(
                CutPhoto.annotation_set_id == window.old_annotation_set_id,
                CutPhoto.kind == "full",
            )
        )
        .scalars()
        .all()
    )
    if len(photos) != 1:
        raise WindowPlacementRefused(
            f"window {window.id} opens on annotation set {window.old_annotation_set_id}, which "
            f"has {len(photos)} full photos; a placement reads exactly one. Load the photo of "
            "the opening cut first (load_cut_photo)."
        )
    return photos[0]


def homology_table_of(session: Session, photo: CutPhoto) -> HomologyTable:
    table = session.execute(
        select(HomologyTable).where(HomologyTable.cut_photo_id == photo.id)
    ).scalar_one_or_none()
    if table is None:
        raise WindowPlacementRefused(
            f"the photo of cut {photo.cut_release} has no homology table; load it first "
            "(load_homology_table, ADR-D61)."
        )
    return table


def window_targets(session: Session, window: EvaluationSet) -> set[str]:
    """Every protein of the window's truth, in any knowledge category."""
    from protea.core.evaluation import load_evaluation_data_for_set

    data, _ = load_evaluation_data_for_set(session, window)
    return set(data.nk) | set(data.lk) | set(data.pk)


def target_sequences(session: Session, photo: CutPhoto, targets: set[str]) -> dict[str, str]:
    """Each target's sequence md5 in the photo, refusing a target that is not in its superset."""
    rows = session.execute(
        select(CutPhotoMember.accession, CutPhotoMember.sequence_md5).where(
            CutPhotoMember.photo_id == photo.id,
            CutPhotoMember.superset.is_(True),
            CutPhotoMember.accession == _any_of("targets", targets),
        )
    ).all()
    found = {accession: md5 for accession, md5 in rows if md5}
    missing = sorted(targets - set(found))
    if missing:
        raise WindowPlacementRefused(
            f"{len(missing)} targets have no sequence in the superset of the photo of cut "
            f"{photo.cut_release} (first {missing[:3]}); a target is reviewed at the window's "
            "start, so this photo is not the window's."
        )
    return found


def truth_accessions(session: Session, photo: CutPhoto) -> dict[str, frozenset[str]]:
    """The accessions of the photo's members with a truth row, by sequence md5."""
    rows = session.execute(
        select(CutPhotoMember.sequence_md5, CutPhotoMember.accession).where(
            CutPhotoMember.photo_id == photo.id,
            CutPhotoMember.truth.is_(True),
            CutPhotoMember.sequence_md5.is_not(None),
        )
    ).all()
    by_md5: dict[str, set[str]] = defaultdict(set)
    for md5, accession in rows:
        by_md5[md5].add(accession)
    return {md5: frozenset(accessions) for md5, accessions in by_md5.items()}


def sequence_lengths(session: Session, md5s: set[str]) -> dict[str, int]:
    """Each sequence's length by its md5, refusing one the store does not hold."""
    rows = session.execute(
        select(Sequence.sequence_hash, func.length(Sequence.sequence)).where(
            Sequence.sequence_hash == _any_of("md5s", md5s)
        )
    ).all()
    lengths = {md5: int(length) for md5, length in rows}
    missing = sorted(md5s - set(lengths))
    if missing:
        raise WindowPlacementRefused(
            f"{len(missing)} target sequences have no row in sequence (first {missing[:3]}), "
            "so their length cannot be read."
        )
    return lengths


def fetch_hits(table: HomologyTable, scratch: Path) -> Path:
    """The cut's hits on local disk, verified against the sha256 the table's row declares."""
    store = get_artifact_store(load_settings(_PROJECT_ROOT))
    return fetch_verified_to_file(
        store,
        table.hits_uri,
        table.hits_sha256,
        scratch / "hits.parquet",
        f"the homology hits of cut {table.cut_release}",
    )


def _scratch_root() -> Path:
    root = load_settings(_PROJECT_ROOT).artifacts_dir / "window_placement"
    root.mkdir(parents=True, exist_ok=True)
    return root


def descriptors(
    window: EvaluationSet, photo: CutPhoto, closing: str, placed: list[Placement]
) -> tuple[str, str]:
    """ADR-D58's short and long descriptors, derived from the window, never typed."""
    twins = sum(1 for p in placed if p.closest_by == "twin")
    hits = sum(1 for p in placed if p.closest_by == "hit")
    cut = photo.cut_release
    return f"placement-{cut}-{closing}-{str(window.id)[:8]}", (
        f"The placement of the {len(placed):,} targets of window {window.id} (cut {cut} -> "
        f"{closing}) on the sequence axes, read at its start: each target's length in the "
        f"photo of cut {cut}, and its identity to the closest other protein of that photo "
        f"with a truth row. That is an identical sequence under another accession for "
        f"{twins:,} targets, the best hit by bit score in the homology table of cut {cut} "
        f"for {hits:,}, and nothing for {len(placed) - twins - hits:,}."
    )


def _rows(placement_id: uuid.UUID, placed: list[Placement]) -> list[dict[str, Any]]:
    return [{"placement_id": placement_id, **p._asdict()} for p in placed]


def placement_of(
    session: Session, window: EvaluationSet
) -> tuple[list[Placement], CutPhoto, HomologyTable]:
    """Read what the rule reads for one window, apply it, and say where it was read from."""
    photo = opening_photo(session, window)
    table = homology_table_of(session, photo)
    targets = target_sequences(session, photo, window_targets(session, window))
    truth = truth_accessions(session, photo)
    lengths = sequence_lengths(session, set(targets.values()))
    with tempfile.TemporaryDirectory(dir=_scratch_root()) as scratch:
        hits_path = fetch_hits(table, Path(scratch))
        best = best_truth_hits(hits_path, needs_a_hit(targets, truth), truth)
    return place(targets, lengths, truth, best), photo, table


def declaration(
    session: Session,
    window: EvaluationSet,
    read: tuple[list[Placement], CutPhoto, HomologyTable],
    job_id: uuid.UUID | None,
) -> WindowPlacement:
    """The ``window_placement`` row for a placement: its sources, counts and descriptors."""
    placed, photo, table = read
    closing = session.get(AnnotationSet, window.new_annotation_set_id)
    short_name, description = descriptors(
        window, photo, str(getattr(closing, "source_version", None) or "?"), placed
    )
    twins = sum(1 for x in placed if x.closest_by == "twin")
    hits = sum(1 for x in placed if x.closest_by == "hit")
    return WindowPlacement(
        evaluation_set_id=window.id,
        cut_photo_id=photo.id,
        homology_table_id=table.id,
        rule=RULE,
        targets=len(placed),
        targets_with_twin=twins,
        targets_with_hit=hits,
        targets_without_homolog=len(placed) - twins - hits,
        short_name=short_name,
        description=description,
        code_revision=code_revision(),
        job_id=job_id,
    )


class PlaceWindowTargetsOperation:
    """Write one window's placement on the length and identity axes."""

    name = "place_window_targets"
    description = (
        "Place each target of a window on the sequence axes as it stood when the window "
        "opened: its length, and its identity to the closest protein measured at that cut, "
        "read from the cut's photo and homology table. Every method's strata read it."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        window = str(payload.get("evaluation_set_id", ""))[:8]
        return f"placement of window {window}" if window else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PlaceWindowTargetsPayload.model_validate(contract_payload(payload))
        window = session.get(EvaluationSet, uuid.UUID(p.evaluation_set_id))
        if window is None:
            raise WindowPlacementRefused(f"no window {p.evaluation_set_id}")
        existing = session.execute(
            select(WindowPlacement).where(WindowPlacement.evaluation_set_id == window.id)
        ).scalar_one_or_none()
        if existing is not None:
            emit(
                "place_window_targets.already_placed",
                None,
                {"placement_id": str(existing.id)},
                "info",
            )
            return OperationResult(result={"placement_id": str(existing.id), "skipped": True})
        read = placement_of(session, window)
        row = declaration(session, window, read, job_id_from_payload(payload))
        session.add(row)
        session.flush()
        session.execute(insert(WindowPlacementTarget), _rows(row.id, read[0]))
        result = {
            "placement_id": str(row.id),
            "evaluation_set_id": str(window.id),
            "cut": read[1].cut_release,
            "targets": row.targets,
            "targets_with_twin": row.targets_with_twin,
            "targets_with_hit": row.targets_with_hit,
            "targets_without_homolog": row.targets_without_homolog,
            "short_name": row.short_name,
        }
        emit("place_window_targets.done", None, result, "info")
        return OperationResult(result=result)


__all__ = [
    "PlaceWindowTargetsOperation",
    "PlaceWindowTargetsPayload",
    "WindowPlacementRefused",
]
