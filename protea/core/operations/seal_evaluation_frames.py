# protea/core/operations/seal_evaluation_frames.py
"""Stamp each stored result with the frame it was produced under.

WHY THIS EXISTS. ``evaluation_result.frame`` is nullable, has no default, and
nothing in the platform writes it. Every result of this campaign carries null,
and so did every result of the one before: the census in
``audit_evaluation_frames`` found the column empty across the board and 396 rows
of 1,296 whose producing job was gone, which made them unattributable for ever.

A number that cannot be attributed cannot be compared. That is not a slogan
about tidiness: two results differing only in which accretion table weighted
their terms move by up to 0.0185 of weighted micro F, which is more than the
distance cap moves them and as much as layer depth. Reading them side by side is
reading two quantities under one name.

WHY A DIGEST AND NOT A LABEL. The column was introduced to hold ``lafa`` or
``internal``, and the audit operation says in its own docstring why that is not
enough: it marks which harness, not which parameters, so two rows both labelled
``lafa`` can still be incomparable. Writing a harness name into eight rows would
turn the surface green while changing nothing about what the numbers mean. So
the seal is a digest of the fields that have to match for two results to be
comparable, and the fields it covers are named in the emitted event rather than
left for a reader to infer.

WHAT IT REFUSES. A row whose frame cannot be recovered is not sealed, and the
operation says how many it left alone. A row already sealed under a different
digest is not overwritten, because a seal that changes silently is worse than no
seal: it would let a reader trust an attribution that had moved underneath them.
Both refusals are counted and reported.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.provenance import capture_provenance

PositiveInt = Annotated[int, Field(gt=0)]

#: The fields that must match for two results to be measuring the same thing.
#:
#: The window fixes which annotations count as gained. The pivot fixes the graph
#: they are expressed in, and it is not implied by the window: the same window
#: reconciled under a different pivot moves whole panels, which is what the
#: phantom gap turned out to be. The accretion set fixes what weights the terms.
#: The excluded-known base fixes whether a prior-knowledge cell scores what the
#: protein already had.
#:
#: The scoring preset is deliberately NOT here. It is a level, not a frame: the
#: frame is what is held still while levels vary, and folding a level into it
#: would give every level its own frame and make the seal say nothing.
_FRAME_FIELDS: tuple[str, ...] = (
    "evaluation_set_id",
    "pivot_snapshot_id",
    "information_accretion_set_id",
    "temporal_window",
    "max_terms",
    "max_distance",
)

#: Rows and the provenance each one can reach. ``information_accretion_set_id``
#: lives only in the producing job's payload: no column records it, which is why
#: a row without a job cannot be sealed however much else survives.
_ROWS = text("""
    SELECT r.id::text                                     AS id,
           r.frame_digest                                 AS frame,
           r.evaluation_set_id::text                       AS evaluation_set_id,
           r.temporal_window                              AS temporal_window,
           es.stats ->> 'pivot_ontology_snapshot_id'      AS pivot_snapshot_id,
           j.payload ->> 'information_accretion_set_id'   AS information_accretion_set_id,
           j.payload ->> 'max_terms'                      AS max_terms,
           j.payload ->> 'max_distance'                   AS max_distance,
           (j.id IS NOT NULL)                             AS has_job
    FROM evaluation_result r
    LEFT JOIN evaluation_set es ON es.id = r.evaluation_set_id
    LEFT JOIN job j             ON j.id = r.job_id
    ORDER BY r.created_at
""")

#: Width of the seal.
#:
#: The seal goes in ``frame_digest`` and not in ``frame``. ``frame`` is
#: ``varchar(8)`` under a check constraint admitting exactly ``lafa`` or
#: ``internal``: the schema settles what that column is for, and it is a harness
#: label. Writing one of its two permitted values into every row would turn the
#: surface green while changing nothing about what the numbers mean, which is
#: the failure this operation exists to end rather than to commit.
_DIGEST_WIDTH = 24

_SEAL = text("UPDATE evaluation_result SET frame_digest = :frame WHERE id = CAST(:id AS uuid)")

#: Store the material beside the address.
#:
#: A digest answers whether two results are comparable and nothing else. Given
#: one alone, nobody can say which window or which accretion table it stands
#: for, and recovering it means recomputing from rows that may be gone. The
#: address without its contents is the same defect the address was introduced to
#: fix, one level along.
#:
#: ON CONFLICT DO NOTHING and not an update: the key is derived from the
#: material, so a second write of different material under one key is a
#: collision or a producer bug. Silently overwriting would make a frame mean
#: something new while every row already sealed under it kept pointing there.
_RECORD_FRAME = text("""
    INSERT INTO evaluation_frame (digest, material, fields, provenance)
    VALUES (:digest, CAST(:material AS jsonb), CAST(:fields AS jsonb),
            CAST(:provenance AS jsonb))
    ON CONFLICT (digest) DO NOTHING
""")


def frame_digest(row: dict[str, Any]) -> str | None:
    """The seal for one row, or nothing when its frame cannot be recovered.

    Content addressed, so the same frame always yields the same seal and two
    results can be compared by string equality without anyone having to know
    which fields the frame is made of. Absent optional values are encoded as
    null rather than skipped, so a run that omitted a cap and a run that set it
    to the same value the default happens to take do not collide.

    Returns nothing when a field that changes the meaning of the number is
    missing. The evaluation set and the accretion set are both required for
    that reason: without the first the population is unknown, and without the
    second the weighting is.
    """
    if not row.get("evaluation_set_id") or not row.get("information_accretion_set_id"):
        return None
    material = {field: row.get(field) for field in _FRAME_FIELDS}
    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return "f-" + hashlib.sha256(canonical.encode()).hexdigest()[:_DIGEST_WIDTH]


class SealEvaluationFramesPayload(ProteaPayload, frozen=True):
    """Inputs for the seal.

    ``dry_run`` defaults to true. An operation that rewrites a provenance column
    should have to be asked twice, and the first answer is a report of exactly
    what the second would write.
    """

    dry_run: bool = True
    max_examples: PositiveInt = 20


class SealEvaluationFramesOperation(Operation):
    name = "seal_evaluation_frames"
    description = (
        "Stamp evaluation_result.frame with a content digest of the window, pivot, "
        "accretion set and evaluation caps each result was produced under. Refuses "
        "rows whose provenance is unrecoverable and never overwrites a differing "
        "seal. Defaults to a dry run."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """Say which of the two runs this is, because they differ in kind.

        The dry run reports what a stamping run would write and touches
        nothing; the second writes a provenance column. A reader scanning a job
        list has to tell them apart without opening the payload, which is the
        whole reason the operation asks to be called twice.
        """
        if payload.get("dry_run", True):
            return "dry run: report the frame digest each result would be stamped with"
        return "stamp evaluation_result.frame with the digest of its window, pivot and caps"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = SealEvaluationFramesPayload.model_validate(payload)
        rows = [dict(r) for r in session.execute(_ROWS).mappings().all()]
        emit(
            "seal.start",
            f"{len(rows)} stored results, sealing over {len(_FRAME_FIELDS)} frame fields",
            {"rows": len(rows), "fields": list(_FRAME_FIELDS), "dry_run": p.dry_run},
            "info",
        )

        sealed = 0
        already = 0
        conflicts: list[dict[str, str]] = []
        unattributable: list[dict[str, str]] = []
        digests: dict[str, int] = {}

        for row in rows:
            digest = frame_digest(row)
            if digest is None:
                if len(unattributable) < p.max_examples:
                    unattributable.append(
                        {
                            "id": row["id"],
                            "why": (
                                "no producing job, so the accretion set is unrecoverable"
                                if not row["has_job"]
                                else "the evaluation set or accretion set is missing"
                            ),
                        }
                    )
                continue

            digests[digest] = digests.get(digest, 0) + 1
            # Recorded for every row whose frame is computable, not only for
            # rows that still need the stamp. A first version wrote it inside
            # the branch that seals, so a re-run over rows already sealed
            # reported success and left the expansion empty: the operation had
            # nothing left to stamp and therefore never reached the insert. An
            # artefact that appears only on the first run of an idempotent
            # operation is one nobody will have when they need it.
            if not p.dry_run:
                session.execute(
                    _RECORD_FRAME,
                    {
                        "digest": digest,
                        "material": json.dumps(
                            {f: row.get(f) for f in _FRAME_FIELDS}, sort_keys=True
                        ),
                        "fields": json.dumps(list(_FRAME_FIELDS)),
                        "provenance": json.dumps(capture_provenance()),
                    },
                )
            current = row.get("frame")
            if current == digest:
                already += 1
                continue
            if current:
                if len(conflicts) < p.max_examples:
                    conflicts.append({"id": row["id"], "held": current, "computed": digest})
                continue
            if not p.dry_run:
                session.execute(_SEAL, {"frame": digest, "id": row["id"]})
            sealed += 1

        if not p.dry_run:
            session.commit()

        n_unattributable = sum(1 for r in rows if frame_digest(r) is None)
        if n_unattributable:
            emit(
                "seal.unattributable",
                f"{n_unattributable} results cannot be sealed and were left alone",
                {"count": n_unattributable, "examples": unattributable},
                "warning",
            )
        if conflicts:
            emit(
                "seal.conflict",
                f"{len(conflicts)} results hold a different seal and were not overwritten",
                {"examples": conflicts},
                "warning",
            )
        emit(
            "seal.done",
            (
                f"{sealed} {'would be' if p.dry_run else ''} sealed, {already} already correct, "
                f"{len(digests)} distinct frames"
            ),
            {"sealed": sealed, "already": already, "frames": digests},
            "info",
        )
        return OperationResult(
            result={
                "dry_run": p.dry_run,
                "rows": len(rows),
                "sealed": sealed,
                "already_sealed": already,
                "unattributable": n_unattributable,
                "conflicts": len(conflicts),
                "distinct_frames": len(digests),
                "frame_fields": list(_FRAME_FIELDS),
            }
        )
