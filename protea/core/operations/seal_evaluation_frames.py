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
from dataclasses import dataclass, field
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.provenance import capture_provenance
from protea.core.utils import contract_payload

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
#:
#: The last three were missing, and their absence was not hypothetical. Three
#: stored results share all six of the fields above -- one window, one pivot,
#: one accretion set, all three caps null -- and sealed to one digest,
#: ``f-2ccee8875ed1f2869baea769``. One of them ran with
#: ``restrict_gt_to_predicted`` at its default and the other two with it off,
#: so one scored the predicted cohort and two scored the whole ground truth.
#: Two of those rows carry 107 proteins. The seal called all three comparable.
#:
#: Each of the three changes the number without being any part of the method:
#:
#: * ``restrict_gt_to_predicted`` decides the population. It is the cohort, and
#:   a cohort difference read through a seal that cannot see it is published as
#:   a method difference.
#: * ``th_step`` is the threshold grid cafaeval sweeps. A finer grid optimises
#:   over more candidate thresholds and reports a higher Fmax, which the
#:   payload's own field documentation says breaks numeric parity with LAFA.
#: * ``toi_file`` decides which terms count toward precision and recall at all.
#:   Null means the terms were derived from the pivot snapshot, which is a
#:   different frame from any supplied file.
#:
#: ``toi_file`` is sealed as the PATH, which is weaker than the thing that
#: matters: two different files written to one path seal alike, and one file
#: copied to two paths seals twice. That is the same weakness ``ia_file`` has
#: and the reason ``information_accretion_set_id`` is preferred over it, so the
#: terms of interest want the same treatment -- a row with a content hash --
#: rather than a better string. Until then the path is recorded, because a
#: field that changes the number and is absent from the seal is worse than one
#: recorded imperfectly.
_FRAME_FIELDS: tuple[str, ...] = (
    "evaluation_set_id",
    "pivot_snapshot_id",
    "information_accretion_set_id",
    "temporal_window",
    "max_terms",
    "max_distance",
    "restrict_gt_to_predicted",
    "th_step",
    "toi_file",
)

#: Fields whose absence from a payload means the payload's default VALUE, not
#: the absence of a choice.
#:
#: The caps are the other case and must keep their current treatment: a payload
#: with no ``max_distance`` did not cap at some default distance, it did not cap
#: at all, so null is the honest material and the docstring of
#: :func:`frame_digest` says why. But a payload with no ``restrict_gt_to_predicted``
#: ran with it true, and scored exactly what a payload passing ``true``
#: explicitly scored. Encoding that as null would split one frame in two over
#: how the request happened to be spelled, which is the mirror image of the
#: defect this field list exists to close.
_PAYLOAD_DEFAULTS: dict[str, Any] = {
    "restrict_gt_to_predicted": True,
    "th_step": 0.01,
}

#: Fields read out of JSONB as text that name a number.
#:
#: ``j.payload ->> 'th_step'`` is text, so ``0.01`` and ``0.010`` arrive as two
#: strings and would seal as two frames although cafaeval swept one grid. These
#: are canonicalised through ``float`` before they reach the digest. The caps
#: are here for the same reason; they were always read as text and the hazard
#: predates this change.
_NUMERIC_FRAME_FIELDS: frozenset[str] = frozenset({"max_terms", "max_distance", "th_step"})

#: Fields naming a truth value that arrives from JSONB as text.
_BOOLEAN_FRAME_FIELDS: frozenset[str] = frozenset({"restrict_gt_to_predicted"})

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
           j.payload ->> 'restrict_gt_to_predicted'       AS restrict_gt_to_predicted,
           j.payload ->> 'th_step'                        AS th_step,
           j.payload ->> 'toi_file'                       AS toi_file,
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


def _frame_value(field: str, raw: Any) -> Any:
    """One field's material: resolved to its default, then canonicalised.

    Two spellings of one run must not seal as two frames. A payload omitting
    ``th_step`` swept the same grid as one passing ``0.01``, and ``0.01`` read
    out of JSONB as text is the same grid as ``0.010``. Both collapse here, so
    the digest is a function of what cafaeval did rather than of how the
    request was written.

    A field in neither map keeps its raw value, which is how the caps stay
    null when absent: see :data:`_PAYLOAD_DEFAULTS` for why they must.
    """
    if raw is None and field in _PAYLOAD_DEFAULTS:
        raw = _PAYLOAD_DEFAULTS[field]
    if raw is None:
        return None
    if field in _BOOLEAN_FRAME_FIELDS:
        return raw if isinstance(raw, bool) else str(raw).strip().lower() == "true"
    if field in _NUMERIC_FRAME_FIELDS:
        try:
            return float(raw)
        except (TypeError, ValueError):
            # Unparseable is not the default and not absent. Keeping the raw
            # string seals it apart from every well-formed value rather than
            # quietly joining one of their frames.
            return str(raw)
    return raw


def frame_material(row: dict[str, Any]) -> dict[str, Any]:
    """The frame's fields, resolved and canonicalised: what the digest is of.

    Exists so the material stored beside the digest is the same material the
    digest was taken over. Recording the raw row instead would put a frame in
    ``evaluation_frame`` whose fields do not hash to the key they are filed
    under, and the address and its contents disagreeing is the defect storing
    the contents was meant to fix.
    """
    return {field: _frame_value(field, row.get(field)) for field in _FRAME_FIELDS}


def frame_digest(row: dict[str, Any]) -> str | None:
    """The seal for one row, or nothing when its frame cannot be recovered.

    Content addressed, so the same frame always yields the same seal and two
    results can be compared by string equality without anyone having to know
    which fields the frame is made of. A cap absent from the payload is encoded
    as null rather than skipped, so a run that omitted it and a run that set it
    to the same value the default happens to take do not collide. Fields whose
    absence means a default VALUE are resolved to it first; :func:`_frame_value`
    draws that line and :data:`_PAYLOAD_DEFAULTS` says which side each is on.

    Returns nothing when a field that changes the meaning of the number is
    missing. The evaluation set and the accretion set are both required for
    that reason: without the first the population is unknown, and without the
    second the weighting is.
    """
    if not row.get("evaluation_set_id") or not row.get("information_accretion_set_id"):
        return None
    canonical = json.dumps(frame_material(row), sort_keys=True, separators=(",", ":"))
    return "f-" + hashlib.sha256(canonical.encode()).hexdigest()[:_DIGEST_WIDTH]


class SealEvaluationFramesPayload(ProteaPayload, frozen=True):
    """Inputs for the seal.

    ``dry_run`` defaults to true. An operation that rewrites a provenance column
    should have to be asked twice, and the first answer is a report of exactly
    what the second would write.
    """

    dry_run: bool = True
    max_examples: PositiveInt = 20


@dataclass
class _Tally:
    """What one pass over the stored results found, before anything is said."""

    sealed: int = 0
    already: int = 0
    conflicts: list[dict[str, str]] = field(default_factory=list)
    unattributable: list[dict[str, str]] = field(default_factory=list)
    digests: dict[str, int] = field(default_factory=dict)


def _walk(session: Session, rows: list[dict[str, Any]], p: Any) -> _Tally:
    """Classify every row and, unless this is a dry run, write what it implies.

    Separated from ``execute`` so the method stays inside its size budget and,
    more usefully, so the classification reads as one thing: each row is
    unattributable, already correct, in conflict, or ready to seal, and those
    four are the whole vocabulary.
    """
    t = _Tally()

    for row in rows:
        digest = frame_digest(row)
        if digest is None:
            if len(t.unattributable) < p.max_examples:
                t.unattributable.append(
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

        t.digests[digest] = t.digests.get(digest, 0) + 1
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
                    "material": json.dumps(frame_material(row), sort_keys=True),
                    "fields": json.dumps(list(_FRAME_FIELDS)),
                    "provenance": json.dumps(capture_provenance()),
                },
            )
        current = row.get("frame")
        if current == digest:
            t.already += 1
            continue
        if current:
            if len(t.conflicts) < p.max_examples:
                t.conflicts.append({"id": row["id"], "held": current, "computed": digest})
            continue
        if not p.dry_run:
            session.execute(_SEAL, {"frame": digest, "id": row["id"]})
        t.sealed += 1

    return t


class SealEvaluationFramesOperation(Operation):
    name = "seal_evaluation_frames"
    description = (
        "Stamp evaluation_result.frame_digest with a content digest of the window, "
        "pivot, accretion set and evaluation caps each result was produced under. "
        "Leaves the frame harness label alone. Refuses rows whose provenance is "
        "unrecoverable and never overwrites a differing seal. Defaults to a dry run."
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
        return "stamp evaluation_result.frame_digest with the digest of its window, pivot and caps"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = SealEvaluationFramesPayload.model_validate(contract_payload(payload))
        rows = [dict(r) for r in session.execute(_ROWS).mappings().all()]
        emit(
            "seal.start",
            f"{len(rows)} stored results, sealing over {len(_FRAME_FIELDS)} frame fields",
            {"rows": len(rows), "fields": list(_FRAME_FIELDS), "dry_run": p.dry_run},
            "info",
        )

        t = _walk(session, rows, p)
        sealed, already = t.sealed, t.already
        conflicts, unattributable, digests = t.conflicts, t.unattributable, t.digests

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
