"""Retire a window of a superseded frame: archive it, then delete, leaving a job.

WHY AN OPERATION AND NOT A SCRIPT. A raw ``DELETE`` is how 124 campaign rows
disappear with nothing recording that they did, who asked, or what they were.
The project's rule is that every operation goes through ``POST /v1/jobs``
because a direct ``execute()`` leaves no job, and a delete is the case where
that matters most: afterwards there is no row left to ask.

THE ORDER IS ARCHIVE, DELETE, PURGE, and the first version got it wrong. It
deleted each row and each file inside the loop, with the commit left to the
worker at the end. A failure halfway -- a reference appearing between the
check and the delete, a dropped connection, a killed job -- rolled the rows
back and left the earlier parquets gone: rows pointing at nothing, which is
the exact thing its own docstring claimed to avoid. The test that was supposed
to cover it simulated a STORE failure, not a transaction failure, so it gave
assurance it had not earned. The desktop session found it by reading.

So now: every parquet is copied to ``retired/`` FIRST, which is additive and
destroys nothing; then the rows go in one transaction that is committed; only
then are the originals purged. A failure at any point leaves archives (
harmless), rows (recoverable) and originals (intact), in that order of
appearance.

AND ARCHIVING RATHER THAN DELETING IS THE POINT. These truths cannot be
rebuilt the same way: they were computed in October by code that has since
changed, under a frame the campaign has left. Deleting them is irreversible
and archiving them costs a few MB each. Purging ``retired/`` is a separate
decision for a later day, and it will have the tombstones to decide with.

WHAT IT REFUSES, structurally:

* a window carrying a ``window_role``. That is the holdout and the two
  declared bands, and ``holdout-227-230`` has ZERO results in PROTEA because
  its single declared look was spent in the laboratory. A "no results" filter
  would have swept it up, and I wrote such a filter once;
* a window of the LIVE frame;
* a window anything points at. THE REFERRERS ARE DERIVED FROM
  ``pg_constraint`` AT RUN TIME and never from a list in this file: the first
  version hard-coded four tables, and ``evaluation_result`` carries
  ``ON DELETE CASCADE``, so a future table with CASCADE that nobody added to
  the list would have been emptied in silence by the delete;
* a window named in the payload of a job that is QUEUED or RUNNING.

``dry_run`` does every check and emits exactly what it would retire, touching
nothing. Run that first, read it, then run the real one with the same ids.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any, NamedTuple

from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_and_verify
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: The frame this operation is for. Anything else is refused.
SUPERSEDED = ("as_closed", "pivot_terms", "any")

#: Where an archived parquet lands. The sha is in the name so a tombstone and
#: an object can be matched without opening either.
RETIRED_PREFIX = "retired"

_REFERRERS_SQL = """
SELECT DISTINCT c.conrelid::regclass::text AS tabla, a.attname AS columna
FROM pg_constraint c
JOIN unnest(c.conkey) AS k(attnum) ON true
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
WHERE c.contype = 'f' AND c.confrelid = 'evaluation_set'::regclass
ORDER BY 1, 2
"""


class RetireEvaluationSetsPayload(ProteaPayload, frozen=True):
    """Which windows to retire. Named one by one, never a predicate."""

    evaluation_set_ids: list[str]
    #: Do every check and emit what would be retired, touching nothing.
    dry_run: bool = False

    @field_validator("evaluation_set_ids")
    @classmethod
    def _named_and_distinct(cls, value: list[str]) -> list[str]:
        """Refuse an empty list, and refuse repeats.

        Empty, because a delete whose scope is a predicate is a delete whose
        scope nobody read. Repeats, because the same id twice passed the check
        twice, deleted zero rows on the second pass and counted a FALSE
        orphan: the run would report damage it had not done.
        """
        if not value:
            raise ValueError(
                "evaluation_set_ids cannot be empty. This operation retires the "
                "windows it is given and takes no predicate: a delete whose scope is "
                "a query is a delete nobody read before it ran."
            )
        repetidos = sorted({v for v in value if value.count(v) > 1})
        if repetidos:
            raise ValueError(
                f"evaluation_set_ids repeats {repetidos}. A repeated id passes the "
                "checks twice, deletes nothing the second time and counts a false "
                "orphan, so the run would report damage it did not do."
            )
        return value


def referrers(session: Session) -> list[tuple[str, str]]:
    """Every table and column whose foreign key points at ``evaluation_set``.

    DERIVED AND NEVER LISTED. The first version of this file hard-coded four
    tables, which happened to be right on the day it was written. But
    ``evaluation_result`` carries ``ON DELETE CASCADE``: a future table with
    CASCADE that nobody added to the list would be emptied in silence by the
    delete, and the operation would report success. A list is a promise about
    a schema nobody re-reads.
    """
    return [(r[0], r[1]) for r in session.execute(text(_REFERRERS_SQL))]


def _live_job_ids(
    session: Session, ids: list[uuid.UUID], myself: uuid.UUID | None
) -> dict[str, str]:
    """``{window id: job id}`` for windows named by ANOTHER live job.

    Retiring a window a live job is about to read is a race nobody can
    diagnose afterwards: the job fails on a row that existed when it was
    enqueued.

    IT EXCLUDES THE ASKING JOB, and leaving that out made the guard refuse
    everything. Measured on 2026-11-08, the first real dry run over the 125
    superseded windows: **125 of 125 refused**, 124 of them because "job
    <this job> is QUEUED or RUNNING and names it in its payload". The job
    doing the asking is RUNNING by definition and its payload lists every
    window it was asked about, so the guard was reading its own reflection.

    The tests did not catch it because their fake sessions returned a
    ``live_payloads`` list that the operation's own job was never in: a guard
    about OTHER jobs was tested with no self to find. ``myself`` is None only
    when the operation runs outside a job, which the platform's own rule says
    never happens.
    """
    rows = session.execute(
        text(
            "SELECT j.id::text, j.payload::text FROM job j WHERE j.status IN ('QUEUED', 'RUNNING')"
        )
    ).all()
    out: dict[str, str] = {}
    for job_id, payload in rows:
        if myself is not None and job_id == str(myself):
            continue
        for vid in ids:
            if str(vid) in (payload or ""):
                out[str(vid)] = job_id
    return out


def _refusal(
    eset: EvaluationSet,
    counts: dict[str, int],
    live: dict[str, str],
) -> str | None:
    """Why this window must not be retired, or ``None``."""
    if eset.window_role is not None:
        return (
            f"it carries window_role={eset.window_role!r}, so it is the holdout or a "
            "declared band. The role is the protection and the result count is not: "
            "holdout-227-230 has zero results here because its look was spent in the "
            "laboratory"
        )
    rules = (eset.truth_profile, eset.toi_rule, eset.not_codes)
    if rules != SUPERSEDED:
        return (
            f"its rules are {rules}, not the superseded frame {SUPERSEDED}. Naming a "
            "live window here is far more likely to be a mistake than an intention"
        )
    apuntan = {t: n for t, n in counts.items() if n}
    if apuntan:
        return f"rows point at it: {apuntan}, and evidence is not retired"
    if str(eset.id) in live:
        return (
            f"job {live[str(eset.id)]} is QUEUED or RUNNING and names it in its "
            "payload. Retiring it now is a race nobody can diagnose afterwards"
        )
    return None


#: How many refusals the exception text carries. The event carries all of them.
MUESTRA = 5


def _refusal_message(rechazos: list[dict[str, str]], asked: int) -> str:
    """One message for every refusal, with a bounded sample of the reasons."""
    muestra = "; ".join(f"{r['evaluation_set_id']}: {r['motivo']}" for r in rechazos[:MUESTRA])
    resto = len(rechazos) - MUESTRA
    cola = (
        f"; and {resto} more, all of them in the retire_evaluation_sets.refused event"
        if resto > 0
        else ""
    )
    return (
        f"refusing to retire: {len(rechazos)} of {asked} windows are not retirable "
        f"and NOTHING was touched. {muestra}{cola}"
    )


def _tombstone(eset: EvaluationSet, blob: bytes, archivo: str) -> dict[str, Any]:
    """Everything the row said, before there is no row to ask.

    The first version's ``retired`` event carried an id and a key. After the
    delete that is not enough to say what was retired, which is the whole
    reason an operation is used instead of a DELETE.
    """
    return {
        "evaluation_set_id": str(eset.id),
        "old_annotation_set_id": str(eset.old_annotation_set_id),
        "new_annotation_set_id": str(eset.new_annotation_set_id),
        "pivot_snapshot_id": str(eset.pivot_snapshot_id),
        "old_native_snapshot_id": str(eset.old_native_snapshot_id),
        "new_native_snapshot_id": str(eset.new_native_snapshot_id),
        "truth_profile": eset.truth_profile,
        "toi_rule": eset.toi_rule,
        "not_codes": eset.not_codes,
        "window_role": eset.window_role,
        "code_revision": getattr(eset, "code_revision", None),
        "groundtruth_uri": eset.groundtruth_uri,
        "created_at": eset.created_at.isoformat() if eset.created_at else None,
        "job_id": str(eset.job_id) if eset.job_id else None,
        "stats": dict(eset.stats or {}),
        "groundtruth_sha256": hashlib.sha256(blob).hexdigest(),
        "groundtruth_bytes": len(blob),
        "archived_key": archivo,
    }


class _Plan(NamedTuple):
    """What does not change between windows, bundled.

    Same reason as ``_Side`` and ``EvalContext``: a signature that took them
    loose is a signature somebody can call with six of the seven, and the
    smell budget counts the seventh.
    """

    store: Any
    tablas: list[tuple[str, str]]
    dry: bool
    #: This operation's own job. The live-job guard has to skip it: the job
    #: asking is RUNNING and its payload names every window it was asked
    #: about, so without this the guard reads its own reflection and refuses
    #: everything.
    myself: uuid.UUID | None


def _counts_of(
    session: Session, eset: EvaluationSet, tablas: list[tuple[str, str]]
) -> dict[str, int]:
    """How many rows of each referring table point at this window."""
    return {
        tabla: session.execute(
            text(f"SELECT count(*) FROM {tabla} WHERE {col} = :v"), {"v": eset.id}
        ).scalar_one()
        for tabla, col in tablas
    }


def _archive_one(plan: _Plan, eset: EvaluationSet) -> dict[str, Any]:
    """Archive one window's parquet, verified, unless dry.

    IT DOES NOT DECIDE. Deciding and archiving used to be one function, so a
    refusal at the fiftieth window left forty-nine copies under ``retired/``
    beside rows that were still alive. Every window is decided first now, and
    nothing is written until all of them passed.

    THE DRY PASS DOES NOT WRITE. The first version archived unconditionally
    and the dry run called it, so the pass that "touches nothing" left copies
    under ``retired/`` beside rows that were still alive. The test meant to
    cover that checked deletes and commits and NOT writes, so it gave
    assurance it had not earned.

    THE COPY IS VERIFIED BEFORE ANY ROW GOES. A ``put`` taken on faith may be
    partial or in another bucket, and the later purge would then destroy the
    only one. It is re-read through the same door the truth is read by.
    """
    from protea.core.evaluation import groundtruth_key_for

    key = groundtruth_key_for(eset.id)
    blob = plan.store.get(key)
    digest = hashlib.sha256(blob).hexdigest()
    archivo = f"{RETIRED_PREFIX}/{eset.id}-{digest[:16]}.parquet"
    if not plan.dry:
        plan.store.put(archivo, blob)
        fetch_and_verify(plan.store, archivo, digest, f"the archived copy of {eset.id}")
    lapida = _tombstone(eset, blob, archivo)
    lapida["original_key"] = key
    return lapida


class RetireEvaluationSetsOperation(Operation):
    name = "retire_evaluation_sets"
    description = (
        "Archive the ground-truth parquets of named EvaluationSet rows of the "
        "superseded as_closed/pivot_terms/any frame under retired/, then delete the "
        "rows and purge the originals, leaving a job and a full tombstone per window. "
        "Refuses a window with a window_role, one of the live frame, one anything "
        "points at, or one a live job names. dry_run does every check and touches "
        "nothing, and either mode lists EVERY refusal instead of stopping at the first."
    )

    def execute(
        self,
        session: Session,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        p = RetireEvaluationSetsPayload.model_validate(contract_payload(payload))
        store = get_artifact_store(load_settings(_project_root()))
        ids = [uuid.UUID(raw) for raw in p.evaluation_set_ids]

        tablas = referrers(session)
        emit(
            "retire_evaluation_sets.referrers",
            None,
            {"derived_from_pg_constraint": [f"{t}.{c}" for t, c in tablas]},
            "info",
        )
        lapidas = self._check_all(
            session,
            ids,
            _Plan(
                store=store,
                tablas=tablas,
                dry=p.dry_run,
                myself=job_id_from_payload(payload),
            ),
            emit,
        )

        if p.dry_run:
            emit(
                "retire_evaluation_sets.done",
                None,
                {"would_retire": len(lapidas), "asked": len(ids), "dry_run": True},
                "info",
            )
            return OperationResult(
                result={"would_retire": len(lapidas), "asked": len(ids), "dry_run": True}
            )

        return self._retire(session, store, lapidas, emit)

    def _decide(
        self,
        session: Session,
        ids: list[uuid.UUID],
        plan: _Plan,
        emit: EmitFn,
    ) -> list[EvaluationSet]:
        """Lock every window and collect EVERY refusal before anything is written.

        It used to refuse on the first bad window. Over 124 ids that turns one
        dry pass into a queue of passes: you learn one reason, fix it, and find
        the next. The reasons are independent, so there is no reason to learn
        them one at a time.

        ``FOR UPDATE`` is what makes the check mean anything for the length of
        the transaction: it takes a row lock that a foreign-key insert into any
        referring table has to wait for, so a reference cannot appear between
        the check and the delete.

        The full list of refusals goes in the EVENT and a bounded sample in the
        exception. A message carrying 124 reasons is unreadable where it lands,
        and the record of what was refused belongs in the data either way.
        """
        session.execute(
            text("SELECT id FROM evaluation_set WHERE id = ANY(:v) FOR UPDATE"),
            {"v": [str(i) for i in ids]},
        )
        live = _live_job_ids(session, ids, plan.myself)
        buenos: list[EvaluationSet] = []
        rechazos: list[dict[str, str]] = []
        for vid in ids:
            eset = session.get(EvaluationSet, vid)
            if eset is None:
                rechazos.append({"evaluation_set_id": str(vid), "motivo": "does not exist"})
                continue
            motivo = _refusal(eset, _counts_of(session, eset, plan.tablas), live)
            if motivo is None:
                buenos.append(eset)
            else:
                rechazos.append({"evaluation_set_id": str(vid), "motivo": motivo})
        if rechazos:
            emit(
                "retire_evaluation_sets.refused",
                f"{len(rechazos)} of {len(ids)} refused, nothing touched",
                {"refused": len(rechazos), "asked": len(ids), "reasons": rechazos},
                "warning",
            )
            raise ValueError(_refusal_message(rechazos, len(ids)))
        return buenos

    def _check_all(
        self,
        session: Session,
        ids: list[uuid.UUID],
        plan: _Plan,
        emit: EmitFn,
    ) -> list[dict[str, Any]]:
        """Decide on every window, and only then archive. Never interleaved."""
        lapidas: list[dict[str, Any]] = []
        for eset in self._decide(session, ids, plan, emit):
            lapida = _archive_one(plan, eset)
            lapidas.append(lapida)
            emit(
                "retire_evaluation_sets.would_archive"
                if plan.dry
                else "retire_evaluation_sets.archived",
                None,
                lapida,
                "info",
            )
        return lapidas

    def _retire(
        self,
        session: Session,
        store: Any,
        lapidas: list[dict[str, Any]],
        emit: EmitFn,
    ) -> OperationResult:
        """Delete the rows, COMMIT, and only then purge the originals."""
        for lapida in lapidas:
            session.execute(
                text("DELETE FROM evaluation_set WHERE id = :v"),
                {"v": uuid.UUID(lapida["evaluation_set_id"])},
            )
        # Committed HERE and not left to the worker: until the rows are durably
        # gone, purging an original would leave a row pointing at nothing if
        # anything rolled the transaction back.
        session.commit()
        emit(
            "retire_evaluation_sets.rows_gone",
            None,
            {"deleted": len(lapidas)},
            "warning",
        )

        # ``delete`` returns False when the key was not there, and the MinIO
        # backend also returns False on any S3Error from its stat. Ignoring
        # the return counted every one of those as purged.
        no_purgados = 0
        for lapida in lapidas:
            try:
                if not store.delete(lapida["original_key"]):
                    no_purgados += 1
            except Exception:  # noqa: BLE001 -- the copy is already in retired/
                no_purgados += 1
        result = {
            "retired": len(lapidas),
            "archived_under": RETIRED_PREFIX,
            "originals_left_behind": no_purgados,
            "asked": len(lapidas),
            "dry_run": False,
        }
        emit("retire_evaluation_sets.done", None, result, "warning")
        return OperationResult(result=result)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        n = len(payload.get("evaluation_set_ids") or [])
        seco = " (dry run)" if payload.get("dry_run") else ""
        return f"retire {n} evaluation set(s) of the superseded frame{seco}"


def _project_root():
    from pathlib import Path

    return Path(__file__).resolve().parents[3]
