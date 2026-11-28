"""EvaluationSet + EvaluationResult helpers for ``annotations_service``.

The evaluation-side reads (``evaluation_set_to_dict``,
``evaluation_result_to_dict``, the list/get/delete handlers) plus the
baseline-scoring auto-attach helper share a divergent-change axis with
the benchmark matrix and the CAFA evaluation pipeline. Snapshot and
annotation-set CRUD evolve independently, so this cluster moves out
into its own sibling module while the parent service keeps the public
import surface via re-export.

Domain exceptions (``EntityNotFoundError``) are imported lazily inside
the functions that raise them to avoid a circular dependency with the
parent module.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


def evaluation_result_to_dict(
    r: EvaluationResult,
    code_revision: str | None = None,
    score_reading: str | None = None,
    arm: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """Serialise an :class:`EvaluationResult` to its API dict shape.

    ``frame_digest`` IS SERVED, AND IT IS THE ONLY FIELD HERE THAT SAYS WHETHER
    TWO NUMBERS ARE COMPARABLE. It was the one column of the provenance group
    this function left out, so the page that exists to check attributability
    asked for the frame and got the harness label beside it instead. The digest
    is computed from the material a frame is made of -- the evaluation set, the
    pivot snapshot, the accretion set, the window and the two caps -- while
    ``frame`` and ``temporal_window`` are strings a harness wrote and all eight
    live rows carry ``NULL`` in both. ``compare_paired_panels`` already reads
    the digest and refuses a pair without one; until now nothing else could see
    what it was refusing on.
    """
    return {
        "id": str(r.id),
        "evaluation_set_id": str(r.evaluation_set_id),
        "prediction_set_id": str(r.prediction_set_id),
        "scoring_config_id": str(r.scoring_config_id) if r.scoring_config_id else None,
        "reranker_model_id": str(r.reranker_model_id) if r.reranker_model_id else None,
        "reranker_config": r.reranker_config,
        "job_id": str(r.job_id) if r.job_id else None,
        # The revision of the code that evaluated, as its job recorded it when it
        # started; None for a result with no job or a job that recorded none.
        "code_revision": code_revision,
        # How the scores were read before cafaeval thresholded them (ADR-D55):
        # the result's own record, else the job's payload, else None.
        "score_reading": ((r.results or {}).get("reading") or {}).get("score_reading")
        or score_reading,
        # What the evaluated set is, in a reader's words: its model, the scoring
        # it read and its method, each None where the set has none.
        "arm": arm,
        "created_at": r.created_at.isoformat(),
        # F-METHOD-EVAL-SURFACE provenance (read-through; ``None`` on legacy
        # rows so the UI shows an "unknown" empty state).
        "frame": r.frame,
        "frame_digest": r.frame_digest,
        "temporal_window": r.temporal_window,
        "arms_enabled": r.arms_enabled,
        "leakage_role": r.leakage_role,
        "results": r.results,
    }


def list_evaluation_results_data(
    session: Session,
    eval_id: uuid.UUID,
) -> list[dict[str, Any]]:
    """List EvaluationResult rows for one EvaluationSet (newest first).

    Raises :class:`EntityNotFoundError` when the EvaluationSet does
    not resolve.
    """
    from protea.services.annotations_service import EntityNotFoundError

    if session.get(EvaluationSet, eval_id) is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    rows = (
        session.query(EvaluationResult)
        .filter(EvaluationResult.evaluation_set_id == eval_id)
        .order_by(EvaluationResult.created_at.desc())
        .all()
    )
    jobs = [r.job_id for r in rows if r.job_id]
    revisions = job_revisions(session, jobs)
    readings = job_readings(session, jobs)
    arms = prediction_set_arms(session, [r.prediction_set_id for r in rows])
    return [
        evaluation_result_to_dict(
            r,
            revisions.get(str(r.job_id)) if r.job_id else None,
            readings.get(str(r.job_id)) if r.job_id else None,
            arms.get(str(r.prediction_set_id)),
        )
        for r in rows
    ]


#: The revision each job recorded when it started, read by the (job_id, event)
#: index. A job retried after a crash starts again; its latest start wins.
_JOB_REVISIONS = text(
    """
    SELECT job_id::text AS job_id, fields ->> 'code_revision' AS code_revision
    FROM job_event
    WHERE job_id = ANY(CAST(:jobs AS uuid[])) AND event = 'job.started'
    ORDER BY ts
    """
)


def job_revisions(session: Session, job_ids: list[uuid.UUID]) -> dict[str, str]:
    """job id -> the code revision its job recorded when it started."""
    if not job_ids:
        return {}
    rows = session.execute(_JOB_REVISIONS, {"jobs": [str(j) for j in job_ids]}).mappings()
    return {r["job_id"]: r["code_revision"] for r in rows if r["code_revision"]}


#: The reading each evaluating job was asked for. ``raw`` is the payload's
#: default and what every run read before the field existed (ADR-D55), so a job
#: that names none read raw.
_JOB_READINGS = text(
    """
    SELECT id::text AS job_id, COALESCE(payload ->> 'score_reading', 'raw') AS score_reading
    FROM job
    WHERE id = ANY(CAST(:jobs AS uuid[])) AND operation = 'run_cafa_evaluation'
    """
)


def job_readings(session: Session, job_ids: list[uuid.UUID]) -> dict[str, str]:
    """job id -> the score reading its evaluation was asked for."""
    if not job_ids:
        return {}
    rows = session.execute(_JOB_READINGS, {"jobs": [str(j) for j in job_ids]}).mappings()
    return {r["job_id"]: r["score_reading"] for r in rows}


#: Each prediction set's model, scoring and method. A scored set carries its
#: candidate set's configuration and method, and names its scoring in ``meta``.
_ARMS = text(
    """
    SELECT ps.id::text AS prediction_set_id,
           ec.model_name AS model,
           sc.name AS scoring,
           m.short_name AS method
    FROM prediction_set ps
    LEFT JOIN embedding_config ec ON ec.id = ps.embedding_config_id
    LEFT JOIN scoring_config sc ON sc.id::text = ps.meta ->> 'scoring_config_id'
    LEFT JOIN method m ON m.id = ps.method_id
    WHERE ps.id = ANY(CAST(:sets AS uuid[]))
    """
)


def prediction_set_arms(
    session: Session, set_ids: list[uuid.UUID]
) -> dict[str, dict[str, str | None]]:
    """prediction set id -> its model, scoring and method, each None where it has none."""
    if not set_ids:
        return {}
    rows = session.execute(_ARMS, {"sets": [str(s) for s in set_ids]}).mappings()
    return {
        r["prediction_set_id"]: {
            "model": r["model"],
            "scoring": r["scoring"],
            "method": r["method"],
        }
        for r in rows
    }


def get_eval_result_with_keys(
    session: Session,
    eval_id: uuid.UUID,
    result_id: uuid.UUID,
) -> tuple[EvaluationResult, list[str]]:
    """Fetch an EvaluationResult belonging to ``eval_id``; return (row, artifact_keys).

    Raises :class:`EntityNotFoundError` ("EvaluationResult") when
    the result does not exist or does not belong to ``eval_id``.
    """
    from protea.services.annotations_service import EntityNotFoundError

    result = session.get(EvaluationResult, result_id)
    if result is None or result.evaluation_set_id != eval_id:
        raise EntityNotFoundError("EvaluationResult", result_id)
    keys: list[str] = (result.results or {}).get("artifacts", {}).get("keys") or []
    return result, keys


def get_eval_result_artifact(
    session: Session,
    eval_id: uuid.UUID,
    result_id: uuid.UUID,
    relpath: str,
) -> tuple[str, str | None]:
    """The store key of one file a result recorded, and the sha256 it recorded.

    ``relpath`` is the file's path under the result, as the evaluator wrote it
    (``PK/evaluation_best_f_micro_w.tsv``). Only a recorded file resolves, so no
    path a caller types can reach another key of the store. The digest is
    ``None`` for a result written before results recorded digests (2026-11-21).

    Raises :class:`EntityNotFoundError` when the result does not belong to
    ``eval_id`` or recorded no file at ``relpath``.
    """
    from protea.core.operations._run_cafa_helpers import eval_artifact_key
    from protea.services.annotations_service import EntityNotFoundError

    result, keys = get_eval_result_with_keys(session, eval_id, result_id)
    key = eval_artifact_key(result_id, relpath)
    if key not in keys:
        raise EntityNotFoundError("EvaluationArtifact", result_id)
    digests: dict[str, str] = (result.results or {}).get("artifacts", {}).get("sha256") or {}
    return key, digests.get(key)


def assert_evaluation_set_exists(session: Session, eval_id: uuid.UUID) -> None:
    """Raise :class:`EntityNotFoundError` when the ``EvaluationSet`` UUID
    does not resolve. Cheap preflight for endpoints that dispatch
    background work but still need a 404 path."""
    from protea.services.annotations_service import EntityNotFoundError

    if session.get(EvaluationSet, eval_id) is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)


def delete_eval_result_collect_keys(
    session: Session,
    eval_id: uuid.UUID,
    result_id: uuid.UUID,
) -> list[str]:
    """Delete the EvaluationResult and return the artifact keys to clean up.

    Same split as :func:`delete_evaluation_set_collect_keys`: the
    DB delete happens here; the artifact-store deletion is the
    router's responsibility (it owns the ``ArtifactStore`` factory).
    """
    result, keys = get_eval_result_with_keys(session, eval_id, result_id)
    session.delete(result)
    return keys


def evaluation_set_to_dict(e: EvaluationSet) -> dict[str, Any]:
    """Serialise an :class:`EvaluationSet` to its API dict shape.

    ALL ELEVEN IDENTITY COLUMNS ARE SERVED. This function used to send two of
    them. ``uq_evaluation_set_identity`` names eleven -- the annotation pair, the
    pivot, the two native snapshots, the truth profile, the term-of-interest
    rule, the NOT codes, the target population, the binding rule and the
    declared sample of the targets -- and the model's own comment says why each
    one is there: the same pair under a different propagation graph is a
    different measurement by 21% of the PK bucket, and ``projected`` against
    ``as_closed`` is 646 proteins and 2.610 entries of difference on the
    211 -> 227 window.

    Serving two of eight meant a reader could not tell two windows apart from
    the API at all, and the record holds exactly that case: ``7aa37929`` and
    ``ea0f4334`` are the same pair and the same graphs, differ only in
    ``truth_profile``, and their deltas are 39.885 and 40.531. A window cited
    without its rules is not identified.
    """
    return {
        "id": str(e.id),
        "old_annotation_set_id": str(e.old_annotation_set_id),
        "new_annotation_set_id": str(e.new_annotation_set_id),
        # The other six of the identity key. Snapshots fix the graph each side
        # is closed under; the three rules fix what the delta IS.
        "pivot_snapshot_id": str(e.pivot_snapshot_id) if e.pivot_snapshot_id else None,
        "old_native_snapshot_id": (
            str(e.old_native_snapshot_id) if e.old_native_snapshot_id else None
        ),
        "new_native_snapshot_id": (
            str(e.new_native_snapshot_id) if e.new_native_snapshot_id else None
        ),
        "truth_profile": e.truth_profile,
        "toi_rule": e.toi_rule,
        "not_codes": e.not_codes,
        # Which proteins the window may score.
        "target_population": e.target_population,
        # Which protein-binding rule was asked for.
        "binding_rule": e.binding_rule,
        # The declared sample of its targets the window is kept to, the identity's
        # last field; None for a window over its whole population.
        "target_query_set_id": str(e.target_query_set_id) if e.target_query_set_id else None,
        # The window that replaced this one, when and why; all None while it stands.
        "superseded_by_id": str(e.superseded_by_id) if e.superseded_by_id else None,
        "superseded_at": e.superseded_at.isoformat() if e.superseded_at else None,
        "supersession_note": e.supersession_note,
        "job_id": str(e.job_id) if e.job_id else None,
        "created_at": e.created_at.isoformat(),
        "stats": e.stats,
        "window_role": e.window_role,
        # What the build APPLIED, beside the three rules above, which are what
        # it was asked for. ``None`` on a row built before the column: legacy,
        # nothing says. See ``protea.core.window_declarations``.
        "truth_rules": e.truth_rules,
    }


def list_evaluation_sets_data(session: Session) -> list[dict[str, Any]]:
    """List all evaluation sets, newest first."""
    rows = session.query(EvaluationSet).order_by(EvaluationSet.created_at.desc()).all()
    return [evaluation_set_to_dict(e) for e in rows]


def get_evaluation_set_data(
    session: Session,
    eval_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single evaluation set.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    from protea.services.annotations_service import EntityNotFoundError

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    return evaluation_set_to_dict(e)


def delete_evaluation_set_collect_keys(
    session: Session,
    eval_id: uuid.UUID,
) -> list[str]:
    """Delete the EvaluationSet and return the artifact-store keys to clean.

    The DB delete cascades to ``EvaluationResult`` rows; this helper
    walks the results before deleting and returns the union of all
    artifact keys those rows referenced (per-result cafaeval outputs)
    so the caller can wipe them from the store. The caller is also
    expected to delete the set's ground-truth artifact via
    ``protea.core.evaluation.groundtruth_key_for(eval_id)``;
    that key is not included here because it is a fixed function of
    ``eval_id``.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    from protea.services.annotations_service import EntityNotFoundError

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    result_keys: list[str] = []
    for r in (
        session.query(EvaluationResult).filter(EvaluationResult.evaluation_set_id == eval_id).all()
    ):
        result_keys.extend((r.results or {}).get("artifacts", {}).get("keys") or [])
    session.delete(e)
    return result_keys
