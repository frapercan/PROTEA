"""Optional InterPro2GO BP noisy-OR graft (serve-offline-reconciliation S3).

The offline champion grafts InterPro2GO BP predictions on top of the
reranker output with a noisy-OR combine, worth +0.0179 board-faithful on
LK-BP / PK-BP. This module ports the combine math as a pure, fully tested
function (:func:`noisy_or_graft_bp`) and exposes a gated post-step
(:func:`apply_interpro_bp_graft`) that the predict post-KNN pipeline calls
ONLY when ``serve.interpro_bp_graft`` is enabled (default off, so behaviour
is unchanged until a deploy turns it on).

Wiring status
-------------
The combine math is complete and tested. The *source* of the InterPro BP
predictions for the query proteins is the remaining integration work:
:func:`load_interpro_bp_predictions` is the single, clearly marked
integration point. It returns an empty list today, so with the flag ON but
no InterPro arm wired the post-step is a safe no-op (it never mutates the
reranker output). See the TODO there; fully wiring it means running the
InterPro arm (``run_interproscan_batch`` + ``predict_go_terms_from_interpro``)
for the uploaded query proteins, which depends on an InterProScan binary on
the host (the documented outstanding prerequisite).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn

#: GO aspect code for biological_process (``GOTerm.aspect`` wire format).
BP_CODE = "P"


def _noisy_or(base: float, other: float) -> float:
    """Combine two independent probabilities: ``1 - (1 - base)(1 - other)``.

    Inputs are clamped to ``[0, 1]`` so a stray out-of-range score can never
    push the result outside the probability range or flip its sign.
    """
    b = min(1.0, max(0.0, float(base)))
    o = min(1.0, max(0.0, float(other)))
    return 1.0 - (1.0 - b) * (1.0 - o)


def noisy_or_graft_bp(
    prediction_dicts: list[dict[str, Any]],
    interpro_preds: Iterable[Mapping[str, Any]],
    *,
    score_key: str = "reranker_score",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Noisy-OR InterPro BP predictions into the BP candidates (BP-only).

    For each InterPro prediction ``(protein_accession, go_id, go_term_id,
    prob)`` (assumed already filtered to biological_process by the loader):

    * If a candidate with the same ``(protein_accession, go_id)`` already
      exists, its ``score_key`` becomes ``1 - (1 - base)(1 - prob)`` where
      ``base`` is the candidate's current ``score_key`` (0.0 when absent).
    * Otherwise a NEW BP candidate row is appended carrying the InterPro
      probability as its ``score_key`` and ``interpro_graft_present = 1.0``.

    The function is pure: it mutates / extends ``prediction_dicts`` and
    returns ``(prediction_dicts, stats)``. ``stats`` reports how many
    candidates were updated vs newly added. KNN / reranker candidates are
    never dropped, so this is a strict, monotone enrichment of the BP slice.
    """
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in prediction_dicts:
        go_id = rec.get("go_id")
        if go_id is None:
            continue
        by_key[(rec.get("protein_accession", ""), go_id)] = rec

    updated = 0
    added = 0
    for pred in interpro_preds:
        go_id = pred.get("go_id")
        prob = pred.get("prob")
        if go_id is None or prob is None:
            continue
        acc = pred.get("protein_accession", "")
        existing = by_key.get((acc, go_id))
        if existing is not None:
            existing[score_key] = _noisy_or(existing.get(score_key, 0.0), prob)
            existing["interpro_graft_present"] = 1.0
            updated += 1
            continue
        rec = _new_interpro_record(acc, go_id, pred.get("go_term_id"), float(prob), score_key)
        by_key[(acc, go_id)] = rec
        prediction_dicts.append(rec)
        added += 1

    return prediction_dicts, {"updated": updated, "added": added}


def _new_interpro_record(
    accession: str,
    go_id: str,
    go_term_id: int | None,
    prob: float,
    score_key: str,
) -> dict[str, Any]:
    """Build an InterPro-only BP candidate dict (KNN features zero/default)."""
    rec: dict[str, Any] = {
        "protein_accession": accession,
        "go_term_id": go_term_id,
        "go_id": go_id,
        "aspect": BP_CODE,
        "ref_protein_accession": "interpro",
        "distance": float("nan"),
        "qualifier": "",
        "evidence_code": "IEA",
        "interpro_graft_present": 1.0,
    }
    rec[score_key] = min(1.0, max(0.0, prob))
    return rec


def load_interpro_bp_predictions(
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
) -> list[dict[str, Any]]:
    """Load InterPro2GO BP predictions for the query proteins.

    INTEGRATION POINT (intentional no-op today). Returning ``[]`` keeps the
    graft post-step a safe no-op even when ``serve.interpro_bp_graft`` is on:
    the reranker output is never mutated until this loader yields real BP
    predictions.

    TODO (serve-offline-reconciliation, follow-up): source BP predictions for
    the uploaded query proteins from the InterPro arm. Concretely, run
    ``run_interproscan_batch`` to persist ``InterProAnnotation`` rows, then
    ``predict_go_terms_from_interpro`` (join against ``InterProGoMapping`` at
    the active ``source_version``, resolve GO ids against ``snapshot_id``),
    filter to ``aspect == 'P'``, and map each row to
    ``{"protein_accession", "go_id", "go_term_id", "prob"}`` where ``prob`` is
    the calibrated InterPro confidence (e.g. ``1 / distance`` normalised). This
    depends on an InterProScan binary on the host (the documented outstanding
    prerequisite), so it is deferred to a follow-up rather than half-wired.
    """
    # Parameters are accepted now so the signature is stable for the follow-up.
    _ = (session, snapshot_id, valid_accessions)
    return []


def apply_interpro_bp_graft(
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
    *,
    score_key: str = "reranker_score",
) -> list[dict[str, Any]]:
    """Gated post-step: graft InterPro2GO BP predictions onto the BP candidates.

    Loads the BP predictions (:func:`load_interpro_bp_predictions`), applies
    :func:`noisy_or_graft_bp`, and emits a completion event. Caller gates this
    on ``serve.interpro_bp_graft``; with the loader's current no-op it leaves
    ``prediction_dicts`` unchanged.
    """
    accessions = [acc for acc in valid_accessions if acc]
    interpro_preds = load_interpro_bp_predictions(session, snapshot_id, accessions)
    prediction_dicts, stats = noisy_or_graft_bp(
        prediction_dicts, interpro_preds, score_key=score_key
    )
    emit(
        "predict_go_terms_batch.interpro_bp_graft_done",
        None,
        {
            "interpro_bp_predictions": len(interpro_preds),
            "candidates_updated": stats["updated"],
            "candidates_added": stats["added"],
        },
        "info",
    )
    return prediction_dicts


__all__ = (
    "BP_CODE",
    "apply_interpro_bp_graft",
    "load_interpro_bp_predictions",
    "noisy_or_graft_bp",
)
