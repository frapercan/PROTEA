"""Setup-phase helpers extracted from ``run_cafa_evaluation``.

Holds the two ``NamedTuple`` bundles threaded through the run pipeline
plus the small functions that load the term universe and emit the
setup events. Living in a sibling keeps the operation file lean and
under the master-plan v3.2 §3 LOC budget.

Re-exported by ``run_cafa_evaluation`` for backwards compatibility.
"""

from __future__ import annotations

import uuid
from typing import Any, NamedTuple

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.storage import ArtifactStore


class _EvalInputs(NamedTuple):
    """Validated entities + supporting data shared across the run pipeline."""

    eval_set_id: uuid.UUID
    pred_set_id: uuid.UUID
    eval_set: EvaluationSet
    pred_set: PredictionSet
    data: EvaluationData
    snapshot: OntologySnapshot
    pivot_snapshot_id: uuid.UUID
    toi_go_ids: list[str]


class _PipelineCtx(NamedTuple):
    """Bundle of run-pipeline inputs threaded through the tempdir block."""

    inputs: _EvalInputs
    scoring_snapshot: ScoringConfig | None
    reranker_models: dict[str, Any]
    result_id: uuid.UUID
    artifact_store: ArtifactStore


def _load_terms_of_interest(session: Session, pivot_snapshot_id: uuid.UUID) -> list[str]:
    """Return every GO id under the pivot snapshot (the term universe).

    Result feeds straight into cafaeval as the ``-toi`` argument so the
    scorer evaluates against exactly the terms present in the resolved
    snapshot, not the full ontology.
    """
    return [
        gid
        for (gid,) in session.query(GOTerm.go_id)
        .filter(GOTerm.ontology_snapshot_id == pivot_snapshot_id)
        .all()
    ]


def _emit_evaluation_setup_events(emit: EmitFn, inputs: _EvalInputs) -> None:
    """Fire the ``start`` + ``delta_done`` events the UI gates on.

    Order matters: ``start`` first (so the UI can flip to running)
    then ``delta_done`` once the per-category counts are computed.
    """
    emit(
        "run_cafa_evaluation.start",
        None,
        {
            "evaluation_set_id": str(inputs.eval_set_id),
            "prediction_set_id": str(inputs.pred_set_id),
            "pivot_ontology_snapshot_id": str(inputs.pivot_snapshot_id),
            "mode": (inputs.eval_set.stats or {}).get("mode", "same_snapshot"),
            "obo_url": inputs.snapshot.obo_url,
        },
        "info",
    )
    emit(
        "run_cafa_evaluation.delta_done",
        None,
        {
            "nk_proteins": inputs.data.nk_proteins,
            "lk_proteins": inputs.data.lk_proteins,
            "pk_proteins": inputs.data.pk_proteins,
        },
        "info",
    )
