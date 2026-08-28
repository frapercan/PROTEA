"""Setup-phase helpers extracted from ``run_cafa_evaluation``.

Holds the two ``NamedTuple`` bundles threaded through the run pipeline
plus the small functions that load the term universe and emit the
setup events. Living in a sibling keeps the operation file lean and
under the master-plan v3.2 §3 LOC budget.

Re-exported by ``run_cafa_evaluation`` for backwards compatibility.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData
from protea.core.operations._run_cafa_eval_driver import CafaEvalRunContext
from protea.core.operations._run_cafa_ia_frame import ia_frame
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.storage import ArtifactStore

if TYPE_CHECKING:  # pragma: no cover - typing only
    # Imported for the annotation alone. run_cafa_evaluation imports this module,
    # so a runtime import would close a cycle; `from __future__ import
    # annotations` already makes the annotation a string, which is why nothing
    # crashed and only the linter noticed the name was never bound.
    from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload


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


class StagedInputs(NamedTuple):
    """What the staging step produced, named.

    It had no name, so the function that consumes it took eleven arguments and
    the size guard was right to object: a parameter list that long is usually a
    missing noun. These seven values are exactly what staging writes or resolves,
    and nothing else in the pipeline produces them.
    """

    obo_path: str
    ia_path: str | None
    gt_paths: dict[str, str]
    toi_path: str
    data: Any
    delta_proteins: set[str]
    has_rerankers: bool


def bundle_run_context(
    p: RunCafaEvaluationPayload,
    ctx: _PipelineCtx,
    artifacts_root: Path,
    staged: StagedInputs,
    emit: EmitFn,
) -> CafaEvalRunContext:
    """Name what was staged, and nothing else.

    Every field here is a value the staging step already produced, so this
    is the one place where a caller can see the whole cafaeval input surface
    at once, including the three frame markers that have to travel with the
    grid artefact or two runs under different frames publish as a method
    difference.
    """
    inputs = ctx.inputs
    return CafaEvalRunContext(
        pred_set_id=inputs.pred_set_id,
        delta_proteins=staged.delta_proteins,
        max_distance=p.max_distance,
        max_k_position=p.max_k_position,
        max_sequence_rank=p.max_sequence_rank,
        artifacts_root=artifacts_root,
        has_rerankers=staged.has_rerankers,
        reranker_models=ctx.reranker_models,
        scoring_config_snapshot=ctx.scoring_snapshot,
        data=staged.data,
        obo_path=staged.obo_path,
        nk_path=staged.gt_paths["nk"],
        lk_path=staged.gt_paths["lk"],
        pk_path=staged.gt_paths["pk"],
        pk_known_path=staged.gt_paths["pk_known"],
        ia_path=staged.ia_path,
        toi_path=staged.toi_path,
        shared_pred_dir=os.path.join(str(artifacts_root), "predictions"),
        ontology_snapshot_id=str(inputs.snapshot.id),
        evaluation_set_id=str(inputs.eval_set_id),
        information_accretion_frame=ia_frame(p, staged.ia_path, emit),
        th_step=p.th_step,
        max_terms=p.max_terms,
        softprop=p.softprop,
        interpro_graft=p.interpro_graft,
        interpro_protein2ipr_file=p.interpro_protein2ipr_file,
        interpro_ipr2go_file=p.interpro_ipr2go_file,
        interpro_graft_weight=p.interpro_graft_weight,
    )
