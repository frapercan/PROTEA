"""Reranker-metrics computation helpers for ``scoring_service``.

The orchestrator ``compute_reranker_metrics_data`` lives here split
into private helpers (validate / load / materialise / score / format)
so neither it nor any helper exceeds the §3 method-LOC ceiling, and
``scoring_service.py`` can shrink toward the file-LOC budget.

The function is re-exported from ``protea.services.scoring_service``
for backwards compatibility with router and CLI callers.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.core.evaluation import compute_evaluation_data
from protea.core.metrics import compute_cafa_metrics
from protea.core.reranker import predict as _reranker_predict
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel


def _resolve_reranker_metrics_entities(
    session: Session,
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
    evaluation_set_id: uuid.UUID,
) -> tuple[PredictionSet, RerankerModel, Any]:
    """Resolve PS + RerankerModel + EvaluationSet, raising on any miss.

    Returned ``EvaluationSet`` is the ORM row (typed ``Any`` to keep
    the import local; only this helper needs it). Imports
    ``EntityNotFoundError`` lazily to avoid a circular dependency
    with ``scoring_service`` (which re-exports this module's
    orchestrator).
    """
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.services.scoring_service import EntityNotFoundError

    ps = session.get(PredictionSet, prediction_set_id)
    if ps is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    rm = session.get(RerankerModel, reranker_id)
    if rm is None:
        raise EntityNotFoundError("RerankerModel", reranker_id)
    es = session.get(EvaluationSet, evaluation_set_id)
    if es is None:
        raise EntityNotFoundError("EvaluationSet", evaluation_set_id)
    return ps, rm, es


def _load_eval_data_for_reranker_metrics(
    session: Session, ps: PredictionSet, es: Any
) -> Any:
    """Reuse persisted ground-truth when available; otherwise compute on the fly.

    Covers the ``mode=reconciled`` case where eval-time annotation
    snapshots differ from ``ps.ontology_snapshot_id``. Legacy
    same-snapshot rows fall through to ``compute_evaluation_data``.
    """
    from protea.core.evaluation import load_evaluation_data_for_set

    if es.groundtruth_uri:
        eval_data, _pivot_id = load_evaluation_data_for_set(session, es)
        return eval_data
    return compute_evaluation_data(
        session,
        old_annotation_set_id=es.old_annotation_set_id,
        new_annotation_set_id=es.new_annotation_set_id,
        ontology_snapshot_id=ps.ontology_snapshot_id,
    )


def _materialise_reranker_records(
    session: Session, prediction_set_id: uuid.UUID
) -> list[dict[str, Any]]:
    """Stream ``GOPrediction`` rows into reranker-ready record dicts.

    Omits the ``label`` key on purpose so the booster ``predict``
    falls into its alignment branch (mirrors the convention in
    ``score_predictions_with_reranker``).
    """
    records: list[dict[str, Any]] = []
    for pred, go_id in (
        session.query(GOPrediction, GOTerm.go_id)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .yield_per(5000)
    ):
        records.append(
            {
                "protein_accession": pred.protein_accession,
                "go_id": go_id,
                "distance": pred.distance,
                "qualifier": pred.qualifier or "",
                "evidence_code": pred.evidence_code or "",
                "identity_nw": pred.identity_nw,
                "similarity_nw": pred.similarity_nw,
                "alignment_score_nw": pred.alignment_score_nw,
                "gaps_pct_nw": pred.gaps_pct_nw,
                "alignment_length_nw": pred.alignment_length_nw,
                "identity_sw": pred.identity_sw,
                "similarity_sw": pred.similarity_sw,
                "alignment_score_sw": pred.alignment_score_sw,
                "gaps_pct_sw": pred.gaps_pct_sw,
                "alignment_length_sw": pred.alignment_length_sw,
                "length_query": pred.length_query,
                "length_ref": pred.length_ref,
                "query_taxonomy_id": pred.query_taxonomy_id,
                "ref_taxonomy_id": pred.ref_taxonomy_id,
                "taxonomic_lca": pred.taxonomic_lca,
                "taxonomic_distance": pred.taxonomic_distance,
                "taxonomic_common_ancestors": pred.taxonomic_common_ancestors,
                "taxonomic_relation": pred.taxonomic_relation or "",
                "vote_count": pred.vote_count,
                "k_position": pred.k_position,
                "go_term_frequency": pred.go_term_frequency,
                "ref_annotation_density": pred.ref_annotation_density,
                "neighbor_distance_std": pred.neighbor_distance_std,
            }
        )
    return records


def _empty_reranker_metrics_response(
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
    reranker_name: str,
    category: str,
) -> dict[str, Any]:
    """Short-circuit response for an empty PredictionSet."""
    return {
        "prediction_set_id": str(prediction_set_id),
        "reranker_id": str(reranker_id),
        "reranker_name": reranker_name,
        "category": category,
        "fmax": 0.0,
        "auc_pr": 0.0,
        "n_predictions": 0,
        "curve": [],
    }


def _score_records_with_booster(
    rm: RerankerModel, records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Load the booster and run inference; pair scores back with PG-coords.

    Booster load + scoring stay inside the same session scope so
    ``rm``'s lazy columns (``model_data`` / ``artifact_uri``) are
    resolved against the live session before downstream numeric
    work begins. ``load_booster`` is imported lazily to avoid a
    circular dependency with ``scoring_service``.
    """
    import pandas as pd

    from protea.services.scoring_service import load_booster

    model = load_booster(rm)
    df = pd.DataFrame(records)
    scores = _reranker_predict(model, df)
    return [
        {
            "protein_accession": records[i]["protein_accession"],
            "go_id": records[i]["go_id"],
            "score": float(scores[i]),
        }
        for i in range(len(records))
    ]


def _format_reranker_metrics_response(
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
    reranker_name: str,
    metrics: Any,
) -> dict[str, Any]:
    """Shape the response to match the legacy endpoint contract."""
    return {
        "prediction_set_id": str(prediction_set_id),
        "reranker_id": str(reranker_id),
        "reranker_name": reranker_name,
        **metrics.summary(),
        "curve": [
            {
                "threshold": p.threshold,
                "precision": p.precision,
                "recall": p.recall,
                "f1": p.f1,
            }
            for p in metrics.curve
        ],
    }


def compute_reranker_metrics_data(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
    evaluation_set_id: uuid.UUID,
    category: str,
) -> dict[str, Any]:
    """Compute CAFA Fmax + AUC-PR using re-ranker scores instead of a ScoringConfig.

    Validates entities, materialises the prediction record set, loads
    the booster, scores each prediction, runs CAFA evaluation against
    the temporal ground truth of the EvaluationSet. Reuses the
    persisted ground-truth artifact when available; falls back to
    on-the-fly ``compute_evaluation_data`` for legacy same-snapshot
    rows. Empty record sets short-circuit.

    Raises
    ------
    EntityNotFoundError
        Any of ``PredictionSet`` / ``RerankerModel`` / ``EvaluationSet``
        does not exist.
    BoosterUnavailableError
        The RerankerModel row exists but no booster bytes are reachable.
    """
    ps, rm, es = _resolve_reranker_metrics_entities(
        session, prediction_set_id, reranker_id, evaluation_set_id
    )
    eval_data = _load_eval_data_for_reranker_metrics(session, ps, es)
    records = _materialise_reranker_records(session, prediction_set_id)

    if not records:
        return _empty_reranker_metrics_response(
            prediction_set_id, reranker_id, rm.name, category
        )

    scored = _score_records_with_booster(rm, records)
    metrics = compute_cafa_metrics(scored, eval_data, category=category)
    return _format_reranker_metrics_response(
        prediction_set_id, reranker_id, rm.name, metrics
    )
