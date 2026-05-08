"""Reranker-pipeline helpers for ``scoring_service``.

The orchestrator ``score_predictions_with_reranker`` lives here split
into private helpers (validate / materialise) so neither it nor any
helper exceeds the §3 method-LOC ceiling, and ``scoring_service.py``
can shrink toward the file-LOC budget.

The function is re-exported from ``protea.services.scoring_service``
for backwards compatibility with existing router and CLI callers.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.core.reranker import predict as _reranker_predict
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel


def _validate_score_request(
    session: Session,
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
) -> RerankerModel:
    """Resolve PS + RerankerModel, raising on either miss.

    Returns the RerankerModel ORM row so the caller can pass it to
    ``load_booster``. Imports ``EntityNotFoundError`` lazily to avoid
    the circular dependency with ``scoring_service`` (which re-exports
    this module's orchestrator).
    """
    from protea.services.scoring_service import EntityNotFoundError

    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    rm = session.get(RerankerModel, reranker_id)
    if rm is None:
        raise EntityNotFoundError("RerankerModel", reranker_id)
    return rm


def _materialise_score_records(
    session: Session, prediction_set_id: uuid.UUID
) -> list[dict[str, Any]]:
    """Stream every ``GOPrediction`` row + its GO term + aspect into record dicts.

    Mirrors the schema expected by the LightGBM booster's alignment
    branch. The ``label`` column is intentionally omitted: presence
    routes ``predict`` through ``prepare_dataset`` which assumes the
    full training column set; absence selects the inference branch
    that fills missing v6 features as NaN.
    """
    records: list[dict[str, Any]] = []
    for pred, go_id, aspect in (
        session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
        .yield_per(5000)
    ):
        records.append(
            {
                "protein_accession": pred.protein_accession,
                "go_id": go_id,
                "aspect": aspect or "",
                "distance": pred.distance,
                "ref_protein_accession": pred.ref_protein_accession or "",
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


def score_predictions_with_reranker(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    reranker_id: uuid.UUID,
) -> Any:
    """Validate entities, load the booster, score every GOPrediction.

    Returns a sorted ``pandas.DataFrame`` (descending ``reranker_score``
    within each protein) ready for TSV emission, or an empty DataFrame
    when the prediction set has no rows.

    Materialises the full record set in memory; this matches the
    existing endpoint's behaviour (the LightGBM batch predict needs
    a single matrix).

    Raises
    ------
    EntityNotFoundError
        Either ``PredictionSet`` or ``RerankerModel`` does not exist.
    BoosterUnavailableError
        The RerankerModel row exists but no booster bytes are reachable.
    """
    import pandas as pd

    from protea.services.scoring_service import load_booster

    rm = _validate_score_request(session, prediction_set_id, reranker_id)
    model = load_booster(rm)
    records = _materialise_score_records(session, prediction_set_id)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["reranker_score"] = _reranker_predict(model, df)
    return df.sort_values(
        ["protein_accession", "reranker_score"],
        ascending=[True, False],
    )
