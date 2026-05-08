"""Streaming-TSV generators extracted from ``scoring_service``.

These pure functions own the per-row formatting + ``yield`` loop for
the three TSV streams the scoring router serves: scored, training,
and re-ranker outputs. The service layer keeps re-exports for
back-compat with router/CLI imports.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session  # noqa: F401  (kept for type-hint clarity)

from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.session import session_scope
from protea.services._scoring_training_helpers import (
    TRAINING_TSV_COLUMNS as _TRAINING_TSV_COLUMNS,
)
from protea.services._scoring_training_helpers import (
    format_training_row as _format_training_row,
)

_SCORED_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "score",
    "distance",
    "ref_protein_accession",
    "evidence_code",
    "qualifier",
    "identity_nw",
    "identity_sw",
    "taxonomic_distance",
    "neighbor_vote_fraction",
)


_RERANK_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "aspect",
    "reranker_score",
    "distance",
    "ref_protein_accession",
    "evidence_code",
    "qualifier",
)


def _format_optional(value: Any) -> str:
    """Render ``None`` as the empty string; otherwise stringify."""
    return "" if value is None else str(value)


def iter_scored_predictions(
    factory: Any,
    *,
    prediction_set_id: uuid.UUID,
    config_snap: ScoringConfig,
    min_score: float | None = None,
    accession: str | None = None,
) -> Any:
    """Yield TSV rows (as bytes) of scored predictions.

    Opens its own session inside the generator so the route's initial
    validation phase can close its session before streaming starts —
    avoids holding a DB connection open for the duration of the
    response. The first yielded chunk is the header line; one row
    per GOPrediction follows.
    """
    header = "\t".join(_SCORED_TSV_COLUMNS) + "\n"
    yield header.encode()

    with session_scope(factory) as session:
        q = (
            session.query(GOPrediction, GOTerm.go_id)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == prediction_set_id)
        )
        if accession:
            q = q.filter(GOPrediction.protein_accession == accession)

        for pred, go_id in q.yield_per(1000):
            pred_dict = {
                "distance": pred.distance,
                "identity_nw": pred.identity_nw,
                "identity_sw": pred.identity_sw,
                "evidence_code": pred.evidence_code,
                "taxonomic_distance": pred.taxonomic_distance,
                "neighbor_vote_fraction": pred.neighbor_vote_fraction,
            }
            score = compute_score(pred_dict, config_snap)
            if min_score is not None and score < min_score:
                continue

            row = (
                "\t".join(
                    [
                        pred.protein_accession,
                        go_id,
                        str(score),
                        _format_optional(pred.distance),
                        pred.ref_protein_accession or "",
                        pred.evidence_code or "",
                        pred.qualifier or "",
                        _format_optional(pred.identity_nw),
                        _format_optional(pred.identity_sw),
                        _format_optional(pred.taxonomic_distance),
                        _format_optional(pred.neighbor_vote_fraction),
                    ]
                )
                + "\n"
            )
            yield row.encode()


def iter_training_data(
    factory: Any,
    *,
    prediction_set_id: uuid.UUID,
    gt_pairs: set[tuple[str, str]],
) -> Any:
    """Yield TSV rows (as bytes) of labeled training data for the re-ranker.

    Streaming generator over GOPrediction rows; opens its own session
    inside so the caller can close the validation session before
    streaming starts. Each row carries the canonical GOPrediction
    feature vector plus a binary ``label`` (1 if the
    ``(protein_accession, go_id)`` pair is in ``gt_pairs``, else 0).
    Row formatting is delegated to ``_format_training_row`` in
    ``_scoring_training_helpers`` so the orchestrator stays under
    the §3 method-LOC ceiling.
    """
    yield ("\t".join(_TRAINING_TSV_COLUMNS) + "\n").encode()

    with session_scope(factory) as session:
        q = (
            session.query(GOPrediction, GOTerm.go_id, GOTerm.aspect)
            .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
            .filter(GOPrediction.prediction_set_id == prediction_set_id)
        )
        for pred, go_id, aspect in q.yield_per(1000):
            label = 1 if (pred.protein_accession, go_id) in gt_pairs else 0
            yield (_format_training_row(pred, go_id, aspect, label) + "\n").encode()


def iter_reranked_predictions_tsv(
    df: Any,
    *,
    min_score: float | None = None,
) -> Any:
    """Yield TSV rows (as bytes) from the scored DataFrame produced by
    :func:`score_predictions_with_reranker`.

    Empty input emits a header-only response (matches the legacy
    endpoint's "no predictions" shape).
    """
    import pandas as pd

    yield ("\t".join(_RERANK_TSV_COLUMNS) + "\n").encode()
    if df is None or df.empty:
        return

    for _, row in df.iterrows():
        if min_score is not None and row["reranker_score"] < min_score:
            continue
        line = (
            "\t".join(
                [
                    str(row["protein_accession"]),
                    str(row["go_id"]),
                    str(row["aspect"]),
                    f"{row['reranker_score']:.6f}",
                    str(row["distance"]) if pd.notna(row["distance"]) else "",
                    str(row["ref_protein_accession"]),
                    str(row["evidence_code"]),
                    str(row["qualifier"]),
                ]
            )
            + "\n"
        )
        yield line.encode()
