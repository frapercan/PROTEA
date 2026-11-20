"""The labelled export a reranker is trained on, streamed as TSV.

It used to stream two more outputs, the predictions scored on the fly by a
ScoringConfig and the predictions reranked on the fly, both retired on
2026-11-20 with the closure's single path: a set's scores are made by its
method and read as stored.
"""

from __future__ import annotations

import uuid
from typing import Any

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.session import session_scope
from protea.services._scoring_training_helpers import (
    TRAINING_TSV_COLUMNS as _TRAINING_TSV_COLUMNS,
)
from protea.services._scoring_training_helpers import (
    format_training_row as _format_training_row,
)


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
