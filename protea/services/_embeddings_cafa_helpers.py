"""CAFA-export helpers for ``embeddings_service``.

The export streams a set's stored scores in CAFA format, exactly as the
evaluator writes them: the same reader (``_method_score_predictions``), the
same ordering and the same four decimals, and, for a window, the same targets
from the window's own truth. It used to score each pair itself, as
``max(0, 1 - distance)``, a formula no other part of the platform used, and to
take a window's targets from another truth builder, without PK; both were
parallel doors, closed on 2026-11-20 with the closure's single path.

Domain exceptions (``EntityNotFoundError``) are imported lazily inside the
functions that raise them to avoid a circular dependency with the parent
module.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.core.operations._method_score_predictions import (
    proteins_of,
    refuse_unless_scored,
    scored_table,
)
from protea.core.operations._run_cafa_artifacts import WritePredictionsContext
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.session import session_scope


def prepare_cafa_export(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    eval_id: uuid.UUID | None,
) -> set[str]:
    """Preflight the export: the set exists and stores scores; which proteins go out.

    With ``eval_id``, the window's targets (NK, LK and PK) from the window's own
    truth, as the evaluator reads them; without it, every protein the set
    stores a score for.

    Raises :class:`EntityNotFoundError` for a missing PredictionSet or
    EvaluationSet, and ``MethodScoreMissing`` for a set of candidates without
    scores, so the router can answer 404 and 409.
    """
    from protea.core.evaluation import load_evaluation_data_for_set
    from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
    from protea.services.embeddings_service import EntityNotFoundError

    if session.get(PredictionSet, prediction_set_id) is None:
        raise EntityNotFoundError("PredictionSet", prediction_set_id)
    refuse_unless_scored(session, prediction_set_id)
    if eval_id is None:
        return proteins_of(session, prediction_set_id)
    window = session.get(EvaluationSet, eval_id)
    if window is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    data, _pivot = load_evaluation_data_for_set(session, window)
    return set(data.nk) | set(data.lk) | set(data.pk)


def iter_predictions_cafa_tsv(
    factory: sessionmaker[Session],
    *,
    prediction_set_id: uuid.UUID,
    proteins: set[str],
    aspect: str | None = None,
) -> Iterator[str]:
    """Stream ``protein_accession\\tgo_id\\tscore``: the stored scores, as the evaluator writes them."""
    with session_scope(factory) as session:
        table = scored_table(
            session,
            WritePredictionsContext(
                pred_set_id=prediction_set_id, delta_proteins=proteins, path=""
            ),
        )
        keep: set[str] | None = None
        if aspect:
            keep = {
                row[0]
                for row in session.execute(
                    text(
                        "SELECT t.go_id FROM go_term t JOIN prediction_set p "
                        "ON p.ontology_snapshot_id = t.ontology_snapshot_id "
                        "WHERE p.id = :s AND t.aspect = :a"
                    ),
                    {"s": prediction_set_id, "a": aspect.upper()},
                )
            }
        for protein, go_id, score in zip(
            table["protein_accession"].to_pylist(),
            table["go_id"].to_pylist(),
            table["score"].to_pylist(),
            strict=True,
        ):
            if keep is None or go_id in keep:
                yield f"{protein}\t{go_id}\t{score}\n"
