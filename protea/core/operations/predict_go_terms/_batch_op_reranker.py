"""Reranker scoring mixin for ``PredictGOTermsBatchOperation``.

Extracted from the monolithic ``predict_go_terms.py`` as part of T2B.6.
Contains the live-vs-expected schema SHA reconciliation plus the
booster load + score path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.operations.predict_go_terms._common import (
    PredictGOTermsBatchPayload,
)
from protea.core.reranker import (
    apply_reranker,
    infer_active_feature_families,
    load_reranker,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class _RerankerMixin:
    """LightGBM reranker scoring methods for the batch op."""

    def _resolve_live_schema_sha(
        self,
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> str | None:
        """Compute the live feature-schema SHA via ``protea_contracts``.

        Returns the SHA on success or ``None`` when the contracts module
        is unavailable (the booster is skipped silently in that case so
        production images without the dev dep can still serve KNN
        distance ordering).
        """
        try:
            from protea_contracts import compute_feature_schema_sha
        except Exception as exc:
            emit(
                "reranker.skipped",
                None,
                {
                    "reason": "contracts_unavailable",
                    "reranker_model_id": p.reranker_model_id,
                    "error": str(exc),
                },
                "warning",
            )
            return None
        live_families = infer_active_feature_families(
            compute_alignments=p.compute_alignments,
            compute_taxonomy=p.compute_taxonomy,
            compute_v6_features=p.compute_v6_features,
        )
        return compute_feature_schema_sha(live_families)

    def _score_with_reranker(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
    ) -> Any:
        """Load the booster, score predictions in-place, return the score array."""
        import pandas as pd

        project_root = Path(__file__).resolve().parents[3]
        settings = load_settings(project_root)
        store = get_artifact_store(settings)
        booster = load_reranker(
            p.reranker_artifact_uri,
            feature_schema_sha=p.reranker_feature_schema_sha,
            store=store,
        )
        self._attach_go_term_aspect(session, prediction_dicts)
        df = pd.DataFrame(prediction_dicts)
        scores = apply_reranker(df, booster)
        for rec, score in zip(prediction_dicts, scores.tolist(), strict=True):
            rec["reranker_score"] = float(score)
        return scores

    def _apply_reranker_if_aligned(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> dict[str, Any] | None:
        """Score ``prediction_dicts`` with the configured reranker.

        The booster is skipped (never crashed) whenever any precondition
        fails: missing artifact context, contracts module unavailable,
        or live schema SHA != expected. On success ``reranker_score``
        lands on every prediction dict in memory (not persisted) and
        the method returns per-batch summary stats.
        """
        if not (p.reranker_artifact_uri and p.reranker_feature_schema_sha):
            emit(
                "reranker.skipped",
                None,
                {"reason": "missing_artifact_context", "reranker_model_id": p.reranker_model_id},
                "warning",
            )
            return None
        live_sha = self._resolve_live_schema_sha(p, emit)
        if live_sha is None:
            return None
        if live_sha != p.reranker_feature_schema_sha:
            emit(
                "reranker.schema_mismatch",
                None,
                {
                    "reranker_model_id": p.reranker_model_id,
                    "expected_sha": p.reranker_feature_schema_sha,
                    "live_sha": live_sha,
                },
                "error",
            )
            return {
                "applied": False,
                "skipped_reason": "schema_mismatch",
                "expected_sha": p.reranker_feature_schema_sha,
                "live_sha": live_sha,
            }
        scores = self._score_with_reranker(session, prediction_dicts, p)
        if scores.size == 0:
            return {"applied": True, "rows": 0}
        return {
            "applied": True,
            "rows": int(scores.size),
            "score_min": float(scores.min()),
            "score_max": float(scores.max()),
            "score_mean": float(scores.mean()),
            "feature_schema_sha": live_sha,
        }
