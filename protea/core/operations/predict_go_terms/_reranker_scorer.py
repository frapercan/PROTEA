"""Compositive ``RerankerScorer`` for ``PredictGOTermsBatchOperation``.

Extracted from the T2B.6 ``_RerankerMixin`` as part of T2B.4. The
scoring path now lives in a dedicated class with single responsibility
(load booster, score predictions, emit audit events) and is injected
into the batch operation via the constructor rather than mixed in via
MRO. The mixin module
(:mod:`protea.core.operations.predict_go_terms._batch_op_reranker`)
is retained as a thin re-export shim so tests that patch the legacy
symbol path keep working.

Design notes:

- The scorer is stateless; one instance per operation is fine, but a
  fresh instance per call is also safe.
- The four external function dependencies (``load_settings``,
  ``get_artifact_store``, ``load_reranker``, ``apply_reranker``) are
  resolved at call time via attribute lookup on the shim module
  (:mod:`._batch_op_reranker`). This preserves the existing test
  pattern that monkeypatches those symbols on the shim module path.
- The single cross-module collaborator (the GO-term-aspect attach
  helper) is injected as a callable, so the scorer never reaches
  back into the orchestrator.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.operations.predict_go_terms import _batch_op_reranker as _shim
from protea.core.operations.predict_go_terms._common import (
    PredictGOTermsBatchPayload,
)

AttachAspectFn = Callable[[Session, list[dict[str, Any]]], None]


class RerankerScorer:
    """LightGBM reranker scoring collaborator.

    Encapsulates the live-vs-expected feature-schema reconciliation,
    the booster load, and the in-place scoring pass over a batch's
    prediction dicts. Consumed by
    :class:`PredictGOTermsBatchOperation` via constructor injection.
    """

    def __init__(self, *, attach_aspect: AttachAspectFn) -> None:
        self._attach_aspect = attach_aspect

    def resolve_live_schema_sha(
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
        live_families = _shim.infer_active_feature_families(
            compute_alignments=p.compute_alignments,
            compute_taxonomy=p.compute_taxonomy,
            compute_v6_features=p.compute_v6_features,
        )
        return compute_feature_schema_sha(live_families)

    def score(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
    ) -> Any:
        """Load the booster, score predictions in-place, return the score array."""
        import pandas as pd

        project_root = Path(__file__).resolve().parents[3]
        settings = _shim.load_settings(project_root)
        store = _shim.get_artifact_store(settings)
        booster = _shim.load_reranker(
            p.reranker_artifact_uri,
            feature_schema_sha=p.reranker_feature_schema_sha,
            store=store,
        )
        self._attach_aspect(session, prediction_dicts)
        df = pd.DataFrame(prediction_dicts)
        scores = _shim.apply_reranker(df, booster)
        for rec, score in zip(prediction_dicts, scores.tolist(), strict=True):
            rec["reranker_score"] = float(score)
        return scores

    def apply_if_aligned(
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
        live_sha = self.resolve_live_schema_sha(p, emit)
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
        scores = self.score(session, prediction_dicts, p)
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


__all__ = ["RerankerScorer", "AttachAspectFn"]
