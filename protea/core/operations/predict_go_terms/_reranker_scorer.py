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

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.operations.predict_go_terms import _batch_op_reranker as _shim
from protea.core.operations.predict_go_terms._category_dispatch import (
    CATEGORIES,
    attach_query_category,
)
from protea.core.operations.predict_go_terms._common import (
    PredictGOTermsBatchPayload,
)

logger = logging.getLogger(__name__)

AttachAspectFn = Callable[[Session, list[dict[str, Any]]], None]
#: Loader for the per-(protein, aspect) CAFA category, injected so unit
#: tests can stub the DB-bound annotation lookup. Signature mirrors
#: :func:`._category_dispatch.attach_query_category` (op is supplied by the
#: scorer's owner when wired; ``None`` falls back to the module default).
AttachCategoryFn = Callable[
    [Session, "uuid.UUID", list[dict[str, Any]]], dict[str, int]
]


@dataclass(frozen=True)
class _CategoryBinding:
    """One per-category booster pointer (artifact_uri + schema fingerprint)."""

    category: str
    artifact_uri: str
    feature_schema_sha: str


class SchemaShaMismatchError(Exception):
    """A booster's recorded ``feature_schema_sha`` does not match the live schema.

    Raised by :class:`RerankerScorer` before booster load when the
    payload's ``reranker_feature_schema_sha`` (the value persisted on
    the :class:`RerankerModel` row at registration time) disagrees
    with what :func:`protea_contracts.compute_feature_schema_sha`
    returns for the currently active feature families.

    The base worker translates this into ``Job.status=FAILED`` with
    ``Job.error_code="SchemaShaMismatchError"``; the structured
    machine token ``reason="schema_sha_mismatch"`` is carried on both
    the emitted ``reranker.schema_mismatch`` event and the
    :attr:`reason` class attribute so downstream consumers can branch
    on a stable literal.

    FARM-EXP.5 guard: silent prediction with a stale booster layout
    is exactly the failure mode this exception exists to prevent.
    """

    #: Canonical machine-readable reason literal. Carried on every
    #: instance so callers can `getattr(exc, 'reason', None)` without
    #: hardcoding the class name.
    reason: str = "schema_sha_mismatch"

    def __init__(
        self,
        *,
        reranker_model_id: str | None,
        expected_sha: str,
        live_sha: str,
    ) -> None:
        self.reranker_model_id = reranker_model_id
        self.expected_sha = expected_sha
        self.live_sha = live_sha
        super().__init__(
            f"feature_schema_sha mismatch (reason={self.reason}): "
            f"recorded={expected_sha} live={live_sha} "
            f"reranker_model_id={reranker_model_id}. Booster trained "
            "on a different feature layout; refusing to score "
            "(FARM-EXP.5 guard)."
        )


class RerankerScorer:
    """LightGBM reranker scoring collaborator.

    Encapsulates the live-vs-expected feature-schema reconciliation,
    the booster load, and the in-place scoring pass over a batch's
    prediction dicts. Consumed by
    :class:`PredictGOTermsBatchOperation` via constructor injection.
    """

    def __init__(
        self,
        *,
        attach_aspect: AttachAspectFn,
        attach_category: AttachCategoryFn | None = None,
    ) -> None:
        self._attach_aspect = attach_aspect
        self._attach_category = attach_category

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
        """Load the booster, score predictions in-place, return the score array.

        Two scoring shapes are supported:

        * **per-cell** boosters: scored via ``apply_reranker`` over the
          booster's own ``feature_name()`` order (the historical path).
        * **universal** boosters (single, pooled, aspect-conditioned,
          K-augmented): detected by the presence of both ``plm_id`` and
          ``k_context`` in the booster's feature list. These carry injected
          source constants and a fixed categorical vocabulary the generic
          ``apply_reranker`` cannot reproduce, so they are routed through
          :func:`protea.core._universal_reranker.score_universal` with the
          ``categorical_codes`` + plm/k metadata recovered from the model's
          sibling ``run.json`` (F-RERANK-UNIVERSAL inference wiring).
        """
        self._attach_aspect(session, prediction_dicts)
        return self._score_with(
            prediction_dicts,
            artifact_uri=p.reranker_artifact_uri,
            feature_schema_sha=p.reranker_feature_schema_sha,
        )

    def _score_with(
        self,
        prediction_dicts: list[dict[str, Any]],
        *,
        artifact_uri: str,
        feature_schema_sha: str,
    ) -> Any:
        """Load one booster by uri and score the dicts in-place.

        Aspect attachment is the caller's responsibility (it is shared
        across per-category subsets so it runs once, not per booster).
        """
        import pandas as pd

        from protea.core._universal_reranker import (
            is_universal_booster,
            score_universal,
        )

        project_root = Path(__file__).resolve().parents[3]
        settings = _shim.load_settings(project_root)
        store = _shim.get_artifact_store(settings)
        booster = _shim.load_reranker(
            artifact_uri,
            feature_schema_sha=feature_schema_sha,
            store=store,
        )
        df = pd.DataFrame(prediction_dicts)
        if is_universal_booster(booster):
            ctx = self._load_universal_context(artifact_uri, store)
            scores = score_universal(
                booster,
                df,
                categorical_codes=ctx["categorical_codes"],
                plm_id=ctx["plm_id"],
                k_context=ctx["k_context"],
            )
        else:
            scores = _shim.apply_reranker(df, booster)
        for rec, score in zip(prediction_dicts, scores.tolist(), strict=True):
            rec["reranker_score"] = float(score)
        return scores

    @staticmethod
    def _load_universal_context(
        artifact_uri: str,
        store: Any,
    ) -> dict[str, Any]:
        """Recover universal-scoring metadata from the model's sibling run.json.

        Delegates to
        :func:`protea.core._universal_reranker.load_universal_context`, the
        single source of truth shared with the evaluation-artifacts path.
        """
        from protea.core._universal_reranker import load_universal_context

        return load_universal_context(artifact_uri, store)

    @staticmethod
    def _raise_schema_sha_mismatch(
        p: PredictGOTermsBatchPayload,
        live_sha: str,
        emit: EmitFn,
    ) -> None:
        """Emit + log + raise on a feature_schema_sha mismatch.

        FARM-EXP.5 guard: silent skip is unacceptable here. A
        stale-sha booster means the layout it trained on no longer
        matches what inference would compute; running it would
        produce numerically plausible but semantically wrong scores.
        We emit the structured audit event, log to the stdlib
        logger at error level, and raise so the parent Job lands in
        FAILED with ``error_code='SchemaShaMismatchError'`` and the
        canonical ``reason='schema_sha_mismatch'`` token visible on
        the event payload + the exception attribute.
        """
        emit(
            "reranker.schema_mismatch",
            None,
            {
                "reason": SchemaShaMismatchError.reason,
                "reranker_model_id": p.reranker_model_id,
                "expected_sha": p.reranker_feature_schema_sha,
                "live_sha": live_sha,
            },
            "error",
        )
        logger.error(
            "reranker schema_sha mismatch: "
            "reranker_model_id=%s expected_sha=%s live_sha=%s reason=%s",
            p.reranker_model_id,
            p.reranker_feature_schema_sha,
            live_sha,
            SchemaShaMismatchError.reason,
        )
        # TODO(FARM-2.1): once the canonical events table lands,
        # mirror this error into it with the structured fields above
        # instead of (or in addition to) the existing emit channel.
        raise SchemaShaMismatchError(
            reranker_model_id=p.reranker_model_id,
            expected_sha=p.reranker_feature_schema_sha,
            live_sha=live_sha,
        )

    def apply_if_aligned(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> dict[str, Any] | None:
        """Score ``prediction_dicts`` with the configured reranker.

        The booster is skipped silently (returns ``None``) only when a
        precondition makes scoring undefined rather than wrong: the
        operation was dispatched without artifact context, or the
        ``protea_contracts`` module is not importable in this image.

        Schema-sha drift, in contrast, is a HARD failure: when the
        live ``compute_feature_schema_sha(...)`` disagrees with the
        booster's recorded ``feature_schema_sha`` the method emits the
        ``reranker.schema_mismatch`` event at error level, logs to
        the stdlib logger, and raises
        :class:`SchemaShaMismatchError`. The base worker translates
        that exception into ``Job.status=failed`` with
        ``Job.error_code='SchemaShaMismatchError'`` (FARM-EXP.5).

        On success ``reranker_score`` lands on every prediction dict
        in memory (not persisted) and the method returns per-batch
        summary stats.
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
            self._raise_schema_sha_mismatch(p, live_sha, emit)
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

    @staticmethod
    def _category_bindings(
        p: PredictGOTermsBatchPayload,
    ) -> list[_CategoryBinding] | None:
        """Extract the NK / LK / PK bindings from the payload, or ``None``.

        Read defensively via ``getattr`` so an image whose ``protea_contracts``
        predates INT-5 simply falls back to the single-booster path. Returns
        a list of three bindings only when every ``(artifact_uri,
        feature_schema_sha)`` pair is present; otherwise ``None``.
        """
        out: list[_CategoryBinding] = []
        for cat in CATEGORIES:
            prefix = cat.lower()
            uri = getattr(p, f"reranker_{prefix}_artifact_uri", None)
            sha = getattr(p, f"reranker_{prefix}_feature_schema_sha", None)
            if not (uri and sha):
                return None
            out.append(_CategoryBinding(category=cat, artifact_uri=uri, feature_schema_sha=sha))
        return out

    def apply(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> dict[str, Any] | None:
        """Entry point: per-category dispatch when bound, else single booster.

        When the payload carries all three per-category bindings the batch is
        split by each candidate's CAFA category (computed from the protein's
        pre-cutoff experimental annotations) and each subset is scored with the
        matching booster. Otherwise behaviour is identical to
        :meth:`apply_if_aligned` (no regression; default off).
        """
        bindings = self._category_bindings(p)
        if bindings is None:
            return self.apply_if_aligned(session, prediction_dicts, p, emit)
        return self._apply_per_category(session, prediction_dicts, p, bindings, emit)

    def _apply_per_category(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
        p: PredictGOTermsBatchPayload,
        bindings: list[_CategoryBinding],
        emit: EmitFn,
    ) -> dict[str, Any] | None:
        """Split by category and score each subset with its own booster."""
        live_sha = self.resolve_live_schema_sha(p, emit)
        if live_sha is None:
            return None
        self._guard_category_shas(p, bindings, live_sha, emit)
        self._attach_aspect(session, prediction_dicts)
        hist = self._attach_categories(session, p, prediction_dicts)
        scored = 0
        for binding in bindings:
            subset = [r for r in prediction_dicts if r.get("category") == binding.category]
            if not subset:
                continue
            self._score_with(
                subset,
                artifact_uri=binding.artifact_uri,
                feature_schema_sha=binding.feature_schema_sha,
            )
            scored += len(subset)
        emit(
            "reranker.per_category_applied",
            None,
            {"by_category": hist, "rows_scored": scored, "feature_schema_sha": live_sha},
            "info",
        )
        return {"applied": True, "per_category": True, "rows": scored, "by_category": hist}

    @staticmethod
    def _guard_category_shas(
        p: PredictGOTermsBatchPayload,
        bindings: list[_CategoryBinding],
        live_sha: str,
        emit: EmitFn,
    ) -> None:
        """Reject the run if any per-category booster's schema sha drifted.

        Same FARM-EXP.5 guard as the single-booster path: all three boosters
        share the one extended LAFA feature layout, so each recorded
        ``feature_schema_sha`` must equal the live one.
        """
        for binding in bindings:
            if binding.feature_schema_sha != live_sha:
                RerankerScorer._raise_schema_sha_mismatch_for(
                    reranker_model_id=getattr(p, f"reranker_model_id_{binding.category.lower()}", None),
                    expected_sha=binding.feature_schema_sha,
                    live_sha=live_sha,
                    emit=emit,
                )

    def _attach_categories(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        prediction_dicts: list[dict[str, Any]],
    ) -> dict[str, int]:
        """Resolve the per-(protein, aspect) category onto every dict."""
        attach = self._attach_category or _default_attach_category
        return attach(session, uuid.UUID(p.annotation_set_id), prediction_dicts)

    @staticmethod
    def _raise_schema_sha_mismatch_for(
        *,
        reranker_model_id: str | None,
        expected_sha: str,
        live_sha: str,
        emit: EmitFn,
    ) -> None:
        """Emit + log + raise on a per-category feature_schema_sha mismatch."""
        emit(
            "reranker.schema_mismatch",
            None,
            {
                "reason": SchemaShaMismatchError.reason,
                "reranker_model_id": reranker_model_id,
                "expected_sha": expected_sha,
                "live_sha": live_sha,
            },
            "error",
        )
        logger.error(
            "reranker schema_sha mismatch (per-category): "
            "reranker_model_id=%s expected_sha=%s live_sha=%s reason=%s",
            reranker_model_id,
            expected_sha,
            live_sha,
            SchemaShaMismatchError.reason,
        )
        raise SchemaShaMismatchError(
            reranker_model_id=reranker_model_id,
            expected_sha=expected_sha,
            live_sha=live_sha,
        )


def _default_attach_category(
    session: Session,
    annotation_set_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
) -> dict[str, int]:
    """Module-level category attach used when the op was not injected.

    Builds a throwaway feature-loading op only to reuse its
    ``_load_annotations_for`` chunked loader; the category assignment itself
    is pure (:func:`._category_dispatch.attach_query_category`).
    """
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
    )

    op = PredictGOTermsBatchOperation()
    return attach_query_category(op, session, annotation_set_id, prediction_dicts)


__all__ = ["AttachAspectFn", "AttachCategoryFn", "RerankerScorer", "SchemaShaMismatchError"]
