"""Reranker-model loader extracted from ``run_cafa_evaluation.execute``.

Resolves stored :class:`RerankerModel` rows into the nested
``{setting → aspect → {"model": str, "cat_codes": dict|None}}``
bundle the rest of the operation consumes. Supports both the legacy
inline ``model_data`` blob and the ``artifact_uri`` (artifact-store)
path that lab-trained boosters use.

The function takes the operation's ``emit`` callback as a parameter
so it can keep the same audit-trail events (`reranker_loaded`)
without coupling the helper module to the worker's event types.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.reranker import load_reranker
from protea.infrastructure.orm.models.embedding.reranker_model import (
    RerankerModel as RerankerModelORM,
)
from protea.infrastructure.settings import load_settings as _load_settings_for_reranker
from protea.infrastructure.storage import get_artifact_store as _get_store_for_reranker

_ASPECT_KEY_TO_CHAR = {"bpo": "P", "mfo": "F", "cco": "C"}


def resolve_reranker_model_bundle(rm: RerankerModelORM) -> dict[str, Any]:
    """Return ``{"model": str, "cat_codes": dict|None}`` from either the
    legacy inline blob or the ``artifact_uri`` cache.

    Boosters trained by the lab and imported via
    ``/reranker-models/import`` only set ``artifact_uri`` and leave
    ``model_data`` NULL, so the operation must transparently support
    both paths.

    ``cat_codes`` (if present in the imported run.json under
    ``__categorical_codes__``) is the lab's per-column sorted-unique
    string vocabulary, used at predict time to reproduce the encoding
    seen during training. Without it, ``reranker_predict`` falls back
    to ``pd.factorize`` over the inference batch — which silently
    produces the wrong codes for per-aspect inference and tanks the
    LK / PK fmax. See :func:`protea.core.reranker.predict`.
    """
    if rm.model_data:
        model_str = rm.model_data
    elif rm.artifact_uri:
        project_root = Path(__file__).resolve().parents[3]
        store = _get_store_for_reranker(_load_settings_for_reranker(project_root))
        booster = load_reranker(
            rm.artifact_uri,
            feature_schema_sha=rm.feature_schema_sha or rm.name,
            store=store,
        )
        model_str = booster.model_to_string()
    else:
        raise ValueError(
            f"RerankerModel {rm.id} has no booster — both ``model_data`` "
            f"(legacy inline) and ``artifact_uri`` (artifact-store path) are NULL."
        )
    cat_codes = (rm.metrics or {}).get("__categorical_codes__")
    return {"model": model_str, "cat_codes": cat_codes}


def load_reranker_models_for_payload(
    session: Session,
    *,
    rerankers_nested: dict[str, dict[str, str]] | None,
    reranker_id_nk: str | None,
    reranker_id_lk: str | None,
    reranker_id_pk: str | None,
    emit: EmitFn,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, str]] | None]:
    """Resolve every payload reranker reference into the nested bundle dict.

    Two branches:
      1. ``rerankers_nested`` (new payload field): per-category +
         per-aspect mapping, one model per (NK/LK/PK, P/F/C) cell.
      2. Legacy flat fields ``reranker_id_{nk,lk,pk}``: one model per
         category, applied to all aspects (key ``""``).

    Returns ``(reranker_models, reranker_config_snapshot)`` where the
    snapshot is the nested-form representation suitable for persisting
    in ``EvaluationResult.reranker_config`` (``None`` for the legacy
    flat path).
    """
    reranker_models: dict[str, dict[str, dict[str, Any]]] = {}
    reranker_config_snapshot: dict[str, dict[str, str]] | None = None

    if rerankers_nested:
        reranker_config_snapshot = {}
        for cat_key, aspect_map in rerankers_nested.items():
            setting = cat_key.upper()
            reranker_models[setting] = {}
            reranker_config_snapshot[cat_key] = {}
            for aspect_key, rid_str in aspect_map.items():
                rid = uuid.UUID(rid_str)
                rm = session.get(RerankerModelORM, rid)
                if rm is None:
                    raise ValueError(f"RerankerModel {rid_str} not found")
                aspect_char = _ASPECT_KEY_TO_CHAR.get(aspect_key, aspect_key)
                reranker_models[setting][aspect_char] = resolve_reranker_model_bundle(rm)
                reranker_config_snapshot[cat_key][aspect_key] = rid_str
                emit(
                    "run_cafa_evaluation.reranker_loaded",
                    None,
                    {
                        "setting": setting,
                        "aspect": aspect_key,
                        "reranker_id": str(rid),
                        "name": rm.name,
                    },
                    "info",
                )
    else:
        for setting, field in [
            ("NK", reranker_id_nk),
            ("LK", reranker_id_lk),
            ("PK", reranker_id_pk),
        ]:
            if field:
                rid = uuid.UUID(field)
                rm = session.get(RerankerModelORM, rid)
                if rm is None:
                    raise ValueError(f"RerankerModel {field} not found")
                reranker_models[setting] = {"": resolve_reranker_model_bundle(rm)}
                emit(
                    "run_cafa_evaluation.reranker_loaded",
                    None,
                    {"setting": setting, "reranker_id": str(rid), "name": rm.name},
                    "info",
                )

    return reranker_models, reranker_config_snapshot
