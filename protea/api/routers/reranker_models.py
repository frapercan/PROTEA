"""Re-ranker model registry.

``POST /reranker-models/import`` accepts a trained booster from the
``protea-reranker-lab`` (or any offline trainer), uploads it to the
configured artifact store, and inserts a ``RerankerModel`` row linked
back to the ``Dataset`` it was trained on. This replaces the in-PROTEA
LightGBM training path (see Phase 4 of the decoupling plan).

Both multipart and JSON-by-reference flows are supported:

* **multipart** — lab sends ``model.txt`` + ``spec.yaml`` + ``run.json``
  inline. Server uploads the booster to ``rerankers/<run_id>/model.txt``.
  Simpler for dev.
* **by-reference** — lab pre-uploads ``model.txt`` to MinIO under its
  own key and POSTs JSON with ``artifact_uri`` + ``run_json`` +
  ``spec_yaml`` text. Cleaner for prod.

Both flows share ``_register_model`` so the DB shape is identical.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.session import session_scope
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

try:
    from protea_reranker_lab.contracts import compute_feature_schema_sha
except Exception:  # pragma: no cover — lab is an editable dev dep
    compute_feature_schema_sha = None  # type: ignore[assignment]


router = APIRouter(prefix="/reranker-models", tags=["reranker-models"])


def _parse_cell(cell: str | None) -> tuple[str, str | None]:
    """Parse ``training.cell`` from spec.yaml into ``(category, aspect)``.

    Accepts ``"pk"``, ``"pk-bpo"``, etc. Falls back to ``("pk", None)``
    — category is NOT NULL on RerankerModel.
    """
    if not cell:
        return ("pk", None)
    cell = cell.strip().lower()
    if "-" in cell:
        cat, asp = cell.split("-", 1)
        return (cat, asp)
    return (cell, None)


def _extract_cell_from_spec(spec_yaml_text: str) -> str | None:
    in_training = False
    for raw in spec_yaml_text.splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" "):
            in_training = line.strip().rstrip(":") == "training"
            continue
        if in_training:
            stripped = line.strip()
            if stripped.startswith("cell:"):
                return stripped.split(":", 1)[1].strip().strip('"').strip("'") or None
    return None


def _compute_feature_schema_sha(run: dict[str, Any]) -> str | None:
    """Derive the feature fingerprint recorded for the run.

    Preference order:
      1. ``compute_feature_schema_sha(families, drop_features)`` — the
         family-aware sha the lab writes into ``run.features``.
      2. The dataset's ``schema_sha`` — fallback for older runs that
         didn't record ``families_enabled``.
    """
    features = run.get("features", {}) or {}
    families: list[str] | None = features.get("families_enabled") or run.get(
        "dataset", {}
    ).get("feature_families")
    drop_features: list[str] = features.get("drop_features") or []
    if families and compute_feature_schema_sha is not None:
        return compute_feature_schema_sha(families, drop_features or None)
    return run.get("dataset", {}).get("schema_sha")


def _register_model(
    *,
    session: Session,
    name: str,
    artifact_uri: str,
    run: dict[str, Any],
    spec_yaml_text: str,
    dataset_id_override: str | None,
    external_source: str | None,
    prediction_set_id: str | None,
    evaluation_set_id: str | None,
    force: bool,
) -> uuid.UUID:
    existing = session.query(RerankerModel).filter(RerankerModel.name == name).first()
    if existing is not None:
        if not force:
            raise HTTPException(
                status_code=409,
                detail=f"RerankerModel name={name!r} already exists (id={existing.id})",
            )
        session.delete(existing)
        session.flush()

    category, aspect = _parse_cell(_extract_cell_from_spec(spec_yaml_text))

    dataset = run.get("dataset", {}) or {}
    dataset_uuid: uuid.UUID | None = None
    if dataset_id_override:
        dataset_uuid = uuid.UUID(dataset_id_override)
    else:
        dataset_name = dataset.get("name")
        if dataset_name:
            row = session.query(Dataset).filter(Dataset.name == dataset_name).first()
            if row is not None:
                dataset_uuid = row.id

    feature_schema_sha = _compute_feature_schema_sha(run)
    embedding_config_id_raw = dataset.get("embedding_config_id")
    ontology_snapshot_id_raw = dataset.get("ontology_snapshot_id")

    # Lab runs may carry FKs to entities that no longer exist in this PROTEA
    # instance (DB resets, different deployment, etc.). NULL them rather than
    # 500'ing — the booster itself is still valid; only the back-references
    # to local entities are unresolvable.
    embedding_config_id: uuid.UUID | None = None
    if embedding_config_id_raw:
        candidate = uuid.UUID(embedding_config_id_raw)
        if session.query(EmbeddingConfig.id).filter(EmbeddingConfig.id == candidate).first():
            embedding_config_id = candidate

    ontology_snapshot_id: uuid.UUID | None = None
    if ontology_snapshot_id_raw:
        candidate = uuid.UUID(ontology_snapshot_id_raw)
        if session.query(OntologySnapshot.id).filter(OntologySnapshot.id == candidate).first():
            ontology_snapshot_id = candidate

    model = RerankerModel(
        name=name,
        prediction_set_id=uuid.UUID(prediction_set_id) if prediction_set_id else None,
        evaluation_set_id=uuid.UUID(evaluation_set_id) if evaluation_set_id else None,
        category=category,
        aspect=aspect,
        model_data=None,
        artifact_uri=artifact_uri,
        feature_schema_sha=feature_schema_sha,
        embedding_config_id=embedding_config_id,
        ontology_snapshot_id=ontology_snapshot_id,
        producer_version=dataset.get("producer_version"),
        producer_git_sha=dataset.get("producer_git_sha"),
        spec_yaml=spec_yaml_text,
        metrics=run.get("metrics", {}) or {},
        feature_importance=run.get("feature_importance", {}) or {},
        # Categorical code maps live in metrics under a reserved key so the
        # predict path can replicate the lab's sorted-unique encoding instead
        # of falling back to ``pd.factorize`` (first-seen order, which gives
        # different codes than training and silently corrupts LK/PK scores).
        dataset_id=dataset_uuid,
        external_source=external_source,
    )
    # Stash categorical_codes in metrics if the lab supplied them. Stored as
    # ``metrics["__categorical_codes__"]`` to keep the column scalar-shaped
    # without bloating spec_yaml.
    cat_codes = run.get("categorical_codes")
    if cat_codes:
        m = dict(model.metrics or {})
        m["__categorical_codes__"] = cat_codes
        model.metrics = m
    session.add(model)
    session.flush()
    return model.id


@router.post("/import", status_code=201, summary="Import a lab-trained booster")
async def import_reranker_model_multipart(
    model_file: UploadFile = File(..., description="LightGBM model.txt"),
    spec_yaml: UploadFile = File(..., description="ExperimentSpec spec.yaml"),
    run_json: UploadFile = File(..., description="Lab run.json with metrics + provenance"),
    name: str | None = Form(default=None, description="Override RerankerModel.name"),
    dataset_id: str | None = Form(default=None, description="Override linked Dataset UUID"),
    external_source: str | None = Form(default=None),
    prediction_set_id: str | None = Form(default=None),
    evaluation_set_id: str | None = Form(default=None),
    force: bool = Form(default=False),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Upload a trained booster and register a ``RerankerModel`` row.

    The three files (``model.txt``, ``spec.yaml``, ``run.json``) mirror
    the artefacts produced by ``protea-reranker-lab`` under ``runs/<name>/``.
    """
    model_bytes = await model_file.read()
    spec_text = (await spec_yaml.read()).decode("utf-8")
    try:
        run = json.loads((await run_json.read()).decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"run.json is not valid JSON: {exc}") from exc

    run_id = run.get("run_id") or (model_file.filename or "unknown").split(".")[0]
    resolved_name = name or run_id

    settings = load_settings(_resolve_project_root())
    store = get_artifact_store(settings)
    artifact_key = f"rerankers/{run_id}/model.txt"
    artifact_uri = store.put(artifact_key, model_bytes)

    with session_scope(factory) as session:
        model_id = _register_model(
            session=session,
            name=resolved_name,
            artifact_uri=artifact_uri,
            run=run,
            spec_yaml_text=spec_text,
            dataset_id_override=dataset_id,
            external_source=external_source,
            prediction_set_id=prediction_set_id,
            evaluation_set_id=evaluation_set_id,
            force=force,
        )

    return {
        "id": str(model_id),
        "name": resolved_name,
        "artifact_uri": artifact_uri,
        "storage_backend": settings.storage_backend,
    }


class ImportRerankerByReferenceRequest(BaseModel):
    """Body for ``POST /reranker-models/import-by-reference``.

    Use this when the lab has already uploaded ``model.txt`` to MinIO
    under its own key and just needs PROTEA to register the URI.
    """

    artifact_uri: str = Field(..., min_length=1)
    spec_yaml: str = Field(..., description="Full spec.yaml text")
    run: dict[str, Any] = Field(..., description="Parsed run.json")
    name: str | None = None
    dataset_id: str | None = None
    external_source: str | None = None
    prediction_set_id: str | None = None
    evaluation_set_id: str | None = None
    force: bool = False


@router.post(
    "/import-by-reference",
    status_code=201,
    summary="Register a booster already in the artifact store",
)
def import_reranker_model_by_reference(
    body: ImportRerankerByReferenceRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Register a ``RerankerModel`` whose booster is already in MinIO.

    The lab uploads the booster directly (faster, no double-hop) and
    POSTs the URI + run.json + spec.yaml here. Server does not re-read
    the artifact — it trusts the URI.
    """
    run_id = body.run.get("run_id") or "unknown"
    resolved_name = body.name or run_id

    with session_scope(factory) as session:
        model_id = _register_model(
            session=session,
            name=resolved_name,
            artifact_uri=body.artifact_uri,
            run=body.run,
            spec_yaml_text=body.spec_yaml,
            dataset_id_override=body.dataset_id,
            external_source=body.external_source,
            prediction_set_id=body.prediction_set_id,
            evaluation_set_id=body.evaluation_set_id,
            force=body.force,
        )

    return {"id": str(model_id), "name": resolved_name, "artifact_uri": body.artifact_uri}


def _resolve_project_root():
    from pathlib import Path

    # protea/api/routers/reranker_models.py → parents[3] = repo root
    return Path(__file__).resolve().parents[3]
