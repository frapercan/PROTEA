"""Re-ranker model registry.

``POST /reranker-models/import`` accepts a trained booster from the
``protea-reranker-lab`` (or any offline trainer), uploads it to the
configured artifact store, and inserts a ``RerankerModel`` row linked
back to the ``Dataset`` it was trained on. This replaces the in-PROTEA
LightGBM training path (see Phase 4 of the decoupling plan).

Both multipart and JSON-by-reference flows are supported:

* **multipart**: lab sends ``model.txt`` + ``spec.yaml`` + ``run.json``
  inline. Server uploads the booster to ``rerankers/<run_id>/model.txt``.
  Simpler for dev.
* **by-reference**: lab pre-uploads ``model.txt`` to MinIO under its
  own key and POSTs JSON with ``artifact_uri`` + ``run_json`` +
  ``spec_yaml`` text. Cleaner for prod.

Both flows share ``_register_model`` so the DB shape is identical.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any, NamedTuple

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.core.schema_sha_v2 import maybe_v2
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


@dataclass(frozen=True)
class _RerankerRegistration:
    """Bundle of inputs ``_register_model`` accepts.

    Groups the nine non-session fields the multipart and by-reference
    endpoints both fill so the registration helper signature stays
    under flake8-bugbear's parameter ceiling.
    """

    name: str
    artifact_uri: str
    run: dict[str, Any]
    spec_yaml_text: str
    dataset_id_override: str | None
    external_source: str | None
    prediction_set_id: str | None
    evaluation_set_id: str | None
    force: bool


router = APIRouter(prefix="/reranker-models", tags=["reranker-models"])


def _parse_cell(cell: str | None) -> tuple[str, str | None]:
    """Parse ``training.cell`` from spec.yaml into ``(category, aspect)``.

    Accepts ``"pk"``, ``"pk-bpo"``, etc. Falls back to ``("pk", None)``;
    category is NOT NULL on RerankerModel.
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
      1. ``compute_feature_schema_sha(families, drop_features)``: the
         family-aware sha the lab writes into ``run.features``.
      2. The sha the lab already computed and recorded in
         ``run.features.feature_schema_sha``: used when the lab contracts
         helper is not importable in this image (so branch 1 is skipped)
         but the run carries the precomputed value. This keeps the
         universal-booster path from landing a NULL ``feature_schema_sha``
         on the ``RerankerModel`` row.
      3. The dataset's ``schema_sha``: fallback for older runs that
         didn't record ``families_enabled``.
    """
    features = run.get("features", {}) or {}
    families: list[str] | None = features.get("families_enabled") or run.get("dataset", {}).get(
        "feature_families"
    )
    drop_features: list[str] = features.get("drop_features") or []
    if families and compute_feature_schema_sha is not None:
        return compute_feature_schema_sha(families, drop_features or None)
    recorded = features.get("feature_schema_sha")
    if recorded:
        return str(recorded)
    return run.get("dataset", {}).get("schema_sha")


def _extract_feature_selection(run: dict[str, Any]) -> dict[str, Any] | None:
    """Pull the feature-selection block out of the lab's ``run.features``.

    Returns the families enabled/available, dropped features, and the
    resolved feature count so the reranker-model view can show exactly
    which feature families fed the booster. ``families_enabled=None`` is
    the lab's "all available families" sentinel and is kept as-is.
    """
    features = run.get("features", {}) or {}
    if not features:
        return None
    return {
        "families_enabled": features.get("families_enabled"),
        "families_available": features.get("families_available") or [],
        "drop_features": features.get("drop_features") or [],
        "feature_count": features.get("feature_count"),
    }


def _resolve_optional_fk(
    session: Session,
    raw_id: str | None,
    pk_column: Any,
) -> uuid.UUID | None:
    """Resolve an optional UUID FK against ``pk_column``.

    Lab runs may carry FKs to entities that no longer exist in this
    PROTEA instance (DB resets, different deployment, etc.). NULL the
    column rather than 500'ing; the booster itself is still valid,
    only the back-references to local entities are unresolvable.
    """
    if not raw_id:
        return None
    candidate = uuid.UUID(raw_id)
    return candidate if session.query(pk_column).filter(pk_column == candidate).first() else None


def _resolve_dataset_uuid(
    session: Session,
    dataset: dict[str, Any],
    override: str | None,
) -> uuid.UUID | None:
    """Pick the explicit override, otherwise look up by lab's dataset name."""
    if override:
        return uuid.UUID(override)
    dataset_name = dataset.get("name")
    if not dataset_name:
        return None
    row = session.query(Dataset).filter(Dataset.name == dataset_name).first()
    return row.id if row is not None else None


def _evict_existing_or_409(
    session: Session,
    name: str,
    force: bool,
) -> None:
    """Delete an existing ``RerankerModel`` row when ``force=True``; otherwise 409."""
    existing = session.query(RerankerModel).filter(RerankerModel.name == name).first()
    if existing is None:
        return
    if not force:
        raise HTTPException(
            status_code=409,
            detail=f"RerankerModel name={name!r} already exists (id={existing.id})",
        )
    session.delete(existing)
    session.flush()


def _enrich_metrics(model: RerankerModel, run: dict[str, Any]) -> None:
    """Stash reserved side-channel keys into ``model.metrics`` in-place.

    Categorical code maps (``__categorical_codes__``) and the
    feature-selection block (``__feature_selection__``) are stored under
    reserved keys so the reranker-model view can render them without
    re-reading run.json. ``families_enabled=None`` is the lab's
    "all available" sentinel and is preserved verbatim.
    """
    cat_codes = run.get("categorical_codes")
    fs = _extract_feature_selection(run)
    if not cat_codes and fs is None:
        return
    m = dict(model.metrics or {})
    if cat_codes:
        m["__categorical_codes__"] = cat_codes
    if fs is not None:
        m["__feature_selection__"] = fs
    model.metrics = m


def _register_model(
    session: Session,
    reg: _RerankerRegistration,
) -> uuid.UUID:
    _evict_existing_or_409(session, reg.name, reg.force)

    category, aspect = _parse_cell(_extract_cell_from_spec(reg.spec_yaml_text))
    dataset = reg.run.get("dataset", {}) or {}
    dataset_uuid = _resolve_dataset_uuid(session, dataset, reg.dataset_id_override)
    embedding_config_id = _resolve_optional_fk(
        session, dataset.get("embedding_config_id"), EmbeddingConfig.id
    )
    ontology_snapshot_id = _resolve_optional_fk(
        session, dataset.get("ontology_snapshot_id"), OntologySnapshot.id
    )
    feature_schema_sha = _compute_feature_schema_sha(reg.run)

    model = RerankerModel(
        name=reg.name,
        prediction_set_id=uuid.UUID(reg.prediction_set_id) if reg.prediction_set_id else None,
        evaluation_set_id=uuid.UUID(reg.evaluation_set_id) if reg.evaluation_set_id else None,
        category=category,
        aspect=aspect,
        model_data=None,
        artifact_uri=reg.artifact_uri,
        feature_schema_sha=feature_schema_sha,
        # T1.6 (ADR D10) dual-write gated by
        # ``PROTEA_SCHEMA_SHA_V2_WRITE_ENABLED``; off by default until
        # the backfill + read-path flip ships in a later slice.
        schema_sha_v2=maybe_v2(feature_schema_sha),
        embedding_config_id=embedding_config_id,
        ontology_snapshot_id=ontology_snapshot_id,
        producer_version=dataset.get("producer_version"),
        producer_git_sha=dataset.get("producer_git_sha"),
        spec_yaml=reg.spec_yaml_text,
        metrics=reg.run.get("metrics", {}) or {},
        feature_importance=reg.run.get("feature_importance", {}) or {},
        dataset_id=dataset_uuid,
        external_source=reg.external_source,
    )
    _enrich_metrics(model, reg.run)
    session.add(model)
    session.flush()
    return model.id


class _RerankerImportFiles(NamedTuple):
    """Three uploaded artefacts mirroring the lab's runs/<name>/ layout."""

    model_file: UploadFile
    spec_yaml: UploadFile
    run_json: UploadFile


def _reranker_import_files_dep(
    model_file: UploadFile = File(..., description="LightGBM model.txt"),
    spec_yaml: UploadFile = File(..., description="ExperimentSpec spec.yaml"),
    run_json: UploadFile = File(..., description="Lab run.json with metrics + provenance"),
) -> _RerankerImportFiles:
    """FastAPI dep collecting the three multipart files."""
    return _RerankerImportFiles(model_file=model_file, spec_yaml=spec_yaml, run_json=run_json)


class _RerankerImportFields(NamedTuple):
    """Optional Form overrides on ``POST /reranker-models/import``."""

    name: str | None
    dataset_id: str | None
    external_source: str | None
    prediction_set_id: str | None
    evaluation_set_id: str | None
    force: bool


def _reranker_import_fields_dep(
    name: str | None = Form(default=None, description="Override RerankerModel.name"),
    dataset_id: str | None = Form(default=None, description="Override linked Dataset UUID"),
    external_source: str | None = Form(default=None),
    prediction_set_id: str | None = Form(default=None),
    evaluation_set_id: str | None = Form(default=None),
    force: bool = Form(default=False),
) -> _RerankerImportFields:
    """FastAPI dep collecting the six optional Form overrides."""
    return _RerankerImportFields(
        name=name,
        dataset_id=dataset_id,
        external_source=external_source,
        prediction_set_id=prediction_set_id,
        evaluation_set_id=evaluation_set_id,
        force=force,
    )


@router.post(
    "/import",
    status_code=201,
    summary="Import a lab-trained booster",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
async def import_reranker_model_multipart(
    files: _RerankerImportFiles = Depends(_reranker_import_files_dep),
    fields: _RerankerImportFields = Depends(_reranker_import_fields_dep),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Upload a trained booster and register a ``RerankerModel`` row.

    The three files (``model.txt``, ``spec.yaml``, ``run.json``) mirror
    the artefacts produced by ``protea-reranker-lab`` under
    ``runs/<name>/``. Wire format unchanged: the FastAPI deps expose
    every File/Form field as a discrete multipart part.
    """
    model_bytes = await files.model_file.read()
    spec_text = (await files.spec_yaml.read()).decode("utf-8")
    try:
        run = json.loads((await files.run_json.read()).decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"run.json is not valid JSON: {exc}") from exc

    run_id = run.get("run_id") or (files.model_file.filename or "unknown").split(".")[0]
    resolved_name = fields.name or run_id

    settings = load_settings(_resolve_project_root())
    store = get_artifact_store(settings)
    artifact_key = f"rerankers/{run_id}/model.txt"
    artifact_uri = store.put(artifact_key, model_bytes)

    with session_scope(factory) as session:
        model_id = _register_model(
            session,
            _RerankerRegistration(
                name=resolved_name,
                artifact_uri=artifact_uri,
                run=run,
                spec_yaml_text=spec_text,
                dataset_id_override=fields.dataset_id,
                external_source=fields.external_source,
                prediction_set_id=fields.prediction_set_id,
                evaluation_set_id=fields.evaluation_set_id,
                force=fields.force,
            ),
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

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "artifact_uri": "s3://protea-rerankers/runs/r1/model.txt",
                "spec_yaml": "# ExperimentSpec contents\nname: r1\nfeature_families: [embedding, alignment]\n",
                "run": {
                    "run_id": "r1",
                    "metrics": {"fmax": 0.5427},
                    "feature_schema_sha": "ab12cd34ef56",
                },
                "name": "r1-k5-bench-v1",
                "dataset_id": "00000000-0000-0000-0000-000000000003",
                "external_source": "protea-reranker-lab@cec8ccd",
                "prediction_set_id": "00000000-0000-0000-0000-000000000004",
                "evaluation_set_id": "00000000-0000-0000-0000-000000000005",
                "force": False,
            }
        },
    }

    artifact_uri: str = Field(
        ...,
        min_length=1,
        description=(
            "Storage URI of the booster blob (``s3://…/model.txt`` or "
            "``file://…/model.txt``). PROTEA registers this verbatim "
            "without re-reading or copying the artifact."
        ),
    )
    spec_yaml: str = Field(..., description="Full spec.yaml text")
    run: dict[str, Any] = Field(..., description="Parsed run.json")
    name: str | None = Field(
        default=None,
        description=(
            "Override row name. When omitted, falls back to the "
            "``run.run_id`` from the run.json payload."
        ),
    )
    dataset_id: str | None = Field(
        default=None,
        description=(
            "UUID of the source ``Dataset`` row this booster was "
            "trained on. Used to track the lab → PROTEA lineage."
        ),
    )
    external_source: str | None = Field(
        default=None,
        description=(
            "Free-form provenance tag (e.g. "
            "``protea-reranker-lab@<git-sha>``) for boosters trained "
            "outside PROTEA."
        ),
    )
    prediction_set_id: str | None = Field(
        default=None,
        description=(
            "Optional UUID of a ``PredictionSet`` whose vote pattern "
            "the booster targets; nullable so abstract boosters can be "
            "registered without binding to a specific run."
        ),
    )
    evaluation_set_id: str | None = Field(
        default=None,
        description=(
            "Optional UUID of an ``EvaluationSet`` the booster was "
            "tuned against (NK / LK / PK delta)."
        ),
    )
    force: bool = Field(
        default=False,
        description=(
            "When true, allow overwriting an existing row with the "
            "same ``name``. Default false rejects duplicates with 409."
        ),
    )


@router.post(
    "/import-by-reference",
    status_code=201,
    summary="Register a booster already in the artifact store",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def import_reranker_model_by_reference(
    body: ImportRerankerByReferenceRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Register a ``RerankerModel`` whose booster is already in MinIO.

    The lab uploads the booster directly (faster, no double-hop) and
    POSTs the URI + run.json + spec.yaml here. Server does not re-read
    the artifact; it trusts the URI.
    """
    run_id = body.run.get("run_id") or "unknown"
    resolved_name = body.name or run_id

    with session_scope(factory) as session:
        model_id = _register_model(
            session,
            _RerankerRegistration(
                name=resolved_name,
                artifact_uri=body.artifact_uri,
                run=body.run,
                spec_yaml_text=body.spec_yaml,
                dataset_id_override=body.dataset_id,
                external_source=body.external_source,
                prediction_set_id=body.prediction_set_id,
                evaluation_set_id=body.evaluation_set_id,
                force=body.force,
            ),
        )

    return {"id": str(model_id), "name": resolved_name, "artifact_uri": body.artifact_uri}


def _resolve_project_root():
    from pathlib import Path

    # protea/api/routers/reranker_models.py → parents[3] = repo root
    return Path(__file__).resolve().parents[3]
