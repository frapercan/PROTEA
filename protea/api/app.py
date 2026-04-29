# protea/api/app.py
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from protea.api.middleware import VisitorCounterMiddleware
from protea.api.routers import admin as admin_router
from protea.api.routers import annotate as annotate_router
from protea.api.routers import annotations as annotations_router
from protea.api.routers import benchmark as benchmark_router
from protea.api.routers import datasets as datasets_router
from protea.api.routers import embeddings as embeddings_router
from protea.api.routers import jobs as jobs_router
from protea.api.routers import maintenance as maintenance_router
from protea.api.routers import proteins as proteins_router
from protea.api.routers import query_sets as query_sets_router
from protea.api.routers import reranker_models as reranker_models_router
from protea.api.routers import scoring as scoring_router
from protea.api.routers import showcase as showcase_router
from protea.api.routers import support as support_router
from protea.core.operation_catalog import build_operation_registry
from protea.infrastructure.benchmark_config import load_benchmark_config
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings


def create_app(project_root: Path | None = None) -> FastAPI:
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]

    settings = load_settings(project_root)
    factory = build_session_factory(settings.db_url)

    app = FastAPI(
        title="PROTEA API",
        version="0.1.0",
        description=(
            "**PROTEA** — Protein Representation and Ontology-Term Enrichment Analysis.\n\n"
            "Manages the full pipeline from UniProt sequence ingestion through GPU embedding "
            "computation (ESM-2, ESM3c, T5) to KNN-based GO term prediction.\n\n"
            "All long-running operations are queued via RabbitMQ and tracked as `Job` rows "
            "with a full event audit trail. Use `GET /jobs/{id}/events` to stream real-time progress."
        ),
        contact={"name": "PROTEA Team", "email": "contact@protea.example.org"},
        openapi_tags=[
            {
                "name": "jobs",
                "description": "Job queue lifecycle — create, monitor, and cancel operations.",
            },
            {"name": "proteins", "description": "UniProt protein lookup and aggregate statistics."},
            {
                "name": "annotations",
                "description": "GO ontology snapshots, annotation sets, and GO subgraph queries.",
            },
            {
                "name": "embeddings",
                "description": "Embedding configs, GPU compute jobs, and prediction sets management.",
            },
            {
                "name": "query-sets",
                "description": "User-uploaded FASTA datasets for custom prediction queries.",
            },
            {
                "name": "maintenance",
                "description": "Housekeeping — identify and remove orphaned sequences or embeddings.",
            },
            {
                "name": "admin",
                "description": "Destructive admin operations (DB reset). Use with caution.",
            },
            {
                "name": "scoring",
                "description": "Scoring configs, scored prediction export, and CAFA metrics.",
            },
            {
                "name": "benchmark",
                "description": (
                    "Per-embedding / per-stage Fmax matrix across every "
                    "evaluation result. Powers the /benchmark page in the UI."
                ),
            },
            {"name": "support", "description": "Community thumbs-up and comments."},
            {
                "name": "annotate",
                "description": "One-click protein annotation — upload FASTA, auto-run the full pipeline.",
            },
            {
                "name": "datasets",
                "description": (
                    "Frozen re-ranker datasets — enqueue export jobs, "
                    "list/fetch registered dumps, resolve URIs for the lab."
                ),
            },
            {
                "name": "reranker-models",
                "description": (
                    "Register lab-trained LightGBM boosters — multipart "
                    "upload or by-reference import of artefacts already in MinIO."
                ),
            },
        ],
    )
    app.state.session_factory = factory
    app.state.amqp_url = settings.amqp_url
    app.state.artifacts_dir = settings.artifacts_dir
    app.state.operation_registry = build_operation_registry()
    app.state.benchmark_config = load_benchmark_config(project_root)

    allowed_origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://protea.ngrok.app",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # Anonymous visitor counter — writes one row per GET into visitor_event
    # with a daily-rotated-salt hash instead of the IP. Powers the Grafana
    # "unique visitors" dashboard.
    app.add_middleware(VisitorCounterMiddleware)

    @app.get("/health", tags=["health"])
    def health_check() -> dict[str, str]:
        """Liveness probe — returns 200 if the API process is up."""
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    def readiness_check() -> dict[str, str]:
        """Readiness probe — verifies database, RabbitMQ, and (if configured) MinIO.

        The artifact-store check is load-bearing: ``POST /datasets`` and
        ``/reranker-models/import`` silently misbehave if MinIO is
        configured but unreachable (``Dataset`` rows would be written
        against the local filesystem). Failing readiness here keeps
        docker / k8s from routing traffic until the store is back.
        """
        from sqlalchemy import text

        from protea.infrastructure.session import session_scope

        with session_scope(factory) as session:
            session.execute(text("SELECT 1"))

        import pika

        try:
            conn = pika.BlockingConnection(pika.URLParameters(settings.amqp_url))
            conn.close()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"RabbitMQ unreachable: {exc}") from exc

        if (settings.storage_backend or "local").lower() == "minio":
            from protea.infrastructure.storage import get_artifact_store
            from protea.infrastructure.storage.factory import ArtifactStoreUnavailable

            try:
                get_artifact_store(settings)
            except ArtifactStoreUnavailable as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc

        return {"status": "ready"}

    app.include_router(annotate_router.router)
    app.include_router(jobs_router.router)
    app.include_router(proteins_router.router)
    app.include_router(annotations_router.router)
    app.include_router(embeddings_router.router)
    app.include_router(query_sets_router.router)
    app.include_router(maintenance_router.router)
    app.include_router(admin_router.router)
    app.include_router(scoring_router.router)
    app.include_router(showcase_router.router)
    app.include_router(benchmark_router.router)
    app.include_router(support_router.router)
    app.include_router(datasets_router.router)
    app.include_router(reranker_models_router.router)

    sphinx_build = project_root / "docs" / "build" / "html"
    if sphinx_build.exists():
        app.mount("/sphinx", StaticFiles(directory=sphinx_build, html=True), name="sphinx")

    static_dir = project_root / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    return app
