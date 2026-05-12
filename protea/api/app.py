# protea/api/app.py
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from protea.api.bearer import assert_bearer_config
from protea.api.middleware import VisitorCounterMiddleware
from protea.api.problem_details import (
    install_problem_openapi_schema,
    register_problem_handlers,
)
from protea.api.rate_limit import install_rate_limiter
from protea.api.routers import admin as admin_router
from protea.api.routers import annotate as annotate_router
from protea.api.routers import annotations as annotations_router
from protea.api.routers import auth_api_keys as auth_api_keys_router
from protea.api.routers import benchmark as benchmark_router
from protea.api.routers import datasets as datasets_router
from protea.api.routers import embeddings as embeddings_router
from protea.api.routers import experiment_runs as experiment_runs_router
from protea.api.routers import jobs as jobs_router
from protea.api.routers import maintenance as maintenance_router
from protea.api.routers import proteins as proteins_router
from protea.api.routers import query_sets as query_sets_router
from protea.api.routers import registry as registry_router
from protea.api.routers import reranker_models as reranker_models_router
from protea.api.routers import scoring as scoring_router
from protea.api.routers import showcase as showcase_router
from protea.api.routers import stack as stack_router
from protea.api.routers import support as support_router
from protea.core.operation_catalog import build_operation_registry
from protea.infrastructure.benchmark_config import load_benchmark_config
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings

_API_DESCRIPTION = (
    "**PROTEA** — Protein Representation and Ontology-Term Enrichment Analysis.\n\n"
    "Manages the full pipeline from UniProt sequence ingestion through GPU embedding "
    "computation (ESM-2, ESM3c, T5) to KNN-based GO term prediction.\n\n"
    "All long-running operations are queued via RabbitMQ and tracked as `Job` rows "
    "with a full event audit trail. Use `GET /jobs/{id}/events` to stream real-time progress."
)

_OPENAPI_TAGS: list[dict[str, str]] = [
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
    {"name": "admin", "description": "Destructive admin operations (DB reset). Use with caution."},
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
    {
        "name": "stack",
        "description": (
            "Cross-repository navigation: registry of the eight "
            "repositories that make up the PROTEA stack and a "
            "live aggregate of their open pull requests."
        ),
    },
    {
        "name": "experiment-runs",
        "description": (
            "Per-research-run narrative + provenance anchor "
            "(decision D11). Powers the F8b Experiments page and "
            "the F-EXP campaign tooling."
        ),
    },
    {
        "name": "auth",
        "description": (
            "API key lifecycle (T5.6a) — mint, list, revoke keys "
            "used to authenticate sensitive POSTs."
        ),
    },
]

_ROUTER_MODULES = (
    annotate_router,
    auth_api_keys_router,
    jobs_router,
    proteins_router,
    annotations_router,
    embeddings_router,
    query_sets_router,
    maintenance_router,
    admin_router,
    scoring_router,
    showcase_router,
    benchmark_router,
    support_router,
    datasets_router,
    reranker_models_router,
    registry_router,
    stack_router,
    experiment_runs_router,
)

# T4.1 (D4 accepted 2026-05-06): version prefix for the public API.
# Mount every router under ``/v1/`` so OpenAPI exposes the canonical
# paths; keep an unprefixed alias hidden from the schema for the
# deprecation window so existing frontend / CLI traffic doesn't break.
# When the legacy aliases get retired, drop the second include_router
# call in ``_register_routers``.
_API_VERSION_PREFIX = "/v1"

def _register_middlewares(app: FastAPI, allowed_origins: tuple[str, ...]) -> None:
    """Wire up CORS + visitor counter middlewares.

    ``allowed_origins`` comes from ``Settings.allowed_origins`` (T5.5):
    env ``PROTEA_ALLOWED_ORIGINS`` > YAML ``cors.allowed_origins`` >
    built-in dev default. An empty tuple disables the CORS middleware
    entirely so a fronting proxy can own the policy.
    """
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(allowed_origins),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    # Anonymous visitor counter — writes one row per GET into visitor_event
    # with a daily-rotated-salt hash instead of the IP. Powers the Grafana
    # "unique visitors" dashboard.
    app.add_middleware(VisitorCounterMiddleware)


def _register_health_endpoints(app: FastAPI, factory, settings) -> None:
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


def _register_routers(app: FastAPI) -> None:
    """Mount each router under ``/v1/`` (canonical) and at the root path
    (deprecated alias hidden from OpenAPI).

    The dual-include strategy lets existing clients keep hitting the
    unprefixed routes during the deprecation window while the OpenAPI
    schema only advertises the versioned paths. Health endpoints stay
    at the root by convention (handled by ``_register_health_endpoints``).
    """
    for module in _ROUTER_MODULES:
        app.include_router(module.router, prefix=_API_VERSION_PREFIX)
        # Legacy alias — same routes mounted at ``/`` so frontends and
        # CLIs that haven't been updated keep working. ``include_in_schema``
        # off so OpenAPI / Swagger only surface the canonical ``/v1`` paths.
        app.include_router(module.router, include_in_schema=False)


def _mount_sibling_docs(app: FastAPI, docs_build_root: Path) -> None:
    for repo_dir in sorted(p for p in docs_build_root.iterdir() if p.is_dir()):
        if repo_dir.name == "html":
            continue
        html_dir = repo_dir / "html"
        if (html_dir / "index.html").exists():
            app.mount(
                f"/docs/{repo_dir.name}",
                StaticFiles(directory=html_dir, html=True),
                name=f"docs-{repo_dir.name}",
            )


def _mount_static_assets(app: FastAPI, project_root: Path) -> None:
    sphinx_build = project_root / "docs" / "build" / "html"
    if sphinx_build.exists():
        app.mount("/sphinx", StaticFiles(directory=sphinx_build, html=True), name="sphinx")

    docs_build_root = project_root / "docs" / "build"
    if docs_build_root.exists():
        _mount_sibling_docs(app, docs_build_root)


def create_app(project_root: Path | None = None) -> FastAPI:
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]

    settings = load_settings(project_root)
    factory = build_session_factory(settings.db_url)

    # T5.6b: fail loudly when the bearer secret is missing in a
    # production-ish configuration so a misconfigured deployment cannot
    # silently 401 every authenticated request.
    assert_bearer_config()

    app = FastAPI(
        title="PROTEA API",
        version="0.1.0",
        description=_API_DESCRIPTION,
        contact={"name": "PROTEA Team", "email": "contact@protea.example.org"},
        openapi_tags=_OPENAPI_TAGS,
    )
    app.state.session_factory = factory
    app.state.amqp_url = settings.amqp_url
    app.state.artifacts_dir = settings.artifacts_dir
    app.state.operation_registry = build_operation_registry()
    app.state.benchmark_config = load_benchmark_config(project_root)

    _register_middlewares(app, settings.allowed_origins)
    # T5.6b: install the slowapi limiter BEFORE the router exception
    # handlers so the 429 handler resolves through the same RFC 7807
    # plumbing as the rest of the API.
    install_rate_limiter(app)
    register_problem_handlers(app)
    install_problem_openapi_schema(app)
    _register_health_endpoints(app, factory, settings)
    _register_routers(app)
    _mount_static_assets(app, project_root)

    return app
