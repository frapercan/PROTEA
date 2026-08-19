# protea/api/app.py
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from protea.api._thesis_pdf import thesis_pdf_path
from protea.api.bearer import assert_bearer_config
from protea.api.middleware import HttpMetricsMiddleware, VisitorCounterMiddleware
from protea.api.problem_details import (
    install_problem_openapi_schema,
    register_problem_handlers,
)
from protea.api.rate_limit import install_rate_limiter
from protea.api.routers import admin as admin_router
from protea.api.routers import annotate as annotate_router
from protea.api.routers import annotations as annotations_router
from protea.api.routers import auth_api_keys as auth_api_keys_router
from protea.api.routers import auth_login as auth_login_router
from protea.api.routers import auth_smtp as auth_smtp_router
from protea.api.routers import auth_user as auth_user_router
from protea.api.routers import benchmark as benchmark_router
from protea.api.routers import datasets as datasets_router
from protea.api.routers import embeddings as embeddings_router
from protea.api.routers import experiment_runs as experiment_runs_router
from protea.api.routers import features as features_router
from protea.api.routers import jobs as jobs_router
from protea.api.routers import jobs_availability as jobs_availability_router
from protea.api.routers import maintenance as maintenance_router
from protea.api.routers import metrics as metrics_router
from protea.api.routers import proteins as proteins_router
from protea.api.routers import proteins_stats as proteins_stats_router
from protea.api.routers import query_sets as query_sets_router
from protea.api.routers import registry as registry_router
from protea.api.routers import reranker_models as reranker_models_router
from protea.api.routers import scoring as scoring_router
from protea.api.routers import showcase as showcase_router
from protea.api.routers import stack as stack_router
from protea.api.routers import strata as strata_router
from protea.api.routers import support as support_router
from protea.api.routers.annotations.sets import (
    ANNOTATION_SETS_TTL_SECONDS,
    prewarm_annotation_sets,
)
from protea.api.routers.annotations.snapshots import (
    SNAPSHOTS_TTL_SECONDS,
    prewarm_snapshots,
)
from protea.api.routers.benchmark import (
    BENCHMARK_EMBEDDINGS_TTL_SECONDS,
    BENCHMARK_MATRIX_TTL_SECONDS,
    prewarm_benchmark_embeddings,
    prewarm_benchmark_matrix,
)
from protea.api.routers.embeddings import (
    EMBEDDING_CONFIGS_TTL_SECONDS,
    PREDICTION_SETS_TTL_SECONDS,
    prewarm_embedding_configs,
    prewarm_prediction_sets,
)
from protea.api.routers.proteins import (
    PROTEIN_STATS_TTL_SECONDS,
    prewarm_protein_stats,
)
from protea.api.routers.proteins_stats import prewarm_all as prewarm_protein_stats_sections
from protea.core.operation_catalog import build_operation_registry
from protea.infrastructure.benchmark_config import load_benchmark_config
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings
from protea.infrastructure.telemetry import build_metric_registry, configure_telemetry

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
    {
        "name": "metrics",
        "description": (
            "Prometheus scrape endpoint (T5.2). Returns process-level "
            "counters and histograms in the standard text exposition "
            "format for the platform Grafana and alerting stack."
        ),
    },
]

_ROUTER_MODULES = (
    annotate_router,
    auth_api_keys_router,
    auth_login_router,
    auth_smtp_router,
    auth_user_router,
    # Mount the GPU-availability route before the main jobs router so the
    # static ``/jobs/gpu-availability`` path resolves ahead of the
    # ``/jobs/{job_id}`` catch-all.
    jobs_availability_router,
    jobs_router,
    proteins_router,
    proteins_stats_router,
    annotations_router,
    embeddings_router,
    query_sets_router,
    maintenance_router,
    admin_router,
    scoring_router,
    showcase_router,
    benchmark_router,
    strata_router,
    support_router,
    datasets_router,
    reranker_models_router,
    registry_router,
    features_router,
    stack_router,
    experiment_runs_router,
    metrics_router,
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

    Wildcard handling: when the resolved list contains ``"*"`` the
    middleware is wired with ``allow_origins=["*"]`` and
    ``allow_credentials=False``. The CORS spec (Fetch §3.2.5) forbids
    credentials + wildcard, and Starlette already refuses to echo the
    wildcard when credentials are on, so we coerce explicitly to make
    the contract visible at registration time instead of at request time.
    """
    if not allowed_origins:
        # Anonymous visitor counter — writes one row per GET into visitor_event
        # with a daily-rotated-salt hash instead of the IP. Powers the Grafana
        # "unique visitors" dashboard.
        app.add_middleware(VisitorCounterMiddleware)
        app.add_middleware(HttpMetricsMiddleware)
        return

    if "*" in allowed_origins:
        # Spec disallows credentials with wildcard; collapse to the
        # canonical permissive policy and turn cookies off.
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    else:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(allowed_origins),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    app.add_middleware(VisitorCounterMiddleware)
    app.add_middleware(HttpMetricsMiddleware)


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


def _register_thesis_pdf(app: FastAPI, project_root: Path) -> None:
    """Serve the thesis PDF from a stable, env-overridable path.

    Decouples the PDF from the frontend build: the file is read at request
    time from :func:`thesis_pdf_path`, so overwriting it at the mounted path
    (``PROTEA_THESIS_PDF_PATH``) updates what the app distributes with no
    rebuild and no restart. The frontend proxies ``/thesis.pdf`` here.
    """

    @app.get("/thesis.pdf", include_in_schema=False)
    def _thesis_pdf() -> FileResponse:
        path = thesis_pdf_path(project_root)
        if path is None:
            raise HTTPException(status_code=404, detail="thesis PDF not available")
        return FileResponse(path, media_type="application/pdf", filename="thesis.pdf")


def _mount_static_assets(app: FastAPI, project_root: Path) -> None:
    # Canonical Sphinx output is `docs/build/html/` (produced by
    # `make html` / `sphinx-build -M html`). Fall back to
    # `docs/build/` when the canonical layout is absent and the
    # flat layout (produced by `sphinx-build -b html src dst`)
    # is present instead, so a deploy with either build flavour
    # serves the docs.
    docs_build_root = project_root / "docs" / "build"
    canonical = docs_build_root / "html"
    sphinx_build: Path | None = None
    if (canonical / "index.html").exists():
        sphinx_build = canonical
    elif (docs_build_root / "index.html").exists():
        sphinx_build = docs_build_root
    if sphinx_build is not None:
        app.mount("/sphinx", StaticFiles(directory=sphinx_build, html=True), name="sphinx")

    if docs_build_root.exists():
        _mount_sibling_docs(app, docs_build_root)


# Refresh cached aggregates one minute before their TTL so the cache is
# never cold from a user's perspective; cold computes take tens of
# seconds (proteins:stats: 30s+, embeddings:prediction-sets: 115s+,
# annotations:sets: 6s+) and overshoot the ngrok 30s upstream deadline.
def _refresh_interval(ttl: float) -> float:
    return max(60.0, ttl - 60.0)


_log = logging.getLogger(__name__)


# Each entry is (label, prewarm_callable, ttl_seconds). The label is used
# for both log messages and the asyncio task name so a hanging refresh
# loop is easy to spot in `asyncio.all_tasks()`. Add new entries here when
# wiring a new cached aggregate that has a slow cold path.
def _prewarm_targets():
    return (
        ("proteins:stats", prewarm_protein_stats, PROTEIN_STATS_TTL_SECONDS),
        (
            "embeddings:prediction-sets",
            prewarm_prediction_sets,
            PREDICTION_SETS_TTL_SECONDS,
        ),
        ("embeddings:configs", prewarm_embedding_configs, EMBEDDING_CONFIGS_TTL_SECONDS),
        ("annotations:snapshots", prewarm_snapshots, SNAPSHOTS_TTL_SECONDS),
        ("annotations:sets", prewarm_annotation_sets, ANNOTATION_SETS_TTL_SECONDS),
        ("proteins:stats:sections", prewarm_protein_stats_sections, 600),
        (
            "benchmark:embeddings",
            prewarm_benchmark_embeddings,
            BENCHMARK_EMBEDDINGS_TTL_SECONDS,
        ),
        ("benchmark:matrix", prewarm_benchmark_matrix, BENCHMARK_MATRIX_TTL_SECONDS),
    )


async def _safe_prewarm(label: str, fn, factory) -> None:  # noqa: ANN001
    try:
        await asyncio.to_thread(fn, factory)
    except Exception as exc:  # pragma: no cover - logged for ops
        _log.warning("%s prewarm failed: %s", label, exc)


def _build_lifespan(factory):  # noqa: ANN001 - sessionmaker factory, mocked in tests
    @asynccontextmanager
    async def _lifespan(_: FastAPI) -> AsyncIterator[None]:
        targets = _prewarm_targets()

        async def _refresh_loop(label: str, fn, ttl: float) -> None:
            # First iteration is the initial prewarm; subsequent iterations
            # refresh on the TTL cadence. Both fire in the background so a
            # slow aggregate (cold count(DISTINCT) over millions of rows,
            # etc) never stalls uvicorn's port-bind. The cache is empty for
            # the first reader of a still-warming endpoint, but with
            # serve-stale-on-error wrappers in the routers no caller sees a
            # 500 in the meantime.
            await _safe_prewarm(label, fn, factory)
            interval = _refresh_interval(ttl)
            while True:
                await asyncio.sleep(interval)
                await _safe_prewarm(label, fn, factory)

        tasks = [
            asyncio.create_task(_refresh_loop(label, fn, ttl), name=f"{label}-refresh")
            for label, fn, ttl in targets
        ]
        # Let the event loop dispatch the just-created tasks before we
        # hand control to the application. Three event-loop ticks is enough
        # for to_thread to flip into its executor; in tests where the
        # prewarm fn is a MagicMock, the mock is invoked synchronously
        # so the assert_called_once_with assertion holds. In production
        # this is a sub-millisecond yield with no startup-latency cost.
        for _tick in range(3):
            await asyncio.sleep(0)
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()

    return _lifespan


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
        lifespan=_build_lifespan(factory),
    )
    app.state.session_factory = factory
    app.state.amqp_url = settings.amqp_url
    app.state.artifacts_dir = settings.artifacts_dir
    app.state.settings = settings
    app.state.operation_registry = build_operation_registry()
    app.state.benchmark_config = load_benchmark_config(project_root)
    # FARM-AUTH.7: expose quota limits so the dependency can read them
    # without importing the full Settings object.
    app.state.user_quota_per_day = settings.user_quota_per_day

    # T5.1a: boot OpenTelemetry before middlewares so FastAPI
    # instrumentation wraps the full middleware chain. Disabled by
    # default; opt in with PROTEA_OTEL_ENABLED=1. See
    # protea/infrastructure/telemetry.py for the env contract.
    app.state.telemetry = configure_telemetry(app)

    # T5.2: build the Prometheus collector registry so /v1/metrics
    # surfaces the five baseline metrics (jobs counter, job /
    # embeddings / predictions histograms, db pool gauge). Returns
    # None when the prometheus_client dependency is absent; the
    # router degrades to 503.
    app.state.metrics = build_metric_registry()

    _register_middlewares(app, settings.allowed_origins)
    # T5.6b: install the slowapi limiter BEFORE the router exception
    # handlers so the 429 handler resolves through the same RFC 7807
    # plumbing as the rest of the API.
    install_rate_limiter(app)
    register_problem_handlers(app)
    install_problem_openapi_schema(app)
    _register_health_endpoints(app, factory, settings)
    _register_routers(app)
    _register_thesis_pdf(app, project_root)
    _mount_static_assets(app, project_root)

    return app
