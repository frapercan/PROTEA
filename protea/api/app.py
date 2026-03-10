# protea/api/app.py
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from protea.api.routers import admin as admin_router
from protea.api.routers import annotations as annotations_router
from protea.api.routers import embeddings as embeddings_router
from protea.api.routers import jobs as jobs_router
from protea.api.routers import maintenance as maintenance_router
from protea.api.routers import proteins as proteins_router
from protea.api.routers import query_sets as query_sets_router
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings


def create_app(project_root: Path | None = None) -> FastAPI:
    if project_root is None:
        project_root = Path(__file__).resolve().parents[2]

    settings = load_settings(project_root)
    factory = build_session_factory(settings.db_url)

    app = FastAPI(title="ProTea API")
    app.state.session_factory = factory
    app.state.amqp_url = settings.amqp_url

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(jobs_router.router)
    app.include_router(proteins_router.router)
    app.include_router(annotations_router.router)
    app.include_router(embeddings_router.router)
    app.include_router(query_sets_router.router)
    app.include_router(maintenance_router.router)
    app.include_router(admin_router.router)

    sphinx_build = project_root / "docs" / "build" / "html"
    if sphinx_build.exists():
        app.mount("/sphinx", StaticFiles(directory=sphinx_build, html=True), name="sphinx")
    return app
