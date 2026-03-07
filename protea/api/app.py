# protea/api/app.py
from __future__ import annotations

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  # <-- ADD

from protea.api.routers import jobs as jobs_router
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
            "http://192.168.1.136:3000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(jobs_router.router)
    return app
