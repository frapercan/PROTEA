"""Shared FastAPI dependency functions for all routers."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from protea.core.contracts.registry import OperationRegistry
from protea.infrastructure.benchmark_config import BenchmarkConfig


def get_session_factory(request: Request) -> sessionmaker[Session]:
    factory = getattr(request.app.state, "session_factory", None)
    if factory is None:
        raise RuntimeError("app.state.session_factory is not set")
    return factory  # type: ignore[no-any-return]


def get_operation_registry(request: Request) -> OperationRegistry:
    reg = getattr(request.app.state, "operation_registry", None)
    if reg is None:
        raise RuntimeError("app.state.operation_registry is not set")
    return reg  # type: ignore[no-any-return]


def get_amqp_url(request: Request) -> str:
    url = getattr(request.app.state, "amqp_url", None)
    if url is None:
        raise RuntimeError("app.state.amqp_url is not set")
    return url  # type: ignore[no-any-return]


def get_artifacts_dir(request: Request) -> Path:
    d = getattr(request.app.state, "artifacts_dir", None)
    if d is None:
        raise RuntimeError("app.state.artifacts_dir is not set")
    return d  # type: ignore[no-any-return]


def get_benchmark_config(request: Request) -> BenchmarkConfig:
    cfg = getattr(request.app.state, "benchmark_config", None)
    if cfg is None:
        raise RuntimeError("app.state.benchmark_config is not set")
    return cfg  # type: ignore[no-any-return]
