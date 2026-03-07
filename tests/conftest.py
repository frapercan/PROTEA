from __future__ import annotations

import os
import subprocess
import time
import uuid

import pytest


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def _docker_exists() -> bool:
    try:
        _run(["docker", "version"])
        return True
    except Exception:
        return False


def _wait_ready(container: str, user: str, db: str, timeout_s: int = 60) -> None:
    start = time.time()
    while True:
        proc = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", user, "-d", db],
            text=True,
            capture_output=True,
        )
        if proc.returncode == 0:
            return
        if time.time() - start > timeout_s:
            logs = subprocess.run(["docker", "logs", container], text=True, capture_output=True)
            raise RuntimeError(
                f"Postgres not ready after {timeout_s}s.\n\npg_isready:\n{proc.stdout}\n{proc.stderr}\n\nlogs:\n{logs.stdout}\n{logs.stderr}"
            )
        time.sleep(1)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--with-postgres",
        action="store_true",
        default=False,
        help="Start a temporary Postgres (pgvector) container for integration tests.",
    )


@pytest.fixture(scope="session")
def postgres_url(pytestconfig: pytest.Config) -> str:
    if not pytestconfig.getoption("--with-postgres"):
        pytest.skip("Pass --with-postgres to run integration tests with a temporary Postgres container.")

    if not _docker_exists():
        pytest.skip("Docker is not available; cannot start Postgres container.")

    image = os.getenv("PROTEA_PG_IMAGE", "pgvector/pgvector:pg16")
    user = os.getenv("PROTEA_PG_USER", "usuario")
    password = os.getenv("PROTEA_PG_PASSWORD", "clave")
    db = os.getenv("PROTEA_PG_DB", "BioData")

    host_port = os.getenv("PROTEA_PG_PORT")
    if host_port is None:
        host_port = str(55000 + (uuid.uuid4().int % 1000))

    container = f"protea-pgtest-{uuid.uuid4().hex[:8]}"

    _run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "-e",
            f"POSTGRES_USER={user}",
            "-e",
            f"POSTGRES_PASSWORD={password}",
            "-e",
            f"POSTGRES_DB={db}",
            "-p",
            f"{host_port}:5432",
            image,
        ]
    )

    try:
        _wait_ready(container, user, db, timeout_s=int(os.getenv("PROTEA_PG_TIMEOUT", "60")))

        # Enable pgvector (optional, but harmless)
        subprocess.run(
            ["docker", "exec", container, "psql", "-U", user, "-d", db, "-c", "CREATE EXTENSION IF NOT EXISTS vector;"],
            text=True,
            capture_output=True,
        )

        url = f"postgresql+psycopg://{user}:{password}@localhost:{host_port}/{db}"

        # IMPORTANT: yield, not return (so cleanup happens after tests finish)
        yield url

    finally:
        subprocess.run(["docker", "rm", "-f", container], text=True, capture_output=True)
