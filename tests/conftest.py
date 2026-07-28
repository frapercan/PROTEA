from __future__ import annotations

import os
import subprocess
import uuid

import pytest

from tests.helpers.wait import wait_until

# ---------------------------------------------------------------------------
# Hypothesis profiles (F6.2)
# ---------------------------------------------------------------------------
# Property-based tests live under tests/property/. The profile picked here
# applies to every Hypothesis run in this pytest session.
#
#   default : interactive runs (small example count, randomized seed).
#   ci      : CI runs (derandomize=True, fixed seed, deadline disabled so
#             slow integration boxes do not flake on a Hypothesis timeout).
#
# The CI profile activates when either PROTEA_HYPOTHESIS_PROFILE=ci or the
# generic CI=true env var is set, so GitHub Actions and any local
# ``CI=1 pytest`` invocation get bit-stable property tests.
try:
    from hypothesis import HealthCheck, settings
except ImportError:  # pragma: no cover - hypothesis is a test-only dep
    settings = None  # type: ignore[assignment]

if settings is not None:
    settings.register_profile(
        "ci",
        max_examples=200,
        derandomize=True,
        deadline=None,
        print_blob=True,
        suppress_health_check=[HealthCheck.too_slow],
    )
    settings.register_profile(
        "dev",
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    _profile = os.getenv("PROTEA_HYPOTHESIS_PROFILE")
    if _profile is None and os.getenv("CI", "").lower() in {"1", "true", "yes"}:
        _profile = "ci"
    settings.load_profile(_profile or "dev")


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def _docker_exists() -> bool:
    try:
        _run(["docker", "version"])
        return True
    except Exception:
        return False


def _wait_ready(container: str, user: str, db: str, timeout_s: int = 60) -> None:
    last_proc: subprocess.CompletedProcess[str] | None = None

    def _ready() -> bool:
        nonlocal last_proc
        last_proc = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", user, "-d", db],
            text=True,
            capture_output=True,
        )
        return last_proc.returncode == 0

    try:
        wait_until(_ready, timeout=float(timeout_s), interval=0.25, msg=f"pg_isready {container}")
    except AssertionError as err:
        logs = subprocess.run(["docker", "logs", container], text=True, capture_output=True)
        stdout = last_proc.stdout if last_proc else ""
        stderr = last_proc.stderr if last_proc else ""
        raise RuntimeError(
            f"Postgres not ready after {timeout_s}s.\n\npg_isready:\n{stdout}\n{stderr}\n\nlogs:\n{logs.stdout}\n{logs.stderr}"
        ) from err


@pytest.fixture()
def noop_emit():
    """Shared no-op emit callback for operation tests."""
    return lambda *_args, **_kwargs: None


@pytest.fixture(autouse=True)
def _disable_authn_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable the T5.6a API-key gate for the test session by default.

    Routes wired with ``require_api_key`` (``POST /jobs``, ``/datasets``,
    ``/reranker-models/import*``) would otherwise 401 every smoke-test.
    Tests that want to exercise the gate explicitly re-enable it with
    ``monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "true")``.

    Also sets the environment to "test" so that slowapi rate limits are
    effectively disabled (9999/hour) to avoid hitting quota walls during
    integration test setup and assertions.
    """
    monkeypatch.setenv("PROTEA_AUTHN_REQUIRED", "false")
    monkeypatch.setenv("PROTEA_ENVIRONMENT", "test")


@pytest.fixture(autouse=True)
def _reset_classifier_output_cache() -> None:
    """Clear the P2 process-level classifier-output memo between tests.

    The export classifier-output cache (``classifier_producer``) is
    process-level by design (one export run reuses a protein's output across
    snapshot pairs). Tests that mock the classifier and reuse the same
    accession must each start from an empty cache, so reset it per test.
    """
    from protea.core.classifier_producer import reset_classifier_output_cache

    reset_classifier_output_cache()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--with-postgres",
        action="store_true",
        default=False,
        help="Start a temporary Postgres (pgvector) container for integration tests.",
    )


# Database names that are known dev/prod stores. The *_pg integration tests
# run ``Base.metadata.drop_all(); create_all()`` against whatever URL this
# fixture yields, so an externally-supplied DB pointed at any of these would
# silently wipe a real schema (the live DB has been wiped four times this way).
_PROTECTED_DB_NAMES = frozenset({"protea", "biodata"})

# The dev Postgres listens here. Refuse to run destructive integration tests
# against it unless the operator explicitly opts in.
_DEV_HOST_PORT = ("localhost", "5432")
_DEV_HOST_ALIASES = frozenset({"localhost", "127.0.0.1", "::1", ""})


def _guard_external_db(user: str, host: str, port: str, db: str) -> None:
    """Abort the session if an external DB looks like a real dev/prod store.

    The ``*_pg.py`` integration suite drops and recreates the schema, so a
    misconfigured ``PROTEA_PG_*`` env (e.g. inherited from a sourced ``.env``
    that points at the live Postgres) would destroy a real database. We
    default-deny: refuse when the DB name is a known prod/dev name, or when
    the target is the dev Postgres host:port, unless the operator sets the
    explicit opt-in sentinel ``PROTEA_ALLOW_DESTRUCTIVE_TESTS=1``.
    """
    if os.getenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS") == "1":
        return

    reasons: list[str] = []
    if db.strip().lower() in _PROTECTED_DB_NAMES:
        reasons.append(f"database name {db!r} is a known dev/prod store")
    if host.strip().lower() in _DEV_HOST_ALIASES and port.strip() == _DEV_HOST_PORT[1]:
        reasons.append(
            f"target {host}:{port} is the dev Postgres ({_DEV_HOST_PORT[0]}:{_DEV_HOST_PORT[1]})"
        )

    if reasons:
        pytest.fail(
            "Refusing to run destructive integration tests against what looks "
            "like a real database: "
            + "; ".join(reasons)
            + f" (user={user!r}, db={db!r}, host={host!r}, port={port!r}). "
            "These tests run Base.metadata.drop_all()/create_all() and would "
            "wipe the schema. Point PROTEA_PG_* at a disposable Postgres, or "
            "set PROTEA_ALLOW_DESTRUCTIVE_TESTS=1 to override this guard.",
            pytrace=False,
        )


def _resolve_db_targets() -> list[tuple[str, str, str, str]]:
    """Best-effort (user, host, port, db) for every DB this process might hit.

    Covers BOTH paths that have wiped the live DB: the ``postgres_url`` fixture
    (PROTEA_PG_* env) and direct ``load_settings().db_url`` access from a tracked
    ``system.yaml`` (the 2026-06-10 wipe went through the latter, bypassing the
    fixture guard).
    """
    from pathlib import Path

    from sqlalchemy.engine import make_url

    urls: list[str] = []
    env_url = os.getenv("PROTEA_DB_URL")
    if env_url:
        urls.append(env_url)
    # Only an EXPLICIT system.yaml counts, never the hard-coded default db_url, so
    # CI and agent worktrees without a system.yaml are not falsely flagged.
    root = Path(__file__).resolve().parents[1]
    if (root / "protea" / "config" / "system.yaml").exists():
        try:
            from protea.infrastructure.settings import load_settings

            urls.append(load_settings(root).db_url)
        except Exception:
            pass

    out: list[tuple[str, str, str, str]] = []
    for url in urls:
        try:
            u = make_url(url)
            out.append((u.username or "", u.host or "", str(u.port or ""), u.database or ""))
        except Exception:
            continue
    return out


def pytest_sessionstart(session: pytest.Session) -> None:
    """Abort the whole session if it is configured against the live dev/prod DB.

    PROTEA's suite drops and recreates the schema; pointed at the live Postgres it
    wipes it (it has happened repeatedly). Default-deny: refuse when any resolved
    target is a protected DB name on the dev host:port, unless the operator opts in
    with ``PROTEA_ALLOW_DESTRUCTIVE_TESTS=1``.
    """
    if os.getenv("PROTEA_ALLOW_DESTRUCTIVE_TESTS") == "1":
        return
    for user, host, port, db in _resolve_db_targets():
        live = (
            db.strip().lower() in _PROTECTED_DB_NAMES
            and host.strip().lower() in _DEV_HOST_ALIASES
            and port.strip() in ("", "5432")
        )
        if live:
            raise pytest.UsageError(
                "Refusing to start: the configured database "
                f"({user}@{host}:{port}/{db}) is the live dev/prod store. PROTEA's "
                "tests can DROP/recreate the schema and have wiped it this way. Point "
                "protea/config/system.yaml or PROTEA_DB_URL at a disposable Postgres "
                "(or use --with-postgres), or set PROTEA_ALLOW_DESTRUCTIVE_TESTS=1."
            )


@pytest.fixture(scope="session")
def postgres_url(pytestconfig: pytest.Config) -> str:
    if not pytestconfig.getoption("--with-postgres"):
        pytest.skip(
            "Pass --with-postgres to run integration tests with a temporary Postgres container."
        )

    user = os.getenv("PROTEA_PG_USER", "usuario")
    password = os.getenv("PROTEA_PG_PASSWORD", "clave")
    db = os.getenv("PROTEA_PG_DB", "BioData")
    host_port = os.getenv("PROTEA_PG_PORT")

    # If all connection params are provided via env vars, assume an external DB
    # is already running (e.g. a GitHub Actions service container) and skip Docker.
    external_db = all(
        [
            os.getenv("PROTEA_PG_USER"),
            os.getenv("PROTEA_PG_PASSWORD"),
            os.getenv("PROTEA_PG_DB"),
            host_port,
        ]
    )

    if external_db:
        _guard_external_db(user=user, host="localhost", port=str(host_port), db=db)
        url = f"postgresql+psycopg://{user}:{password}@localhost:{host_port}/{db}"
        yield url
        return

    if not _docker_exists():
        pytest.skip("Docker is not available; cannot start Postgres container.")

    image = os.getenv("PROTEA_PG_IMAGE", "pgvector/pgvector:pg16")
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

        subprocess.run(
            [
                "docker",
                "exec",
                container,
                "psql",
                "-U",
                user,
                "-d",
                db,
                "-c",
                "CREATE EXTENSION IF NOT EXISTS vector;",
            ],
            text=True,
            capture_output=True,
        )

        url = f"postgresql+psycopg://{user}:{password}@localhost:{host_port}/{db}"
        yield url

    finally:
        subprocess.run(["docker", "rm", "-f", container], text=True, capture_output=True)
