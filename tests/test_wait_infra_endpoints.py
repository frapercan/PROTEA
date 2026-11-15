"""The infra readiness gate takes its endpoints from the environment.

``deploy/systemd/protea-wait-infra.sh`` used to wait on two hard-coded
ports, which made it unusable for a second stack on the same machine. The
dev stack (``deploy/dev``) runs beside production on its own loopback ports
and reuses the gate through ``PROTEA_WAIT_ENDPOINTS``; with the variable
unset, the gate must still wait on exactly what it always did.

Real sockets on ephemeral ports, because the gate's whole job is a real TCP
connect.
"""

from __future__ import annotations

import socket
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "systemd" / "protea-wait-infra.sh"


def _run(**env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT)],
        env={"PATH": "/usr/bin:/bin", **env},
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture()
def listening_port() -> Iterator[int]:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(8)
        yield server.getsockname()[1]


def _closed_port() -> int:
    """A loopback port nothing listens on: bound once, then released."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _answers(endpoint: str) -> bool:
    host, _, port = endpoint.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            return True
    except OSError:
        return False


def test_an_endpoint_from_the_environment_that_answers_passes(listening_port):
    result = _run(PROTEA_WAIT_ENDPOINTS=f"127.0.0.1:{listening_port}")
    assert result.returncode == 0, result.stderr
    assert f"127.0.0.1:{listening_port}" in result.stdout


def test_an_endpoint_from_the_environment_that_does_not_answer_fails():
    closed = _closed_port()
    result = _run(PROTEA_WAIT_ENDPOINTS=f"127.0.0.1:{closed}", PROTEA_WAIT_DEADLINE="0")
    assert result.returncode == 1
    assert f"127.0.0.1:{closed} not answering" in result.stderr


def test_an_endpoint_that_fails_after_one_that_answered_is_still_named(listening_port):
    # The gate used to close its probe with `exec 3<&- 2>/dev/null`, which
    # silenced the shell's stderr for good once one endpoint had answered.
    closed = _closed_port()
    result = _run(
        PROTEA_WAIT_ENDPOINTS=f"127.0.0.1:{listening_port} 127.0.0.1:{closed}",
        PROTEA_WAIT_DEADLINE="0",
    )
    assert result.returncode == 1
    assert f"127.0.0.1:{closed} not answering" in result.stderr


@pytest.mark.parametrize(
    "endpoints",
    ["127.0.0.1", "127.0.0.1:", ":5432", "db:abc", "db:0", "db:70000", "   "],
)
def test_a_malformed_endpoint_is_a_configuration_error(endpoints):
    result = _run(PROTEA_WAIT_ENDPOINTS=endpoints, PROTEA_WAIT_DEADLINE="0")
    assert result.returncode == 78, result.stderr


#: What production's units wait on, in this order, since none of them sets
#: PROTEA_WAIT_ENDPOINTS: the Postgres, then the RabbitMQ.
PRODUCTION_DEFAULTS = ("127.0.0.1:5432", "127.0.0.1:5672")


def test_unset_it_waits_on_the_production_postgres_then_rabbitmq():
    # The gate stops at the first endpoint that does not answer, so the one
    # it must name depends on what listens on this machine; with both up it
    # must name both. Every branch asserts: none of them skips.
    down = [hp for hp in PRODUCTION_DEFAULTS if not _answers(hp)]
    result = _run(PROTEA_WAIT_DEADLINE="0")
    if down:
        assert result.returncode == 1
        assert f"{down[0]} not answering" in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert " ".join(PRODUCTION_DEFAULTS) in result.stdout


def test_the_default_list_is_exactly_the_production_pair():
    # Read from the script as well, because a run on a machine where the
    # Postgres is down never reaches the second endpoint.
    default = " ".join(PRODUCTION_DEFAULTS)
    assert f'"${{PROTEA_WAIT_ENDPOINTS:-{default}}}"' in SCRIPT.read_text()
