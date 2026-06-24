"""Tests for the self-healing logic in ``scripts/manage.sh`` (FIX-STACK-SELF-HEAL).

These shell out to bash and exercise the pure-bash helpers in isolation, so no
live PROTEA stack, Postgres, or RabbitMQ is needed:

* ``_source_env`` must load ``.env`` then ``.env.local`` (latter wins) and be a
  silent no-op when neither exists.
* ``_heal_dead_workers`` must restart a tracked worker whose PID is dead by
  replaying its recorded ``.cmd``, and must leave a live worker untouched (no
  duplicate spawn).

The script's bottom-of-file dispatch (which would ``exit`` on an unknown
command) is stripped before sourcing so we can call the functions directly.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MANAGE_SH = REPO_ROOT / "scripts" / "manage.sh"


def _functions_only(tmp_path: Path) -> Path:
    """Copy manage.sh up to (excluding) the dispatch block into tmp_path.

    The dispatch block runs commands / exits on source, which we do not want
    when we only need the helper functions.
    """
    text = MANAGE_SH.read_text()
    marker = "# ── dispatch"
    head = text.split(marker, 1)[0]
    out = tmp_path / "manage_funcs.sh"
    out.write_text(head)
    return out


def _run(
    script: str, tmp_path: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=full_env,
        check=False,
    )


def test_source_env_precedence(tmp_path: Path) -> None:
    """.env loads first, .env.local overrides it; vars are exported."""
    funcs = _functions_only(tmp_path)
    (tmp_path / ".env").write_text("FOO=from_env\nBAR=base\n")
    (tmp_path / ".env.local").write_text("FOO=from_local\n")

    script = f"""
        set -euo pipefail
        source {funcs}
        ROOT={tmp_path}
        LOG_DIR="$ROOT/logs"; PID_DIR="$ROOT/logs/pids"
        GREEN=""; RED=""; YELLOW=""; CYAN=""; BOLD=""; RESET=""
        _source_env >/dev/null
        echo "FOO=$FOO"
        echo "BAR=$BAR"
    """
    res = _run(script, tmp_path)
    assert res.returncode == 0, res.stderr
    assert "FOO=from_local" in res.stdout  # .env.local wins
    assert "BAR=base" in res.stdout  # .env-only var still present


def test_source_env_noop_when_absent(tmp_path: Path) -> None:
    """No .env / .env.local present is a clean no-op, not an error."""
    funcs = _functions_only(tmp_path)
    script = f"""
        set -euo pipefail
        source {funcs}
        ROOT={tmp_path}
        LOG_DIR="$ROOT/logs"; PID_DIR="$ROOT/logs/pids"
        GREEN=""; RED=""; YELLOW=""; CYAN=""; BOLD=""; RESET=""
        _source_env
        echo OK
    """
    res = _run(script, tmp_path)
    assert res.returncode == 0, res.stderr
    assert "OK" in res.stdout


def test_heal_restarts_dead_worker(tmp_path: Path) -> None:
    """A tracked worker with a dead PID is restarted from its .cmd file."""
    funcs = _functions_only(tmp_path)
    pid_dir = tmp_path / "logs" / "pids"
    pid_dir.mkdir(parents=True)
    sentinel = tmp_path / "restarted.flag"

    # A dead PID (99999 is almost certainly not a live process) + a .cmd that
    # touches a sentinel when replayed.
    (pid_dir / "worker-jobs.pid").write_text("999999\n")
    (pid_dir / "worker-jobs.cmd").write_text(f"touch\n{sentinel}\n")

    script = f"""
        set -euo pipefail
        source {funcs}
        ROOT={tmp_path}
        LOG_DIR="$ROOT/logs"; PID_DIR="$ROOT/logs/pids"
        GREEN=""; RED=""; YELLOW=""; CYAN=""; BOLD=""; RESET=""
        _heal_dead_workers || true
        sleep 0.3
    """
    res = _run(script, tmp_path)
    assert res.returncode == 0, res.stderr
    assert sentinel.exists(), "dead worker was not restarted from its .cmd"


def test_heal_leaves_live_worker_untouched(tmp_path: Path) -> None:
    """A worker whose PID is alive must NOT be restarted (no duplicate spawn)."""
    funcs = _functions_only(tmp_path)
    pid_dir = tmp_path / "logs" / "pids"
    pid_dir.mkdir(parents=True)
    sentinel = tmp_path / "should_not_exist.flag"

    # $$ of the test harness bash is alive while _heal runs; use the parent
    # shell PID via a live sleeper instead.
    sleeper = subprocess.Popen(["sleep", "30"])
    try:
        (pid_dir / "worker-jobs.pid").write_text(f"{sleeper.pid}\n")
        (pid_dir / "worker-jobs.cmd").write_text(f"touch\n{sentinel}\n")

        script = f"""
            set -euo pipefail
            source {funcs}
            ROOT={tmp_path}
            LOG_DIR="$ROOT/logs"; PID_DIR="$ROOT/logs/pids"
            GREEN=""; RED=""; YELLOW=""; CYAN=""; BOLD=""; RESET=""
            _heal_dead_workers || true
            sleep 0.2
        """
        res = _run(script, tmp_path)
        assert res.returncode == 0, res.stderr
        assert not sentinel.exists(), "live worker was wrongly restarted"
    finally:
        sleeper.terminate()
        sleeper.wait()
