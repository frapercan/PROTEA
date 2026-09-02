"""The server refuses to start a worker on a revision nobody declared.

On 2026-09-02 three revisions were live on one queue: this machine's workers ran
code loaded on 30 August, the compute node ran the declared commit and refused
every batch, and the deploy tree was further on than either. The node has this
check and the server did not, which is backwards -- the machine that owns every
artifact was the one starting workers on whatever happened to be checked out.

These tests build throwaway git repositories rather than mocking git, because
the guard's whole job is to be right about what a real checkout says.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

GUARD = Path(__file__).resolve().parents[1] / "deploy" / "systemd" / "protea-guard-revision.sh"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    ).stdout.strip()


def _repo(path: Path, commits: int = 1) -> list[str]:
    """A repository with ``commits`` commits, newest last. Returns their shas."""
    path.mkdir(parents=True, exist_ok=True)
    _git(path.parent, "init", "-q", "-b", "main", path.name)
    _git(path, "config", "user.email", "t@t")
    _git(path, "config", "user.name", "t")
    shas = []
    for i in range(commits):
        (path / "f").write_text(str(i))
        _git(path, "add", "f")
        _git(path, "commit", "-qm", f"c{i}")
        shas.append(_git(path, "rev-parse", "HEAD"))
    return shas


def _farm(path: Path, declared: str | None) -> None:
    """A stand-in agent-farm whose origin/main carries the declaration.

    The guard reads ``git show origin/main:...`` and never a checkout, so the
    fixture has to give it a real remote-tracking ref rather than a file.
    """
    upstream = path.parent / (path.name + "-upstream")
    _repo(upstream)
    if declared is not None:
        (upstream / "plans").mkdir(exist_ok=True)
        (upstream / "plans" / "DECLARED-REVISION.txt").write_text(
            f"# a declaration\ncoordinator {declared}\nschema-applied {declared}\n"
        )
        _git(upstream, "add", "plans/DECLARED-REVISION.txt")
        _git(upstream, "commit", "-qm", "declare")
    subprocess.run(
        ["git", "clone", "-q", str(upstream), str(path)], check=True, capture_output=True
    )


def _run(farm: Path, slot: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(GUARD)],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "HOME": str(farm.parent), "AGENT_FARM_ROOT": str(farm), "DEPLOY": str(slot)},
    )


@pytest.fixture
def world(tmp_path: Path):
    shas = _repo(tmp_path / "slot", commits=3)
    return tmp_path, shas


def test_it_allows_a_tree_at_the_declared_commit(world) -> None:
    tmp, shas = world
    _farm(tmp / "farm", shas[-1])
    assert _run(tmp / "farm", tmp / "slot").returncode == 0


def test_it_refuses_a_tree_behind_the_declaration(world) -> None:
    """The state this machine was actually in: workers three commits stale."""
    tmp, shas = world
    _farm(tmp / "farm", shas[-1])
    _git(tmp / "slot", "checkout", "-q", shas[0])
    r = _run(tmp / "farm", tmp / "slot")
    assert r.returncode == 1
    assert "BEHIND" in r.stderr


def test_it_refuses_a_tree_ahead_of_the_declaration(world) -> None:
    """The other half: developing inside the deployment, the reuse-in-place hazard."""
    tmp, shas = world
    _farm(tmp / "farm", shas[0])
    r = _run(tmp / "farm", tmp / "slot")
    assert r.returncode == 1
    assert "AHEAD of" in r.stderr


def test_it_refuses_a_dirty_tree_even_at_the_right_commit(world) -> None:
    """HEAD stops describing what a worker would import the moment a file moves."""
    tmp, shas = world
    _farm(tmp / "farm", shas[-1])
    (tmp / "slot" / "f").write_text("edited")
    r = _run(tmp / "farm", tmp / "slot")
    assert r.returncode == 1
    assert "uncommitted" in r.stderr


def test_it_allows_when_nothing_is_declared(world) -> None:
    """A machine with no declaration is not a machine running the wrong code.

    Refusing here would take down every worker on any host that does not
    participate in the declaration protocol, which is a worse failure than the
    one being guarded against.
    """
    tmp, shas = world
    _farm(tmp / "farm", None)
    assert _run(tmp / "farm", tmp / "slot").returncode == 0


def test_it_allows_when_there_is_no_farm_clone_at_all(tmp_path: Path) -> None:
    shas = _repo(tmp_path / "slot")
    r = _run(tmp_path / "farm-does-not-exist", tmp_path / "slot")
    assert r.returncode == 0
    assert shas
