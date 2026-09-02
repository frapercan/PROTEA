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


def _run(
    farm: Path,
    slot: Path,
    *,
    cwd: Path | None = None,
    env_overrides: dict[str, str] | None = None,
    drop: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    """Run the guard the way systemd does: in the unit's WorkingDirectory.

    ``cwd`` defaults to the slot, because that is the invariant the guard now
    enforces -- it must be watching the tree the worker will start in.
    """
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(farm.parent),
        "AGENT_FARM_ROOT": str(farm),
        "DEPLOY": str(slot),
    }
    env.update(env_overrides or {})
    for key in drop:
        env.pop(key, None)
    return subprocess.run(
        ["bash", str(GUARD)], capture_output=True, text=True, env=env, cwd=str(cwd or slot)
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


def test_a_missing_farm_clone_is_now_a_refusal_and_not_an_allowance(tmp_path: Path) -> None:
    """This test asserted the opposite until 2026-09-02, and the opposite was wrong.

    Allowing here meant a mistyped path read as "nothing is declared" and started
    the worker. That is failing open, and it is the mechanism by which the
    compute node ran three revisions behind while its guard reported in-sync.
    """
    _repo(tmp_path / "slot")
    r = _run(tmp_path / "farm-does-not-exist", tmp_path / "slot")
    assert r.returncode == 1
    assert "not a git clone" in r.stderr


def test_a_host_outside_the_protocol_says_so_out_loud(tmp_path: Path) -> None:
    """The exemption survives, but it has to be claimed rather than inferred.

    Taking down the workers of a host that legitimately does not participate
    would be a worse failure than the one guarded against. The difference from
    before is that the host now says so with one variable, and a typo does not
    say it on its behalf.
    """
    _repo(tmp_path / "slot")
    r = _run(
        tmp_path / "farm-does-not-exist",
        tmp_path / "slot",
        env_overrides={"PROTEA_GUARD_UNDECLARED": "1"},
    )
    assert r.returncode == 0
    assert "outside the revision protocol" in r.stderr


class TestItRefusesToGuessWhereToLook:
    """A default is a guess that never announces itself.

    The first version of this guard defaulted both paths. Hours later the
    compute node showed what that costs: its worker unit and its sync guard read
    the same variable with different defaults, neither was set, and the guard
    spent a day reporting in-sync about a tree no process was executing while
    the worker ran a revision three commits behind that had no such check in it
    at all.
    """

    def test_it_refuses_when_the_deploy_tree_is_not_named(self, world) -> None:
        tmp, shas = world
        _farm(tmp / "farm", shas[-1])
        r = _run(tmp / "farm", tmp / "slot", drop=("DEPLOY",))
        assert r.returncode == 1
        assert "DEPLOY is not set" in r.stderr

    def test_it_refuses_when_the_declaration_root_is_not_named(self, world) -> None:
        """Guessing here fails OPEN, which is the worst way to be wrong.

        A guessed path that happens not to exist reads as "nothing is declared"
        and lets the worker start.
        """
        tmp, shas = world
        _farm(tmp / "farm", shas[-1])
        r = _run(tmp / "farm", tmp / "slot", drop=("AGENT_FARM_ROOT",))
        assert r.returncode == 1
        assert "AGENT_FARM_ROOT is not set" in r.stderr

    def test_a_wrong_farm_path_is_a_misconfiguration_not_an_absence(self, world) -> None:
        tmp, shas = world
        _farm(tmp / "farm", shas[-1])
        r = _run(tmp / "farm", tmp / "slot", env_overrides={"AGENT_FARM_ROOT": str(tmp / "nope")})
        assert r.returncode == 1
        assert "not a git clone" in r.stderr


class TestItRefusesToWatchATreeTheWorkerIsNotStartingIn:
    """The node's failure at its root rather than three revisions downstream."""

    def test_it_refuses_when_it_is_not_running_in_the_tree_it_watches(self, world) -> None:
        tmp, shas = world
        _farm(tmp / "farm", shas[-1])
        _repo(tmp / "other")
        r = _run(tmp / "farm", tmp / "slot", cwd=tmp / "other")
        assert r.returncode == 1
        assert "the worker is not starting in" in r.stderr

    def test_a_symlinked_deploy_path_is_the_same_tree(self, world) -> None:
        """Refusing on a symlink would be a false alarm, and false alarms get guards removed."""
        tmp, shas = world
        _farm(tmp / "farm", shas[-1])
        link = tmp / "slot-link"
        link.symlink_to(tmp / "slot")
        assert _run(tmp / "farm", link, cwd=tmp / "slot").returncode == 0
