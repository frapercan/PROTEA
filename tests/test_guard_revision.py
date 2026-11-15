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

#: The declaration the guard reads when its unit names none, and the one the
#: development instance's units name through ``PROTEA_DECL_PATH``.
DECLARATION = "plans/DECLARED-REVISION.txt"
DEV_DECLARATION = "plans/DECLARED-REVISION-DEV.txt"
#: What a dev unit adds to the environment the guard runs in.
DEV_UNIT = {"PROTEA_DECL_PATH": DEV_DECLARATION}


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


def _farm(path: Path, declared: str | None, *, also: dict[str, str] | None = None) -> None:
    """A stand-in agent-farm whose origin/main carries the declarations.

    The guard reads ``git show origin/main:...`` and never a checkout, so the
    fixture has to give it a real remote-tracking ref rather than a file.
    ``declared`` goes in the default declaration, which is absent when it is
    ``None``; ``also`` maps further paths in the clone, a dev declaration for
    one, to the commit each of them declares.
    """
    upstream = path.parent / (path.name + "-upstream")
    _repo(upstream)
    declarations = {} if declared is None else {DECLARATION: declared}
    declarations.update(also or {})
    for relative, sha in declarations.items():
        (upstream / relative).parent.mkdir(parents=True, exist_ok=True)
        (upstream / relative).write_text(
            f"# a declaration\ncoordinator {sha}\nschema-applied {sha}\n"
        )
        _git(upstream, "add", relative)
    if declarations:
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


def test_a_missing_declaration_is_now_a_refusal_and_not_an_allowance(world) -> None:
    """This test asserted the opposite until 2026-11-15, and the opposite failed open.

    Allowing here was meant for a host that does not take part in the
    declaration protocol, and such a host now says so with
    ``PROTEA_GUARD_UNDECLARED=1`` (below). Without that, a missing declaration
    reads exactly like a mistyped one, and once a unit hands the guard the path
    of its declaration, a typo there or a dev declaration nobody has merged yet
    would start the worker on nothing at all.
    """
    tmp, shas = world
    _farm(tmp / "farm", None)
    r = _run(tmp / "farm", tmp / "slot")
    assert r.returncode == 1
    assert f"cannot read {DECLARATION}" in r.stderr


def test_a_farm_clone_without_origin_main_declares_nothing_and_refuses(world) -> None:
    """A clone with no remote-tracking main is a misconfiguration, not an empty declaration."""
    tmp, shas = world
    _repo(tmp / "farm")
    r = _run(tmp / "farm", tmp / "slot")
    assert r.returncode == 1
    assert f"cannot read {DECLARATION}" in r.stderr


def test_a_worktree_is_refused_so_a_dev_tree_is_a_clone_of_its_own(world) -> None:
    """The development instance cannot run from a worktree of a working clone.

    A worktree's ``.git`` is a file pointing back into the clone it hangs from,
    and the guard requires a directory, so it reports a worktree as no clone at
    all. The dev bootstrap relies on that refusal and clones instead, so this
    pins it. The worktree sits at the declared commit with nothing changed, so
    the refusal can only be about what the tree is.
    """
    tmp, shas = world
    _farm(tmp / "farm", shas[0])
    _git(tmp / "slot", "worktree", "add", "-q", "--detach", str(tmp / "worktree"), shas[0])
    r = _run(tmp / "farm", tmp / "worktree")
    assert r.returncode == 1
    assert "not a git clone" in r.stderr


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


class TestTheUnitNamesItsDeclaration:
    """The development instance reads a declaration of its own.

    Its tree runs commits of the integration branch, which the production
    declaration never names, so its units hand the guard
    ``plans/DECLARED-REVISION-DEV.txt`` through ``PROTEA_DECL_PATH``, the
    variable agent-farm's protea-node-sync.sh reads with the same default. Each
    test is built so that a guard reading any other file, the production
    declaration or a checkout's copy, could not pass.
    """

    def test_it_allows_a_tree_at_the_commit_its_declaration_names(self, world) -> None:
        tmp, shas = world
        _farm(tmp / "farm", shas[0], also={DEV_DECLARATION: shas[-1]})
        r = _run(tmp / "farm", tmp / "slot", env_overrides=DEV_UNIT)
        assert r.returncode == 0, r.stderr
        assert f"declared in {DEV_DECLARATION}" in r.stderr

    def test_it_refuses_a_tree_its_declaration_does_not_name(self, world) -> None:
        """The production declaration names this tree, and that does not help it."""
        tmp, shas = world
        _farm(tmp / "farm", shas[-1], also={DEV_DECLARATION: shas[0]})
        r = _run(tmp / "farm", tmp / "slot", env_overrides=DEV_UNIT)
        assert r.returncode == 1
        assert "AHEAD of" in r.stderr
        assert f"declared in {DEV_DECLARATION}" in r.stderr

    def test_a_named_declaration_that_is_missing_refuses_and_never_falls_back(self, world) -> None:
        """A dev declaration nobody merged yet must not start the worker on production's."""
        tmp, shas = world
        _farm(tmp / "farm", shas[-1])
        r = _run(tmp / "farm", tmp / "slot", env_overrides=DEV_UNIT)
        assert r.returncode == 1
        assert f"cannot read {DEV_DECLARATION}" in r.stderr

    def test_a_declaration_committed_only_in_the_checkout_is_not_declared(self, world) -> None:
        """The guard reads origin/main, because a checkout is a tree that can say anything.

        Here the farm's own checkout carries a dev declaration naming the very
        commit the slot is at, committed locally and never pushed. Reading the
        checkout would allow; reading origin/main finds nothing and refuses.
        """
        tmp, shas = world
        farm = tmp / "farm"
        _farm(farm, shas[0])
        _git(farm, "config", "user.email", "t@t")
        _git(farm, "config", "user.name", "t")
        (farm / DEV_DECLARATION).parent.mkdir(parents=True, exist_ok=True)
        (farm / DEV_DECLARATION).write_text(f"coordinator {shas[-1]}\n")
        _git(farm, "add", DEV_DECLARATION)
        _git(farm, "commit", "-qm", "declared here and nowhere else")
        r = _run(farm, tmp / "slot", env_overrides=DEV_UNIT)
        assert r.returncode == 1
        assert f"cannot read {DEV_DECLARATION}" in r.stderr
