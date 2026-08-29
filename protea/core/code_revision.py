"""Which revision of the code produced a row.

WHY THIS EXISTS. The queue is shared between a server and a compute node that
clone the repository separately. On 2026-08-29 the node ran a revision that
predated the donor-policy fix and wrote 193,303 rows into a prediction set the
server believed was homogeneous. The run reported success. Nothing in the
database could say which code had produced which rows: the mismatch was found
by reading a log file on the other machine, days later, and only because the
missing columns happened to be NOT NULL somewhere else.

A revision recorded and never compared would repeat that. So this module gives
two things, and the second is the point: a value to record, and a rule for
deciding when two recorded values are in conflict.

DIRTY TREES ARE NOT IDENTITIES. The laptop runs the fleet directly out of its
working tree, which is edited between runs. A sha read from a dirty tree names
a commit the running code is not. Two such trees can carry the same sha and
different code, so their revisions must never be treated as agreeing, and a
clean tree must never be trusted to agree with a dirty one. They are marked and
the rule below refuses to draw any conclusion from them, which is weaker than a
refusal and honest, rather than a comparison that passes for the wrong reason.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

#: Lets a deployment that carries no ``.git`` declare its own revision, for
#: instance an image built from a tag. Trusted verbatim: something that knows
#: what it is running is a better source than a guess.
ENV_VAR = "PROTEA_CODE_REVISION"

#: What is recorded when the code cannot say what it is. Never equal to
#: anything for the purpose of ``revisions_conflict``.
UNKNOWN = "unknown"

#: Appended to a sha read from a working tree with uncommitted changes.
DIRTY_SUFFIX = "+dirty"


class ForeignRevisionError(RuntimeError):
    """Raised when a worker is asked to add to work another revision opened.

    Fatal on purpose. The alternative is a set whose rows were produced by two
    codebases, which is not detectable afterwards from the rows themselves and
    invalidates every comparison the set takes part in.
    """


def resolve_protea_git_sha() -> str | None:
    """Best-effort current HEAD sha of the PROTEA repo. Returns None when
    the code is not running inside a git checkout or git is unavailable.

    On its own this names a commit, not the running code. Prefer
    :func:`code_revision`, which also says whether the tree was clean.
    """
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        ).strip()
        return out or None
    except Exception:
        return None


def _working_tree_is_dirty() -> bool | None:
    """True, False, or None when git cannot be asked."""
    try:
        out = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=Path(__file__).resolve().parents[2],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    return bool(out.strip())


def code_revision() -> str:
    """The revision the running process was built from.

    Returns a bare sha only when it is genuinely identifying: a clean checkout,
    or an explicit declaration through the environment.
    """
    declared = os.environ.get(ENV_VAR, "").strip()
    if declared:
        return declared
    sha = resolve_protea_git_sha()
    if not sha:
        return UNKNOWN
    dirty = _working_tree_is_dirty()
    if dirty is None:
        return UNKNOWN
    return f"{sha}{DIRTY_SUFFIX}" if dirty else sha


def is_identifying(revision: str | None) -> bool:
    """Whether a recorded revision names code that can be checked out again."""
    if not revision or revision == UNKNOWN:
        return False
    return not revision.endswith(DIRTY_SUFFIX)


def revisions_conflict(recorded: str | None, running: str | None) -> bool:
    """Whether two revisions are known to be different code.

    False when either side is unidentifying. That is not the same as agreement
    and callers must not report it as such: it means the question cannot be
    answered, and the caller is expected to say so out loud rather than pass
    quietly.
    """
    if not is_identifying(recorded) or not is_identifying(running):
        return False
    return recorded != running
