#!/usr/bin/env bash
# protea-guard-revision.sh
#
# Exit 0 if the deploy tree is at the revision agent-farm declares. Exit 1 if it
# is not, so systemd fails the unit and the worker stays DOWN.
#
# WHY THIS EXISTS. On 2026-09-02 three revisions were live on one queue at once:
# this machine's worker processes had loaded their code on 30 August and never
# reloaded, so they opened prediction sets stamping b0f164dc; the compute node
# was correctly in-sync at the declared a5de702 and refused every batch it was
# handed; and the deploy tree itself was three commits further on. Two arms of a
# declared experiment failed instantly and a third had to be cancelled before it
# finished a short prediction set.
#
# Nothing was mislabelled, because the node refuses rather than guesses. That is
# the point: THE RISK SITS THE WRONG WAY ROUND. The stateless node, which holds
# nothing and can be rebuilt from scratch, checks the declaration before it runs
# anything. The server, which owns every artifact the project has, starts a
# worker on whatever happens to be checked out and says nothing. This closes
# that half.
#
# WHY IT REFUSES INSTEAD OF SYNCING. The node's protea-node-sync.sh moves its
# deploy slot to the declaration, and it can, because that slot is only ever a
# deployment. Here the deploy tree IS the development tree -- farm.env sets
# DEPLOY to the repository itself -- so a worker that moved it would discard
# whatever a human or an agent was working on. A worker may read the tree. It
# may never move it.
#
# READ-ONLY: two git reads and a string comparison. Starts nothing, kills
# nothing, moves nothing.
set -euo pipefail

DECL_PATH="plans/DECLARED-REVISION.txt"

refuse() { echo "protea-guard-revision: REFUSING, $*" >&2; exit 1; }
allow()  { echo "protea-guard-revision: $*" >&2; exit 0; }

# NEITHER PATH HAS A DEFAULT, AND THAT IS THE POINT.
#
# The first version of this script fell back to $HOME/Thesis-laptop/PROTEA and
# $HOME/Thesis-laptop/agent-farm. Hours after it was written the compute node
# demonstrated why that is not acceptable in a guard. Its worker unit sets
# WorkingDirectory to ~/Thesis2/repositories/PROTEA and its wrapper read
# PROTEA_REPO defaulting to that path, while protea-node-sync.sh read the SAME
# variable defaulting to ~/Thesis2/worktrees/protea-deploy. Neither the unit nor
# the environment set it. So the sync guard spent a day reporting "in-sync
# a5de702" about a tree no process was executing, while the worker ran c28c4ae
# -- a revision predating the code-revision check entirely, which therefore
# could not refuse anything and completed its batches reporting success. A
# prediction set of 1,092,945 rows came out stamped with the coordinator's
# revision and holding content produced by another.
#
# A guard that does not share a tree with the process it watches is not a guard,
# it is a report about something else. A default is exactly how the two come
# apart without anyone choosing it, because a default is a guess that never
# announces itself. So both paths must be handed in, and an absent one is a
# refusal rather than an assumption.
# A HOST OUTSIDE THE PROTOCOL SAYS SO, IT IS NOT INFERRED.
#
# Some host may legitimately run workers without participating in the
# declaration at all, and taking its workers down would be a worse failure than
# the one guarded against. But "this host does not participate" and "somebody
# mistyped a path" produce the same silence, and the first version of this
# script could not tell them apart: an unset or wrong AGENT_FARM_ROOT read as
# "nothing is declared" and let the worker start. That fails OPEN.
#
# So the exemption exists and has to be claimed out loud. An operator who means
# it sets one variable; a typo does not set it for them.
if [[ "${PROTEA_GUARD_UNDECLARED:-}" == "1" ]]; then
  allow "PROTEA_GUARD_UNDECLARED=1: this host is declared to be outside the revision protocol"
fi

[[ -n "${DEPLOY:-}" ]] || refuse \
  "DEPLOY is not set. This guard must be told which tree the worker will load, \
because guessing is the failure it exists to prevent: a guard reading one tree \
while its worker runs another reports on something else and calls it in-sync. \
Set it in the unit, from the same source the worker reads."
[[ -n "${AGENT_FARM_ROOT:-}" ]] || refuse \
  "AGENT_FARM_ROOT is not set. Without it this guard would guess where the \
declaration lives, and a guessed path that happens not to exist reads as \
'nothing is declared' and lets the worker start. That fails open, which is the \
worst way for a guard to be wrong."

FARM="${AGENT_FARM_ROOT}"
SLOT="${DEPLOY}"

# A configured path that is not a clone is a misconfiguration, not an absence of
# declaration, and the two must not collapse into the same verdict.
[[ -d "${FARM}/.git" ]] || refuse \
  "AGENT_FARM_ROOT is ${FARM}, which is not a git clone. That is a wrong path \
rather than a machine with nothing declared, and treating it as the latter \
would start the worker on the strength of a typo."
[[ -d "${SLOT}/.git" ]] || refuse "DEPLOY is ${SLOT}, which is not a git clone"

# AND THE TREE THIS GUARD READS MUST BE THE TREE THE WORKER WILL RUN IN.
#
# This is the check that would have caught the node's failure at its root
# rather than three revisions downstream. systemd runs ExecStartPre in the
# unit's WorkingDirectory, which is the directory the worker itself starts in,
# so comparing DEPLOY against $PWD compares the guard's subject against the
# worker's. If they differ, everything below is true about a tree nobody
# executes -- which is precisely the state the node was in while reporting
# in-sync.
#
# Resolved with `cd -P` so a symlinked deploy path and its target compare equal;
# they are the same tree and refusing there would be a false alarm.
if [[ -n "${PWD:-}" && -d "${PWD}" ]]; then
  slot_real="$(cd -P "${SLOT}" && pwd)"
  here_real="$(cd -P "${PWD}" && pwd)"
  [[ "${slot_real}" == "${here_real}" ]] || refuse \
    "this guard was told to watch ${slot_real} but it is running in ${here_real}, \
so it would be checking a tree the worker is not starting in. That is the shape \
of the failure it exists to catch: on 2026-09-02 a sync guard reported in-sync \
for a day about a worktree while the worker ran from a different clone three \
revisions behind, and completed batches reporting success."
fi

# `git show origin/main:<path>` reads the committed file. A checkout cannot be
# trusted: the whole failure this guards against is a tree that says one thing
# and a process that runs another.
DECL="$(git -C "${FARM}" show "origin/main:${DECL_PATH}" 2>/dev/null)" \
  || allow "no ${DECL_PATH} on agent-farm origin/main; nothing declared, nothing to enforce"

WANT="$(printf '%s\n' "${DECL}" | awk '$1=="coordinator"{print $2; exit}')"
[[ "${WANT}" =~ ^[0-9a-f]{40}$ ]] \
  || refuse "the declaration carries no 40 character coordinator sha"

HAVE="$(git -C "${SLOT}" rev-parse HEAD 2>/dev/null)" \
  || refuse "cannot read HEAD of the deploy tree at ${SLOT}"

if [[ "${HAVE}" != "${WANT}" ]]; then
  # Name both, and say which way the divergence runs, because the fix differs.
  # A tree BEHIND the declaration is a stale checkout. A tree AHEAD of it is
  # somebody developing in the deployment, which is the reuse-in-place hazard
  # and wants a clone, not a checkout.
  rel="diverged from"
  if git -C "${SLOT}" merge-base --is-ancestor "${HAVE}" "${WANT}" 2>/dev/null; then
    rel="BEHIND"
  elif git -C "${SLOT}" merge-base --is-ancestor "${WANT}" "${HAVE}" 2>/dev/null; then
    rel="AHEAD of"
  fi
  refuse "the deploy tree at ${SLOT} is ${HAVE:0:12}, which is ${rel} the declared ${WANT:0:12}. \
A worker started here would stamp a revision the declaration does not name. Move the tree to the \
declared commit, or move the declaration -- and the check before moving the declaration is on the \
SERVER: no job in QUEUED or RUNNING, and no prediction set being written."
fi

# Uncommitted changes make HEAD a lie about what will be imported.
if ! git -C "${SLOT}" diff --quiet HEAD -- 2>/dev/null; then
  refuse "the deploy tree at ${SLOT} is at the declared ${WANT:0:12} but has uncommitted changes, \
so the code a worker would import is not the code the sha names"
fi

allow "deploy tree is at the declared ${WANT:0:12}"
