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

FARM="${AGENT_FARM_ROOT:-$HOME/Thesis-laptop/agent-farm}"
SLOT="${DEPLOY:-$HOME/Thesis-laptop/PROTEA}"
DECL_PATH="plans/DECLARED-REVISION.txt"

refuse() { echo "protea-guard-revision: REFUSING, $*" >&2; exit 1; }
allow()  { echo "protea-guard-revision: $*" >&2; exit 0; }

[[ -d "${FARM}/.git" ]] || allow "no agent-farm clone at ${FARM}; nothing declares a revision here"

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
