#!/usr/bin/env bash
# promote.sh — promote develop's tree onto main as a single release snapshot.
#
# Branch/release model (see docs/source/runbooks/release-process.rst):
#   * develop = trunk + development environment. ALL changes land here (the dev
#     box tracks develop HEAD; nothing heavy fires on a develop push).
#   * main = production line: a clean sequence of single-commit snapshots of
#     develop's tree, versioned by release-please.
#
# This script performs the snapshot. It is O(1) and conflict-free no matter how
# far develop and main have diverged, because it copies develop's tree wholesale
# (read-tree) instead of three-way-merging two long-lived histories. The single
# clean commit also keeps the coauthor-guard green regardless of develop's
# history.
#
# After the promotion PR merges, release-please opens a release PR on main.
# Merging THAT is the deliberate "ship to production" act and the ONLY event
# that builds containers / publishes packages (the heavy workflows are gated to
# `release: published`). If the release PR sits BLOCKED with no checks running,
# close and reopen it once to re-trigger them (a known release-please quirk).
#
# Usage:
#   scripts/promote.sh           # create the promotion PR, print its URL
#   scripts/promote.sh --auto    # also arm auto-merge on the promotion PR
set -euo pipefail

DEV=develop
PROD=main

REPO="$(git rev-parse --show-toplevel)"
SLUG="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
AUTO=""
[ "${1:-}" = "--auto" ] && AUTO=1

git -C "$REPO" fetch -q origin "$DEV" "$PROD"
DEV_SHA="$(git -C "$REPO" rev-parse --short "origin/$DEV")"

# release-please reads the current version from the manifest on main; keep the
# snapshot's version markers there so the next release bumps from the released
# version rather than from develop's (stale) version.
MAIN_VER="$(git -C "$REPO" show "origin/$PROD:.release-please-manifest.json" \
            | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
[ -n "$MAIN_VER" ] || { echo "promote: could not read main version from manifest" >&2; exit 1; }

BR="release/promote-${DEV_SHA}"
WT="$(mktemp -d)"
git -C "$REPO" worktree add -q "$WT" -b "$BR" "origin/$PROD"
trap 'git -C "$REPO" worktree remove --force "$WT" 2>/dev/null || true' EXIT
cd "$WT"

# 1. snapshot develop's tree wholesale
git read-tree --reset -u "origin/$DEV"
# 2. restore the release-lineage files from main so release-please stays consistent
git checkout "origin/$PROD" -- CHANGELOG.md .release-please-manifest.json
# 3. pin the python release-type version markers to main's released version
sed -i -E "s/^version = \".*\"/version = \"$MAIN_VER\"/" pyproject.toml
[ -f protea/__init__.py ] && sed -i -E "s/^__version__ = \".*\"/__version__ = \"$MAIN_VER\"/" protea/__init__.py

git add -A
if git diff --cached --quiet; then
  echo "promote: main already matches develop; nothing to promote"
  exit 0
fi
git commit -q -m "feat: promote develop to ${PROD} (snapshot ${DEV_SHA})

Single-commit snapshot of develop's tree onto ${PROD}. Release-lineage metadata
kept at ${MAIN_VER} so release-please cuts the next version from here."
git push -q -u origin "$BR"

PR_URL="$(gh pr create --repo "$SLUG" --base "$PROD" --head "$BR" \
  --title "feat: promote develop to ${PROD} for release" \
  --body "Snapshot of develop (\`${DEV_SHA}\`) onto \`${PROD}\`, versioned from ${MAIN_VER}.

After this merges, release-please opens the release PR; merging that cuts the tag
and is the only event that builds containers / publishes packages. If the release
PR is BLOCKED with no checks running, close and reopen it once.")"
echo "promotion PR: $PR_URL"

if [ -n "$AUTO" ]; then
  gh pr merge "$PR_URL" --repo "$SLUG" --squash --auto >/dev/null && echo "auto-merge armed"
fi
