ADR-D29: Release pipeline
===========================

:Status: Pending
:Date: 2026-05-05
:Phase: F-OPS
:Gate: opens at F-OPS entry

Context
-------
Seven repos need an independent SemVer release cadence. Releasing one
repo alone cannot break another; cross-repo integration testing on
tag is required. ``protea-contracts`` is the most disruptive: bumps
ripple through all consumers.

Decision (recommended)
----------------------
Per-repo SemVer plus cross-repo integration test on tag:

- A SemVer tag (``vX.Y.Z``) in any repo dispatches a build, image
  push (D27), and integration test that pulls the new image plus the
  pinned versions of the other six and runs a smoke pipeline.
- Failures block image promotion; the tag remains but the image is
  marked as ``release-candidate`` until the integration test passes.
- ``protea-contracts`` releases trigger a re-pin in all consumers as
  a follow-up automated PR.

Consequences
------------
- Tag is the release primitive; PRs are not.
- One canonical integration test stack lives in ``protea-bundle``.
- Manual rollback is repo-local (revert tag, push fix, retag).

Resolution
----------
Pending; gate opens with F-OPS (T-OPS.8).
