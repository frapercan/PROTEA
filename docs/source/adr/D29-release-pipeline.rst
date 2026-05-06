ADR-D29: Release pipeline
===========================

:Status: Accepted
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
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
**Accepted with release-please tooling.** User confirmation 2026-05-06.
The original direction ("semantic-release") implied
``python-semantic-release``, which push-commits the version bump back
to ``main`` and bypasses branch protection. Branch protection on
``main`` is a hard project rule; we keep the spirit (Conventional
Commits driving version + CHANGELOG) but switch to ``release-please``
which opens a release PR instead of pushing to ``main`` directly.

Version bumps + CHANGELOG generation are driven by Conventional
Commits parsed by ``release-please``: ``feat:`` → minor, ``fix:`` →
patch, ``BREAKING CHANGE:`` footer or ``feat!:`` → major. The
commit-message style is already in place from the F2 phase (every
commit during F2A.6-real, F2B, D-MIGR-06, Doc-T11 is conventional).

Cross-repo integration test on tag stays as recommended. Per-repo
implementation: a ``release-please.yml`` GitHub Action +
``release-please-config.json`` + ``.release-please-manifest.json`` at
the repo root. The action keeps a release PR open at all times; when
that PR is merged, ``release-please`` creates the GitHub Release and
tag, which in turn triggers ``docker.yml`` (D27) and the cross-repo
integration test job.
