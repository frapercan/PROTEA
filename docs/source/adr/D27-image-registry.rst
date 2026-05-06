ADR-D27: Image registry
=========================

:Status: Accepted
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
:Phase: F-OPS
:Gate: opens at F-OPS entry

Context
-------
Seven OCI images need a hosting registry visible from cloud
deployments, HPC tooling that pulls before converting to ``.sif``,
and external adopters consuming ``protea-method-runtime``.

Decision (recommended)
----------------------
``ghcr.io`` (GitHub Container Registry).

Consequences
------------
- GitHub Actions push images on tag using the repository's own
  GITHUB_TOKEN.
- Public visibility for ``protea-method-runtime``; private or
  org-scoped for internal images if needed.
- Mirror to Docker Hub considered later if external pull rates demand
  it.

Resolution
----------
**Accepted as recommended.** ``ghcr.io`` confirmed by user 2026-05-06.
Implementation: GitHub Actions workflow per repo on tag push, login via
the built-in ``GITHUB_TOKEN``, image tag set from the SemVer tag.
Public visibility for ``protea-method-runtime``; org-scoped or private
for internal images if/when needed. Mirror to Docker Hub deferred until
external pull rates demand it.
