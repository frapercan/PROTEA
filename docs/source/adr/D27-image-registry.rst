ADR-D27: Image registry
=========================

:Status: Pending
:Date: 2026-05-05
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
Pending; gate opens with F-OPS (T-OPS.8).
