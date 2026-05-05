ADR-D28: Secrets management
=============================

:Status: Pending
:Date: 2026-05-05
:Phase: F-OPS
:Gate: opens at F-OPS entry

Context
-------
PostgreSQL credentials, MinIO keys, OIDC client secrets, optional
external API tokens and SSH keys cannot live in plaintext in
repositories. Multi-target deployment (cloud, HPC, airgap) requires a
single mechanism that works across all of them.

Decision (recommended)
----------------------
``sops`` with ``age`` keys. Encrypted ``secrets.enc.yaml`` committed
in repos; CI decrypts with the age key stored in GitHub Secrets.
Local development uses a developer-specific age key checked into the
user's keyring.

Consequences
------------
- Plaintext secrets never on disk persistente.
- Per-environment file (``secrets.dev.enc.yaml``,
  ``secrets.prod.enc.yaml``).
- Rotation procedure documented.

Resolution
----------
Pending; gate opens with F-OPS (T-OPS.7).
