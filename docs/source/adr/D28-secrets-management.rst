ADR-D28: Secrets management
=============================

:Status: Accepted
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
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
**Accepted as recommended.** ``sops + age`` confirmed by user
2026-05-06. Two reasons captured: (a) age keys are post-PGP ed25519,
short and revocation-chain-free; (b) sops is file-format agnostic so
the same workflow handles yaml/json/env. First migration target:
``secrets.enc.yaml`` containing DB URL + AMQP URL + MinIO creds + GitHub
release token. Bootstrap script invokes ``sops -d`` before
``manage.sh start``. Per-environment files (``secrets.dev.enc.yaml`` /
``secrets.prod.enc.yaml``). Rotation procedure to be documented at
implementation time.
