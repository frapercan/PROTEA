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
``secrets.prod.enc.yaml``).

Implementation (2026-05-06)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Scaffolding committed (does not yet hold any real secrets):

- ``.sops.yaml`` at the repo root with ``creation_rules`` for
  ``secrets.dev`` and ``secrets.prod``. Recipient public keys are
  placeholders (``AGE_RECIPIENT_DEV_PLACEHOLDER`` /
  ``AGE_RECIPIENT_PROD_PLACEHOLDER``) until a maintainer generates an
  age keypair and pastes their public key in.
- ``secrets/secrets.dev.example.yaml`` plaintext template showing the
  schema (committed). Real values go in ``secrets/secrets.dev.yaml``
  (gitignored), then ``sops --encrypt --in-place`` produces
  ``secrets/secrets.dev.enc.yaml`` which is what gets committed.
- ``.gitignore`` updated to forbid plaintext ``secrets.{dev,prod}.yaml``
  while keeping the encrypted form and the example template tracked.
- :doc:`/appendix/secrets` runbook covering install, key generation,
  daily editing, CI integration (``SOPS_AGE_KEY`` repo secret), and
  recipient rotation.

Outstanding (requires the maintainer to act on a local machine):

1. Install ``age`` and ``sops`` (binaries to ``~/.local/bin``; see the
   runbook).
2. Run ``age-keygen -o ~/.config/sops/age/keys.txt`` and copy the
   ``# public key:`` line into ``.sops.yaml``.
3. Copy ``secrets/secrets.dev.example.yaml`` to
   ``secrets/secrets.dev.yaml``, fill in real values, encrypt in
   place, ``git mv`` to ``.enc.yaml``, commit.
4. Add ``SOPS_AGE_KEY`` (entire ``keys.txt`` body) to GitHub repo
   secrets so workflows can decrypt.
