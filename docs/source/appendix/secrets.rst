Secrets management (sops + age)
===============================

PROTEA uses `sops <https://github.com/getsops/sops>`_ with
`age <https://github.com/FiloSottile/age>`_ recipients for all secret
material. Plaintext values never live on disk persistently; instead, an
encrypted ``secrets/secrets.<env>.enc.yaml`` file is committed and
decrypted on demand by tools and CI.

This is ADR :doc:`/adr/D28-secrets-management`.

What is encrypted
-----------------

The full schema lives in ``secrets/secrets.dev.example.yaml`` (tracked
plaintext template, used as a copy-paste starting point). It currently
covers:

- ``database.url`` (PostgreSQL connection string with password)
- ``queue.amqp_url`` (RabbitMQ connection string with credentials)
- ``storage.minio.{access_key,secret_key,endpoint,bucket,secure}``
- ``admin.token`` (administrative HTTP token)
- ``github.release_token`` (PAT for release automation, optional)

Per-environment files are namespaced:

- ``secrets/secrets.dev.enc.yaml`` (local development)
- ``secrets/secrets.prod.enc.yaml`` (production deployment)

The encryption rules (which age public keys may decrypt which file)
live in ``.sops.yaml`` at the repo root.

One-time setup
--------------

1. **Install age and sops.** On Debian/Ubuntu via the
   distribution package or by downloading the official binaries::

      curl -L https://github.com/FiloSottile/age/releases/latest/download/age-v1.2.0-linux-amd64.tar.gz \
          | tar xz -C /tmp
      install /tmp/age/age /tmp/age/age-keygen ~/.local/bin/

      curl -L https://github.com/getsops/sops/releases/latest/download/sops-v3.9.1.linux.amd64 \
          -o ~/.local/bin/sops
      chmod +x ~/.local/bin/sops

2. **Generate your age keypair.** Stored in the OS user keyring (sops
   reads it from this default path)::

      mkdir -p ~/.config/sops/age
      age-keygen -o ~/.config/sops/age/keys.txt
      grep '^# public key:' ~/.config/sops/age/keys.txt

   The ``# public key:`` line (starts with ``age1...``) is your age
   recipient. Paste it into ``.sops.yaml`` under the appropriate rule,
   replacing ``AGE_RECIPIENT_DEV_PLACEHOLDER`` (or ``…_PROD_…``).

3. **Encrypt the development secrets file.** Copy the example, fill in
   real values, then encrypt in place::

      cp secrets/secrets.dev.example.yaml secrets/secrets.dev.yaml
      $EDITOR secrets/secrets.dev.yaml      # fill in real values
      sops --encrypt --in-place secrets/secrets.dev.yaml
      git mv secrets/secrets.dev.yaml secrets/secrets.dev.enc.yaml
      git add secrets/secrets.dev.enc.yaml
      git commit -m "feat(secrets): add encrypted dev secrets"

   The plaintext form is gitignored (see ``.gitignore``); only the
   ``.enc.yaml`` form is ever committed.

Daily use
---------

**Edit an encrypted file in your editor**::

   sops secrets/secrets.dev.enc.yaml

**Decrypt to stdout** (one-shot read)::

   sops --decrypt secrets/secrets.dev.enc.yaml

**Decrypt and source as environment variables** (for ``manage.sh``
bootstrap)::

   eval "$(sops --decrypt --output-type dotenv secrets/secrets.dev.enc.yaml)"

CI integration
--------------

GitHub Actions workflows that need decryption read the entire
``keys.txt`` body from a repository secret named ``SOPS_AGE_KEY``. Add
this snippet before any step that reads secrets::

   - name: Configure sops age key
     env:
       SOPS_AGE_KEY: ${{ secrets.SOPS_AGE_KEY }}
     run: |
       mkdir -p ~/.config/sops/age
       printf '%s\n' "$SOPS_AGE_KEY" > ~/.config/sops/age/keys.txt

After this step, ``sops --decrypt`` works as it does locally.

Rotation
--------

To revoke a recipient (e.g. departing collaborator):

1. Remove their public key from ``.sops.yaml``.
2. Re-encrypt every affected file::

      sops updatekeys secrets/secrets.dev.enc.yaml
      sops updatekeys secrets/secrets.prod.enc.yaml

3. Commit the result. The revoked key can no longer decrypt updates,
   though the old encrypted blobs remain decryptable in git history.
   Treat any leaked secret as compromised regardless: rotate the
   underlying credential at the source (database, MinIO, etc.).
