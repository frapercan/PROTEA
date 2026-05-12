Secrets management runbook (sops + age onboarding)
====================================================

PROTEA stores deployment credentials encrypted at rest in the
repository under ``secrets/*.enc.yaml`` using
`sops <https://github.com/getsops/sops>`_ with
`age <https://github.com/FiloSottile/age>`_ recipients. The
``.sops.yaml`` file at the repo root declares which age public keys are
allowed to decrypt each path glob; only holders of a matching age
private key can read the plaintext.

This page is the operational runbook (install, keypair, request access,
day-to-day commands, troubleshooting). For the architectural rationale
and the appendix-style reference, see
:doc:`/appendix/secrets` and ADR :doc:`/adr/D28-secrets-management`.

.. contents:: On this page
   :local:
   :depth: 2

Why sops + age
---------------

- Plaintext secrets stay out of git history (only the encrypted
  ``*.enc.yaml`` form is committed).
- Multiple recipients (devs, CI) decrypt the same file independently;
  no shared password to rotate when a single team member leaves.
- Granular path-based rules: a CI runner can be a recipient for
  ``secrets/secrets.prod.enc.yaml`` while not being a recipient for
  ``secrets/secrets.dev.enc.yaml``, or vice versa.
- ``sops`` integrates cleanly with ``$EDITOR`` so a recipient can edit
  encrypted files without touching plaintext on disk.

Onboarding: first-time setup
-----------------------------

A new developer joining the project needs three things:

1. ``age`` and ``sops`` installed on their workstation.
2. An age keypair on disk (private key only on their machine; public
   key shared with the team).
3. Their public key added to ``.sops.yaml`` by an existing recipient,
   followed by a re-encryption of the affected files so the new key is
   one of the recipients.

Install age and sops
~~~~~~~~~~~~~~~~~~~~~

**Debian / Ubuntu** (when the distro package is recent enough):

.. code-block:: bash

   sudo apt-get install -y age
   # sops is not packaged on most Debian releases; install from GitHub
   # releases (see below).

**From upstream release tarballs** (works on any Linux):

.. code-block:: bash

   # age v1.2.0
   curl -fsSL \
     https://github.com/FiloSottile/age/releases/download/v1.2.0/age-v1.2.0-linux-amd64.tar.gz \
     -o /tmp/age.tar.gz
   tar -xzf /tmp/age.tar.gz -C /tmp
   sudo install -m 0755 /tmp/age/age /tmp/age/age-keygen /usr/local/bin/

   # sops 3.9.1
   curl -fsSL \
     https://github.com/getsops/sops/releases/download/v3.9.1/sops-v3.9.1.linux.amd64 \
     -o /tmp/sops
   sudo install -m 0755 /tmp/sops /usr/local/bin/sops

**Without sudo** (install to ``~/.local/bin`` and add to ``PATH``):

.. code-block:: bash

   mkdir -p ~/.local/bin
   # ... same downloads as above, but copy to ~/.local/bin instead of
   # /usr/local/bin, then prepend ~/.local/bin to PATH in ~/.bashrc.
   echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc

Verify both tools are on ``PATH``:

.. code-block:: bash

   age --version
   sops --version

Generate your age keypair
~~~~~~~~~~~~~~~~~~~~~~~~~~

The conventional location for the sops age key file is
``~/.config/sops/age/keys.txt``. ``sops`` reads it via the
``SOPS_AGE_KEY_FILE`` environment variable or this default path.

.. code-block:: bash

   mkdir -p ~/.config/sops/age
   age-keygen -o ~/.config/sops/age/keys.txt
   chmod 600 ~/.config/sops/age/keys.txt

   # Print your public key (safe to share).
   age-keygen -y < ~/.config/sops/age/keys.txt

.. warning::

   ``~/.config/sops/age/keys.txt`` contains your **private** key. Never
   commit it, never paste it into a chat or a PR, never share it. The
   repo ``.gitignore`` excludes ``**/age/keys.txt`` defensively, but
   you should still treat the file like an SSH private key. Back it up
   somewhere offline; losing it means re-keying every file you were a
   recipient of.

Request access: add your pubkey to .sops.yaml
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Send your public key (the ``age1...`` string) to an existing recipient.
They will open a PR that:

1. Adds your pubkey under each ``creation_rules`` entry you need access
   to (typically all of them for active devs; CI keys only get the
   paths they need).
2. Re-encrypts every affected file so the new recipient list is
   embedded in the ciphertext header:

   .. code-block:: bash

      sops updatekeys secrets/secrets.dev.enc.yaml
      sops updatekeys secrets/secrets.prod.enc.yaml
      sops updatekeys secrets/example.enc.yaml

   ``sops updatekeys`` rewrites the file's recipient set in place using
   the current ``.sops.yaml`` rules; it does not change the underlying
   data key, so existing recipients keep working.

After the PR merges to ``develop`` you can decrypt any file your
pubkey is a recipient of:

.. code-block:: bash

   sops --decrypt secrets/secrets.dev.enc.yaml

Day-to-day workflow
--------------------

Decrypt for inspection
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   sops --decrypt secrets/secrets.dev.enc.yaml

Edit in place (encrypted)
~~~~~~~~~~~~~~~~~~~~~~~~~

``sops`` opens the plaintext in ``$EDITOR``, then re-encrypts on save.
The plaintext never touches disk.

.. code-block:: bash

   sops secrets/secrets.dev.enc.yaml

Encrypt a new plaintext file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``creation_rules`` in ``.sops.yaml`` are keyed off the **input
path**. Place the plaintext at its eventual ``*.enc.yaml`` location
first, then encrypt in place:

.. code-block:: bash

   cp my-new-secret.yaml secrets/my-new-secret.enc.yaml
   sops --encrypt --in-place secrets/my-new-secret.enc.yaml
   rm my-new-secret.yaml           # delete the plaintext copy
   git add secrets/my-new-secret.enc.yaml

If you forget the ``.sops.yaml`` rule for that path, ``sops`` exits
with ``no matching creation rules found``. Add a ``creation_rules``
entry for the new path glob before retrying.

Roundtrip verification
~~~~~~~~~~~~~~~~~~~~~~~

Before opening a PR that touches ``.sops.yaml`` or any
``secrets/*.enc.yaml``, run:

.. code-block:: bash

   sops --decrypt secrets/example.enc.yaml

A successful decrypt with the current ``~/.config/sops/age/keys.txt``
confirms your private key is one of the recipients embedded in the
file. The committed ``secrets/example.enc.yaml`` exists for exactly
this smoke test.

CI integration
---------------

GitHub Actions reads a single repository secret ``SOPS_AGE_KEY``
containing the full body of a CI-only ``keys.txt`` (the matching
public key is one of the recipients in ``.sops.yaml``). The workflow
writes that body to a temp file and exports ``SOPS_AGE_KEY_FILE``
before invoking ``sops --decrypt``:

.. code-block:: yaml

   - name: Decrypt secrets
     run: |
       mkdir -p $RUNNER_TEMP/sops
       printf '%s' "${{ secrets.SOPS_AGE_KEY }}" > $RUNNER_TEMP/sops/keys.txt
       chmod 600 $RUNNER_TEMP/sops/keys.txt
       export SOPS_AGE_KEY_FILE=$RUNNER_TEMP/sops/keys.txt
       sops --decrypt secrets/secrets.dev.enc.yaml > /tmp/dev.yaml

Rotating the CI key follows the same steps as rotating a human
recipient: generate a new keypair locally, update the GitHub Actions
secret, swap the pubkey in ``.sops.yaml``, ``sops updatekeys`` every
affected file, open a PR.

Rotation: removing a recipient
-------------------------------

When a recipient (human or CI) should no longer have access:

1. Remove their pubkey from every ``creation_rules`` entry in
   ``.sops.yaml``.
2. Run ``sops updatekeys`` on every ``*.enc.yaml`` so the file headers
   no longer list that recipient.
3. **Rotate the underlying secrets.** ``sops updatekeys`` only changes
   the recipient list; the data key inside the file is unchanged, and
   the departing party may still have a copy of the plaintext from
   before they were removed. Treat the credentials as exposed and
   rotate them at the source (postgres password, AMQP password, MinIO
   access keys, admin token, GitHub PAT, etc.) and re-encrypt the new
   values.

Troubleshooting
----------------

``no matching creation rules found``
    The input path does not match any ``path_regex`` in
    ``.sops.yaml``. Either move the file to a matching path or add a
    rule (in order from most specific to least specific).

``Failed to get the data key required to decrypt the SOPS file``
    Your private key is not one of the recipients embedded in the
    ciphertext. Either request your pubkey be added (see "Request
    access" above), or confirm ``SOPS_AGE_KEY_FILE`` points at the
    correct ``keys.txt``.

``age-keygen: command not found``
    The age install put the binaries in ``~/.local/bin`` but that
    directory is not on ``PATH``. Either reinstall to
    ``/usr/local/bin`` or run ``export PATH="$HOME/.local/bin:$PATH"``.

See also
---------

- :doc:`deployment` for how production credentials flow from sops
  through Docker Swarm secrets and Kubernetes secrets into the
  running stack.
- `sops documentation <https://github.com/getsops/sops#readme>`_ for
  the full command reference.
- `age documentation <https://github.com/FiloSottile/age#readme>`_
  for keypair handling.
