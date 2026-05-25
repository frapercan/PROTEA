Authentication
==============

PROTEA ships a self-contained authentication system (FARM-AUTH.1 through
FARM-AUTH.11, ADR D37) covering human users (email + password), programmatic
clients (API keys + JWT exchange), optional SMTP magic-link login, per-user
quota enforcement, session revocation, and a structured audit log. The design
is documented in :doc:`/adr/D37-feat-auth-users-roles-multi-instance`.

.. rubric:: Auth surfaces at a glance

Two independent credential flows coexist:

* **Human / browser flow.** ``POST /auth/signup`` creates a pending user
  account. An admin approves it via the ``/admin/users`` interface.
  Once active, ``POST /auth/login`` accepts email + password and sets an
  HttpOnly ``protea_session`` cookie carrying a signed HS256 JWT.
  ``POST /auth/logout`` revokes the cookie and the server-side session row.
  ``GET /auth/me`` returns the current user's profile and role.

* **Programmatic / API-key flow.** An admin or operator mints an API key at
  ``POST /v1/auth/api-keys``. Programmatic clients either pass the raw key in
  an ``Authorization: ApiKey <key>`` (or ``X-Api-Key: <key>``) header directly,
  or exchange it for a short-lived JWT at ``POST /auth/api-key-login`` and send
  subsequent requests as ``Authorization: Bearer <jwt>``.

All credential paths are validated by the same ``require_role(min_role)``
FastAPI dependency. Every mutable endpoint carries an explicit role floor;
GET endpoints remain open.

.. rubric:: Request gate (simplified)

.. code-block:: text

                      HTTP request
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  RATE LIMITER  (slowapi per-principal, FARM-AUTH.7)               │
   │  Key: API-key prefix · JWT sub · IP-hash (anonymous)             │
   └───────────────────────┬───────────────────────────────────────────┘
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  AUTH GATE  (require_role / require_api_key_or_bearer)            │
   │                                                                   │
   │  Credential forms accepted:                                       │
   │    Authorization: ApiKey <raw_key>                                │
   │    X-Api-Key: <raw_key>                                           │
   │    Authorization: Bearer <jwt>      (HS256, PROTEA_JWT_SECRET)    │
   │    Cookie: protea_session=<jwt>     (browser / human flow)        │
   │                                                                   │
   │  API-key check: sha256(raw_key) vs ApiKey.key_hash                │
   │  JWT check: HS256 sig + exp + iat + sub + jti; jti checked        │
   │             against user_session.revoked_at (cookie sessions)     │
   │  On failure: 401 + WWW-Authenticate: ApiKey, Bearer               │
   │                                                                   │
   │  Dev override: PROTEA_AUTHN_REQUIRED=false skips gate             │
   └───────────────────────┬───────────────────────────────────────────┘
                           │
   ┌───────────────────────▼───────────────────────────────────────────┐
   │  FASTAPI ROUTER  (role-gated by require_role(min_role))           │
   └───────────────────────────────────────────────────────────────────┘

.. rubric:: Role hierarchy

PROTEA recognises four roles in ascending privilege order:
``guest < researcher < operator < admin``.

.. list-table:: Role capabilities
   :header-rows: 1
   :widths: 14 86

   * - Role
     - Capabilities
   * - ``guest`` (anonymous)
     - No account row. Anonymous requests receive guest-level access.
       Read-only access to ``/benchmark``, ``/showcase``, ``/proteins``,
       ``/stack``, ``/docs``, ``/evaluation/*``, ``/datasets``,
       ``/reranker-models``. Anonymous quick-annotate via
       ``POST /annotate?save_history=false`` (IP-hash quota: 10 sequences/day).
   * - ``researcher``
     - Default role for newly approved accounts. Persistent annotation history,
       ``POST /annotate?save_history=true``, own job submission
       (``predict_go_terms`` and related lightweight operations),
       1 000 sequences/day, 100 jobs/day, one self-managed API key.
   * - ``operator``
     - All researcher capabilities plus pipeline management:
       ``POST /datasets`` (export_research_dataset),
       ``POST /reranker-models/import``,
       ``POST /jobs`` (run_cafa_evaluation, heavy ops),
       ``GET /maintenance``, ``GET /workers/status``.
   * - ``admin``
     - All operator capabilities plus user management (approve, role change,
       deactivate), DB maintenance (vacuum, reset), API-key management for any
       user, ``POST /maintenance``, ``POST /admin/reset-db``,
       ``DELETE /users/{id}``, ``GET /admin/audit``,
       ``POST /auth/admin/revoke-sessions/{user_id}``.

The ``require_role`` dependency normalises unknown role strings to
``researcher`` (the lowest named role), so a malformed JWT or a stale key
cannot escalate privileges.

.. rubric:: Endpoint access map

.. code-block:: text

   guest (anonymous):
     GET  /v1/benchmark /v1/showcase /v1/proteins /v1/stack /v1/docs
          /v1/evaluation/* /v1/datasets /v1/reranker-models /v1/scoring/*
     POST /v1/annotate?save_history=false   (IP-hash quota: 10/day)

   researcher (authenticated user):
     POST /v1/annotate?save_history=true
     GET  /v1/jobs/{own}
     POST /v1/jobs  (predict_go_terms, fetch_uniprot_metadata, etc.)
     POST /v1/auth/api-keys  (self only, one key)

   operator:
     POST /v1/datasets  (export_research_dataset)
     POST /v1/reranker-models/import*
     POST /v1/jobs  (run_cafa_evaluation, all heavy ops)
     POST /v1/maintenance/vacuum-*
     GET  /v1/maintenance /v1/workers/status
     POST|DELETE /v1/annotations/sets/*
     POST|DELETE /v1/scoring/configs*

   admin:
     POST /v1/auth/api-keys         (any user)
     GET  /v1/auth/api-keys
     DELETE /v1/auth/api-keys/{id}
     POST /v1/admin/reset-db
     DELETE /v1/users/{id}
     GET  /v1/admin/audit
     POST /v1/auth/admin/revoke-sessions/{user_id}

.. rubric:: Human login flow

1. **Signup.** ``POST /auth/signup`` with ``{email, username, display_name,
   password, intended_use}`` creates a ``User`` row with
   ``status=pending``. Returns ``{id, email, username, status}``. Returns
   409 on duplicate email or username.

2. **Approval.** An admin opens the ``/admin/users`` UI tab (Pending), reviews
   the ``intended_use`` field, and approves. The role defaults to
   ``researcher``; the admin may promote to ``operator`` at approval time.

3. **Login.** ``POST /auth/login`` with ``{email, password}``. Returns 401 for
   invalid credentials, 403 with ``account_pending_approval`` or
   ``account_deactivated`` for non-active accounts. On success:

   * Sets a ``protea_session`` cookie (HttpOnly, Secure, SameSite=Strict,
     Max-Age=30 days) containing a signed HS256 JWT.
   * Inserts a ``user_session`` row so the session can be revoked server-side
     without waiting for JWT expiry.
   * Updates ``last_login_at`` on the ``User`` row.
   * Records a ``login_ok`` audit event.
   * Returns ``{id, email, username, display_name, role, status}``.

4. **Session validation.** On every request carrying the cookie the middleware
   decodes the JWT and checks ``jti`` against ``user_session.revoked_at``. A
   revoked or missing row returns 401.

5. **Logout.** ``POST /auth/logout`` sets ``revoked_at`` on the session row and
   clears the cookie. Idempotent: returns 204 even when no cookie is present.

.. rubric:: API-key flow (programmatic clients)

Mint a key (admin or self for researcher with scope=researcher):

.. code-block:: bash

   curl -X POST http://localhost:8000/v1/auth/api-keys \
     -H 'Authorization: ApiKey <admin_key>' \
     -H 'Content-Type: application/json' \
     -d '{"name": "lab-runner-2026-05", "role": "operator"}'

Response (copy the raw key now; it is never stored):

.. code-block:: json

   {
     "id": "<uuid>",
     "prefix": "abc12345",
     "name": "lab-runner-2026-05",
     "key": "abc12345_the_rest_of_the_secret",
     "created_at": "2026-05-11T12:00:00+00:00",
     "role": "operator",
     "revoked_at": null,
     "last_used_at": null
   }

PROTEA stores only the sha256 hash and an 8-character display prefix. Lost
keys cannot be recovered; revoke and replace.

Exchange for a short-lived JWT (optional, useful for browser-initiated scripts):

.. code-block:: bash

   curl -X POST http://localhost:8000/auth/api-key-login \
     -H 'Content-Type: application/json' \
     -d '{"api_key": "abc12345_...", "ttl_seconds": 3600}'

The response contains ``{token, token_type, expires_in, role, sub}``. Pass
the token as ``Authorization: Bearer <token>`` on subsequent requests.

Revoking a key:

.. code-block:: bash

   curl -X DELETE http://localhost:8000/v1/auth/api-keys/<key_id> \
     -H 'Authorization: ApiKey <admin_key>'

The row is preserved with ``revoked_at`` set; subsequent uses return 401.

.. rubric:: Session cookie (browser flow)

* **Name:** ``protea_session``
* **Algorithm:** HS256, secret from ``PROTEA_JWT_SECRET``
* **Attributes:** HttpOnly, Secure, SameSite=Strict, Max-Age=30 days
* **Payload:** ``{sub, jti, role, status, exp, iat}``

The ``jti`` is a random UUID. On login a ``user_session`` row is inserted with
``token_hash = sha256(raw_jwt)``. On logout ``revoked_at`` is set. The
``/auth/me`` endpoint checks ``revoked_at`` on every call and updates
``last_seen_at`` so admins can distinguish stale sessions from active ones.

.. rubric:: Optional SMTP integration (FARM-AUTH.11)

If ``PROTEA_SMTP_*`` env vars are set, magic-link login and email-driven
password reset are enabled via ``POST /auth/magic-link`` and
``POST /auth/reset-password``. Without SMTP both features are disabled and
password reset is admin-driven out of band. The deployment is fully functional
without SMTP.

.. rubric:: Quota system (FARM-AUTH.6 and FARM-AUTH.7)

Anonymous (guest) requests are throttled by daily-rotated IP hash. Named users
are throttled by user ID. Limits are configurable per role in
``protea/config/system.yaml`` under the ``auth.quotas`` block.

Default limits:

.. list-table::
   :header-rows: 1
   :widths: 24 20 20 36

   * - Resource
     - researcher
     - operator
     - Notes
   * - Sequences/day (annotate)
     - 1 000
     - unlimited
     - guest: 10/day by IP hash
   * - Jobs/day
     - 100
     - unlimited
     -
   * - ``POST /v1/jobs``
     - 10/minute
     - 10/minute
     - ``PROTEA_RATELIMIT_JOBS`` override
   * - ``POST /v1/datasets``
     - (not permitted)
     - 5/minute
     - ``PROTEA_RATELIMIT_DATASETS`` override
   * - ``POST /v1/auth/api-keys``
     - 5/hour (self only)
     - 5/hour
     - ``PROTEA_RATELIMIT_API_KEYS`` override
   * - ``POST /auth/api-key-login``
     - 5/hour
     - 5/hour
     -

Exceeding a limit returns 429 with a ``Retry-After`` header. Overrides accept
any slowapi syntax (e.g. ``"100/minute"``).

.. rubric:: Audit log

Every security-sensitive action is appended to the ``audit_log`` table:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Action
     - Recorded on
   * - ``signup``
     - New account created
   * - ``login_ok`` / ``login_fail``
     - Login attempt (success or failure)
   * - ``logout``
     - Explicit logout
   * - ``role_change``
     - Admin changes a user's role
   * - ``user_approve``
     - Admin approves a pending account
   * - ``user_deactivate``
     - Admin deactivates an account
   * - ``api_key_mint``
     - New API key created
   * - ``api_key_revoke``
     - API key revoked
   * - ``admin_session_revoke``
     - Admin revokes all sessions for a user
   * - ``db_reset``
     - Admin triggers ``POST /admin/reset-db``

The audit log is append-only and queryable at ``GET /v1/admin/audit`` (admin
role required).

.. rubric:: Bootstrap admin (multi-instance)

Three modes, evaluated in order on first startup:

a. **Env var bootstrap:** if ``PROTEA_BOOTSTRAP_ADMIN_EMAIL`` is set and no
   admin row exists, an admin user is created (password from
   ``PROTEA_BOOTSTRAP_ADMIN_PASSWORD`` or printed once to stderr).
b. **CLI:** ``protea-cli admin add-user`` with the email, role, and
   password-prompt flags for break-glass situations.
c. **Neither:** the instance is functional for guest access but ``/admin`` is
   unreachable; pending signup requests accumulate. Valid posture when the
   admin acts solely via the CLI.

All three modes are idempotent: re-running with an existing email is a no-op.
See :doc:`/runbooks/deployment-process-stack` for the full bootstrap walkthrough.

.. rubric:: Dev override

For local development without credentials, set
``PROTEA_AUTHN_REQUIRED=false`` in the API process environment. The
``require_role`` dependency short-circuits and passes every request through as
the maximum role. The default is ``true``, so production deployments stay safe
without explicit configuration.

.. rubric:: Data model

Five ORM tables back the auth system (``protea/infrastructure/orm/models/``):

* ``user``: ``id`` (UUID PK), ``email``, ``username``, ``display_name``,
  ``password_hash`` (argon2id), ``role`` (UserRole enum), ``status``
  (UserStatus enum), ``intended_use``, ``created_at``, ``last_login_at``.

* ``user_session``: ``id``, ``user_id`` (FK), ``token_hash`` (sha256 of raw
  JWT), ``expires_at``, ``revoked_at``, ``last_seen_at``, ``user_agent``,
  ``client_ip_hash``.

* ``api_key``: ``id``, ``user_id`` (FK), ``name``, ``prefix``, ``key_hash``
  (sha256 of raw key), ``role`` (scope bounded by owner's role),
  ``created_at``, ``last_used_at``, ``revoked_at``.

* ``quota``: ``key`` (user UUID or daily-rotated IP hash), ``resource``,
  ``period_start``, ``count``, ``limit``.

* ``audit_log``: append-only; ``actor_user_id`` (nullable), ``action``,
  ``target``, ``payload`` (JSONB), ``occurred_at``.

All five tables were introduced in FARM-AUTH.1 (PR #489) and FARM-AUTH.7
(PR #499). Migrations live in ``alembic/versions/``.

.. seealso::

   - :doc:`/adr/D37-feat-auth-users-roles-multi-instance`: full design rationale.
   - :doc:`/adr/D06-authentication`: prior auth strategy (Authentik/OIDC), superseded by D37.
   - :doc:`/runbooks/deployment-process-stack`: bootstrap and rotation runbooks.
