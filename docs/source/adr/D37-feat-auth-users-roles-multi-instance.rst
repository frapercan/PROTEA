ADR-D37: Single auth system, manual approvals, multi-instance (FEAT-AUTH)
=========================================================================

:Status: Accepted
:Date: 2026-05-25
:Author: Francisco Miguel Pérez Canales
:Phase: F-AUTH

Context
~~~~~~~

PROTEA currently has two parallel authentication surfaces that conflict
with each other:

1. A legacy ``PROTEA_ADMIN_TOKEN`` bearer scheme, enforced by
   ``_require_admin_token`` scattered across ``/admin/*`` and
   ``/maintenance/*`` routes. This token is a single shared secret with
   no per-user identity or audit trail.
2. A partially-wired session JWT (HttpOnly cookie) introduced in an
   earlier iteration of FEAT-AUTH (PR #456), covering only a subset of
   endpoints.

Neither surface provides per-user roles, quotas, or an audit log. The
combination creates undefined behavior on endpoints that are caught by
one mechanism but not the other, and makes it impossible to delegate
different privilege levels to different collaborators in a deployment.

PROTEA deployments are academic, non-commercial, and multi-instance by
design. Each institution or research group runs a sovereign instance
with its own data, its own user table, and its own administrator. There
is no shared identity plane across instances.

The operational tax of automated email verification and OAuth provider
integrations outweighs their benefit for the target deployment
population. Manual administrator approval is the correct posture: the
admin already performs due-diligence vetting of signup requests at
approval time, making automated email verification redundant.

PR #456 (partial FEAT-AUTH) introduced the session JWT cookie and a
preliminary ``User`` model but did not complete the role hierarchy,
quota subsystem, audit log, or frontend. This ADR supersedes the
partial decisions implicit in PR #456 and records the complete design
adopted in the F-AUTH phase.

Decision
~~~~~~~~

**1. Role hierarchy**

Four roles form a strict linear order: guest < researcher < operator < admin.
A single ``require_role(min_role)`` FastAPI dependency enforces the
minimum. No endpoint may call ``_require_admin_token``; that function is
deleted in FARM-AUTH.4.

Roles and their capabilities:

.. list-table:: Role capabilities
   :header-rows: 1
   :widths: 14 86

   * - Role
     - Capabilities
   * - ``guest``
     - Read-only access to ``/benchmark``, ``/showcase``, ``/proteins``,
       ``/stack``, ``/docs``, ``/evaluation/*``, ``/go-timeline``,
       ``/datasets``, ``/reranker-models``. Quick-annotate via
       ``POST /annotate?save_history=false`` up to 10 sequences per day
       per IP-hash (anonymous, no persistence).
   * - ``researcher``
     - All guest capabilities plus persistent annotation history,
       ``POST /annotate?save_history=true``, ``GET /jobs/{own}``,
       ``POST /jobs`` for ``predict_go_terms`` and related lightweight
       operations, 1000 sequences per day, 100 jobs per day, one
       self-managed API key.
   * - ``operator``
     - All researcher capabilities plus queue management, dispatch of
       heavy operations (``export_research_dataset``,
       ``train_reranker``, ``run_cafa_evaluation``), ``POST
       /datasets``, ``POST /reranker-models/import``, ``GET
       /maintenance``, ``GET /workers/status``.
   * - ``admin``
     - All operator capabilities plus user management (approve, role
       change, deactivate), DB maintenance (vacuum, reset), mint API
       keys for any user, ``POST /maintenance``, ``POST
       /admin/reset-db``, ``DELETE /users/{id}``, ``GET /admin/audit``.

**2. Identity backbone (ORM tables)**

``User`` table: ``id`` (UUID PK), ``email`` (unique), ``username``
(unique), ``display_name``, ``password_hash`` (argon2id),
``role`` (enum: guest, researcher, operator, admin), ``status`` (enum:
pending, active, deactivated), ``intended_use`` (text), ``created_at``,
``last_login_at``, ``deactivated_at``.

``session_revocation`` table: ``jti`` (PK), ``user_id`` (FK User),
``revoked_at``, ``reason``. Checked by the session-auth middleware on
every request carrying a JWT. Enables both per-session and
"sign-out everywhere" revocation without a full token store.

``api_key`` table: ``id`` (UUID PK), ``user_id`` (FK User), ``name``,
``hash`` (sha256 of the raw secret), ``scope`` (subset of the user's
role, as a role-enum value), ``created_at``, ``last_used_at``,
``expires_at`` (nullable), ``revoked_at`` (nullable). The raw secret is
shown exactly once at creation and never stored. API-key scope is
bounded above by the owner's current role.

``quota`` table: ``key`` (user_id or daily-rotated-salt IP hash),
``resource`` (enum: annotate, job), ``period_start`` (UTC date),
``count`` (integer), ``limit`` (integer). Guest quota is keyed by
IP-hash using the same daily-salt rotation as
``VisitorCounter``. Quota limits are configurable per role via a YAML
block in ``protea/config/system.yaml``.

``audit_log`` table: append-only, ``id`` (UUID PK), ``actor_user_id``
(FK User, nullable for anonymous), ``action`` (text), ``target``
(text), ``payload`` (JSONB), ``occurred_at``. Actions recorded: login
success and failure, role change, API key mint and revoke, user
deactivate, DB reset, signup approval.

**3. Session JWT**

The session JWT cookie introduced in PR #456 is retained
(``Secure``, ``SameSite=Strict``). The middleware is extended to
check ``jti`` against ``session_revocation`` before accepting the
token. The JWT payload gains a ``role`` claim, avoiding a DB lookup on
every request for the common case; the middleware falls back to DB if
the claim is absent (backward compat with tokens minted before this
ADR).

**Amendment (FARM-AUTH.10, LOGIN-PERSIST-DEBUG)**: the ``HttpOnly``
attribute was dropped. The entire frontend chrome (``lib/auth.ts``,
``lib/api.ts``, ``useRole``, ``AuthChip``, sidebar admin gate, every
POST/PATCH/DELETE) reads the JWT via ``document.cookie`` to render
role-conditional UI and to mint ``Authorization: Bearer`` headers on
mutations. Marking the cookie ``HttpOnly`` hides it from JavaScript and
collapses the chrome to anonymous after a successful login; the symptom
is "the login does not persist". A purely server-side cookie-auth path
that the gate would honor without a Bearer header was considered and
rejected as scope: it would require teaching ``require_api_key_or_bearer``
to read the cookie, plus a CSRF token surface to preserve the protection
``SameSite=Strict`` alone gives us with a Bearer header on every
mutation. The realistic CSRF / network surface is still covered by
``SameSite=Strict`` + ``Secure``; the JWT remains short-lived (30 day
``exp``) and server-revocable via the ``user_session`` table.

**4. Endpoint access map**

Minimum role per route family (specific endpoints can downgrade via an
explicit ``require_role`` call):

.. code-block:: text

   guest:       GET /benchmark /showcase /proteins /stack /docs
                    /evaluation/* /go-timeline /datasets /reranker-models
                POST /annotate?save_history=false  (quota: 10/day/IP)
   researcher:  POST /annotate?save_history=true
                GET  /jobs/{own}
                POST /jobs (predict_go_terms, fetch_uniprot_metadata, etc.)
                POST /auth/api-keys (self only)
   operator:    POST /datasets (export_research_dataset)
                POST /reranker-models/import
                POST /jobs (run_cafa_evaluation)
                GET  /maintenance /workers/status
   admin:       POST /maintenance (vacuum, prune)
                POST /admin/reset-db
                DELETE /users/{id}
                POST /auth/api-keys (other user)
                GET  /admin/audit

**5. Bootstrap admin (multi-instance critical)**

Three bootstrap modes, evaluated in this order on startup:

a. **Env var**: if ``PROTEA_BOOTSTRAP_ADMIN_EMAIL`` is set and no admin
   row exists, create an admin user (password prompted from stderr on
   first run or generated and printed once).
b. **CLI**: ``protea-cli admin add-user`` with ``email``, ``role``, and
   ``password-prompt`` flags for break-glass situations.
c. **Neither**: the instance is functional but ``/admin`` is
   unreachable; signup requests pile up in ``pending`` status. This is
   a valid production posture when the admin acts solely via the CLI.

All three modes are idempotent (re-running a bootstrap with an existing
email is a no-op).

The three bootstrap modes are documented in
``docs/source/runbooks/deployment-process-stack.rst``.

**6. Signup and approval flow**

``POST /auth/signup`` accepts email, display_name, and intended_use.
The row is inserted with ``status=pending`` and ``role=researcher``
(pending approval). No automated email is sent unless SMTP is
configured (FARM-AUTH.11). The admin sees pending requests in the
``/admin/users`` UI tab and approves each one, optionally overriding
the role to operator or admin at approval time.

``POST /auth/login`` accepts email and password. Successful login
rotates the session JWT cookie and records ``last_login_at``. Failed
login is recorded in ``audit_log``.

**7. Optional SMTP integration (FARM-AUTH.11)**

If ``PROTEA_SMTP_*`` env vars are set, magic-link login and
email-driven password reset are enabled. If SMTP is unconfigured (the
default), both features are disabled; password reset is admin-driven
out-of-band. The deployment stays fully functional without SMTP.

**8. Frontend pages**

- ``/signup``: email, display_name, intended_use textarea. On submit:
  creates pending row, shows "Your request has been sent to the admin."
- ``/login``: email and password. Magic-link button shown only when
  SMTP is configured.
- ``/profile``: change password, manage own API keys, see own quota
  usage, job history.
- ``/admin/users``: tabbed view (Pending / Active / Deactivated). Per-row
  actions: approve with role selection, deactivate, reset password,
  mint an API key on behalf of the user.
- ``AuthChip``: avatar, username, role badge. Menu: Sign out, Profile,
  Admin (if admin), Operator console (if operator).

Consequences
~~~~~~~~~~~~

**Positive**

- A single ``require_role(min_role)`` dependency replaces the dual
  ``_require_admin_token`` and session-JWT surface. Every new endpoint
  uses the same guard; there is no hidden second path.
- Per-user identity enables an audit log, per-user quota, and per-user
  API key management.
- Manual approval is operationally cheap and consistent with the
  academic deployment posture: no email provider configuration is
  required.
- Each deployment is sovereign. There is no shared identity plane, no
  cross-instance dependency, and no cloud service account to rotate.
- The quota subsystem is explicit anti-abuse infrastructure, not a
  monetization gate. Researcher limits (1000 seq/day, 100 jobs/day) are
  generous for interactive use and configurable per deployment.

**Negative**

- The ``User``, ``session_revocation``, ``api_key``, ``quota``, and
  ``audit_log`` tables require five new Alembic migrations (split
  across FARM-AUTH.1 and FARM-AUTH.7 to keep each migration reviewable).
- Deleting ``_require_admin_token`` (FARM-AUTH.4) is a breaking change
  for any consumer that passes ``PROTEA_ADMIN_TOKEN``; those consumers
  must be updated to use a researcher or operator account with an API
  key.
- The PR #456 partial implementation (session JWT and preliminary User
  model) must be reconciled; any divergence from the schema defined
  here must be resolved in FARM-AUTH.1 before FARM-AUTH.2 onwards can
  proceed.
- Frontend work (FARM-AUTH.10) is substantial: four new pages plus the
  AuthChip refactor. It cannot share state with the API work and is
  therefore gated on FARM-AUTH.3 (login/signup endpoints).

**Neutral**

- SMTP integration (FARM-AUTH.11) is optional. Deployments that omit
  it lose magic-link login and email password reset but gain no
  operational dependency on an external email service.
- The guest role is not stored in the ``User`` table. Anonymous
  requests that pass no credentials are treated as guest by the
  middleware; the quota check uses an IP-hash key in the ``quota``
  table rather than a user row.

Rejected alternatives
~~~~~~~~~~~~~~~~~~~~~

**OAuth providers (Google, GitHub, etc.)**

Requires per-deployment OAuth app registration, client-secret rotation,
and "Terms of Service" pages to satisfy provider policies. The
operational tax is not justified for academic deployments where the
administrator knows every expected user personally. Manual approval
provides stronger vetting than OAuth email-ownership proof.

**Mandatory email verification**

The admin already performs identity vetting at approval time, making
automated email verification a redundant step. It also introduces a
dependency on an email transport that many academic deployments do not
have configured.

**Monetization-style quota upsells**

PROTEA is a non-commercial research tool. Quota exists as an
anti-abuse safeguard, not as a conversion mechanism. The "upgrade your
plan" framing is explicitly out of scope.

**Cross-instance SSO (shared identity plane)**

Each PROTEA deployment serves a distinct research context with its own
data and its own user population. A shared identity plane would couple
otherwise independent deployments and introduce a single point of
failure. Sovereignty per instance is the correct invariant.

**Keycloak or Authentik OIDC (D6 resolution)**

ADR D6 accepted Authentik as the OIDC provider. That decision predates
the multi-instance requirement and the recognition that OIDC setup
requires per-deployment OAuth app configuration and a running Authentik
container. The simpler argon2id-native path (no external IdP container)
reduces the deployment surface from three processes (PROTEA API,
PROTEA frontend, Authentik) to two, which is a meaningful operational
improvement for researchers self-hosting on a single machine.

References
~~~~~~~~~~

- ADR D6 (``D06-authentication.rst``): prior auth strategy, superseded
  by this document for the F-AUTH phase.
- PR #456: partial FEAT-AUTH implementation (session JWT and preliminary
  User model); schema reconciled in FARM-AUTH.1.
- Memory entry ``project_orphan_jobs_2026_05_18``: reminder that any
  auth or CI surface change must be reviewable; cited as motivating
  evidence for the single-dependency design.
- ``docs/source/runbooks/deployment-process-stack.rst``: the three
  bootstrap modes documented in F7.7 / PR #479 runbook.
- ``protea/api/auth/``: implementation home for the role dependency,
  password helpers, and JWT middleware (FARM-AUTH.1 through FARM-AUTH.5).
- ``protea/infrastructure/orm/models/user.py``: ``User``,
  ``session_revocation``, ``api_key``, ``quota``, ``audit_log`` ORM
  models (FARM-AUTH.1).
- Slice catalog: ``agent-farm/plans/farm-platform/PLAN.md``,
  phase ``F-AUTH`` (FARM-AUTH.1 through FARM-AUTH.11).
