ADR-D39: Defense-in-depth guards on destructive database operations
===================================================================

:Status: Accepted
:Date: 2026-06-03
:Author: Francisco Miguel Pérez Canales
:Phase: F-SEC

Context
~~~~~~~

The live PROTEA Postgres has been wiped four times. A forensic audit of
the codebase traced two distinct paths that can destroy a real schema
with no meaningful guard:

1. **Test fixture path.** ``tests/conftest.py::postgres_url`` has an
   ``external_db`` branch: when all four ``PROTEA_PG_USER``,
   ``PROTEA_PG_PASSWORD``, ``PROTEA_PG_DB`` and ``PROTEA_PG_PORT`` env
   vars are set, the fixture assumes an externally-managed Postgres and
   yields that connection URL directly (skipping the throwaway Docker
   container). Roughly fifteen ``*_pg.py`` integration tests then call
   ``Base.metadata.drop_all(engine); create_all(engine)`` against it.
   If those env vars are inherited from a sourced ``.env`` that points
   at the dev or prod Postgres (the canonical secret file exports
   exactly those names), a routine ``pytest --with-postgres`` run drops
   the live schema. There was no guard at all.

2. **HTTP path.** ``POST /admin/reset-db`` runs
   ``DROP SCHEMA public CASCADE``. It is gated by
   ``require_role("admin")``, but ``roles.py::role_of(None)`` returns
   ``ROLE_ADMIN`` as a dev convenience ("gate disabled in dev"). When
   ``PROTEA_AUTHN_REQUIRED`` is falsy the auth dependency returns a
   ``None`` principal, which ``role_of`` promotes to admin, so an
   unauthenticated caller on a dev stack can drop the schema.

Decision
~~~~~~~~

Add default-deny, opt-in guards on both paths. The convenience defaults
(Docker-managed throwaway DB for tests, ``None`` to admin for read-only
dev routes) are preserved everywhere except the destructive paths.

**Test fixture guard.** Before yielding an external DB URL, the fixture
calls ``_guard_external_db``. It aborts the session via ``pytest.fail``
when the resolved database name is a known dev/prod store
(``protea`` or ``biodata``, case-insensitive) or the target is the dev
Postgres (loopback host on port ``5432``). The explicit sentinel
``PROTEA_ALLOW_DESTRUCTIVE_TESTS=1`` overrides the guard for the rare
operator who really does want to run the destructive suite against a
named DB. A normal CI or local ``pytest`` run can no longer drop the
dev or prod schema.

**reset-db route guard.** Two checks are added ahead of the
``DROP SCHEMA`` on ``POST /admin/reset-db``:

* The handler rejects a ``None`` principal with ``401``. A genuine admin
  is always an ``ApiKey`` or a ``BearerPrincipal``; ``None`` means the
  auth gate is disabled, and the dev ``None`` to admin fallback is not
  accepted for this destructive operation. The global dev convenience is
  left untouched for read-only routes.
* The handler additionally requires the explicit sentinel
  ``PROTEA_ALLOW_DB_RESET=1`` on the API process, returning ``403`` when
  it is unset, even for a real authenticated admin.

Consequences
~~~~~~~~~~~~

* ``pytest --with-postgres`` against a ``PROTEA_PG_*`` env that points at
  ``protea``/``biodata`` or ``localhost:5432`` now fails fast with a
  clear message instead of dropping the schema.
* The reset-db endpoint cannot fire on a no-auth dev stack and stays
  inert in production unless an operator deliberately exports
  ``PROTEA_ALLOW_DB_RESET=1`` for the lifetime of a planned reset.
* Both guards are opt-in by a single env var, keeping the legitimate
  recovery workflows (intentional throwaway-DB suites, deliberate schema
  resets) one explicit flag away.
