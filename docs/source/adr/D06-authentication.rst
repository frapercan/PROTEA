ADR-D6: Authentication strategy
================================

:Status: Pending
:Date: 2026-05-05
:Phase: F5
:Gate: opens at F5 entry

Context
-------
Sensitive endpoints (job creation, dataset import, re-ranker model
upload, evaluation triggers) are currently unauthenticated. Public
exposure (cloud deployment, LAFA submission tooling, external adopters)
requires an authentication layer.

Decision (recommended)
----------------------
Two complementary mechanisms:

- **API key** for service-to-service calls (``ApiKey`` ORM table,
  ``Authorization: Bearer …``).
- **OIDC** via reverse proxy (oauth2-proxy) for human users.

Rate limiting via ``slowapi``.

Consequences
------------
- Migration adds ``ApiKey`` table.
- ``deploy/nginx/`` ships an oauth2-proxy configuration.
- Rate-limit policy documented per endpoint.

Resolution
----------
Pending; gate opens with F5 (T5.6).
