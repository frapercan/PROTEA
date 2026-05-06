ADR-D6: Authentication strategy
================================

:Status: Accepted
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
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
**Accepted as recommended, with the OIDC provider pinned.**

- API key path: ``ApiKey`` ORM table + ``Authorization: Bearer …`` for
  service-to-service calls (LAFA containers, downstream pipelines).
- OIDC path: **Authentik** as the identity provider behind
  ``oauth2-proxy`` for human users. User picked Authentik on
  2026-05-06 ("contra menos custom mejor"); Authentik chosen for its
  lighter footprint and simpler Docker-Compose setup vs Keycloak's
  JBoss-flavour weight.

Rate limiting via ``slowapi`` per F5 plan. Implementation gate at F5
entry (T5.6).
