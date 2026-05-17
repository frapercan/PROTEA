ADR-D4: API versioning strategy
===============================

:Status: Accepted
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
:Phase: F4
:Gate: opens at F4 entry

Context
-------
PROTEA exposes a REST API consumed by the front-end and by external
clients (LAFA containers, downstream pipelines). As the API surface
stabilises, breaking changes need a versioning strategy that does not
strand existing consumers.

Decision (recommended)
----------------------
Universal ``api_v1`` (first major) path prefix on all endpoints. Future
major bumps branch into a parallel ``api_v2`` mount without breaking
existing ``api_v1`` consumers. ``Accept`` header negotiation only
considered if a real need surfaces.

Consequences
------------
- Front-end fetchers and external clients update once.
- OpenAPI documents per version.
- Schemathesis runs against the version under test.

Resolution
----------
**Accepted as recommended.** Universal ``api_v1`` prefix on all routers.
Implementation when F4 entry opens (T4.1); keep deprecated unprefixed
mounts for one release to avoid breaking ``protea.ngrok.app`` and
front-end clients during the transition.
