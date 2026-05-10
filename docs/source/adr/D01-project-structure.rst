ADR-D1: Project structure (7 code repositories plus thesis)
=============================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F0 (closed); enacted across F0-F2
:Supersedes: earlier monolith assumption in plan v1

Context
-------
PROTEA started as a single repository combining the API, workers, ORM,
front-end, four PLM backends, three annotation sources, and the LightGBM
re-ranker training pipeline. As the system grew towards eight backends
and external adoption became a goal, plugin extensibility for third
parties surfaced as a primary architectural concern.

Decision
--------
Structure C: seven code repositories plus the thesis manuscript. Plugins
are discovered via Python ``entry_points``. Granularity is per group
(sources, runners, backends), not per individual plugin. Thesis lives at
``~/Thesis2/thesis/``.

Repos: ``protea-core``, ``protea-contracts``, ``protea-method``,
``protea-cafaeval``, ``protea-sources``, ``protea-runners``,
``protea-backends``.

Consequences
------------
- Adding a new backend, source, or runner touches one repository or one
  sub-module within the relevant group repo.
- ``protea-method`` ships independently of the platform.
- Cross-repo release coordination required (see D29).
- Per-plugin repository granularity deferred to F9 post-defense (see D14).

Resolution
----------
Closed in master plan v3, 2026-05-05.
