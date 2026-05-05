ADR-D9: OBSOLETE: lab as runtime dependency
=============================================

:Status: Obsolete
:Date: 2026-05-05
:Supersedes: earlier plan revision (v1)
:Superseded-by: D1 (Structure C)

Context
-------
An earlier revision of the plan considered shipping the
``protea-reranker-lab`` repository as a runtime dependency of
``protea-core`` so that LightGBM training could execute inside the
PROTEA worker pool.

Decision
--------
Obsolete. Plan v3 adopts Structure C: the lab merges into
``protea-runners.lightgbm`` as a plugin discovered via ``entry_points``.
There is no runtime coupling.

Consequences
------------
- ``protea-runners.lightgbm`` is the canonical home for LightGBM
  training.
- The dataset-publishing contract (Dataset row + artifact store URI)
  remains the only interface between platform and trainer.

Resolution
----------
Declared obsolete on 2026-05-05.
