ADR-D5: Front-end co-located in ``protea-core``
================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F1

Context
-------
The Next.js front-end is the primary consumer of the PROTEA REST API and
relies tightly on its contract. Two options: keep it inside
``protea-core/apps/web``, or split into a separate ``protea-web`` repo.

Decision
--------
Keep the front-end in ``protea-core/apps/web``. Coupling the API to its
primary consumer reduces contract drift; separation can happen later if a
dedicated UI team or external front-end appears.

Consequences
------------
- One repository contains both API and UI commits; release tags cover
  both surfaces simultaneously.
- Splitting later costs roughly one week.

Resolution
----------
Closed.
