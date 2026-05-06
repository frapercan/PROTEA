ADR-D8: UI component library
=============================

:Status: Accepted
:Date: 2026-05-05
:Phase: F8a

Context
-------
The front-end needs a consistent component system supporting
accessibility AA, dark mode, and rapid composition of dashboards
(jobs, embeddings, predictions, evaluation, experiments).

Decision
--------
``shadcn/ui`` on top of Tailwind v4. Components are copy-paste owned
under ``apps/web/components/ui/``, not imported from a versioned
library.

Consequences
------------
- Customisation is in-tree; no library version bumps.
- Visual regression coverage via Chromatic or Percy (T8a.2).
- Lighthouse a11y target ≥95.

Resolution
----------
Closed.
