ADR-D14: Per-plugin repository granularity (deferred)
======================================================

:Status: Deferred
:Date: 2026-05-05
:Phase: F9 (post-defense)

Context
-------
Structure C ships plugins grouped per concept (sources, runners,
backends), not per individual plugin. This is simpler today (3 group
repos vs 9-12 micro-repos) but might invert if third parties publish
their own plugins and prefer independent release cadence.

Decision
--------
Defer the split until after defense. If third parties materialise,
splitting a group into per-plugin repos is estimated at 0.5-1 day per
sub-module, contained in F9.

Consequences
------------
- Until then, third parties contribute via PR to the relevant group
  repository.
- Group repos must keep ``entry_points`` boundaries clean to make a
  later split mechanical.

Resolution
----------
Deferred to F9 post-defense.
