ADR-D2: ``export_research_dataset`` lives in ``protea-core``
=============================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F1

Context
-------
The export operation produces frozen ``train.parquet`` and ``eval.parquet``
artefacts consumed by the LightGBM lab. It needs the feature schema, the
KNN reference cache, and access to the relational data model. Two options
were considered: keep it in ``protea-core``, or move it into the
``protea-runners`` repository alongside the LightGBM trainer.

Decision
--------
Keep ``export_research_dataset`` in ``protea-core``. The feature schema is
imported from ``protea-contracts``.

Consequences
------------
- Schema bumps in ``protea-contracts`` force a new ``protea-core`` release
  but not a new ``protea-runners`` release.
- The lab consumes the dataset over the artifact store, no Python import
  coupling.

Resolution
----------
Closed.
