PROTEA stack
============

.. note::

   This page is generated from ``docs/source/_data/stack.yaml``.
   Run ``python scripts/sync_stack.py`` to regenerate it.

PROTEA is split across eight repositories. The platform repository
(this one) hosts the orchestration, ORM, queue, and HTTP surface;
the rest are pluggable contracts, runtime modules, and tooling.

.. list-table::
   :header-rows: 1
   :widths: 18 14 12 56

   * - Repository
     - Role
     - Status
     - Summary
   * - `PROTEA <https://github.com/frapercan/PROTEA>`_
     - Platform
     - ``active``
     - Backend platform. Hosts the ORM, job queue, FastAPI surface, frontend, and orchestration.
   * - `protea-contracts <https://github.com/frapercan/protea-contracts>`_
     - Contracts
     - ``beta``
     - Shared contract surface. ABCs, pydantic payloads, feature schema, schema_sha. Imported by every other repo.
   * - `protea-method <https://github.com/frapercan/protea-method>`_
     - Inference
     - ``active``
     - Pure inference path (KNN, feature compute, reranker apply). Delegation target for the F2C extraction; live in production since F2C.5b. Bind-mounted by the LAFA containers.
   * - `protea-sources <https://github.com/frapercan/protea-sources>`_
     - Source plugin
     - ``active``
     - Annotation source plugins (GOA, QuickGO, UniProt). Discovered via Python entry_points (goa, quickgo, uniprot).
   * - `protea-runners <https://github.com/frapercan/protea-runners>`_
     - Runner plugin
     - ``active``
     - Experiment runner plugins (LightGBM, KNN, baseline). Discovered via Python entry_points (lightgbm, knn, baseline).
   * - `protea-backends <https://github.com/frapercan/protea-backends>`_
     - Backend plugin
     - ``active``
     - Protein language model embedding backends (ESM family, T5/ProstT5, Ankh, ESM3-C). Discovered via Python entry_points (esm, t5, ankh, esm3c).
   * - `protea-reranker-lab <https://github.com/frapercan/protea-reranker-lab>`_
     - Lab
     - ``active``
     - LightGBM reranker training lab. Pulls datasets from PROTEA, trains boosters, publishes them back via /reranker-models/import-by-reference.
   * - `cafaeval-protea <https://github.com/frapercan/cafaeval-protea>`_
     - Evaluator
     - ``active``
     - Standalone fork of cafaeval (CAFA-evaluator-PK) with the PK-coverage fix and a bit-exact parity guarantee against the upstream.

Cross-cutting concerns
----------------------

Every other repository depends on ``protea-contracts`` as its
shared surface (ABCs, payloads, feature schema). The platform
discovers source, runner and backend plugins via Python
``entry_points`` groups ``protea.sources``, ``protea.runners``,
and ``protea.backends``.

