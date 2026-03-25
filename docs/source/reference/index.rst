API Reference
=============

This section documents every public module in PROTEA at the symbol level.
It is generated from source docstrings via Sphinx autodoc and is always
in sync with the installed codebase.

The reference is organised into four pages:

:doc:`core`
   The domain layer: the ``Operation`` protocol and ``OperationRegistry``,
   shared HTTP utilities, KNN search backends, feature engineering, scoring,
   metrics, evaluation, re-ranker, and all registered operations.

:doc:`infrastructure`
   The persistence and messaging layer: SQLAlchemy ORM models (including
   evaluation sets, scoring configs, and support entries), session management,
   logging, RabbitMQ publisher and consumer, and the configuration loader.

:doc:`api`
   The HTTP API: FastAPI application factory, all eleven routers, and a
   complete endpoint summary table.

:doc:`workers`
   The execution layer: ``BaseWorker`` (two-session job lifecycle),
   ``StaleJobReaper``, worker entry points, and the ``QueueConsumer`` /
   ``OperationConsumer`` distinction.

.. toctree::
   :maxdepth: 2
   :hidden:

   core
   infrastructure
   api
   workers
