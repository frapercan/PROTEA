API Reference
=============

This section documents every public module in PROTEA at the symbol level.
It is generated from source docstrings via Sphinx autodoc and is always
in sync with the installed codebase.

The reference is organised into four pages:

:doc:`core`
   The domain layer: the ``Operation`` protocol and ``OperationRegistry``,
   shared HTTP utilities, KNN search backends, feature engineering functions,
   and all eight registered operations.

:doc:`infrastructure`
   The persistence and messaging layer: SQLAlchemy ORM models, session
   management, RabbitMQ publisher and consumer, and the configuration loader.

:doc:`api`
   The HTTP API: FastAPI application factory, all six routers, and a
   summary table of all 21 public endpoints.

:doc:`workers`
   The execution layer: ``BaseWorker`` (two-session job lifecycle),
   worker entry points, and the ``QueueConsumer`` / ``OperationConsumer``
   distinction.

.. toctree::
   :maxdepth: 2
   :hidden:

   core
   infrastructure
   api
   workers
