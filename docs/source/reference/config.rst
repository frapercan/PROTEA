Configuration
=============

.. contents:: On this page
   :local:
   :depth: 2

The ``protea.config`` subpackage provides tuning parameters and
configuration helpers used across the PROTEA stack.

.. rubric:: Tuning parameters

``protea.config.tuning`` exposes ``TuningSettings``, a Pydantic settings
class that aggregates knobs for batch sizes, timeouts, and algorithm
parameters. Values are read from environment variables (with
``PROTEA_`` prefix) and fall back to documented defaults. Workers
instantiate a single ``TuningSettings`` object at startup; operations
receive it via dependency injection rather than importing it directly,
keeping them independently testable.

.. automodule:: protea.config.tuning
   :members:
   :undoc-members:
   :show-inheritance:

.. seealso::

   - :doc:`/appendix/configuration`: full environment-variable reference.
   - :doc:`infrastructure`: ``protea.infrastructure.settings`` for the
     database and AMQP connection strings.
