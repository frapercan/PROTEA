Services
========

.. contents:: On this page
   :local:
   :depth: 2

The ``protea.services`` package contains business-logic modules that
routers delegate to. Services are pure Python: they accept a SQLAlchemy
session and return domain objects or raise domain exceptions. Routers
map those exceptions to HTTP status codes. This separation allows the
same logic to be exercised from CLI tools or batch scripts without
importing FastAPI.

.. rubric:: Public service modules

.. automodule:: protea.services.jobs_service
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services.annotations_service
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services.embeddings_service
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

.. automodule:: protea.services.scoring_service
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

.. rubric:: Internal helper modules

The following modules are internal helpers that implement specific phases
of each service. They are documented here for completeness but are not
intended to be called directly by routers or external code.

Annotations service helpers
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: protea.services._annotations_evaluation_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._annotations_method_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._annotations_streaming_helpers
   :members:
   :undoc-members:
   :show-inheritance:

Embeddings service helpers
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: protea.services._embeddings_admin_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._embeddings_cafa_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._embeddings_predictions_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._embeddings_proteins_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._embeddings_validation_helpers
   :members:
   :undoc-members:
   :show-inheritance:

Scoring service helpers
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: protea.services._scoring_metrics_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._scoring_models
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._scoring_pipeline_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._scoring_prediction_metrics_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._scoring_streaming_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._scoring_training_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: protea.services._scoring_validation_helpers
   :members:
   :undoc-members:
   :show-inheritance:

.. seealso::

   - :doc:`api`: routers that call into these service modules.
   - :doc:`infrastructure`: ORM models and session utilities used by services.
