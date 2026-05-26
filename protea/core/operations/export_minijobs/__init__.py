"""Export minijob pipeline operations.

Env gate: ``PROTEA_EXPORT_MINIJOBS=1`` (default ``0``).  When the gate
is off the existing ``export_research_dataset`` monolithic path is used.
When on, the pipeline is decomposed into:

* ``export_coordinator`` (``protea.training``) - receives the cell spec,
  partitions snapshot pairs into per-pair KNN minijobs.
* ``export_knn_batch`` (``protea.training.knn-batch``) - runs KNN for
  one (protein_snapshot, go_snapshot) pair and writes a temp npz shard.
* ``export_features_batch`` (``protea.training.features``) - reads the
  temp KNN result and computes CPU features for a pair.
* ``export_write`` (``protea.training.write``) - assembles all shards
  into train/eval parquets, uploads, and registers the Dataset row.
"""

from protea.core.operations.export_minijobs._export_features_batch import (
    ExportFeaturesBatchOperation,
)
from protea.core.operations.export_minijobs._export_knn_batch import (
    ExportKnnBatchOperation,
)
from protea.core.operations.export_minijobs._export_write import (
    ExportWriteOperation,
)
from protea.core.operations.export_minijobs.export_coordinator import (
    ExportCoordinatorOperation,
)

__all__ = [
    "ExportCoordinatorOperation",
    "ExportKnnBatchOperation",
    "ExportFeaturesBatchOperation",
    "ExportWriteOperation",
]
