"""Helpers used to generate frozen re-ranker datasets in-process.

Survives as a container for the KNN, feature-engineering,
streaming-parquet, and reference-loading utilities consumed by
``ExportResearchDatasetOperation``. The module used to expose two
operations (single-pair and multi-split training) wired into the
OperationRegistry; LightGBM training itself moved to the standalone
protea-reranker-lab repo, so the operations are unregistered.
``TrainRerankerAutoOperation.execute()`` still runs the dump pipeline
(KNN + feature generation + parquet emission) for the export operation
in ``dump_only=True`` mode.

All execution is in-process; no RabbitMQ coordination.

T2B.6 (2026-05-16): the body of this module was extracted into the
``protea.core.training_dump`` package so each submodule stays under the
§3 file-LOC ceiling. This file is preserved as a re-export shim so
existing imports of ``protea.core.training_dump_helpers.*`` keep
working. New code should import from the canonical submodules:

  - ``protea.core.training_dump._contexts``       parameter objects
  - ``protea.core.training_dump._payload``        TrainRerankerAutoPayload
  - ``protea.core.training_dump._data_loaders``   bulk loaders
  - ``protea.core.training_dump._knn_transfer``   _knn_transfer_and_label
  - ``protea.core.training_dump._test_split``     test split orchestration
  - ``protea.core.training_dump._train_split``    per-pair train loop
  - ``protea.core.training_dump._runner``         _DumpRunner + auto op
"""

from __future__ import annotations

# Evaluation loader pulled in for the test patch target
# ``protea.core.training_dump_helpers.load_evaluation_data_for_set``.
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.training_dump._constants import _ASPECT_NAMES, _CATEGORIES
from protea.core.training_dump._contexts import (
    KnnTransferContext,
    SequenceContext,
    StreamOutput,
)
from protea.core.training_dump._data_loaders import (
    _build_reference_from_cache,
    _load_parent_map,
    _load_sequences,
    _load_taxonomy_ids,
    _preload_all_embeddings,
)
from protea.core.training_dump._knn_transfer import _knn_transfer_and_label
from protea.core.training_dump._payload import PositiveInt, TrainRerankerAutoPayload
from protea.core.training_dump._runner import TrainRerankerAutoOperation, _DumpRunner
from protea.core.training_dump._test_split import (
    _compute_test_cat_membership,
    _label_test_split_per_category,
    _load_test_sequences_and_taxonomy,
    _prepare_test_query_inputs,
    _run_test_split,
    _stream_test_predictions,
    _write_labeled_test_batches,
)
from protea.core.training_dump._train_split import (
    _emit_split_skipped,
    _knn_and_filter_to_pivot,
    _label_and_write_train_split_shards,
    _prepare_split_query_inputs,
    _resolve_train_split_eval,
    _run_train_split,
)

__all__ = (
    "KnnTransferContext",
    "PositiveInt",
    "SequenceContext",
    "StreamOutput",
    "TrainRerankerAutoOperation",
    "TrainRerankerAutoPayload",
    "_ASPECT_NAMES",
    "_CATEGORIES",
    "_DumpRunner",
    "_build_reference_from_cache",
    "_compute_test_cat_membership",
    "_emit_split_skipped",
    "_knn_and_filter_to_pivot",
    "_knn_transfer_and_label",
    "_label_and_write_train_split_shards",
    "_label_test_split_per_category",
    "_load_parent_map",
    "_load_sequences",
    "_load_taxonomy_ids",
    "_load_test_sequences_and_taxonomy",
    "_preload_all_embeddings",
    "_prepare_split_query_inputs",
    "_prepare_test_query_inputs",
    "_resolve_train_split_eval",
    "_run_test_split",
    "_run_train_split",
    "_stream_test_predictions",
    "_write_labeled_test_batches",
    "load_evaluation_data_for_set",
)
