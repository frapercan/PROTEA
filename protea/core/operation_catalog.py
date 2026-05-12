"""Single source of truth for the registered operation set.

Both ``scripts/worker.py`` and ``protea/api/app.py`` build their
``OperationRegistry`` through ``build_operation_registry()`` so that the API
and the workers stay in sync about which operations exist and what their
metadata looks like.
"""

from __future__ import annotations

from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.compute_embeddings import (
    ComputeEmbeddingsBatchOperation,
    ComputeEmbeddingsOperation,
    StoreEmbeddingsOperation,
)
from protea.core.operations.export_research_dataset import (
    ExportResearchDatasetOperation,
)
from protea.core.operations.fetch_uniprot_metadata import FetchUniProtMetadataOperation
from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetOperation
from protea.core.operations.insert_proteins import InsertProteinsOperation
from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsOperation
from protea.core.operations.load_interpro_go_mapping import (
    LoadInterProGoMappingOperation,
)
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotOperation
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsOperation
from protea.core.operations.ping import PingOperation
from protea.core.operations.predict_go_terms import (
    PredictGOTermsBatchOperation,
    PredictGOTermsOperation,
    StorePredictionsOperation,
)
from protea.core.operations.predict_go_terms_from_interpro import (
    PredictGOTermsFromInterProOperation,
)
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationOperation
from protea.core.operations.run_interproscan_batch import RunInterProScanBatchOperation


def build_operation_registry() -> OperationRegistry:
    registry = OperationRegistry()
    registry.register(PingOperation())
    registry.register(InsertProteinsOperation())
    registry.register(FetchUniProtMetadataOperation())
    registry.register(LoadOntologySnapshotOperation())
    registry.register(LoadQuickGOAnnotationsOperation())
    registry.register(LoadGOAAnnotationsOperation())
    registry.register(LoadInterProGoMappingOperation())
    registry.register(RunInterProScanBatchOperation())
    registry.register(GenerateEvaluationSetOperation())
    registry.register(RunCafaEvaluationOperation())
    registry.register(ComputeEmbeddingsOperation())
    registry.register(ComputeEmbeddingsBatchOperation())
    registry.register(StoreEmbeddingsOperation())
    registry.register(PredictGOTermsOperation())
    registry.register(PredictGOTermsBatchOperation())
    registry.register(StorePredictionsOperation())
    registry.register(PredictGOTermsFromInterProOperation())
    # TrainRerankerOperation / TrainRerankerAutoOperation are no longer
    # publicly registered: all re-ranker training moves to
    # protea-reranker-lab. They remain importable as internal helpers —
    # ExportResearchDatasetOperation still uses TrainRerankerAutoOperation
    # in-process to run the dump-only pipeline.
    registry.register(ExportResearchDatasetOperation())
    return registry
