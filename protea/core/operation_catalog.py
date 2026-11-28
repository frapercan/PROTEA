"""Single source of truth for the registered operation set, and the spec each one declares.

Both ``scripts/worker.py`` and ``protea/api/app.py`` build their
``OperationRegistry`` through ``build_operation_registry()`` so that the API
and the workers stay in sync about which operations exist and what their
metadata looks like.

Every operation is registered together with its :class:`OperationSpec`
(ADR-D60, decision 6), one row per operation in the tables below, so neither
can be added without the other. A spec states what the code does today. Where
no code path routes an operation to a queue, its queue is the one the phase 1
plan's section C table gives it, and
``tests/test_every_operation_declares_its_spec.py`` names those operations.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from protea.core import queues
from protea.core.contracts.operation import Operation
from protea.core.contracts.registry import OperationRegistry
from protea.core.operation_spec import OperationSpec
from protea.core.operations.analyze_annotation_evolution import (
    AnalyzeAnnotationEvolutionOperation,
)
from protea.core.operations.apply_learned_encoder import ApplyLearnedEncoderOperation
from protea.core.operations.archive_ontology_snapshot import (
    ArchiveOntologySnapshotOperation,
)
from protea.core.operations.audit_evaluation_frames import (
    AuditEvaluationFramesOperation,
)
from protea.core.operations.audit_per_protein_artifacts import (
    AuditPerProteinArtifactsOperation,
)
from protea.core.operations.build_go_cooccurrence import BuildGoCooccurrenceOperation
from protea.core.operations.compare_paired_panels import (
    ComparePairedPanelsOperation,
)
from protea.core.operations.compute_embedding_space_statistics import (
    ComputeEmbeddingSpaceStatisticsOperation,
)
from protea.core.operations.compute_embeddings import (
    ComputeEmbeddingsBatchOperation,
    ComputeEmbeddingsOperation,
    StoreEmbeddingsOperation,
)
from protea.core.operations.compute_information_accretion import (
    ComputeInformationAccretionOperation,
)
from protea.core.operations.count_backend_parameters import (
    CountBackendParametersOperation,
)
from protea.core.operations.declare_corpus_population import (
    DeclareCorpusPopulationOperation,
)
from protea.core.operations.declare_exam_population import (
    DeclareExamPopulationOperation,
)
from protea.core.operations.declare_member_taxa import DeclareMemberTaxaOperation
from protea.core.operations.declare_operating_threshold import (
    DeclareOperatingThresholdOperation,
)
from protea.core.operations.declare_photo_member_set import (
    DeclarePhotoMemberSetOperation,
)
from protea.core.operations.declare_query_set_provenance import (
    DeclareQuerySetProvenanceOperation,
)
from protea.core.operations.declare_window_sample import DeclareWindowSampleOperation
from protea.core.operations.encode_residue_sparse import (
    EncodeResidueSparseBatchOperation,
    EncodeResidueSparseOperation,
)
from protea.core.operations.encode_residue_vocabulary import (
    EncodeResidueVocabularyBatchOperation,
    EncodeResidueVocabularyOperation,
)
from protea.core.operations.export_evaluation_targets import (
    ExportEvaluationTargetsOperation,
)
from protea.core.operations.export_gate_bundle import ExportGateBundleOperation
from protea.core.operations.export_minijobs import (
    ExportCoordinatorOperation,
    ExportFeaturesBatchOperation,
    ExportKnnBatchOperation,
    ExportWriteOperation,
)
from protea.core.operations.export_research_dataset import (
    ExportResearchDatasetOperation,
)
from protea.core.operations.export_transitions_dataset import (
    ExportTransitionsDatasetOperation,
)
from protea.core.operations.extract_goa_universe import ExtractGoaUniverseOperation
from protea.core.operations.fetch_uniprot_metadata import FetchUniProtMetadataOperation
from protea.core.operations.generate_evaluation_set import GenerateEvaluationSetOperation
from protea.core.operations.ingest_method_predictions import (
    IngestMethodPredictionsOperation,
)
from protea.core.operations.insert_proteins import InsertProteinsOperation
from protea.core.operations.link_protein_sequences import (
    LinkProteinSequencesOperation,
)
from protea.core.operations.load_cut_photo import LoadCutPhotoOperation
from protea.core.operations.load_embedding_bank import LoadEmbeddingBankOperation
from protea.core.operations.load_goa_annotations import LoadGOAAnnotationsOperation
from protea.core.operations.load_homology_table import LoadHomologyTableOperation
from protea.core.operations.load_interpro_go_mapping import (
    LoadInterProGoMappingOperation,
)
from protea.core.operations.load_lexicon_bank import LoadLexiconBankOperation
from protea.core.operations.load_ontology_aliases import (
    LoadOntologyAliasesOperation,
)
from protea.core.operations.load_ontology_snapshot import LoadOntologySnapshotOperation
from protea.core.operations.load_quickgo_annotations import LoadQuickGOAnnotationsOperation
from protea.core.operations.load_segment_families import LoadSegmentFamiliesOperation
from protea.core.operations.load_swissprot_release import (
    LoadSwissProtReleaseOperation,
)
from protea.core.operations.measure_embedding_magnitude import (
    MeasureEmbeddingMagnitudeOperation,
)
from protea.core.operations.ping import PingOperation
from protea.core.operations.place_window_targets import PlaceWindowTargetsOperation
from protea.core.operations.predict_go_terms import (
    PredictGOTermsBatchOperation,
    PredictGOTermsOperation,
    StorePredictionsOperation,
)
from protea.core.operations.predict_go_terms_from_interpro import (
    PredictGOTermsFromInterProOperation,
)
from protea.core.operations.read_operating_point import ReadOperatingPointOperation
from protea.core.operations.refresh_goa_release_dates import (
    RefreshGoaReleaseDatesOperation,
)
from protea.core.operations.register_ingested_method import (
    RegisterIngestedMethodOperation,
)
from protea.core.operations.resolve_protein_metadata import (
    ResolveProteinMetadataOperation,
)
from protea.core.operations.resolve_protein_sequences import (
    ResolveProteinSequencesOperation,
)
from protea.core.operations.retire_evaluation_sets import (
    RetireEvaluationSetsOperation,
)
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationOperation
from protea.core.operations.run_count_map import RunCountMapOperation
from protea.core.operations.run_interproscan_batch import RunInterProScanBatchOperation
from protea.core.operations.run_lab_method import RunLabMethodOperation
from protea.core.operations.run_own_annotations import RunOwnAnnotationsOperation
from protea.core.operations.score_prediction_set import ScorePredictionSetOperation
from protea.core.operations.seal_evaluation_frames import (
    SealEvaluationFramesOperation,
)
from protea.core.operations.seal_evaluation_groundtruth import (
    SealEvaluationGroundtruthOperation,
)
from protea.core.operations.store_residue_codes import StoreResidueCodesOperation
from protea.core.operations.stratify_evaluation import StratifyEvaluationOperation
from protea.core.operations.summarize_prediction_sets import SummarizePredictionSetsOperation
from protea.core.operations.supersede_evaluation_sets import (
    SupersedeEvaluationSetsOperation,
)
from protea.core.operations.upload_residue_shard import UploadResidueShardOperation
from protea.core.operations.verify_window_against_reference import (
    VerifyWindowAgainstReferenceOperation,
)

#: One row of the catalog: how to build the operation, and the spec it declares.
_Row = tuple[Callable[[], Operation], OperationSpec]

#: A universe loader that reads a public source over the network.
_FETCHED = OperationSpec(
    domain="universe",
    kind="job",
    queue=queues.JOBS,
    purity="database",
    resource="network",
)
#: A universe job that works from what the database and the store already hold.
_DERIVED = OperationSpec(
    domain="universe",
    kind="job",
    queue=queues.JOBS,
    purity="database",
    resource="cpu",
)
#: An evaluation job, kept off the general queue so a long one blocks nothing.
_EVALUATED = OperationSpec(
    domain="evaluation",
    kind="job",
    queue=queues.EVALUATIONS,
    purity="database",
    resource="cpu",
)

#: The universe: what one cut is built from, read by every family (ADR-D60, decision 1).
_UNIVERSE: tuple[_Row, ...] = (
    (InsertProteinsOperation, _FETCHED),
    (FetchUniProtMetadataOperation, _FETCHED),
    (LoadOntologySnapshotOperation, _FETCHED),
    (ArchiveOntologySnapshotOperation, _FETCHED),
    (LoadOntologyAliasesOperation, _DERIVED),
    (LoadQuickGOAnnotationsOperation, replace(_FETCHED, status="review")),
    (ExtractGoaUniverseOperation, _FETCHED),
    (ResolveProteinSequencesOperation, _FETCHED),
    # Today's entries, for display only: nothing on a cut reads what it writes.
    (ResolveProteinMetadataOperation, _FETCHED),
    # The photos' sequences, linked without asking UniProt (the author, 2026-11-24).
    (LinkProteinSequencesOperation, _DERIVED),
    # Its auto-frame child is a generate_evaluation_set job on protea.jobs.
    (LoadGOAAnnotationsOperation, replace(_FETCHED, publishes_to=(queues.JOBS,))),
    (LoadSwissProtReleaseOperation, _FETCHED),
    (LoadCutPhotoOperation, _DERIVED),
    (LoadHomologyTableOperation, _DERIVED),
    (DeclareMemberTaxaOperation, _DERIVED),
    (RefreshGoaReleaseDatesOperation, _FETCHED),
    (LoadInterProGoMappingOperation, replace(_FETCHED, status="mirror")),
    (RunInterProScanBatchOperation, replace(_DERIVED, status="mirror")),
    (AnalyzeAnnotationEvolutionOperation, _DERIVED),
    (DeclareCorpusPopulationOperation, _DERIVED),
    # A photo's members by admission flag (a donor pool's breadth, a head's members), and
    # a stratified sample of a window's targets: declared query sets the ladder reads.
    (DeclarePhotoMemberSetOperation, _DERIVED),
    (DeclareWindowSampleOperation, _DERIVED),
    (ComputeInformationAccretionOperation, _DERIVED),
    (GenerateEvaluationSetOperation, _DERIVED),
    (ExportEvaluationTargetsOperation, _DERIVED),
    # Marks the windows a frame left behind, deleting nothing (closure, step 3).
    (SupersedeEvaluationSetsOperation, _DERIVED),
    # The three repairs of production rows, kept for review (plan section C).
    (DeclareQuerySetProvenanceOperation, replace(_DERIVED, status="review")),
    (RetireEvaluationSetsOperation, replace(_DERIVED, status="review")),
    (SealEvaluationGroundtruthOperation, replace(_DERIVED, status="review")),
)

#: The embeddings coordinator's spec; the residue coordinators differ only in family.
_EMBEDDINGS_COORDINATOR = OperationSpec(
    domain="representation",
    kind="coordinator",
    queue=queues.EMBEDDINGS,
    purity="database",
    resource="cpu",
    publishes_to=(queues.EMBEDDINGS_BATCH,),
)
#: A batch on the card that reads the database (phase 4 makes the embeddings one pure).
_CARD_BATCH = OperationSpec(
    domain="representation",
    kind="message_stage",
    queue=queues.EMBEDDINGS_BATCH,
    purity="database",
    resource="gpu",
)
#: The write on the host that owns the database.
_EMBEDDINGS_WRITE = OperationSpec(
    domain="representation",
    kind="write",
    queue=queues.EMBEDDINGS_WRITE,
    purity="database",
    resource="cpu",
)
#: A representation job on the general queue.
_REPRESENTATION_JOB = OperationSpec(
    domain="representation",
    kind="job",
    queue=queues.JOBS,
    purity="database",
    resource="cpu",
)

#: Representations: pooled embeddings every family reads, and the world model's
#: per-residue codes (ADR-D51) with what measures and exports them.
_REPRESENTATION: tuple[_Row, ...] = (
    (ComputeEmbeddingsOperation, _EMBEDDINGS_COORDINATOR),
    (
        ComputeEmbeddingsBatchOperation,
        replace(_CARD_BATCH, publishes_to=(queues.EMBEDDINGS_WRITE,)),
    ),
    (StoreEmbeddingsOperation, _EMBEDDINGS_WRITE),
    (
        EncodeResidueVocabularyOperation,
        replace(_EMBEDDINGS_COORDINATOR, family="world-model"),
    ),
    (
        EncodeResidueVocabularyBatchOperation,
        replace(
            _CARD_BATCH,
            family="world-model",
            publishes_to=(queues.EMBEDDINGS_WRITE, queues.EMBEDDINGS_UPLOAD),
        ),
    ),
    (
        UploadResidueShardOperation,
        OperationSpec(
            domain="representation",
            kind="message_stage",
            queue=queues.EMBEDDINGS_UPLOAD,
            purity="files",
            resource="network",
            family="world-model",
            publishes_to=(queues.EMBEDDINGS_WRITE,),
        ),
    ),
    (StoreResidueCodesOperation, replace(_EMBEDDINGS_WRITE, family="world-model")),
    (
        EncodeResidueSparseOperation,
        replace(_EMBEDDINGS_COORDINATOR, family="world-model", status="review"),
    ),
    # Writes its codes from the batch itself, so it publishes nothing.
    (
        EncodeResidueSparseBatchOperation,
        replace(_CARD_BATCH, family="world-model", status="review"),
    ),
    # Runs a model on the card, so it goes where a job consumer holds one.
    (
        MeasureEmbeddingMagnitudeOperation,
        replace(_REPRESENTATION_JOB, queue=queues.METHODS, resource="gpu"),
    ),
    (ApplyLearnedEncoderOperation, replace(_REPRESENTATION_JOB, status="review")),
    (CountBackendParametersOperation, _REPRESENTATION_JOB),
    # The bank computed on the cluster (ADR-D62), loaded where the database is.
    (LoadEmbeddingBankOperation, _REPRESENTATION_JOB),
    # The label-free statistics of a bank's space at a cut, read from its shards on the
    # node that holds them; one at a time, like the other long reads of protea.training.
    (
        ComputeEmbeddingSpaceStatisticsOperation,
        replace(_REPRESENTATION_JOB, queue=queues.TRAINING),
    ),
    # The lexicon bank decoded on the cluster (ADR-D63), for the explainable-units family.
    (LoadLexiconBankOperation, replace(_REPRESENTATION_JOB, family="explainable-units")),
    # The segment families built on the cluster (ADR-D65), units of the count map.
    (LoadSegmentFamiliesOperation, replace(_REPRESENTATION_JOB, family="explainable-units")),
    (
        ExportGateBundleOperation,
        replace(_REPRESENTATION_JOB, family="world-model", status="review"),
    ),
    (DeclareExamPopulationOperation, replace(_REPRESENTATION_JOB, family="world-model")),
)

#: The nearest-neighbour predictions' coordinator, batch and write.
_KNN_COORDINATOR = OperationSpec(
    domain="method",
    kind="coordinator",
    queue=queues.PREDICTIONS,
    purity="database",
    resource="cpu",
    family="embedding-reranker",
    publishes_to=(queues.PREDICTIONS_BATCH,),
)
#: A method job on the general queue, shared by every family.
_METHOD_JOB = OperationSpec(
    domain="method",
    kind="job",
    queue=queues.JOBS,
    purity="database",
    resource="cpu",
)
#: A dataset export, one at a time on the training queue.
_EXPORT = replace(_METHOD_JOB, queue=queues.TRAINING, family="embedding-reranker")
#: A stage of the export minijobs (``PROTEA_EXPORT_MINIJOBS=1``), kept for review.
_EXPORT_STAGE = OperationSpec(
    domain="method",
    kind="message_stage",
    queue=queues.TRAINING_KNN_BATCH,
    purity="database",
    resource="cpu",
    family="embedding-reranker",
    status="review",
)

#: Methods: the nearest-neighbour arm of the embedding-reranker family with its
#: features and exports, the world model's dataset, and the doors any family's
#: method enters through (ADR-D53).
_METHOD: tuple[_Row, ...] = (
    (PredictGOTermsOperation, _KNN_COORDINATOR),
    (
        PredictGOTermsBatchOperation,
        replace(
            _KNN_COORDINATOR,
            kind="message_stage",
            queue=queues.PREDICTIONS_BATCH,
            publishes_to=(queues.PREDICTIONS_WRITE,),
        ),
    ),
    (
        StorePredictionsOperation,
        replace(_KNN_COORDINATOR, kind="write", queue=queues.PREDICTIONS_WRITE, publishes_to=()),
    ),
    (PredictGOTermsFromInterProOperation, replace(_METHOD_JOB, status="mirror")),
    # A transfer method's candidates become predictions here, with a record: the
    # evaluator scores what a method stored and nothing else (closure, step 4).
    (ScorePredictionSetOperation, _METHOD_JOB),
    (SummarizePredictionSetsOperation, _METHOD_JOB),
    # The offline half of the association feature the prediction path reads.
    (BuildGoCooccurrenceOperation, replace(_METHOD_JOB, family="embedding-reranker")),
    # TrainRerankerOperation / TrainRerankerAutoOperation are no longer publicly
    # registered: all re-ranker training moves to protea-reranker-lab. They remain
    # importable as internal helpers: ExportResearchDatasetOperation still uses
    # TrainRerankerAutoOperation in-process to run the dump-only pipeline.
    (ExportResearchDatasetOperation, _EXPORT),
    (ExportTransitionsDatasetOperation, replace(_EXPORT, family="world-model")),
    # The explainable-units family's count map, fitted at a window's start and read on its
    # targets inside the platform (ADR-D64, ADR-D65).
    (RunCountMapOperation, replace(_METHOD_JOB, queue=queues.TRAINING, family="explainable-units")),
    # democafa's goa_nonexp baseline: a target's own non-experimental annotations at t0.
    (
        RunOwnAnnotationsOperation,
        replace(_METHOD_JOB, queue=queues.TRAINING, family="explainable-units"),
    ),
    (RegisterIngestedMethodOperation, _METHOD_JOB),
    (
        RunLabMethodOperation,
        replace(
            _METHOD_JOB,
            queue=queues.METHODS,
            resource="gpu",
            publishes_to=(queues.METHODS_WRITE,),
        ),
    ),
    (IngestMethodPredictionsOperation, replace(_METHOD_JOB, queue=queues.METHODS_WRITE)),
    # Export minijob pipeline (env-gated: PROTEA_EXPORT_MINIJOBS=1). export_coordinator
    # runs on protea.training, like the monolithic export_research_dataset (only one of
    # the two is dispatched per job); the three stages run on their own sub-queues.
    (
        ExportCoordinatorOperation,
        replace(
            _EXPORT,
            kind="coordinator",
            publishes_to=(queues.TRAINING_KNN_BATCH,),
            status="review",
        ),
    ),
    (
        ExportKnnBatchOperation,
        replace(_EXPORT_STAGE, publishes_to=(queues.TRAINING_FEATURES,)),
    ),
    (
        ExportFeaturesBatchOperation,
        replace(
            _EXPORT_STAGE,
            queue=queues.TRAINING_FEATURES,
            publishes_to=(queues.TRAINING_WRITE,),
        ),
    ),
    (ExportWriteOperation, replace(_EXPORT_STAGE, kind="write", queue=queues.TRAINING_WRITE)),
)

#: Evaluation: one evaluator for every family (ADR-D60, decision 5).
_EVALUATION: tuple[_Row, ...] = (
    (RunCafaEvaluationOperation, _EVALUATED),
    (PlaceWindowTargetsOperation, _EVALUATED),
    (StratifyEvaluationOperation, _EVALUATED),
    (ComparePairedPanelsOperation, _EVALUATED),
    (DeclareOperatingThresholdOperation, _EVALUATED),
    (ReadOperatingPointOperation, _EVALUATED),
    (SealEvaluationFramesOperation, _EVALUATED),
    (AuditEvaluationFramesOperation, replace(_EVALUATED, status="review")),
    (AuditPerProteinArtifactsOperation, replace(_EVALUATED, status="review")),
    (VerifyWindowAgainstReferenceOperation, _EVALUATED),
)

#: Infrastructure: the smoke test.
_INFRASTRUCTURE: tuple[_Row, ...] = (
    (
        PingOperation,
        OperationSpec(
            domain="infrastructure",
            kind="job",
            queue=queues.PING,
            purity="database",
            resource="cpu",
        ),
    ),
)

#: Every registered operation with its spec, by domain.
CATALOG: tuple[_Row, ...] = (
    *_UNIVERSE,
    *_REPRESENTATION,
    *_METHOD,
    *_EVALUATION,
    *_INFRASTRUCTURE,
)


def build_operation_registry() -> OperationRegistry:
    """A registry holding every operation of :data:`CATALOG` with its spec."""
    registry = OperationRegistry()
    for build, spec in CATALOG:
        registry.register(build(), spec)
    return registry
