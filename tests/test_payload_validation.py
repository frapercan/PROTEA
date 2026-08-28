"""Boundary payload validation tests (T1.8 of master plan v3).

Every operation registered in :class:`OperationRegistry` is expected to
call ``XxxPayload.model_validate(payload)`` as the first statement of
its ``execute`` method, so malformed dispatch payloads fail loudly at
the operation entry point rather than corrupting downstream state.

This module covers the runtime side of that guarantee with one
negative case per registered payload class. The static guarantee that
every payload class derives from ``protea_contracts.ProteaPayload`` is
covered by ``tests/test_contracts_invariants.py`` (T1.7, PR #371).

Approach: each test is unit shaped. We construct a deliberately bad
``dict`` payload, call ``Payload.model_validate(bad)`` directly, and
assert that ``pydantic.ValidationError`` is raised with the expected
field flagged. This is fast (no DB, no HTTP), and it mirrors what
runs inside ``execute`` at line 1 of the operation body, so it pins
the boundary contract without depending on the dispatch layer (where
``ValidationError`` is wrapped into a FAILED job and the underlying
error type is lost).

The ``ping`` operation is exempt: it has no payload schema by design.

Negative-case matrix
--------------------

For each operation we exercise one of three failure modes (whichever
is most representative for the payload):

* missing-required: drop a required field, assert the field appears
  in the error locations.
* wrong-type: send a string where an int is expected (strict mode is
  inherited from ``ProteaPayload``), assert the field appears.
* invariant-violation: send a payload that satisfies field types but
  breaks a ``model_validator``, assert the error message matches.
"""

from __future__ import annotations

from typing import Any

import pytest
from protea_contracts import (
    PredictGOTermsBatchPayload,
    PredictGOTermsPayload,
    StorePredictionsPayload,
)
from pydantic import BaseModel, ValidationError

from protea.core.operation_catalog import build_operation_registry
from protea.core.operations.apply_learned_encoder import ApplyLearnedEncoderPayload
from protea.core.operations.archive_ontology_snapshot import (
    ArchiveOntologySnapshotPayload,
)
from protea.core.operations.audit_evaluation_frames import (
    AuditEvaluationFramesPayload,
)
from protea.core.operations.audit_per_protein_artifacts import (
    AuditPerProteinArtifactsPayload,
)
from protea.core.operations.batch_rescore_evaluation import BatchRescoreEvaluationPayload
from protea.core.operations.build_go_cooccurrence import BuildGoCooccurrencePayload
from protea.core.operations.compare_paired_panels import ComparePairedPanelsPayload
from protea.core.operations.compute_embeddings import (
    ComputeEmbeddingsBatchPayload,
    ComputeEmbeddingsPayload,
    StoreEmbeddingsPayload,
)
from protea.core.operations.compute_information_accretion import (
    ComputeInformationAccretionPayload,
)
from protea.core.operations.count_backend_parameters import (
    CountBackendParametersPayload,
)
from protea.core.operations.encode_residue_sparse import (
    EncodeResidueSparseBatchPayload,
    EncodeResidueSparsePayload,
)
from protea.core.operations.export_evaluation_targets import (
    ExportEvaluationTargetsPayload,
)
from protea.core.operations.export_gate_bundle import ExportGateBundlePayload
from protea.core.operations.export_minijobs._export_features_batch import (
    ExportFeaturesBatchPayload,
)
from protea.core.operations.export_minijobs._export_knn_batch import (
    ExportKnnBatchPayload,
)
from protea.core.operations.export_minijobs._export_write import (
    ExportWritePayload,
)
from protea.core.operations.export_minijobs.export_coordinator import (
    ExportCoordinatorPayload,
)
from protea.core.operations.export_research_dataset import (
    ExportResearchDatasetPayload,
)
from protea.core.operations.fetch_uniprot_metadata import (
    FetchUniProtMetadataPayload,
)
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetPayload,
)
from protea.core.operations.insert_proteins import InsertProteinsPayload
from protea.core.operations.load_goa_annotations import (
    LoadGOAAnnotationsPayload,
)
from protea.core.operations.load_interpro_go_mapping import (
    LoadInterProGoMappingPayload,
)
from protea.core.operations.load_ontology_snapshot import (
    LoadOntologySnapshotPayload,
)
from protea.core.operations.load_quickgo_annotations import (
    LoadQuickGOAnnotationsPayload,
)
from protea.core.operations.measure_embedding_magnitude import (
    MeasureEmbeddingMagnitudePayload,
)
from protea.core.operations.predict_go_terms_from_interpro import (
    PredictGOTermsFromInterProPayload,
)
from protea.core.operations.refresh_goa_release_dates import (
    RefreshGoaReleaseDatesPayload,
)
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload
from protea.core.operations.run_interproscan_batch import (
    RunInterProScanBatchPayload,
)
from protea.core.operations.seal_evaluation_frames import SealEvaluationFramesPayload
from protea.core.operations.stratify_evaluation import StratifyEvaluationPayload

# (op_name, payload_class, bad_payload, expected_error_field)
#
# ``op_name`` matches the name registered in OperationRegistry so the
# CI dashboard can correlate test failures with dispatch failures.
# ``expected_error_field`` is the JSON-path-like locator pydantic puts
# in ``error['loc']`` (a tuple whose entries may be field names or list
# indices). For invariant failures we use the empty tuple, which is
# pydantic's locator for model level validators.
PayloadNegativeCase = tuple[str, type[BaseModel], dict[str, Any], tuple[str | int, ...]]
PAYLOAD_NEGATIVE_CASES: list[PayloadNegativeCase] = [
    # invariant: a weighted estimator name over unweighted components is the
    # defect this operation exists to prevent, so "auto" is not a value. There
    # is no fallback from ia_weighted; an artefact with no weighted components
    # is a refusal, not an unweighted number under a weighted name.
    (
        "compare_paired_panels",
        ComparePairedPanelsPayload,
        {
            "evaluation_result_id": "a",
            "baseline_evaluation_result_id": "b",
            "weighting": "auto",
        },
        ("weighting",),
    ),
    # invariant: a population floor of zero would report every cell, including
    # the ones holding a single protein, at the same weight as one holding
    # thousands. The floor is the whole point of withholding.
    (
        "stratify_evaluation",
        StratifyEvaluationPayload,
        {"prediction_set_id": "pset", "artifacts_root": "/tmp/x", "min_population": 0},
        ("min_population",),
    ),
    # invariant: a reference pool of zero would publish a bundle with no donors,
    # which a consumer cannot distinguish from an empty store
    (
        "export_gate_bundle",
        ExportGateBundlePayload,
        {"embedding_config_id": "cfg", "annotation_set_id": "ann", "queries": ["P1"], "ref_n": 0},
        ("ref_n",),
    ),
    # invariant: a sample of zero per band would measure nothing and then
    # recommend scale 1.0, which is the dangerous answer arrived at by accident
    (
        "measure_embedding_magnitude",
        MeasureEmbeddingMagnitudePayload,
        {"embedding_config_id": "cfg", "per_band": 0},
        ("per_band",),
    ),
    # missing-required: embedding_config_id
    (
        "compute_embeddings",
        ComputeEmbeddingsPayload,
        {"accessions": ["P12345"]},
        ("embedding_config_id",),
    ),
    # wrong-type: sequence_ids must be list[int], not list[str]
    (
        "compute_embeddings_batch",
        ComputeEmbeddingsBatchPayload,
        {
            "embedding_config_id": "cfg",
            "sequence_ids": ["not", "ints"],
            "parent_job_id": "job",
        },
        ("sequence_ids", 0),
    ),
    # missing-required: sequences
    (
        "store_embeddings",
        StoreEmbeddingsPayload,
        {"parent_job_id": "job", "embedding_config_id": "cfg"},
        ("sequences",),
    ),
    # missing-required: output_name (one of many required fields)
    (
        "export_research_dataset",
        ExportResearchDatasetPayload,
        {
            "embedding_config_id": "cfg",
            "ontology_snapshot_id": "ont",
            "train_versions": [1],
            "test_versions": [2],
        },
        ("output_name",),
    ),
    # missing-required: search_criteria
    (
        "fetch_uniprot_metadata",
        FetchUniProtMetadataPayload,
        {"page_size": 100},
        ("search_criteria",),
    ),
    # missing-required: old_annotation_set_id and new_annotation_set_id
    (
        "generate_evaluation_set",
        GenerateEvaluationSetPayload,
        {},
        ("old_annotation_set_id",),
    ),
    # missing-required: search_criteria
    (
        "insert_proteins",
        InsertProteinsPayload,
        {"page_size": 50},
        ("search_criteria",),
    ),
    # missing-required: source_version
    (
        "load_goa_annotations",
        LoadGOAAnnotationsPayload,
        {
            "ontology_snapshot_id": "ont",
            "gaf_url": "http://example.org/goa.gaf",
        },
        ("source_version",),
    ),
    # missing-required: source_version
    (
        "load_interpro_go_mapping",
        LoadInterProGoMappingPayload,
        {"mapping_url": "http://example.org/ipr2go"},
        ("source_version",),
    ),
    # missing-required: obo_url
    (
        "load_ontology_snapshot",
        LoadOntologySnapshotPayload,
        {"force_relationships": True},
        ("obo_url",),
    ),
    # missing-required: source_version
    (
        "load_quickgo_annotations",
        LoadQuickGOAnnotationsPayload,
        {"ontology_snapshot_id": "ont"},
        ("source_version",),
    ),
    # missing-required: ontology_snapshot_id
    (
        "predict_go_terms",
        PredictGOTermsPayload,
        {"embedding_config_id": "cfg", "annotation_set_id": "ann"},
        ("ontology_snapshot_id",),
    ),
    # missing-required: query_accessions
    (
        "predict_go_terms_batch",
        PredictGOTermsBatchPayload,
        {
            "embedding_config_id": "cfg",
            "annotation_set_id": "ann",
            "ontology_snapshot_id": "ont",
            "prediction_set_id": "pred",
            "parent_job_id": "job",
        },
        ("query_accessions",),
    ),
    # wrong-type: predictions must be a list, not a string
    (
        "store_predictions",
        StorePredictionsPayload,
        {
            "parent_job_id": "job",
            "prediction_set_id": "pred",
            "predictions": "not-a-list",
        },
        ("predictions",),
    ),
    # missing-required: source_version
    (
        "predict_go_terms_from_interpro",
        PredictGOTermsFromInterProPayload,
        {
            "embedding_config_id": "cfg",
            "annotation_set_id": "ann",
            "ontology_snapshot_id": "ont",
        },
        ("source_version",),
    ),
    # missing-required: prediction_set_id
    (
        "run_cafa_evaluation",
        RunCafaEvaluationPayload,
        {"evaluation_set_id": "eval"},
        ("prediction_set_id",),
    ),
    # missing-required: scoring_config_ids
    (
        "batch_rescore_evaluation",
        BatchRescoreEvaluationPayload,
        {"evaluation_set_id": "eval", "prediction_set_id": "pred"},
        ("scoring_config_ids",),
    ),
    # invariant-violation: must provide exactly one of
    # query_set_id / accessions (model_validator after-mode)
    (
        "run_interproscan_batch",
        RunInterProScanBatchPayload,
        {},
        (),
    ),
    # wrong-type: timeout_seconds must be a positive int
    (
        "refresh_goa_release_dates",
        RefreshGoaReleaseDatesPayload,
        {"timeout_seconds": 0},
        ("timeout_seconds",),
    ),
    # missing-required: annotation_set_id
    (
        "build_go_cooccurrence",
        BuildGoCooccurrencePayload,
        {"known_freq_cap": 1000},
        ("annotation_set_id",),
    ),
    # missing-required: output_name
    (
        "export_coordinator",
        ExportCoordinatorPayload,
        {
            "embedding_config_id": "cfg",
            "annotation_set_id": "ann",
            "ontology_snapshot_id": "ont",
            "train_versions": [220, 221],
            "test_versions": [222],
        },
        ("output_name",),
    ),
    # missing-required: pair_id
    (
        "export_knn_batch",
        ExportKnnBatchPayload,
        {
            "coordinator_job_id": "coord",
            "train_snapshot_id": 220,
            "test_snapshot_id": 220,
            "embedding_config_id": "cfg",
            "annotation_set_id": "ann",
            "ontology_snapshot_id": "ont",
        },
        ("pair_id",),
    ),
    # missing-required: coordinator_job_id
    (
        "export_features_batch",
        ExportFeaturesBatchPayload,
        {
            "pair_id": "train-220",
            "temp_knn_uri": None,
            "embedding_config_id": "cfg",
            "annotation_set_id": "ann",
            "ontology_snapshot_id": "ont",
        },
        ("coordinator_job_id",),
    ),
    # missing-required: pair_id (per-pair assembly contract since F-EXPORT-MINIJOB.4)
    (
        "export_write",
        ExportWritePayload,
        {
            "coordinator_job_id": "coord",
            "temp_features_uri": "s3://bucket/temp/x.parquet",
            "embedding_config_id": "cfg",
            "ontology_snapshot_id": "ont",
            "annotation_set_id": "ann",
        },
        ("pair_id",),
    ),
    # missing-required: source_embedding_config_id
    (
        "apply_learned_encoder",
        ApplyLearnedEncoderPayload,
        {"encoder_artifact_path": "/tmp/enc.pt"},
        ("source_embedding_config_id",),
    ),
    # bad-vocabulary: evidence_regime must name a known regime. Falling back to
    # a default here would silently widen the IA corpus (ADR-D46).
    (
        "compute_information_accretion",
        ComputeInformationAccretionPayload,
        {
            "ontology_snapshot_id": "snap",
            "annotation_set_id": "ann",
            "evidence_regime": "everything",
        },
        ("evidence_regime",),
    ),
    # missing-required: ontology_snapshot_id
    (
        "archive_ontology_snapshot",
        ArchiveOntologySnapshotPayload,
        {"force": True},
        ("ontology_snapshot_id",),
    ),
    # invariant: the combination cap must be positive. The census takes no
    # required inputs, so this is the only thing it can refuse, and it is a
    # real knob: the combination list is written into a JobEvent as JSONB.
    (
        "audit_evaluation_frames",
        AuditEvaluationFramesPayload,
        {"max_combinations": 0},
        ("max_combinations",),
    ),
    # invariant: the census probes the object store once per (result, setting),
    # so an unbounded run is unbounded I/O against the store and an unbounded
    # detail list written into a JobEvent as JSONB. A cap of zero would probe
    # nothing and report a complete-looking census of an empty corpus, which is
    # the one way a calibration helper can do harm.
    (
        "audit_per_protein_artifacts",
        AuditPerProteinArtifactsPayload,
        {"max_results": 0},
        ("max_results",),
    ),
    # invariant: a report of zero examples is a report of nothing. The seal is
    # asked twice on purpose, and the first answer is the list of what the second
    # would write, so a caller that asks for none has disabled the safeguard
    # rather than tuned it.
    (
        "seal_evaluation_frames",
        SealEvaluationFramesPayload,
        {"max_examples": 0},
        ("max_examples",),
    ),
    # invariant: an empty selection reads as the opposite of itself. Omitting the
    # field means every configuration, and an empty list looks like it means
    # none, but an empty list is falsy, so a selection that narrowed to nothing
    # upstream would silently widen to everything and load every checkpoint in
    # the registry.
    (
        "count_backend_parameters",
        CountBackendParametersPayload,
        {"embedding_config_ids": []},
        ("embedding_config_ids",),
    ),
    # invariant: ``removed`` holds proteins that LOST annotation over the window.
    # The evaluation reports them and never scores them, so accepting them as
    # targets would be a plausible request that silently changes the population
    # being measured.
    (
        "export_evaluation_targets",
        ExportEvaluationTargetsPayload,
        {"evaluation_set_id": "e", "categories": ["removed"]},
        ("categories",),
    ),
    (
        # An unnamed encoder would silently pick nothing to project through.
        # The error is model-level rather than field-level because the rule is
        # between two fields: a blank path with no URI is no address at all, and
        # naming one of the two would suggest the other was not an option.
        "encode_residue_sparse",
        EncodeResidueSparsePayload,
        {"source_embedding_config_id": "cfg", "encoder_artifact_path": "   "},
        (),
    ),
    (
        # A batch with no sequences is a message that reports success having done
        # nothing, and the parent's progress would advance for it.
        "encode_residue_sparse_batch",
        EncodeResidueSparseBatchPayload,
        {
            "source_embedding_config_id": "src",
            "target_embedding_config_id": "tgt",
            "sequence_ids": [1],
            "parent_job_id": "job",
            "encoder_artifact_path": "e.npz",
            "batch_size": 0,
        },
        ("batch_size",),
    ),
    (
        # Two addresses can disagree and nothing downstream could say which was meant.
        "encode_residue_sparse",
        EncodeResidueSparsePayload,
        {
            "source_embedding_config_id": "cfg",
            "encoder_artifact_path": "e.npz",
            "encoder_artifact_uri": "encoders/e.npz",
        },
        (),
    ),
    (
        # A batch of zero is an infinite loop that reports success.
        "encode_residue_sparse",
        EncodeResidueSparsePayload,
        {
            "source_embedding_config_id": "cfg",
            "encoder_artifact_path": "e.npz",
            "batch_size": 0,
        },
        ("batch_size",),
    ),
]


PAYLOAD_IDS = [case[0] for case in PAYLOAD_NEGATIVE_CASES]


@pytest.mark.parametrize(
    ("op_name", "payload_cls", "bad_payload", "expected_loc"),
    PAYLOAD_NEGATIVE_CASES,
    ids=PAYLOAD_IDS,
)
def test_payload_validation_rejects_malformed(
    op_name: str,
    payload_cls: type[BaseModel],
    bad_payload: dict[str, Any],
    expected_loc: tuple[str | int, ...],
) -> None:
    """Each registered operation's payload rejects a malformed dispatch dict.

    The assertion is intentionally narrow: we only check that
    ``ValidationError`` is raised and (for field-level failures) that
    the expected field is among the reported error locations. We do not
    pin error messages, since pydantic phrasing can vary across minor
    versions.
    """

    with pytest.raises(ValidationError) as exc_info:
        payload_cls.model_validate(bad_payload)

    if expected_loc:
        all_locs = [tuple(err["loc"]) for err in exc_info.value.errors()]
        assert expected_loc in all_locs, (
            f"expected field {expected_loc} in error locs for {op_name}, got {all_locs}"
        )


def test_negative_matrix_covers_every_registered_operation() -> None:
    """The negative-case matrix has at least one entry per registered op
    that has a payload schema.

    The ``ping`` operation is exempt because it has no payload schema
    by design. Any other dispatch name registered in
    :func:`build_operation_registry` that is missing from
    ``PAYLOAD_NEGATIVE_CASES`` should fail this test, so the matrix
    cannot silently drift out of sync with the registry.
    """

    registry = build_operation_registry()
    covered = {case[0] for case in PAYLOAD_NEGATIVE_CASES}
    registered = set(registry._ops.keys())
    exempt = {"ping"}

    missing = (registered - exempt) - covered
    assert not missing, (
        f"operations registered in OperationRegistry but missing from the "
        f"negative-case matrix: {sorted(missing)}"
    )

    spurious = covered - registered
    assert not spurious, (
        f"negative-case matrix entries that do not map to a registered "
        f"operation name: {sorted(spurious)}"
    )


def test_ping_operation_has_no_payload_schema() -> None:
    """``ping`` is the only operation without a payload class.

    Documenting the exemption here makes the gap explicit, so the next
    time a payload-less operation is added the developer has a clear
    template for either wiring a schema or extending the exempt set.
    """

    # PingOperation.execute does not pin a payload type beyond ``dict``;
    # the public surface is the absence of any ``XxxPayload`` import in
    # the module. Mirrored from the audit done for T1.8.
    import protea.core.operations.ping as ping_module
    from protea.core.operations.ping import PingOperation

    payload_attrs = [name for name in dir(ping_module) if name.endswith("Payload")]
    assert payload_attrs == [], (
        f"ping module unexpectedly declares payload classes: {payload_attrs}. "
        "If a schema was added, drop ping from the exempt set in the matrix "
        "test above and add a negative case for it."
    )
    assert PingOperation.name == "ping"
