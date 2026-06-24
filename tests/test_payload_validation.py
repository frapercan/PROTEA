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
from protea.core.operations.batch_rescore_evaluation import BatchRescoreEvaluationPayload
from protea.core.operations.build_go_cooccurrence import BuildGoCooccurrencePayload
from protea.core.operations.compute_embeddings import (
    ComputeEmbeddingsBatchPayload,
    ComputeEmbeddingsPayload,
    StoreEmbeddingsPayload,
)
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
