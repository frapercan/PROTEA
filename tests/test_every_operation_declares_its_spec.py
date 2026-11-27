"""Every registered operation declares an ``OperationSpec``, and the specs say what the code does.

ADR-D60, decision 6: routing, the consumer a worker runs, the checks of
``POST /jobs`` and the stages the cluster may run as file stages all derive from the spec, so a spec that
disagrees with the code would route work where nothing consumes it. These tests
pin the specs to today's routing, read independently of the catalog: from the
code paths that send each operation to a queue, from CLAUDE.md's queue routing
list and from the docs. Nothing is rerouted by declaring it.

Where no code path, CLAUDE.md entry or doc routes an operation (it reaches its
queue only through ``POST /jobs``, from whoever posts it), the queue is the one
the phase 1 plan's section C table gives it. Those operations are listed in
:data:`ROUTED_BY_THE_PLAN`, so the author can confirm them one by one.
"""

from __future__ import annotations

import importlib
from dataclasses import replace

import pytest

from protea.core import queues
from protea.core.contracts.registry import OperationRegistry
from protea.core.operation_catalog import CATALOG, build_operation_registry
from protea.core.operation_spec import (
    JOB_KINDS,
    KINDS,
    MESSAGE_KINDS,
    OperationSpec,
    job_queues,
    message_queues,
)
from protea.core.operations.ping import PingOperation

#: Today's routing of every registered operation, with where it was read.
TODAY: dict[str, str] = {
    # Code: the API routes and the coordinators that create or publish them.
    "load_ontology_snapshot": queues.JOBS,  # api/routers/annotations/snapshots.py
    "load_goa_annotations": queues.JOBS,  # api/routers/annotations/sets.py
    "load_quickgo_annotations": queues.JOBS,  # api/routers/annotations/sets.py
    "generate_evaluation_set": queues.JOBS,  # evaluation_sets.py; load_goa_annotations auto-frame
    "run_cafa_evaluation": queues.EVALUATIONS,  # api/routers/annotations/evaluation_sets.py
    "compute_embeddings": queues.EMBEDDINGS,  # api/routers/annotate.py
    "compute_embeddings_batch": queues.EMBEDDINGS_BATCH,  # _compute_embeddings_helpers.py
    "store_embeddings": queues.EMBEDDINGS_WRITE,  # _compute_embeddings_helpers.py
    "encode_residue_vocabulary_batch": queues.EMBEDDINGS_BATCH,  # encode_residue_vocabulary.py
    "encode_residue_sparse_batch": queues.EMBEDDINGS_BATCH,  # encode_residue_sparse.py
    "store_residue_codes": queues.EMBEDDINGS_WRITE,  # store_residue_codes.py
    "upload_residue_shard": queues.EMBEDDINGS_UPLOAD,  # _residue_shard_spool.py
    "predict_go_terms": queues.PREDICTIONS,  # api/routers/embeddings.py
    "predict_go_terms_batch": queues.PREDICTIONS_BATCH,  # predict_go_terms/_common.py
    "store_predictions": queues.PREDICTIONS_WRITE,  # predict_go_terms/_common.py
    "run_lab_method": queues.METHODS,  # run_lab_method.py
    "ingest_method_predictions": queues.METHODS_WRITE,  # run_lab_method.py
    "export_research_dataset": queues.TRAINING,  # api/routers/datasets.py
    "export_coordinator": queues.TRAINING,  # api/routers/datasets.py, minijobs on
    "export_knn_batch": queues.TRAINING_KNN_BATCH,  # export_minijobs/export_coordinator.py
    "export_features_batch": queues.TRAINING_FEATURES,  # export_minijobs/_export_knn_batch.py
    "export_write": queues.TRAINING_WRITE,  # export_minijobs/_export_features_batch.py
    # CLAUDE.md: the queue routing list and the operation entries ("Runs on ...").
    "ping": queues.PING,
    "insert_proteins": queues.JOBS,
    "fetch_uniprot_metadata": queues.JOBS,
    "extract_goa_universe": queues.JOBS,
    "resolve_protein_sequences": queues.JOBS,
    "link_protein_sequences": queues.JOBS,
    "resolve_protein_metadata": queues.JOBS,
    "load_interpro_go_mapping": queues.JOBS,
    "run_interproscan_batch": queues.JOBS,
    "predict_go_terms_from_interpro": queues.JOBS,
    "refresh_goa_release_dates": queues.JOBS,
    "compute_information_accretion": queues.JOBS,
    "archive_ontology_snapshot": queues.JOBS,
    "count_backend_parameters": queues.JOBS,
    "export_evaluation_targets": queues.JOBS,
    "declare_query_set_provenance": queues.JOBS,
    "export_transitions_dataset": queues.TRAINING,
    "load_cut_photo": queues.JOBS,
    "load_homology_table": queues.JOBS,
    # Its own pull request: it streams a cut's 2.5 GB of hits, which the evaluations
    # worker's 8G holds and the general queue's 2G does not.
    "place_window_targets": queues.EVALUATIONS,
    "load_embedding_bank": queues.JOBS,
    "declare_member_taxa": queues.JOBS,
    "load_lexicon_bank": queues.JOBS,
    "load_segment_families": queues.JOBS,
    "run_count_map": queues.TRAINING,
    "run_own_annotations": queues.TRAINING,
    "compute_embedding_space_statistics": queues.TRAINING,
    # Docs and dispatchers: the residue vocabulary's coordinator (architecture/
    # operations.rst; the weights dialog posts it), build_go_cooccurrence
    # (runbooks/lafa-native-parity.rst) and apply_learned_encoder (its pull request:
    # "Registered on protea.jobs").
    "encode_residue_vocabulary": queues.EMBEDDINGS,
    "build_go_cooccurrence": queues.JOBS,
    "apply_learned_encoder": queues.JOBS,
    # The plan's section C table: see ROUTED_BY_THE_PLAN.
    "encode_residue_sparse": queues.EMBEDDINGS,
    "stratify_evaluation": queues.EVALUATIONS,
    "compare_paired_panels": queues.EVALUATIONS,
    "seal_evaluation_frames": queues.EVALUATIONS,
    "audit_evaluation_frames": queues.EVALUATIONS,
    "audit_per_protein_artifacts": queues.EVALUATIONS,
    "verify_window_against_reference": queues.EVALUATIONS,
    "measure_embedding_magnitude": queues.METHODS,
    "export_gate_bundle": queues.JOBS,
    "declare_exam_population": queues.JOBS,
    "declare_corpus_population": queues.JOBS,
    "retire_evaluation_sets": queues.JOBS,
    "supersede_evaluation_sets": queues.JOBS,
    "score_prediction_set": queues.JOBS,
    "summarize_prediction_sets": queues.JOBS,
    "seal_evaluation_groundtruth": queues.JOBS,
    "register_ingested_method": queues.JOBS,
    "analyze_annotation_evolution": queues.JOBS,
    "load_swissprot_release": queues.JOBS,
    "load_ontology_aliases": queues.JOBS,
}

#: The operations nothing in the code, CLAUDE.md or the docs routes today, whose
#: queue is the plan's: the sparse residue coordinator on protea.embeddings beside the
#: other two coordinators whose batches go to the card (the docs route its batches, not
#: the coordinator), the evaluation jobs on protea.evaluations, the one job that runs a
#: model on protea.methods (the GPU node, with its device ``cuda``), and the rest on the
#: general queue.
ROUTED_BY_THE_PLAN: frozenset[str] = frozenset(
    {
        "encode_residue_sparse",
        "stratify_evaluation",
        "compare_paired_panels",
        "seal_evaluation_frames",
        "audit_evaluation_frames",
        "audit_per_protein_artifacts",
        "verify_window_against_reference",
        "measure_embedding_magnitude",
        "export_gate_bundle",
        "declare_exam_population",
        "declare_corpus_population",
        "retire_evaluation_sets",
        "supersede_evaluation_sets",
        "score_prediction_set",
        "summarize_prediction_sets",
        "seal_evaluation_groundtruth",
        "register_ingested_method",
        "analyze_annotation_evolution",
        "load_swissprot_release",
        "load_ontology_aliases",
    }
)

#: The plan's section C table, status column: kept until the author decides...
UNDER_REVIEW: frozenset[str] = frozenset(
    {
        "load_quickgo_annotations",
        "declare_query_set_provenance",
        "retire_evaluation_sets",
        "seal_evaluation_groundtruth",
        "encode_residue_sparse",
        # The batch of the sparse coordinator above and the write stage of the export
        # minijobs below: the table marks their pipelines, and Part 1, section 5
        # reviews encode_residue_sparse and the export minijobs as a whole.
        "encode_residue_sparse_batch",
        "export_coordinator",
        "export_knn_batch",
        "export_features_batch",
        "export_write",
        "apply_learned_encoder",
        "export_gate_bundle",
        "audit_evaluation_frames",
        "audit_per_protein_artifacts",
    }
)
#: ...and the InterPro mirror.
MIRRORS: frozenset[str] = frozenset(
    {"load_interpro_go_mapping", "run_interproscan_batch", "predict_go_terms_from_interpro"}
)

#: Where each publication happens: (operation, module, constant naming the queue).
PUBLISHED_IN_CODE: tuple[tuple[str, str, str], ...] = (
    ("compute_embeddings", "protea.core.operations._compute_embeddings_helpers", "_BATCH_QUEUE"),
    (
        "compute_embeddings_batch",
        "protea.core.operations._compute_embeddings_helpers",
        "_WRITE_QUEUE",
    ),
    (
        "encode_residue_vocabulary",
        "protea.core.operations.encode_residue_vocabulary",
        "_BATCH_QUEUE",
    ),
    (
        "encode_residue_vocabulary_batch",
        "protea.core.operations.store_residue_codes",
        "WRITE_QUEUE",
    ),
    (
        "encode_residue_vocabulary_batch",
        "protea.core.operations._residue_shard_spool",
        "UPLOAD_QUEUE",
    ),
    ("upload_residue_shard", "protea.core.operations.store_residue_codes", "WRITE_QUEUE"),
    ("encode_residue_sparse", "protea.core.operations.encode_residue_sparse", "_BATCH_QUEUE"),
    ("predict_go_terms", "protea.core.operations.predict_go_terms._common", "_BATCH_QUEUE"),
    ("predict_go_terms_batch", "protea.core.operations.predict_go_terms._common", "_WRITE_QUEUE"),
    ("run_lab_method", "protea.core.operations.run_lab_method", "INGEST_QUEUE"),
    ("load_goa_annotations", "protea.core.operations.load_goa_annotations", "_AUTO_EVAL_QUEUE"),
    (
        "export_coordinator",
        "protea.core.operations.export_minijobs.export_coordinator",
        "_KNN_BATCH_QUEUE",
    ),
    (
        "export_knn_batch",
        "protea.core.operations.export_minijobs._export_knn_batch",
        "_FEATURES_QUEUE",
    ),
    (
        "export_features_batch",
        "protea.core.operations.export_minijobs._export_features_batch",
        "_WRITE_QUEUE",
    ),
)

#: Where the code names the queue it sends each operation to: (operation, module,
#: constant), read from the API routes and the operations that create its Job row or
#: publish its message. :data:`TODAY` pins the specs once; this reads the code on every
#: run, so a route moved without its spec fails here. The two routes that write
#: ``queues.X`` inline (``compute_embeddings`` in api/routers/annotate.py, the exports
#: in api/routers/datasets.py) are pinned by :data:`TODAY` alone.
SENT_IN_CODE: tuple[tuple[str, str, str], ...] = (
    ("load_ontology_snapshot", "protea.api.routers.annotations._common", "JOBS_QUEUE"),
    ("load_goa_annotations", "protea.api.routers.annotations._common", "JOBS_QUEUE"),
    ("load_quickgo_annotations", "protea.api.routers.annotations._common", "JOBS_QUEUE"),
    ("generate_evaluation_set", "protea.api.routers.annotations._common", "JOBS_QUEUE"),
    ("generate_evaluation_set", "protea.core.operations.load_goa_annotations", "_AUTO_EVAL_QUEUE"),
    ("run_cafa_evaluation", "protea.api.routers.annotations._common", "EVALUATIONS_QUEUE"),
    ("predict_go_terms", "protea.api.routers.embeddings", "_PREDICTIONS_QUEUE"),
    (
        "compute_embeddings_batch",
        "protea.core.operations._compute_embeddings_helpers",
        "_BATCH_QUEUE",
    ),
    ("store_embeddings", "protea.core.operations._compute_embeddings_helpers", "_WRITE_QUEUE"),
    (
        "encode_residue_vocabulary_batch",
        "protea.core.operations.encode_residue_vocabulary",
        "_BATCH_QUEUE",
    ),
    (
        "encode_residue_sparse_batch",
        "protea.core.operations.encode_residue_sparse",
        "_BATCH_QUEUE",
    ),
    ("upload_residue_shard", "protea.core.operations._residue_shard_spool", "UPLOAD_QUEUE"),
    ("store_residue_codes", "protea.core.operations.store_residue_codes", "WRITE_QUEUE"),
    ("predict_go_terms_batch", "protea.core.operations.predict_go_terms._common", "_BATCH_QUEUE"),
    ("store_predictions", "protea.core.operations.predict_go_terms._common", "_WRITE_QUEUE"),
    ("run_lab_method", "protea.core.operations.run_lab_method", "NODE_QUEUE"),
    ("ingest_method_predictions", "protea.core.operations.run_lab_method", "INGEST_QUEUE"),
    (
        "export_knn_batch",
        "protea.core.operations.export_minijobs.export_coordinator",
        "_KNN_BATCH_QUEUE",
    ),
    (
        "export_features_batch",
        "protea.core.operations.export_minijobs._export_knn_batch",
        "_FEATURES_QUEUE",
    ),
    (
        "export_write",
        "protea.core.operations.export_minijobs._export_features_batch",
        "_WRITE_QUEUE",
    ),
)


@pytest.fixture(scope="module")
def specs() -> dict[str, OperationSpec]:
    return build_operation_registry().specs()


def _constant(module: str, name: str) -> str:
    value = getattr(importlib.import_module(module), name)
    assert isinstance(value, str)
    return value


# The catalog ---------------------------------------------------------------------------


def test_every_registered_operation_declares_a_spec() -> None:
    registry = build_operation_registry()
    assert set(registry.specs()) == set(registry._ops)
    assert len(CATALOG) == len(registry._ops) == 72


def test_each_spec_routes_where_the_code_routes_today(specs: dict[str, OperationSpec]) -> None:
    assert {name: spec.queue for name, spec in specs.items()} == TODAY


def test_the_plan_routes_only_what_nothing_else_routes() -> None:
    assert ROUTED_BY_THE_PLAN <= set(TODAY)
    assert not ROUTED_BY_THE_PLAN & {operation for operation, _, _ in SENT_IN_CODE}


@pytest.mark.parametrize(("operation", "module", "constant"), SENT_IN_CODE)
def test_the_code_sends_each_operation_to_the_queue_its_spec_names(
    specs: dict[str, OperationSpec], operation: str, module: str, constant: str
) -> None:
    assert _constant(module, constant) == specs[operation].queue


@pytest.mark.parametrize(("operation", "module", "constant"), PUBLISHED_IN_CODE)
def test_a_spec_names_the_queue_its_code_publishes_to(
    specs: dict[str, OperationSpec], operation: str, module: str, constant: str
) -> None:
    assert _constant(module, constant) in specs[operation].publishes_to


def test_every_declared_publication_is_one_the_code_makes(
    specs: dict[str, OperationSpec],
) -> None:
    declared = {(name, queue) for name, spec in specs.items() for queue in spec.publishes_to}
    in_code = {(op, _constant(module, name)) for op, module, name in PUBLISHED_IN_CODE}
    assert declared == in_code


def test_every_queue_published_to_has_an_operation_that_consumes_it(
    specs: dict[str, OperationSpec],
) -> None:
    served = {spec.queue for spec in specs.values()}
    published = {queue for spec in specs.values() for queue in spec.publishes_to}
    assert published <= served


def test_one_queue_carries_one_kind_of_message(specs: dict[str, OperationSpec]) -> None:
    both = message_queues(specs.values()) & job_queues(specs.values())
    assert both == frozenset()


def test_the_status_is_the_plans(specs: dict[str, OperationSpec]) -> None:
    by_status = {
        status: {name for name, spec in specs.items() if spec.status == status}
        for status in ("review", "mirror")
    }
    assert by_status == {"review": UNDER_REVIEW, "mirror": MIRRORS}


def test_the_shared_domains_name_no_family(specs: dict[str, OperationSpec]) -> None:
    named = {
        name
        for name, spec in specs.items()
        if spec.family is not None and spec.domain in {"universe", "evaluation", "infrastructure"}
    }
    assert named == set()


# The spec refuses itself ---------------------------------------------------------------

_VALID = OperationSpec("universe", "job", queues.JOBS, "database", "cpu")


def test_every_kind_travels_as_a_job_or_as_a_message() -> None:
    """The consumer a queue gets is decided by kind, so no kind may be left out."""
    assert JOB_KINDS | MESSAGE_KINDS == set(KINDS)
    assert not JOB_KINDS & MESSAGE_KINDS


def test_a_consistent_spec_builds() -> None:
    assert _VALID.carries_jobs
    assert not replace(_VALID, kind="write", domain="method").carries_jobs


@pytest.mark.parametrize(
    ("changes", "complaint"),
    [
        ({"domain": "arm"}, "domain 'arm'"),
        ({"kind": "batch"}, "kind 'batch'"),
        ({"purity": "clean"}, "purity 'clean'"),
        ({"resource": "tpu"}, "resource 'tpu'"),
        ({"status": "deleted"}, "status 'deleted'"),
        ({"domain": "method", "family": "arm-1"}, "family 'arm-1'"),
        ({"queue": "protea.picasso"}, "not a work queue"),
        ({"publishes_to": ("protea.nowhere",)}, "undeclared queues"),
        ({"publishes_to": (queues.JOBS, queues.JOBS)}, "a queue twice"),
        ({"publishes_to": [queues.JOBS]}, "must be a tuple"),
        ({"purity": "pure"}, "its purity is database"),
        ({"kind": "write", "purity": "files"}, "its purity is database"),
        ({"kind": "file_stage"}, "runs away from the database"),
        ({"kind": "coordinator"}, "publishes to at least one queue"),
        ({"family": "world-model"}, "names no family"),
        ({"domain": "evaluation", "family": "embedding-reranker"}, "names no family"),
    ],
)
def test_an_inconsistent_spec_is_refused(changes: dict[str, object], complaint: str) -> None:
    with pytest.raises(ValueError, match=complaint):
        replace(_VALID, **changes)  # type: ignore[arg-type]


def test_a_file_stage_away_from_the_database_builds() -> None:
    stage = replace(_VALID, domain="universe", kind="file_stage", purity="files")
    assert stage.kind in MESSAGE_KINDS and not stage.carries_jobs


# The registry refuses ------------------------------------------------------------------

_PING = OperationSpec("infrastructure", "job", queues.PING, "database", "cpu")


def test_an_operation_without_a_spec_is_refused() -> None:
    registry = OperationRegistry()
    with pytest.raises(TypeError):
        registry.register(PingOperation())  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="declares no OperationSpec"):
        registry.register(PingOperation(), None)  # type: ignore[arg-type]
    assert registry.specs() == {}


def test_a_queue_cannot_carry_jobs_and_messages() -> None:
    registry = OperationRegistry()
    registry.register(PingOperation(), _PING)

    class _Stage:
        name = "a_stage"
        description = ""

    stage = OperationSpec("infrastructure", "message_stage", queues.PING, "database", "cpu")
    with pytest.raises(ValueError, match="Job ids or operation messages, never both"):
        registry.register(_Stage(), stage)  # type: ignore[arg-type]
    assert set(registry.specs()) == {"ping"}


def test_the_spec_of_an_unknown_operation_is_a_key_error() -> None:
    with pytest.raises(KeyError, match="Unknown operation"):
        OperationRegistry().spec("does_not_exist")


def test_specs_is_a_copy() -> None:
    registry = OperationRegistry()
    registry.register(PingOperation(), _PING)
    registry.specs().clear()
    assert registry.spec("ping") is _PING
