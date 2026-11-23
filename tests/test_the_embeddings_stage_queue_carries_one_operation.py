"""The embeddings stage queue carries one operation, which becomes the first file stage.

:data:`~protea.core.operation_spec.EMBEDDINGS_STAGE_QUEUE` (protea.embeddings.batch)
carries ``compute_embeddings_batch``, the first stage that leaves the database. Phase 4
makes it pure and gives it a file-stage form that the cluster runs from files placed
beforehand, with an ingest operation on the platform; the cluster never joins a queue
(ADR-D60, decision 4). The stages the cluster may run are derived from the specs
(:func:`~protea.core.operation_spec.cluster_file_stages`), never listed by hand.

Today the queue breaks the rule in three places. Each break is an exception
below that names the phase that removes it, and the test fails both when a new
break appears and when a listed one stops being a break, so the list can only
shrink. It is empty once phase 4 lands.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from protea.core import queues
from protea.core.operation_catalog import build_operation_registry
from protea.core.operation_spec import EMBEDDINGS_STAGE_QUEUE, OperationSpec, cluster_file_stages

#: The one operation the embeddings stage queue may carry.
STAGE_OPERATION = "compute_embeddings_batch"

#: Every break of the queue's rule today, with the phase that removes it.
EXCEPTIONS: dict[tuple[str, str], str] = {
    (STAGE_OPERATION, "purity database"): (
        "phase 4 (P4.4, payload v2): the batch carries its recipe in the message "
        "and stops reading its configuration from the database"
    ),
    ("encode_residue_vocabulary_batch", "on the embeddings stage queue"): (
        "phase 4 (P4.8): the residue batches move to a queue of their own"
    ),
    ("encode_residue_sparse_batch", "on the embeddings stage queue"): (
        "phase 4 (P4.8): the residue batches move to a queue of their own"
    ),
}


def _breaks(specs: dict[str, OperationSpec]) -> set[tuple[str, str]]:
    """Where the specs break the queue's rule, as (operation, what breaks)."""
    breaks = set()
    for name, spec in specs.items():
        if spec.queue != EMBEDDINGS_STAGE_QUEUE:
            continue
        if name != STAGE_OPERATION:
            breaks.add((name, "on the embeddings stage queue"))
        elif spec.purity != "pure":
            breaks.add((name, f"purity {spec.purity}"))
    return breaks


@pytest.fixture(scope="module")
def specs() -> dict[str, OperationSpec]:
    return build_operation_registry().specs()


def test_the_stage_queue_is_the_embeddings_batch_queue(specs: dict[str, OperationSpec]) -> None:
    assert EMBEDDINGS_STAGE_QUEUE == queues.EMBEDDINGS_BATCH
    assert specs[STAGE_OPERATION].queue == EMBEDDINGS_STAGE_QUEUE


def test_the_breaks_of_the_queue_rule_are_exactly_the_listed_exceptions(
    specs: dict[str, OperationSpec],
) -> None:
    assert _breaks(specs) == set(EXCEPTIONS)


def test_every_exception_names_the_phase_that_removes_it() -> None:
    for reason in EXCEPTIONS.values():
        assert reason.startswith("phase 4 (P4."), reason


def test_the_cluster_may_run_only_the_embeddings_batch_as_a_file_stage(
    specs: dict[str, OperationSpec],
) -> None:
    allowed = cluster_file_stages(specs)
    assert allowed <= {STAGE_OPERATION}
    # Nothing until phase 4 makes the batch pure.
    still_reads_the_database = (STAGE_OPERATION, "purity database") in EXCEPTIONS
    assert allowed == (frozenset() if still_reads_the_database else {STAGE_OPERATION})


def test_a_pure_newcomer_on_the_stage_queue_is_a_break(specs: dict[str, OperationSpec]) -> None:
    newcomer = replace(specs[STAGE_OPERATION], purity="pure")
    grown = {**specs, "a_pure_stage": newcomer}
    assert ("a_pure_stage", "on the embeddings stage queue") in _breaks(grown)
    assert "a_pure_stage" in cluster_file_stages(grown)


def test_a_pure_embeddings_batch_is_what_the_cluster_runs_as_a_file_stage(
    specs: dict[str, OperationSpec],
) -> None:
    pure = {**specs, STAGE_OPERATION: replace(specs[STAGE_OPERATION], purity="pure")}
    assert (STAGE_OPERATION, "purity database") not in _breaks(pure)
    assert cluster_file_stages(pure) == {STAGE_OPERATION}
