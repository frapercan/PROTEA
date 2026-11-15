"""The pilot queue carries one operation, and a pilot runs only what is pure on it.

A cluster pilot consumes :data:`~protea.core.operation_spec.PILOT_QUEUE`
(protea.embeddings.batch) on a node with no database (ADR-D60, decision 4; plan
C2 and C15). The rule its mode will enforce is that the queue carries
``compute_embeddings_batch`` alone, and that it is pure. The operations a pilot
may run are derived from the specs (:func:`~protea.core.operation_spec.pilot_operations`),
never listed by hand.

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
from protea.core.operation_spec import PILOT_QUEUE, OperationSpec, pilot_operations

#: The one operation the pilot queue may carry.
PILOT_OPERATION = "compute_embeddings_batch"

#: Every break of the pilot rule today, with the phase that removes it.
EXCEPTIONS: dict[tuple[str, str], str] = {
    (PILOT_OPERATION, "purity database"): (
        "phase 4 (P4.4, payload v2): the batch carries its recipe in the message "
        "and stops reading its configuration from the database"
    ),
    ("encode_residue_vocabulary_batch", "on the pilot queue"): (
        "phase 4 (P4.8): the residue batches move to a queue of their own"
    ),
    ("encode_residue_sparse_batch", "on the pilot queue"): (
        "phase 4 (P4.8): the residue batches move to a queue of their own"
    ),
}


def _breaks(specs: dict[str, OperationSpec]) -> set[tuple[str, str]]:
    """Where the specs break the pilot rule, as (operation, what breaks)."""
    breaks = set()
    for name, spec in specs.items():
        if spec.queue != PILOT_QUEUE:
            continue
        if name != PILOT_OPERATION:
            breaks.add((name, "on the pilot queue"))
        elif spec.purity != "pure":
            breaks.add((name, f"purity {spec.purity}"))
    return breaks


@pytest.fixture(scope="module")
def specs() -> dict[str, OperationSpec]:
    return build_operation_registry().specs()


def test_the_pilot_consumes_the_embeddings_batch_queue(specs: dict[str, OperationSpec]) -> None:
    assert PILOT_QUEUE == queues.EMBEDDINGS_BATCH
    assert specs[PILOT_OPERATION].queue == PILOT_QUEUE


def test_the_breaks_of_the_pilot_rule_are_exactly_the_listed_exceptions(
    specs: dict[str, OperationSpec],
) -> None:
    assert _breaks(specs) == set(EXCEPTIONS)


def test_every_exception_names_the_phase_that_removes_it() -> None:
    for reason in EXCEPTIONS.values():
        assert reason.startswith("phase 4 (P4."), reason


def test_a_pilot_may_run_only_the_embeddings_batch(specs: dict[str, OperationSpec]) -> None:
    allowed = pilot_operations(specs)
    assert allowed <= {PILOT_OPERATION}
    # Nothing until phase 4 makes the batch pure.
    still_reads_the_database = (PILOT_OPERATION, "purity database") in EXCEPTIONS
    assert allowed == (frozenset() if still_reads_the_database else {PILOT_OPERATION})


def test_a_pure_newcomer_on_the_pilot_queue_is_a_break(specs: dict[str, OperationSpec]) -> None:
    newcomer = replace(specs[PILOT_OPERATION], purity="pure")
    grown = {**specs, "a_pure_stage": newcomer}
    assert ("a_pure_stage", "on the pilot queue") in _breaks(grown)
    assert "a_pure_stage" in pilot_operations(grown)


def test_a_pure_embeddings_batch_is_what_the_pilot_runs(specs: dict[str, OperationSpec]) -> None:
    pure = {**specs, PILOT_OPERATION: replace(specs[PILOT_OPERATION], purity="pure")}
    assert (PILOT_OPERATION, "purity database") not in _breaks(pure)
    assert pilot_operations(pure) == {PILOT_OPERATION}
