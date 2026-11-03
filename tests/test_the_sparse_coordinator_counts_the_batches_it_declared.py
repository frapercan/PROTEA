"""``encode_residue_sparse`` declared a total that nothing ever counted toward.

The coordinator returned ``progress_total=len(operations)`` and published one
message per batch, but not ``deferred``, so ``base_worker`` marked it SUCCEEDED
the instant the fan-out left. Its children then wrote their codes directly --
no store child, no ``update_parent_progress`` anywhere in the module -- so
``progress_current`` stayed NULL for the life of the job. The surface reads
100 % for a SUCCEEDED row, which made a pass that had not yet touched the card
indistinguishable from one that had finished.

MEASURED ON ITS SIBLING, which is why this is not a hypothetical. On 2026-11-03
coordinator ``15624da2`` of ``encode_residue_vocabulary`` ran on a revision that
still lacked ``deferred`` (#1091 was already merged, but the workers held the
previous code until they were restarted 39 seconds later) and closed at
dispatch. Its 5.329 batches went on writing for hours against a row that said
SUCCEEDED, 100 %, with ``progress_current`` NULL of 5.329.

TWO TRAPS THIS PINS, both of which make the obvious one-line fix wrong:

- ``deferred`` alone would hang the job. Nothing in the module advanced the
  parent, so a deferred coordinator would stay RUNNING until the reaper failed
  it. The counting side has to arrive with it.
- A batch whose sequences every head already holds publishes nothing and runs no
  forward pass. If it does not count, the coordinator stops one short of its
  total for every duplicate message and never closes. A requeued coordinator
  produces exactly those duplicates.

WHAT IS NOT COVERED HERE. The normal path's write runs a language model on a
card, so these tests drive the coordinator's result, the already-held path and
the guard, and assert the wiring of the call rather than the contract behind it
-- that contract is pinned against Postgres in
``test_a_reaped_parent_still_counts_its_children_pg.py``.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from protea.core.operations import encode_residue_sparse as mod
from protea.core.operations.encode_residue_sparse import (
    EncodeResidueSparseBatchPayload,
    EncodeResidueSparseBatchOperation,
    _dispatch_result,
)
from protea.infrastructure.orm.models.job import JobStatus

PARENT = uuid.UUID("15624da2-3ea7-4d4e-ae05-4200dd101772")


def _emit_recorder() -> tuple[Any, list[tuple[str, dict[str, Any]]]]:
    seen: list[tuple[str, dict[str, Any]]] = []

    def emit(event: str, _message: Any = None, fields: Any = None, _level: str = "info") -> None:
        seen.append((event, dict(fields or {})))

    return emit, seen


def _heads(n: int = 2) -> list[Any]:
    return [SimpleNamespace(target_id=uuid.uuid4()) for _ in range(n)]


def _result(batches: int = 3) -> Any:
    operations = [("protea.embeddings.batch", {"i": i}) for i in range(batches)]
    heads = [("addr", {k: "v" for k in mod.REQUIRED_META}, uuid.uuid4())]
    return _dispatch_result(operations, sequence_ids=[1, 2, 3, 4], heads=heads)


def test_the_coordinator_defers_so_the_total_it_declares_is_counted() -> None:
    """The pair is the contract, so both halves are asserted together.

    A declared ``progress_total`` with no deferral is precisely the defect: the
    job announces a denominator and is closed before any numerator exists.
    """
    r = _result(batches=3)
    assert r.deferred is True
    assert r.progress_total == 3
    assert len(r.publish_operations) == 3


def test_a_batch_whose_sequences_every_head_already_holds_still_counts() -> None:
    """The batch that costs nothing must not cost the coordinator its close."""
    op = EncodeResidueSparseBatchOperation()
    emit, _seen = _emit_recorder()
    session = object()
    with patch.object(mod, "update_parent_progress") as advanced:
        out = op._close_already_encoded(session, PARENT, _heads(2), emit)  # type: ignore[arg-type]

    assert advanced.call_count == 1
    assert advanced.call_args.args[:2] == (session, PARENT)
    assert advanced.call_args.kwargs["event_name"].endswith(".parent_succeeded")
    assert out.result["encoded"] == 0


def test_a_batch_that_held_everything_still_says_what_it_skipped() -> None:
    """The saving stays legible, because a silent zero reads like a lost batch."""
    with patch.object(mod, "update_parent_progress"):
        out = EncodeResidueSparseBatchOperation()._close_already_encoded(
            object(), PARENT, _heads(3), _emit_recorder()[0]  # type: ignore[arg-type]
        )
    assert len(out.result["embedding_config_ids"]) == 3
    assert "already had a code" in out.result["note"]


def _payload(parent: str) -> dict[str, Any]:
    return {
        "source_embedding_config_id": str(uuid.uuid4()),
        "sequence_ids": [1, 2],
        "parent_job_id": parent,
    }


class _ParentOnly:
    """A session that answers only ``get(Job, ...)`` and refuses anything else.

    Refusing is the assertion: a batch stopped by its parent must not reach the
    card, so a call for the embedding config means the guard let it through.
    """

    def __init__(self, status: JobStatus) -> None:
        self._parent = SimpleNamespace(status=status, error_code=None)

    def get(self, entity: Any, _ident: Any) -> Any:
        if entity.__name__ == "Job":
            return self._parent
        raise AssertionError(f"the batch went on to load {entity.__name__}")


def test_a_cancelled_coordinator_stops_the_batch_before_the_card() -> None:
    """A person saying stop is honoured before the model is loaded, not after."""
    emit, seen = _emit_recorder()
    out = EncodeResidueSparseBatchOperation().execute(
        _ParentOnly(JobStatus.CANCELLED), _payload(str(PARENT)), emit=emit  # type: ignore[arg-type]
    )
    assert out.result == {"skipped": True}
    assert [e for e, _f in seen] == ["encode_residue_sparse_batch.skipped"]


def test_a_failed_coordinator_does_not_stop_the_batch() -> None:
    """#1095's rule holds here too: only a cancellation stops a child.

    The reaper writes FAILED on a coordinator whose children merely went quiet,
    and the write is idempotent, so a batch in flight must still run. The
    session's refusal to serve anything but the parent is what proves it did.
    """
    emit, seen = _emit_recorder()
    with pytest.raises(AssertionError, match="went on to load"):
        EncodeResidueSparseBatchOperation().execute(
            _ParentOnly(JobStatus.FAILED), _payload(str(PARENT)), emit=emit  # type: ignore[arg-type]
        )
    assert [e for e, _f in seen] == ["encode_residue_sparse_batch.parent_failed"]


def test_a_batch_message_has_to_name_its_parent() -> None:
    """The field had a default of ``""``, which is a batch that cannot count.

    Under a deferred coordinator that is not a missing label but a job that
    never closes, so the message is refused at validation instead of raising
    inside the worker on a malformed UUID.
    """
    with pytest.raises(ValidationError):
        EncodeResidueSparseBatchPayload.model_validate(
            {"source_embedding_config_id": str(uuid.uuid4()), "sequence_ids": [1]}
        )
