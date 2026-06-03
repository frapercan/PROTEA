"""Regression tests for F-OPS-CHILD-FAILED-EMIT (incident 2026-05-27).

EXP.13 torch smoke ``1c3b5bc5`` ran KNN cleanly on GPU but every
``export_features_batch`` dispatch crashed with an ``S3 NoSuchKey``
(proximal bug fixed in PR #570 ``_uri_to_key`` dropping the bucket
prefix). ``OperationConsumer._handle_general_failure`` caught the
exception, rolled the session back, and logged at ERROR but emitted
NOTHING back to the coordinator's ``job_event`` row. As a result
``coord-fail-propagate`` (PR #563) was blind to the storm and the
reaper had to kill the coordinator at the 6h timeout.

These tests pin the fix:

* every non-OOM child failure carries a structured ``child.failed``
  ``JobEvent`` against the parent with ``operation``, ``error_class``,
  truncated ``error_message`` and, when present, ``pair_id``;
* the rolled-back outer session is not reused — the emit opens its
  own fresh session through ``self._factory()``;
* the delivery is still ``basic_nack``-ed (not acked) and obeys the
  ``requeue_on_failure`` flag.

The coord-fail-propagate integration (parent transitions to FAILED
after the failure-ratio threshold is met) is exercised end-to-end in
``tests/test_export_coordinator_failure_propagation.py`` plus the
aggregator's own ``maybe_aggregate_parent_failure`` already called
from ``_handle_general_failure``; the TODO below documents the
remaining wiring check.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from protea.infrastructure.queue.consumer import ConsumerOptions, OperationConsumer

# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------


def _make_method(delivery_tag: int = 1) -> MagicMock:
    method = MagicMock()
    method.delivery_tag = delivery_tag
    return method


def _body(
    *,
    operation: str,
    parent_job_id: UUID | None,
    payload: dict[str, Any] | None = None,
) -> bytes:
    msg: dict[str, Any] = {"operation": operation, "payload": payload or {}}
    if parent_job_id is not None:
        msg["job_id"] = str(parent_job_id)
    return json.dumps(msg).encode()


def _make_consumer(
    *,
    raises: BaseException,
    requeue_on_failure: bool = False,
) -> tuple[OperationConsumer, list[MagicMock]]:
    """Build an OperationConsumer whose operation always raises ``raises``."""
    op = MagicMock()
    op.execute.side_effect = raises
    registry = MagicMock()
    registry.get.return_value = op

    sessions: list[MagicMock] = []

    def make_session() -> MagicMock:
        s = MagicMock()
        sessions.append(s)
        # Make the parent-lookup path inside
        # ``maybe_aggregate_parent_failure`` return ``None`` so the
        # aggregator short-circuits without trying to update a Job row.
        s.get.return_value = None
        return s

    factory = MagicMock(side_effect=make_session)

    consumer = OperationConsumer(
        amqp_url="amqp://localhost/",
        queue_name="test.ops",
        registry=registry,
        session_factory=factory,
        options=ConsumerOptions(requeue_on_failure=requeue_on_failure),
    )
    return consumer, sessions


def _collect_child_failed_events(sessions: list[MagicMock]) -> list[Any]:
    """Return every ``child.failed`` JobEvent passed to any session's ``add``."""
    events: list[Any] = []
    for session in sessions:
        for call in session.add.call_args_list:
            args, _ = call
            if not args:
                continue
            event = args[0]
            if getattr(event, "event", None) == "child.failed":
                events.append(event)
    return events


# ---------------------------------------------------------------------------
# Behaviour under test
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "operation_name",
    [
        "export_knn_batch",
        "export_features_batch",
        "export_write",
    ],
)
def test_child_failure_emits_structured_event_on_parent(operation_name: str) -> None:
    """Every non-OOM failure must write a ``child.failed`` JobEvent."""
    parent_job_id = uuid4()
    consumer, sessions = _make_consumer(raises=RuntimeError("boom"))

    channel = MagicMock()
    method = _make_method(7)
    props = MagicMock()
    props.headers = None

    body = _body(
        operation=operation_name,
        parent_job_id=parent_job_id,
        payload={"pair_id": "train-220", "k": 5},
    )

    consumer._on_message(channel, method, props, body)

    events = _collect_child_failed_events(sessions)
    assert len(events) == 1, f"expected exactly one child.failed JobEvent, got {len(events)}"
    event = events[0]
    assert event.job_id == parent_job_id
    assert event.level == "error"
    assert event.message == "boom"

    fields = event.fields
    assert fields["operation"] == operation_name
    assert fields["error_class"] == "RuntimeError"
    assert fields["error_message"] == "boom"
    assert fields["pair_id"] == "train-220"
    # ``error_code`` alias is kept for back-compat with dashboards that
    # already filter on it.
    assert fields["error_code"] == "RuntimeError"


def test_child_failure_omits_pair_id_when_absent_in_payload() -> None:
    """Operations without a ``pair_id`` payload (e.g. ``store_*``) must not
    synthesise one; the field is simply absent from the event."""
    parent_job_id = uuid4()
    consumer, sessions = _make_consumer(raises=RuntimeError("boom"))

    channel = MagicMock()
    method = _make_method(8)
    props = MagicMock()
    props.headers = None

    body = _body(
        operation="store_embeddings",
        parent_job_id=parent_job_id,
        payload={"sequence_ids": []},
    )

    consumer._on_message(channel, method, props, body)

    events = _collect_child_failed_events(sessions)
    assert len(events) == 1
    fields = events[0].fields
    assert "pair_id" not in fields
    assert fields["operation"] == "store_embeddings"
    assert fields["error_class"] == "RuntimeError"


def test_child_failure_truncates_error_message_to_500_chars() -> None:
    """Long exception messages are clipped so a flood of failures cannot
    explode the JSONB event row size."""
    parent_job_id = uuid4()
    long_message = "x" * 5000
    consumer, sessions = _make_consumer(raises=RuntimeError(long_message))

    channel = MagicMock()
    method = _make_method(9)
    props = MagicMock()
    props.headers = None

    body = _body(
        operation="export_features_batch",
        parent_job_id=parent_job_id,
        payload={"pair_id": "eval-226"},
    )

    consumer._on_message(channel, method, props, body)

    events = _collect_child_failed_events(sessions)
    assert len(events) == 1
    fields = events[0].fields
    assert fields["error_message"] == "x" * 500
    assert len(fields["error_message"]) == 500


def test_child_failure_nacks_delivery_not_acks() -> None:
    """The delivery must be nack-ed (default: no requeue). Acking would
    pretend the work succeeded and starve coord-fail-propagate."""
    parent_job_id = uuid4()
    consumer, _sessions = _make_consumer(raises=RuntimeError("boom"))

    channel = MagicMock()
    method = _make_method(42)
    props = MagicMock()
    props.headers = None

    body = _body(
        operation="export_features_batch",
        parent_job_id=parent_job_id,
        payload={"pair_id": "train-225"},
    )

    consumer._on_message(channel, method, props, body)

    channel.basic_nack.assert_called_once_with(delivery_tag=42, requeue=False)
    channel.basic_ack.assert_not_called()


def test_child_failure_with_requeue_flag_passes_requeue_true() -> None:
    """When the consumer is constructed with ``requeue_on_failure=True``
    the nack must reflect that so the broker redelivers."""
    parent_job_id = uuid4()
    consumer, _sessions = _make_consumer(raises=RuntimeError("boom"), requeue_on_failure=True)

    channel = MagicMock()
    method = _make_method(43)
    props = MagicMock()
    props.headers = None

    body = _body(
        operation="export_knn_batch",
        parent_job_id=parent_job_id,
        payload={"pair_id": "train-220"},
    )

    consumer._on_message(channel, method, props, body)

    channel.basic_nack.assert_called_once_with(delivery_tag=43, requeue=True)


def test_child_failure_uses_fresh_session_after_rollback() -> None:
    """The execution session is rolled back before the emit; the emit
    must therefore acquire a NEW session from ``_factory``."""
    parent_job_id = uuid4()
    consumer, sessions = _make_consumer(raises=RuntimeError("boom"))

    channel = MagicMock()
    method = _make_method(11)
    props = MagicMock()
    props.headers = None

    body = _body(
        operation="export_features_batch",
        parent_job_id=parent_job_id,
        payload={"pair_id": "train-220"},
    )

    consumer._on_message(channel, method, props, body)

    # Sessions opened (in order): 1) parent-cancellation lookup,
    # 2) op.execute (rolled back), 3) emit (child.failed), 4) aggregator
    # parent lookup (returns None, no write). At minimum the emit must
    # have opened its own session.
    assert len(sessions) >= 3

    # The session that ran ``op.execute`` is the one whose ``.rollback``
    # was called by the outer except block.
    rolled_back = [s for s in sessions if s.rollback.called]
    assert rolled_back, "expected the execution session to be rolled back"

    # That same rolled-back session must NOT also have committed a
    # JobEvent; the emit happens on a separate, fresh session.
    for session in rolled_back:
        for call in session.add.call_args_list:
            event = call.args[0] if call.args else None
            assert getattr(event, "event", None) != "child.failed", (
                "child.failed must not be written on the rolled-back session"
            )


def test_child_failure_without_parent_job_id_skips_emit() -> None:
    """No parent → no parent_job_id → emit is a best-effort no-op."""
    consumer, sessions = _make_consumer(raises=RuntimeError("boom"))

    channel = MagicMock()
    method = _make_method(12)
    props = MagicMock()
    props.headers = None

    # Operation message without a ``job_id`` field.
    body = _body(operation="export_features_batch", parent_job_id=None)

    consumer._on_message(channel, method, props, body)

    # The execution still nacks…
    channel.basic_nack.assert_called_once_with(delivery_tag=12, requeue=False)
    # …but nobody wrote a child.failed event because there is no parent.
    assert _collect_child_failed_events(sessions) == []


def test_cuda_oom_path_is_not_disturbed_by_general_failure_emit() -> None:
    """Regression guard: the CUDA OOM branch must keep working and use
    its own ``child.cuda_oom_retry`` event, not ``child.failed``."""
    parent_job_id = uuid4()
    exc = RuntimeError("CUDA out of memory. Tried to allocate 2 GiB")
    consumer, sessions = _make_consumer(raises=exc)

    channel = MagicMock()
    method = _make_method(60)
    props = MagicMock()
    props.headers = None

    import sys
    from unittest.mock import patch

    body = _body(
        operation="export_knn_batch",
        parent_job_id=parent_job_id,
        payload={"pair_id": "train-220"},
    )

    mock_torch = MagicMock()
    with patch.dict(sys.modules, {"torch": mock_torch}):
        consumer._on_message(channel, method, props, body)

    # OOM branch must NOT emit a ``child.failed`` (that belongs to the
    # general-failure path); it emits ``child.cuda_oom_retry`` instead.
    assert _collect_child_failed_events(sessions) == []
    # And the OOM handler republishes + acks, never nacks the original.
    channel.basic_ack.assert_called_once_with(delivery_tag=60)
    channel.basic_nack.assert_not_called()


# TODO(F-OPS-CHILD-FAILED-EMIT): exercise the full integration with
# PR #563 (``coord-fail-propagate``): N failing children whose
# ``child.failed`` events trigger ``report_child_failure`` and
# transition the coordinator parent to ``FAILED`` once the failure
# ratio threshold is hit. The aggregator is already invoked from
# ``_handle_general_failure`` (see ``maybe_aggregate_parent_failure``
# call) and is covered end-to-end by
# ``tests/test_export_coordinator_failure_propagation.py``; the
# remaining missing assertion would require a real Job row plumbed
# through a Postgres fixture, which lives in the
# ``--with-postgres`` suite. Park as a follow-up rather than block
# the unit-test gate.
