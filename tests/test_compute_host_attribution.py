"""Naming the machine that computed a batch, on the row rather than by inference.

The compute is distributed across two machines through a shared queue, and until
this landed nothing recorded which one did which piece. Every field on a batch
event described the work and none described the worker, so a run that executed
across both boxes was indistinguishable from one that executed on either, and a
result that differed between them had no attributable cause.

These tests pin the two things that make the field trustworthy: that it is always
present and never empty, and that a machine can declare its role rather than being
identified by an installation accident.
"""

from __future__ import annotations

import pytest

from protea.infrastructure.queue._host import compute_host


@pytest.fixture(autouse=True)
def _uncached():
    """The value is cached for the process, so each test needs a clean read."""
    compute_host.cache_clear()
    yield
    compute_host.cache_clear()


def test_a_declared_name_wins_over_the_hostname(monkeypatch):
    """A hostname is an accident of installation. The topology role is the fact
    a reader needs in two years."""
    monkeypatch.setenv("PROTEA_COMPUTE_HOST", "desktop-gpu")

    assert compute_host() == "desktop-gpu"


def test_an_empty_declaration_falls_back_rather_than_stamping_nothing(monkeypatch):
    """A unit file exporting the variable empty would otherwise mark every row
    with nothing, which reads as a defect in this function."""
    monkeypatch.setenv("PROTEA_COMPUTE_HOST", "   ")

    assert compute_host().strip() != ""


def test_an_undeclared_host_still_gets_a_name(monkeypatch):
    monkeypatch.delenv("PROTEA_COMPUTE_HOST", raising=False)

    assert compute_host().strip() != ""


def test_the_name_is_stable_within_a_process(monkeypatch):
    monkeypatch.setenv("PROTEA_COMPUTE_HOST", "laptop")

    assert compute_host() == compute_host()


def test_a_failure_to_resolve_the_hostname_does_not_break_event_writing(monkeypatch):
    """Recording an event must never fail because the machine could not be named."""
    import socket

    monkeypatch.delenv("PROTEA_COMPUTE_HOST", raising=False)
    monkeypatch.setattr(socket, "gethostname", lambda: (_ for _ in ()).throw(OSError))

    assert compute_host() == "unknown"


def test_an_empty_hostname_is_replaced_rather_than_recorded(monkeypatch):
    import socket

    monkeypatch.delenv("PROTEA_COMPUTE_HOST", raising=False)
    monkeypatch.setattr(socket, "gethostname", lambda: "")

    assert compute_host() == "unknown"


def test_the_consumer_stamps_it_on_every_child_event():
    """The regression this exists for: one central emitter, so a new operation
    cannot forget to attribute itself."""
    import inspect

    from protea.infrastructure.queue import consumer

    source = inspect.getsource(consumer.OperationConsumer._make_raw_emit)

    assert '"host": compute_host()' in source


def test_the_stamp_does_not_displace_the_operations_own_fields():
    """Merged into the caller's dict rather than replacing it, or every batch
    event would lose queries, references and rows."""
    import inspect

    from protea.infrastructure.queue import consumer

    source = inspect.getsource(consumer.OperationConsumer._make_raw_emit)

    assert "**(fields or {})" in source
