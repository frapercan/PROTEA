"""The batch loop when the card runs out of room.

Both machines in this topology have a card and only one can hold a batch of eight
sequences at five thousand residues, so "has a GPU" is not a useful predicate and no
static description of anybody's memory would stay true. The loop halves instead.
"""

from __future__ import annotations

import pytest

from protea.core.operations._encode_residue_sparse_batch import (
    _is_out_of_memory,
    encode_until_done,
)


class _Run:
    batch_size = 8


class _Session:
    def __init__(self):
        self.commits = 0
        self.written = []

    def execute(self, _stmt, rows=None):
        if rows:
            self.written.extend(rows)

    def commit(self):
        self.commits += 1


def _emit(bucket):
    def emit(event, message, fields, level):
        bucket.append((event, fields))

    return emit


def _sequences(n):
    return [(i, "A" * 100) for i in range(1, n + 1)]


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("CUDA out of memory. Tried to allocate 2.00 GiB"),
        MemoryError("CUDA OOM"),
        type("OutOfMemoryError", (RuntimeError,), {})("allocation failed"),
    ],
)
def test_a_memory_fault_is_recognised_however_it_surfaces(exc):
    """Matched on the message because the same condition arrives as three different types."""
    assert _is_out_of_memory(exc)


def test_an_unrelated_failure_is_not_mistaken_for_one():
    assert not _is_out_of_memory(ValueError("the artifact declares no order"))


def test_the_batch_halves_until_it_fits_and_encodes_everything(monkeypatch):
    """Eight refused, four refused, two fit. Nothing is lost and the size sticks."""
    sizes_tried = []

    def fake_encode(_run, batch, _emit):
        sizes_tried.append(len(batch))
        if len(batch) > 2:
            raise RuntimeError("CUDA out of memory")
        rows = [{"sequence_id": i} for i, _s in batch]
        return rows, {"clipped": 0, "residues": 10 * len(batch), "densities": [0.0625] * len(batch)}

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", fake_encode)
    events, session = [], _Session()

    encoded, clipped, residues, densities = encode_until_done(
        session, _Run(), _sequences(8), _emit(events)
    )

    assert encoded == 8, "every sequence encoded despite two refusals"
    assert residues == 80
    assert len(densities) == 8
    assert sizes_tried == [8, 4, 2, 2, 2, 2], "halves, then keeps the size that worked"
    assert [e for e, _f in events].count("encode.shrinking") == 2
    assert session.commits == 4


def test_a_single_sequence_that_does_not_fit_is_raised_rather_than_halved(monkeypatch):
    """There is nothing left to halve, and pretending otherwise would loop forever."""

    def always_oom(_run, _batch, _emit):
        raise RuntimeError("CUDA out of memory")

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", always_oom)

    with pytest.raises(RuntimeError, match="out of memory"):
        encode_until_done(_Session(), _Run(), _sequences(1), _emit([]))


def test_an_unrelated_failure_is_not_retried_at_all(monkeypatch):
    calls = []

    def boom(_run, batch, _emit):
        calls.append(len(batch))
        raise ValueError("the artifact declares no order")

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", boom)

    with pytest.raises(ValueError, match="declares no order"):
        encode_until_done(_Session(), _Run(), _sequences(8), _emit([]))

    assert calls == [8], "raised on the first attempt rather than halved"
