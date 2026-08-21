"""The batch loop when the card runs out of room.

Both machines in this topology have a card and only one can hold a batch of eight
sequences at five thousand residues, so "has a GPU" is not a useful predicate and no
static description of anybody's memory would stay true. The loop halves instead.
"""

from __future__ import annotations

import pytest

import protea.core.operations._encode_residue_sparse_batch as batch_mod
from protea.core.operations._encode_residue_sparse_batch import (
    _is_out_of_memory,
    _starting_size,
    encode_until_done,
    take_batch,
)


@pytest.fixture(autouse=True)
def _forget_the_learned_size(monkeypatch):
    """Each test starts with a process that has learned nothing."""
    monkeypatch.setattr(batch_mod, "_LAST_GOOD_SIZE", None)


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

    encoded, clipped, residues, densities, oversized = encode_until_done(
        session, _Run(), _sequences(8), _emit(events)
    )

    assert oversized == [], "nothing was too large here"


    assert encoded == 8, "every sequence encoded despite two refusals"
    assert residues == 80
    assert len(densities) == 8
    assert sizes_tried == [8, 4, 2, 2, 2, 2], "halves, then keeps the size that worked"
    assert [e for e, _f in events].count("encode.shrinking") == 2
    assert session.commits == 4


def test_a_single_sequence_that_does_not_fit_is_skipped_and_recorded(monkeypatch):
    """Raising here costs every protein behind it in the message, not just this one.

    The consumer retries the whole batch and eventually dead-letters it, so one protein the
    card cannot hold takes thousands of ordinary ones with it. Observed: a 35,991-residue
    sequence stopped a corpus run at 91 per cent with 44,904 unencoded.
    """

    def always_oom(_run, _batch, _emit):
        raise RuntimeError("CUDA out of memory")

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", always_oom)
    events = []

    encoded, _c, _r, _d, oversized = encode_until_done(
        _Session(), _Run(), [(7, "A" * 35991)], _emit(events)
    )

    assert encoded == 0
    assert oversized == [{"sequence_id": 7, "residues": 35991}]
    assert [e for e, _f in events].count("encode.too_large") == 1, "skipped loudly, not silently"


def test_an_oversized_sequence_does_not_stop_the_ones_behind_it(monkeypatch):
    """The whole point: one protein that cannot fit must not cost the rest of the batch."""

    def oom_only_on_the_giant(_run, batch, _emit):
        if any(len(s) > 10_000 for _i, s in batch):
            raise RuntimeError("CUDA out of memory")
        rows = [{"sequence_id": i} for i, _s in batch]
        return rows, {"clipped": 0, "residues": 1, "densities": [0.0] * len(batch)}

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", oom_only_on_the_giant)
    pending = [(1, "A" * 100), (2, "A" * 35991), (3, "A" * 100)]

    encoded, _c, _r, _d, oversized = encode_until_done(_Session(), _Run(), pending, _emit([]))

    assert encoded == 2, "the two ordinary sequences are encoded"
    assert [o["sequence_id"] for o in oversized] == [2]


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


# ------------------------------------------------------ remembering what already worked

# A worker consumes many messages and the card does not change between them. Without
# this, the descent runs again on every message and pays an out-of-memory fault per step,
# forever. It still encodes nobody's memory: the number is learned from what the machine
# actually did, it lives only in the process, and a machine that never faults never sets it.


def test_a_process_that_has_learned_nothing_starts_where_it_was_asked():
    assert _starting_size(8) == 8


def test_a_process_that_learned_a_size_starts_there(monkeypatch):
    monkeypatch.setattr(batch_mod, "_LAST_GOOD_SIZE", 2)

    assert _starting_size(8) == 2


def test_the_payload_is_still_a_ceiling(monkeypatch):
    """A remembered size never raises a batch above what the caller asked for."""
    monkeypatch.setattr(batch_mod, "_LAST_GOOD_SIZE", 64)

    assert _starting_size(8) == 8


def test_the_second_message_does_not_descend_again(monkeypatch):
    """The whole point: one descent per process, not one per message."""
    tried = []

    def fake_encode(_run, batch, _emit):
        tried.append(len(batch))
        if len(batch) > 2:
            raise RuntimeError("CUDA out of memory")
        rows = [{"sequence_id": i} for i, _s in batch]
        return rows, {"clipped": 0, "residues": 10 * len(batch), "densities": [0.0] * len(batch)}

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", fake_encode)

    encode_until_done(_Session(), _Run(), _sequences(4), _emit([]))
    first = list(tried)
    tried.clear()
    encode_until_done(_Session(), _Run(), _sequences(4), _emit([]))

    assert first == [4, 2, 2], "the first message finds the size the hard way"
    assert tried == [2, 2], "the second starts at what worked and never faults"


# ------------------------------------------------------- residues, not sequence counts

# Nothing truncates: a 35,991-residue protein enters the model whole. So eight long
# proteins are thousands of times the work of eight short ones, and a count of sequences
# is the wrong unit for what the card can hold. Measured on this corpus, the remaining
# 477,407 proteins need 477,407 database round trips at one per batch and 48,705 at a
# 4,000-residue budget.


def _seqs(*lengths):
    return [(i, "A" * n) for i, n in enumerate(lengths, start=1)]


def test_short_sequences_are_grouped_up_to_the_budget():
    batch = take_batch(_seqs(300, 300, 300, 300), 0, budget=1000, cap=32)

    assert [i for i, _s in batch] == [1, 2, 3], "the fourth would exceed 1000 residues"


def test_a_sequence_longer_than_the_whole_budget_goes_alone():
    """Alone rather than skipped or split: splitting changes what is encoded and
    skipping drops a protein silently."""
    batch = take_batch(_seqs(9000, 100), 0, budget=1000, cap=32)

    assert [i for i, _s in batch] == [1]


def test_the_sequence_cap_still_applies_under_a_generous_budget():
    batch = take_batch(_seqs(10, 10, 10, 10), 0, budget=1_000_000, cap=2)

    assert len(batch) == 2


def test_an_empty_tail_yields_an_empty_batch():
    assert take_batch(_seqs(10, 10), 2, budget=1000, cap=8) == []


def test_the_budget_halves_on_the_residues_carried_not_on_the_count(monkeypatch):
    """The fault is about residues, so the retry has to be about residues too."""
    seen = []

    def fake_encode(_run, batch, _emit):
        residues = sum(len(s) for _i, s in batch)
        seen.append(residues)
        if residues > 600:
            raise RuntimeError("CUDA out of memory")
        rows = [{"sequence_id": i} for i, _s in batch]
        return rows, {"clipped": 0, "residues": residues, "densities": [0.0] * len(batch)}

    import protea.core.operations.encode_residue_sparse as op

    monkeypatch.setattr(op, "_encode_batch", fake_encode)

    class _R:
        batch_size = 32
        residue_budget = 2400

    encode_until_done(_Session(), _R(), _seqs(*([300] * 8)), _emit([]))

    assert seen[0] == 2400, "starts at the declared budget"
    assert seen[1] == 1200, "halves the residues, not the sequence count"
    assert seen[2] == 600, "and again, until it fits"
    assert sum(1 for r in seen if r <= 600) * 600 >= 2400 - 600
