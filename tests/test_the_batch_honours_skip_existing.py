"""A batch whose work is already stored must not pay for the forward pass.

``ComputeEmbeddingsBatchPayload`` declares ``skip_existing`` and the dispatch
message carries it, but the batch worker never read it: the flag was honoured
one stage later, inside ``store_group_rows``, which skips the INSERT for a
pair that already exists. The pass had already been paid by then.

Measured on 2026-10-28: the compute node left the network with 8.524 of 10.689
batches queued, and the reaper's rule for a lease-less RUNNING job past its
six-hour timeout is to RE-ENQUEUE, so the coordinator would dispatch a second
message for every still-missing sequence. At the 4,38 s per batch that run
measured, those duplicates are 10,5 h of card time spent recomputing stored
rows.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

from protea.core.operations._batch_skip_existing import (
    close_fully_stored_batch,
    sequences_still_missing,
)


class _Q:
    """A query whose filter chain records whether the predicate was applied."""

    def __init__(self, rows, filtered_rows):
        self._rows = rows
        self._filtered = filtered_rows
        self.filters = 0

    def filter(self, *_a):
        self.filters += 1
        return self if self.filters == 1 else _Q(self._filtered, self._filtered)

    def all(self):
        return self._rows


class _Session:
    def __init__(self, rows, filtered_rows):
        self.q = _Q(rows, filtered_rows)

    def query(self, *_a):
        return self.q


def _payload(ids, skip=True):
    return SimpleNamespace(sequence_ids=ids, skip_existing=skip)


def _configs(n=1):
    return [SimpleNamespace(id=uuid.uuid4()) for _ in range(n)]


def _spy():
    seen: list[tuple] = []
    return seen, lambda kind, _m, fields, level: seen.append((kind, fields, level))


class TestTheFilter:
    def test_skip_existing_false_returns_the_whole_batch(self):
        """The flag means what it says: turning it off still re-infers
        everything, which is how a deliberate recompute is asked for."""
        rows = [object(), object()]
        s = _Session(rows, [])
        _seen, emit = _spy()
        assert sequences_still_missing(s, _payload([1, 2], skip=False), _configs(), emit) is rows
        assert s.q.filters == 1  # only the id filter; no predicate

    def test_skip_existing_true_applies_the_group_predicate(self):
        s = _Session([object()] * 3, [object()])
        _seen, emit = _spy()
        kept = sequences_still_missing(s, _payload([1, 2, 3]), _configs(2), emit)
        assert len(kept) == 1
        assert s.q.filters == 2  # the id filter and the missing-for-any-config one

    def test_the_saving_is_reported_and_not_silent(self):
        """The only visible difference from the old behaviour is a smaller
        ``sequences_inferred``, which on its own reads like lost rows."""
        s = _Session([object()] * 3, [object()])
        seen, emit = _spy()
        sequences_still_missing(s, _payload([1, 2, 3]), _configs(), emit)
        (kind, fields, level), = seen
        assert kind == "compute_embeddings_batch.already_stored"
        assert level == "info"
        assert (fields["requested"], fields["still_missing"]) == (3, 1)
        assert fields["already_stored"] == 2
        assert fields["whole_batch"] is False

    def test_a_batch_with_nothing_left_says_whole_batch(self):
        s = _Session([], [])
        seen, emit = _spy()
        assert sequences_still_missing(s, _payload([1, 2]), _configs(), emit) == []
        assert seen[0][1]["whole_batch"] is True

    def test_nothing_saved_emits_nothing(self):
        """The ordinary path, which is every batch of a first run."""
        rows = [object(), object()]
        s = _Session(rows, rows)

        def boom(*_a, **_k):
            raise AssertionError("a batch with nothing already stored is not an event")

        assert len(sequences_still_missing(s, _payload([1, 2]), _configs(), boom)) == 2


class TestClosingASkippedBatch:
    def test_the_parent_counter_is_advanced(self):
        """The reason this is a function. The WRITE stage normally advances the
        parent and a skipped batch publishes no write message, so without this
        the job would sit below its total for ever."""
        calls = []
        import protea.core.operations._batch_skip_existing as mod

        original = mod.update_parent_progress
        mod.update_parent_progress = lambda *a, **k: calls.append((a, k))
        try:
            pid = uuid.uuid4()
            seen, emit = _spy()
            res = close_fully_stored_batch(object(), pid, 64, emit)
        finally:
            mod.update_parent_progress = original
        assert len(calls) == 1
        assert calls[0][1]["event_name"] == "compute_embeddings_batch.parent_succeeded"
        assert res.result == {"skipped": True, "already_stored": True}
        (kind, fields, level), = seen
        assert kind == "compute_embeddings_batch.skipped"
        assert fields["reason"] == "already_stored"
        assert fields["requested"] == 64
        assert level == "info"


class TestItIsWiredBeforeThePass:
    def test_the_filter_runs_before_infer_group(self):
        """Fixed by reading the source: the whole point is the ORDER. A version
        that filtered after ``_infer_group`` would be correct and useless, and
        it would look identical in every count the job reports."""
        import inspect

        from protea.core.operations.compute_embeddings import ComputeEmbeddingsBatchOperation

        src = inspect.getsource(ComputeEmbeddingsBatchOperation.execute)
        assert src.index("sequences_still_missing(") < src.index("_infer_group(")
        assert src.index("close_fully_stored_batch(") < src.index("_infer_group(")

    def test_the_batch_payload_field_is_no_longer_dead(self):
        """It was declared, carried in the message, and read by nobody on this
        side. That is what this change fixes."""
        import inspect

        from protea.core.operations.compute_embeddings import ComputeEmbeddingsBatchOperation

        src = inspect.getsource(ComputeEmbeddingsBatchOperation.execute)
        assert "sequences_still_missing(session, p, configs, emit)" in src
