"""One sequence the card cannot fit must not dead-letter the 63 beside it.

A batch that raises CUDA OOM is retried five times with exponential backoff
and then dead-lettered, with ``child.cuda_oom_dead_letter`` written on the
parent at error level. That is correct and it is not silent. What it cannot do
is save the sequences that merely shared the batch, because the partition is by
position in an unordered list.

Measured on the ankh-base full-length run of 2026-10-28: 146 of the 594.512
pending sequences are above the ~8.650-residue ceiling that transformers
4.48.1's four ``L^2`` tensors per layer leave on a 12 GB card, and at 64 a
batch they would take up to 9.198 sequences down with them.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.operations._compute_embeddings_helpers import (
    build_batch_dispatch_messages,
    partition_sequence_ids,
)
from protea.core.operations._long_sequence_isolation import (
    refuse_and_record,
    refuse_over_ceiling,
)
from protea.core.operations.compute_embeddings import ComputeEmbeddingsPayload


def _ids(n: int) -> list[int]:
    return list(range(1, n + 1))


class TestWithNoCeiling:
    def test_the_partition_is_exactly_what_it_always_was(self):
        """The regression this change has to not cause. Absent is the default,
        and every job already dispatched in this deployment was partitioned
        this way."""
        ids = _ids(150)
        assert partition_sequence_ids(ids, 64, None, None) == [
            ids[0:64], ids[64:128], ids[128:150],
        ]

    def test_lengths_are_not_read_when_no_ceiling_is_given(self):
        """Passing them changes nothing, so a caller that loads them for some
        other reason cannot accidentally alter the partition."""
        ids = _ids(10)
        hostile = dict.fromkeys(ids, 10**9)
        assert partition_sequence_ids(ids, 4, None, hostile) == (
            partition_sequence_ids(ids, 4, None, None)
        )

    def test_an_empty_run_is_no_batches(self):
        assert partition_sequence_ids([], 64, None, None) == []


class TestWithACeiling:
    def test_an_over_long_sequence_is_alone_in_its_batch(self):
        lengths = {1: 300, 2: 300, 3: 35991, 4: 300, 5: 300}
        assert partition_sequence_ids(_ids(5), 64, 16384, lengths) == [[1, 2], [3], [4, 5]]

    def test_the_rest_still_fill_to_the_batch_size(self):
        lengths = dict.fromkeys(_ids(10), 300) | {5: 40000}
        assert partition_sequence_ids(_ids(10), 3, 16384, lengths) == [
            [1, 2, 3], [4], [5], [6, 7, 8], [9, 10],
        ]

    def test_the_ceiling_is_strict_so_exactly_the_ceiling_still_shares(self):
        """The number names the longest length known to fit, so a sequence AT
        it is one that fits."""
        lengths = {1: 16384, 2: 16385}
        assert partition_sequence_ids([1, 2], 64, 16384, lengths) == [[1], [2]]

    def test_a_run_of_nothing_but_long_sequences_is_all_singletons(self):
        lengths = dict.fromkeys(_ids(4), 30000)
        assert partition_sequence_ids(_ids(4), 64, 16384, lengths) == [[1], [2], [3], [4]]

    def test_consecutive_long_sequences_do_not_share(self):
        """The 57 are not spread evenly: the eight longest pending are all over
        33.000 residues, so they can arrive together."""
        lengths = {1: 300, 2: 35991, 3: 35213, 4: 34484, 5: 300}
        assert partition_sequence_ids(_ids(5), 64, 16384, lengths) == [
            [1], [2], [3], [4], [5],
        ]

    def test_every_sequence_appears_exactly_once(self):
        """Isolation reorders nothing and drops nothing: the union of the
        batches is the run, in order."""
        ids = _ids(200)
        lengths = {i: (40000 if i % 17 == 0 else 300) for i in ids}
        batches = partition_sequence_ids(ids, 64, 16384, lengths)
        assert [s for b in batches for s in b] == ids

    def test_a_sequence_with_no_known_length_is_not_treated_as_long(self):
        """A missing row reads as 0 rather than as a reason to isolate: the
        conservative direction is the one that does not change the partition
        on an incomplete lookup."""
        assert partition_sequence_ids([1, 2], 64, 16384, {1: 300}) == [[1, 2]]


class TestTheRefusal:
    def test_a_ceiling_with_no_lengths_is_refused(self):
        """Partitioning as though nothing were long would silently produce the
        behaviour the caller asked to avoid, which is worse than failing."""
        with pytest.raises(ValueError, match="no sequence lengths were loaded"):
            partition_sequence_ids(_ids(3), 64, 16384, None)


class TestThePayload:
    def test_the_default_is_absent(self):
        p = ComputeEmbeddingsPayload(embedding_config_ids=[str(uuid.uuid4())])
        assert p.isolate_above_residues is None

    def test_a_ceiling_survives_the_json_round_trip(self):
        import json

        p = ComputeEmbeddingsPayload.model_validate(
            json.loads(json.dumps({"embedding_config_ids": [str(uuid.uuid4())],
                                   "isolate_above_residues": 16384})))
        assert p.isolate_above_residues == 16384

    def test_a_ceiling_of_zero_is_refused_by_the_type(self):
        """Zero would isolate every sequence, which is a batch size of one
        written in the wrong field."""
        with pytest.raises(ValueError):
            ComputeEmbeddingsPayload(embedding_config_ids=[str(uuid.uuid4())],
                                     isolate_above_residues=0)


class TestTheMessages:
    def test_the_dispatched_messages_carry_the_isolated_batches(self):
        p = ComputeEmbeddingsPayload(
            embedding_config_ids=[str(uuid.uuid4())], sequences_per_job=3,
            isolate_above_residues=16384)
        lengths = dict.fromkeys(_ids(7), 300) | {4: 40000}
        msgs = build_batch_dispatch_messages(p, uuid.uuid4(), _ids(7), lengths)
        sent = [m[1]["payload"]["sequence_ids"] for m in msgs]
        assert sent == [[1, 2, 3], [4], [5, 6, 7]]

    def test_without_a_ceiling_the_messages_are_unchanged(self):
        p = ComputeEmbeddingsPayload(
            embedding_config_ids=[str(uuid.uuid4())], sequences_per_job=3)
        msgs = build_batch_dispatch_messages(p, uuid.uuid4(), _ids(7))
        assert [m[1]["payload"]["sequence_ids"] for m in msgs] == [[1, 2, 3], [4, 5, 6], [7]]


class TestTheHardCeiling:
    """Isolation turns 9.198 lost sequences into 146 and leaves the other cost
    alone: an isolated sequence that still does not fit burns six attempts and
    155 s with the consumer stopped, which for the 146 is 6,9 h of a node
    producing nothing. Measured at 19:10 against the running job: the longest
    that has fitted is 8.002, 54 pending sit between that and 8.650, and 146
    are above it."""

    def test_nothing_is_refused_without_a_ceiling(self):
        ids = _ids(5)
        assert refuse_over_ceiling(ids, None, None) == (ids, [])

    def test_what_is_known_not_to_fit_is_held_back(self):
        lengths = {1: 300, 2: 9000, 3: 400, 4: 36000}
        assert refuse_over_ceiling(_ids(4), 8650, lengths) == ([1, 3], [2, 4])

    def test_order_is_preserved_in_both_lists(self):
        lengths = {i: (9000 if i % 2 == 0 else 300) for i in _ids(8)}
        kept, refused = refuse_over_ceiling(_ids(8), 8650, lengths)
        assert kept == [1, 3, 5, 7] and refused == [2, 4, 6, 8]

    def test_the_ceiling_is_strict_so_exactly_the_ceiling_is_dispatched(self):
        assert refuse_over_ceiling([1, 2], 8650, {1: 8650, 2: 8651}) == ([1], [2])

    def test_an_unknown_length_is_dispatched_and_not_refused(self):
        """An incomplete lookup must not decide that the bank lacks a protein."""
        assert refuse_over_ceiling([1, 2], 8650, {1: 300}) == ([1, 2], [])

    def test_a_ceiling_with_no_lengths_is_refused(self):
        with pytest.raises(ValueError, match="no sequence lengths were loaded"):
            refuse_over_ceiling(_ids(3), 8650, None)


class TestTheReceipt:
    def test_the_gap_is_written_down_with_its_reason(self):
        """The deliverable, not the saving: nothing downstream can tell a
        protein that was refused from one that was never in the population."""
        p = ComputeEmbeddingsPayload(
            embedding_config_ids=[str(uuid.uuid4())], refuse_above_residues=8650)
        lengths = {1: 300, 2: 36000, 3: 9000}
        seen = []
        kept = refuse_and_record(
            _ids(3), p, lengths,
            lambda k, _m, f, lvl: seen.append((k, f, lvl)), "cuda")
        assert kept == [1]
        (kind, fields, level), = seen
        assert kind == "compute_embeddings.refused_over_ceiling"
        assert level == "warning"
        assert fields["refused"] == 2
        assert fields["longest"] == 36000
        assert fields["shortest_refused"] == 9000
        assert fields["sequence_ids"] == [2, 3]
        assert "not dispatched rather than" in fields["reason"]
        # The ceiling belongs to the card, the attention and the version at
        # once, so the gap can be closed when any of them changes.
        belongs = fields["ceiling_belongs_to"]
        assert belongs["attention"] == "eager bf16"
        assert belongs["transformers"] and belongs["transformers"] != ""
        assert belongs["device"] == "cuda"

    def test_the_version_is_read_and_not_declared(self):
        """The same card and model give a different ceiling under a version that
        keeps fewer tensors alive, so it is read from the worker that refused."""
        import transformers

        from protea.core.operations._long_sequence_isolation import transformers_version

        assert transformers_version() == str(transformers.__version__)

    def test_a_run_with_nothing_over_the_ceiling_says_nothing(self):
        p = ComputeEmbeddingsPayload(
            embedding_config_ids=[str(uuid.uuid4())], refuse_above_residues=8650)

        def boom(*_a, **_k):
            raise AssertionError("a run that refuses nothing is not an event")

        assert refuse_and_record(_ids(3), p, {1: 300, 2: 300, 3: 300}, boom) == _ids(3)

    def test_no_ceiling_emits_nothing_and_keeps_everything(self):
        p = ComputeEmbeddingsPayload(embedding_config_ids=[str(uuid.uuid4())])

        def boom(*_a, **_k):
            raise AssertionError("absent is the default and the default is silent")

        assert refuse_and_record(_ids(3), p, None, boom) == _ids(3)


class TestTheTwoCeilingsTogether:
    def test_the_uncertain_are_isolated_and_the_impossible_are_not_sent(self):
        """The shape the campaign actually needs: 8.002 isolates, 8.650 refuses,
        and a sequence between the two is sent alone rather than dropped."""
        p = ComputeEmbeddingsPayload(
            embedding_config_ids=[str(uuid.uuid4())], sequences_per_job=2,
            isolate_above_residues=8002, refuse_above_residues=8650)
        lengths = {1: 300, 2: 8300, 3: 300, 4: 36000, 5: 300}
        kept = refuse_and_record(_ids(5), p, lengths, lambda *_a: None)
        assert kept == [1, 2, 3, 5]
        assert partition_sequence_ids(kept, 2, p.isolate_above_residues, lengths) == [
            [1], [2], [3, 5],
        ]
