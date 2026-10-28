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
