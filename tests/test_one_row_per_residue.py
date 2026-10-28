"""The per-residue pass claims it does not truncate. Now it has to prove it.

``_encode_residue_sparse_batch`` budgets in RESIDUES rather than in sequences,
and says why: "nothing here truncates: a 35,991-residue protein enters the
model whole". The whole design follows from that sentence and nothing checked
it.

What made it reachable, measured on the served tree: the Ankh plugin tokenises
with ``truncation=True`` and NO ``max_length``
(``protea_backends/ankh/__init__.py:273``). Full length therefore holds only
while this checkpoint declares no ``model_max_length``. Give that tokeniser a
bound -- a new revision's ``tokenizer_config.json``, another checkpoint, an
explicit kwarg -- and transformers substitutes its own. Nothing raises: the
codes are computed over a prefix, written, scored, and the arm reports success.

The check is on the COUNT, not on the kwarg, because a count catches every
cause and a kwarg catches one.
"""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations._residue_count_guard import (
    ResidueCountMismatch,
    assert_one_row_per_residue,
)

DIM = 768


def _residues(n: int) -> np.ndarray:
    return np.zeros((n, DIM), dtype=np.float32)


def test_one_row_per_residue_passes():
    """The contract read off the plugin: attention mask minus the trailing EOS,
    one token per character, so exactly one row per residue."""
    assert_one_row_per_residue("MKV" * 10, _residues(30), sequence_id=1) is None


class TestTruncationIsRefused:
    def test_a_prefix_is_refused(self):
        with pytest.raises(ResidueCountMismatch, match="TRUNCATED"):
            assert_one_row_per_residue("M" * 2000, _residues(1022), sequence_id=7)

    def test_the_refusal_carries_both_counts(self):
        """A refusal nobody can act on gets widened into a tolerance."""
        with pytest.raises(ResidueCountMismatch) as exc:
            assert_one_row_per_residue("M" * 2000, _residues(1022), sequence_id=7)
        assert "1022 rows for 2000 residues" in str(exc.value)

    def test_the_refusal_names_where_to_look_first(self):
        """The cause is one line in a plugin, and a reader who does not know
        that will go looking in the operation."""
        with pytest.raises(ResidueCountMismatch) as exc:
            assert_one_row_per_residue("M" * 100, _residues(50), sequence_id=1)
        assert "model_max_length" in str(exc.value)

    def test_the_sequence_id_is_in_the_message(self):
        """The batch carries many sequences and the failure is about one."""
        with pytest.raises(ResidueCountMismatch, match="sequence_id 4242"):
            assert_one_row_per_residue("M" * 10, _residues(9), sequence_id=4242)

    def test_one_residue_short_is_already_a_refusal(self):
        """No tolerance. Off by one here is the trailing EOS being kept or
        dropped twice, and that displaces every position by one."""
        with pytest.raises(ResidueCountMismatch):
            assert_one_row_per_residue("MKVLA", _residues(4), sequence_id=1)


class TestLongerIsRefusedToo:
    def test_more_rows_than_residues_is_refused(self):
        with pytest.raises(ResidueCountMismatch, match="LONGER"):
            assert_one_row_per_residue("MKVLA", _residues(6), sequence_id=1)

    def test_and_the_reason_is_the_mapping_and_not_symmetry(self):
        """Refused because every position the encoder reads is displaced from
        the residue it describes, which is quieter than truncation: the code
        still comes out the right shape."""
        with pytest.raises(ResidueCountMismatch) as exc:
            assert_one_row_per_residue("MKVLA", _residues(7), sequence_id=1)
        assert "displaced" in str(exc.value)


class TestTheEdges:
    def test_an_empty_sequence_with_no_rows_agrees(self):
        assert_one_row_per_residue("", _residues(0), sequence_id=1) is None

    def test_an_empty_sequence_with_rows_is_refused(self):
        with pytest.raises(ResidueCountMismatch):
            assert_one_row_per_residue("", _residues(1), sequence_id=1)

    @pytest.mark.parametrize("n", [1, 1021, 1022, 1023, 2048, 35991])
    def test_it_holds_at_every_length_the_store_actually_has(self, n: int):
        """1022 is the baseline's bound and 35.991 is the longest sequence in
        the store, measured. Neither is special to this guard, and that is the
        point: the bound belongs to a config, not to the pass."""
        assert_one_row_per_residue("M" * n, _residues(n), sequence_id=1) is None


def test_the_guard_runs_before_the_encoder_sees_the_residues():
    """Order is load-bearing: encode_one does a matmul that would succeed on a
    prefix and produce a code for part of a protein."""
    import inspect

    from protea.core.operations import encode_residue_sparse

    source = inspect.getsource(encode_residue_sparse._encode_batch)
    guard_at = source.index("assert_one_row_per_residue")
    encode_at = source.index("code = encode_one")
    assert guard_at < encode_at
