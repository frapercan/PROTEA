"""Cost needs a unit the corpus does not distort, and residues are it.

``compute_embeddings_batch.done`` carried a clock and no residues; the backends
carried residues and no clock. Neither could form residues per second, so the
only available cost figure was sequences per second -- and this corpus has a
median of 318 residues against a maximum of 35,991, so a batch of 256 short
sequences and a batch of 256 long ones are two different jobs under one name.
The layer grid compares four lineages with different tokenizers, which is
exactly where that stops being tolerable.
"""

from __future__ import annotations

from protea.core.operations.compute_embeddings import _residue_budget


class _Seq:
    def __init__(self, n: int) -> None:
        self.sequence = "A" * n


class _Config:
    def __init__(self, *, max_length: int = 1022, use_chunking: bool = False) -> None:
        self.max_length = max_length
        self.use_chunking = use_chunking


class TestWhatTheModelActuallySees:
    def test_short_sequences_are_processed_whole(self):
        budget = _residue_budget([_Seq(318), _Seq(400)], _Config())
        assert budget["residues_available"] == 718
        assert budget["residues_processed"] == 718
        assert budget["residues_truncated"] == 0

    def test_a_long_sequence_is_counted_at_what_the_model_saw(self):
        """Without chunking the tokenizer stops at max_length, so the cost is
        capped there and the rest of the protein never reaches the forward pass."""
        budget = _residue_budget([_Seq(35991)], _Config(max_length=1022))
        assert budget["residues_processed"] == 1022
        assert budget["residues_truncated"] == 35991 - 1022

    def test_chunking_processes_the_whole_sequence(self):
        budget = _residue_budget([_Seq(35991)], _Config(use_chunking=True))
        assert budget["residues_processed"] == 35991
        assert budget["residues_truncated"] == 0

    def test_the_three_numbers_always_close(self):
        seqs = [_Seq(n) for n in (10, 1021, 1022, 1023, 5000)]
        budget = _residue_budget(seqs, _Config())
        assert (
            budget["residues_processed"] + budget["residues_truncated"]
            == budget["residues_available"]
        )

    def test_an_empty_batch_reports_zeroes_not_an_error(self):
        assert _residue_budget([], _Config()) == {
            "residues_available": 0,
            "residues_processed": 0,
            "residues_truncated": 0,
        }


class TestTruncationIsNotUniform:
    def test_truncation_lands_entirely_on_the_long_stratum(self):
        """Length is one of the campaign's stratification axes, so a cost unit
        that hides truncation would let 'long proteins score worse' mean either
        'they are harder' or 'the model saw less of them', with no way to tell.

        Measured on the live corpus: 6.93% of 210,699,856 residues never reach
        the model, concentrated in the 23,405 sequences (4.4%) over 1022.
        """
        short = [_Seq(300)] * 10
        long = [_Seq(3000)] * 10
        assert _residue_budget(short, _Config())["residues_truncated"] == 0
        assert _residue_budget(long, _Config())["residues_truncated"] > 0
