"""A pass that says it does not truncate has to prove it, per sequence.

THE CAMPAIGN'S OBJECTIVE RESTS ON THIS. The point of the per-residue arms is to
work on the PLM's signal at FULL LENGTH, and
``_encode_residue_sparse_batch`` says so in as many words: "nothing here
truncates: a 35,991-residue protein enters the model whole". Its whole batching
design follows from that, budgeting in residues rather than in sequences.

But nothing checked it. ``_encode_batch`` summed ``residues.shape[0]`` into a
counter it reported and never compared that count to the sequence it came from,
and the only strict thing on the path, ``zip(..., strict=True)``, catches a
missing SEQUENCE and not a shortened one.

AND FULL LENGTH IS HONOURED BY ACCIDENT, not by request. The Ankh plugin
tokenises with ``truncation=True`` and NO ``max_length``
(``protea_backends/ankh/__init__.py:273``), which means full length only because
this checkpoint declares no ``model_max_length``. A new revision's
``tokenizer_config.json``, a different checkpoint, or an explicit kwarg and
transformers substitutes its own bound. Nothing would fail: the codes would be
computed over a prefix, written, and scored, and the arm would report success
having measured a truncation nobody asked for.

So the check is on the COUNT and not on the kwarg. A count catches every cause
-- a tokeniser bound, a padding change, a plugin edit, a checkpoint swap --
where fixing the kwarg would catch one and leave the next.

WHAT EQUALITY MEANS HERE, read off the plugin rather than assumed:

    actual_len = int(attn[i].sum().item())          # non-pad tokens, EOS included
    residues = layer_pooled[i, : actual_len - 1, :]  # trailing EOS stripped

Ankh is fed ``[list(c) for c in cleaned]`` with ``is_split_into_words=True``, so
one token per character, and the ``[UZOB] -> X`` cleaning substitutes rather
than deletes. One row per residue, exactly, is therefore the contract.
"""

from __future__ import annotations

import numpy as np


class ResidueCountMismatch(ValueError):
    """The residue tensor does not have one row per residue of its sequence.

    A ``ValueError`` so the batch worker treats it as a hard, non-retryable
    failure like every other bad-input refusal on this path: retrying would
    re-run the same pass over the same sequence and get the same prefix.
    """


def assert_one_row_per_residue(sequence: str, residues: np.ndarray, *, sequence_id: int) -> None:
    """Raise unless ``residues`` carries exactly one row per residue.

    SHORT is truncation, which is the case this exists for and the one that is
    silent: a prefix of the signal, encoded and stored as though it were the
    protein.

    LONG is refused too, and not out of symmetry. More rows than residues means
    the row-to-residue mapping is off by something -- a special token left in, a
    tokeniser that split a character -- so every position the encoder reads is
    displaced from the residue it is supposed to describe. That is a worse
    failure than truncation and an even quieter one, because the code still has
    the right shape.
    """
    got = int(residues.shape[0])
    want = len(sequence)
    if got == want:
        return
    direction = (
        "TRUNCATED: the pass returned a prefix"
        if got < want
        else "LONGER than the sequence: the row-to-residue mapping is displaced"
    )
    raise ResidueCountMismatch(
        f"sequence_id {sequence_id}: {direction}. {got} rows for {want} residues. "
        "This path's batching exists because nothing here truncates, so a count that "
        "does not match is not a tolerance to widen: the code would describe a part of "
        "the protein, or the wrong positions of it, and nothing downstream could tell. "
        "The Ankh plugin asks for truncation with no bound, so full length holds only "
        "while the checkpoint declares no model_max_length; check that first."
    )
