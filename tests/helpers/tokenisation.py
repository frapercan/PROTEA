"""How each seeded EmbeddingConfig turns a sequence into token ids.

Shared by ``test_a_tokenisation_that_moved_is_not_the_same_tokenisation`` and by
``scripts/capture_tokeniser_golden.py``, so the reference and the check are
produced by the same code. They are deliberately the same function: a capture
path that drifted from the verification path would let a green run mean nothing.

Nothing here executes on import. An earlier draft of the capture tool did its
work at module level, and the verifier -- which imported it to reuse these
definitions -- rewrote the reference with whatever the current environment
produced and then compared it against itself. It passed under every version of
``transformers``, including the one that corrupts Ankh's tokenisation.
"""

from __future__ import annotations

import re

# A fixed, real sequence. Not random: the point is that two runs on two machines
# under two library versions compare the same thing.
SEQUENCE = re.sub(
    r"[UZOB]",
    "X",
    "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVKVKALPDAQFEVVHSLAKWKR",
)

# (key, backend, model_name, tokeniser repo, mode) for the eight rows seeded by
# migration e7a1c4f9b2d6 and its predecessors. ``model_name`` is the checkpoint
# the backend loads; the tokeniser repo is not always the same (protst loads
# ESM-1b's tokeniser against a ProtST checkpoint).
CONFIGS: tuple[tuple[str, str, str, str | None, str], ...] = (
    ("ankh_base", "ankh", "ElnaggarLab/ankh-base", "ElnaggarLab/ankh-base", "chars"),
    ("ankh_large", "ankh", "ElnaggarLab/ankh-large", "ElnaggarLab/ankh-large", "chars"),
    ("esm2_8m", "esm", "facebook/esm2_t6_8M_UR50D", "facebook/esm2_t6_8M_UR50D", "raw"),
    ("esm2_650m", "esm", "facebook/esm2_t33_650M_UR50D", "facebook/esm2_t33_650M_UR50D", "raw"),
    ("prostt5", "t5", "Rostlab/ProstT5", "Rostlab/ProstT5", "aa2fold"),
    (
        "prot_t5",
        "t5",
        "Rostlab/prot_t5_xl_half_uniref50-enc",
        "Rostlab/prot_t5_xl_half_uniref50-enc",
        "spaced",
    ),
    ("protst", "protst", "mila-intel/ProtST-esm1b", "facebook/esm1b_t33_650M_UR50S", "raw"),
    ("esmc_600m", "esm3c", "esmc_600m", None, "esmc"),
)


def tokenise(tokeniser_repo: str | None, mode: str, sequence: str = SEQUENCE) -> tuple[str, list[int]]:
    """Reproduce the call the corresponding backend actually makes.

    ``chars``   -- ``ankh``: list of characters, ``is_split_into_words=True``
                   (protea_backends/ankh/__init__.py:270).
    ``spaced``  -- ``t5``: space-joined residues, ``T5Tokenizer`` loaded
                   explicitly rather than through ``AutoTokenizer``
                   (protea_backends/t5/__init__.py:115, :273).
    ``aa2fold`` -- ``t5`` for ProstT5: as ``spaced`` with the ``<AA2fold>``
                   prefix the backend adds for that checkpoint.
    ``raw``     -- ``esm`` and ``protst``: the bare string.
    ``esmc``    -- ESM-C has no HuggingFace tokeniser; it builds its own from
                   the ``esm`` package (protea_backends/esm3c/__init__.py).

    Returns the tokeniser class name alongside the ids: a class change with
    identical ids is worth reporting but is not a failure, while identical
    classes with different ids is the failure this exists to catch.
    """
    if mode == "esmc":
        from esm.tokenization import EsmSequenceTokenizer

        tokeniser = EsmSequenceTokenizer()
        return type(tokeniser).__name__, [int(i) for i in tokeniser.encode(sequence)]

    if mode == "chars":
        from transformers import AutoTokenizer

        tokeniser = AutoTokenizer.from_pretrained(tokeniser_repo)
        encoded = tokeniser(
            [list(sequence)],
            padding="longest",
            truncation=True,
            add_special_tokens=True,
            is_split_into_words=True,
        )
    elif mode in ("aa2fold", "spaced"):
        from transformers import T5Tokenizer

        tokeniser = T5Tokenizer.from_pretrained(tokeniser_repo, do_lower_case=False)
        prefix = "<AA2fold> " if mode == "aa2fold" else ""
        encoded = tokeniser(
            [prefix + " ".join(sequence)],
            padding="longest",
            truncation=True,
            add_special_tokens=True,
        )
    else:
        from transformers import AutoTokenizer

        tokeniser = AutoTokenizer.from_pretrained(tokeniser_repo)
        encoded = tokeniser(
            [sequence], padding="longest", truncation=True, add_special_tokens=True
        )

    return type(tokeniser).__name__, [int(i) for i in encoded["input_ids"][0]]
