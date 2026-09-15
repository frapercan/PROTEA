"""Every seeded EmbeddingConfig tokenises a fixed sequence into the same ids.

WHY THIS TEST EXISTS. On 2026-09-15, costing a ``transformers`` 5 upgrade, the
``ankh`` backend was found to tokenise a 78-residue sequence into 157 tokens
instead of 79, interleaving ``<unk>`` before every residue. It raised nothing.

The cause is upstream and invisible from the call site: in v5 ``T5Tokenizer`` is
a ``TokenizersBackend`` whose ``__init__`` rebuilds the tokeniser from the
vocabulary and hardcodes ``Metaspace(prepend_scheme="always")``, discarding the
``pre_tokenizer: null`` that Ankh ships in its ``tokenizer.json``. Ankh's
vocabulary is bare letters with no ``U+2581``, so every prefixed residue misses
and falls back to ``<unk>``.

What earns a test is the shape of the failure, not the cause. With
``pooling='mean'`` and ``use_chunking=False`` -- the seeded recipe for all eight
configs -- each sequence still yields exactly one vector of the expected
dimension, and the ``sequence_embedding`` row looks perfectly normal. The vector
is a mean over 2L positions of which half are the embedding of ``<unk>``.
Nothing downstream compares a residue count against the length of the protein.
``ankh_base`` and ``ankh_large`` are two of the four cells of the rung-1
matched-parameter grid.

No existing test could have caught it. The assertions in ``test_real_models.py``
check shape, finiteness, unit norm and within-process determinism: all
self-referential, all satisfied by a corrupt tokenisation.

The comparison is on the ids, not on their count. Two lists of the same length
with different contents are precisely the defect being guarded against, and a
length check would pass. A tokeniser that cannot be loaded fails rather than
skips: not checked is not the same as correct.

Regenerate with ``scripts/capture_tokeniser_golden.py``, and only when a
tokenisation change is intended and understood. Rewriting the reference to make
this pass is the one move that empties it of meaning.

NOT HERMETIC YET. The tokeniser files come from the HuggingFace cache or the
network; only ESM-C's is built in-process from the ``esm`` package. Vendoring
them under ``tests/golden/tokenisers/`` (about 1 MB) would make this run offline
and deterministic. That is a licensing decision, not a technical one, and it has
not been taken.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.helpers.tokenisation import CONFIGS, tokenise

GOLDEN = Path(__file__).parent / "golden" / "tokeniser_ids.json"


def _golden() -> dict:
    with GOLDEN.open() as fh:
        return json.load(fh)


def test_the_reference_covers_every_seeded_config() -> None:
    """A reference with holes is a rubber stamp for the configs it omits."""
    golden = _golden()
    missing = [key for key, *_ in CONFIGS if key not in golden["configs"]]
    holed = [key for key, entry in golden["configs"].items() if "ERROR" in entry]
    assert not missing, f"no golden value for {missing}; recapture before trusting a green run"
    assert not holed, f"golden value is an error for {holed}; recapture before trusting a green run"


@pytest.mark.parametrize(
    ("key", "backend", "model_name", "tokeniser_repo", "mode"),
    CONFIGS,
    ids=[c[0] for c in CONFIGS],
)
def test_a_tokenisation_that_moved_is_not_the_same_tokenisation(
    key: str, backend: str, model_name: str, tokeniser_repo: str | None, mode: str
) -> None:
    golden = _golden()
    expected = golden["configs"][key]

    try:
        klass, ids = tokenise(tokeniser_repo, mode, golden["_sequence"])
    except Exception as exc:  # noqa: BLE001 - the failure mode IS the finding
        pytest.fail(
            f"{key} ({backend}, {model_name}): its tokeniser could not be loaded -- "
            f"{type(exc).__name__}: {exc}. Not checked is not the same as correct."
        )

    assert ids == expected["ids"], (
        f"{key} ({backend}, {model_name}): {expected['n_tokens']} -> {len(ids)} tokens "
        f"for {golden['_n_residues']} residues, tokeniser class "
        f"{expected['class']} -> {klass}. The ids differ, so embeddings computed before "
        "and after this change are not comparable, and nothing downstream would say so."
    )
