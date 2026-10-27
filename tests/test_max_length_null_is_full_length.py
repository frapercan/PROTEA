"""A NULL ``max_length`` means full length, and nothing else moves.

``embedding_config.max_length`` could not express "no truncation applied": it
was ``integer NOT NULL`` defaulting to 1022, so every configuration asserted a
truncation point. A configuration computed over whole sequences -- the
ankh_base full-length mean_max bank, whose longest sequence is 35.213 residues
against 35.991 in this store -- had no honest way to describe itself, and a
sentinel integer would have made the recipe a function of the corpus.

``max_length`` is an identity field, so this file also pins what must NOT
change: a recipe that names an integer keeps the id it already had.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from protea.core.embedding_identity import derive_embedding_config_id
from protea.core.operations._compute_embeddings_backends import _truncation_kwargs
from protea.core.operations._compute_embeddings_helpers import _residue_budget

# ---------------------------------------------------------------------------
# The tokeniser contract
# ---------------------------------------------------------------------------


class TestTruncationKwargs:
    """Truncation is requested only when the configuration names a length."""

    def test_a_named_length_truncates_at_it(self) -> None:
        got = _truncation_kwargs(SimpleNamespace(max_length=1022))
        assert got == {"truncation": True, "max_length": 1022}

    def test_null_does_not_request_truncation_at_all(self) -> None:
        # Passing truncation=True with max_length=None would NOT mean "no
        # truncation": Hugging Face then falls back to the checkpoint's own
        # model_max_length. The truncation point would come from the weights
        # instead of from the recipe, and nothing would say so.
        got = _truncation_kwargs(SimpleNamespace(max_length=None))
        assert got == {"truncation": False}
        assert "max_length" not in got


# ---------------------------------------------------------------------------
# Identity: what changes and what must not
# ---------------------------------------------------------------------------

#: The live ankh_base recipe on this server, read from embedding_config.
#: ``embedding_scale`` is 1.0 and not 1: the digest is taken over JSON, where
#: ``1`` and ``1.0`` are different strings, so an integer here mints a
#: different id for the same embedding.
_ANKH_BASE_RECIPE = {
    "chunk_overlap": 0,
    "chunk_size": 512,
    "embedding_scale": 1.0,
    "layer_agg": "mean",
    "layer_indices": [0],
    "max_length": 1022,
    "model_backend": "ankh",
    "model_name": "ElnaggarLab/ankh-base",
    "normalize": True,
    "normalize_residues": False,
    "pooling": "mean",
    "use_chunking": False,
}

#: The id that recipe already carries. If this moves, every stored embedding
#: that cites it has been orphaned.
_ANKH_BASE_ID = "e7ba37da-c18d-5261-b5b4-49a3093d9d4d"


class TestIdentityIsUndisturbed:
    def test_the_live_recipe_still_derives_its_stored_id(self) -> None:
        assert str(derive_embedding_config_id(_ANKH_BASE_RECIPE)) == _ANKH_BASE_ID

    def test_a_null_length_is_a_different_configuration(self) -> None:
        full = dict(_ANKH_BASE_RECIPE, max_length=None)
        assert str(derive_embedding_config_id(full)) != _ANKH_BASE_ID

    def test_a_null_length_derives_deterministically(self) -> None:
        full = dict(_ANKH_BASE_RECIPE, max_length=None)
        once = derive_embedding_config_id(full)
        twice = derive_embedding_config_id(dict(reversed(list(full.items()))))
        assert once == twice

    def test_null_is_a_value_and_not_an_absent_field(self) -> None:
        # recipe_of refuses a recipe with a field missing, because an id
        # assembled from defaults would look as authoritative as a real one.
        # None has to pass that check: it is a described configuration.
        full = dict(_ANKH_BASE_RECIPE, max_length=None)
        derive_embedding_config_id(full)  # does not raise

        incomplete = dict(_ANKH_BASE_RECIPE)
        del incomplete["max_length"]
        with pytest.raises(ValueError, match="max_length"):
            derive_embedding_config_id(incomplete)

    def test_the_scale_must_be_a_float_or_the_id_moves(self) -> None:
        # Not a property of this change, but the sharpest edge in the scheme:
        # the digest is over JSON, so 1 and 1.0 are different recipes. Pinned
        # so a caller that assembles a recipe from an integer column is caught
        # here rather than by a mysteriously unfamiliar id.
        as_int = dict(_ANKH_BASE_RECIPE, embedding_scale=1)
        assert str(derive_embedding_config_id(as_int)) != _ANKH_BASE_ID


# ---------------------------------------------------------------------------
# Cost accounting
# ---------------------------------------------------------------------------


def _residue_counts(
    max_length: int | None, use_chunking: bool, lengths: list[int]
) -> dict[str, int]:
    config = SimpleNamespace(max_length=max_length, use_chunking=use_chunking)
    seqs = [SimpleNamespace(sequence="A" * n) for n in lengths]
    return _residue_budget(seqs, config)  # type: ignore[arg-type]


class TestResidueAccounting:
    """What the model saw, when nothing was truncated."""

    def test_a_named_length_counts_truncation(self) -> None:
        got = _residue_counts(100, False, [50, 150])
        assert got["residues_available"] == 200
        assert got["residues_processed"] == 150
        assert got["residues_truncated"] == 50

    def test_null_processes_everything(self) -> None:
        got = _residue_counts(None, False, [50, 150, 35991])
        assert got["residues_available"] == 36191
        assert got["residues_processed"] == 36191
        assert got["residues_truncated"] == 0

    def test_null_does_not_raise_against_min(self) -> None:
        # min(n, None) is a TypeError in Python 3; this is the crash the
        # nullable column would otherwise have introduced.
        _residue_counts(None, False, [10])
