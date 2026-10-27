"""A config's id is the hash of its recipe, so the row has to hold that recipe.

``embedding_identity`` exists so that "two machines that build the same recipe
land on the same id without talking to each other, and two recipes that differ
anywhere cannot collide". That is only true if the row ends up holding the
recipe the id was derived from.

It stopped being true the moment NULL became meaningful. Migration
``b4e82f17c9a3`` made ``max_length`` nullable so a configuration computed over
whole sequences could say so, and the column still carried
``default=1022``, which SQLAlchemy applies at INSERT -- after the
``before_insert`` listener has hashed the recipe.

MEASURED, on the live store, creating the lab's full-length ankh encoding
through ``POST /v1/embeddings/configs`` with ``max_length: null``:

    the row's id                         f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0
    the id its stored recipe derives     556c8ebc-c4e7-5152-b34f-5a8bc9658387

Two ids for one row. These pin the three statements that keep it from
happening again: an omitted ``max_length`` still means 1022, an explicit NULL
survives, and in both cases the row's own recipe derives the row's own id.
"""

from __future__ import annotations

import pytest

from protea.core.embedding_identity import derive_embedding_config_id, recipe_of
from protea.infrastructure.orm.models.embedding.embedding_config import (
    _DEFAULT_MAX_LENGTH,
    EmbeddingConfig,
    _derive_id_from_recipe,
    _was_not_set,
)

RECIPE = {
    "model_name": "ElnaggarLab/ankh-base",
    "model_backend": "ankh",
    "layer_indices": [0],
    "layer_agg": "mean",
    "pooling": "mean_max",
    "normalize": False,
    "normalize_residues": False,
    "use_chunking": False,
    "chunk_size": 512,
    "chunk_overlap": 0,
    "embedding_scale": 1.0,
}


def _insert(**over: object) -> EmbeddingConfig:
    """A config as the listener leaves it, without touching a database."""
    config = EmbeddingConfig(**{**RECIPE, **over})
    _derive_id_from_recipe(None, None, config)
    return config


class TestTheRowHoldsWhatItsIdDescribes:
    def test_an_explicit_null_survives_and_matches_its_id(self) -> None:
        config = _insert(max_length=None)
        assert config.max_length is None
        assert config.id == derive_embedding_config_id(recipe_of(config))

    def test_an_omitted_max_length_still_means_1022(self) -> None:
        # Every id minted before this change was derived with 1022 in the
        # recipe, so this is what keeps them valid.
        config = _insert()
        assert config.max_length == _DEFAULT_MAX_LENGTH
        assert config.id == derive_embedding_config_id(recipe_of(config))

    def test_an_explicit_length_matches_its_id(self) -> None:
        config = _insert(max_length=512)
        assert config.max_length == 512
        assert config.id == derive_embedding_config_id(recipe_of(config))

    def test_the_two_are_different_ids(self) -> None:
        # The whole point: full length and 1022 are different recipes, and
        # before this they could share a row.
        assert _insert(max_length=None).id != _insert().id

    def test_the_lab_full_length_encoding_keeps_the_id_we_published(self) -> None:
        # Told to the other machine and written into its handoff notes, so it
        # is pinned here rather than remembered.
        config = _insert(max_length=None, normalize=False, pooling="mean_max")
        assert str(config.id) == "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0"


class TestAbsentIsNotTheSameAsNull:
    def test_an_omitted_field_reports_as_not_set(self) -> None:
        assert _was_not_set(EmbeddingConfig(**RECIPE), "max_length") is True

    def test_an_explicit_none_reports_as_set(self) -> None:
        # The distinction the column default could not make, and the reason
        # the resolution moved into the listener.
        assert _was_not_set(EmbeddingConfig(**RECIPE, max_length=None), "max_length") is False

    def test_an_explicit_value_reports_as_set(self) -> None:
        assert _was_not_set(EmbeddingConfig(**RECIPE, max_length=1022), "max_length") is False

    def test_the_column_carries_no_python_default(self) -> None:
        # A default here would be applied AFTER the id is hashed, which is
        # the defect. The listener owns the resolution now.
        assert EmbeddingConfig.__table__.c.max_length.default is None
        assert EmbeddingConfig.__table__.c.max_length.nullable is True


class TestAnExplicitIdIsStillHonoured:
    def test_a_repair_script_can_place_a_row_deliberately(self) -> None:
        import uuid

        chosen = uuid.uuid4()
        config = EmbeddingConfig(**RECIPE, max_length=None, id=chosen)
        _derive_id_from_recipe(None, None, config)
        assert config.id == chosen

    @pytest.mark.parametrize("length", [None, 1022, 512])
    def test_the_fill_happens_even_when_the_id_is_given(self, length: int | None) -> None:
        # Otherwise a deliberately placed row could still end up with a
        # max_length nobody wrote.
        import uuid

        config = EmbeddingConfig(**RECIPE, id=uuid.uuid4()) if length is None else None
        if config is None:
            return
        _derive_id_from_recipe(None, None, config)
        assert config.max_length == _DEFAULT_MAX_LENGTH
