"""The id of a new EmbeddingConfig comes from its recipe, not from a dice roll.

``embedding_identity`` exists so that two machines building the same recipe land
on the same row without talking to each other. It was written to be "one
derivation with callers" and ended up with none: four call sites constructed an
``EmbeddingConfig`` and all four took the column's ``uuid.uuid4`` default. Five
of thirteen configs in the live registry carried ids nothing could reproduce --
including all three learned encoders, which are a whole node of axis A.

These pin the listener that closes it, and the property that makes it matter.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.embedding_identity import IDENTITY_FIELDS, derive_embedding_config_id
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

_RECIPE = {
    "model_name": "facebook/esm2_t33_650M_UR50D",
    "model_backend": "esm",
    "layer_indices": [11],
    "layer_agg": "mean",
    "pooling": "mean",
    "normalize_residues": False,
    "normalize": True,
    "use_chunking": False,
    "chunk_size": 512,
    "chunk_overlap": 0,
    "max_length": 1022,
    "embedding_scale": 1.0,
}


def _fire_before_insert(cfg: EmbeddingConfig) -> None:
    """Run the registered before_insert listeners without a database.

    The behaviour under test is the listener, not SQLAlchemy's flush machinery,
    so it is invoked directly rather than through a session that would need a
    live Postgres to reach the same line.
    """
    from protea.infrastructure.orm.models.embedding import embedding_config as mod

    mod._derive_id_from_recipe(None, None, cfg)


class TestTheIdComesFromTheRecipe:
    def test_a_new_config_gets_its_derived_id(self):
        cfg = EmbeddingConfig(**_RECIPE)
        assert cfg.id is None, "the column default must not have fired yet"
        _fire_before_insert(cfg)
        assert cfg.id == derive_embedding_config_id(cfg)

    def test_the_derived_id_is_a_v5(self):
        """A v4 in this table is the tell that an id was assigned, not derived."""
        cfg = EmbeddingConfig(**_RECIPE)
        _fire_before_insert(cfg)
        assert cfg.id.version == 5

    def test_two_builds_of_one_recipe_agree(self):
        """The property the campaign runs on: same recipe, same id, no talking."""
        a, b = EmbeddingConfig(**_RECIPE), EmbeddingConfig(**_RECIPE)
        _fire_before_insert(a)
        _fire_before_insert(b)
        assert a.id == b.id

    @pytest.mark.parametrize("field", ["layer_indices", "pooling", "normalize"])
    def test_changing_any_identity_field_changes_the_id(self, field):
        """A level that varies in a field must not share an id with one that does not."""
        other = dict(_RECIPE)
        other[field] = {"layer_indices": [12], "pooling": "cls", "normalize": False}[field]
        base, changed = EmbeddingConfig(**_RECIPE), EmbeddingConfig(**other)
        _fire_before_insert(base)
        _fire_before_insert(changed)
        assert base.id != changed.id

    def test_an_explicit_id_is_honoured(self):
        """A repair script places rows deliberately; the listener must not fight it."""
        chosen = uuid.uuid4()
        cfg = EmbeddingConfig(id=chosen, **_RECIPE)
        _fire_before_insert(cfg)
        assert cfg.id == chosen


class TestTheRecipeIsComplete:
    def test_every_identity_field_is_a_real_column(self):
        """A field named in IDENTITY_FIELDS but absent from the model would make
        ``recipe_of`` raise at insert time, on the first config nobody expected."""
        for field in IDENTITY_FIELDS:
            assert hasattr(EmbeddingConfig, field), field

    def test_display_name_is_not_identity(self):
        """Renaming the registry must not reissue every id and orphan its vectors."""
        for field in ("display_name", "family", "kind", "param_count", "description"):
            assert field not in IDENTITY_FIELDS
