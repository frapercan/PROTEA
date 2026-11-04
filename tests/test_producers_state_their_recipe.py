"""Every identity field is decided before the hash, so the id describes its row.

Measured 2026-11-04 on the live registry, rederiving each config id from the
row it names: three configs made by producers did not reconcile.

    live id    producer                    the omission that reproduces it
    22166300   encode_residue_vocabulary   chunk_size, chunk_overlap, embedding_scale
    76792dc2   apply_learned_encoder       chunk_size, chunk_overlap, embedding_scale
    9d72dc44   apply_learned_encoder       chunk_size, chunk_overlap, embedding_scale

The producers left those three out. The ``before_insert`` listener hashed them
as None, and the INSERT then stored their column defaults 512, 0 and 1.0. The
listener already resolved ``max_length`` for exactly this reason, and only
``max_length``.

These pin the cause, the listener that now resolves every omitted identity
field before hashing, the producers that now state their recipes in full, and
the ids a repair moves the three rows onto.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from protea.core.embedding_identity import IDENTITY_FIELDS, derive_embedding_config_id
from protea.core.operations import apply_learned_encoder as learned
from protea.core.operations import encode_residue_sparse as sparse
from protea.core.operations import encode_residue_vocabulary as vocabulary
from protea.infrastructure.orm.models.embedding.embedding_config import (
    OMISSION_MEANS,
    UNCHUNKED,
    EmbeddingConfig,
    _derive_id_from_recipe,
    _was_not_set,
)

CUT = uuid.UUID("00000000-0000-4000-8000-000000000211")
FULL_LENGTH_ANKH = uuid.UUID("f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0")
ANKH_BASE_MEAN = uuid.UUID("e7ba37da-c18d-5261-b5b4-49a3093d9d4d")

#: The recipes the three rows hold today, as read from the live registry.
VOCABULARY_ROW: dict[str, Any] = {
    "model_name": "residue-vocabulary:06fed75ca76d17be:f3a7fe16",
    "model_backend": "residue-vocabulary",
    "layer_indices": [46, 38, 26, 8, 0],
    "layer_agg": "concat",
    "pooling": "per-residue",
    "normalize_residues": False,
    "normalize": False,
    "max_length": 1022,
    "embedding_scale": 1.0,
    **UNCHUNKED,
}


def _learned_row(cut: str) -> dict[str, Any]:
    return {
        "model_name": f"learned-code:mean:sfjepa-world-holes-topk:{cut}:e7ba37da",
        "model_backend": "learned-code",
        "layer_indices": [0],
        "layer_agg": "none",
        "pooling": "learned",
        "normalize_residues": False,
        "normalize": False,
        "max_length": 1022,
        "embedding_scale": 1.0,
        **UNCHUNKED,
    }


class TestTheCause:
    @pytest.mark.parametrize(
        ("row", "live"),
        [
            (VOCABULARY_ROW, "22166300-a178-599e-835c-afe1c007feb5"),
            (_learned_row("cut211"), "76792dc2-52fc-501f-9370-cf7a58e8336c"),
            (_learned_row("cut227"), "9d72dc44-7eed-517c-b45d-82b6e9f73bb1"),
        ],
    )
    def test_each_live_id_is_its_row_hashed_with_three_fields_as_none(
        self, row: dict[str, Any], live: str
    ) -> None:
        hashed = {**row, "chunk_size": None, "chunk_overlap": None, "embedding_scale": None}
        assert str(derive_embedding_config_id(hashed)) == live
        assert str(derive_embedding_config_id(row)) != live


class TestTheListenerResolvesEveryOmission:
    @pytest.mark.parametrize("field", sorted(OMISSION_MEANS))
    def test_an_omitted_field_is_stored_and_hashed_as_the_same_value(self, field: str) -> None:
        stated = {**_learned_row("cut211"), field: OMISSION_MEANS[field]}
        omitted = {k: v for k, v in stated.items() if k != field}
        config = EmbeddingConfig(**omitted)
        _derive_id_from_recipe(None, None, config)
        assert getattr(config, field) == OMISSION_MEANS[field]
        assert config.id == derive_embedding_config_id(stated)

    def test_nothing_is_left_for_the_insert_to_fill(self) -> None:
        # The defect in one line: whatever the INSERT fills is a value the
        # hash never saw.
        config = EmbeddingConfig(model_name="m", layer_indices=[0])
        _derive_id_from_recipe(None, None, config)
        assert [f for f in IDENTITY_FIELDS if _was_not_set(config, f)] == []

    @pytest.mark.parametrize("field", IDENTITY_FIELDS)
    def test_every_identity_field_is_either_resolved_or_required(self, field: str) -> None:
        # A new identity field with a default this listener cannot read
        # (a callable, or a server default alone) would reopen the defect.
        column = EmbeddingConfig.__table__.c[field]
        if field in OMISSION_MEANS:
            assert column.default is None or column.default.is_scalar
        else:
            assert column.default is None and column.server_default is None
            assert column.nullable is False

    def test_the_unchunked_fields_are_the_column_defaults(self) -> None:
        assert {k: OMISSION_MEANS[k] for k in UNCHUNKED} == UNCHUNKED


class _Session:
    """Just enough session for a producer's ``_ensure_target_config``.

    No row carries the name yet, ``get`` returns the source config, and
    ``flush`` runs the listener the INSERT would run. ``unstated`` records the
    identity fields each added config left to the listener.
    """

    def __init__(self, source: Any = None) -> None:
        self.source = source
        self.added: list[EmbeddingConfig] = []
        self.unstated: list[list[str]] = []

    def execute(self, _statement: Any) -> SimpleNamespace:
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    def get(self, _model: Any, _ident: Any) -> Any:
        return self.source

    def add(self, config: EmbeddingConfig) -> None:
        self.unstated.append([f for f in IDENTITY_FIELDS if _was_not_set(config, f)])
        self.added.append(config)

    def flush(self) -> None:
        for config in self.added:
            _derive_id_from_recipe(None, None, config)


@pytest.fixture(autouse=True)
def _any_cut(monkeypatch: pytest.MonkeyPatch) -> None:
    for module in (vocabulary, learned, sparse):
        monkeypatch.setattr(module, "resolve_training_cut", lambda _s, _m: CUT)


def _vocabulary_config() -> tuple[_Session, uuid.UUID]:
    session = _Session()
    source = SimpleNamespace(id=FULL_LENGTH_ANKH, model_name="ElnaggarLab/ankh-base")
    meta = {
        "layer_indices": [46, 38, 26, 8, 0],
        "hidden_state_indices": [2, 10, 22, 40, 48],
        "dict_dim": 128,
        "actives": 4,
        "levels": 4,
        "training_release": "211",
    }
    target = vocabulary._ensure_target_config(session, meta, source, "06fed75ca76d17be" + "0" * 48)
    return session, target


def _learned_config(release: str) -> tuple[_Session, uuid.UUID]:
    session = _Session(source=SimpleNamespace(id=ANKH_BASE_MEAN, max_length=1022))
    p = SimpleNamespace(
        target_model_name="learned-code", source_embedding_config_id=str(ANKH_BASE_MEAN)
    )
    meta = {
        "pooling": "mean",
        "objective": "sfjepa-world-holes-topk",
        "training_release": release,
        "top_k": 64,
        "dict_dim": 8192,
    }
    return session, learned._ensure_target_config(session, p, meta)


class TestTheProducersStateTheirRecipe:
    def test_the_vocabulary_states_every_identity_field(self) -> None:
        session, _ = _vocabulary_config()
        assert session.unstated == [[]]

    def test_the_vocabulary_says_it_does_not_truncate(self) -> None:
        session, _ = _vocabulary_config()
        assert session.added[0].max_length is None

    def test_the_vocabulary_lands_on_the_repair_target(self) -> None:
        # The id the repair of 22166300 moves its rows onto. Its row holds
        # 1022, which the pass never applied, so the repair corrects
        # max_length to NULL as well as the id.
        _, target = _vocabulary_config()
        assert str(target) == "43193409-de70-526b-ae8d-43892a744049"

    @pytest.mark.parametrize(
        ("release", "repair_target"),
        [
            ("211", "34446081-a1c0-5a73-bf8e-04da532839f6"),
            ("227", "d4b4c057-762c-5e24-b4a1-6a542490275e"),
        ],
    )
    def test_the_learned_code_lands_on_its_own_row(self, release: str, repair_target: str) -> None:
        # Its row's recipe is the right one, so the target is what that row
        # derives: 76792dc2 and 9d72dc44 move onto these two.
        session, target = _learned_config(release)
        assert session.unstated == [[]]
        assert str(target) == repair_target
        assert target == derive_embedding_config_id(_learned_row(f"cut{release}"))

    def test_the_learned_code_inherits_its_source_truncation(self) -> None:
        session = _Session(source=SimpleNamespace(id=FULL_LENGTH_ANKH, max_length=None))
        p = SimpleNamespace(
            target_model_name="learned-code", source_embedding_config_id=str(FULL_LENGTH_ANKH)
        )
        meta = {"pooling": "mean", "training_release": "211", "top_k": 64, "dict_dim": 8192}
        learned._ensure_target_config(session, p, meta)
        assert session.added[0].max_length is None

    def test_the_learned_code_refuses_a_source_that_does_not_exist(self) -> None:
        p = SimpleNamespace(
            target_model_name="learned-code", source_embedding_config_id=str(uuid.uuid4())
        )
        meta = {"pooling": "mean", "training_release": "211", "top_k": 64, "dict_dim": 8192}
        with pytest.raises(ValueError, match="does not exist"):
            learned._ensure_target_config(_Session(source=None), p, meta)

    def test_the_sparse_residue_code_states_every_identity_field(self) -> None:
        session = _Session()
        source = SimpleNamespace(
            id=FULL_LENGTH_ANKH, model_name="ElnaggarLab/ankh-base", **UNCHUNKED
        )
        p = SimpleNamespace(
            target_model_name="residue-sparse", source_embedding_config_id=str(FULL_LENGTH_ANKH)
        )
        meta = {"k_residue": 8, "dict_dim": 4096, "k_sequence": 64, "layer_indices": [0]}
        sparse._ensure_target_config(session, p, meta, source)
        assert session.unstated == [[]]
        assert session.added[0].max_length is None
