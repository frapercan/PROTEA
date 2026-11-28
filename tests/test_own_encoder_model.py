"""The own segment encoder's model and identity (``protea.core.own_encoder``).

The model is the laboratory's segment arm, ported: the tokens, the hidden marks, the
batches and the loss follow its rules, a seed decides everything, and the weights
file has the same bytes for the same weights, so its sha256 names the configuration.
The comparison with the laboratory's own script is ``scripts/own_encoder_port_check.py``
(its helpers are tested here; the script itself needs the laboratory's file).
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import uuid
from pathlib import Path

import numpy as np
import pytest
import torch

from protea.core import own_encoder
from protea.core._evaluation_leakage import FITTED_BACKENDS
from protea.core.contracts.method import Family
from protea.core.embedding_identity import IDENTITY_FIELDS, derive_embedding_config_id
from protea.core.own_encoder import model as stage
from protea.infrastructure.orm.models.embedding.embedding_config import (
    OWN_ENCODER_BACKEND,
    VALID_BACKENDS,
)
from protea.services._embeddings_validation_helpers import validate_embedding_config_body
from protea.services.embeddings_service import InvalidEmbeddingConfigError

ROOT = Path(__file__).resolve().parents[1]


def _segments(n: int, seed: int = 0) -> stage.Segments:
    rng = np.random.default_rng(seed)
    lengths = rng.integers(1, 400, size=n)
    start = np.concatenate([[0], np.cumsum(lengths)[:-1]]).astype(np.int64)
    return start, start + lengths, rng.integers(0, stage.THEMES, size=n).astype(np.int64)


def _sha(model: stage.Model) -> str:
    return hashlib.sha256(stage.encoder_bytes(model.target)).hexdigest()


# ---------------------------------------------------------------- tokens and batches


def test_a_segment_length_falls_in_its_log2_bucket_the_last_one_open() -> None:
    got = stage.length_bucket(np.array([0, 1, 2, 3, 4, 1023, 1024, 10**9]))
    assert got.tolist() == [0, 0, 1, 1, 2, 9, 10, 15]


def test_an_item_hides_about_a_third_of_its_segments_and_never_none() -> None:
    rng = np.random.default_rng(1)
    for n, hidden in ((2, 1), (3, 1), (10, 3), (20, 6)):
        item = stage.segment_item(_segments(n), rng)
        assert int(item["hidden"].sum()) == hidden
        assert item["order"].tolist() == list(range(n))


def test_a_long_protein_trains_on_a_run_of_64_and_is_read_on_its_first_64() -> None:
    start, end, theme = _segments(100)
    trained = stage.segment_item((start, end, theme), np.random.default_rng(3))
    read = stage.segment_item((start, end, theme), np.random.default_rng(3), hide=False)
    assert len(trained["theme"]) == len(read["theme"]) == stage.MAX_SEGMENTS
    assert read["theme"].tolist() == theme[:64].tolist() and not read["hidden"].any()
    first = int(np.flatnonzero(theme == trained["theme"][0])[0])
    assert trained["theme"].tolist() == theme[first : first + 64].tolist()


def test_a_batch_is_padded_to_its_longest_item() -> None:
    rng = np.random.default_rng(2)
    batch = stage.collate([stage.segment_item(_segments(n), rng) for n in (2, 5)], "cpu")
    assert batch["theme"].shape == (2, 5) and batch["padding"].tolist()[0] == [0, 0, 1, 1, 1]
    assert not (batch["hidden"] & batch["padding"]).any()


def test_micro_batches_keep_their_padded_size_within_the_cap() -> None:
    rng = np.random.default_rng(4)
    items = [stage.segment_item(_segments(n), rng) for n in (2, 3, 7, 7, 9, 30)]
    groups = stage.micro_batches(items, 20)
    assert sum(len(g) for g in groups) == len(items)
    for group in groups:
        assert len(group) == 1 or len(group) * max(len(i["order"]) for i in group) <= 20


def test_a_draw_is_a_function_of_its_generator() -> None:
    corpus = [_segments(n, seed=n) for n in (1, 2, 3, 5, 8, 13, 70)]
    pool = np.arange(1, len(corpus))

    def drawn(seed: list[int]) -> list[list[int]]:
        groups = stage.draw(corpus.__getitem__, pool, np.random.default_rng(seed), 64)
        return [[int(t) for item in g for t in item["theme"]] for g in groups]

    assert drawn([0, 5]) == drawn([0, 5]) and drawn([0, 5]) != drawn([0, 6])


# ---------------------------------------------------------------- model and loss


def test_one_seed_builds_one_model_and_the_target_starts_as_the_encoder() -> None:
    first, again = stage.build(0), stage.build(0)
    assert _sha(first) == _sha(again)
    assert _sha(first) == hashlib.sha256(stage.encoder_bytes(first.encoder)).hexdigest()
    assert not any(p.requires_grad for p in first.target.parameters())
    assert _sha(stage.build(1)) != _sha(first)
    assert first.predictor.position.num_embeddings == stage.PREDICTOR_POSITIONS


def test_the_loss_reads_only_the_hidden_tokens_and_trains() -> None:
    model = stage.build(0)
    rng = np.random.default_rng(5)
    batch = stage.collate([stage.segment_item(_segments(n), rng) for n in (4, 9, 2)], "cpu")
    spread: list[float] = []
    loss = stage.masked_segment_loss(model, batch, spread)
    assert torch.isfinite(loss) and loss.item() > 0 and spread and spread[0] > 0
    optimizer = torch.optim.AdamW(model.trained_parameters(), lr=1e-3)
    for _ in range(3):
        optimizer.zero_grad()
        stage.masked_segment_loss(model, batch).backward()
        optimizer.step()
    assert stage.masked_segment_loss(model, batch).item() < loss.item()


# ---------------------------------------------------------------- the weights file


def test_the_same_weights_write_the_same_bytes_and_read_back_the_same_encoder() -> None:
    model = stage.build(7)
    blob = stage.encoder_bytes(model.target)
    assert blob == stage.encoder_bytes(model.target)
    back = stage.encoder_from_bytes(blob)
    batch = stage.collate(
        [stage.segment_item(_segments(6), np.random.default_rng(0), False)], "cpu"
    )
    model.target.eval()
    back.eval()
    with torch.no_grad():
        assert torch.equal(model.target(batch), back(batch))
    with torch.no_grad():
        model.target.norm.bias.add_(1e-3)
    assert stage.encoder_bytes(model.target) != blob


def test_a_weights_file_missing_a_tensor_is_refused() -> None:
    import io
    import zipfile

    blob = stage.encoder_bytes(stage.build(0).target)
    sink = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(blob)) as source, zipfile.ZipFile(sink, "w") as kept:
        for info in source.infolist()[1:]:
            kept.writestr(info, source.read(info))
    with pytest.raises(RuntimeError, match="Missing key"):
        stage.encoder_from_bytes(sink.getvalue())


# ---------------------------------------------------------------- the configuration


def test_two_weight_files_give_two_configuration_ids() -> None:
    first, second = _sha(stage.build(0)), _sha(stage.build(1))
    assert own_encoder.model_name(first) == f"jepa-segments/{first}"
    assert own_encoder.configuration_id(first) != own_encoder.configuration_id(second)
    assert own_encoder.configuration_id(first) == own_encoder.configuration_id(first)
    assert own_encoder.configuration_id(first).version == 5


def test_the_recipe_states_every_identity_field() -> None:
    recipe = own_encoder.configuration_recipe("a" * 64)
    assert set(recipe) == set(IDENTITY_FIELDS)
    assert recipe["model_backend"] == OWN_ENCODER_BACKEND == "own-encoder"
    assert recipe["max_length"] is None and recipe["normalize"] is False
    with pytest.raises(ValueError, match="not a sha256"):
        own_encoder.model_name("jepa")


def test_the_backend_is_a_producer_the_api_accepts_and_the_guard_does_not_date() -> None:
    assert OWN_ENCODER_BACKEND in VALID_BACKENDS and OWN_ENCODER_BACKEND not in FITTED_BACKENDS
    assert own_encoder.FAMILY == Family.WORLD_MODEL.value


def _training_record() -> dict:
    return {
        "members": {"md5s": 2_048_865, "sha256": "b" * 64},
        "lexicon": {"name": "lexico-93k"},
        "plan": {"plan_minutes": 55.0, "max_segments": 64},
    }


def test_the_create_body_is_accepted_and_derives_the_configuration_id() -> None:
    sha = "c" * 64
    body = own_encoder.configuration(sha, _training_record())
    validated = validate_embedding_config_body(body)
    assert derive_embedding_config_id(validated) == own_encoder.configuration_id(sha)
    assert validated["kind"] == "pretrained" and validated["trained_on_annotation_set_id"] is None
    assert validated["family"] == "jepa-segments"
    assert "2,048,865" in body["description"] and "world-model" in body["description"]


def test_a_training_release_on_the_encoder_is_refused() -> None:
    body = own_encoder.configuration("d" * 64, _training_record())
    body["trained_on_annotation_set_id"] = str(uuid.uuid4())
    with pytest.raises(InvalidEmbeddingConfigError, match="was FITTED"):
        validate_embedding_config_body(body)


# ---------------------------------------------------------------- the port check's helpers


def _port_check():
    spec = importlib.util.spec_from_file_location(
        "own_encoder_port_check", ROOT / "scripts" / "own_encoder_port_check.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_port_check_maps_the_laboratory_names_and_finds_a_gap() -> None:
    check = _port_check()
    assert check.stage_name("len.weight") == "length.weight"
    assert check.stage_name("ord.weight") == "order.weight"
    assert check.stage_name("pos.weight") == "position.weight"
    assert check.stage_name("body.layers.0.linear1.weight") == "body.layers.0.linear1.weight"
    state = stage.build(0).encoder.state_dict()
    lab = {("len." + k[7:] if k.startswith("length.") else k): v for k, v in state.items()}
    lab = {("ord." + k[6:] if k.startswith("order.") else k): v for k, v in lab.items()}
    same = check.compare_states(lab, state)
    assert same["same_names"] and same["max_abs_difference"] == 0.0
    moved = {k: (v + 1e-3 if k == "norm.bias" else v) for k, v in lab.items()}
    assert check.compare_states(moved, state)["max_abs_difference"] == pytest.approx(1e-3)
    assert not check.compare_states({"len.weight": state["length.weight"]}, state)["same_names"]


@pytest.mark.skipif(
    not os.environ.get("PROTEA_LAB_SEGMENT_ARM"),
    reason="the laboratory's jepa_lexico.py is not in this repository",
)
def test_the_port_agrees_with_the_laboratory_script() -> None:
    record = _port_check().check(Path(os.environ["PROTEA_LAB_SEGMENT_ARM"]), seed=0)
    assert record["agree"], record
