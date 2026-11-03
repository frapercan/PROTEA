"""The platform's residue vocabulary against the lab's reference encoder (ADR-D51).

The fixture was produced by the lab's reference encoder (``vectores_prueba_vocabulario.py``
in the sf-jepa repository), so these tests check two implementations against each other:
every comparison is exact, because the contract is a code, not an approximation of one.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pytest

torch = pytest.importorskip("torch")

from protea.core.residue_vocabulary import (  # noqa: E402
    REQUIRED_ARRAYS,
    ResidueCode,
    ResidueVocabulary,
    load_residue_vocabulary,
)

FIXTURE = Path(__file__).parent / "fixtures" / "residue_vocabulary_vectors.npz"


@pytest.fixture(scope="module")
def vectors():
    with np.load(FIXTURE, allow_pickle=False) as data:
        return {k: np.asarray(data[k]) for k in data.files}


def _vocabulary(vectors, **changes):
    meta = json.loads(str(vectors["meta_json"]))
    arrays = {k: vectors[k] for k in REQUIRED_ARRAYS}
    arrays.update(changes.pop("arrays", {}))
    meta.update(changes)
    return ResidueVocabulary(arrays, meta)


def _sequences(vectors):
    """(start, end) residue spans of the fixture's sequences."""
    ends = np.cumsum(vectors["lengths"])
    return list(zip(np.concatenate([[0], ends[:-1]]), ends, strict=True))


def _layers(states):
    return [states[:, i] for i in range(states.shape[1])]


def test_standardised_inputs_are_the_reference_inputs(vectors):
    voc = _vocabulary(vectors)
    for a, b in _sequences(vectors):
        x = voc.standardise(_layers(vectors["states"][a:b]))
        assert torch.equal(x, torch.from_numpy(vectors["x"][a:b]))


def test_symbols_are_the_reference_symbols(vectors):
    voc = _vocabulary(vectors)
    for a, b in _sequences(vectors):
        s = voc.symbols(torch.from_numpy(vectors["x"][a:b]))
        assert torch.equal(s.to(torch.int16), torch.from_numpy(vectors["symbols"][a:b]))


def test_codes_are_the_reference_codes(vectors):
    voc = _vocabulary(vectors)
    entry = 0
    offsets_at = 0
    for a, b in _sequences(vectors):
        code = voc.encode_states(_layers(vectors["states"][a:b]))
        ref_offsets = vectors["code_offsets"][offsets_at : offsets_at + (b - a) + 1]
        offsets_at += (b - a) + 1
        n = int(ref_offsets[-1])
        assert np.array_equal(code.offsets, ref_offsets)
        assert np.array_equal(code.features, vectors["code_features"][entry : entry + n])
        assert np.array_equal(code.levels, vectors["code_levels"][entry : entry + n])
        entry += n
    assert entry == vectors["code_features"].shape[0]


def test_read_back_is_the_reference_read_back(vectors):
    voc = _vocabulary(vectors)
    for a, b in _sequences(vectors):
        code = voc.encode(torch.from_numpy(vectors["x"][a:b]))
        assert np.array_equal(voc.read_back(code), vectors["values"][a:b])
        assert np.array_equal(code.symbols(voc.dict_dim), vectors["symbols"][a:b])


def test_a_feature_is_active_only_strictly_above_its_threshold(vectors):
    voc = _vocabulary(vectors)
    thr = voc._threshold
    p = torch.stack(
        [
            thr,
            torch.nextafter(thr, torch.full_like(thr, math.inf)),
            torch.nextafter(thr, torch.zeros_like(thr)),
            -thr,
        ]
    )
    s = voc.symbols_of(p)
    assert not (s[0] > 0).any()
    assert (s[1] > 0).all()
    assert not (s[2:] > 0).any()


def test_levels_follow_the_rule_at_their_edges(vectors):
    dict_dim = int(json.loads(str(vectors["meta_json"]))["dict_dim"])
    rng = np.full(dict_dim, 2.0, dtype=np.float32)
    low = np.full(dict_dim, math.log(1e-3), dtype=np.float32)
    voc = _vocabulary(vectors, arrays={"level_range": rng, "log_threshold": low})
    # p / 2 * 256 = 128 p: level floor(128 p) clamped to 0..255, symbol = level + 1
    p_values = [1e-2, 1 / 128, 1 / 128 - 1e-6, 1.999, 2.0, 50.0, 1e-4, -1.0]
    expected = [2, 2, 1, 256, 256, 256, 0, 0]
    p = torch.tensor(p_values)[:, None].expand(-1, dict_dim).contiguous()
    assert voc.symbols_of(p).tolist() == [[e] * dict_dim for e in expected]


def test_the_layout_of_the_stored_weights_does_not_change_the_code(vectors):
    strided = np.asfortranarray(vectors["encoder_weight_t"])
    voc_c = _vocabulary(vectors)
    voc_f = _vocabulary(vectors, arrays={"encoder_weight_t": strided})
    x = torch.from_numpy(vectors["x"])
    assert torch.equal(voc_c.preactivation(x), voc_f.preactivation(x))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"kind": "residue-sparse"}, "kind"),
        ({"arrays": {"level_means": None}}, "inactive"),
        ({"arrays": {"encoder_bias": np.zeros(3, np.float32)}}, "shape"),
        ({"levels": 512}, "shape"),
    ],
)
def test_an_artifact_that_does_not_describe_a_vocabulary_is_refused(vectors, change, message):
    if change.get("arrays", {}).get("level_means", 1) is None:
        means = vectors["level_means"].copy()
        means[3, 0] = 0.1
        change = {"arrays": {"level_means": means}}
    with pytest.raises(ValueError, match=message):
        _vocabulary(vectors, **change)


def test_missing_meta_is_refused(vectors):
    meta = json.loads(str(vectors["meta_json"]))
    del meta["training_release"]
    with pytest.raises(ValueError, match="training_release"):
        ResidueVocabulary({k: vectors[k] for k in REQUIRED_ARRAYS}, meta)


def test_the_artifact_loads_from_an_npz_without_pickle(vectors, tmp_path):
    path = tmp_path / "vocabulary.npz"
    np.savez(path, meta_json=vectors["meta_json"], **{k: vectors[k] for k in REQUIRED_ARRAYS})
    voc = load_residue_vocabulary(str(path))
    x = torch.from_numpy(vectors["x"])
    assert torch.equal(voc.symbols(x).to(torch.int16), torch.from_numpy(vectors["symbols"]))


def test_an_input_of_the_wrong_width_is_refused(vectors):
    voc = _vocabulary(vectors)
    with pytest.raises(ValueError, match="reads"):
        voc.preactivation(torch.zeros(3, voc.in_dim + 1))


def test_the_dense_symbols_of_an_empty_code_are_empty():
    code = ResidueCode(
        features=np.zeros(0, np.int16),
        levels=np.zeros(0, np.uint8),
        offsets=np.zeros(3, np.int64),
    )
    assert code.residues == 2 and code.entries == 0
    assert not code.symbols(5).any()


def test_chunking_the_elementwise_steps_changes_no_code(vectors, monkeypatch):
    import protea.core.residue_vocabulary as rv

    voc = _vocabulary(vectors)
    x = torch.from_numpy(vectors["x"])
    whole = voc.encode(x)
    monkeypatch.setattr(rv, "CHUNK_ROWS", 7)
    chunked = voc.encode(x)
    assert np.array_equal(whole.features, chunked.features)
    assert np.array_equal(whole.levels, chunked.levels)
    assert np.array_equal(whole.offsets, chunked.offsets)
    assert torch.equal(voc.symbols(x), torch.from_numpy(vectors["symbols"]))


def test_a_sequence_without_residues_has_an_empty_code(vectors):
    voc = _vocabulary(vectors)
    code = voc.encode(torch.zeros(0, voc.in_dim))
    assert code.residues == 0 and code.entries == 0
