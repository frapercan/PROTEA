"""Encoding a corpus with a residue-level sparse encoder.

The defect this operation exists to close is not a crash. ``apply_learned_encoder``
runs happily on any map of the right shape, and a map fitted for per-residue use
would produce, from pooled vectors, a complete and plausible set of codes computed
by the wrong mechanism. So the tests that matter are about ORDER (that selection
happens before pooling and that this is not a distinction without a difference) and
about REFUSAL (that an artifact which does not declare its recipe cannot run).
"""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations.encode_residue_sparse import (
    AGGREGATES,
    REQUIRED_META,
    TARGET_BACKEND,
    EncodeResidueSparseOperation,
    EncodeResidueSparsePayload,
    code_density,
    encode_one,
    load_frozen_encoder,
    reduce_residues,
    topk_real,
)


def _artifact(tmp_path, **overrides):
    meta = {
        "k_residue": 4,
        "k_sequence": 8,
        "dict_dim": 16,
        "in_dim": 3,
        "layer_indices": [-1],
        "aggregate": "mean",
        "order": "select-then-pool",
    }
    meta.update(overrides)
    path = tmp_path / "encoder.npz"
    np.savez(
        path,
        W=np.ones((meta["in_dim"], meta["dict_dim"]), np.float32),
        b=np.zeros(meta["dict_dim"], np.float32),
        **meta,
    )
    return str(path)


# --------------------------------------------------------------------------- the order


def test_selection_happens_before_pooling():
    """The whole reason this operation exists rather than reusing the pooled path."""
    residues = np.array([[10.0, 0.0], [0.0, 1.0], [0.0, 1.0]], np.float32)
    weight = np.eye(2, dtype=np.float32)
    bias = np.zeros(2, np.float32)

    got = encode_one(residues, weight, bias, k_residue=1, k_sequence=2)

    # Each residue contributes only its own strongest atom, so the intense first
    # residue keeps atom 0 at a third of its magnitude rather than being averaged
    # against two residues that never activated it.
    assert got[0] == pytest.approx(10.0 / 3)
    assert got[1] == pytest.approx(2.0 / 3)


def test_the_two_orders_genuinely_disagree():
    """If they agreed, running the language model again would buy nothing.

    Five residues where atom 0 wins narrowly and one where atom 1 is intense.
    Pooling first lets atom 1 bank the five second places it never won and take the
    protein; selecting per residue never admits a second place at all, so atom 0
    keeps it. Which is right is the empirical question; that they differ is what
    makes the question exist.
    """
    residues = np.array([[1.0, 0.9]] * 5 + [[0.0, 4.0]], np.float32)
    weight = np.eye(2, dtype=np.float32)
    bias = np.zeros(2, np.float32)

    per_residue = encode_one(residues, weight, bias, k_residue=1, k_sequence=1)
    pooled_first = topk_real(residues.mean(axis=0)[None, :], 1)[0]

    assert np.argmax(np.abs(per_residue)) == 0
    assert np.argmax(np.abs(pooled_first)) == 1


def test_a_protein_of_one_residue_is_just_its_own_code():
    residues = np.array([[1.0, 2.0, 3.0]], np.float32)
    weight = np.eye(3, dtype=np.float32)

    got = encode_one(residues, weight, np.zeros(3, np.float32), k_residue=2, k_sequence=3)

    assert got.tolist() == [0.0, 2.0, 3.0]


def test_top_k_keeps_the_largest_magnitude_including_negatives():
    """Magnitude, not value: a strongly negative atom carries as much as a positive one."""
    got = topk_real(np.array([[-5.0, 1.0, 2.0]], np.float32), 1)

    assert got.tolist() == [[-5.0, 0.0, 0.0]]


def test_asking_for_more_atoms_than_exist_returns_the_input():
    x = np.array([[1.0, 2.0]], np.float32)

    assert topk_real(x, 5).tolist() == x.tolist()


# --------------------------------------------------------------------------- the refusal


@pytest.mark.parametrize("missing", REQUIRED_META)
def test_an_artifact_that_does_not_declare_its_recipe_is_refused(tmp_path, missing):
    """A pooled-encoder artifact has the same tensor shapes, so only the declaration
    separates them and defaulting it would hide the mistake behind valid output."""
    meta = {
        "k_residue": 4,
        "k_sequence": 8,
        "dict_dim": 16,
        "in_dim": 3,
        "layer_indices": [-1],
        "aggregate": "mean",
        "order": "select-then-pool",
    }
    del meta[missing]
    path = tmp_path / "bad.npz"
    np.savez(path, W=np.ones((3, 16), np.float32), b=np.zeros(16, np.float32), **meta)

    with pytest.raises(ValueError, match=missing):
        load_frozen_encoder(str(path))


def test_a_map_whose_shape_contradicts_its_recipe_is_refused(tmp_path):
    path = tmp_path / "bad.npz"
    np.savez(
        path,
        W=np.ones((5, 16), np.float32),
        b=np.zeros(16, np.float32),
        k_residue=4,
        k_sequence=8,
        dict_dim=16,
        in_dim=3,
        layer_indices=[-1],
    )

    with pytest.raises(ValueError, match="declares"):
        load_frozen_encoder(str(path))


def test_a_selection_that_takes_the_whole_dictionary_is_refused(tmp_path):
    """Not a sparse code, and it would silently cost the dictionary's whole width."""
    with pytest.raises(ValueError, match="not a sparse code"):
        load_frozen_encoder(_artifact(tmp_path, k_residue=16))


def test_a_declared_recipe_loads_with_its_numbers(tmp_path):
    weight, bias, meta = load_frozen_encoder(_artifact(tmp_path))

    assert weight.shape == (3, 16)
    assert bias.shape == (16,)
    assert meta["k_residue"] == 4 and meta["k_sequence"] == 8
    assert meta["aggregate"] == "mean"


# --------------------------------------------------------------------------- the aggregate


def test_an_unknown_aggregate_is_refused(tmp_path):
    """The aggregate decides the code's WIDTH, so guessing it would halve or double a
    corpus without anything failing."""
    with pytest.raises(ValueError, match="aggregate"):
        load_frozen_encoder(_artifact(tmp_path, aggregate="median"))


def test_the_second_moment_doubles_the_width():
    """A code of two moments is twice a code of one, which is why the aggregate has to
    be declared rather than inferred from the weights: both have the same weights."""
    residues = np.array([[1.0, 0.0], [0.0, 1.0]], np.float32)
    weight, bias = np.eye(2, dtype=np.float32), np.zeros(2, np.float32)

    mean = reduce_residues(residues, "mean")
    both = reduce_residues(residues, "moments")

    assert mean.size == 2
    assert both.size == 4


def test_the_dispersion_separates_two_proteins_the_mean_cannot():
    """The whole reason to carry it. Both have the same mean for the atom and arrive
    at it in opposite ways: spread thinly, or concentrated in one place."""
    even = np.tile(np.array([[0.5, 0.0]], np.float32), (4, 1))
    spiky = np.array([[2.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]], np.float32)

    a, b = reduce_residues(even, "moments"), reduce_residues(spiky, "moments")

    assert a[0] == pytest.approx(b[0])  # la media no las distingue
    assert b[2] > a[2]  # la dispersión sí


def test_every_declared_aggregate_is_implemented():
    """A name in the list that the reducer does not handle would fail at corpus scale
    and pass every test that never names it."""
    residues = np.array([[1.0, 2.0], [3.0, 4.0]], np.float32)

    for name in AGGREGATES:
        assert reduce_residues(residues, name).size in (2, 4)


# --------------------------------------------------------------------------- reporting


def test_density_is_the_share_of_atoms_used():
    assert code_density(np.array([1.0, 0.0, 0.0, 2.0])) == pytest.approx(0.5)


def test_an_empty_code_has_no_density_rather_than_dividing_by_zero():
    assert code_density(np.array([])) == 0.0


# --------------------------------------------------------------------------- the operation


def test_the_payload_refuses_an_empty_artifact_path():
    with pytest.raises(ValueError, match="non-empty"):
        EncodeResidueSparsePayload(source_embedding_config_id="c", encoder_artifact_path="  ")


def test_the_payload_refuses_a_batch_of_zero():
    with pytest.raises(ValueError):
        EncodeResidueSparsePayload(
            source_embedding_config_id="c", encoder_artifact_path="e.npz", batch_size=0
        )


def test_the_description_says_it_runs_the_model_and_why():
    op = EncodeResidueSparseOperation()

    assert "commute" in op.description
    assert "Trains nothing" in op.description


def test_the_backend_tag_separates_these_codes_from_pooled_ones():
    assert TARGET_BACKEND == "residue-sparse"


def test_summarize_names_the_source_config():
    op = EncodeResidueSparseOperation()

    assert "cfg-1" in op.summarize_payload({"source_embedding_config_id": "cfg-1"})


def test_it_is_registered_so_it_can_be_dispatched():
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()

    assert registry.get("encode_residue_sparse").name == "encode_residue_sparse"


# ---------------------------------------------------------------------------
# The order the map was fitted for
# ---------------------------------------------------------------------------
#
# Two pipelines produce a code from the same weights. select-then-pool maps
# each residue, keeps k_residue atoms of each, aggregates, keeps k_sequence.
# pool-then-select aggregates first and maps the result.
#
# Nothing about the weights distinguishes them. The map is affine, so
# mean(X @ W + b) equals mean(X) @ W + b to within 7e-07, and an artifact
# fitted either way has identical shapes and passes every check on them.
# Served under the wrong order, a control arm produced a code sharing 130 of
# 2048 atoms with the intended one at cosine 0.10. Not a degradation: a
# different encoder.


def test_an_artifact_that_does_not_declare_its_order_is_refused(tmp_path):
    path = _artifact(tmp_path)
    data = dict(np.load(path, allow_pickle=True))
    del data["order"]
    np.savez(path, **data)
    with pytest.raises(ValueError, match="order"):
        load_frozen_encoder(str(path))


def test_an_unknown_order_is_refused_rather_than_guessed(tmp_path):
    path = _artifact(tmp_path, order="whatever")
    with pytest.raises(ValueError, match="cannot be recovered from the"):
        load_frozen_encoder(str(path))


def test_the_unimplemented_order_is_refused_rather_than_run_as_the_other(tmp_path):
    # The dangerous case. Running it as select-then-pool would succeed,
    # produce a complete code, and be wrong in a way nothing downstream
    # could see.
    path = _artifact(tmp_path, order="pool-then-select")
    with pytest.raises(ValueError, match="does not implement"):
        load_frozen_encoder(str(path))


def test_the_declared_order_survives_into_the_recipe(tmp_path):
    _, _, meta = load_frozen_encoder(str(_artifact(tmp_path)))
    assert meta["order"] == "select-then-pool"


def test_the_whole_dictionary_refusal_now_explains_the_hole(tmp_path):
    # k_residue = dict_dim expresses pooling EXACTLY, since the map is
    # affine. So the one value that would have made a pooled control work
    # is the one value this check has always refused, and the order field
    # is what closes that.
    path = _artifact(tmp_path, k_residue=16)
    with pytest.raises(ValueError, match="pool-then-select"):
        load_frozen_encoder(str(path))
