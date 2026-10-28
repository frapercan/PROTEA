"""The pass is the cost on this path, and it used to be paid once per head.

A residue-level encoder is a matmul over the residue tensor. Producing that
tensor is a full-length forward pass of the PLM, which on this corpus is
299.373.587 residues with nothing truncated by design, and
``encode_residue_sparse`` accepted exactly one artifact. Five heads over the
same states -- Light Attention and its two variants, C-REC and C-JEPA -- were
therefore five passes of the model and one arm's worth of arithmetic each.

``_embedding_pass_group`` had already drawn this line one level up for pooled
embeddings. This is the same split where it costs more: there the pass is
expensive and the reduction trivial; here the whole protein enters the model.

TWO REFUSALS THAT NOTHING REFUSED BEFORE, both found by reading:

* the LAYERS come from the artifact's meta and not from the source config, so
  two heads asking for different layers select different hidden states and
  cannot share a pass;
* ``in_dim`` was checked by an unguarded matmul inside ``encode_one``, on the
  card worker, after the model had loaded, once per batch. Alone that wastes a
  load; in a group it takes every other head's pass with it.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.operations._residue_pass_group import (
    PASS_FIELDS,
    REDUCTION_FIELDS,
    SharedResiduePassError,
    assert_in_dim_matches_source,
    assert_shared_residue_pass,
)
from protea.core.operations.encode_residue_sparse import EncodeResidueSparsePayload


def _meta(**over) -> dict:
    base = {
        "in_dim": 768,
        "layer_indices": [0],
        "dict_dim": 2048,
        "k_residue": 8,
        "k_sequence": 64,
        "aggregate": "mean",
        "order": "select-then-pool",
        "activation": "relu",
        "training_release": "211",
    }
    base.update(over)
    return base


class TestWhatSharesAPass:
    def test_heads_differing_only_in_the_reduction_share_one(self):
        """The axis the group exists to fan out over: five Light Attention and
        compressor heads differ in the dictionary and in how many atoms survive,
        and read the same residues."""
        metas = [
            _meta(dict_dim=2048, k_residue=8),
            _meta(dict_dim=8192, k_residue=64),
            _meta(aggregate="moments"),
            _meta(k_sequence=128),
            _meta(activation="gelu"),
        ]
        assert_shared_residue_pass(metas, names=[f"h{i}" for i in range(5)])

    def test_one_head_is_trivially_a_group(self):
        assert_shared_residue_pass([_meta()], names=["only"])

    def test_an_empty_group_does_not_raise_here(self):
        """The payload refuses an empty group; this function is not the place
        that decides a job has no work."""
        assert_shared_residue_pass([], names=[])

    def test_the_two_lists_do_not_overlap(self):
        """A field required to agree and excused from agreeing at once would be
        decided by whichever list the code read first."""
        assert not set(PASS_FIELDS) & set(REDUCTION_FIELDS)


class TestWhatDoesNot:
    def test_different_layers_are_refused(self):
        """The one that would have been silent: the layers come from the
        ARTIFACT, so a group sharing a pass would encode every head against the
        first head's hidden states."""
        with pytest.raises(SharedResiduePassError, match="layer_indices"):
            assert_shared_residue_pass(
                [_meta(), _meta(layer_indices=[4])], names=["last", "fifth-from-last"]
            )

    def test_the_refusal_explains_why_layers_cannot_be_shared(self):
        with pytest.raises(SharedResiduePassError) as exc:
            assert_shared_residue_pass([_meta(), _meta(layer_indices=[1])], names=["a", "b"])
        assert "read from the ARTIFACT" in str(exc.value)

    def test_different_in_dim_is_refused(self):
        with pytest.raises(SharedResiduePassError, match="in_dim"):
            assert_shared_residue_pass([_meta(), _meta(in_dim=1024)], names=["a", "b"])

    def test_the_refusal_names_both_heads_and_both_values(self):
        """A refusal that said "the group is inconsistent" sends a reader to
        read six artifacts."""
        with pytest.raises(SharedResiduePassError) as exc:
            assert_shared_residue_pass(
                [_meta(), _meta(layer_indices=[7])], names=["la-shared", "cstar-w7"]
            )
        msg = str(exc.value)
        assert "la-shared" in msg and "cstar-w7" in msg
        assert "(0,)" in msg and "(7,)" in msg

    def test_a_list_and_a_tuple_of_the_same_layers_agree(self):
        """``layer_indices`` arrives from JSON as a list and from a .pt as
        either, and a group split by the container type would refuse a group
        that does share a pass."""
        assert_shared_residue_pass(
            [_meta(layer_indices=[0]), _meta(layer_indices=(0,))], names=["a", "b"]
        )


class TestTheWidthIsCheckedAtDispatch:
    def test_a_head_expecting_another_width_is_refused(self):
        with pytest.raises(SharedResiduePassError, match="expects residues of width 1024"):
            assert_in_dim_matches_source([_meta(in_dim=1024)], names=["h"], source_dim=768)

    def test_the_refusal_says_where_it_used_to_fail(self):
        """It used to be an unguarded numpy error on the card, after the model
        had loaded, once per batch."""
        with pytest.raises(SharedResiduePassError) as exc:
            assert_in_dim_matches_source([_meta(in_dim=512)], names=["h"], source_dim=768)
        assert "after the model had loaded" in str(exc.value)

    def test_a_matching_width_passes(self):
        assert_in_dim_matches_source([_meta(in_dim=768)], names=["h"], source_dim=768)

    def test_a_source_with_no_rows_yet_is_not_second_guessed(self):
        """The width is read off a stored row, and a corpus with no rows has no
        width. Refusing there would block a group this guard knows nothing
        about."""
        assert_in_dim_matches_source([_meta(in_dim=1024)], names=["h"], source_dim=None)

    def test_a_head_that_declares_no_width_is_not_second_guessed_either(self):
        assert_in_dim_matches_source(
            [{k: v for k, v in _meta().items() if k != "in_dim"}], names=["h"], source_dim=768
        )


class TestThePayloadFoldsTheOldShapeIn:
    """Every dispatched job in this deployment is written with one artifact, and
    a payload shape that stops working is a worse defect than a field that
    outlives its reason."""

    def test_a_single_uri_becomes_a_group_of_one(self):
        p = EncodeResidueSparsePayload.model_validate(
            {"source_embedding_config_id": str(uuid.uuid4()), "encoder_artifact_uri": "k/h.pt"}
        )
        assert p.encoder_artifact_addresses == ["k/h.pt"]

    def test_a_single_path_becomes_a_group_of_one(self):
        p = EncodeResidueSparsePayload.model_validate(
            {"source_embedding_config_id": str(uuid.uuid4()), "encoder_artifact_path": "/t/h.pt"}
        )
        assert p.encoder_artifact_addresses == ["/t/h.pt"]

    def test_a_group_is_kept_in_the_order_it_was_written(self):
        """The batch messages and the reported result are read against what the
        caller wrote, so the order is part of the contract."""
        p = EncodeResidueSparsePayload.model_validate(
            {
                "source_embedding_config_id": str(uuid.uuid4()),
                "encoder_artifact_addresses": ["la.pt", "la-last.pt", "cstar-w7.pt"],
            }
        )
        assert p.encoder_artifact_addresses == ["la.pt", "la-last.pt", "cstar-w7.pt"]

    @pytest.mark.parametrize(
        ("bad", "why"),
        [
            ({}, "no address at all"),
            ({"encoder_artifact_path": "p", "encoder_artifact_uri": "u"}, "both singular"),
            (
                {"encoder_artifact_uri": "u", "encoder_artifact_addresses": ["a"]},
                "singular and group at once",
            ),
            ({"encoder_artifact_addresses": ["a.pt", "a.pt"]}, "the same artifact twice"),
        ],
    )
    def test_the_four_refusals(self, bad: dict, why: str):
        with pytest.raises(Exception):
            EncodeResidueSparsePayload.model_validate(
                {"source_embedding_config_id": str(uuid.uuid4()), **bad}
            )


def test_the_pass_is_outside_the_head_loop():
    """The whole economy of the change, pinned where it can be read.

    If a future edit moves ``embed_batch_per_residue`` inside the head loop the
    operation still works and silently costs one pass per head again, which is
    exactly the state this change exists to leave.
    """
    import inspect

    from protea.core.operations import encode_residue_sparse

    source = inspect.getsource(encode_residue_sparse._encode_batch)
    pass_at = source.index("embed_batch_per_residue")
    loop_at = source.index("for head in run.heads")
    assert pass_at < loop_at


def test_the_residue_counter_counts_the_pass_and_not_the_writes():
    """``residues_seen`` stays the number of residues the model actually read,
    however many heads consume them. A counter that multiplied by the head count
    would report work nobody did."""
    import inspect

    from protea.core.operations import encode_residue_sparse

    source = inspect.getsource(encode_residue_sparse._encode_batch)
    counter_at = source.index("residues_seen += int(residues.shape[0])")
    loop_at = source.index("for head in run.heads")
    assert counter_at < loop_at
