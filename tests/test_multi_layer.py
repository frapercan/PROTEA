"""One forward pass standing in for N, and the ways it must refuse to.

The saving is real -- inference is 0.999 of a batch's time, so four depths of
one model cost four passes where one would do -- but the shortcut is only sound
while every configuration in the group differs from the others in the layer and
nothing else. These pin both halves: that the split is exact, and that a group
which cannot share a pass is rejected rather than silently mixed.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.operations._multi_layer import plan_for, split_pooled


class _Cfg:
    """The identity fields a shared pass has to agree on, plus the layer."""

    def __init__(self, name: str, layers: list[int], **overrides: object) -> None:
        self.id = uuid.uuid4()
        self.display_name = name
        self.layer_indices = layers
        self.model_name = "ElnaggarLab/ankh-base"
        self.model_backend = "ankh"
        self.pooling = "mean"
        self.normalize = True
        self.normalize_residues = False
        self.use_chunking = False
        self.chunk_size = 512
        self.chunk_overlap = 0
        self.max_length = 2048
        self.embedding_scale = 1.0
        for k, v in overrides.items():
            setattr(self, k, v)


class TestThePlan:
    def test_layers_are_requested_ascending(self):
        """The backend sorts its request, so the concatenation follows that
        order and the slots have to be assigned against it, not against the
        order the caller happened to list."""
        p = plan_for([_Cfg("d0", [48]), _Cfg("d100", [0]), _Cfg("d67", [16])])
        assert p.layer_indices == [0, 16, 48]

    def test_each_config_gets_the_slot_of_its_own_layer(self):
        a, b, c = _Cfg("d0", [48]), _Cfg("d100", [0]), _Cfg("d67", [16])
        p = plan_for([a, b, c])
        assert p.slot_of[str(b.id)] == 0
        assert p.slot_of[str(c.id)] == 1
        assert p.slot_of[str(a.id)] == 2

    def test_two_configs_on_one_layer_share_a_slot(self):
        a, b = _Cfg("uno", [16]), _Cfg("otro", [16])
        p = plan_for([a, b])
        assert len(p) == 1
        assert p.slot_of[str(a.id)] == p.slot_of[str(b.id)] == 0


class TestItRefusesWhatItCannotShare:
    @pytest.mark.parametrize(
        "field,value",
        [
            ("model_name", "facebook/esm2_t33_650M_UR50D"),
            ("pooling", "cls"),
            ("max_length", 1022),
            ("normalize", False),
            ("use_chunking", True),
        ],
    )
    def test_a_recipe_difference_beyond_the_layer_raises(self, field, value):
        """A shared pass cannot vary these after the fact, so folding such a
        config in would store vectors for a recipe nobody asked for -- under
        the right config id, which is what makes it invisible."""
        with pytest.raises(ValueError, match=field):
            plan_for([_Cfg("base", [0]), _Cfg("otro", [16], **{field: value})])

    def test_a_config_aggregating_several_layers_raises(self):
        """Already-aggregated layers are not a depth on this axis."""
        with pytest.raises(ValueError, match="selects 2 layers"):
            plan_for([_Cfg("base", [0]), _Cfg("agregada", [0, 10])])

    def test_an_empty_group_raises(self):
        with pytest.raises(ValueError, match="no configurations"):
            plan_for([])


class TestTheSplitIsExact:
    def test_a_slice_is_the_matching_third(self):
        v = [1.0, 0.0, 0.0] + [0.0, 2.0, 0.0] + [0.0, 0.0, 3.0]
        assert split_pooled(v, 0, 3, normalize=False) == [1.0, 0.0, 0.0]
        assert split_pooled(v, 1, 3, normalize=False) == [0.0, 2.0, 0.0]
        assert split_pooled(v, 2, 3, normalize=False) == [0.0, 0.0, 3.0]

    def test_each_slice_is_renormalised(self):
        """The unit norm was applied to the whole concatenation, so every slice
        comes out short by a factor that depends on what the OTHER layers held
        -- a per-layer scaling nothing downstream could see or undo."""
        v = [3.0, 4.0, 6.0, 8.0]
        assert split_pooled(v, 0, 2, normalize=True) == pytest.approx([0.6, 0.8])
        assert split_pooled(v, 1, 2, normalize=True) == pytest.approx([0.6, 0.8])

    def test_a_zero_slice_is_returned_untouched(self):
        assert split_pooled([0.0, 0.0, 1.0, 1.0], 0, 2, normalize=True) == [0.0, 0.0]

    def test_a_width_that_does_not_divide_raises(self):
        """The pass and the plan disagreeing about the layer count would
        otherwise slice each configuration a window off from its own."""
        with pytest.raises(ValueError, match="does not divide"):
            split_pooled([1.0] * 7, 0, 2, normalize=False)

    def test_one_slot_is_the_whole_vector(self):
        v = [3.0, 4.0]
        assert split_pooled(v, 0, 1, normalize=True) == pytest.approx([0.6, 0.8])
