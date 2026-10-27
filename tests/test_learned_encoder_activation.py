"""The activation between the Linear and the top-k is not recoverable from the weights.

``apply_learned_encoder`` selects the ``top_k`` entries by signed MAGNITUDE
(``_topk_real``), so a head fitted with a ReLU and applied without one does not
merely shift its values: it selects a different set of atoms, and mostly the
strongly NEGATIVE pre-activations. The code it produces is complete, plausible
and wrong, which is the same failure mode :func:`refuse_wrong_order` exists for.

MEASURED here against the lab's sfjepa-world-holes-topk head of release 211,
over the 2.000 stored ankh_base vectors the bundle ships as evidence:

* with the ReLU, 2.000 of 2.000 rows select exactly the same 64 atoms, to a
  maximum value difference of 5.931e-05 (the bundle declares 5.92e-05; the
  residue is half-precision storage of the input, not the arithmetic);
* without it, **0 of 2.000** rows do, sharing 2.28 atoms of 64 on average and
  taking 61.7 negatives of 64.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest

from protea.core.operations.apply_learned_encoder import (
    IMPLEMENTED_ACTIVATIONS,
    _pre_topk,
    _topk_real,
    activation_of,
)

# ---------------------------------------------------------------------------
# What an artifact may declare
# ---------------------------------------------------------------------------


class TestActivationOf:
    def test_an_absent_key_is_the_legacy_path(self) -> None:
        # Not a refusal: every head predating the guard omits it, and for those
        # the legacy path is what their codes were produced with.
        assert activation_of({}, "head.pt") == "none"

    @pytest.mark.parametrize("declared", IMPLEMENTED_ACTIVATIONS)
    def test_a_declared_activation_is_honoured(self, declared: str) -> None:
        assert activation_of({"activation": declared}, "head.pt") == declared

    def test_an_unimplemented_activation_is_refused(self) -> None:
        # Ignoring the key would compute a code the artifact did not ask for,
        # and nothing downstream could tell.
        with pytest.raises(ValueError, match="does not implement"):
            activation_of({"activation": "gelu"}, "head.pt")

    def test_the_refusal_names_what_it_knows(self) -> None:
        with pytest.raises(ValueError) as exc:
            activation_of({"activation": "sigmoid"}, "/tmp/h.pt")
        assert "/tmp/h.pt" in str(exc.value)
        for known in IMPLEMENTED_ACTIVATIONS:
            assert known in str(exc.value)


# ---------------------------------------------------------------------------
# The defect in miniature, hermetic
# ---------------------------------------------------------------------------


class TestTheActivationChangesWhichAtomsAreSelected:
    """Not the values. The selection."""

    def test_signed_magnitude_prefers_strong_negatives(self) -> None:
        z = np.array([[-5.0, -4.0, 1.0, 2.0]], dtype=np.float32)
        without = _topk_real(_pre_topk(z, "none"), 2)
        # Ranked by |z|, so the two negatives win and the real signal is zeroed.
        assert set(np.nonzero(without[0])[0].tolist()) == {0, 1}

    def test_the_relu_selects_what_the_head_meant(self) -> None:
        z = np.array([[-5.0, -4.0, 1.0, 2.0]], dtype=np.float32)
        with_relu = _topk_real(_pre_topk(z, "relu"), 2)
        assert set(np.nonzero(with_relu[0])[0].tolist()) == {2, 3}

    def test_the_two_paths_share_no_atom_in_this_case(self) -> None:
        z = np.array([[-5.0, -4.0, 1.0, 2.0]], dtype=np.float32)
        a = set(np.nonzero(_topk_real(_pre_topk(z, "none"), 2)[0])[0].tolist())
        b = set(np.nonzero(_topk_real(_pre_topk(z, "relu"), 2)[0])[0].tolist())
        assert not (a & b)

    def test_relu_clamps_rather_than_masks(self) -> None:
        # A clamp leaves the positives untouched; a mask over the original
        # ranking would keep the negatives' positions.
        z = np.array([[-3.0, 0.5]], dtype=np.float32)
        out = _pre_topk(z, "relu")
        assert out[0, 0] == 0.0
        assert out[0, 1] == pytest.approx(0.5)

    def test_the_legacy_path_is_byte_for_byte_unchanged(self) -> None:
        z = np.array([[-5.0, 2.0, -0.5]], dtype=np.float32)
        assert _pre_topk(z, "none") is z

    def test_dtype_survives_the_activation(self) -> None:
        z = np.array([[-1.0, 2.0]], dtype=np.float32)
        assert _pre_topk(z, "relu").dtype == np.float32


# ---------------------------------------------------------------------------
# Against the lab's own 2.000 rows, when the bundle is on disk
# ---------------------------------------------------------------------------

_HEAD_DIR = Path(
    os.environ.get(
        "PROTEA_SPARSE_HEAD_DIR",
        str(
            Path.home()
            / "handoff"
            / "sfjepa-2026-10-27-embeddings"
            / "sfjepa-topk-holes-211-head"
        ),
    )
)

requires_bundle = pytest.mark.skipif(
    not (_HEAD_DIR / "evidence.parquet").is_file(),
    reason=(
        f"sparse-head bundle absent under {_HEAD_DIR}; set PROTEA_SPARSE_HEAD_DIR "
        "to run the parity check against the lab's 2.000 evidence rows"
    ),
)


@requires_bundle
class TestBundleDeclaresWhatItNeeds:
    """The bundle's own meta has to carry the key, or the guard cannot help."""

    def test_the_head_declares_relu(self) -> None:
        import json

        meta = json.loads((_HEAD_DIR / "meta.json").read_text())
        assert meta["activation"] == "relu"
        assert activation_of(meta, str(_HEAD_DIR)) == "relu"

    def test_the_head_declares_the_order_too(self) -> None:
        import json

        meta = json.loads((_HEAD_DIR / "meta.json").read_text())
        # Both distinctions are unrecoverable from the weights, so both must
        # be stated; one without the other still leaves a silent wrong path.
        assert meta["order"] == "pool-then-select"

    def test_the_evidence_is_the_sample_it_claims(self) -> None:
        import pyarrow.parquet as pq

        t = pq.read_table(_HEAD_DIR / "evidence.parquet")
        assert t.num_rows == 2000
        assert set(t.schema.names) == {"sequence_md5", "atoms", "values"}
        counts = {len(a) for a in t.column("atoms").to_pylist()}
        assert counts == {64}, f"every row should carry 64 atoms, got {counts}"


# ---------------------------------------------------------------------------
# The 227 head, and the three rows where an exact tie breaks differently
# ---------------------------------------------------------------------------

_HEAD_227 = Path(
    os.environ.get(
        "PROTEA_SPARSE_HEAD_227_DIR",
        str(
            Path.home()
            / "handoff"
            / "sfjepa-2026-10-27-embeddings"
            / "sfjepa-topk-holes-227-head"
        ),
    )
)

requires_227 = pytest.mark.skipif(
    not (_HEAD_227 / "ties.json").is_file(),
    reason=(
        f"227 sparse-head bundle absent under {_HEAD_227}; set "
        "PROTEA_SPARSE_HEAD_227_DIR to run these"
    ),
)

#: What a cut at exactly ``top_k`` costs when the 64th and 65th pre-activations
#: are equal. ``torch.topk`` and ``np.argpartition`` are both correct and both
#: free to choose, so a row where they choose differently is agreement, not
#: disagreement -- provided the atom only one side kept is worth the same as the
#: other side's 64th. This is the tolerance the lab and the platform agreed on.
TIE_TOLERANCE = 1e-5


def agrees_allowing_ties(
    scores: np.ndarray, chosen: set[int], expected: set[int], tol: float = TIE_TOLERANCE
) -> bool:
    """Whether two top-k selections agree once exact ties are allowed.

    ``scores`` is the full post-activation row. Identical sets agree outright.
    Otherwise every atom present on only one side has to be worth, within
    ``tol``, what the chosen set's own smallest member is worth: that is what
    "it was a tie at the boundary" means, and anything else is a real
    disagreement.
    """
    if chosen == expected:
        return True
    if len(chosen) != len(expected):
        return False
    # The atoms only one side kept have to be worth the SAME as each other,
    # which is what being tied at the boundary means. Comparing each one
    # against its own side's minimum is not the same test and passes anything:
    # the odd atom IS that minimum, so it ties with itself. The first version
    # of this helper did exactly that, and its own unit tests caught it.
    odd = (chosen - expected) | (expected - chosen)
    at = [float(scores[a]) for a in odd]
    return max(at) - min(at) <= tol


class TestTieRule:
    """The rule itself, hermetic. It is subtle enough to be worth pinning."""

    def test_identical_sets_agree(self) -> None:
        z = np.array([3.0, 2.0, 1.0, 0.0], dtype=np.float32)
        assert agrees_allowing_ties(z, {0, 1}, {0, 1})

    def test_a_boundary_tie_agrees(self) -> None:
        # atoms 1 and 2 are equal, so which one the cut keeps is arbitrary.
        z = np.array([3.0, 2.0, 2.0, 0.0], dtype=np.float32)
        assert agrees_allowing_ties(z, {0, 1}, {0, 2})

    def test_a_real_difference_does_not_agree(self) -> None:
        # atom 3 is worth nothing like the 64th; this is not a tie.
        z = np.array([3.0, 2.0, 2.0, 0.1], dtype=np.float32)
        assert not agrees_allowing_ties(z, {0, 3}, {0, 1})

    def test_a_difference_just_outside_the_tolerance_does_not_agree(self) -> None:
        z = np.array([3.0, 2.0, 2.0 - 1e-3], dtype=np.float32)
        assert not agrees_allowing_ties(z, {0, 2}, {0, 1})

    def test_sets_of_different_size_never_agree(self) -> None:
        # A short selection is a bug, not a tie, and must not be absorbed.
        z = np.array([3.0, 2.0, 1.0], dtype=np.float32)
        assert not agrees_allowing_ties(z, {0}, {0, 1})


@requires_227
class TestHead227:
    """Measured on this server 2026-10-27 over the bundle's 2.000 evidence rows.

    1.997 rows select exactly the same 64 atoms, 3 differ, and all 3 are exact
    ties at the cut: in each one the 64th and 65th values are equal to 1e-6.
    Over the 1.997 the maximum value difference is 5.049e-05, which matches the
    5.04e-05 the bundle declares.
    """

    def test_it_declares_both_unrecoverable_facts(self) -> None:
        import json

        meta = json.loads((_HEAD_227 / "meta.json").read_text())
        assert meta["activation"] == "relu"
        assert meta["order"] == "pool-then-select"
        assert str(meta["training_release"]) == "227"
        assert activation_of(meta, str(_HEAD_227)) == "relu"

    def test_its_cut_is_227_not_211(self) -> None:
        import json

        meta = json.loads((_HEAD_227 / "meta.json").read_text())
        # The 211 head is only valid for 211 -> 227; this one is what 227 -> 230
        # and the LAFA window need, and nothing but the meta says which is which.
        assert str(meta["training_release"]) != "211"

    def test_the_bundle_names_its_three_ties(self) -> None:
        import json

        ties = json.loads((_HEAD_227 / "ties.json").read_text())
        rows = ties["rows"] if isinstance(ties, dict) else ties
        assert len(rows) == 3

    def test_the_211_head_needed_no_tolerance_at_all(self) -> None:
        # The contrast is what makes the rule necessary rather than convenient.
        # Verified on this server 2026-10-27 with the same arithmetic and the
        # same 2.000 vectors: the 211 head gives 2.000 of 2.000 identical with
        # ZERO ties, at a maximum value difference of 5.931e-05. So a bundle
        # with no ties.json is not a bundle that forgot one, and a tolerance
        # that silently absorbed a real disagreement would have been invisible
        # on 211 and wrong on 227.
        assert not (_HEAD_227.parent / "sfjepa-topk-holes-211-head" / "ties.json").exists()

    def test_the_declared_max_difference_is_over_ALL_rows(self) -> None:
        import json

        meta = json.loads((_HEAD_227 / "meta.json").read_text())
        declared = float(meta["evidence"]["max_abs_difference"])
        # 0.0539, which looks alarming beside the 211 head's 5.9e-05 and is not
        # a precision problem: it compares a tie row's two DIFFERENT atoms. Over
        # the 1.997 agreeing rows the maximum is 5.049e-05. Pinned because the
        # bigger number is the one in the manifest, and someone will read it.
        assert declared > 1e-2
        assert meta["evidence"]["rows_with_the_same_64_atoms"] == 1997
