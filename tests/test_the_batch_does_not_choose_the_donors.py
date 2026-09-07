"""A stored neighbourhood is a function of the query and the bank.

It stopped being one. PROTEA pre-searched at k+1 dropping by accession, while
the method asked for k plus a margin measured from the bank and dropped by
sequence. The method therefore asked for more than the pre-search had
delivered, and its own bank is built from the narrowed annotation dict, which
is the union of the WHOLE chunk's hits. So each dropped twin was refilled from
whatever else that chunk happened to contain.

Measured on the real pool before the fix: 304 queries received at least one
donor that was not one of their own pre-search hits, 264 of them having a bank
twin, 887 donor slots. Re-running the identical payload with a different batch
composition changed the stored rows for those queries, and every substituted
donor also lost its pair features, because those are keyed by the pair and
built only from the query's own hits.

The property is the one worth testing, not the mechanism: the same query must
get the same donors whoever it shares a batch with.

WHY THESE ARE PARAMETRISED OVER BOTH PATHS. The first version of this file
opened with "Both searches now take their depth from ``extra_neighbours_for``"
and then read one file. It was true of the aspect path and false of the
unified one, and it stayed green for the whole time the unified path was
broken. On 2026-09-07 every ``exclude_self_neighbour`` arm of axis C died on
``SequenceIdentityMissingError``, in the path this test named and did not
read. There are two retrieval paths; the property belongs to both, so the
assertions take the path as a parameter and a third path added later gets the
requirement by adding a row.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

_PRE_SEARCH = [
    pytest.param(
        "protea/core/operations/predict_go_terms/_aspect_helpers.py",
        "_knn_one_aspect",
        id="aspect",
    ),
    pytest.param(
        "protea/core/operations/predict_go_terms/_unified_path.py",
        "unified_load_annotations",
        id="unified",
    ),
]

#: (module, caller, the map read, the pre-search it must precede)
_ORDERING = [
    pytest.param(
        "protea/core/operations/predict_go_terms/_aspect_helpers.py",
        "_build_aspect_adapter_inputs",
        "_sequence_keys_for",
        "_AspectKnnPreSearch.run",
        id="aspect",
    ),
    pytest.param(
        "protea/core/operations/predict_go_terms/_unified_path.py",
        "unified_predict_via_pipeline",
        "_unified_sequence_keys",
        "_unified_load_annotations",
        id="unified",
    ),
]


def _body_of(module: str, function: str) -> str:
    tree = ast.parse(pathlib.Path(module).read_text())
    fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == function
    )
    return ast.unparse(fn)


@pytest.mark.parametrize("module,function", _PRE_SEARCH)
class TestTheMarginAndTheDropAgreeBetweenTheTwoSearches:
    """The property that makes the batch irrelevant, checked in both places
    it lives."""

    def test_the_depth_comes_from_the_same_function_the_method_uses(
        self, module: str, function: str
    ) -> None:
        assert "extra_neighbours_for" in _body_of(module, function), (
            f"{function} no longer takes its depth from the same function the "
            "method uses, so the two horizons can differ again"
        )

    def test_the_drop_is_by_sequence(self, module: str, function: str) -> None:
        assert "without_own_sequence" in _body_of(module, function), (
            f"{function} no longer drops by sequence, so it hands back twins "
            "the method will drop and then refill from the batch"
        )

    def test_the_accession_based_exclusion_is_not_back(
        self, module: str, function: str
    ) -> None:
        body = _body_of(module, function)
        assert "search_k_for" not in body and "without_self(" not in body, (
            f"the accession-based exclusion is back in {function}"
        )


@pytest.mark.parametrize("module,caller,read,pre_search", _ORDERING)
def test_the_sequence_map_is_read_before_the_pre_search(
    module: str, caller: str, read: str, pre_search: str
) -> None:
    """Building it from the pre-search's hits would be circular, and the
    pre-search needs it to know how deep to ask."""
    lines = _body_of(module, caller).splitlines()
    read_at = next(i for i, ln in enumerate(lines) if read in ln)
    search_at = next(i for i, ln in enumerate(lines) if pre_search in ln)
    assert read_at < search_at, (
        f"in {caller} the sequence map is built after the pre-search, so the "
        "pre-search cannot use it and the two searches diverge again"
    )


@pytest.mark.parametrize("module,caller,read,pre_search", _ORDERING)
def test_the_map_covers_the_whole_bank_not_the_hits(
    module: str, caller: str, read: str, pre_search: str
) -> None:
    """The failure of 2026-09-07 in one line: a map built from the pre-search's
    own trimmed hits cannot cover what the method reaches past them."""
    body = _body_of(module, read)
    assert "unique_neighbors" not in body, (
        f"{read} builds the map from the pre-search's hits, which is the "
        "population that is too small by construction"
    )


class TestTheDropActuallyHappens:
    """Behaviour, not source text. The assertions above pin which functions
    the paths call; this pins what goes wrong when they call the other pair,
    and it is worse than a short list: with more than one twin in the bank,
    the accession-based drop finds nothing to drop and the query is returned
    as its own donor. That is the state the 95.0-per-cent measurement in
    ``_self_neighbour`` says makes a depth sweep meaningless.
    """

    @staticmethod
    def _ctx_with_two_twins():
        import uuid

        import numpy as np
        from protea_contracts import PredictGOTermsBatchPayload

        from protea.core.operations.predict_go_terms import _batch_op as pgt

        p = PredictGOTermsBatchPayload.model_validate(
            {
                "embedding_config_id": str(uuid.uuid4()),
                "annotation_set_id": str(uuid.uuid4()),
                "ontology_snapshot_id": str(uuid.uuid4()),
                "prediction_set_id": str(uuid.uuid4()),
                "parent_job_id": str(uuid.uuid4()),
                "query_accessions": ["Q1"],
                "aspect_separated_knn": False,
                "exclude_self_neighbour": True,
                "limit_per_entry": 2,
                "search_backend": "numpy",
                "metric": "l2",
                "compute_alignments": False,
                "compute_taxonomy": False,
            }
        )
        bank = ["R1", "R2", "R3", "R4"]
        ctx = pgt._UnifiedPredictContext(
            p=p,
            annotation_set_id=uuid.uuid4(),
            prediction_set_id=uuid.uuid4(),
            valid_accessions=["Q1"],
            query_embeddings=np.zeros((1, 2), dtype=np.float32),
            ref_data={
                "accessions": bank,
                "embeddings_f32": np.zeros((4, 2), dtype=np.float32),
                "embeddings_f32_cos": np.zeros((4, 2), dtype=np.float32),
            },
        )
        # R1 and R2 carry the query's own sequence. Two, not one, because a
        # single twin is the case the naive k+1 happens to survive.
        keys = {"Q1": "S1", "R1": "S1", "R2": "S1", "R3": "S3", "R4": "S4"}
        return ctx, bank, keys

    def test_no_donor_carries_the_querys_own_sequence(self) -> None:
        from unittest.mock import MagicMock, patch

        from protea.core.operations.predict_go_terms._batch_op import (
            PredictGOTermsBatchOperation,
        )

        op = PredictGOTermsBatchOperation()
        ctx, bank, keys = self._ctx_with_two_twins()
        asked: dict[str, int] = {}

        def fake_search(_q, _refs, _accs, *, k, **_kw):
            asked["k"] = k
            return [[(a, float(i)) for i, a in enumerate(bank[:k])]]

        with (
            patch("protea.core.knn_search.search_knn", side_effect=fake_search),
            patch.object(op, "_load_annotations_for", return_value={}),
        ):
            _anns, neighbours = op._unified_load_annotations(MagicMock(), ctx, keys)

        assert asked["k"] == 4, (
            "the search must ask for k plus a margin measured from the bank; "
            "k+1 cannot survive two twins"
        )
        assert neighbours == {"R3", "R4"}, (
            f"the query's own sequence is still among its donors: {neighbours}"
        )

    def test_every_returned_neighbour_is_in_the_map(self) -> None:
        """The failure of 2026-09-07: the method reaches a neighbour the map
        was not built over, and refuses."""
        from unittest.mock import MagicMock, patch

        from protea.core.operations.predict_go_terms._batch_op import (
            PredictGOTermsBatchOperation,
        )

        op = PredictGOTermsBatchOperation()
        ctx, bank, keys = self._ctx_with_two_twins()

        with (
            patch(
                "protea.core.knn_search.search_knn",
                side_effect=lambda _q, _r, _a, *, k, **_kw: [
                    [(a, float(i)) for i, a in enumerate(bank[:k])]
                ],
            ),
            patch.object(op, "_load_annotations_for", return_value={}),
        ):
            _anns, neighbours = op._unified_load_annotations(MagicMock(), ctx, keys)

        assert neighbours <= set(keys), f"unmapped neighbours: {neighbours - set(keys)}"
