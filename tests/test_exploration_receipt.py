"""The receipt is computed from rows, says what is not recorded, and blocks what it must.

The method layer is the method seal ``compare_paired_panels`` already applies,
so the rows here are whole ``prediction_set`` rows in the shape ``SELECT *``
returns, the same shape ``tests/test_a_comparison_declares_its_axis.py`` uses.
The pair that was published as a bank difference on 2026-08-28 and also moved
``expand_votes_to_ancestors`` is the crossed case.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from protea.services import exploration_receipt as receipt
from protea.services import explorations_service as svc

_DEPENDENCIES = {
    "protea-method": "a120f613b9d2f1646ab5a566f97a8eadc1310a23",
    "protea-runners": "ccd06fd80c12b5195339478dae7255d1ea58f4a6",
    "protea-sources": "ad0810b9a2d117c336189d6f3f55cb3fa9aba86a",
    "protea-backends": "9b76719a14d729390be7d0faf957fae06a31ced5",
    "protea-contracts": "74d329ff22a77baa5142f477889dd6e2e2ca2a4f",
    "protea-reranker-lab": "6d4dc2d4b5330485b8830b20f7789356af7be1d4",
}


def _pset(pset_id: str, annotation_set_id: str, **meta_over: Any) -> dict[str, Any]:
    """One whole prediction_set row; only the bank and the meta overrides vary."""
    return {
        "id": pset_id,
        "embedding_config_id": "4d5d29ee-5a8c-53d2-bdc2-080187971454",
        "annotation_set_id": annotation_set_id,
        "ontology_snapshot_id": "6b78af68-eb01-477e-a599-fcde11ff0135",
        "query_set_id": "4951cdda-c352-4b54-84d0-0076834bc1ca",
        "limit_per_entry": 200,
        "distance_threshold": None,
        "created_at": "2026-08-27T22:14:03Z",
        "meta": {
            "job_id": f"job-for-{pset_id[:8]}",
            "batch_size": 1024,
            "metric": "cosine",
            "search_backend": "numpy",
            "features": [],
            "donor_policy": {
                "reviewed_only": False,
                "evidence_codes": None,
                "exclude_reference_prefixes": [],
            },
            "exclude_self_neighbour": True,
            "code_revision": "8699bfd7eec382aadd03f471b127f8be6f71cffd",
            "dependency_revisions": dict(_DEPENDENCIES),
            "aspect_separated_knn": True,
            "expand_votes_to_ancestors": False,
            **meta_over,
        },
    }


BANK_220 = "31dd3eb8-f36f-423a-bd03-45515dfe44ce"
BANK_165 = "0e076cb3-d616-402b-8ca5-60743038cf92"
CLEAN = {
    "base": _pset(BANK_220, "cbb35a32-44e4-4e39-b524-05b4b7433727"),
    "candidate": _pset(BANK_165, "575cb8da-f302-474e-9525-2be2f93d6791"),
}
CROSSED = {
    "base": _pset(BANK_220, "cbb35a32-44e4-4e39-b524-05b4b7433727"),
    "candidate": _pset(
        BANK_165, "575cb8da-f302-474e-9525-2be2f93d6791", expand_votes_to_ancestors=True
    ),
}


class TestTheMethodLayer:
    def test_two_arms_that_differ_only_in_the_axis_hold_it(self) -> None:
        layer = receipt.method_layer("one_axis", ["annotation_set_id"], CLEAN)
        assert layer == {"status": "one axis", "axis": ["annotation_set_id"]}

    def test_an_arm_that_also_moves_another_field_is_crossed(self) -> None:
        # The comparison published on 2026-08-28: the bank and the ancestor
        # expansion both moved, and the delta was attributed to the bank.
        layer = receipt.method_layer("one_axis", ["annotation_set_id"], CROSSED)
        assert layer["status"] == "crossed"
        assert "expand_votes_to_ancestors" in layer["detail"]

    def test_a_full_method_is_not_refused_and_lists_what_differs(self) -> None:
        layer = receipt.method_layer("full_method", [], CROSSED)
        assert layer["status"] == "full method"
        assert {"annotation_set_id", "expand_votes_to_ancestors"} <= set(layer["differs_in"])

    @pytest.mark.parametrize("n", [0, 1, 3])
    def test_anything_but_two_arms_is_not_recorded(self, n: int) -> None:
        arms = {f"arm{i}": CLEAN["base"] for i in range(n)}
        assert receipt.method_layer("one_axis", [], arms)["status"] == receipt.NOT_RECORDED


class TestTheRecipeIds:
    def test_a_recipe_id_a_name_id_and_a_broken_id_are_told_apart(self) -> None:
        shape = {
            "normalize": False,
            "normalize_residues": False,
            "embedding_scale": 1.0,
            "use_chunking": False,
            "chunk_size": 512,
            "chunk_overlap": 0,
        }
        full_length_ankh = {
            "id": "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0",
            "model_name": "ElnaggarLab/ankh-base",
            "model_backend": "ankh",
            "layer_indices": [0],
            "layer_agg": "mean",
            "pooling": "mean_max",
            "max_length": None,
            **shape,
        }
        ingested = {
            **full_length_ankh,
            "id": "db16eaa9-e71a-596b-bf21-a7f62d05f094",
            "model_name": "sfjepa-recipe-ens8-cut230",
            "model_backend": "ingested-method",
            "layer_agg": "none",
            "pooling": "reader",
            "max_length": 1022,
        }
        broken = {**full_length_ankh, "id": str(uuid.uuid4())}
        assert receipt._reproduced(full_length_ankh) == "recipe"
        assert receipt._reproduced(ingested) == "name"
        assert receipt._reproduced(broken) == "nothing"


def _result(digest: str | None, results: dict[str, Any] | None = None) -> Any:
    return SimpleNamespace(id=uuid.uuid4(), frame_digest=digest, results=results or {}, job_id=None)


class TestTheFrameAndTheScale:
    def test_one_digest_is_one_frame(self) -> None:
        layer = receipt._frame_layer([_result("f-a"), _result("f-a")])
        assert layer["status"] == "one frame"

    def test_two_digests_cross_and_a_missing_one_is_unsealed(self) -> None:
        assert receipt._frame_layer([_result("f-a"), _result("f-b")])["status"] == "crossed"
        assert receipt._frame_layer([_result("f-a"), _result(None)])["status"] == "unsealed"

    def test_no_result_is_not_recorded(self) -> None:
        assert receipt._frame_layer([])["status"] == receipt.NOT_RECORDED

    def test_a_maximum_at_an_edge_is_named_by_its_cell(self) -> None:
        results = {
            "NK": {
                "BPO": {
                    "maxima": {"f": {"tau": 0.99, "edge": "top"}, "f_w": {"tau": 0.5, "edge": None}}
                },
                "MFO": {"maxima": {"f": {"tau": 0.4, "edge": None}}},
            },
            "artifacts": {"keys": []},
        }
        assert receipt._censored(results) == ["NK/BPO/f at the top"]


class _Exploration(SimpleNamespace):
    pass


def test_only_a_crossed_one_axis_pair_blocks_a_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    row = _Exploration(id=uuid.uuid4(), kind="one_axis", rules={"axis": "annotation_set_id"})
    monkeypatch.setattr(receipt, "_arm_sets", lambda _s, _e: {"base": [1], "candidate": [2]})
    monkeypatch.setattr(receipt, "arm_rows", lambda _s, _e: CROSSED)
    blockers, layer = receipt.launch_blockers(None, row)  # type: ignore[arg-type]
    assert blockers == ["the arms differ in more than the declared axis"]
    monkeypatch.setattr(receipt, "arm_rows", lambda _s, _e: CLEAN)
    assert receipt.launch_blockers(None, row)[0] == []  # type: ignore[arg-type]
    row.kind = "full_method"
    monkeypatch.setattr(receipt, "arm_rows", lambda _s, _e: CROSSED)
    assert receipt.launch_blockers(None, row)[0] == []  # type: ignore[arg-type]


def test_the_service_refuses_to_launch_on_a_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        receipt,
        "launch_blockers",
        lambda _s, _e: (["the arms differ in more than the declared axis"], {"detail": "moved"}),
    )
    row = SimpleNamespace(
        state="draft", archived_reason=None, proposer="key:lab", number=1, kind="one_axis"
    )
    with pytest.raises(svc.ExplorationError, match="declared axis"):
        svc.launch(None, row, svc.Actor(name="key:server", type="api_key"))  # type: ignore[arg-type]
