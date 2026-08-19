"""Serving the per-stratum cells of a finished evaluation.

The endpoint computes nothing: it reads the `strata.parquet` that
`stratify_evaluation` wrote beside the evaluation's other artefacts. What is
worth testing is therefore what it does when they are absent, and that it does
not quietly narrow what it returns.
"""

from __future__ import annotations

import io
import uuid

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers import strata as strata_router


def _parquet(rows: list[dict]) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False)
    return buf.getvalue()


_CELLS = [
    {"category": "NK", "aspect": "F", "homology": "<=30",
     "n_proteins": 271, "precision_w": 0.2, "recall_w": 0.3,
     "f_micro_w": 0.2461, "reportable": True},
    {"category": "NK", "aspect": "F", "homology": ">90",
     "n_proteins": 4, "precision_w": 0.4, "recall_w": 0.5,
     "f_micro_w": 0.4039, "reportable": False},
]


class _Store:
    """Holds parquet bytes under whichever keys the test seeds."""

    def __init__(self, keys: dict[str, bytes]):
        self._keys = keys
        self.asked: list[str] = []

    def exists(self, key: str) -> bool:
        self.asked.append(key)
        return key in self._keys

    def get(self, key: str) -> bytes:
        return self._keys[key]


@pytest.fixture()
def result_id() -> uuid.UUID:
    return uuid.uuid4()


def _client(monkeypatch: pytest.MonkeyPatch, store: _Store) -> TestClient:
    monkeypatch.setattr(strata_router, "get_artifact_store", lambda _s: store)
    app = FastAPI()
    app.include_router(strata_router.router)
    app.dependency_overrides[strata_router.get_settings] = lambda: object()
    return TestClient(app)


class TestWhenTheArtefactsExist:
    def _store(self, result_id: uuid.UUID) -> _Store:
        from protea.core.operations._run_cafa_helpers import eval_artifact_key

        return _Store({eval_artifact_key(result_id, "NK/strata.parquet"): _parquet(_CELLS)})

    def test_it_returns_the_cells_of_each_setting_present(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        c = _client(monkeypatch, self._store(result_id))
        body = c.get(f"/strata/{result_id}").json()
        assert list(body["settings"]) == ["NK"]
        assert len(body["settings"]["NK"]) == 2

    def test_it_names_the_axes_that_were_crossed(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        """A cell is unreadable without knowing what it is a cell of."""
        c = _client(monkeypatch, self._store(result_id))
        assert c.get(f"/strata/{result_id}").json()["axes"] == [
            "category", "aspect", "homology"
        ]

    def test_thin_cells_are_returned_by_default(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        """Off by default on purpose: a response holding only what survived
        looks identical to one that covered everything."""
        c = _client(monkeypatch, self._store(result_id))
        cells = c.get(f"/strata/{result_id}").json()["settings"]["NK"]
        assert [x["reportable"] for x in cells] == [True, False]

    def test_the_caller_can_ask_for_the_narrower_view(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        c = _client(monkeypatch, self._store(result_id))
        cells = c.get(f"/strata/{result_id}?reportable_only=true").json()["settings"]["NK"]
        assert [x["reportable"] for x in cells] == [True]

    def test_the_population_travels_with_every_cell(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        c = _client(monkeypatch, self._store(result_id))
        cells = c.get(f"/strata/{result_id}").json()["settings"]["NK"]
        assert [x["n_proteins"] for x in cells] == [271, 4]

    def test_it_probes_the_three_knowledge_settings(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        store = self._store(result_id)
        _client(monkeypatch, store).get(f"/strata/{result_id}")
        assert [k.rsplit("/", 2)[-2] for k in store.asked] == ["NK", "LK", "PK"]


class TestWhenTheyDoNot:
    def test_an_unstratified_evaluation_is_a_404_not_an_empty_body(
        self, monkeypatch: pytest.MonkeyPatch, result_id: uuid.UUID
    ) -> None:
        """An empty body and a run nobody stratified would otherwise look the
        same, and only one of them is worth fixing."""
        r = _client(monkeypatch, _Store({})).get(f"/strata/{result_id}")
        assert r.status_code == 404
        assert "stratify_evaluation" in r.json()["detail"]

    def test_a_malformed_id_is_rejected_before_any_storage_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = _Store({})
        assert _client(monkeypatch, store).get("/strata/not-a-uuid").status_code == 422
        assert store.asked == []
