"""The transpose of the strata panel, tested where the decisions are.

The existing view answers "how did this arm do across strata". This
answers "who wins inside this stratum", which is the question a reader
has once they know the strata separate more than the arms do: in rung 1
the twilight band scores 0.0535 against 0.1978 for the close band, while
the whole spread across eight backbones is 0.022.
"""

from __future__ import annotations

from protea.api.routers.strata import _Coordinates, _arm_rows, _matches


class TestCoordinates:
    def test_an_unset_axis_is_not_a_filter(self):
        # Omitting an axis means "do not pin it", not "pin it to empty".
        at = _Coordinates(setting="NK", category="NK", aspect=None, length=None, homology="<=30")
        assert at.where() == {"category": "NK", "homology": "<=30"}

    def test_nothing_pinned_filters_nothing(self):
        at = _Coordinates(setting="NK", category=None, aspect=None, length=None, homology=None)
        assert at.where() == {}


class TestMatches:
    def test_every_pinned_axis_has_to_agree(self):
        cell = {"category": "NK", "aspect": "P", "homology": "<=30"}
        assert _matches(cell, {"category": "NK", "homology": "<=30"})
        assert not _matches(cell, {"category": "NK", "homology": ">90"})

    def test_a_cell_missing_a_pinned_axis_does_not_match(self):
        # Crossed on three axes, asked about a fourth: the honest answer
        # is no, not a match on the axes it happens to have.
        assert not _matches({"category": "NK"}, {"length": "<=512"})


class _Store:
    """Artifact store stub: only the arms named here have been stratified."""

    def __init__(self, have: dict[str, list[dict]]):
        self._have = have

    def exists(self, key: str) -> bool:
        return any(key.startswith(k) or k in key for k in self._have)

    def get(self, key: str) -> bytes:
        for k in self._have:
            if k in key:
                return k.encode()
        raise KeyError(key)


class TestArmRows:
    @staticmethod
    def _arms():
        return [
            {"evaluation_result_id": "aaa", "model": "ankh", "display_name": "Ankh", "k": 3},
            {"evaluation_result_id": "bbb", "model": "esm", "display_name": "ESM", "k": 3},
        ]

    def test_an_unstratified_arm_is_absent_not_zero(self, monkeypatch):
        # Never stratified is a different fact from scored nothing here,
        # and drawing them the same way invents a result.
        store = _Store({"aaa": []})
        monkeypatch.setattr(
            "protea.api.routers.strata._cells",
            lambda raw: [{"category": "NK", "f_micro_w": 0.4, "n_proteins": 100}],
        )
        rows = _arm_rows(store, self._arms(), "NK", {"category": "NK"})
        assert [r["evaluation_result_id"] for r in rows] == ["aaa"]

    def test_the_arm_identity_travels_with_the_cell(self, monkeypatch):
        # A score with no arm beside it cannot be compared to anything.
        store = _Store({"aaa": [], "bbb": []})
        monkeypatch.setattr(
            "protea.api.routers.strata._cells",
            lambda raw: [{"category": "NK", "f_micro_w": 0.4, "n_proteins": 100}],
        )
        rows = _arm_rows(store, self._arms(), "NK", {"category": "NK"})
        assert {r["model"] for r in rows} == {"ankh", "esm"}
        assert all("k" in r and "f_micro_w" in r for r in rows)

    def test_cells_outside_the_stratum_are_dropped(self, monkeypatch):
        store = _Store({"aaa": []})
        monkeypatch.setattr(
            "protea.api.routers.strata._cells",
            lambda raw: [
                {"category": "NK", "f_micro_w": 0.4},
                {"category": "LK", "f_micro_w": 0.9},
            ],
        )
        rows = _arm_rows(store, self._arms(), "NK", {"category": "NK"})
        assert [r["f_micro_w"] for r in rows] == [0.4]
