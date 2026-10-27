"""``GET /v1/graph`` can be pointed at a window instead of guessing one.

The graph reads its window from ``record["evaluation_sets"][0]`` in four
places, and the query behind it returns every set ordered by ``created_at
DESC``. With one window that was fine. The campaign now has three -- the long
validation 211 to 227, the single-pass test 227 to 230, and LAFA's external
window -- so the surface was showing whichever was built last with no way to
ask for another.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from protea.api.routers.graph import _promote_head


def _record(*ids: str) -> dict[str, list[dict[str, object]]]:
    return {"evaluation_sets": [{"id": i, "window_from": i} for i in ids]}


class TestPromoteHead:
    def test_no_id_leaves_the_order_alone(self) -> None:
        # The historical behaviour: newest first, which the query already gives.
        rec = _record("a", "b", "c")
        _promote_head(rec, None)
        assert [r["id"] for r in rec["evaluation_sets"]] == ["a", "b", "c"]

    def test_the_requested_set_becomes_the_head(self) -> None:
        rec = _record("a", "b", "c")
        _promote_head(rec, "c")
        assert [r["id"] for r in rec["evaluation_sets"]] == ["c", "a", "b"]

    def test_asking_for_the_head_is_a_no_op(self) -> None:
        rec = _record("a", "b")
        _promote_head(rec, "a")
        assert [r["id"] for r in rec["evaluation_sets"]] == ["a", "b"]

    def test_it_reorders_and_does_not_filter(self) -> None:
        # The other rows are not noise: the ``declared`` block counts published
        # results and the sets it did not choose still belong to the record.
        rec = _record("a", "b", "c")
        _promote_head(rec, "b")
        assert len(rec["evaluation_sets"]) == 3

    def test_an_unknown_id_is_a_404_and_not_a_fallback(self) -> None:
        # Falling back to the newest window would answer a question about one
        # window with the numbers of another, silently.
        rec = _record("a", "b")
        with pytest.raises(HTTPException) as exc:
            _promote_head(rec, "zzz")
        assert exc.value.status_code == 404
        assert "zzz" in str(exc.value.detail)
        assert [r["id"] for r in rec["evaluation_sets"]] == ["a", "b"]

    def test_an_empty_record_with_an_id_is_still_a_404(self) -> None:
        rec: dict[str, list[dict[str, object]]] = {"evaluation_sets": []}
        with pytest.raises(HTTPException):
            _promote_head(rec, "a")

    def test_an_empty_record_without_an_id_is_fine(self) -> None:
        rec: dict[str, list[dict[str, object]]] = {"evaluation_sets": []}
        _promote_head(rec, None)
        assert rec["evaluation_sets"] == []

    def test_ids_are_compared_as_text(self) -> None:
        # The rows come back from the driver as UUID objects on some paths and
        # as text on others; comparing str() to str() is what makes both work.
        import uuid

        u = uuid.uuid4()
        rec = {"evaluation_sets": [{"id": "x"}, {"id": u}]}
        _promote_head(rec, str(u))
        assert rec["evaluation_sets"][0]["id"] == u
