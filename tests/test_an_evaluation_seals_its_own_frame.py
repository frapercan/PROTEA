"""An evaluation seals the row it just created, from the stored material.

Two properties, and only the second is about this function's code.

**The two paths read the same material.** The seal is a content address, so
the bulk operation and the self seal must produce byte-equal digests for one
frame. They do because they share one SELECT, and that is asserted here rather
than trusted: a copy of the column list is how two producers of one address
start disagreeing, and nothing downstream could tell, because a digest that
does not match reads exactly like a frame that moved.

**It never fails the evaluation.** The operation it runs inside has already
scored, so a frame that cannot be recovered is reported and left alone. Until
2026-11-05 the seal was a separate operation with ``dry_run`` defaulting to
true, which is right for rewriting rows nobody is looking at and wrong for the
row just created: exploration 1's comparison failed on two unsealed results
whose own job had finished successfully twenty minutes earlier.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from protea.core.operations import seal_evaluation_frames as seal

A_ROW: dict[str, Any] = {
    "id": "11111111-1111-1111-1111-111111111111",
    "frame": None,
    "evaluation_set_id": "22222222-2222-2222-2222-222222222222",
    "temporal_window": "211-227",
    "pivot_snapshot_id": "33333333-3333-3333-3333-333333333333",
    "information_accretion_set_id": "44444444-4444-4444-4444-444444444444",
    "max_terms": None,
    "max_distance": None,
    "restrict_gt_to_predicted": "true",
    "th_step": "0.01",
    "toi_file": None,
    "toi_rule": None,
    "score_reading": "raw",
    "has_job": True,
}


class _Session:
    """Answers the one SELECT with a fixed row and records what gets written."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row
        self.writes: list[dict[str, Any]] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> Any:
        if statement is seal._SEAL:
            self.writes.append(params or {})
            return None
        return SimpleNamespace(
            mappings=lambda: SimpleNamespace(one_or_none=lambda: self.row)
        )


def _events() -> tuple[list[tuple[str, dict[str, Any]]], Any]:
    seen: list[tuple[str, dict[str, Any]]] = []

    def emit(event: str, _message: Any, fields: dict[str, Any], _level: str) -> None:
        seen.append((event, fields))

    return seen, emit


class TestTheTwoPathsReadOneMaterial:
    def test_the_bulk_query_and_the_one_row_query_select_the_same_fields(self) -> None:
        def columns(sql: str) -> str:
            return sql.split("FROM evaluation_result")[0]

        assert columns(str(seal._ROWS)) == columns(str(seal._ONE_ROW))

    def test_the_one_row_query_asks_for_one_row(self) -> None:
        assert "WHERE r.id = CAST(:id AS uuid)" in str(seal._ONE_ROW)
        assert "WHERE" not in str(seal._ROWS)


class TestSealingTheRowItJustCreated:
    def test_it_writes_what_the_shared_digest_says(self) -> None:
        session = _Session(dict(A_ROW))
        seen, emit = _events()
        got = seal.seal_one(session, A_ROW["id"], emit)
        assert got == seal.frame_digest(A_ROW)
        assert session.writes == [{"frame": got, "id": A_ROW["id"]}]
        assert [e for e, _ in seen] == ["seal.one_done"]

    def test_an_unrecoverable_frame_is_reported_and_nothing_is_written(self) -> None:
        session = _Session(dict(A_ROW, evaluation_set_id=None))
        seen, emit = _events()
        assert seal.seal_one(session, A_ROW["id"], emit) is None
        assert session.writes == []
        assert seen[0][0] == "seal.one_unattributable"
        assert "population is unknown" in seen[0][1]["why"]

    def test_a_row_with_no_job_says_which_of_the_two_things_is_missing(self) -> None:
        session = _Session(dict(A_ROW, evaluation_set_id=None, has_job=False))
        seen, emit = _events()
        assert seal.seal_one(session, A_ROW["id"], emit) is None
        assert "no producing job" in seen[0][1]["why"]

    def test_a_different_stored_seal_is_never_overwritten(self) -> None:
        session = _Session(dict(A_ROW, frame="f-somebodyelsesframe00000"))
        seen, emit = _events()
        assert seal.seal_one(session, A_ROW["id"], emit) is None
        assert session.writes == []
        assert seen[0][0] == "seal.one_conflict"

    def test_the_same_seal_already_stored_is_rewritten_without_complaint(self) -> None:
        session = _Session(dict(A_ROW, frame=seal.frame_digest(A_ROW)))
        seen, emit = _events()
        assert seal.seal_one(session, A_ROW["id"], emit) == seal.frame_digest(A_ROW)
        assert [e for e, _ in seen] == ["seal.one_done"]

    def test_a_row_that_cannot_be_hashed_is_reported_and_does_not_raise(self) -> None:
        """Nothing out of the database should do this, and it still must not raise."""
        session = _Session(dict(A_ROW, toi_rule=object()))
        seen, emit = _events()
        assert seal.seal_one(session, A_ROW["id"], emit) is None
        assert session.writes == []
        assert seen[0][0] == "seal.one_failed"

    def test_a_row_that_is_not_there_is_reported_and_does_not_raise(self) -> None:
        session = _Session(None)
        seen, emit = _events()
        assert seal.seal_one(session, A_ROW["id"], emit) is None
        assert session.writes == []
        assert seen[0][0] == "seal.one_missing"
