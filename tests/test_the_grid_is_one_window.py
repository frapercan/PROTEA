"""A nine-cell grid belongs to one window, and says which.

The leaderboard's key was ``(category, aspect)`` alone, so with no
``evaluation_set_id`` pinned the per-cell maximum ran over the rows of every
stored window and each cell could land on a different one: nine populations
under nine truths, printed as one grid.

Measured 2026-10-27 against the live API, the matrix returned 27 rows over
three windows and all nine cells came from ``ea0f4334`` -- because that run
wins everywhere. The grid was right by luck. These tests are about the code.
"""

from __future__ import annotations

from protea.api.routers._benchmark_grid import make_leaderboard, newest_window

CATS = ("NK", "LK", "PK")
ASPECTS = ("BPO", "CCO", "MFO")

VIEJA = "11111111-1111-1111-1111-111111111111"
NUEVA = "22222222-2222-2222-2222-222222222222"


def _row(window: str, cat: str, asp: str, primary: float) -> dict:
    """A matrix row with every key the leaderboard entry reads.

    Spelled out rather than trimmed to what these assertions touch: the entry
    builder reads fifteen of them, and a fixture that carried only the three
    under test would pass while the real row shape drifted -- which is the
    failure this session has already paid for twice.
    """
    return {
        "evaluation_set_id": window,
        "evaluation_result_id": f"r-{window[:4]}-{cat}-{asp}",
        "category": cat,
        "aspect": asp,
        "primary": primary,
        "primary_metric": "f_micro_w",
        "f_micro_w": primary,
        "f_micro": primary,
        "fmax_w": primary,
        "fmax": primary,
        "precision_w": 0.5,
        "recall_w": 0.5,
        "precision": 0.5,
        "recall": 0.5,
        "coverage_w": 1.0,
        "coverage": 1.0,
        "stage": "knn",
        "embedding_config_id": "e",
        "k": 3,
    }


def _cells(lb: list[dict]) -> dict[tuple[str, str], dict]:
    return {(c["category"], c["aspect"]): c for c in lb if c.get("primary") is not None}


class TestTheMaximumStaysInsideItsWindow:
    def test_a_higher_cell_from_another_window_does_not_win(self) -> None:
        # The whole defect in one assertion: the old window scores higher in
        # NK/BPO, and asking for the new window's grid must not import it.
        rows = [_row(NUEVA, "NK", "BPO", 0.40), _row(VIEJA, "NK", "BPO", 0.90)]
        cells = _cells(make_leaderboard(rows, CATS, ASPECTS, window=NUEVA))
        assert cells[("NK", "BPO")]["primary"] == 0.40
        assert cells[("NK", "BPO")]["evaluation_set_id"] == NUEVA

    def test_every_cell_of_a_grid_carries_the_same_window(self) -> None:
        rows = [
            _row(NUEVA, cat, asp, 0.3)
            for cat in CATS
            for asp in ASPECTS
        ] + [_row(VIEJA, cat, asp, 0.95) for cat in CATS for asp in ASPECTS]
        cells = _cells(make_leaderboard(rows, CATS, ASPECTS, window=NUEVA))
        assert len(cells) == 9
        assert {c["evaluation_set_id"] for c in cells.values()} == {NUEVA}
        # And every one of them is the NEW window's number, not the old
        # window's higher one.
        assert {round(c["primary"], 2) for c in cells.values()} == {0.3}

    def test_a_window_with_no_rows_gives_an_empty_grid(self) -> None:
        # Empty and not borrowed: a window nothing scored has no cells, and
        # filling them from elsewhere is the defect wearing a label.
        rows = [_row(VIEJA, "NK", "BPO", 0.9)]
        assert _cells(make_leaderboard(rows, CATS, ASPECTS, window=NUEVA)) == {}

    def test_only_cells_with_a_winner_are_emitted(self) -> None:
        # The builder's own contract: one entry per cell that HAS a winner,
        # in canonical order, and nothing for the cells that do not. So a
        # window with one measured cell yields one entry and not nine nulls.
        lb = make_leaderboard([_row(NUEVA, "NK", "BPO", 0.4)], CATS, ASPECTS, window=NUEVA)
        assert len(lb) == 1
        assert (lb[0]["category"], lb[0]["aspect"]) == ("NK", "BPO")


class TestWhichWindowAGridDefaultsTo:
    def test_the_newest_window_that_has_rows(self) -> None:
        payload = [
            {"id": VIEJA, "created_at": "2026-10-11T00:00:00Z"},
            {"id": NUEVA, "created_at": "2026-10-27T00:00:00Z"},
        ]
        rows = [_row(VIEJA, "NK", "BPO", 0.9), _row(NUEVA, "NK", "BPO", 0.4)]
        assert newest_window(payload, rows) == NUEVA

    def test_a_newer_window_with_no_rows_is_skipped(self) -> None:
        # It would print nine empty cells under a correct label, which is a
        # worse answer than the window that was actually measured.
        payload = [
            {"id": VIEJA, "created_at": "2026-10-11T00:00:00Z"},
            {"id": NUEVA, "created_at": "2026-10-27T00:00:00Z"},
        ]
        assert newest_window(payload, [_row(VIEJA, "NK", "BPO", 0.9)]) == VIEJA

    def test_nothing_measured_means_nothing_to_show(self) -> None:
        assert newest_window([{"id": NUEVA, "created_at": "2026-10-27T00:00:00Z"}], []) is None
