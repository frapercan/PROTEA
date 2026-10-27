"""Which window a benchmark grid is about, and the winner inside it.

Two functions, together because they are one decision made in two halves: the
window a nine-cell grid belongs to, and the per-cell maximum taken WITHIN that
window. Apart they were easy to call in the wrong order, and the wrong order
is a grid whose nine cells come from nine different measurements.

ONE WINDOW, ALWAYS. The leaderboard key was ``(category, aspect)`` alone, so
with no ``evaluation_set_id`` pinned the maximum ran over every stored window
and each of the nine cells could land on a different one: nine populations
under nine truths, printed as one grid. Measured 2026-10-27, the matrix
returns 27 rows over three windows and all nine cells came from ``ea0f4334``
only because that run wins everywhere -- a property of the data and not of
the code, which is the worst kind of safe.
"""

from __future__ import annotations

from typing import Any

__all__ = ["make_leaderboard", "newest_window"]


def _entry(cat: str, asp: str, entry: dict[str, Any]) -> dict[str, Any]:
    """One leaderboard cell: the winning row, flattened to the grid contract.

    Its own function so the contract is one readable list. Every key here is
    read by the front-end, and three of them -- ``leak``, ``training_release``
    and ``evaluation_set_id`` -- are what stop a cell being read as a number
    without a frame.
    """
    return {
        "category": cat,
        "aspect": asp,
        "primary": entry["primary"],
        "primary_metric": entry["primary_metric"],
        "f_micro_w": entry.get("f_micro_w"),
        "f_micro": entry.get("f_micro"),
        "fmax_w": entry.get("fmax_w"),
        "fmax": entry["fmax"],
        "precision_w": entry.get("precision_w"),
        "recall_w": entry.get("recall_w"),
        "precision": entry["precision"],
        "recall": entry["recall"],
        "coverage_w": entry.get("coverage_w"),
        "coverage": entry["coverage"],
        "embedding_config_id": entry["embedding_config_id"],
        "k": entry["k"],
        "stage": entry["stage"],
        "evaluation_result_id": entry["evaluation_result_id"],
        "evaluation_set_id": entry["evaluation_set_id"],
        # F-METHOD-EVAL-SURFACE provenance for the winning cell.
        "frame": entry.get("frame"),
        "temporal_window": entry.get("temporal_window"),
        "arms_enabled": entry.get("arms_enabled"),
        "leakage_role": entry.get("leakage_role"),
        # The winner carries its verdict. A leaderboard that showed a
        # contaminated cell on top without saying so is the worst case
        # this badge exists for.
        "leak": entry.get("leak"),
        "training_release": entry.get("training_release"),
        "prediction_set_status": entry.get("prediction_set_status"),
        "self_hit_rate": entry.get("self_hit_rate"),
    }


def make_leaderboard(
    rows: list[dict[str, Any]],
    categories: tuple[str, ...] | list[str],
    aspects: tuple[str, ...] | list[str],
    *,
    window: Any,
) -> list[dict[str, Any]]:
    """Cross-model best-cell leaderboard per ``(category, aspect)`` cell.

    One entry per cell that has a winner, in canonical order, with the
    winning embedding / stage / K. The winner is the highest IA-weighted
    ``primary`` (``f_micro_w``, fmax fallback for legacy rows): a *best-cell*
    maximum, to be labelled as such and never as the headline number
    (winner's-curse, FIX-METRIC-IA). ``window`` is required and filters the
    rows first; the module docstring says what that prevents.
    """
    leaderboard: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        if str(r.get("evaluation_set_id")) != str(window):
            continue
        lkey = (r["category"], r["aspect"])
        cur = leaderboard.get(lkey)
        if cur is None or r["primary"] > cur["primary"]:
            leaderboard[lkey] = r
    return [
        _entry(cat, asp, entry)
        for cat in categories
        for asp in aspects
        if (entry := leaderboard.get((cat, asp))) is not None
    ]


def newest_window(
    eval_sets_payload: list[dict[str, Any]], rows: list[dict[str, Any]]
) -> str | None:
    """The most recently created window that actually has rows, or ``None``.

    Rows first: a window with no result cannot be what a grid is about.
    Among those that have rows the newest wins, because a reader with no
    opinion is asking what the current answer is. ``None`` when nothing has
    rows, and the grids then come back empty, which is the honest shape.

    ``None`` ALSO when no candidate carries a ``created_at``, and that is not
    a technicality. Without the date this sorted on empty strings, kept the
    payload's own order and returned whatever was first: measured on the
    deployed API, ``ea94e57e`` (02:17) over ``ea0f4334`` (17:39), so the
    screen printed the older window's nine cells as the current answer.
    Picking arbitrarily is the defect this function exists to remove, so an
    incomplete payload returns nothing and the empty grid makes it visible.
    """
    with_rows = {str(r["evaluation_set_id"]) for r in rows}
    dated = [
        e for e in eval_sets_payload if str(e.get("id")) in with_rows and e.get("created_at")
    ]
    if not dated:
        return None
    return str(max(dated, key=lambda e: str(e["created_at"]))["id"])
