"""A cell with several generations must not report the best of them.

The board folds every EvaluationResult into one winner per
(model, K, stage, category, aspect). Until now the winner was the highest
``primary``. That is a maximum over repeated measurements of a single quantity:
it can only move up as a cell is recomputed, it never moves down, and it gets
more optimistic the more often we rerun. After the 2026-08-18 recompute 140
cells held more than one generation, so the bias was live rather than
theoretical.

The rule is now trust first, then recency, and never score. These tests pin
that, including the tie cases, because a rule whose result depends on the order
the database happened to return rows is not a rule.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from protea.api.routers.benchmark import _GENERATION_RANK, _prefers

T0 = datetime(2026, 8, 1, tzinfo=UTC)
T1 = T0 + timedelta(days=17)


def _row(status: str, primary: float, created: datetime | None = T0) -> dict:
    return {"prediction_set_status": status, "primary": primary, "_created_at": created}


class TestTrustBeatsScore:
    def test_the_current_generation_wins_even_when_it_scores_lower(self) -> None:
        """The whole point: a recompute that lowers a number must still win.

        The damaged runs scored HIGHER in some cells, because misattributed
        neighbours are not uniformly worse. Preferring the maximum would keep
        exactly those.
        """
        assert _prefers(_row("current", 0.30), _row("damaged", 0.90)) is True

    def test_a_damaged_generation_never_displaces_a_clean_one(self) -> None:
        assert _prefers(_row("damaged", 0.99), _row("superseded", 0.10)) is False

    def test_incomplete_is_the_least_trusted(self) -> None:
        assert _prefers(_row("incomplete", 0.99), _row("damaged", 0.10)) is False

    def test_an_unlabelled_set_sits_between_clean_and_damaged(self) -> None:
        """Sets predating the audit carry no verdict, for or against."""
        assert _prefers(_row("", 0.10), _row("damaged", 0.99)) is True
        assert _prefers(_row("", 0.99), _row("superseded", 0.10)) is False

    def test_an_unknown_status_is_treated_as_unlabelled_rather_than_trusted(self) -> None:
        """A status this code has not heard of must not outrank a known-good one."""
        assert _prefers(_row("something-new", 0.99), _row("current", 0.10)) is False


class TestRecencyOnlyBreaksTiesWithinAGeneration:
    def test_the_newer_of_two_equally_trusted_rows_wins(self) -> None:
        assert _prefers(_row("current", 0.10, T1), _row("current", 0.90, T0)) is True

    def test_the_older_one_does_not(self) -> None:
        assert _prefers(_row("current", 0.90, T0), _row("current", 0.10, T1)) is False

    def test_an_exact_tie_keeps_the_incumbent(self) -> None:
        """Otherwise the board depends on the order the database returned rows."""
        assert _prefers(_row("current", 0.90, T0), _row("current", 0.10, T0)) is False

    def test_a_missing_timestamp_does_not_displace_an_incumbent(self) -> None:
        assert _prefers(_row("current", 0.99, None), _row("current", 0.10, T0)) is False


class TestTheEmptyCase:
    def test_the_first_row_always_wins(self) -> None:
        assert _prefers(_row("damaged", 0.0), None) is True


def test_every_status_the_audit_writes_has_a_rank() -> None:
    """The labels come from the prediction_set audit; a new one must be ranked.

    If the audit grows a status and nobody ranks it here it silently lands in
    the unlabelled tier, which is a judgement nobody made.
    """
    for status in ("current", "superseded", "damaged", "incomplete"):
        assert status in _GENERATION_RANK
