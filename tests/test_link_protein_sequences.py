"""``link_protein_sequences``: the link comes from the photos, latest cut first, and only fills gaps."""

from __future__ import annotations

from unittest.mock import MagicMock

from protea.core.operations.link_protein_sequences import (
    COUNT_SQL,
    LINK_SQL,
    UNLINKED_SQL,
    LinkProteinSequencesOperation,
)


def _emit(*_args: object) -> None:
    return None


def _session(before: int, linked: int, after: int | None) -> MagicMock:
    """A session that answers the three statements with the figures given."""
    unlinked = iter([before, after])
    session = MagicMock()

    def execute(statement: object) -> MagicMock:
        answer = MagicMock()
        if statement is UNLINKED_SQL:
            answer.scalar_one.return_value = next(unlinked)
        elif statement is COUNT_SQL:
            answer.scalar_one.return_value = linked
        elif statement is LINK_SQL:
            answer.rowcount = linked
        return answer

    session.execute.side_effect = execute
    return session


class TestTheLink:
    def test_it_links_and_reports_what_is_left(self) -> None:
        # The figures of the clean store and dev on 2026-11-24.
        session = _session(2_758_480, 2_758_264, 216)
        result = LinkProteinSequencesOperation().execute(session, {}, emit=_emit).result
        assert result["linked"] == 2_758_264
        assert result["unlinked_after"] == 216
        session.commit.assert_called_once()

    def test_a_dry_run_counts_and_writes_nothing(self) -> None:
        session = _session(2_758_480, 2_758_264, None)
        result = (
            LinkProteinSequencesOperation().execute(session, {"dry_run": True}, emit=_emit).result
        )
        assert result["linked"] == 2_758_264
        assert result["unlinked_after"] == 216
        assert all(call.args[0] is not LINK_SQL for call in session.execute.call_args_list)
        session.commit.assert_not_called()

    def test_the_sequence_is_the_latest_cut_s_and_only_gaps_are_filled(self) -> None:
        sql = str(LINK_SQL)
        assert "ORDER BY m.accession, p.cut_release DESC" in sql
        assert "s.sequence_hash = m.sequence_md5" in sql
        assert "protein.sequence_id IS NULL" in sql
