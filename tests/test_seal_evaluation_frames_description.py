"""Tests that the seal describes the column it actually writes.

The operation catalogue and the API hand a reader the ``description`` string
and nothing else. When that string named ``evaluation_result.frame`` it named
the harness label: a ``varchar(8)`` under a check constraint admitting exactly
``lafa`` or ``internal``, a column that would have rejected the digest this
operation computes. A reader who believed it would have gone looking for the
seal in a column that never holds one, and a reviewer who believed it would
have thought the operation commits the very failure its own module docstring
says it exists to end rather than to commit.

These pin the description to the SQL rather than to a remembered string. The
column is read out of the ``UPDATE`` statement, so the day the seal moves to
another column the assertion follows it instead of quietly going stale.
"""

from __future__ import annotations

import re

from protea.core.operations.seal_evaluation_frames import _SEAL, SealEvaluationFramesOperation

#: The column the operation writes, taken from the statement that writes it.
#: Parsed rather than hardcoded so this file cannot be the thing that is wrong.
_SEALED_COLUMN = re.search(r"SET\s+(\w+)\s*=", str(_SEAL), re.IGNORECASE)


def test_the_statement_still_names_one_column_to_write() -> None:
    """The parse below is only evidence if it actually parsed something."""
    assert _SEALED_COLUMN is not None, str(_SEAL)


def test_the_description_names_the_column_the_sql_writes() -> None:
    """The catalogue entry is the only account of this operation most readers
    will ever see, so it has to agree with the statement it summarises."""
    assert _SEALED_COLUMN is not None
    column = _SEALED_COLUMN.group(1)
    description = SealEvaluationFramesOperation.description
    assert f"evaluation_result.{column}" in description, description


def test_no_user_facing_string_promises_to_write_the_harness_label() -> None:
    """``frame`` is not a shorter spelling of ``frame_digest``: it is a
    different column, closed by check constraint to two harness names. A string
    that says the operation stamps it is describing a write the database would
    refuse, so neither the description nor either payload summary may say so.
    """
    op = SealEvaluationFramesOperation()
    for text in (
        op.description,
        op.summarize_payload({"dry_run": True}),
        op.summarize_payload({"dry_run": False}),
    ):
        assert re.search(r"evaluation_result\.frame(?!_)", text) is None, text
