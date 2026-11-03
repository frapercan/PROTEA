"""Which parents a child's progress still counts for, read off the predicate itself.

``update_parent_progress`` advanced a parent only ``WHERE status = RUNNING``, so a
coordinator the reaper had given up on stopped counting: its children kept
arriving, their rows kept landing, and the parent sat FAILED for ever with a
progress number frozen at whatever it was when the reaper got to it.

The reaper's FAILED is not a failure. A deferred coordinator clears its lease
once its own work is done, and ``stale_job_reaper`` marks it FAILED when its
children go quiet for longer than its window; on 2026-10-30 it did that to
``b185a6d0`` at exactly six hours, with progress ``0/7`` and
``error_code = lease_expired``. A node that cedes its card for two hours is
enough to produce it. So that one code, and only that one, keeps the parent
counting.

WHY THE BEHAVIOUR IS PINNED IN A ``_pg`` FILE AND THE PREDICATE HERE. The real
test drives ``update_parent_progress`` against Postgres, because ``job.id`` is a
``postgresql.UUID`` and the statement uses ``RETURNING``, so sqlite cannot hold
the table. That suite drops and recreates the schema, and on the machine that
owns every artifact this project has it is refused by ``conftest``'s guard --
correctly: the live store has been wiped that way more than once. These tests
therefore pin the predicate, which needs no database at all and is what a
mutation would reach for first.
"""

from __future__ import annotations

from sqlalchemy.dialects import postgresql

from protea.core.contracts.parent_progress import REAPED, _STILL_COUNTS


def _sql() -> str:
    """The predicate as Postgres will read it, with its literals inlined."""
    return str(
        _STILL_COUNTS.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )


def test_a_running_parent_counts() -> None:
    """The case that always worked, and must keep working."""
    assert "job.status = 'RUNNING'" in _sql()


def test_a_reaped_parent_counts_too() -> None:
    """The case this change exists for.

    Asserted on the pair and not on ``FAILED`` alone, because admitting FAILED
    on its own is the mistake this predicate is written to avoid.
    """
    sql = _sql()
    assert "job.status = 'FAILED'" in sql
    assert f"job.error_code = '{REAPED}'" in sql
    assert sql.index("job.status = 'FAILED'") < sql.index("job.error_code"), (
        "the status and the code have to be conjoined, so a FAILED parent is "
        "admitted only when the reaper is the reason"
    )


def test_no_other_failure_counts() -> None:
    """A real failure keeps its children out.

    The codes in production are exception names -- ``HoldoutTouchedError``,
    ``OperationalError``, ``S3Error`` -- and a parent that raised is a run that
    is over, so a batch still in flight must not advance it or close it.
    """
    sql = _sql()
    assert sql.count("job.error_code") == 1, (
        "more than one code admitted; the predicate must name exactly the "
        "reaper's and nothing else"
    )
    assert "IS NOT NULL" not in sql.upper()
    for real in ("HoldoutTouchedError", "OperationalError", "S3Error", "ValueError"):
        assert real not in sql


def test_cancelled_never_counts() -> None:
    """A person saying stop is not the reaper being wrong.

    CANCELLED is deliberate, and its children's work is not wanted however
    idempotent the write is.
    """
    assert "CANCELLED" not in _sql()


def test_the_reaper_and_this_module_name_the_same_code() -> None:
    """One string, two files, and nothing to keep them equal but this.

    ``stale_job_reaper`` writes the code and this module reads it. They are in
    different modules on purpose -- the reaper must not import a core contract --
    so the only thing that can catch a rename is an assertion that they match.
    """
    from protea.workers.stale_job_reaper import LEASE_EXPIRED_FAILED_EVENT

    assert REAPED == "lease_expired"
    assert LEASE_EXPIRED_FAILED_EVENT == "job.lease_expired", (
        "the reaper's event name moved; check that the error_code it writes "
        f"is still {REAPED!r}"
    )
