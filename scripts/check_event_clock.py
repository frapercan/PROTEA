#!/usr/bin/env python
"""Refuse a shared history whose stamps disagree with the order they were written in.

``job_event.id`` is a sequence, so the order rows were INSERTED is known
independently of any clock. A stamp that goes backwards while the id goes
forwards therefore cannot be a slow job, a pause, or a retry: it is a clock
that moved, and the only way it reaches this table is a process stamping its
own rows.

That is what this looks for, and it is exact rather than heuristic. It needs no
threshold on job duration, no knowledge of which machine wrote what, and no
guess at how large a skew to expect.

Run against the live store it answers one question: has anything sealed a row
with a clock nobody checked. Over 798,081 events on 2026-08-29 it found two,
both on the same compute node, both 7,200 seconds, eighteen days apart. The
cause was a dual boot: Windows writes local time to the real-time clock and
Linux reads it as UTC, so the offset is a timezone and not a drift.

Since the migration that moved these stamps to the server, this check should
never fire again. That is the point of keeping it: if it does fire, something
found its way around the server default, and the day it happens is a better
time to learn that than eighteen days later.

Exit codes: 0 clean, 1 skew found, 2 could not check.
"""

from __future__ import annotations

import os
import sys

_QUERY = """
WITH ordered AS (
    SELECT id, job_id, ts,
           lag(ts) OVER (ORDER BY id) AS previous_ts
    FROM job_event
)
SELECT job_id, ts, previous_ts,
       EXTRACT(EPOCH FROM (previous_ts - ts)) AS backwards_seconds
FROM ordered
WHERE ts < previous_ts - interval '60 seconds'
ORDER BY backwards_seconds DESC
"""


def main() -> int:
    url = os.environ.get("PROTEA_DB_URL")
    if not url:
        print("PROTEA_DB_URL is not set, so there is nothing to check.")
        print("This reads a live store; it is not a unit test and has no fixture.")
        return 2

    from sqlalchemy import create_engine, text

    with create_engine(url).connect() as connection:
        found = connection.execute(text(_QUERY)).fetchall()
        total = connection.execute(text("SELECT count(*) FROM job_event")).scalar_one()

    if not found:
        print(f"event clock OK: {total} events, none stamped out of insertion order.")
        return 0

    print(f"CLOCK SKEW: {len(found)} of {total} events carry a stamp that goes")
    print("backwards while the insertion sequence goes forwards.\n")
    for job_id, ts, previous_ts, backwards in found:
        print(f"  job {job_id}")
        print(f"    {previous_ts}  then  {ts}")
        print(f"    {backwards:.1f} s backwards\n")
    print("A stamp cannot move backwards on its own. Some process is stamping its")
    print("own rows rather than letting Postgres do it, or its clock moved while")
    print("it held the connection. Find which machine wrote these jobs.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
