#!/usr/bin/env python
"""Seed the alignment cache from alignments already stored in go_prediction.

Every prediction row already carries the pair's NW/SW metrics next to the two
accessions, so the work is done; it is simply not addressable by sequence. This
walks existing rows, resolves both accessions to their sequence hashes, and
writes one cache entry per distinct pair.

Accession to hash is resolved through the sequence table rather than trusted
from the row: if a protein's sequence changed since the prediction was made,
the stored alignment belongs to the OLD sequence and must not be filed under
the new hash. Those rows are counted and skipped, not guessed at.

Usage:
    python scripts/backfill_alignment_cache.py [--since-hours N] [--limit N]
                                               [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from protea.core.alignment_cache import ALIGNMENT_FIELDS, store  # noqa: E402
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402

log = logging.getLogger("backfill_alignment_cache")

# go_prediction is large, so walk it in slices and commit as we go: a backfill
# that has to finish in one transaction is a backfill that gets killed.
_PAGE = 50_000


def _rows_sql(since_hours: int | None) -> str:
    window = (
        "AND ps.created_at > now() - interval ':hours hours'".replace(
            ":hours", str(int(since_hours))
        )
        if since_hours
        else ""
    )
    fields = ", ".join(f"g.{f}" for f in ALIGNMENT_FIELDS)
    return f"""
        SELECT DISTINCT ON (qs.sequence_hash, rs.sequence_hash)
               qs.sequence_hash AS qh, rs.sequence_hash AS rh, {fields}
        FROM go_prediction g
        JOIN prediction_set ps ON ps.id = g.prediction_set_id
        JOIN protein qp ON qp.accession = g.protein_accession
        JOIN sequence qs ON qs.id = qp.sequence_id
        JOIN protein rp ON rp.accession = g.ref_protein_accession
        JOIN sequence rs ON rs.id = rp.sequence_id
        WHERE g.identity_nw IS NOT NULL {window}
        ORDER BY qs.sequence_hash, rs.sequence_hash, ps.created_at DESC
        LIMIT :page OFFSET :offset
    """


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None, help="stop after N pairs")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    factory = build_session_factory(os.environ["PROTEA_DB_URL"])
    seeded = offset = 0
    sql = text(_rows_sql(args.since_hours))
    while True:
        with session_scope(factory) as session:
            rows = session.execute(sql, {"page": _PAGE, "offset": offset}).mappings().all()
            if not rows:
                break
            batch = {
                (r["qh"], r["rh"]): {f: r[f] for f in ALIGNMENT_FIELDS}
                for r in rows
                if all(r[f] is not None for f in ALIGNMENT_FIELDS)
            }
            if not args.dry_run:
                store(session, batch)
            seeded += len(batch)
            offset += _PAGE
            log.info("seeded %s pairs (offset %s)", f"{seeded:,}", f"{offset:,}")
        if args.limit and seeded >= args.limit:
            break

    log.info("%s %s pairs", "would seed" if args.dry_run else "seeded", f"{seeded:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
