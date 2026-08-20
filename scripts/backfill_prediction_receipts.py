"""Give already-stored prediction sets the receipt they were created without.

Sets made before ``run_receipt`` existed carry ``meta={}``. The parameters
that produced them are still recoverable, because the coordinator emitted
``predict_go_terms.dispatching`` naming the set it had just created, and
that event carries the job id. The job still holds the payload.

That link is exact, not a heuristic. It matters, because the obvious
alternative is a natural-key join on
(embedding_config, annotation_set, ontology, K, query_set), and that key
is not unique: the same combination was run more than once, and the
colliding jobs disagree about the search backend. A receipt built that
way would look authoritative and be wrong.

Idempotent: a set that already carries ``job_id`` is left alone unless
``--force`` is given. The write merges rather than replaces, because some
sets already hold a self-hit assessment from the damage probe and that is
not this script's to discard. Read-only with ``--dry-run``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from protea_contracts import PredictGOTermsPayload
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms._receipt import run_receipt
from protea.infrastructure.database.engine import create_engine
from protea.infrastructure.settings import load_settings

#: The dispatching event is the only exact set-to-job link in the record.
_LINKED = text(
    """
    SELECT ps.id                AS prediction_set_id,
           j.id                 AS job_id,
           j.payload            AS payload
    FROM prediction_set ps
    JOIN LATERAL (
        SELECT je.job_id
        FROM job_event je
        WHERE je.event = 'predict_go_terms.dispatching'
          AND je.fields ->> 'prediction_set_id' = ps.id::text
        LIMIT 1
    ) link ON TRUE
    JOIN job j ON j.id = link.job_id
    WHERE (:force OR NOT (COALESCE(ps.meta, '{}'::jsonb) ? 'job_id'))
    ORDER BY ps.created_at
    """
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report without writing")
    ap.add_argument("--force", action="store_true", help="rewrite receipts that exist")
    ap.add_argument(
        "--db-url",
        default=None,
        help="override the configured database; needed when running from a git worktree, "
        "which has no local settings of its own",
    )
    args = ap.parse_args()

    written = skipped = 0
    db_url = args.db_url or load_settings(Path(__file__).resolve().parents[1]).db_url
    with Session(create_engine(db_url)) as session:
        rows = session.execute(_LINKED, {"force": args.force}).mappings().all()
        print(f"{len(rows)} prediction set(s) with an exact job link")
        for row in rows:
            payload = dict(row["payload"] or {})
            # The query list is the bulk of the payload and says nothing
            # about the regime; dropping it keeps validation cheap.
            payload.pop("query_accessions", None)
            try:
                p = PredictGOTermsPayload.model_validate(payload)
            except Exception as exc:  # noqa: BLE001 - report and continue
                print(f"  skip {row['prediction_set_id']}: payload will not validate ({exc})")
                skipped += 1
                continue
            receipt = run_receipt(p, row["job_id"])
            if args.dry_run:
                print(f"  would write {row['prediction_set_id']}: {json.dumps(receipt)[:120]}")
            else:
                # Merge, never replace. Some sets already carry a
                # self-hit assessment written by the damage probe, and it
                # is not this script's to discard.
                session.execute(
                    text(
                        "UPDATE prediction_set "
                        "SET meta = COALESCE(meta, '{}'::jsonb) || CAST(:m AS jsonb) "
                        "WHERE id = :i"
                    ),
                    {"m": json.dumps(receipt), "i": row["prediction_set_id"]},
                )
            written += 1
        if not args.dry_run:
            session.commit()

    verb = "would write" if args.dry_run else "wrote"
    print(f"{verb} {written}, skipped {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
