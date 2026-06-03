"""Audit script for the protea.dead-letter queue (F-OPS-JOBS.3).

Connects to RabbitMQ, peeks all DLQ messages without consuming them,
groups them by operation and source queue, and prints a structured
summary.  Intended for one-off baseline audits committed to docs/incidents/.

Usage::

    poetry run python scripts/admin/dlq_audit.py [--amqp-url AMQP_URL]
                                                  [--max-peek N]
                                                  [--output-json FILE]

Output is printed to stdout in human-readable form.  If --output-json is
provided the raw groups JSON is also written to that file.

Purge recommendation: messages with operation=store_embeddings and
age > 7 days are candidates for safe removal.  The recommended SQL to
inspect the corresponding Job rows (best-effort, these messages have no
job_id in body) is printed at the end.  This script does NOT auto-purge;
use POST /v1/admin/dlq/purge (admin role) for that.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from protea.services.dlq_manager import dlq_summary  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit the protea.dead-letter queue")
    parser.add_argument(
        "--amqp-url",
        default=os.environ.get("PROTEA_AMQP_URL", "amqp://guest:guest@localhost:5672/"),
        help="RabbitMQ AMQP URL (default: $PROTEA_AMQP_URL or localhost guest)",
    )
    parser.add_argument(
        "--max-peek",
        type=int,
        default=5000,
        help="Maximum messages to inspect (default: 5000)",
    )
    parser.add_argument(
        "--output-json",
        metavar="FILE",
        help="Write raw groups JSON to FILE",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    print(f"Connecting to {args.amqp_url!r} ...")
    result = dlq_summary(args.amqp_url, max_peek=args.max_peek)

    now = datetime.now(tz=UTC).isoformat()
    total_in_queue = result["queue_message_count"]
    total_peeked = result["total_peeked"]
    groups = result["groups"]

    print(f"\n=== DLQ Audit Report ({now}) ===")
    print(f"Queue depth  : {total_in_queue:>8,}")
    print(f"Messages peeked: {total_peeked:>6,}")
    print(f"Groups identified: {len(groups)}")
    print()
    print(f"{'Operation':<35} {'Source queue':<35} {'Age':<8} {'Count':>8}")
    print("-" * 90)
    for g in groups:
        print(
            f"{g['operation']:<35} {g['first_death_queue']:<35} "
            f"{g['age_bucket']:<8} {g['count']:>8,}"
        )

    print()

    # Identify the dominant historic store_embeddings cohort
    se_groups = [g for g in groups if g["operation"] == "store_embeddings"]
    se_total = sum(g["count"] for g in se_groups)
    if se_total > 0:
        pct = se_total / total_peeked * 100 if total_peeked else 0
        print(f"store_embeddings cohort: {se_total:,} messages ({pct:.1f}% of peeked)")
        print("These are historic failures from prior re-ingestion events.")
        print("They have no active job_id and are safe to purge.")
        print()
        print("Recommended purge (dry-run first):")
        print("  POST /v1/admin/dlq/purge")
        print('  {"operation": "store_embeddings", "dry_run": true}')
        print()
        print("Proposed SQL to inspect corresponding job rows (best-effort):")
        print("  SELECT id, status, error_code, created_at")
        print("    FROM job")
        print("   WHERE operation = 'store_embeddings'")
        print("     AND created_at < now() - interval '7 days'")
        print("     AND status = 'FAILED';")

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "audited_at": now,
                    "queue_message_count": total_in_queue,
                    "total_peeked": total_peeked,
                    "groups": groups,
                },
                indent=2,
            )
        )
        print(f"Raw JSON written to {out}")


if __name__ == "__main__":
    main()
