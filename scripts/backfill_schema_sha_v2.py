"""ONE-TIME BACKFILL: idempotent population of schema_sha_v2 fingerprints (ADR D10).

THIS SCRIPT IS A ONE-TIME BACKFILL AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

This script populates schema_sha_v2 on dataset and reranker_model rows using the canonical
feature registry's column list as it exists at backfill runtime, matching ADR D10 policy and
allowing inference to switch to canonical fingerprints without re-training boosters. For each
Dataset row, it skips if already set (unless --force), computes the v2 SHA from
protea_contracts.compute_schema_sha, and persists. For each RerankerModel, it copies the
schema_sha_v2 from the linked Dataset if available, otherwise computes from canonical features.
The script commits every --batch-size rows for recoverability. Fully idempotent by construction:
skipping already-populated rows, making re-runs unnecessary and redundant.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select  # noqa: E402

from protea.infrastructure.orm.models.embedding.dataset import Dataset  # noqa: E402
from protea.infrastructure.orm.models.embedding.reranker_model import (  # noqa: E402
    RerankerModel,
)
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402


def _canonical_v2_sha() -> str:
    """Return the v2 SHA for the current canonical feature registry.

    Imports happen lazily so the module loads even if
    ``protea_contracts`` is not installed (the script will exit 1 with a
    clear error in that case).
    """
    try:
        from protea_contracts import compute_schema_sha
    except ImportError as exc:
        raise RuntimeError(
            "protea_contracts.compute_schema_sha unavailable; install the "
            "protea-contracts package before running this backfill."
        ) from exc

    from protea.core.parquet_export import _registry_feature_names

    canonical = _registry_feature_names()
    if not canonical:
        raise RuntimeError(
            "Feature registry returned an empty column list. "
            "Refusing to backfill with a meaningless digest."
        )
    return compute_schema_sha(canonical)


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    p.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Commit after this many rows are updated (default: 100).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be backfilled; do not write.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite ``schema_sha_v2`` even when it is already set.",
    )
    return p.parse_args()


def _backfill_datasets(
    session, canonical_sha: str, batch_size: int, dry_run: bool, force: bool
) -> tuple[int, int]:
    """Walk ``Dataset`` rows in chunks. Returns ``(updated, skipped)``."""
    updated = 0
    skipped = 0
    chunk_updates_pending = 0
    rows = session.scalars(select(Dataset).order_by(Dataset.created_at)).all()
    print(f"[dataset] inspecting {len(rows)} row(s)")
    for row in rows:
        if row.schema_sha_v2 is not None and not force:
            skipped += 1
            continue
        if dry_run:
            print(
                f"[dataset] dry-run  id={row.id} name={row.name!r}  "
                f"sha_v1={row.schema_sha!r} -> sha_v2={canonical_sha!r}"
            )
        else:
            row.schema_sha_v2 = canonical_sha
            chunk_updates_pending += 1
            if chunk_updates_pending >= batch_size:
                session.commit()
                print(f"[dataset] committed {chunk_updates_pending} rows")
                chunk_updates_pending = 0
        updated += 1
    if not dry_run and chunk_updates_pending > 0:
        session.commit()
        print(f"[dataset] committed {chunk_updates_pending} rows (final)")
    return updated, skipped


def _backfill_reranker_models(
    session, canonical_sha: str, batch_size: int, dry_run: bool, force: bool
) -> tuple[int, int]:
    """Walk ``RerankerModel`` rows in chunks. Returns ``(updated, skipped)``."""
    updated = 0
    skipped = 0
    chunk_updates_pending = 0
    rows = session.scalars(select(RerankerModel).order_by(RerankerModel.created_at)).all()
    print(f"[reranker_model] inspecting {len(rows)} row(s)")
    for row in rows:
        if row.schema_sha_v2 is not None and not force:
            skipped += 1
            continue
        # Prefer the dataset's v2 SHA when the booster is linked to one
        # whose v2 column is already populated; otherwise fall back to
        # the canonical SHA. Dataset-derived value avoids drift when the
        # registry has evolved since the booster was trained.
        new_sha = canonical_sha
        if row.dataset_id is not None:
            ds = session.get(Dataset, row.dataset_id)
            if ds is not None and ds.schema_sha_v2 is not None:
                new_sha = ds.schema_sha_v2
        if dry_run:
            print(
                f"[reranker_model] dry-run  id={row.id} name={row.name!r}  "
                f"feature_schema_sha={row.feature_schema_sha!r} -> sha_v2={new_sha!r}"
            )
        else:
            row.schema_sha_v2 = new_sha
            chunk_updates_pending += 1
            if chunk_updates_pending >= batch_size:
                session.commit()
                print(f"[reranker_model] committed {chunk_updates_pending} rows")
                chunk_updates_pending = 0
        updated += 1
    if not dry_run and chunk_updates_pending > 0:
        session.commit()
        print(f"[reranker_model] committed {chunk_updates_pending} rows (final)")
    return updated, skipped


def main() -> int:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: backfill scripts are disabled by default (one-time use only).",
            "Set PROTEA_ALLOW_BACKFILL=1 to enable.",
            sep="\n",
            file=sys.stderr,
        )
        return 1

    args = _args()
    try:
        canonical_sha = _canonical_v2_sha()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"canonical schema_sha_v2 = {canonical_sha}")

    settings = load_settings(PROJECT_ROOT)
    factory = build_session_factory(settings.db_url)
    print(f"db = {settings.db_url}")

    with session_scope(factory) as session:
        ds_updated, ds_skipped = _backfill_datasets(
            session, canonical_sha, args.batch_size, args.dry_run, args.force
        )
        rm_updated, rm_skipped = _backfill_reranker_models(
            session, canonical_sha, args.batch_size, args.dry_run, args.force
        )

    print(
        f"summary: dataset updated={ds_updated} skipped={ds_skipped}  "
        f"reranker_model updated={rm_updated} skipped={rm_skipped}  "
        f"dry_run={args.dry_run} force={args.force}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
