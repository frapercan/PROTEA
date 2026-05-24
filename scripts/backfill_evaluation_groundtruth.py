"""ONE-TIME BACKFILL: idempotent materialization of ground-truth evaluation sets.

THIS SCRIPT IS A ONE-TIME BACKFILL AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

This script materializes the ground-truth parquet for every EvaluationSet row lacking a
groundtruth_uri (a condition affecting rows created before the generate_evaluation_set
operation began persisting deltas to the artifact store). For each row, it is fully idempotent:
skips if already set, recomputes evaluation data via the same code path the worker uses
(compute_evaluation_data for matching snapshots, compute_evaluation_data_reconciled otherwise),
serializes to parquet, uploads to the artifact store, and updates the column. No re-run needed
or desired; backfill is complete after first execution.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select  # noqa: E402

from protea.core.evaluation import (  # noqa: E402
    compute_evaluation_data,
    compute_evaluation_data_reconciled,
    groundtruth_key_for,
    serialize_evaluation_data_to_parquet,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet  # noqa: E402
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet  # noqa: E402
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402
from protea.infrastructure.storage import get_artifact_store  # noqa: E402


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--only-id", help="Backfill a single EvaluationSet UUID and exit")
    p.add_argument("--dry-run", action="store_true",
                   help="Report what would be backfilled, don't compute or upload")
    return p.parse_args()


def _resolve_pivot(eval_set: EvaluationSet, ann_old: AnnotationSet,
                   ann_new: AnnotationSet) -> uuid.UUID:
    stats = eval_set.stats or {}
    pivot_raw = stats.get("pivot_ontology_snapshot_id")
    if pivot_raw:
        return uuid.UUID(str(pivot_raw))
    return ann_new.ontology_snapshot_id if ann_new else ann_old.ontology_snapshot_id


def _backfill_one(session, store, eval_set: EvaluationSet) -> str | None:
    if eval_set.groundtruth_uri:
        return None

    ann_old = session.get(AnnotationSet, eval_set.old_annotation_set_id)
    ann_new = session.get(AnnotationSet, eval_set.new_annotation_set_id)
    if ann_old is None or ann_new is None:
        raise RuntimeError(
            f"EvaluationSet {eval_set.id}: missing AnnotationSet "
            f"old={eval_set.old_annotation_set_id} new={eval_set.new_annotation_set_id}"
        )

    pivot_id = _resolve_pivot(eval_set, ann_old, ann_new)
    same_snapshot = (
        ann_old.ontology_snapshot_id == ann_new.ontology_snapshot_id == pivot_id
    )

    print(
        f"  {eval_set.id} mode={'same' if same_snapshot else 'reconciled':<10} "
        f"old={str(ann_old.id)[:8]} new={str(ann_new.id)[:8]} pivot={str(pivot_id)[:8]}"
    )

    if same_snapshot:
        data = compute_evaluation_data(session, ann_old.id, ann_new.id, pivot_id)
    else:
        data = compute_evaluation_data_reconciled(
            session, ann_old.id, ann_new.id,
            ann_old.ontology_snapshot_id, ann_new.ontology_snapshot_id, pivot_id,
        )

    key = groundtruth_key_for(eval_set.id)
    with tempfile.TemporaryDirectory(prefix="protea_eval_gt_backfill_") as tmp:
        local = Path(tmp) / "groundtruth.parquet"
        serialize_evaluation_data_to_parquet(data, local)
        size = local.stat().st_size
        uri = store.put(key, local)
    eval_set.groundtruth_uri = uri
    print(
        f"    delta_proteins={data.delta_proteins} nk={data.nk_proteins} "
        f"lk={data.lk_proteins} pk={data.pk_proteins} parquet={size:,}B → {uri}"
    )
    return uri


def main() -> None:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: backfill scripts are disabled by default (one-time use only).",
            "Set PROTEA_ALLOW_BACKFILL=1 to enable.",
            sep="\n",
            file=sys.stderr,
        )
        sys.exit(1)

    a = _args()
    settings = load_settings(PROJECT_ROOT)
    factory = build_session_factory(settings.db_url)
    store = get_artifact_store(settings)

    with session_scope(factory) as session:
        if a.only_id:
            target_id = uuid.UUID(a.only_id)
            row = session.get(EvaluationSet, target_id)
            if row is None:
                raise SystemExit(f"EvaluationSet {target_id} not found")
            rows = [row]
        else:
            stmt = (
                select(EvaluationSet)
                .where(EvaluationSet.groundtruth_uri.is_(None))
                .order_by(EvaluationSet.created_at.asc())
            )
            rows = list(session.execute(stmt).scalars())

        print(f"[backfill] {len(rows)} EvaluationSet(s) without groundtruth_uri")
        if not rows:
            return

        if a.dry_run:
            for row in rows:
                print(f"  [dry-run] would backfill {row.id}")
            return

        ok = 0
        for row in rows:
            try:
                _backfill_one(session, store, row)
                session.flush()
                ok += 1
            except Exception as exc:
                print(f"  ERROR backfilling {row.id}: {exc}")
                session.rollback()
                raise

        print(f"[backfill] {ok}/{len(rows)} backfilled")


if __name__ == "__main__":
    sys.exit(main())
