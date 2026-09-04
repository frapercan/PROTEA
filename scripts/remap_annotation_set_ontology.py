"""ONE-TIME FIX: re-express an AnnotationSet's terms in the ontology it belongs to.

THIS SCRIPT IS A ONE-TIME FIX AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

GOA 220 (published 2024-04-16) was loaded on 2026-08-18, three weeks after the
other releases, and inherited the ontology GOA 226 used: ``releases/2025-03-16``,
eleven months into its own future. Nothing rejected it, because at the time
``load_goa_annotations`` stored whatever snapshot id the payload named.

Evaluation survives the mismatch — ``compute_evaluation_data_reconciled`` works
in ``go_id`` text space and takes an explicit per-side snapshot — but prediction
cannot: every predicted term *is* a donor annotation's ``go_term_id``, so the
bank's graph is the candidate graph and there is no parameter to translate. The
stored ids have to move.

This is deliberately a script and not a registered operation. It runs once, on
one row, on the machine that owns the state: an operation would pay for a
payload class, a catalog entry, a queue, and a slot in every worker's memory
without collecting retry, progress, or distribution in return.

The remap is content-preserving by construction — it rewrites ``go_term_id`` to
the row with the same ``go_id`` under the target snapshot — and refuses unless
every term maps. Nothing derived needs recomputing: the IA set and both
evaluation sets already read the corpus under the target ontology.

Usage:
    PROTEA_ALLOW_BACKFILL=1 python scripts/remap_annotation_set_ontology.py \
        --annotation-set-id <uuid> --target-snapshot-id <uuid> [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402

from protea.core.operations._gaf_header import (  # noqa: E402
    assert_not_newer_than_declared,
    declared_release,
    fetch_header,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet  # noqa: E402
from protea.infrastructure.orm.models.annotation.ontology_snapshot import (  # noqa: E402
    OntologySnapshot,
)
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--annotation-set-id", required=True, help="AnnotationSet to re-express")
    p.add_argument("--target-snapshot-id", required=True, help="OntologySnapshot it belongs to")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run every check and report, but roll back instead of committing",
    )
    return p.parse_args()


def _check_declared_ontology(session, ann_set: AnnotationSet, target: OntologySnapshot) -> None:
    """Refuse a target newer than the GO build the source GAF declares.

    Reuses the same rule ``load_goa_annotations`` enforces, so a correction
    cannot install a binding the loader would have rejected.
    """
    gaf_url = (ann_set.meta or {}).get("gaf_url")
    if not gaf_url:
        raise SystemExit(
            f"AnnotationSet {ann_set.id} records no gaf_url in meta, so the ontology it "
            "was built against cannot be verified. Refusing to guess."
        )
    checked = assert_not_newer_than_declared(
        gaf_url=gaf_url,
        obo_version=target.obo_version,
        declared=declared_release(fetch_header(gaf_url, 120)),
        allow_unverified=False,
    )
    print(f"  GAF declares {checked['declared']}, target is {checked['bound']} — ok")


def _check_every_term_maps(session, set_id: uuid.UUID, target_id: uuid.UUID) -> None:
    """Refuse unless every distinct ``go_id`` exists under the target snapshot."""
    rows = (
        session.execute(
            text(
                "SELECT DISTINCT t.go_id FROM protein_go_annotation p "
                "JOIN go_term t ON t.id = p.go_term_id "
                "WHERE p.annotation_set_id = :s AND NOT EXISTS ("
                "  SELECT 1 FROM go_term n WHERE n.go_id = t.go_id "
                "  AND n.ontology_snapshot_id = :target) LIMIT 20"
            ),
            {"s": str(set_id), "target": str(target_id)},
        )
        .scalars()
        .all()
    )
    if rows:
        raise SystemExit(
            f"{len(rows)}+ term(s) have no counterpart under the target snapshot, so the "
            f"remap would lose annotations: {', '.join(rows[:20])}. Refusing."
        )


def _count(session, set_id: uuid.UUID) -> int:
    return session.execute(
        text("SELECT count(*) FROM protein_go_annotation WHERE annotation_set_id = :s"),
        {"s": str(set_id)},
    ).scalar_one()


def _remap(session, set_id: uuid.UUID, target_id: uuid.UUID, before: int) -> None:
    """Rewrite the term ids and rebind the set, then prove nothing moved but ids."""
    session.execute(
        text(
            "UPDATE protein_go_annotation p SET go_term_id = n.id "
            "FROM go_term o, go_term n "
            "WHERE p.annotation_set_id = :s AND o.id = p.go_term_id "
            "AND n.go_id = o.go_id AND n.ontology_snapshot_id = :target"
        ),
        {"s": str(set_id), "target": str(target_id)},
    )
    session.execute(
        text("UPDATE annotation_set SET ontology_snapshot_id = :target WHERE id = :s"),
        {"s": str(set_id), "target": str(target_id)},
    )
    after = _count(session, set_id)
    outside = session.execute(
        text(
            "SELECT count(*) FROM protein_go_annotation p JOIN go_term t ON t.id = p.go_term_id "
            "WHERE p.annotation_set_id = :s AND t.ontology_snapshot_id <> :target"
        ),
        {"s": str(set_id), "target": str(target_id)},
    ).scalar_one()
    if after != before or outside != 0:
        raise SystemExit(
            f"VERIFICATION FAILED, rolling back: {before} -> {after} annotations, "
            f"{outside} still outside the target snapshot."
        )
    print(f"  {after:,} annotations re-expressed, 0 outside the target — verified")


def main() -> None:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: one-time fix scripts are disabled by default.",
            "Set PROTEA_ALLOW_BACKFILL=1 to enable.",
            sep="\n",
            file=sys.stderr,
        )
        sys.exit(1)

    a = _args()
    set_id = uuid.UUID(a.annotation_set_id)
    target_id = uuid.UUID(a.target_snapshot_id)

    settings = load_settings(PROJECT_ROOT)
    with session_scope(build_session_factory(settings.db_url)) as session:
        ann_set = session.get(AnnotationSet, set_id)
        target = session.get(OntologySnapshot, target_id)
        if ann_set is None:
            raise SystemExit(f"AnnotationSet {set_id} not found")
        if target is None:
            raise SystemExit(f"OntologySnapshot {target_id} not found")

        current = session.get(OntologySnapshot, ann_set.ontology_snapshot_id)
        before = _count(session, set_id)
        print(
            f"[remap] {ann_set.source} {ann_set.source_version} — {before:,} annotations\n"
            f"  {current.obo_version if current else '?'} -> {target.obo_version}"
        )
        if ann_set.ontology_snapshot_id == target_id:
            print("  already bound to the target; nothing to do")
            return

        _check_declared_ontology(session, ann_set, target)
        _check_every_term_maps(session, set_id, target_id)
        print("  every distinct go_id has a counterpart under the target — ok")

        _remap(session, set_id, target_id, before)

        if a.dry_run:
            session.rollback()
            print("  [dry-run] verified and rolled back; nothing was committed")


if __name__ == "__main__":
    sys.exit(main())
