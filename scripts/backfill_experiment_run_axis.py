"""ONE-TIME BACKFILL: idempotent derivation of FARM-EXP.1 axis tuple columns.

THIS SCRIPT IS A ONE-TIME BACKFILL AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

This script backfills the 9 axis columns on experiment_run rows added by the FARM-EXP.1 migration,
deriving values from legacy config/provenance JSONB overlays and computing the canonical
axis_tuple_shortid. Fully idempotent: skips rows where axis_tuple_shortid is already set, writes
only to NULL columns, and re-running after a successful pass yields zero UPDATE statements. Handles
partial derivation (when some axis values are unresolvable, shortid is left NULL but resolved
columns are still written), detects cross-row shortid collisions, and supports --force to recompute
every row if provenance schema changes. No re-run needed after completion; safe to leave in place
only for historical documentation of the migration.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from protea.core.axis_tuple import (  # noqa: E402
    CANONICAL_AXIS_KEYS,
    axis_tuple_shortid,
)
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402

DEFAULT_PROPAGATION = "none"
# Backfill operates on JSONB + the 9 FARM-EXP.1 columns only; the
# original draft used raw ``text()`` here to sidestep an
# enum-name vs enum-value mismatch on ``experiment_run_status`` (rows
# loaded through the ORM raised ``LookupError`` because SQLAlchemy
# defaulted to member *names* while the Postgres enum stores lowercase
# *values*). That bug is fixed in FIX-EXP-RUN-ENUM via
# ``values_callable`` on the ``ExperimentRun.status`` mapping, so a
# follow-up cleanup may swap this script onto the ORM. The raw SQL
# stays for now because it is independently a fine fit for a one-shot
# backfill (narrow projection, no relationship hydration, predictable
# UPDATE batches).


def _first_present(payload: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    """Return the first non-empty value found by walking ``payload``.

    Each path is a tuple of keys; the function descends through nested
    dicts. Returns ``None`` when every path is missing / falsy.
    """
    for path in paths:
        cur: Any = payload
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok and cur not in (None, "", [], {}):
            return cur
    return None


def derive_axis_payload(
    config: dict[str, Any] | None,
    provenance: dict[str, Any] | None,
) -> dict[str, Any]:
    """Derive the 8 axis values from a row's ``config`` + ``provenance``.

    Returns a dict keyed by :data:`CANONICAL_AXIS_KEYS`. Values that
    cannot be resolved are ``None`` (and the shortid step will then
    leave the row partially filled).
    """
    cfg = config or {}
    prov = provenance or {}
    merged = {"config": cfg, "provenance": prov}

    plm = _first_present(
        merged,
        ("config", "plm"),
        ("provenance", "plm"),
        ("config", "embedding_config", "plm"),
    )
    k_value = _first_present(merged, ("config", "k"), ("provenance", "k"))
    reranker_spec_id = _first_present(
        merged,
        ("config", "reranker_spec_id"),
        ("provenance", "reranker_spec_id"),
        ("config", "model", "id"),
    )
    feature_schema_sha = _first_present(
        merged,
        ("config", "feature_schema_sha"),
        ("provenance", "feature_schema_sha"),
    )
    eval_set_name = _first_present(
        merged,
        ("config", "eval_set_name"),
        ("provenance", "eval_set_name"),
        ("config", "evaluation_set"),
    )
    eval_set_manifest_sha = _first_present(
        merged,
        ("config", "eval_set_manifest_sha"),
        ("provenance", "eval_set_manifest_sha"),
    )
    propagation = _first_present(
        merged,
        ("config", "propagation"),
        ("provenance", "propagation"),
    )
    ensemble_spec = _first_present(
        merged,
        ("config", "ensemble_spec"),
        ("provenance", "ensemble_spec"),
    )

    return {
        "plm": plm,
        "k": int(k_value) if k_value is not None else None,
        "reranker_spec_id": reranker_spec_id,
        "feature_schema_sha": feature_schema_sha,
        "eval_set_name": eval_set_name,
        "eval_set_manifest_sha": eval_set_manifest_sha,
        "propagation": propagation or DEFAULT_PROPAGATION,
        "ensemble_spec": ensemble_spec,
    }


#: Axis keys that *must* be non-NULL for the shortid to be meaningful.
#: ``propagation`` defaults to ``"none"`` so it always resolves, and
#: ``ensemble_spec`` is legitimately ``None`` for single-model runs;
#: both still participate in the digest (so two cells with different
#: ensembles produce different shortids) but they need not block the
#: shortid from being computed.
SHORTID_REQUIRED_KEYS: tuple[str, ...] = (
    "plm",
    "k",
    "reranker_spec_id",
    "feature_schema_sha",
    "eval_set_name",
    "eval_set_manifest_sha",
)


def _compute_shortid_if_complete(payload: dict[str, Any]) -> str | None:
    """Return the shortid only when the 6 required keys are resolved.

    ``propagation`` always resolves (defaulted to ``"none"``) and
    ``ensemble_spec`` is legitimately ``None`` for non-ensemble runs;
    both still feed the digest but they are not gating.
    """
    if any(payload.get(k) in (None, "", [], {}) for k in SHORTID_REQUIRED_KEYS):
        return None
    return axis_tuple_shortid(payload)


_SELECT_SQL = text(
    """
    SELECT id::text AS id, name, config, provenance,
           plm, k, reranker_spec_id, feature_schema_sha,
           eval_set_name, eval_set_manifest_sha,
           propagation, ensemble_spec, axis_tuple_shortid
    FROM experiment_run
    ORDER BY created_at
    """
)

_UPDATE_SQL = text(
    """
    UPDATE experiment_run
    SET plm = :plm,
        k = :k,
        reranker_spec_id = :reranker_spec_id,
        feature_schema_sha = :feature_schema_sha,
        eval_set_name = :eval_set_name,
        eval_set_manifest_sha = :eval_set_manifest_sha,
        propagation = :propagation,
        ensemble_spec = CAST(:ensemble_spec AS jsonb),
        axis_tuple_shortid = :axis_tuple_shortid
    WHERE id = CAST(:id AS uuid)
    """
)


def _row_to_update_params(
    row_id: str,
    derived: dict[str, Any],
    shortid: str | None,
) -> dict[str, Any]:
    import json as _json

    ensemble = derived.get("ensemble_spec")
    return {
        "id": row_id,
        "plm": derived.get("plm"),
        "k": derived.get("k"),
        "reranker_spec_id": derived.get("reranker_spec_id"),
        "feature_schema_sha": derived.get("feature_schema_sha"),
        "eval_set_name": derived.get("eval_set_name"),
        "eval_set_manifest_sha": derived.get("eval_set_manifest_sha"),
        "propagation": derived.get("propagation"),
        "ensemble_spec": _json.dumps(ensemble) if ensemble is not None else None,
        "axis_tuple_shortid": shortid,
    }


def _process_session(
    session: Session,
    *,
    force: bool,
    batch_size: int,
) -> tuple[int, int, list[str]]:
    """Walk every row; return (scanned, updated, warnings)."""
    scanned = 0
    updated = 0
    warnings: list[str] = []
    seen_shortids: dict[str, str] = {}

    rows = session.execute(_SELECT_SQL).mappings().all()

    for row in rows:
        scanned += 1
        existing_shortid = row["axis_tuple_shortid"]
        if existing_shortid is not None and not force:
            continue

        derived = derive_axis_payload(row["config"], row["provenance"])
        # Preserve existing non-null values unless --force is set.
        merged = dict(derived)
        if not force:
            for key in CANONICAL_AXIS_KEYS:
                current = row[key]
                if current is not None:
                    merged[key] = current

        shortid = _compute_shortid_if_complete(merged)

        # Idempotency gate: skip the UPDATE if every target column would
        # be set to the value it already carries. Cheaper than a no-op
        # write and lets ``--commit`` re-runs report ``updated=0``.
        new_values = {**{k: merged.get(k) for k in CANONICAL_AXIS_KEYS},
                      "axis_tuple_shortid": shortid}
        current_values = {k: row[k] for k in new_values}
        if new_values == current_values:
            if shortid is None and existing_shortid is None:
                warnings.append(
                    f"row {row['id']} ({row['name']}): partial backfill "
                    "(missing axis keys -> shortid left NULL)"
                )
            continue

        if shortid is None and existing_shortid is None:
            # Partial-only update is still useful; warn but proceed.
            warnings.append(
                f"row {row['id']} ({row['name']}): partial backfill "
                "(missing axis keys -> shortid left NULL)"
            )

        # Detect cross-row collisions before we ever flush.
        if shortid is not None:
            prior = seen_shortids.get(shortid)
            if prior is not None and prior != row["id"]:
                raise RuntimeError(
                    "shortid collision: rows "
                    f"{prior} and {row['id']} both resolve to "
                    f"{shortid}; inspect their provenance."
                )
            seen_shortids[shortid] = row["id"]

        params = _row_to_update_params(row["id"], merged, shortid)
        session.execute(_UPDATE_SQL, params)
        updated += 1
        if updated % batch_size == 0:
            session.flush()

    return scanned, updated, warnings


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--commit", action="store_true", help="Persist updates (default is dry-run).")
    p.add_argument("--dry-run", action="store_true", help="Force dry-run mode.")
    p.add_argument("--force", action="store_true", help="Recompute every row.")
    p.add_argument("--batch-size", type=int, default=200)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: backfill scripts are disabled by default (one-time use only).",
            "Set PROTEA_ALLOW_BACKFILL=1 to enable.",
            sep="\n",
            file=sys.stderr,
        )
        return 1

    args = _parse_args(argv)
    dry_run = args.dry_run or not args.commit

    settings = load_settings(PROJECT_ROOT)
    factory = build_session_factory(settings.db_url)

    try:
        with session_scope(factory) as session:
            scanned, updated, warnings = _process_session(
                session, force=args.force, batch_size=args.batch_size
            )
            if dry_run:
                session.rollback()
            else:
                session.commit()
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    mode = "dry-run" if dry_run else "commit"
    print(f"backfill OK ({mode}): scanned={scanned} updated={updated}")
    for warn in warnings:
        print(f"  warn: {warn}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
