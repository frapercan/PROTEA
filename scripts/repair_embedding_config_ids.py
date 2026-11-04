"""ONE-TIME FIX: move three producer configs onto the ids their own recipes derive.

THIS SCRIPT IS A ONE-TIME FIX AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

Measured on 2026-11-04 by ``scripts/audit_embedding_config_ids.py``: three configs
hold ids their rows do not derive. Their producers left ``chunk_size``,
``chunk_overlap`` and ``embedding_scale`` out of the recipe, the ``before_insert``
listener hashed the three as None, and the INSERT stored the column defaults.

THE PLAN IS PINNED, NOT DERIVED. ``scripts/rederive_embedding_config_ids.py`` moves a
row to whatever its recipe derives as it stands, and for ``22166300`` that is wrong:
the row also records ``max_length`` 1022, a truncation ``encode_residue_vocabulary``
never applied, since it refuses any sequence whose backend rows differ from its
residues. Its target is derived after correcting that field. Each move below names
its target, and the script refuses unless the corrected row derives exactly it.

THE REFERENCES ARE READ FROM THE CATALOG, NOT LISTED. The older script's list
predates ``residue_code`` and ``exploration_link``, and a move that left one table
pointing at the old id would orphan whatever it holds while the rest looked right.

HOW A MOVE RUNS, resumable at every step:

1. the row is copied under its target id, with the corrections applied;
2. every reference is repointed in batches, committed one by one, so a stopped run
   continues where it stopped and both rows exist throughout;
3. the old row is deleted once nothing references it;
4. the target is re-read and must derive its own id.

Run it with the passes that write these configs stopped. While the copy and the
original coexist, they share a ``model_name``, and a producer looking its config up
by name finds two rows and fails rather than choosing one.

Shard keys of ``residue_code`` keep the old id in their path. A key is an address
the row stores, verified by its sha256, and no reader rebuilds it from the config
id, so rewriting the store would move bytes to say nothing new. Nothing may list
shards by a config prefix.

Usage:
    PROTEA_ALLOW_BACKFILL=1 python scripts/repair_embedding_config_ids.py          # dry run
    PROTEA_ALLOW_BACKFILL=1 python scripts/repair_embedding_config_ids.py --apply
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402

from protea.core.embedding_identity import IDENTITY_FIELDS, derive_embedding_config_id  # noqa: E402
from protea.infrastructure.session import build_session_factory  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402


@dataclass(frozen=True)
class Move:
    """One config, the id its corrected recipe derives, and the corrections."""

    old: uuid.UUID
    new: uuid.UUID
    corrections: dict[str, Any] = field(default_factory=dict)
    why: str = ""


PLAN: tuple[Move, ...] = (
    Move(
        old=uuid.UUID("22166300-a178-599e-835c-afe1c007feb5"),
        new=uuid.UUID("43193409-de70-526b-ae8d-43892a744049"),
        corrections={"max_length": None},
        why="residue vocabulary codes: full length, never truncated at 1022",
    ),
    Move(
        old=uuid.UUID("76792dc2-52fc-501f-9370-cf7a58e8336c"),
        new=uuid.UUID("34446081-a1c0-5a73-bf8e-04da532839f6"),
        why="learned code over e7ba37da, cut 211: the row's recipe is right",
    ),
    Move(
        old=uuid.UUID("9d72dc44-7eed-517c-b45d-82b6e9f73bb1"),
        new=uuid.UUID("d4b4c057-762c-5e24-b4a1-6a542490275e"),
        why="learned code over e7ba37da, cut 227: the row's recipe is right",
    ),
)

#: Every foreign key that points at ``embedding_config.id``, as the catalog has it.
_REFERENCES_SQL = text(
    """
    SELECT cl.relname AS table_name, att.attname AS column_name
    FROM pg_constraint con
    JOIN pg_class cl ON cl.oid = con.conrelid
    JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY (con.conkey)
    WHERE con.contype = 'f' AND con.confrelid = 'embedding_config'::regclass
    ORDER BY 1, 2
    """
)


def corrected_target(row: dict[str, Any], move: Move) -> uuid.UUID:
    """The id the row's recipe derives once the move's corrections are applied."""
    recipe = {f: row[f] for f in IDENTITY_FIELDS}
    recipe.update({k: v for k, v in move.corrections.items() if k in IDENTITY_FIELDS})
    return derive_embedding_config_id(recipe)


def refuse_unless_it_derives(row: dict[str, Any], move: Move) -> None:
    derived = corrected_target(row, move)
    if derived != move.new:
        raise SystemExit(
            f"refusing {move.old}: corrected, its recipe derives {derived}, not the pinned "
            f"{move.new}. The row is not the one this plan was measured on."
        )


def _row(session: Any, config_id: uuid.UUID) -> dict[str, Any] | None:
    found = (
        session.execute(
            text("SELECT * FROM embedding_config WHERE id = :id"), {"id": str(config_id)}
        )
        .mappings()
        .one_or_none()
    )
    return dict(found) if found is not None else None


def _references(session: Any) -> list[tuple[str, str]]:
    return [(r.table_name, r.column_name) for r in session.execute(_REFERENCES_SQL)]


def _count(session: Any, table: str, column: str, config_id: uuid.UUID) -> int:
    return int(
        session.execute(
            text(f"SELECT count(*) FROM {table} WHERE {column} = :id"),  # noqa: S608 - catalog names
            {"id": str(config_id)},
        ).scalar_one()
    )


def _columns(session: Any) -> list[str]:
    """The table's own columns, read rather than remembered, so the copy drops none."""
    rows = session.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'embedding_config' ORDER BY ordinal_position"
        )
    )
    return [r[0] for r in rows]


def _copy(session: Any, move: Move) -> None:
    """Copy the row under the target id with the corrections, unless a run already did.

    Done in SQL, column by column from the original, so every type travels as the
    database holds it. Only the null correction this plan needs is supported, and
    anything else is refused rather than guessed at.
    """
    if _row(session, move.new) is not None:
        return
    if any(value is not None for value in move.corrections.values()):
        raise SystemExit(f"{move.old}: only corrections to NULL are supported")
    columns = _columns(session)
    expressions = [
        "CAST(:new AS uuid)" if c == "id" else "NULL" if c in move.corrections else c
        for c in columns
    ]
    session.execute(
        text(
            f"INSERT INTO embedding_config ({', '.join(columns)}) "  # noqa: S608 - catalog names
            f"SELECT {', '.join(expressions)} FROM embedding_config WHERE id = CAST(:old AS uuid)"
        ),
        {"new": str(move.new), "old": str(move.old)},
    )


def _repoint(session: Any, table: str, column: str, move: Move, batch: int) -> int:
    """Move every reference in ``table`` in batches, committing each. Returns rows moved."""
    moved = 0
    while True:
        n = session.execute(
            text(
                f"UPDATE {table} SET {column} = :new WHERE ctid IN ("  # noqa: S608
                f"SELECT ctid FROM {table} WHERE {column} = :old LIMIT :n)"
            ),
            {"new": str(move.new), "old": str(move.old), "n": batch},
        ).rowcount
        session.commit()
        moved += n
        if n < batch:
            return moved


def _report(session: Any, references: list[tuple[str, str]], move: Move) -> dict[str, int]:
    counts = {f"{t}.{c}": _count(session, t, c, move.old) for t, c in references}
    return {k: v for k, v in counts.items() if v}


def _apply(session: Any, references: list[tuple[str, str]], move: Move, batch: int) -> None:
    row = _row(session, move.old)
    if row is not None:
        refuse_unless_it_derives(row, move)
        _copy(session, move)
        session.commit()
        for table, column in references:
            n = _repoint(session, table, column, move, batch)
            if n:
                print(f"    {table}.{column}: {n:,} moved")
        left = _report(session, references, move)
        if left:
            raise SystemExit(f"refusing to delete {move.old}: still referenced by {left}")
        session.execute(text("DELETE FROM embedding_config WHERE id = :id"), {"id": str(move.old)})
        session.commit()
    target = _row(session, move.new)
    if target is None or derive_embedding_config_id(target) != move.new:
        raise SystemExit(f"VERIFICATION FAILED: {move.new} does not derive its own id")
    print(f"    {move.new} derives its own id")


def main() -> int:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print("ERROR: one-time fix scripts are disabled by default.", file=sys.stderr)
        print("Set PROTEA_ALLOW_BACKFILL=1 to enable.", file=sys.stderr)
        return 1
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true", help="Move the rows; without it, report")
    ap.add_argument("--batch", type=int, default=50_000, help="Rows repointed per commit")
    args = ap.parse_args()

    factory = build_session_factory(load_settings(PROJECT_ROOT).db_url)
    with factory() as session:
        if not args.apply:
            # A dry run cannot write even by mistake.
            session.execute(text("SET TRANSACTION READ ONLY"))
        references = _references(session)
        print(f"[repair] {len(references)} references to embedding_config: {references}")
        for move in PLAN:
            row = _row(session, move.old)
            state = "to move" if row is not None else "already moved or absent"
            print(f"  {str(move.old)[:8]} -> {move.new} ({move.why}): {state}")
            if row is not None:
                refuse_unless_it_derives(row, move)
                print(f"    references: {_report(session, references, move) or 'none'}")
            if args.apply:
                _apply(session, references, move, args.batch)
        if not args.apply:
            session.rollback()
            print("[repair] dry run: nothing written. Pass --apply to move the rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
