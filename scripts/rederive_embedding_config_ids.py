"""ONE-TIME FIX: move assigned config ids onto their content-derived ones.

THIS SCRIPT IS A ONE-TIME FIX AND MUST NOT BE RE-RUN OR SCHEDULED IN NORMAL OPERATION.
It is safe to leave in the repository for provenance and historical reference only.

``embedding_identity`` says an embedding config id is a hash of the recipe, not
a random number, so that two machines building the same recipe land on the same
id without talking to each other. It also says the registry has been split that
way once. It is still split: eight configs carry v5 ids that match their recipe
exactly, and five carry v4 ids that nothing can reproduce -- ``ankh_base@d79``,
``esm2_3b``, and all three learned rung2 encoders. The module that would have
prevented it has no callers anywhere in the tree.

Those five are not a cosmetic problem. They are whole nodes of axis A, and the
campaign runs on two machines: the compute node rebuilding any of those recipes
would derive a different id, store its vectors under it, and both halves would
look correct.

Collisions are checked before anything moves. Two configs deriving to one id
would mean the recipe does not capture what varies between them, which is a
defect to report rather than to write into the database.

Usage:
    PROTEA_ALLOW_BACKFILL=1 python scripts/rederive_embedding_config_ids.py [--dry-run]
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

from protea.core.embedding_identity import derive_embedding_config_id  # noqa: E402
from protea.infrastructure.orm.models.embedding.embedding_config import (  # noqa: E402
    EmbeddingConfig,
)
from protea.infrastructure.session import build_session_factory, session_scope  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402

#: Every column pointing at an embedding_config id, so a move updates all of
#: them or none. Leaving one out would orphan whatever it holds while the rest
#: looked consistent.
_REFERENCES = (
    ("sequence_embedding", "embedding_config_id"),
    ("prediction_set", "embedding_config_id"),
    ("reranker_model", "embedding_config_id"),
    ("dataset", "embedding_config_id"),
    ("embedding_config", "derived_from_embedding_config_id"),
)


def _columns(session) -> list[str]:
    """The table's own column list, read rather than remembered.

    The copy below has to name every column because one of them -- the id --
    takes a different value, and a hand-kept list would silently drop whatever
    a later migration adds.
    """
    rows = session.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'embedding_config' ORDER BY ordinal_position"
        )
    )
    return [r[0] for r in rows]


def _plan(session) -> list[tuple[EmbeddingConfig, uuid.UUID]]:
    """Configs whose id is not what their recipe derives, paired with that id."""
    rows = session.query(EmbeddingConfig).all()
    derived = {c.id: derive_embedding_config_id(c) for c in rows}

    seen: dict[uuid.UUID, list[str]] = {}
    for c in rows:
        seen.setdefault(derived[c.id], []).append(c.display_name or str(c.id))
    clashes = {k: v for k, v in seen.items() if len(v) > 1}
    if clashes:
        lines = "; ".join(f"{k} <- {', '.join(v)}" for k, v in clashes.items())
        raise SystemExit(
            "refusing: some recipes derive to the same id, which means the recipe "
            f"does not capture what varies between them -- {lines}"
        )

    return [(c, derived[c.id]) for c in rows if derived[c.id] != c.id]


def _move(session, cols: list[str], old: uuid.UUID, new: uuid.UUID) -> dict[str, int]:
    """Copy the row under its derived id, repoint every reference, drop the old.

    The copy is written first so no foreign key ever points at nothing, and the
    original goes last, once nothing refers to it. Doing it the other way round
    would need the constraints dropped, which is a larger blast radius than the
    problem.
    """
    names = ", ".join(cols)
    values = ", ".join(":new" if c == "id" else c for c in cols)
    session.execute(
        text(
            f"INSERT INTO embedding_config ({names}) "
            f"SELECT {values} FROM embedding_config WHERE id = :old"
        ),
        {"new": str(new), "old": str(old)},
    )
    moved: dict[str, int] = {}
    for table, column in _REFERENCES:
        res = session.execute(
            text(f"UPDATE {table} SET {column} = :new WHERE {column} = :old"),
            {"new": str(new), "old": str(old)},
        )
        if res.rowcount:
            moved[f"{table}.{column}"] = res.rowcount
    session.execute(text("DELETE FROM embedding_config WHERE id = :old"), {"old": str(old)})
    return moved


def main() -> None:
    if not os.getenv("PROTEA_ALLOW_BACKFILL"):
        print(
            "ERROR: one-time fix scripts are disabled by default.",
            "Set PROTEA_ALLOW_BACKFILL=1 to enable.",
            sep="\n",
            file=sys.stderr,
        )
        sys.exit(1)

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="Report, do not commit")
    a = ap.parse_args()

    settings = load_settings(PROJECT_ROOT)
    with session_scope(build_session_factory(settings.db_url)) as session:
        plan = _plan(session)
        if not plan:
            print("[rederive] every config id already matches its recipe")
            return

        cols = _columns(session)
        total = session.query(EmbeddingConfig).count()
        print(f"[rederive] {len(plan)} of {total} carry an id nothing can reproduce")
        for cfg, new in plan:
            name, old = cfg.display_name or "?", cfg.id
            moved = _move(session, cols, old, new)
            refs = ", ".join(f"{k}={v:,}" for k, v in moved.items()) or "sin referencias"
            print(f"  {name:<32} {str(old)[:8]} -> {str(new)[:8]}  {refs}")

        session.flush()
        left = session.execute(
            text("SELECT count(*) FROM embedding_config WHERE substring(id::text, 15, 1) <> '5'")
        ).scalar_one()
        if left:
            raise SystemExit(f"VERIFICATION FAILED, rolling back: {left} id(s) still not derived")

        if a.dry_run:
            session.rollback()
            print("  [dry-run] verificado y revertido; no se ha confirmado nada")
            return
        print(f"  {len(plan)} movidas; toda id de configuracion deriva de su receta")


if __name__ == "__main__":
    sys.exit(main())
