"""Read only: does every embedding config id still describe its own row?

An embedding config id is derived, never assigned, so any machine can rebuild
it from what the row holds. This rederives every live id and says which
derivation reproduces it:

- ``recipe``: ``derive_embedding_config_id`` over the row's identity fields,
  the scheme of every config except an ingested method's;
- ``name``: ``register_ingested_method.config_id_for(model_name)``, the scheme
  of an ingested method, by design: an arm read from a file has no recipe of
  ours to hash, so its name is what identifies it.

A row that neither reproduces is reported with the omission that does: the
smallest set of defaulted identity fields that, hashed as None, gives the live
id. That names the producer defect behind the row instead of only its symptom.
The id the row's own recipe derives is printed beside it as the CANDIDATE
repair target, a candidate because the row can be wrong too: ``22166300``
holds a ``max_length`` of 1022 that its pass never applied, so its target is
derived after correcting that field, not from the row as it stands.

Opens a READ ONLY transaction with a statement timeout. The table is small
(fourteen rows on 2026-11-04) and is read once, in creation order.

Usage:
    python scripts/audit_embedding_config_ids.py

Exit status 0 when every id is reproduced, 1 otherwise.
"""

from __future__ import annotations

import sys
import uuid
from itertools import combinations
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402

from protea.core.embedding_identity import IDENTITY_FIELDS, derive_embedding_config_id  # noqa: E402
from protea.core.operations.register_ingested_method import config_id_for  # noqa: E402
from protea.infrastructure.orm.models.embedding.embedding_config import (  # noqa: E402
    INGESTED_METHOD_BACKEND,
    OMISSION_MEANS,
)
from protea.infrastructure.session import build_session_factory  # noqa: E402
from protea.infrastructure.settings import load_settings  # noqa: E402

_COLUMNS = ("id", "created_at", "model_backend", *sorted(set(IDENTITY_FIELDS) - {"model_backend"}))


def omission_that_mints(row: dict[str, Any], live: uuid.UUID) -> tuple[str, ...] | None:
    """The smallest set of defaulted identity fields that, as None, derives ``live``.

    These are the fields the ``before_insert`` listener resolves today and a
    column default used to fill after the hash, i.e. the only ones whose
    omission could ever have reached a hash as None.
    """
    fields = sorted(OMISSION_MEANS)
    for size in range(1, len(fields) + 1):
        for subset in combinations(fields, size):
            if derive_embedding_config_id({**row, **dict.fromkeys(subset)}) == live:
                return subset
    return None


def classify(row: dict[str, Any]) -> tuple[str, uuid.UUID, tuple[str, ...] | None]:
    """Which derivation reproduces the row's id, what its recipe derives, and why not."""
    live = uuid.UUID(str(row["id"]))
    recipe = {f: row[f] for f in IDENTITY_FIELDS}
    derived = derive_embedding_config_id(recipe)
    if derived == live:
        return "recipe", derived, None
    if row["model_backend"] == INGESTED_METHOD_BACKEND and config_id_for(row["model_name"]) == live:
        return "name", derived, None
    return "nothing", derived, omission_that_mints(recipe, live)


def _rows() -> list[dict[str, Any]]:
    factory = build_session_factory(load_settings(PROJECT_ROOT).db_url)
    with factory() as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout = '30s'"))
        result = session.execute(
            text(f"SELECT {', '.join(_COLUMNS)} FROM embedding_config ORDER BY created_at")
        )
        rows = [dict(r) for r in result.mappings()]
        session.rollback()
    return rows


def main() -> int:
    rows = _rows()
    unreproduced = 0
    print(f"{'id':<9} {'backend':<20} {'created':<11} reproduced by")
    for row in rows:
        how, derived, omission = classify(row)
        line = (
            f"{str(row['id'])[:8]:<9} {row['model_backend']:<20} "
            f"{row['created_at']:%Y-%m-%d}  {how}"
        )
        if how == "nothing":
            unreproduced += 1
            minted = ", ".join(omission) if omission else "no omission of a defaulted field"
            line += (
                f"\n    live id      {row['id']}\n    minted with  {minted} hashed as None"
                f"\n    row derives  {derived}  (candidate target; check the row first)"
            )
        print(line)
    print(f"\n{len(rows) - unreproduced} of {len(rows)} ids reproduced")
    return 1 if unreproduced else 0


if __name__ == "__main__":
    sys.exit(main())
