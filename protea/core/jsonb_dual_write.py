"""Env-flag helper for the T3.1 ``GOPrediction.predictions_jsonb`` dual-write.

T3.1 scaffolds the prediction-tuple JSONB dual-write: writers continue
filling the typed prediction columns (``go_term_id``, ``distance``,
``evidence_code``) and additionally serialise the same tuple into the
``predictions_jsonb`` blob when the environment opt-in is set. This
keeps the scaffolding inert in production until a human-coordinated
deploy window flips the flag.

Environment variable
--------------------

``PROTEA_GO_PREDICTION_JSONB_WRITE_ENABLED``
    Truthy values (``1``, ``true``, ``yes``, ``on``; case-insensitive)
    enable the dual-write. Anything else (unset, empty, ``0``,
    ``false``...) leaves the writer skipping the JSONB column so
    ``predictions_jsonb`` stays ``NULL`` on new rows. Default off.

Design notes
------------

The helper API is intentionally tiny so every writer site can opt in
with a single import + one-line call::

    from protea.core.jsonb_dual_write import maybe_jsonb

    row["predictions_jsonb"] = maybe_jsonb(
        [(pred["go_term_id"], pred["distance"], pred.get("evidence_code"))]
    )

``maybe_jsonb`` returns ``None`` when the flag is off (so the row
dict carries an explicit ``NULL`` for SQL) and a compact dict when on.
The compact shape is a single-level dict with two keys:

* ``predictions``: a list of ``{"go_term_id": int, "score": float,
  "evidence": str | None}`` records, preserving caller order.
* ``count``: len(predictions), redundant but useful for cheap GIN
  filtering and sanity checks (``WHERE predictions_jsonb @>
  '{"count": 5}'`` etc.).

The list shape lets a single row carry one or more prediction tuples
without re-shaping the helper at the T3.2 backfill / T3.3 reader
cut-over.
"""
from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any

#: Environment variable name; exported for tests / docs.
JSONB_DUAL_WRITE_ENV: str = "PROTEA_GO_PREDICTION_JSONB_WRITE_ENABLED"

#: Truthy values (case-insensitive). Matches the existing
#: ``PROTEA_OTEL_ENABLED`` convention in ``protea.infrastructure.telemetry``
#: so operators learn one rule, not several.
_TRUTHY: frozenset[str] = frozenset({"1", "true", "yes", "on"})


def _as_bool(value: str | None) -> bool:
    """Return ``True`` when ``value`` is one of the canonical truthy strings."""
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY


def is_jsonb_dual_write_enabled(env: dict[str, str] | None = None) -> bool:
    """Return ``True`` iff the env opt-in is set to a truthy value.

    ``env`` defaults to :data:`os.environ`; the explicit parameter is
    a test hook so callers can verify the helper without mutating
    process state.
    """
    env_map: Any = os.environ if env is None else env
    return _as_bool(env_map.get(JSONB_DUAL_WRITE_ENV))


def maybe_jsonb(
    predictions: Sequence[tuple[int, float, str | None]],
    *,
    env: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Serialise a list of (go_term_id, score, evidence) tuples.

    Returns ``None`` when the dual-write env opt-in is off so the
    caller can assign the result directly to a row dict / ORM
    attribute without an explicit guard at every writer site.

    When the flag is on, returns the compact JSONB shape documented in
    the module docstring. Empty input still returns a well-formed dict
    (``{"predictions": [], "count": 0}``) so downstream consumers can
    rely on the keys being present.
    """
    if not is_jsonb_dual_write_enabled(env=env):
        return None

    records: list[dict[str, Any]] = [
        {
            "go_term_id": int(go_term_id),
            "score": float(score),
            "evidence": evidence,
        }
        for go_term_id, score, evidence in predictions
    ]
    return {"predictions": records, "count": len(records)}
