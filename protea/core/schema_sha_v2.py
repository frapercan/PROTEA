"""``schema_sha_v2`` dual-write feature flag (T1.6 of master plan v3).

The :mod:`protea.infrastructure.orm.models` Dataset, RerankerModel, and
ExperimentRun rows carry both the legacy ``schema_sha`` /
``feature_schema_sha`` column and the canonical
``schema_sha_v2`` column added by ADR D10. Until the production
cut-over window, write paths only fill the legacy column; once the
operator flips ``PROTEA_SCHEMA_SHA_V2_WRITE_ENABLED=true`` new rows
dual-write both columns.

The flag defaults to ``false`` (off) so a normal deploy of this slice
is a pure no-op for the data plane. The accompanying alembic
migration ships the column and a one-shot backfill; production reads
still come off ``schema_sha`` until a later slice flips the routers.

Truthy values follow the same convention as
:func:`protea.api.auth._authn_required` so operators do not learn two
different env-flag dialects: ``1``, ``true``, ``yes``, ``on`` (case
insensitive). Anything else (including unset) is treated as ``false``.
"""

from __future__ import annotations

import os

ENV_VAR_NAME = "PROTEA_SCHEMA_SHA_V2_WRITE_ENABLED"

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def is_dual_write_enabled() -> bool:
    """Return ``True`` when ``schema_sha_v2`` should be written on inserts.

    The check is read-fresh each call so tests can flip the value via
    :meth:`pytest.MonkeyPatch.setenv` without restarting the process and
    operators can roll the flag forward without redeploying.
    """
    raw = os.getenv(ENV_VAR_NAME)
    if raw is None:
        return False
    return raw.strip().lower() in _TRUTHY


def maybe_v2(value: str | None) -> str | None:
    """Pass-through helper for dual-write call sites.

    Returns ``value`` when the flag is on, ``None`` otherwise. Lets
    write paths stay terse: ``schema_sha_v2=maybe_v2(legacy_sha)``
    instead of an inline ternary at every insert site.
    """
    return value if is_dual_write_enabled() else None


__all__ = ["ENV_VAR_NAME", "is_dual_write_enabled", "maybe_v2"]
