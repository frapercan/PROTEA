"""Per-run provenance capture (T3.11 of master plan v3.2 §24 Fase 4).

Lightweight utility that snapshots the runtime context of an in-process
operation so jobs / experiments / artefacts can carry a small audit trail.
Designed to be cheap (no DB, no network) and side-effect-free; safe to call
inside operation handlers, lab dumps, or one-shot scripts.

Output schema
-------------

``capture_provenance(extra)`` returns a flat ``dict[str, Any]`` with the
following keys (in declaration order):

* ``protea_version``  — distribution version from ``importlib.metadata``;
  ``None`` when the package metadata is missing (editable install in a
  worktree without ``pip install -e .`` etc.).
* ``protea_git_sha``  — current HEAD sha of the PROTEA repo (or ``None``
  outside a git checkout). Reuses :func:`resolve_protea_git_sha` from
  ``parquet_export``.
* ``python_version``  — full ``sys.version`` string (compiler + build
  date), useful for reproducibility audits.
* ``platform``        — ``platform.platform()``: OS, kernel, arch.
* ``hostname``        — ``socket.gethostname()``; cheap.
* ``captured_at``     — ISO8601 UTC timestamp at the call moment.
* every key in ``extra`` is overlaid last (caller wins on collision).

The helper never raises — every probe wraps a broad ``except Exception``
with a ``None`` fallback so a missing metadata package or a non-git
working dir doesn't poison the manifest.
"""

from __future__ import annotations

import platform as _platform
import socket
import sys
from typing import Any

from protea.core.parquet_export import resolve_protea_git_sha
from protea.core.utils import utcnow


def _resolve_protea_version() -> str | None:
    """Return the installed ``protea`` distribution version, or ``None``."""
    try:
        from importlib.metadata import PackageNotFoundError, version
    except Exception:
        return None
    try:
        return version("protea")
    except PackageNotFoundError:
        return None
    except Exception:
        return None


def _safe_hostname() -> str | None:
    try:
        return socket.gethostname() or None
    except Exception:
        return None


def _safe_platform() -> str | None:
    try:
        return _platform.platform()
    except Exception:
        return None


def capture_provenance(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """Snapshot runtime provenance into a flat ``dict``.

    The optional ``extra`` mapping is overlaid last so callers can pin
    domain-specific tags (``run_id``, ``embedding_config_id``, ``role``)
    alongside the generic environment fields without losing any of the
    automatic probes when their key happens to coincide.

    Always safe to call: every system probe wraps ``Exception`` and falls
    back to ``None`` so a missing distribution metadata or a non-git
    working dir never breaks the caller.
    """
    payload: dict[str, Any] = {
        "protea_version": _resolve_protea_version(),
        "protea_git_sha": resolve_protea_git_sha(),
        "python_version": sys.version,
        "platform": _safe_platform(),
        "hostname": _safe_hostname(),
        "captured_at": utcnow().isoformat(),
    }
    if extra:
        payload.update(extra)
    return payload


__all__ = ["capture_provenance"]
