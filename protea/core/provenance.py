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
* ``libraries``       — versions of the packages that decide what a vector
  contains; see :func:`_library_versions`. Read from distribution metadata,
  so nothing heavy is imported.
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


#: The packages whose version changes what a computed vector holds. Not a
#: general dependency list: each earned its place by being measured.
#:
#: * ``transformers`` and ``tokenizers`` decide the TOKENISATION. Between
#:   4.48.1 and 5.17.0 the ankh backend goes from 79 tokens to 157 for the same
#:   78-residue sequence, interleaving ``<unk>``, and raises nothing.
#: * ``torch`` decides the ARITHMETIC, and its version string carries the build
#:   (``+cpu`` / ``+cu130``) -- which is the axis that matters, because the two
#:   machines differ there BY DESIGN. Measured on esmc_600m: transformers
#:   4.48.1 -> 5.17.0 at a fixed torch is byte-identical, while torch 2.12 ->
#:   2.14 moves 1136 of 1152 coordinates.
#: * ``numpy`` does the pooling.
_LIBRARIES: tuple[str, ...] = ("transformers", "tokenizers", "torch", "numpy")


def _library_versions() -> dict[str, str | None]:
    """Versions of :data:`_LIBRARIES`, read from distribution metadata.

    Deliberately does NOT import the packages: importing ``torch`` costs
    seconds and this helper is documented as cheap and safe to call anywhere.
    Metadata carries what is needed anyway -- ``torch``'s local version
    segment names the build, so ``2.14.0+cu130`` and ``2.14.0+cpu`` are
    distinguishable without loading either.

    An absent package records ``None`` rather than being omitted: a key that
    disappears when a package is missing reads, later, as a field nobody
    captured, which is the opposite of what a provenance record is for.
    """
    try:
        from importlib.metadata import PackageNotFoundError, version
    except Exception:
        return dict.fromkeys(_LIBRARIES)
    out: dict[str, str | None] = {}
    for name in _LIBRARIES:
        try:
            out[name] = version(name)
        except PackageNotFoundError:
            out[name] = None
        except Exception:
            out[name] = None
    return out


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
        "libraries": _library_versions(),
        "captured_at": utcnow().isoformat(),
    }
    if extra:
        payload.update(extra)
    return payload


def stamp_library_provenance(emit: Any) -> None:
    """Record, through ``emit``, the library versions about to compute.

    Called by the worker immediately before an operation executes. The job row
    records WHAT ran and the revision guard records WHICH CODE; this is the
    third quantity, and the one that decides the numbers.

    Emitted from the worker and never from the server, because the two machines
    run different ``torch`` builds by design: only the process that executes an
    operation can say which build produced a given vector.
    """
    emit("provenance.libraries", None, capture_provenance()["libraries"], "info")


__all__ = ["capture_provenance", "stamp_library_provenance"]
