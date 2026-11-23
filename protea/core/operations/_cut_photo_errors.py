"""The refusal of ADR-D59's loader, apart so its helpers can raise it without a cycle."""

from __future__ import annotations

from protea.core.source_guards import SourceGuardError


class PhotoRefused(SourceGuardError):
    """A photo the loader will not write, with the reason."""
