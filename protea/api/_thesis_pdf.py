"""Thesis PDF path resolution for the serve-mount route.

The served thesis PDF is decoupled from the frontend build: instead of baking a
copy into ``apps/web/public`` (which only refreshes on a full ``npm run build``),
the API serves it from a stable, env-overridable path, read at request time. A
plain file overwrite at that path updates what the app distributes with no
rebuild and no restart.
"""

from __future__ import annotations

import os
from pathlib import Path

_DEFAULT_ROOT = Path(__file__).resolve().parents[2]  # PROTEA repo root

#: Env var pointing at the canonical (mounted) thesis PDF. Set this in the
#: deploy environment to a durable path outside the build tree so redeploys
#: never revert it.
THESIS_PDF_ENV = "PROTEA_THESIS_PDF_PATH"


def thesis_pdf_path(project_root: Path | None = None) -> Path | None:
    """Return the first existing thesis-PDF path, or ``None``.

    Priority: ``PROTEA_THESIS_PDF_PATH`` (the mount) then ``<root>/static/
    thesis.pdf`` then the legacy ``<root>/apps/web/public/thesis.pdf``.
    """
    root = project_root or _DEFAULT_ROOT
    candidates: list[Path] = []
    env = os.environ.get(THESIS_PDF_ENV)
    if env:
        candidates.append(Path(env))
    candidates.append(root / "static" / "thesis.pdf")
    candidates.append(root / "apps" / "web" / "public" / "thesis.pdf")
    for c in candidates:
        if c.is_file():
            return c
    return None
