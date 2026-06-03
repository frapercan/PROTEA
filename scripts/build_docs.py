"""Build Sphinx HTML for PROTEA + sibling repos in the stack.

Each repo with a ``docs/source/conf.py`` is built and the result lands in
``docs/build/<slug>/html/`` inside the current PROTEA tree, where the
FastAPI app mounts it under ``/docs/<slug>/``. The legacy
``docs/build/html/`` path (mounted at ``/sphinx/``) is also refreshed
from the protea-core build for backwards compatibility.

Usage:
    poetry run task docs                  # build everything that's available
    SIBLINGS_DIR=/path python scripts/build_docs.py

Repos that are not present on disk (sibling repo not cloned) are skipped
silently. Repos that fail to build are reported and produce a non-zero
exit code so CI / the deploy script can surface the failure.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

# Slug → on-disk folder name. The slug comes from
# docs/source/_data/stack.yaml; for the platform repo itself the folder
# is simply ``PROTEA`` rather than ``protea-core``.
_FOLDER_OVERRIDES = {"protea-core": "PROTEA"}

# The five Sphinx-using repos in the stack. Repos without docs/ are
# omitted from this list rather than being detected at runtime so a
# missing folder is treated as "not cloned" rather than "no docs".
_DOC_SLUGS = (
    "protea-core",
    "protea-contracts",
    "protea-sources",
    "protea-runners",
    "protea-backends",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIBLINGS_DIR = PROJECT_ROOT.parent.parent / "repositories"


def _folder_for(slug: str) -> str:
    return _FOLDER_OVERRIDES.get(slug, slug)


def _build_one(slug: str, repo_root: Path, out_dir: Path) -> int:
    src = repo_root / "docs" / "source"
    if not (src / "conf.py").exists():
        print(f"skip {slug}: no docs/source/conf.py at {src}")
        return 0
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"building {slug}: {src} -> {out_dir}")
    return subprocess.call(
        ["sphinx-build", "-q", "-b", "html", str(src), str(out_dir)],
    )


def main() -> int:
    siblings_dir = Path(
        os.environ.get("SIBLINGS_DIR", str(DEFAULT_SIBLINGS_DIR))
    ).resolve()
    build_root = PROJECT_ROOT / "docs" / "build"

    failures: list[str] = []
    built: list[str] = []

    for slug in _DOC_SLUGS:
        repo_root = (
            PROJECT_ROOT if slug == "protea-core" else siblings_dir / _folder_for(slug)
        )
        if not repo_root.exists():
            print(f"skip {slug}: {repo_root} not found")
            continue
        out_dir = build_root / slug / "html"
        if _build_one(slug, repo_root, out_dir) != 0:
            failures.append(slug)
        else:
            built.append(slug)

    # Mirror protea-core into the legacy docs/build/html path so the
    # existing /sphinx mount keeps working for callers that hard-coded
    # the URL before /docs/<slug>/ existed.
    legacy = build_root / "html"
    src = build_root / "protea-core" / "html"
    if src.exists():
        if legacy.exists():
            shutil.rmtree(legacy)
        shutil.copytree(src, legacy)

    print(f"built: {built or '[]'}")
    if failures:
        print(f"failed: {failures}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
