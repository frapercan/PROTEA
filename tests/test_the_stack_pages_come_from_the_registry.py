"""The README block, stack.json and the stack page are generated from stack.yaml.

The page said it was generated while scripts/sync_stack.py never wrote it, and it
drifted from the registry: a repository's status and the count of repositories were
typed by hand. The script writes all three now, and these fail when any of them is
not what the registry gives, so neither an edit of the registry that forgets the
script nor a hand edit of a generated block goes unnoticed.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _sync_stack() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "sync_stack", _REPO_ROOT / "scripts" / "sync_stack.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_generated_stack_file_matches_the_registry() -> None:
    sync = _sync_stack()
    stale = sync.stale_files(sync.load_repos())
    assert not stale, (
        f"run python scripts/sync_stack.py; stale: {[str(p.relative_to(_REPO_ROOT)) for p in stale]}"
    )


def test_the_page_counts_the_repositories_of_the_registry() -> None:
    sync = _sync_stack()
    repos = sync.load_repos()
    assert f"The registry lists {len(repos)} repositories" in sync.rst_block(repos)


def test_a_hand_edit_of_the_page_is_caught(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sync = _sync_stack()
    for name in ("STACK_YAML", "STACK_JSON", "STACK_RST", "README"):
        source = getattr(sync, name)
        copy = tmp_path / source.name
        copy.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        monkeypatch.setattr(sync, name, copy)
    page = sync.STACK_RST.read_text(encoding="utf-8")
    sync.STACK_RST.write_text(page.replace("``active``", "``beta``", 1), encoding="utf-8")
    assert sync.stale_files(sync.load_repos()) == [sync.STACK_RST]
