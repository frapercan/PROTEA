"""Regenerate stack-navigation artefacts from ``docs/source/_data/stack.yaml``.

Single source of truth for cross-repo links. Run this whenever the YAML
changes; CI then opens follow-up PRs in the sibling repos with the
refreshed README block.

Targets:
  * The README block between ``<!-- protea-stack:start -->`` and
    ``<!-- protea-stack:end -->`` (this repo by default; pass
    ``--readme PATH`` to point elsewhere).
  * The Sphinx page ``docs/source/appendix/stack.rst``.
  * A JSON dump at ``docs/source/_data/stack.json`` for the frontend
    and the API endpoint to consume.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
STACK_YAML = ROOT / "docs" / "source" / "_data" / "stack.yaml"
STACK_JSON = ROOT / "docs" / "source" / "_data" / "stack.json"
SPHINX_PAGE = ROOT / "docs" / "source" / "appendix" / "stack.rst"
DEFAULT_README = ROOT / "README.md"

START_MARKER = "<!-- protea-stack:start -->"
END_MARKER = "<!-- protea-stack:end -->"

STATUS_BADGES = {
    "active": "active",
    "beta": "beta",
    "skeleton": "skeleton",
    "archived": "archived",
}


def load_stack() -> list[dict[str, Any]]:
    data = yaml.safe_load(STACK_YAML.read_text(encoding="utf-8"))
    return data["repos"]


def render_markdown_block(repos: list[dict[str, Any]], current_slug: str | None) -> str:
    lines = [
        START_MARKER,
        "",
        "## Repositories in the PROTEA stack",
        "",
        "Single source of truth: [`docs/source/_data/stack.yaml`](https://github.com/frapercan/PROTEA/blob/develop/docs/source/_data/stack.yaml) in PROTEA. Run `python scripts/sync_stack.py` to regenerate this block.",
        "",
        "| Repo | Role | Status | Summary |",
        "|------|------|--------|---------|",
    ]
    for repo in repos:
        name = repo["name"]
        if repo["slug"] == current_slug:
            label = f"**{name}** (this repo)"
        else:
            label = f"[{name}]({repo['github_url']})"
        lines.append(
            f"| {label} | {repo['role_label']} | `{repo['status']}` | {repo['summary']} |"
        )
    lines += [
        "",
        END_MARKER,
    ]
    return "\n".join(lines)


def render_sphinx_page(repos: list[dict[str, Any]]) -> str:
    lines = [
        "PROTEA stack",
        "============",
        "",
        ".. note::",
        "",
        "   This page is generated from ``docs/source/_data/stack.yaml``.",
        "   Run ``python scripts/sync_stack.py`` to regenerate it.",
        "",
        "PROTEA is split across eight repositories. The platform repository",
        "(this one) hosts the orchestration, ORM, queue, and HTTP surface;",
        "the rest are pluggable contracts, runtime modules, and tooling.",
        "",
        ".. list-table::",
        "   :header-rows: 1",
        "   :widths: 18 14 12 56",
        "",
        "   * - Repository",
        "     - Role",
        "     - Status",
        "     - Summary",
    ]
    for repo in repos:
        lines += [
            f"   * - `{repo['name']} <{repo['github_url']}>`_",
            f"     - {repo['role_label']}",
            f"     - ``{repo['status']}``",
            f"     - {repo['summary']}",
        ]
    lines += [
        "",
        "Cross-cutting concerns",
        "----------------------",
        "",
        "Every other repository depends on ``protea-contracts`` as its",
        "shared surface (ABCs, payloads, feature schema). The platform",
        "discovers source, runner and backend plugins via Python",
        "``entry_points`` groups ``protea.sources``, ``protea.runners``,",
        "and ``protea.backends``.",
        "",
    ]
    return "\n".join(lines) + "\n"


def update_readme(readme_path: Path, repos: list[dict[str, Any]], current_slug: str | None) -> bool:
    text = readme_path.read_text(encoding="utf-8")
    block = render_markdown_block(repos, current_slug)
    if START_MARKER in text and END_MARKER in text:
        start = text.index(START_MARKER)
        end = text.index(END_MARKER) + len(END_MARKER)
        new_text = text[:start] + block + text[end:]
    else:
        sep = "\n\n" if not text.endswith("\n") else "\n"
        new_text = text + sep + block + "\n"
    if new_text == text:
        return False
    readme_path.write_text(new_text, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--readme",
        type=Path,
        default=DEFAULT_README,
        help="README path to update (default: this repo's README.md).",
    )
    parser.add_argument(
        "--current-slug",
        default="protea-core",
        help="Slug of the repo being updated, marked as 'this repo' in the table.",
    )
    parser.add_argument(
        "--skip-sphinx",
        action="store_true",
        help="Skip writing the Sphinx page (use when running against a sibling repo).",
    )
    parser.add_argument(
        "--skip-json",
        action="store_true",
        help="Skip writing the JSON dump (use when running against a sibling repo).",
    )
    args = parser.parse_args()

    repos = load_stack()

    readme_changed = update_readme(args.readme, repos, args.current_slug)
    print(f"README {args.readme}: {'updated' if readme_changed else 'unchanged'}")

    if not args.skip_sphinx:
        SPHINX_PAGE.parent.mkdir(parents=True, exist_ok=True)
        SPHINX_PAGE.write_text(render_sphinx_page(repos), encoding="utf-8")
        print(f"Sphinx page: {SPHINX_PAGE}")

    if not args.skip_json:
        STACK_JSON.write_text(json.dumps({"repos": repos}, indent=2) + "\n", encoding="utf-8")
        print(f"JSON dump: {STACK_JSON}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
