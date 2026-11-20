#!/usr/bin/env python3
"""Synchronize stack.yaml to README.md, stack.json and the stack page.

Reads docs/source/_data/stack.yaml, the single source of truth for the
repositories of the stack, and regenerates:
  - the markdown table in README.md (between <!-- protea-stack:start --> markers);
  - the JSON version at docs/source/_data/stack.json, which the API serves;
  - the table of the Sphinx page docs/source/appendix/stack.rst (between
    ``.. protea-stack:start`` markers).

The page used to say it was generated while this script never wrote it, so it
drifted from the registry. All three are generated now, and ``--check`` fails
when any of them is not what the registry gives (tests/test_the_stack_pages_
come_from_the_registry.py runs it).

Usage:
    poetry run python scripts/sync_stack.py           # write the three files
    poetry run python scripts/sync_stack.py --check   # fail if one is stale
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STACK_YAML = PROJECT_ROOT / "docs" / "source" / "_data" / "stack.yaml"
STACK_JSON = PROJECT_ROOT / "docs" / "source" / "_data" / "stack.json"
STACK_RST = PROJECT_ROOT / "docs" / "source" / "appendix" / "stack.rst"
README = PROJECT_ROOT / "README.md"

README_MARKERS = ("<!-- protea-stack:start -->", "<!-- protea-stack:end -->")
RST_MARKERS = (".. protea-stack:start", ".. protea-stack:end")


def load_repos() -> list[dict]:
    """The repositories of the registry, in its order."""
    with open(STACK_YAML, encoding="utf-8") as f:
        return yaml.safe_load(f).get("repos", [])


def stack_json(repos: list[dict]) -> str:
    """The registry as the JSON the API serves."""
    return json.dumps({"repos": repos}, indent=2)


def readme_block(repos: list[dict]) -> str:
    """The README section between its markers: a heading, a pointer and the table."""
    rows = ["| Repo | Role | Status | Summary |", "|------|------|--------|---------|"]
    for repo in repos:
        if repo["slug"] == "protea-core":
            repo_cell = f"**{repo['name']}** (this repo)"
        else:
            repo_cell = f"[{repo['name']}]({repo.get('github_url', '')})"
        role = repo.get("role_label", repo.get("role", ""))
        rows.append(
            f"| {repo_cell} | {role} | `{repo.get('status', '')}` | {repo.get('summary', '')} |"
        )
    return (
        "\n\n## Repositories in the PROTEA stack\n\n"
        "Single source of truth: [`docs/source/_data/stack.yaml`](https://github.com/frapercan/PROTEA/blob/develop/docs/source/_data/stack.yaml) in PROTEA. "
        "Run `python scripts/sync_stack.py` to regenerate this block.\n\n"
        + "\n".join(rows)
        + "\n\n"
    )


def rst_block(repos: list[dict]) -> str:
    """The Sphinx page section between its markers: a note, a count and the table."""
    lines = [
        "",
        "",
        ".. note::",
        "",
        "   This table is generated from ``docs/source/_data/stack.yaml`` by",
        "   ``scripts/sync_stack.py``. Edit the registry and run the script; do not",
        "   edit this block by hand.",
        "",
        f"The registry lists {len(repos)} repositories: this platform, its contract package",
        "and plugins, and the tools, libraries and models around it. The platform",
        "repository hosts the orchestration, ORM, queue and HTTP surface.",
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
            f"   * - `{repo['name']} <{repo.get('github_url', '')}>`_",
            f"     - {repo.get('role_label', repo.get('role', ''))}",
            f"     - ``{repo.get('status', '')}``",
            f"     - {repo.get('summary', '')}",
        ]
    return "\n".join(lines) + "\n\n"


def replace_between(text: str, markers: tuple[str, str], block: str, where: Path) -> str:
    """Replace what lies between the two markers of ``text`` with ``block``."""
    start, end = markers
    if start not in text or end not in text:
        raise SystemExit(f"Error: stack markers not found in {where}")
    head = text[: text.index(start) + len(start)]
    return head + block + text[text.index(end) :]


def expected_files(repos: list[dict]) -> dict[Path, str]:
    """Every generated file, with the contents the registry gives."""
    readme = README.read_text(encoding="utf-8")
    page = STACK_RST.read_text(encoding="utf-8")
    return {
        STACK_JSON: stack_json(repos),
        README: replace_between(readme, README_MARKERS, readme_block(repos), README),
        STACK_RST: replace_between(page, RST_MARKERS, rst_block(repos), STACK_RST),
    }


def stale_files(repos: list[dict]) -> list[Path]:
    """The generated files whose contents differ from what the registry gives."""
    return [
        path
        for path, text in expected_files(repos).items()
        if path.read_text(encoding="utf-8") != text
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Synchronize stack.yaml to README.md, stack.json and the stack page."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if a generated file is stale instead of writing it",
    )
    args = parser.parse_args(argv)
    if not STACK_YAML.exists():
        print(f"Error: {STACK_YAML} not found", file=sys.stderr)
        return 1
    repos = load_repos()
    if args.check:
        stale = stale_files(repos)
        for path in stale:
            print(f"Stale: {path.relative_to(PROJECT_ROOT)}", file=sys.stderr)
        if stale:
            print("Run: python scripts/sync_stack.py", file=sys.stderr)
            return 1
        print("All files match the registry.")
        return 0
    for path, text in expected_files(repos).items():
        path.write_text(text, encoding="utf-8")
        print(f"Updated {path}")
    print("All files synchronized.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
