#!/usr/bin/env python3
"""Synchronize stack.yaml to README.md and stack.json.

Reads docs/source/_data/stack.yaml and regenerates:
  - The markdown table in README.md (between <!-- protea-stack:start --> markers)
  - The JSON version at docs/source/_data/stack.json

Usage:
    poetry run python scripts/sync_stack.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STACK_YAML = PROJECT_ROOT / "docs" / "source" / "_data" / "stack.yaml"
STACK_JSON = PROJECT_ROOT / "docs" / "source" / "_data" / "stack.json"
README = PROJECT_ROOT / "README.md"


def main() -> int:
    if not STACK_YAML.exists():
        print(f"Error: {STACK_YAML} not found", file=sys.stderr)
        return 1

    # Load stack.yaml
    with open(STACK_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    repos = data.get("repos", [])

    # Generate JSON
    json_data = {"repos": repos}
    with open(STACK_JSON, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    print(f"Updated {STACK_JSON}")

    # Generate README table
    rows = [
        "| Repo | Role | Status | Summary |",
        "|------|------|--------|---------|",
    ]

    for repo in repos:
        name = repo["name"]
        slug = repo["slug"]
        role_label = repo.get("role_label", repo.get("role", ""))
        status = repo.get("status", "")
        summary = repo.get("summary", "")
        github_url = repo.get("github_url", "")

        # Special case for PROTEA itself (this repo)
        if slug == "protea-core":
            repo_cell = f"**{name}** (this repo)"
        else:
            repo_cell = f"[{name}]({github_url})"

        rows.append(f"| {repo_cell} | {role_label} | `{status}` | {summary} |")

    table = "\n".join(rows)

    # Read README and replace the marked section
    with open(README, encoding="utf-8") as f:
        readme_text = f.read()

    start_marker = "<!-- protea-stack:start -->"
    end_marker = "<!-- protea-stack:end -->"

    if start_marker not in readme_text or end_marker not in readme_text:
        print(f"Error: stack markers not found in {README}", file=sys.stderr)
        return 1

    before = readme_text[: readme_text.index(start_marker) + len(start_marker)]
    after = readme_text[readme_text.index(end_marker) :]

    new_readme = (
        before
        + "\n\n## Repositories in the PROTEA stack\n\n"
        + "Single source of truth: [`docs/source/_data/stack.yaml`](https://github.com/frapercan/PROTEA/blob/develop/docs/source/_data/stack.yaml) in PROTEA. "
        + "Run `python scripts/sync_stack.py` to regenerate this block.\n\n"
        + table
        + "\n\n"
        + after
    )

    with open(README, "w", encoding="utf-8") as f:
        f.write(new_readme)
    print(f"Updated {README}")

    print("All files synchronized.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
