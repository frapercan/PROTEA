#!/usr/bin/env python3
"""Check that publishable prose files contain no em-dashes.

Detects:
- Unicode em-dash: U+2014 (—)
- ASCII double-dash used as prose em-dash: ` -- ` (space–dash–dash–space)

Exempt contexts (not flagged):
- Fenced code blocks in Markdown (``` ... ```)
- RST code-block directives (.. code-block::, .. code::, ::)
- RST literal-include blocks
- Inline code spans (`code`) — the em-dash must be outside backtick spans
- Lines whose non-whitespace content is entirely dashes (RST headings/transitions/rules)
- Lines inside YAML/TOML frontmatter (--- markers)

Usage:
  python scripts/check_no_em_dashes.py [paths...]   # default: docs/source/ + top-level *.md

Exits 0 if no violations, 1 if any found.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Regex for ASCII double-dash used as prose em-dash.
# Matches ` -- ` (must be surrounded by non-dash chars on both sides).
# Does NOT match --flag, ---rule, ---- (headings, transitions, horizontal rules).
_ASCII_EMDASH_RE = re.compile(r"(?<![- ])\s--\s(?![-])")

# Regex for unicode em-dash outside inline code spans.
# We strip inline code spans first before checking.
_INLINE_CODE_RE = re.compile(r"`[^`]+`")

_UNICODE_EMDASH = "—"


def _strip_inline_code(line: str) -> str:
    """Replace inline code spans with spaces so em-dashes inside them are invisible."""
    return _INLINE_CODE_RE.sub(" " * 3, line)


def _is_heading_or_rule(line: str) -> bool:
    """Return True if the line is an RST heading underline, transition, or horizontal rule."""
    stripped = line.strip()
    if not stripped:
        return False
    return all(c in "-=~^+#*" for c in stripped) and len(stripped) >= 2


def check_file(path: Path) -> list[tuple[int, str]]:
    """Return list of (lineno, line) pairs that violate the em-dash rule."""
    violations: list[tuple[int, str]] = []
    suffix = path.suffix.lower()
    is_md = suffix in (".md", ".markdown")
    is_rst = suffix == ".rst"

    if not (is_md or is_rst):
        return violations

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    in_code_block = False  # Markdown fenced block or RST literal/code-block
    rst_literal_indent: int | None = None  # indent level of RST literal block body
    in_frontmatter = False
    frontmatter_seen = False

    for lineno, raw_line in enumerate(lines, start=1):
        # --- YAML/TOML frontmatter (Markdown only) ---
        if is_md and not frontmatter_seen and lineno == 1 and raw_line.strip() == "---":
            in_frontmatter = True
            frontmatter_seen = True
            continue
        if in_frontmatter:
            if raw_line.strip() == "---" and lineno > 1:
                in_frontmatter = False
            continue

        # --- Markdown fenced code block ---
        if is_md:
            stripped = raw_line.strip()
            if stripped.startswith("```") or stripped.startswith("~~~"):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                continue

        # --- RST code/literal blocks ---
        if is_rst:
            stripped = raw_line.rstrip()

            # Detect start of a literal block: line ending with `::`
            # or explicit `.. code-block::` / `.. code::` directive
            if re.match(r"^\s*\.\.\s+(code-block|code|literalinclude)::", stripped):
                in_code_block = True
                rst_literal_indent = None
                continue
            if stripped.endswith("::"):
                in_code_block = True
                rst_literal_indent = None
                continue

            if in_code_block:
                if stripped == "":
                    # blank line inside or after code block — keep waiting
                    continue
                # Determine indent of first content line after the directive
                indent = len(raw_line) - len(raw_line.lstrip())
                if rst_literal_indent is None:
                    rst_literal_indent = indent
                    continue
                if indent >= rst_literal_indent:
                    continue
                # Dedented back — code block ended
                in_code_block = False
                rst_literal_indent = None
                # Fall through: this line is prose

        # --- RST heading underlines and transitions (all-dash lines) ---
        if is_rst and _is_heading_or_rule(raw_line):
            continue

        # --- Check for em-dashes in prose ---
        check_line = _strip_inline_code(raw_line)

        if _UNICODE_EMDASH in check_line:
            violations.append((lineno, raw_line))
            continue

        if _ASCII_EMDASH_RE.search(check_line):
            violations.append((lineno, raw_line))

    return violations


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to scan (default: docs/source/ + top-level *.md)",
    )
    args = parser.parse_args(argv)

    # Resolve paths
    repo_root = Path(__file__).resolve().parent.parent
    if args.paths:
        search_roots = [Path(p) for p in args.paths]
    else:
        search_roots = [
            repo_root / "docs" / "source",
            *repo_root.glob("*.md"),
        ]

    # Files excluded from checking (auto-generated or machine-maintained)
    excluded_names = {"CHANGELOG.md"}

    files: list[Path] = []
    for root in search_roots:
        if root.is_file():
            if root.name not in excluded_names:
                files.append(root)
        elif root.is_dir():
            files.extend(
                f for f in root.rglob("*.rst") if f.name not in excluded_names
            )
            files.extend(
                f for f in root.rglob("*.md") if f.name not in excluded_names
            )

    files = sorted(set(files))

    all_violations: list[tuple[Path, int, str]] = []
    for fpath in files:
        for lineno, line in check_file(fpath):
            all_violations.append((fpath, lineno, line))

    if all_violations:
        print(f"check_no_em_dashes: {len(all_violations)} violation(s) found:\n")
        for fpath, lineno, line in all_violations:
            rel = fpath.relative_to(repo_root) if fpath.is_relative_to(repo_root) else fpath
            print(f"  {rel}:{lineno}: {line.rstrip()}")
        print(
            "\nFix: replace em-dashes (— or ' -- ') in publishable prose with"
            " point, comma, or parentheses.\n"
            "Exempt: code blocks, fenced Markdown, RST code-block directives, inline `code`."
        )
        return 1

    print(f"check_no_em_dashes: OK ({len(files)} files scanned, 0 violations)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
