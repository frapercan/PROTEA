#!/usr/bin/env python3
"""Refuse Claude/Anthropic attribution in commit messages and pull request bodies.

No commit and no pull request in this repository carries an AI attribution
line. ``.github/workflows/coauthor-guard.yml`` runs this script twice, over the
messages of the commits a pull request introduces and over its body, so both
scans read the patterns below and nothing else.

WHY ONE SCRIPT. The two scans used to carry their own regexes inline in the
workflow, and they drifted apart: the body's lacked the optional bracket the
commit scan had, so the footer the tool appends to a pull request body
(a Markdown link, ``[Claude Code](...)``) passed the body scan, and neither
scan knew the session trailer the tool appends to a commit message.

Scanning is line by line, as ``grep`` did, so a pattern never spans two lines.
Mentioning Claude in prose passes: what is refused is the shape of an
attribution (a trailer or the footer), not the name.

Usage:
  python3 scripts/check_coauthor.py [PATH ...]

Each PATH is a text file; ``-``, or no PATH at all, reads standard input.
Every offending line is printed as ``source:line: text``. Exits 1 if there is
one, 0 if there is none.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

FORBIDDEN: tuple[re.Pattern[str], ...] = (
    # A co-author trailer naming Claude or Anthropic, in the name or the address.
    re.compile(r"co-authored-by\s*:.*(claude|anthropic)", re.IGNORECASE),
    # The session trailer, whatever the URL after it.
    re.compile(r"claude-session\s*:", re.IGNORECASE),
    # The footer, with or without the bracket of its Markdown link.
    re.compile(r"generated\s+with\s+\[?claude", re.IGNORECASE),
    re.compile(r"generated\s+by\s+\[?anthropic", re.IGNORECASE),
)


def forbidden_lines(text: str) -> list[tuple[int, str]]:
    """Return ``(line number, line)`` for every line carrying a forbidden attribution."""
    return [
        (number, line)
        for number, line in enumerate(text.splitlines(), start=1)
        if any(pattern.search(line) for pattern in FORBIDDEN)
    ]


def _read(path: str) -> tuple[str, str]:
    """Return ``(source name, text)``; undecodable bytes are replaced, never fatal."""
    if path == "-":
        return "<stdin>", sys.stdin.buffer.read().decode("utf-8", errors="replace")
    return path, Path(path).read_text(encoding="utf-8", errors="replace")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refuse Claude/Anthropic attribution in commit messages and PR bodies."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["-"],
        help="text files to scan; '-' or nothing reads standard input",
    )
    args = parser.parse_args(argv)

    found = 0
    for path in args.paths:
        source, text = _read(path)
        for number, line in forbidden_lines(text):
            print(f"{source}:{number}: {line}")
            found += 1

    if found:
        print(f"check_coauthor: {found} line(s) carry a forbidden Claude/Anthropic attribution.")
        return 1
    print("check_coauthor: OK, no forbidden attribution.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
