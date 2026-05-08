#!/usr/bin/env python3
"""Smell budget enforcer (master plan v3.2 §3).

Thresholds:
- File: >800 LOC
- Class: >500 LOC
- Method/function: >60 LOC
- Parameter count: >6

Enforcement model: ratchet. The baseline records existing offenders at
their current size. New runs fail if they introduce a new offender or
worsen an existing one. Removed offenders shrink the baseline silently
on --write-baseline.

Usage:
  python scripts/check_smells.py                   # check vs baseline
  python scripts/check_smells.py --write-baseline  # seed/refresh after a legit refactor
  python scripts/check_smells.py --target src      # different package root
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path

THRESHOLDS: dict[str, int] = {
    "file_loc": 800,
    "class_loc": 500,
    "method_loc": 60,
    "param_count": 6,
}

DEFAULT_EXCLUDES: tuple[str, ...] = (
    "/tests/",
    "/test_",
    "/migrations/",
    "/alembic/versions/",
    "/.venv/",
    "/venv/",
    "/build/",
    "/dist/",
    "/node_modules/",
    "conftest.py",
)


@dataclass(frozen=True)
class Offender:
    kind: str
    path: str
    name: str
    line: int
    metric: int
    threshold: int

    @property
    def key(self) -> str:
        # Line is intentionally excluded so an Extract Method that
        # shifts the offender up or down the file does not look like
        # a brand-new offender to the ratchet.
        return f"{self.kind}::{self.path}::{self.name}"


def is_excluded(path: Path, excludes: tuple[str, ...]) -> bool:
    s = str(path)
    return any(token in s for token in excludes)


def _span(node: ast.AST) -> int:
    end = getattr(node, "end_lineno", None)
    start = getattr(node, "lineno", None)
    if end is None or start is None:
        return 1
    return end - start + 1


class _OffenderVisitor(ast.NodeVisitor):
    """Walks the AST tracking the enclosing class so methods get a
    qualified ``Class.name`` identifier instead of bare ``name``.

    Without the qualifier, two ``execute`` methods in different
    classes of the same file collapse to one ratchet key and either
    can shadow the other.
    """

    def __init__(self, rel: str) -> None:
        self.rel = rel
        self.out: list[Offender] = []
        self._class_stack: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        length = _span(node)
        if length > THRESHOLDS["class_loc"]:
            self.out.append(
                Offender("class", self.rel, node.name, node.lineno, length, THRESHOLDS["class_loc"])
            )
        self._class_stack.append(node.name)
        self.generic_visit(node)
        self._class_stack.pop()

    def _visit_func(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        qualified = ".".join((*self._class_stack, node.name))
        length = _span(node)
        if length > THRESHOLDS["method_loc"]:
            self.out.append(
                Offender(
                    "method", self.rel, qualified, node.lineno, length, THRESHOLDS["method_loc"]
                )
            )
        args = node.args
        count = len(args.args) + len(args.kwonlyargs) + len(args.posonlyargs)
        if count > THRESHOLDS["param_count"]:
            self.out.append(
                Offender(
                    "params", self.rel, qualified, node.lineno, count, THRESHOLDS["param_count"]
                )
            )
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._visit_func(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._visit_func(node)


def scan_file(path: Path, root: Path) -> list[Offender]:
    text = path.read_text(encoding="utf-8", errors="replace")
    rel = str(path.relative_to(root)) if path.is_relative_to(root) else str(path)
    loc = len(text.splitlines())
    out: list[Offender] = []
    if loc > THRESHOLDS["file_loc"]:
        out.append(Offender("file", rel, "", 0, loc, THRESHOLDS["file_loc"]))
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return out
    visitor = _OffenderVisitor(rel)
    visitor.visit(tree)
    out.extend(visitor.out)
    return out


def scan(target: Path, excludes: tuple[str, ...]) -> list[Offender]:
    target = target.resolve()
    out: list[Offender] = []
    for path in sorted(target.rglob("*.py")):
        if is_excluded(path, excludes):
            continue
        out.extend(scan_file(path, target.parent))
    return out


def load_baseline(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {entry["key"]: int(entry["metric"]) for entry in data.get("offenders", [])}


def write_baseline(path: Path, offenders: list[Offender]) -> None:
    payload = {
        "thresholds": THRESHOLDS,
        "offenders": [
            {
                "key": o.key,
                "kind": o.kind,
                "path": o.path,
                "name": o.name,
                "line": o.line,
                "metric": o.metric,
                "threshold": o.threshold,
            }
            for o in sorted(offenders, key=lambda x: (x.kind, x.path, x.line))
        ],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def diff(
    current: list[Offender], baseline: dict[str, int]
) -> tuple[list[Offender], list[tuple[Offender, int]]]:
    new: list[Offender] = []
    worsened: list[tuple[Offender, int]] = []
    for o in current:
        prev = baseline.get(o.key)
        if prev is None:
            new.append(o)
        elif o.metric > prev:
            worsened.append((o, prev))
    return new, worsened


def summarize(offenders: list[Offender]) -> dict[str, int]:
    counts: dict[str, int] = {"file": 0, "class": 0, "method": 0, "params": 0}
    for o in offenders:
        counts[o.kind] += 1
    return counts


def fmt_offender(o: Offender) -> str:
    unit = "LOC" if o.kind in ("file", "class", "method") else "args"
    where = o.path if o.kind == "file" else f"{o.path}:{o.line} {o.name}"
    return f"  {o.kind:7s} {o.metric:5d} {unit:4s}  (>{o.threshold})  {where}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", type=Path, default=Path("protea"))
    parser.add_argument("--baseline", type=Path, default=Path(".smell-baseline.json"))
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Seed/refresh baseline with current offenders.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=None,
        help="Substring exclusion (repeatable). Replaces defaults if used.",
    )
    args = parser.parse_args()

    if not args.target.exists():
        print(f"target {args.target} does not exist", file=sys.stderr)
        return 2

    excludes = tuple(args.exclude) if args.exclude else DEFAULT_EXCLUDES
    current = scan(args.target, excludes)
    counts = summarize(current)

    if args.write_baseline:
        write_baseline(args.baseline, current)
        total = sum(counts.values())
        print(f"baseline written to {args.baseline}: {total} offenders")
        print(
            f"  files >{THRESHOLDS['file_loc']} LOC: {counts['file']}  "
            f"classes >{THRESHOLDS['class_loc']}: {counts['class']}  "
            f"methods >{THRESHOLDS['method_loc']}: {counts['method']}  "
            f"params >{THRESHOLDS['param_count']}: {counts['params']}"
        )
        return 0

    baseline = load_baseline(args.baseline)
    if not baseline and current:
        print("no baseline found; run with --write-baseline to seed.", file=sys.stderr)
        print(f"current state would record {sum(counts.values())} offenders.", file=sys.stderr)
        return 2

    new, worsened = diff(current, baseline)

    if not new and not worsened:
        print(f"smell budget OK: {len(current)} known offenders, none new or worsened.")
        return 0

    if new:
        print(f"\nNEW offenders ({len(new)}):", file=sys.stderr)
        for o in sorted(new, key=lambda x: (x.kind, -x.metric)):
            print(fmt_offender(o), file=sys.stderr)

    if worsened:
        print(f"\nWORSENED offenders ({len(worsened)}):", file=sys.stderr)
        for o, prev in sorted(worsened, key=lambda x: -(x[0].metric - x[1])):
            print(f"{fmt_offender(o)}  (was {prev})", file=sys.stderr)

    print(
        "\nFix the issue, or if intentional after a legit refactor:\n"
        "  python scripts/check_smells.py --write-baseline",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
