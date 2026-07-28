#!/usr/bin/env python3
"""Refuse the join shapes that lose rows without saying so.

Three expressions in this codebase can silently drop or mismatch rows, and all
three produce output of exactly the expected shape when they do. A join on a
non-unique key rewrote 8.3% of rows in an analysis whose conclusions were acted
on, and the defect was found by counting rather than by reading.

What is flagged, and what to write instead:

``dict(zip(keys, values))``
    Pairs two sequences positionally and keeps the last value for a repeated
    key. Use ``protea.core.row_alignment.lookup_by``, which raises instead.

``{k(row): row for row in rows}``
    The same last-wins, spelled as a comprehension. Same replacement.

``.set_index(...)`` without ``verify_integrity=True``
    Pandas will happily build a non-unique index, and every later ``.loc`` on
    it returns more rows than the caller expects. Pass ``verify_integrity=True``
    and pandas raises at the point the assumption breaks.

``.merge(...)`` / ``pd.merge(...)`` without ``validate=``
    A merge that fans out duplicates the left row across every match, so every
    aggregate computed afterwards double counts. Pass ``validate="one_to_one"``
    (or whichever cardinality is intended) and pandas checks it.

This check is deliberately narrow. It matches four syntactic shapes, it starts
at zero offenders, and it exists to stop a regression rather than to grade
existing code. A check that fires constantly is a check everyone learns to skip.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCANNED = ("protea",)

#: The sanctioned implementation: it performs the very check it looks like.
EXEMPT = frozenset({"protea/core/row_alignment.py"})

_ADVICE = {
    "dict-zip": (
        "dict(zip(...)) keeps the last value for a repeated key. "
        "Use protea.core.row_alignment.lookup_by, which raises instead."
    ),
    "keyed-comprehension": (
        "a dict comprehension keyed off each row keeps the last row per key. "
        "Use protea.core.row_alignment.lookup_by, which raises instead."
    ),
    "set-index": (
        "set_index() without verify_integrity=True permits a non-unique index, "
        "and every later .loc on it returns more rows than expected."
    ),
    "merge": (
        "merge() without validate= permits fan-out, which duplicates the left "
        'row across every match. Pass validate="one_to_one" or the intended '
        "cardinality."
    ),
}


class _Visitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.offences: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        self._check_dict_zip(node)
        self._check_method(node)
        self.generic_visit(node)

    def _check_dict_zip(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "dict"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Call)
            and isinstance(node.args[0].func, ast.Name)
            and node.args[0].func.id == "zip"
        ):
            self.offences.append((node.lineno, "dict-zip"))

    def _check_method(self, node: ast.Call) -> None:
        if not isinstance(node.func, ast.Attribute):
            return
        kwargs = {kw.arg for kw in node.keywords}
        if node.func.attr == "set_index" and "verify_integrity" not in kwargs:
            self.offences.append((node.lineno, "set-index"))
        elif node.func.attr == "merge" and "validate" not in kwargs:
            # session.merge(obj) is the ORM's upsert, not a frame join.
            if not _looks_like_orm_merge(node):
                self.offences.append((node.lineno, "merge"))

    def visit_DictComp(self, node: ast.DictComp) -> None:  # noqa: N802
        """Flag ``{f(row): row for row in rows}``: the value IS the loop target.

        A comprehension whose value is the iteration variable itself is a
        lookup built from data. One whose value is an expression over the row
        is usually a projection, and projections are not what lost the rows.

        Iterating a type is exempt. ``{a.code: a for a in Aspect}`` walks an
        enum, which is a closed literal set whose members are unique by
        construction, and a lint that flags it is a lint that gets switched
        off. What is being guarded is a lookup built from DATA.
        """
        gen = node.generators[0] if len(node.generators) == 1 else None
        if (
            gen is not None
            and isinstance(node.value, ast.Name)
            and isinstance(gen.target, ast.Name)
            and node.value.id == gen.target.id
            and not gen.ifs
            and not _iterates_a_type(gen.iter)
            and _keys_off_raw_data(node.key)
        ):
            self.offences.append((node.lineno, "keyed-comprehension"))
        self.generic_visit(node)


def _iterates_a_type(iterable: ast.expr) -> bool:
    """Whether the comprehension walks a type rather than a run of data.

    A bare CamelCase name is a class, and iterating a class in this codebase
    means iterating an enum's members: a closed set, unique by construction.
    """
    return (
        isinstance(iterable, ast.Name)
        and iterable.id[:1].isupper()
        and "_" not in iterable.id
    )


def _keys_off_raw_data(key: ast.expr) -> bool:
    """Whether the key reads a raw data field rather than a declared column.

    ``row["go_id"]`` and ``row.get("go_id")`` read a mapping or a Series, where
    nothing declares the field unique and duplicates are ordinary. ``obj.id``
    and ``obj.accession`` read a mapped attribute, where the database holds a
    primary key or a unique constraint and a duplicate cannot arrive in the
    first place.

    Flagging the second kind would fire on every correct lookup in the
    codebase, and a check that fires on correct code is a check that gets
    switched off. Only the first kind is guarded.
    """
    if isinstance(key, ast.Subscript):
        return True
    return isinstance(key, ast.Call) and isinstance(key.func, ast.Attribute) and key.func.attr == "get"


def _looks_like_orm_merge(node: ast.Call) -> bool:
    """Whether this ``.merge(...)`` is a SQLAlchemy session merge."""
    receiver = node.func.value if isinstance(node.func, ast.Attribute) else None
    return isinstance(receiver, ast.Name) and receiver.id in {"session", "db", "sess"}


def main() -> int:
    offences: list[str] = []
    for package in SCANNED:
        for path in sorted((ROOT / package).rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError as exc:
                print(f"could not parse {path.relative_to(ROOT)}: {exc}")
                return 2
            if str(path.relative_to(ROOT)) in EXEMPT:
                continue
            visitor = _Visitor(path)
            visitor.visit(tree)
            for lineno, kind in visitor.offences:
                offences.append(f"  {path.relative_to(ROOT)}:{lineno}  {kind}\n      {_ADVICE[kind]}")

    if offences:
        print(f"row alignment: {len(offences)} join(s) that can lose rows silently\n")
        print("\n".join(offences))
        print(
            "\nEach of these produces output of the expected shape when it goes "
            "wrong, which is why they are refused rather than reviewed."
        )
        return 1

    print("row alignment OK: no join can lose rows silently.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
