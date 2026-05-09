"""T3.6 partial: nullable-flag vs ``Mapped[]`` type-annotation parity.

Walk every ORM model under ``protea.infrastructure.orm.models`` and assert
that for each ``mapped_column``, the ``nullable=`` flag matches whether
the type annotation includes ``None``. Catches the silent drift that hit
``RerankerModel.prediction_set_id`` and ``evaluation_set_id`` in the
9-may audit (declared ``nullable=True`` with annotation
``Mapped[uuid.UUID]`` instead of ``Mapped[uuid.UUID | None]``).

Pinned as a regression test: any future ORM addition with mismatched
nullability fails CI before review.
"""
from __future__ import annotations

import os
import re

import pytest

ORM_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "protea",
    "infrastructure",
    "orm",
    "models",
)


_PATTERN = re.compile(
    r"(\w+):\s*Mapped\[([^\]]+)\]\s*=\s*mapped_column\(((?:[^()]|\([^)]*\))*)\)",
    re.DOTALL,
)


def _collect_columns() -> list[tuple[str, int, str, str, bool]]:
    """Return ``(path, lineno, field, type_, is_nullable)`` for every match.

    ``is_nullable`` is ``None`` when the call body lacks an explicit
    ``nullable=`` flag (those rows are filtered out — they default to
    ``True`` but parity is enforced only when authors made the choice
    explicit).
    """
    rows: list[tuple[str, int, str, str, bool]] = []
    for dirpath, _, files in os.walk(ORM_ROOT):
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(dirpath, f)
            text = open(path).read()
            for m in _PATTERN.finditer(text):
                field, type_, body = m.groups()
                nullable_match = re.search(r"nullable\s*=\s*(True|False)", body)
                if not nullable_match:
                    continue
                is_nullable = nullable_match.group(1) == "True"
                lineno = text.count("\n", 0, m.start()) + 1
                rows.append((path, lineno, field, type_.strip(), is_nullable))
    return rows


def test_all_orm_columns_have_nullable_aware_annotations() -> None:
    """Type annotation must include ``None`` iff the column is nullable."""
    mismatches = []
    for path, lineno, field, type_, is_nullable in _collect_columns():
        has_none = "None" in type_ or "Optional" in type_
        if has_none != is_nullable:
            mismatches.append(
                f"{path}:{lineno}  {field}: Mapped[{type_}]  nullable={is_nullable}"
            )
    if mismatches:
        joined = "\n  ".join(mismatches)
        pytest.fail(
            "ORM column annotation/nullable mismatch (T3.6):\n  " + joined
        )


def test_audit_finds_at_least_some_columns() -> None:
    """Sanity check: the regex should match a substantial set of columns.

    Catches accidental breakage of the pattern itself (e.g. someone
    refactoring all columns to a helper that changes the line shape).
    """
    rows = _collect_columns()
    assert len(rows) >= 30, (
        f"audit only found {len(rows)} columns; "
        "the regex pattern likely needs updating"
    )
