"""T3.6 full audit: ``nullable=`` flag vs ``Mapped[]`` annotation parity.

Walk every ORM model under ``protea.infrastructure.orm.models`` and assert
that for each ``mapped_column``, the ``nullable=`` flag matches whether
the type annotation includes ``None``. Catches the silent drift that hit
``RerankerModel.prediction_set_id`` and ``evaluation_set_id`` in the
9-may audit (declared ``nullable=True`` with annotation
``Mapped[uuid.UUID]`` instead of ``Mapped[uuid.UUID | None]``).

The collector uses ``ast`` rather than a single-line regex so that
multi-line ``mapped_column(...)`` calls (the majority of FK columns)
are covered. The previous regex-based pass found 271 columns; the AST
pass finds every column SQLAlchemy actually compiles.

Pinned as a regression test: any future ORM addition with mismatched
nullability fails CI before review.
"""

from __future__ import annotations

import ast
import os
from typing import NamedTuple

import pytest

ORM_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "protea",
    "infrastructure",
    "orm",
    "models",
)


class _Column(NamedTuple):
    path: str
    lineno: int
    field: str
    annotation: str
    nullable: bool | None  # ``None`` when no explicit kw was supplied
    is_primary_key: bool


def _mapped_column_call(value: ast.expr) -> ast.Call | None:
    """Return the ``mapped_column(...)`` call node, or ``None``.

    ``mapped_column`` is sometimes wrapped (e.g. ``deferred(mapped_column(...))``).
    We unwrap one level of call to find the underlying ``mapped_column``.
    """
    if not isinstance(value, ast.Call):
        return None
    func = value.func
    name = (
        func.id
        if isinstance(func, ast.Name)
        else func.attr
        if isinstance(func, ast.Attribute)
        else None
    )
    if name == "mapped_column":
        return value
    # One level of wrapping (e.g. ``deferred(mapped_column(...))``).
    for arg in value.args:
        inner = _mapped_column_call(arg)
        if inner is not None:
            return inner
    return None


def _collect_columns() -> list[_Column]:
    """Return every ``mapped_column`` declaration in the ORM tree.

    Uses ``ast`` to handle multi-line calls. Returns each column
    regardless of whether ``nullable=`` was supplied — callers filter as
    needed.
    """
    rows: list[_Column] = []
    for dirpath, _, files in os.walk(ORM_ROOT):
        for fname in files:
            if not fname.endswith(".py"):
                continue
            path = os.path.join(dirpath, fname)
            with open(path) as fh:
                src = fh.read()
            tree = ast.parse(src)
            for node in ast.walk(tree):
                if not (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name)
                    and node.value is not None
                ):
                    continue
                call = _mapped_column_call(node.value)
                if call is None:
                    continue
                nullable_kw: bool | None = None
                is_pk = False
                for kw in call.keywords:
                    if (
                        kw.arg == "nullable"
                        and isinstance(kw.value, ast.Constant)
                        and isinstance(kw.value.value, bool)
                    ):
                        nullable_kw = kw.value.value
                    if (
                        kw.arg == "primary_key"
                        and isinstance(kw.value, ast.Constant)
                        and kw.value.value is True
                    ):
                        is_pk = True
                ann = ast.unparse(node.annotation)
                rows.append(
                    _Column(
                        path=path,
                        lineno=node.lineno,
                        field=node.target.id,
                        annotation=ann,
                        nullable=nullable_kw,
                        is_primary_key=is_pk,
                    )
                )
    return rows


def _annotation_admits_none(annotation: str) -> bool:
    """``True`` iff the annotation includes ``None`` / ``Optional``.

    Strips a leading ``Mapped[...]`` wrapper before inspecting so that
    e.g. ``Mapped[uuid.UUID | None]`` is read as the inner union.
    """
    inside = annotation.strip()
    if inside.startswith("Mapped[") and inside.endswith("]"):
        inside = inside[len("Mapped[") : -1]
    # Token-aware check: avoid matching attribute names containing
    # ``None`` (none exist today, but cheap insurance).
    tokens = {t.strip() for t in inside.replace("|", ",").split(",")}
    if "None" in tokens or "Optional" in inside:
        return True
    return False


def test_all_orm_columns_have_nullable_aware_annotations() -> None:
    """Annotation must include ``None`` iff ``nullable=True``.

    Only columns with an explicit ``nullable=`` keyword are checked;
    columns that rely on SQLAlchemy's defaulting (primary keys, etc.)
    are exempt — they are non-nullable by construction and annotated as
    non-Optional.
    """
    mismatches: list[str] = []
    for col in _collect_columns():
        if col.nullable is None:
            continue
        has_none = _annotation_admits_none(col.annotation)
        if has_none != col.nullable:
            mismatches.append(
                f"{col.path}:{col.lineno}  {col.field}: {col.annotation}  nullable={col.nullable}"
            )
    if mismatches:
        joined = "\n  ".join(mismatches)
        pytest.fail("ORM column annotation/nullable mismatch (T3.6):\n  " + joined)


def test_columns_without_explicit_nullable_are_primary_keys() -> None:
    """Columns that omit ``nullable=`` must be primary keys.

    SQLAlchemy's defaulting rules make ``nullable`` depend on PK status
    and the annotation's ``Optional``-ness. To keep the audit's promise
    crisp ("explicit ``nullable=`` matches annotation"), we require the
    only exemption to be primary keys, which are always non-nullable and
    annotated as non-Optional.
    """
    offenders: list[str] = []
    for col in _collect_columns():
        if col.nullable is not None:
            continue
        if col.is_primary_key and not _annotation_admits_none(col.annotation):
            continue
        offenders.append(
            f"{col.path}:{col.lineno}  {col.field}: {col.annotation}  (no explicit nullable=)"
        )
    if offenders:
        joined = "\n  ".join(offenders)
        pytest.fail(
            "ORM column without explicit nullable= must be a non-Optional "
            "primary key (T3.6):\n  " + joined
        )


def test_audit_finds_at_least_some_columns() -> None:
    """Sanity check: the collector should find a substantial set of columns.

    Catches accidental breakage of the AST walker (e.g. someone
    refactoring all columns to a helper that changes the call shape).
    Threshold reflects the AST-based pass, which is materially higher
    than the old regex pass.
    """
    rows = _collect_columns()
    assert len(rows) >= 200, (
        f"audit only found {len(rows)} columns; the AST collector likely needs updating"
    )
