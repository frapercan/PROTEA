"""The mark is stated once, in ``protea.core.marks``, and every other module imports it.

The campaign's test, the 227->230 window, is held out for the decisive read, so
the mark is the value every guard of the universe compares against (ADR-D60). Two copies of
it are two marks the day one of them is edited, and a guard reading the stale
copy admits what the other refuses. The scan below reads every module of the
package and of ``scripts/`` and fails on a second statement of either value: a
name for the mark or the ceiling bound to a literal, a comparison against the
mark's number, or the ceiling's release name written out.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from protea.core.marks import MARK_RELEASE, UNIPROT_CEILING

_ROOT = Path(__file__).resolve().parents[1]
_MARKS = _ROOT / "protea" / "core" / "marks.py"

#: A name that holds the mark or the ceiling, however a module spells it.
_MARK_NAME = re.compile(r"(?:^|_)MARK(?:$|_)|CEILING", re.IGNORECASE)

#: The ways the two values could be written down a second time.
_RESTATED: frozenset[object] = frozenset(
    {MARK_RELEASE, str(MARK_RELEASE), f"v{MARK_RELEASE}", UNIPROT_CEILING}
)

#: The modules the plan names as readers of the mark.
_READERS = (
    "protea/core/cut_photo/measure.py",
    "protea/core/split_registry.py",
    "protea/core/band_registry.py",
)


def _modules() -> Iterator[Path]:
    for top in ("protea", "scripts"):
        for path in sorted((_ROOT / top).rglob("*.py")):
            if path != _MARKS:
                yield path


def _bound_names(node: ast.Assign | ast.AnnAssign) -> list[str]:
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    return [t.id for t in targets if isinstance(t, ast.Name)]


def _restatements(source: str) -> list[str]:
    """Every place in ``source`` that writes the mark or the ceiling down again."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(node.value, ast.Constant):
            if node.value.value in _RESTATED:
                for name in _bound_names(node):
                    if _MARK_NAME.search(name):
                        found.append(f"line {node.lineno}: {name} = {node.value.value!r}")
        elif isinstance(node, ast.Compare):
            for operand in (node.left, *node.comparators):
                if (
                    isinstance(operand, ast.Constant)
                    and type(operand.value) is int
                    and operand.value == MARK_RELEASE
                ):
                    found.append(f"line {node.lineno}: a comparison against {MARK_RELEASE}")
        elif isinstance(node, ast.Constant) and node.value == UNIPROT_CEILING:
            found.append(f"line {node.lineno}: the ceiling {UNIPROT_CEILING!r} written out")
    return found


class TestTheMarkItself:
    def test_the_mark_does_not_move(self) -> None:
        # The test it guards was read once and is spent; moving the mark would
        # let the universe read the window that read used.
        assert MARK_RELEASE == 227
        assert UNIPROT_CEILING == "2025_04"

    def test_the_module_imports_nothing(self) -> None:
        tree = ast.parse(_MARKS.read_text(encoding="utf-8"))
        imports = [n for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))]
        assert imports == [], "a leaf module: the photo builder reads it with no database stack"


class TestEveryReaderImportsIt:
    @pytest.mark.parametrize("relative", _READERS)
    def test_the_reader_takes_it_from_the_leaf_module(self, relative: str) -> None:
        tree = ast.parse((_ROOT / relative).read_text(encoding="utf-8"))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "protea.core.marks"
            for alias in node.names
        }
        assert "MARK_RELEASE" in imported

    def test_the_board_mark_is_the_mark_spelled_as_a_release(self) -> None:
        from protea.core.split_registry import BOARD_MARK

        assert BOARD_MARK == f"v{MARK_RELEASE}"

    def test_the_photo_guard_reads_the_same_value(self) -> None:
        from protea.core.cut_photo import measure

        assert measure.MARK_RELEASE == MARK_RELEASE


class TestNoOtherModuleRestatesIt:
    @pytest.mark.parametrize(
        "source",
        [
            "MARK_RELEASE = 227",
            "BOARD_MARK = 'v227'",
            "_MARK: int = 227",
            "UNIPROT_CEILING = '2025_04'",
            "if cut_release > 227:\n    pass",
            "def refuse(release):\n    return release >= '2025_04'",
        ],
    )
    def test_the_scan_sees_a_restatement(self, source: str) -> None:
        # Without this the scan below could pass by seeing nothing.
        assert _restatements(source)

    @pytest.mark.parametrize(
        "source",
        [
            "from protea.core.marks import MARK_RELEASE\nBOARD_MARK = f'v{MARK_RELEASE}'",
            "BANDS = {'v227': None}",
            "window = ('v211', 'v227')",
            "BENCHMARK_TOKEN = 'v227'",
            "count = 227",
        ],
    )
    def test_the_scan_leaves_a_release_named_as_a_release(self, source: str) -> None:
        # A band, a window or a count that happens to be 227 is not the mark.
        assert _restatements(source) == []

    def test_no_module_restates_the_mark_or_the_ceiling(self) -> None:
        offenders = {
            str(path.relative_to(_ROOT)): found
            for path in _modules()
            if (found := _restatements(path.read_text(encoding="utf-8")))
        }
        assert offenders == {}, f"import them from protea.core.marks instead: {offenders}"
