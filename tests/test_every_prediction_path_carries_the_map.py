"""Every path that predicts supplies the sequence map, or none of them should.

There are two places that build ``AdapterInputs`` and therefore two ways
to write a candidate row. Only one of them was wired when the sequence
rank landed, so the aspect-separated path would have written rows with a
donor ledger and no sequence rank on them.

That is the worst of the three states. A prediction set with no ranks at
all is refused by ``_depth_unit_guard`` and says so. A set with ranks
everywhere is answerable. A set with ranks on the part that came through
one path is *partial*, and partial is what the guard has to refuse with a
count rather than a reason, because from the outside it looks like a
half-finished migration rather than a code path nobody wired.

So this walks the source instead of trusting a list: every construction
of ``AdapterInputs`` must name ``ref_sequence_identities``. A third path
added later fails here on the day it is added.
"""

from __future__ import annotations

import ast
import pathlib

_ROOT = pathlib.Path(__file__).resolve().parents[1] / "protea"
_FIELD = "ref_sequence_identities"


def _constructions() -> list[tuple[str, int, set[str]]]:
    """Every ``AdapterInputs(...)`` call in the tree, with its keyword names."""
    found: list[tuple[str, int, set[str]]] = []
    for path in _ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name != "AdapterInputs":
                continue
            found.append((
                str(path.relative_to(_ROOT.parent)),
                node.lineno,
                {kw.arg for kw in node.keywords if kw.arg},
            ))
    return found


def test_there_is_more_than_one_way_to_write_a_candidate_row() -> None:
    """The premise. If this ever drops to one, the test below is trivia."""
    assert len(_constructions()) >= 2


def test_every_one_of_them_supplies_the_sequence_map() -> None:
    missing = [
        f"{path}:{line}"
        for path, line, keywords in _constructions()
        if _FIELD not in keywords
    ]
    assert not missing, (
        f"{len(missing)} prediction path(s) build AdapterInputs without "
        f"{_FIELD!r}, so rows written through them carry no sequence rank while "
        f"rows from the other paths do. That leaves the prediction set partly "
        f"ranked, which a depth cut in sequences must refuse: {missing}"
    )
