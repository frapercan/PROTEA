"""Nothing may join two tables on ``go_term_id``.

WHY. ``go_term.id`` is an internal surrogate scoped to one ontology snapshot.
In this database 48,196 GO accessions carry more than one of them, up to nine
for a single accession. Two rows holding the same ``go_term_id`` are the same
term; two rows holding DIFFERENT ``go_term_id`` values may still be the same
term, named in different snapshots.

Under the temporal protocol that is not an edge case, it is the normal case: a
prediction set is produced against the t0 snapshot and scored against the t1
one, so every cross-set comparison straddles snapshots. Joining the surrogate
there silently matches nothing.

THE INCIDENT THIS ENCODES. ``_load_match_counts`` joined
``ProteinGOAnnotation.go_term_id == GOPrediction.go_term_id`` to compute the
``match_count`` precision proxy on the protein listing. Every prediction set in
the database resolves to a different snapshot than its annotation set, so the
proxy read zero for every protein, on every screen, for as long as it existed.
A 300-protein sample found 0 matches on the surrogate against 5,133 on the
accession. The docstring said ``(protein, go_id)`` throughout; only the code
disagreed.

The same trap has been hit three times in analysis queries across two machines,
twice producing a plausible wrong number rather than an obvious one. That is
what makes it worth a check rather than a comment: it fails quietly and the
result looks like data.

THE RULE. Resolve each side to ``go_term.go_id`` and compare the accessions.
Joining ``GOTerm.id == X.go_term_id`` is correct and untouched: that resolves a
surrogate within one snapshot, which is exactly what it is for.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "protea"

#: Attribute name that carries the snapshot-scoped surrogate.
SURROGATE = "go_term_id"


def _is_surrogate(node: ast.expr) -> str | None:
    """Owner name when ``node`` is ``<Owner>.go_term_id``, else None."""
    if isinstance(node, ast.Attribute) and node.attr == SURROGATE:
        owner = node.value
        if isinstance(owner, ast.Name):
            return owner.id
        if isinstance(owner, ast.Attribute):
            return owner.attr
    return None


def _surrogate_to_surrogate_comparisons(tree: ast.AST) -> list[tuple[int, str, str]]:
    """Every ``A.go_term_id == B.go_term_id`` where A and B differ."""
    found: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or len(node.ops) != 1:
            continue
        if not isinstance(node.ops[0], ast.Eq):
            continue
        left = _is_surrogate(node.left)
        right = _is_surrogate(node.comparators[0])
        if left and right and left != right:
            found.append((node.lineno, left, right))
    return found


def _python_sources() -> list[Path]:
    return sorted(p for p in ROOT.rglob("*.py") if "__pycache__" not in p.parts)


def test_the_surrogate_is_never_compared_across_tables() -> None:
    offenders: list[str] = []
    for path in _python_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for lineno, left, right in _surrogate_to_surrogate_comparisons(tree):
            rel = path.relative_to(ROOT.parent)
            offenders.append(f"{rel}:{lineno}: {left}.{SURROGATE} == {right}.{SURROGATE}")

    assert not offenders, (
        "Two tables joined on the snapshot-scoped surrogate. Resolve both sides "
        "to go_term.go_id and compare accessions instead:\n  " + "\n  ".join(offenders)
    )


def test_the_check_sees_the_shape_it_is_written_for() -> None:
    """The detector fires on the exact line that shipped the incident."""
    guilty = ast.parse(
        "q.join(Ann, (Ann.go_term_id == Pred.go_term_id) & (Ann.acc == Pred.acc))"
    )
    assert _surrogate_to_surrogate_comparisons(guilty)


@pytest.mark.parametrize(
    "innocent",
    [
        # Resolving a surrogate within one snapshot: the correct use.
        "q.join(GOTerm, GOTerm.id == GOPrediction.go_term_id)",
        # Comparing accessions: the fix.
        "q.join(a, (a.id == Ann.go_term_id) & (a.go_id == p.go_id))",
        # A surrogate against a literal or bound value is not a cross-table join.
        "q.filter(GOPrediction.go_term_id == wanted_id)",
        # Same owner on both sides is a tautology, not a snapshot straddle.
        "q.filter(Pred.go_term_id == Pred.go_term_id)",
    ],
)
def test_the_check_does_not_fire_on_correct_code(innocent: str) -> None:
    assert not _surrogate_to_surrogate_comparisons(ast.parse(innocent))
