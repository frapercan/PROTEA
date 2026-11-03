"""The catalogue of producer sentinels has to cover what the tree writes.

A sentinel legitimised BY WRITING stops being legitimate the moment somebody
adds a seventh form and does not write it down, and nothing would fail: the
column is a string, every value fits, and the three graph nodes that reason off
the catalogue would simply reason off a shorter list. So the test reads the
tree. It is the same shape as
``test_count_backend_parameters.test_the_map_names_what_each_backend_executes``,
and for the same reason: the knowledge is duplicated between a constant and the
code it describes, so the constant has to be checked against the code.

TWO DIRECTIONS, AND THE FIRST ONE IS THE ONE THAT BITES. Writing the column
through a named constant is what makes the catalogue complete by construction,
so a bare literal anywhere is the defect; and a constant nobody writes any more
reads as a live form and would never be exercised.
"""

from __future__ import annotations

import re
from pathlib import Path

from protea.core.producer_sentinels import (
    CLASSIFIER,
    INTERPRO_GRAFT,
    LITERAL_SENTINELS,
    NO_PRODUCER,
    is_literal_sentinel,
)

#: An assignment of a bare STRING LITERAL to the column. A dynamic value
#: (``method_name``, ``inputs.ref_acc``, ``voters[0][0]``) is deliberately not
#: matched: it cannot be checked against a list of constants, and the module
#: docstring says which three those are and why no predicate can tell them.
LITERAL_WRITE = re.compile(r'"ref_protein_accession":\s*"([^"]*)"')

#: An assignment of a bare identifier, which is how a named constant reads.
NAMED_WRITE = re.compile(r'"ref_protein_accession":\s*([A-Z_][A-Z_0-9]*)\b')

PACKAGE = Path(__file__).resolve().parents[1] / "protea"


def _writes(pattern: re.Pattern[str]) -> dict[str, list[str]]:
    """``{matched text: [files that write it]}`` over the whole package."""
    found: dict[str, list[str]] = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        for hit in pattern.findall(path.read_text(encoding="utf-8")):
            found.setdefault(hit, []).append(str(path.relative_to(PACKAGE)))
    return found


def test_no_bare_literal_is_written_into_the_producer_column():
    """The one that fails when a seventh form arrives unwritten.

    Every fixed producer value goes through ``producer_sentinels``, so the
    catalogue covers the tree by construction instead of by somebody
    remembering to add a line to a docstring.
    """
    bare = _writes(LITERAL_WRITE)

    assert not bare, (
        f"{bare} writes a bare string into go_prediction.ref_protein_accession. "
        "Name it in protea.core.producer_sentinels, with what it means, and "
        "write the constant: the graph's producer sentence reasons off that "
        "list, so a form missing from it is a surface stating something it "
        "cannot know."
    )


def test_every_named_sentinel_is_still_written_somewhere():
    """A stale constant reads as a form in use and is never exercised."""
    named = _writes(NAMED_WRITE)

    for constant in ("NO_PRODUCER", "CLASSIFIER", "INTERPRO_GRAFT"):
        assert named.get(constant), (
            f"nothing writes producer_sentinels.{constant} any more, so the "
            "catalogue names a form this tree does not produce"
        )


def test_an_opaque_producer_is_not_a_literal_sentinel():
    """The asymmetry the module exists to state: these cannot be told apart."""
    assert not is_literal_sentinel("sfjepa-recipe-ens8-cut211")  # a method name
    assert not is_literal_sentinel("Q7Z5P9")  # a donor accession
    assert not is_literal_sentinel("IPR000719")  # an InterPro accession


def test_the_three_literals_are_recognised():
    assert is_literal_sentinel(NO_PRODUCER)
    assert is_literal_sentinel(CLASSIFIER)
    assert is_literal_sentinel(INTERPRO_GRAFT)
    assert LITERAL_SENTINELS == {NO_PRODUCER, CLASSIFIER, INTERPRO_GRAFT}


def test_none_is_not_reported_as_a_sentinel():
    """The column is NOT NULL, so a None is a caller's bug, not a seventh form."""
    assert not is_literal_sentinel(None)
