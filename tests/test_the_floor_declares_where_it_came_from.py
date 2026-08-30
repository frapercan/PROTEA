"""The two sigmas name a source, and the source has to be reachable.

WHY THIS TEST EXISTS. ``_SIGMA_PAIRED`` decides what the graph panels call a
separation, through ``detectable_effect``. It was documented as "Derived in
ABLATION-ARCHITECTURE.md from a fold study". That file is on no trunk of any
repository on this machine: it exists only inside agent-farm PR #246, unmerged
since 2026-08-17, whose own population figure of 5,674 protein-aspect units is
contradicted by the live frame's 17,438. ``_SIGMA_ROUTING`` cited
SURVIVOR-CASCADE.md, which is in the same position inside PR #257.

A constant whose derivation cannot be read is not a measurement, it is a habit.
The comments now say so, and this test keeps them saying so: it fails if
someone restores a citation to a file that is not present, which is the exact
way the claim became unverifiable in the first place.

It does not assert the VALUES. Those are provisional by construction and the
comment says what replaces them: the first two arms evaluated on 9995651a,
which has 0 evaluations today, produce the paired sigma from data that exists.
"""

from __future__ import annotations

import re
from pathlib import Path

_SOURCE = Path(__file__).resolve().parents[1] / "protea/api/routers/_graph_panels.py"
_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Documents the constants were once attributed to, which are not on any trunk.
_ABSENT_SOURCES = ("ABLATION-ARCHITECTURE.md", "SURVIVOR-CASCADE.md")


def _sigma_block(name: str) -> str:
    """The comment block immediately above a constant's assignment."""
    text = _SOURCE.read_text()
    at = text.index(f"{name} = ")
    start = text.rindex("\n\n", 0, at)
    return text[start:at]


def test_the_absent_documents_really_are_absent() -> None:
    """The premise. If one of them lands, this test should be the reminder."""
    for name in _ABSENT_SOURCES:
        found = [p for p in _REPO_ROOT.rglob(name) if ".git" not in p.parts]
        assert not found, (
            f"{name} now exists at {found}; the constant it backs can be "
            "re-derived and its comment should stop calling it unreachable"
        )


def test_neither_sigma_claims_a_derivation_a_reader_cannot_open() -> None:
    for constant in ("_SIGMA_PAIRED", "_SIGMA_ROUTING"):
        block = _sigma_block(constant)
        for name in _ABSENT_SOURCES:
            if name not in block:
                continue
            # Naming it is allowed, and useful. Claiming it as the derivation
            # without saying it is unreachable is not.
            assert re.search(r"no trunk|not in this repository|unmerged", block, re.I), (
                f"{constant} cites {name} without saying it cannot be read"
            )


def test_the_paired_sigma_says_what_would_replace_it() -> None:
    """A provisional number that does not name its replacement stays forever."""
    block = _sigma_block("_SIGMA_PAIRED")
    assert "9995651a" in block
    assert "per_protein" in block or "_persist_per_protein_grid" in block


def test_the_floor_is_still_computed() -> None:
    """Documenting the doubt must not turn the guard off.

    A panel with no floor invites reading a gap of 0.002 as a result, which is
    the thing the floor exists to prevent, and is worse than a floor whose
    provenance is under repair.
    """
    from protea.api.routers._graph_panels import detectable_effect

    assert detectable_effect(17438) is not None
    assert detectable_effect(0) is None
    assert detectable_effect(None) is None
    # Smaller populations resolve less, which is the whole content of it.
    assert detectable_effect(1109) > detectable_effect(17438)
