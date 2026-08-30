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


def test_the_paired_sigma_carries_the_measurement_that_produced_it() -> None:
    """It was derived on 2026-08-30 and the derivation is in the file.

    A number that names its source in a repository nobody can open is what
    this constant was for a month. The nine panel values and the pair of arms
    they came from are now beside it, so the next person to doubt it can
    re-run the comparison instead of re-deriving the folklore.
    """
    from protea.api.routers._graph_panels import _SIGMA_PAIRED

    block = _sigma_block("_SIGMA_PAIRED")
    assert "9995651a" in block
    assert "MEASURED ON THIS CAMPAIGN" in block
    # The nine values it was taken from, so the block cannot be trimmed to a
    # bare assertion later.
    for value in ("0.1157", "0.4051", "0.2528"):
        assert value in block, value
    # The minimum of the nine, kept as the low end the docstring promises.
    assert _SIGMA_PAIRED == 0.1157


def test_the_floor_is_not_finer_than_the_class_it_names() -> None:
    """The failure the old value produced, asserted as a bound.

    0.081 was below every one of the nine measured values, so the panel
    reported a floor about three times finer than the record supports. Any
    future value has to stay at or above the smallest measured spread.
    """
    from protea.api.routers._graph_panels import _SIGMA_PAIRED

    smallest_measured = 0.1157  # PK.BPO, n=5810, the tightest of the nine
    assert _SIGMA_PAIRED >= smallest_measured


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


def test_the_routing_class_records_that_it_is_not_one_number() -> None:
    """Measured across five pairs, and it varies by a factor of six.

    The constant survives, but the comment now carries the reason it is only
    defensible for adjacent arms. A future reader who trims the table back to
    a bare value re-creates the state this repository was in for a month: a
    number nobody could check.
    """
    from protea.api.routers._graph_panels import _SIGMA_PAIRED, _SIGMA_ROUTING

    block = _sigma_block("_SIGMA_ROUTING")
    assert "MEASURED 2026-08-30" in block
    # The span the class actually covers, so the single value cannot be read
    # as if it were the whole story.
    for value in ("0.0638", "0.3983", "0.1215"):
        assert value in block, value
    assert _SIGMA_ROUTING == 0.13
    # And the prior that was wrong, kept so it is not rediscovered.
    assert "QUIETER" in block
    # Adjacent retrieval arms measured quieter than two scoring configs, which
    # is the opposite of what was assumed. Asserted so the two constants
    # cannot be silently reordered on the old intuition.
    assert _SIGMA_ROUTING > _SIGMA_PAIRED
