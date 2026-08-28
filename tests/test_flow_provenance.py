"""Provenance is a set, and zero is not a member of it.

The two properties this file pins are the ones that make unique reach
computable, and both are easy to lose in a refactor that "simplifies" the mask
to a single value.
"""

from __future__ import annotations

import pytest

from protea.core.flows import LEGACY_FLOW, Flow, is_unique_to, proposed_by


def test_a_candidate_can_be_proposed_by_several_flows():
    """The interesting case, not the exception.

    Unique reach is 'proposed by this one and by no other', so a schema that
    records a single winner destroys the quantity being measured before it is
    measured.
    """
    both = int(Flow.NEIGHBOUR_TRANSFER | Flow.CLASSIFIER)
    assert proposed_by(both, Flow.NEIGHBOUR_TRANSFER)
    assert proposed_by(both, Flow.CLASSIFIER)
    assert not is_unique_to(both, Flow.CLASSIFIER)
    assert not is_unique_to(both, Flow.NEIGHBOUR_TRANSFER)


def test_unique_means_alone():
    alone = int(Flow.CLASSIFIER)
    assert is_unique_to(alone, Flow.CLASSIFIER)
    assert not is_unique_to(alone, Flow.NEIGHBOUR_TRANSFER)


def test_zero_is_not_unique_to_anything():
    """Zero means 'we did not record it', never 'nobody proposed it'.

    The column exists so that 'no flow proposes this term here' becomes a
    sayable statement. If an unrecorded row counted as that statement, the
    affinity map would read absence of instrumentation as absence of signal,
    which is the exact confusion the flow column is added to remove.
    """
    for f in Flow:
        assert not is_unique_to(0, f)
        assert not proposed_by(0, f)


def test_legacy_rows_are_neighbour_transfer_and_not_zero():
    """Every row written before this column existed came from one flow.

    Backfilling to that flow is a fact about how those rows were produced.
    Backfilling to zero would assert something false about 128 million rows.
    """
    assert LEGACY_FLOW is Flow.NEIGHBOUR_TRANSFER
    assert int(LEGACY_FLOW) != 0


def test_the_bits_are_distinct_powers_of_two():
    """A mask only works if no two flows share a bit and none is zero."""
    seen = set()
    for f in Flow:
        v = int(f)
        assert v != 0, f"{f.name} would be indistinguishable from unrecorded"
        assert v & (v - 1) == 0, f"{f.name}={v} is not a single bit"
        assert v not in seen, f"{f.name} collides with an earlier flow"
        seen.add(v)


def test_a_configuration_is_not_a_flow():
    """The distinction the ladder never made.

    Neighbour transfer over a different embedding is the same flow configured
    differently. If configurations got their own bits, the mask would grow with
    every backbone and unique reach would count configurations rather than
    sources, which is the axis two closed steps of the ladder varied without
    ever varying the mechanism.
    """
    names = {f.name for f in Flow}
    for banned in ("ANKH", "ESM", "T5", "K30", "POOLED", "RESIDUE", "LAYER"):
        assert not any(banned in n for n in names), (
            f"{banned} names a configuration, not a source plus a mechanism"
        )
    assert len(list(Flow)) <= 16, "a mask of flows should not grow like a mask of arms"


@pytest.mark.parametrize("flow", list(Flow))
def test_the_migration_backfill_value_matches_the_enum(flow):
    """The migration hardcodes 1 rather than importing this module.

    That duplication is deliberate (a migration must not import application
    code) and therefore needs a test, or the two drift and 128 million rows end
    up labelled with a flow that no longer means what the number said.
    """
    if flow is Flow.NEIGHBOUR_TRANSFER:
        assert int(flow) == 1
