"""Opening a stratum cell, and being honest about what the count is.

The strata panel says a cell holds 778 proteins and scores 0.0842. It does
not say which, so the chain from a published number to the thing it is
about stops at the count.
"""

from __future__ import annotations

from protea.api.routers.stratum_members import _Where, stratum_members


def test_the_response_names_the_band_population_not_a_total():
    # The router filters on length and homology, which it computes, and
    # NOT on category or aspect, which the caller asserts. So the count is
    # the band's population and exceeds the cell's. A field called "total"
    # would read as a contradiction of the panel rather than as a
    # different quantity, which is how a number gets quoted wrong.
    import inspect

    src = inspect.getsource(stratum_members)
    assert '"band_population"' in src
    # The response KEY specifically. A local named total is fine; a
    # response field called total is the thing that gets quoted.
    assert '"total":' not in src


def test_the_module_says_what_it_does_not_filter_on():
    import protea.api.routers.stratum_members as mod

    assert "ASSERTED" in mod.__doc__
    assert "not about the neighbourhood" in mod.__doc__


def test_truncation_is_named_rather_than_implied():
    # A capped list that does not say it is capped reads as the whole cell.
    import inspect

    assert '"truncated"' in inspect.getsource(stratum_members)


def test_the_asserted_axes_say_so_in_their_own_descriptions():
    # The docstring is not enough: a caller reading the generated schema
    # sees only the parameter description.
    fields = _Where.__dataclass_fields__
    assert "ASSERTED" in str(fields["category"].default.description)
    assert "asserted" in str(fields["aspect"].default.description).lower()
