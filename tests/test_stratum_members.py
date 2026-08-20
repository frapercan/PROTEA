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


def test_a_cell_opens_with_the_code_its_own_table_displays():
    # Every user-facing surface names an aspect BPO / MFO / CCO: the
    # benchmark matrix, the strata panel, the evaluation results JSON.
    # stratum_for takes the single-char wire code, so opening a cell with
    # the value printed IN that cell raised KeyError('BPO') and the caller
    # got a 500. Anyone wiring a screen would have hit it on click one.
    from protea.api.routers.stratum_members import _aspect_code

    assert _aspect_code("BPO") == "P"
    assert _aspect_code("MFO") == "F"
    assert _aspect_code("CCO") == "C"


def test_the_wire_code_still_works_because_the_column_holds_it():
    from protea.api.routers.stratum_members import _aspect_code

    assert _aspect_code("P") == "P"
    assert _aspect_code("F") == "F"
    assert _aspect_code("C") == "C"


def test_an_unknown_aspect_is_a_bad_request_naming_the_vocabulary():
    # Not a KeyError escaping as a 500. A caller who guessed the spelling
    # needs to be told which spellings exist.
    import pytest
    from fastapi import HTTPException

    from protea.api.routers.stratum_members import _aspect_code

    with pytest.raises(HTTPException) as excinfo:
        _aspect_code("BP")
    assert excinfo.value.status_code == 422
    assert "BPO" in str(excinfo.value.detail)


def test_the_row_does_not_claim_a_donor_it_cannot_name():
    # The docstring said every row carries its nearest donor. It carries
    # the identity to that donor and whether the donor is experimental;
    # Neighbourhood does not resolve the accession, so the row cannot name
    # it. A reader can check the distance but not follow it, and the
    # difference is the difference between checking a band and trusting it.
    import inspect

    from protea.api.routers.stratum_members import _walk, stratum_members

    doc = inspect.getdoc(stratum_members) or ""
    assert "does NOT yet carry WHICH donor" in doc

    emitted = inspect.getsource(_walk)
    assert '"best_identity"' in emitted
    assert '"donor_accession"' not in emitted
