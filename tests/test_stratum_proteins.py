"""Opening a panel into the proteins it is made of.

The panel prints a population and a pooled score. Neither can be opened, so a
reader who doubts one of them has nowhere to go. These tests pin the two things
that make the drill-down worth trusting: the population it reports is the
panel's own, and the bands it prints beside a protein are the bands that
protein was pooled into.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi import HTTPException

from protea.api.routers.stratum_proteins import (
    _SORTS,
    _join,
    _namespace_for,
    _ordered,
)
from protea.core.strata import Neighbourhood


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _row(accession: str, namespace: str = "biological_process", f: float = 0.5) -> dict:
    return {
        "protein_accession": accession,
        "namespace": namespace,
        "tau": 0.31,
        "tp_w": 1.0,
        "pred_w": 2.0,
        "n_gt_w": 2.0,
        "precision_w": 0.5,
        "recall_w": 0.5,
        "f_w": f,
    }


def _hood(identity: float | None = 45.0, experimental: bool = False) -> Neighbourhood:
    return Neighbourhood(
        best_identity=identity,
        donor_is_experimental=experimental,
        taxonomic_relation="same",
        nearest_any=None,
        nearest_experimental=None,
    )


def test_the_population_is_the_panels_own_and_not_the_query_sets():
    # This is the whole reason the endpoint reads the per-protein artefact
    # rather than the membership router. The artefact's rows for one namespace
    # under one setting ARE the panel: the category is the directory and the
    # aspect is the column. A protein scored in another aspect belongs to
    # another panel and must not be counted here.
    frame = _frame(
        [
            _row("P1"),
            _row("P2"),
            _row("P3", namespace="molecular_function"),
        ]
    )
    placed = _join(
        frame[frame["namespace"] == "biological_process"],
        "biological_process",
        {"P1": _hood(), "P2": _hood()},
        {"P1": 300, "P2": 300},
        "NK",
    )
    assert len(placed.rows) == 2


def test_a_protein_that_cannot_be_placed_stays_in_the_panel():
    # It scored, so it is part of the population the panel prints. Dropping it
    # would make this page's count smaller than the panel's for no reason a
    # reader could see. It carries null bands instead, because a band it was
    # never measured into would be worse than an absence.
    frame = _frame([_row("P1"), _row("P2")])
    placed = _join(frame, "biological_process", {"P1": _hood()}, {"P1": 300, "P2": 300}, "NK")

    assert len(placed.rows) == 2
    assert placed.placed == 1
    assert placed.no_donor == 1
    orphan = next(r for r in placed.rows if r["accession"] == "P2")
    assert orphan["length_band"] is None
    assert orphan["homology_band"] is None
    assert orphan["best_identity"] is None
    # It still carries its score. The score is a fact about the evaluation and
    # does not depend on the retrieval being placeable.
    assert orphan["f_w"] == 0.5


def test_the_three_ways_of_being_unplaced_are_counted_apart():
    # No donor is a retrieval fact, no length is a corpus fact, and an identity
    # outside [0, 100] is a defect. One total would report a number that three
    # different actions would fix.
    frame = _frame([_row("P1"), _row("P2"), _row("P3")])
    placed = _join(
        frame,
        "biological_process",
        {"P1": _hood(), "P2": _hood(identity=140.0), "P3": _hood()},
        {"P1": 300, "P2": 300},
        "NK",
    )
    assert (placed.no_donor, placed.no_length, placed.off_scale) == (0, 1, 1)
    assert placed.placed == 1


def test_the_score_is_read_and_never_recomputed():
    # The panel's number is a micro sum over exactly these columns. A page that
    # recomputed F from precision and recall would disagree with the panel by a
    # rounding nobody chose.
    frame = _frame([_row("P1", f=0.1234)])
    placed = _join(frame, "biological_process", {"P1": _hood()}, {"P1": 300}, "NK")
    assert placed.rows[0]["f_w"] == 0.1234


def test_one_threshold_or_none_at_all():
    # Every row of a namespace is scored at the threshold the run reported.
    # Two thresholds in one artefact would mean naming one of them attributes
    # every row to a threshold it was not scored at.
    one = _frame([_row("P1"), _row("P2")])
    assert _join(one, "biological_process", {}, {}, "NK").tau == 0.31

    mixed = _frame([_row("P1"), {**_row("P2"), "tau": 0.44}])
    assert _join(mixed, "biological_process", {}, {}, "NK").tau is None


def test_absent_identities_sort_last_under_every_ordering():
    # A protein with no donor has no identity. Sorting it as if it had one of
    # zero puts the retrieval failures at the head of a table about homology,
    # where they read as the hardest cases rather than as the absent ones.
    rows = [
        {"accession": "A", "best_identity": None, "f_w": 0.9},
        {"accession": "B", "best_identity": 20.0, "f_w": 0.1},
        {"accession": "C", "best_identity": 80.0, "f_w": 0.5},
    ]
    assert [r["accession"] for r in _ordered(rows, "identity_asc")] == ["B", "C", "A"]
    assert [r["accession"] for r in _ordered(rows, "identity_desc")] == ["C", "B", "A"]


def test_the_default_ordering_opens_on_what_failed():
    # The reason to open a cell is usually to see what makes it hard.
    rows = [
        {"accession": "A", "best_identity": 20.0, "f_w": 0.9},
        {"accession": "B", "best_identity": 80.0, "f_w": 0.0},
    ]
    assert _ordered(rows, "f_asc")[0]["accession"] == "B"
    assert "f_asc" == _SORTS[0]


def test_ties_are_broken_by_accession_so_paging_is_stable():
    # Scores tie in bulk: every protein the run predicted nothing for scores
    # exactly zero. Without a tiebreak, page two can repeat a row from page one
    # after the cache is rebuilt.
    rows = [
        {"accession": "C", "best_identity": 10.0, "f_w": 0.0},
        {"accession": "A", "best_identity": 20.0, "f_w": 0.0},
        {"accession": "B", "best_identity": 30.0, "f_w": 0.0},
    ]
    assert [r["accession"] for r in _ordered(rows, "f_asc")] == ["A", "B", "C"]


def test_a_panel_opens_with_the_code_the_panel_itself_prints():
    # Every user-facing surface names an aspect BPO / MFO / CCO. The strata
    # artefact's column carries P / F / C. Both arrive here.
    assert _namespace_for("BPO") == "biological_process"
    assert _namespace_for("P") == "biological_process"
    assert _namespace_for("mfo") == "molecular_function"
    assert _namespace_for("C") == "cellular_component"


def test_an_unknown_aspect_is_a_bad_request_naming_the_vocabulary():
    with pytest.raises(HTTPException) as excinfo:
        _namespace_for("BP")
    assert excinfo.value.status_code == 422
    assert "BPO" in str(excinfo.value.detail)


def test_the_module_says_why_it_is_not_the_membership_endpoint():
    # The two endpoints answer different questions and the difference is a
    # factor of nine on this campaign. A reader wiring a screen has to be told
    # which one a panel drills into.
    import protea.api.routers.stratum_proteins as mod

    assert "members" in mod.__doc__
    assert "14,025" in mod.__doc__


def test_the_cell_says_how_much_of_it_scored_nothing():
    # A cell reading 0.09 over 375 proteins is one claim when the mass is
    # spread and a different one when most of it is exactly zero. The pooled
    # number cannot tell them apart, and neither can a page of rows: the wall
    # of zeros the weakest-first ordering opens on is a finding once the reader
    # knows how many there are and a mystery until then.
    import inspect

    from protea.api.routers.stratum_proteins import stratum_proteins

    src = inspect.getsource(stratum_proteins)
    assert '"scored_zero"' in src
    # A count, never a rate. A share would be a pooled statistic over the cell,
    # which is the collapse this project refuses.
    assert 'scored_zero = sum(1 for r in rows if r["f_w"] == 0.0)' in src


def test_the_module_does_not_claim_the_two_panel_counts_are_equal():
    # They are counted from different places. The graph counts proteins that
    # GAINED a term of an aspect in a bucket, from the window's ground truth;
    # this counts proteins cafaeval SCORED. On this campaign they agree to the
    # protein on eight of the nine panels and differ by one on PK x BPO, under
    # both prediction sets. A docstring claiming identity would have made that
    # one look like a defect in this endpoint.
    import protea.api.routers.stratum_proteins as mod

    assert "5,810" in mod.__doc__ and "5,811" in mod.__doc__
    assert "scored population" in mod.__doc__
