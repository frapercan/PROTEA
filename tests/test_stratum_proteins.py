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


def _row(
    accession: str,
    namespace: str = "biological_process",
    f: float = 0.5,
    variant: str = "unweighted",
) -> dict:
    """What ``_sliced_at_the_published_tau`` hands ``_join``: scalars, divided.

    The grid stores the components over the whole sweep and the slicer divides
    them at the one threshold the panel published, so by the time ``_join``
    sees a row the ratios are already there and ``variant`` says which
    weighting they are. ``f`` is a parameter so a test can prove the value
    travels rather than being derived again downstream.
    """
    return {
        "protein_accession": accession,
        "namespace": namespace,
        "tau": 0.31,
        "variant": variant,
        "tp": 1.0,
        "pred": 2.0,
        "n_gt": 2.0,
        "precision": 0.5,
        "recall": 0.5,
        "f": f,
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
    assert orphan["f"] == 0.5


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


def test_the_score_travels_and_is_not_derived_again():
    # ``_join`` places and pages; it does not do arithmetic. The division from
    # the grid's components happens once, in the slicer, at the threshold the
    # panel published, which is why a row cannot disagree with the cell it is
    # inside. A second derivation here would reintroduce exactly that risk.
    frame = _frame([_row("P1", f=0.1234)])
    placed = _join(frame, "biological_process", {"P1": _hood()}, {"P1": 300}, "NK")
    assert placed.rows[0]["f"] == 0.1234
    assert placed.rows[0]["variant"] == "unweighted"


def test_the_weighting_is_carried_and_never_assumed():
    # Three of the seven scoring runs on this deployment were unweighted, and
    # the row used to call its numbers ``f_w`` whatever the run had done.
    frame = _frame([_row("P1", variant="weighted")])
    placed = _join(frame, "biological_process", {"P1": _hood()}, {"P1": 300}, "NK")
    assert placed.rows[0]["variant"] == "weighted"
    assert "f_w" not in placed.rows[0]


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

    from protea.api.routers.stratum_proteins import _response, _scored_zero

    # The count reaches the payload under its own name.
    assert '"scored_zero"' in inspect.getsource(_response)

    # A count, never a rate. A share would be a pooled statistic over the
    # cell, which is the collapse this project refuses.
    #
    # Asserted on the FUNCTION and not on a source line. This test used to pin
    # the exact expression ``sum(1 for r in rows if r["f_w"] == 0.0)``, and
    # when the row's metric lost its ``_w`` suffix the assertion went stale
    # while saying nothing about whether the count was still right. A test
    # that pins text is debt of the same kind it is meant to catch.
    counted = _scored_zero([_row("P1", f=0.0), _row("P2", f=0.4), _row("P3", f=0.0)])
    assert counted == 2
    assert isinstance(counted, int)


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


# ---------------------------------------------------------------------------
# Reading the grid: the fixture goes through the PRODUCTION writer, so the
# filename, the footer and the column layout are the producer's and not this
# file's. The endpoint used to read ``per_protein.parquet``, which
# ``rows_from_sink`` writes only for the weighted variant, so a run with no
# information_accretion_set_id wrote none and every panel of it answered 404
# saying the run had scored no proteins. Measured over the seven scoring runs
# on this deployment the correlation was perfect: four weighted runs wrote
# three files each, three unweighted runs wrote none.
# ---------------------------------------------------------------------------

_TAUS = (0.25, 0.5, 0.75)
_REPORTED = 0.5


def _grid_bytes(rows: list[dict], *, variant: str = "unweighted", setting: str = "NK") -> bytes:
    import io as _io

    from protea.core.operations._run_cafa_grid_artifact import (
        GRID_FILENAME,
        GridArtifact,
        GridFrame,
        write_grid_parquet,
    )

    frame = GridFrame(
        setting=setting,
        th_step=0.25,
        max_terms=None,
        normalization="cafa",
        prop="fill",
        no_orphans=True,
        ontology_snapshot_id="11111111-1111-1111-1111-111111111111",
        evaluation_set_id="22222222-2222-2222-2222-222222222222",
        information_accretion_frame="null",
        producer_git_sha="0" * 40,
    )
    artifact = GridArtifact(rows=rows, variants=(variant,), n_tau=len(_TAUS), dropped=[])
    import tempfile
    from pathlib import Path as _Path

    with tempfile.TemporaryDirectory() as tmp:
        path = write_grid_parquet(
            _Path(tmp) / setting / GRID_FILENAME, artifact, frame.stamp((variant,))
        )
        data = path.read_bytes()
    return _io.BytesIO(data).getvalue()


def _grid_row(accession: str, *, variant: str = "unweighted", tp: float = 1.0) -> dict:
    """Components over the sweep, WRONG either side of the reported threshold.

    A slice at the wrong column then shows up as a wrong number instead of as
    an index error.
    """
    from protea.core.operations._run_cafa_strata import VARIANT_COLUMNS

    tp_col, pred_col, gt_col = VARIANT_COLUMNS[variant]
    row = {
        "protein_accession": accession,
        "namespace": "biological_process",
        tp_col: [tp + 100.0, tp, 0.0],
        pred_col: [102.0, 2.0, 0.0],
        gt_col: 2.0,
    }
    # The producer requires the unweighted ``n_gt`` whatever variants a file
    # declares: it is the eligibility marker and the population denominator,
    # and the consumer reads it in both cases. So a weighted row carries both.
    row.setdefault("n_gt", 2.0)
    return row


def _best_f(tau: float = _REPORTED, ns: str = "biological_process") -> str:
    return f"filename\tns\ttau\tf\npredictions.tsv\t{ns}\t{tau}\t0.5\n"


def _slice(grid: bytes, best_f: str, ns: str = "biological_process"):
    import io as _io

    from protea.api.routers.stratum_proteins import _sliced_at_the_published_tau

    return _sliced_at_the_published_tau(
        _io.BytesIO(grid), best_f, ns, result_id="r", setting="NK"
    )


def test_the_slice_is_at_the_threshold_the_panel_published():
    frame = _slice(_grid_bytes([_grid_row("P1")]), _best_f())
    row = frame.to_dict("records")[0]
    assert row["tp"] == 1.0
    assert row["pred"] == 2.0
    assert row["precision"] == pytest.approx(0.5)
    assert row["recall"] == pytest.approx(0.5)
    assert row["f"] == pytest.approx(0.5)
    assert row["tau"] == _REPORTED


def test_an_unweighted_grid_is_read_under_its_own_name():
    # The whole point. This file used to be unreadable for such a run.
    frame = _slice(_grid_bytes([_grid_row("P1")], variant="unweighted"), _best_f())
    assert frame.to_dict("records")[0]["variant"] == "unweighted"


def test_a_weighted_grid_is_preferred_when_it_carries_one():
    frame = _slice(
        _grid_bytes([_grid_row("P1", variant="weighted")], variant="weighted"), _best_f()
    )
    assert frame.to_dict("records")[0]["variant"] == "weighted"


def test_a_namespace_the_run_never_reported_comes_back_empty():
    # Empty and not guessed: it is absent from the best-F table, so the run
    # published no threshold for it and there is no column to read.
    frame = _slice(_grid_bytes([_grid_row("P1")]), _best_f(ns="cellular_component"))
    assert frame.empty


def test_a_threshold_the_grid_does_not_carry_is_refused():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as err:
        _slice(_grid_bytes([_grid_row("P1")]), _best_f(tau=0.57))
    assert err.value.status_code == 409
    assert "not a threshold this grid carries" in str(err.value.detail)


def test_the_endpoint_asks_for_the_grid_and_the_best_f_table():
    # The seam. Both keys are resolved before the cache, and neither is the
    # legacy file: an absent legacy file reads as "this run scored nothing",
    # which was false for three of the seven runs here.
    import inspect

    from protea.api.routers import stratum_proteins as module

    source = inspect.getsource(module._panel_keys)
    assert "GRID_FILENAME" in source
    assert "BEST_F_FILENAME" in source
    # The legacy name survives in the module docstring, which explains why the
    # endpoint moved; what must not survive is a key built from it.
    assert "per_protein.parquet" not in source
    assert "per_protein.parquet" not in inspect.getsource(module._sliced_at_the_published_tau)


def test_the_zero_count_reads_the_metric_by_its_current_name():
    """The regression for a KeyError that shipped inside a rename.

    ``scored_zero`` was one line in the endpoint reading ``r["f_w"]``. The
    row's metric lost that suffix when the variant started travelling beside
    it, and nothing caught the stale key: ruff cannot see into a dict
    subscript, and no test reached the line. Every request would have raised.
    """
    from protea.api.routers.stratum_proteins import _scored_zero

    rows = [_row("P1", f=0.0), _row("P2", f=0.4), _row("P3", f=0.0)]
    assert _scored_zero(rows) == 2


def test_the_zero_count_is_a_count_and_not_a_share():
    # A share would be the pooled number again. The point of this one is that
    # a cell at 0.09 over 375 proteins is a different claim when two thirds of
    # the mass is exactly zero, and the pooled number cannot say which.
    from protea.api.routers.stratum_proteins import _scored_zero

    assert _scored_zero([_row("P1", f=0.0)] * 4) == 4
