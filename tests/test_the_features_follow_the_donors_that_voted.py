"""Every emitted row carries the features of the donor it names.

WHY THIS TEST EXISTS. ``pair_features`` is built from PROTEA's pre-search and
the rows come from the method's own search. The two see the same bank members
and do not compute identical distances: same values, different array layout,
summed in a different order, differing at the 1e-7 level. Measured against the
2026-08-29 run's own cached arrays, the stored distances differ from the
pre-search ordering on 736 of 861 control rows, median 1.19e-07. That is
harmless for ranking except where the k-th distance is a tie, and ties are not
rare here: 38,694 sequences are shared by more than one protein, and a shared
sequence is an identical embedding.

Where a tie straddles the cut the two searches keep different donors, and the
donor the method kept had no key. The row was emitted with all fifteen
pair-feature columns NULL, which is exactly what an uncomputable pair looks
like, so nothing distinguished them: 76 rows of 2,441,584, 64 of them at the
deepest slot, when 33 of the 37 pairs already had their alignment in the cache.

The repair is keyed on what was actually emitted, so there is nothing left for
a tie to fall through. These tests drive it with a donor the pre-search never
saw, which is the situation itself and not an approximation of it.
"""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np

from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    _repair_pair_features,
)


class _Payload:
    """Only the two flags the repair reads."""

    def __init__(self, alignments: bool = True, taxonomy: bool = False) -> None:
        self.compute_alignments = alignments
        self.compute_taxonomy = taxonomy


def _inputs(payload: _Payload) -> AdapterInputs:
    return AdapterInputs(
        p=payload,  # type: ignore[arg-type]
        valid_accessions=["Q1"],
        query_embeddings=np.zeros((1, 4), dtype=np.float32),
        ref_data={"accessions": ["R1", "R2"]},
        annotations={},
        go_id_map={},
        go_aspect_map={},
        prediction_set_id=uuid.uuid4(),
        # R2 is the donor the pre-search never proposed. Its sequence is here
        # because the method's bank is a subset of the pre-search union, so a
        # donor it keeps always has a loaded sequence.
        ref_sequences={"R1": "MKV", "R2": "MKW"},
        query_sequences={"Q1": "MKV"},
        ref_tax_ids={"R1": None, "R2": None},
        query_tax_ids={"Q1": None},
    )


def _row(query: str, donor: str) -> dict[str, Any]:
    return {"protein_accession": query, "ref_protein_accession": donor}


def test_a_donor_the_pre_search_never_proposed_is_repaired() -> None:
    rows = [_row("Q1", "R1"), _row("Q1", "R2")]
    pair_features = {("Q1", "R1"): {"identity_nw": 1.0}}

    repaired = _repair_pair_features(_inputs(_Payload()), rows, pair_features)

    assert repaired == 1
    # The row that was already keyed is left exactly as predict left it. The
    # repair only touches what predict could not, so a run where the two
    # searches agree is bit-identical with and without it.
    assert rows[0] == {"protein_accession": "Q1", "ref_protein_accession": "R1"}
    # The one that was not keyed now carries real numbers, not fifteen NULLs.
    assert rows[1].get("identity_nw") is not None
    assert rows[1]["length_query"] == 3
    # The map itself is repaired too, because the v6 enricher reads it.
    assert ("Q1", "R2") in pair_features


def test_agreement_costs_nothing_and_says_nothing() -> None:
    """The expected case has to be free, or the guard becomes the cost."""
    rows = [_row("Q1", "R1")]
    pair_features = {("Q1", "R1"): {"identity_nw": 0.7}}

    assert _repair_pair_features(_inputs(_Payload()), rows, pair_features) == 0
    assert rows[0] == {
        "protein_accession": "Q1",
        "ref_protein_accession": "R1",
    }


def test_features_switched_off_repairs_nothing() -> None:
    """With both flags off there are no pair features to be missing.

    Without this the repair would align every pair of a run that deliberately
    asked for no alignments, which is expensive and wrong.
    """
    rows = [_row("Q1", "R1"), _row("Q1", "R2")]
    payload = _Payload(alignments=False, taxonomy=False)

    assert _repair_pair_features(_inputs(payload), rows, {}) == 0
    assert "identity_nw" not in rows[1]


def test_one_missing_pair_repairs_every_row_that_names_it() -> None:
    """A pair is missing once and can appear on many rows.

    Each (query, donor) pair carries one row per GO term it voted for, so a
    single missing key drops the columns from a whole group. Counting pairs
    while repairing only the first row would leave most of the damage.
    """
    rows = [_row("Q1", "R2") for _ in range(4)]
    for i, row in enumerate(rows):
        row["go_term_id"] = i

    repaired = _repair_pair_features(_inputs(_Payload()), rows, {})

    assert repaired == 1
    assert all(r.get("identity_nw") is not None for r in rows)
