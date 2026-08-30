"""What the adapter computes survives all the way to the insert row.

WHY THIS TEST EXISTS. There are two gates between a computed pair feature and
the database, and each is a hand-written list of field names:
``protea_method.pipeline.PAIR_FEATURE_KEYS`` decides what is copied onto a
prediction row, and ``_row_from_prediction`` decides what is copied onto the
insert. A field the adapter computes and either list omits is computed, paid
for, and thrown away in silence.

Three were. ``compute_taxonomy`` returns ``taxonomic_lca``, and
``_pair_features_for`` sets ``query_taxonomy_id`` and ``ref_taxonomy_id``
beside it. All three name real GOPrediction columns and none was in
PAIR_FEATURE_KEYS, so all four prediction sets carry 0 non-null values in
7,082,480 rows, on runs where compute_taxonomy was on and its three sibling
columns were filled on the same rows. A null lca reads as a pair with no common
ancestor, which is an answer the function really returns, so nothing looked
wrong.

An existing test walks the other direction: given a prediction dict, every key
that names a column reaches the row. It could not see this, because its
prediction dict is hand-written and never contained the three missing names.
This one starts from what the adapter actually computes, so a field that is
produced and dropped is visible without anyone having to think of it.
"""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np
from protea_method.pipeline import PAIR_FEATURE_KEYS, propagate_pair_features

from protea.core.operations._predict_go_terms_adapter import (
    AdapterInputs,
    _pair_features_for,
)
from protea.core.operations.predict_go_terms._common import _row_from_prediction
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

_COLUMNS = {c.name for c in GOPrediction.__table__.columns}

#: Same taxon on both sides. compute_taxonomy returns early on equality, so the
#: lca is filled without reaching for a lineage this test cannot load, and the
#: assertion is about plumbing rather than about NCBI.
_TAXON = 9606


def _inputs() -> AdapterInputs:
    class _P:
        compute_alignments = True
        compute_taxonomy = True

    return AdapterInputs(
        p=_P(),  # type: ignore[arg-type]
        valid_accessions=["Q1"],
        query_embeddings=np.zeros((1, 4), dtype=np.float32),
        ref_data={"accessions": ["R1"]},
        annotations={},
        go_id_map={},
        go_aspect_map={},
        prediction_set_id=uuid.uuid4(),
        ref_sequences={"R1": "MKWVTFISLL"},
        query_sequences={"Q1": "MKWVTFISLL"},
        ref_tax_ids={"R1": _TAXON},
        query_tax_ids={"Q1": _TAXON},
    )


def _computed() -> dict[str, Any]:
    return _pair_features_for(_inputs(), "Q1", "R1")


def test_the_adapter_computes_the_three_that_were_lost() -> None:
    """The premise. Without this the test below could pass vacuously."""
    computed = _computed()
    for field in ("taxonomic_lca", "query_taxonomy_id", "ref_taxonomy_id"):
        assert field in computed, field
        assert computed[field] is not None, field


def test_no_computed_field_is_dropped_by_the_field_list() -> None:
    computed = _computed()
    dropped = sorted(k for k in computed if k not in set(PAIR_FEATURE_KEYS))
    assert not dropped, (
        f"{len(dropped)} field(s) are computed and never copied onto a row, so "
        f"they stay null on every row forever: {dropped}"
    )


def test_no_computed_field_is_dropped_by_the_row_builder() -> None:
    """The second gate. Surviving the first one is not enough."""
    prediction: dict[str, Any] = {
        "protein_accession": "Q1",
        "go_term_id": 7,
        "ref_protein_accession": "R1",
        "distance": 0.1,
    }
    propagate_pair_features(prediction, _computed())
    row = _row_from_prediction(prediction, uuid.uuid4())

    named = {k for k in prediction if k in _COLUMNS}
    dropped = sorted(k for k in named if row.get(k) is None)
    assert not dropped, (
        f"{len(dropped)} field(s) reach the prediction and not the insert: {dropped}"
    )


def test_the_taxonomy_block_arrives_whole() -> None:
    """Named individually, because a count passes when one is swapped for another.

    The three that were filled and the three that were not are asserted
    together on purpose: what made this invisible for four prediction sets is
    that half the block arrived and looked like all of it.
    """
    prediction: dict[str, Any] = {
        "protein_accession": "Q1",
        "go_term_id": 7,
        "ref_protein_accession": "R1",
        "distance": 0.1,
    }
    propagate_pair_features(prediction, _computed())
    row = _row_from_prediction(prediction, uuid.uuid4())

    assert row["taxonomic_lca"] == _TAXON
    assert row["query_taxonomy_id"] == _TAXON
    assert row["ref_taxonomy_id"] == _TAXON
    assert row["taxonomic_distance"] == 0
    assert row["taxonomic_common_ancestors"] == 1
    assert row["taxonomic_relation"] == "same"
