"""What the candidate's producer column means, and how much of the record uses it.

``go_prediction.ref_protein_accession`` is NOT NULL, and three nodes of the
graph argue off that one catalog row. The sentence they shared said "no
candidate CAN arrive without a donor", which is true of the column and false of
the record: the constraint is about NULL, and a producer that is not a donor
protein is exactly what an ingested method writes there.

So the column names the row's PRODUCER. Decided 2026-10-28, as the sentinel
legitimised in writing rather than a nullable column: the constraint keeps its
promise that nothing arrives anonymous, and the graph stops implying a stronger
one it cannot keep.

THE CATALOGUE LIVES IN :mod:`protea.core.producer_sentinels` AND IT IS SIX AND
NOT THREE. This module said "three forms are admitted" until 2026-10-29, and
enumerating every assignment to the column across ``protea/`` found six: the
three opaque ones named here before (a donor accession, an InterPro accession,
a method's name) and three fixed strings the tree also writes, ``""``,
``"classifier"`` and ``"interpro"``. A sentinel legitimised BY WRITING is only
legitimate while the writing covers its own inputs, so the list moved to one
module with a test that reads the tree and fails when a seventh literal appears.
"""

from __future__ import annotations

from typing import Any

from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
)

#: Why three nodes have nowhere to write their artifact while every candidate
#: must name something. Written once: they argue it off one catalog row, and a
#: second copy is the one that goes stale.
NO_DONORLESS_ROW = (
    "The candidate column naming the row's producer is NOT NULL, so a term that arrived with no "
    "producer at all has nowhere to be written, and neither has an answer carrying two flows. "
    "That is the shape of the record rather than its contents: no run moves it, only a "
    "migration. It does NOT mean every candidate names a donor protein: the column admits a "
    "donor accession, an InterPro accession, or a method's name, and a set produced by an "
    "ingested method carries the third."
)


def sentinel_sets(record: dict[str, list[dict[str, Any]]]) -> tuple[int, int]:
    """``(sets whose producer is not a donor protein, sets in all)``.

    COUNTED OVER SETS AND NEVER OVER ROWS, and that is not a simplification.
    The sentinel is a property of the producing operation, so it is constant
    within a prediction set, and the set count answers the same question for a
    fraction of the cost.

    Measured on this deployment 2026-10-28: joining the distinct
    ``ref_protein_accession`` values against ``protein`` to count the ROWS takes
    **9,0 s** over 21 M candidate rows, against **0,05 s** for the set-level
    question. This version costs nothing at all, because it crosses two lists
    the graph has already fetched.

    That is the same trap :func:`_graph_nodes._donor_required` documents for the
    NULL count, one quantity over: the affordable question is the one the
    catalog or a small table answers, never the one that scans the candidates.

    AND IT COUNTS ONE OF THE SIX FORMS, which is worth saying out loud because
    the sentence built from it reads as though it covered them all. The test is
    the SUBSTRATE's backend, so a set is counted when an ingested method
    produced it and not when a classifier head or an InterPro graft did, even
    though those write ``producer_sentinels.CLASSIFIER`` and
    ``producer_sentinels.INTERPRO_GRAFT`` in the same column and are no more a
    donor protein than a method name is.

    That is affordable TODAY and only today: measured 2026-10-29 with a left
    join against ``protein``, this deployment holds 18.415.500 method-name rows
    and zero rows carrying either literal, so the count is exact. It stops
    being exact the first time a graft or a classifier arm runs. The fix then is
    a producer kind recorded ON THE PREDICTION SET, not a scan of its rows: the
    9,0 s above is the measurement that rules the scan out, and a set-level
    column keeps this function free.
    """
    backend_of = {
        str(row.get("id")): row.get("model_backend") for row in record.get("substrates") or []
    }
    sets = record.get("prediction_sets") or []
    sentinel = sum(
        1
        for row in sets
        if backend_of.get(str(row.get("embedding_config_id"))) == INGESTED_METHOD_BACKEND
    )
    return sentinel, len(sets)


def sentinel_sentence(record: dict[str, list[dict[str, Any]]]) -> str:
    """The clause naming how much of the record carries a sentinel, or nothing.

    Said out loud because the constraint sentence beside it is about a column
    and a reader takes it for a statement about the record. These sets DID
    arrive without a donor protein; what they could not do is arrive without
    naming anything.

    Empty when no set carries one, so a deployment where every candidate really
    does name a donor reads exactly as it did before.
    """
    sentinel, all_sets = sentinel_sets(record)
    if not sentinel:
        return ""
    return (
        f" {sentinel} of {all_sets} prediction sets were produced by an ingested method, "
        "so their producer column names the method and not a donor protein."
    )
