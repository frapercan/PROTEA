"""Every form ``go_prediction.ref_protein_accession`` takes, named in one place.

The column is NOT NULL and names the row's PRODUCER, which is a donor protein
on the retrieval path and something else on every other path. That was decided
on 2026-10-28 as the sentinel legitimised in writing rather than a nullable
column, and :mod:`protea.api.routers._graph_producer_column` is where the
graph's three nodes read the consequence.

WHY THIS MODULE EXISTS AND THE OTHER ONE WAS NOT ENOUGH. The writing said
"three forms are admitted" and the tree writes SIX. A sentinel is only
legitimised by writing if the writing is complete: an incomplete catalogue is
the same defect one level down, a surface reasoning off a list that does not
cover its own inputs. Measured on 2026-10-29 by enumerating every assignment to
the column across ``protea/``.

The six, and the distinction that matters is not how they look but whether a
READER can tell:

TOLD FROM THE VALUE ALONE, because they are fixed strings this module owns:

    NO_PRODUCER     ``""``            the training dump's own row for a term
                                      that no candidate proposed, built by
                                      ``_leaf_record_builder`` so the label
                                      column has a negative to learn from
    CLASSIFIER      ``"classifier"``  a classifier-only candidate, from
                                      ``predict_go_terms/_classifier`` and the
                                      dump's twin builder
    INTERPRO_GRAFT  ``"interpro"``    an InterPro-only BP candidate, from
                                      ``predict_go_terms/_interpro_graft``

NOT TOLD FROM THE VALUE, because all three are opaque strings:

    a donor protein accession, from the retrieval path
    an InterPro accession, which ``predict_go_terms_from_interpro`` writes for
      the domain that voted
    a method's name, which ``ingest_method_predictions`` writes

THE ASYMMETRY IS THE POINT, and it is why :func:`is_literal_sentinel` is the
only predicate here. "Does this row name a donor protein?" cannot be answered
from the value for the last three: a method name and an accession are both just
strings, and joining the column against ``protein`` to decide costs 9,0 s over
21 M rows against 0,05 s for the set-level question the graph asks instead. So
the producing OPERATION is the only thing that knows, and a reader that needs
the answer per set has to be told by the set and not by its rows.

WHAT THIS DEPLOYMENT ACTUALLY HOLDS, measured 2026-10-29 with a left join
against ``protein`` and not with a pattern: of 25.399.802 candidate rows,
**18.415.500 are a method's name** (12.159.300 ``sfjepa-recipe-ens8-cut211``
and 6.256.200 ``sfjepa-recipe-ens8-cut230``) and the rest are donor accessions.
None of the three literals above is present. So the gap this module closes is
LATENT: no stored number is wrong because of it today, and the reason to close
it is that the next arm to run a graft or a classifier would make the graph's
sentence false without anything failing.
"""

from __future__ import annotations

#: No producer: a term no candidate proposed, carried by the training dump so
#: the label column has a negative row. Never reaches ``go_prediction``.
NO_PRODUCER = ""

#: A classifier-only candidate: the head proposed the term with no neighbour.
CLASSIFIER = "classifier"

#: An InterPro-only BP candidate grafted onto a retrieval run.
INTERPRO_GRAFT = "interpro"

#: The forms a reader can recognise without asking the producing operation.
#: A test reads the tree and fails when a seventh literal appears without being
#: named here, which is the mechanism that keeps "in writing" true.
LITERAL_SENTINELS: frozenset[str] = frozenset({NO_PRODUCER, CLASSIFIER, INTERPRO_GRAFT})


def is_literal_sentinel(value: str | None) -> bool:
    """Whether this producer value is one of the fixed strings named above.

    False for a donor accession, for an InterPro accession and for a method's
    name alike, because none of the three can be distinguished from the others
    by looking at it. False for ``None`` as well: the column is NOT NULL, so a
    ``None`` reaching here is a caller's bug and not a seventh form, and
    reporting it as a sentinel would hide that.
    """
    return value is not None and value in LITERAL_SENTINELS


__all__ = [
    "CLASSIFIER",
    "INTERPRO_GRAFT",
    "LITERAL_SENTINELS",
    "NO_PRODUCER",
    "is_literal_sentinel",
]
