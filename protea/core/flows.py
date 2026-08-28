"""Which flow proposed a candidate, as a property of the candidate.

WHY THIS EXISTS. Every column on ``go_prediction`` describes ONE flow.
``ref_protein_accession`` names the donor a candidate came from and ``distance``
names what found it, which together are a complete description of neighbour
transfer over an embedding and no description at all of anything else. A
candidate proposed by the classifier has no donor; so does a candidate nobody
proposed. Today those two are the same row.

That gap is invisible in a ladder, where the question was always which
CONFIGURATION of one flow wins, and every candidate came from that flow by
construction. It is fatal to a graph, where the claim is that channelling beats
not channelling: unique reach ("what does this source propose that no other
source does, here") cannot be computed at all until a proposal carries its
source, and the affinity map is built out of unique reach.

A SET, NOT A VALUE. A candidate can be proposed by several flows at once, and
that is the interesting case rather than the exception: unique reach is
precisely "proposed by this one and by no other", so collapsing the provenance
to a single winner destroys the quantity being measured. The provenance is
therefore a set, stored as a bitmask so that it costs one small column on a
table of 128,191,567 rows rather than a join.

A FLOW IS A SOURCE PLUS A MECHANISM. Running neighbour transfer over a
different embedding is the SAME flow configured differently, and gets the same
bit. This is the distinction the ladder never made, and the reason two of its
steps closed without a winner: they varied the space of one mechanism and never
the mechanism.
"""

from __future__ import annotations

from enum import IntFlag


class Flow(IntFlag):
    """The mechanisms that can propose a candidate. One bit each.

    Values are frozen once assigned: a bit is an identity written into
    128 million rows, not a display order. Append, never renumber.
    """

    #: Transfer from annotated donors found by embedding proximity. The flow
    #: every existing row carries, and the only one the schema could express.
    NEIGHBOUR_TRANSFER = 1

    #: Transfer from donors found by sequence alignment rather than embedding.
    #: A different mechanism over the same source, hence a different flow.
    ALIGNMENT_TRANSFER = 2

    #: A full-vocabulary classifier scoring every term directly, with no donor.
    CLASSIFIER = 4

    #: The query's own pre-existing non-experimental annotation at t0.
    SELF_PRIOR = 8

    #: Terms implied by co-occurrence with terms already proposed.
    ASSOCIATION = 16

    #: Terms implied by ontology lineage over what is already proposed.
    LINEAGE = 32

    #: Domain-signature evidence mapped to terms.
    DOMAIN_SIGNATURE = 64

    #: Terms implied by the label space itself, from text or label embeddings.
    LABEL_SEMANTICS = 128


#: What every row written before flow provenance existed carries. Not zero:
#: zero would mean "no flow proposed this", which is the very statement the
#: column exists to make sayable, and no historical row is evidence for it.
LEGACY_FLOW = Flow.NEIGHBOUR_TRANSFER


def is_unique_to(mask: int, flow: Flow) -> bool:
    """True when ``flow`` proposed this candidate and nothing else did.

    The predicate unique reach is counted over. A mask of zero is never
    unique to anything: it means the provenance was not recorded, which is
    a different fact from "one flow proposed it".
    """
    return mask == int(flow)


def proposed_by(mask: int, flow: Flow) -> bool:
    """True when ``flow`` is among the proposers, alone or with others."""
    return bool(mask & int(flow))
