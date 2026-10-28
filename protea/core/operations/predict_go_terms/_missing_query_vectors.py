"""Say which queries the population join dropped, instead of scoring fewer.

``_load_query_embeddings`` resolves the query population with an INNER JOIN
against ``sequence_embedding`` on ``(sequence_id, embedding_config_id)``, and
its own docstring says what that means: "only accessions that actually have an
embedding are included". A member of the declared query set that has no vector
under this configuration is therefore not an error and not a zero row. It
simply is not scored, and nothing in the run says so.

WHY THAT IS A COMPARABILITY DEFECT AND NOT A LOGGING GAP. Two configurations
scored against one ``query_set_id`` are supposed to answer the same question
about the same proteins. If one of them is missing vectors, it answers about
FEWER proteins, under the same query-set id, with a receipt that reads
identically. Dividing the paired sigma by sqrt(n) uses an n that was never
checked. It is the single-field comparison this campaign keeps paying for, in
the population rather than in the metric.

MEASURED 2026-10-28, which is what prompted this. The full-length ankh-base
bank ``f3a7fe16`` will be missing roughly 151 sequences that do not fit on the
compute node's card, while the truncated ``e7ba37da`` has a vector for every
one of them. Of the 40.531 targets of query set ``c92deab6``, 25 sit on those
sequences, and they are not a random 0,062%: they run from 8.784 to 34.350
residues with a mean of 11.202, which is the long tail entire. The donor side
is worse and this module does not address it -- 138 annotated proteins would
have no vector in that bank, which changes what the other 40.506 queries can
retrieve.

This module does not fix any of that. It makes the hole SAY its size, so the
decision is taken before the numbers are read rather than after, and it does so
for every configuration and not just this one: a bank that is short a vector
for any other reason drops its queries exactly the same way.

The accession list is capped, because a batch dispatched against the wrong
configuration drops every query it holds and a receipt is not a place for a
thousand ids. The COUNT is never capped and ``truncated`` says when the list
is.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from uuid import UUID

from protea.core.contracts.operation import EmitFn

#: How many dropped accessions the event lists before truncating. The count and
#: the flag survive the cap, so the size of the hole is never the thing lost.
MAX_LISTED = 200


def report_queries_without_vector(
    requested: Sequence[str],
    scored: Iterable[str],
    *,
    embedding_config_id: UUID | str,
    emit: EmitFn,
) -> list[str]:
    """Emit which of ``requested`` were dropped for want of a vector; return them.

    Order follows ``requested``, so two runs over the same batch list the same
    accessions in the same order and a diff between receipts is readable.
    Silent when nothing was dropped, which is the ordinary path.
    """
    have = set(scored)
    dropped = [a for a in requested if a not in have]
    if not dropped:
        return []
    emit(
        "predict_go_terms.queries_without_vector",
        None,
        {
            "embedding_config_id": str(embedding_config_id),
            "requested": len(requested),
            "scored": len(requested) - len(dropped),
            "dropped": len(dropped),
            "accessions": dropped[:MAX_LISTED],
            "truncated": len(dropped) > MAX_LISTED,
            "reason": "the declared query set holds these, this embedding config "
                      "has no vector for them, and the population join is an inner "
                      "join, so they are not scored at all. A second config that "
                      "does have their vectors answers about more proteins under "
                      "the same query_set_id",
        },
        "warning",
    )
    return dropped


__all__ = ["MAX_LISTED", "report_queries_without_vector"]
