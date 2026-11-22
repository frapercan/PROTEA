"""The query's own-neighbour exclusion, read one way by both searches of a batch.

A batch searches twice. PROTEA's pre-search decides whose annotations are
loaded, and the method's own search produces the rows. Both have to drop the
same proteins at the same depth, or the method reaches past what the
pre-search delivered and refills the gap from whoever shared the batch
(tests/test_the_batch_does_not_choose_the_donors.py). So the pre-search applies
the rule through protea-method's own margin and drop, reading the same two
payload fields the method's configuration carries.

The rule is protea-method's ``resolve_self_exclusion``: ``self_exclusion`` when
the payload names it, otherwise ``exclude_self_neighbour`` (true is "sequence",
false is "none"). "sequence" drops the query and its twins, other proteins with
the identical sequence. "accession" drops the query only and keeps its twins as
donors, since they are other proteins annotated by others.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from protea_method._self_by_sequence import resolve_self_exclusion


def needs_sequence_keys(p: Any) -> bool:
    """Whether the pre-search needs the whole bank's sequence identities first.

    Only the by-sequence rule does: it has to recognise every twin, and how deep
    to ask depends on how many twins the bank holds. The by-accession rule drops
    the query by name and asks one deeper when the query is in the bank.
    """
    return bool(resolve_self_exclusion(p) == "sequence")


def exclusion_view(p: Any, sequence_keys: dict[str, str] | None) -> SimpleNamespace:
    """The payload as protea-method's ``self_exclusion_margin`` and ``drop_self`` read it.

    :raises RuntimeError: if the rule is by sequence and the identities are
        missing. protea-method would then drop nothing and say nothing.
    """
    if needs_sequence_keys(p) and sequence_keys is None:
        raise RuntimeError(
            "the by-sequence exclusion needs the bank's sequence identities, read "
            "before the pre-search; without them no twin would be dropped"
        )
    return SimpleNamespace(
        self_exclusion=getattr(p, "self_exclusion", None),
        exclude_self_neighbour=bool(getattr(p, "exclude_self_neighbour", False)),
        k=p.limit_per_entry,
        sequence_keys=sequence_keys,
    )
