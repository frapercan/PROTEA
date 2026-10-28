"""Give a sequence the card cannot fit a batch of its own.

WHY A BATCH IS THE UNIT OF LOSS. A batch that raises CUDA OOM is retried five
times with exponential backoff and then DEAD-LETTERED: the consumer frees the
cache, republishes with a bumped header, and past the cap nacks without requeue
and writes ``child.cuda_oom_dead_letter`` on the parent at error level. That is
the right behaviour and it is not silent. What it cannot do is save the
sequences that merely sat next to the impossible one, because the partition is
by POSITION in an unordered list: one sequence over the card's ceiling takes the
other 63 of its batch down with it.

MEASURED on the ankh-base full-length run of 2026-10-28 (job ``cbaba8bb``,
``batch_size=1``, so there is no padding waste and the cost is a single
sequence). The ceiling is not the attention matrix alone: ``T5Attention`` in
transformers 4.48.1 holds FOUR tensors of ``L^2`` per layer at once -- the bf16
scores, the masked ``position_bias`` that is built in the first layer and stays
alive through all 48, and the fp32 copy and output of the softmax -- which is
about ``144 * L^2`` bytes, three times the attention term by itself. On the
12 GB card running this job the longest sequence that fits is therefore around
**8.650 residues**, not the 14.400 the attention term alone suggests, and the
longest one actually embedded so far is 7.763.

Of the 594.512 sequences still pending, **146 are above that ceiling** and 211
are above the longest known to have fitted. The longest is 35.991 residues,
which by this path would ask for some 186 GB in a layer -- and its bias-bucket
matrix alone is 10,4 GB of int64 in one allocation. At 64 sequences a batch
those 146 would dead-letter up to **9.198** sequences, a 1,5% hole in the bank
instead of 0,02%, and cost 6,3 h of a sleeping consumer at 155 s of backoff
each. The eight longest pending are all over 33.000 residues, so they can also
arrive consecutively.

The first version of this module put the ceiling near 14.400 from the attention
term alone. The three tensors it left out are the difference between 57
sequences and 146, and the compute node found them by reading its own
transformers version rather than by the run failing.

WHY THE CEILING IS A PAYLOAD FIELD AND NOT A CONSTANT. The coordinator runs on
the host that owns the state, which has no card and cannot know the device's
memory; the operator can, and the number also depends on the transformers
version in the worker's environment rather than on the model alone. It defaults
to absent, and with no ceiling the partition is exactly the one every job
already dispatched in this deployment was built with.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.infrastructure.orm.models.sequence.sequence import Sequence


def partition_sequence_ids(
    sequence_ids: list[int],
    sequences_per_job: int,
    isolate_above: int | None,
    lengths: dict[int, int] | None,
) -> list[list[int]]:
    """Split the run into batches, isolating anything longer than the ceiling.

    Order is preserved and nothing is dropped: the concatenation of the
    batches is ``sequence_ids``. ``isolate_above`` is STRICT, because the
    number names the longest length known to FIT. A sequence whose length is
    not in ``lengths`` reads as 0 and is not isolated, which is the direction
    that does not change the partition on an incomplete lookup.

    :raises ValueError: when a ceiling is given with no lengths to apply it to.
        Partitioning as though nothing were long would silently produce the
        behaviour the caller asked to avoid.
    """
    if isolate_above is None:
        return [
            sequence_ids[i : i + sequences_per_job]
            for i in range(0, len(sequence_ids), sequences_per_job)
        ]
    if lengths is None:
        raise ValueError(
            "isolate_above_residues was given but no sequence lengths were loaded, "
            "so no sequence can be recognised as over-long and every batch would "
            "be partitioned as if none were"
        )
    batches: list[list[int]] = []
    run: list[int] = []
    for sid in sequence_ids:
        if lengths.get(sid, 0) > isolate_above:
            if run:
                batches.append(run)
                run = []
            batches.append([sid])
            continue
        run.append(sid)
        if len(run) == sequences_per_job:
            batches.append(run)
            run = []
    if run:
        batches.append(run)
    return batches


def load_lengths_for_isolation(
    session: Session, sequence_ids: list[int], p: Any, emit: EmitFn
) -> dict[int, int] | None:
    """Residue counts for this run, or ``None`` when nobody asked for a ceiling.

    Only queried when a ceiling is set, so the ordinary path pays for no extra
    read. The receipt names the LONGEST sequence in the run as well as how many
    were isolated, because choosing the ceiling needs the distribution and the
    operator would otherwise be guessing: on the run above the longest was
    35.991 residues while the median was 332.

    ``would_have_shared_a_batch_with`` is the number this module exists for:
    the sequences that a dead-lettered batch would have taken with it.
    """
    isolate_above = p.isolate_above_residues
    if isolate_above is None:
        return None
    rows = session.execute(
        select(Sequence.id, func.length(Sequence.sequence)).where(
            Sequence.id.in_(sequence_ids)
        )
    ).all()
    lengths = {int(sid): int(n or 0) for sid, n in rows}
    isolated = sum(1 for n in lengths.values() if n > isolate_above)
    emit(
        "compute_embeddings.isolated_long_sequences",
        None,
        {
            "isolate_above_residues": int(isolate_above),
            "isolated": isolated,
            "longest": max(lengths.values(), default=0),
            "sequences": len(lengths),
            "would_have_shared_a_batch_with": isolated * (p.sequences_per_job - 1),
        },
        "warning" if isolated else "info",
    )
    return lengths


__all__ = ["load_lengths_for_isolation", "partition_sequence_ids"]
