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

WHY THERE ARE TWO CEILINGS, WHICH IS NOT ONE KNOB TOO MANY. Isolation turns
9.198 lost sequences into 146, which is 98,4% of the damage, and leaves the
other cost untouched: an isolated sequence that still does not fit burns six
attempts and 155 s of backoff with the consumer stopped, because the queue runs
at prefetch 1 and the compute node has exactly one consumer. Waiting buys
nothing there -- the card will not empty itself while the only thing that uses
it is the worker that is waiting. For the 146 that is 6,9 h of a node producing
nothing.

So the two fields say two different things, each with its own measurement:

    isolate_above_residues   "I do not know whether this fits. Send it alone,
                             so a failure costs 1 sequence and not 64."
    refuse_above_residues    "I know this does not fit. Do not send it, and
                             record that the bank does not contain it."

AND THE TWO ASYMMETRIES RUN IN OPPOSITE DIRECTIONS, which is why neither
number is a default and why they are not the same number read twice. Isolating
one that would have fitted costs an extra message, about a second. Not
isolating one that does not fit sends 63 good sequences to the dead-letter
queue -- so the isolation ceiling goes DOWN when in doubt, to the highest
length actually observed to pass. Refusing one that would have fitted loses
that vector for good, while isolating one that does not fit costs 155 s -- so
the refusal ceiling goes UP when in doubt.

Measured 2026-10-28 at 17:10 UTC against the running job: the longest sequence that
has fitted is 8.002 residues, so that is the isolation ceiling. The refusal one
is harder, and 8.650 is an ESTIMATE of the crossover rather than a demonstrated
failure: the compute node's lower bound crosses usable memory somewhere between
8.640 and 8.700 depending on what the driver reserves, exceeds the card by
0,2 GB at 8.800 and by 0,7 GB at 9.000. Pending counts in that band: 200 above
8.002, 146 above 8.650, 145 above 8.700, 136 above 8.800, 112 above 9.000, with
individual lengths at 8.602, 8.648, 8.697, 8.710 and 8.749. Given the direction
of its asymmetry, the refusal ceiling belongs at the top of that band or above
it until the card has measured one.

WHICH IT IS ABOUT TO, AND THE TWO NUMBERS HAVE TO BE NAMED CAREFULLY. The
isolation ceiling is the longest sequence OBSERVED TO PASS. The refusal ceiling
is the LEAST MAXIMUM OF A DEAD BATCH, which is not the same thing as the
shortest sequence in one: memory grows with length, so a dead batch proves its
own maximum cannot run -- either that sequence failed, or a shorter one in the
same batch did and then the maximum would have failed too. The other long
sequences in a dead batch are NOT measured. Said loosely as "the shortest that
killed a message", the statistic reads as 6.713 on this run, which is simply
the shortest sequence that happened to share a batch with a doomed one.

THE DEAD BATCHES ARE READABLE FROM THE DATABASE, which is how the numbers below
were obtained and which corrects an earlier claim here that they were not.
Dispatch order is ascending ``sequence_id``: over the 103.104 rows written by
17:50 UTC the correlation between position-by-time and position-by-id is
exactly 1. Batches are 64 consecutive ids in that order, so a batch that failed
is SKIPPED -- its 64 ids stay unwritten while higher ids are already in -- and
the dead batches are the gaps inside the processed id range. Nothing in the
queue needs reading.

MEASURED THAT WAY: six gaps of exactly 64 ids, matching the six
``child.cuda_oom_retry`` events one for one, with maxima of 8.545, 8.922,
10.287, 10.746, 11.103 and 13.084 residues. Every gap holds a sequence above
8.002 and nothing above 8.002 has passed. So on this card these failures are
about LENGTH and not about the moment: there is no evidence here of a transient
out-of-memory, and an earlier version of this paragraph claimed the opposite
from a correlation that did not hold.

Of the 13 sequences above 8.002 in the processed range, 6 are their batch's
maximum and are therefore proven to fail; the other 7 are unmeasured. "13 of
13 failed" is the batch's observation attributed to its members, which is the
error this campaign keeps paying for, one level down.

SO THE EVIDENCE POINTS AT 8.544 AND NOT AT A ROUND 8.500. The field is strict,
so 8.544 refuses 8.545 and dispatches everything shorter. Between 8.500 and
8.545 nothing has been measured and four sequences live there -- 8.515, 8.525
twice and 8.527 -- so refusing at 8.500 would lose four vectors with no
evidence, 155 refused against 151, while isolating them costs at most 155 s
each. The refusal asymmetry says the ceiling goes up, and a round number is not
a measurement. It becomes final when a dead letter confirms it: so far there
are six retries and zero dead letters, because a republish goes to the TAIL of
the queue (``_republish_oom`` publishes to ``routing_key=self._queue_name``),
so those six batches have not been retried yet and will come back near the end
of the run.

AND THE FORECAST FOLLOWS THE REAL PARTITION, not one doomed sequence per batch.
The 187 pending sequences above 8.002 fall into only 96 distinct batches,
because long sequences cluster: 4,1 h of backoff rather than 6,9, and 6.144
sequences at risk of which 5.957 are bystanders. 73 of those 96 batches have a
maximum above 8.545 and 23 sit in the band the run is about to narrow.

WHAT THE EVENTS STILL DO NOT CARRY. ``child.cuda_oom_retry`` and
``child.cuda_oom_dead_letter`` record the operation, the retry count and the
error string, and no sequence ids. The gap query above answers the question
better than they would, so this is noted rather than worked around; the
dead-letter queue, filtered by job id, remains a second source.

A REFUSAL IS NOT A FAILURE, AND THAT IS THE POINT. The alternative the compute
node offered was to stop retrying isolated messages, which saves the same time
and leaves the hole just as quiet. A bank missing 146 proteins has to SAY so,
with the count, the lengths and the reason, rather than let a reader discover it
by subtracting row counts. The refusal is emitted at warning level with the
sequence ids, so the gap is a recorded decision instead of an absence.

TWO DEFECTS OF THE FIRST VERSION, both found in production and both in the
loader rather than in the arithmetic.

It put every id of the run into one ``IN``. That is fine for the 64 ids of a
batch and fatal for the population of a coordinator: job ``48c500ac`` died in
two seconds on 2026-10-29 with ``number of parameters must be between 0 and
65535`` over 399.952 ids, and left a 10,3 MB error message because the driver
echoes its parameters back. The unit tests could not see it -- they pass lists
of three -- so the query is now chunked at the size the annotation loaders
already use, and there is one answer in this codebase to "how many ids fit in
a query" rather than two.

And it loaded the lengths only when ``isolate_above_residues`` was set, while
:func:`refuse_over_ceiling` RAISES when handed a refusal ceiling and no
lengths. A payload asking only to refuse therefore died on a message that
blamed the caller for the loader's omission. The gate reads both fields now.

WHY BOTH ARE PAYLOAD FIELDS AND NOT CONSTANTS. The coordinator runs on the host
that owns the state, which has no card and cannot know the device's memory; the
operator can, and the number also depends on the transformers version in the
worker's environment rather than on the model alone. Both default to absent,
and with neither set the partition is exactly the one every job already
dispatched in this deployment was built with.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.config.tuning import get_tuning
from protea.core.contracts.operation import EmitFn
from protea.core.utils import chunks
from protea.infrastructure.orm.models.sequence.sequence import Sequence


def refuse_over_ceiling(
    sequence_ids: list[int],
    refuse_above: int | None,
    lengths: dict[int, int] | None,
) -> tuple[list[int], list[int]]:
    """``(dispatched, refused)``, splitting off what is known not to fit.

    Order is preserved in both lists. With no ceiling nothing is refused, which
    is the default and leaves every existing caller unchanged. A sequence whose
    length is not in ``lengths`` is dispatched, the same direction
    :func:`partition_sequence_ids` takes: an incomplete lookup must not decide
    that the bank lacks a protein.

    :raises ValueError: when a ceiling is given with no lengths, for the same
        reason as the sibling: refusing nothing because nothing could be
        measured is the silent version of the bug.
    """
    if refuse_above is None:
        return sequence_ids, []
    if lengths is None:
        raise ValueError(
            "refuse_above_residues was given but no sequence lengths were loaded, "
            "so no sequence can be recognised as over-ceiling and every one would "
            "be dispatched as if it fitted"
        )
    dispatched = [s for s in sequence_ids if lengths.get(s, 0) <= refuse_above]
    refused = [s for s in sequence_ids if lengths.get(s, 0) > refuse_above]
    return dispatched, refused


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


def transformers_version() -> str:
    """The installed transformers version, or ``"unknown"``.

    Read at refusal time and not declared, because the ceiling is a property of
    THIS worker's attention implementation: the same card and the same model
    give a different number under a version that keeps fewer tensors alive.
    """
    try:
        import transformers

        return str(transformers.__version__)
    except Exception:
        return "unknown"


def refuse_and_record(
    sequence_ids: list[int],
    p: Any,
    lengths: dict[int, int] | None,
    emit: EmitFn,
    device_label: str = "unknown",
) -> list[int]:
    """The ids still worth dispatching, with the refused ones written down.

    The receipt is the deliverable here, not the saving. It carries the count,
    the longest, and the ids themselves, so the bank's gap is a decision with a
    reason attached rather than a row count that does not add up. Emitted at
    warning, because nothing downstream can tell a protein that was refused
    from one that was never in the population.
    """
    dispatched, refused = refuse_over_ceiling(sequence_ids, p.refuse_above_residues, lengths)
    if refused:
        sizes = sorted((lengths or {}).get(s, 0) for s in refused)
        emit(
            "compute_embeddings.refused_over_ceiling",
            None,
            {
                "refuse_above_residues": int(p.refuse_above_residues),
                "refused": len(refused),
                "longest": sizes[-1],
                "shortest_refused": sizes[0],
                "sequence_ids": refused,
                "reason": "longer than the declared ceiling for this device, so the "
                          "forward pass cannot run; not dispatched rather than "
                          "retried five times and dead-lettered",
                # The ceiling belongs to three things at once, so the gap can be
                # closed when any of them changes rather than read as a property
                # of the corpus. An exact-attention path that never materialises
                # the matrix removes it entirely.
                "ceiling_belongs_to": {
                    "device": device_label,
                    "attention": "eager bf16",
                    "transformers": transformers_version(),
                },
            },
            "warning",
        )
    return dispatched


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

    Reads in CHUNKS and gates on BOTH ceilings; the module docstring says why.
    """
    isolate_above = p.isolate_above_residues
    refuse_above = getattr(p, "refuse_above_residues", None)
    if isolate_above is None and refuse_above is None:
        return None
    chunk_size = get_tuning().operation.annotation_chunk_size
    lengths: dict[int, int] = {}
    for chunk in chunks(list(sequence_ids), chunk_size):
        rows = session.execute(
            select(Sequence.id, func.length(Sequence.sequence)).where(
                Sequence.id.in_(list(chunk))
            )
        ).all()
        lengths.update({int(sid): int(n or 0) for sid, n in rows})
    if isolate_above is None:
        # Nothing to say about isolation when none was asked for, and saying it
        # with a ceiling of 0 would read as "every sequence was isolated".
        return lengths
    isolated = sum(1 for n in lengths.values() if n > isolate_above)
    emit(
        "compute_embeddings.isolated_long_sequences",
        None,
        {
            "isolate_above_residues": int(isolate_above),
            "isolated": isolated,
            "longest": max(lengths.values(), default=0),
            "sequences": len(lengths),
            "chunks_read": max(1, -(-len(sequence_ids) // chunk_size)),
            "would_have_shared_a_batch_with": isolated * (p.sequences_per_job - 1),
        },
        "warning" if isolated else "info",
    )
    return lengths


__all__ = [
    "load_lengths_for_isolation",
    "partition_sequence_ids",
    "refuse_and_record",
    "transformers_version",
    "refuse_over_ceiling",
]
