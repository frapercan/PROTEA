# Where the retrieval ceiling comes from

> **Corrected 2026-08-30.** The first version of this document put the ceiling
> at 55.8 per cent. That number matched a predicted term against a true term as
> strings. The evaluation does not do that: it propagates predictions up the
> ontology before scoring, so predicting a descendant of a true term reaches
> that term. Measured the way the scorer measures, the ceiling is **77.9 per
> cent**. Every number below has been recomputed. The section "what changed and
> what survived" states which conclusions of the first version stand.

This is a measurement of what nearest-neighbour retrieval over learned sequence
embeddings can and cannot reach, on one window, one bank and one donor policy,
on 2026-08-30.

## Where the mistake was

A GO annotation carries its ancestors. A protein annotated with
`GO:0004439` is thereby annotated with every ancestor of `GO:0004439`, and
`cafaeval` closes both the predictions and the ground truth before it counts
anything. Matching the two as raw strings therefore scores a prediction wrong
for being *more specific* than the curated statement, which is the direction a
prediction is supposed to err in.

The scale of it, on the same union of twelve arms and the same window:

| | pairs |
|---|---|
| new pairs, exact | 56,816 |
| new pairs, propagated | 179,135 |
| union predicted, exact | 9,420,452 |
| union predicted, propagated | 26,246,268 |

and the reach, in the three pairings that have a reading:

| predictions | truth | reached | of |
|---|---|---|---|
| exact | exact | 31,694 | 56,816 (55.8%) |
| **propagated** | **exact** | **44,273** | **56,816 (77.9%)** |
| propagated | propagated | 151,231 | 179,135 (84.4%) |

The first row reproduces the superseded number exactly, which is how the
reconstruction was verified. The second is the honest one and the one used
below: it asks whether the run reached the curated statement, without giving
credit for reaching an ancestor so general it was never in doubt. The third is
the space the scorer literally works in and is quoted for completeness.

## The corrected decomposition

Of 56,816 true new (protein, term) pairs, with predictions propagated:

| | pairs | share |
|---|---|---|
| reached by the union of twelve arms | 44,273 | 77.9% |
| missed, term absent from the propagated bank | 2,376 | 4.2% |
| missed, term present in the bank | 10,167 | 17.9% |

4.2 per cent is beyond any annotation-transfer method: nothing in the bank
carries the term or anything below it. For the other 95.8 per cent the answer
is present, and the union finds 81 per cent of that.

## Depth does not close what is left

For the missed pairs the nearest bank protein carrying the term sits deep, and
that has not changed: median rank 1,484 in a pool of 86,068, p75 at 5,185, p90
at 21,793. Raising k from 30 to 100 recovers about 4.5 per cent of them. Depth
is measured monotone on this campaign, deeper being worse in all five cells of
the depth series, so what is missing sits where going to get it costs more than
it returns.

## The space has much less signal about the missed pairs

Rank alone would be a weak argument, because missed terms are rarer and a rarer
term's nearest carrier is further away by construction. With c carriers in a
pool of N, chance puts the nearest at about N/(c+1); the ratio of that to the
observed rank is how much the embedding beat chance.

Recomputed on the corrected split, pair by pair:

| | pairs | carriers p50 | rank p50 | chance p50 | better than chance | worse than chance |
|---|---|---|---|---|---|---|
| reached | 43,174 | 88 | 42 | 967 | **7.10x** | 18% |
| missed | 10,431 | 21 | 1,647 | 3,912 | **1.78x** | 34% |

A four-fold difference in signal, and on a third of the missed pairs the
embedding orders the carrier worse than chance would. The first version put
this at eleven-fold (18.49x against 1.67x). The reached side fell because the
corrected reached set now includes pairs reached only through a descendant,
which are harder. The missed side barely moved.

## It is not a property of one encoder

> This table was computed on the superseded split and **has not been
> recomputed**. It is kept because the overall missed-side statistic moved only
> from 1.67x to 1.78x between the two splits, so the spread is unlikely to have
> changed materially. It should be read as indicative and not quoted as a
> measurement on the current split.

The same statistic on a 400 pair sample of the (old) missed set, in five spaces
spanning a 375-fold parameter range and four pretraining lineages:

| arm | rank p50 | better than chance | worse than chance |
|---|---|---|---|
| esm2_650m | 1,484 | 1.64x | 35% |
| rung2-pooled | 1,410 | 1.82x | 34% |
| esmc_600m | 1,419 | 1.93x | 35% |
| esm2_t36_3B | 1,160 | 1.96x | 30% |
| esm2_8m | 1,256 | 1.92x | 32% |

The spread is 1.64 to 1.96 against 7.10 on the reached side: a step between
pairs the representation knows about and pairs it does not, rather than a
gradient between better and worse encoders.

## What changed and what survived

**Survives.** Depth is monotone and shallower is better. Zero of nine strata
have a resolved winner among twelve arms. The embedding has several times less
signal about the pairs it misses, and orders a third of them worse than chance.
Encoder choice is a step and not a gradient. The 4.2 per cent that no transfer
method can reach is real, and was slightly overstated before at 5.7.

**Does not survive.** "The whole gap is retrieval." The gap attributable to
retrieval is 17.9 points, not 36.8, and the reachable-but-missed material is
less than half what the first version claimed.

**Is new, and is the reason this correction matters.** Reach is no longer the
binding constraint, and something else now is. The three quantities, each
measured on the same window and each meaning something different:

| | |
|---|---|
| union of twelve, propagated, reaches | **77.9%** of new pairs |
| one arm (esm2_650m), propagated, reaches | **64.3%** of new pairs |
| that same arm's recall at its operating point | **0.22 to 0.61** across nine cells |

The first two are recall ceilings with no threshold and no precision cost. The
third is what survives scoring. Between "the right candidate is in the list"
and "the right candidate is scored above tau" the method loses more than it
loses to retrieval, and its precision at that point is 0.087 to 0.496.

The first version read the ceiling as a property of sequence-similarity
geometry and concluded that a different retrieval signal was the way forward.
That conclusion is not wrong, but it was aimed at the smaller of the two
losses.

## What it does not say

The window is one bank (`cbb35a32`, to 2024-04-10) against one truth
(`ec9f5c2c`, to 2025-09-03), one donor policy of thirteen experimental evidence
codes, one query set of 14,032 proteins, one pool of 86,068. NOT-qualified
annotations are excluded from both sides, which is why the new-pair count is
56,816 here against 56,895 in the first version.

Propagation is under `is_a` and `part_of` only, to depth 20, which is the
closure the ontology's own annotation semantics define. Other relations were
not traversed and a different choice would move these numbers.

The measurement is reproducible from `scripts/analysis/ceiling.sql`, which
builds every table above from the database and prints all of them, including
the superseded cell.
