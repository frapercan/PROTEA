# Where the retrieval ceiling comes from

Nearest-neighbour retrieval over learned sequence embeddings reaches 55.8 per
cent of the annotations these 14,032 proteins gain between the bank and the
truth. This is a measurement of why, on the same window, the same bank and one
donor policy, on 2026-08-30.

## The decomposition

Of 56,895 true new (protein, term) pairs:

| | pairs | share |
|---|---|---|
| reached by the union of twelve arms | 31,751 | 55.8% |
| term does not exist in the bank at all | 3,215 | 5.7% |
| a retrieved donor carried it and it did not transfer | 979 | 1.7% |
| the carrying protein was never retrieved | 20,950 | 36.8% |

Only 5.7 per cent is beyond any annotation-transfer method: for everything
else the answer is present in the bank. And the aggregation is not the
problem: when a donor carrying the term is retrieved, the term transfers 95.5
per cent of the time.

**The whole gap is retrieval.**

## Depth does not close it

For the 36.8 per cent, the nearest bank protein carrying the term sits at

| percentile | rank in a pool of 86,068 |
|---|---|
| p10 | 173 |
| p50 | 1,484 |
| p75 | 5,185 |
| p90 | 21,793 |

Raising k from 30 to 100 recovers 4.5 per cent of them; k=1000 recovers 41.8
per cent. Depth is measured to be monotone on this campaign, deeper being worse
in all five cells of the depth series, so the material that is missing sits
exactly where going to get it costs more than it returns.

## And the space has almost no signal about those pairs

Rank alone would be a weak argument, because the missed terms are rarer: median
34 carriers in the bank against 151 for the reached ones, and a rarer term's
nearest carrier is further away by construction. Normalising for that is what
makes the measurement mean something. With c carriers in a pool of N, chance
puts the nearest at about N/(c+1), and the ratio of that to the observed rank
is how much the embedding beat chance.

All 53,680 pairs whose term exists in the bank, computed pair by pair:

| | pairs | carriers | rank p50 | chance p50 | better than chance | worse than chance |
|---|---|---|---|---|---|---|
| reached | 31,751 | 151 | 13 | 566 | **18.49x** | 11% |
| missed | 21,929 | 34 | 1,398 | 2,459 | **1.67x** | 35% |

An eleven-fold difference in signal, and on more than a third of the missed
pairs the embedding orders the carrier WORSE than chance would.

## It is not a property of one encoder

The same statistic on a 400 pair sample of the missed set, in five spaces
spanning a 375-fold parameter range and four pretraining lineages:

| arm | rank p50 | better than chance | worse than chance |
|---|---|---|---|
| esm2_650m | 1,484 | 1.64x | 35% |
| rung2-pooled | 1,410 | 1.82x | 34% |
| esmc_600m | 1,419 | 1.93x | 35% |
| esm2_t36_3B | 1,160 | 1.96x | 30% |
| esm2_8m | 1,256 | 1.92x | 32% |

The spread is 1.64 to 1.96. Against 18.49 on the reached side, that is not a
gradient between better and worse encoders, it is a step between pairs the
representation knows about and pairs it does not.

## What it explains

Four measurements from the same day stop being four facts:

- **Zero of nine strata have a resolved winner** among twelve arms. They all
  capture the same signal, and none has signal where there is none.
- **Depth is monotone and shallower is better.** Going deeper adds candidates
  ordered near-randomly with respect to the truth.
- **The union of twelve reaches 15 points more than the best single arm, and
  the extra truth is seen by a median of 2 arms of 12.** Twelve near-random
  orderings hit by luck in different places. That is a weaker and better
  supported reading than twelve models knowing different things.
- **The ceiling is 55.8 per cent** and it is the reach of learned sequence
  similarity, not of the method built on top of it.

## What it does not say

The window is one bank (cbb35a32, to 2024-04-10) against one truth (ec9f5c2c,
to 2025-09-03), one donor policy of thirteen experimental evidence codes, one
query set of 14,032 proteins, one pool of 86,068. The five-space comparison is
a 400 pair sample; the two-way table is the full 53,680.

It says nothing about whether a different retrieval signal reaches those pairs.
Profiles, domain architecture, InterPro membership and term co-occurrence are
untested here, and they are the obvious candidates precisely because they do
not derive from the same sequence-similarity geometry that this measurement
shows to be uninformative on the missing 36.8 per cent.
