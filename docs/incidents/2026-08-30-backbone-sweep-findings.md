# What the twelve arm sweep measured

Twelve embedding configurations retrieved at k=30 over the same 14,032 queries,
one annotation set, one donor policy, one code revision, on 2026-08-30. Every
number below was measured on this machine on that day. Where a number was
retracted or corrected during the day, the correction is what appears here and
the retraction is named.

## 1. The arms are indistinguishable in aggregate

Nine strata, three knowledge categories by three aspects, twelve arms each.
Winner against runner up, paired per protein, with two standard errors of the
paired difference:

| stratum | winner | margin |
|---------|--------|--------|
| NK.BPO | rung2-dense 0.1541 | +0.0041 ± 0.0064 |
| NK.MFO | esm2_3B 0.3279 | +0.0022 ± 0.0156 |
| NK.CCO | protst 0.3042 | +0.0118 ± 0.0122 |
| LK.BPO | esm2_650m 0.1768 | +0.0004 ± 0.0122 |
| LK.MFO | esm2_650m 0.3547 | +0.0003 ± 0.0176 |
| LK.CCO | esmc_600m 0.3466 | +0.0028 ± 0.0180 |
| PK.BPO | protst 0.0675 | +0.0017 ± 0.0025 |
| PK.MFO | protst 0.1702 | +0.0025 ± 0.0067 |
| PK.CCO | protst 0.1701 | +0.0002 ± 0.0078 |

**Zero of nine strata have a resolved winner.** Models from 8M to 3B
parameters, across ESM, Ankh, T5 and ProtST lineages, sit inside each other's
noise. These are orderings, not winners.

## 2. And they are strongly complementary per protein

An oracle that picks the better of two arms per protein gains, over the better
arm alone, between +0.0157 and +0.0650 depending on the panel. Over twelve arms
it gains +0.0439 to +0.1842, which is **10.2 to 24.4 times the detectable floor
in every one of the nine panels**.

The null for this statistic is **exactly 0.0000**, measured by running one
recipe twice on identical code: the two per-protein tables are byte identical.
So none of the oracle gain is retrieval noise.

Saturation is slow. Two arms reach 33 to 44 per cent of the twelve-way ceiling,
three reach 54 to 68, five reach 76 to 84. Routing is a lever, not a cheap one.

**A router keyed on stratum alone is worth +0.0039 and nothing at all in four of
the nine panels**, which is below the floor. The headroom is inside the strata,
not between them, so routing needs per-protein features rather than a lookup.

## 3. The retriever's recall ceiling is the binding constraint

Of the 56,895 true new annotations the 14,032 queries gain between the bank
(cbb35a32, to 2024-04-10) and the truth (ec9f5c2c, to 2025-09-03):

| arm | ceiling | | arm | ceiling |
|---|---|---|---|---|
| protst | 40.9% | | esm2_3B | 38.4% |
| ankh_large | 39.9% | | prostt5 | 38.2% |
| ankh_base | 39.6% | | esm2_650m | 37.6% |
| ankh-base L10 | 39.5% | | esm2_8m | 36.5% |
| rung2-pooled | 40.5% | | esmc_600m | 35.3% |

A 5.6 point spread across the whole zoo, and **the union of twelve reaches
55.8 per cent**. Fifteen points of truth are reachable by some model and not by
the best one. No reranking recovers them: only proposing more candidates does.

The extra truth is not in the deep tail. Median k_position 13, p90 27, against
median 2 for the truth the best arm already reaches. But it is nearly private:
**a median of 2 arms of 12 see it, against 12 of 12 for the easy truth.**

## 4. Complementarity does not follow quality

Greedy forward selection on recall, excluding protst (its lead is unresolved
text leakage, and dropping it costs 444 pairs, 0.8 per cent of the ceiling):

| # | arm | adds | cumulative |
|---|-----|------|------------|
| 1 | rung2-pooled | +23,051 | 40.5% |
| 2 | esmc_600m | +3,004 | 45.8% |
| 3 | esm2_t36_3B | +1,546 | 48.5% |
| 4 | esm2_8m | +1,056 | 50.4% |

**Four arms reach 91.5 per cent of the eleven-arm ceiling.** The second pick,
esmc_600m, has the LOWEST individual ceiling of the twelve at 35.3 per cent.
The 8M model contributes more new truth than the 650M model. The two ankh arms,
with high individual ceilings, contribute nothing new in the first seven picks.

Nobody would have chosen this set by reasoning. It comes only from measuring
marginal contribution instead of mean quality.

## 5. A reranker over those four beats the served ordering

6,036,763 candidates, 28,657 positive (0.47 per cent), five folds cross-fitted
**by protein** so no protein is scored by a model that saw it.

| | AUC | AP |
|---|---|---|
| reranker | 0.8733 | 0.0523 |
| distance ordering | 0.7372 | 0.0219 |

Recall when each protein keeps its top k:

| k | distance | reranker |
|---|---|---|
| 1 | 2.6% | 5.2% |
| 3 | 7.4% | 12.1% |
| 5 | 11.6% | 17.9% |
| 10 | 20.7% | 29.7% |
| 30 | 41.0% | 53.4% |

The reranker reaches at k=10 what the distance ordering needs k=30 for. Since
depth is monotone and shallower is better, ordering better and cutting harder
push the same way.

**Dropping n_arms changes nothing** (AUC 0.8734 against 0.8733), even though
that column alone separates 0.14 per cent positives from 2.96 per cent. The
count is redundant with the other columns, which are themselves cross-arm
aggregates. So the two models are not two systems: both require serving all
four arms. A genuinely single-arm reranker needs single-arm features and has
not been built.

## 6. Two facts about the instruments themselves

**The retriever is nondeterministic and the evaluator is not.** One recipe run
twice gave 2,508,265 rows against 2,508,266, and 738 of 14,032 queries received
different donors. Not one protein's score moved: the swapped donors are
sequence twins at distances within 1e-7 carrying the same annotations. 94.4 per
cent of the differing pairs have a counterpart within 1e-6, median gap 1.2e-7,
which is float non-associativity rather than anything larger.

**PROTEA's reading surfaces report a different statistic than CAFA scores.**
Each result stores four: `fmax` and `fmax_w` are per-protein averages, which is
cafaeval's headline, and `f_micro` and `f_micro_w` come from the pooled
confusion matrix. All three graph surfaces select `f_micro_w`, and one has
`WHERE asp.v ? 'f_micro_w'` so a result without that key does not appear at
all. On one arm pair the same contrast is +0.0708 in `fmax_w` and +0.0255 in
`f_micro_w`. Whether that choice was deliberate is not recorded anywhere found.

## What this says about the campaign

The three measurements are one fact seen three times: **choosing a
representation does not change how much you know, it changes what you know.**
Aggregate quality is flat, per-protein headroom is large, and reachable truth
differs by arm. A campaign that picks one backbone and proceeds cannot see any
of it.

The binding constraint is the candidate generator at 55 per cent, not the
ordering. The reranker is worth building, over a union rather than an arm, and
its ceiling is that 55 per cent.
