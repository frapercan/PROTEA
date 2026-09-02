# What the direct method has to get right

Before writing the predictor, an inventory of what is actually in the store and
a ranked list of what will quietly ruin the result if it is not handled. Ranked
by how likely each one is to produce a number that looks good and means
nothing, because that is the failure this project keeps having.

## What we have

Measured on 2026-08-30, not assumed.

| | count |
|---|---|
| sequences | 528,294 |
| sequence embeddings | 6,867,762 across 12 spaces, every space covering all 528,294 |
| proteins | 616,846, all reviewed, all with taxonomy and length |
| distinct organisms | 14,898 |
| GO annotations, all sets | 21,876,159 |
| GO terms in snapshot 36038118 | 40,214 non-obsolete (P 26,037, F 10,154, C 4,023) |
| GO relationships | 77,600, of which 69,188 is_a or part_of |
| sequence alignments | 4,441,922 |
| UniProt metadata rows | 575,503 |
| information accretion sets | 1 |

Inside the UniProt metadata, with real content rather than an empty field:
keywords 569,726, EC number 280,036, catalytic activity 260,924, binding site
225,241, features 575,503.

**`interpro_go_mapping` is empty.** The feature code in
`protea/core/_interpro_features.py` exists and has nothing to read. Domain
architecture is not currently available, and it is the most obvious source of
signal that does not derive from the same geometry the ceiling measurement
showed to be uninformative on the pairs we miss.

## The factors, worst first

### 1. The UniProt metadata is dated after the cutoff, and part of it is the answer

Every one of the 575,503 metadata rows was loaded on 2026-07-30. The bank is
dated 2024-04-10 and the truth 2025-09-03. The metadata therefore describes
proteins as they are known **now**, which is after both.

This is not a mild concern about one column. An EC number is a molecular
function statement: it and the MFO annotation are curated together, from the
same evidence, by the same people. 280,036 proteins have one.
`catalytic_activity` is the same statement in prose. `keywords` includes
GO-derived vocabulary.

Feeding any of these to a model that predicts 2025 annotations would produce
excellent MFO numbers that mean nothing. The options are to exclude them, or to
rebuild a t0-dated version from a UniProt release archive, which is real work
and has to be planned rather than assumed.

`taxonomy_id`, `length` and the sequence itself are safe: they are properties of
the protein, not of what anyone has concluded about it.

### 2. Absence of an annotation is not a negative

This is the assumption that decides whether the model learns biology or learns
frequency. A protein with no annotation for a term has usually not been tested
for it. Training on all unannotated pairs as negatives teaches the model the
marginal distribution of GO terms, which gives a good AUC and a poor Fmax, and
the two are easy to confuse.

What we actually have:

- **5,603 curated NOT annotations**, which are true negatives with evidence.
  They are the only ones we have. 956 are about a query protein and the run
  violated 478 of them, so they are also informative about exactly where the
  model will go wrong.
- **Structural negatives from the ontology.** If a protein has a parent term and
  one of its children, the sibling children are plausible negatives. This is
  where the ontology encoder earns its place, and it is a much larger set than
  5,603.
- **Everything else is unknown**, and should be weighted as unknown rather than
  labelled zero.

### 3. Cross-fitting has to be by homology, not by protein

The method is homology transfer. A random protein split leaves close homologues
on both sides of it, and then the model is evaluated on proteins it has
effectively seen. We have 4,441,922 alignments, so the clusters can be built
rather than guessed. Any result should be reported against maximum identity to
the training set, so a reader can see where it degrades.

### 4. The propagated label matrix is mostly shallow terms

The window adds 56,816 new (protein, term) pairs read exactly and 179,135 read
with propagation. Most of the difference is ancestors so general that predicting
them is not an achievement. An unweighted loss over the propagated matrix
teaches the model to predict `biological_process`.

Information accretion weighting exists and the surfaces already read
`f_micro_w` and `fmax_w`. The training objective should agree with the statistic
it will be judged by, and this project has already learned what happens when
four F statistics are in play and nobody says which one is meant.

### 5. Three aspects are three problems

P has 26,037 terms, F 10,154, C 4,023. The achieved recall for one arm ranges
0.223 to 0.606 across the nine cells. One loss over all three lets BPO dominate
by sheer count while CCO, the cell that works best, contributes almost nothing
to the gradient.

### 6. Two baselines, or the number cannot be read

The ontology encoder's own evaluation needed a degree baseline before its rank
of 19 meant anything. The same applies here, and more so:

- **The frequency prior.** Measured below at median rank 106 of 40,214 for a
  held-out term. It is stubbornly strong and it costs nothing.
- **The current method at its operating point**, which is what a new method has
  to replace, not merely differ from.

### 7. Scoring is the measured bottleneck, so calibration is part of the method

The corrected ceiling measurement says the union of twelve reaches 77.9 per cent
of new pairs while one arm recovers 0.22 to 0.61 at its operating point. The
loss between having the right candidate and scoring it above tau is larger than
the loss to retrieval. A model that ranks well and calibrates badly will
reproduce exactly that failure. Thresholds per aspect, and the threshold policy
recorded as part of the frame.

### 8. Every new parameter needs a guard that it governed the computation

The recurring defect: a parameter accepted, recorded where a reader will trust
it, and not governing the computation. It has now happened with the depth cut,
with the code revision, and with the denial filter's three writer paths. A
training run has more knobs than anything before it. Each one that reaches a
stored result needs an assertion that it was applied, not a field that says so.

### 9. Determinism, and a measured floor before any comparison

Evaluation reproduces exactly, 117 of 117 metrics. Retrieval does not. Training
introduces a third regime. Before any two configurations are compared, the same
configuration has to be trained twice and the spread measured, or every
difference smaller than that spread is noise being read as a result.

### 10. The compute is on the machine that reboots

The graphics card is on the stateless node and the state is here. Training has
to checkpoint to this machine and resume, and the declared-revision discipline
has to hold, or a node running different code will produce results labelled as
something they are not. That has happened once already.

## What the probe says about the target space

Part 2 rests on one idea: a protein that has terms T sits at the coordinate-wise
maximum of their order vectors, because that is the smallest point subsuming the
whole set. Scoring any term for any protein is then one penalty and needs no
per-term parameter, which is what would make it work for the 8,804 terms with no
carrier at all.

Tested directly, with no sequence involved. For 795 query proteins with at least
eight known terms, hold one out and rank it against the whole ontology:

| | median rank | h@10 | h@100 |
|---|---|---|---|
| join of the protein's other terms | **53** | 37.6% | 58.5% |
| term frequency | 106 | 27.2% | 49.4% |
| chance | 20,107 | 0.02% | 0.2% |

The geometry carries a protein: a held-out true term costs 762 times less than a
random one, and the join places it at rank 53 of 40,214.

Two things stop this being better news than it is. Against 500 sampled
candidates the same measurement reads rank 2, which is what a smaller candidate
pool buys and not what the method is worth; the full ontology is the honest
denominator. And frequency alone reaches rank 106, so the ontology's
contribution over counting is a factor of two, not an order of magnitude.

It also only exists where there is something to join. For the 2,413 NK proteins
there are no known terms, so this prior is absent exactly where the method has
the least to work with.

## What Part 2 therefore is

A map from a sequence embedding to a point in the frozen order space, trained so
that a true term's penalty against that point is small and a structural
negative's is large. The ontology encoder is the target and does not move, which
is what keeps a term with one example placed by the ontology rather than by its
single observation.

Judged against the frequency prior and against the current method, per aspect,
cross-fitted by homology, with the metadata of factor 1 excluded until it can be
dated.
