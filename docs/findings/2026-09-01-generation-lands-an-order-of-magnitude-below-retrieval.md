# Generation lands an order of magnitude below retrieval, twice

A negative result, recorded because it is a result. Between 2026-08-30 and
2026-09-01 four architectures were built to emit a GO annotation set directly
from a sequence, without retrieving candidates and reordering them. Three were
measured end to end and all three lost. The fourth is the reference they were
measured against.

The code is on `feat/subsumption-is-containment-in-a-sparse-code`, tip
`222b4a3`. That branch is abandoned and is deliberately **not deleted**: this
note is the reason to keep it, and deleting the ref would make its objects
unreachable. Two pieces were salvaged into `develop` by PROTEA#919 and are
described at the end.

## The headline

Two independent attempts at one-shot generation over the full vocabulary land
an order of magnitude below retrieve-and-rerank.

| attempt | `f_micro_w` | against |
|---|---|---|
| `entail_kwta` (earlier lab work, EL balls over k-WTA) | 0.011 | anchor 0.144 |
| sparse containment (this work) | 0.016 | deployed 0.196 |

Different paths, the same distance. `entail_kwta` had already diagnosed this as
a candidate-generation ceiling; it is now replicated by an architecture that
shares none of its machinery. The candidate-generation step does real work that
generation does not replace.

## Arm by arm

Everything below is measured, not argued. All windows are curated additions
between releases, evaluated on a window the model never saw.

### The order encoder, free parameter table. This one works

The reference, and the only arm that is not a loss. No protein involved,
5 per cent of edges held out, GO snapshot 36038118.

| | trained | untrained |
|---|---|---|
| subsumption, hard negatives (reversed pairs + siblings) | 98.89% | 50.22% |
| held-out parent, median rank among non-ancestors | 19 of 40,214 | 19,755 |

A degree baseline, ranking by descendant count, gives median rank 2,112.

### The graph encoder over the ontology's own text. Loses, with an inversion

95.36 per cent on hard negatives against the free table's 98.89. But stratified
by Resnik similarity there is an inversion that the aggregate hides:

| regime | free table | graph | lexical containment |
|---|---|---|---|
| low Resnik, distant terms | rank 7 | rank 15 | rank 321 |
| high Resnik, close terms | rank 56 | rank 52 | **rank 7** |

**Among semantically close terms, string matching beats both learned encoders
by a wide margin**, and that is precisely the regime that matters: the hard
decision in a descent is choosing between siblings, not telling a term from an
unrelated one. Both learned encoders degrade from a global rank of 14 to 19
down to 52 to 56 exactly there.

This is the most useful thing in the note that is not a failure. It says the
learned geometry is being asked for the one thing it is worst at.

### The descent generator, CCO. Loses

Run 2, 60,000 proteins, frequency as a feature, prior tuned on validation.
Test 227 to 230, 903 proteins, 3,736 true additions.

| | `f_micro_w` | P | R | emitted |
|---|---|---|---|---|
| generative descent | 0.0987 | 0.0636 | 0.2202 | 9,734 |
| frequency prior, top 1 | **0.1640** | 0.1301 | 0.2219 | 10,836 |

**Recall identical, precision half, volume comparable.** The same hits, twice
the punishment. Under IA-weighted micro F, being wrong on a deep term costs a
lot and on a general one costs little. The prior stays high in the ontology and
is cheap; the descent is specific and pays for it.

The structural guarantee held: zero unreachable additions across all three
transitions, by induction, because every term in the target has its parents in
the target.

### Sparse containment. Three runs, three losses

Full corpus: 1,889,171 pairs, three aspects, 40,214 terms, 40,000 proteins.
Test 227 to 230, 1,023 proteins, 10,905 additions.

| run | `f_micro_w` | P | R | emitted |
|---|---|---|---|---|
| uniform negatives | 0.0160 | 0.0082 | 0.2969 | 378,818 |
| hard negatives | 0.0082 | 0.0042 | 0.1989 | 395,185 |
| binary k-WTA codes | 0.0004 | 0.0002 | 0.0185 | 407,942 |
| frequency prior, top 50 | **0.0852** | 0.0742 | 0.1000 | 33,210 |
| deployed method | **0.1955 to 0.2085** | | | |

395,185 emitted over 1,023 proteins is 386 terms per protein.

## Why the sparse arm dies, which is not the same as why it failed

Three defects were found in it. Two are ordinary and are fixed. The third is
the reason the architecture does not work.

**Defect 1, a parameter that did not govern.** `SparseCodeConfig` declared
`own_k` and the model never used it, so the codes were dense vectors of small
positive numbers. A near-zero code is contained in *any* protein, and zero is a
fixed point satisfying every positive example, so nothing in the loss pushed
the codes off it. This is the fourth time in this project that a parameter has
been accepted, recorded where a reader would trust it, and not governed the
computation.

**Defect 2, tests that asserted at initialisation.** Every assertion ran on an
untrained module. The collapse happens during training, so the suite was green
throughout.

**Defect 3, silent saturation.** Found by the asymmetry test rather than by
reading the code: with too few atoms the deepest codes fill the space, a parent
and its child both hold every atom, and containment becomes *symmetric*, which
is the one property the construction exists to provide. A symmetric containment
passes every other assertion in the file.

**And then, with the codes correctly binary and sparse, the loss did not
fall.** 6.1972 to 0.6186 and flat for eight epochs. The negative term is zero,
so negatives separate cleanly; the positives cannot be satisfied. With random
atom sets the codes overlap arbitrarily and there is no assignment that
satisfies a protein's terms without also satisfying dozens it does not have.
The codes would have to be *learned* to be discriminative, and the gradient
does not reach them: a straight-through estimator behind twenty-four chained
max-propagations.

That is circular, not a knob. Fixing the three defects makes the construction
correct and does not make the method work.

## What this does not establish

It does not show that generation cannot work. It shows that two architectures
built on a full flat-or-hierarchical vocabulary, trained on this corpus, land
an order of magnitude below the deployed pipeline, and it gives a mechanism for
one of them.

The descent arm's structural properties are established and its score is one
run on one aspect. Nothing here was cross-fitted or bootstrapped; these are
single measurements against a prior, taken to decide whether to continue, and
they are not the kind of number that belongs in a results table.

## Where the evidence points instead

The reranker sits at AUC 0.491 in PK-BPO against 0.6263 for a clean entailment
signal. It is the component that decides what crosses tau, and tau is where the
loss happens. That has the strongest evidence of anything measured here, and
nothing has attacked it directly.

## What was salvaged

PROTEA#919 brought two files onto `develop`:
`protea/core/ontology/sparse_containment.py` and
`tests/test_subsumption_is_containment.py`. Not for the method, for the two
guards, both of which are the same defect class: a check that cannot detect the
thing it is named after.

`SparseTermCodes` now refuses a saturating configuration at construction rather
than degrading quietly. GO's deepest term has 68 ancestors, so at `own_k=4` it
demands 276 atoms, and the 512 the analysis script was about to use would have
been rejected.

`test_it_survives_a_gradient_step` takes forty Adam steps on `codes().sum()`,
which pushes every atom down, which is the collapse, and asserts the code
survives. Measured both ways: the tests that existed are 8 green against the
broken module, and the tests added are 4 red against it.

The remaining 23 files of the branch were not brought over. They belong to the
method that failed.
