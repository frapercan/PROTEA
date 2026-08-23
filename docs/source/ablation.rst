How the Method Was Arrived At
==============================

.. contents:: On this page
   :local:
   :depth: 1

This chapter is the ablation: the measurements that decided each part of the
served method, and the ones that decided a part could be left alone. It reads
in the order the argument runs. The frame comes first, because a measurement on
a leaky frame decides nothing. Then the representation, which turns out to be
worth a great deal where it is the only evidence and nothing at all where it is
not. Then the same question asked of model capacity and network depth, which
answer the same way. What is left standing at the end is the ranking stage, and
that is where the remaining headroom is argued to be.

The single headline number lives in :doc:`results` and is not restated here.
The operational gotchas this material used to sit among are in
:doc:`insights`.

.. _reproducibility-section:

Reproducibility as a methodological contribution
-------------------------------------------------

**Context.**
Protein function prediction has a reproducibility problem that is rarely
acknowledged. Most CAFA-style methods publish a single Fmax number computed
against a benchmark dataset, but the accompanying code does not specify which
GO annotation release served as t0 for feature construction, which proteins
were in the query set, or whether the GO DAG propagation was applied before
or after the temporal split. Two groups running nominally the same method
against the same CAFA benchmark can produce different numbers for legitimate
reasons that are invisible in the paper.

**What the PROTEA campaign discovered empirically.**
During the F-EXP campaign, running the same trained booster against two
slightly different annotation snapshots (differing only in which GOA release
was used to resolve the training pairs) produced Fmax differences of 0.03 to
0.05 across NK cells. These differences are not noise: they are deterministic
given the snapshot pair, reproducible to four decimal places. But they would
look like unexplained variance to a researcher who had not pinned the snapshot
version in their run record.

The ``anc2vec_query`` artefact (see above) made this concrete. The
cross-category replication in the early training-table assembly turned
``anc2vec_query_known_count`` into a bucket identifier and inflated its
apparent importance. The corrected pipeline filters
``(protein, aspect)`` by category membership before replication, so each
feature carries only the information its definition implies and not
metadata about how its row was constructed.

**The contribution.**
PROTEA's experiment infrastructure was designed to make the full snapshot pair
list, the annotation source, the embedding config id, the schema_sha, and the
producer git sha part of every ``Dataset`` row. Every ``EvaluationResult`` row
is linked back through ``PredictionSet`` to the specific prediction run and
its parameters. The thesis (chapter 6) reports numbers that can be reproduced
from the commands in appendix B because the snapshot versions are part of the
record, not implicit.

This is not a novel insight in the machine learning literature, but it is
novel in the protein function prediction literature, where the dominant
publication format does not include this level of provenance. The PROTEA
campaign's methodological angle is that temporal holdout correctness is not
a detail: it is the thing being measured, and it requires explicit
infrastructure to get right.

**Prevention for future campaigns.**
Any evaluation that involves a temporal split must specify: (1) the annotation
release used as t0, (2) the annotation release used as t1, (3) whether GO
lineage propagation was applied before or after splitting, (4) which proteins
were in the query set, and (5) whether the same protein could appear in both
training and evaluation sets (a common source of leakage in methods that
aggregate across multiple snapshot pairs). The ``EvaluationSet`` row in PROTEA
captures (1), (2), (3), and (4). The training dataset manifest captures the
snapshot pair list. Cross-checking that the eval snapshot pair does not
overlap with any training pair is enforced by the ``export_research_dataset``
payload validator. See :doc:`/operate/reproduce-the-sealed-board` for the ordered
reproduction path.

The served last layer is a weak retrieval base, and standardisation is the lever
--------------------------------------------------------------------------------

PROTEA's retrieval encoder stores learned, GO-aligned codes rather than a raw
protein-language-model vector. A controlled ablation on the ankh-base substrate
motivates that choice, and its lesson is not the one a reader expects.

**The finding.**
Scored board-faithfully (cosine top-30 KNN GO transfer into the 15,000-protein
reference, ``f_micro_w`` over the nine cells), the learned k-WTA encoder
``d8979601`` reaches mean 0.21500, versus 0.14597 for the best fixed
representation (a standardised mid layer, L10, k-WTA) and 0.13356 for the served
last-layer dense baseline. That is plus 47.3 percent over the best fixed choice
and plus 61.0 percent over the served baseline, winning all nine cells, with the
largest gains on molecular function.

**Why the last layer is a poor base.**
Mean-pooled activations of ankh-base's final layer are compressed to a peak
absolute value near 0.6 by the model's closing LayerNorm, while the mid layers
reach magnitudes above 400,000. The final layer's flattened geometry is a weak
substrate for cosine retrieval, which is one reason the served last layer sat at
the bottom of the ranking.

**Standardisation and depth were varied together, and only the pair was
measured.**
Among fixed representations, per-dimension standardisation (z-score, statistics
fit on the reference pool only, non-transductive) appears as the dominant lever.
The arm that established it reads:

.. list-table:: The 2026-07-08 board-faithful confirm
   :header-rows: 1
   :widths: 46 18 18

   * - Arm
     - Score
     - Against the base
   * - ``L48-dense``, raw, L2 only
     - 0.1336
     - reference
   * - ``L10-dense-std``
     - 0.1447
     - +0.0111
   * - ``L10-kWTA128-std``
     - 0.1460
     - +0.0124

The winning arms are standardised and the baseline they beat is raw, so this
measures layer AND standardisation together against a control that has neither.
It cannot be read as a result about either one. The chapter previously read it
as a layer result, concluding that a shallower layer beats the served base;
that conclusion does not follow from this table, and no arm pairing a
standardised last layer against a raw one exists to attribute the gain.

**At matched treatment, in both directions, the shallower layer loses.** Two
measurements hold the transform fixed rather than varying it alongside the
depth, one with no standardisation on either side and one with standardisation
on both:

.. list-table:: Depth 10 against the last layer, treatment held fixed
   :header-rows: 1
   :widths: 40 30 30

   * - Treatment
     - Instrument
     - Depth 10 against last
   * - L2 only, both sides
     - retrieval probe, 85,982 donors
     - -0.042 to -0.045
   * - z-scored, both sides
     - lab, ``07_layers_standardised``
     - -0.0292

Two extractions, two metrics, the same direction and a comparable magnitude.
The only comparison in which the shallower layer wins is the one where it also
receives a transform its opponent does not.

That also rules out the obvious reconciliation, which was that the lab's local
extraction is a weaker base (see the caveat below) and might compress what
separates layers. It does not: run inside the lab, on the lab's own extraction,
the depth axis resolves cleanly at ``z-layer48`` 0.2545, ``z-layer19`` 0.2391,
``z-layer10`` 0.2253 and ``z-layer0`` 0.1599.

**Two further reasons not to lean on the July arm.** Its population is 7,401
LAFA queries of which 86 percent carry prior knowledge, against the
no-knowledge cell this project names as its frontier, and its reference side is
a 15,000-protein subset against the probe's 85,982. And the +0.0124 sits at the
selection floor that :ref:`insight-representation-matters-only-in-twilight`
prices at about 0.0094 for a search of this size, over a grid of six layers by
four sparsities by two normalisations.

**A claim withdrawn for want of a receipt.** This section previously stated that
the raw layer choice is statistically null. No arm supporting that has been
found. The lab's raw-layer arms are not null: layer 19 at -0.0208, layer 10 at
-0.0397 and layer 0 at -0.0680, all separating, and all recorded as carrying an
input-scale confound. The nearest thing to a null in the July record is a global
Spearman of 0.038 to 0.059 across layers, which that record's own text calls
low and which measures rank agreement on a different question. The sentence is
removed rather than rephrased.

What the rerun does support, on arms that vary one field, is that training-pool
size, the hard-negative objective and a learned multi-layer mixture are all null
in this harness.

**The caveat, and what resolved it.**
A controlled re-training of the encoder inside the offline lab harness, on a
local mean-pool of the ankh-base last layer, reaches only the fixed-representation
band (about 0.14 to 0.16). The resolution is the base embedding, not the training
procedure: the identical head recipe, trained on the production-stored embedding
for this backbone, reproduces the served encoder (mean 0.220 against 0.215). The
lab arms fell short only because their local extraction is a weaker base than the
production one. The precise extraction difference (pooling, normalisation, or which
tensor is read as the last layer) is the one detail still to pin down. These are all KNN-only retrieval numbers and are
distinct from the sealed 0.40765 reranked board in :doc:`results`; they explain
why the champion stores learned GO-aligned codes. See
:doc:`/adr/D35-canonical-8plm-embedding-configs` for the embedding config
registry and :doc:`/adr/D38-neural-head-deferred-dataset-pack-pivot` for the
neural-head decision this evidence informs.

.. _insight-representation-matters-only-in-twilight:

The representation earns its place at retrieval and loses it at scoring
-----------------------------------------------------------------------

The ablation above measures the encoding where the encoding is the only
evidence available, and finds it worth a great deal. Measured again at the
other end of the same pipeline, with the rest of the evidence switched on, the
same axis is worth nothing. Both numbers are correct, and the distance between
them is the most useful thing this project has measured about its own
representation.

**The grid.** Four encodings (pretrained ankh-base, a dense fitted map, a
sparse pooled map at 128 of 2048 atoms, and a sparse per-residue code) crossed
with six neighbourhood sizes, nine score weightings and three knowledge
regimes, banded by sequence identity, scored board-faithfully on the nine
category-by-aspect cells. 104 arms per cell.

**No arm beats any other.** The margin between first and second never exceeds
0.0015 in any of the nine cells, and in five of them the second place is the
same encoding under a different weighting. The per-cell winner this grid was
built to find cannot be determined, and that is the result rather than a gap in
it.

**What separates arms is the channel, not the encoding.** Holding the weighting
fixed, the four encodings spread 0.0540 under ``embedding_only`` at K=30 and
0.0025 under ``composite_no_embedding``. The second of those carries weight
exactly 0.0 on embedding similarity and wins 72.7 percent of cells.

The 0.0540 is almost entirely the instrument, and this can be shown rather
than asserted. It is measured under ``embedding_only``, the channel whose
scores :ref:`insight-capacity-is-read-through-one-channel` shows are collinear
with the reported protein count. Here that correlation is -0.809, and the shape
is specific: the un-encoded baseline reports the highest count and the worst
score.

.. list-table:: The four arms at K=30 under ``embedding_only``
   :header-rows: 1
   :widths: 34 22 22

   * - Arm
     - Score
     - Mean ``n_proteins``
   * - sparse pooled
     - 0.23656
     - 2,261
   * - sparse per-residue
     - 0.22580
     - 2,218
   * - dense fitted
     - 0.21353
     - 2,237
   * - pretrained ankh-base (un-encoded)
     - 0.18251
     - 2,350

Of the 0.0540, some 0.0313 is the baseline against the encoded arms across a
5.9 percent gap in that count, and 0.023 separates the three encoded arms
across a 1.9 percent one.

**Restricted to scorers whose arms report matching counts, the encoding axis at
K=30 is what it was at K=1.** Across four composite scorers whose arms agree to
within 1.4 percent, the four encodings spread 0.00194, 0.00227, 0.00248 and
0.00326. Two to three thousandths, against a headline of 0.0540 from the same
arms at the same budget. Whether the count reflects coverage or the operating
point, holding it fixed removes the effect.

**No sentence here should name a winning encoding.** Which one wins at K=30
flips with the scorer, four scorers to four: the dense fitted map wins under
one set and the sparse pooled map under the other. That is the same
self-inconsistency the backbone ordering shows under its winning weighting, and
it means the ordering inside these numbers is not reportable at any budget. The layer
axis reproduces the pattern independently: the last layer beats depth 38 in all
sixteen comparisons, by 0.0307 through ``embedding_only`` and 0.0026 through
the winner, an attenuation of about twelve.

**In the regime this project names as its frontier, the representation does not
participate at all.** In half the prior-knowledge cells the four encodings score
identically to six decimal places. That is a stronger statement than a small
difference, because it is falsifiable and it failed to be falsified: the four
encodings retrieve the same neighbours and transfer the same terms.

**The ties are smaller than chance would produce**, which is what makes the
absence of a winner an explanation rather than only an observation. A maximum
taken over many arms has a floor of roughly ``sigma * sqrt(2 * ln N)``; at the
study's measured spread that is about 0.0093 for the 104 arms in one cell and
0.0108 for the 528 in the grid. Every margin here is five to seven times below
it, including the 0.0021 by which ProtST leads ankh-base on the board. A
comparison decided before it ran is held to the resolution floor of 0.0013
instead, which is why a 0.0099 loss on a two-arm test counts and a 0.0021
margin over a search does not. The discriminator is the search budget, not the
size of the number.

**Depth and encoding are not the same lever seen twice.** Two instruments
sharing no code, one scoring ``f_micro_w`` on the task and one measuring
reachability on a retrieval bank, agree by identity band on what changing depth
costs:

.. list-table:: Depth 38 minus the last layer, by sequence identity band
   :header-rows: 1
   :widths: 20 25 25

   * - Identity band
     - Task, ``f_micro_w``
     - Retrieval, reachability
   * - <= 30 percent
     - +0.0068
     - +0.0010
   * - 30 to 60 percent
     - -0.0142
     - -0.0158
   * - 60 to 90 percent
     - -0.0129
     - -0.0099
   * - > 90 percent
     - -0.0010
     - -0.0162

Same sign in three of four bands and close in magnitude in the two middle ones.
Both instruments say depth is inert in the twilight zone and costs elsewhere,
which is the opposite shape to the encoding axis, whose effect was largest in
twilight and zero above ninety percent identity. Depth costs where the answer
was already easy; the encoding matters only where it was hard.

**How to read this against the section above.** The retrieval ablation reports
the learned encoder at 0.21500 against 0.13356 for the served last-layer dense
baseline, a 61.0 percent gain. That measurement gives the encoding the whole
job: cosine top-30 transfer, with no identity signal, no neighbour consensus and
no taxonomic prior in the score. The grid here gives it the job it actually has
in the served pipeline, alongside those other channels, and the winning
weighting reads it at zero. Neither number is wrong and neither supersedes the
other. The learned encoder earns its place by retrieving a better candidate set,
and it does not additionally earn one by scoring it, because by the time the
candidates are scored the evidence that orders them is coming from somewhere
else.

The practical consequence is that the retrieval axis is closed by measurement.
What limits the board is the ordering of candidates already retrieved, which is
the same conclusion the BP wall reaches from the other direction below.


.. _insight-capacity-is-read-through-one-channel:

An 8M-parameter backbone is within the noise of a 650M one, once the score stops asking
----------------------------------------------------------------------------------------

The section above measures one axis, the encoding, at two points in the
pipeline. The backbone axis, measured on a different grid in an earlier rung,
does the same thing, and putting the three axes side by side shows the property
is the pipeline's rather than any one axis's.

Eight pretrained protein language models were scored on the nine cells across
neighbourhood sizes and score weightings, on the GOA 226 to 227 frame. That is
not the sealed board's window and these are not board numbers; the comparison
between the two columns is the finding, not their level. Read through the
channel that asks the embedding for everything, the backbones separate. Read
through a weighting that also has identity and neighbour consensus available,
they do not separate at all. The second of those is the result; the first, as
set out below, is not safe to attribute to capacity.

.. list-table:: Best arm per backbone, mean ``f_micro_w`` over the nine cells
   :header-rows: 1
   :widths: 34 11 22 22

   * - Backbone
     - Params
     - ``embedding_only``
     - ``composite``
   * - ``facebook/esm2_t33_650M_UR50D``
     - 650M
     - 0.30826
     - **0.35728**
   * - ``Rostlab/prot_t5_xl_half_uniref50-enc``
     - 1.2B
     - **0.34194**
     - 0.35673
   * - ``ElnaggarLab/ankh-large``
     - 1.15B
     - 0.33509
     - 0.35647
   * - ``Rostlab/ProstT5``
     - 1.2B
     - 0.32803
     - 0.35632
   * - ``esmc_600m``
     - 600M
     - 0.32034
     - 0.35628
   * - ``facebook/esm2_t6_8M_UR50D``
     - 8M
     - 0.29610
     - 0.35617
   * - ``mila-intel/ProtST-esm1b``
     - 652M
     - 0.34039
     - 0.35608
   * - ``ElnaggarLab/ankh-base``
     - 450M
     - 0.32838
     - 0.35487
   * - **spread**
     -
     - **0.04584**
     - **0.00241**

**The 8-million-parameter model is the clearest case.** Against the best
composite arm it is eighty-one times smaller, and through ``composite`` that
costs 0.00111, which is below the study's 0.0013 resolution floor: on this
evidence the platform cannot tell the two apart.

.. warning::

   **The ``embedding_only`` column must not be read as a capacity effect.**
   Its scores are collinear with ``n_proteins``: the eight maxima report 6,193
   to 6,655 proteins summed over the nine cells, a 7.5 percent range, and the
   rank correlation between that count and the score is **-0.976**. Sorted by
   protein count, the column is very nearly sorted by score.

   What that collinearity means cannot be settled from these summaries, and
   the ambiguity is the point. ``n_proteins`` is the count of proteins
   carrying a prediction *at the threshold where the metric maximised*, so it
   moves with the operating point as well as with coverage. The platform
   records a case where a single 0.98 to 0.99 step in that threshold moved the
   count by 17 percent across 32 rung-1 runs whose scored cohort was provably
   identical (see the note in
   ``protea/core/operations/_run_cafa_artifacts.py``). So a -0.976 correlation
   is consistent with arms covering different populations and equally
   consistent with arms whose optima landed at different thresholds, and
   ``cafaeval`` reports per cell rather than per protein, so neither reading
   can be confirmed or excluded after the fact.

   Under either reading the column is not a clean measurement of what backbone
   choice buys. The 0.04584 spread is an upper bound on it, and any attenuation
   ratio built on that numerator inherits the ambiguity.

   The ``composite`` column does not have the problem. Its populations span
   2.4 percent, 6,116 to 6,264, and the population-to-score correlation is
   -0.143. That is the column the finding rests on.

**There is no ordering to report in the composite column**, and it fails to be
one in three independent ways.

*It does not reproduce against its own neighbour.* The rank correlation between
the two columns is 0.000. That is not a reversal but the signature of an absent
signal: the composite column has no order for the ``embedding_only`` order to
agree or disagree with.

*It does not reproduce across neighbourhood sizes.* Taking the four budgets as
near-independent slices and correlating the orderings they produce, the
``embedding_only`` side reproduces itself with a mean rank correlation of 0.85
and never falls below 0.69. The composite side manages 0.56, with three of its
six pairings below 0.42. An ordering that cannot agree with itself across
budgets cannot be a reversal of anything.

*It does not survive a change of average.* The nine cells are nine different
denominators, and the mean used here weights them equally. Recomputed as a
population-weighted mean, the winner of this very column changes, at spreads of
0.00241 and 0.00227 that both clear the resolution floor, so this is not the
floor excusing it.

Any claim that one of these backbones is the right one, made on this evidence,
would be a claim about which arm won a coin toss.

**The flatness is not an artefact of the operating point.** Every one of the
sixteen maxima above is at K=3, so the table is a balanced cut of two arms per
backbone rather than a maximum over unequal budgets. Repeating the comparison
at the other neighbourhood sizes keeps the result:

.. list-table:: Backbone spread under ``composite``, by neighbourhood size
   :header-rows: 1
   :widths: 12 26 26 36

   * - K
     - Spread
     - Population range
     - Note
   * - 3
     - 0.00241
     - 2.4 percent
     - the cut published above
   * - 5
     - 0.04579
     - 551 percent
     - unusable, see below
   * - 10
     - 0.00943
     - 2.9 percent
     - nothing removed
   * - 30
     - 0.04354
     - 2.9 percent
     - 0.00740 excluding one arm, see below

Two budgets need their exceptions named rather than quietly dropped. At K=5
one arm is scored on 954 proteins against a norm near 6,100, which is a
truncated evaluation and not a backbone result; the spread at that budget
measures the truncation. At K=30 the spread is one outlier: seven backbones
span 0.00740 and ``ankh-base`` sits alone 0.036 below the next worst, on a
normal population, and only under this scorer. That arm is a suspect data
point, not evidence that the backbone matters at K=30.

With those two named, the backbone axis is flat under the winning weighting at
every budget where the evaluation is sound.

**Which average, and what changing it moves.** The nine cells are not nine
views of one population. They span 1,003 to 6,901 scored proteins, a factor of
5.6, because a protein enters an aspect only where it gained a term of that
aspect. Every figure in this chapter is an unweighted mean over the nine, which
gives a 1,003-protein cell the same voice as a 6,901-protein one. That is a
choice, and it had never been stated as one.

The alternative was computed rather than argued about: 76 comparisons across
both grids, each recomputed as a mean weighted by the median ``n_proteins`` per
cell across the grid. The median is used rather than each arm's own count so
that every arm carries the same weight vector, which keeps the ambiguity
described above out of the comparison.

.. list-table:: What the population-weighted mean changes
   :header-rows: 1
   :widths: 44 28 28

   * - Quantity
     - Unweighted
     - Population-weighted
   * - Composite spread, median of 56
     - 0.00267
     - 0.00195
   * - Composite spread, maximum of 56
     - 0.04977
     - 0.02887
   * - Backbone spread through ``embedding_only``
     - 0.04584
     - 0.02206

**The flatness survives and gets flatter.** Of 56 composite comparisons, exactly
one widens under reweighting, by 0.00018, which is a seventh of the resolution
floor. So the claim that the winning weighting absorbs these axes is not an
artefact of the unweighted mean.

**The rankings do not survive.** The nominal winner changes in 15 of the 76
comparisons. Five of those sit below the floor, where reordering is expected and
means nothing. **Ten are at spreads that clear it**, and one of the ten is the
composite column tabulated above. That is the third of the three independent
reasons this chapter names no winning backbone, and it is the strongest, because
it does not appeal to a noise floor at all.

**And the upper bound is bounded under the average that maximises it.** Across
the twenty ``embedding_only`` comparisons the weighted spread is a median 0.633
of the unweighted one, near enough to two thirds throughout and closer to a half
at the larger budgets. So where this chapter calls 0.04584 an upper bound on
what backbone choice buys through that channel, the same quantity is 0.02206
under the other average. Both are quoted because quoting only the larger invites
exactly the question this paragraph answers.

The campaign keeps the unweighted mean, because it is the average the sealed
board and the CAFA cell structure are already expressed in and changing it would
make the two incomparable. What is now on record is that the choice is not
immaterial, that what it moves is the ranking rather than the flatness, and that
the ranking was already unreportable for two other reasons.

**The same shape appears on every representation axis measured**, and it is
the right-hand column that carries it.

.. list-table:: Spread under the winning weighting, against an upper bound on
                the spread through the embedding channel alone
   :header-rows: 1
   :widths: 28 16 22 22

   * - Axis
     - Rung
     - Winning weighting
     - ``embedding_only``
   * - Backbone (8 pretrained PLMs)
     - 1
     - **0.00241**
     - at most 0.04584
   * - Encoding (4 representations)
     - 2
     - **0.0025**
     - 0.0540
   * - Layer depth (4 depths)
     - 2
     - **0.0026**
     - 0.0307

Three axes measured in different rungs, on different grids and on different
frames, spanning an eighty-one-fold range of model capacity, four ways of
encoding a protein and three depths of the same network. Under the weighting
that wins, all three collapse to between 0.0024 and 0.0026, which is under
twice the study's resolution floor and far under the selection floor that
applies to a maximum taken over a grid. That is one property of the pipeline
observed three times, not three findings, and it is the reason this project's
remaining headroom is argued for at the ranking stage rather than the
representation stage.

The left-hand column is deliberately not converted into an attenuation ratio.
The backbone entry is an upper bound for the reason given above, and the three
rows sit on different frames and different populations, so a ratio computed
down that column would be arithmetic performed on numbers that are not
commensurable. What the three rows share is the right-hand column, where the
populations are tight and the axis is gone.

**What it does not say.** None of this shows that the representation is
unimportant in general, and none of it licenses picking the cheapest backbone
for a different pipeline. It says that in a pipeline whose winning score
weighting reads identity and neighbour consensus, those channels are already
carrying what the embedding would otherwise have supplied, and the embedding is
consulted for the part they cannot reach. The twilight-zone result in the
section above is that part, and it is where the representation still pays.


.. _insight-the-midpoint-was-measured:

The network's midpoint was measured because the plan said it would be
----------------------------------------------------------------------

The two ablations this campaign set out to reconcile disagreed about *mid*
layers. The arms first run did not contain a mid layer: the platform indexes
hidden states in reverse, so ``layer_indices [10]`` is depth 38 of 48, and the
comparison intended as mid-against-last was a near-top-against-last. The error
is recorded here rather than quietly corrected, because the arm had already
been dispatched at corpus scale before anyone noticed the convention.

**There is no mid peak.** Depth 24, the true midpoint and the fixed point of
the two conventions, loses to the last layer at every candidate budget:

.. list-table:: Reachability against the last layer, one pipeline, by budget
   :header-rows: 1
   :widths: 28 24 24 24

   * - Depth
     - Budget 10
     - Budget 25
     - Budget 50
   * - 10 of 48
     - -0.0423
     - -0.0444
     - -0.0446
   * - 24 of 48
     - -0.0076
     - -0.0118
     - -0.0112
   * - 48 of 48
     - reference
     - reference
     - reference

Every interval separates. Those three depths come from **one** pipeline, float32
end to end, pooled as the mean over residues and then L2 normalised, with the
same encoder, chunking, populations and metric, so the curve is one measurement
rather than three arms compared across instruments. Adding depth 38 from the
platform pipeline at -0.0053 to -0.0099 gives four depths reading -0.044,
-0.012, -0.008 and reference: monotone, with the loss shrinking about sixfold
from depth 10 to the last layer. A peak at the midpoint would have had to be
invisible to both its neighbours.

**Depth is not free where it matters.** By identity band at budget 25, against
the last layer:

.. list-table:: What going shallower costs, by sequence identity band
   :header-rows: 1
   :widths: 16 21 21 21 21

   * - Depth
     - Twilight
     - Distant
     - Close
     - Near-identical
   * - 10 of 48
     - -0.0703
     - -0.0665
     - -0.0131
     - -0.0049
   * - 24 of 48
     - -0.0164
     - -0.0158
     - -0.0058
     - -0.0058

The penalty is largest in twilight and vanishes among near-identical pairs, so
depth has teeth exactly in the band the served population sits in. That is the
same shape the encoding axis shows and the opposite of what a free parameter
would look like.

**What is not claimed.** Depth 38 read flat in twilight, +0.0010 on an interval
of [-0.013, +0.015], against depth 24's -0.0164 on [-0.032, -0.000]. Those
intervals overlap heavily and the two arms come from different pipelines, so no
change of band profile between depths 24 and 38 is asserted here. What is solid
is the aggregate ordering across all four depths and the
twilight-against-near-identical contrast within each one.

**On what it cost to answer a question rather than assume it.** The midpoint
took 226 minutes of one card against a 228-minute estimate. The saving was not
the hardware but declining to write: only the pooled vector was wanted, so
residues were averaged as they came off the card and never stored, 274 MB
instead of 129 GB. The same depth computed through the serving platform would
have been a full-corpus job of roughly seventeen hours to answer a question that
needed 89,013 proteins. The layer field of the frozen recipe is therefore
measured at four depths rather than inherited, and it stays at the last layer.


.. _insight-bp-wall-is-a-ranking-limit:

The BP wall is a ranking limit, not an evidence ceiling
-------------------------------------------------------

The sealed board (:doc:`results`) is first in seven of nine cells. The two it does
not win are LK-BPO and PK-BPO, the Biological Process branch for the proteins with
limited or no prior knowledge. This section locates that limit. Every figure below is
measured on one harness, against the full ground truth, on the PK-BPO cell.

**The evidence is present.**
97.0 percent of the true protein to Biological Process term pairs the pipeline misses
use a term that some protein in the same cohort already carried before the target
window opened. Weighted by information accretion, which is what the metric scores,
the figure is 95.2 percent. Only 3 percent of the pairs, and 4.8 percent of the
weight, are genuinely novel. Nothing is missing from the vocabulary.

**Retrieval is not the binding constraint.**
Candidate recall is 0.322. Submitting every pool cell that belongs to the propagated
ground truth, which is what a perfect ordering of the candidates already retrieved
would be worth, yields ``f_micro_w`` **0.7519** at precision 1.000, verified through
the evaluator itself. Allowing that ordering to also keep the pool cells whose own
ancestors are true, which a real ranker may do and an oracle has no reason to refuse,
reaches **0.7764**, and the ceiling of the pool lies in [0.7764, 0.8326]. The deployed
re-ranker delivers **0.2131** on that same pool: **27.4 percent** of what its own
shortlist allows.

**So adding candidates does not pay.**
A co-occurrence expansion lifting recall from 0.322 to 0.480 moves the score by a
small fraction of the gap. More candidates do not help a ranker that cannot order the
ones it already holds.

**Nor is it where the list is cut.**
A global threshold cannot express a per-protein term count: every protein is cut at
the same ``tau`` whether it deserves three terms or thirty. Freezing the pipeline's
own ordering and granting each protein its true count, an oracle no method could
have, moves ``f_micro_w`` from 0.2017 to 0.2379. That is plus 0.036 of a 0.406 gap,
**about a tenth**. The other nine tenths is ordering.

**It is ordering, and no feature carries it.**
On PK-BPO no feature the pipeline carries exceeds AUC 0.68 (``classifier_present``;
then ``protst_text`` at 0.64 on 41 percent coverage, ``classifier_score`` 0.63,
alignment near 0.60) against a 2.47 percent positive rate.

**The deployed recipe is the best technique we have.**
Every variation tested scores below it:

.. list-table:: PK-BPO, one harness, full ground truth
   :header-rows: 1
   :widths: 60 20 20

   * - Recipe
     - ``f_micro_w``
     - vs deployed
   * - **deployed: per-category ``lambdarank``, aspects pooled**
     - **0.2131**
     - reference
   * - trained per cell instead of pooling aspects
     - 0.2017
     - minus 0.011
   * - ``binary`` objective instead of ``lambdarank``
     - 0.1518
     - minus 0.061
   * - plus within-protein rank and z-score features
     - 0.1465
     - minus 0.067
   * - plus class weighting
     - 0.1441
     - minus 0.069
   * - classifier-proposed candidates dropped from the pool
     - flat
     - coverage 0.978 to 0.846

The ``binary`` result is worth stating twice, because it is counterintuitive: it
carries a **better** AUC than the deployed recipe (0.8227 against 0.7903) and a
**worse** ``f_micro_w``. AUC ranks these recipes in the opposite order to the metric
that decides. Do not triage ranking levers by AUC.

**Ruled out by measurement.**
GO-DAG hierarchical proximity as a feature (AUC 0.5501, decorrelated from the
re-ranker yet adding plus 0.0002 when blended); a text-aligned scorer as a re-ranker
feature (plus 0.0016); an InterPro graft (negative on BP); and a larger base
representation, which reorders the same candidates and so cannot help where recall is
not the constraint.

**Where that leaves the two cells.**
The gap to the leading external method is plus 0.072 (LK-BPO) and plus 0.076
(PK-BPO). The ranking headroom inside the pool already retrieved is several times
that, so the work is a ranker and not a retriever. The technique levers available are
exhausted: the deployed recipe sits at their optimum. The signal that would close the
gap is not identified, and the two structural candidates testable with these
resources, term co-occurrence and ontology proximity, are both dead. "Improve the
ranker" is a direction, not yet a plan.

**Method note.**
Two rules this cell earned, both cheap to apply and both load-bearing here. A recall
number does not identify what binds a pipeline; the ceiling of the pool does, and
obtaining it costs one evaluation with the labels used as the score. And a monotone
rescaling of a score is **not** free under a threshold-swept metric: ``f_micro_w``
sweeps ``tau`` on a fixed grid, so remapping the score distribution changes which
cuts the sweep can reach. Scoring one booster with its raw output and with a global
rank-percentile of that output differs by 0.088 on this cell. Any transform applied
before evaluation is part of the measurement.

.. note::
   These figures come from a retrained booster on an exported dataset rather than the
   sealed board, and they track it closely: the deployed recipe measures 0.2131 here
   against the board's 0.2181 for PK-BPO. Every figure shares one ground truth and one
   harness.
