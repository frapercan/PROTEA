The research line
=================

Everything this project has explored sits in one line of research, built by
stages. Each component is defined, run and validated in the platform at every
cut, documented, and the line ends in a combination of the best components,
routed stratum by stratum during the stratified evaluation.

At its centre is one question: **two ways of learning to read the same
evidence.** A *world model*, which learns how a protein's knowledge changes
over time and predicts it whole, against *decision trees*, which score each
candidate term from signals written by hand. Both read the same inputs, are
trained the same way at each cut and are scored on the same candidates.

The stages at a glance
----------------------

.. list-table::
   :header-rows: 1
   :widths: 8 26 66

   * - Stage
     - Name
     - What it holds
   * - 0
     - The frame
     - Cuts, the corpus of each cut (:doc:`corpus`), the benchmark windows,
       one evaluator and its strata.
   * - 1
     - Representations of the protein
     - Eight protein language models; vectors learned on top of them; the
       lexicon of sequence segments; self-supervised perception.
   * - 2
     - Sources of candidates
     - The mechanisms by which a term can be proposed: what the protein already
       says, transfer from neighbours, association between terms, the dynamics
       of knowledge, and what is frequent.
   * - 3
     - Signals
     - One dated context per candidate and cut, readable by any learner:
       engineered features and our own encodings of terms, annotations and
       neighbourhood.
   * - 4
     - Learners
     - The world model and the decision trees, side by side.
   * - 5
     - Stratified evaluation
     - Every arm at every cut, compared in pairs within each stratum.
   * - 6
     - Routing
     - A weighting of the arms per stratum, learned in validation.
   * - 7
     - Closure
     - Freeze, a single clean read, and the documentation.

Stage 0: the frame
------------------

A *cut* is a date, and what a method may know at a cut is fixed by the photo of
that cut (:doc:`corpus`). Methods are scored on the windows of the LAFA
benchmark's rules: the proteins that gain an experimental annotation between
two releases of the Gene Ontology annotation (GOA) files, scored on what they
gain. The windows in use are 160->194, 177->205, 194->211 and 211->227.

- **Nothing is learned after the cut.** Every component that sees function is
  fitted on transitions that end on or before the cut it is applied at. The
  window 211->227 is the validation window: it may be read as often as needed
  to choose, and nothing is ever fitted on its labels.
- **One evaluator, nine cells.** Three knowledge categories, from proteins with
  no experimental annotation before the window (no knowledge) through those
  annotated in other branches only (limited knowledge) to those already
  annotated in the same branch (partial knowledge), crossed with the three
  branches of GO: biological process, molecular function and cellular
  component.
- **Strata.** Every result is also reported by the sequence identity of the
  query to its closest annotated protein and by sequence length, always with
  the number of proteins in each stratum. An axis by organism is planned.

Stage 1: representations of the protein
---------------------------------------

**1.1 Eight protein language models, frozen.** ESM-2 (150M, 650M and 3B
parameters), ESM C 600M, ProtT5-XL, ProstT5, Ankh base and Ankh large, each
giving one vector per protein, computed for every cut from 2016 to 2025. They
were compared on 211->227 with the query excluded from its own neighbours by
accession (table below).

**1.2 Vectors learned on top of the language models.** A light head over the
residues of a frozen model, supervised with GO and therefore refitted at every
cut on earlier windows only.

**1.3 The lexicon.** Units of sequence segments learned from UniRef50 sequences
alone. The lexicon never sees function, so it is trained once and kept fixed.

**1.4 Self-supervised perception.** A joint-embedding predictive architecture
(JEPA) over sequences: it masks regions of a protein and predicts their
representation from the rest, without seeing any function. It is an optional
pretraining of perception, compared on top of a language model and on top of
the lexicon.

.. list-table:: Transfer from the nearest neighbour on LAFA 211->227, by language model
   :header-rows: 1
   :widths: 34 26 18 22

   * - Model
     - Weighted micro F, mean of nine cells
     - Gap to the best
     - Evaluation result
   * - ProtT5-XL
     - 0.3241
     - 0
     - ``1db46620``
   * - Ankh large
     - 0.3231
     - 0.0010
     - ``99a48782``
   * - Ankh base
     - 0.3219
     - 0.0022
     - ``b3f8543c``
   * - ESM-2 3B
     - 0.3217
     - 0.0024
     - ``c189426f``
   * - ESM-2 650M
     - 0.3203
     - 0.0038
     - ``7ea12145``
   * - ProstT5
     - 0.3199
     - 0.0042
     - ``faa9c427``
   * - ESM-2 150M
     - 0.3162
     - 0.0079
     - ``f2a07a8d``
   * - ESM C 600M
     - 0.3149
     - 0.0092
     - ``52b87a1e``

The scores are read raw, as the benchmark reads them; each row names the stored
evaluation result it was read from (the first eight hexadecimal digits of its
id). The choice of model moves
the result by less than a hundredth, so the expensive stages (1.2 and the world
model's perception) start from the best few.

Stage 2: sources of candidates
------------------------------

A *source* proposes candidate terms for a protein. Five mechanisms cover what
has been tried:

- **What the protein already says.** Its own non-experimental annotations at
  the cut. A good share of them are later confirmed by experiment, which makes
  this the strongest single source measured so far.
- **Transfer.** Terms carried by the protein's neighbours, found with each
  representation of stage 1 and with sequence homology.
- **Association.** Terms that tend to appear together with the ones the protein
  already has.
- **Dynamics.** How knowledge about proteins changes from one release to the
  next; this is what the world model learns.
- **What is frequent.** The prior: how often a term is annotated at all.

Stage 3: signals
----------------

One context per candidate and per cut, which any learner can read, every
signal dated so that it uses nothing published after the cut.

- **Engineered features.** Seventy-eight features in nineteen families: the
  strength and agreement of the evidence for a candidate, its place in the
  ontology, the protein's annotation history and its neighbourhood (see the
  :doc:`feature registry <reference/feature_reference>`).
- **Our own encodings**, which replace a pretrained ontology embedding from
  2020:

  1. *Of the ontology:* one vector per GO term from its text and its usage up
     to the cut.
  2. *Of the annotations:* the usage of each term up to the cut and the
     annotation state of each protein at the cut.
  3. *Of the neighbourhood:* what the protein's neighbours carry, aggregated.

Stage 4: the learners
---------------------

**Decision trees.** Gradient-boosted trees over the signals and the encodings,
one model per knowledge category, that score each (protein, candidate term)
pair. Each score can be explained by the contribution of every signal to it.

**The world model.** It predicts, in a learned representation, the protein's
*future knowledge state*: what will be known about it some time after the cut.

- *Its input, at the cut:* the protein's sequence (language model and
  lexicon), its neighbours and what they carry, and its own annotations.
- *What it predicts:* not the terms one by one, but the representation of the
  future state, trained to match the representation that a slowly updated copy
  of the encoder gives to the real future state. That is what makes it a JEPA:
  it predicts in representation space, not in label space.
- *How it is trained:* on real past transitions, pairs of states at two dates
  from the windows before the cut, using every protein that changed, not only
  the evaluation targets.
- *How it is read:* each term is scored by its compatibility with the predicted
  state. With a variable horizon, the reading is the probability that the term
  appears before that horizon; a horizon of zero reads the gaps, what should be
  annotated already and is not.

The five mechanisms of stage 2 all live inside the world model: what the
protein says is its state, transfer is its context, association is the geometry
of the term space, dynamics is the learned transition, and frequency is the
bias of the reading. That is why it is the natural rival of the trees.

**A fair comparison.** Both learners score the same thing, the probability that
a term is confirmed, over the same candidate set. If the world model also
generates terms outside that set, that is a separate arm, measured separately,
so that a gain can be told apart from seeing more candidates.

**Should the future non-experimental knowledge be a target?** The benchmark
scores experimental annotation, so that is always predicted. Non-experimental
annotation arrives earlier and in far greater quantity, and much of it is later
confirmed; but much of it is also the output of annotation pipelines being
updated, not new knowledge about the protein. The line measures the fork
directly: the world model trained on future experimental annotation only,
against the same model trained on experimental and non-experimental annotation
as two readings of one state, both scored on experimental truth only.

Stage 5: stratified evaluation
------------------------------

Every arm is evaluated in the platform at every cut, as a series in time, by
cell, by identity band and by length, with the number of proteins in each
stratum. Arms are compared in pairs within each stratum by a paired bootstrap,
and a difference counts when it is at least 0.01 with enough power to see it.

Stage 6: routing
----------------

The final method is a weighted mix of the arms per stratum. Each stratum has its
own weights, shrunk towards those of its cell and those towards the global
weights when the stratum has little evidence, so small strata are not overfitted
and no threshold is set by hand. Choosing a single arm per stratum is kept as a
control: it is the case of weights zero or one. The router is fitted in
validation and frozen before the clean read; every stratum it uses can be
computed at the cut.

Stage 7: closure
----------------

The method is frozen in a record that names every file it reads by its digest,
the revisions of the platform that ran it, and the comparisons that will be
made. It is then read once on a window opened after the freeze, and documented.

Where we are
------------

The measurements so far, on the validation window LAFA 211->227 (weighted micro
F-measure, mean of the nine cells):

.. list-table::
   :header-rows: 1
   :widths: 50 16 14 20

   * - Arm
     - Fitted on
     - 211->227
     - Evaluation result
   * - The protein's own annotations, ordered by a model that reads
       co-annotation (**champion**)
     - 194->211
     - 0.3543
     - ``fc04821a``
   * - The protein's own annotations, ordered without co-annotation
     - 194->211
     - 0.3505
     - ``0ddb3363``
   * - Transfer from the nearest neighbour, ProtT5-XL
     - (no fitting)
     - 0.3241
     - ``1db46620``

The current state and the next steps are on :doc:`news`.
