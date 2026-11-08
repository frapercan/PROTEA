ADR-D57: The vocabulary exams run inside the platform, on the codes it stores
=============================================================================

:Status: Proposed. The three questions it turns on were decided by the author on
         2026-11-08 and are recorded in the laboratory's design note
         (``agent-farm plans/DISENO-EXAMENES-VOCABULARIO.md`` at ``f334758``).
         The schema and the invariants below are this record's own, reviewed once
         by the laboratory session on 2026-11-08 and corrected by that review in
         six places, each marked where it lands.
:Date: 2026-11-08
:Author: Francisco Miguel Pérez Canales
:Phase: platform consolidation (the lab's research enters the platform)
:Depends on: ADR-D51 (the per-residue code), ADR-D54 (the model weights registry)

Context: four exams chose the vocabulary and none of them is in the platform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The author's rule, restated on 2026-11-08: no exam outside the platform, only
training. The vocabularies of the sparse line were chosen by four exams that
exist only in the laboratory, and deciding between them needed an exception
granted once (the lab's D37, widened in D38).

Verified on ``develop`` on 2026-11-08: of the 48 registered operations, none
examines a vocabulary. ``encode_residue_vocabulary`` computes a code per residue
and stores it; nothing reads it back as a measurement. So the component the
thesis rests on is the one component whose evidence is outside.

What the four exams measure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The specification is the laboratory's own code, branch ``compresion``, under
``compresion/scripts/``. What follows is a summary and not a second
specification: where they differ, the scripts are right.

1. **Domains** (``step11_dominios.py``). For each domain concept of Swiss-Prot
   2022_03 (``DOMAIN``, ``REPEAT``, ``ZN_FING``, ``DNA_BIND``, by name) and each
   feature of the vocabulary, residues are ranked by the feature's activation and
   the **average precision per residue** is counted, over a histogram of **64**
   levels. The feature is chosen on one sub-half of the proteins and measured on
   the other, both ways round, and averaged. The mean is over the concepts that
   were read. It also reports F1 per instance, detection, coverage and borders,
   but the rule uses the AP.
2. **Sites** (``step7_localization.py``, the second site exam). The same, over
   the site concepts: active sites, binding sites (also per frequent ligand),
   catalytic residues and the remaining Swiss-Prot 2022_03 feature types that
   mark residues, separated by evidence class. Its histogram has **256** levels,
   and residues where the feature is not active share the last place.
3. **Domains in a group** (``step22_dominios_grupo.py``, n = 10). On the
   selection half, the n features with the largest difference between their mean
   activation inside and outside the concept are chosen, a logistic regression is
   fitted over them (standardised, L2 1e-4, L-BFGS) and the AP per residue of its
   score is measured on the other half. **This one does not bin:** it computes the
   exact step-wise AP with ties together, the way scikit-learn does.
4. **Sites in a group** (``step23_sitios_grupo.py``). The same, as a curve over
   n = 1, 2, 5, 10, 20 and 50 (SAEBench's sizes), also with the exact AP.
   Reported without a rule.

**The number of levels is per exam and was nearly written as one number for all
four.** The first draft of this record said the exams bin into 256 levels, which
is true of sites only: domains uses 64 and the two group exams do not bin at all.
The error has the shape this project keeps meeting, and met twice on the same day
in another table: a measurement taken on one case and carried to the others
because the note it came from only described that case.

**The paired comparison** (``step38_remuestreo_examenes.py``, over the dumps of
``volcado_examen.py``) resamples proteins with replacement inside each half,
1,000 times, with the same draws for every vocabulary. Each draw recomputes the
AP the way the exam does, with the feature choices of the complete halves. The
result is each vocabulary's mean, its error, and the difference against a
reference vocabulary with its 95 per cent interval. The lab's D37 rule is read
on this.

The binning contract, which is where exactness lives or dies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For the exams that bin, with ``NB`` levels and an activation ``z``, all in
float32:

* ``scale = (NB - 1) / max(fmax, 1e-12)``, where ``fmax`` is each feature's
  maximum **over every protein the run reads**;
* ``level = ceil(z * scale)``, clipped to ``[1, NB - 1]``, when ``z > 0``;
* ``level = 0`` otherwise.

``fmax`` therefore depends on the set of proteins the run reads, which is a
second and independent reason for the rule in section 2: two runs over different
exclusion lists do not share a histogram, so their per-concept rows are not
comparable even before any feature is chosen.

Decision
~~~~~~~~

1. The exam reads the quantised codes the platform stores, by their value
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Not the continuous activations the laboratory examined. The exam then certifies
what is deposited, which is what the model reads.

**What it reads from a code is the DEQUANTISED value**, ``level_means[f, level +
1]`` of the vocabulary's own level table, where column 0 is the inactive state.
Not the level index. The distinction is not cosmetic: binning by a maximum gives
a different histogram from the index than from the value, so an exam that read
the index would be examining a different quantity and could not be compared with
anything. The test vectors are computed over the same reading.

The active features are the same either way; only the value is rounded to one of
256 levels. What that costs is measured ONCE, on the first vocabulary, by running
the lab's exam over both the quantised and the continuous codes: in
reconstruction that vocabulary goes from an R2 of 0.7592 to 0.7534.

Each candidate registers its level table before being examined, as the first one
did. A code without the table that quantised it is not reproducible, and the
table is what the paragraph above reads through.

2. The exam population is a declared set, and the exclusion is DERIVED
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each population enters as a declared set under the rule of 2026-11-08: declared,
described and verifiable. What is declared is not only the membership:

* the **pairs of (accession, md5 of the exam's sequence)**, one per line, with
  the file's sha256 and the partition into selection and confirmation;
* the **concepts**, because they are exam data and not metadata:
  ``sites.parquet`` (1,986,242 rows), ``instances.parquet`` (10,070 instances)
  and ``concepts.json``, which carries each concept's role, its ``read_before``
  mark and the small ones. Each with its own sha256;
* the derivation: the script and the laboratory commit that built them.

**The set holds all 3,335 and all 4,227.** It is NOT declared already trimmed of
the proteins whose sequence moved, and that is a decision with a reason: a set
declared trimmed makes the exclusion an assertion nobody can check afterwards,
and the right trim changes the day ``resolve_protein_sequences`` updates one more
sequence. So the exam derives it: for each member it compares the md5 of the
platform's sequence against the md5 the set declares, examines only the proteins
that agree, and **names the excluded ones in its own event**. The figure is 10
and 40 today; the mechanism does not depend on it.

Two consequences, and they are what makes a comparison between two runs valid:

* **The paired resampling only compares vocabularies examined with the SAME
  exclusion list.** If a later run excludes more because another sequence moved,
  either the comparison is restricted to the proteins common to both or the
  reference is examined again in that same run. Comparing rows from two runs with
  different lists is the defect this project keeps meeting under another name: a
  level named by fewer fields than vary. And by the binning contract above the
  two runs do not even share a histogram. The run therefore records the sha256 of
  its sorted exclusion list, so two runs can be checked against each other by one
  value instead of by a list.
* **The test vectors are computed for the list the exam's event names**, never
  for "the 50 of today".

3. The confirmation set is spent once, and its protocol is not the other one
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With the candidates and the rule written down before, which is the lab's D39.
That is the number of the thesis. Until then everything is read on the selection
set.

The two protocols differ, so a run states ``chosen_on`` and ``measured_on`` and
not only which half it sits in. A single ``half`` column would print the same
value for two different estimators:

* **on the selection set**, the feature is chosen on one sub-half and measured on
  the other, in both directions, and the two are averaged;
* **on the confirmation set**, the feature is chosen on the WHOLE selection set
  and measured on the confirmation proteins, in one direction only. In the group
  exams the regression is fitted on the selection set.

``fmax`` in a confirmation run is computed over every protein the run reads. It
uses no labels, so it filters nothing and cannot leak the half it is about to
measure.

The reserved concepts, never read, are a **second run** and not a second mean on
the same row: the set of concepts is part of the declared population and enters
the run's key, so every row carries one mean. In sites, the two families that
were looked at before the exam was frozen are reported apart the same way.

4. One run examines every candidate it compares, over one exclusion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The proteins removed because they trained a vocabulary are the **union** over
every vocabulary the run compares, applied to all of them at once. Not one
exclusion per vocabulary: if each candidate drops only its own, the rows stop
being paired and the paired resampling is measuring two populations. This is how
the laboratory did it, with ``--out-of-sample 20k`` removing the same proteins
from every code.

5. The training population is a prerequisite the registry does not carry
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For the exam to know what to exclude, a vocabulary has to declare what it was
fitted on. Today ``model_weights`` cannot say it. It has
``trained_on_annotation_set_id``, which names a GOA **annotation set**: a set of
annotations, not a population of sequences, and a sample of ten million UniRef50
representatives is not an annotation set at all. So the gap is real and it
belongs to ADR-D54, not to the exam. It is closed by its own change before this
operation runs, and the exam refuses a candidate that does not declare one rather
than examining it against an empty exclusion.

The populations are of two classes, and the exam treats them differently:

* **The training side of the corpus's 20k scale**, for the first vocabulary and
  for each corpus-final one. These are REMOVED, as the lab removed them.
* **The UniRef50 2022_03 samples** (one and ten million representatives) without
  the reserve, for the UniRef50 phases. Here the reserve of the lab's D4 (the
  union of the 50 per cent rule and the exact membership of the evaluation
  proteins) already keeps the exam proteins' groups out, so the exam **checks
  that the intersection is empty**, by accession and by md5, and removes nothing.
  A non-empty intersection is a failure of the reserve and the exam says so
  loudly instead of quietly fixing it.

Both enter as declared artifacts with their sha256 rather than as rows: the ten
million ids of a sample are a file, and copying them into a table buys nothing
the digest does not already give.

6. Three tables, and the laboratory's own column names
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The per-concept numbers live in the database and not in a parquet beside it. The
author's aim is that the result be visible and deposited in the platform, and a
figure that can only be read by downloading a file is deposited but not visible.
The volume does not argue otherwise: about 230 concepts per vocabulary and exam.

``vocabulary_exam_run``, one row per (vocabulary, exam, protocol, population,
concept set):

* the vocabulary (``model_weights``, ``RESTRICT``) and the exam, one of
  ``domains``, ``sites``, ``domains_group``, ``sites_group``;
* ``n`` and ``n_source`` for the group exams, null for the other two, so a curve
  over six sizes is six runs and not one row with six numbers;
* the exam population (``query_set``), the concept set, ``chosen_on``,
  ``measured_on``, and whether the codes were quantised;
* ``levels``: the number of histogram levels this exam used, null for the two
  exact ones, because a run that cannot say it is a run nobody can reproduce;
* the exclusions: the sorted list, its count and its sha256, and the sha256 of
  the training-population union that was applied;
* the laboratory's own per-vocabulary figures: ``features``, ``features_on`` (or
  ``features_on_in_exam`` in sites, ``features_used`` in the group exams),
  ``concepts_read``, ``mean_ap_held_out``, ``median_lift_held_out`` in sites, and
  ``mean_ap_held_out_grupo`` with ``mean_ap_held_out_uno`` in the group exams;
* the dumps for the paired resampling, by uri and sha256;
* its job, its code revision and the digests of everything it read.

``vocabulary_exam_concept``, one row per (run, concept). The column on the left
is this table's; the one on the right is the laboratory's, read from its output
and not coined here:

.. list-table::
   :header-rows: 1

   * - this table
     - the laboratory's
     - note
   * - ``ap``
     - ``ap_held_out``, ``ap_held_out_grupo``, ``ap_held_out_by_n[n]``
     - the number the rule reads
   * - ``ap_in_sample``
     - ``ap_in_sample``
     - sites only, and DESCRIPTIVE: the choice made within sample over both
       sub-halves together. It is not the feature behind ``ap_held_out``
   * - ``ap_single``
     - ``ap_held_out_uno``
     - the group exams, the exact AP of the first selected feature
   * - ``base_rate``
     - ``base_rate``
     - all four
   * - ``n_instances``
     - ``instances``
     - domains only
   * - ``n_positive_residues``
     - ``positives``
     - sites and sites in a group
   * - ``f1_instance``
     - ``f1_held_out``
     -
   * - ``detection``
     - ``detection_auroc_held_out``
     -
   * - ``coverage``
     - ``residue_jaccard_held_out``
     -
   * - ``borders``
     - ``boundary_iou_held_out``
     -
   * - ``group_size``
     - ``group_size``
     - two values, one per direction, each at most ``n``: only features with a
       positive difference of means enter, or the first one if none does
   * - ``reserved``, ``read_before``
     - the role in ``concepts.json``
     - whether the concept was never read, or was looked at before the freeze

There is no ``n_proteins`` column, because the laboratory does not write one per
concept and a count this record invented would be a number with no counterpart
to be accepted against.

A concept with no positives in both sub-halves has no ``ap_held_out`` and does
not enter the mean. The row exists, with the AP null, because "measured and
empty" and "not measured" are different facts and a missing row says neither.

``vocabulary_exam_choice``, one row per (run, concept, chosen_on_half, metric,
rank): the feature, its ``exam_bin`` where the metric has a threshold, and its
coefficient where the metric fits one. The threshold is a bin of THIS EXAM's
histogram and is deliberately not called ``level``, which in every other table
means a level of the vocabulary's quantisation: two different quantities one
character apart in the same schema is how a reader joins the wrong one. It exists because **the chosen feature is
two, not one**: the single-feature AP is ``(a1[argmax a0] + a0[argmax a1]) / 2``,
so there is a feature chosen on sub-half 0 and measured on 1 and another the
other way round, and today they survive only inside the dump. The extras choose
independently of the AP: F1 and coverage each pick a (feature, level) per
sub-half, borders reuse coverage's, and detection takes its own argmax. One table
keyed by the metric holds all of them, and it answers the question a JSONB column
could not: which feature did each vocabulary choose for the same concept.

Acceptance, which is exact in two exams and measured in the other two
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the selection set, without the proteins whose sequence moved, against the
laboratory's scripts run over the same quantised codes of the first vocabulary.

Exactness is available at all because the platform's quantised codes and the
lab's reference encoder are identical bit for bit: zero differences over
309,370,880 entries. Any difference that appears is therefore in the exam.

**Domains and sites: exact, and in two steps.** First the histograms compared bit
for bit (``HA`` and ``hp_hist`` of the dump), because an AP that agrees over
histograms that do not is two errors cancelling. Then the AP in double precision
to 1e-12, concept by concept.

**The two group exams: nothing is required to be identical, because nothing in
them is.** The first version of this record said the feature selection was a
ranking of mean differences with ties broken by index, therefore deterministic,
and required it to be identical across devices. That is false, and it is false
in four places inside ``step22`` alone:

* each protein's contribution is summed in float32 ON THE DEVICE and only then
  widened (``tot[h] += z.sum(0).double()``), and a card and a CPU do not reduce
  in the same order;
* ``diff = (mp - mn).float()`` rounds the difference back to float32;
* ``torch.topk`` does not fix the order of ties;
* when no feature has a positive difference the code keeps ``top[:1]``, and there
  the ties are at exactly zero.

Over values dequantised to 256 levels, near-ties are not rare. So the selection
can differ between the laboratory's card and the platform's CPU, and with it
``ap_held_out_uno``, which is the exact AP of whichever feature came first.

The same error twice in one record, and the same error this project keeps paying
for: a property asserted from reading what the code MEANS instead of what it
executes. The measured precedent was already written down, about this very
vocabulary: a 1-ulp difference in a matmul makes a k-WTA select a different atom.

So the acceptance for these two exams **measures agreement rather than requiring
it**, over one double run of the laboratory's own exam, once per device, on the
same codes:

* the selection is compared concept by concept. If it is identical everywhere it
  becomes a requirement; where it is not, the concepts that move are COUNTED AND
  NAMED, and the same for ``ap_held_out_uno``;
* the regression's AP is accepted within the floor that double run measures. It
  is torch's L-BFGS (lr 1, max_iter 100, strong Wolfe, L2 1e-4, standardised with
  the deviation clipped to 1e-6), bit-identical only under the same
  implementation on the same device.

A tolerance chosen by hand would be a number with no population behind it, which
is the error this project has paid for more than once.

The runs of the lab's D37 and D38 do not serve as the criterion as they stand:
they include the 50 and they are continuous, not quantised.

What this record does not decide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Which vocabulary is adopted.** The exam produces the numbers the lab's D39
  rule reads. The decision is the author's.
* **Whether the exam campaign is an exploration under ADR-D55.** It would fit
  what that record is for, since an exploration already ties rules frozen before
  any number is read to its arms and its review. It does not fit the table:
  ``exploration.evaluation_set_id`` is ``NOT NULL``, and these exams have no
  evaluation window at all. They sit on Swiss-Prot 2022_03, which has nothing to
  do with the cut of a transition window. Widening that column is a change to
  D55's unit and is not made here on the way past.
* **The paired resampling as an operation.** The design is the lab's
  ``step38``, the dumps are written by the runs above, and the bootstrap is a
  second operation with its own record.
