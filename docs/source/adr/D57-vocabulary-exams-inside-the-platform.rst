ADR-D57: The vocabulary exams run inside the platform, on the codes it stores
=============================================================================

:Status: Proposed. The three questions it turns on were decided by the author on
         2026-11-08 and are recorded in the laboratory's design note
         (``agent-farm plans/DISENO-EXAMENES-VOCABULARIO.md`` at ``f334758``).
         The schema and the invariants below are this record's own and are for
         review.
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
   the **average precision per residue** is counted. The feature is chosen on one
   half of the proteins and measured on the other, both ways round, and averaged.
   The mean is over the concepts that were read. It also reports F1 per instance,
   detection, coverage and borders, but the rule uses the AP.
2. **Sites** (``step7_localization.py``, exam v2). The same, over the site
   concepts: active sites, binding sites (also per frequent ligand), catalytic
   residues and the remaining Swiss-Prot 2022_03 feature types that mark
   residues, separated by evidence class. The AP is computed from histograms of
   256 levels of each feature's maximum in the exam, and residues where the
   feature is not active share the last place.
3. **Domains in a group** (``step22_dominios_grupo.py``, n = 10). On the
   selection half, the n features with the largest difference between their mean
   activation inside and outside the concept are chosen, a logistic regression is
   fitted over them (standardised, L2 1e-4, L-BFGS) and the AP per residue of its
   score is measured on the other half, with ties together.
4. **Sites in a group** (``step23_sitios_grupo.py``). The same, as a curve over
   n = 1, 2, 5, 10, 20 and 50 (SAEBench's sizes). Reported without a rule.

**The paired comparison** (``step38_remuestreo_examenes.py``, over the dumps of
``volcado_examen.py``) resamples proteins with replacement inside each half,
1,000 times, with the same draws for every vocabulary. Each draw recomputes the
AP the way the exam does, with the feature choices of the complete halves. The
result is each vocabulary's mean, its error, and the difference against a
reference vocabulary with its 95 per cent interval. The lab's D37 rule is read
on this.

The two populations, and the one number that bites
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Both were built once and frozen. Counted on 2026-11-08 from ``split.parquet``
and ``concepts.json``, membership only, with no label read:

.. list-table::
   :header-rows: 1

   * - counted 2026-11-08
     - sites (exam v2)
     - domains
   * - proteins
     - 3,335
     - 4,227
   * - selection set
     - 1,662
     - 2,128
   * - confirmation set
     - 1,673
     - 2,099
   * - concepts of the selection set
     - 79
     - 71
   * - reserved concepts, never read
     - 42
     - 40
   * - sequence differs from the platform's today
     - **10**
     - **40**

Every one of the 7,562 proteins is in ``protein``, has a vector in ``f3a7fe16``
and has codes of the first vocabulary. **Examining that vocabulary needs no new
pass.**

The last row is the one that bites, and it is a problem of POSITIONS and not of
coverage. An exam concept is a set of positions in the Swiss-Prot 2022_03
sequence. The platform stores today's sequence and computed its codes over that
one; there are cases of 377 residues against 1,751. In those 50 proteins a
concept would point at residues that are not the ones it means.

Decision
~~~~~~~~

1. The exam reads the quantised codes the platform stores
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Not the continuous activations the laboratory examined. The exam then certifies
what is deposited, which is what the model reads.

The active features are the same either way; only the value is rounded to one of
256 levels, and the exams already bin activations into histograms of 256 levels,
so little is expected to move. What it costs is measured ONCE, on the first
vocabulary, by running the lab's exam over both: in reconstruction that
vocabulary goes from an R2 of 0.7592 to 0.7534.

Each candidate registers its level table before being examined, as the first one
did. A code without the table that quantised it is not reproducible.

2. The exam population is a declared set, and the exclusion is DERIVED
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each population enters as a declared set under the rule of 2026-11-08: declared,
described and verifiable. Its artifact holds **pairs of (accession, md5 of the
exam's sequence)**, one per line, with the file's sha256, its description, its
derivation (the script and the lab commit that built it) and the partition into
selection and confirmation.

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
  level named by fewer fields than vary. The run therefore records the sha256 of
  its sorted exclusion list, so two runs can be checked against each other by one
  value instead of by a list.
* **The test vectors are computed for the list the exam's event names**, never
  for "the 50 of today".

3. The confirmation set is spent once, at the end, by the platform
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With the candidates and the rule written down before, which is the lab's D39.
The feature of each concept is chosen on the WHOLE selection set and measured on
the confirmation proteins. The reserved concepts, never read, give a second and
separate reading, of concepts nobody has seen. In sites, the two families that
were looked at before the exam was frozen are reported apart.

That is the number of the thesis. Until then everything is read on the selection
set.

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

6. Where the rows go, and the shape the lab's numbers are compared against
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Two tables, and the per-concept numbers live in the database and not in a
parquet beside it. The author's aim is that the result be visible and deposited
in the platform, and a figure that can only be read by downloading a file is
deposited but not visible. The volume does not argue otherwise: about 230
concepts per vocabulary and exam.

``vocabulary_exam_run``, one row per (vocabulary, exam, half, population):

* the vocabulary (``model_weights``, ``RESTRICT``) and the exam, one of
  ``domains``, ``sites``, ``domains_group``, ``sites_group``;
* ``n_features``, null for the two single-feature exams and the size of the group
  for the other two, so a curve over six sizes is six runs and not one row with
  six numbers;
* the exam population (``query_set``), the half (``selection`` or
  ``confirmation``), and whether the codes were quantised;
* the exclusions: the sorted list, its count and its sha256, and the sha256 of
  the training-population union that was applied;
* the means the exam reports, and how many concepts were read;
* the dumps for the paired resampling, by uri and sha256;
* its job, its code revision and the digests of everything it read.

``vocabulary_exam_concept``, one row per (run, concept):

* the concept, its kind, and whether it is reserved or was read before freezing;
* the AP, which is the number the rule uses;
* the counts the AP is over: proteins, positive residues, instances;
* the feature chosen, and for the group exams the features chosen;
* the extras the exams report: F1 per instance, detection, coverage, borders.

**The column names of the extras are the laboratory's**, and this record does not
coin them. They are confirmed against the scripts' own output before the
operation is written, so that the acceptance below is a row-by-row comparison and
not a mapping exercise.

Acceptance
~~~~~~~~~~

On the selection set, without the proteins whose sequence moved, the ported exam
must give **the same AP, concept by concept**, as the laboratory's scripts run
over the same quantised codes of the first vocabulary.

The exactness is available because the platform's quantised codes and the lab's
reference encoder are identical bit for bit: zero differences over 309,370,880
entries. So any difference that appears is in the exam and not in the code, which
is what makes an exact criterion worth stating instead of a tolerance.

The runs of the lab's D37 and D38 do not serve as that criterion as they stand:
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
