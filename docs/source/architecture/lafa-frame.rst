LAFA's frame
============

.. contents:: On this page
   :local:
   :depth: 2

LAFA's frame builds a window's ground truth the way LAFA builds its own
windows, so that a number PROTEA reports on a window means what the same number
means on LAFA's leaderboard. It is the frame of the benchmark: on 2026-11-20 the
author asked for the benchmark to be faithful to CAFA and LAFA, with one
departure of his own, the ``NOT`` rows, described below. The code is
:mod:`protea.core.lafa_frame`; a window built by it is named by the rule set
``lafa-frame``.

Where the rules come from
-------------------------

LAFA publishes the code that builds its windows, and the frame follows that code
rather than a description of it:

- the workflow, `CAFA_forever <https://github.com/anphan0828/CAFA_forever>`_,
  read at commit ``2ce245715aa9``: ``modules/local/timepoint_build.nf`` builds
  each end of a window and ``modules/local/evaluation.nf`` builds the window and
  scores it;
- the package that workflow calls,
  `democafa_package <https://github.com/anphan0828/democafa_package>`_, read at
  commit ``1102e34ec2cd``.

Both commits are stamped on every window the frame builds, in its stats
(``lafa_workflow`` and ``lafa_package``). The frame is PROTEA's own code written
from that reading; nothing is copied, and the package carries no licence. The
paper that describes LAFA states the rules only in outline and points to these
two repositories for the details.

Each end, alone, under its own ontology
---------------------------------------

The workflow builds each end of a window on its own, from that end's GOA release
and the GO release paired with it:

1. **The experimental rows.** The thirteen codes (EXP, IDA, IPI, IMP, IGI, IEP,
   HTP, HDA, HMP, HGI, HEP, IC, TAS), and no row whose qualifier says ``NOT``
   (``retrieve_terms -sgc 'Experimental,IC,TAS'``).
2. **Every id as that end's ontology names it.** A secondary id (``alt_id``)
   becomes its primary, then an obsolete term with a ``replaced_by`` becomes its
   successor, one step each and in that order; whatever is still not a live term
   of that ontology is dropped.
3. **Propagated** to its ancestors through ``is_a`` and ``part_of`` inside one
   aspect (``propagate_and_ia``).

The window
----------

``classify_ground_truth`` then compares the two ends:

4. **The terms of interest** are the ids live in both ontologies, and both ends
   are trimmed to them before anything is compared.
5. **The protein-binding rule.** A protein whose trimmed F side at t1 holds
   nothing but "protein binding" (GO:0005515) and its ancestors in the t0
   ontology ("binding" and the molecular-function root) loses that F side. t0
   is left alone.
6. **The classification.** A gain is a t1 term the protein did not hold at t0. A
   protein with nothing at t0 is NK, and every t1 term of it is truth.
   Otherwise, per aspect with gains: LK when t0 held nothing in that aspect, PK
   when it did, and PK's known terms are the trimmed t0 terms of that aspect.

The targets
-----------

LAFA scores the accessions in Swiss-Prot at both ends whose sequence is the same
at both. In PROTEA that is the target population ``swissprot_same_sequence``:
the Swiss-Prot release paired with each end's cut photo, joined on the
accession, keeping the ones whose sequence md5 agrees. The classification is per
protein, so applying the population after it, as the operation does for every
population, gives the same truth as LAFA's filter before it.

The departure: ``NOT`` rows take truth away
-------------------------------------------

LAFA drops a ``NOT`` row and keeps every positive annotation the row
contradicts. The frame does not. A ``NOT`` row with an experimental code, at
either end, removes its term and every term under it from the **direct**
annotations of both ends, before step 3. Because it happens before the
propagation, the ancestors that only the negated annotations supported never
enter the truth either.

An example. A protein has an experimental annotation to "protein kinase
activity" and to "identical protein binding", and an experimental ``NOT`` to
"kinase activity". The kinase annotation is removed, and with it "catalytic
activity", which only the kinase supported. The molecular-function root stays,
because the binding annotation supports it.

The window's ``not_codes`` says which ``NOT`` rows count; ``lafa-frame`` says
``experimental``.

Known flaws
-----------

Recorded here even when they are LAFA's, because a reader comparing PROTEA's
numbers with LAFA's needs to know them.

**LAFA keeps what a NOT contradicts.** Described above; the frame departs from
LAFA here on purpose.

**A change to the ontology alone counts as new knowledge.** Each end is closed
under its own ontology, so when the t1 ontology gives a term a new parent that
was live at t0 already, a protein annotated to that term at both ends gains the
parent at t1 without any new annotation. LAFA's own code asks about it in a
comment ("should not be gaining?"). The frame keeps it, to stay faithful, and
``tests/test_lafa_frame.py`` pins it. The longer the window, the more the
ontology changes, so the larger this share of PK: every window of the frame
records it in its stats, over every protein before the population, as
``lafa_pk_pairs_made_by_the_ontology`` (gained pairs that no new direct
annotation explains) and ``lafa_pk_proteins_made_by_the_ontology`` (PK proteins
none of whose gains one explains).

**PROTEA's reconciled path negates after its closure.** Every window PROTEA
built before this frame removed the negated terms from the propagated set, not
from the direct annotations, so an ancestor that only a negated annotation
supported stayed in its truth. The same-snapshot path does not have this flaw;
the frame does not either.

Scoring
-------

LAFA scores with ``cafaeval`` at the t0 ontology, with t0's information
accretion over the t0 Swiss-Prot cohort, the terms of interest, and
``-prop fill -norm cafa -no_orphans``, adding ``-known`` for PK. PROTEA keeps
its own optimised evaluator, ``cafaeval-protea``, which reproduces LAFA's nine
cells exactly when it is given LAFA's own truth. A window of the frame
therefore has its pivot at the t0 ontology (the closure ``per_side_t0_pivot``),
and is scored over the terms of interest its gain rule names: the ids live in
both ontologies, the terms step 4 trims to (39,028 on 211 -> 227). The
evaluator takes them from the window, never from the request; it refuses a
window whose recorded count of them differs, and records on each result the
rule, the count and the sha256 of the file cafaeval read. Until 2026-11-20 a
request named the rule and defaulted to every term of the pivot, obsolete ones
included, so a frame window evaluated without naming it was scored over 47,328
terms.

LAFA's information accretion is a table of PROTEA's own
``compute_information_accretion`` with the evidence regime ``lafa`` and the
cohort ``swissprot``: the corpus's own Swiss-Prot list, the fourth axis a table
is identified by. On cut 211 it is the table LAFA's own code computes from the
same files, term by term:

.. literalinclude:: ../_data/lafa-information-accretion-check-211.json
   :language: json

What a window of the frame declares
-----------------------------------

======================  ==============================
``truth_rules`` key     value
======================  ==============================
``profile``             ``own_graph``
``toi_rule``            ``both_ends_trimmed``
``not_codes``           ``experimental``
``closure``             ``per_side_t0_pivot``
``binding``             ``cafa``
======================  ==============================

Its target population is ``swissprot_same_sequence``, and its stats carry the
two commits and the counts of every step: ids renamed and dropped at each end,
proteins and direct terms negated, pairs before and after the trim, the size of
the terms of interest and the F sides the binding rule dropped.

To build one, name the rule set and nothing else:

.. code-block:: json

   {
     "operation": "generate_evaluation_set",
     "payload": {
       "old_annotation_set_id": "<the t0 corpus>",
       "new_annotation_set_id": "<the t1 corpus>",
       "truth_rule_set": "lafa-frame",
       "window_role": "validation"
     }
   }

How it is checked
-----------------

**Against LAFA's own code.** On the window 211 -> 227, LAFA's own code and the
frame read the same inputs, and their truths are identical: every NK, LK and PK
pair, PK's known terms, the targets and the terms of interest, each with a
Jaccard index of 1.0 for the frame as LAFA publishes it. With the author's
``NOT`` rule, the frame differs from LAFA by exactly what that rule removes. The
record of the check holds the digest of every input, the commits on both sides
and the comparison per category:

.. literalinclude:: ../_data/lafa-frame-check-211-227.json
   :language: json

The check is three scripts in ``scripts/lafa_check``, and anyone with a store
can run it again:

1. ``export_inputs.py`` writes, from the store and read only, each end's
   ontology (from the artifact store, its digest verified), its experimental
   rows as a GAF sorted by every column, its Swiss-Prot FASTA from the cached
   release archive, and the frame's truth in LAFA's file format;
2. ``run_lafa.sh`` runs LAFA's own code on those files, step by step as LAFA's
   workflow calls it, in the environment ``lafa-environment.txt`` pins. That
   step reads files only;
3. ``compare_truth.py`` writes the record above, and
   ``compare_information_accretion.py`` the one for the information accretion,
   in the Scoring section.

Building that environment turned up a fault of LAFA's package worth knowing:
its ``pyproject.toml`` does not declare ``obonet``, ``networkx`` or ``tqdm``,
which it imports, so an environment built from it alone fails at the first step
that needs one.

**In the test suite.** ``tests/test_lafa_frame.py`` pins every step on
ontologies small enough to read, including the order of the two renamings, the
binding rule after the trim, the departure and the gains made by the ontology;
``tests/test_lafa_frame_store.py`` runs the frame end to end through its SQL in
Postgres, together with LAFA's targets. On the six cuts of the frame in the
development store, the binding set read from each ontology is exactly the three
terms, and the ontology of cut 227 has as many live terms as the information
accretion LAFA published for its window starting there
(``IA_TERMS`` in ``tests/test_lafa_profile_acceptance.py``).
