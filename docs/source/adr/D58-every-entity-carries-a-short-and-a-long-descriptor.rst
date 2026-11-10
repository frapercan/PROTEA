ADR-D58: Every cited entity carries a short descriptor and a long one
=====================================================================

:Status: Proposed. The requirement is the author's, stated on 2026-11-10
         ("Necesito siempre que la plataforma utilice un descriptor semantico
         breve, y otro largo... Para cada entidad practicamente") after
         correcting the same defect twice. The scope rule, the contents of each
         descriptor and the gate below are this record's own.
:Date: 2026-11-10
:Author: Francisco Miguel Perez Canales
:Phase: platform consolidation
:Depends on: ADR-D48 (the stage-1 configs, whose ``display_name`` is the
             pattern this generalises), ADR-D56 (a window cannot be named by
             its version pair)

Context: the platform can name four of its entities and cites eighty-seven rows it cannot name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Measured on this deployment on 2026-11-10, over every table in the schema:

.. list-table::
   :header-rows: 1
   :widths: 30 12 30 28

   * - entity
     - rows
     - short descriptor
     - long descriptor
   * - ``embedding_config``
     - 14
     - ``display_name``
     - ``description``
   * - ``query_set``
     - 12
     - ``name``
     - ``description``
   * - ``model_weights``
     - 2
     - ``name``
     - ``description``
   * - ``scoring_config``
     - 1
     - ``name``
     - ``description``
   * - ``experiment_run``
     - 0
     - ``name``
     - ``description``
   * - ``job``
     - 778
     - none
     - ``description``, ``findings``
   * - ``exploration``
     - 3
     - ``title``
     - none
   * - ``dataset``
     - 0
     - ``name``
     - none
   * - ``reranker_model``
     - 0
     - ``name``
     - none
   * - ``external_reference``
     - 0
     - ``title``
     - none
   * - ``interpro_go_mapping``
     - 0
     - none
     - none
   * - ``evaluation_set``
     - 87
     - none
     - none
   * - ``annotation_set``
     - 75
     - none
     - none
   * - ``ontology_snapshot``
     - 66
     - none
     - none
   * - ``prediction_set``
     - 26
     - none
     - none
   * - ``evaluation_result``
     - 21
     - none
     - none
   * - ``information_accretion_set``
     - 18
     - none
     - none
   * - ``evaluation_frame``
     - 8
     - none
     - none
   * - ``swissprot_release``
     - 3
     - none
     - none
   * - ``reference_reproduction``
     - 1
     - none
     - none

Five of the twenty cited entities carry both. Fifteen have a gap: **ten carry
neither descriptor**, four carry the short one and no long one, and ``job``,
with 778 rows, is the only one with a long descriptor and no name. The three
biggest holes are the three that appear most often in a sentence: the evaluation
windows with 87 rows, the annotation sets with 75 and the ontology snapshots
with 66.

**Why this is a schema defect and not a style preference.** The author was
given three validation windows as bare uuid prefixes on 2026-11-10 and answered
with the correction, for the second time. A hex prefix carries nothing he can
check: a sentence built on one cannot be agreed or disagreed with until somebody
looks the code up, which moves the cost of the sentence from the writer to the
reader. It is also the shape of a defect this project has already paid for
twice, recorded in the laboratory notes as a relayed digest that was right in
its first eight characters and named different bytes.

**And the platform has nothing else to offer for a window.** ``evaluation_set``
carries the two annotation sets, the job, the stats, the ground truth uri and
digest, the window role, three snapshots, the truth profile, the term rule and
the revision. Every one of those is a property; none is a name. ADR-D56 already
settled that the version pair cannot serve as one, because 62 of 69 pairs carry
more than one window. So the hex is not laziness: it is the only identifier the
row has.

Decision 1: the scope is what gets cited, not what gets counted
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A descriptor belongs to a row somebody names in a sentence: a configuration, a
set, a snapshot, a run, a frame, a model, a window, a release. It does not
belong to the facts inside one of those. The reason is measured and not
aesthetic: ``protein_go_annotation`` holds 663.408.837 rows,
``go_prediction`` 33.423.682 and ``sequence_embedding`` 2.736.193. A descriptor
per row there is storage spent on text nobody will ever read, and worse, it
invites a per-row label that disagrees with the set's own.

So: **cited entities carry the two descriptors; counted entities carry none, and
are named by the entity they belong to.** The twenty in the table above are
the cited ones. The counted ones are the association and event tables
(annotations, predictions, embeddings, events, entries, memberships,
alignments, relationships, aliases, visits).

Three entities sit on the line and are decided here:

* ``protein`` and ``sequence`` are cited constantly and are already named: the
  accession is the short descriptor and UniProt's own protein name, already
  stored in ``protein_uniprot_metadata``, is the long one. Nothing is added.
* ``go_term`` carries ``name`` from the ontology and its long descriptor is the
  OBO ``def:`` field, which is real data rather than something this platform
  would invent. Over 3.118.456 rows that is a few hundred megabytes, so it is
  **deferred to the author** rather than decided here.
* ``api_key`` and ``user`` are operational rows, not research entities. They
  keep what they have.

Decision 2: the short descriptor is a name, which means unique and stable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``short_name`` is a lowercase slug, at most 64 characters, matching
``[a-z0-9][a-z0-9-]*``, **UNIQUE per table and never rewritten once a row
exists**. A label that can change is not a name: the point of it is to replace
the uuid in a sentence, a commit message and a conversation six months later,
and a mutable label makes two sentences about the same row look like sentences
about two rows.

It is **derived from what distinguishes the row from its siblings**, by the
operation that creates it, inside the job. It is not typed by a person into a
form, for the same reason set provenance is not: a name living in a payload is
a claim, and the four query sets registered by hand are the project's own
example of what that costs.

What it must not be:

* a uuid prefix, which is the thing being replaced;
* a version pair for a window (ADR-D56: the pair does not identify one);
* a timestamp alone, which distinguishes rows without describing them.

Decision 3: the long descriptor names every field that varies among its siblings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``description`` is a sentence, in English, that says what the row is and what
separates it from the rows it will be compared against. The rule for its
contents is the one this project keeps relearning: a level must be named by
every field that varies. The three long validation windows are the worked
example, because they are the rows that caused this record:

* two of them close on the pivot's terms (``toi_rule = pivot_terms``) with
  39.885 and 63.734 delta proteins;
* the third takes the terms live in both ends (``toi_rule =
  live_in_both_ends``) with 40.531, and its truth is the projection rather than
  the closure (``truth_profile = projected``).

A description that said "the long validation window" for any of them would be
true of three different populations. One that says "projected truth, terms live
in both ends, 40.531 delta proteins" names exactly one, and a reader can check
every clause against the row.

Decision 4: both are written at creation, by the operation, and the backfill derives
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

New rows get both descriptors in the same flush that creates them, from the
operation that measured the row. Not by a later ``UPDATE``, and not by a
migration that invents prose: the declaring operations already build their
description by counting what they wrote, and that is the pattern.

For the rows that exist, each entity gets a migration whose backfill **derives**
the two descriptors from columns already on the row, and **refuses rather than
guessing**. Where a derivation is impossible the row keeps a NULL and the
operation reports it, because for a name "cannot say" beats a wrong one. The
columns become ``NOT NULL`` only for an entity whose backfill derived every
row.

Correspondence, one line per entity, naming what each descriptor derives from:

.. list-table::
   :header-rows: 1
   :widths: 26 36 38

   * - entity
     - short descriptor derives from
     - long descriptor derives from
   * - ``evaluation_set``
     - window role, truth profile, term rule, the two releases
     - the above plus the delta and the three knowledge regimes
   * - ``annotation_set``
     - source and source version
     - the ontology snapshot, the publication date, the row count
   * - ``ontology_snapshot``
     - the obo version
     - the term count, the archive digest, the release date
   * - ``prediction_set``
     - the embedding config, the annotation set, k
     - the feature arms enabled, the donor policy, the revision
   * - ``evaluation_result``
     - the prediction set and the evaluation set
     - the scoring config, the reranker, the frame digest
   * - ``evaluation_frame``
     - the frame digest's readable parts
     - every field the digest covers
   * - ``information_accretion_set``
     - snapshot, corpus, evidence regime
     - the resolved code list and the shape counters
   * - ``swissprot_release``
     - the release name
     - the entry count and what was loaded from it
   * - ``dataset``
     - the existing ``name``
     - the dump parameters and the row counts
   * - ``reranker_model``
     - the existing ``name``
     - the dataset, the schema digest, the metrics
   * - ``job``
     - the operation and its target
     - the existing ``description``
   * - ``reference_reproduction``
     - what it reproduces
     - the comparison and its outcome
   * - ``exploration``
     - the existing ``title``
     - the evaluation set it is scored on and its stage
   * - ``external_reference``
     - the existing ``title``
     - the publication and what this project takes from it
   * - ``interpro_go_mapping``
     - the release version of the mapping file
     - the pair count and the file it was read from

Decision 5: a gate, because a rule that depends on remembering produces the next gap
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``scripts/check_sets.py`` already walks the ORM metadata and reports four
criteria over the six set tables. This adds a fifth criterion over a wider
list: **a cited entity has a short descriptor column and a long one.** Same
shape, same report-first behaviour, and ``--strict`` turns it into a block when
the last migration lands.

The entity list lives in the gate, so **a new cited entity is a failure until it
is declared there**. That is the mechanism that made ``query_set`` the first set
to satisfy all four criteria, and it is the reason this record does not end with
a list of good intentions.

Acceptance
~~~~~~~~~~

Exact, and checkable against the database rather than the code:

#. every entity in the twenty-row table carries both columns, or the gate
   names it as a gap;
#. no two rows of one entity share a ``short_name``, enforced by a UNIQUE
   constraint and not by the writer's care;
#. for each entity whose backfill completed, zero rows have a NULL or empty
   descriptor;
#. the three long validation windows get three distinct short names and three
   descriptions that differ in the truth profile and the term rule, which is the
   case that motivated the record;
#. ``check_sets.py --strict`` exits non-zero while any declared entity lacks a
   column, and zero once they all have one.

What this record does not decide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Whether ``go_term`` gets the OBO definition as its long descriptor. It is
  3.118.456 rows of real data, not invented text, and the cost is the author's
  call.
* The naming vocabulary itself: whether a window's short name reads
  ``validacion-larga-proyectada`` or ``long-valid-projected``. The code and the
  documents of this project are in English and the conversation is in Spanish,
  so the author decides which side a name that appears in both belongs to.
* Whether the frontend shows the short descriptor instead of the uuid
  everywhere it shows a row today. It should, but that is a surface change with
  its own review.
