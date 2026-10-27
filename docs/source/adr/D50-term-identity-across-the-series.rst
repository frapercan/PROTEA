ADR-D50: GO term identity has to survive the series, so alt_id becomes data
===========================================================================

:Status: Proposed
:Date: 2026-10-06
:Author: Francisco Miguel Pérez Canales
:Phase: thesis campaign, clean run (corpus construction)

Context: a renamed term reads as a retraction plus a gain
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GO merges terms continuously. When it does, the absorbed id survives in the OBO as
an ``alt_id`` of its successor, and nothing is lost upstream: the mapping is
published and permanent.

``load_ontology_snapshot`` does not read ``alt_id`` lines. Nothing in this database
records them. The consequence is not a dropped row at load time, which was
measured and is zero, but a **broken identity across releases**, which is measured
and is not.

**Measured 2026-10-06**, comparing the OBO paired with GOA 156 (2016-06-01) against
the one paired with GOA 231 (2026-03-25), the two ends of the campaign window:

.. list-table::
   :header-rows: 1
   :widths: 60 40

   * - Quantity
     - Measured
   * - primary terms in the 2016 OBO
     - 44.797
   * - primary terms in the 2026 OBO
     - 48.291
   * - ``alt_id`` declared in the 2026 OBO
     - 3.646
   * - 2016 primary terms that are no longer primary in 2026
     - **1.657**
   * - of those, merged (an ``alt_id`` of another term in 2026)
     - **1.657, all of them**
   * - of those, gone with no successor
     - **0**

Every single term that stopped being primary across the window was merged, and its
successor is known. Examples: ``GO:0000040`` to ``GO:0034755``, ``GO:0000042`` to
``GO:0034067``, ``GO:0000066`` to ``GO:1990575``.

So 1.657 terms, 3,7% of the 2016 vocabulary, change identity during the window
while meaning the same thing. Any comparison between an early annotation set and a
late one reads each of them as **one retraction and one gain**.

Why the existing reconciliation does not cover it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``generate_evaluation_set`` already supports cross-OBO reconciliation through a
pivot snapshot, and ``protea/core/evaluation.py`` implements it as an
intersection::

    in_pivot = closure & pivot_go_ids

A term the pivot no longer carries as primary simply **falls out of the
intersection**. For scoring that is the conservative and correct thing: a term the
pivot does not know cannot be scored. For following a protein through the series it
is a silent break, because the row is dropped rather than mapped to the successor
the OBO names.

What this costs, where
~~~~~~~~~~~~~~~~~~~~~~~

- **The delta the campaign measures.** A protein annotated with ``GO:0000040`` in
  2016 and ``GO:0034755`` in 2026 appears to have lost one term and gained
  another. ADR-D40's window roles do not help: both sides are correct about their
  own release.
- **The churn record.** ``archive/goa-apparent-delta-hides-four-causes`` already
  establishes that only half of what looks "removed" over ten years is retraction.
  Term merges are a candidate for a measurable part of the other half, and this is
  the first time the quantity has a number.
- **Dataset generation.** A training pair built on a term that was renamed between
  the two snapshots it spans is a pair about a renaming.
- **Nothing at load time.** Measured over 2,7 million sampled rows of releases 221
  and 231 against their paired OBOs: zero rows cite an ``alt_id`` and zero cite a
  term absent from the OBO. GOA normalises to primary ids against its own GO
  release, so no row is currently dropped for this. The instrumentation that would
  catch it if the old end of the series behaves differently landed separately, as
  ``skipped_go_term_unknown``.

Decision (proposed)
~~~~~~~~~~~~~~~~~~~

**1. Record alt_id per snapshot.** ``load_ontology_snapshot`` already parses the
OBO stanza by stanza and sees the ``alt_id:`` lines; it discards them. It should
persist them as an association from the absorbed id to the ``GOTerm`` row of its
successor, within the same snapshot. Volume: 3.646 per snapshot over some 64
distinct snapshots, about 230.000 rows. Negligible.

**2. Canonicalise before comparing, never instead of storing.** The stored
annotation keeps the id its release used: ``annotation_set(156)`` must remain a
faithful record of GOA 156, and rewriting ids at load time would destroy the thing
the series exists to preserve. Canonicalisation happens at **read** time, against
a declared pivot.

**3. Map, do not drop, in the pivot intersection.** Where ``evaluation.py``
currently intersects with the pivot's primary terms, an id that the pivot carries
as an ``alt_id`` resolves to its successor first. A term with no successor in the
pivot still drops, which is the current behaviour and stays.

**4. It is a frame change, so it is declared.** This alters measured numbers: the
same prediction set scored before and after will give different Fmax, because the
ground truth gains the mapped rows. It therefore belongs in the frame seal's
provenance, and every number produced under the old behaviour has to be labelled
as such rather than silently superseded.

Consequences
~~~~~~~~~~~~

- Cross-release deltas stop counting 1.657 renamings as biology.
- The ``skipped_go_term_unknown`` counter gains a second use: after this lands it
  should stay at zero for a different reason, and a non-zero value then means a
  term with no successor at all.
- One more axis the frame has to name, and one more thing that must be identical
  between two numbers for them to be comparable.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Where the pivot comes from.** The evaluation takes it as a payload field. A
  campaign-wide canonical pivot would make every analysis comparable by default,
  and would be one more thing to declare.
- **Obsolete terms that are not merges.** An obsoleted term with
  ``consider``/``replaced_by`` rather than an ``alt_id`` relationship is a
  different case, is not covered by this measurement, and is not addressed here.
- **Whether to backfill.** The snapshots are created during phase 2. If this lands
  after them, the OBOs are archived by ``archive_ontology_snapshot`` and the
  mapping can be rebuilt from the archive without re-downloading anything. So it
  is not on phase 2's critical path.
- **The size of the effect on the actual window.** 1.657 terms is the vocabulary
  count, not the annotation-row count. How many rows of THIS corpus sit on a
  renamed term is measurable only once the annotation sets exist.

References
~~~~~~~~~~

- ``protea/core/operations/load_ontology_snapshot.py`` (parses and discards
  ``alt_id``), ``protea/core/evaluation.py`` (the pivot intersection)
- ADR-D40 (temporal protocol), ADR-D47 (the archived OBO this can be rebuilt
  from), ADR-D49 (the corpus the comparison runs over)
- ``protea/core/operations/_goa_load_report.py`` for the counter that would make a
  regression visible
