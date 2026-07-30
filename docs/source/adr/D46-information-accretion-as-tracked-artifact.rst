ADR-D46: Information Accretion is a corpus artifact, not an ontology attribute
==============================================================================

:Status: Accepted
:Date: 2026-07-30
:Author: Francisco Miguel Pérez Canales
:Phase: T-GOBERNANZA

Context: one column for a three-axis object
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Information Accretion is the term weight cafaeval and LAFA use to compute
``f_micro_w``, the headline metric of the board benchmark:

.. code-block:: text

   IA(v) = -log2( P(v | parents(v)) )
         = -log2( |proteins[v]| / |intersect proteins[p] for p in parents(v)| )

The numerator and the denominator are both protein COUNTS. IA is therefore
estimated from an annotation corpus and is not a function of the ontology
alone. It is identified by three axes:

1. the ontology snapshot (the DAG, the True Path Rule edges, the term universe),
2. the annotation set (the corpus the frequencies are counted over),
3. the evidence regime (which subset of that corpus is counted).

Until this record, PROTEA stored an IA table as ``ontology_snapshot.ia_url``: a
single nullable ``String`` column, no foreign key, no evidence field. It pinned
zero of the three. The only producer was
``scripts/compute_ia_for_snapshot.py``, an offline CLI outside the
``OperationRegistry`` which wrote ``data/benchmarks/IA_<snapshot_id>.tsv`` to
local disk and optionally set ``ia_url`` to a ``file://`` path that only the
producing machine could resolve. It accepted ``--annotation-set`` as a free
argument and had no evidence predicate at all.

The consequences were already on the record before they were measured.
``docs/IA_PROVENANCE_v227.md`` documents two legitimate v227 SwissProt IA
tables, computed on the same Sep 2025 t0, that differ by up to 14.59 with a
Pearson r of 0.982. The only guard against confusing them is
``band_registry.ia_token``, which compares the FILE BASENAME. Two tables from
different corpora both named ``IA.tsv`` are indistinguishable to it.

Measurement: the evidence regime moves the table by 8.8x
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Computed on the v226 pivot snapshot (``releases/2025-03-16``, 40214 live terms,
69188 True Path Rule edges) against the GOA v226 corpus (5907336 annotations,
of which 89.78 percent carry IEA or another non-scored code):

.. code-block:: text

   IA (all evidence)   max 18.956   mean 3.261
   IA (lafa regime)    max 15.943   mean 2.681
   IA_cafa6.tsv        max 15.880   mean 2.647

   lafa regime  vs IA_cafa6   mean abs diff 0.0918   pearson r 0.9924
   all evidence vs IA_cafa6   mean abs diff 0.8081   pearson r 0.9495

Restricting the corpus to the evidence set LAFA scores on moves the mean
absolute difference from 0.808 to 0.092 against the reference table, and brings
the summary statistics into near coincidence. An IA computed over all evidence
mostly measures how ubiquitous electronic annotation is. That is a valid
quantity and a different one, and it was the only quantity the previous
producer could express.

The arithmetic was never the problem
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``protea.core.ia`` was validated against an independent reimplementation of
LAFA's ``calc_ia`` on the real DAG and the real corpus, not only on the
synthetic 60-term fixtures of ``tests/test_ia.py``: max absolute difference
**0.0 exactly** over all 40214 terms, zero True Path Rule violations, zero
cycles, three roots at exactly 0.0, zero annotations dropped for absence from
the target snapshot.

That validation shares its propagated protein sets with the implementation, so
it gates the FORMULA and not the PROPAGATION.
``tests/test_information_accretion.py`` closes that hole with a fixpoint
relaxation oracle that builds no ancestor closure and never walks upward from
an annotation.

Decision
~~~~~~~~
1. **A new table, ``information_accretion_set``**, unique by
   ``(ontology_snapshot_id, annotation_set_id, evidence_regime)``. The first two
   are foreign keys with ``ON DELETE RESTRICT``; the third is a closed
   vocabulary from ``protea.core.ia_regimes``. The RESOLVED evidence code list
   is stored alongside the regime name, so a table keeps its meaning if a regime
   definition is later revised.

2. **A registered operation, ``compute_information_accretion``**, on
   ``protea.jobs``. It publishes the TSV through the ``ArtifactStore`` and
   records ``artifact_uri`` plus ``content_sha256``, satisfying the campaign
   rule that no artifact exists without a registered operation.

3. **The shape counters are part of the row**: ``term_count``,
   ``nonzero_count``, ``annotation_count``, ``protein_count``,
   ``propagated_pairs``, ``ia_max``, ``ia_mean``. They are not decoration. A
   table loaded under the wrong provenance is visible from its own row without
   fetching the artifact, because ``ia_max`` and ``ia_mean`` separate the
   regimes cleanly (18.956 / 3.261 versus 15.943 / 2.681 on the same snapshot
   and corpus).

4. **The default regime is ``lafa``.** The unrestricted regime has to be asked
   for by name. ``resolve_regime`` raises on an unknown name rather than
   falling back, because silently widening the corpus is the failure this
   module exists to prevent.

5. **The structural invariants are gates that raise, not warnings.**
   ``protea.core.ia.term_ia`` clamps ``num > denom`` to 0.0 rather than
   failing, so a broken propagation would otherwise surface as a plausible
   table of mostly-zero weights. Every gate was measured clean on v226 before
   being made load-bearing.

6. **``download_tsv`` gains an ``s3://`` branch** resolved through the
   ``ArtifactStore``, which is what makes a published table readable from a
   second machine. The bucket in the URI must match the configured bucket:
   reading a same-named key out of a different bucket would hand back a
   different file under the right-looking name.

7. **``run_cafa_evaluation`` gains ``information_accretion_set_id``**, mutually
   exclusive with ``ia_file``. It verifies the recorded sha256 after fetching
   and rejects a table computed on a different ontology snapshot.

Consequences
~~~~~~~~~~~~
``ontology_snapshot.ia_url`` is NOT dropped. It remains the legacy fallback in
``_resolve_ia_file`` so existing dispatches keep working, and the migration is
additive with a clean downgrade.

The band guard is unchanged and still resolves the IA by basename. A computed
``InformationAccretionSet`` always serialises to ``IA.tsv``, so a band whose
token set omits that name will reject it. This is deliberate: promoting a
computed table to authoritative for a band changes which artifact the board
numbers are weighted by, and that is a registry decision taken explicitly, not
a side effect of a resolver preferring the newer object.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Whether the recomputed v226-corpus table should replace ``IA_cafa6.tsv`` as the
authoritative v226 IA. They are different artifacts (mean absolute difference
0.092 even in the matching regime), and ``IA_cafa6.tsv`` is what the band
registry currently names. Answering it requires deciding whether v226 numbers
are meant to be comparable to the CAFA6 benchmark or to this run's own corpus.
