ADR-D49: The corpus is four tiers of the GAF series, built in two operations
===========================================================================

:Status: Accepted
:Date: 2026-10-06
:Author: Francisco Miguel Pérez Canales
:Phase: thesis campaign, clean run (corpus construction)

Context: the scope of the corpus was never declared
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``protein_go_annotation.protein_accession`` is a foreign key, so
``load_goa_annotations`` can only store an annotation whose accession is already
in ``protein``, and it skips the rest in silence. For the whole clean campaign
that filter meant *reviewed only*, a scope that existed nowhere as a decision.
It was a ``search_criteria: "reviewed:true"`` inside one ``insert_proteins``
payload submitted on 2026-09-15, and the count of how many annotations it dropped
lived in a job event nobody read.

Measured on 2026-10-05 against UniProtKB with the thirteen LAFA evidence codes:
**93.526 reviewed against 149.774 in total**. Roughly 56.000 proteins carrying
curated experimental labels never reached the corpus.

Two further facts made a present-day query the wrong definition regardless of its
filter:

- **A query describes today.** The campaign spans 2016 to 2026. A protein that
  held experimental evidence in 2018 and lost it, or left UniProt entirely, does
  not appear in today's answer, yet it took part in the deltas the model learns
  from. Measured over three releases (160, 194, 235), the union of accessions
  with reliable evidence is **196.164**, which is **46.390 more** than today's
  query returns, and the curve was still climbing: even that is a floor.
- **Today's ``reviewed`` is not 2016's.** Of today's Swiss-Prot, **24.854
  entries (4,3%)** were not in Swiss-Prot in 2016. Filtering a temporal window by
  a present-day review status injects them into the past.

The first attempt at a criterion was a complement (*anything that is not IEA*),
recorded in the driver as ``reliable``. It admitted two families nobody had
decided to admit, and the measurements are what settled them:

- ``IBA`` / ``IBD`` come from the PAINT phylogenetic pipeline: a curator
  annotates an ancestral or descendant node and the annotation propagates
  mechanically. **58%** of the corpus entered by ``IBA`` alone, and its share of
  curated annotation rose from 52% on GOA 156 to 83% on GOA 231. There is a
  person, but not one looking at this protein, and the label is by construction
  its family's consensus, which is the quantity a nearest-neighbour method is
  supposed to be evaluated against rather than trained on.
- ``ND`` records that a curator looked and found NOTHING. Its rows sit on the
  three ontology root terms, and IA(v) = -log2 P(v | parents(v)) makes a root's
  Information Accretion **zero by construction**. Measured on GOA 156: **83.950**
  accessions carry only ``ND``, and 83.949 of them have every non-IEA row on a
  root term. An ND-only donor contributes nothing to an IA-weighted metric *and*
  occupies a slot among the k neighbours. Not inert: harmful.

Decision
~~~~~~~~

**1. Admission is four named tiers, enumerated, never a complement.** The tiers
are declared per job in ``extract_goa_universe``'s ``admit`` field, so the scope
that defines the corpus sits on the job row and is queryable after the fact.

.. list-table:: The tiers, and what each one is
   :header-rows: 1
   :widths: 22 30 48

   * - Tier
     - Codes
     - What admits a protein
   * - ``truth``
     - the thirteen LAFA codes: the eleven GO experimental codes plus ``IC`` and
       ``TAS``
     - A measurement on THIS protein. The only tier that makes a protein an
       evaluation target, and the set is identical to
       ``protea.core.ia_regimes.LAFA_EVIDENCE`` deliberately: admission criterion
       and truth criterion are one set. Measured on GOA 156: **117.136**
       accessions (69.927 Swiss-Prot, 47.209 TrEMBL).
   * - ``curated_inference``
     - ``ISS ISO ISA ISM IGC RCA NAS IKR IRD``
     - A curator's judgement about THIS protein, from sequence similarity,
       orthology, a sequence model, genomic context or a reviewed computational
       analysis. Never truth. Measured on GOA 156: **62.363** proteins enter by
       these and by nothing else.
   * - ``swissprot_of_release``
     - none; read from the entry name
     - The entry was reviewed AT THIS RELEASE. Measured on GOA 156: **527.149**
       accessions.
   * - *excluded*
     - ``IEA``, ``IBA``, ``IBD``, ``ND``
     - Each for its own reason, and the reasons are not interchangeable: no
       person, a person looking at a family, a person recording absence.

The partition is **exact**: 13 + 9 + 4 = 26, which is every code the ECO mapping
knows. Nothing is unclassified, so an unknown evidence code is a code GO added
after this was written: it is **counted and rejected**, reported under
``codigos_desconocidos``, and needs a decision rather than a default. The
complement would have admitted it in silence.

``ISS`` and its family are IN, and not as a concession to corpus size. They are
the direct, independent probe of the question the campaign asks: they are
curator-reviewed ALIGNMENT inferences, so comparing an embedding-neighbourhood
prediction against them tests whether an embedding neighbourhood captures what
curated alignment captures, without touching the scored truth, which is
``truth`` alone.

**2. Swiss-Prot membership per release is read from the GAF itself.** UniProt
names a TrEMBL entry ``<accession>_<ORGANISM>`` and a Swiss-Prot entry
``<mnemonic>_<ORGANISM>``, and the GAF's DB Object Synonym column carries the
entry name **of its own release**. So ``P12345_HUMAN`` is TrEMBL and
``HLA_A_HUMAN`` is reviewed, and the two forms are disjoint by construction: a
false positive would require a TrEMBL entry whose mnemonic is not its accession,
which UniProt does not produce.

Measured on GOA 156 (2016-11-01): the rule names **527.149** accessions as
reviewed, against **551.705** entries in the Swiss-Prot 2016_07 tarball, a
shortfall of 24.556 (**-4,45%**) accounted for by entries with no annotation row
in that release. The shortfall is in the safe direction, since membership is
never invented, and the alternative was a 40 GB download of historical
Swiss-Prot per release plus a release-pairing problem, for a column the file
already carries.

**3. The series is walked ascending, and every protein records the release that
admitted it.** 75 files, releases 156 to 235 (206 to 210 absent), **802,1 GB**
measured with HEAD; the largest single release is 202 at 24,28 GB. Ascending is
not a preference: it is what makes *the first release that admitted this protein*
observable, and that is the knowledge-gain event the campaign measures.

``protein.first_admitted_release`` (migration ``f2a8c41d9e37``) stores it. It is a
different quantity from the ``first_release`` that migration ``e4b7a19c0f83``
deliberately refused: that one meant *the earliest release that existed once this
entry existed*, is derivable from ``date_created`` against
``annotation_set.source_published_at``, and remains unstored. This one is not
derivable at all for a protein admitted only as a reviewed entry of its release,
because per-release Swiss-Prot membership lives in no table. It is written as a
minimum (``IS NULL OR > N``), so the column stays true if the series is ever
processed out of order.

**4. Sequences, metadata and audit dates come from today, once, at the end.**
``protein.sequence_id`` is nullable, so an accession can be a corpus member before
it has a chain. The GAF passes therefore insert accession-only rows, and one
``resolve_protein_sequences`` job fetches every sequence afterwards over the union
of the 75 releases.

This is the asymmetry that justifies it: **annotations are historical, sequences
are not.** An annotation belongs to its release and must be loaded release by
release. A sequence is a property of the protein, and today's UniProt is the only
place to get it. Doing it per release asked UniProt the same question up to 75
times and asked repeatedly about accessions it does not serve at all: **34% of the
requests** across the ten passes that ran that way.

Annotations of a protein whose sequence never arrives are **kept**. The protein
took part in the deltas; every query that needs a chain filters on
``sequence_id IS NOT NULL``. What cannot be rescued is now a list of names
(``sin_resolver.txt`` in the artifact store) rather than a count: of 25 sampled,
25 are ``entryType: "Inactive"`` with no sequence.

**5. Two operations, because they are two different jobs.**
``ensure_goa_universe`` is replaced by:

- ``extract_goa_universe``: one GAF, one pass, no network beyond the file.
  Admissible accessions in, accession-only ``protein`` rows out, plus
  ``first_admitted_release``. A pure function of a cached file, which is what
  makes it safe to re-run.
- ``resolve_protein_sequences``: one pass over *every row with no sequence*,
  through the plugin's one-shot fetches. Three passes internally, with three
  different predicates: sequences (``sequence_id IS NULL``), merged accessions
  (still ``NULL`` after the first), audit dates (``date_created IS NULL``, which
  is a different and much larger population: roughly 575.000 of some 680.000
  rows, every one of them loaded by ``insert_proteins`` with a sequence and no
  dates).

The shape matches what 8 of the platform's 9 networked operations already do: one
declared source in the payload, one pass, transport in a ``protea_sources``
plugin. ``protea/core/operations/_universe_http.py``, 115 lines duplicating the
plugin's retry logic down to the same ``{429, 500, 502, 503, 504}`` set and the
same ``Retry-After`` handling, is deleted, and the plugin gained
``fetch_accessions_tsv`` and ``search_secondary_accessions`` instead
(protea-sources#38). The ``multistage.py`` Coordinator contract was considered and
rejected: this is two sequential independent operations, not a coordinator
fan-out.

The private coupling goes with it. ``ensure_goa_universe`` called
``InsertProteinsOperation._store_records``, a private method of another operation,
with a test pinning its 4-tuple return because a change would otherwise have
failed hours into a load. The MD5 dedup and the protein upsert now live in
``protea/core/operations/_protein_store.py``, imported by both, returning a named
``StoreCounts``.

Why the evidence predicate reads raw columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Of 280.922.738 lines in GOA 156, **671.138 carry one of the thirteen (0,24%)**.
The GOA plugin accepts an ``accept`` callback over the raw split line
(protea-sources#35), so the evidence test runs before a record is built: **4,4
minutes a release against 11,5**. The GAF column indices therefore appear in
``extract_goa_universe`` as named constants, and two tests pin them against the
plugin's own parse rather than against its private names.

Consequences
~~~~~~~~~~~~

- **The corpus grows by roughly an order of magnitude** over the 93.526 the old
  filter admitted, and its composition is recoverable after the fact: a protein's
  admitting tier is readable from its annotations (``truth`` and
  ``curated_inference`` by evidence code), and a ``swissprot_of_release``-only
  member is the one with no reliable annotation at all. No tier column is stored
  for that reason.
- **The donor bank is not fixed by this decision.**
  ``donor_policy.evidence_codes`` is an arbitrary list filtered in the bank query,
  so which tiers donate is chosen at query time. **Embedding cost is not
  recoverable that way**: every admitted protein with a sequence is embedded once,
  so the tier decision is a cost decision even where it is not an analysis
  decision.
- **Information Accretion is invariant to tiers 2 and 3.**
  ``compute_information_accretion``'s ``evidence_regime`` defaults to ``lafa``, so
  the IA table is computed over the thirteen codes whatever the corpus admits, and
  ``information_accretion_set_id`` remains one of the six fields of the frame
  seal.
- **A release pass is now re-runnable and offline.** The whole series can be
  re-extracted from cached GAFs with no network, and ``resolve_protein_sequences``
  re-run costs only the deleted accessions it cannot resolve.
- **The 802 GB series does not fit alongside the database.** Free space measured
  at 661,9 GB, so the GAFs are fetched, used and discarded one at a time by the
  driver; no filtered cache can be built during phase 1, because the universe is
  not known until phase 1 ends.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **The existing ``protein`` rows.** The 108.532 proteins in the table on
  2026-10-06 were admitted by one release-156 pass under the ``reliable``
  criterion, which admitted ``IBA`` and ``ND``. They are a superset in the wrong
  direction and the table has to be truncated before the first ascending pass, or
  the declared criterion and the corpus will not match.
- **The holdout guard.** Loading all 75 releases dissolves the physical
  protection that "GOA 230 is not in the database" provided. ADR-D40's window
  roles are the logical protection; a guard that refuses to read a TEST-side
  annotation set does not exist yet.
- **The split registry.** ``RELEASES`` in ``split_registry.py`` does not list
  these releases, so TRAIN cannot be named until it does.
- **The embedding configs.** ``embedding_config`` is empty after the restart; the
  eight ADR-D48 recipes have to be re-declared before any embedding is computed.
- **Whether a demerged accession has a successor at all.** A demerge points at
  several entries, and choosing one is a curatorial decision no operation can
  take. They are recorded with their destinations in ``demerges.tsv`` and left
  unresolved.

References
~~~~~~~~~~

- Operations ``protea/core/operations/extract_goa_universe.py`` and
  ``protea/core/operations/resolve_protein_sequences.py``; shared format and
  criterion logic in ``_universe_sources.py``, shared storage in
  ``_protein_store.py``
- Migration ``alembic/versions/f2a8c41d9e37_protein_records_the_release_that_admitted_it.py``
- ``tests/test_extract_goa_universe.py``, ``tests/test_resolve_protein_sequences.py``,
  ``tests/test_protein_store.py``
- protea-sources#35 (raw-line ``accept``), protea-sources#38 (the two one-shot
  UniProt fetches)
- ADR-D40 (temporal protocol), ADR-D46 (IA as a corpus artifact), ADR-D48
  (stage-1 configs, whose "What this record does not settle" named this rebuild)
- ``agent-farm/plans/clean-campaign/MARCO-DECLARADO.md`` for the run-level
  declaration and the measurements as they were taken
