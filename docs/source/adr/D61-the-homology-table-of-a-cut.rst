ADR-D61: The homology table of a cut: its superset searched against its photo with a pinned MMseqs2
====================================================================================================

:Status: Proposed. ADR-D60 (decision 1) puts in the universe of each cut "one
         PLM-free homology table and the exclusion groups built from it, with a
         pinned MMseqs2" and leaves its shape open. The author approved on
         2026-11-16 the order of the rest of phase 2 (windows, information
         accretion per cut, this table, then the embeddings of the new members)
         and asked for this record ("Perfecto, dale"). The scope, the storage
         and the use of UniRef50 for the exclusion groups below are this
         record's own and wait for the author's word.
:Date: 2026-11-16
:Author: Francisco Miguel Perez Canales
:Phase: platform refactor, phase 2 (the universe from zero)
:Depends on: ADR-D59 (the photo of a cut: its members, their sequences and
             their UniRef50 clusters), ADR-D60 (one universe read by id; the
             windows and their targets), ADR-D46 (a corpus artifact referenced
             by content sha256), ADR-D58 (descriptors)

Context: three readers need the same alignments, and none of them has a source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Three parts of the plan read sequence similarity between a target and what was
known at the start of its window:

- **The evaluation's strata.** Results are reported by cell, identity and
  length, never by the flat mean alone. Identity here is the identity of a
  target to the closest protein that carried an experimental annotation at the
  start of the window.
- **The explainable family's first step.** Transfer by homology, with the
  alignment as the explanation.
- **The exclusion groups.** Fitting on a window (a combiner, a calibration)
  splits its targets into folds, and two near-identical proteins on both sides
  of a split leak.

Each of these used to compute its own similarity, with its own tool and on its
own population. The photos (ADR-D59) now fix, per cut, the population and the
sequences of the paired release, so one table per cut can serve all three.

Decision
~~~~~~~~

**1. One table per cut that opens a window.** Cuts 160, 177, 194 and 211 open
the four windows; cut 227 is the start of the instance the public instrument
will serve. Cut 205 only closes a window and gets no table.

**2. Queries and bank.**

- The queries are the superset of the cut's photo: every protein that can be a
  target of a window starting there (ADR-D60, ``superset_at_t0``). That is
  552,259 at 160 and 573,661 at 227.
- The bank is every member of the same photo: the superset, the truth and
  curated members outside it and the annotated UniRef50 neighbours. That is
  what a method may transfer from at that cut: 1,131,551 members at 160 and
  1,850,077 at 227.
- Nothing after the cut enters. The sequences are the paired release's,
  read from the photo's own outputs (``sequences.parquet`` and
  ``unreviewed_sequences.parquet``).

**3. Sequences, not accessions.** The search runs on unique sequences (md5) and
the hits are keyed by them: ``query_md5``, ``target_md5``. Identity is a
property of sequences, and several accessions share one. A reader reaches
accessions through ``cut_photo_member.sequence_md5``.

- The hit of a sequence with itself is dropped and counted.
- Two accessions with the same sequence are identical through the membership
  table. They need no hit.

**4. MMseqs2 with its defaults, pinned.**

- The version is the MMseqs2 release current when the job is written. It is
  pinned by its tag and the sha256 of its static binary, and both go into the
  manifest.
- The search uses the tool's defaults (sensitivity, e-value, maximum number of
  hits per query). The full command line is recorded.
- No threshold of ours is applied to the table. Identity bands, coverage cuts
  and e-value cuts belong to the reader that uses them: the evaluation decides
  its strata in phase 6, and the explainable family its transfer rule in
  phase 5.

**5. Columns.** ``query_md5``, ``target_md5``, ``fident``, ``alnlen``,
``qcov``, ``tcov``, ``evalue``, ``bits``. Sorted by query and then by bit
score, descending.

**6. Storage: a file in the store, declared in the database.**

- The hits are a parquet in the artifact store, referenced by URI and sha256,
  as the information accretion tables of ADR-D46 are. They are not database
  rows: nobody joins them in SQL, and tens of millions of rows per cut would
  cost the development disk more than any reader gains.
- A row of a new table, ``homology_table``, declares each one:
  - the cut's photo (a foreign key);
  - the MMseqs2 tag and binary sha256, and the command line;
  - the query, bank and hit counts, and the self hits dropped;
  - the URI and sha256;
  - the job, the loader's revision and the builder's revision;
  - the ADR-D58 descriptors (``homology-<cut>``).
- A cut has one table, as it has one photo.

**7. Built away from the server, loaded by the platform**, as the photos are:

- A file stage, ``protea.core.homology.build``, runs on the cluster. It reads
  the photo's outputs where they already are, so no data is sent. It writes
  the FASTA files, runs MMseqs2 and writes the parquet with a manifest. The
  manifest names the photo's manifest sha256, every source and the sha256 of
  every output.
- A platform operation, ``load_homology_table``, refuses before writing
  anything unless:
  - the manifest hashes to the sha256 the payload names;
  - the photo it names is the loaded one;
  - every query is a superset member and every target a member;
  - the outputs hash to what the manifest recorded.
- Code is shipped to the cluster as code only (ADR-D59).

**8. Exclusion groups are the photo's UniRef50 clusters.** The photo already
holds each member's UniRef50 cluster, from the UniRef release paired with the
cut.

- UniProt builds these clusters at 50 % identity and 80 % coverage. They are
  dated with the cut and need no computation and no threshold of ours.
- A member in no cluster is its own group (1,193 superset proteins at 211).
- The homology table stays the measure of similarity. The groups are a
  partition, which is what a fold split needs and what a table of pairs is
  not.

**9. Measured before sized.**

- A first job searches a sample of 10,000 queries against the bank of 211 and
  records time, memory and hits per query. The full jobs are sized from it.
- The rules for jobs on the cluster still apply: each job lasts 2 hours or
  less (queries split into chunks when needed), and there are no chains.
- Running cuts in parallel needs the author's word, as it did for the photos.

Consequences
~~~~~~~~~~~~

- The evaluation, the explainable family and any fold split read one table
  per cut by id, and a test can assert that the methods of an instance cite
  the same one.
- Identity strata become a reading of the table plus the window's truth at t0.
  Changing the bands changes no stored data.
- The development database grows by five small rows. The parquets live in the
  store: estimated at a few GB per cut, to be measured by the first job.
- The photos on the cluster must stay until their tables are built. The
  knowledgebase archives and the GOA files are not needed for this.
- A new MMseqs2 release does not change a stored table. A new table for the
  same cut is refused, as a second photo is, until the first is removed on the
  author's word.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

- **Search only each window's targets.** Cheaper, but each window would need
  its own search. The targets of the training windows and the proteins a
  method predicts for are the whole superset, so the table per cut serves
  every window that starts there. Rejected.
- **Store the hits as database rows.** The size is the cost and no SQL reader
  needs them. Rejected, as it was for the information accretion tables.
- **Our own clustering for the exclusion groups** (MMseqs2 linclust at an
  identity we choose). It adds a threshold nobody published and a computation.
  UniRef50 is dated, published and already in the photo. Rejected unless the
  author prefers it.
- **DIAMOND instead of MMseqs2.** It is also PLM-free and faster at scale, but
  ADR-D60 names MMseqs2 and nothing measured asks to change it. Kept as a
  fallback if the first job shows MMseqs2 cannot fit the 2-hour jobs.
- **Fix identity bands now.** It would put a reader's decision into the data.
  Rejected (decision 4).

Open for the author
~~~~~~~~~~~~~~~~~~~

1. The scope: superset against every member of the photo, neighbours included
   (decision 2).
2. UniRef50 clusters as the exclusion groups (decision 8).
3. Parallel jobs on the cluster for the five cuts, after the measured first
   job (decision 9).
