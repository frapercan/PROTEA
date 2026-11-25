ADR-D59: The corpus at a cut is a photo: its Swiss-Prot, its live tiers and their annotated UniRef50 neighbours
===============================================================================================================

:Status: Accepted (2026-11-16, the author: "Acepto!"), with that day's
         amendments to decisions 3, 4 and 5, which say what the build reads
         today, and the later one to decisions 1 and 3 (annotated UniRef50
         neighbours enter as members with all their rows, the author's "A
         oK!"). The taxon groups stay open. The requirement is the author's, stated on
         2026-11-15:
         "Dado un testsuperset, el corpus debe tener union revisadas,
         experimentales, y deberiamos enriquecer con vecinos sacados de uniref
         que aunque tengan evidencia no experimental puedan aportar algo a los
         de baja homologia [...] debemos entrenar con anotaciones de calidad,
         pero la transferencia debe producirse de la forma mas fina posible";
         "Debemos de procedimentar la seleccion, extraccion y almacenamiento de
         proteinas en cada t, se acumulan"; "Las fotos pueden y deben ir en
         picasso. Aunque debe de salir de la plataforma explicada la
         herramienta". The three member sets, the channel table and the two
         halves below are this record's own.
:Date: 2026-11-15
:Author: Francisco Miguel Perez Canales
:Phase: thesis campaign, one recipe validated at two instances (211 and 227)
:Depends on: ADR-D49 (the tiers; this record replaces its third tier at the
             cuts it builds), ADR-D40 (temporal protocol), ADR-D56 (tables keyed
             by what they depend on), ADR-D58 (descriptors)

Context: the third tier went empty at release 179, and nothing declares what a cut is asked to annotate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**The third tier of ADR-D49 is empty from release 179.** ``swissprot_of_release``
reads the entry name GOA used to write first in DB Object Synonym. Measured on
2026-10-06 over the cached releases (``_universe_sources.py``): 100% of rows carry
an entry name in 164 to 178, 0% in 179 and 180, 5% in 231. The rule now demands
positive evidence, so it fails closed: from 179 on, new proteins enter through two
tiers, not three. Proteins admitted up to 178 stay, so the loss is the reviewed
entries that arrived later. Measured at 211 against Swiss-Prot 2022_03 (568,002
entries) with this record's step-1 tool but run OUTSIDE the platform, so an order
of magnitude until a platform run declares it: 539,295 (94,9%) are in the corpus, 536,264 of
them admitted by 178, and **28,707 are not**: 9,415 whose only GOA 211 rows are
electronic or phylogenetic, 113 with only ``ND`` and 19,179 with no GOA 211 row at
all. Every reviewed entry with a truth or curated row is in.

**No cut declares the set it is asked to annotate.** In CAFA and LAFA a target set
is published at the cut and only what changes inside it is evaluated; LAFA's
targets are the accessions that are in Swiss-Prot at both ends (read from the
archived pipeline's code, 2026-10-11). The platform's evaluation populations are
built from the deltas of whatever the corpus admitted, so a protein that enters
the corpus after the cut can be scored as if it had been a target. The
laboratory's 211->227 frame has 2,645 queries without prior annotations that were
first admitted at 227. Some may have been reviewed at 211 (a real target the
corpus missed); others did not exist (not a target at all). Nothing in the store
can tell the two apart.

**The project now has two instances and four windows.** The method is validated
at 211 (read on 211->227) and tested at 227 (read on 227->230 once). It trains on
windows of about three years ending at or before the instance: 160->194,
177->205, 194->211 and, for the 227 instance only, 211->227. Five cuts (160, 177,
194, 211 and 227) therefore need the same construction, and the construction has
to be identical at every one of them, or a model trained at one cut reads a
different population at another.

Decision
~~~~~~~~

**1. A photo of cut t is three member sets, all from sources dated on or before
t.**

.. list-table:: The member sets
   :header-rows: 1
   :widths: 18 42 40

   * - Set
     - Source
     - What admits a protein
   * - ``superset``
     - The Swiss-Prot accessions of the last UniProt release published on or
       before GOA t's publication date (the date
       ``refresh_goa_release_dates`` reads from the EBI index), as
       ``load_swissprot_release`` already stores them for the 227->230 window
       (``SwissProtRelease`` and ``SwissProtMembership``).
     - Reviewed AT THIS CUT. This is the set the cut is asked to annotate, and
       the only one that can hold an evaluation target.
   * - ``truth``, ``curated_inference``
     - GOA t, unchanged from ADR-D49.
     - A measurement on this protein, or a curator's judgement about it.

A protein can enter by several sets, and every set it entered by is recorded.
``ND``-only proteins stay out for the reason ADR-D49 gave.

**A fourth set: the annotated UniRef50 neighbours** (amended 2026-11-16). A
member of a UniRef50 cluster of the paired UniRef release that holds a
``superset`` protein, in no other set, enters the photo as a protein when it
carries at least one GOA t row in a channel of ``NEIGHBOUR_CHANNELS``: PAINT's
(``phylogenetic``) or electronic orthology (``orthology_electronic``). Truth or
curated rows would have admitted it already, and a field translation on an
unreviewed entry is a rule's output (``signature_electronic``), so these two are
the recipe channels a protein outside the base can carry. Membership is UniRef's
and the channel table's, with no hand-set parameter. A neighbour is never an
evaluation target and gives no label (it has no ``truth`` row by construction);
it is a donor, with its sequence and every GOA t row it has (decision 2), so a
method can weigh it by its own similarity to the query. The author, 2026-11-16:
"mi idea original era alimentar el corpus con homologos interesantes (con sus
anotaciones, probablemente IEA)", and, asked whether to keep all of their rows or
only the recipe channels', "A oK!" (all of them).

This replaces the first draft's per-cluster profile (the author, 2026-11-15: "ok,
perfil por grupo"), which counted exactly these neighbours by (cluster, term,
channel) and lost which neighbour carried what.

A neighbour depends on TWO releases, and neither derives from the other: the GOA
cut whose rows admit it and the UniRef release whose clustering makes it a
neighbour. Both are named on the photo; naming only the cut would make two photos
built on different clusterings look like the same corpus, the defect ADR-D56
closed for windows. The clustering must be published on or before the cut, or the
groups would know sequences that did not exist at the cut. At 211 it is: UniRef
2022_03 was released on 03-Aug-2022 (its release note) and GOA 211 on 2022-09-16
(EBI index).

Measured at 211 by the photo's own code (Picasso job 3681144, 2026-11-16): the
196,353 clusters holding a reviewed protein have 38,151,372 members outside the
base, 35,304,037 of them with a GOA 211 row; 868,800 are annotated neighbours,
spread over 63,913 clusters that hold 144,177 reviewed proteins (median 7
neighbours per cluster, 99th percentile 69, at most 204). They carry 13,604,622
GOA 211 rows: 1,054,472 PAINT, 6,782,553 electronic orthology, 5,765,078
signatures and 2,519 ``ND``. The photo of 211 has 1,530,992 members.

**2. Every GOA t row of a member is kept, and its evidence channel is a declared
table, not a column.** The channel is a pure function of the row's evidence code,
its reference (``GO_REF`` or a publication) and whether the protein is reviewed at
t. Following ADR-D56 it is stored by what it depends on: a table
``evidence_channel`` keyed by those three facts, versioned by the digest every
photo records and dated by the day the GO_REF list it was checked against was
read (``evidence_channel_table``). ``assigned_by`` decides no channel, so it is
not a key; it stays in the inventory of refused keys, where it helps to place a
new one. Every ``GO_REF`` seen in GOA t must appear in the table; an unknown one
is counted and rejects the photo rather than being guessed.

Two SQL functions classify a stored row from the table, so any reader computes
the channel without Python: ``evidence_reference_class`` reads the reference
(including the empty ``GO_REF`` below) and ``evidence_channel_of`` returns the
channel, or NULL for a key the table does not list. A table is written once, by
the migration that introduced it or by the first load that classifies with it,
and a later load refuses a stored table that differs from the code's
(amended 2026-11-16, when the table was built).

GOA 160 to 194 carry three obsolete references the first table did not list,
placed by the author on 2026-11-16 from their titles in GO's list: Ensembl
Compara's projections to fungal and to plant orthologs (``GO_REF:0000049``,
62,810 rows at 160; ``GO_REF:0000035``, 11,000) are orthology, and InterPro2GO
with UniProt's conservative changes (``GO_REF:0000042``, at most 23 rows a cut) is
a signature. The table then has 92 rows and a new digest, so every photo is built
with it.

A ``GO_REF`` with no number is read through its with/from (the author,
2026-11-16: "Leer por with/from"). GOA 211 has 419,822 ``IEA`` rows by UniProt,
dated 2022-09-07 and all cellular component, that cite ``GO_REF:`` alone; their
with/from is a subcellular location (419,742) or a keyword (80), the two field
mappings whose other rows cite ``GO_REF:0000044`` and ``GO_REF:0000043``. Such a
reference takes that ``GO_REF`` when every with/from item is in one of the two
vocabularies, and stays unread, so it refuses the photo, otherwise. At 211 every
one of these rows lands in the mirror either way (the 50 member rows belong to
proteins outside Swiss-Prot), so the reading changes no recipe row.

.. list-table:: The channels
   :header-rows: 1
   :widths: 26 38 36

   * - Channel
     - What it holds
     - Use
   * - ``truth``
     - The thirteen LAFA codes.
     - Label and evidence.
   * - ``curated``
     - ``ISS ISO ISA ISM IGC RCA NAS``.
     - Evidence.
   * - ``phylogenetic``
     - PAINT: ``IBA IBD IKR IRD``; ``NOT`` rows are evidence against.
     - Evidence. Never a label: ADR-D49's reason for not admitting by it
       (a family's consensus) stands for labels.
   * - ``orthology_electronic``
     - Electronic transfers through orthology (Ensembl Compara and similar).
     - Evidence.
   * - ``reviewed_field_electronic``
     - Electronic translations of fields a curator set on a reviewed entry
       (keywords, subcellular location, EC number).
     - Evidence.
   * - ``signature_electronic``
     - Electronic annotations derived from signatures or family models
       (InterPro2GO, UniRule, ARBA and similar), and field translations on
       unreviewed entries, which come from the same rules.
     - Stored, never used by the recipe: Pfam and InterPro are a mirror only.
       One declared mirror arm measures what they would add.
   * - ``no_data``
     - ``ND``.
     - Not used.

The partition is enumerated, like ADR-D49's tiers. Whether a channel enters the
recipe is decided at query time, so changing that decision changes a filter, not
the data.

Channels are not tiers. ``IKR`` and ``IRD`` still admit a protein through
ADR-D49's curated tier, and are classed as ``phylogenetic`` here because PAINT
produces them.

**3. Storage is append-only and accumulates across cuts, and nothing the store
already holds is copied.**

- ``cut_photo``: one row per cut, with a short and a long descriptor (ADR-D58),
  the GOA annotation set of t, the ontology snapshot, the paired
  ``SwissProtRelease``, the UniRef release and the digest of its cluster table,
  the date the GO_REF list was read and the digest of the channel table, the
  manifest URI and its sha256, the job and the revisions of the loader and the
  builder. A taxonomy version joins them if taxon groups do (decision 4).
- The superset is the paired release's ``SwissProtMembership``, accession and
  sequence md5 included; it is referenced, not copied.
- The live tiers are read from the annotations, as ADR-D49 already does; no tier
  column is stored.
- ``cut_photo_member``: one row per (photo, member) with the sets it entered by
  (``superset``, ``truth``, ``curated``, ``neighbour``), its UniRef50 cluster when
  it is in one that holds a superset protein, and the md5 of its sequence in the
  paired release (decision 5), NULL when the release does not hold it. A
  neighbour enters by UniRef, which no annotation records, so who is in a photo
  is stored rather than derived; the superset flags agree with the paired
  release's ``SwissProtMembership`` because the loader refuses a photo whose
  superset is not that release. Its rows are COUNTED entities under ADR-D58,
  named by their photo. It replaces the first draft's ``cut_photo_cluster`` (the
  cluster of each superset protein) and ``cut_photo_cluster_profile`` (amended
  2026-11-16), and the profile set's descriptors on ``cut_photo`` go with them.
- ``protein``: a member that is not yet in the table enters as an accession-only
  row; ``first_admitted_release`` is written as a minimum, as ADR-D49 does.
- ``protein_go_annotation``: the GOA t rows of the new members are appended to
  the annotation set of release t. New members carry no ``truth`` and no
  ``curated`` row by construction (those would have admitted them already), so
  every evaluation set, every Information Accretion table under the ``lafa``
  regime and every donor bank restricted to those codes is unchanged. The
  neighbours' rows enter the same set, so a donor bank that reads PAINT or
  orthology rows sees them (they are what admits a neighbour).

**Reading a cut sees that cut's photo and nothing admitted after it.** A later
photo adds rows and never rewrites an earlier one.

**4. The tool has two halves, because they run in two places.**

- Step 1, the measurement, is ``protea.core.cut_photo.measure``: three passes
  (``superset``, ``neighbours``, ``gaf``), each a pure function of its source
  that writes its own record. It counts the superset by what it carries (truth,
  curated, only electronic or phylogenetic, only ``ND``, no rows), the
  neighbours the same way, and every member row by (code, reference,
  ``assigned_by``, negation): the inventory the channel table is written from.
  The Swiss-Prot pass writes the canonical table ``load_swissprot_release``
  verifies, with the same parser, now in ``protea.core.swissprot_table`` so a
  node without the database stack can import it.
- The build, ``protea.core.cut_photo.build``, is a pure function of three
  downloaded, dated sources: GOA t, the paired UniProt release (its Swiss-Prot
  archive, and its full knowledgebase archive for the TrEMBL sequences of
  decision 5) and the paired UniRef release, which reaches it through the
  measurement's records. It opens no database connection and writes parquet
  files plus a manifest that records each source's URL, publication date and
  sha256. It runs on a compute node (Picasso, in this campaign) from code
  shipped by the platform; only code is shipped, and the public sources are
  downloaded on the cluster's login node, since its compute nodes have no
  internet (runbook *Cut photo on a cluster*).
- Two inputs the first draft named as sources are not downloaded. The GO_REF
  list the channel table was checked against is the one read on the date the
  photo records (``goref_list_read_on``), next to the digest of the table it was
  classified with; a reference the table does not place refuses the photo,
  whatever that date. The NCBI taxonomy archive of t's month becomes a fourth
  source only if the author decides on taxon groups.
- ``load_cut_photo`` runs on the server. It verifies the manifest, writes the
  rows of decision 3 and declares the photo. The photo's GO graph is the newest
  loaded graph not after the GO release the GAF declares in its ``!go-version``
  line. A GAF that declares none would take the newest loaded graph not after
  the cut's date (amended 2026-11-16 with the author; the manifest names the rule
  it applied). Corrected 2026-11-23: GOA's GAF 2.1 files (cuts 160, 177 and 194)
  do declare a release, with capitalised keys (``!GO-version:``), which the first
  reading of the header matched case-sensitively and missed. Cut 160 declares
  2016-10-29, the graph ADR-D60 gives it; 177 declares 2018-04-20 and 194
  2019-11-09.

**Two kinds of photo** (amended 2026-11-16 with the author: "Si, ok"). A ``full``
photo is everything above. A ``truth_only`` photo is the photo of a cut that only
ends a window, from which a method takes what each target gained and whether it
is still in Swiss-Prot, and never what it predicts from: its members (the
superset, truth and curated inference) with all their GOA t rows and its
Swiss-Prot sequences, with no UniRef neighbours and no TrEMBL. It names no UniRef
release; ``cut_photo.kind`` records which kind a photo is, and a check holds a
full photo to naming its UniRef release and a truth-only one to naming none. The
builder takes it only when asked (``--truth-only``, exclusive with
``--neighbours``), so no photo turns truth-only by omission. Cut 205 is the one
truth-only photo of the campaign: building it full would have read 166 GB of
UniRef and 146 GB of TrEMBL nothing uses.

**The guard is in both halves.** The builder refuses a source published after
the cut and any cut after the instance mark (227); the loader refuses a manifest
that names one.

**5. Sequences come from the paired release** (the author, 2026-11-15). A
superset member takes the sequence its paired Swiss-Prot release carries, which
``SwissProtMembership`` already records by md5. A base protein outside the
superset takes its sequence from the paired release's files. A photo never uses
today's sequence (ADR-D49 decision 4 still governs the series loaded before this
record), and the drift against today's is measurable from the md5s.

The build reads them out of the paired release's full knowledgebase archive,
the only file in which UniProt publishes a past release's TrEMBL, and there only as
a flat file (``uniprot_trembl.dat.gz``; the 2022_03 archive holds the two ``.dat``
files and the docs, no FASTA): the job streams the archive's parts in order, takes
the md5 of the stream, and keeps the entries of the members outside the superset
(``unreviewed_sequences.parquet``), each sequence's md5 taken as the FASTA reader
takes it. The manifest refuses the photo unless that md5 is the
one the release's ``RELEASE.metalink`` publishes, and a member the release does
not hold is counted and named, never guessed: a GOA release can cite an
accession created after its paired UniProt release. The loader checks each
sequence against its md5 and stores it by md5, as it does the Swiss-Prot ones;
``cut_photo_member`` links each member to it by md5 (amended 2026-11-16, when the
author approved reading TrEMBL; until that day the build read the Swiss-Prot
archive alone). At 211 the members outside Swiss-Prot are 962,990: 94,190 that
GOA admits and 868,800 neighbours.

The five pairings
~~~~~~~~~~~~~~~~~

Read on 2026-11-15: GOA publication dates from the EBI index (``Last-Modified``),
UniProt dates from each release's ``relnotes.txt``, sizes and md5 from each
release's ``RELEASE.metalink``. In 2016, 2018 and 2019 the next UniProt release
came out ONE day after GOA (GOA is generated by UniProt just before its own
release), so the strict rule pairs each of those cuts with the release before;
the one-day-later release is a source published after the cut and is refused.

.. list-table::
   :header-rows: 1
   :widths: 8 14 12 14 26 26

   * - Cut
     - GOA published
     - UniProt pair
     - pair published
     - Swiss-Prot-only archive (bytes, md5)
     - UniRef archive (bytes, md5)
   * - 160
     - 2016-11-01
     - 2016_09
     - 05-Oct-2016 (2016_10 is 02-Nov-2016)
     - 1,519,611,132; 00a6141e...
     - 53,314,603,968; 079b469b...
   * - 177
     - 2018-04-24
     - 2018_03
     - 28-Mar-2018 (2018_04 is 25-Apr-2018)
     - 1,383,637,717; 0667c47f...
     - 87,113,728,578; 9285c7a1...
   * - 194
     - 2019-11-12
     - 2019_09
     - 16-Oct-2019 (2019_10 is 13-Nov-2019)
     - 1,431,339,610; da3b72a2...
     - 126,651,777,106; 7acb3e25...
   * - 211
     - 2022-09-16
     - 2022_03
     - 03-Aug-2022 (2022_04 is 12-Oct-2022)
     - 1,553,168,496; c4462ac7...
     - 189,698,335,699; d81da082...
   * - 227
     - 2025-09-04
     - 2025_03
     - 18-Jun-2025
     - 1,708,978,241; 9d0f5a80...
     - 272,876,614,176; 0af081c0...

Cut 205 is truth only (the end of 177->205): its photo is ``truth_only`` and pairs
with Swiss-Prot 2021_03 alone, for the targets still reviewed at its end (ADR-D60).
The full md5
values are the ones each metalink publishes; the builder checks the whole
digest, these are abbreviated here.

Consequences
~~~~~~~~~~~~

- **Each cut declares its target set.** An evaluation population can follow
  LAFA: the proteins that gain inside ``superset(t0)`` and are still reviewed at
  ``t1``. The laboratory's 2,645 queries first admitted at 227 are then decided
  one by one instead of as a block.
- **The five cuts become homogeneous.** Today 160 and 177 are built through three
  tiers and 194, 211 and 227 through two. With a superset per cut, all five are
  built the same way.
- **The order of the work is measurement first.** The photo of 211 is built
  before any other, and the sizes of each set and channel are reported before the
  rest are built.
- **Cost moves to the compute node.** Each cut needs its GOA file (the largest
  release is 24,28 GB, per ADR-D49), its Swiss-Prot list and its UniRef50
  membership, read once to admit the neighbours and then discarded, and the
  paired release's knowledgebase archive, read once for the TrEMBL sequences.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Distant homologs.** The neighbours are UniRef50 co-members, at least 50%
  identical to their cluster's seed. Homologs below that, in other clusters, come
  with the homology table (MMseqs2), searched against the corpus and against the
  paired release's UniRef50 representatives, a separate record.
- **Bridges.** UniRef members with no annotation at all could link a query to
  distant annotated proteins through intermediate sequences. They are not members
  under this record.
- **Whether ``phylogenetic`` stays in the recipe or joins the mirror.** It is an
  analysis decision, a filter on the channel table.
- **The evaluation option itself.** Restricting ``generate_evaluation_set`` to a
  photo's superset is a separate change.
- **Taxon groups.** Whether the photo records a coarse taxon group per member,
  and from which taxonomy, is the author's decision. Until then the photo has no
  taxonomy source (the GAF's own taxon column stays on each row).

References
~~~~~~~~~~

- ADR-D49 (tiers and the release-179 break documented in
  ``protea/core/operations/_universe_sources.py``), ADR-D40, ADR-D46, ADR-D56,
  ADR-D58
- ``protea/core/cut_photo/`` (formats and the measurement),
  ``protea/core/swissprot_table.py`` (the parser and canonical table shared with
  ``load_swissprot_release``), ``tests/test_cut_photo_formats.py``,
  ``tests/test_cut_photo_measure.py``
- ``protea/core/cut_photo/build.py`` and ``protea/core/cut_photo/channels.py``
  (the build and the channel table), ``protea/core/operations/load_cut_photo.py``
  (the loader), ``deploy/cluster/`` and the runbook *Cut photo on a cluster*
- The laboratory card ``compresion/fichas/fotos-cinco-cortes.md`` (sf-jepa), which
  fixes the parity checks before any photo is built
