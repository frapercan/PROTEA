ADR-D59: The corpus at a cut is a photo: its Swiss-Prot, its live tiers and their UniRef50 neighbours
======================================================================================================

:Status: Proposed. The requirement is the author's, stated on 2026-11-15:
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
positive evidence, so it fails closed: at 194, 211 and 227 the corpus is admitted
through two tiers, not three, and **a reviewed protein whose only annotations are
electronic or phylogenetic is not in the corpus at those cuts.** The corpus the
campaign describes as *reviewed plus experimental* is, after 178, *experimental
plus curated inference*.

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
       ``refresh_goa_release_dates`` reads from the EBI index).
     - Reviewed AT THIS CUT. This is the set the cut is asked to annotate, and
       the only one that can hold an evaluation target.
   * - ``truth``, ``curated_inference``
     - GOA t, unchanged from ADR-D49.
     - A measurement on this protein, or a curator's judgement about it.
   * - ``neighbour``
     - The UniRef50 release paired with t the same way.
     - A member of the UniRef50 cluster of some ``superset`` protein, with at
       least one row in GOA t other than ``ND``. No hand-set parameter:
       membership is UniRef's.

A protein can enter by several sets, and every set it entered by is recorded.
``ND``-only proteins stay out for the reason ADR-D49 gave.

**2. Every GOA t row of a member is kept, and its evidence channel is a declared
table, not a column.** The channel is a pure function of the row's evidence code,
its reference (``GO_REF`` or a publication), ``assigned_by`` and whether the
protein is reviewed at t. Following ADR-D56 it is stored by what it depends on: a
table ``evidence_channel`` keyed by those four fields, versioned with the GO_REF
list it was checked against. Every ``GO_REF`` seen in GOA t must appear in the
table; an unknown one is counted and rejects the photo rather than being guessed.

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

**3. Storage is append-only and accumulates across cuts.**

- ``cut_photo``: one row per cut, with a short and a long descriptor (ADR-D58),
  the GOA annotation set of t, the paired UniProt, UniRef, taxonomy and GO_REF
  versions, the manifest URI and its sha256, the job and the code revision.
- ``cut_photo_member``: one row per (photo, accession, set), with whether the
  entry is reviewed at t, its NCBI taxon, its UniRef50 cluster and the md5 of its
  sequence in the paired release.
- ``protein``: a member that is not yet in the table enters as an accession-only
  row; ``first_admitted_release`` is written as a minimum, as ADR-D49 does.
- ``protein_go_annotation``: the GOA t rows of the new members are appended to
  the annotation set of release t. New members carry no ``truth`` and no
  ``curated`` row by construction (those would have admitted them already), so
  every evaluation set, every Information Accretion table under the ``lafa``
  regime and every donor bank restricted to those codes is unchanged.

**Reading a cut sees that cut's photo and nothing admitted after it.** A later
photo adds rows and never rewrites an earlier one.

**4. The tool has two halves, because they run in two places.**

- ``build_cut_photo`` is a pure function of five downloaded, dated sources: GOA
  t, the paired UniProt release, the paired UniRef release, the NCBI taxonomy
  archive of t's month and the GO_REF list as of t. It opens no database
  connection and writes parquet files plus a manifest that records each
  source's URL, publication date and sha256. It runs on a compute node (Picasso,
  in this campaign) from code shipped by the platform; only code is shipped, and
  the public sources are downloaded on the node.
- ``load_cut_photo`` runs on the server. It verifies the manifest, writes the
  rows of decision 3 and declares the photo.

**The guard is in both halves.** The builder refuses a source published after
the cut and any cut after the instance mark (227); the loader refuses a manifest
that names one.

**5. Sequences follow ADR-D49 decision 4, with one exception.** A member UniProt
still serves gets today's sequence through ``resolve_protein_sequences``; the md5
of its sequence in the paired release is recorded on its member row, so drift is
measurable instead of assumed. A member UniProt no longer serves, which is common
for neighbours of older releases, takes the sequence of the paired release and
says so.

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
  release is 24,28 GB, per ADR-D49), its Swiss-Prot list, its UniRef50 membership
  and, for neighbours UniProt no longer serves, their release sequences.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **The size of the neighbour set.** If it is too large, a restriction (for
  example, neighbours with at least one row in a channel the recipe uses) is the
  author's decision, taken on the numbers of 211.
- **Bridges.** UniRef members with no annotation at all could link a query to
  distant annotated proteins through intermediate sequences. They are not members
  under this record.
- **Whether ``phylogenetic`` stays in the recipe or joins the mirror.** It is an
  analysis decision, a filter on the channel table.
- **The evaluation option itself.** Restricting ``generate_evaluation_set`` to a
  photo's superset is a separate change.

References
~~~~~~~~~~

- ADR-D49 (tiers and the release-179 break documented in
  ``protea/core/operations/_universe_sources.py``), ADR-D40, ADR-D46, ADR-D56,
  ADR-D58
- The laboratory card ``compresion/fichas/fotos-cinco-cortes.md`` (sf-jepa), which
  fixes the parity checks before any photo is built
