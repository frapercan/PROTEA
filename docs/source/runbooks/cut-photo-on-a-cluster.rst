Cut photo on a cluster
======================

How the photo of a cut (ADR-D59) is measured and built on a compute cluster and
loaded into PROTEA. The cluster runs file stages only: it gets code, downloads the
public sources itself, writes files with a manifest, and never opens a connection to
the platform. The platform fetches the files and ``load_cut_photo`` verifies them
before writing a row.

The scripts live in ``deploy/cluster/``; the tools they run are
``protea.core.cut_photo.measure`` and ``protea.core.cut_photo.build``.

What runs where
---------------

.. list-table::
   :header-rows: 1
   :widths: 24 28 48

   * - Step
     - Where
     - Script
   * - Ship the code
     - the server (the repository)
     - ``deploy/cluster/send_builder_code.sh``
   * - Fetch the sources
     - the cluster's login node
     - ``deploy/cluster/fetch_source.sh``
   * - Measure
     - one compute job
     - ``deploy/cluster/cut_photo_measure.sh``
   * - Build
     - one compute job
     - ``deploy/cluster/cut_photo_build.sh``
   * - Load
     - the server, as a job
     - ``load_cut_photo`` on ``protea.jobs``

Rules the scripts assume
------------------------

- **Compute nodes have no internet.** Every source is downloaded on the login node
  first and read from the shared scratch.
- **Only code is shipped.** ``send_builder_code.sh`` sends the files listed in
  ``deploy/cluster/builder_files.txt`` from a commit, never the working tree: the
  two job scripts and the import closure of the two tools, and
  ``tests/test_cluster_builder_files.py`` fails when that list stops being the
  closure. The closure needs the standard library and
  pyarrow only; the cluster provides a Python with pyarrow, passed as the first
  argument of each job.
- **Arguments are positional.** Some clusters cancel jobs submitted with
  ``--export``; every script takes its inputs in order.
- **Jobs stay within two hours.** Longer jobs fall in a slower queue; on Picasso a
  nine-hour job was placed two and a half days ahead while a two-hour one started at
  once. Pass the partition on the command line (Picasso: ``-p cpu_partition
  --constraint=cpu``).
- **The mark is checked twice.** The scripts refuse a GOA release after 227 and a
  UniProt release from 2025_04 on before touching a byte, and the tools refuse them
  again (``protea.core.source_guards``).

Before you start
----------------

Take the cut's pairing from ADR-D59 ("The five pairings"): the GOA release and its
publication date (the cut's date), the paired UniProt release and its date, and the
sizes and md5 of its Swiss-Prot and UniRef archives. The release's TrEMBL is published
only inside its full knowledgebase archive (``knowledgebase<release>.tar.gz``), as the
flat file ``uniprot_trembl.dat.gz``; take
that archive's size and md5 from the same ``RELEASE.metalink``. Take the GO graph from the rule
the loader enforces: the newest loaded graph not after the GO release the GAF's
``!go-version`` line declares, or, for a GAF 2.1 that declares none (GOA up to
2019), the newest loaded graph not after the cut's date. Use a revision merged on the integration branch.

Steps
-----

1. **Ship the code** (on the server)::

      deploy/cluster/send_builder_code.sh <revision> <ssh-host> <code-root>

   The files land in ``<code-root>/<short revision>``: that directory is the
   ``CODE_DIR`` of both jobs, and its ``deploy/cluster/`` holds the job scripts of
   the same revision, which is where steps 3 and 4 submit them from.
   ``fetch_source.sh`` runs on the login node and is copied there by hand.

2. **Fetch the sources** (on the login node, each with ``nohup``)::

      fetch_source.sh <swiss-prot url> <src>/uniprot_sprot-only<release>.tar.gz <bytes> 4 <md5>
      fetch_source.sh <uniref url> <src>/uniref<release>.tar.gz <bytes> 16 <md5> nojoin
      fetch_source.sh <goa url> <src>/goa_uniprot_all.gaf.<cut>.gz <bytes> 8
      fetch_source.sh <knowledgebase url> <src>/knowledgebase<release>.tar.gz <bytes> 16 <md5> nojoin

   The last line of each log gives the file's sha256. Record the GOA one: the build
   job refuses a GOA file that does not hash to it. The UniRef and knowledgebase
   archives stay in parts and are checked by the md5 of their stream by the job that
   reads them.

3. **Measure** (one job)::

      sbatch [partition] <code-dir>/deploy/cluster/cut_photo_measure.sh <python> <code-dir> <cut> <cut-date> \
        <uniprot-release> <uniprot-date> <src>/uniprot_sprot-only<release>.tar.gz \
        <src>/uniref<release>.tar.gz <uniref-md5> <measured> <revision>

   It writes ``superset.tsv`` and ``neighbours.parquet`` with their records into
   ``<measured>``. When it ends with ``CUT_PHOTO_MEASURED``, the UniRef parts can go.

4. **Build** (one job)::

      sbatch [partition] <code-dir>/deploy/cluster/cut_photo_build.sh <python> <code-dir> <cut> <cut-date> \
        <uniprot-release> <uniprot-date> <src> <measured> <photo> <revision> \
        <goa-url> <goa-sha256> <swiss-prot-url> \
        <src>/knowledgebase<release>.tar.gz <knowledgebase-md5> <knowledgebase-url>

   After the Swiss-Prot sequences, the job runs the rows pass and the TrEMBL pass side
   by side (both read only the members). The TrEMBL pass streams the knowledgebase
   parts in order, takes ``uniprot_trembl.dat.gz`` out of the archive (a past
   release publishes TrEMBL only as this flat file) and keeps the entries of the
   members outside Swiss-Prot (``unreviewed_sequences.parquet``); it writes the md5
   of that stream beside the photo, and the manifest refuses the photo unless it is
   the published one. The log
   ends with ``manifest exit 0`` (complete) or ``3`` (refused) and the sha256 of
   ``photo.json``.

   A cut that only ends a window (205, the end of 177->205) is built **truth-only**
   instead, in one job that also measures its superset and reads neither UniRef nor
   TrEMBL::

      sbatch [partition] cut_photo_truth.sh <python> <code-dir> <cut> <cut-date> \
        <uniprot-release> <uniprot-date> <src> <measured> <photo> <revision> \
        <goa-url> <goa-sha256> <swiss-prot-url>

5. **Read a refusal.** A photo is refused when an evidence key is not in the
   channel table, an output is missing, or two passes disagree about a source
   (``photo.json``, ``refusals``). ``unknown_evidence.tsv`` lists each unplaced key
   with its row count and up to three raw references. Placing a key is the author's
   decision and goes into ADR-D59. At 211 the only key was ``IEA`` with the
   reference ``GO_REF:`` and no number: 419,822 rows of GOA 211, all from UniProt on
   2022-09-07, whose with/from named the subcellular location or keyword mapping;
   the author decided to read such a reference through its with/from.

6. **Bring the photo to the server** and check every output against the manifest
   (``scp`` from the login node into a directory the workers read, then compare the
   sha256 of each file with ``photo.json``'s ``outputs``).

7. **Load** it with ``POST /jobs``::

      {"operation": "load_cut_photo", "queue_name": "protea.jobs",
       "payload": {"photo_dir": "<server dir>", "manifest_sha256": "<sha256 of photo.json>",
                   "ontology_snapshot_id": "<the cut's graph>"}}

   The job verifies everything again before writing (see ``load_cut_photo`` in the
   operations list) and emits a ``load_cut_photo.stage_done`` event per stage, so a
   long load is never taken for a stalled one.

8. **Clean up.** Delete the UniRef parts after the measure job, the knowledgebase
   parts after the build, and the GOA file once the photo is loaded. Keep
   ``photo.json`` with the photo's files until the load succeeds.

The run at 211, measured
------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - What
     - Measured
   * - GOA 211 (16,551,869,595 bytes)
     - fetched in 952 s in 8 parts; sha256 ``b5c7eb18...``
   * - Swiss-Prot 2022_03 (1,553,168,496 bytes)
     - fetched in 19 s, md5 checked
   * - UniRef 2022_03 (189,698,335,699 bytes)
     - fetched in 829 s in 16 parts, never joined
   * - Knowledgebase 2022_03, with TrEMBL (157,495,106,389 bytes)
     - fetched in 1,407 s in 16 parts, never joined
   * - Build job, before neighbours were members (members, rows, sequences, manifest)
     - 57 minutes on 8 cores (members 15, rows 40); peak memory 8.7 GB
   * - That photo
     - 172 MB: 662,192 members (568,002 in Swiss-Prot), 9,995,432 member rows,
       566,809 clustered superset proteins
   * - Its load in the dev database
     - 34 minutes: 9,498,933 annotation rows (the GAF lines that differ only in
       with/from, assigned_by or date are one row), every one given by SQL the
       channel the builder gave it
   * - The annotated neighbours (one job, 30 minutes, 11.5 GB)
     - 868,800 of the 38,151,372 cluster members outside the base, with 13,604,622
       GOA 211 rows; with them the photo has 1,530,992 members
