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

The homology table of a cut
---------------------------

ADR-D61. It is built from a photo that is already on the cluster, so nothing is
downloaded but the MMseqs2 binary and no data is sent.

1. **Fetch the pinned MMseqs2 on the login node.** Download
   ``mmseqs-linux-avx2.tar.gz`` of the release the record pins (``18-8cc5c``)
   and check it against the digest GitHub publishes for the asset. Unpack it and
   take the sha256 of ``mmseqs/bin/mmseqs``: the job refuses any other binary.
2. **Ship the code** with ``send_builder_code.sh`` at a merged integration
   revision. The homology builder and its job are in ``builder_files.txt``.
3. **Measure first.** Submit ``homology_build.sh`` with a SAMPLE, for example
   10,000 queries against the bank of 211. Its manifest says ``measurement`` and
   the platform never loads it. Size the full jobs from its time, memory and hits
   per query, keeping each job under two hours.
4. **Build each cut**, with the author's word for parallel jobs::

     sbatch -p cpu_partition --constraint=cpu homology_build.sh PYTHON CODE_DIR \
       PHOTO_DIR OUT REVISION MMSEQS_BIN MMSEQS_SHA256

   The job writes ``queries.fasta``, ``bank.fasta``, ``hits.parquet`` and
   ``homology.json``. Only ``hits.parquet`` is an output; the FASTA files are
   recorded by their sha256 and can be deleted.
5. **Fetch** ``hits.parquet`` and ``homology.json`` and check the output against
   the manifest before loading.

The embedding bank of the photos
--------------------------------

ADR-D62. Every member sequence of the photos already on the cluster, embedded
with one configuration per card, so nothing is downloaded but the models' weights
and no data is sent but the recipes.

1. **Build the environment on the login node**, once. A virtual environment with
   the stage-1 regime (torch ``2.11.0+cu130`` and torchvision ``0.26.0+cu130``
   from PyTorch's cu130 index, transformers ``4.48.1``, tokenizers ``0.21.4``,
   esm ``3.2.1.post1`` installed without its torchtext requirement, httpx,
   sentencepiece, pyarrow, numpy), then protea-contracts at the commit
   protea-backends pins, and protea-backends at a commit with
   ``embed_chunks_padded``. Keep the environment's ``pip freeze`` beside it.
2. **Fetch the weights on the login node**, each at a fixed revision (the one the
   development node pins, or the hub's current one, recorded). Point each
   repository's ``refs/main`` in the cache at that revision, so an offline
   ``from_pretrained(name)`` loads exactly that snapshot. The jobs set
   ``HF_HUB_OFFLINE=1``.
3. **Ship the code** with ``send_builder_code.sh`` at a merged integration
   revision, and copy ``recipes.json`` into the bank directory. The file holds
   the configurations exported from the platform, each with its id; every pass
   refuses a recipe that does not derive its id.
4. **Prepare** in one CPU job::

     sbatch embedding_bank_prepare.sh PYTHON CODE_DIR BANK_DIR 8000000 \
       PHOTO_DIR [PHOTO_DIR ...]

   It writes ``sequences.parquet`` (one row group per shard), ``shards.json`` and
   ``prepare.json``.
5. **Measure first.** One ``compare`` job per configuration on the card the bank
   will use::

     sbatch -p gpu_partition --gres=gpu:1 --constraint=b200 -c 8 --mem=64G \
       -t 00:30:00 embedding_bank.sh PYTHON CODE_DIR BANK_DIR HF_HOME REVISION \
       CONFIG_ID compare 2000 65536

   Read the gap and both speeds in ``compare-<config>-<job>.json``, and size the
   full jobs from the padded speed.
6. **Embed** each configuration over ranges of shards, one card per job::

     sbatch -p gpu_partition --gres=gpu:1 --constraint=b200 -c 8 --mem=64G \
       embedding_bank.sh PYTHON CODE_DIR BANK_DIR HF_HOME REVISION CONFIG_ID \
       embed FIRST LAST TOKEN_BUDGET

   A job stops before a shard it could not finish by its limit and exits 4.
   Submit the same range again and it resumes; done shards are skipped.
7. **Write the manifest** once every shard is there::

     sbatch -c 2 --mem=8G embedding_bank.sh PYTHON CODE_DIR BANK_DIR HF_HOME \
       REVISION CONFIG_ID manifest

   It exits 3 when refused: a shard missing or changed, a vector that is not
   finite, or shards computed under two revisions, software sets or cards.
8. **Fetch** ``bank-<config>.json`` and ``vectors/<config>/`` and run
   ``load_embedding_bank`` with the manifest's sha256. It verifies everything
   again before writing, then loads shard by shard and can be rerun.

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
