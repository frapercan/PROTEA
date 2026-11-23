ADR-D62: The embedding bank of the photos: every member sequence, eight models, padded batches on the cluster
=============================================================================================================

:Status: Accepted (2026-11-17). The author chose the cluster file stage for the
         embeddings ("d me parece bien"), all of it from the start ("todo desde
         el principio": every member sequence of the six photos with the eight
         stage-1 models), and the batch: "Queremos eficiencia, no malgastar
         recursos computacionales, las diferencias numericas no nos importan
         siempre y cuando sean mínimas." The measured gap of decision 3 is
         recorded below once the pilot has run.
:Date: 2026-11-17
:Author: Francisco Miguel Perez Canales
:Phase: platform refactor, phase 4 (representations)
:Depends on: ADR-D48 (the eight stage-1 configurations and their software
             regime), ADR-D59 (the photo of a cut: its members and their
             sequences), ADR-D60 (one universe read by id), ADR-D61 (a file
             stage on the cluster, loaded by the platform), ADR-D58
             (descriptors), ADR-D35 (the eight model ids)

Context: eight models, two and a half million sequences, no vectors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The development database holds the eight stage-1 configurations and no
vectors. Every family reads embeddings of the photos' members: the
nearest-neighbour bank and its queries, the re-ranker's features and the world
model's inputs. The six photos have 2,523,987 distinct member sequences with
1,402,305,064 residues; 265,344 of them are longer than the 1,022 residues the
recipe reads.

The queue path (``compute_embeddings``) runs one sequence per forward pass on
the desktop's RTX 3060. That is the stage-1 regime (``docs/campaign/stage1/
REGIME.md``): byte reproducible, 136 of 136 vectors identical across
processes. It is also slow, and on a cluster card it leaves the card mostly
idle: the eight models together run about 8.4 billion encoder parameters, so the
bank is some 2e19 floating-point operations, a few hours of a B200 when the card
is kept busy and several times that one sequence at a time.

Decision
~~~~~~~~

**1. The population: every member sequence of the six photos, once.** Cuts
160, 177, 194, 205, 211 and 227, keyed by sequence md5 (recomputed from the
sequence). A member whose sequence its photo lacks is counted and named, never
guessed; at 205, a truth-only photo, those are 86,506 members outside
Swiss-Prot. The union is exactly the set of ``cut_photo_member.sequence_md5``
the platform loaded for those photos, and the loader checks that it is.

**2. The eight stage-1 configurations, unchanged.** Last layer, mean over
layers, mean pooling, L2-normalised, ``max_length`` 1022, no chunking
(ADR-D48). The recipes travel to the cluster in ``recipes.json``, exported from
the platform, and every pass refuses a recipe that does not derive its id.

**3. One padded pass per batch.** protea-backends gains
``embed_chunks_padded`` (its PR #50): the batch is padded on the right, the
encoder runs once under the attention mask, and every row is reduced over its
own span by the same function as the one-sequence path. For ESM-2 and ESM-C this
is new; T5 and Ankh always ran a padded batch, and the method is that path under
the shared name. The batches are length-sorted, and rows times the longest row
stay under a token budget.

- A vector now depends on its batch by floating-point rounding: the kernels see
  other shapes. Byte reproducibility across runs is not claimed for this bank.
- The ``compare`` pass measures the cost per model before the full run: a sample
  spread over the length order, embedded both ways, with the identical rows,
  the largest absolute difference, the smallest cosine and the speed of each
  path.
- ``embed_chunks`` does not change, so the queue path and its regime stay as
  they were.

**4. Built on the cluster, as a file stage.** ``protea.core.embedding_bank.build``
mirrors the homology table of ADR-D61:

- ``prepare`` reads the photos where they are and writes the population,
  length-sorted and cut into shards of about eight million tokens.
- ``embed`` runs one configuration on one card over a range of shards. It
  skips the shards already written and stops before a shard it could not
  finish within the job's limit.
- ``compare`` is the measurement of decision 3.
- ``manifest`` writes ``bank-<config>.json``.

The code is shipped as code only (``builder_files.txt``). The weights are
fetched on the login node at a pinned revision, and the hub is offline in the
jobs. The cluster's environment pins the regime of ADR-D48 (torch
2.11.0+cu130, transformers 4.48.1, tokenizers 0.21.4, esm 3.2.1.post1) and
protea-backends at the commit of its padded pass. Every run records its
software and card.

**5. One regime per configuration.** A bank whose shards were computed under
two weights revisions, two software sets or two kinds of card is refused by its
own manifest. All eight run on the cluster's B200 cards.

**6. Rows in the database, declared as a bank.** The vectors are
``sequence_embedding`` rows in half precision.

- The cast is done on the cluster with the store path's rule
  (``scale_and_clip_embedding``, then half precision); a test pins the two to
  each other.
- Each row names its bank in ``sequence_embedding.embedding_bank_id``. The
  column is nullable: NULL is a row the queue computed.
- One ``embedding_bank`` row per (configuration, manifest) records:

  - the photos, by their manifests' sha256;
  - the counts, the weights' commit, the software, the card and the token
    budgets;
  - the manifest, by URI and sha256;
  - the loading status, the job and both revisions;
  - the ADR-D58 descriptors (``bank-<model>-cuts-<cuts>``).

- The shard files are not kept in the store: the rows are the data, and the
  manifest names each shard's sha256.

**7. The loader refuses before writing.** ``load_embedding_bank`` runs on
``protea.jobs``. It refuses unless:

- the manifest hashes as named, is ``complete`` and is the payload's
  configuration;
- the stored configuration is the manifest's recipe, field for field;
- every photo named is loaded;
- every shard hashes as recorded and holds its rows;
- the md5s are unique, each is a ``sequence`` row, and together they are
  exactly the photos' member md5s.

Then it loads shard by shard: a binary COPY into a temporary table, then an
INSERT that keeps, and counts, a row already there. A rerun resumes at the
first shard not loaded, and a loaded bank is a no-op.

**8. Jobs.** Two hours or less, one card each, parallel single-card jobs, no
chains (the cluster rules in force). The pilot is the ``compare`` pass of every
model; the full jobs are sized from its speeds.

Consequences
~~~~~~~~~~~~

- The development database grows by about 20.2 million rows: 9,984 halves per
  sequence over the eight models, some 50 GB of vectors before indexes.
- Everything downstream reads embeddings by configuration, as before. Each row
  says which bank wrote it, and each bank says how it was computed.
- A later photo gets its own bank. The sequences already embedded are kept and
  counted, not recomputed into the same rows.
- The bank's vectors differ from one-sequence vectors by the gap the
  ``compare`` pass measured, recorded per model below.

Measured
~~~~~~~~

The population (prepare, job 3700971, 96 s on 4 cores, peak 3.9 GB):

.. list-table::
   :header-rows: 1
   :widths: 20 20 30 30

   * - Cut
     - Kind
     - Members
     - Member sequences (without one)
   * - 160
     - full
     - 1,131,551
     - 1,012,331 (5)
   * - 177
     - full
     - 1,186,657
     - 1,058,740 (1)
   * - 194
     - full
     - 1,479,088
     - 1,314,437 (4)
   * - 205
     - truth only
     - 651,760
     - 477,262 (86,506)
   * - 211
     - full
     - 1,530,992
     - 1,331,813 (13)
   * - 227
     - full
     - 1,850,077
     - 1,609,561 (0)

The union: 2,523,987 sequences, 1,402,305,064 residues, the longest 35,478,
in 157 shards. The gap and the speeds of decision 3 are added here from the
pilot's ``compare`` records.

Alternatives considered
~~~~~~~~~~~~~~~~~~~~~~~

- **One sequence per pass on the cluster.** Byte reproducible, and most of the
  card idle. Rejected by the author's rule: efficiency, with differences that
  are small.
- **The desktop's RTX 3060.** Weeks of the development node's only card.
  Rejected.
- **Keeping the shard files in the store as well.** It would hold the same data
  twice on a disk that has to last. Rejected; the manifest keeps their sha256.
- **Single-precision files.** Twice the transfer, and the column stores half
  precision anyway. Rejected.
