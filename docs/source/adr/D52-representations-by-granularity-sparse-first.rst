ADR-D52: Representations by granularity in one registry, sparse first
=====================================================================

:Status: Proposed (draft for the author's review)
:Date: 2026-11-03
:Author: Francisco Miguel Pérez Canales
:Phase: platform consolidation (the lab's research enters the platform)
:Depends on: ADR-D51 (per-residue codes from a frozen vocabulary)

Context: one table for vectors, and the next representations are not vectors per sequence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The platform registers recipes in ``embedding_config`` and stores their output in
``sequence_embedding``: one dense half precision vector per sequence (or per chunk).
Everything the platform retrieves, re-ranks and evaluates passes through that pair.

Three facts say the pair is no longer enough.

1. **Sparse codes are stored dense.** The ``learned-code`` bank is 8192 dimensions with
   64 non-zeros per row. Measured on 2026-10-28 over 684048 sequences: 22.41 GB as a
   dense float32 copy against 0.35 GB as CSR, so ``predict_go_terms`` rebuilds a sparse
   bank in memory from rows that were stored dense (``_sparse_bank.py``).
2. **ADR-D51 adds a second granularity.** The code of every residue goes to shards in the
   artifact store with a ``residue_code`` index, because it cannot be rows.
3. **The lab's line produces more kinds.** Sequence summaries of residue codes, regions
   (the region JEPA), term embeddings per cut, and named features (the atlas: 1335
   features named after a domain and 337 after a site, out of 8192). Today the canonical
   copies live in lab caches that the platform cannot see, and the platform holds nothing
   of what explains a prediction.

The question is not whether to rewrite the schema. It is how to add these without a
parallel registry for each, and without moving what already works.

Decision (proposed)
~~~~~~~~~~~~~~~~~~~

1. One registry, two new columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``embedding_config`` gains ``granularity`` (``sequence``, ``residue``, ``region``,
``term``) and ``storage`` (``dense``, ``sparse``, ``shards``). Every existing row is
``sequence`` / ``dense``. ADR-D51's configurations are ``residue`` / ``shards``.

They are NOT identity fields. ``derive_embedding_config_id`` hashes the recipe, and adding
a field to it reissues every id in the registry, which is a migration of every row that
references one. A configuration's granularity and storage follow from its producer, so
they describe the recipe without being part of what makes it distinct.

2. Per-protein representations of residue codes are learned, not pooled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The author, 2026-11-03: "Queriamos evitar las agregaciones que pierdan mucha
información", and "el objetivo es no utilizar agregaciones max mean... sino learned
representations mejor si fuese posible".

* **Nothing fixed is stored per protein.** No maximum or mean of the residue codes is
  written as a representation of record. A fixed pooling loses where a feature fires, how
  often it repeats and which features occur together, which is what the aggregation pillar
  of the thesis studies.
* **A per-protein representation, when a consumer needs one, is learned.** It is the
  output of an aggregator fitted in the lab over the residue codes (attention pooling, the
  reader's own aggregation, or the term generator), entered as an artifact with its hashes
  and cut, registered as a configuration of kind ``learned`` and granularity ``sequence``,
  and computed by a platform operation that reads the residue store.
* **Fixed pooling stays a baseline.** The maximum and the mean are measured against the
  learned aggregator on the 211 to 227 frame, stratified by length, identity and panel as
  every platform evaluation is, and are never the representation a consumer reads.
* **Retrieval does not need a pooled vector.** Neighbours can be found per region (one
  sparse vector per domain or window, the best match among regions) and measured against
  the residue-level match.

A new table ``sequence_sparse_embedding`` (sequence, configuration, ``sparsevec``,
non-zero count) holds the sparse per-protein vectors that a learned aggregator produces.
pgvector's ``sparsevec`` stores an index and a value per non-zero (8 bytes) and supports
nearest neighbour search with an HNSW index for up to 1000 non-zeros. It needs pgvector
0.7.0 or later on the server.

* **``learned-code``** can move to this table later, which removes the in-memory CSR
  rebuild. That is a separate migration with its own parity check.

3. A feature catalog
^^^^^^^^^^^^^^^^^^^^

``vocabulary_feature`` holds one row per feature of a residue vocabulary: configuration,
feature index, layer group, activation frequency and mean value, and what the atlas
says: the kind of concept (domain, site, none), its identifier and name, the average
precision with which the feature localises it, and the atlas version. An operation
imports it from the lab's atlas as an artifact. Explanations read it, so a prediction can
name the features that carry a term ("feature 4031, protein kinase domain, average
precision 0.83") from the platform alone.

4. An inverted index, only as wide as its consumers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``sequence_feature`` (configuration, feature, sequence, value) keeps, for each sequence,
its strongest features (64 by default), which is about 44 million rows for the corpus.
It answers "which sequences carry this feature" for explanations, atlas audits and
retrieval by feature. A full inverted index would be the residue store again. It is
created only when one of those consumers exists.

5. Lab caches become views
^^^^^^^^^^^^^^^^^^^^^^^^^^

A cache built in the lab from platform data names, in its manifest, the configuration
ids and the hashes of what it was built from, and it can be rebuilt from the platform.
A cache that cannot is a lab artifact, not platform data, and nothing in the platform
reads it.

What stays outside
~~~~~~~~~~~~~~~~~~

Training. Dictionaries, generators, readers and their weights are fitted in the lab and
on the cluster, and they enter as artifacts with their hashes and cuts, as ADR-D51's
vocabulary does. The platform registers, stores, reads, predicts and evaluates.

Consequences
~~~~~~~~~~~~

* **Storage.** A learned sparse per-protein vector at up to 1000 non-zeros is at most
  8 KB per sequence, about 5.5 GB for the corpus, or 1.4 GB at 256. The catalog is 8192 rows per vocabulary. The
  inverted index at 64 features per sequence is about 44 million rows.
* **Nothing existing moves.** ``sequence_embedding`` and its readers are untouched. Every
  new table is additive, and each comes with the operation that needs it.
* **Order.** ADR-D51 first (residue codes). Then the first learned aggregator, once it
  beats fixed pooling on the 211 to 227 frame, stratified. Then the catalog, with the first explanation in the
  platform. The inverted index comes last, and only if a consumer asks for it.

Open questions for the author
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Settled on 2026-11-03 (section 2): no fixed summary; learned per-protein
   representations, with fixed pooling as a baseline.
2. The population of the residue store: the whole corpus, or the annotated sequences and
   the queries the readers need. The first corpus pass covers the whole bank.
3. Whether ``learned-code`` migrates to ``sparsevec``.
