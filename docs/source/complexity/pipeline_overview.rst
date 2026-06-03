Pipeline Overview: Big-O per Stage
====================================

The table below summarises the asymptotic cost of every major PROTEA
stage. Symbols used throughout this section:

.. list-table::
   :header-rows: 1
   :widths: 12 88

   * - Symbol
     - Meaning
   * - ``N``
     - Number of sequences in the corpus (reference + query)
   * - ``L``
     - Sequence length (residues). ESM-2 / ESM-C cap at 1024; T5/ProstT5 cap
       at the configured ``max_length``.
   * - ``d``
     - Embedding dimension (320–2560 depending on PLM)
   * - ``n``
     - Reference pool size (the "train" snapshot). Typically 300k–550k
       in the PROTEA v226 corpus.
   * - ``q``
     - Query pool size (the "test" snapshot). Typically 3k–15k.
   * - ``k``
     - Number of nearest neighbours (3, 5, or 10 in the FARM-EXP.13 grid)
   * - ``T``
     - Number of candidate GO terms transferred per (query, reference) pair
   * - ``F``
     - Number of features in the LightGBM feature vector (fixed at schema
       compile time; currently O(100))

.. list-table:: Big-O summary by pipeline stage
   :header-rows: 1
   :widths: 28 30 28 14

   * - Stage
     - Dominant cost
     - Notes
     - Page
   * - PLM forward pass (embedding)
     - O(N * L^2 * d) for attention-based PLMs
     - Per sequence; chunked to cap peak VRAM
     - :doc:`plm_attention`
   * - KNN search (numpy backend)
     - O(q * n * d) per aspect
     - Matrix multiply + argpartition; chunked over queries
     - :doc:`faiss_knn`
   * - KNN search (FAISS Flat)
     - O(q * n * d)
     - Same asymptotic; 3-10x faster constant via BLAS
     - :doc:`faiss_knn`
   * - KNN search (FAISS IVFFlat / HNSW)
     - O(q * sqrt(n) * d) approximate
     - Recall trade-off; not used in PROTEA production
     - :doc:`faiss_knn`
   * - GO transfer + feature engineering
     - O(q * k * T) + alignment O(q * k)
     - Alignment is the hot path; cached + parallelised in PR #421
     - :doc:`export_pipeline`
   * - anc2vec lookup
     - O(1) per GO term (hash table)
     - 200-dim vectors; entire index loaded into RAM once
     - :doc:`anc2vec`
   * - LightGBM inference
     - O(q * k * T * F * depth)
     - Linear in candidates; negligible vs KNN
     - :doc:`lightgbm`

.. rubric:: End-to-end: FARM-EXP.13 measured times

The 24-cell grid (8 PLMs x 3 K values) exported from the v226 corpus was
timed on the production stack (single A100, 48-core CPU, 256 GB RAM). The
bottleneck per cell was the GPU embedding pass for the two PLMs with the
longest sequences (ProstT5-XL, ESM-C 600M); the KNN and feature steps
were always sub-10% of total wall clock.

Detailed flamegraphs are planned under ``docs/perf/`` as part of the
upcoming PERF.1 profiling slice (scalene).

.. seealso::

   Thesis Ch. 5 for the full derivation and empirical scaling plots.
