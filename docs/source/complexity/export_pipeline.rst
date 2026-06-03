Export Pipeline Complexity
===========================

.. note::

   **Hot path: O(q * k) pairwise sequence alignments, parallelised
   across a process pool (PR #421).**

   The ``export_research_dataset`` operation materialises
   ``train.parquet`` + ``eval.parquet`` for the reranker lab. Its runtime
   is dominated by the pairwise NW+SW alignment step in
   ``_KnnTransferRunner._compute_pair_features``: for each (query,
   reference) pair in the KNN result, a full Needleman-Wunsch traceback
   is computed via parasail (BLOSUM62, gap-open 10, gap-extend 1). With
   q query proteins and k neighbours per query there are q * k unique
   pairs to align. At the FARM-EXP.13 scale (q ~ 5000, k = 10) this is
   50k alignments per aspect, each taking ~2 ms single-threaded, for a
   total of ~100 s per aspect before parallelisation.

Two structural optimisations landed in PR #421:

1. **Process-level parallelism.** Alignments are distributed across a
   ``ProcessPoolExecutor`` (``PROTEA_PAIR_FEATURE_WORKERS``, default:
   serial). parasail's traceback variants do not release the GIL
   efficiently, so threads do not scale; processes do (~3x on a 12-core
   box).

2. **Persistent on-disk cache.** A SQLite file keyed by the ordered
   sequence pair plus alignment parameters stores computed alignment
   features. Because the K=10 neighbour set is a superset of K=5 and
   K=3, the first run at K=10 pays the alignment cost; subsequent runs
   at smaller K are near-free cache hits.

.. literalinclude:: ../../../protea/core/_pair_feature_compute.py
   :language: python
   :lines: 1-30

.. rubric:: Stage-by-stage breakdown

The operation is orchestrated in three session windows (see
``ExportResearchDatasetOperation.execute``). Within the second window,
``_dump_to_stage`` calls the internal ``_KnnTransferRunner``:

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Sub-step
     - Complexity
     - Parallelism
   * - Load embeddings from DB (``SequenceEmbedding``)
     - O(n + q) rows
     - Single DB query
   * - KNN search (numpy backend, per aspect)
     - O(q * n * d) per aspect
     - Serial (one aspect at a time)
   * - GO transfer
     - O(q * k * T)
     - Vectorised
   * - Pairwise alignment
     - O(q * k)
     - ProcessPoolExecutor + SQLite cache
   * - Taxonomy features
     - O(q * k) with ete3 lru_cache
     - In-process (cheap)
   * - anc2vec lookup
     - O(q * k * T)
     - O(1) per term via hash table
   * - PCA projection
     - O((n + q) * d * p) fit; O(q * d * p) transform
     - NumPy (p = 16 components, transductive)
   * - Parquet write + MinIO upload
     - O(rows)
     - Sequential (single connection)

.. rubric:: Environment variables

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Variable
     - Effect
   * - ``PROTEA_PAIR_FEATURE_WORKERS``
     - Number of process-pool workers for alignment (default: 1, serial)
   * - ``PROTEA_ALIGN_CACHE_DIR``
     - Directory for the persistent SQLite alignment cache (unset disables)
   * - ``PROTEA_METHOD_NUMPY_QUERY_CHUNK``
     - Per-chunk query count for the numpy KNN backend (default: 500)
   * - ``PROTEA_ANC2VEC_PATH``
     - Path to the anc2vec NPZ artifact (required for export to succeed)

.. rubric:: Forward reference

Scalene flamegraphs for the full FARM-EXP.13 run (24-cell grid, 8 PLMs
x 3 K values) will be published under ``docs/perf/`` as part of the
upcoming PERF.1 profiling slice. They will show the fraction of wall
clock consumed by each sub-step and guide future parallelisation work.

.. rubric:: Cross-reference

Thesis Ch. 5.5 presents end-to-end timing measurements for the EXP.13
export grid, the alignment cache hit rate per PLM, and the performance
model used to predict export duration for larger corpora.
