Computational Complexity
========================

This section is the **developer-facing reference** for PROTEA's computational
complexity. It covers the Big-O profile of every pipeline stage, measured hot
paths, and practical advice for profiling.

The corresponding academic narrative (derivations, empirical scaling plots,
and the ablation study that justifies the numpy + FAISS design) lives in
thesis Chapter 5 ("Computational Analysis"), which is outside the Sphinx
tree. The two documents are complementary: this reference tells you *where*
the cost lives and *how* to measure it; the thesis explains *why* the current
design was chosen over the alternatives.

.. rubric:: Division of labour

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - This Sphinx section
     - Thesis Ch. 5
   * - Function signatures + source anchors
     - Formal Big-O derivations
   * - Measured wall-clock numbers (EXP.13)
     - Empirical scaling plots
   * - Profiling recipes (scalene, etc.)
     - Architecture motivation
   * - Forward-ref to PERF.1 flamegraphs
     - Comparison to pgvector / GPU KNN

.. toctree::
   :maxdepth: 2

   pipeline_overview
   plm_attention
   faiss_knn
   lightgbm
   anc2vec
   export_pipeline
   measuring
