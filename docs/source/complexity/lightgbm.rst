LightGBM Complexity
====================

.. note::

   **Inference: O(C * depth) per candidate, where C = q * k * T.**

   For each (query, reference, GO-term) candidate triple, the LightGBM
   booster traverses a fixed tree ensemble of ``depth`` levels. With ``q``
   query proteins, ``k`` nearest neighbours, and ``T`` transferred GO terms
   per (query, reference) pair, there are at most C = q * k * T rows in the
   scoring DataFrame. Tree depth is a training hyperparameter (typically 6);
   the feature vector has F columns (fixed at schema compile time; currently
   O(100)). Inference is dominated by the KNN and alignment steps; the
   LightGBM call is rarely more than 5% of total export wall clock.

.. rubric:: Architecture note

LightGBM training is **not** done inside PROTEA. The pipeline is:

1. ``export_research_dataset`` generates ``train.parquet`` + ``eval.parquet``
   and uploads them to the artifact store.
2. ``protea-reranker-lab`` downloads the parquets, trains a booster, and
   writes ``model.txt``.
3. ``POST /reranker-models/import`` or ``import-by-reference`` registers the
   booster in the PROTEA DB (``RerankerModel`` row), linking it back to the
   source ``Dataset`` via ``dataset_id`` and pinning ``feature_schema_sha``
   for schema-drift detection.
4. Inference calls ``predict`` from ``protea_method.reranker``:

.. literalinclude:: ../../../protea/core/reranker.py
   :language: python
   :lines: 87,94,95,96,97,98,99,100,101,102

.. rubric:: Schema-drift guard

The booster refuses to score a feature vector whose column layout
(captured by ``feature_schema_sha``) differs from the layout it was
trained on. This makes the schema an explicit versioning contract between
PROTEA's export pipeline and the lab's training code.

.. rubric:: Training complexity

Training happens in ``protea-reranker-lab`` (offline). The dominant cost is
the GBDT boosting loop: for ``m`` boosting rounds, ``n_train`` rows, and
``F`` features, the complexity is O(m * n_train * F). For the v226 corpus a
single-PLM dataset has ~1M training rows; 400 rounds at 100 features runs in
under 10 minutes on CPU.

.. rubric:: Cross-reference

Thesis Ch. 5.4 covers the feature importance analysis, the LM.3 stability
result (9 aspect-stable features dominate), and the selective-deploy decision
for NK + LK categories.
