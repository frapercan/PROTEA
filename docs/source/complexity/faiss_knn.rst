KNN Search Complexity
======================

.. note::

   **Dominant cost (exact): O(q * n * d) per aspect.**

   For the numpy and FAISS Flat backends the search is an exact brute-force
   nearest-neighbour retrieval. With ``q`` query vectors, ``n`` reference
   vectors of dimension ``d``, the dot-product matrix is ``(q, n)`` and each
   entry requires ``d`` multiply-adds: O(q * n * d) total. For PROTEA's v226
   corpus (n ~ 530k, d = 480 for ESM-2 150M, q ~ 5k per aspect) this is
   roughly 1.3 * 10^12 FLOPs per KNN call, handled in chunks to cap RAM.

PROTEA's KNN is a thin shim over ``protea_method.knn_search`` (extracted in
the F2C.5 refactor). The shim lives at
``protea/core/knn_search.py``; new code should import
``protea_method.knn_search.search_knn`` directly.

.. rubric:: Backend selection

Three backends are available via the ``backend`` parameter of ``search_knn``:

``"numpy"`` (default for export)
   Exact, chunked cosine or L2 via NumPy matrix multiplication. No extra
   dependencies. Query chunk size is controlled by
   ``PROTEA_METHOD_NUMPY_QUERY_CHUNK`` (default 500). A 500-query chunk
   against 530k references at float32 costs ~1 GB RAM, staying comfortably
   inside the export worker's budget.

``"faiss"``
   Wraps FAISS. ``"Flat"`` is exact (same asymptotic as numpy, ~3-10x
   faster via BLAS). ``"IVFFlat"`` and ``"HNSW"`` are approximate;
   not used in PROTEA production runs (the accuracy loss is unacceptable
   for the temporal-holdout evaluation).

``"torch"``
   Chunked GPU/CPU KNN via ``torch.cdist`` + ``torch.topk``. Recommended
   for production runs where PyTorch is already loaded in the process (e.g.,
   LAFA containers). Device selection: ``PROTEA_KNN_DEVICE`` (default
   ``"auto"``); chunk size: ``PROTEA_KNN_CHUNK_SIZE`` (default 4096).

.. rubric:: Top-k selection optimisation

The numpy backend avoids a full O(n log n) ``argsort`` by using
``np.argpartition`` (O(n) per query) to isolate the top-k candidates,
then sorting only that k-slice:

.. code-block:: python

   # Chunk-invariant work computed once, reused across all query chunks
   R_T = R_ready.T
   for start in range(0, n_queries, query_chunk):
       Q_chunk = Q[start : start + query_chunk]
       dist = 1.0 - (Q_n @ R_T)          # (chunk, n_refs) matrix
       # argpartition is O(n_refs) per row; ~30x faster than full argsort for k << n_refs
       part = np.argpartition(dist, k - 1, axis=1)[:, :k]
       # sort the k-slice only
       del dist

For ``k = 5`` and ``n = 530k``, argpartition is approximately 30x faster
than a full sort on 500k-element arrays.

.. rubric:: RAM usage

The ``(q, n)`` distance matrix is float32. For the default chunk of 500
queries and n = 530k: 500 * 530000 * 4 B = 1.06 GB. This is the dominant
allocation in the export worker and is freed at the end of each chunk loop
via ``del dist``.

.. rubric:: Design constraint

pgvector is explicitly excluded from KNN search in PROTEA (see
``docs/source/adr/001-knn-without-pgvector.rst``). The hard rule is:
for reference pools larger than 500k vectors, use numpy or FAISS only.

.. rubric:: Cross-reference

Thesis Ch. 5.2 contains the measured throughput curve (queries/s vs n)
comparing numpy chunked, FAISS Flat, and FAISS IVFFlat on the v226
corpus, and justifies the numpy-default decision.
