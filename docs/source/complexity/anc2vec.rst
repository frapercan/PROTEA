Anc2Vec Complexity
===================

.. note::

   **Lookup: O(1) per GO term (hash table). Batch lookup: O(N) in the
   number of GO terms.**

   The anc2vec index is a 200-dimensional float32 embedding matrix for
   the GO release of 2020-10-06, loaded once into RAM and accessed via
   a Python dict (hash table). Single-term lookup is O(1); the batch
   variant allocates an (N, 200) output matrix and fills it row-by-row,
   so it is O(N) in the number of requested GO terms. The full index
   occupies approximately 25 MB in RAM.

The implementation is in ``protea_method.anc2vec`` (extracted from
``protea/core/anc2vec_embeddings.py``):

.. literalinclude:: ../../../protea/core/anc2vec_embeddings.py
   :language: python
   :pyobject: get_index

.. rubric:: Runtime characteristics

The ``@lru_cache(maxsize=2)`` decorator on ``get_index`` ensures the
index is loaded from disk once per process (keyed by path), after which
all downstream callers share the same in-memory object. The ``.npz``
file is loaded with ``np.load(..., allow_pickle=True)``; the embeddings
array is made contiguous with ``np.ascontiguousarray`` so that
column-slicing in ``batch()`` avoids copies.

.. rubric:: Usage in the export pipeline

During feature engineering, ``batch()`` is called for the transferred GO
terms of each (query, reference) pair. For k = 5 neighbours and T ~ 30
GO terms per pair, a single query generates 150 anc2vec lookups.
Across q ~ 5000 queries per aspect this is 750k lookups per KNN call,
all O(1) via the hash table, typically completing in milliseconds.

.. rubric:: Artifact path

The NPZ file lives at ``PROTEA/artifacts/anc2vec/anc2vec_2020-10.npz``
and is resolved at runtime by the ``PROTEA_METHOD_ANC2VEC_PATH``
environment variable (or ``PROTEA_ANC2VEC_PATH`` in the PROTEA worker
context). The embedding dimension is 200 (not 256; verify with
``Anc2VecIndex.dim`` before consuming it downstream).

.. rubric:: Cross-reference

Thesis Ch. 5.3 covers the role of anc2vec features in the LightGBM
ensemble and the GeOKG no-go decision (2026-05-17): anc2vec remains
the default GO embedding until a full-text Fmax gain greater than 0.005
can be demonstrated.
