ADR-001: KNN on CPU, not pgvector or GPU
========================================

:Date: 2025-12-15
:Author: frapercan

The problem
-----------

GO term prediction requires K-nearest-neighbor search over 500K+ embeddings
of 1280 dimensions.  The natural options were ``pgvector`` (we already store
vectors there) or PyTorch on GPU (we already have the GPU for inference).
Both failed:

- **pgvector** with an IVFFlat index on 527K vectors: index build took
  >20 minutes, and each individual query cost 100-500ms.  For a job with
  thousands of queries, unacceptable.
- **PyTorch on GPU**: the GPU is busy with ESM-2/ESM-3c/T5 inference.
  Loading the distance matrix competes with model forward passes and
  causes CUDA OOM.

What we do
----------

KNN runs **on CPU**, entirely in Python:

- **NumPy** (brute-force via matrix multiplication) for small datasets
  (<100K).
- **FAISS** (Flat, IVFFlat, HNSW) for large datasets.  Uses SIMD and
  multithreading on CPU without touching the GPU.

Reference embeddings are loaded once from PostgreSQL into a process-level
cache (``_REF_CACHE``, float16, ~4 GB for 500K vectors).  ``pgvector``
remains as storage only — the ``VECTOR`` type is there, but we never
search with ``<=>``.

Trade-offs
----------

- The cache consumes worker RAM (~4 GB).  If the worker restarts, the
  first prediction takes ~15s extra to reload from DB.
- KNN and inference run in parallel without contention: CPU computes
  distances while GPU computes embeddings.

Rejected
--------

- **Dedicated vector database** (Milvus, Qdrant): one more infra
  dependency for something NumPy/FAISS solves in-process.
- **Persistent FAISS index on disk**: IVFFlat training takes a few
  seconds; not worth the complexity of serialising/deserialising for now.
