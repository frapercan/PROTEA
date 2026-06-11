D41 - Lean f32 reference pool for high-dimensional PLMs
========================================================

:Status: Accepted
:Date: 2026-06-11
:Deciders: Francisco Miguel Pérez Canales
:Context: fix(predict): materialise only the f32 ref copy each run reads
          (commit 0948dc6, fix/refpool-lean-f32-highdim-plm)

.. rubric:: Context

The KNN reference loader (``_ReferenceMixin`` in
``protea/core/operations/predict_go_terms/_batch_op_reference.py``) and
the helper ``_derive_reference_views`` in ``protea/core/disk_cache.py``
previously materialised **three** float32 copies of every embedding
matrix for the unified pool and all per-aspect slices:

- ``embeddings`` (float16, base)
- ``embeddings_f32`` (raw float32 upcast)
- ``embeddings_f32_cos`` (L2-normalised float32)

Plus a per-aspect float16 slice for each of BPO, MFO, and CCO.

For a 2560-dim PLM such as ESM2-3B with a 500 K-vector reference pool,
each float32 matrix is roughly 5 GB. Keeping all three copies in the
process cache consumed approximately 53 GB of RAM, pushing the
development box to 99% utilisation and triggering ``systemd-oomd`` kills
in a load-die-reload loop that never advanced the prediction job.

.. rubric:: Decision

Thread two boolean flags, ``need_cos`` and ``need_plain``, through the
entire reference-loading call chain:

- ``need_cos`` is ``True`` when ``metric == "cosine"`` (the KNN search
  reads ``embeddings_f32_cos``).
- ``need_plain`` is ``True`` when the metric is not cosine, **or** when
  the embedding-PCA state file for this ``EmbeddingConfig`` is absent
  from disk. The PCA fit consumes the raw float32 pool; once cached the
  artefact is loaded from disk and the plain copy is not needed.

The flags are resolved once per batch by the static helper
``_reference_dtype_needs(p)`` and included in the ``_REF_CACHE`` key so
that a cosine run and a non-cosine run on the same worker never share a
pool that materialised only one metric's copy.

``_derive_reference_views`` sets the unused slot to ``None``; downstream
readers already select by metric, and the PCA pool builder guards with
``is not None``. The per-aspect float16 slice is dropped entirely; no
consumer read it.

.. rubric:: Consequences

**Positive**

- ESM2-3B peak RAM drops from approximately 53 GB to approximately 21 GB,
  keeping the full reference pool inside available memory on the
  development box.
- Prediction features (same cached PCA state, same ``f32_cos`` KNN
  distances) are unchanged; results are identical.
- The cache key is slightly wider but the LRU eviction logic is
  unchanged.

**Negative / constraints**

- A run switching between cosine and non-cosine metrics on the same
  worker will miss the cache (different ``need_cos`` / ``need_plain``
  values). This is intentional: serving the wrong copy silently would
  corrupt results.
- When the PCA artefact is absent, both ``need_cos`` and ``need_plain``
  may be ``True`` simultaneously (cosine run on first boot), so the
  saving only materialises after the first PCA fit completes and the
  artefact is written to disk.

**Affected code paths**

- ``protea/core/disk_cache.py`` :: ``_derive_reference_views``
- ``protea/core/operations/predict_go_terms/_batch_op_reference.py`` ::
  ``_ReferenceMixin._ensure_reference_cache``,
  ``_reference_dtype_needs``,
  ``_load_reference_data``,
  ``_load_reference_data_per_aspect``,
  ``_assemble_all_aspect_views``
