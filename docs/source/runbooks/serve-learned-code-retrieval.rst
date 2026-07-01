Serve Learned-Code Retrieval (novel queries)
============================================

The validated k-WTA retrieval encoder (config ``d8979601``) stores
GO-aligned codes, not a raw PLM vector. Offline those codes were
materialised by ``apply_learned_encoder`` over BASE embeddings already
pre-computed for every pool protein. A NOVEL ``/annotate`` query has no
pre-computed base embedding, so pinning retrieval to a learned config used
to leave the query un-embeddable (the compute path tried to load
``"learned-code:..."`` as a HuggingFace model and failed).

The serve embed path now embeds learned-code configs on the fly. When
``ComputeEmbeddingsBatchOperation``
(``protea/core/operations/compute_embeddings.py``) sees a
``model_backend="learned-code"`` config it routes to
``protea/core/operations/_learned_code_embed.py``, which:

1. resolves the BASE ``EmbeddingConfig`` the head was trained over,
2. embeds the query with that base config (a standard PLM the backend
   already supports),
3. applies the learned head, reusing ``apply_learned_encoder``'s
   apply-builder (no duplicated k-WTA / attention-pool math),
4. persists the 2048-d codes as ``SequenceEmbedding`` rows under the
   learned config, so subsequent KNN retrieval reuses them (computed once
   per novel query).

Base-config resolution
----------------------

``apply_learned_encoder`` names a learned config
``"{target_model_name}:{pool_tag}:{objective}:{source_id[:8]}"`` (for
example ``"learned-code:hard-neg:08234f06"``). The trailing colon-segment
is the first 8 hex chars of the SOURCE (base) config id, so the base config
is recovered by matching that id prefix. No path or id is hard-coded in the
serve code, and the base config must already be embedded in the pool.

Enabling it
-----------

Set both the retrieval pin and the head artifact in the serve environment:

- ``PROTEA_DEFAULT_EMBEDDING_CONFIG_ID`` (or
  ``PROTEA_TUNING__serve__default_embedding_config_id``): the learned config
  UUID, for example ``d8979601-ea59-4de1-9c16-21036ed67c36``.
- ``PROTEA_LEARNED_ENCODER_ARTIFACT``: an explicit path to the head ``.pt``
  blob (for ``d8979601`` this is ``ankh_base_hardneg.pt``), OR
- ``PROTEA_LEARNED_ENCODER_DIR``: a directory searched for
  ``<config_id>.pt`` then ``<config_id[:8]>.pt``.

The pin is honoured only when the config exists AND already has embeddings
(pool codes), so a stale or typo'd pin can never break serve. Unset, the
serve path keeps the legacy smallest-param auto-pick.

Failure modes
-------------

The learned path fails fast with a clear ``ValueError`` (it never hangs):

- head artifact unset or file missing: ``needs a head artifact`` /
  ``does not exist`` / ``no learned head for config``.
- base config not embedded yet: ``no base EmbeddingConfig with id prefix``.
- ambiguous base id prefix: ``id prefix ... is ambiguous``.

Standard PLM configs (esm / ankh / t5 / esm3c) are unaffected: the
learned-code branch is entered only when
``model_backend == "learned-code"``.
