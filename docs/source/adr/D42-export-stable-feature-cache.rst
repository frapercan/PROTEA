D42 - Export decouple via a content-addressed stable-feature cache
==================================================================

:Status: Accepted
:Date: 2026-06-16
:Deciders: Francisco Miguel Pérez Canales
:Context: NFR-ARCH improvement so classifier A/B testing stops re-running the
          full ~8h dataset export (feat/export-stable-feature-cache)

.. rubric:: Context

``export_research_dataset`` runs the KNN + feature pipeline over every
temporal snapshot pair and publishes ``train.parquet`` / ``eval.parquet`` /
``manifest.json`` for the lab. The real ``fullgo-native-parity-SELECT-220-227``
export was 53.8M train rows over 12 snapshot pairs (v160..v220), test 227, with
the 6-PLM 7-seed classifier feature per candidate. The classifier feature is
the slow part; everything else (KNN, alignment, taxonomy, anc2vec, lineage,
emb_pca, self_prior, association) is the stable, classifier-independent bulk.

A/B testing classifier variants (7-seed Deep Ensemble vs EMA vs SWA vs
late-fusion) historically meant re-running the whole export for each variant,
because the classifier columns are baked into the same parquet as the stable
columns. That is hours of recompute for a column family that takes minutes.

.. rubric:: Decision

Split the export's per-candidate feature columns into two classes and cache the
stable half content-addressed.

**STABLE** columns depend only on the *stable config*: ``embedding_config_id``,
``ontology_snapshot_id``, ``train_versions`` / ``test_versions``, ``k``,
``distance_threshold`` / ``metric`` / ``search_backend`` / FAISS knobs, and the
stable feature flags (``compute_alignments``, ``compute_taxonomy``,
``expand_votes_to_ancestors``, ``use_embedding_pca``, ``compute_self_prior``,
``compute_association``). They are byte-identical across classifier swaps.

**VARIABLE** columns are ``classifier_score`` / ``classifier_present`` (and the
future per-variant ``classifier_score__<label>`` families). They change when
the classifier model changes and nothing else does.

The cache key (``training_dump._stable_feature_cache.compute_stable_cache_key``)
is a SHA-256 over exactly the stable inputs above plus a
``STABLE_CACHE_PRODUCER_VERSION``. The classifier flags and variant specs are
deliberately ABSENT from the key, which is the whole point: a classifier A/B
reuses the same stable table. The stable ``train.parquet`` / ``eval.parquet`` /
``manifest.json`` are stored under ``stable_features/<hash>/`` in the configured
``ArtifactStore`` (local FS in dev, MinIO in prod) next to a
``cache_manifest.json`` recording the resolved key inputs.

Behind the ``stable_feature_cache`` payload flag (default False):

- **cache MISS**: run the heavy pipeline ONCE with the classifier forced OFF, so
  the staged parquet is purely stable. Upload it to ``stable_features/<hash>/``.
  Then re-stamp the classifier column(s) for the requested classifier.
- **cache HIT**: fetch the cached stable parquets, skip the KNN + feature
  pipeline entirely, and re-stamp the classifier column(s) for the requested
  classifier.

The classifier re-stamp (``training_dump._classifier_postpass``) reuses the
predict-path producer (``classifier_producer.load_concat_features`` /
``get_classifier`` / ``resolve_go_term_ids``) on the consolidated frame keyed by
``(protein_accession, go_term_id)``, so the values are identical to what the
inline applier (``_export_features._mark_classifier_candidates``) stamps. The
classifier reads ONLY the query protein's frozen PLM embeddings, never an
annotation set, so the post-pass is leakage-equivalent to the inline producer
regardless of which stable table it re-stamps.

.. rubric:: Multi-classifier-column API (phase 2)

The payload also carries ``classifier_variants``: a list of variant specs, each
a label plus a seed-dir / seed-paths / model-path selector (mirroring
``PROTEA_CLASSIFIER_SEED_DIR`` / ``_SEED_PATHS`` / ``_MODEL_PATH``). When given,
the export emits one ``classifier_score__<label>`` / ``classifier_present__<label>``
family per variant in a single pass over the cached stable table, so the lab
trains one booster per variant from the same parquet. With no variants the
export keeps the canonical single ``classifier_score`` / ``classifier_present``
columns, so existing lab boosters are unaffected. Phase 1 ships the
content-addressed stable cache and the single-classifier re-stamp (the bigger
lever); the multi-column emission is sketched (``ClassifierVariantSpec``, the
``score_column`` / ``present_column`` parameters on
``apply_classifier_to_frame``, and the ``_classifier_for_variant`` seam) and
left as a clearly-marked TODO rather than half-wired.

.. rubric:: Invalidation

The cache is content-addressed, so invalidation is implicit: any change to a
stable axis produces a different key and a fresh table. The
``STABLE_CACHE_PRODUCER_VERSION`` constant is the manual escape hatch, bumped
only when the stable producers change their output for an unchanged config (so
every existing cached table is routed around rather than served wrong). Stale
``stable_features/<hash>/`` blobs are inert once the key moves on; pruning is a
storage-housekeeping concern, not a correctness one.

.. rubric:: Consequences

- A classifier A/B drops from the full ~8h export to the classifier-compute
  time alone once the stable table is cached.
- The default export path (flag off) and the golden parquet are byte-identical:
  the cache branch is entered only when ``stable_feature_cache`` is set, and the
  stable columns are produced by the unchanged inline pipeline.
- One extra copy of the stable parquets lives under ``stable_features/<hash>/``.
  On MinIO this is cheap; on the dev FS it is the same magnitude as the
  published dataset and prunable.
- No contracts release is needed: the new fields live on the PROTEA-side
  ``TrainRerankerAutoPayload`` / ``ExportResearchDatasetPayload``, not in
  ``protea-contracts``.
